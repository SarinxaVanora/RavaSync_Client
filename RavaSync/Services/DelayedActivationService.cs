using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;

namespace RavaSync.WebAPI.Files
{
    public sealed class DelayedActivatorService : IDisposable
    {
        private readonly ILogger<DelayedActivatorService> _logger;
        private readonly IFramework _framework;
        private readonly MareConfigService _cfgSvc;
        private readonly IpcCallerPenumbra _penumbra;
        private readonly FileCompactor _fileCompactor;
        private readonly FileCacheManager _fileDbManager;
        private readonly MareMediator _mareMediator;
        private readonly DalamudUtilService _dalamudUtil;
        private readonly IObjectTable _objectTable;
        private const int MaxFilesPerFrame = 2;
        private readonly List<PendingFile> _pendingDrain = new(MaxFilesPerFrame);
        private readonly List<PendingFile> _pendingBatch = new(MaxFilesPerFrame);

        private readonly ConcurrentDictionary<string, byte> _pendingHashes = new(StringComparer.OrdinalIgnoreCase);

        private sealed record ApplyResult(PendingFile File, ApplyOutcome Outcome, string? Error = null);

        private enum ApplyOutcome
        {
            Success,
            Requeue,
            Fail
        }

        private readonly ConcurrentQueue<PendingFile> _applyWorkQueue = new();
        private readonly ConcurrentQueue<ApplyResult> _applyCompletedQueue = new();
        private readonly SemaphoreSlim _applySignal = new(0, int.MaxValue);
        private readonly CancellationTokenSource _applyCts = new();
        private readonly Task _applyWorker;

        private sealed class ActorRedrawState
        {
            public readonly object Gate = new();
            public DateTime FirstTouchUtc;
            public DateTime LastTouchUtc;
            public DateTime DueUtc;
            public DateTime LastRedrawUtc;
        }

        private readonly ConcurrentDictionary<nint, ActorRedrawState> _actorRedrawStates = new();


        private readonly Random _rng = new();

        private SafetyGate? _gate;
        private readonly ConcurrentQueue<PendingFile> _pending = new();

        public string QuarantineRoot { get; }

        public DelayedActivatorService(
            ILogger<DelayedActivatorService> logger,
            IFramework framework,
            MareConfigService cfgSvc,
            IpcCallerPenumbra penumbra,
            FileCompactor fileCompactor,
            FileCacheManager fileDbManager,
            MareMediator mareMediator,
            DalamudUtilService dalamudUtil,
            IObjectTable objectTable)
        {
            _logger = logger;
            _framework = framework;
            _cfgSvc = cfgSvc;
            _penumbra = penumbra;
            _fileCompactor = fileCompactor;
            _fileDbManager = fileDbManager;
            _mareMediator = mareMediator;
            _dalamudUtil = dalamudUtil;
            _objectTable = objectTable;

            // quarantine outside hot-read roots to avoid mid-frame reads
            QuarantineRoot = Path.Combine(cfgSvc.ConfigurationDirectory, "_ravasync_quarantine");
            Directory.CreateDirectory(QuarantineRoot);
            RecoverQuarantineOrphans();

            _framework.Update += OnFrameworkUpdate;
            _applyWorker = Task.Run(() => ApplyWorkerLoop(_applyCts.Token));
        }

        public void Initialize(ICondition condition)
        {
            _gate = new SafetyGate(condition);
        }

        public bool IsHashPending(string hash) => _pendingHashes.ContainsKey(hash);

        public void Enqueue(PendingFile f)
        {
            if (f is null) return;
            if (string.IsNullOrWhiteSpace(f.FileHash)) return;

            if (_pendingHashes.TryAdd(f.FileHash, 0))
                _pending.Enqueue(f);
        }
        private const int ActorCoalesceDelayMs = 150;
        private const int ActorMaxHoldMs = 1500;  
        private const int ActorMinRedrawIntervalMs = 750; 

        private void TouchActor(nint addr)
        {
            if (addr == nint.Zero) return;

            var now = DateTime.UtcNow;

            _actorRedrawStates.AddOrUpdate(addr,
                _ => new ActorRedrawState
                {
                    FirstTouchUtc = now,
                    LastTouchUtc = now,
                    DueUtc = now.AddMilliseconds(ActorCoalesceDelayMs),
                    LastRedrawUtc = DateTime.MinValue
                },
                (_, state) =>
                {
                    lock (state.Gate)
                    {
                        state.LastTouchUtc = now;

                        // Push the due time out, but cap by max-hold so we still redraw even under constant churn.
                        var pushed = now.AddMilliseconds(ActorCoalesceDelayMs);
                        var capped = state.FirstTouchUtc.AddMilliseconds(ActorMaxHoldMs);
                        state.DueUtc = pushed <= capped ? pushed : capped;
                    }
                    return state;
                });
        }

        public void NotifyActorTouched(nint? actorAddress)
        {
            if (actorAddress is nint addr && addr != nint.Zero)
                TouchActor(addr);
        }

        private async Task ApplyWorkerLoop(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await _applySignal.WaitAsync(ct).ConfigureAwait(false);

                    while (_applyWorkQueue.TryDequeue(out var f))
                    {
                        if (ct.IsCancellationRequested) break;

                        ApplyResult result;
                        try
                        {
                            result = ApplyOneFileWorker(f);
                        }
                        catch (Exception ex)
                        {
                            result = new ApplyResult(f, ApplyOutcome.Fail, ex.ToString());
                        }

                        _applyCompletedQueue.Enqueue(result);

                        // Keep the worker cooperative; avoids long uninterrupted bursts on one thread.
                        await Task.Yield();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DelayedActivatorService apply worker crashed");
            }
        }

        private ApplyResult ApplyOneFileWorker(PendingFile f)
        {
            // Everything heavy happens here: validate, copy, timestamp randomization, cache registration, cleanup
            // Framework tick only drains results + schedules redraws.

            try
            {
                if (!File.Exists(f.QuarantinePath))
                {
                    if (File.Exists(f.FinalPath) && DestinationIsExpectedHash(f.FinalPath, f.FileHash))
                    {
                        // Already applied by someone else; just treat as success.
                        try { _fileDbManager.UnstageFile(f.FileHash); } catch { /* best effort */ }
                        return new ApplyResult(f, ApplyOutcome.Success);
                    }

                    _fileDbManager.UnstageFile(f.FileHash);
                    return new ApplyResult(f, ApplyOutcome.Fail, "Quarantine missing");
                }

                if (!DestinationIsExpectedHash(f.QuarantinePath, f.FileHash))
                {
                    try { File.Delete(f.QuarantinePath); } catch { /* best effort */ }
                    _fileDbManager.UnstageFile(f.FileHash);
                    return new ApplyResult(f, ApplyOutcome.Fail, "Quarantine failed hash validation");
                }

                try
                {
                    CopyFileBuffered(f.QuarantinePath, f.FinalPath);
                }
                catch (Exception ex) when (IsFileInUse(ex))
                {
                    // If final already correct, succeed; otherwise requeue
                    if (File.Exists(f.FinalPath) && DestinationIsExpectedHash(f.FinalPath, f.FileHash))
                    {
                        try { File.Delete(f.QuarantinePath); } catch { /* best effort */ }
                        _fileDbManager.UnstageFile(f.FileHash);
                        return new ApplyResult(f, ApplyOutcome.Success);
                    }

                    return new ApplyResult(f, ApplyOutcome.Requeue);
                }

                // Best-effort timestamp randomization
                try
                {
                    var fi = new FileInfo(f.FinalPath);
                    DateTime start = new(1995, 1, 1, 1, 1, 1, DateTimeKind.Local);
                    int range = (DateTime.Today - start).Days;
                    fi.CreationTime = start.AddDays(_rng.Next(range));
                    fi.LastAccessTime = DateTime.Today;
                    fi.LastWriteTime = start.AddDays(_rng.Next(range));
                }
                catch { /* best effort */ }

                try
                {
                    var entry = _fileDbManager.CreateCacheEntry(f.FinalPath);
                    if (entry != null && !string.Equals(entry.Hash, f.FileHash, StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogError(
                            "Hash mismatch after delayed apply; expected {expected}, got {actual} → {path}",
                            f.FileHash, entry.Hash, f.FinalPath);

                        try { File.Delete(f.FinalPath); } catch { /* best effort */ }
                        try { _fileDbManager.RemoveHashedFile(entry.Hash, entry.PrefixedFilePath); } catch { /* best effort */ }
                        _fileDbManager.UnstageFile(f.FileHash);

                        return new ApplyResult(f, ApplyOutcome.Fail, "Cache registration hash mismatch");
                    }
                }
                catch (Exception ex)
                {
                    if (IsFileInUse(ex))
                        return new ApplyResult(f, ApplyOutcome.Requeue);

                    _logger.LogWarning(ex, "Cache registration error for delayed file {path}", f.FinalPath);
                    _fileDbManager.UnstageFile(f.FileHash);
                    return new ApplyResult(f, ApplyOutcome.Fail, "Cache registration error");
                }

                // Cleanup quarantine + unstage
                try { File.Delete(f.QuarantinePath); } catch { /* best effort */ }
                try { _fileDbManager.UnstageFile(f.FileHash); } catch { /* best effort */ }

                return new ApplyResult(f, ApplyOutcome.Success);
            }
            catch (Exception ex)
            {
                try { _fileDbManager.UnstageFile(f.FileHash); } catch { /* best effort */ }
                return new ApplyResult(f, ApplyOutcome.Fail, ex.ToString());
            }
        }

        private void OnFrameworkUpdate(IFramework framework)
        {
            try
            {
                const int MaxCompletedPerFrame = 64;
                for (int i = 0; i < MaxCompletedPerFrame && _applyCompletedQueue.TryDequeue(out var r); i++)
                {
                    var f = r.File;

                    switch (r.Outcome)
                    {
                        case ApplyOutcome.Success:
                            {
                                // Worker already did unstage + cleanup; just clear tracking and schedule redraw if needed
                                _pendingHashes.TryRemove(f.FileHash, out _);

                                if (IsRedrawCriticalFinalPath(f.FinalPath))
                                {
                                    if (f.ActorAddress is nint addr && addr != nint.Zero)
                                        TouchActor(addr);
                                    else
                                        _logger.LogDebug("Redraw-critical finalize without actor address: {path} ({hash})", f.FinalPath, f.FileHash);
                                }

                                break;
                            }

                        case ApplyOutcome.Requeue:
                            {
                                // File was in use - try later, keep hash pending
                                _pending.Enqueue(f);
                                break;
                            }

                        case ApplyOutcome.Fail:
                        default:
                            {
                                _logger.LogWarning("Failed to apply delayed file {path} ({hash}): {err}", f.FinalPath, f.FileHash, r.Error);
                                _pendingHashes.TryRemove(f.FileHash, out _);
                                break;
                            }
                    }
                }

                if (_gate == null)
                {
                    TryDoRedrawPass();
                    return;
                }

                var cfg = _cfgSvc.Current;

                if (!cfg.DelayActivationEnabled && _pending.IsEmpty && _actorRedrawStates.IsEmpty)
                {
                    TryDoRedrawPass();
                    return;
                }

                if (!_gate.SafeNow(cfg.SafeIdleSeconds, cfg.ApplyOnlyOnZoneChange))
                {
                    TryDoRedrawPass();
                    return;
                }

                var drained = _pendingDrain;
                drained.Clear();

                while (drained.Count < MaxFilesPerFrame && _pending.TryDequeue(out var f))
                {
                    if (f is not null)
                        drained.Add(f);
                }

                if (drained.Count == 0)
                {
                    TryDoRedrawPass();
                    return;
                }

                var batch = _pendingBatch;
                batch.Clear();
                if (batch.Capacity < drained.Count) batch.Capacity = drained.Count;

                foreach (var f in drained)
                {
                    if (f.HardDelay ||
                        (!cfg.DelayAnimationsOnly && f.SoftDelay) ||
                        (!f.HardDelay && !f.SoftDelay))
                    {
                        batch.Add(f);
                    }
                    else
                    {
                        _pending.Enqueue(f);
                    }
                }

                if (batch.Count == 0)
                {
                    TryDoRedrawPass();
                    return;
                }

                foreach (var f in batch)
                {
                    _applyWorkQueue.Enqueue(f);
                    _applySignal.Release();
                }

                TryDoRedrawPass();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception in DelayedActivatorService.OnFrameworkUpdate");
            }
        }

        private void FinalizeSuccess(PendingFile f, bool deleteQuarantine)
        {
            if (deleteQuarantine)
            {
                try { File.Delete(f.QuarantinePath); } catch { /* best effort */ }
            }

            _fileDbManager.UnstageFile(f.FileHash);
            _pendingHashes.TryRemove(f.FileHash, out _);

            if (IsRedrawCriticalFinalPath(f.FinalPath))
            {
                if (f.ActorAddress is nint addr && addr != nint.Zero)
                    TouchActor(addr);
                else
                    _logger.LogDebug("Redraw-critical finalize without actor address: {path} ({hash})", f.FinalPath, f.FileHash);
            }

        }
        private void TryDoRedrawPass()
        {
            try
            {
                if (_actorRedrawStates.IsEmpty)
                    return;

                var now = DateTime.UtcNow;

                foreach (var kvp in _actorRedrawStates)
                {
                    var state = kvp.Value;
                    DateTime lastTouch;
                    lock (state.Gate) lastTouch = state.LastTouchUtc;

                    if ((now - lastTouch) > TimeSpan.FromSeconds(30))
                        _actorRedrawStates.TryRemove(kvp.Key, out _);
                }

                var local = _dalamudUtil.GetPlayerCharacter();
                var localAddr = local?.Address ?? nint.Zero;

                if (_actorRedrawStates.IsEmpty)
                    return;

                var toRemove = new List<nint>();

                foreach (var obj in _objectTable)
                {
                    if (obj is not IPlayerCharacter pc) continue;
                    if (pc.Address == localAddr) continue;

                    if (!_actorRedrawStates.TryGetValue(pc.Address, out var state))
                        continue;

                    var doRedraw = false;

                    lock (state.Gate)
                    {
                        if (now >= state.DueUtc)
                        {
                            if (state.LastRedrawUtc == DateTime.MinValue ||
                                (now - state.LastRedrawUtc) >= TimeSpan.FromMilliseconds(ActorMinRedrawIntervalMs))
                            {
                                doRedraw = true;
                                state.LastRedrawUtc = now;
                            }

                            toRemove.Add(pc.Address);
                        }
                    }

                    if (doRedraw)
                        _mareMediator.Publish(new PenumbraRedrawCharacterMessage(pc));
                }

                foreach (var addr in toRemove)
                    _actorRedrawStates.TryRemove(addr, out _);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during delayed redraw pass");
            }
        }


        private static void CopyFileBuffered(string srcPath, string dstPath)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(dstPath)!);

            const int BufferSize = 256 * 1024;

            var tmpPath = dstPath + ".tmp." + Environment.ProcessId + "." + Guid.NewGuid().ToString("N");

            var srcOpts = new FileStreamOptions
            {
                Access = FileAccess.Read,
                Mode = FileMode.Open,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
                BufferSize = BufferSize,
            };

            var dstOpts = new FileStreamOptions
            {
                Access = FileAccess.Write,
                Mode = FileMode.CreateNew,
                Share = FileShare.None,
                Options = FileOptions.SequentialScan,
                BufferSize = BufferSize,
            };

            byte[]? buffer = null;

            try
            {
                buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

                using (var src = new FileStream(srcPath, srcOpts))
                using (var dst = new FileStream(tmpPath, dstOpts))
                {
                    int read;
                    const int YieldEveryBytes = 2 * 1024 * 1024; // 2MB
                    var sinceYield = 0;

                    while ((read = src.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        dst.Write(buffer, 0, read);
                        sinceYield += read;

                        // Cooperative scheduling: avoid long uninterrupted file-copy bursts on the game update.
                        if (sinceYield >= YieldEveryBytes)
                        {
                            sinceYield = 0;
                            Thread.Yield();
                        }
                    }
                }

                File.Move(tmpPath, dstPath, overwrite: true);
            }
            catch
            {
                try { if (File.Exists(tmpPath)) File.Delete(tmpPath); } catch { /* best effort */ }
                throw;
            }
            finally
            {
                if (buffer != null)
                    ArrayPool<byte>.Shared.Return(buffer);
            }
        }


        private static bool DestinationIsExpectedHash(string path, string expectedHash)
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var sha1 = SHA1.Create();
                var bytes = sha1.ComputeHash(fs);

            #if NET5_0_OR_GREATER
                var actual = Convert.ToHexString(bytes);
            #else
                var actual = BitConverter.ToString(bytes).Replace("-", "", StringComparison.Ordinal);
            #endif
                return string.Equals(actual, expectedHash, StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                return false;
            }
        }

        private static bool IsFileInUse(Exception ex)
        {
            if (ex is UnauthorizedAccessException) return true;

            if (ex is IOException ioEx && OperatingSystem.IsWindows())
            {
                const int SharingViolation = unchecked((int)0x80070020);
                const int LockViolation = unchecked((int)0x80070021);
                return ioEx.HResult == SharingViolation || ioEx.HResult == LockViolation;
            }

            return false;
        }

        private void RecoverQuarantineOrphans()
        {
            try
            {
                foreach (var originalPath in Directory.EnumerateFiles(QuarantineRoot))
                {
                    var currentPath = originalPath;

                    try
                    {
                        if (new FileInfo(currentPath).Length < 16)
                        {
                            try { File.Delete(currentPath); } catch { }
                            continue;
                        }
                    }
                    catch
                    {
                        continue;
                    }

                    var ext = Path.GetExtension(currentPath);
                    if (string.IsNullOrWhiteSpace(ext))
                        continue;

                    var name = Path.GetFileNameWithoutExtension(currentPath);

                    string hash;
                    bool looksLikeHash =
                        name.Length == 40 &&
                        name.All(c =>
                            (c >= '0' && c <= '9') ||
                            (c >= 'a' && c <= 'f') ||
                            (c >= 'A' && c <= 'F'));

                    if (looksLikeHash)
                    {
                        hash = name.ToUpperInvariant();
                    }
                    else
                    {
                        hash = currentPath.GetFileHash();

                        // Best effort rename so we never have to re-hash again
                        var desired = Path.Combine(QuarantineRoot, hash + ext);
                        try
                        {
                            if (!File.Exists(desired))
                            {
                                File.Move(currentPath, desired);
                                currentPath = desired;
                            }
                        }
                        catch
                        {
                            // best effort
                        }
                    }

                    var extNoDot = ext.TrimStart('.');
                    var finalPath = _fileDbManager.GetCacheFilePath(hash, extNoDot);

                    var hard = ActivationPolicy.IsHardDelayed(finalPath);
                    var soft = !hard && ActivationPolicy.IsSoftDelayed(finalPath);

                    _fileDbManager.StageFile(hash, finalPath);

                    // Enqueue once
                    if (_pendingHashes.TryAdd(hash, 0))
                    {
                        _pending.Enqueue(new PendingFile(currentPath, finalPath, hash, hard, soft, null));
                        _logger.LogInformation("Recovered quarantine file {file} as {hash}", currentPath, hash);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to recover quarantine orphans");
            }
        }

        private static bool IsRedrawCriticalFinalPath(string finalPath)
        {
            if (string.IsNullOrWhiteSpace(finalPath)) return false;

            return finalPath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
                || finalPath.EndsWith(".skp", StringComparison.OrdinalIgnoreCase);
        }

        public void Dispose()
        {
            _framework.Update -= OnFrameworkUpdate;

            try
            {
                _applyCts.Cancel();
                _applySignal.Release(); 
                _applyWorker.Wait(500);
            }
            catch { /* best effort */ }
            finally
            {
                _applyCts.Dispose();
                _applySignal.Dispose();
            }
        }
    }
}
