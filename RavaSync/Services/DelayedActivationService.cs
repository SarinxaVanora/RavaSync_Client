using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using RavaSync.Utils;
using System.Buffers;
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

        private DateTime _nextAllowedRedrawUtc = DateTime.MinValue;
        private readonly ConcurrentDictionary<string, byte> _pendingHashes = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<nint, byte> _touchedActors = new();

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
        }

        // fed from Plugin ctor (Dalamud's ICondition)
        public void Initialize(ICondition condition)
        {
            _gate = new SafetyGate(condition);
        }

        public bool IsHashPending(string hash) => _pendingHashes.ContainsKey(hash);

        public void Enqueue(PendingFile f)
        {
            // record pending hash first to prevent duplicate downloads
            if (f is null) return;
            if (string.IsNullOrWhiteSpace(f.FileHash)) return;

            _pendingHashes.TryAdd(f.FileHash, 0);
            _pending.Enqueue(f);
        }

        private void OnFrameworkUpdate(IFramework framework)
        {
            try
            {
                if (_gate == null) return;

                var cfg = _cfgSvc.Current;
                if (!cfg.DelayActivationEnabled && _pending.IsEmpty && _touchedActors.IsEmpty)
                    return;

                if (!_gate.SafeNow(cfg.SafeIdleSeconds, cfg.ApplyOnlyOnZoneChange)) return;

                const int MaxFilesPerFrame = 2;

                var drained = new List<PendingFile>(MaxFilesPerFrame);
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

                var batch = new List<PendingFile>(drained.Count);
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
                    try
                    {
                        if (!File.Exists(f.QuarantinePath))
                        {
                            if (File.Exists(f.FinalPath) && DestinationIsExpectedHash(f.FinalPath, f.FileHash))
                            {
                                FinalizeSuccess(f, deleteQuarantine: false);
                                continue;
                            }

                            _logger.LogWarning("Quarantine missing: {file}", f.QuarantinePath);
                            _fileDbManager.UnstageFile(f.FileHash);
                            _pendingHashes.TryRemove(f.FileHash, out _);
                            continue;
                        }

                        try
                        {
                            CopyFileBuffered(f.QuarantinePath, f.FinalPath);
                        }
                        catch (Exception ex) when (IsFileInUse(ex))
                        {
                            if (File.Exists(f.FinalPath) && DestinationIsExpectedHash(f.FinalPath, f.FileHash))
                            {
                                FinalizeSuccess(f, deleteQuarantine: true);
                                continue;
                            }

                            _pending.Enqueue(f);
                            continue;
                        }

                        // match PersistFileToStorage timestamp behavior
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

                                File.Delete(f.FinalPath);
                                _fileDbManager.RemoveHashedFile(entry.Hash, entry.PrefixedFilePath);
                                _fileDbManager.UnstageFile(f.FileHash);

                                _pendingHashes.TryRemove(f.FileHash, out _);
                                continue;
                            }
                        }
                        catch (Exception ex)
                        {
                            if (IsFileInUse(ex))
                            {
                                _pending.Enqueue(f);
                                continue;
                            }

                            _logger.LogWarning(ex, "Cache registration error for delayed file {path}", f.FinalPath);
                            _fileDbManager.UnstageFile(f.FileHash);
                            _pendingHashes.TryRemove(f.FileHash, out _);
                            continue;
                        }

                        FinalizeSuccess(f, deleteQuarantine: true);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to apply delayed file {file}", f.FinalPath);
                        _fileDbManager.UnstageFile(f.FileHash);
                        _pendingHashes.TryRemove(f.FileHash, out _);
                    }
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

            if (f.ActorAddress is nint addr && addr != nint.Zero)
                _touchedActors.TryAdd(addr, 0);
        }

        private void TryDoRedrawPass()
        {
            try
            {
                if (_touchedActors.IsEmpty)
                    return;

                if (DateTime.UtcNow < _nextAllowedRedrawUtc)
                    return;

                var local = _dalamudUtil.GetPlayerCharacter();
                var localAddr = local?.Address ?? nint.Zero;

                var touchedAddresses = _touchedActors.Keys.ToArray();
                var set = new HashSet<nint>(touchedAddresses);
                var redrawn = new List<nint>(touchedAddresses.Length);

                foreach (var obj in _objectTable)
                {
                    if (obj is not IPlayerCharacter pc) continue;
                    if (pc.Address == localAddr) continue; // skip self
                    if (!set.Contains(pc.Address)) continue;

                    _mareMediator.Publish(new PenumbraRedrawCharacterMessage(pc));
                    redrawn.Add(pc.Address);
                }

                foreach (var addr in redrawn)
                    _touchedActors.TryRemove(addr, out _);

                _nextAllowedRedrawUtc = DateTime.UtcNow.AddSeconds(1);
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
                Mode = FileMode.Create,
                Share = FileShare.None,
                Options = FileOptions.SequentialScan,
                BufferSize = BufferSize,
            };

            byte[]? buffer = null;

            using var src = new FileStream(srcPath, srcOpts);
            using var dst = new FileStream(dstPath, dstOpts);

            try
            {
                buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

                int read;
                while ((read = src.Read(buffer, 0, buffer.Length)) > 0)
                    dst.Write(buffer, 0, read);

                dst.Flush();
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

        public void Dispose()
        {
            _framework.Update -= OnFrameworkUpdate;
            _gate?.Dispose();
        }
    }
}
