using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private sealed class RepairCoordinator : CoordinatorBase
    {
        public RepairCoordinator(PairHandler owner) : base(owner)
        {
        }

            public bool HasAnyMissingCacheFiles(Guid applicationBase, CharacterData characterData)
            {
                    try
                    {
                        var seenHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        foreach (var kv in characterData.FileReplacements)
                        {
                            var list = kv.Value;
                            if (list == null) continue;

                            for (int i = 0; i < list.Count; i++)
                            {
                                var item = list[i];
                                if (!string.IsNullOrEmpty(item.FileSwapPath))
                                    continue;

                                var hash = item.Hash;
                                if (string.IsNullOrWhiteSpace(hash))
                                    continue;

                                if (!seenHashes.Add(hash))
                                    continue;

                                var fileCache = _fileDbManager.GetFileCacheByHash(hash);
                                if (fileCache == null)
                                {
                                    //Logger.LogDebug(
                                    //    "[BASE-{appBase}] Detected missing cache entry for hash {hash} during apply self-check",
                                    //    applicationBase,
                                    //    hash);
                                    return true;
                                }

                                var resolvedPath = fileCache.ResolvedFilepath;
                                if (string.IsNullOrEmpty(resolvedPath) || !File.Exists(resolvedPath))
                                {
                                    //Logger.LogDebug(
                                    //    "[BASE-{appBase}] Detected missing cache file on disk for hash {hash} (path: {path}) during apply self-check",
                                    //    applicationBase,
                                    //    hash,
                                    //    resolvedPath);
                                    return true;
                                }

                                try
                                {
                                    var fi = new FileInfo(resolvedPath);
                                    if (fi.Length == 0)
                                    {
                                        if ((DateTime.UtcNow - fi.LastWriteTimeUtc) < TimeSpan.FromSeconds(10))
                                            continue;

                                        return true;
                                    }
                                }
                                catch (IOException)
                                {
                                    continue;
                                }
                                catch (UnauthorizedAccessException)
                                {
                                    continue;
                                }
                                catch
                                {
                                    //Logger.LogDebug(
                                    //    "[BASE-{appBase}] Detected unreadable cache file for hash {hash} (path: {path}) during apply self-check",
                                    //    applicationBase,
                                    //    hash,
                                    //    resolvedPath);
                                    return true;
                                }
                            }
                        }
                    }
                    catch (IOException ex)
                    {
                        Logger.LogDebug(ex, "[BASE-{appBase}] IO during HasAnyMissingCacheFiles; treating as pass", applicationBase);
                        return false;
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        Logger.LogDebug(ex, "[BASE-{appBase}] Access during HasAnyMissingCacheFiles; treating as pass", applicationBase);
                        return false;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[BASE-{appBase}] Error during HasAnyMissingCacheFiles check, falling back to re-apply", applicationBase);
                        return true;
                    }

                    return false;
                }

                public void RequestManualFileRepair()
            {
                    if (_cachedData == null)
                    {
                        Logger.LogInformation("Manual repair requested for {pair} but no cached data is present yet", Pair);
                        Mediator.Publish(new EventMessage(new Event(
                            PlayerName,
                            Pair.UserData,
                            nameof(PairHandler),
                            EventSeverity.Warning,
                            "RavaSync: Cannot verify files for this user yet (no cached data).")));
                        return;
                    }

                    if (_charaHandler == null || PlayerCharacter == nint.Zero)
                    {
                        Logger.LogInformation("Manual repair requested for {pair} but character is not currently valid", Pair);
                        Mediator.Publish(new EventMessage(new Event(
                            PlayerName,
                            Pair.UserData,
                            nameof(PairHandler),
                            EventSeverity.Warning,
                            "RavaSync: Cannot verify files while this user is not visible/loaded.")));
                        return;
                    }

                    // prevent overlap / spam click
                    if (Interlocked.Exchange(ref Owner._manualRepairRunning, 1) == 1)
                    {
                        Logger.LogInformation("Manual repair already running for {pair}, ignoring duplicate request", Pair);
                        return;
                    }

                    var appBase = Guid.NewGuid();

                    _ = Task.Run(async () =>
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

                        try
                        {
                            await ManualVerifyAndRepairAsync(appBase, _cachedData!.DeepClone(), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            Logger.LogInformation("[BASE-{appBase}] Manual verify/repair for {pair} was cancelled or timed out",
                                appBase, Pair.UserData.AliasOrUID);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "[BASE-{appBase}] Error during manual verify/repair for {pair}",
                                appBase, Pair.UserData.AliasOrUID);
                        }
                        finally
                        {
                            Interlocked.Exchange(ref Owner._manualRepairRunning, 0);
                        }
                    });
                }

                public async Task ManualVerifyAndRepairAsync(Guid applicationBase, CharacterData charaData, CancellationToken token, bool verifyFileHashes = true, bool publishEvents = true)
            {
                    if (publishEvents)
                    {
                        Logger.LogInformation("[BASE-{appBase}] Starting manual verify/repair for {pair}",applicationBase, Pair.UserData.AliasOrUID);
                    }

                    Dictionary<(string GamePath, string? Hash), string> moddedPaths;
                    var missing = _modPathResolver.Calculate(applicationBase, charaData, out moddedPaths, token);

                    var invalidHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var pathByHash = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var kvp in moddedPaths)
                    {
                        var hash = kvp.Key.Hash;
                        if (string.IsNullOrEmpty(hash)) continue;

                        pathByHash.TryAdd(hash, kvp.Value);
                    }

                    foreach (var item in missing)
                    {
                        if (string.IsNullOrWhiteSpace(item.Hash))
                            continue;

                        invalidHashes.Add(item.Hash);
                    }


                    foreach (var (hash, path) in pathByHash)
                    {
                        if (string.IsNullOrWhiteSpace(hash)) continue;
                    
                        token.ThrowIfCancellationRequested();

                        try
                        {
                            var fi = new FileInfo(path);
                            if (!fi.Exists || fi.Length == 0)
                            {
                                if (publishEvents)
                                {
                                    Logger.LogDebug("[BASE-{appBase}] Manual validation: file for {hash} missing or zero-length at {path}",applicationBase, hash, path);
                                }
               
                                invalidHashes.Add(hash);
                                continue;
                            }
                            if (verifyFileHashes)
                            {
                                var computed = Crypto.GetFileHash(fi.FullName);

                                if (!string.Equals(computed, hash, StringComparison.OrdinalIgnoreCase))
                                {
                                    Logger.LogWarning("[BASE-{appBase}] hash mismatch for {hash} at {path}, computed {computed}", applicationBase, hash, fi.FullName, computed);
                                    invalidHashes.Add(hash);
                                }
                            }
                        }
                        catch (IOException)
                        {

                            continue;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            continue;
                        }
                        catch (Exception ex)
                        {
                            Logger.LogDebug(ex, "[BASE-{appBase}] Manual validation threw for hash {hash} at {path}", applicationBase, hash, path);
                            invalidHashes.Add(hash);
                        }

                    }

                    if (invalidHashes.Count == 0)
                    {
                        if (publishEvents)
                        {
                            Logger.LogInformation(
                                "[BASE-{appBase}] No missing or corrupt files detected for {pair}",
                                applicationBase, Pair.UserData.AliasOrUID);
                        }
                        else
                        {
                            Logger.LogDebug(
                                "[BASE-{appBase}] Background post-apply verification found no missing or corrupt files for {pair}",
                                applicationBase, Pair.UserData.AliasOrUID);
                        }

                        if (publishEvents)
                        {
                            Mediator.Publish(new EventMessage(new Event(
                                PlayerName,
                                Pair.UserData,
                                nameof(PairHandler),
                                EventSeverity.Informational,
                                "RavaSync: File verification complete — no issues detected for this user.")));
                        }

                        return;
                    }

                    foreach (var badHash in invalidHashes)
                    {
                        try
                        {
                            var entry = _fileDbManager.GetFileCacheByHash(badHash);
                            if (entry == null) continue;

                            try
                            {
                                if (!string.IsNullOrEmpty(entry.ResolvedFilepath)
                                    && File.Exists(entry.ResolvedFilepath))
                                {
                                    Logger.LogWarning(
                                        "[BASE-{appBase}] Manual repair: deleting invalid cache file for {hash} at {path}",
                                        applicationBase, badHash, entry.ResolvedFilepath);
                                    File.Delete(entry.ResolvedFilepath);
                                }
                            }
                            catch (Exception exDel)
                            {
                                Logger.LogWarning(
                                    exDel,
                                    "[BASE-{appBase}] Manual repair: failed to delete invalid cache file for {hash} at {path}",
                                    applicationBase, badHash, entry.ResolvedFilepath);
                            }

                            try
                            {
                                _fileDbManager.RemoveHashedFile(entry.Hash, entry.PrefixedFilePath);
                            }
                            catch (Exception exDb)
                            {
                                Logger.LogWarning(
                                    exDb,
                                    "[BASE-{appBase}] Manual repair: failed to remove cache DB entry for {hash}",
                                    applicationBase, badHash);
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(
                                ex,
                                "[BASE-{appBase}] Manual repair: error while cleaning up invalid cache for {hash}",
                                applicationBase, badHash);
                        }
                    }

                    Logger.LogWarning(
                        "[BASE-{appBase}] {count} missing/corrupt cache files detected for {pair}; starting repair download",
                        applicationBase, invalidHashes.Count, Pair.UserData.AliasOrUID);

                    if (publishEvents)
                    {
                        Mediator.Publish(new EventMessage(new Event(
                            PlayerName,
                            Pair.UserData,
                            nameof(PairHandler),
                            EventSeverity.Warning,
                            $"RavaSync: Detected {invalidHashes.Count} missing/corrupt cache files; starting repair download.")));
                    }

                    _downloadManager?.ClearDownload();
                    _pairDownloadTask = null;

                    _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
                    _downloadCancellationTokenSource ??= new CancellationTokenSource();

                    _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();
                    _applicationCancellationTokenSource ??= new CancellationTokenSource();

                    _hasRetriedAfterMissingDownload = true;
                    _hasRetriedAfterMissingAtApply = true;
                    _forceApplyMods = true;

                    Owner.ApplyCharacterData(applicationBase, charaData, forceApplyCustomization: true);
                    await Task.CompletedTask;
                }

                public bool TryGetRecentMissingCheck(string dataHash, out bool hadMissing)
            {
                    lock (_missingCheckGate)
                    {
                        // treat as fresh for 5 seconds
                        if (string.Equals(_lastMissingCheckedHash, dataHash, StringComparison.Ordinal)
                            && (Environment.TickCount64 - _lastMissingCheckedTick) < 5000)
                        {
                            hadMissing = _lastMissingCheckHadMissing;
                            return true;
                        }
                    }

                    hadMissing = false;
                    return false;
                }

                public void ScheduleMissingCheck(Guid applicationBase, CharacterData characterData)
            {
                    var hash = characterData.DataHash.Value;
                    var dataCopy = characterData.DeepClone();

                    // If a check is already running, coalesce to the latest request and return.
                    if (Interlocked.CompareExchange(ref Owner._missingCheckRunning, 1, 0) != 0)
                    {
                        lock (_missingCheckGate)
                        {
                            _pendingMissingCheckHash = hash;
                            _pendingMissingCheckData = dataCopy;
                            _pendingMissingCheckBase = applicationBase;
                        }

                        Logger.LogTrace("[BASE-{appBase}] Missing-check already running; queued latest hash {hash}", applicationBase, hash);
                        return;
                    }

                    _ = Task.Run(() =>
                    {
                        try
                        {
                            Guid currentBase = applicationBase;
                            string currentHash = hash;
                            CharacterData currentData = dataCopy;

                            while (true)
                            {
                                try
                                {
                                    var missing = HasAnyMissingCacheFiles(currentBase, currentData);

                                    lock (_missingCheckGate)
                                    {
                                        _lastMissingCheckedHash = currentHash;
                                        _lastMissingCheckedTick = Environment.TickCount64;
                                        _lastMissingCheckHadMissing = missing;
                                    }

                                    if (missing)
                                    {
                                        var applyCopy = currentData.DeepClone();
                                        _ = Task.Run(() =>
                                        {
                                            try
                                            {
                                                Owner.ApplyCharacterData(Guid.NewGuid(), applyCopy, forceApplyCustomization: true);
                                            }
                                            catch (Exception ex)
                                            {
                                                Logger.LogTrace(ex, "[BASE-{appBase}] Deferred missing-check reapply failed for hash {hash}", currentBase, currentHash);
                                            }
                                        });
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Logger.LogTrace(ex, "[BASE-{appBase}] Missing-check failed for hash {hash}", currentBase, currentHash);
                                }

                                // Drain one queued request (latest wins). If none queued, we're done.
                                lock (_missingCheckGate)
                                {
                                    if (_pendingMissingCheckData == null || string.IsNullOrEmpty(_pendingMissingCheckHash))
                                        break;

                                    currentBase = _pendingMissingCheckBase;
                                    currentHash = _pendingMissingCheckHash;
                                    currentData = _pendingMissingCheckData;

                                    _pendingMissingCheckBase = Guid.Empty;
                                    _pendingMissingCheckHash = null;
                                    _pendingMissingCheckData = null;
                                }
                            }
                        }
                        finally
                        {
                            Interlocked.Exchange(ref Owner._missingCheckRunning, 0);

                            // Tiny race guard: if something queued right after we dropped the flag, kick another runner.
                            lock (_missingCheckGate)
                            {
                                if (_pendingMissingCheckData != null && !string.IsNullOrEmpty(_pendingMissingCheckHash))
                                {
                                    var queuedBase = _pendingMissingCheckBase;
                                    var queuedData = _pendingMissingCheckData;
                                    _pendingMissingCheckBase = Guid.Empty;
                                    _pendingMissingCheckHash = null;
                                    _pendingMissingCheckData = null;

                                    if (queuedData != null)
                                        ScheduleMissingCheck(queuedBase, queuedData);
                                }
                            }
                        }
                    });
                }

            public void RequestPostApplyRepair(CharacterData appliedData)
            {
                    var hash = appliedData.DataHash.Value;
                    var now = Environment.TickCount64;

                    // Per-hash cooldown
                    if (string.Equals(_lastPostApplyRepairHash, hash, StringComparison.Ordinal)
                        && (now - _lastPostApplyRepairTick) < 30000)
                        return;

                    var dataCopy = appliedData.DeepClone();

                    // If one is already active for this pair, coalesce to the latest request and return.
                    if (Interlocked.CompareExchange(ref Owner._manualRepairRunning, 1, 0) != 0)
                    {
                        lock (_postRepairGate)
                        {
                            _pendingPostApplyRepairHash = hash;
                            _pendingPostApplyRepairData = dataCopy;
                        }

                        Logger.LogTrace("Post-apply repair already running for {pair}; queued latest hash {hash}", Pair, hash);
                        return;
                    }

                    _lastPostApplyRepairHash = hash;
                    _lastPostApplyRepairTick = now;

                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            CharacterData currentData = dataCopy;
                            string currentHash = hash;

                            while (true)
                            {
                                var applicationBase = Guid.NewGuid();
                                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
                                var acquired = false;

                                try
                                {
                                    await Task.Delay(1500, cts.Token).ConfigureAwait(false);

                                    await GlobalPostApplyRepairSemaphore.WaitAsync(cts.Token).ConfigureAwait(false);
                                    acquired = true;

                                    await ManualVerifyAndRepairAsync(
                                        applicationBase,
                                        currentData,
                                        cts.Token,
                                        verifyFileHashes: false,
                                        publishEvents: false).ConfigureAwait(false);
                                }
                                catch (OperationCanceledException)
                                {
                                }
                                catch (Exception ex)
                                {
                                    Logger.LogWarning(ex, "[BASE-{appBase}] Post-apply repair failed", applicationBase);
                                }
                                finally
                                {
                                    if (acquired)
                                        GlobalPostApplyRepairSemaphore.Release();
                                }

                                lock (_postRepairGate)
                                {
                                    if (_pendingPostApplyRepairData == null || string.IsNullOrEmpty(_pendingPostApplyRepairHash))
                                        break;

                                    currentData = _pendingPostApplyRepairData;
                                    currentHash = _pendingPostApplyRepairHash;

                                    _pendingPostApplyRepairData = null;
                                    _pendingPostApplyRepairHash = null;
                                }

                                _lastPostApplyRepairHash = currentHash;
                                _lastPostApplyRepairTick = Environment.TickCount64;
                            }
                        }
                        finally
                        {
                            Interlocked.Exchange(ref Owner._manualRepairRunning, 0);

                            // Tiny race guard
                            CharacterData? queuedData = null;
                            string? queuedHash = null;

                            lock (_postRepairGate)
                            {
                                if (_pendingPostApplyRepairData != null && !string.IsNullOrEmpty(_pendingPostApplyRepairHash))
                                {
                                    queuedData = _pendingPostApplyRepairData;
                                    queuedHash = _pendingPostApplyRepairHash;
                                    _pendingPostApplyRepairData = null;
                                    _pendingPostApplyRepairHash = null;
                                }
                            }

                            if (queuedData != null && queuedHash != null)
                            {
                                RequestPostApplyRepair(queuedData);
                            }
                        }
                    });
                }
    }
}
