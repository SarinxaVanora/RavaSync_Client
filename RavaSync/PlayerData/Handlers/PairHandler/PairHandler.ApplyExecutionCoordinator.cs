using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glamourer.Api.Enums;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Services;
using RavaSync.PlayerData.Pairs;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private sealed class ApplyExecutionCoordinator : CoordinatorBase
    {
        private const string TempFilesModName = "MareChara_Files";
        private const int TempFilesModPriority = 100;
        private const int DrawSettleWaitMs = 1500;
        private const int LifecycleDrawSettleWaitMs = 2500;
        private const int ApplyFileReadyWaitMs = 750;
        private const int LifecycleFileReadyWaitMs = 2000;

        public ApplyExecutionCoordinator(PairHandler owner) : base(owner)
        {
        }

        public async Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken token)
        {
            if (!updatedData.Any())
                return PairSyncCommitResult.Succeeded();

            _applicationId = Guid.NewGuid();
            var applicationId = _applicationId;
            var acquireLane = false;
            var queuedLane = false;

            SyncStorm.RegisterApplyQueued();
            queuedLane = true;
            var lane = SyncStorm.IsActive ? StormApplySemaphore : NormalApplySemaphore;

            try
            {
                var block = CaptureCommitBlockReason("commit start");
                if (block != null)
                    return block;

                var initialTarget = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                if (!initialTarget.Valid)
                    return PairSyncCommitResult.ActorChanged(initialTarget.Reason, retryImmediately: false);

                await lane.WaitAsync(token).ConfigureAwait(false);
                acquireLane = true;
                queuedLane = false;
                SyncStorm.RegisterApplyStarted();

                block = CaptureCommitBlockReason("lane acquired");
                if (block != null)
                    return block;

                await WaitForTargetDrawSettleAsync(applicationId, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);

                var target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                if (!target.Valid)
                    return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                var needsPenumbraBinding = updateModdedPaths || updateManip || lifecycleRedrawRequestedFromPlan || _redrawOnNextApplication || forceApplyModsForThisApply;
                var bindingReassigned = false;
                if (needsPenumbraBinding)
                {
                    var bound = await EnsurePenumbraCollectionBindingAsync(applicationId).ConfigureAwait(false);
                    if (!bound.Bound)
                    {
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.ApplyFailed("verified receiver collection binding failed", retryImmediately: true);
                    }

                    bindingReassigned = bound.Reassigned;
                    await YieldToFrameworkAsync(token, bindingReassigned ? 75 : 25).ConfigureAwait(false);

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);
                }

                if (requiresFileReadyGate && (updateModdedPaths || downloadedAny || forceApplyModsForThisApply || lifecycleRedrawRequestedFromPlan))
                {
                    var missingAtApply = await WaitForResolvedCachePathsReadyAsync(applicationId, moddedPaths, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);
                    if (missingAtApply.Count > 0)
                    {
                        if (lifecycleRedrawRequestedFromPlan)
                            MarkInitialApplyRequired();

                        var firstMissingApplyRetry = !_hasRetriedAfterMissingAtApply;
                        if (firstMissingApplyRetry)
                            _hasRetriedAfterMissingAtApply = true;

                        Logger.LogWarning(
                            "[{applicationId}] Refusing partial receiver apply for {pair}: {count} required files were not readable at commit time",
                            applicationId,
                            Pair.UserData.AliasOrUID,
                            missingAtApply.Count);

                        return PairSyncCommitResult.MissingFiles($"{missingAtApply.Count} required files were not ready before receiver apply", retryImmediately: lifecycleRedrawRequestedFromPlan || firstMissingApplyRetry);
                    }
                }

                var penumbraChanged = false;
                var tempMods = PairApplyUtilities.BuildPenumbraTempMods(moddedPaths);
                var tempModsFingerprint = PairApplyUtilities.ComputeTempModsFingerprint(tempMods);
                var previousTempMods = _lastAppliedTempModsSnapshot;
                var previousTransientSupportFingerprint = _lastAppliedTransientSupportFingerprint;
                var tempModsChanged = !string.Equals(tempModsFingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal);

                if (updateModdedPaths || forceApplyModsForThisApply || bindingReassigned)
                {
                    if (tempMods.Count == 0)
                    {
                        if (!string.IsNullOrWhiteSpace(_lastAppliedTempModsFingerprint) && !string.Equals(_lastAppliedTempModsFingerprint, "EMPTY", StringComparison.Ordinal))
                        {
                            await _ipcManager.Penumbra.ClearNamedTemporaryModsAsync(Logger, applicationId, _penumbraCollection, TempFilesModName, TempFilesModPriority).ConfigureAwait(false);
                            penumbraChanged = true;
                        }

                        _activeTempFilesModName = null;
                        _activeTempFilesModPriority = 0;
                        _lastAppliedTempModsFingerprint = "EMPTY";
                        _lastAppliedTempModsSnapshot = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _lastAppliedTransientSupportFingerprint = "EMPTY";
                    }
                    else if (tempModsChanged || forceApplyModsForThisApply || bindingReassigned || !string.Equals(_activeTempFilesModName, TempFilesModName, StringComparison.Ordinal))
                    {
                        var applied = await _ipcManager.Penumbra.SetNamedTemporaryModsAsync(Logger, applicationId, _penumbraCollection, TempFilesModName, tempMods, TempFilesModPriority).ConfigureAwait(false);
                        if (!applied)
                        {
                            _forceApplyMods = true;
                            MarkInitialApplyRequired();
                            return PairSyncCommitResult.ApplyFailed("Penumbra rejected receiver file temp mod", retryImmediately: true);
                        }

                        penumbraChanged = true;
                        _activeTempFilesModName = TempFilesModName;
                        _activeTempFilesModPriority = TempFilesModPriority;
                        _lastAppliedTempModsFingerprint = tempModsFingerprint;
                        _lastAppliedTempModsSnapshot = new Dictionary<string, string>(tempMods, StringComparer.OrdinalIgnoreCase);
                        _lastAppliedTransientSupportFingerprint = assetPlan.TransientSupportFingerprint;
                    }
                }

                var manipulationChanged = false;
                if (updateManip || forceApplyModsForThisApply || bindingReassigned)
                {
                    var effectiveManipulationData = Pair.GetEffectiveManipulationData(charaData.ManipulationData);
                    var manipulationFingerprint = PairApplyUtilities.ComputeManipulationFingerprint(effectiveManipulationData);
                    if (!string.Equals(manipulationFingerprint, _lastAppliedManipulationFingerprint, StringComparison.Ordinal))
                    {
                        if (string.IsNullOrWhiteSpace(effectiveManipulationData))
                            await _ipcManager.Penumbra.ClearManipulationDataAsync(Logger, applicationId, _penumbraCollection).ConfigureAwait(false);
                        else
                            await _ipcManager.Penumbra.SetManipulationDataAsync(Logger, applicationId, _penumbraCollection, effectiveManipulationData).ConfigureAwait(false);

                        _lastAppliedManipulationFingerprint = manipulationFingerprint;
                        manipulationChanged = true;
                    }
                }

                if (penumbraChanged || manipulationChanged)
                {
                    await YieldToFrameworkAsync(token, lifecycleRedrawRequestedFromPlan ? 125 : 50).ConfigureAwait(false);

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);
                }

                var redrawRequired = ShouldRedrawForApply(lifecycleRedrawRequestedFromPlan, assetPlan, previousTempMods, tempMods, previousTransientSupportFingerprint, penumbraChanged, manipulationChanged, updatedData);
                var awaitGlamourer = redrawRequired || penumbraChanged || manipulationChanged || forceApplyModsForThisApply || bindingReassigned;
                var forceLightweightMetadataReapply = forceApplyModsForThisApply || bindingReassigned;
                var glamourerApplyFlags = SelectGlamourerApplyFlags(lifecycleRedrawRequestedFromPlan, redrawRequired, penumbraChanged, manipulationChanged, forceApplyModsForThisApply, bindingReassigned, updatedData);
                var playerGlamourerApplyRequested = updatedData.TryGetValue(ObjectKind.Player, out var playerApplyChanges)
                    && playerApplyChanges.Contains(PlayerChanges.Glamourer)
                    && HasPlayerGlamourerPayload(charaData);

                if (playerGlamourerApplyRequested && glamourerApplyFlags != ApplyFlagEx.StateDefault)
                    Logger.LogTrace("[{applicationId}] Using lightweight Glamourer apply flags {flags} for {pair}", applicationId, glamourerApplyFlags, Pair.UserData.AliasOrUID);

                if (redrawRequired || awaitGlamourer)
                    _suppressClassJobRedrawUntilTick = Environment.TickCount64 + 1500;

                foreach (var changeSet in OrderChangeSets(updatedData))
                {
                    token.ThrowIfCancellationRequested();

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    await ApplyCustomizationDataAsync(applicationId, changeSet, charaData, redrawRequired, forceLightweightMetadataReapply, awaitGlamourer && changeSet.Key == ObjectKind.Player, glamourerApplyFlags, token).ConfigureAwait(false);
                }

                if (redrawRequired)
                {
                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    var criticalTransientSupportRefresh = assetPlan.RequiresTransientSupportRefresh(previousTransientSupportFingerprint);
                    var redrawFired = await OnePassRedrawAsync(applicationId, token, criticalRedraw: lifecycleRedrawRequestedFromPlan || HasCriticalRedrawAsset(assetPlan) || criticalTransientSupportRefresh).ConfigureAwait(false);
                    if ((lifecycleRedrawRequestedFromPlan || HasCriticalRedrawAsset(assetPlan) || criticalTransientSupportRefresh) && !redrawFired)
                    {
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.Deferred("required receiver redraw did not acknowledge", retryImmediately: true);
                    }
                }
                else if (penumbraChanged && !playerGlamourerApplyRequested && HasPlayerGlamourerPayload(charaData) && _charaHandler != null && _charaHandler.Address != nint.Zero)
                {
                    await _ipcManager.Glamourer.ReapplyDirectAsync(Logger, _charaHandler, applicationId, token).ConfigureAwait(false);
                }

                _cachedData = charaData;
                _redrawOnNextApplication = false;
                _lifecycleRedrawApplications.TryRemove(applicationBase, out _);
                _forceApplyMods = false;
                _hasRetriedAfterMissingAtApply = false;
                _lastApplyCompletedTick = Environment.TickCount64;

                UpdateAppliedSizeEstimate(assetPlan, token);

                if (downloadedAny || _hasRetriedAfterMissingDownload)
                    RequestPostApplyRepair(charaData);

                return PairSyncCommitResult.Succeeded();
            }
            catch (OperationCanceledException)
            {
                if (lifecycleRedrawRequestedFromPlan)
                    MarkInitialApplyRequired();

                return PairSyncCommitResult.Cancelled("receiver apply was cancelled");
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "[{applicationId}] Receiver apply failed for {pair}", applicationId, Pair.UserData.AliasOrUID);

                if (lifecycleRedrawRequestedFromPlan)
                    MarkInitialApplyRequired();

                return PairSyncCommitResult.ApplyFailed("receiver apply threw", retryImmediately: true);
            }
            finally
            {
                if (queuedLane)
                    SyncStorm.RegisterApplyQueueCancelled();

                if (acquireLane)
                {
                    lane.Release();
                    SyncStorm.RegisterApplyFinished();
                }
            }
        }

        private PairSyncCommitResult? CaptureCommitBlockReason(string phase)
        {
            if (_lifetime.ApplicationStopping.IsCancellationRequested)
                return PairSyncCommitResult.Cancelled($"application is stopping during {phase}");

            if (!IsVisible)
                return PairSyncCommitResult.Hidden($"pair is hidden during {phase}");

            if (Pair.IsPaused)
                return PairSyncCommitResult.Paused($"pair is paused during {phase}");

            if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
                return PairSyncCommitResult.YieldedToOtherSync($"pair yielded to OtherSync during {phase}");

            if (_localOtherSyncDecisionYield && !Pair.EffectiveOverrideOtherSync)
                return PairSyncCommitResult.YieldedToOtherSync($"local OtherSync gate yielded during {phase}");

            if (_dalamudUtil.IsInCombatOrPerforming)
                return PairSyncCommitResult.CombatOrPerformance($"combat/performance blocked receiver apply during {phase}");

            if (_dalamudUtil.IsInGpose || _dalamudUtil.IsInCutscene || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
                return PairSyncCommitResult.PluginsUnavailable($"framework/plugins unavailable during {phase}");

            if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                return PairSyncCommitResult.Hidden($"receiver actor handler is missing during {phase}");

            var strictAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
            if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
                return PairSyncCommitResult.ActorChanged($"receiver actor identity changed during {phase}", retryImmediately: false);

            return null;
        }

        private async Task<CommitTargetSnapshot> CaptureRemoteApplyTargetAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                    return CommitTargetSnapshot.Invalid("receiver actor handler is unavailable");

                var strictAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
                if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
                    return CommitTargetSnapshot.Invalid("receiver actor no longer matches expected pair identity");

                var localPlayer = _dalamudUtil.GetPlayerPtr();
                if (localPlayer != nint.Zero && strictAddress == localPlayer)
                    return CommitTargetSnapshot.Invalid("receiver actor resolved to local player");

                var obj = _dalamudUtil.CreateGameObject(strictAddress);
                var index = obj?.ObjectIndex ?? -1;
                if (index < 0)
                    return CommitTargetSnapshot.Invalid("receiver actor has no stable object index");

                return new CommitTargetSnapshot(true, strictAddress, index, string.Empty);
            }).ConfigureAwait(false);
        }

        private async Task WaitForTargetDrawSettleAsync(Guid applicationId, bool lifecycleCritical, CancellationToken token)
        {
            if (_charaHandler == null || _charaHandler.CurrentDrawCondition == GameObjectHandler.DrawCondition.None)
                return;

            var maxWait = lifecycleCritical ? LifecycleDrawSettleWaitMs : DrawSettleWaitMs;
            if (SyncStorm.IsActive)
                maxWait = Math.Min(maxWait, lifecycleCritical ? 900 : 450);

            var sw = Stopwatch.StartNew();
            try
            {
                await _dalamudUtil.WaitWhileCharacterIsDrawing(Logger, _charaHandler, applicationId, maxWait, token).ConfigureAwait(false);
            }
            finally
            {
                if (sw.ElapsedMilliseconds >= maxWait)
                    Logger.LogDebug("[{applicationId}] Receiver draw settle wait reached {elapsed}ms for {pair}; continuing with staged apply", applicationId, sw.ElapsedMilliseconds, Pair.UserData.AliasOrUID);
            }
        }

        private async Task<IReadOnlyList<KeyValuePair<(string GamePath, string? Hash), string>>> WaitForResolvedCachePathsReadyAsync(Guid applicationId, Dictionary<(string GamePath, string? Hash), string> applyPaths, bool lifecycleCritical, CancellationToken token)
        {
            if (applyPaths.Count == 0)
                return Array.Empty<KeyValuePair<(string GamePath, string? Hash), string>>();

            var deadline = Environment.TickCount64 + (lifecycleCritical ? LifecycleFileReadyWaitMs : ApplyFileReadyWaitMs);
            var pending = applyPaths.ToList();
            var cacheRoot = _fileDbManager.CacheFolder;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                var notReady = new List<KeyValuePair<(string GamePath, string? Hash), string>>(pending.Count);
                var checkedCount = 0;
                foreach (var item in pending)
                {
                    if (!IsResolvedApplyPathReady(item.Value, cacheRoot))
                        notReady.Add(item);

                    if (++checkedCount % 256 == 0)
                        await Task.Yield();
                }

                if (notReady.Count == 0)
                    return Array.Empty<KeyValuePair<(string GamePath, string? Hash), string>>();

                if (Environment.TickCount64 >= deadline)
                {
                    Logger.LogDebug("[{applicationId}] {count} receiver files were still not readable after the bounded commit gate", applicationId, notReady.Count);
                    return notReady;
                }

                pending = notReady;
                await Task.Delay(50, token).ConfigureAwait(false);
            }
        }

        private static bool IsResolvedApplyPathReady(string? path, string? cacheRoot)
        {
            if (string.IsNullOrWhiteSpace(path))
                return false;

            if (!IsPathInsideCacheRoot(path, cacheRoot))
                return true;

            try
            {
                var fileInfo = new FileInfo(path);
                if (!fileInfo.Exists || fileInfo.Length <= 0)
                    return false;

                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                return stream.Length > 0;
            }
            catch
            {
                return false;
            }
        }

        private static bool IsPathInsideCacheRoot(string path, string? cacheRoot)
        {
            if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(cacheRoot))
                return false;

            try
            {
                var fullPath = Path.GetFullPath(path);
                var fullRoot = Path.GetFullPath(cacheRoot).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
                return fullPath.StartsWith(fullRoot, StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                return false;
            }
        }

        private static IReadOnlyList<KeyValuePair<ObjectKind, HashSet<PlayerChanges>>> OrderChangeSets(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        {
            return updatedData
                .OrderBy(static kvp => kvp.Key == ObjectKind.Player ? 0 : 1)
                .ThenBy(static kvp => (int)kvp.Key)
                .ToList();
        }

        private static ApplyFlag SelectGlamourerApplyFlags(bool lifecycleRedrawRequested, bool redrawRequired, bool penumbraChanged, bool manipulationChanged, bool forceApplyMods, bool bindingReassigned, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        {
            if (lifecycleRedrawRequested || redrawRequired || penumbraChanged || manipulationChanged || forceApplyMods || bindingReassigned)
                return ApplyFlagEx.StateDefault;

            if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges) || !playerChanges.Contains(PlayerChanges.Glamourer))
                return ApplyFlagEx.StateDefault;

            if (playerChanges.Contains(PlayerChanges.ModFiles)
                || playerChanges.Contains(PlayerChanges.ModManip)
                || playerChanges.Contains(PlayerChanges.Customize)
                || playerChanges.Contains(PlayerChanges.ForcedRedraw))
                return ApplyFlagEx.StateDefault;

            return ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;
        }

        private bool ShouldRedrawForApply(bool lifecycleRedrawRequested, PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods, string? previousTransientSupportFingerprint, bool penumbraChanged, bool manipulationChanged, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        {
            if (lifecycleRedrawRequested || _redrawOnNextApplication)
                return true;

            if (manipulationChanged
                && updatedData.TryGetValue(ObjectKind.Player, out var playerManipChanges)
                && playerManipChanges.Contains(PlayerChanges.ForcedRedraw))
                return true;

            if (!penumbraChanged)
                return false;

            if (assetPlan.RequiresTransientSupportRefresh(previousTransientSupportFingerprint))
                return true;

            return HasCriticalRedrawAssetChanged(assetPlan, previousTempMods, currentTempMods);
        }

        private static bool HasCriticalRedrawAsset(PairSyncAssetPlan assetPlan)
            => assetPlan.Entries.Any(static entry => IsRedrawCriticalAsset(entry));

        private static bool HasCriticalRedrawAssetChanged(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!assetPlan.HasAssets)
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (!IsRedrawCriticalAsset(entry))
                    continue;

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!currentTempMods.TryGetValue(gamePath, out var currentPath) || string.IsNullOrWhiteSpace(currentPath))
                    continue;

                if (previousTempMods == null || previousTempMods.Count == 0)
                    return true;

                if (!previousTempMods.TryGetValue(gamePath, out var previousPath))
                    return true;

                if (!string.Equals(previousPath, currentPath, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static bool IsRedrawCriticalAsset(PairSyncAssetEntry entry)
        {
            if (IsCriticalRedrawAsset(entry.GamePath, entry.Kind))
                return true;

            // Child actors do not reliably rebuild themselves from a player-only redraw when their
            // base model/material/texture mapping changes. Treat non-player appearance assets as
            // redraw-critical so mounts/minions/companions do not stay on a vanilla or stale skeleton/model.
            return entry.ObjectKind != ObjectKind.Player
                && entry.Criticality == PairSyncAssetCriticality.AppearanceCritical;
        }

        private static bool IsCriticalRedrawAsset(string gamePath, PairSyncAssetKind kind)
        {
            if (kind == PairSyncAssetKind.Sound)
                return true;

            return PairApplyUtilities.IsTransientRedrawCriticalGamePath(gamePath)
                || PairApplyUtilities.IsSkeletonOrPhysicsCriticalGamePath(gamePath);
        }

        private static string NormalizeGamePath(string gamePath)
            => string.IsNullOrWhiteSpace(gamePath) ? string.Empty : gamePath.Replace('\\', '/').Trim();

        private static bool HasPlayerGlamourerPayload(CharacterData charaData)
            => charaData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
                && !string.IsNullOrWhiteSpace(glamourerPayload);

        private void UpdateAppliedSizeEstimate(PairSyncAssetPlan assetPlan, CancellationToken token)
        {
            if (assetPlan.UniqueHashes.Count == 0)
            {
                LastAppliedDataBytes = -1;
                return;
            }

            long totalBytes = 0;
            var any = false;
            foreach (var hash in assetPlan.UniqueHashes)
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    var cache = _fileDbManager.GetFileCacheByHash(hash);
                    var size = cache?.Size;
                    if (size.HasValue && size.Value > 0)
                    {
                        totalBytes += size.Value;
                        any = true;
                    }
                }
                catch
                {
                    // best effort metric only
                }
            }

            LastAppliedDataBytes = any ? totalBytes : -1;
        }

        private async Task YieldToFrameworkAsync(CancellationToken token, int delayMs)
        {
            token.ThrowIfCancellationRequested();
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            if (delayMs > 0)
                await Task.Delay(delayMs, token).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
        }

        private readonly record struct CommitTargetSnapshot(bool Valid, nint Address, int ObjectIndex, string Reason)
        {
            public static CommitTargetSnapshot Invalid(string reason) => new(false, nint.Zero, -1, reason);
        }
    }
}
