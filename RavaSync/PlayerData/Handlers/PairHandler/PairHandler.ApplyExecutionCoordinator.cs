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

        public async Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken token)
        {
            if (!updatedData.Any())
                return PairSyncCommitResult.Succeeded();

            _applicationId = Guid.NewGuid();
            var applicationId = _applicationId;
            var applyTouchesPlayer = updatedData.ContainsKey(ObjectKind.Player);
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

                var needsPenumbraBinding = updateModdedPaths || updateManip || lifecycleRedrawRequestedFromPlan || _redrawOnNextApplication || forceApplyModsForThisApply;
                var primePenumbraBeforeDrawSettle = needsPenumbraBinding && (updateModdedPaths || downloadedAny || forceApplyModsForThisApply || lifecycleApplyRequestedFromPlan || lifecycleRedrawRequestedFromPlan);
                if (!primePenumbraBeforeDrawSettle)
                    await WaitForTargetDrawSettleAsync(applicationId, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);
                else if (_charaHandler?.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                    Logger.LogTrace("[{applicationId}] Priming receiver temp files before draw settle for {pair}; draw state={draw}", applicationId, Pair.UserData.AliasOrUID, _charaHandler?.CurrentDrawCondition);

                var target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                if (!target.Valid)
                    return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);
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

                var playerWornEquipmentOrAccessoryOnlyApply = IsPlayerWornEquipmentOrAccessoryOnlyApply(assetPlan, updatedData, previousTempMods, tempMods);
                var redrawRequired = ShouldRedrawForApply(lifecycleRedrawRequestedFromPlan, assetPlan, previousTempMods, tempMods, previousTransientSupportFingerprint, penumbraChanged, manipulationChanged, updatedData, playerWornEquipmentOrAccessoryOnlyApply);
                var forceLightweightMetadataReapply = forceApplyModsForThisApply || bindingReassigned;
                var playerGlamourerApplyRequested = updatedData.TryGetValue(ObjectKind.Player, out var playerApplyChanges)
                    && playerApplyChanges.Contains(PlayerChanges.Glamourer)
                    && HasPlayerGlamourerPayload(charaData);
                var authoritativePlayerAppearanceApply = ShouldUseAuthoritativePlayerAppearanceApply(
                    playerGlamourerApplyRequested,
                    lifecycleApplyRequestedFromPlan,
                    lifecycleRedrawRequestedFromPlan,
                    redrawRequired,
                    penumbraChanged,
                    manipulationChanged,
                    forceApplyModsForThisApply,
                    bindingReassigned,
                    downloadedAny,
                    updateModdedPaths,
                    playerWornEquipmentOrAccessoryOnlyApply,
                    previousTempMods,
                    tempMods);
                var glamourerApplyFlags = SelectGlamourerApplyFlags(lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, redrawRequired, penumbraChanged, manipulationChanged, forceApplyModsForThisApply, bindingReassigned, updatedData, playerWornEquipmentOrAccessoryOnlyApply, authoritativePlayerAppearanceApply);
                var awaitGlamourer = authoritativePlayerAppearanceApply
                    || redrawRequired
                    || (manipulationChanged && !playerWornEquipmentOrAccessoryOnlyApply)
                    || (penumbraChanged && glamourerApplyFlags == ApplyFlagEx.StateDefault)
                    || (playerGlamourerApplyRequested && (penumbraChanged || downloadedAny || updateModdedPaths || forceApplyModsForThisApply || bindingReassigned));

                if (playerWornEquipmentOrAccessoryOnlyApply && manipulationChanged)
                    Logger.LogTrace("[{applicationId}] Player worn equipment/accessory-only apply has manipulation metadata; keeping it on the non-redraw equipment path for {pair}", applicationId, Pair.UserData.AliasOrUID);

                if (authoritativePlayerAppearanceApply)
                    Logger.LogTrace("[{applicationId}] Using non-blocking authoritative Glamourer state for {pair} to replace the previous locked appearance before/through draw", applicationId, Pair.UserData.AliasOrUID);
                else if (playerGlamourerApplyRequested && glamourerApplyFlags != ApplyFlagEx.StateDefault)
                    Logger.LogTrace("[{applicationId}] Using lightweight Glamourer apply flags {flags} for {pair}", applicationId, glamourerApplyFlags, Pair.UserData.AliasOrUID);

                if (redrawRequired || awaitGlamourer)
                    _suppressClassJobRedrawUntilTick = Environment.TickCount64 + 1500;

                var queuePostDrawPlayerSnap = applyTouchesPlayer
                    && playerGlamourerApplyRequested
                    && !redrawRequired
                    && (authoritativePlayerAppearanceApply || lifecycleApplyRequestedFromPlan || forceApplyModsForThisApply || bindingReassigned || penumbraChanged || manipulationChanged);
                var expectedPostDrawPayloadFingerprint = queuePostDrawPlayerSnap
                    ? PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(charaData)
                    : string.Empty;

                foreach (var changeSet in OrderChangeSets(updatedData))
                {
                    token.ThrowIfCancellationRequested();

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    await ApplyCustomizationDataAsync(applicationId, changeSet, charaData, redrawRequired, forceLightweightMetadataReapply, awaitGlamourer && changeSet.Key == ObjectKind.Player, false, glamourerApplyFlags, token).ConfigureAwait(false);
                }

                if (redrawRequired)
                {
                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    var playerCriticalRedrawAsset = HasPlayerCriticalRedrawAsset(assetPlan);
                    var criticalTransientSupportRefresh = RequiresPlayerTransientSupportRefresh(assetPlan, previousTransientSupportFingerprint);
                    var redrawFired = await OnePassRedrawAsync(applicationId, token, criticalRedraw: lifecycleRedrawRequestedFromPlan || playerCriticalRedrawAsset || criticalTransientSupportRefresh).ConfigureAwait(false);
                    if ((lifecycleRedrawRequestedFromPlan || playerCriticalRedrawAsset || criticalTransientSupportRefresh) && !redrawFired)
                    {
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.Deferred("required receiver redraw did not acknowledge", retryImmediately: true);
                    }
                }
                else if (penumbraChanged && !playerWornEquipmentOrAccessoryOnlyApply && !playerGlamourerApplyRequested && HasPlayerGlamourerPayload(charaData) && _charaHandler != null && _charaHandler.Address != nint.Zero)
                {
                    await _ipcManager.Glamourer.ReapplyDirectAsync(Logger, _charaHandler, applicationId, token).ConfigureAwait(false);
                }

                _cachedData = charaData;
                if (applyTouchesPlayer)
                    _redrawOnNextApplication = false;

                if (queuePostDrawPlayerSnap)
                    QueuePostDrawPlayerAppearanceSnap(applicationId, charaData, glamourerApplyFlags, expectedPostDrawPayloadFingerprint);
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

        private void QueuePostDrawPlayerAppearanceSnap(Guid applicationId, CharacterData charaData, ApplyFlag glamourerApplyFlags, string expectedPayloadFingerprint)
        {
            if (!charaData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
                || string.IsNullOrWhiteSpace(glamourerPayload))
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    var token = _lifetime.ApplicationStopping;
                    await Task.Delay(60, token).ConfigureAwait(false);

                    var handler = _charaHandler;
                    if (!IsVisible || handler == null || handler.Address == nint.Zero)
                        return;

                    var strictAddress = ResolveStrictVisiblePlayerAddress(handler.Address);
                    if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
                        return;

                    if (handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                    {
                        Logger.LogTrace("[{applicationId}] Waiting in background for receiver draw to settle before fast player appearance snap for {pair}", applicationId, Pair.UserData.AliasOrUID);
                        await _dalamudUtil.WaitWhileCharacterIsDrawing(Logger, handler, applicationId, 5000, token).ConfigureAwait(false);
                    }

                    await Task.Delay(25, token).ConfigureAwait(false);

                    handler = _charaHandler;
                    if (!IsVisible || handler == null || handler.Address == nint.Zero)
                        return;

                    strictAddress = ResolveStrictVisiblePlayerAddress(handler.Address);
                    if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
                        return;

                    var currentPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_cachedData);
                    if (!string.Equals(currentPayloadFingerprint, expectedPayloadFingerprint, StringComparison.Ordinal))
                        return;

                    Logger.LogTrace("[{applicationId}] Fast post-draw player appearance snap for {pair} using Glamourer flags={flags}", applicationId, Pair.UserData.AliasOrUID, glamourerApplyFlags);
                    await _ipcManager.Glamourer.ApplyAllAsync(Logger, handler, glamourerPayload, applicationId, token, fireAndForget: false, flags: glamourerApplyFlags, waitForDrawSettle: false).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Plugin shutdown or a cancelled wait; nothing to recover here.
                }
                catch (Exception ex)
                {
                    Logger.LogTrace(ex, "[{applicationId}] Fast post-draw player appearance snap failed for {pair}", applicationId, Pair.UserData.AliasOrUID);
                }
            });
        }

        private static ApplyFlag SelectGlamourerApplyFlags(bool lifecycleApplyRequested, bool lifecycleRedrawRequested, bool redrawRequired, bool penumbraChanged, bool manipulationChanged, bool forceApplyMods, bool bindingReassigned, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool playerWornEquipmentOrAccessoryOnlyApply, bool authoritativePlayerAppearanceApply)
        {
            if (authoritativePlayerAppearanceApply)
                return ApplyFlagEx.StateDefault;

            if (updatedData.TryGetValue(ObjectKind.Player, out var playerChanges)
                && playerChanges.Contains(PlayerChanges.Glamourer)
                && (!playerChanges.Contains(PlayerChanges.ModManip) || playerWornEquipmentOrAccessoryOnlyApply)
                && !playerChanges.Contains(PlayerChanges.Customize)
                && !playerChanges.Contains(PlayerChanges.ForcedRedraw)
                && !redrawRequired
                && (!manipulationChanged || playerWornEquipmentOrAccessoryOnlyApply))
            {
                // Plain worn equipment/accessory swaps should follow Glamourer's equipment
                // path only. Including Customization here can make simple ring/neck/bracelet
                // swaps look like a receiver-side redraw even though they are just gear.
                if (playerWornEquipmentOrAccessoryOnlyApply)
                    return ApplyFlag.Once | ApplyFlag.Equipment;

                return ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;
            }

            if (redrawRequired || lifecycleRedrawRequested)
                return ApplyFlagEx.StateDefault;

            if (lifecycleApplyRequested || manipulationChanged || forceApplyMods || bindingReassigned || penumbraChanged)
                return ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;

            return ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;
        }

        private static bool IsPlayerWornEquipmentOrAccessoryOnlyApply(PairSyncAssetPlan assetPlan, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
                return false;

            // Accessory mods commonly include Eqdp/Imc/Eqp manipulations. Those must be
            // applied to the Penumbra collection, but they should not promote a plain
            // ring/neck/bracelet/earring change into the heavy player-redraw path.
            if (playerChanges.Any(static change => change is not PlayerChanges.ModFiles and not PlayerChanges.ModManip and not PlayerChanges.Glamourer))
                return false;

            var changedGamePaths = GetChangedTempModGamePaths(previousTempMods, currentTempMods);
            if (changedGamePaths.Count == 0 && assetPlan.HasAssets)
            {
                changedGamePaths = assetPlan.Entries
                    .Where(static entry => entry.ObjectKind == ObjectKind.Player)
                    .Select(static entry => entry.GamePath)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

            return changedGamePaths.Count > 0
                && changedGamePaths.All(PairApplyUtilities.IsWornEquipmentOrAccessoryGamePath);
        }

        private static List<string> GetChangedTempModGamePaths(IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in currentTempMods)
            {
                if (previousTempMods == null
                    || !previousTempMods.TryGetValue(kvp.Key, out var previousPath)
                    || !string.Equals(previousPath, kvp.Value, StringComparison.OrdinalIgnoreCase))
                {
                    paths.Add(kvp.Key);
                }
            }

            if (previousTempMods != null)
            {
                foreach (var kvp in previousTempMods)
                {
                    if (!currentTempMods.ContainsKey(kvp.Key))
                        paths.Add(kvp.Key);
                }
            }

            return paths.ToList();
        }

        private static bool ShouldUseAuthoritativePlayerAppearanceApply(bool playerGlamourerApplyRequested, bool lifecycleApplyRequested, bool lifecycleRedrawRequested, bool redrawRequired, bool penumbraChanged, bool manipulationChanged, bool forceApplyMods, bool bindingReassigned, bool downloadedAny, bool updateModdedPaths, bool playerWornEquipmentOrAccessoryOnlyApply, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!playerGlamourerApplyRequested || redrawRequired || lifecycleRedrawRequested)
                return false;

            // A fast Once-only Glamourer apply is perfect for simple equipment swaps, but it
            // does not replace Glamourer's locked state. During a class/job redraw the game can
            // briefly rebuild the actor, Glamourer can restore the previous lock, and then our
            // post-draw snap applies the new payload. That appears as vanilla -> old outfit ->
            // new outfit. For full player appearance transitions, update the authoritative lock
            // immediately, but still do not wait on draw-settle in the RavaSync apply lane.
            if (lifecycleApplyRequested || forceApplyMods || bindingReassigned || downloadedAny || updateModdedPaths)
                return !IsAccessoryOnlyTempModDelta(previousTempMods, currentTempMods);

            if (penumbraChanged || manipulationChanged)
                return !playerWornEquipmentOrAccessoryOnlyApply && !IsAccessoryOnlyTempModDelta(previousTempMods, currentTempMods);

            return false;
        }

        private static bool IsAccessoryOnlyTempModDelta(IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            var changedGamePaths = GetChangedTempModGamePaths(previousTempMods, currentTempMods);
            return changedGamePaths.Count > 0
                && changedGamePaths.All(PairApplyUtilities.IsWornAccessoryGamePath);
        }

        private bool ShouldRedrawForApply(bool lifecycleRedrawRequested, PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods, string? previousTransientSupportFingerprint, bool penumbraChanged, bool manipulationChanged, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool playerWornEquipmentOrAccessoryOnlyApply)
        {
            if (lifecycleRedrawRequested)
                return true;

            if (!playerWornEquipmentOrAccessoryOnlyApply
                && manipulationChanged
                && updatedData.TryGetValue(ObjectKind.Player, out var playerManipChanges)
                && playerManipChanges.Contains(PlayerChanges.ForcedRedraw))
                return true;

            if (!penumbraChanged)
                return false;

            if (RequiresPlayerTransientSupportRefresh(assetPlan, previousTransientSupportFingerprint))
                return true;

            return HasPlayerCriticalRedrawAssetChanged(assetPlan, previousTempMods, currentTempMods);
        }

        private static bool HasPlayerCriticalRedrawAsset(PairSyncAssetPlan assetPlan)
            => assetPlan.Entries.Any(static entry => IsPlayerRedrawCriticalAsset(entry));

        private static bool RequiresPlayerTransientSupportRefresh(PairSyncAssetPlan assetPlan, string? previousTransientSupportFingerprint)
            => assetPlan.RequiresTransientSupportRefresh(previousTransientSupportFingerprint)
                && assetPlan.Entries.Any(static entry => entry.ObjectKind == ObjectKind.Player
                    && !LooksLikeOwnedObjectAssetPath(entry.GamePath)
                    && PairApplyUtilities.IsPlayerTransientSupportRefreshGamePath(entry.GamePath));

        private static bool HasPlayerCriticalRedrawAssetChanged(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!assetPlan.HasAssets)
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (!IsPlayerRedrawCriticalAsset(entry))
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

        private static bool IsPlayerRedrawCriticalAsset(PairSyncAssetEntry entry)
            => entry.ObjectKind == ObjectKind.Player
                && !LooksLikeOwnedObjectAssetPath(entry.GamePath)
                && IsCriticalRedrawAsset(entry.GamePath, entry.Kind);

        private static bool LooksLikeOwnedObjectAssetPath(string gamePath)
        {
            gamePath = NormalizeGamePath(gamePath);
            return gamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
                || gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
                || gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase);
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
