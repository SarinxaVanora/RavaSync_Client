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
using RavaSync.Utils;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private sealed class ApplyExecutionCoordinator : CoordinatorBase
    {
        private const string TempFilesModName = "MareChara_Files";
        private const int TempFilesModPriority = 100;
        private static readonly int DrawSettleWaitMs = IsWineRuntime ? 1400 : 1500;
        private static readonly int LifecycleDrawSettleWaitMs = IsWineRuntime ? 2400 : 2500;
        private static readonly int ApplyFileReadyWaitMs = IsWineRuntime ? 1000 : 750;
        private static readonly int LifecycleFileReadyWaitMs = IsWineRuntime ? 2500 : 2000;

        public ApplyExecutionCoordinator(PairHandler owner) : base(owner)
        {
        }

        private static async Task WaitForLifecycleApplyDispatchSpacingAsync(CancellationToken token)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();

                var now = Environment.TickCount64;
                var last = Interlocked.Read(ref _lastLifecycleApplyDispatchTick);
                var wait = LifecycleApplyDispatchSpacingMs - (now - last);
                if (wait <= 0)
                {
                    if (Interlocked.CompareExchange(ref _lastLifecycleApplyDispatchTick, now, last) == last)
                        return;

                    continue;
                }

                await Task.Delay((int)Math.Min(wait, 50), token).ConfigureAwait(false);
            }
        }

        private static async Task WaitForReceiverApplyDispatchSpacingAsync(bool lifecycleReceiverCommit, CancellationToken token)
        {
            if (!IsWineRuntime)
                return;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                var now = Environment.TickCount64;
                var last = Interlocked.Read(ref _lastReceiverApplyDispatchTick);
                var wait = ReceiverApplyDispatchSpacingMs(lifecycleReceiverCommit) - (now - last);
                if (wait <= 0)
                {
                    if (Interlocked.CompareExchange(ref _lastReceiverApplyDispatchTick, now, last) == last)
                        return;

                    continue;
                }

                await Task.Delay((int)Math.Min(wait, 75), token).ConfigureAwait(false);
            }
        }

        private static int WineAwareFrameworkYieldMs(int windowsDelayMs)
            => LinuxSmoothMode.ComputeFrameworkYield(windowsDelayMs);


        public async Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken token, bool authoritativeCommit = true, bool suppressNonLifecycleRedraw = false)
        {
            if (!updatedData.Any())
            {
                LogVisibilityDiagnostic("APPLY skip pair={pair} reason=no-updated-data hash={hash}", Pair.UserData.AliasOrUID, charaData.DataHash.Value);
                return PairSyncCommitResult.Succeeded();
            }

            _applicationId = Guid.NewGuid();
            var applicationId = _applicationId;
            var applyTotalSw = Stopwatch.StartNew();
            var applyPhaseSw = Stopwatch.StartNew();
            var applyPhases = new List<(string Phase, long Elapsed)>();

            void LogWineApplyPhase(string phase)
            {
                var elapsed = applyPhaseSw.ElapsedMilliseconds;
                applyPhases.Add((phase, elapsed));
                LinuxSmoothMode.RecordReceiverApplyPhase(phase, elapsed);

                if (IsWineRuntime && elapsed >= WineApplyPhaseWarnMs)
                    Logger.LogWarning("[{applicationId}] Linux/Wine receiver apply phase '{phase}' took {elapsed}ms for {pair}; pressure={pressure}", applicationId, phase, elapsed, Pair.UserData.AliasOrUID, LinuxSmoothMode.Pressure);
                else
                    Logger.LogTrace("[{applicationId}] Receiver apply phase '{phase}' took {elapsed}ms for {pair}", applicationId, phase, elapsed, Pair.UserData.AliasOrUID);

                applyPhaseSw.Restart();
            }

            string BuildWineApplyPhaseSummary(long totalMs)
            {
                var loggedMs = applyPhases.Sum(static p => p.Elapsed);
                var untrackedMs = Math.Max(0, totalMs - loggedMs);
                var parts = applyPhases
                    .OrderByDescending(static p => p.Elapsed)
                    .Take(5)
                    .Select(static p => $"{p.Phase}={p.Elapsed}ms")
                    .ToList();

                if (untrackedMs >= WineApplyPhaseWarnMs)
                    parts.Add($"untracked={untrackedMs}ms");

                return parts.Count == 0 ? "none" : string.Join(", ", parts);
            }

            var applyTouchesPlayer = updatedData.ContainsKey(ObjectKind.Player);
            LogVisibilityDiagnostic("APPLY start pair={pair} appBase={appBase} hash={hash} changes={changes} updateFiles={updateFiles} updateManip={updateManip} fileGate={fileGate} assets={assets} missing={missing} downloadedAny={downloadedAny} force={force} lifecycle={lifecycle} redraw={redraw} authoritative={authoritative}",
                Pair.UserData.AliasOrUID,
                applicationBase,
                charaData.DataHash.Value,
                string.Join("; ", updatedData.Select(kvp => $"{kvp.Key}:{string.Join("|", kvp.Value)}")),
                updateModdedPaths,
                updateManip,
                requiresFileReadyGate,
                assetPlan.Entries.Count,
                assetPlan.MissingFiles.Count,
                downloadedAny,
                forceApplyModsForThisApply,
                lifecycleApplyRequestedFromPlan,
                lifecycleRedrawRequestedFromPlan,
                authoritativeCommit);
            var acquireLane = false;
            var queuedLane = false;
            var acquireLifecycleLane = false;

            SyncStorm.RegisterApplyQueued();
            queuedLane = true;
            var lane = SyncStorm.IsActive ? StormApplySemaphore : NormalApplySemaphore;

            try
            {
                var block = CaptureCommitBlockReason("commit start");
                if (block != null)
                {
                    LogVisibilityDiagnostic("APPLY block pair={pair} app={app} stage=commit-start status={status} reason={reason}", Pair.UserData.AliasOrUID, applicationId, block.Status, block.Reason);
                    return block;
                }

                var initialTarget = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                LogVisibilityDiagnostic("APPLY target initial pair={pair} app={app} valid={valid} reason={reason}", Pair.UserData.AliasOrUID, applicationId, initialTarget.Valid, initialTarget.Reason);
                if (!initialTarget.Valid)
                    return PairSyncCommitResult.ActorChanged(initialTarget.Reason, retryImmediately: false);

                var lifecycleReceiverCommit = lifecycleApplyRequestedFromPlan
                    || lifecycleRedrawRequestedFromPlan
                    || _initialApplyPending
                    || _redrawOnNextApplication
                    || forceApplyModsForThisApply;
                var runResolvedCacheReadyGate = ShouldRunResolvedCacheReadyGate(applicationId, requiresFileReadyGate, updateModdedPaths, downloadedAny, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, moddedPaths, assetPlan);
                LogVisibilityDiagnostic("APPLY gates pair={pair} app={app} resolvedCacheGate={cacheGate} moddedPaths={paths} lifecycleCommit={lifecycleCommit}",
                    Pair.UserData.AliasOrUID, applicationId, runResolvedCacheReadyGate, moddedPaths.Count, lifecycleReceiverCommit);
                var fastCachedMeshApply = updateModdedPaths
                    && !downloadedAny
                    && !forceApplyModsForThisApply
                    && !lifecycleReceiverCommit
                    && !lifecycleApplyRequestedFromPlan
                    && !lifecycleRedrawRequestedFromPlan
                    && assetPlan.MissingFiles.Count == 0;
                var resolvedCacheReadyGateCompletedBeforeLane = false;

                if (IsWineRuntime && runResolvedCacheReadyGate)
                {
                    var missingBeforeLane = await WaitForResolvedCachePathsReadyAsync(applicationId, moddedPaths, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);
                    resolvedCacheReadyGateCompletedBeforeLane = true;
                    if (missingBeforeLane.Count > 0)
                    {
                        if (lifecycleRedrawRequestedFromPlan)
                            MarkInitialApplyRequired();

                        var firstMissingApplyRetry = !_hasRetriedAfterMissingAtApply;
                        if (firstMissingApplyRetry)
                            _hasRetriedAfterMissingAtApply = true;

                        Logger.LogWarning(
                            "[{applicationId}] Refusing partial receiver apply for {pair}: {count} required files were not readable before Linux/Wine commit lane",
                            applicationId,
                            Pair.UserData.AliasOrUID,
                            missingBeforeLane.Count);

                        return PairSyncCommitResult.MissingFiles($"{missingBeforeLane.Count} required files were not ready before receiver apply", retryImmediately: lifecycleRedrawRequestedFromPlan || firstMissingApplyRetry);
                    }

                    LogWineApplyPhase("resolved cache ready gate");

                    block = CaptureCommitBlockReason("after Linux/Wine pre-lane cache ready gate");
                    if (block != null)
                        return block;

                    initialTarget = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!initialTarget.Valid)
                        return PairSyncCommitResult.ActorChanged(initialTarget.Reason, retryImmediately: false);
                }

                if (lifecycleReceiverCommit)
                {
                    await GlobalLifecycleApplySemaphore.WaitAsync(token).ConfigureAwait(false);
                    acquireLifecycleLane = true;
                    await WaitForLifecycleApplyDispatchSpacingAsync(token).ConfigureAwait(false);
                }

                if (!fastCachedMeshApply)
                    await WaitForReceiverApplyDispatchSpacingAsync(lifecycleReceiverCommit, token).ConfigureAwait(false);
                else if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("[{applicationId}] Fast cached mesh apply for {pair}; skipping receiver dispatch spacing", applicationId, Pair.UserData.AliasOrUID);

                await lane.WaitAsync(token).ConfigureAwait(false);
                acquireLane = true;
                queuedLane = false;
                SyncStorm.RegisterApplyStarted();

                if (IsWineRuntime)
                    LogWineApplyPhase("apply dispatch/lane wait");

                block = CaptureCommitBlockReason("lane acquired");
                if (block != null)
                    return block;

                var needsPenumbraBinding = updateModdedPaths || updateManip || lifecycleRedrawRequestedFromPlan || _redrawOnNextApplication || forceApplyModsForThisApply;
                var primePenumbraBeforeDrawSettle = needsPenumbraBinding && (updateModdedPaths || downloadedAny || forceApplyModsForThisApply || lifecycleApplyRequestedFromPlan || lifecycleRedrawRequestedFromPlan);
                if (!primePenumbraBeforeDrawSettle)
                    await WaitForTargetDrawSettleAsync(applicationId, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);
                else if (_charaHandler?.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                    Logger.LogTrace("[{applicationId}] Priming receiver temp files before draw settle for {pair}; draw state={draw}", applicationId, Pair.UserData.AliasOrUID, _charaHandler?.CurrentDrawCondition);

                if (IsWineRuntime)
                    LogWineApplyPhase("receiver draw settle");

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
                    var bindingYieldMs = fastCachedMeshApply && !bindingReassigned ? 0 : WineAwareFrameworkYieldMs(bindingReassigned ? 75 : 25);
                    if (bindingYieldMs > 0)
                        await YieldToFrameworkAsync(token, bindingYieldMs).ConfigureAwait(false);
                    LogWineApplyPhase("Penumbra binding");

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);
                }

                if (runResolvedCacheReadyGate && !resolvedCacheReadyGateCompletedBeforeLane)
                {
                    LogVisibilityDiagnostic("APPLY cache-gate start pair={pair} app={app} paths={paths}", Pair.UserData.AliasOrUID, applicationId, moddedPaths.Count);
                    var missingAtApply = await WaitForResolvedCachePathsReadyAsync(applicationId, moddedPaths, lifecycleRedrawRequestedFromPlan, token).ConfigureAwait(false);
                    LogVisibilityDiagnostic("APPLY cache-gate result pair={pair} app={app} missing={missing}", Pair.UserData.AliasOrUID, applicationId, missingAtApply.Count);
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

                if (runResolvedCacheReadyGate && !resolvedCacheReadyGateCompletedBeforeLane)
                    LogWineApplyPhase("resolved cache ready gate");
                else if (IsWineRuntime)
                    LogWineApplyPhase("pre-temp commit setup");

                token.ThrowIfCancellationRequested();
                block = CaptureCommitBlockReason("before Penumbra temp mod commit");
                if (block != null)
                    return block;

                var penumbraChanged = false;
                var tempMods = PairApplyUtilities.BuildNonMountMusicPenumbraTempMods(moddedPaths);
                var mountMusicTempMods = PairApplyUtilities.BuildMountMusicPenumbraTempMods(moddedPaths);
                var tempModsFingerprint = PairApplyUtilities.ComputeTempModsFingerprint(tempMods);
                var mountMusicTempModsFingerprint = PairApplyUtilities.ComputeTempModsFingerprint(mountMusicTempMods);
                var previousTempMods = _lastAppliedTempModsSnapshot;
                var previousTransientSupportFingerprint = _lastAppliedTransientSupportFingerprint;
                LogVisibilityDiagnostic("APPLY penumbra-temp plan pair={pair} app={app} tempMods={tempMods} mountMusic={mountMusic} tempChanged={tempChanged} mountChanged={mountChanged} activeMod={activeMod} force={force} bindingReassigned={bindingReassigned}",
                    Pair.UserData.AliasOrUID,
                    applicationId,
                    tempMods.Count,
                    mountMusicTempMods.Count,
                    !string.Equals(tempModsFingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal),
                    !string.Equals(mountMusicTempModsFingerprint, _lastAppliedMountMusicTempModsFingerprint, StringComparison.Ordinal),
                    _activeTempFilesModName ?? string.Empty,
                    forceApplyModsForThisApply,
                    bindingReassigned);
                var tempModsChanged = !string.Equals(tempModsFingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal);
                var mountMusicTempModsChanged = !string.Equals(mountMusicTempModsFingerprint, _lastAppliedMountMusicTempModsFingerprint, StringComparison.Ordinal);

                var reassertPlayerTempModsForPureFileUpdate = IsPurePlayerModFilesOnlyApply(updatedData) && updateModdedPaths;

                if (updateModdedPaths || forceApplyModsForThisApply || bindingReassigned)
                {
                    if (tempMods.Count == 0)
                    {
                        if (!string.IsNullOrWhiteSpace(_lastAppliedTempModsFingerprint) && !string.Equals(_lastAppliedTempModsFingerprint, "EMPTY", StringComparison.Ordinal))
                        {
                            LogVisibilityDiagnostic("APPLY penumbra-temp clear pair={pair} app={app} collection={collection} mod={mod}", Pair.UserData.AliasOrUID, applicationId, _penumbraCollection, TempFilesModName);
                            await _ipcManager.Penumbra.ClearNamedTemporaryModsAsync(Logger, applicationId, _penumbraCollection, TempFilesModName, TempFilesModPriority).ConfigureAwait(false);
                            penumbraChanged = true;
                        }

                        _activeTempFilesModName = null;
                        _activeTempFilesModPriority = 0;
                        _lastAppliedTempModsFingerprint = "EMPTY";
                        _lastAppliedTempModsSnapshot = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _lastAppliedTransientSupportFingerprint = "EMPTY";
                    }
                    else if (tempModsChanged || forceApplyModsForThisApply || bindingReassigned || reassertPlayerTempModsForPureFileUpdate || !string.Equals(_activeTempFilesModName, TempFilesModName, StringComparison.Ordinal))
                    {
                        LogVisibilityDiagnostic("APPLY penumbra-temp set pair={pair} app={app} collection={collection} mod={mod} entries={entries} fingerprint={fingerprint}", Pair.UserData.AliasOrUID, applicationId, _penumbraCollection, TempFilesModName, tempMods.Count, tempModsFingerprint);
                        var applied = await _ipcManager.Penumbra.SetNamedTemporaryModsAsync(Logger, applicationId, _penumbraCollection, TempFilesModName, tempMods, TempFilesModPriority).ConfigureAwait(false);
                        LogVisibilityDiagnostic("APPLY penumbra-temp set-result pair={pair} app={app} applied={applied}", Pair.UserData.AliasOrUID, applicationId, applied);
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

                    if (mountMusicTempMods.Count == 0)
                    {
                        if (!string.IsNullOrWhiteSpace(_lastAppliedMountMusicTempModsFingerprint) && !string.Equals(_lastAppliedMountMusicTempModsFingerprint, "EMPTY", StringComparison.Ordinal))
                            LogVisibilityDiagnostic("APPLY mount-music-temp clear pair={pair} app={app}", Pair.UserData.AliasOrUID, applicationId);
                            await _ipcManager.Penumbra.ClearMountMusicTemporaryModOnDefaultCollectionAsync(Logger, applicationId, MountMusicTempModPriority).ConfigureAwait(false);

                        _lastAppliedMountMusicTempModsFingerprint = "EMPTY";
                        _lastAppliedMountMusicTempModsSnapshot = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    }
                    else if (mountMusicTempModsChanged || forceApplyModsForThisApply || bindingReassigned)
                    {
                        LogVisibilityDiagnostic("APPLY mount-music-temp set pair={pair} app={app} entries={entries}", Pair.UserData.AliasOrUID, applicationId, mountMusicTempMods.Count);
                        var mountMusicApplied = await _ipcManager.Penumbra.SetMountMusicTemporaryModOnDefaultCollectionAsync(Logger, applicationId, mountMusicTempMods, MountMusicTempModPriority).ConfigureAwait(false);
                        LogVisibilityDiagnostic("APPLY mount-music-temp set-result pair={pair} app={app} applied={applied}", Pair.UserData.AliasOrUID, applicationId, mountMusicApplied);
                        if (!mountMusicApplied)
                        {
                            _forceApplyMods = true;
                            MarkInitialApplyRequired(redrawOnNextApplication: false);
                            return PairSyncCommitResult.ApplyFailed("Penumbra rejected receiver mount music default temp mod", retryImmediately: true);
                        }

                        _lastAppliedMountMusicTempModsFingerprint = mountMusicTempModsFingerprint;
                        _lastAppliedMountMusicTempModsSnapshot = new Dictionary<string, string>(mountMusicTempMods, StringComparer.OrdinalIgnoreCase);
                    }
                }

                if (updateModdedPaths || forceApplyModsForThisApply || bindingReassigned)
                {
                    var appliedIndicatorGamePaths = tempMods.Keys
                        .Concat(mountMusicTempMods.Keys)
                        .Select(static path => (path ?? string.Empty).Replace('\\', '/').Trim().TrimStart('/').ToLowerInvariant())
                        .Where(static path => !string.IsNullOrWhiteSpace(path))
                        .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    Pair.ReconcileActiveSyncResourcesWithAppliedPaths(appliedIndicatorGamePaths);
                }

                var manipulationChanged = false;
                if (updateManip || forceApplyModsForThisApply || bindingReassigned)
                {
                    var effectiveManipulationData = Pair.GetEffectiveManipulationData(charaData.ManipulationData);
                    var manipulationFingerprint = PairApplyUtilities.ComputeManipulationFingerprint(effectiveManipulationData);
                    var forceManipulationReassert = forceApplyModsForThisApply || bindingReassigned;
                    if (forceManipulationReassert || !string.Equals(manipulationFingerprint, _lastAppliedManipulationFingerprint, StringComparison.Ordinal))
                    {
                        LogVisibilityDiagnostic("APPLY manipulation commit pair={pair} app={app} collection={collection} empty={empty} fingerprint={fingerprint} force={force} bindingReassigned={bindingReassigned}",
                            Pair.UserData.AliasOrUID,
                            applicationId,
                            _penumbraCollection,
                            string.IsNullOrWhiteSpace(effectiveManipulationData),
                            manipulationFingerprint,
                            forceApplyModsForThisApply,
                            bindingReassigned);
                        if (string.IsNullOrWhiteSpace(effectiveManipulationData))
                            await _ipcManager.Penumbra.ClearManipulationDataAsync(Logger, applicationId, _penumbraCollection).ConfigureAwait(false);
                        else
                            await _ipcManager.Penumbra.SetManipulationDataAsync(Logger, applicationId, _penumbraCollection, effectiveManipulationData).ConfigureAwait(false);

                        _lastAppliedManipulationFingerprint = manipulationFingerprint;
                        manipulationChanged = true;

                        if (forceManipulationReassert)
                        {
                            Logger.LogTrace(
                                "[{applicationId}] Reasserted receiver Penumbra manipulation metadata for {pair}; forceApply={force}, bindingReassigned={bindingReassigned}",
                                applicationId,
                                Pair.UserData.AliasOrUID,
                                forceApplyModsForThisApply,
                                bindingReassigned);
                        }
                    }
                }

                LogWineApplyPhase("Penumbra temp/meta IPC");

                if (penumbraChanged || manipulationChanged)
                {
                    var tempMetaSettleMs = fastCachedMeshApply && penumbraChanged && !manipulationChanged ? 0 : WineAwareFrameworkYieldMs(lifecycleRedrawRequestedFromPlan ? 125 : 50);
                    if (tempMetaSettleMs > 0)
                        await YieldToFrameworkAsync(token, tempMetaSettleMs).ConfigureAwait(false);
                    LogWineApplyPhase("Penumbra temp/meta settle");

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);
                }

                var playerWornEquipmentOrAccessoryOnlyApply = IsPlayerWornEquipmentOrAccessoryOnlyApply(assetPlan, updatedData, previousTempMods, tempMods);
                var redrawRequired = ShouldRedrawForApply(lifecycleRedrawRequestedFromPlan, assetPlan, previousTempMods, tempMods, previousTransientSupportFingerprint, penumbraChanged, manipulationChanged, updatedData, playerWornEquipmentOrAccessoryOnlyApply);
                if (suppressNonLifecycleRedraw && !lifecycleRedrawRequestedFromPlan)
                    redrawRequired = false;
                var playerCriticalRedrawAsset = HasPlayerCriticalRedrawAsset(assetPlan, updatedData, previousTempMods, tempMods);
                var forceLightweightMetadataReapply = forceApplyModsForThisApply || bindingReassigned;
                var playerGlamourerApplyRequested = updatedData.TryGetValue(ObjectKind.Player, out var playerApplyChanges)
                    && playerApplyChanges.Contains(PlayerChanges.Glamourer)
                    && HasPlayerGlamourerPayload(charaData);
                var authoritativePlayerAppearanceApply = ShouldUseAuthoritativePlayerAppearanceApply(playerGlamourerApplyRequested, lifecycleRedrawRequestedFromPlan, redrawRequired);
                var glamourerApplyFlags = SelectGlamourerApplyFlags(lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, redrawRequired, penumbraChanged, manipulationChanged, forceApplyModsForThisApply, bindingReassigned, updatedData, playerWornEquipmentOrAccessoryOnlyApply, authoritativePlayerAppearanceApply);
                LogVisibilityDiagnostic("APPLY appearance-decision pair={pair} app={app} penumbraChanged={penumbraChanged} manipulationChanged={manipChanged} redrawRequired={redraw} playerGlamourer={playerGlamourer} authoritativePlayer={authoritativePlayer} equipmentOnly={equipmentOnly}",
                    Pair.UserData.AliasOrUID,
                    applicationId,
                    penumbraChanged,
                    manipulationChanged,
                    redrawRequired,
                    playerGlamourerApplyRequested,
                    authoritativePlayerAppearanceApply,
                    playerWornEquipmentOrAccessoryOnlyApply);
                var wineLightweightRedrawGlamourerApply = IsWineRuntime
                    && playerGlamourerApplyRequested
                    && redrawRequired
                    && !lifecycleRedrawRequestedFromPlan
                    && glamourerApplyFlags != ApplyFlagEx.StateDefault;
                var awaitGlamourer = playerGlamourerApplyRequested
                    || authoritativePlayerAppearanceApply
                    || redrawRequired
                    || (manipulationChanged && !playerWornEquipmentOrAccessoryOnlyApply)
                    || (penumbraChanged && glamourerApplyFlags == ApplyFlagEx.StateDefault);

                if (playerWornEquipmentOrAccessoryOnlyApply && manipulationChanged)
                    Logger.LogTrace("[{applicationId}] Player worn equipment/accessory-only apply has manipulation metadata; keeping it on the non-redraw equipment path for {pair}", applicationId, Pair.UserData.AliasOrUID);

                if (authoritativePlayerAppearanceApply)
                    Logger.LogTrace("[{applicationId}] Using non-blocking authoritative Glamourer state for {pair} to replace the previous locked appearance before/through draw", applicationId, Pair.UserData.AliasOrUID);
                else if (playerGlamourerApplyRequested && glamourerApplyFlags != ApplyFlagEx.StateDefault)
                    Logger.LogTrace("[{applicationId}] Using lightweight Glamourer apply flags {flags} for {pair}", applicationId, glamourerApplyFlags, Pair.UserData.AliasOrUID);

                if (redrawRequired || awaitGlamourer)
                    _suppressClassJobRedrawUntilTick = Environment.TickCount64 + 1500;

                var waitForAuthoritativePlayerAppearanceSettle = authoritativePlayerAppearanceApply
                    && applyTouchesPlayer
                    && (downloadedAny
                        || lifecycleApplyRequestedFromPlan
                        || lifecycleRedrawRequestedFromPlan
                        || _initialApplyPending
                        || _redrawOnNextApplication
                        || forceApplyModsForThisApply
                        || bindingReassigned);
                if (waitForAuthoritativePlayerAppearanceSettle
                    && IsWineRuntime
                    && !lifecycleApplyRequestedFromPlan
                    && !lifecycleRedrawRequestedFromPlan
                    && !(redrawRequired && playerCriticalRedrawAsset))
                {
                    // On Wine this full draw-settle wait is the 1.5s+ spike seen in receiver
                    // applies.  The Glamourer state is still submitted synchronously below, then
                    // the existing guarded post-draw snap verifies the final payload once the
                    // actor has finished drawing, so the final visual state stays unchanged.
                    waitForAuthoritativePlayerAppearanceSettle = false;
                    Logger.LogTrace(
                        "[{applicationId}] Linux/Wine authoritative player appearance will finish via guarded post-draw snap for {pair}",
                        applicationId,
                        Pair.UserData.AliasOrUID);
                }
                if (waitForAuthoritativePlayerAppearanceSettle)
                {
                    Logger.LogTrace(
                        "[{applicationId}] Waiting for authoritative player appearance settle before first visible commit for {pair}",
                        applicationId,
                        Pair.UserData.AliasOrUID);
                }

                var queuePostDrawPlayerSnap = applyTouchesPlayer
                    && playerGlamourerApplyRequested
                    && (!redrawRequired || wineLightweightRedrawGlamourerApply)
                    && !waitForAuthoritativePlayerAppearanceSettle
                    && (authoritativePlayerAppearanceApply || wineLightweightRedrawGlamourerApply || lifecycleApplyRequestedFromPlan || forceApplyModsForThisApply || bindingReassigned || penumbraChanged || manipulationChanged);
                var postDrawGlamourerApplyFlags = wineLightweightRedrawGlamourerApply ? ApplyFlagEx.StateDefault : glamourerApplyFlags;
                var expectedPostDrawPayloadFingerprint = queuePostDrawPlayerSnap
                    ? PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(charaData)
                    : string.Empty;

                foreach (var changeSet in OrderChangeSets(updatedData))
                {
                    token.ThrowIfCancellationRequested();

                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    LogVisibilityDiagnostic("APPLY customization start pair={pair} app={app} kind={kind} changes={changes} redraw={redraw} awaitGlamourer={awaitGlamourer} flags={flags}",
                        Pair.UserData.AliasOrUID,
                        applicationId,
                        changeSet.Key,
                        string.Join("|", changeSet.Value),
                        redrawRequired,
                        awaitGlamourer && changeSet.Key == ObjectKind.Player,
                        glamourerApplyFlags);
                    var customizationApplied = await ApplyCustomizationDataAsync(
                        applicationId,
                        changeSet,
                        charaData,
                        redrawRequired,
                        forceLightweightMetadataReapply,
                        awaitGlamourer && changeSet.Key == ObjectKind.Player,
                        waitForAuthoritativePlayerAppearanceSettle && changeSet.Key == ObjectKind.Player,
                        glamourerApplyFlags,
                        token).ConfigureAwait(false);

                    LogVisibilityDiagnostic("APPLY customization result pair={pair} app={app} kind={kind} applied={applied}", Pair.UserData.AliasOrUID, applicationId, changeSet.Key, customizationApplied);

                    if (!customizationApplied && changeSet.Key == ObjectKind.Player)
                    {
                        _forceApplyMods = true;
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.ApplyFailed("receiver player customization/Glamourer state was not accepted", retryImmediately: true);
                    }
                }

                LogWineApplyPhase("Customization/Glamourer IPC");

                if (redrawRequired)
                {
                    target = await CaptureRemoteApplyTargetAsync(token).ConfigureAwait(false);
                    if (!target.Valid)
                        return PairSyncCommitResult.ActorChanged(target.Reason, retryImmediately: false);

                    LogVisibilityDiagnostic("APPLY redraw start pair={pair} app={app} critical={critical}", Pair.UserData.AliasOrUID, applicationId, lifecycleRedrawRequestedFromPlan || playerCriticalRedrawAsset);
                    var redrawFired = await OnePassRedrawAsync(applicationId, token, criticalRedraw: lifecycleRedrawRequestedFromPlan || playerCriticalRedrawAsset).ConfigureAwait(false);
                    LogVisibilityDiagnostic("APPLY redraw result pair={pair} app={app} fired={fired}", Pair.UserData.AliasOrUID, applicationId, redrawFired);
                    if ((lifecycleRedrawRequestedFromPlan || playerCriticalRedrawAsset) && !redrawFired)
                    {
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.Deferred("required receiver redraw did not acknowledge", retryImmediately: true);
                    }
                }
                else if (penumbraChanged && !playerWornEquipmentOrAccessoryOnlyApply && !playerGlamourerApplyRequested && HasPlayerGlamourerPayload(charaData) && _charaHandler != null && _charaHandler.Address != nint.Zero)
                {
                    await _ipcManager.Glamourer.ReapplyDirectAsync(Logger, _charaHandler, applicationId, token).ConfigureAwait(false);
                }

                LogWineApplyPhase("redraw/reapply");

                if (!authoritativeCommit)
                {
                    Logger.LogDebug("[{applicationId}] Completed appearance-first preview apply for {pair}; final payload remains pending", applicationId, Pair.UserData.AliasOrUID);
                    return PairSyncCommitResult.Succeeded();
                }

                if (applyTouchesPlayer && HasPlayerFilePayload(charaData) && _penumbraCollection != Guid.Empty && _lastAssignedObjectIndex.HasValue)
                {
                    var liveBinding = await _ipcManager.Penumbra.TryGetObjectEffectiveCollectionMatchAsync(
                        Logger,
                        _penumbraCollection,
                        _lastAssignedObjectIndex.Value,
                        Pair.Ident,
                        _lastAssignedPlayerAddress,
                        PlayerName ?? Pair.UserData.AliasOrUID).ConfigureAwait(false);

                    LogVisibilityDiagnostic("APPLY commit-verify binding pair={pair} app={app} checked={checked} matches={matches} effective={effective}/{name} expected={expected}",
                        Pair.UserData.AliasOrUID,
                        applicationId,
                        liveBinding.Checked,
                        liveBinding.Matches,
                        liveBinding.EffectiveCollectionId,
                        liveBinding.EffectiveCollectionName,
                        _penumbraCollection);
                    if (!liveBinding.Checked || !liveBinding.Matches)
                    {
                        Logger.LogDebug(
                            "[{applicationId}] Refusing receiver commit for {pair}: Penumbra effective collection at idx {idx} is {effective}/{name}, expected {expected}; reasserting instead of caching vanilla state",
                            applicationId,
                            Pair.UserData.AliasOrUID,
                            _lastAssignedObjectIndex.Value,
                            liveBinding.EffectiveCollectionId,
                            liveBinding.EffectiveCollectionName,
                            _penumbraCollection);
                        _forceApplyMods = true;
                        MarkInitialApplyRequired();
                        return PairSyncCommitResult.ApplyFailed("receiver player Penumbra temp collection binding was not live at commit", retryImmediately: true);
                    }

                    if (tempMods.Count > 0)
                    {
                        var tempModLive = await _ipcManager.Penumbra.HasTemporaryModOnCollectionAsync(Logger, _penumbraCollection, TempFilesModName, TempFilesModPriority).ConfigureAwait(false);
                        LogVisibilityDiagnostic("APPLY commit-verify tempmod pair={pair} app={app} collection={collection} mod={mod} live={live}", Pair.UserData.AliasOrUID, applicationId, _penumbraCollection, TempFilesModName, tempModLive);
                        if (!tempModLive)
                        {
                            Logger.LogDebug(
                                "[{applicationId}] Refusing receiver commit for {pair}: Penumbra collection {collection} is assigned but {tempModName} is not live; reasserting temp mods instead of caching vanilla state",
                                applicationId,
                                Pair.UserData.AliasOrUID,
                                _penumbraCollection,
                                TempFilesModName);
                            _forceApplyMods = true;
                            MarkInitialApplyRequired();
                            return PairSyncCommitResult.ApplyFailed("receiver player Penumbra temp files mod was not live at commit", retryImmediately: true);
                        }
                    }
                }

                LogVisibilityDiagnostic("APPLY commit-cache pair={pair} app={app} hash={hash} payload={payload} queuePostDraw={postDraw}",
                    Pair.UserData.AliasOrUID,
                    applicationId,
                    charaData.DataHash.Value,
                    PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(charaData),
                    queuePostDrawPlayerSnap);
                _cachedData = charaData;

                if (applyTouchesPlayer)
                    _redrawOnNextApplication = false;

                if (queuePostDrawPlayerSnap)
                    QueuePostDrawPlayerAppearanceSnap(applicationId, charaData, postDrawGlamourerApplyFlags, expectedPostDrawPayloadFingerprint);
                _lifecycleRedrawApplications.TryRemove(applicationBase, out _);
                _forceApplyMods = false;
                _hasRetriedAfterMissingAtApply = false;
                _lastApplyCompletedTick = Environment.TickCount64;

                if (downloadedAny || _hasRetriedAfterMissingDownload)
                    RequestPostApplyRepair(charaData);

                applyTotalSw.Stop();
                if (IsWineRuntime && applyTotalSw.ElapsedMilliseconds >= WineApplyTotalWarnMs)
                {
                    var phaseSummary = BuildWineApplyPhaseSummary(applyTotalSw.ElapsedMilliseconds);
                    Logger.LogWarning("[{applicationId}] Linux/Wine receiver apply phases for {pair}: {phases}", applicationId, Pair.UserData.AliasOrUID, phaseSummary);
                    Logger.LogWarning("[{applicationId}] Linux/Wine receiver apply total took {elapsed}ms for {pair}; assets={assets}, hashes={hashes}, tempMods={tempMods}, redraw={redraw}, downloadedAny={downloadedAny}, pressure={pressure}", applicationId, applyTotalSw.ElapsedMilliseconds, Pair.UserData.AliasOrUID, assetPlan.Entries.Count, assetPlan.UniqueHashes.Count, tempMods.Count, redrawRequired, downloadedAny, LinuxSmoothMode.Pressure);
                }

                LogVisibilityDiagnostic("APPLY success pair={pair} app={app} elapsedMs={elapsed} assets={assets} tempMods={tempMods} redraw={redraw}",
                    Pair.UserData.AliasOrUID,
                    applicationId,
                    applyTotalSw.ElapsedMilliseconds,
                    assetPlan.Entries.Count,
                    tempMods.Count,
                    redrawRequired);
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

                if (acquireLifecycleLane)
                    GlobalLifecycleApplySemaphore.Release();
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

        private bool ShouldRunResolvedCacheReadyGate(Guid applicationId, bool requiresFileReadyGate, bool updateModdedPaths, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan)
        {
            if (!requiresFileReadyGate)
                return false;

            if (!(updateModdedPaths || downloadedAny || forceApplyModsForThisApply || lifecycleRedrawRequestedFromPlan))
                return false;

            // ModPathResolver already performs the expensive cache/path existence validation while
            // building the asset plan, and the download path recalculates the plan after file writes
            // complete. On Wine, reopening every resolved path again here is extremely expensive for
            // large outfits/animation packs and shows up as multi-second receiver stutter even when
            // downloadedAny=false. Only keep this gate when the plan still has known missing files.
            if (assetPlan.MissingFiles.Count == 0)
            {
                Logger.LogDebug(
                    "[{applicationId}] Skipping duplicate resolved cache-readiness scan for {pair}; {count} resolved files were already validated by the {source} plan",
                    applicationId,
                    Pair.UserData.AliasOrUID,
                    moddedPaths.Count,
                    downloadedAny ? "completed download/recalculate" : "cached");
                return false;
            }

            return true;
        }

        private async Task<IReadOnlyList<KeyValuePair<(string GamePath, string? Hash), string>>> WaitForResolvedCachePathsReadyAsync(Guid applicationId, Dictionary<(string GamePath, string? Hash), string> applyPaths, bool lifecycleCritical, CancellationToken token)
        {
            if (applyPaths.Count == 0)
                return Array.Empty<KeyValuePair<(string GamePath, string? Hash), string>>();

            var deadline = Environment.TickCount64 + (lifecycleCritical ? LifecycleFileReadyWaitMs : ApplyFileReadyWaitMs);
            var pending = applyPaths.ToList();
            var normalizedCacheRoot = NormalizeCacheRootForReadyGate(_fileDbManager.CacheFolder);

            while (true)
            {
                token.ThrowIfCancellationRequested();

                var notReady = new List<KeyValuePair<(string GamePath, string? Hash), string>>(pending.Count);
                var checkedCount = 0;
                foreach (var item in pending)
                {
                    if (!IsResolvedApplyPathReady(item.Value, normalizedCacheRoot))
                        notReady.Add(item);

                    if (++checkedCount % 128 == 0)
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
                await Task.Delay(LinuxSmoothMode.ComputeCacheReadyRetryDelay(50), token).ConfigureAwait(false);
            }
        }

        private static bool IsResolvedApplyPathReady(string? path, string? normalizedCacheRoot)
        {
            if (string.IsNullOrWhiteSpace(path))
                return false;

            if (!IsPathInsideCacheRoot(path, normalizedCacheRoot))
                return true;

            try
            {
                var fileInfo = new FileInfo(path);
                return fileInfo.Exists && fileInfo.Length > 0;
            }
            catch
            {
                return false;
            }
        }

        private static string NormalizeCacheRootForReadyGate(string? cacheRoot)
        {
            if (string.IsNullOrWhiteSpace(cacheRoot))
                return string.Empty;

            try
            {
                return Path.GetFullPath(cacheRoot).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
            }
            catch
            {
                return cacheRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
            }
        }

        private static bool IsPathInsideCacheRoot(string path, string? normalizedCacheRoot)
        {
            if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(normalizedCacheRoot))
                return false;

            try
            {
                if (path.StartsWith(normalizedCacheRoot, StringComparison.OrdinalIgnoreCase))
                    return true;

                var fullPath = Path.GetFullPath(path);
                return fullPath.StartsWith(normalizedCacheRoot, StringComparison.OrdinalIgnoreCase);
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
                    await Task.Delay(IsWineRuntime ? 220 : 60, token).ConfigureAwait(false);

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

                    await Task.Delay(IsWineRuntime ? 120 : 25, token).ConfigureAwait(false);

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
                    var applied = await _ipcManager.Glamourer.ApplyAllAsync(Logger, handler, glamourerPayload, applicationId, token, fireAndForget: false, flags: glamourerApplyFlags, waitForDrawSettle: false).ConfigureAwait(false);
                    if (!applied)
                    {
                        Logger.LogTrace("[{applicationId}] Fast post-draw player appearance snap did not resolve a live Glamourer target for {pair}; marking the next visible apply as initial", applicationId, Pair.UserData.AliasOrUID);
                        MarkInitialApplyRequired(redrawOnNextApplication: false);
                    }
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
            {
                if (IsWineRuntime && redrawRequired && !lifecycleRedrawRequested)
                    return ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;

                return ApplyFlagEx.StateDefault;
            }

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

        private static bool ShouldUseAuthoritativePlayerAppearanceApply(bool playerGlamourerApplyRequested, bool lifecycleRedrawRequested, bool redrawRequired)
        {
            if (!playerGlamourerApplyRequested || redrawRequired || lifecycleRedrawRequested)
                return false;

            // RavaMesh/server character data is an authoritative visible-pair snapshot.  A
            // lightweight Once-only apply can show the right equipment briefly, but it does not
            // replace Glamourer's locked state; the next actor rebuild or stale lock restore can
            // put the target back into vanilla/old gear even though every file is already local.
            // Use StateDefault for every visible player Glamourer snapshot that reaches this lane.
            // This still avoids redraw unless the normal redraw gate requested one.
            return true;
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

            // Transient prop/support assets must stay part of the character's temp-mod state,
            // but they should not force a receiver redraw by themselves.  Redraw remains reserved
            // for actual redraw-critical player assets, Proteus-style pure overlay cache updates,
            // or explicit lifecycle redraws.
            if (HasPlayerProteusOverlayCacheAdditionChanged(assetPlan, previousTempMods, currentTempMods, updatedData))
                return true;

            return HasPlayerCriticalRedrawAssetChanged(assetPlan, previousTempMods, currentTempMods);
        }

        private static bool HasPlayerCriticalRedrawAsset(PairSyncAssetPlan assetPlan, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
            => assetPlan.Entries.Any(static entry => IsPlayerRedrawCriticalAsset(entry))
                || HasPlayerProteusOverlayCacheAdditionChanged(assetPlan, previousTempMods, currentTempMods, updatedData);

        private static bool RequiresPlayerTransientSupportRefresh(PairSyncAssetPlan assetPlan, string? previousTransientSupportFingerprint)
            => assetPlan.RequiresTransientSupportRefresh(previousTransientSupportFingerprint)
                && assetPlan.Entries.Any(static entry => entry.ObjectKind == ObjectKind.Player
                    && !LooksLikeOwnedObjectAssetPath(entry.GamePath)
                    && PairApplyUtilities.IsPlayerTransientSupportRefreshGamePath(entry.GamePath));

        private static bool HasPlayerProteusOverlayCacheAdditionChanged(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        {
            if (!IsPurePlayerModFilesOnlyApply(updatedData) || !assetPlan.HasAssets)
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (entry.ObjectKind != ObjectKind.Player || LooksLikeOwnedObjectAssetPath(entry.GamePath))
                    continue;

                if (entry.Kind is not PairSyncAssetKind.Texture and not PairSyncAssetKind.Material)
                    continue;

                if (!PairApplyUtilities.IsProteusOverlayCacheCandidateGamePath(entry.GamePath))
                    continue;

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!currentTempMods.TryGetValue(gamePath, out var currentPath) || string.IsNullOrWhiteSpace(currentPath))
                    continue;

                // Overlay enable/refresh writes a new resolved file. Overlay disable/removal
                // already clears visually without needing the receiver redraw, so require a
                // current path and a genuinely new/changed target.
                if (previousTempMods == null || previousTempMods.Count == 0)
                    return true;

                if (!previousTempMods.TryGetValue(gamePath, out var previousPath))
                    return true;

                if (!string.Equals(previousPath, currentPath, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static bool IsPurePlayerModFilesOnlyApply(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        {
            if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
                return false;

            if (playerChanges.Count != 1 || !playerChanges.Contains(PlayerChanges.ModFiles))
                return false;

            return updatedData.All(static item => item.Key == ObjectKind.Player || item.Value.Count == 0);
        }

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
            // SCD/sound payloads are support assets, not a reason to redraw the visible
            // character. They should ride the temp-mod state without promoting minion
            // summons or dance support updates into a player redraw.
            if (kind == PairSyncAssetKind.Sound)
                return false;

            return PairApplyUtilities.IsTransientRedrawCriticalGamePath(gamePath)
                || PairApplyUtilities.IsSkeletonOrPhysicsCriticalGamePath(gamePath);
        }

        private static string NormalizeGamePath(string gamePath)
            => string.IsNullOrWhiteSpace(gamePath) ? string.Empty : gamePath.Replace('\\', '/').Trim();

        private static bool HasPlayerGlamourerPayload(CharacterData charaData)
            => charaData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
                && !string.IsNullOrWhiteSpace(glamourerPayload);

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
