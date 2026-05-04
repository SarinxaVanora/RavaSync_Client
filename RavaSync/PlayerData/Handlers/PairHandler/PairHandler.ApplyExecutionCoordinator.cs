using System;
using System.Diagnostics;
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
    private sealed class ApplyExecutionCoordinator : CoordinatorBase
    {
        public ApplyExecutionCoordinator(PairHandler owner) : base(owner)
        {
        }

        public async Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken token)
        {
                    var acquired = false;
                    SemaphoreSlim? applySemaphore = null;
                    var result = PairSyncCommitResult.ApplyFailed("application commit exited before completion", retryImmediately: true);

                    try
                    {
                        _applicationId = Guid.NewGuid();

                        if (!IsVisible)
                        {
                            return PairSyncCommitResult.Hidden("pair became hidden before application commit");
                        }

                        if (Pair.IsPaused)
                        {
                            return PairSyncCommitResult.Paused("pair became paused before application commit");
                        }

                        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
                        {
                            return PairSyncCommitResult.YieldedToOtherSync("OtherSync acquired actor before application commit");
                        }

                        if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                        {
                            return PairSyncCommitResult.Hidden("visible actor handler disappeared before application commit");
                        }

                        var strictVisibleAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
                        if (strictVisibleAddress == nint.Zero || !IsExpectedPlayerAddress(strictVisibleAddress))
                        {
                            MarkInitialApplyRequired();
                            return PairSyncCommitResult.ActorChanged("visible actor became unstable before application commit", retryImmediately: false);
                        }

                        if (_charaHandler!.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                        {
                            await _dalamudUtil.WaitWhileCharacterIsDrawing(Logger, _charaHandler!, _applicationId, 30000, token).ConfigureAwait(false);
                        }

                        token.ThrowIfCancellationRequested();

                        applySemaphore = SyncStorm.IsActive ? StormApplySemaphore : NormalApplySemaphore;
                        //applySemaphore = NormalApplySemaphore;

                        await applySemaphore.WaitAsync(token).ConfigureAwait(false);
                        acquired = true;

                        var lifecycleRedrawRequested = false;

                        try
                        {

                            bool needsRedraw = false;
                            var lifecycleBindingRequired = lifecycleRedrawRequestedFromPlan || _redrawOnNextApplication || _lifecycleRedrawApplications.ContainsKey(applicationBase);

                            var (bindingOk, bindingReassigned) = updateModdedPaths || updateManip || lifecycleBindingRequired
                                ? await EnsurePenumbraCollectionBindingAsync(_applicationId).ConfigureAwait(false)
                                : (true, false);

                            if (!bindingOk)
                            {
                                return PairSyncCommitResult.ApplyFailed("Penumbra collection binding failed before application commit", retryImmediately: true);
                            }

                            if (bindingReassigned)
                            {
                                Logger.LogTrace("[{applicationId}] Penumbra collection binding was reassigned; waiting one settle breath before temp mod application", _applicationId);
                                await LetPenumbraCollectionBindingSettleAsync(token).ConfigureAwait(false);
                            }

                            var tempModsWereAppliedThisPass = false;
                            var explicitRedrawRequiredByTempMods = false;
                            var silentGlamourerRefreshRequiredByTempMods = false;
                            var softGlamourerReapplyRequiredByTempMods = false;
                            var manipulationRedrawRequired = false;
                            var manipulationWasAppliedThisPass = false;
                            var rigAssetRedrawRequired = false;
                            var transientAssetChanged = false;
                            var transientAssetRedrawRequired = false;
                            lifecycleRedrawRequested = lifecycleBindingRequired;

                            var applyNowPaths = moddedPaths;

                            if (requiresFileReadyGate && !updateModdedPaths)
                            {
                                var missingAtApply = await WaitForResolvedCachePathsReadyAsync(
                                    _applicationId,
                                    applyNowPaths,
                                    lifecycleRedrawRequested,
                                    downloadedAny,
                                    token).ConfigureAwait(false);

                                if (missingAtApply.Count > 0)
                                {
                                    var details = string.Join(", ", missingAtApply.Select(x =>
                                        string.IsNullOrEmpty(x.Key.Hash)
                                            ? $"{x.Key.GamePath} {x.Value}"
                                            : $"{x.Key.Hash} ({x.Key.GamePath})"));

                                    Logger.LogWarning(
                                        "[{applicationId}] Aborting appearance apply for {player}: {count} required files were not ready before Glamourer/Customize/Manip application. {details}",
                                        _applicationId,
                                        Pair.UserData.AliasOrUID,
                                        missingAtApply.Count,
                                        details);

                                    var retryImmediately = lifecycleRedrawRequested || !_hasRetriedAfterMissingAtApply;
                                    if (!_hasRetriedAfterMissingAtApply)
                                        _hasRetriedAfterMissingAtApply = true;

                                    if (lifecycleRedrawRequested)
                                        MarkInitialApplyRequired();

                                    return PairSyncCommitResult.MissingFiles($"{missingAtApply.Count} required files were not ready before appearance application", retryImmediately);
                                }

                                if (!assetPlan.HasAssets || !ReferenceEquals(assetPlan.ModdedPaths, applyNowPaths))
                                    assetPlan = BuildPairSyncAssetPlanFromResolvedPaths(applyNowPaths, [], charaData);
                            }

                            List<(string Name, int Priority)>? staleTempFilesModsToClear = null;
                            if (updateModdedPaths)
                            {

                            var missingAtApply = await WaitForResolvedCachePathsReadyAsync(
                                _applicationId,
                                applyNowPaths,
                                lifecycleRedrawRequested,
                                downloadedAny,
                                token).ConfigureAwait(false);

                            if (missingAtApply.Count > 0)
                            {
                                var details = string.Join(", ", missingAtApply.Select(x =>
                                    string.IsNullOrEmpty(x.Key.Hash)
                                        ? $"{x.Key.GamePath} {x.Value}"
                                        : $"{x.Key.Hash} ({x.Key.GamePath})"));

                                Logger.LogWarning(
                                    "[{applicationId}] Aborting pair apply for {player}: {count} non-optional files were not ready at apply time. {details}",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID,
                                    missingAtApply.Count,
                                    details);

                                var retryImmediately = lifecycleRedrawRequested || !_hasRetriedAfterMissingAtApply;
                                if (!_hasRetriedAfterMissingAtApply)
                                    _hasRetriedAfterMissingAtApply = true;

                                if (lifecycleRedrawRequested)
                                    MarkInitialApplyRequired();

                                return PairSyncCommitResult.MissingFiles($"{missingAtApply.Count} required files were not ready before application commit", retryImmediately);
                            }


                            if (!assetPlan.HasAssets || !ReferenceEquals(assetPlan.ModdedPaths, applyNowPaths))
                                assetPlan = BuildPairSyncAssetPlanFromResolvedPaths(applyNowPaths, [], charaData);

                            if (assetPlan.RequiresTransientPrime && _charaHandler != null && _charaHandler.Address != nint.Zero)
                                Mediator.Publish(new PrimeTransientPathsMessage(_charaHandler.Address, ObjectKind.Player, assetPlan.PrimeTransientPaths));

                            var tempMods = PairApplyUtilities.BuildPenumbraTempMods(applyNowPaths);
                            var fingerprint = assetPlan.TempModsFingerprint;
                            var tempModsChanged = !string.Equals(fingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal);
                            var transientSupportFingerprint = assetPlan.TransientSupportFingerprint;
                            var requiresTransientSupportRefresh = assetPlan.RequiresTransientSupportRefresh(_lastAppliedTransientSupportFingerprint);

                            var tempFilesAlreadyApplied = IsTempFilesModSlot(_activeTempFilesModName)
                                && !string.IsNullOrWhiteSpace(_lastAppliedTempModsFingerprint)
                                && string.Equals(fingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal)
                                && !requiresTransientSupportRefresh;

                            var mustReapplyTempMods = tempModsChanged
                                || downloadedAny
                                || forceApplyModsForThisApply
                                || bindingReassigned
                                || requiresTransientSupportRefresh
                                || !tempFilesAlreadyApplied;

                        if (!mustReapplyTempMods && (downloadedAny || forceApplyModsForThisApply || bindingReassigned))
                            {
                                Logger.LogTrace(
                                    "[{applicationId}] Skipping Penumbra temp files reapply for {player}: already applied slot={slot}, downloadedAny={downloadedAny}, force={force}, bindingReassigned={bindingReassigned}",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID,
                                    _activeTempFilesModName,
                                    downloadedAny,
                                    forceApplyModsForThisApply,
                                    bindingReassigned);
                            }

                            rigAssetRedrawRequired = RequiresPlayerRigAssetRedraw(assetPlan, _lastAppliedTempModsSnapshot, tempMods)
                                || (forceApplyModsForThisApply && ContainsPlayerRigCriticalAsset(assetPlan));
                            var transientSupportAssetChanged = RequiresPlayerTransientSupportAssetRedraw(assetPlan, _lastAppliedTempModsSnapshot, tempMods);

                            transientAssetChanged = RequiresPlayerTransientAssetRedraw(assetPlan, _lastAppliedTempModsSnapshot, tempMods)
                                || transientSupportAssetChanged;

                            transientAssetRedrawRequired = transientAssetChanged && (downloadedAny || transientSupportAssetChanged);

                            var mustRedrawForContent = lifecycleRedrawRequested || rigAssetRedrawRequired || transientAssetRedrawRequired;

                            Logger.LogDebug(
                                "[{applicationId}] Temp-file apply decision for {player}: lifecycle={lifecycle}, redrawForContent={redrawContent}, rig={rig}, transientChanged={transientChanged}, transientRedraw={transientRedraw}, tempChanged={tempChanged}, downloadedAny={downloadedAny}, force={force}, bindingReassigned={bindingReassigned}, transientRefresh={transientRefresh}, alreadyApplied={alreadyApplied}, entries={entries}",
                                _applicationId,
                                Pair.UserData.AliasOrUID,
                                lifecycleRedrawRequested,
                                mustRedrawForContent,
                                rigAssetRedrawRequired,
                                transientAssetChanged,
                                transientAssetRedrawRequired,
                                tempModsChanged,
                                downloadedAny,
                                forceApplyModsForThisApply,
                                bindingReassigned,
                                requiresTransientSupportRefresh,
                                tempFilesAlreadyApplied,
                                tempMods.Count);

                            if (rigAssetRedrawRequired && !lifecycleRedrawRequested)
                            {
                                Logger.LogDebug(
                                    "[{applicationId}] Rig-critical temp files detected for {player}; forcing Penumbra redraw for skeleton/physics correctness",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID);
                            }

                            if (transientAssetChanged && !transientAssetRedrawRequired && !lifecycleRedrawRequested)
                            {
                                Logger.LogTrace(
                                    "[{applicationId}] Transient animation/VFX temp files changed for {player}, but no file downloaded during this apply; priming without Penumbra redraw",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID);
                            }

                            if (transientAssetRedrawRequired && !lifecycleRedrawRequested && !rigAssetRedrawRequired)
                            {
                                Logger.LogDebug(
                                    "[{applicationId}] Newly-downloaded transient animation/VFX temp files changed for {player}; forcing one Penumbra redraw for spawned asset refresh",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID);
                            }

                            if (mustReapplyTempMods)
                            {
                            var previousTempFilesModName = _activeTempFilesModName;
                            var previousTempFilesModPriority = _activeTempFilesModPriority;
                            var nextTempFilesModName = TempFilesModName;
                            var nextTempFilesModPriority = TempFilesModBasePriority;

                            Logger.LogDebug(
                                    "[{applicationId}] Applying Penumbra temp files mod {slot} for {player}: entries={entries}, changed={changed}, transientRefresh={transientRefresh}, downloadedAny={downloadedAny}, force={force}, bindingReassigned={bindingReassigned}",
                                    _applicationId,
                                    nextTempFilesModName,
                                    Pair.UserData.AliasOrUID,
                                    tempMods.Count,
                                    tempModsChanged,
                                    requiresTransientSupportRefresh,
                                    downloadedAny,
                                    forceApplyModsForThisApply,
                                    bindingReassigned);

                                var tempModsApplied = await _ipcManager.Penumbra.SetNamedTemporaryModsAsync(Logger, _applicationId, _penumbraCollection, nextTempFilesModName, tempMods, nextTempFilesModPriority).ConfigureAwait(false);

                                if (tempModsApplied)
                                {
                                    tempModsWereAppliedThisPass = true;

                                    staleTempFilesModsToClear = GetStaleTempFilesModsToClear(nextTempFilesModName, nextTempFilesModPriority, previousTempFilesModName, previousTempFilesModPriority).ToList();

                                    _activeTempFilesModName = nextTempFilesModName;
                                    _activeTempFilesModPriority = nextTempFilesModPriority;
                                    _lastAppliedTempModsFingerprint = fingerprint;
                                    _lastAppliedTempModsSnapshot = new Dictionary<string, string>(tempMods, StringComparer.Ordinal);
                                    _lastAppliedTransientSupportFingerprint = transientSupportFingerprint;

                                    if (assetPlan.RequiresTransientPrime && _charaHandler != null && _charaHandler.Address != nint.Zero)
                                    {
                                        Logger.LogTrace(
                                            "[{applicationId}] Re-priming transient paths for {player} after temp-mod apply: count={count}",
                                            _applicationId,
                                            Pair.UserData.AliasOrUID,
                                            assetPlan.PrimeTransientPaths.Count);

                                        Mediator.Publish(new PrimeTransientPathsMessage(_charaHandler.Address, ObjectKind.Player, assetPlan.PrimeTransientPaths));
                                    }


                                    if (mustRedrawForContent)
                                    {
                                        explicitRedrawRequiredByTempMods = true;
                                        needsRedraw = true;
                                    }

                                    if (!lifecycleRedrawRequested && HasPlayerGlamourerPayload(charaData))
                                    {
                                        silentGlamourerRefreshRequiredByTempMods = true;
                                    }

                                    if (!mustRedrawForContent)
                                    {
                                        softGlamourerReapplyRequiredByTempMods = true;
                                    }

                                    long totalBytes = 0;
                                    bool any = false;

                                    if (assetPlan.UniqueHashes.Count > 0)
                                    {
                                        foreach (var hash in assetPlan.UniqueHashes)
                                        {
                                            token.ThrowIfCancellationRequested();

                                            try
                                            {
                                                var cache = _fileDbManager.GetFileCacheByHash(hash);
                                                var sz = cache?.Size;

                                                if (sz.HasValue && sz.Value > 0)
                                                {
                                                    totalBytes += sz.Value;
                                                    any = true;
                                                }
                                            }
                                            catch
                                            {
                                            }
                                        }
                                    }

                                    LastAppliedDataBytes = any ? totalBytes : -1;
                                }
                                else
                                {
                                    Logger.LogWarning("[{applicationId}] Penumbra rejected temp files mod update for {player}; refusing to mark pair sync as committed", _applicationId, Pair.UserData.AliasOrUID);

                                    _forceApplyMods = true;

                                    return PairSyncCommitResult.ApplyFailed( "Penumbra rejected temporary mod update before application commit", retryImmediately: true);
                                }

                            }
                            else if (lifecycleRedrawRequested)
                            {
                                needsRedraw = true;
                            }
                            }

                            if (updateManip)
                            {
                                var effectiveManipulationData = Pair.GetEffectiveManipulationData(charaData.ManipulationData);
                                var newManipFingerprint = PairApplyUtilities.ComputeManipulationFingerprint(effectiveManipulationData);
                                var manipulationChangeRequestsRedraw = lifecycleRedrawRequested
                                    && updatedData.TryGetValue(ObjectKind.Player, out var playerManipChanges)
                                    && playerManipChanges.Contains(PlayerChanges.ForcedRedraw);

                                if (!lifecycleRedrawRequested
                                    && updatedData.TryGetValue(ObjectKind.Player, out var suppressedPlayerManipChanges)
                                    && suppressedPlayerManipChanges.Contains(PlayerChanges.ForcedRedraw))
                                {
                                    Logger.LogDebug("[{applicationId}] Ignoring player manipulation ForcedRedraw outside lifecycle apply for {player}", _applicationId, Pair.UserData.AliasOrUID);
                                }

                                if (!string.Equals(newManipFingerprint, _lastAppliedManipulationFingerprint, StringComparison.Ordinal))
                                {
                                    if (string.IsNullOrEmpty(effectiveManipulationData))
                                    {
                                        await _ipcManager.Penumbra.ClearManipulationDataAsync(Logger, _applicationId, _penumbraCollection).ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        await _ipcManager.Penumbra.SetManipulationDataAsync(Logger, _applicationId, _penumbraCollection, effectiveManipulationData).ConfigureAwait(false);
                                    }

                                    _lastAppliedManipulationFingerprint = newManipFingerprint;
                                    manipulationWasAppliedThisPass = true;

                                    if (manipulationChangeRequestsRedraw)
                                    {
                                        manipulationRedrawRequired = true;
                                        needsRedraw = true;
                                    }
                                    else
                                    {
                                        Logger.LogTrace("[{applicationId}] Manipulation data changed/applied without explicit redraw request", _applicationId);
                                    }
                                }
                                else
                                {
                                    Logger.LogTrace("[{applicationId}] Skipping Penumbra manipulation reapply for {player}: fingerprint unchanged", _applicationId, Pair.UserData.AliasOrUID);
                                }
                            }


                            if ((tempModsWereAppliedThisPass && updateModdedPaths) || manipulationWasAppliedThisPass)
                            {
                                Logger.LogTrace(
                                    "[{applicationId}] Waiting for Penumbra temp mod settle for {player}: filesApplied={filesApplied}, manipulationApplied={manipulationApplied}",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID,
                                    tempModsWereAppliedThisPass,
                                    manipulationWasAppliedThisPass);

                                await LetPenumbraTempModsSettleAsync(token, lifecycleRedrawRequested).ConfigureAwait(false);
                            }

                            if (tempModsWereAppliedThisPass && assetPlan.RequiresFirstUseModelSupportWarmup)
                            {
                                await WarmUpFirstUseModelSupportAsync(assetPlan, token).ConfigureAwait(false);
                            }

                            token.ThrowIfCancellationRequested();

                            if (silentGlamourerRefreshRequiredByTempMods)
                            {
                                if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChangesForSilentRefresh))
                                {
                                    playerChangesForSilentRefresh = [];
                                    updatedData[ObjectKind.Player] = playerChangesForSilentRefresh;
                                }

                                playerChangesForSilentRefresh.Add(PlayerChanges.Glamourer);
                            }


                            var shouldRunExplicitRedraw = lifecycleRedrawRequested || manipulationRedrawRequired || explicitRedrawRequiredByTempMods;
                            var confirmedRedrawRequired = lifecycleRedrawRequested || rigAssetRedrawRequired;
                            var allowPlayerRedraw = shouldRunExplicitRedraw;
                            var forceLightweightMetadataReapply = forceApplyModsForThisApply;

                            Logger.LogDebug(
                                "[{applicationId}] Apply path redraw gate for {player}: explicit={explicit}, lifecycle={lifecycle}, manipulation={manipulation}, tempContent={tempContent}, silentGlamourerRefresh={silent}, softGlamourerReapply={soft}, changes={changes}",
                                _applicationId,
                                Pair.UserData.AliasOrUID,
                                shouldRunExplicitRedraw,
                                lifecycleRedrawRequested,
                                manipulationRedrawRequired,
                                explicitRedrawRequiredByTempMods,
                                silentGlamourerRefreshRequiredByTempMods,
                                softGlamourerReapplyRequiredByTempMods,
                                PairHandler.DescribeUpdatedChanges(updatedData));

                            var playerGlamourerApplySideRefresh = updatedData.TryGetValue(ObjectKind.Player, out var playerChangesForApplySideRefresh)
                                && playerChangesForApplySideRefresh.Contains(PlayerChanges.Glamourer);

                            if (shouldRunExplicitRedraw || playerGlamourerApplySideRefresh || tempModsWereAppliedThisPass)
                                _suppressClassJobRedrawUntilTick = Environment.TickCount64 + 1500;

                            var awaitPlayerGlamourerApply = playerGlamourerApplySideRefresh
                                || silentGlamourerRefreshRequiredByTempMods
                                || shouldRunExplicitRedraw
                                || forceApplyModsForThisApply;

                            foreach (var kind in updatedData)
                            {
                                needsRedraw |= await ApplyCustomizationDataAsync(_applicationId, kind, charaData, allowPlayerRedraw, forceLightweightMetadataReapply, awaitPlayerGlamourerApply, token).ConfigureAwait(false);
                                token.ThrowIfCancellationRequested();
                            }

                            if (softGlamourerReapplyRequiredByTempMods
                                && !shouldRunExplicitRedraw
                                && _charaHandler != null
                                && _charaHandler.Address != nint.Zero)
                            {
                                Logger.LogTrace("[{applicationId}] Reapplying Glamourer after temp files update for {player} without Penumbra redraw", _applicationId, Pair.UserData.AliasOrUID);
                                await _ipcManager.Glamourer.ReapplyDirectAsync(Logger, _charaHandler, _applicationId, token).ConfigureAwait(false);
                            }

                            if (shouldRunExplicitRedraw && _charaHandler != null && _charaHandler.Address != nint.Zero)
                            {
                                var firstUseTransientWarmup = lifecycleRedrawRequested && assetPlan.RequiresFirstUseTransientWarmup;
                                if (firstUseTransientWarmup)
                                {
                                    Logger.LogTrace(
                                        "[{applicationId}] Waiting for first-use transient/VFX temp-mod warmup before lifecycle redraw for {player}",
                                        _applicationId,
                                        Pair.UserData.AliasOrUID);

                                    await LetPenumbraFirstUseTransientSettleAsync(token).ConfigureAwait(false);
                                }

                                var redrawFired = await OnePassRedrawAsync(_applicationId, token, criticalRedraw: confirmedRedrawRequired).ConfigureAwait(false);

                                if (confirmedRedrawRequired && !redrawFired)
                                {
                                    Logger.LogWarning(
                                        "[{applicationId}] Required Penumbra redraw did not fire for {player}; keeping apply pending and retrying. lifecycle={lifecycle}, rig={rig}",
                                        _applicationId,
                                        Pair.UserData.AliasOrUID,
                                        lifecycleRedrawRequested,
                                        rigAssetRedrawRequired);

                                    MarkInitialApplyRequired();
                                    result = PairSyncCommitResult.Deferred("required Penumbra redraw did not fire", retryImmediately: true);
                                    return result;
                                }

                                if (lifecycleRedrawRequested)
                                {
                                    if (firstUseTransientWarmup)
                                    {
                                        await RunSecondChanceFirstUseTransientRedrawAsync(_applicationId, token).ConfigureAwait(false);
                                    }

                                    _redrawOnNextApplication = false;
                                    _lifecycleRedrawApplications.TryRemove(applicationBase, out _);
                                }
                            }
                            else if (lifecycleRedrawRequested)
                            {
                                Logger.LogWarning(
                                    "[{applicationId}] First-visible/lifecycle redraw target was unavailable for {player}; keeping lifecycle redraw pending and retrying",
                                    _applicationId,
                                    Pair.UserData.AliasOrUID);

                                MarkInitialApplyRequired();
                                result = PairSyncCommitResult.Deferred("lifecycle redraw target unavailable", retryImmediately: true);
                                return result;
                            }
                            else if (needsRedraw)
                            {
                                // Sender-side option/temp-mod swaps are handled by the settled silent
                                // Glamourer refresh above.
                            }

                            _cachedData = charaData;

                            var shouldPostRepair = downloadedAny || updateManip || (updateModdedPaths && _hasRetriedAfterMissingDownload) || (updateModdedPaths && _hasRetriedAfterMissingAtApply);

                            if (shouldPostRepair)
                                RequestPostApplyRepair(charaData);

                            if (staleTempFilesModsToClear is { Count: > 0 })
                                ScheduleDeferredTempFilesModCleanup(staleTempFilesModsToClear.Max(x => x.Priority));

                            _lastApplyCompletedTick = Environment.TickCount64;
                            result = PairSyncCommitResult.Succeeded();
                        }
                        catch (OperationCanceledException)
                        {
                            result = PairSyncCommitResult.Cancelled("application commit was cancelled");

                            if (lifecycleRedrawRequested)
                                MarkInitialApplyRequired();
                        }
                        catch (Exception ex)
                        {
                            if (ex is AggregateException aggr && aggr.InnerExceptions.Any(e => e is ArgumentNullException))
                            {
                                Logger.LogDebug("[{applicationId}] Actor invalidated during apply for {pair}; hard wiping visible state", _applicationId, Pair.UserData.AliasOrUID);
                                ResetToUninitializedState();
                                result = PairSyncCommitResult.ActorChanged("application commit hit actor invalidation", retryImmediately: false);
                            }
                            else
                            {
                                Logger.LogWarning(ex, "[{applicationId}] Error during application", _applicationId);
                                result = PairSyncCommitResult.ApplyFailed("application commit threw before completion", retryImmediately: true);
                            }

                            if (lifecycleRedrawRequested)
                                MarkInitialApplyRequired();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.LogDebug("[BASE-{applicationBase}] Application cancelled before semaphore acquired for {player}",
                            applicationBase, PlayerName);
                        result = PairSyncCommitResult.Cancelled("application commit was cancelled before semaphore acquisition");
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "[BASE-{applicationBase}] Unhandled error during apply for {player}",
                            applicationBase, PlayerName);
                        result = PairSyncCommitResult.ApplyFailed("application commit failed before completion", retryImmediately: true);
                    }
                    finally
                    {
                        if (acquired)
                        {
                            applySemaphore?.Release();
                        }
                    }

                    return result;
                }

        private async Task<List<KeyValuePair<(string GamePath, string? Hash), string>>> WaitForResolvedCachePathsReadyAsync(Guid applicationId, Dictionary<(string GamePath, string? Hash), string> applyNowPaths, bool lifecycleRedrawRequested, bool downloadedAny, CancellationToken token)
        {
            var cacheRoot = _fileDbManager.CacheFolder;
            var maxWaitMs = lifecycleRedrawRequested ? 4000 : downloadedAny ? 2500 : 750;
            var deadlineTick = Environment.TickCount64 + maxWaitMs;
            var lastLogTick = 0L;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                var notReady = new List<KeyValuePair<(string GamePath, string? Hash), string>>();

                foreach (var kvp in applyNowPaths)
                {
                    if (!IsResolvedApplyPathReady(kvp.Value, cacheRoot))
                        notReady.Add(kvp);
                }

                if (notReady.Count == 0)
                    return [];

                var nowTick = Environment.TickCount64;
                if (nowTick >= deadlineTick)
                    return notReady;

                if (nowTick - lastLogTick >= 1000)
                {
                    lastLogTick = nowTick;
                    Logger.LogDebug(
                        "[{applicationId}] Waiting for {count} resolved cache files to become readable before Penumbra temp-mod apply for {player}; lifecycle={lifecycle}, downloadedAny={downloadedAny}",
                        applicationId,
                        notReady.Count,
                        Pair.UserData.AliasOrUID,
                        lifecycleRedrawRequested,
                        downloadedAny);
                }

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

        private const string TempFilesModName = "MareChara_Files";
        private const string TempFilesModSlotA = "MareChara_Files_A";
        private const string TempFilesModSlotB = "MareChara_Files_B";
        private const int TempFilesModBasePriority = 100;
        private const int DeferredTempFilesModCleanupDelayMs = 1500;

        private static bool IsTempFilesModSlot(string? tempFilesModName)
            => string.Equals(tempFilesModName, TempFilesModName, StringComparison.Ordinal);

        private void ScheduleDeferredTempFilesModCleanup(int maxPriorityToClear)
        {
            var collection = _penumbraCollection;
            var keepName = _activeTempFilesModName;
            var keepPriority = _activeTempFilesModPriority;
            maxPriorityToClear = Math.Max(maxPriorityToClear, keepPriority);

            if (collection == Guid.Empty || string.IsNullOrWhiteSpace(keepName) || keepPriority < TempFilesModBasePriority)
                return;

            var cleanupApplicationId = _applicationId;
            var cleanupCts = new CancellationTokenSource();
            var oldCts = Interlocked.Exchange(ref Owner._deferredTempFilesModCleanupCts, cleanupCts);
            oldCts?.CancelDispose();

            Logger.LogTrace(
                "[{applicationId}] Deferring stale Penumbra temp mod cleanup for {player}: keep={keepName}@{keepPriority}, delay={delayMs}ms",
                cleanupApplicationId,
                Pair.UserData.AliasOrUID,
                keepName,
                keepPriority,
                DeferredTempFilesModCleanupDelayMs);

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(DeferredTempFilesModCleanupDelayMs, cleanupCts.Token).ConfigureAwait(false);

                    if (cleanupCts.Token.IsCancellationRequested)
                        return;

                    if (!IsVisible || _penumbraCollection != collection)
                        return;

                    await _ipcManager.Penumbra
                        .ClearNamedTemporaryModsPriorityRangeAsync(
                            Logger,
                            cleanupApplicationId,
                            collection,
                            new[] { TempFilesModName, TempFilesModSlotA, TempFilesModSlotB },
                            TempFilesModBasePriority,
                            maxPriorityToClear,
                            keepName,
                            keepPriority)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // A newer item/file apply owns cleanup now.
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Deferred stale Penumbra temp mod cleanup failed for {player}", cleanupApplicationId, Pair.UserData.AliasOrUID);
                }
                finally
                {
                    Interlocked.CompareExchange(ref Owner._deferredTempFilesModCleanupCts, null, cleanupCts);
                    cleanupCts.Dispose();
                }
            });
        }

        private static bool ContainsPlayerRigCriticalAsset(PairSyncAssetPlan assetPlan)
        {
            if (!assetPlan.HasAssets)
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (entry.ObjectKind != ObjectKind.Player)
                    continue;

                if (IsRigCriticalGamePath(NormalizeGamePath(entry.GamePath), entry.Kind))
                    return true;
            }

            return false;
        }

        private static bool RequiresPlayerRigAssetRedraw(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!assetPlan.HasAssets && (previousTempMods == null || previousTempMods.Count == 0))
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (entry.ObjectKind != ObjectKind.Player)
                    continue;

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!IsRigCriticalGamePath(gamePath, entry.Kind))
                    continue;

                if (!currentTempMods.TryGetValue(gamePath, out var currentPath) || string.IsNullOrWhiteSpace(currentPath))
                    continue;

                if (previousTempMods == null || previousTempMods.Count == 0)
                    return true;

                if (!previousTempMods.TryGetValue(gamePath, out var previousPath)
                    || !string.Equals(previousPath, currentPath, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            if (previousTempMods == null || previousTempMods.Count == 0)
                return false;

            foreach (var previousGamePath in previousTempMods.Keys)
            {
                var gamePath = NormalizeGamePath(previousGamePath);
                if (!IsRigCriticalGamePath(gamePath, ClassifyPairSyncAssetKind(gamePath, hash: null)))
                    continue;

                if (!currentTempMods.ContainsKey(gamePath))
                    return true;
            }

            return false;
        }

        private static bool RequiresPlayerTransientAssetRedraw(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!assetPlan.HasAssets && (previousTempMods == null || previousTempMods.Count == 0))
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (entry.ObjectKind != ObjectKind.Player)
                    continue;

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!IsTransientRedrawCriticalGamePath(gamePath, entry.Kind))
                    continue;

                if (!currentTempMods.TryGetValue(gamePath, out var currentPath) || string.IsNullOrWhiteSpace(currentPath))
                    continue;

                if (previousTempMods == null || previousTempMods.Count == 0)
                    return true;

                if (!previousTempMods.TryGetValue(gamePath, out var previousPath)
                    || !string.Equals(previousPath, currentPath, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            if (previousTempMods == null || previousTempMods.Count == 0)
                return false;

            foreach (var previousGamePath in previousTempMods.Keys)
            {
                var gamePath = NormalizeGamePath(previousGamePath);
                if (!IsTransientRedrawCriticalGamePath(gamePath, ClassifyPairSyncAssetKind(gamePath, hash: null)))
                    continue;

                if (!currentTempMods.ContainsKey(gamePath))
                    return true;
            }

            return false;
        }

        private static bool RequiresPlayerTransientSupportAssetRedraw(PairSyncAssetPlan assetPlan, IReadOnlyDictionary<string, string>? previousTempMods, IReadOnlyDictionary<string, string> currentTempMods)
        {
            if (!assetPlan.ContainsVfxPropSupport)
                return false;

            foreach (var entry in assetPlan.Entries)
            {
                if (entry.ObjectKind != ObjectKind.Player)
                    continue;

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!IsTransientModelSupportRedrawGamePath(gamePath))
                    continue;

                if (!currentTempMods.TryGetValue(gamePath, out var currentPath) || string.IsNullOrWhiteSpace(currentPath))
                    continue;

                if (previousTempMods == null || previousTempMods.Count == 0)
                    return true;

                if (!previousTempMods.TryGetValue(gamePath, out var previousPath)
                    || !string.Equals(previousPath, currentPath, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            if (previousTempMods == null || previousTempMods.Count == 0)
                return false;

            foreach (var previousGamePath in previousTempMods.Keys)
            {
                var gamePath = NormalizeGamePath(previousGamePath);
                if (!IsTransientModelSupportRedrawGamePath(gamePath))
                    continue;

                if (!currentTempMods.ContainsKey(gamePath))
                    return true;
            }

            return false;
        }

        private static bool IsTransientModelSupportRedrawGamePath(string gamePath)
        {
            gamePath = NormalizeGamePath(gamePath);
            if (string.IsNullOrEmpty(gamePath))
                return false;

            // Keep this away from normal outfit/body/customisation paths.
            if (gamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
                || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
                || gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!PairApplyUtilities.IsVfxPropSupportGamePath(gamePath)
                && !gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsTransientRedrawCriticalGamePath(string gamePath, PairSyncAssetKind kind)
            => kind == PairSyncAssetKind.Sound
                || PairApplyUtilities.IsTransientRedrawCriticalGamePath(gamePath);

        private static string NormalizeGamePath(string gamePath)
            => string.IsNullOrWhiteSpace(gamePath)
                ? string.Empty
                : gamePath.Replace('\\', '/').Trim();

        private static bool IsRigCriticalGamePath(string gamePath, PairSyncAssetKind kind)
            => PairApplyUtilities.IsSkeletonOrPhysicsCriticalGamePath(gamePath);

        private static IEnumerable<(string Name, int Priority)> GetStaleTempFilesModsToClear(string activeTempFilesModName, int activeTempFilesModPriority, string? previousTempFilesModName, int previousTempFilesModPriority)
        {
            // The returned values are only used to decide whether to schedule the deferred sweep
            // and how high the stale-priority cleanup range needs to go.
            if (!string.IsNullOrWhiteSpace(previousTempFilesModName)
                && previousTempFilesModPriority >= TempFilesModBasePriority
                && (!string.Equals(previousTempFilesModName, activeTempFilesModName, StringComparison.Ordinal)
                    || previousTempFilesModPriority != activeTempFilesModPriority))
            {
                yield return (previousTempFilesModName, previousTempFilesModPriority);
            }
            else if (activeTempFilesModPriority > TempFilesModBasePriority)
            {
                yield return (activeTempFilesModName, activeTempFilesModPriority - 1);
            }
        }

        private static bool HasPlayerGlamourerPayload(CharacterData charaData)
            => charaData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
                && !string.IsNullOrWhiteSpace(glamourerPayload);


        private async Task WarmUpFirstUseModelSupportAsync(PairSyncAssetPlan assetPlan, CancellationToken token)
        {
            if (!assetPlan.RequiresFirstUseModelSupportWarmup)
                return;

            var supportPaths = assetPlan.TransientSupportPaths
                .Where(IsFirstUseModelSupportWarmupGamePath)
                .Select(NormalizeGamePath)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            if (supportPaths.Length == 0)
                return;

            var supportSet = new HashSet<string>(supportPaths, StringComparer.OrdinalIgnoreCase);

            Logger.LogTrace(
                "[{applicationId}] Warming first-use model support for {player}: supportPaths={count}",
                _applicationId,
                Pair.UserData.AliasOrUID,
                supportPaths.Length);

            await LetPenumbraFirstUseTransientSettleAsync(token).ConfigureAwait(false);

            foreach (var entry in assetPlan.Entries)
            {
                token.ThrowIfCancellationRequested();

                var gamePath = NormalizeGamePath(entry.GamePath);
                if (!supportSet.Contains(gamePath))
                    continue;

                TouchFirstUseModelSupportFile(entry.ResolvedPath);
            }

            try
            {
                await _ipcManager.Penumbra.ResolvePathsAsync(supportPaths, []).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "[{applicationId}] First-use model support warmup resolve was ignored for {player}", _applicationId, Pair.UserData.AliasOrUID);
            }

            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
        }

        private static bool IsFirstUseModelSupportWarmupGamePath(string gamePath)
        {
            gamePath = NormalizeGamePath(gamePath);
            if (string.IsNullOrEmpty(gamePath))
                return false;

            if (!PairApplyUtilities.IsVfxPropSupportGamePath(gamePath)
                && !gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase);
        }

        private static void TouchFirstUseModelSupportFile(string? resolvedPath)
        {
            if (string.IsNullOrWhiteSpace(resolvedPath))
                return;

            try
            {
                var fileInfo = new FileInfo(resolvedPath);
                if (!fileInfo.Exists || fileInfo.Length <= 0)
                    return;

                using var stream = new FileStream(resolvedPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                _ = stream.ReadByte();
            }
            catch
            {
                // Best-effort cache touch only. Never break pair application over warmup.
            }
        }

        private async Task RunSecondChanceFirstUseTransientRedrawAsync(Guid applicationId, CancellationToken token)
        {
            if (!IsVisible || _charaHandler == null || _charaHandler.Address == nint.Zero)
                return;

            Logger.LogTrace(
                "[{applicationId}] Running second-chance first-use transient/VFX redraw for {player}",
                applicationId,
                Pair.UserData.AliasOrUID);

            await LetPenumbraFirstUseTransientSettleAsync(token).ConfigureAwait(false);

            if (!IsVisible || _charaHandler == null || _charaHandler.Address == nint.Zero)
                return;

            var secondPassFired = await OnePassRedrawAsync(applicationId, token, criticalRedraw: false).ConfigureAwait(false);
            if (!secondPassFired)
            {
                Logger.LogTrace(
                    "[{applicationId}] Second-chance first-use transient/VFX redraw did not acknowledge for {player}; initial lifecycle redraw already acknowledged",
                    applicationId,
                    Pair.UserData.AliasOrUID);
            }
        }

        private async Task LetPenumbraFirstUseTransientSettleAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            await Task.Delay(125, token).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            await Task.Delay(75, token).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
        }

        private async Task LetPenumbraTempModsSettleAsync(CancellationToken token, bool lifecycleCritical = false)
        {
            token.ThrowIfCancellationRequested();

            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            await Task.Delay(lifecycleCritical ? 75 : 25, token).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);

            if (lifecycleCritical)
            {
                await Task.Delay(25, token).ConfigureAwait(false);
                await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            }
        }

        private async Task LetPenumbraCollectionBindingSettleAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
            await Task.Delay(25, token).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
        }
    }
}
