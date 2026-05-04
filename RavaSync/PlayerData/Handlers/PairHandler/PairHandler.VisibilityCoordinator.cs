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
    private sealed class VisibilityCoordinator : CoordinatorBase
    {
        private bool _remoteFalseProposalBlockActive;
        private string _remoteFalseProposalBlockedOwner = string.Empty;

        public VisibilityCoordinator(PairHandler owner) : base(owner)
        {
        }

            public void FrameworkUpdate()
            {
                var nowTick = Environment.TickCount64;

                bool isActivelyTransitioning =
                    !IsVisible
                    || string.IsNullOrEmpty(PlayerName)
                    || _initializeTask is { IsCompleted: false }
                    || _charaHandler == null
                    || _initialApplyPending
                    || _cachedData == null
                    || Pair.AutoPausedByOtherSync
                    || Pair.RemoteOtherSyncOverrideActive
                    || Pair.EffectiveOverrideOtherSync
                    || _addressZeroSinceTick >= 0
                    || _identityDriftSinceTick >= 0
                    || _zoneRecoveryUntilTick > nowTick
                    || (_nextOwnedObjectCustomizationRetryTick >= 0 && nowTick >= _nextOwnedObjectCustomizationRetryTick)
                    || (_charaHandler != null && _charaHandler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None);

                var visibilityIntervalMs = isActivelyTransitioning ? ActiveVisibilityUpdateIntervalMs : StableVisibilityUpdateIntervalMs;
                if (_nextVisibilityWorkTick > nowTick)
                    return;

                _nextVisibilityWorkTick = nowTick + visibilityIntervalMs;

                bool ShouldPollNow(int intervalMs)
                {
                    if ((nowTick - _otherSyncPollTick) < intervalMs) return false;
                    _otherSyncPollTick = nowTick;
                    return true;
                }

                bool ShouldReleaseOtherSyncLatch()
                {
                    const int ReleaseGraceMs = 75;

                    if (_otherSyncReleaseCandidateSinceTick < 0)
                    {
                        _otherSyncReleaseCandidateSinceTick = nowTick;
                        return false;
                    }

                    return (nowTick - _otherSyncReleaseCandidateSinceTick) >= ReleaseGraceMs;
                }

                void ClearOtherSyncReleaseCandidate()
                {
                    _otherSyncReleaseCandidateSinceTick = -1;
                }

                bool ShouldAcquireOtherSyncLatch(string owner)
                {
                    const int AcquireGraceMs = 125;

                    owner ??= string.Empty;

                    if (!string.Equals(_otherSyncAcquireCandidateOwner, owner, StringComparison.OrdinalIgnoreCase))
                    {
                        _otherSyncAcquireCandidateOwner = owner;
                        _otherSyncAcquireCandidateSinceTick = nowTick;
                        return false;
                    }

                    if (_otherSyncAcquireCandidateSinceTick < 0)
                    {
                        _otherSyncAcquireCandidateSinceTick = nowTick;
                        return false;
                    }

                    return (nowTick - _otherSyncAcquireCandidateSinceTick) >= AcquireGraceMs;
                }

                void ClearOtherSyncAcquireCandidate()
                {
                    _otherSyncAcquireCandidateSinceTick = -1;
                    _otherSyncAcquireCandidateOwner = string.Empty;
                }

                bool pairLookupFetched = false;
                (string Name, nint Address) pairLookup = default;

                (string Name, nint Address) GetPairLookup()
                {
                    if (!pairLookupFetched)
                    {
                        pairLookup = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
                        pairLookupFetched = true;
                    }

                    return pairLookup;
                }

                nint ResolveOwnershipAddress()
                {
                    var stableAddr = ResolveStrictVisiblePlayerAddress();
                    if (stableAddr != nint.Zero)
                    {
                        _lastKnownOwnershipAddr = stableAddr;
                        return stableAddr;
                    }

                    var canUseTableFallback =!string.IsNullOrEmpty(PlayerName) || Interlocked.CompareExchange(ref Owner._initializeStarted, 0, 0) != 0 || Pair.AutoPausedByOtherSync;

                    if (canUseTableFallback)
                    {
                        var pc = GetPairLookup();
                        if (pc != default((string, nint)) && pc.Address != nint.Zero)
                        {
                            _lastKnownOwnershipAddr = pc.Address;
                            return pc.Address;
                        }
                    }
                    return nint.Zero;
                }

                bool PollAndActOtherSync(string context, int pollIntervalMs)
                {
                    var effectivePoll = Math.Min(pollIntervalMs, 125);
                    if (!ShouldPollNow(effectivePoll))
                        return Pair.AutoPausedByOtherSync;

                    bool TryResolveLocalDesiredOwner(out string owner)
                    {
                        owner = string.Empty;

                        if (Pair.OverrideOtherSync)
                            return false;

                        if (Pair.AutoPausedByOtherSync
                            && _localOtherSyncDecisionYield
                            && !string.IsNullOrWhiteSpace(_localOtherSyncDecisionOwner)
                            && _ipcManager.OtherSync.IsOwnerAvailable(_localOtherSyncDecisionOwner))
                        {
                            owner = _localOtherSyncDecisionOwner;
                            return true;
                        }

                        var localAddress = ResolveOwnershipAddress();
                        if (localAddress == nint.Zero)
                            return false;

                        return _ipcManager.OtherSync.TryGetOwningOtherSync(localAddress, out owner, preferFresh: true)
                            && _ipcManager.OtherSync.IsOwnerAvailable(owner);
                    }

                    var localDesiredYield = TryResolveLocalDesiredOwner(out var localDesiredOwner);
                    BroadcastLocalOtherSyncYieldState(localDesiredYield, localDesiredOwner);

                    var remoteYieldActive = Pair.RemoteOtherSyncOverrideActive && Pair.RemoteOtherSyncYield;
                    var remoteOwner = remoteYieldActive
                        ? (string.IsNullOrWhiteSpace(Pair.RemoteOtherSyncOwner) ? "OtherSync" : Pair.RemoteOtherSyncOwner)
                        : string.Empty;

                    var localYieldActive = _localOtherSyncDecisionYield && !string.IsNullOrWhiteSpace(_localOtherSyncDecisionOwner);
                    var localOwner = localYieldActive ? _localOtherSyncDecisionOwner : string.Empty;

                    var shouldYieldToOtherSync =
                        remoteYieldActive
                        && localYieldActive
                        && string.Equals(remoteOwner, localOwner, StringComparison.OrdinalIgnoreCase);

                    if (shouldYieldToOtherSync)
                    {
                        ClearOtherSyncReleaseCandidate();

                        if (Pair.AutoPausedByOtherSync)
                        {
                            if (string.Equals(Pair.AutoPausedByOtherSyncName, remoteOwner, StringComparison.OrdinalIgnoreCase))
                            {
                                ClearOtherSyncAcquireCandidate();
                                return true;
                            }

                            HandleOtherSyncReleased(requestApplyIfPossible: true);
                            ScheduleRefreshUi();
                        }

                        if (!ShouldAcquireOtherSyncLatch(remoteOwner))
                            return true;

                        EnterYieldedState(remoteOwner);
                        return true;
                    }

                    ClearOtherSyncAcquireCandidate();

                    if (Pair.AutoPausedByOtherSync)
                    {
                        if (!ShouldReleaseOtherSyncLatch())
                            return true;

                        HandleOtherSyncReleased(requestApplyIfPossible: true);
                        ScheduleRefreshUi();
                        ClearOtherSyncReleaseCandidate();
                        return true;
                    }

                    ClearOtherSyncReleaseCandidate();
                    return false;
                }

                if (PollAndActOtherSync("global", pollIntervalMs: 250))
                    return;

                if (string.IsNullOrEmpty(PlayerName))
                {
                    if (Pair.IsPaused|| _dalamudUtil.IsZoning)
                        return;

                    var pc = GetPairLookup();
                    if (pc == default((string, nint))) return;

                    if (Pair.EffectiveOverrideOtherSync && Pair.AutoPausedByOtherSync)
                    {
                        Pair.AutoPausedByOtherSync = false;
                        Pair.AutoPausedByOtherSyncName = string.Empty;
                        ScheduleRefreshUi();
                    }

                    if (Interlocked.CompareExchange(ref Owner._initializeStarted, 1, 0) == 0)
                    {
                        _initializeTask = Task.Run(async () =>
                        {
                            try
                            {
                                await InitializeAsync(pc.Name).ConfigureAwait(false);

                                Mediator.Publish(new EventMessage(new Event(PlayerName!, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
                                    $"Initializing User For Character {pc.Name}")));
                            }
                            catch (Exception)
                            {
                                Interlocked.Exchange(ref Owner._initializeStarted, 0);
                            }
                        });
                    }

                    return;
                }

                if (_charaHandler != null && _charaHandler.Address != nint.Zero && !IsExpectedPlayerAddress(_charaHandler.Address))
                {
                    if (_identityDriftSinceTick < 0)
                    {
                        _identityDriftSinceTick = nowTick;
                        return;
                    }

                    var driftGraceMs = _zoneRecoveryUntilTick > nowTick
                        ? Math.Max(ZoneVisibilityLossGraceMs, IdentityDriftGraceMs)
                        : IdentityDriftGraceMs;

                    if ((nowTick - _identityDriftSinceTick) < driftGraceMs)
                        return;

                    Logger.LogDebug("[{applicationId}] Sustained identity drift for {player}; hard wiping stale visible state at {addr:X}",
                        _applicationId, PlayerName, _charaHandler.Address);

                    ResetToUninitializedState();
                    CancelDownloadCancellationDeferred();
                    return;
                }
                else
                {
                    _identityDriftSinceTick = -1;
                }

                var addr = ResolveStrictVisiblePlayerAddress();

                    if (addr != nint.Zero)
                    {
                        _addressZeroSinceTick = -1;

                    if (_lastAssignedPlayerAddress != nint.Zero && _lastAssignedPlayerAddress != addr)
                    {
                        Logger.LogDebug(
                            "[{applicationId}] Visible actor address changed for {pair}: old={oldAddr:X}, new={newAddr:X}; treating as fresh visible lifecycle replay",
                            _applicationId,
                            Pair.UserData.AliasOrUID,
                            _lastAssignedPlayerAddress,
                            addr);

                        Owner.BeginVisibleLifecycleReplay(addr, "visible actor address changed");
                    }

                    if (Pair.EffectiveOverrideOtherSync && Pair.AutoPausedByOtherSync)
                    {
                        Pair.AutoPausedByOtherSync = false;
                        Pair.AutoPausedByOtherSyncName = string.Empty;
                        _initialApplyPending = true;
                        _lastAttemptedDataHash = null;
                        _redrawOnNextApplication = true;
                        ScheduleRefreshUi();
                    }

                    if (!IsVisible)
                    {
                        Logger.LogDebug(
                            "[{applicationId}] Visibility gained for {pair} at {addr:X}; scheduling clean state apply and confirmed lifecycle redraw",
                            _applicationId,
                            Pair.UserData.AliasOrUID,
                            addr);

                        IsVisible = true;

                        SyncStorm.RegisterVisibleNow();

                        Owner.BeginVisibleLifecycleReplay(addr, "visibility gained");
                    }
                }
                else if (IsVisible)
                {
                    if (Pair.AutoPausedByOtherSync)
                        _initialApplyPending = true;

                    if (_charaHandler != null && IsTransientDrawOnlyCondition(_charaHandler.CurrentDrawCondition))
                    {
                        _addressZeroSinceTick = nowTick;
                        return;
                    }

                    if (_addressZeroSinceTick < 0)
                        _addressZeroSinceTick = nowTick;

                    var visibilityLossGraceMs = _zoneRecoveryUntilTick > nowTick ? ZoneVisibilityLossGraceMs : VisibilityLossGraceMs;
                    if (nowTick - _addressZeroSinceTick >= visibilityLossGraceMs)
                    {
                        var pc = GetPairLookup();
                        if (pc == default((string, nint)))
                        {
                            var oldIdx = _lastAssignedObjectIndex;

                            Logger.LogDebug("[{applicationId}] Visibility lost for {pair}; hard wiping live receiver state", _applicationId, Pair.UserData.AliasOrUID);
                            ResetToUninitializedState();

                            //CleanupOldAssignedIndexIfNeeded(oldIdx, _applicationId);

                            return;
                        }

                        _addressZeroSinceTick = nowTick;
                    }
                }

                ProcessPendingOwnedObjectCustomizationRetry(nowTick);

                if (Pair.IsPaused) return;

                if (!IsVisible || !_initialApplyPending) return;

                var dataForInitialApply = Pair.LastReceivedCharacterData ?? _cachedData;
                if (_charaHandler == null || dataForInitialApply == null) return;
                if (_charaHandler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None) return;

                var replayAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
                if (!IsVisibleReplayReady(replayAddress, Environment.TickCount64))
                    return;

                var cachedData = Pair.PrepareCharacterDataForLocalApply(dataForInitialApply.DeepClone());
                if (cachedData == null) return;

                var nowTick2 = Environment.TickCount64;
                if (_lastInitialApplyDispatchTick >= 0 && nowTick2 - _lastInitialApplyDispatchTick < VisibleReplayDispatchCooldownMs)
                    return;

                var cachedHash = cachedData.DataHash.Value;
                var cachedPipelineKey = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(cachedData);

                if (!string.IsNullOrEmpty(cachedHash))
                {
                    if (IsPairSyncBusyForPayload(cachedPipelineKey))
                        return;

                    if (string.Equals(cachedHash, _lastAttemptedDataHash, StringComparison.Ordinal)
                        && (nowTick2 - _lastApplyCompletedTick) < 2500
                        && !_forceApplyMods
                        && !_redrawOnNextApplication)
                    {
                        return;
                    }
                }

                var appData = Guid.NewGuid();
                _lastInitialApplyDispatchTick = nowTick2;
                _initialApplyPending = false;

                _ = Task.Run(() =>
                {
                    try
                    {
                        Owner.ApplyCharacterData(appData, cachedData, forceApplyCustomization: true, PairSyncReason.BecameVisible);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "[BASE-{appBase}] Deferred initial apply dispatch failed for {player}", appData, PlayerName);
                    }
                });
            }

            private static bool IsTransientDrawOnlyCondition(GameObjectHandler.DrawCondition condition)
                => condition is GameObjectHandler.DrawCondition.RenderFlags
                    or GameObjectHandler.DrawCondition.ModelInSlotLoaded
                    or GameObjectHandler.DrawCondition.ModelFilesInSlotLoaded;

            public async Task InitializeAsync(string name)
            {

                PlayerName = name;

                _charaHandler = await _gameObjectHandlerFactory.Create(
                    ObjectKind.Player,
                    () => _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident),
                    isWatched: false).ConfigureAwait(false);

                _serverConfigManager.AutoPopulateNoteForUid(Pair.UserData.UID, name);


            if (Pair.LastReceivedCharacterData != null)
            {
                _cachedData = null;
                _dataReceivedInDowntime = null;
                _lastAttemptedDataHash = null;
                _forceApplyMods = true;
                _redrawOnNextApplication = true;
                _initialApplyPending = true;
                ResetVisibleReplayReadiness();
            }
        }

        public void ResetToUninitializedState(bool revertLiveCustomizationState = true)
        {
            var applicationId = Guid.NewGuid();
            var oldIdx = _lastAssignedObjectIndex;
            var name = PlayerName ?? string.Empty;
            var playerAddress = ResolveStablePlayerAddress(_charaHandler?.Address ?? nint.Zero);
            var customizeIdsSnapshot = _customizeIds.Count == 0
                ? null
                : _customizeIds.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var objectKindsToRevert = BuildObjectKindsToRevert().ToArray();
            var shouldRevertLiveCustomizationState = revertLiveCustomizationState
                && !_lifetime.ApplicationStopping.IsCancellationRequested
                && !_dalamudUtil.IsZoning
                && !_dalamudUtil.IsInCutscene
                && !string.IsNullOrEmpty(name)
                && playerAddress != nint.Zero;

            IEnumerable<ObjectKind> BuildObjectKindsToRevert()
            {
                var kinds = new HashSet<ObjectKind> { ObjectKind.Player };

                if (_cachedData != null)
                {
                    foreach (var kind in _cachedData.FileReplacements?.Keys ?? Enumerable.Empty<ObjectKind>())
                        kinds.Add(kind);

                    foreach (var kind in _cachedData.GlamourerData?.Keys ?? Enumerable.Empty<ObjectKind>())
                        kinds.Add(kind);

                    foreach (var kind in _cachedData.CustomizePlusData?.Keys ?? Enumerable.Empty<ObjectKind>())
                        kinds.Add(kind);
                }

                foreach (var kind in _customizeIds.Keys)
                    kinds.Add(kind);

                return kinds;
            }

            Logger.LogDebug("[{applicationId}] Hard visibility teardown for {pair}; wiping temp collections, Penumbra bindings, live apply work and applied customization state", applicationId, Pair.UserData.AliasOrUID);

            ResetPairSyncPipelineState();

            var oldApplicationCts = Interlocked.Exchange(ref Owner._applicationCancellationTokenSource, new CancellationTokenSource());
            oldApplicationCts?.CancelDispose();
            _applicationTask = null;
            _pairSyncApplicationTask = null;

            SetUploading(isUploading: false);
            Owner._deferredTempFilesModCleanupCts?.CancelDispose();
            Owner._deferredTempFilesModCleanupCts = null;

            // Forget all live/applied state for this pair.
            // The next time they become visible we want a clean initialize + apply
            // from Pair.LastReceivedCharacterData, not recovery against stale handler state.
            _cachedData = null;
            _dataReceivedInDowntime = null;

            _lastAttemptedDataHash = null;
            _lastAppliedTempModsFingerprint = null;
            _lastAppliedTempModsSnapshot = null;
            _activeTempFilesModName = null;
            _activeTempFilesModPriority = 0;
            _lastAppliedTransientSupportFingerprint = null;
            _lastAppliedManipulationFingerprint = null;
            ClearAppliedLightweightState();
            ResetVisibleReplayReadiness();

            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;

            _forceApplyMods = true;
            _redrawOnNextApplication = true;

            _lastAssignedObjectIndex = null;
            _lastAssignedPlayerAddress = nint.Zero;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;
            _nextTempCollectionRetryNotBeforeUtc = DateTime.MinValue;
            _customizeIds.Clear();
            _pendingOwnedObjectCustomizationRetry.Clear();
            _nextOwnedObjectCustomizationRetryTick = -1;

            _ = Task.Run(async () =>
            {
                try
                {
                    await RemovePenumbraCollectionAsync(applicationId).ConfigureAwait(false);

                    if (shouldRevertLiveCustomizationState)
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        foreach (var objectKind in objectKindsToRevert)
                        {
                            try
                            {
                                await RevertCustomizationDataAsync(objectKind, name, applicationId, cts.Token, playerAddress, customizeIdsSnapshot).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch (InvalidOperationException ex)
                            {
                                Logger.LogDebug(ex, "[{applicationId}] Visibility teardown customization revert skipped for {pair} {objectKind}; actor no longer matched", applicationId, Pair.UserData.AliasOrUID, objectKind);
                            }
                            catch (Exception ex)
                            {
                                Logger.LogWarning(ex, "[{applicationId}] Visibility teardown customization revert failed for {pair} {objectKind}", applicationId, Pair.UserData.AliasOrUID, objectKind);
                            }
                        }
                    }

                    CleanupOldAssignedIndexIfNeeded(oldIdx, applicationId);
                }
                catch (OperationCanceledException)
                {
                    Logger.LogDebug("[{applicationId}] Visibility teardown cleanup timed out for {pair}", applicationId, Pair.UserData.AliasOrUID);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Visibility teardown cleanup failed for {pair}", applicationId, Pair.UserData.AliasOrUID);
                }
            });

            _addressZeroSinceTick = -1;
            _zoneRecoveryUntilTick = -1;
            _identityDriftSinceTick = -1;
            _initialApplyPending = true;
            _otherSyncReleaseCandidateSinceTick = -1;
            _nextVisibilityWorkTick = 0;
            _otherSyncAcquireCandidateSinceTick = -1;
            _otherSyncAcquireCandidateOwner = string.Empty;
            _lastKnownOwnershipAddr = nint.Zero;
            _remoteFalseProposalBlockActive = false;
            _remoteFalseProposalBlockedOwner = string.Empty;

            PlayerName = string.Empty;

            _charaHandler?.Dispose();
            _charaHandler = null;
            DisposeDownloadManager();

            Interlocked.Exchange(ref Owner._initializeStarted, 0);
            _initializeTask = null;

            Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
            Pair.SetCurrentDownloadStatus(null);
            Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
            Pair.ClearDisplayedPerformanceMetrics();

            IsVisible = false;

            _lastBroadcastYield = null;
            _lastBroadcastOwner = string.Empty;

            ScheduleRefreshUi(immediate: true);
        }
    }
}
