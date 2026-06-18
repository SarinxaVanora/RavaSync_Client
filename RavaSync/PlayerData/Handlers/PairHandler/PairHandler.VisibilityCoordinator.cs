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
        private const int VisibleSupportReapplyCooldownMs = 1500;

        private readonly int _stableVisibilityJitterMs;
        private bool _remoteFalseProposalBlockActive;
        private string _remoteFalseProposalBlockedOwner = string.Empty;
        private GameObjectHandler.DrawCondition _lastObservedVisibleDrawCondition = GameObjectHandler.DrawCondition.None;
        private long _visibleDrawReloadStartedTick = -1;
        private long _lastVisibleSupportReapplyTick = -1;

        public VisibilityCoordinator(PairHandler owner) : base(owner)
        {
            _stableVisibilityJitterMs = (int)((uint)(owner.Pair.UserData.UID ?? owner.Pair.Ident ?? string.Empty).GetHashCode() % 83);
        }

        public void ResetVisibleSupportReapplyTracking()
        {
            _lastObservedVisibleDrawCondition = GameObjectHandler.DrawCondition.None;
            _visibleDrawReloadStartedTick = -1;
            _lastVisibleSupportReapplyTick = -1;
        }

        public void WakeOtherSyncOwnershipCheck()
        {
            _otherSyncPollTick = 0;
            _nextVisibilityWorkTick = 0;
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
                    || Volatile.Read(ref Owner._vanillaTeardownInProgress) != 0
                    || _addressZeroSinceTick >= 0
                    || _identityDriftSinceTick >= 0
                    || _zoneRecoveryUntilTick > nowTick
                    || (_nextOwnedObjectCustomizationRetryTick >= 0 && nowTick >= _nextOwnedObjectCustomizationRetryTick)
                    || (_charaHandler != null && _charaHandler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None);

                var visibilityIntervalMs = isActivelyTransitioning ? ActiveVisibilityUpdateIntervalMs : StableVisibilityUpdateIntervalMs;
                if (_nextVisibilityWorkTick > nowTick)
                    return;

                _nextVisibilityWorkTick = nowTick + visibilityIntervalMs + (isActivelyTransitioning ? 0 : _stableVisibilityJitterMs);

                if (Volatile.Read(ref Owner._vanillaTeardownInProgress) != 0)
                {
                    // Cleanup must never own the visibility state machine.  The vanilla overwrite can
                    // still be finishing, but room re-entry has to be allowed to initialise/replay
                    // immediately.  Capture the live edge and continue through the normal visibility
                    // path instead of returning here.
                    try
                    {
                        var live = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
                        if (live != default((string, nint)) && live.Address != nint.Zero && !Pair.IsPaused)
                        {
                            _lastKnownOwnershipAddr = live.Address;
                            _initialApplyPending = true;
                            _nextVisibilityWorkTick = 0;
                            Interlocked.Exchange(ref Owner._initializeStarted, 0);
                        }
                    }
                    catch
                    {
                        // Best-effort edge capture only. Normal framework polling will retry.
                    }
                }

                bool ShouldPollNow(int intervalMs)
                {
                    if ((nowTick - _otherSyncPollTick) < intervalMs) return false;
                    _otherSyncPollTick = nowTick;
                    return true;
                }

                bool ShouldReleaseOtherSyncLatch()
                {
                    const int ReleaseGraceMs = 50;

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
                    const int AcquireGraceMs = 50;

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
                    var effectivePoll = Math.Max(pollIntervalMs, Pair.AutoPausedByOtherSync || Pair.RemoteOtherSyncOverrideActive ? 100 : 150);
                    if (!ShouldPollNow(effectivePoll))
                        return Pair.AutoPausedByOtherSync;

                    bool TryResolveLocalDesiredOwner(out string owner)
                    {
                        owner = string.Empty;

                        if (Pair.OverrideOtherSync)
                            return false;

                        var localAddress = ResolveOwnershipAddress();
                        if (localAddress == nint.Zero)
                            return false;

                        var preferFreshOwnership = Pair.RemoteOtherSyncOverrideActive || Pair.AutoPausedByOtherSync || _localOtherSyncDecisionYield;

                        // The local half of the OtherSync handshake must be real current state,
                        // not a remembered previous yes. If the other sync service is connected
                        // but this actor is no longer handled there, RavaSync reclaims the pair.
                        if (_localOtherSyncDecisionYield
                            && !string.IsNullOrWhiteSpace(_localOtherSyncDecisionOwner)
                            && _ipcManager.OtherSync.IsOwnerHandlingAddress(_localOtherSyncDecisionOwner, localAddress, preferFresh: true))
                        {
                            owner = _localOtherSyncDecisionOwner;
                            return true;
                        }

                        return _ipcManager.OtherSync.TryGetOwningOtherSync(localAddress, out owner, preferFresh: preferFreshOwnership)
                            && _ipcManager.OtherSync.IsOwnerAvailable(owner)
                            && _ipcManager.OtherSync.IsOwnerHandlingAddress(owner, localAddress, preferFresh: preferFreshOwnership);
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

                var shouldPollOtherSync = Pair.AutoPausedByOtherSync
                    || Pair.RemoteOtherSyncOverrideActive
                    || Pair.OverrideOtherSync
                    || Pair.EffectiveOverrideOtherSync
                    || _ipcManager.OtherSync.ShouldPollOwnership();

                if (shouldPollOtherSync && PollAndActOtherSync("global", pollIntervalMs: 100))
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
                                await InitializeAsync(pc.Name, pc.Address).ConfigureAwait(false);

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

                    GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
                    CancelDownloadCancellationDeferred();
                    return;
                }
                else
                {
                    _identityDriftSinceTick = -1;
                }

                var addr = ResolveStrictVisiblePlayerAddress();

                if (IsVisible)
                    TrackVisibleDrawReloadForAssociatedSupportAssets(nowTick);

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

                        Owner.BeginVisibleLifecycleReplay(addr, "visibility gained");

                        IsVisible = true;

                        SyncStorm.RegisterVisibleNow();
                        Owner._syncWorker?.Signal(PairSyncReason.BecameVisible);
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
                            GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);

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

                var nowTick2 = Environment.TickCount64;
                if (_lastInitialApplyDispatchTick >= 0 && nowTick2 - _lastInitialApplyDispatchTick < VisibleReplayDispatchCooldownMs)
                    return;

                var sourceData = dataForInitialApply;
                var sourceHash = sourceData.DataHash.Value;

                if (!string.IsNullOrEmpty(sourceHash)
                    && string.Equals(sourceHash, _lastAttemptedDataHash, StringComparison.Ordinal)
                    && (nowTick2 - _lastApplyCompletedTick) < 2500
                    && !_forceApplyMods
                    && !_redrawOnNextApplication)
                {
                    return;
                }

                var appData = Guid.NewGuid();
                _lastInitialApplyDispatchTick = nowTick2;
                _initialApplyPending = false;

                _ = Task.Run(() =>
                {
                    try
                    {
                        var cachedData = Pair.PrepareCharacterDataForLocalApply(sourceData.DeepClone());
                        if (cachedData == null)
                        {
                            MarkInitialApplyRequired();
                            return;
                        }

                        var cachedPipelineKey = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(cachedData);
                        if (IsPairSyncBusyForPayload(cachedPipelineKey))
                        {
                            MarkInitialApplyRequired();
                            return;
                        }

                        Owner.ApplyCharacterData(appData, cachedData, forceApplyCustomization: true, PairSyncReason.BecameVisible);
                    }
                    catch (Exception ex)
                    {
                        MarkInitialApplyRequired();
                        Logger.LogWarning(ex, "[BASE-{appBase}] Deferred initial apply dispatch failed for {player}", appData, PlayerName);
                    }
                });
            }

            private void TrackVisibleDrawReloadForAssociatedSupportAssets(long nowTick)
            {
                if (!IsVisible || _charaHandler == null)
                {
                    _lastObservedVisibleDrawCondition = GameObjectHandler.DrawCondition.None;
                    _visibleDrawReloadStartedTick = -1;
                    return;
                }

                var drawCondition = _charaHandler.CurrentDrawCondition;
                if (drawCondition != GameObjectHandler.DrawCondition.None)
                {
                    if (IsVisibleActorRebuildCondition(drawCondition))
                    {
                        if (_visibleDrawReloadStartedTick < 0)
                            _visibleDrawReloadStartedTick = nowTick;

                        _lastObservedVisibleDrawCondition = drawCondition;
                        return;
                    }

                    // RenderFlags/ModelInSlotLoaded/ModelFilesInSlotLoaded are normal transient draw-settle
                    // states and can be produced by the reapply itself, emotes, VFX support loads, or Penumbra
                    // settling.  They are not proof that the actor was rebuilt and should never start a new
                    // receiver reapply by themselves.  If we already saw a real rebuild, keep waiting for the
                    // draw to settle; otherwise ignore the transient condition entirely.
                    if (_visibleDrawReloadStartedTick >= 0)
                        _lastObservedVisibleDrawCondition = drawCondition;
                    else
                        _lastObservedVisibleDrawCondition = GameObjectHandler.DrawCondition.None;

                    return;
                }

                var hadVisibleActorRebuild = _visibleDrawReloadStartedTick >= 0;

                _lastObservedVisibleDrawCondition = GameObjectHandler.DrawCondition.None;
                _visibleDrawReloadStartedTick = -1;

                if (!hadVisibleActorRebuild)
                    return;

                if (nowTick - _lastVisibleSupportReapplyTick < VisibleSupportReapplyCooldownMs)
                    return;

                var sourceData = Pair.LastReceivedCharacterData ?? _cachedData;
                if (!HasAssociatedTransientSupportPayload(sourceData))
                    return;

                // A real visible actor redraw/rebuild can drop the receiver-side temporary collection binding
                // even though the pair never became IsVisible=false.  Re-apply the already authoritative
                // payload and temp files, but do not request another redraw just because prop/support assets
                // exist.  Crucially, only a real ObjectZero/DrawObjectZero rebuild gets here; ordinary
                // RenderFlags/model-load settle states are ignored so support assets cannot create a reapply loop.
                _lastVisibleSupportReapplyTick = nowTick;
                _forceApplyMods = true;
                _initialApplyPending = true;
                _lastAttemptedDataHash = null;
                _nextVisibilityWorkTick = 0;

                Logger.LogDebug("Visible actor rebuild completed for {pair}; scheduling support-asset temp-mod reapply without forcing a redraw", Pair.UserData.AliasOrUID);
            }

            private static bool IsVisibleActorRebuildCondition(GameObjectHandler.DrawCondition condition)
                => condition is GameObjectHandler.DrawCondition.ObjectZero
                    or GameObjectHandler.DrawCondition.DrawObjectZero;

            private static bool HasAssociatedTransientSupportPayload(CharacterData? data)
            {
                if (data?.FileReplacements == null || data.FileReplacements.Count == 0)
                    return false;

                foreach (var replacementSet in data.FileReplacements.Values)
                {
                    if (replacementSet == null)
                        continue;

                    foreach (var replacement in replacementSet)
                    {
                        if (replacement?.GamePaths == null)
                            continue;

                        foreach (var path in replacement.GamePaths)
                        {
                            if (PairApplyUtilities.IsVfxPropSupportGamePath(path))
                                return true;
                        }
                    }
                }

                return false;
            }

            private static bool IsTransientDrawOnlyCondition(GameObjectHandler.DrawCondition condition)
                => condition is GameObjectHandler.DrawCondition.RenderFlags
                    or GameObjectHandler.DrawCondition.ModelInSlotLoaded
                    or GameObjectHandler.DrawCondition.ModelFilesInSlotLoaded;

            private async Task RunVisibilityTeardownStepAsync(Guid applicationId, string step, Func<Task> action, bool warn = false)
            {
                try
                {
                    await action().ConfigureAwait(false);
                }
                catch (OperationCanceledException ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Visibility teardown {step} timed out/cancelled for {pair}", applicationId, step, Pair.UserData.AliasOrUID);
                }
                catch (Exception ex)
                {
                    if (warn)
                        Logger.LogWarning(ex, "[{applicationId}] Visibility teardown {step} failed for {pair}", applicationId, step, Pair.UserData.AliasOrUID);
                    else
                        Logger.LogDebug(ex, "[{applicationId}] Visibility teardown {step} skipped/failed for {pair}", applicationId, step, Pair.UserData.AliasOrUID);
                }
            }

            private async Task FastClearLightweightPlayerStateAsync(Guid applicationId, string name, nint playerAddress)
            {
                if (playerAddress == nint.Zero || string.IsNullOrWhiteSpace(name))
                    return;

                using var tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Player, () => playerAddress, isWatched: false).ConfigureAwait(false);
                tempHandler.CompareNameAndThrow(name);

                await _ipcManager.Heels.RestoreOffsetForPlayerAsync(playerAddress).ConfigureAwait(false);
                await _ipcManager.Honorific.ClearTitleAsync(playerAddress).ConfigureAwait(false);
                await _ipcManager.Moodles.RevertStatusAsync(playerAddress).ConfigureAwait(false);
                await _ipcManager.PetNames.ClearPlayerData(playerAddress).ConfigureAwait(false);

                Logger.LogDebug("[{applicationId}] Fast vanilla teardown cleared lightweight player state for {pair}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            }

            private async Task<bool> IsVerifiedVanillaObjectIndexTargetAsync(Guid applicationId, int objectIndex, string name, nint expectedAddress)
            {
                try
                {
                    return await _dalamudUtil.RunOnFrameworkThread(() =>
                    {
                        var target = _dalamudUtil.GetCharacterFromObjectTableByIndex(objectIndex);
                        var targetAddress = target?.Address ?? nint.Zero;

                        if (targetAddress == nint.Zero)
                            return false;

                        var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                        if (localPlayerAddress != nint.Zero && targetAddress == localPlayerAddress)
                        {
                            Logger.LogWarning("[{applicationId}] Blocked fast vanilla teardown at idx {idx}; target is local player", applicationId, objectIndex);
                            return false;
                        }

                        if (expectedAddress != nint.Zero && targetAddress == expectedAddress)
                            return true;

                        if (_dalamudUtil.AddressMatchesPlayerIdentCached(Pair.Ident, targetAddress))
                            return true;

                        var targetName = target?.Name.TextValue ?? string.Empty;
                        return !string.IsNullOrWhiteSpace(name)
                            && string.Equals(targetName, name, StringComparison.OrdinalIgnoreCase);
                    }).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Fast vanilla teardown idx validation failed for {pair} idx {idx}", applicationId, Pair.UserData.AliasOrUID, objectIndex);
                    return false;
                }
            }

            private async Task FastVanillaRedrawAsync(Guid applicationId, string name, nint playerAddress, CancellationToken token)
            {
                if (playerAddress == nint.Zero || string.IsNullOrWhiteSpace(name))
                    return;

                if (_lifetime.ApplicationStopping.IsCancellationRequested || _dalamudUtil.IsZoning || _dalamudUtil.IsInCutscene)
                    return;

                var stillExpected = await _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    try
                    {
                        return _dalamudUtil.AddressMatchesPlayerIdentCached(Pair.Ident, playerAddress);
                    }
                    catch
                    {
                        return false;
                    }
                }).ConfigureAwait(false);

                if (!stillExpected)
                {
                    Logger.LogTrace("[{applicationId}] Fast vanilla redraw skipped for {pair}; captured actor is no longer the expected player", applicationId, Pair.UserData.AliasOrUID);
                    return;
                }

                using var tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Player, () => playerAddress, isWatched: false).ConfigureAwait(false);
                tempHandler.CompareNameAndThrow(name);

                if (await _ipcManager.Penumbra.RedrawDirectAsync(Logger, tempHandler, applicationId, token).ConfigureAwait(false))
                    Logger.LogDebug("[{applicationId}] Fast vanilla teardown fired immediate redraw for {pair}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            }

            private async Task<(int Index, nint Address)> ResolveCurrentTeardownTargetAsync(Guid applicationId, string name, nint fallbackAddress)
            {
                try
                {
                    return await _dalamudUtil.RunOnFrameworkThread(() =>
                    {
                        bool TryBuildCandidate(nint address, out (int Index, nint Address) candidate)
                        {
                            candidate = (-1, nint.Zero);

                            if (address == nint.Zero)
                                return false;

                            var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                            if (localPlayerAddress != nint.Zero && address == localPlayerAddress)
                                return false;

                            var obj = _dalamudUtil.CreateGameObject(address);
                            if (obj == null || obj.Address == nint.Zero || obj.ObjectIndex < 0)
                                return false;

                            if (obj.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc)
                                return false;

                            if (!_dalamudUtil.AddressMatchesPlayerIdent(Pair.Ident, address))
                            {
                                var targetName = obj.Name.TextValue ?? string.Empty;
                                if (string.IsNullOrWhiteSpace(name) || !string.Equals(targetName, name, StringComparison.OrdinalIgnoreCase))
                                    return false;
                            }

                            candidate = (obj.ObjectIndex, address);
                            return true;
                        }

                        var address = ResolveStablePlayerAddress(fallbackAddress);
                        if (TryBuildCandidate(address, out var resolvedCandidate))
                            return resolvedCandidate;

                        var live = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
                        if (TryBuildCandidate(live.Address, out var liveCandidate))
                            return liveCandidate;

                        return (-1, nint.Zero);
                    }).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Failed resolving current teardown target for {pair}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    return (-1, nint.Zero);
                }
            }

            private async Task HardClearPenumbraBindingForTeardownAsync(Guid applicationId, int? oldIdx, string name, nint playerAddress, Guid capturedCollection, Task<Guid>? capturedCollectionTask)
            {
                var coll = await ResolvePenumbraCollectionForTeardownAsync(capturedCollection, capturedCollectionTask).ConfigureAwait(false);

                // Owned temporary state is removed by ownership, not by trusting a stale object index.
                // Visibility loss is still targeted to this pair; the global all-player wipe is only
                // queued by PairManager for disconnect/zone-wide reset.
                await _ipcManager.Penumbra.ClearRavaSyncTemporaryCollectionForPlayerAsync(
                    Logger,
                    applicationId,
                    Pair.Ident,
                    name,
                    playerAddress,
                    oldIdx,
                    coll).ConfigureAwait(false);
            }

            private async Task<Guid> ResolvePenumbraCollectionForTeardownAsync(Guid capturedCollection, Task<Guid>? capturedCollectionTask)
            {
                if (capturedCollection != Guid.Empty)
                    return capturedCollection;

                if (capturedCollectionTask == null)
                    return Guid.Empty;

                try
                {
                    return await capturedCollectionTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Temp collection creation task faulted while resolving teardown collection for {pair}", Pair.UserData.AliasOrUID);
                    return Guid.Empty;
                }
            }

            private async Task ClearPenumbraTemporaryModsForTeardownAsync(Guid applicationId, Guid capturedCollection, Task<Guid>? capturedCollectionTask)
            {
                var coll = await ResolvePenumbraCollectionForTeardownAsync(capturedCollection, capturedCollectionTask).ConfigureAwait(false);
                if (coll == Guid.Empty)
                    return;

                await _ipcManager.Penumbra.ClearRavaSyncTemporaryModsFastAsync(Logger, applicationId, coll).ConfigureAwait(false);
            }

            private async Task RunFastVanillaTeardownAsync(Guid applicationId, int teardownGeneration, int? oldIdx, string name, nint playerAddress, Guid capturedCollection, Task<Guid>? capturedCollectionTask, IReadOnlyDictionary<ObjectKind, Guid?>? customizeIdsSnapshot, bool shouldRevertLiveCustomizationState)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));

                bool TeardownStillCurrent() => Volatile.Read(ref Owner._visibilityLifecycleGeneration) == teardownGeneration;

                if (!TeardownStillCurrent())
                {
                    Logger.LogDebug("[{applicationId}] Skipping stale vanilla teardown before Penumbra clear for {pair}", applicationId, Pair.UserData.AliasOrUID);
                    return;
                }

                // The absolute invariant: no RavaSync Penumbra collection assignment or temp mod may
                // survive visibility loss, disconnect, or zoning. Clear by the assigned collection first,
                // not only by the previously recorded object index, because indices/addresses can already
                // be stale by the time teardown runs.
                await RunVisibilityTeardownStepAsync(applicationId, "hard Penumbra collection/slot clear", () => HardClearPenumbraBindingForTeardownAsync(applicationId, oldIdx, name, playerAddress, capturedCollection, capturedCollectionTask), warn: true).ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(_lastAppliedMountMusicTempModsFingerprint) && !string.Equals(_lastAppliedMountMusicTempModsFingerprint, "EMPTY", StringComparison.Ordinal))
                {
                    await RunVisibilityTeardownStepAsync(applicationId, "default collection mount music clear", () => _ipcManager.Penumbra.ClearMountMusicTemporaryModOnDefaultCollectionAsync(Logger, applicationId, MountMusicTempModPriority)).ConfigureAwait(false);
                }
                _lastAppliedMountMusicTempModsFingerprint = null;
                _lastAppliedMountMusicTempModsSnapshot = null;

                if (!TeardownStillCurrent())
                {
                    Logger.LogDebug("[{applicationId}] Stale vanilla teardown stopped after Penumbra clear for {pair}; visible replay now owns restore", applicationId, Pair.UserData.AliasOrUID);
                    return;
                }

                if (!shouldRevertLiveCustomizationState)
                    return;

                var tasks = new List<Task>();

                if (oldIdx.HasValue && oldIdx.Value >= 0)
                {
                    var objectIndexVerified = await IsVerifiedVanillaObjectIndexTargetAsync(applicationId, oldIdx.Value, name, playerAddress).ConfigureAwait(false);
                    if (objectIndexVerified)
                    {
                        tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast Glamourer object-index revert", () => _ipcManager.Glamourer.RevertByObjectIndexAsync(Logger, oldIdx.Value, applicationId)));

                        if (oldIdx.Value <= ushort.MaxValue)
                        {
                            tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast C+ object-index revert", () => _ipcManager.CustomizePlus.RevertByObjectIndexAsync((ushort)oldIdx.Value)));
                            tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast Honorific object-index clear", () => _ipcManager.Honorific.ClearTitleByObjectIndexAsync(oldIdx.Value)));
                            tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast PetNames object-index clear", () => _ipcManager.PetNames.ClearPlayerDataByObjectIndexAsync(oldIdx.Value)));
                        }
                    }
                    else
                    {
                        Logger.LogDebug("[{applicationId}] Fast vanilla teardown skipped unsafe object-index clears for {pair} idx {idx}", applicationId, Pair.UserData.AliasOrUID, oldIdx.Value);
                    }

                    // Do not assign Guid.Empty here. The hard Penumbra path above has already
                    // overwritten the owned RavaSync collection with empty redirects/meta, assigned
                    // that empty collection to the actor, and redrawn. Switching immediately to
                    // Guid.Empty can leave Penumbra rendering the old cached temporary state.
                }

                if (playerAddress != nint.Zero)
                {
                    tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast lightweight player clear", () => FastClearLightweightPlayerStateAsync(applicationId, name, playerAddress)));

                    if (customizeIdsSnapshot != null && customizeIdsSnapshot.TryGetValue(ObjectKind.Player, out var playerCustomizeId) && playerCustomizeId.HasValue)
                        tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast C+ unique-id revert", () => _ipcManager.CustomizePlus.RevertByIdAsync(playerCustomizeId.Value)));
                }

                if (!string.IsNullOrWhiteSpace(name))
                    tasks.Add(RunVisibilityTeardownStepAsync(applicationId, "fast name-based Glamourer revert", () => _ipcManager.Glamourer.RevertByNameAsync(Logger, name, applicationId)));

                if (tasks.Count > 0)
                    await Task.WhenAll(tasks).ConfigureAwait(false);

                if (!TeardownStillCurrent())
                {
                    Logger.LogDebug("[{applicationId}] Stale vanilla teardown skipped final redraw for {pair}; visible replay now owns redraw", applicationId, Pair.UserData.AliasOrUID);
                    return;
                }

                await RunVisibilityTeardownStepAsync(applicationId, "fast immediate redraw", () => FastVanillaRedrawAsync(applicationId, name, playerAddress, cts.Token)).ConfigureAwait(false);
            }


            private void QueueNameOnlyGlamourerRevertIfStillInvisible(string reason)
            {
                var name = !string.IsNullOrWhiteSpace(PlayerName)
                    ? PlayerName!
                    : Owner._lastVanillaTeardownPlayerName ?? string.Empty;

                if (string.IsNullOrWhiteSpace(name) || !_ipcManager.Glamourer.APIAvailable)
                    return;

                var applicationId = Guid.NewGuid();
                var generation = Volatile.Read(ref Owner._visibilityLifecycleGeneration);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        if (Volatile.Read(ref Owner._visibilityLifecycleGeneration) != generation || IsVisible)
                            return;

                        Logger.LogDebug("[{applicationId}] Name-only Glamourer revert for {pair}/{name} during {reason}", applicationId, Pair.UserData.AliasOrUID, name, reason);
                        await _ipcManager.Glamourer.RevertByNameAsync(Logger, name, applicationId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Name-only Glamourer revert failed for {pair}/{name} during {reason}", applicationId, Pair.UserData.AliasOrUID, name, reason);
                    }
                });
            }


            public void HandleZoneSwitchEnd()
            {
                _zoneRecoveryUntilTick = -1;
                _addressZeroSinceTick = -1;
                _identityDriftSinceTick = -1;
                ResetVisibleReplayReadiness();
                _nextVisibilityWorkTick = 0;

                if (Pair.IsPaused)
                    return;

                if (!IsVisible)
                {
                    // Zone start already moved this pair into the same unrendered/vanilla
                    // state as normal visibility loss. On zone end, do not apply while the
                    // actor is still missing; just force the next framework tick to resolve
                    // the actor and enter the normal visibility-gain lifecycle.
                    QueueNameOnlyGlamourerRevertIfStillInvisible("zone switch end invisible cleanup");
                    Interlocked.Exchange(ref Owner._initializeStarted, 0);
                    return;
                }

                var addr = ResolveStrictVisiblePlayerAddress(_charaHandler?.Address ?? nint.Zero);
                if (addr == nint.Zero)
                {
                    MarkInitialApplyRequired();
                    return;
                }

                Logger.LogDebug(
                    "[{applicationId}] Zone switch end found {pair} still visible at {addr:X}; forcing fresh visible lifecycle replay",
                    Guid.NewGuid(),
                    Pair.UserData.AliasOrUID,
                    addr);

                Owner.BeginVisibleLifecycleReplay(addr, "zone switch end visible replay");
                Owner._syncWorker?.Signal(PairSyncReason.BecameVisible);
            }

            public async Task InitializeAsync(string name, nint knownAddress = 0)
            {
                // Visibility gain must be allowed even while an older vanilla cleanup is still
                // finishing.  Stale cleanup is generation-guarded below, so do not drop the
                // IsVisible=true edge here.
                Interlocked.Increment(ref Owner._visibilityLifecycleGeneration);

                PlayerName = name;
                if (knownAddress != nint.Zero)
                    _lastKnownOwnershipAddr = knownAddress;

                nint ResolveInitializedActorAddress()
                {
                    var candidate = _lastKnownOwnershipAddr != nint.Zero ? _lastKnownOwnershipAddr : knownAddress;
                    if (candidate != nint.Zero && _dalamudUtil.AddressMatchesPlayerIdent(Pair.Ident, candidate))
                        return candidate;

                    return _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident);
                }

                var previousHandler = _charaHandler;
                _charaHandler = await _gameObjectHandlerFactory.Create(
                    ObjectKind.Player,
                    ResolveInitializedActorAddress,
                    isWatched: false).ConfigureAwait(false);

                if (previousHandler != null && !ReferenceEquals(previousHandler, _charaHandler))
                    previousHandler.Dispose();

                _serverConfigManager.AutoPopulateNoteForUid(Pair.UserData.UID, name);


                var visibleAddress = nint.Zero;
                try
                {
                    visibleAddress = await _dalamudUtil.RunOnFrameworkThread(() =>
                    {
                        if (knownAddress != nint.Zero && _dalamudUtil.AddressMatchesPlayerIdent(Pair.Ident, knownAddress))
                            return knownAddress;

                        var handlerAddress = _charaHandler?.Address ?? nint.Zero;
                        var resolved = ResolveStrictVisiblePlayerAddress(handlerAddress);
                        if (resolved != nint.Zero)
                            return resolved;

                        var live = _dalamudUtil.FindPlayerByNameHash(Pair.Ident, forceLiveScan: true);
                        return live.Address;
                    }).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Failed to resolve immediate visible address during initialize for {pair}", Pair.UserData.AliasOrUID);
                }

                if (visibleAddress != nint.Zero && !Pair.IsPaused)
                {
                    Logger.LogDebug(
                        "[{applicationId}] Initialize resolved visible actor for {pair} at {addr:X}; immediately restoring visible lifecycle",
                        Guid.NewGuid(),
                        Pair.UserData.AliasOrUID,
                        visibleAddress);

                    Owner.BeginVisibleLifecycleReplay(visibleAddress, "initialize resolved visible actor");
                    IsVisible = true;
                    SyncStorm.RegisterVisibleNow();
                }

                if (Pair.LastReceivedCharacterData != null)
                {
                    _cachedData = null;
                    _dataReceivedInDowntime = null;
                    _lastAttemptedDataHash = null;
                    _forceApplyMods = true;
                    _redrawOnNextApplication = true;
                    _initialApplyPending = true;
                    ResetVisibleReplayReadiness();
                    _nextVisibilityWorkTick = 0;
                    Owner._syncWorker?.Signal(PairSyncReason.BecameVisible);
                }
        }

        public void ResetToUninitializedState(bool revertLiveCustomizationState = true)
        {
            var applicationId = Guid.NewGuid();
            var teardownGeneration = Interlocked.Increment(ref Owner._visibilityLifecycleGeneration);
            var handlerSnapshot = _charaHandler;
            var oldIdx = _lastAssignedObjectIndex ?? Owner._lastVanillaTeardownObjectIndex;
            var name = !string.IsNullOrWhiteSpace(PlayerName)
                ? PlayerName!
                : Owner._lastVanillaTeardownPlayerName ?? string.Empty;
            var playerAddress = ResolveStablePlayerAddress(handlerSnapshot?.Address ?? nint.Zero);
            if (playerAddress == nint.Zero)
                playerAddress = Owner._lastVanillaTeardownPlayerAddress;
            var customizeIdsSnapshot = _customizeIds.Count == 0
                ? null
                : _customizeIds.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var penumbraCollectionSnapshot = _penumbraCollection;
            var penumbraCollectionTaskSnapshot = _penumbraCollectionTask;
            // Keep the owned temp collection identity until the vanilla overwrite has landed.
            // The collection is forced to empty and can be reused on the next visibility replay;
            // dropping it here creates orphaned Penumbra state that can reattach on re-entry.

            var shouldRunImmediatePenumbraTeardown = revertLiveCustomizationState
                && !_lifetime.ApplicationStopping.IsCancellationRequested
                && !_dalamudUtil.IsInCutscene;

            var shouldRevertLiveCustomizationState = shouldRunImmediatePenumbraTeardown
                && !string.IsNullOrEmpty(name);

            Logger.LogDebug("[{applicationId}] Fast visibility teardown for {pair}; wiping Penumbra binding, live apply work and applied customization state", applicationId, Pair.UserData.AliasOrUID);

            Interlocked.Increment(ref Owner._vanillaTeardownInProgress);

            Owner._lastVanillaTeardownPlayerName = name;
            Owner._lastVanillaTeardownObjectIndex = oldIdx;
            Owner._lastVanillaTeardownPlayerAddress = playerAddress;

            // This reset queues the fast vanilla clear using captured name/address/state.
            // If the pair is disposed immediately afterwards (disconnect/offline), do not run a
            // second, stale synchronous restore path against a handler we have already detached.
            Owner._disposeRestoreAlreadyQueued = true;

            ResetPairSyncPipelineState();

            // Release the init latch immediately.  Cleanup completion must not be required before
            // a room re-entry can create a fresh visible handler and replay the last received data.
            Interlocked.Exchange(ref Owner._initializeStarted, 0);
            _initializeTask = null;

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

            var teardownTask = Task.Run(async () =>
            {
                try
                {
                    // The fast verified path is now the single live-state vanilla teardown.
                    // Do not run a second delayed revert/redraw pass afterwards: that was causing
                    // visible two-stage vanilla transitions and duplicate IPC churn on disconnect.
                    if (shouldRunImmediatePenumbraTeardown)
                    {
                        await RunFastVanillaTeardownAsync(applicationId, teardownGeneration, oldIdx, name, playerAddress, penumbraCollectionSnapshot, penumbraCollectionTaskSnapshot, customizeIdsSnapshot, shouldRevertLiveCustomizationState).ConfigureAwait(false);

                    }
                    else
                    {
                        Logger.LogDebug("[{applicationId}] Skipping per-pair Penumbra teardown for {pair}; global wipe or shutdown path owns collection removal", applicationId, Pair.UserData.AliasOrUID);
                    }

                    Logger.LogDebug("[{applicationId}] Visibility teardown completed fast vanilla clear for {pair}; releasing captured player name", applicationId, Pair.UserData.AliasOrUID);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "[{applicationId}] Fast visibility teardown failed for {pair}", applicationId, Pair.UserData.AliasOrUID);
                }
                finally
                {
                    var teardownStillCurrent = Volatile.Read(ref Owner._visibilityLifecycleGeneration) == teardownGeneration;
                    if (teardownStillCurrent)
                    {
                        PlayerName = string.Empty;

                        if (ReferenceEquals(_charaHandler, handlerSnapshot))
                        {
                            _charaHandler?.Dispose();
                            _charaHandler = null;
                        }

                        Interlocked.Exchange(ref Owner._initializeStarted, 0);
                        _initializeTask = null;
                    }
                    else
                    {
                        Logger.LogDebug(
                            "[{applicationId}] Visibility teardown finalizer for {pair} is stale; preserving newer visible lifecycle state",
                            applicationId,
                            Pair.UserData.AliasOrUID);

                        if (IsVisible && Pair.LastReceivedCharacterData != null && !Pair.IsPaused)
                        {
                            _cachedData = null;
                            _dataReceivedInDowntime = null;
                            _lastAttemptedDataHash = null;
                            _forceApplyMods = true;
                            _redrawOnNextApplication = true;
                            _initialApplyPending = true;
                            ResetVisibleReplayReadiness();
                            _nextVisibilityWorkTick = 0;
                            Owner._syncWorker?.Signal(PairSyncReason.BecameVisible);
                        }
                    }

                    Interlocked.Decrement(ref Owner._vanillaTeardownInProgress);
                    ScheduleRefreshUi(immediate: true);
                }
            });

            Owner._vanillaTeardownTask = teardownTask;

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

            DisposeDownloadManager();

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
