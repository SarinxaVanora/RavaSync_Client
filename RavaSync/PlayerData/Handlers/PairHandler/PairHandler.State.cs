using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Pairs;
using RavaSync.Utils;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private void ClearAppliedLightweightState()
    {
        _lastAppliedMoodlesData.Clear();
        _lastAppliedHonorificData.Clear();
        _lastAppliedPetNamesData.Clear();
    }

    private bool ShouldApplyLightweightMetadata(Dictionary<ObjectKind, string> cache, ObjectKind kind, string data)
    {
        data ??= string.Empty;

        if (cache.TryGetValue(kind, out var lastApplied)
            && string.Equals(lastApplied, data, StringComparison.Ordinal))
        {
            return false;
        }

        cache[kind] = data;
        return true;
    }

    private void ResetCollectionBindingState()
    {
        // Keep the last known Penumbra binding identity for any later hard-vanilla teardown.
        // Clearing these too early loses the only object-index handle Penumbra may still have,
        // which lets stale temp collection assignments reattach when the actor appears again.
        if (_lastAssignedObjectIndex.HasValue)
            _lastVanillaTeardownObjectIndex = _lastAssignedObjectIndex;
        if (_lastAssignedPlayerAddress != nint.Zero)
            _lastVanillaTeardownPlayerAddress = _lastAssignedPlayerAddress;
        if (!string.IsNullOrWhiteSpace(PlayerName))
            _lastVanillaTeardownPlayerName = PlayerName;

        _lastAssignedObjectIndex = null;
        _lastAssignedPlayerAddress = nint.Zero;
        _lastAssignedOwnedObjectIndices.Clear();
        _summonedActorBindingInFlight.Clear();
        _nextSummonedActorBindingAttemptTick.Clear();
        _summonedActorRedrawCooldownByKey.Clear();
        _penumbraCoordinator.ClearSummonedActorBindingState();
        _lastAssignedCollectionAssignUtc = DateTime.MinValue;
        _nextTempCollectionRetryNotBeforeUtc = DateTime.MinValue;
        _nextVisiblePenumbraBindingValidationTick = 0;
        Interlocked.Exchange(ref _visiblePenumbraBindingValidationInFlight, 0);
    }

    private void ResetAppliedModTrackingState()
    {
        _lastAppliedTempModsFingerprint = null;
        _lastAppliedTempModsSnapshot = null;
        _lastAppliedMountMusicTempModsFingerprint = null;
        _lastAppliedMountMusicTempModsSnapshot = null;
        _lastAppliedTransientSupportFingerprint = null;
        _lastAppliedManipulationFingerprint = null;
        ClearPairSyncAssetPlanCache();
        Pair.ClearActiveSyncIndicators();
        ClearAppliedLightweightState();
    }

    private void ResetOwnedObjectRetryState()
    {
        _pendingOwnedObjectCustomizationRetry.Clear();
        _nextOwnedObjectCustomizationRetryTick = -1;
    }

    private void ResetOtherSyncCandidateState()
    {
        _otherSyncReleaseCandidateSinceTick = -1;
        _otherSyncAcquireCandidateSinceTick = -1;
        _otherSyncAcquireCandidateOwner = string.Empty;
        _lastKnownOwnershipAddr = nint.Zero;
        _nextVisibilityWorkTick = 0;
    }

    private void ResetReapplyTrackingState(bool resetRetries = true)
    {
        _lastAttemptedDataHash = null;

        if (resetRetries)
        {
            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;
        }
    }

    private void MarkInitialApplyRequired(bool redrawOnNextApplication = true, bool signalWorker = true)
    {
        _initialApplyPending = true;
        _lastAttemptedDataHash = null;
        _redrawOnNextApplication = redrawOnNextApplication;
        ResetVisibleReplayReadiness();
        _nextVisibilityWorkTick = 0;

        if (signalWorker)
            _syncWorker?.Signal(PairSyncReason.InitialApplyRequired);
    }

    private void ResetVisibleReplayReadiness()
    {
        _visibleReplayCandidateAddress = nint.Zero;
        _visibleReplayCandidateSinceTick = -1;
        _visibleReplayStableFrames = 0;
        _lastInitialApplyDispatchTick = -1;
    }

    private bool IsRecentlyVisibleLifecycleReplay(long windowMs = 3000)
    {
        var tick = _lastVisibleLifecycleReplayTick;
        return tick >= 0 && unchecked(Environment.TickCount64 - tick) <= windowMs;
    }


    private void BeginVisibleLifecycleReplay(nint address, string reason)
    {
        var applicationId = Guid.NewGuid();

        Logger.LogDebug(
            "[{applicationId}] Beginning visible lifecycle replay for {pair}: reason={reason}, addr={addr:X}; clearing applied live state and forcing full apply/redraw",
            applicationId,
            Pair.UserData.AliasOrUID,
            reason,
            address);

        ResetPairSyncPipelineState();

        var oldApplicationCts = Interlocked.Exchange(ref _applicationCancellationTokenSource, new CancellationTokenSource());
        oldApplicationCts?.CancelDispose();

        _applicationTask = null;
        _pairSyncApplicationTask = null;

        ResetCollectionBindingState();
        ResetAppliedModTrackingState();
        ResetOwnedObjectRetryState();
        ResetReapplyTrackingState();
        ResetVisibleReplayReadiness();
        _visibilityCoordinator.ResetVisibleSupportReapplyTracking();
        _lastVisibleLifecycleReplayTick = Environment.TickCount64;
        Interlocked.Increment(ref _visibilityLifecycleGeneration);

        _customizeIds.Clear();

        _cachedData = null;
        _dataReceivedInDowntime = null;

        _forceApplyMods = true;
        _redrawOnNextApplication = true;
        _initialApplyPending = true;

        _addressZeroSinceTick = -1;
        _identityDriftSinceTick = -1;
        _zoneRecoveryUntilTick = -1;
        _lastKnownOwnershipAddr = address;
        _nextVisibilityWorkTick = 0;

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        Pair.SetCurrentDownloadStatus(null);
        Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);

        if (IsVisible && !Pair.IsPaused && Pair.LastReceivedCharacterData is CharacterData)
            _ = Task.Run(() => TrySubmitLastReceivedVisibleAuthoritativeApply(reason, PairSyncReason.BecameVisible, skipIfBusy: false));
    }


    private bool IsVisibleReplayReady(nint address, long nowTick)
    {
        if (address == nint.Zero)
            return false;

        if (_visibleReplayCandidateAddress != address)
        {
            _visibleReplayCandidateAddress = address;
            _visibleReplayCandidateSinceTick = nowTick;
            _visibleReplayStableFrames = 1;
            return false;
        }

        _visibleReplayStableFrames++;

        return _visibleReplayStableFrames >= VisibleReplayStableFramesRequired
            && _visibleReplayCandidateSinceTick >= 0
            && nowTick - _visibleReplayCandidateSinceTick >= VisibleReplaySettleMs;
    }

    private void BeginVisibilityRecoveryWindow(long nowTick, bool isZoneTransition)
    {
        _addressZeroSinceTick = nowTick;
        _zoneRecoveryUntilTick = isZoneTransition ? nowTick + ZoneVisibilityLossGraceMs : -1;
        _identityDriftSinceTick = -1;
    }

    private void ResetVisibilityTracking()
    {
        _addressZeroSinceTick = -1;
        _zoneRecoveryUntilTick = -1;
        _identityDriftSinceTick = -1;
        ResetVisibleReplayReadiness();
        _visibilityCoordinator.ResetVisibleSupportReapplyTracking();
        _nextVisibilityWorkTick = 0;
    }

    private void ResetLiveApplicationState(bool resetRetryState = true)
    {
        ResetCollectionBindingState();
        ResetAppliedModTrackingState();
        ResetOwnedObjectRetryState();
        ResetReapplyTrackingState(resetRetryState);
    }
}
