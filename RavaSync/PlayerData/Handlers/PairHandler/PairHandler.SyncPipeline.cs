using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    public sealed record PairSyncDiagnosticsSnapshot(string PairUid, string Stage, long DesiredVersion, long PreparedVersion, long AppliedVersion, string ActivePayloadFingerprint, string LastStatus, string LastReason, DateTime LastUpdatedUtc, long ReadinessMs, long PlanMs, long ExecuteMs, long TotalMs, int AssetCount, int MissingFileCount, int PrimeTransientPathCount, bool ContainsAnimationCritical, bool ContainsVfxCritical, string SenderManifestFingerprint)
    {
        public static PairSyncDiagnosticsSnapshot Empty(string pairUid) => new(pairUid, "Idle", 0, 0, 0, string.Empty, "None", string.Empty, DateTime.MinValue, 0, 0, 0, 0, 0, 0, 0, false, false, string.Empty);
    }

    public PairSyncDiagnosticsSnapshot SyncDiagnostics => _syncWorker?.GetDiagnostics() ?? PairSyncDiagnosticsSnapshot.Empty(Pair.UserData.UID);

    internal bool HasCommittedPairSyncPayload(string payloadFingerprint)
        => !string.IsNullOrWhiteSpace(payloadFingerprint) && _syncWorker?.IsAppliedPayload(payloadFingerprint) == true;

    internal bool HasPairSyncWorkPendingForPayload(string payloadFingerprint)
        => !string.IsNullOrWhiteSpace(payloadFingerprint) && _syncWorker?.IsPendingOrActiveForPayload(payloadFingerprint) == true;

    private enum PairSyncReason
    {
        IncomingData,
        BecameVisible,
        InitialApplyRequired,
        ManualReapply,
    }

    private enum PairSyncStage
    {
        Idle,
        Planning,
        Downloading,
        Committing,
    }

    private enum PairSyncPlanDisposition
    {
        Execute,
        NoOp,
        Deferred,
    }

    private enum PairSyncCommitStatus
    {
        Success,
        CommitStarted,
        NoWork,
        Hidden,
        Paused,
        YieldedToOtherSync,
        CombatOrPerformance,
        PluginsUnavailable,
        StalePlan,
        ActorChanged,
        MissingFiles,
        DownloadFailed,
        ThresholdBlocked,
        ApplyFailed,
        VerificationFailed,
        Deferred,
        Failed,
        Cancelled,
    }

    private sealed record PairSyncCommitResult(
        PairSyncCommitStatus Status,
        string Reason,
        bool RetryImmediately)
    {
        public bool Success => Status == PairSyncCommitStatus.Success;
        public bool CommitStarted => Status == PairSyncCommitStatus.CommitStarted;

        public static PairSyncCommitResult Succeeded()
            => new(PairSyncCommitStatus.Success, string.Empty, false);

        public static PairSyncCommitResult StartedCommit(string reason = "application commit started")
            => new(PairSyncCommitStatus.CommitStarted, reason, false);

        public static PairSyncCommitResult NoWork(string reason)
            => new(PairSyncCommitStatus.NoWork, reason, false);

        public static PairSyncCommitResult Hidden(string reason)
            => new(PairSyncCommitStatus.Hidden, reason, false);

        public static PairSyncCommitResult Paused(string reason)
            => new(PairSyncCommitStatus.Paused, reason, false);

        public static PairSyncCommitResult YieldedToOtherSync(string reason)
            => new(PairSyncCommitStatus.YieldedToOtherSync, reason, false);

        public static PairSyncCommitResult CombatOrPerformance(string reason)
            => new(PairSyncCommitStatus.CombatOrPerformance, reason, false);

        public static PairSyncCommitResult PluginsUnavailable(string reason)
            => new(PairSyncCommitStatus.PluginsUnavailable, reason, false);

        public static PairSyncCommitResult StalePlan(string reason, bool retryImmediately = true)
            => new(PairSyncCommitStatus.StalePlan, reason, retryImmediately);

        public static PairSyncCommitResult ActorChanged(string reason, bool retryImmediately = true)
            => new(PairSyncCommitStatus.ActorChanged, reason, retryImmediately);

        public static PairSyncCommitResult MissingFiles(string reason, bool retryImmediately = false)
            => new(PairSyncCommitStatus.MissingFiles, reason, retryImmediately);

        public static PairSyncCommitResult DownloadFailed(string reason, bool retryImmediately = true)
            => new(PairSyncCommitStatus.DownloadFailed, reason, retryImmediately);

        public static PairSyncCommitResult ThresholdBlocked(string reason)
            => new(PairSyncCommitStatus.ThresholdBlocked, reason, false);

        public static PairSyncCommitResult ApplyFailed(string reason, bool retryImmediately = true)
            => new(PairSyncCommitStatus.ApplyFailed, reason, retryImmediately);

        public static PairSyncCommitResult VerificationFailed(string reason, bool retryImmediately = true)
            => new(PairSyncCommitStatus.VerificationFailed, reason, retryImmediately);

        public static PairSyncCommitResult Deferred(string reason, bool retryImmediately = false)
            => new(PairSyncCommitStatus.Deferred, reason, retryImmediately);

        public static PairSyncCommitResult Failed(string reason, bool retryImmediately = false)
            => new(PairSyncCommitStatus.Failed, reason, retryImmediately);

        public static PairSyncCommitResult Cancelled(string reason)
            => new(PairSyncCommitStatus.Cancelled, reason, false);
    }

    private sealed record PairSyncRequest(long Version, Guid ApplicationBase, CharacterData CharacterData, bool ForceApplyCustomization, PairSyncReason Reason, DateTime CreatedUtc);

    private sealed record ActorBinding(string Ident, string? PlayerName, nint Address, int? ObjectIndex);

    private sealed record AppliedPairState(long Version, string DataHash, string PayloadFingerprint, DateTime AppliedUtc)
    {
        public static readonly AppliedPairState Empty = new(0, "NODATA", string.Empty, DateTime.MinValue);
    }

    private sealed record PreparedPairState(long Version, string PayloadFingerprint, DateTime PreparedUtc);

    private enum PairSyncAssetKind
    {
        Unknown,
        FileSwap,
        Model,
        Material,
        Texture,
        Animation,
        Timeline,
        Vfx,
        Sound,
        Skeleton,
        Physics,
    }

    private enum PairSyncAssetCriticality
    {
        Normal,
        AppearanceCritical,
        AnimationCritical,
        VfxCritical,
        VfxSupportCritical,
    }

    private sealed record PairSyncAssetEntry(ObjectKind ObjectKind, string GamePath, string? Hash, string ResolvedPath, PairSyncAssetKind Kind, PairSyncAssetCriticality Criticality);

    private sealed record PairSyncReplacementLookupEntry(ObjectKind ObjectKind, string GamePath, string? Hash, string? FileSwapPath);

    private sealed record PairSyncAssetPlan(Dictionary<(string GamePath, string? Hash), string> ModdedPaths, List<FileReplacementData> MissingFiles, List<PairSyncAssetEntry> Entries, HashSet<string> UniqueHashes, List<string> PrimeTransientPaths, Dictionary<ObjectKind, List<string>> PrimeTransientPathsByKind, List<string> TransientSupportPaths, bool ContainsAnimationCritical, bool ContainsVfxCritical, bool ContainsVfxPropSupport, string TempModsFingerprint, string TransientSupportFingerprint)
    {
        public static PairSyncAssetPlan Empty { get; } = new([], [], [], new(StringComparer.OrdinalIgnoreCase), [], new(), [], false, false, false, "EMPTY", "EMPTY");
        public bool HasAssets => Entries.Count > 0;
        public bool HasMissingFiles => MissingFiles.Count > 0;
        public bool RequiresTransientPrime => PrimeTransientPathsByKind.Count > 0;
        public bool RequiresFirstUseTransientWarmup => ContainsAnimationCritical || ContainsVfxCritical || ContainsVfxPropSupport || PrimeTransientPathsByKind.Count > 0 || TransientSupportPaths.Count > 0;
        public bool RequiresTransientSupportRefresh(string? lastAppliedTransientSupportFingerprint) => ContainsVfxPropSupport && !string.Equals(TransientSupportFingerprint, lastAppliedTransientSupportFingerprint, StringComparison.Ordinal);
    }

    private sealed record PairSyncReadiness(bool Ready, bool RetryImmediately, string Reason, ActorBinding? Binding, ApplyFrameworkSnapshot? Snapshot)
    {
        public static PairSyncReadiness NotReady(string reason, bool retryImmediately = false)
            => new(false, retryImmediately, reason, null, null);

        public static PairSyncReadiness ReadyNow(ActorBinding binding, ApplyFrameworkSnapshot snapshot)
            => new(true, false, string.Empty, binding, snapshot);
    }

    private sealed record PairSyncPlan(PairSyncRequest Request, ActorBinding Binding, ApplyFrameworkSnapshot Snapshot, ApplyPreparation Preparation, PairSyncAssetPlan AssetPlan, Dictionary<ObjectKind, HashSet<PlayerChanges>> UpdatedData, bool UpdateModdedPaths, bool UpdateManipulation, bool RequiresFileReadyGate, bool ForceApplyModsForThisApply, bool LifecycleApplyRequested, bool LifecycleRedrawRequested, string PayloadFingerprint)
    {
        public bool HasWork => UpdatedData.Count > 0;
    }

    private sealed record PairSyncPlanBuildResult(
        PairSyncPlanDisposition Disposition,
        PairSyncPlan? Plan,
        bool RetryImmediately,
        string Reason)
    {
        public static PairSyncPlanBuildResult Execute(PairSyncPlan plan)
            => new(PairSyncPlanDisposition.Execute, plan, false, string.Empty);

        public static PairSyncPlanBuildResult NoOp(string reason)
            => new(PairSyncPlanDisposition.NoOp, null, false, reason);

        public static PairSyncPlanBuildResult Deferred(string reason, bool retryImmediately = false)
            => new(PairSyncPlanDisposition.Deferred, null, retryImmediately, reason);
    }

    private sealed class PairSyncWorker : IDisposable
    {
        private readonly PairHandler _owner;
        private readonly object _gate = new();
        private readonly SemaphoreSlim _wake = new(0, 1);
        private readonly CancellationTokenSource _shutdownCts = new();
        private readonly Task _runner;

        private PairSyncRequest? _latestRequest;
        private PreparedPairState? _prepared;
        private AppliedPairState _applied = AppliedPairState.Empty;
        private string? _activePayloadFingerprint;
        private PairSyncCommitResult _lastCommitResult = PairSyncCommitResult.NoWork("worker has not processed a request yet");
        private DateTime _lastCommitResultUtc = DateTime.MinValue;
        private PairSyncDiagnosticsSnapshot _diagnostics;
        private CancellationTokenSource? _activeCts;
        private PairSyncStage _stage = PairSyncStage.Idle;
        private long _nextVersion;
        private int _wakeQueued;
        private bool _disposed;

        public PairSyncWorker(PairHandler owner)
        {
            _owner = owner;
            _diagnostics = PairSyncDiagnosticsSnapshot.Empty(owner.Pair.UserData.UID);
            _runner = Task.Run(RunAsync);
        }

        public void Submit(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization, PairSyncReason reason)
        {
            if (characterData == null)
                return;

            var request = new PairSyncRequest(
                Interlocked.Increment(ref _nextVersion),
                applicationBase == Guid.Empty ? Guid.NewGuid() : applicationBase,
                characterData,
                forceApplyCustomization,
                reason,
                DateTime.UtcNow);

            var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(characterData);
            _owner.LogVisibilityDiagnostic("PIPE submit pair={pair} app={app} version={version} reason={reason} forced={forced} visible={visible} hash={hash} payload={payload}",
                _owner.Pair.UserData.AliasOrUID,
                request.ApplicationBase,
                request.Version,
                reason,
                forceApplyCustomization,
                _owner.IsVisible,
                characterData.DataHash.Value,
                payloadFingerprint);

            _owner.Logger.LogDebug(
                "[BASE-{appBase}] Pair sync submit for {pair}: reason={reason}, forced={forced}, visible={visible}, hash={hash}, payload={payload}, version={version}",
                request.ApplicationBase,
                _owner.Pair.UserData.AliasOrUID,
                reason,
                forceApplyCustomization,
                _owner.IsVisible,
                characterData.DataHash.Value,
                payloadFingerprint,
                request.Version);

            lock (_gate)
            {
                if (reason == PairSyncReason.IncomingData
                    && !forceApplyCustomization
                    && _stage != PairSyncStage.Idle
                    && !string.IsNullOrEmpty(_activePayloadFingerprint)
                    && string.Equals(_activePayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
                {
                    _owner.LogVisibilityDiagnostic("PIPE submit coalesced pair={pair} app={app} version={version} stage={stage} payload={payload}",
                        _owner.Pair.UserData.AliasOrUID,
                        request.ApplicationBase,
                        request.Version,
                        _stage,
                        payloadFingerprint);
                    _owner.Logger.LogTrace(
                        "Coalescing duplicate active payload for {pair}: stage={stage}, payload={payload}",
                        _owner.Pair.UserData.AliasOrUID,
                        _stage,
                        payloadFingerprint);
                    return;
                }

                _latestRequest = request;
                CancelActivePrepareOrDownload_NoLock();
            }

            Wake();
        }

        public void Signal(PairSyncReason reason)
        {
            var hasLatest = false;
            lock (_gate)
            {
                hasLatest = _latestRequest != null;
            }

            _owner.LogVisibilityDiagnostic("PIPE signal pair={pair} reason={reason} hasLatest={hasLatest} visible={visible} hasLastReceived={hasLastReceived} stage={stage}",
                _owner.Pair.UserData.AliasOrUID,
                reason,
                hasLatest,
                _owner.IsVisible,
                _owner.Pair.LastReceivedCharacterData is CharacterData,
                _stage);

            _owner.Logger.LogDebug(
                "Pair sync signal for {pair}: reason={reason}, hasLatest={hasLatest}, visible={visible}, hasLastReceived={hasLastReceived}, stage={stage}",
                _owner.Pair.UserData.AliasOrUID,
                reason,
                hasLatest,
                _owner.IsVisible,
                _owner.Pair.LastReceivedCharacterData is CharacterData,
                _stage);

            if (!hasLatest && _owner.IsVisible && _owner.Pair.LastReceivedCharacterData is CharacterData)
            {
                _ = Task.Run(() => _owner.TrySubmitLastReceivedVisibleAuthoritativeApply(reason.ToString(), reason, skipIfBusy: false));
            }

            Wake();
        }

        public void CancelActiveWork(bool clearDesired)
        {
            lock (_gate)
            {
                if (clearDesired)
                {
                    _latestRequest = null;
                    _prepared = null;
                }

                _activeCts?.Cancel();
            }
        }

        public void ResetPipelineState()
        {
            lock (_gate)
            {
                _latestRequest = null;
                _prepared = null;
                _applied = AppliedPairState.Empty;
                _activePayloadFingerprint = null;
                _lastCommitResult = PairSyncCommitResult.NoWork("pipeline reset");
                _lastCommitResultUtc = DateTime.UtcNow;
                _diagnostics = PairSyncDiagnosticsSnapshot.Empty(_owner.Pair.UserData.UID);
                _activeCts?.Cancel();
            }
        }

        private void CancelActivePrepareOrDownload_NoLock()
        {
            if (_stage is PairSyncStage.Planning or PairSyncStage.Downloading)
            {
                try
                {
                    _activeCts?.Cancel();
                }
                catch
                {
                    // best effort
                }
            }
        }

        private bool IsRequestStillLatestOrEquivalent(PairSyncRequest request, string payloadFingerprint)
        {
            lock (_gate)
            {
                if (_latestRequest == null)
                    return false;

                if (_latestRequest.Version == request.Version)
                    return true;

                if (_latestRequest.ForceApplyCustomization || request.ForceApplyCustomization)
                    return false;

                var latestPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_latestRequest.CharacterData);
                return string.Equals(latestPayloadFingerprint, payloadFingerprint, StringComparison.Ordinal);
            }
        }

        private void Wake()
        {
            if (_disposed)
                return;

            if (Interlocked.Exchange(ref _wakeQueued, 1) == 0)
            {
                try
                {
                    _wake.Release();
                }
                catch (SemaphoreFullException)
                {
                    // Already awake.
                }
            }
        }

        private async Task RunAsync()
        {
            var token = _shutdownCts.Token;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await _wake.WaitAsync(token).ConfigureAwait(false);
                    Interlocked.Exchange(ref _wakeQueued, 0);
                    await ProcessLatestAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _owner.Logger.LogWarning(ex, "Pair sync worker failed for {pair}", _owner.Pair.UserData.AliasOrUID);
                }
            }
        }

        private PairSyncRequest? GetLatestRequest()
        {
            lock (_gate)
            {
                return _latestRequest;
            }
        }

        private void SetStage(PairSyncStage stage, CancellationTokenSource? cts = null, string? activePayloadFingerprint = null)
        {
            lock (_gate)
            {
                _stage = stage;
                _activeCts = cts;

                if (stage == PairSyncStage.Idle)
                    _activePayloadFingerprint = null;
                else if (!string.IsNullOrEmpty(activePayloadFingerprint))
                    _activePayloadFingerprint = activePayloadFingerprint;
            }
        }

        public bool IsBusyForPayload(string payloadFingerprint)
        {
            lock (_gate)
            {
                if (_stage == PairSyncStage.Idle)
                    return false;

                return string.IsNullOrEmpty(payloadFingerprint)
                    || string.Equals(_activePayloadFingerprint, payloadFingerprint, StringComparison.Ordinal);
            }
        }

        public bool IsPendingOrActiveForPayload(string payloadFingerprint)
        {
            if (string.IsNullOrWhiteSpace(payloadFingerprint))
                return false;

            lock (_gate)
            {
                if (!string.IsNullOrEmpty(_activePayloadFingerprint)
                    && string.Equals(_activePayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
                    return true;

                if (_latestRequest == null)
                    return false;

                var latestPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_latestRequest.CharacterData);
                return string.Equals(latestPayloadFingerprint, payloadFingerprint, StringComparison.Ordinal);
            }
        }

        public bool IsAppliedPayload(string payloadFingerprint)
        {
            if (string.IsNullOrWhiteSpace(payloadFingerprint))
                return false;

            lock (_gate)
            {
                return string.Equals(_applied.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal);
            }
        }

        private void RecordCommitResult(PairSyncCommitResult result)
        {
            lock (_gate)
            {
                _lastCommitResult = result;
                _lastCommitResultUtc = DateTime.UtcNow;
            }
        }

        public PairSyncDiagnosticsSnapshot GetDiagnostics()
        {
            lock (_gate)
            {
                return _diagnostics;
            }
        }

        private void RecordDiagnostics(PairSyncRequest? request, PairSyncPlan? plan, PairSyncCommitResult result, long readinessMs, long planMs, long executeMs, long totalMs)
        {
            lock (_gate)
            {
                _diagnostics = new PairSyncDiagnosticsSnapshot(
                    _owner.Pair.UserData.UID,
                    _stage.ToString(),
                    request?.Version ?? _latestRequest?.Version ?? 0,
                    _prepared?.Version ?? 0,
                    _applied.Version,
                    _activePayloadFingerprint ?? string.Empty,
                    result.Status.ToString(),
                    result.Reason ?? string.Empty,
                    DateTime.UtcNow,
                    readinessMs,
                    planMs,
                    executeMs,
                    totalMs,
                    plan?.AssetPlan.Entries.Count ?? 0,
                    plan?.AssetPlan.MissingFiles.Count ?? 0,
                    plan?.AssetPlan.PrimeTransientPaths.Count ?? 0,
                    plan?.AssetPlan.ContainsAnimationCritical ?? false,
                    plan?.AssetPlan.ContainsVfxCritical ?? false,
                    _owner.Pair.LastReceivedSyncManifest?.mf ?? string.Empty);
            }
        }

        private void LogDiagnostics(PairSyncDiagnosticsSnapshot diagnostics)
        {
            if (!_owner.Logger.IsEnabled(LogLevel.Debug))
                return;

            _owner.Logger.LogDebug(
                "Pair sync diagnostics for {pair}: status={status}, reason={reason}, stage={stage}, desired={desired}, prepared={prepared}, applied={applied}, assets={assets}, missing={missing}, transientPrime={prime}, animCritical={anim}, vfxCritical={vfx}, timings ready/plan/exec/total={ready}/{plan}/{exec}/{total}ms, senderManifest={manifest}",
                _owner.Pair.UserData.AliasOrUID,
                diagnostics.LastStatus,
                diagnostics.LastReason,
                diagnostics.Stage,
                diagnostics.DesiredVersion,
                diagnostics.PreparedVersion,
                diagnostics.AppliedVersion,
                diagnostics.AssetCount,
                diagnostics.MissingFileCount,
                diagnostics.PrimeTransientPathCount,
                diagnostics.ContainsAnimationCritical,
                diagnostics.ContainsVfxCritical,
                diagnostics.ReadinessMs,
                diagnostics.PlanMs,
                diagnostics.ExecuteMs,
                diagnostics.TotalMs,
                diagnostics.SenderManifestFingerprint);
        }

        public void SetCommittingForObservedApplication()
        {
            lock (_gate)
            {
                if (_stage == PairSyncStage.Downloading)
                    _stage = PairSyncStage.Committing;
            }
        }

        private async Task ProcessLatestAsync(CancellationToken shutdownToken)
        {
            var request = GetLatestRequest();
            if (request == null)
                return;

            var incomingCoalesceDelayMs = WineIncomingDataCoalesceDelayMs;
            if (incomingCoalesceDelayMs > 0 && request.Reason == PairSyncReason.IncomingData && !request.ForceApplyCustomization)
            {
                await Task.Delay(incomingCoalesceDelayMs, shutdownToken).ConfigureAwait(false);
                var latestAfterDelay = GetLatestRequest();
                if (latestAfterDelay != null && latestAfterDelay.Version != request.Version)
                {
                    _owner.Logger.LogTrace(
                        "Coalescing Linux/Wine incoming payload for {pair}: skipped version {oldVersion}, latest version is {newVersion}",
                        _owner.Pair.UserData.AliasOrUID,
                        request.Version,
                        latestAfterDelay.Version);
                    return;
                }
            }

            var totalStopwatch = Stopwatch.StartNew();
            var readinessMs = 0L;
            var planMs = 0L;
            var executeMs = 0L;
            PairSyncPlan? diagnosticPlan = null;

            using var activeCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
            var token = activeCts.Token;
            var requestPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(request.CharacterData);
            SetStage(PairSyncStage.Planning, activeCts, requestPayloadFingerprint);

            try
            {
                var readinessStopwatch = Stopwatch.StartNew();
                var readiness = await _owner.CaptureVisiblePairSyncReadinessAsync(request, token).ConfigureAwait(false);
                readinessStopwatch.Stop();
                readinessMs = readinessStopwatch.ElapsedMilliseconds;

                if (!readiness.Ready)
                {
                    _owner.LogVisibilityDiagnostic("PIPE readiness not-ready pair={pair} app={app} version={version} reason={reason} retry={retry} visible={visible}",
                        _owner.Pair.UserData.AliasOrUID,
                        request.ApplicationBase,
                        request.Version,
                        readiness.Reason,
                        readiness.RetryImmediately,
                        _owner.IsVisible);
                    var result = PairSyncCommitResult.Deferred(readiness.Reason, readiness.RetryImmediately);
                    RecordCommitResult(result);
                    RecordDiagnostics(request, null, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());
                    _owner.Logger.LogTrace("[BASE-{appBase}] Pair sync deferred for {pair}: {reason}", request.ApplicationBase, _owner.Pair.UserData.AliasOrUID, readiness.Reason);
                    if (readiness.RetryImmediately)
                        Wake();
                    return;
                }

                var planStopwatch = Stopwatch.StartNew();
                var build = await _owner.BuildPairSyncPlanAsync(request, readiness, token).ConfigureAwait(false);
                planStopwatch.Stop();
                planMs = planStopwatch.ElapsedMilliseconds;

                if (build.Disposition == PairSyncPlanDisposition.NoOp)
                {
                    _owner.LogVisibilityDiagnostic("PIPE plan no-op pair={pair} app={app} version={version} reason={reason} hash={hash} payload={payload}",
                        _owner.Pair.UserData.AliasOrUID,
                        request.ApplicationBase,
                        request.Version,
                        build.Reason,
                        request.CharacterData.DataHash.Value,
                        requestPayloadFingerprint);
                    var result = PairSyncCommitResult.NoWork(build.Reason);
                    RecordCommitResult(result);
                    RecordDiagnostics(request, null, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());
                    _owner.Logger.LogTrace("[BASE-{appBase}] Pair sync no-op for {pair}: {reason}", request.ApplicationBase, _owner.Pair.UserData.AliasOrUID, build.Reason);
                    return;
                }

                if (build.Disposition == PairSyncPlanDisposition.Deferred || build.Plan == null)
                {
                    _owner.LogVisibilityDiagnostic("PIPE plan deferred pair={pair} app={app} version={version} reason={reason} retry={retry}",
                        _owner.Pair.UserData.AliasOrUID,
                        request.ApplicationBase,
                        request.Version,
                        build.Reason,
                        build.RetryImmediately);
                    var result = PairSyncCommitResult.Deferred(build.Reason, build.RetryImmediately);
                    RecordCommitResult(result);
                    RecordDiagnostics(request, null, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());
                    _owner.Logger.LogTrace("[BASE-{appBase}] Pair sync plan deferred for {pair}: {reason}", request.ApplicationBase, _owner.Pair.UserData.AliasOrUID, build.Reason);
                    if (build.RetryImmediately)
                        Wake();
                    return;
                }

                var plan = build.Plan;
                diagnosticPlan = plan;
                _owner.LogVisibilityDiagnostic("PIPE plan execute pair={pair} app={app} version={version} changes={changes} assets={assets} missing={missing} updateFiles={updateFiles} updateManip={updateManip} force={force} fileGate={fileGate} lifecycle={lifecycle} redraw={redraw}",
                    _owner.Pair.UserData.AliasOrUID,
                    plan.Request.ApplicationBase,
                    plan.Request.Version,
                    DescribeUpdatedChanges(plan.UpdatedData),
                    plan.AssetPlan.Entries.Count,
                    plan.AssetPlan.MissingFiles.Count,
                    plan.UpdateModdedPaths,
                    plan.UpdateManipulation,
                    plan.ForceApplyModsForThisApply,
                    plan.RequiresFileReadyGate,
                    plan.LifecycleApplyRequested,
                    plan.LifecycleRedrawRequested);
                _prepared = new PreparedPairState(plan.Request.Version, plan.PayloadFingerprint, DateTime.UtcNow);

                if (!IsRequestStillLatestOrEquivalent(plan.Request, plan.PayloadFingerprint))
                {
                    var result = PairSyncCommitResult.StalePlan("newer pair payload superseded this plan before download/apply handoff", retryImmediately: true);
                    RecordCommitResult(result);
                    RecordDiagnostics(request, diagnosticPlan, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());
                    _owner.Logger.LogTrace(
                        "[BASE-{appBase}] Pair sync skipped stale plan for {pair}: planned version={plannedVersion}",
                        request.ApplicationBase,
                        _owner.Pair.UserData.AliasOrUID,
                        plan.Request.Version);
                    Wake();
                    return;
                }

                SetStage(PairSyncStage.Downloading, activeCts, plan.PayloadFingerprint);
                var executeStopwatch = Stopwatch.StartNew();
                _owner.LogVisibilityDiagnostic("PIPE execute start pair={pair} app={app} version={version} hash={hash} payload={payload}",
                    _owner.Pair.UserData.AliasOrUID,
                    plan.Request.ApplicationBase,
                    plan.Request.Version,
                    plan.Request.CharacterData.DataHash.Value,
                    plan.PayloadFingerprint);
                var commitResult = await _owner.ExecutePairSyncPlanAsync(plan, token).ConfigureAwait(false);
                executeStopwatch.Stop();
                executeMs = executeStopwatch.ElapsedMilliseconds;

                token.ThrowIfCancellationRequested();

                if (!commitResult.Success)
                {
                    _owner.LogVisibilityDiagnostic("PIPE execute result pair={pair} app={app} version={version} status={status} reason={reason} retry={retry}",
                        _owner.Pair.UserData.AliasOrUID,
                        plan.Request.ApplicationBase,
                        plan.Request.Version,
                        commitResult.Status,
                        commitResult.Reason,
                        commitResult.RetryImmediately);
                    RecordCommitResult(commitResult);
                    RecordDiagnostics(request, diagnosticPlan, commitResult, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());

                    _owner.Logger.LogTrace(
                        "[BASE-{appBase}] Pair sync did not commit for {pair}: {status} - {reason}",
                        request.ApplicationBase,
                        _owner.Pair.UserData.AliasOrUID,
                        commitResult.Status,
                        commitResult.Reason);

                    if (commitResult.RetryImmediately)
                        Wake();

                    return;
                }

                _owner.LogVisibilityDiagnostic("PIPE execute committed pair={pair} app={app} version={version} hash={hash} payload={payload}",
                    _owner.Pair.UserData.AliasOrUID,
                    plan.Request.ApplicationBase,
                    plan.Request.Version,
                    plan.Request.CharacterData.DataHash.Value,
                    plan.PayloadFingerprint);
                RecordCommitResult(commitResult);
                _owner.RememberAppliedPairSyncAssetPlan(plan);

                lock (_gate)
                {
                    var shouldRecordApplied = _latestRequest?.Version == plan.Request.Version;
                    if (!shouldRecordApplied && _latestRequest != null)
                    {
                        var latestPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_latestRequest.CharacterData);
                        shouldRecordApplied = string.Equals(latestPayloadFingerprint, plan.PayloadFingerprint, StringComparison.Ordinal)
                            && (!_latestRequest.ForceApplyCustomization || plan.Request.ForceApplyCustomization);
                    }

                    if (shouldRecordApplied)
                    {
                        _applied = new AppliedPairState(
                            _latestRequest?.Version ?? plan.Request.Version,
                            plan.Request.CharacterData.DataHash.Value,
                            plan.PayloadFingerprint,
                            DateTime.UtcNow);
                    }
                }

                RecordDiagnostics(request, diagnosticPlan, commitResult, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                LogDiagnostics(GetDiagnostics());
            }
            catch (OperationCanceledException) when (!shutdownToken.IsCancellationRequested)
            {
                var result = PairSyncCommitResult.Cancelled("cancelled for newer data");
                RecordCommitResult(result);
                RecordDiagnostics(request, diagnosticPlan, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                LogDiagnostics(GetDiagnostics());
                _owner.Logger.LogTrace("[BASE-{appBase}] Pair sync cancelled for newer data: {pair}", request.ApplicationBase, _owner.Pair.UserData.AliasOrUID);
            }
            finally
            {
                totalStopwatch.Stop();
                SetStage(PairSyncStage.Idle, null);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _shutdownCts.Cancel();
            }
            catch
            {
                // best effort
            }

            lock (_gate)
            {
                try
                {
                    _activeCts?.Cancel();
                }
                catch
                {
                    // best effort
                }
            }

            try
            {
                _wake.Dispose();
            }
            catch
            {
                // best effort
            }

            try
            {
                _shutdownCts.Dispose();
            }
            catch
            {
                // best effort
            }
        }
    }

    private Task<PairSyncReadiness> CaptureVisiblePairSyncReadinessAsync(PairSyncRequest request, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return _dalamudUtil.RunOnFrameworkThread(() => CaptureVisiblePairSyncReadiness(request));
    }

    private PairSyncReadiness CaptureVisiblePairSyncReadiness(PairSyncRequest request)
    {
        if (_lifetime.ApplicationStopping.IsCancellationRequested)
        {
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=application-stopping", Pair.UserData.AliasOrUID);
            return PairSyncReadiness.NotReady("application stopping");
        }

        if (!IsVisible)
        {
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=not-visible", Pair.UserData.AliasOrUID);
            return PairSyncReadiness.NotReady("pair is not visible");
        }

        if (Pair.IsPaused)
        {
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=paused", Pair.UserData.AliasOrUID);
            return PairSyncReadiness.NotReady("pair is paused");
        }

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
        {
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=other-sync owner={owner}", Pair.UserData.AliasOrUID, Pair.RemoteOtherSyncOwner);
            return PairSyncReadiness.NotReady("yielded to OtherSync");
        }

        if (_dalamudUtil.IsInCombatOrPerforming)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in combat or performing music, deferring application")));
            _dataReceivedInDowntime = new(request.ApplicationBase, request.CharacterData, request.ForceApplyCustomization);
            SetUploading(isUploading: false);
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=combat-performance", Pair.UserData.AliasOrUID);
            return PairSyncReadiness.NotReady("combat/performance");
        }

        if (_charaHandler == null)
        {
            MarkInitialApplyRequired(signalWorker: false);
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=no-character-handler", Pair.UserData.AliasOrUID);
            return PairSyncReadiness.NotReady("no character handler");
        }

        var strictAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
        if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
        {
            ResetVisibilityTracking();
            MarkInitialApplyRequired();
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=unstable-address strictAddress={address} expected={expected}", Pair.UserData.AliasOrUID, strictAddress, _lastAssignedPlayerAddress);
            return PairSyncReadiness.NotReady("visible actor address is not stable", retryImmediately: false);
        }

        if (_dalamudUtil.IsInGpose || _dalamudUtil.IsInCutscene || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in GPose, a cutscene, or Penumbra/Glamourer is not available")));
            MarkInitialApplyRequired(signalWorker: false);
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=framework-plugins gpose={gpose} cutscene={cutscene} penumbra={penumbra} glamourer={glamourer}",
                Pair.UserData.AliasOrUID,
                _dalamudUtil.IsInGpose,
                _dalamudUtil.IsInCutscene,
                _ipcManager.Penumbra.APIAvailable,
                _ipcManager.Glamourer.APIAvailable);
            return PairSyncReadiness.NotReady("framework/plugins unavailable");
        }

        var snapshot = CaptureApplyFrameworkSnapshot();
        if (!snapshot.HasCharaHandler || snapshot.ResolvedPlayerAddress == nint.Zero)
        {
            MarkInitialApplyRequired(signalWorker: false);
            LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=false reason=snapshot-not-ready hasHandler={hasHandler} resolvedAddress={address}", Pair.UserData.AliasOrUID, snapshot.HasCharaHandler, snapshot.ResolvedPlayerAddress);
            return PairSyncReadiness.NotReady("framework snapshot is not apply-ready");
        }

        var binding = new ActorBinding(Pair.Ident, PlayerName, strictAddress, _lastAssignedObjectIndex);
        LogVisibilityDiagnostic("PIPE readiness pair={pair} ready=true ident={ident} objectIndex={idx} address={address} cachedHash={hash} cachedPayload={payload} force={force}",
            Pair.UserData.AliasOrUID,
            Pair.Ident,
            _lastAssignedObjectIndex,
            strictAddress,
            snapshot.CachedHash,
            snapshot.CachedPayloadFingerprint,
            snapshot.ForceApplyMods);
        return PairSyncReadiness.ReadyNow(binding, snapshot);
    }

    private async Task<PairSyncPlanBuildResult> BuildPairSyncPlanAsync(PairSyncRequest request, PairSyncReadiness readiness, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        if (readiness.Snapshot == null || readiness.Binding == null)
            return PairSyncPlanBuildResult.Deferred("missing readiness snapshot");

        var snapshot = readiness.Snapshot;
        var incomingHash = request.CharacterData.DataHash.Value;
        var incomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(request.CharacterData);

        LogVisibilityDiagnostic("PLAN build start pair={pair} app={app} version={version} reason={reason} hash={hash} payload={payload} cachedHash={cachedHash} cachedPayload={cachedPayload} forceReq={forceReq} forceMods={forceMods} redrawNext={redrawNext}",
            Pair.UserData.AliasOrUID,
            request.ApplicationBase,
            request.Version,
            request.Reason,
            incomingHash,
            incomingPayloadFingerprint,
            snapshot.CachedHash,
            snapshot.CachedPayloadFingerprint,
            request.ForceApplyCustomization,
            _forceApplyMods,
            _redrawOnNextApplication);

        if (request.Reason != PairSyncReason.BecameVisible
            && !request.ForceApplyCustomization
            && !_redrawOnNextApplication
            && !_forceApplyMods
            && !string.IsNullOrEmpty(incomingHash)
            && string.Equals(incomingHash, snapshot.CachedHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, snapshot.CachedPayloadFingerprint, StringComparison.Ordinal))
        {
            if (TryGetRecentMissingCheck(incomingHash, out var hadMissing))
            {
                if (!hadMissing && await CanTreatSameVisiblePayloadAsNoOpAsync(request, readiness, "recent cache check passed", token).ConfigureAwait(false))
                    return PairSyncPlanBuildResult.NoOp("same payload and recent cache check passed");
            }
            else if (AllPayloadHashesSessionKnownPresent(request.CharacterData))
            {
                if (await CanTreatSameVisiblePayloadAsNoOpAsync(request, readiness, "all hashes are session-known present", token).ConfigureAwait(false))
                    return PairSyncPlanBuildResult.NoOp("same payload and all hashes are session-known present");
            }
            else
            {
                ScheduleMissingCheck(request.ApplicationBase, request.CharacterData);
                return PairSyncPlanBuildResult.Deferred("same payload; scheduled missing-cache check");
            }
        }

        var preparation = await Task.Run(() => PrepareApplyData(request.ApplicationBase, request.CharacterData, request.ForceApplyCustomization, snapshot), token).ConfigureAwait(false);
        LogVisibilityDiagnostic("PLAN preparation pair={pair} app={app} sameHash={sameHash} samePayload={samePayload} hasDiffMods={diff} changes={changes}",
            Pair.UserData.AliasOrUID,
            request.ApplicationBase,
            preparation.SameHash,
            preparation.SamePayload,
            preparation.HasDiffMods,
            DescribeUpdatedChanges(preparation.UpdatedData));

        token.ThrowIfCancellationRequested();

        var requiresFileReadyGate = RequiresAppearanceFileReadyGate(preparation.UpdatedData)
            || request.ForceApplyCustomization
            || _redrawOnNextApplication
            || snapshot.ForceApplyMods;

        var assetPlan = requiresFileReadyGate
            ? await Task.Run(() => BuildPairSyncAssetPlan(request.ApplicationBase, snapshot.CachedData, request.CharacterData, preparation.UpdatedData, snapshot.CachedPayloadFingerprint, incomingPayloadFingerprint, token), token).ConfigureAwait(false)
            : PairSyncAssetPlan.Empty;
        LogVisibilityDiagnostic("PLAN asset-plan pair={pair} app={app} requiresFileGate={fileGate} assets={assets} hashes={hashes} missing={missing} criticalAnim={anim} criticalVfx={vfx} support={support}",
            Pair.UserData.AliasOrUID,
            request.ApplicationBase,
            requiresFileReadyGate,
            assetPlan.Entries.Count,
            assetPlan.UniqueHashes.Count,
            assetPlan.MissingFiles.Count,
            assetPlan.ContainsAnimationCritical,
            assetPlan.ContainsVfxCritical,
            assetPlan.ContainsVfxPropSupport);

        token.ThrowIfCancellationRequested();

        return await _dalamudUtil.RunOnFrameworkThread(() => FinalizePairSyncPlan(request, readiness.Binding, snapshot, preparation, assetPlan)).ConfigureAwait(false);
    }

    private PairSyncPlanBuildResult FinalizePairSyncPlan(PairSyncRequest request, ActorBinding binding, ApplyFrameworkSnapshot snapshot, ApplyPreparation preparation, PairSyncAssetPlan assetPlan)
    {
        if (!IsVisible)
        {
            LogVisibilityDiagnostic("PLAN finalize deferred pair={pair} app={app} reason=hidden-before-finalize", Pair.UserData.AliasOrUID, request.ApplicationBase);
            return PairSyncPlanBuildResult.Deferred("pair became hidden before plan finalized");
        }

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
        {
            LogVisibilityDiagnostic("PLAN finalize deferred pair={pair} app={app} reason=other-sync owner={owner}", Pair.UserData.AliasOrUID, request.ApplicationBase, Pair.RemoteOtherSyncOwner);
            return PairSyncPlanBuildResult.Deferred("OtherSync acquired actor before plan finalized");
        }

        if (!IsApplyPreparationStillValid(snapshot))
        {
            LogVisibilityDiagnostic("PLAN finalize deferred pair={pair} app={app} reason=state-changed-while-planning", Pair.UserData.AliasOrUID, request.ApplicationBase);
            return PairSyncPlanBuildResult.Deferred("applied state changed while planning", retryImmediately: true);
        }

        if (!string.Equals(preparation.NewHash, _lastAttemptedDataHash, StringComparison.Ordinal))
        {
            _lastAttemptedDataHash = preparation.NewHash;
            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;
        }

        if (preparation.SameHash && preparation.SamePayload && request.Reason != PairSyncReason.BecameVisible && !request.ForceApplyCustomization && !_redrawOnNextApplication && !_forceApplyMods)
        {
            if (HasAuthoritativePlayerAppearancePayload(request.CharacterData))
            {
                Logger.LogDebug(
                    "[BASE-{appBase}] Refusing final same-payload no-op for {pair}: payload contains authoritative player appearance state and the live actor was not reasserted on this framework snapshot; forcing authoritative reassert",
                    request.ApplicationBase,
                    Pair.UserData.AliasOrUID);
                _forceApplyMods = true;
                _initialApplyPending = true;
            }
            else if (TryGetRecentMissingCheck(preparation.NewHash, out var hadMissing))
            {
                if (!hadMissing)
                    return PairSyncPlanBuildResult.NoOp("same hash/payload and recent missing-cache check passed");
            }
            else if (AllPayloadHashesSessionKnownPresent(request.CharacterData))
            {
                return PairSyncPlanBuildResult.NoOp("same hash/payload and all hashes are session-known present");
            }
            else
            {
                ScheduleMissingCheck(request.ApplicationBase, request.CharacterData);
                return PairSyncPlanBuildResult.Deferred("same hash/payload; scheduled missing-cache check");
            }
        }

        var updatedData = preparation.UpdatedData.ToDictionary(
            k => k.Key,
            v => new HashSet<PlayerChanges>(v.Value));

        if (_forceApplyMods && HasAuthoritativePlayerAppearancePayload(request.CharacterData))
            EnsureAuthoritativePlayerReassertChanges(request.CharacterData, updatedData);

        var lifecycleApplyRequested = _redrawOnNextApplication;
        var nonPlayerOnlyDetectedChange = updatedData.Count > 0 && updatedData.Keys.All(static kind => kind != ObjectKind.Player);
        var allowLifecyclePlayerApply = lifecycleApplyRequested && !nonPlayerOnlyDetectedChange;
        var lifecycleRedrawRequested = allowLifecyclePlayerApply && RequiresLifecyclePlayerRedraw(assetPlan);
        if (allowLifecyclePlayerApply)
        {
            if (!updatedData.TryGetValue(ObjectKind.Player, out var player))
            {
                player = [];
                updatedData[ObjectKind.Player] = player;
            }

            player.Add(PlayerChanges.ModFiles);
            if (lifecycleRedrawRequested)
                player.Add(PlayerChanges.ForcedRedraw);

            if (request.CharacterData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
                && !string.IsNullOrWhiteSpace(glamourerPayload))
            {
                player.Add(PlayerChanges.Glamourer);
            }

            if (Pair.IsCustomizePlusEnabled
                && request.CharacterData.CustomizePlusData.TryGetValue(ObjectKind.Player, out var customizePayload)
                && !string.IsNullOrWhiteSpace(customizePayload))
            {
                player.Add(PlayerChanges.Customize);
            }

            var effectiveManipulationData = Pair.GetEffectiveManipulationData(request.CharacterData.ManipulationData);
            if (!string.IsNullOrWhiteSpace(effectiveManipulationData))
            {
                player.Add(PlayerChanges.ModManip);
            }

            if (lifecycleRedrawRequested)
                _lifecycleRedrawApplications[request.ApplicationBase] = 0;

            _initialApplyPending = false;
        }
        else if (lifecycleApplyRequested && nonPlayerOnlyDetectedChange && Logger.IsEnabled(LogLevel.Trace))
        {
            Logger.LogTrace("[BASE-{appBase}] Deferring pending lifecycle player apply while processing non-player-only changes for {pair}; keeping minion/mount updates from redrawing the owner", request.ApplicationBase, Pair.UserData.AliasOrUID);
        }

        if (!lifecycleRedrawRequested)
            SuppressNonLifecyclePlayerForcedRedraw(updatedData, request.ApplicationBase);

        PromotePlayerManipulationToGlamourerApply(request.ApplicationBase, request.CharacterData, updatedData);

        EnsureNonPlayerRedrawChangesForCriticalAssets(updatedData, assetPlan);

        if (!updatedData.Any() && HasAuthoritativePlayerAppearancePayload(request.CharacterData))
        {
            Logger.LogDebug(
                "[BASE-{appBase}] Pair sync plan for {pair} had no diff changes, but the payload carries authoritative player appearance state; forcing visible reassert instead of cache-only NoOp",
                request.ApplicationBase,
                Pair.UserData.AliasOrUID);
            _forceApplyMods = true;
            _initialApplyPending = true;
            EnsureAuthoritativePlayerReassertChanges(request.CharacterData, updatedData);
        }

        if (!updatedData.Any())
        {
            LogVisibilityDiagnostic("PLAN finalize no-op pair={pair} app={app} reason=no-player-changes hash={hash} payload={payload}", Pair.UserData.AliasOrUID, request.ApplicationBase, preparation.NewHash, PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(request.CharacterData));
            return PairSyncPlanBuildResult.NoOp("no player changes detected");
        }

        Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
            "Applying Character Data")));

        _forceApplyMods |= request.ForceApplyCustomization;
        var forceApplyModsForThisApply = _forceApplyMods;

        if (updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            _pluginWarningNotificationManager.NotifyForMissingPlugins(Pair.UserData, PlayerName!, playerChanges);

        var updateModdedPaths = updatedData.Values.Any(v => v.Contains(PlayerChanges.ModFiles));
        var updateManip = updatedData.Values.Any(v => v.Contains(PlayerChanges.ModManip));
        var requiresFileReadyGate = RequiresAppearanceFileReadyGate(updatedData);
        if (requiresFileReadyGate && CanBypassFileReadyGateForKnownReadyDelta(updatedData, assetPlan, forceApplyModsForThisApply, lifecycleRedrawRequested))
            requiresFileReadyGate = false;
        var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(request.CharacterData);

        LogVisibilityDiagnostic("PLAN finalize execute pair={pair} app={app} reason={reason} lifecycleRedraw={lifecycle} forceMods={force} updateFiles={files} updateManip={manip} fileReadyGate={fileGate} changes={changes} assets={assets} missing={missing}",
            Pair.UserData.AliasOrUID,
            request.ApplicationBase,
            request.Reason,
            lifecycleRedrawRequested,
            forceApplyModsForThisApply,
            updateModdedPaths,
            updateManip,
            requiresFileReadyGate,
            DescribeUpdatedChanges(updatedData),
            assetPlan.Entries.Count,
            assetPlan.MissingFiles.Count);

        Logger.LogDebug(
            "[BASE-{appBase}] Pair sync plan for {pair}: reason={reason}, lifecycleRedraw={lifecycle}, forceMods={force}, updateFiles={files}, updateManip={manip}, fileReadyGate={fileGate}, changes={changes}, assets={assetCount}, missing={missing}, animCritical={anim}, vfxCritical={vfx}",
            request.ApplicationBase,
            Pair.UserData.AliasOrUID,
            request.Reason,
            lifecycleRedrawRequested,
            forceApplyModsForThisApply,
            updateModdedPaths,
            updateManip,
            requiresFileReadyGate,
            DescribeUpdatedChanges(updatedData),
            assetPlan.Entries.Count,
            assetPlan.MissingFiles.Count,
            assetPlan.ContainsAnimationCritical,
            assetPlan.ContainsVfxCritical);

        return PairSyncPlanBuildResult.Execute(new PairSyncPlan(request, binding, snapshot, preparation, assetPlan, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, forceApplyModsForThisApply, allowLifecyclePlayerApply, lifecycleRedrawRequested, payloadFingerprint));
    }

    private bool CanBypassFileReadyGateForKnownReadyDelta(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleRedrawRequested)
    {
        if (forceApplyModsForThisApply || lifecycleRedrawRequested || assetPlan == null)
            return false;

        if (assetPlan.MissingFiles.Count != 0
            || assetPlan.ContainsAnimationCritical
            || assetPlan.ContainsVfxCritical
            || assetPlan.ContainsVfxPropSupport)
        {
            return false;
        }

        if (updatedData.Count != 1 || !updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            return false;

        if (!playerChanges.Contains(PlayerChanges.ModFiles))
            return false;

        foreach (var change in playerChanges)
        {
            if (change is not (PlayerChanges.ModFiles or PlayerChanges.Glamourer or PlayerChanges.Customize or PlayerChanges.ModManip or PlayerChanges.Heels or PlayerChanges.Honorific or PlayerChanges.Moodles or PlayerChanges.PetNames))
                return false;
        }

        return assetPlan.UniqueHashes.Count == 0
            || assetPlan.UniqueHashes.All(hash => HasUsableCachedFileForHash(hash));
    }

    private void PromotePlayerManipulationToGlamourerApply(Guid applicationBase, CharacterData characterData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            return;

        if (!playerChanges.Contains(PlayerChanges.ModManip) || playerChanges.Contains(PlayerChanges.Glamourer))
            return;

        if (!HasPlayerGlamourerPayload(characterData))
            return;

        playerChanges.Add(PlayerChanges.Glamourer);
        Logger.LogDebug("[BASE-{appBase}] Promoting player manipulation update to Glamourer apply for {pair}; outfit metadata toggles need both Penumbra meta and Glamourer state to settle", applicationBase, Pair.UserData.AliasOrUID);
    }

    private async Task<bool> CanTreatSameVisiblePayloadAsNoOpAsync(PairSyncRequest request, PairSyncReadiness readiness, string reason, CancellationToken token)
    {
        if (!HasAuthoritativePlayerAppearancePayload(request.CharacterData))
            return true;

        if (!HasPlayerFilePayload(request.CharacterData))
        {
            ForceVisiblePlayerReassertForUnverifiedBinding(
                request.ApplicationBase,
                reason,
                "payload contains player Glamourer/Customize/manipulation appearance state without file-map proof; same-payload no-op would bless a potentially vanilla live actor");
            return false;
        }

        if (readiness.Binding == null || !readiness.Binding.ObjectIndex.HasValue || readiness.Binding.ObjectIndex.Value < 0)
        {
            ForceVisiblePlayerReassertForUnverifiedBinding(request.ApplicationBase, reason, "no object index in readiness binding");
            return false;
        }

        if (_penumbraCollection == Guid.Empty)
        {
            ForceVisiblePlayerReassertForUnverifiedBinding(request.ApplicationBase, reason, "no receiver temp collection exists yet");
            return false;
        }

        var match = await _ipcManager.Penumbra.TryGetObjectEffectiveCollectionMatchAsync(
            Logger,
            _penumbraCollection,
            readiness.Binding.ObjectIndex.Value,
            Pair.Ident,
            readiness.Binding.Address,
            PlayerName ?? Pair.UserData.AliasOrUID).ConfigureAwait(false);

        token.ThrowIfCancellationRequested();

        if (match.Checked && match.Matches)
        {
            ForceVisiblePlayerReassertForUnverifiedBinding(
                request.ApplicationBase,
                reason,
                "live collection binding matches, but same-payload player appearance data still needs authoritative reassertion rather than cache-only NoOp");
            return false;
        }

        ForceVisiblePlayerReassertForUnverifiedBinding(
            request.ApplicationBase,
            reason,
            $"effective collection {match.EffectiveCollectionId}/{match.EffectiveCollectionName} did not match expected {_penumbraCollection}");

        return false;
    }

    private void ForceVisiblePlayerReassertForUnverifiedBinding(Guid applicationBase, string reason, string detail)
    {
        Logger.LogDebug(
            "[BASE-{appBase}] Refusing same-payload no-op for {pair}: authoritative player appearance payload requires live apply proof ({reason}; {detail}); forcing authoritative reassert",
            applicationBase,
            Pair.UserData.AliasOrUID,
            reason,
            detail);

        _forceApplyMods = true;
        _initialApplyPending = true;
        _lastAttemptedDataHash = null;
    }

    private static bool HasPlayerFilePayload(CharacterData? charaData)
        => charaData?.FileReplacements != null
            && charaData.FileReplacements.TryGetValue(ObjectKind.Player, out var playerFiles)
            && playerFiles != null
            && playerFiles.Any(static item => item != null
                && (!string.IsNullOrWhiteSpace(item.Hash)
                    || !string.IsNullOrWhiteSpace(item.FileSwapPath)
                    || (item.GamePaths?.Any(static path => !string.IsNullOrWhiteSpace(path)) ?? false)));

    private static bool HasPlayerCustomizePayload(CharacterData charaData)
        => charaData?.CustomizePlusData != null
            && charaData.CustomizePlusData.TryGetValue(ObjectKind.Player, out var customizePayload)
            && !string.IsNullOrWhiteSpace(customizePayload);

    private static bool HasPlayerManipulationPayload(Pair pair, CharacterData charaData)
        => !string.IsNullOrWhiteSpace(pair.GetEffectiveManipulationData(charaData.ManipulationData));

    private bool HasAuthoritativePlayerAppearancePayload(CharacterData? charaData)
        => charaData != null
            && (HasPlayerFilePayload(charaData)
                || HasPlayerGlamourerPayload(charaData)
                || HasPlayerCustomizePayload(charaData)
                || HasPlayerManipulationPayload(Pair, charaData));

    private void EnsureAuthoritativePlayerReassertChanges(CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (!updatedData.TryGetValue(ObjectKind.Player, out var player))
        {
            player = [];
            updatedData[ObjectKind.Player] = player;
        }

        if (HasPlayerFilePayload(charaData))
            player.Add(PlayerChanges.ModFiles);

        if (HasPlayerGlamourerPayload(charaData))
            player.Add(PlayerChanges.Glamourer);

        if (Pair.IsCustomizePlusEnabled && HasPlayerCustomizePayload(charaData))
            player.Add(PlayerChanges.Customize);

        if (HasPlayerManipulationPayload(Pair, charaData))
            player.Add(PlayerChanges.ModManip);

        if (!string.IsNullOrWhiteSpace(charaData.HeelsData))
            player.Add(PlayerChanges.Heels);

        if (!string.IsNullOrWhiteSpace(charaData.HonorificData))
            player.Add(PlayerChanges.Honorific);

        if (!string.IsNullOrWhiteSpace(charaData.MoodlesData))
            player.Add(PlayerChanges.Moodles);

        if (!string.IsNullOrWhiteSpace(charaData.PetNamesData))
            player.Add(PlayerChanges.PetNames);
    }

    private static bool HasPlayerGlamourerPayload(CharacterData charaData)
        => charaData?.GlamourerData != null
            && charaData.GlamourerData.TryGetValue(ObjectKind.Player, out var glamourerPayload)
            && !string.IsNullOrWhiteSpace(glamourerPayload);

    private static bool RequiresAppearanceFileReadyGate(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        => updatedData.Values.Any(v => v.Contains(PlayerChanges.ModFiles)
            || v.Contains(PlayerChanges.Customize)
            || v.Contains(PlayerChanges.ModManip));

    private bool AllPayloadHashesSessionKnownPresent(CharacterData data)
    {
        if (data?.FileReplacements == null || data.FileReplacements.Count == 0)
            return false;

        var hashes = data.FileReplacements.Values
            .Where(static list => list != null)
            .SelectMany(static list => list)
            .Where(static item => item != null && string.IsNullOrWhiteSpace(item.FileSwapPath) && !string.IsNullOrWhiteSpace(item.Hash))
            .Select(static item => item.Hash)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return hashes.Length > 0 && hashes.All(hash => HasUsableCachedFileForHash(hash));
    }

    private static bool RequiresLifecyclePlayerRedraw(PairSyncAssetPlan assetPlan)
    {
        if (!assetPlan.HasAssets)
            return false;

        return assetPlan.Entries.Any(static entry => entry.ObjectKind == ObjectKind.Player
            && !LooksLikeOwnedObjectAssetPathForPairSync(entry.GamePath)
            && (entry.Criticality == PairSyncAssetCriticality.AppearanceCritical
                || PairApplyUtilities.IsTransientRedrawCriticalGamePath(entry.GamePath)
                || PairApplyUtilities.IsSkeletonOrPhysicsCriticalGamePath(entry.GamePath)));
    }

    private static bool LooksLikeOwnedObjectAssetPathForPairSync(string gamePath)
    {
        var normalized = PairApplyUtilities.NormalizeGamePath(gamePath);
        return normalized.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase);
    }

    private void SuppressNonLifecyclePlayerForcedRedraw(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, Guid applicationBase)
    {
        if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            return;

        if (!playerChanges.Remove(PlayerChanges.ForcedRedraw))
            return;

        Logger.LogDebug(
            "[BASE-{appBase}] Suppressed non-lifecycle player ForcedRedraw for {pair}; normal receiver state changes will use temp mods/Glamourer refresh unless content is redraw-critical",
            applicationBase,
            Pair.UserData.AliasOrUID);

        if (playerChanges.Count == 0)
            updatedData.Remove(ObjectKind.Player);
    }

    private static void EnsureNonPlayerRedrawChangesForCriticalAssets(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, PairSyncAssetPlan assetPlan)
    {
        if (!assetPlan.HasAssets)
            return;

        foreach (var objectKind in assetPlan.Entries
            .Where(static entry => IsNonPlayerRedrawCriticalAsset(entry))
            .Select(static entry => entry.ObjectKind)
            .Distinct())
        {
            if (!updatedData.TryGetValue(objectKind, out var changes))
            {
                changes = [];
                updatedData[objectKind] = changes;
            }

            changes.Add(PlayerChanges.ModFiles);
            changes.Add(PlayerChanges.ForcedRedraw);
        }
    }

    private static bool IsNonPlayerRedrawCriticalAsset(PairSyncAssetEntry entry)
    {
        if (entry.ObjectKind == ObjectKind.Player)
            return false;

        if (entry.Criticality == PairSyncAssetCriticality.AppearanceCritical)
            return true;

        return PairApplyUtilities.IsTransientRedrawCriticalGamePath(entry.GamePath)
            || PairApplyUtilities.IsSkeletonOrPhysicsCriticalGamePath(entry.GamePath);
    }

    private static string DescribeUpdatedChanges(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (updatedData.Count == 0)
            return "none";

        return string.Join(";", updatedData.OrderBy(k => (int)k.Key).Select(k => $"{k.Key}:{string.Join('|', k.Value.OrderBy(v => v.ToString()))}"));
    }

    private PairSyncAssetPlan BuildPairSyncAssetPlan(Guid applicationBase, CharacterData previousData, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, string previousPayloadFingerprint, string incomingPayloadFingerprint, CancellationToken token)
    {
        if (TryBuildPairSyncDeltaAssetPlan(applicationBase, previousData, charaData, updatedData, previousPayloadFingerprint, incomingPayloadFingerprint, token, out var deltaPlan))
            return deltaPlan;

        var missingFiles = _modPathResolver.Calculate(applicationBase, charaData, out var moddedPaths, token, Pair.EffectiveScreenShakeEnabled);
        return BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, missingFiles, charaData);
    }

    private bool TryBuildPairSyncDeltaAssetPlan(Guid applicationBase, CharacterData previousData, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, string previousPayloadFingerprint, string incomingPayloadFingerprint, CancellationToken token, out PairSyncAssetPlan deltaPlan)
    {
        deltaPlan = PairSyncAssetPlan.Empty;

        if (!CanUseReceiverDeltaAssetPlan(updatedData, previousPayloadFingerprint, incomingPayloadFingerprint))
            return false;

        PairSyncAssetPlan? previousPlan;
        lock (_pairSyncAssetPlanCacheGate)
        {
            if (string.IsNullOrWhiteSpace(_lastResolvedPairSyncPayloadFingerprint)
                || !string.Equals(_lastResolvedPairSyncPayloadFingerprint, previousPayloadFingerprint, StringComparison.Ordinal)
                || _lastResolvedPairSyncAssetPlan == null)
            {
                return false;
            }

            previousPlan = _lastResolvedPairSyncAssetPlan;
        }

        if (!TryBuildDeltaReplacementData(previousData, charaData, out var deltaData, out var touchedGamePaths, out var removedGamePaths))
            return false;

        if (touchedGamePaths.Count == 0)
            return false;

        var missingFiles = _modPathResolver.Calculate(applicationBase, deltaData, out var deltaModdedPaths, token, Pair.EffectiveScreenShakeEnabled);
        var fullModdedPaths = previousPlan.ModdedPaths.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        var deltaEntryPaths = new Dictionary<(string GamePath, string? Hash), string>();

        foreach (var existing in previousPlan.ModdedPaths)
        {
            if (!touchedGamePaths.Contains(NormalizePairSyncDeltaGamePath(existing.Key.GamePath)))
                continue;

            deltaEntryPaths[existing.Key] = existing.Value;
        }

        foreach (var path in touchedGamePaths)
            RemoveModdedPathEntriesForGamePath(fullModdedPaths, path);

        foreach (var item in deltaModdedPaths)
        {
            fullModdedPaths[item.Key] = item.Value;
            deltaEntryPaths[item.Key] = item.Value;
        }

        deltaPlan = BuildPairSyncAssetPlanFromResolvedPaths(fullModdedPaths, missingFiles, charaData, deltaEntryPaths);

        if (Logger.IsEnabled(LogLevel.Debug))
        {
            Logger.LogDebug(
                "[BASE-{appBase}] Receiver delta asset plan for {pair}: touched={touched}, removed={removed}, deltaResolved={deltaResolved}, missing={missing}, fullResolved={fullResolved}, previousPayload={previousPayload}, incomingPayload={incomingPayload}",
                applicationBase,
                Pair.UserData.AliasOrUID,
                touchedGamePaths.Count,
                removedGamePaths.Count,
                deltaModdedPaths.Count,
                missingFiles.Count,
                fullModdedPaths.Count,
                previousPayloadFingerprint,
                incomingPayloadFingerprint);
        }

        return true;
    }

    private bool CanUseReceiverDeltaAssetPlan(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, string previousPayloadFingerprint, string incomingPayloadFingerprint)
    {
        if (string.IsNullOrWhiteSpace(previousPayloadFingerprint)
            || string.Equals(previousPayloadFingerprint, "EMPTY", StringComparison.Ordinal)
            || string.Equals(previousPayloadFingerprint, incomingPayloadFingerprint, StringComparison.Ordinal)
            || _forceApplyMods
            || _redrawOnNextApplication
            || _initialApplyPending)
        {
            return false;
        }

        if (updatedData.Count != 1 || !updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            return false;

        if (!playerChanges.Contains(PlayerChanges.ModFiles) || playerChanges.Contains(PlayerChanges.ForcedRedraw))
            return false;

        foreach (var change in playerChanges)
        {
            if (change is not (PlayerChanges.ModFiles or PlayerChanges.Glamourer or PlayerChanges.Customize or PlayerChanges.ModManip or PlayerChanges.Heels or PlayerChanges.Honorific or PlayerChanges.Moodles or PlayerChanges.PetNames))
                return false;
        }

        return true;
    }

    private static PairSyncAssetPlan BuildPairSyncAssetPlanFromResolvedPaths(Dictionary<(string GamePath, string? Hash), string> moddedPaths, List<FileReplacementData>? missingFiles = null, CharacterData? charaData = null, Dictionary<(string GamePath, string? Hash), string>? entryModdedPaths = null)
    {
        var entrySource = entryModdedPaths ?? moddedPaths;
        if (moddedPaths.Count == 0 && entrySource.Count == 0 && (missingFiles == null || missingFiles.Count == 0))
            return PairSyncAssetPlan.Empty;

        var objectKindByGamePath = charaData == null ? null : BuildPairSyncObjectKindLookup(charaData);
        var entries = new List<PairSyncAssetEntry>(entrySource.Count);
        var uniqueHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var containsAnimationCritical = false;
        var containsVfxCritical = false;
        var containsVfxPropSupport = false;

        foreach (var item in entrySource)
        {
            var gamePath = item.Key.GamePath?.Replace('\\', '/').Trim();
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            var hash = item.Key.Hash;
            if (!string.IsNullOrWhiteSpace(hash))
                uniqueHashes.Add(hash);

            var kind = ClassifyPairSyncAssetKind(gamePath, hash);
            var criticality = ClassifyPairSyncAssetCriticality(gamePath, kind);
            containsAnimationCritical |= criticality == PairSyncAssetCriticality.AnimationCritical;
            containsVfxCritical |= criticality == PairSyncAssetCriticality.VfxCritical;
            containsVfxPropSupport |= PairApplyUtilities.IsVfxPropSupportGamePath(gamePath);

            var objectKind = objectKindByGamePath != null && objectKindByGamePath.TryGetValue(gamePath, out var mappedKind)
                ? mappedKind
                : GuessPairSyncObjectKindFromGamePath(gamePath);
            entries.Add(new PairSyncAssetEntry(objectKind, gamePath, hash, item.Value, kind, criticality));
        }

        var primeTransientPaths = new List<string>();
        var primeTransientPathsByKind = new Dictionary<ObjectKind, HashSet<string>>();
        var transientSupportPaths = new List<string>();

        if (containsAnimationCritical || containsVfxCritical)
        {
            foreach (var entry in entries)
            {
                var isPropSupport = PairApplyUtilities.IsVfxPropSupportGamePath(entry.GamePath);
                if (entry.Criticality == PairSyncAssetCriticality.AnimationCritical
                    || entry.Criticality == PairSyncAssetCriticality.VfxCritical
                    || isPropSupport
                    || (containsVfxCritical && PairApplyUtilities.IsVfxModelSupportGamePath(entry.GamePath)))
                {
                    primeTransientPaths.Add(entry.GamePath);
                    if (!primeTransientPathsByKind.TryGetValue(entry.ObjectKind, out var byKindSet))
                        primeTransientPathsByKind[entry.ObjectKind] = byKindSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    byKindSet.Add(entry.GamePath);
                }

                if (isPropSupport)
                    transientSupportPaths.Add(entry.GamePath);
            }
        }
        else
        {
            foreach (var entry in entries)
            {
                if (PairApplyUtilities.IsVfxPropSupportGamePath(entry.GamePath))
                {
                    primeTransientPaths.Add(entry.GamePath);
                    if (!primeTransientPathsByKind.TryGetValue(entry.ObjectKind, out var byKindSet))
                        primeTransientPathsByKind[entry.ObjectKind] = byKindSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    byKindSet.Add(entry.GamePath);
                    transientSupportPaths.Add(entry.GamePath);
                }
            }
        }

        primeTransientPaths = primeTransientPaths.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        transientSupportPaths = transientSupportPaths.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        var tempMods = PairApplyUtilities.BuildPenumbraTempMods(moddedPaths);
        var tempModsFingerprint = PairApplyUtilities.ComputeTempModsFingerprint(tempMods);
        var transientSupportFingerprint = PairApplyUtilities.ComputePathSetFingerprint(transientSupportPaths);
        var finalizedPrimeTransientPathsByKind = primeTransientPathsByKind.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase).ToList());

        return new PairSyncAssetPlan(moddedPaths, missingFiles?.ToList() ?? [], entries, uniqueHashes, primeTransientPaths, finalizedPrimeTransientPathsByKind, transientSupportPaths, containsAnimationCritical, containsVfxCritical, containsVfxPropSupport, tempModsFingerprint, transientSupportFingerprint);
    }

    private static bool TryBuildDeltaReplacementData(CharacterData previousData, CharacterData charaData, out CharacterData deltaData, out HashSet<string> touchedGamePaths, out HashSet<string> removedGamePaths)
    {
        deltaData = new CharacterData();
        touchedGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        removedGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var previous = BuildPairSyncReplacementLookup(previousData);
        var current = BuildPairSyncReplacementLookup(charaData);

        foreach (var previousItem in previous)
        {
            if (current.ContainsKey(previousItem.Key))
                continue;

            touchedGamePaths.Add(previousItem.Key);
            removedGamePaths.Add(previousItem.Key);
        }

        foreach (var currentItem in current)
        {
            var changed = !previous.TryGetValue(currentItem.Key, out var previousItem)
                || !string.Equals(previousItem.Hash ?? string.Empty, currentItem.Value.Hash ?? string.Empty, StringComparison.OrdinalIgnoreCase)
                || !string.Equals(previousItem.FileSwapPath ?? string.Empty, currentItem.Value.FileSwapPath ?? string.Empty, StringComparison.OrdinalIgnoreCase);

            if (!changed)
                continue;

            touchedGamePaths.Add(currentItem.Key);
            AddDeltaReplacement(deltaData, currentItem.Value);
        }

        return touchedGamePaths.Count > 0;
    }

    private static Dictionary<string, PairSyncReplacementLookupEntry> BuildPairSyncReplacementLookup(CharacterData? data)
    {
        var lookup = new Dictionary<string, PairSyncReplacementLookupEntry>(StringComparer.OrdinalIgnoreCase);
        if (data?.FileReplacements == null)
            return lookup;

        foreach (var objectFiles in data.FileReplacements)
        {
            foreach (var replacement in objectFiles.Value ?? [])
            {
                if (replacement?.GamePaths == null)
                    continue;

                foreach (var gamePath in replacement.GamePaths)
                {
                    var normalized = NormalizePairSyncDeltaGamePath(gamePath);
                    if (string.IsNullOrWhiteSpace(normalized))
                        continue;

                    lookup[normalized] = new PairSyncReplacementLookupEntry(objectFiles.Key, normalized, replacement.Hash, replacement.FileSwapPath);
                }
            }
        }

        return lookup;
    }

    private static void AddDeltaReplacement(CharacterData deltaData, PairSyncReplacementLookupEntry entry)
    {
        if (!deltaData.FileReplacements.TryGetValue(entry.ObjectKind, out var replacements) || replacements == null)
        {
            replacements = [];
            deltaData.FileReplacements[entry.ObjectKind] = replacements;
        }

        replacements.Add(new FileReplacementData
        {
            GamePaths = [entry.GamePath],
            Hash = entry.Hash,
            FileSwapPath = entry.FileSwapPath ?? string.Empty,
        });
    }

    private static void RemoveModdedPathEntriesForGamePath(Dictionary<(string GamePath, string? Hash), string> moddedPaths, string gamePath)
    {
        if (moddedPaths.Count == 0 || string.IsNullOrWhiteSpace(gamePath))
            return;

        foreach (var key in moddedPaths.Keys.Where(key => string.Equals(NormalizePairSyncDeltaGamePath(key.GamePath), gamePath, StringComparison.OrdinalIgnoreCase)).ToArray())
            moddedPaths.Remove(key);
    }

    private static string NormalizePairSyncDeltaGamePath(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Replace('\\', '/').Trim();

    private static ObjectKind GuessPairSyncObjectKindFromGamePath(string gamePath)
    {
        var normalized = PairApplyUtilities.NormalizeGamePath(gamePath);
        if (normalized.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase))
        {
            return ObjectKind.MinionOrMount;
        }

        return ObjectKind.Player;
    }

    private static Dictionary<string, ObjectKind> BuildPairSyncObjectKindLookup(CharacterData charaData)
    {
        var lookup = new Dictionary<string, ObjectKind>(StringComparer.OrdinalIgnoreCase);

        foreach (var objectFiles in charaData.FileReplacements)
        {
            foreach (var replacement in objectFiles.Value ?? [])
            {
                foreach (var gamePath in replacement.GamePaths ?? [])
                {
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    var normalized = gamePath.Replace('\\', '/').Trim();
                    if (string.IsNullOrWhiteSpace(normalized))
                        continue;

                    // If a prop/support path is present in the player bucket, keep it associated
                    // with the visible character even when the path itself looks like a monster,
                    // demihuman, minion, or weapon asset.  These support assets are part of the
                    // player's authoritative state; they just must not automatically promote into
                    // a player redraw request.
                    if (lookup.TryGetValue(normalized, out var existingKind))
                    {
                        if (existingKind == ObjectKind.Player)
                            continue;

                        if (objectFiles.Key != ObjectKind.Player)
                            continue;
                    }

                    lookup[normalized] = objectFiles.Key;
                }
            }
        }

        return lookup;
    }

    private static PairSyncAssetKind ClassifyPairSyncAssetKind(string gamePath, string? hash)
    {
        var extension = Path.GetExtension(gamePath);
        if (!string.IsNullOrWhiteSpace(extension))
        {
            return extension.ToLowerInvariant() switch
            {
                ".mdl" => PairSyncAssetKind.Model,
                ".mtrl" => PairSyncAssetKind.Material,
                ".tex" => PairSyncAssetKind.Texture,
                ".atex" => PairSyncAssetKind.Vfx,
                ".pap" => PairSyncAssetKind.Animation,
                ".tmb" or ".tmb2" => PairSyncAssetKind.Timeline,
                ".avfx" or ".shpk" or ".eid" or ".skp" => PairSyncAssetKind.Vfx,
                ".scd" => PairSyncAssetKind.Sound,
                ".sklb" or ".atch" => PairSyncAssetKind.Skeleton,
                ".phyb" or ".pbd" => PairSyncAssetKind.Physics,
                _ => string.IsNullOrWhiteSpace(hash) ? PairSyncAssetKind.FileSwap : PairSyncAssetKind.Unknown,
            };
        }

        return string.IsNullOrWhiteSpace(hash)
            ? PairSyncAssetKind.FileSwap
            : PairSyncAssetKind.Unknown;
    }

    private static PairSyncAssetCriticality ClassifyPairSyncAssetCriticality(string gamePath, PairSyncAssetKind kind)
    {
        if (PairApplyUtilities.IsAnimationCriticalGamePath(gamePath))
            return PairSyncAssetCriticality.AnimationCritical;

        if (PairApplyUtilities.IsVfxCriticalGamePath(gamePath))
            return PairSyncAssetCriticality.VfxCritical;

        if (PairApplyUtilities.IsVfxModelSupportGamePath(gamePath))
            return PairSyncAssetCriticality.VfxSupportCritical;

        return kind is PairSyncAssetKind.Model or PairSyncAssetKind.Material or PairSyncAssetKind.Texture
            ? PairSyncAssetCriticality.AppearanceCritical
            : PairSyncAssetCriticality.Normal;
    }

    private void RememberAppliedPairSyncAssetPlan(PairSyncPlan plan)
    {
        if (plan == null || string.IsNullOrWhiteSpace(plan.PayloadFingerprint))
            return;

        lock (_pairSyncAssetPlanCacheGate)
        {
            if (plan.UpdateModdedPaths || plan.AssetPlan.HasAssets || _lastResolvedPairSyncAssetPlan == null)
            {
                _lastResolvedPairSyncAssetPlan = plan.AssetPlan;
            }

            _lastResolvedPairSyncPayloadFingerprint = plan.PayloadFingerprint;
        }
    }

    private void ClearPairSyncAssetPlanCache()
    {
        lock (_pairSyncAssetPlanCacheGate)
        {
            _lastResolvedPairSyncAssetPlan = null;
            _lastResolvedPairSyncPayloadFingerprint = null;
        }
    }

    private async Task<PairSyncCommitResult> ExecutePairSyncPlanAsync(PairSyncPlan plan, CancellationToken token)
    {
        if (!plan.HasWork)
        {
            LogVisibilityDiagnostic("EXEC skip pair={pair} app={app} reason=no-work", Pair.UserData.AliasOrUID, plan.Request.ApplicationBase);
            return PairSyncCommitResult.Succeeded();
        }

        if (!IsVisible || _charaHandler == null || ResolveStrictVisiblePlayerAddress(_charaHandler.Address) == nint.Zero)
        {
            LogVisibilityDiagnostic("EXEC hidden pair={pair} app={app} visible={visible} hasHandler={handler}", Pair.UserData.AliasOrUID, plan.Request.ApplicationBase, IsVisible, _charaHandler != null);
            return PairSyncCommitResult.Hidden("pair became hidden or lost actor before execution");
        }

        if (Pair.IsPaused)
        {
            LogVisibilityDiagnostic("EXEC paused pair={pair} app={app}", Pair.UserData.AliasOrUID, plan.Request.ApplicationBase);
            return PairSyncCommitResult.Paused("pair became paused before execution");
        }

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
        {
            LogVisibilityDiagnostic("EXEC other-sync pair={pair} app={app} owner={owner}", Pair.UserData.AliasOrUID, plan.Request.ApplicationBase, Pair.RemoteOtherSyncOwner);
            return PairSyncCommitResult.YieldedToOtherSync("yielded to OtherSync before execution");
        }

        var startedTick = Environment.TickCount64;

        PairSyncCommitResult handoffResult;
        try
        {
            LogVisibilityDiagnostic("EXEC handoff start pair={pair} app={app} hash={hash} changes={changes} files={files} manip={manip} fileGate={fileGate}",
                Pair.UserData.AliasOrUID,
                plan.Request.ApplicationBase,
                plan.Request.CharacterData.DataHash.Value,
                DescribeUpdatedChanges(plan.UpdatedData),
                plan.UpdateModdedPaths,
                plan.UpdateManipulation,
                plan.RequiresFileReadyGate);
            handoffResult = await DownloadAndApplyCharacterAsync(plan.Request.ApplicationBase, plan.Request.CharacterData, plan.UpdatedData, plan.UpdateModdedPaths, plan.UpdateManipulation, plan.RequiresFileReadyGate, plan.AssetPlan, plan.ForceApplyModsForThisApply, plan.LifecycleApplyRequested, plan.LifecycleRedrawRequested, token).ConfigureAwait(false);
            LogVisibilityDiagnostic("EXEC handoff result pair={pair} app={app} status={status} reason={reason} commitStarted={commitStarted}",
                Pair.UserData.AliasOrUID,
                plan.Request.ApplicationBase,
                handoffResult.Status,
                handoffResult.Reason,
                handoffResult.CommitStarted);
        }
        catch (OperationCanceledException)
        {
            return PairSyncCommitResult.Cancelled("prepare/download was cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "[BASE-{appBase}] Pair sync prepare/download failed for {pair}",
                plan.Request.ApplicationBase, Pair.UserData.AliasOrUID);
            return PairSyncCommitResult.DownloadFailed("prepare/download threw before commit handoff", retryImmediately: true);
        }

        if (!handoffResult.CommitStarted)
            return handoffResult;

        SetSyncStageForCommit();

        try
        {
            var applicationTask = _pairSyncApplicationTask;
            if (applicationTask == null)
                return PairSyncCommitResult.ApplyFailed("commit handoff reported success, but no pair-sync application task was available", retryImmediately: true);

            var applicationResult = await applicationTask.ConfigureAwait(false);
            token.ThrowIfCancellationRequested();

            LogVisibilityDiagnostic("EXEC application-task result pair={pair} app={app} status={status} reason={reason}",
                Pair.UserData.AliasOrUID,
                plan.Request.ApplicationBase,
                applicationResult.Status,
                applicationResult.Reason);

            if (!applicationResult.Success)
                return applicationResult;
        }
        catch (OperationCanceledException)
        {
            return PairSyncCommitResult.Cancelled("application commit was cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "[BASE-{appBase}] Pair sync application commit failed for {pair}",
                plan.Request.ApplicationBase, Pair.UserData.AliasOrUID);
            return PairSyncCommitResult.ApplyFailed("application task threw before commit verification", retryImmediately: true);
        }

        if (WasPairSyncPlanCommitted(plan, startedTick))
        {
            LogVisibilityDiagnostic("EXEC commit-observed pair={pair} app={app} hash={hash} payload={payload}", Pair.UserData.AliasOrUID, plan.Request.ApplicationBase, plan.Request.CharacterData.DataHash.Value, plan.PayloadFingerprint);
            return PairSyncCommitResult.Succeeded();
        }

        if (!IsVisible || _charaHandler == null || ResolveStrictVisiblePlayerAddress(_charaHandler.Address) == nint.Zero)
            return PairSyncCommitResult.Hidden("actor was lost before commit was observed");

        if (Pair.IsPaused)
            return PairSyncCommitResult.Paused("pair became paused before commit was observed");

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
            return PairSyncCommitResult.YieldedToOtherSync("yielded to OtherSync before commit was observed");

        LogVisibilityDiagnostic("EXEC verification-failed pair={pair} app={app} hash={hash} payload={payload} cachedHash={cachedHash}",
            Pair.UserData.AliasOrUID,
            plan.Request.ApplicationBase,
            plan.Request.CharacterData.DataHash.Value,
            plan.PayloadFingerprint,
            _cachedData?.DataHash.Value ?? string.Empty);
        return PairSyncCommitResult.VerificationFailed("commit was not observed; desired state remains pending", retryImmediately: true);
    }

    private void SetSyncStageForCommit()
    {
        _syncWorker?.SetCommittingForObservedApplication();
    }

    private bool WasPairSyncPlanCommitted(PairSyncPlan plan, long startedTick)
    {
        var applied = _cachedData;
        if (applied == null)
            return false;

        var expectedHash = plan.Request.CharacterData.DataHash.Value;
        var appliedHash = applied.DataHash.Value;
        if (!string.Equals(expectedHash, appliedHash, StringComparison.Ordinal))
            return false;

        var appliedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(applied);
        if (!string.Equals(appliedPayloadFingerprint, plan.PayloadFingerprint, StringComparison.Ordinal))
            return false;

        var completedTick = Volatile.Read(ref _lastApplyCompletedTick);
        if (completedTick <= 0)
            return false;

        return unchecked(completedTick - startedTick) >= 0;
    }
}
