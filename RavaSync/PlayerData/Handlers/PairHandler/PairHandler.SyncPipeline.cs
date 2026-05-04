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
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    public sealed record PairSyncDiagnosticsSnapshot(string PairUid, string Stage, long DesiredVersion, long PreparedVersion, long AppliedVersion, string ActivePayloadFingerprint, string LastStatus, string LastReason, DateTime LastUpdatedUtc, long ReadinessMs, long PlanMs, long ExecuteMs, long TotalMs, int AssetCount, int MissingFileCount, int PrimeTransientPathCount, bool ContainsAnimationCritical, bool ContainsVfxCritical, string SenderManifestFingerprint)
    {
        public static PairSyncDiagnosticsSnapshot Empty(string pairUid) => new(pairUid, "Idle", 0, 0, 0, string.Empty, "None", string.Empty, DateTime.MinValue, 0, 0, 0, 0, 0, 0, 0, false, false, string.Empty);
    }

    public PairSyncDiagnosticsSnapshot SyncDiagnostics => _syncWorker?.GetDiagnostics() ?? PairSyncDiagnosticsSnapshot.Empty(Pair.UserData.UID);

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

    private sealed record PairSyncAssetPlan(Dictionary<(string GamePath, string? Hash), string> ModdedPaths, List<FileReplacementData> MissingFiles, List<PairSyncAssetEntry> Entries, HashSet<string> UniqueHashes, List<string> PrimeTransientPaths, List<string> TransientSupportPaths, bool ContainsAnimationCritical, bool ContainsVfxCritical, bool ContainsVfxPropSupport, string TempModsFingerprint, string TransientSupportFingerprint)
    {
        public static PairSyncAssetPlan Empty { get; } = new([], [], [], new(StringComparer.OrdinalIgnoreCase), [], [], false, false, false, "EMPTY", "EMPTY");
        public bool HasAssets => Entries.Count > 0;
        public bool HasMissingFiles => MissingFiles.Count > 0;
        public bool RequiresTransientPrime => PrimeTransientPaths.Count > 0;
        public bool RequiresFirstUseTransientWarmup => ContainsAnimationCritical || ContainsVfxCritical || PrimeTransientPaths.Count > 0 || TransientSupportPaths.Count > 0;
        public bool RequiresFirstUseModelSupportWarmup => (ContainsAnimationCritical || ContainsVfxCritical) && TransientSupportPaths.Count > 0;
        public bool RequiresTransientSupportRefresh(string? lastAppliedTransientSupportFingerprint) => (ContainsAnimationCritical || ContainsVfxCritical) && ContainsVfxPropSupport && !string.Equals(TransientSupportFingerprint, lastAppliedTransientSupportFingerprint, StringComparison.Ordinal);
    }

    private sealed record PairSyncReadiness(bool Ready, bool RetryImmediately, string Reason, ActorBinding? Binding, ApplyFrameworkSnapshot? Snapshot)
    {
        public static PairSyncReadiness NotReady(string reason, bool retryImmediately = false)
            => new(false, retryImmediately, reason, null, null);

        public static PairSyncReadiness ReadyNow(ActorBinding binding, ApplyFrameworkSnapshot snapshot)
            => new(true, false, string.Empty, binding, snapshot);
    }

    private sealed record PairSyncPlan(PairSyncRequest Request, ActorBinding Binding, ApplyFrameworkSnapshot Snapshot, ApplyPreparation Preparation, PairSyncAssetPlan AssetPlan, Dictionary<ObjectKind, HashSet<PlayerChanges>> UpdatedData, bool UpdateModdedPaths, bool UpdateManipulation, bool RequiresFileReadyGate, bool ForceApplyModsForThisApply, bool LifecycleRedrawRequested, string PayloadFingerprint)
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

            _owner.Logger.LogDebug(
                "Pair sync signal for {pair}: reason={reason}, hasLatest={hasLatest}, visible={visible}, hasLastReceived={hasLastReceived}, stage={stage}",
                _owner.Pair.UserData.AliasOrUID,
                reason,
                hasLatest,
                _owner.IsVisible,
                _owner.Pair.LastReceivedCharacterData != null,
                _stage);

            if (!hasLatest && _owner.IsVisible && _owner.Pair.LastReceivedCharacterData != null)
            {
                _ = Task.Run(() =>
                {
                    try
                    {
                        _owner.Pair.ApplyLastReceivedData(forced: true);
                    }
                    catch (Exception ex)
                    {
                        _owner.Logger.LogWarning(ex, "Failed to submit latest received data for {pair} after {reason}", _owner.Pair.UserData.AliasOrUID, reason);
                    }
                });
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
                    var result = PairSyncCommitResult.NoWork(build.Reason);
                    RecordCommitResult(result);
                    RecordDiagnostics(request, null, result, readinessMs, planMs, executeMs, totalStopwatch.ElapsedMilliseconds);
                    LogDiagnostics(GetDiagnostics());
                    _owner.Logger.LogTrace("[BASE-{appBase}] Pair sync no-op for {pair}: {reason}", request.ApplicationBase, _owner.Pair.UserData.AliasOrUID, build.Reason);
                    return;
                }

                if (build.Disposition == PairSyncPlanDisposition.Deferred || build.Plan == null)
                {
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
                _prepared = new PreparedPairState(plan.Request.Version, plan.PayloadFingerprint, DateTime.UtcNow);

                SetStage(PairSyncStage.Downloading, activeCts, plan.PayloadFingerprint);
                var executeStopwatch = Stopwatch.StartNew();
                var commitResult = await _owner.ExecutePairSyncPlanAsync(plan, token).ConfigureAwait(false);
                executeStopwatch.Stop();
                executeMs = executeStopwatch.ElapsedMilliseconds;

                token.ThrowIfCancellationRequested();

                if (!commitResult.Success)
                {
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

                RecordCommitResult(commitResult);

                lock (_gate)
                {
                    if (_latestRequest?.Version == plan.Request.Version)
                    {
                        _applied = new AppliedPairState(
                            plan.Request.Version,
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
            return PairSyncReadiness.NotReady("application stopping");

        if (!IsVisible)
            return PairSyncReadiness.NotReady("pair is not visible");

        if (Pair.IsPaused)
            return PairSyncReadiness.NotReady("pair is paused");

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
            return PairSyncReadiness.NotReady("yielded to OtherSync");

        if (_localOtherSyncDecisionYield && !Pair.EffectiveOverrideOtherSync)
            return PairSyncReadiness.NotReady("local OtherSync gate is yielded");

        if (_dalamudUtil.IsInCombatOrPerforming)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in combat or performing music, deferring application")));
            _dataReceivedInDowntime = new(request.ApplicationBase, request.CharacterData, request.ForceApplyCustomization);
            SetUploading(isUploading: false);
            return PairSyncReadiness.NotReady("combat/performance");
        }

        if (_charaHandler == null)
            return PairSyncReadiness.NotReady("no character handler");

        var strictAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
        if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
        {
            ResetVisibilityTracking();
            MarkInitialApplyRequired();
            return PairSyncReadiness.NotReady("visible actor address is not stable", retryImmediately: false);
        }

        if (_dalamudUtil.IsInGpose || _dalamudUtil.IsInCutscene || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in GPose, a cutscene, or Penumbra/Glamourer is not available")));
            return PairSyncReadiness.NotReady("framework/plugins unavailable");
        }

        var snapshot = CaptureApplyFrameworkSnapshot();
        if (!snapshot.HasCharaHandler || snapshot.ResolvedPlayerAddress == nint.Zero)
            return PairSyncReadiness.NotReady("framework snapshot is not apply-ready");

        var binding = new ActorBinding(Pair.Ident, PlayerName, strictAddress, _lastAssignedObjectIndex);
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

        if (!request.ForceApplyCustomization
            && !_redrawOnNextApplication
            && !_forceApplyMods
            && !string.IsNullOrEmpty(incomingHash)
            && string.Equals(incomingHash, snapshot.CachedHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, snapshot.CachedPayloadFingerprint, StringComparison.Ordinal))
        {
            if (TryGetRecentMissingCheck(incomingHash, out var hadMissing))
            {
                if (!hadMissing)
                    return PairSyncPlanBuildResult.NoOp("same payload and recent cache check passed");
            }
            else
            {
                ScheduleMissingCheck(request.ApplicationBase, request.CharacterData);
                return PairSyncPlanBuildResult.Deferred("same payload; scheduled missing-cache check");
            }
        }

        var preparation = await Task.Run(() => PrepareApplyData(request.ApplicationBase, request.CharacterData, request.ForceApplyCustomization, snapshot), token).ConfigureAwait(false);

        token.ThrowIfCancellationRequested();

        var requiresFileReadyGate = RequiresAppearanceFileReadyGate(preparation.UpdatedData)
            || request.ForceApplyCustomization
            || _redrawOnNextApplication
            || snapshot.ForceApplyMods;

        var assetPlan = requiresFileReadyGate
            ? await Task.Run(() => BuildPairSyncAssetPlan(request.ApplicationBase, request.CharacterData, token), token).ConfigureAwait(false)
            : PairSyncAssetPlan.Empty;

        token.ThrowIfCancellationRequested();

        return await _dalamudUtil.RunOnFrameworkThread(() => FinalizePairSyncPlan(request, readiness.Binding, snapshot, preparation, assetPlan)).ConfigureAwait(false);
    }

    private PairSyncPlanBuildResult FinalizePairSyncPlan(PairSyncRequest request, ActorBinding binding, ApplyFrameworkSnapshot snapshot, ApplyPreparation preparation, PairSyncAssetPlan assetPlan)
    {
        if (!IsVisible)
            return PairSyncPlanBuildResult.Deferred("pair became hidden before plan finalized");

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
            return PairSyncPlanBuildResult.Deferred("OtherSync acquired actor before plan finalized");

        if (!IsApplyPreparationStillValid(snapshot))
            return PairSyncPlanBuildResult.Deferred("applied state changed while planning", retryImmediately: true);

        if (!string.Equals(preparation.NewHash, _lastAttemptedDataHash, StringComparison.Ordinal))
        {
            _lastAttemptedDataHash = preparation.NewHash;
            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;
        }

        if (preparation.SameHash && preparation.SamePayload && !request.ForceApplyCustomization && !_redrawOnNextApplication)
        {
            if (TryGetRecentMissingCheck(preparation.NewHash, out var hadMissing))
            {
                if (!hadMissing)
                    return PairSyncPlanBuildResult.NoOp("same hash/payload and recent missing-cache check passed");
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

        var lifecycleRedrawRequested = _redrawOnNextApplication;
        if (lifecycleRedrawRequested)
        {
            if (!updatedData.TryGetValue(ObjectKind.Player, out var player))
            {
                player = [];
                updatedData[ObjectKind.Player] = player;
            }

            player.Add(PlayerChanges.ModFiles);
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

            _lifecycleRedrawApplications[request.ApplicationBase] = 0;
            _initialApplyPending = false;
        }

        if (!lifecycleRedrawRequested)
            SuppressNonLifecyclePlayerForcedRedraw(updatedData, request.ApplicationBase);

        if (!updatedData.Any())
            return PairSyncPlanBuildResult.NoOp("no player changes detected");

        Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
            "Applying Character Data")));

        _forceApplyMods |= request.ForceApplyCustomization;
        var forceApplyModsForThisApply = _forceApplyMods;

        if (_charaHandler != null && _forceApplyMods)
            _forceApplyMods = false;

        if (updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            _pluginWarningNotificationManager.NotifyForMissingPlugins(Pair.UserData, PlayerName!, playerChanges);

        var updateModdedPaths = updatedData.Values.Any(v => v.Contains(PlayerChanges.ModFiles));
        var updateManip = updatedData.Values.Any(v => v.Contains(PlayerChanges.ModManip));
        var requiresFileReadyGate = RequiresAppearanceFileReadyGate(updatedData);
        var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(request.CharacterData);

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

        return PairSyncPlanBuildResult.Execute(new PairSyncPlan(request, binding, snapshot, preparation, assetPlan, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, forceApplyModsForThisApply, lifecycleRedrawRequested, payloadFingerprint));
    }

    private static bool RequiresAppearanceFileReadyGate(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
        => updatedData.Values.Any(v => v.Contains(PlayerChanges.ModFiles)
            || v.Contains(PlayerChanges.Glamourer)
            || v.Contains(PlayerChanges.Customize)
            || v.Contains(PlayerChanges.ModManip));

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

    private static string DescribeUpdatedChanges(Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (updatedData.Count == 0)
            return "none";

        return string.Join(";", updatedData.OrderBy(k => (int)k.Key).Select(k => $"{k.Key}:{string.Join('|', k.Value.OrderBy(v => v.ToString()))}"));
    }

    private PairSyncAssetPlan BuildPairSyncAssetPlan(Guid applicationBase, CharacterData charaData, CancellationToken token)
    {
        var missingFiles = _modPathResolver.Calculate(applicationBase, charaData, out var moddedPaths, token);
        return BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, missingFiles, charaData);
    }

    private static PairSyncAssetPlan BuildPairSyncAssetPlanFromResolvedPaths(Dictionary<(string GamePath, string? Hash), string> moddedPaths, List<FileReplacementData>? missingFiles = null, CharacterData? charaData = null)
    {
        if (moddedPaths.Count == 0 && (missingFiles == null || missingFiles.Count == 0))
            return PairSyncAssetPlan.Empty;

        var objectKindByGamePath = charaData == null ? null : BuildPairSyncObjectKindLookup(charaData);
        var entries = new List<PairSyncAssetEntry>(moddedPaths.Count);
        var uniqueHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var containsAnimationCritical = false;
        var containsVfxCritical = false;
        var containsVfxPropSupport = false;

        foreach (var item in moddedPaths)
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

            var objectKind = objectKindByGamePath != null && objectKindByGamePath.TryGetValue(gamePath, out var mappedKind) ? mappedKind : ObjectKind.Player;
            entries.Add(new PairSyncAssetEntry(objectKind, gamePath, hash, item.Value, kind, criticality));
        }

        var primeTransientPaths = new List<string>();
        var transientSupportPaths = new List<string>();

        if (containsAnimationCritical || containsVfxCritical)
        {
            foreach (var entry in entries)
            {
                if (entry.Criticality == PairSyncAssetCriticality.AnimationCritical
                    || entry.Criticality == PairSyncAssetCriticality.VfxCritical
                    || (containsVfxCritical && PairApplyUtilities.IsVfxModelSupportGamePath(entry.GamePath)))
                {
                    primeTransientPaths.Add(entry.GamePath);
                }

                if (PairApplyUtilities.IsVfxPropSupportGamePath(entry.GamePath))
                    transientSupportPaths.Add(entry.GamePath);
            }
        }
        else
        {
            foreach (var entry in entries)
            {
                if (PairApplyUtilities.IsVfxPropSupportGamePath(entry.GamePath))
                    transientSupportPaths.Add(entry.GamePath);
            }
        }

        primeTransientPaths = primeTransientPaths.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        transientSupportPaths = transientSupportPaths.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        var tempMods = PairApplyUtilities.BuildPenumbraTempMods(moddedPaths);
        var tempModsFingerprint = PairApplyUtilities.ComputeTempModsFingerprint(tempMods);
        var transientSupportFingerprint = PairApplyUtilities.ComputePathSetFingerprint(transientSupportPaths);
        return new PairSyncAssetPlan(moddedPaths, missingFiles?.ToList() ?? [], entries, uniqueHashes, primeTransientPaths, transientSupportPaths, containsAnimationCritical, containsVfxCritical, containsVfxPropSupport, tempModsFingerprint, transientSupportFingerprint);
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

                    lookup[gamePath.Replace('\\', '/').Trim()] = objectFiles.Key;
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
                ".sklb" => PairSyncAssetKind.Skeleton,
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

    private async Task<PairSyncCommitResult> ExecutePairSyncPlanAsync(PairSyncPlan plan, CancellationToken token)
    {
        if (!plan.HasWork)
            return PairSyncCommitResult.Succeeded();

        if (!IsVisible || _charaHandler == null || ResolveStrictVisiblePlayerAddress(_charaHandler.Address) == nint.Zero)
            return PairSyncCommitResult.Hidden("pair became hidden or lost actor before execution");

        if (Pair.IsPaused)
            return PairSyncCommitResult.Paused("pair became paused before execution");

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
            return PairSyncCommitResult.YieldedToOtherSync("yielded to OtherSync before execution");

        var startedTick = Environment.TickCount64;

        PairSyncCommitResult handoffResult;
        try
        {
            handoffResult = await DownloadAndApplyCharacterAsync(plan.Request.ApplicationBase, plan.Request.CharacterData, plan.UpdatedData, plan.UpdateModdedPaths, plan.UpdateManipulation, plan.RequiresFileReadyGate, plan.AssetPlan, plan.ForceApplyModsForThisApply, plan.LifecycleRedrawRequested, token).ConfigureAwait(false);
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
            return PairSyncCommitResult.Succeeded();

        if (!IsVisible || _charaHandler == null || ResolveStrictVisiblePlayerAddress(_charaHandler.Address) == nint.Zero)
            return PairSyncCommitResult.Hidden("actor was lost before commit was observed");

        if (Pair.IsPaused)
            return PairSyncCommitResult.Paused("pair became paused before commit was observed");

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
            return PairSyncCommitResult.YieldedToOtherSync("yielded to OtherSync before commit was observed");

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
