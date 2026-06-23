using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Utility;
using Glamourer.Api.Enums;
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

public sealed partial class PairHandler : DisposableMediatorSubscriberBase
{
    private sealed record CombatData(Guid ApplicationId, CharacterData CharacterData, bool Forced);

    private sealed record ApplyFrameworkSnapshot(bool IsInCombatOrPerforming,bool HasCharaHandler,nint ResolvedPlayerAddress,bool IsInGpose,bool PenumbraApiAvailable,bool GlamourerApiAvailable,string CachedHash,string CachedPayloadFingerprint,CharacterData CachedData,bool ForceApplyMods);

    private sealed record ApplyPreparation(string NewHash,string OldHash,bool SameHash,bool SamePayload,bool HasDiffMods,Dictionary<ObjectKind, HashSet<PlayerChanges>> UpdatedData);

    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileDownloadManagerFactory _fileDownloadManagerFactory;
    private FileDownloadManager? _downloadManager;
    private readonly FileCacheManager _fileDbManager;
    private readonly GameObjectHandlerFactory _gameObjectHandlerFactory;
    private readonly IpcManager _ipcManager;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly PlayerPerformanceService _playerPerformanceService;
    private readonly ServerConfigurationManager _serverConfigManager;
    private readonly PluginWarningNotificationService _pluginWarningNotificationManager;
    private readonly ModPathResolver _modPathResolver;
    private readonly PapSanitisationService _papSanitisationService;
    private readonly VisibilityCoordinator _visibilityCoordinator;
    private readonly OtherSyncCoordinator _otherSyncCoordinator;
    private readonly DownloadCoordinator _downloadCoordinator;
    private readonly ApplyExecutionCoordinator _applyExecutionCoordinator;
    private readonly PenumbraCoordinator _penumbraCoordinator;
    private readonly CustomizationCoordinator _customizationCoordinator;
    private readonly RepairCoordinator _repairCoordinator;
    private readonly TransientsCoordinator _transientsCoordinator;
    private PairSyncWorker? _syncWorker;
    private CancellationTokenSource? _applicationCancellationTokenSource = new();
    private CancellationTokenSource? _downloadCancellationTokenSource = new();
    private Guid _applicationId;
    private Task? _applicationTask;
    private Task<PairSyncCommitResult>? _pairSyncApplicationTask;
    private Task? _pairDownloadTask;
    private CharacterData? _cachedData = null;
    private GameObjectHandler? _charaHandler;
    private readonly Dictionary<ObjectKind, Guid?> _customizeIds = [];
    private CombatData? _dataReceivedInDowntime;
    private bool _forceApplyMods = false;
    private bool _isVisible;
    private Guid _penumbraCollection;
    private Task<Guid>? _penumbraCollectionTask;
    private Task? _initializeTask;
    private int _initializeStarted;
    private bool _redrawOnNextApplication = false;
    private bool _hasRetriedAfterMissingDownload = false;
    private bool _hasRetriedAfterMissingAtApply = false;
    private int _missingFileSelfHealRunning = 0;
    private readonly object _missingFileSelfHealGate = new();
    private CharacterData? _pendingMissingFileSelfHealData;
    private List<string>? _pendingMissingFileSelfHealHashes;
    private int _manualRepairRunning = 0;
    private int? _lastAssignedObjectIndex = null;
    private nint _lastAssignedPlayerAddress = nint.Zero;
    private readonly Dictionary<ObjectKind, (int ObjectIndex, nint Address)> _lastAssignedOwnedObjectIndices = [];
    private readonly ConcurrentDictionary<nint, byte> _summonedActorBindingInFlight = new();
    private readonly ConcurrentDictionary<nint, long> _nextSummonedActorBindingAttemptTick = new();
    private readonly ConcurrentDictionary<string, long> _summonedActorRedrawCooldownByKey = new(StringComparer.OrdinalIgnoreCase);
    private DateTime _lastAssignedCollectionAssignUtc = DateTime.MinValue;
    private DateTime _nextTempCollectionRetryNotBeforeUtc = DateTime.MinValue;
    private long _lastApplyCompletedTick;
    private long _nextVisiblePenumbraBindingValidationTick;
    private int _visiblePenumbraBindingValidationInFlight;
    private long _nextActiveSyncIndicatorValidationTick;
    private Task? _activeSyncIndicatorValidationTask;
    private static readonly bool IsWineRuntime = SafeIsWine();

    // RAVASYNC_VISIBILITY_DIAGNOSTICS: temporary Info-level tracing for the visible-pair send/apply invariant.
    // Remove this helper and all '[VIS-DIAG]' log lines once the root cause is confirmed in live logs.
    private const string VisibilityDiagnosticsPrefix = "[VIS-DIAG]";

    private void LogVisibilityDiagnostic(string message, params object[] args)
    {
        Logger.LogInformation(VisibilityDiagnosticsPrefix + " " + message, args);
    }

    private static bool SafeIsWine()
    {
        try { return Util.IsWine(); }
        catch { return false; }
    }

    private void ClearPairSyncTransferStatus()
    {
        Pair.SetCurrentDownloadStatus(null);
        Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
    }

    private void PublishPairSyncLoadingFilesStatus(int fileCount)
    {
        var totalFiles = Math.Max(1, fileCount);

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.LoadingFiles);
        Pair.SetCurrentDownloadSummary(new Pair.DownloadProgressSummary(
            HasAny: true,
            AnyDownloading: false,
            AnyLoading: true,
            TotalBytes: 0,
            TransferredBytes: 0,
            TotalFiles: totalFiles,
            TransferredFiles: totalFiles));

        Pair.SetCurrentDownloadStatus(new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal)
        {
            ["__pair_loading_files"] = new()
            {
                DownloadStatus = DownloadStatus.Decompressing,
                TotalFiles = totalFiles,
                TransferredFiles = totalFiles,
                TotalBytes = 0,
                TransferredBytes = 0,
            }
        });

        Mediator.Publish(new RefreshUiMessage());
    }

    private static int ComputeNormalApplyConcurrency()
    {
        var logical = Environment.ProcessorCount;

        if (IsWineRuntime)
            return 1;

        if (logical <= 8) return 2;
        if (logical <= 16) return 4;
        return 6;
    }

    private static int ComputeNormalDownloadConcurrency()
    {
        var logical = Environment.ProcessorCount;
        if (logical <= 8) return 2;
        if (logical <= 16) return 3;
        return 4;
    }

    private static int ComputeStormApplyConcurrency()
    {
        var logical = Environment.ProcessorCount;
        if (IsWineRuntime) return 1;
        if (logical <= 8) return 2;
        return 3;
    }

    private static int ComputeStormDownloadConcurrency()
    {
        var logical = Environment.ProcessorCount;
        if (logical <= 8) return 2;
        return 3;
    }

    private static readonly int NormalApplyConcurrency = ComputeNormalApplyConcurrency();
    private static readonly int StormApplyConcurrency = ComputeStormApplyConcurrency();
    private static readonly int NormalDownloadConcurrency = ComputeNormalDownloadConcurrency();
    private static readonly int StormDownloadConcurrency = ComputeStormDownloadConcurrency();

    private static readonly SemaphoreSlim NormalApplySemaphore = new(NormalApplyConcurrency, NormalApplyConcurrency);
    private static readonly SemaphoreSlim StormApplySemaphore = new(StormApplyConcurrency, StormApplyConcurrency);
    private static readonly SemaphoreSlim NormalDownloadSemaphore = new(NormalDownloadConcurrency, NormalDownloadConcurrency);
    private static readonly SemaphoreSlim StormDownloadSemaphore = new(StormDownloadConcurrency, StormDownloadConcurrency);

    // Smooth redraw spikes globally (room-entry/reconnect bursts). Penumbra redraw is one of the
    // most hitch-prone IPC paths, so keep it strictly one-by-one and add a tiny dispatch gap.
    private const int GlobalRedrawConcurrency = 1;
    private static int CriticalGlobalRedrawSpacingMs => LinuxSmoothMode.ComputeDispatchSpacing(175, 600);
    private static int NonCriticalGlobalRedrawSpacingMs => LinuxSmoothMode.ComputeDispatchSpacing(75, 325);
    private static long _lastGlobalRedrawDispatchTick;
    private static readonly SemaphoreSlim GlobalRedrawSemaphore = new(GlobalRedrawConcurrency, GlobalRedrawConcurrency);
    private static readonly ConcurrentDictionary<int, byte> RedrawObjectIndicesInFlight = new();

    private static int LifecycleApplyDispatchSpacingMs => LinuxSmoothMode.ComputeDispatchSpacing(125, 525);
    private static int ReceiverApplyDispatchSpacingMs(bool lifecycleApply) => LinuxSmoothMode.ComputeReceiverApplyDispatchSpacing(lifecycleApply);
    private static long _lastLifecycleApplyDispatchTick;
    private static long _lastReceiverApplyDispatchTick;
    private static readonly SemaphoreSlim GlobalLifecycleApplySemaphore = new(1, 1);

    private static readonly int GlobalPostApplyRepairConcurrency = IsWineRuntime ? 1 : 2;
    private static readonly SemaphoreSlim GlobalPostApplyRepairSemaphore = new(GlobalPostApplyRepairConcurrency, GlobalPostApplyRepairConcurrency);

    private const int MountMusicTempModPriority = 100;

    private static int WineIncomingDataCoalesceDelayMs => LinuxSmoothMode.ComputeIncomingCoalesceDelay();
    private static readonly int WineApplyPhaseWarnMs = IsWineRuntime ? 350 : 1200;
    private static readonly int WineApplyTotalWarnMs = IsWineRuntime ? 1200 : 5000;
    private string? _lastAttemptedDataHash;
    private readonly object _pairSyncAssetPlanCacheGate = new();
    private PairSyncAssetPlan? _lastResolvedPairSyncAssetPlan;
    private string? _lastResolvedPairSyncPayloadFingerprint;
    private string? _lastAppliedTempModsFingerprint;
    private Dictionary<string, string>? _lastAppliedTempModsSnapshot;
    private string? _lastAppliedMountMusicTempModsFingerprint;
    private Dictionary<string, string>? _lastAppliedMountMusicTempModsSnapshot;
    private string? _activeTempFilesModName;
    private int _activeTempFilesModPriority;
    private CancellationTokenSource? _deferredTempFilesModCleanupCts;
    private volatile bool _disposeRestoreAlreadyQueued;
    private int _vanillaTeardownInProgress;
    private int _visibilityLifecycleGeneration;
    private Task? _vanillaTeardownTask;
    private string? _lastVanillaTeardownPlayerName;
    private int? _lastVanillaTeardownObjectIndex;
    private nint _lastVanillaTeardownPlayerAddress;
    private string? _lastAppliedTransientSupportFingerprint;
    private string? _lastAppliedManipulationFingerprint = null;
    private readonly ConcurrentDictionary<Guid, byte> _lifecycleRedrawApplications = new();
    private long _suppressClassJobRedrawUntilTick = -1;

    private nint _visibleReplayCandidateAddress = nint.Zero;
    private long _visibleReplayCandidateSinceTick = -1;
    private int _visibleReplayStableFrames = 0;
    private long _lastInitialApplyDispatchTick = -1;
    private long _lastVisibleLifecycleReplayTick = -1;
    private const int VisibleReplayStableFramesRequired = 1;
    private const int VisibleReplaySettleMs = 0;
    private const int VisibleReplayDispatchCooldownMs = 0;

    private readonly object _missingCheckGate = new();
    private string? _lastMissingCheckedHash;
    private long _lastMissingCheckedTick;
    private bool _lastMissingCheckHadMissing;
    private int _missingCheckRunning;

    private string? _pendingMissingCheckHash;
    private CharacterData? _pendingMissingCheckData;
    private Guid _pendingMissingCheckBase;

    private string? _lastPostApplyRepairHash;
    private long _lastPostApplyRepairTick;

    private readonly object _postRepairGate = new();
    private CharacterData? _pendingPostApplyRepairData;
    private string? _pendingPostApplyRepairHash;

    private readonly object _refreshUiGate = new();
    private bool _refreshUiPending;
    private long _refreshUiPublishTick;

    private readonly Dictionary<ObjectKind, string> _lastAppliedMoodlesData = [];
    private readonly Dictionary<ObjectKind, string> _lastAppliedHonorificData = [];
    private readonly Dictionary<ObjectKind, string> _lastAppliedPetNamesData = [];

    private long _addressZeroSinceTick = -1;
    private long _zoneRecoveryUntilTick = -1;
    private long _identityDriftSinceTick = -1;
    private const int VisibilityLossGraceMs = 900;
    private const int ZoneVisibilityLossGraceMs = 1400;
    private const int IdentityDriftGraceMs = 900;
    private const int StableVisibilityUpdateIntervalMs = 250;
    private const int ActiveVisibilityUpdateIntervalMs = 33;
    private bool _initialApplyPending;
    private long _otherSyncPollTick;
    private long _nextVisibilityWorkTick;
    private long _otherSyncReleaseCandidateSinceTick = -1;
    private long _otherSyncAcquireCandidateSinceTick = -1;
    private string _otherSyncAcquireCandidateOwner = string.Empty;
    private nint _lastKnownOwnershipAddr = nint.Zero;
    private readonly HashSet<ObjectKind> _pendingOwnedObjectCustomizationRetry = [];
    private long _nextOwnedObjectCustomizationRetryTick = -1;

    private bool? _lastBroadcastYield;
    private string _lastBroadcastOwner = string.Empty;
    private bool _localOtherSyncDecisionYield;
    private string _localOtherSyncDecisionOwner = string.Empty;


    public PairHandler(ILogger<PairHandler> logger, Pair pair,GameObjectHandlerFactory gameObjectHandlerFactory,IpcManager ipcManager, FileDownloadManagerFactory fileDownloadManagerFactory,PluginWarningNotificationService pluginWarningNotificationManager,DalamudUtilService dalamudUtil, 
        IHostApplicationLifetime lifetime,FileCacheManager fileDbManager, MareMediator mediator,PlayerPerformanceService playerPerformanceService,ServerConfigurationManager serverConfigManager,ModPathResolver modPathResolver,PapSanitisationService papSanitizationService) 
        : base(logger, mediator)
    {
        Pair = pair;
        _gameObjectHandlerFactory = gameObjectHandlerFactory;
        _ipcManager = ipcManager;
        _fileDownloadManagerFactory = fileDownloadManagerFactory;
        _downloadManager = null;
        _pluginWarningNotificationManager = pluginWarningNotificationManager;
        _dalamudUtil = dalamudUtil;
        _lifetime = lifetime;
        _fileDbManager = fileDbManager;
        _playerPerformanceService = playerPerformanceService;
        _serverConfigManager = serverConfigManager;
        _modPathResolver = modPathResolver;
        _papSanitisationService = papSanitizationService;
        _visibilityCoordinator = new VisibilityCoordinator(this);
        _otherSyncCoordinator = new OtherSyncCoordinator(this);
        _downloadCoordinator = new DownloadCoordinator(this);
        _applyExecutionCoordinator = new ApplyExecutionCoordinator(this);
        _penumbraCoordinator = new PenumbraCoordinator(this);
        _customizationCoordinator = new CustomizationCoordinator(this);
        _repairCoordinator = new RepairCoordinator(this);
        _transientsCoordinator = new TransientsCoordinator(this);
        _syncWorker = new PairSyncWorker(this);
        _penumbraCollection = Guid.Empty;
        _penumbraCollectionTask = null;
        SubscribeMediatorEvents();
    }

    public bool IsVisible
    {
        get => _isVisible;
        private set
        {
            if (_isVisible != value)
            {
                _isVisible = value;

                if (value)
                    RequestAuthoritativeVisibleApply("IsVisible=true");

                string text = "User Visibility Changed, now: " + (_isVisible ? "Is Visible" : "Is not Visible");
                Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler),EventSeverity.Informational, text)));
                ScheduleRefreshUi();
            }
        }
    }

    public Pair Pair { get; private set; }

    private void RequestAuthoritativeVisibleApply(string reason)
    {
        if (!IsVisible)
            return;

        _cachedData = null;
        _dataReceivedInDowntime = null;
        _initialApplyPending = true;
        _lastAttemptedDataHash = null;
        _forceApplyMods = true;
        _redrawOnNextApplication = true;
        ResetVisibleReplayReadiness();
        _nextVisibilityWorkTick = 0;

        if (Pair.LastReceivedCharacterData is not CharacterData || Pair.IsPaused)
        {
            _syncWorker?.Signal(PairSyncReason.BecameVisible);
            return;
        }

        _ = Task.Run(() => TrySubmitLastReceivedVisibleAuthoritativeApply(reason, PairSyncReason.BecameVisible));
    }

    private bool TrySubmitLastReceivedVisibleAuthoritativeApply(string reason, PairSyncReason syncReason, bool skipIfBusy = true)
    {
        try
        {
            if (!IsVisible || Pair.IsPaused)
                return false;

            if (Pair.LastReceivedCharacterData is not CharacterData sourceData)
                return false;

            var preparedDataMaybe = Pair.PrepareCharacterDataForLocalApply(sourceData.DeepClone());
            if (preparedDataMaybe is not CharacterData preparedData)
                return false;

            var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(preparedData);
            if (skipIfBusy && IsPairSyncBusyForPayload(payloadFingerprint))
            {
                Logger.LogTrace("Skipping immediate visible authoritative apply for {pair}; payload {payload} is already queued/active", Pair.UserData.AliasOrUID, payloadFingerprint);
                return false;
            }

            Logger.LogDebug("Submitting immediate visible authoritative apply for {pair}: reason={reason}, payload={payload}", Pair.UserData.AliasOrUID, reason, payloadFingerprint);
            ApplyCharacterData(Guid.NewGuid(), preparedData, forceApplyCustomization: true, syncReason);
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to submit immediate visible authoritative apply for {pair}: reason={reason}", Pair.UserData.AliasOrUID, reason);
            return false;
        }
    }

    private nint ResolveStablePlayerAddress(nint fallbackAddress = 0)
    {
        var liveAddress = fallbackAddress != nint.Zero ? fallbackAddress : _charaHandler?.Address ?? nint.Zero;

        if (!_dalamudUtil.IsOnFrameworkThread)
            return liveAddress != nint.Zero ? liveAddress : _lastAssignedPlayerAddress;

        var resolved = _dalamudUtil.ResolvePlayerAddress(Pair.Ident, liveAddress);
        if (resolved != nint.Zero)
            return resolved;

        if (_lastAssignedPlayerAddress != nint.Zero
            && _lastAssignedPlayerAddress != liveAddress
            && _dalamudUtil.AddressMatchesPlayerIdentCached(Pair.Ident, _lastAssignedPlayerAddress))
        {
            return _lastAssignedPlayerAddress;
        }

        return nint.Zero;
    }

    private nint ResolveStrictVisiblePlayerAddress(nint fallbackAddress = 0)
    {
        var liveAddress = fallbackAddress != nint.Zero ? fallbackAddress : _charaHandler?.Address ?? nint.Zero;

        if (!_dalamudUtil.IsOnFrameworkThread)
            return liveAddress;

        return _dalamudUtil.ResolvePlayerAddress(Pair.Ident, liveAddress);
    }

    private bool IsExpectedPlayerAddress(nint address)
    {
        if (address == nint.Zero)
            return false;

        if (!_dalamudUtil.IsOnFrameworkThread)
        {
            var liveHandlerAddress = _charaHandler?.Address ?? nint.Zero;

            if (liveHandlerAddress != nint.Zero && liveHandlerAddress == address)
                return true;

            if (_lastAssignedPlayerAddress != nint.Zero && _lastAssignedPlayerAddress == address)
                return true;

            return false;
        }

        return _dalamudUtil.AddressMatchesPlayerIdentCached(Pair.Ident, address);
    }


    private FileDownloadManager GetOrCreateDownloadManager()
    {
        return _downloadManager ??= _fileDownloadManagerFactory.Create();
    }

    private void DisposeDownloadManager(bool runAsync = true)
    {
        var manager = Interlocked.Exchange(ref _downloadManager, null);
        if (manager == null)
            return;

        void DisposeCore()
        {
            try
            {
                manager.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Error disposing download manager for {pair}", Pair.UserData.AliasOrUID);
            }
        }

        if (runAsync && !_lifetime.ApplicationStopping.IsCancellationRequested)
        {
            _ = Task.Run(DisposeCore);
            return;
        }

        DisposeCore();
    }

    public nint PlayerCharacter => ResolveStablePlayerAddress();
    public unsafe uint PlayerCharacterId => PlayerCharacter == nint.Zero
        ? uint.MaxValue
        : ((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)PlayerCharacter)->EntityId;
    public string? PlayerName { get; private set; }
    public string PlayerNameHash => Pair.Ident;
    internal string LastKnownVanillaTeardownPlayerName => !string.IsNullOrWhiteSpace(PlayerName)
        ? PlayerName!
        : _lastVanillaTeardownPlayerName ?? string.Empty;

    internal bool IsObjectKindActive(ObjectKind kind)
    {
        var playerAddress = PlayerCharacter;
        if (playerAddress == nint.Zero)
            return false;

        return kind switch
        {
            ObjectKind.Player => true,
            ObjectKind.Companion => _dalamudUtil.GetCompanionPtr(playerAddress) != nint.Zero,
            ObjectKind.Pet => _dalamudUtil.GetPetPtr(playerAddress) != nint.Zero,
            ObjectKind.MinionOrMount => _dalamudUtil.GetMinionOrMountPtr(playerAddress) != nint.Zero,
            _ => false,
        };
    }

    public void ApplyCharacterData(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization = false)
        => ApplyCharacterData(applicationBase, characterData, forceApplyCustomization, PairSyncReason.IncomingData);

    private void ApplyCharacterData(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization, PairSyncReason reason)
    {
        _disposeRestoreAlreadyQueued = false;
        _syncWorker?.Submit(applicationBase, characterData, forceApplyCustomization, reason);
    }

    public void ForceManipulationReapply()
    {
        _forceApplyMods = true;
        _lastAppliedManipulationFingerprint = null;
        _syncWorker?.Signal(PairSyncReason.ManualReapply);
    }

    private void SubscribeMediatorEvents()
    {
        Mediator.Subscribe<FrameworkUpdateMessage>(this, (_) => FrameworkUpdate());
        Mediator.Subscribe<OtherSyncCurrentStateChangedMessage>(this, (_) => WakeOtherSyncOwnershipCheck());
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) =>
        {
            Logger.LogDebug("Zone switch start: treating {pair} as visibility lost; running targeted vanilla teardown", Pair.UserData.AliasOrUID);
            GoBackToVanillaState(revertLiveCustomizationState: true);
        });

        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (_) =>
        {
            Logger.LogDebug("Zone switch end: waking visibility/replay lifecycle for {pair} without resetting local state", Pair.UserData.AliasOrUID);
            HandleZoneSwitchEnd();
        });

        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) =>
        {
            _penumbraCollection = Guid.Empty;
            _penumbraCollectionTask = null;

            ResetAppliedModTrackingState();
            ResetCollectionBindingState();
            ResetOwnedObjectRetryState();

            if (!IsVisible && _charaHandler != null && Volatile.Read(ref _vanillaTeardownInProgress) == 0)
            {
                PlayerName = string.Empty;
                _charaHandler.Dispose();
                _charaHandler = null;

                Interlocked.Exchange(ref _initializeStarted, 0);
                _initializeTask = null;
            }
        });

        Mediator.Subscribe<ClassJobChangedMessage>(this, msg =>
        {
            if (msg.GameObjectHandler == _charaHandler)
            {
                var suppressUntil = Interlocked.Read(ref _suppressClassJobRedrawUntilTick);
                var nowTick = Environment.TickCount64;
                if (suppressUntil > 0 && unchecked(nowTick - suppressUntil) < 0)
                {
                    Logger.LogTrace("Suppressing apply/redraw-side class-job refresh for {player}", PlayerName);
                    return;
                }

                // A real job/class swap can invalidate the live draw/object binding while our cached
                // data hash is unchanged. Re-apply the existing temp mods and Glamourer state,
                // but do not turn this into a lifecycle Penumbra redraw. Initial visibility is
                // the path that is allowed to request the one loud redraw.
                _forceApplyMods = true;
                MarkInitialApplyRequired(redrawOnNextApplication: false);
            }
        });

        Mediator.Subscribe<DownloadStartedMessage>(this, msg =>
        {
            if (_charaHandler == null || msg.DownloadId != _charaHandler)
                return;

            Pair.SetCurrentDownloadSummary(SummarizeDownloadStatus(msg.DownloadStatus));
            Pair.SetCurrentDownloadStatus(SnapshotStatus(msg.DownloadStatus));
        });

        Mediator.Subscribe<DownloadFinishedMessage>(this, msg =>
        {
            if (_charaHandler == null || msg.DownloadId != _charaHandler)
                return;

            var activePairDownload = _pairDownloadTask;
            if (activePairDownload != null && !activePairDownload.IsCompleted)
            {
                PublishPairSyncLoadingFilesStatus(1);
                return;
            }

            ClearPairSyncTransferStatus();
        });

        Mediator.Subscribe<CombatOrPerformanceEndMessage>(this, _ =>
        {
            if (!IsVisible || _dataReceivedInDowntime == null)
                return;

            ApplyCharacterData(
                _dataReceivedInDowntime.ApplicationId,
                _dataReceivedInDowntime.CharacterData,
                _dataReceivedInDowntime.Forced);
            _dataReceivedInDowntime = null;
        });

        Mediator.Subscribe<CombatOrPerformanceStartMessage>(this, _ =>
        {
            _dataReceivedInDowntime = null;
            _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
            _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();
        });

        Mediator.Subscribe<HonorificReadyMessage>(this, async _ =>
        {
            if (string.IsNullOrEmpty(_cachedData?.HonorificData) || PlayerCharacter == nint.Zero)
                return;

            Logger.LogTrace("Reapplying Honorific data for {this}", this);
            await _ipcManager.Honorific.SetTitleAsync(PlayerCharacter, _cachedData.HonorificData).ConfigureAwait(false);
        });

        Mediator.Subscribe<PetNamesReadyMessage>(this, async _ =>
        {
            if (string.IsNullOrEmpty(_cachedData?.PetNamesData) || PlayerCharacter == nint.Zero)
                return;

            Logger.LogTrace("Reapplying Pet Names data for {this}", this);
            await _ipcManager.PetNames.SetPlayerData(PlayerCharacter, _cachedData.PetNamesData).ConfigureAwait(false);
        });

        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, HandlePenumbraResourceLoadForCompactIndicators);
        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, HandlePenumbraResourceLoadForSummonedActorBinding);
        Mediator.Subscribe<MissingFileRepairCompletedMessage>(this, HandleMissingFileRepairCompleted);
    }

    private void HandleMissingFileRepairCompleted(MissingFileRepairCompletedMessage msg)
    {
        if (msg == null || !IsVisible || Pair.IsPaused)
            return;

        if (!string.Equals(msg.SenderUid, Pair.UserData.UID, StringComparison.Ordinal))
            return;

        var expectedDataHash = Pair.LastReceivedCharacterData?.DataHash.Value ?? _cachedData?.DataHash.Value ?? string.Empty;
        if (!string.IsNullOrWhiteSpace(msg.DataHash)
            && !string.IsNullOrWhiteSpace(expectedDataHash)
            && !string.Equals(msg.DataHash, expectedDataHash, StringComparison.Ordinal))
        {
            Logger.LogDebug(
                "Ignoring stale mesh missing-hash repair completion for {pair}: completionHash={completionHash}, currentHash={currentHash}",
                Pair.UserData.AliasOrUID,
                msg.DataHash,
                expectedDataHash);
            return;
        }

        var requestedHashes = (msg.Hashes ?? Array.Empty<string>())
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (requestedHashes.Count == 0)
            return;

        var failedHashes = (msg.FailedHashes ?? Array.Empty<string>())
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var stillMissing = requestedHashes
            .Where(hash => failedHashes.Contains(hash) || !HasUsableCachedFileForHash(hash))
            .ToList();

        // This is the missing half of the retry loop: the sender has now run its upload/share
        // path for exactly the hashes we were missing, so allow the receiver-side download/apply
        // worker to re-check local cache and CDN state instead of waiting for a reconnect.
        // Also clear the per-hash repair-attempt cap for the still-missing files; otherwise the
        // retry can wake correctly but never actually ask the sender to upload/share those hashes again.
        _downloadManager?.ResetCentralFileRepairAttempts(stillMissing);
        _hasRetriedAfterMissingDownload = false;

        Logger.LogInformation(
            "Mesh missing-hash repair completed for {pair}: requested={requested}, stillMissingOrFailed={missing}; waking receiver retry",
            Pair.UserData.AliasOrUID,
            requestedHashes.Count,
            stillMissing.Count);

        _syncWorker?.Signal(PairSyncReason.IncomingData);
        ScheduleMissingFileSelfHealRetry(Pair.LastReceivedCharacterData ?? _cachedData, requestedHashes, immediate: stillMissing.Count == 0);
    }

    private void ScheduleMissingFileSelfHealRetry(CharacterData? characterData, IEnumerable<string?> hashes, bool immediate = false)
    {
        if (characterData == null || !IsVisible || Pair.IsPaused)
            return;

        var hashList = (hashes ?? Array.Empty<string?>())
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h!.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (hashList.Count == 0)
            return;

        lock (_missingFileSelfHealGate)
        {
            _pendingMissingFileSelfHealData = characterData.DeepClone();
            _pendingMissingFileSelfHealHashes = hashList;
        }

        if (Interlocked.CompareExchange(ref _missingFileSelfHealRunning, 1, 0) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    CharacterData? data;
                    List<string>? pendingHashes;
                    lock (_missingFileSelfHealGate)
                    {
                        data = _pendingMissingFileSelfHealData;
                        pendingHashes = _pendingMissingFileSelfHealHashes;
                        _pendingMissingFileSelfHealData = null;
                        _pendingMissingFileSelfHealHashes = null;
                    }

                    if (data == null || pendingHashes == null || pendingHashes.Count == 0)
                        return;

                    if (!immediate)
                        await Task.Delay(IsWineRuntime ? 3500 : 1500).ConfigureAwait(false);
                    immediate = false;

                    if (!IsVisible || Pair.IsPaused)
                        return;

                    var stillMissing = pendingHashes
                        .Where(hash => !HasUsableCachedFileForHash(hash))
                        .ToList();

                    _downloadManager?.ResetCentralFileRepairAttempts(stillMissing.Count > 0 ? stillMissing : pendingHashes);
                    _hasRetriedAfterMissingDownload = false;
                    _hasRetriedAfterMissingAtApply = false;
                    _forceApplyMods = true;

                    Logger.LogInformation(
                        "Missing-file self-heal retry for {pair}: requested={requested}, stillMissing={missing}; re-submitting authoritative visible apply",
                        Pair.UserData.AliasOrUID,
                        pendingHashes.Count,
                        stillMissing.Count);

                    ApplyCharacterData(Guid.NewGuid(), data.DeepClone(), forceApplyCustomization: true);

                    lock (_missingFileSelfHealGate)
                    {
                        if (_pendingMissingFileSelfHealData == null || _pendingMissingFileSelfHealHashes == null || _pendingMissingFileSelfHealHashes.Count == 0)
                            return;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Missing-file self-heal retry failed for {pair}", Pair.UserData.AliasOrUID);
            }
            finally
            {
                Interlocked.Exchange(ref _missingFileSelfHealRunning, 0);

                CharacterData? queuedData = null;
                List<string>? queuedHashes = null;
                lock (_missingFileSelfHealGate)
                {
                    if (_pendingMissingFileSelfHealData != null && _pendingMissingFileSelfHealHashes is { Count: > 0 })
                    {
                        queuedData = _pendingMissingFileSelfHealData;
                        queuedHashes = _pendingMissingFileSelfHealHashes;
                        _pendingMissingFileSelfHealData = null;
                        _pendingMissingFileSelfHealHashes = null;
                    }
                }

                if (queuedData != null && queuedHashes != null)
                    ScheduleMissingFileSelfHealRetry(queuedData, queuedHashes, immediate: false);
            }
        });
    }

    private bool HasUsableCachedFileForHash(string? hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return false;

        try
        {
            var entry = _fileDbManager.GetFileCacheByHash(hash.Trim().ToUpperInvariant());
            if (entry == null || string.IsNullOrWhiteSpace(entry.ResolvedFilepath))
                return false;

            var fi = new FileInfo(entry.ResolvedFilepath);
            var usable = fi.Exists && fi.Length > 0;
            if (usable)
                FileDownloadManager.RegisterSessionKnownPresentHash(hash);

            return usable;
        }
        catch
        {
            return false;
        }
    }

    private void HandlePenumbraResourceLoadForSummonedActorBinding(PenumbraResourceLoadMessage msg)
    {
        if (!_dalamudUtil.IsOnFrameworkThread)
        {
            _ = _dalamudUtil.RunOnFrameworkThread(() => HandlePenumbraResourceLoadForSummonedActorBinding(msg));
            return;
        }

        if (!IsVisible || Pair.IsPaused || _cachedData == null || msg.GameObject == IntPtr.Zero)
            return;

        var actorAddress = (nint)msg.GameObject;
        var gamePath = NormalizeLiveIndicatorGamePath(msg.GamePath);
        if (string.IsNullOrWhiteSpace(gamePath) || !HasSummonedActorPayloadForGamePath(_cachedData, gamePath))
            return;

        var playerAddress = ResolveStablePlayerAddress();
        if (playerAddress == nint.Zero || actorAddress == playerAddress)
            return;

        if (!_dalamudUtil.IsOwnedActorOfPlayer(playerAddress, actorAddress))
            return;

        var actorRedrawKey = TryBuildSummonedActorRedrawCooldownKey(actorAddress, gamePath);
        if (string.IsNullOrWhiteSpace(actorRedrawKey))
            return;

        var nowTick = Environment.TickCount64;
        if (_nextSummonedActorBindingAttemptTick.TryGetValue(actorAddress, out var nextTick)
            && unchecked(nowTick - nextTick) < 0)
        {
            return;
        }

        if (!_summonedActorBindingInFlight.TryAdd(actorAddress, 0))
            return;

        // Keep binding eager. The redraw cooldown is checked only after a successful
        // bind so actor replacement/redraw storms do not stop us assigning the
        // collection to the newest owned actor address.
        _nextSummonedActorBindingAttemptTick[actorAddress] = nowTick + 100;

        var applicationId = Guid.NewGuid();
        _ = Task.Run(async () =>
        {
            var bound = false;
            try
            {
                if (!IsVisible || Pair.IsPaused)
                    return;

                bound = await EnsureSummonedActorPenumbraCollectionBindingAsync(applicationId, actorAddress, "summoned actor").ConfigureAwait(false);
                if (!bound)
                    return;

                var redrawTick = Environment.TickCount64;
                if (_summonedActorRedrawCooldownByKey.TryGetValue(actorRedrawKey, out var redrawNotBefore)
                    && unchecked(redrawTick - redrawNotBefore) < 0)
                {
                    Logger.LogTrace("[{applicationId}] Bound {pair} summoned actor {addr:X} without redraw because scope {scope} already redrew recently",
                        applicationId, Pair.UserData.AliasOrUID, actorAddress, actorRedrawKey);
                    return;
                }

                // One redraw per summon scope is enough to force the actor onto the
                // pair temp collection. Further resource-load callbacks may keep
                // binding newer actor addresses, but they must not keep redrawing and
                // flashing the actor while the summon settles.
                _summonedActorRedrawCooldownByKey[actorRedrawKey] = redrawTick + 1000;

                if (IsVisible && !Pair.IsPaused)
                    Mediator.Publish(new PenumbraRedrawAddressMessage(actorAddress));
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[{applicationId}] Failed to bind/redraw summoned actor {addr:X} for {pair}", applicationId, actorAddress, Pair.UserData.AliasOrUID);
            }
            finally
            {
                _summonedActorBindingInFlight.TryRemove(actorAddress, out _);
                _nextSummonedActorBindingAttemptTick[actorAddress] = Environment.TickCount64 + (bound ? 250 : 100);
            }
        });
    }


    private string TryBuildSummonedActorRedrawCooldownKey(nint actorAddress, string gamePath)
    {
        var scope = GetSummonedActorRedrawScope(gamePath);
        if (!string.IsNullOrWhiteSpace(scope) && !string.Equals(scope, "unknown", StringComparison.OrdinalIgnoreCase))
            return actorAddress == nint.Zero ? scope : $"{scope}|actor:{actorAddress:X}";

        return actorAddress == nint.Zero ? string.Empty : $"actor:{actorAddress:X}";
    }

    private static string GetSummonedActorRedrawScope(string gamePath)
    {
        var normalized = NormalizeLiveIndicatorGamePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return "unknown";

        var parts = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length >= 3
            && string.Equals(parts[0], "chara", StringComparison.OrdinalIgnoreCase)
            && (string.Equals(parts[1], "monster", StringComparison.OrdinalIgnoreCase)
                || string.Equals(parts[1], "demihuman", StringComparison.OrdinalIgnoreCase)
                || string.Equals(parts[1], "weapon", StringComparison.OrdinalIgnoreCase)))
        {
            return string.Join("/", parts.Take(3));
        }

        if (parts.Length >= 2
            && (string.Equals(parts[0], "vfx", StringComparison.OrdinalIgnoreCase)
                || string.Equals(parts[0], "sound", StringComparison.OrdinalIgnoreCase)))
        {
            return parts[0];
        }

        return normalized;
    }


    private static bool HasSummonedActorPayloadForGamePath(CharacterData data, string gamePath)
    {
        if (data?.FileReplacements == null || string.IsNullOrWhiteSpace(gamePath))
            return false;

        foreach (var objectFiles in data.FileReplacements)
        {
            foreach (var replacement in objectFiles.Value ?? [])
            {
                foreach (var candidate in replacement.GamePaths ?? [])
                {
                    if (string.Equals(NormalizeLiveIndicatorGamePath(candidate), gamePath, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }
        }

        return false;
    }

    private void HandlePenumbraResourceLoadForCompactIndicators(PenumbraResourceLoadMessage msg)
    {
        if (!_dalamudUtil.IsOnFrameworkThread)
        {
            _ = _dalamudUtil.RunOnFrameworkThread(() => HandlePenumbraResourceLoadForCompactIndicators(msg));
            return;
        }

        if (!IsVisible || Pair.IsPaused)
            return;

        var gamePath = NormalizeLiveIndicatorGamePath(msg.GamePath);
        var filePath = NormalizeLiveIndicatorGamePath(msg.FilePath);
        var soundPath = IsLiveSoundIndicatorPath(gamePath) ? gamePath : IsLiveSoundIndicatorPath(filePath) ? filePath : string.Empty;
        if (string.IsNullOrWhiteSpace(soundPath))
            return;

        var isPairActorLoad = msg.GameObject != IntPtr.Zero && IsPairResourceLoadAddress((nint)msg.GameObject);
        var isAppliedMountMusicLoad = IsAppliedMountMusicTempModPath(gamePath)
            || IsAppliedMountMusicTempModPath(filePath)
            || IsAppliedMountMusicTempModPath(soundPath);

        // Normal actor-bound SCDs still have to belong to this pair. Default-collection mount music
        // can arrive without a useful pair actor pointer, so allow it only when it matches this pair's
        // currently applied mount-music temp-mod snapshot. The lifetime is still the known-good
        // incoming user-data boundary: observed SCD keeps the icon; next payload clears it unless
        // another relevant SCD was observed for this pair.
        if (!isPairActorLoad && !isAppliedMountMusicLoad)
            return;

        var longLived = IsLongLivedLiveIndicatorPath(gamePath)
            || IsLongLivedLiveIndicatorPath(filePath)
            || isAppliedMountMusicLoad;

        Pair.MarkActiveSyncResource(vfx: false, sound: true, longLived, soundPath, isPairActorLoad ? (nint)msg.GameObject : nint.Zero);
    }

    private bool IsCurrentAppliedRavaSyncResourceLoad(string? gamePath, string? resolvedFilePath)
    {
        var normalizedGamePath = NormalizeLiveIndicatorGamePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalizedGamePath))
            return false;

        // Penumbra's resource-load callback is authoritative that a replacement is being loaded,
        // but the resolved file path it reports is not stable enough to exact-match against our
        // temp-mod cache path on every machine/redirect style. For the UI indicator we only need
        // to know that the live game path belongs to this pair's currently applied RavaSync payload.
        // Validation below then keeps it alive while Penumbra still reports the resource on the object.
        if (AppliedTempModGamePathMatches(_lastAppliedTempModsSnapshot, normalizedGamePath))
            return true;

        if (AppliedTempModGamePathMatches(_lastAppliedMountMusicTempModsSnapshot, normalizedGamePath))
            return true;

        return false;
    }

    private static bool AppliedTempModGamePathMatches(IReadOnlyDictionary<string, string>? appliedTempMods, string normalizedGamePath)
    {
        if (appliedTempMods == null || appliedTempMods.Count == 0)
            return false;

        if (appliedTempMods.ContainsKey(normalizedGamePath))
            return true;

        // Defensive: most snapshots are already normalized, but older/current call sites can keep
        // original slash/case formatting. Do the cheap precise check first, then fall back here.
        return appliedTempMods.Keys.Any(path => string.Equals(NormalizeLiveIndicatorGamePath(path), normalizedGamePath, StringComparison.OrdinalIgnoreCase));
    }

    private bool IsAppliedMountMusicTempModPath(string? path)
        => AppliedTempModKeyOrValueMatches(_lastAppliedMountMusicTempModsSnapshot, path);

    private static bool AppliedTempModKeyOrValueMatches(IReadOnlyDictionary<string, string>? appliedTempMods, string? path)
    {
        if (appliedTempMods == null || appliedTempMods.Count == 0 || string.IsNullOrWhiteSpace(path))
            return false;

        var normalizedGamePath = NormalizeLiveIndicatorGamePath(path);
        var normalizedPhysicalPath = NormalizePhysicalLiveIndicatorPath(path);

        foreach (var kvp in appliedTempMods)
        {
            if (!string.IsNullOrWhiteSpace(normalizedGamePath)
                && string.Equals(NormalizeLiveIndicatorGamePath(kvp.Key), normalizedGamePath, StringComparison.OrdinalIgnoreCase))
                return true;

            if (!string.IsNullOrWhiteSpace(normalizedGamePath)
                && string.Equals(NormalizeLiveIndicatorGamePath(kvp.Value), normalizedGamePath, StringComparison.OrdinalIgnoreCase))
                return true;

            if (!string.IsNullOrWhiteSpace(normalizedPhysicalPath)
                && string.Equals(NormalizePhysicalLiveIndicatorPath(kvp.Value), normalizedPhysicalPath, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static string NormalizePhysicalLiveIndicatorPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        var trimmed = path.Trim();
        if (!Path.IsPathRooted(trimmed))
            return string.Empty;

        return NormalizeLiveIndicatorFilePath(trimmed).ToLowerInvariant();
    }

    private static string NormalizeLiveIndicatorGamePath(string? path)
    {
        return (path ?? string.Empty)
            .Replace('\\', '/')
            .Trim()
            .TrimStart('/')
            .ToLowerInvariant();
    }

    private static string NormalizeLiveIndicatorFilePath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        try
        {
            return Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Replace('\\', '/');
        }
        catch
        {
            return path.Trim().TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Replace('\\', '/');
        }
    }

    private static bool IsLiveSoundIndicatorPath(string? path)
    {
        return string.Equals(Path.GetExtension(NormalizeLiveIndicatorGamePath(path)), ".scd", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsLiveVfxIndicatorPath(string? path)
    {
        return string.Equals(Path.GetExtension(NormalizeLiveIndicatorGamePath(path)), ".avfx", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsLongLivedLiveIndicatorPath(string? path)
    {
        var normalized = NormalizeLiveIndicatorGamePath(path);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return PairApplyUtilities.IsMountMusicGamePath(normalized)
            || normalized.Contains("loop", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("bgm_ride", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("music/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/music/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("sound/bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/sound/bgm/", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsPairResourceLoadAddress(nint address)
    {
        if (address == nint.Zero)
            return false;

        if (_charaHandler?.Address == address || _lastAssignedPlayerAddress == address)
            return true;

        if (!_dalamudUtil.IsOnFrameworkThread)
            return false;

        var playerAddress = ResolveStablePlayerAddress();
        if (playerAddress == nint.Zero)
            return false;

        if (address == playerAddress)
            return true;

        return _dalamudUtil.GetCompanionPtr(playerAddress) == address
            || _dalamudUtil.GetPetPtr(playerAddress) == address
            || _dalamudUtil.GetMinionOrMountPtr(playerAddress) == address
            || _dalamudUtil.IsOwnedActorOfPlayer(playerAddress, address);
    }

    private void ValidateActiveSyncIndicators()
    {
        Pair.PruneExpiredActiveSyncIndicators();

        var activeIndicators = Pair.SnapshotActiveSyncResourceIndicators();
        if (activeIndicators.Count == 0 || !IsVisible || Pair.IsPaused)
            return;

        var now = Environment.TickCount64;
        if (unchecked(_nextActiveSyncIndicatorValidationTick - now) > 0)
            return;

        if (_activeSyncIndicatorValidationTask != null && !_activeSyncIndicatorValidationTask.IsCompleted)
            return;

        var validationIntervalMs = IsWineRuntime
            ? LinuxSmoothMode.ComputeActiveIndicatorValidationInterval(1000)
            : 1000;
        _nextActiveSyncIndicatorValidationTick = unchecked(now + validationIntervalMs);
        _activeSyncIndicatorValidationTask = ValidateActiveSyncIndicatorsAsync(activeIndicators);
    }

    private async Task ValidateActiveSyncIndicatorsAsync(IReadOnlyList<Pair.ActiveSyncResourceIndicator> activeIndicators)
    {
        try
        {
            foreach (var group in activeIndicators.Where(i => i.Address != nint.Zero).GroupBy(i => i.Address).ToArray())
            {
                var currentResources = await _ipcManager.Penumbra.GetGameObjectResourcePathsAsync(Logger, group.Key).ConfigureAwait(false);
                var liveGamePaths = currentResources == null
                    ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    : FlattenLiveIndicatorGamePaths(currentResources);

                Pair.ReconcileActiveSyncResourcesForAddress(group.Key, liveGamePaths);
            }
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, "Failed to validate active sync indicators for {pair}", Pair.UserData.AliasOrUID);
        }
    }

    private static HashSet<string> FlattenLiveIndicatorGamePaths(Dictionary<string, HashSet<string>> resources)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in resources)
        {
            var keyPath = NormalizeLiveIndicatorGamePath(item.Key);
            if (!string.IsNullOrWhiteSpace(keyPath))
                output.Add(keyPath);

            var paths = item.Value;
            if (paths == null)
                continue;

            foreach (var path in paths)
            {
                var normalized = NormalizeLiveIndicatorGamePath(path);
                if (!string.IsNullOrWhiteSpace(normalized))
                    output.Add(normalized);
            }
        }

        return output;
    }

    private void FrameworkUpdate()
    {
        LinuxSmoothMode.RecordFrameworkTick();
        FlushScheduledRefreshUi();
        ValidateActiveSyncIndicators();
        _visibilityCoordinator.FrameworkUpdate();
    }

    private Task InitializeAsync(string name, nint knownAddress = 0) => _visibilityCoordinator.InitializeAsync(name, knownAddress);

    private void HandleZoneSwitchEnd() => _visibilityCoordinator.HandleZoneSwitchEnd();

    private void ResetToUninitializedState(bool revertLiveCustomizationState = true) => _visibilityCoordinator.ResetToUninitializedState(revertLiveCustomizationState);

    public void GoBackToVanillaState(bool revertLiveCustomizationState = true, bool waitForCompletion = false)
    {
        CancelPairSyncWork();
        ResetToUninitializedState(revertLiveCustomizationState);

        if (!waitForCompletion || _dalamudUtil.IsOnFrameworkThread)
            return;

        try
        {
            _vanillaTeardownTask?.Wait(TimeSpan.FromSeconds(4));
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Timed out or failed while waiting for vanilla teardown for {pair}", Pair.UserData.AliasOrUID);
        }
    }

    private void WakeOtherSyncOwnershipCheck()
        => _visibilityCoordinator.WakeOtherSyncOwnershipCheck();

    private Task<(bool Bound, bool Reassigned)> EnsurePenumbraCollectionBindingAsync(Guid applicationId)
        => _penumbraCoordinator.EnsurePenumbraCollectionBindingAsync(applicationId);

    private Task<bool> EnsureOwnedObjectPenumbraCollectionBindingAsync(Guid applicationId, GameObjectHandler handler, ObjectKind objectKind)
        => _penumbraCoordinator.EnsureOwnedObjectPenumbraCollectionBindingAsync(applicationId, handler, objectKind);

    private Task<bool> EnsureSummonedActorPenumbraCollectionBindingAsync(Guid applicationId, nint actorAddress, string reason)
        => _penumbraCoordinator.EnsureSummonedActorPenumbraCollectionBindingAsync(applicationId, actorAddress, reason);

    private Task EnsurePenumbraCollectionAsync() => _penumbraCoordinator.EnsurePenumbraCollectionAsync();

    private Task RemovePenumbraCollectionAsync(Guid applicationId) => _penumbraCoordinator.RemovePenumbraCollectionAsync(applicationId);

    private Task<bool> OnePassRedrawAsync(Guid applicationId, CancellationToken token, bool criticalRedraw = false)
        => _transientsCoordinator.OnePassRedrawAsync(applicationId, token, criticalRedraw);

    private void BroadcastLocalOtherSyncYieldState(bool yieldToOtherSync, string owner)
        => _otherSyncCoordinator.BroadcastLocalOtherSyncYieldState(yieldToOtherSync, owner);

    public void ReclaimFromOtherSync(bool requestApplyIfPossible, bool treatAsFirstVisible)
        => _otherSyncCoordinator.ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible);

    private void HandleOtherSyncReleased(bool requestApplyIfPossible)
        => _otherSyncCoordinator.HandleOtherSyncReleased(requestApplyIfPossible);

    private void EnterYieldedState(string owner) => _otherSyncCoordinator.EnterYieldedState(owner);

    private Task<PairSyncCommitResult> DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
        => _downloadCoordinator.DownloadAndApplyCharacterAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, assetPlan, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken);

    private bool HasAnyMissingCacheFiles(Guid applicationBase, CharacterData characterData)
        => _repairCoordinator.HasAnyMissingCacheFiles(applicationBase, characterData);

    public void RequestManualFileRepair() => _repairCoordinator.RequestManualFileRepair();

    private Task ManualVerifyAndRepairAsync(Guid applicationBase, CharacterData charaData, CancellationToken token, bool verifyFileHashes = true, bool publishEvents = true)
        => _repairCoordinator.ManualVerifyAndRepairAsync(applicationBase, charaData, token, verifyFileHashes, publishEvents);

    private bool TryGetRecentMissingCheck(string dataHash, out bool hadMissing)
        => _repairCoordinator.TryGetRecentMissingCheck(dataHash, out hadMissing);

    private void ScheduleMissingCheck(Guid applicationBase, CharacterData characterData)
        => _repairCoordinator.ScheduleMissingCheck(applicationBase, characterData);

    private void RequestPostApplyRepair(CharacterData appliedData) => _repairCoordinator.RequestPostApplyRepair(appliedData);

    private void QueueOwnedObjectCustomizationRetry(ObjectKind objectKind)
        => _customizationCoordinator.QueueOwnedObjectCustomizationRetry(objectKind);

    private bool HasPendingOwnedObjectCustomizationPayload(CharacterData charaData, ObjectKind objectKind)
        => _customizationCoordinator.HasPendingOwnedObjectCustomizationPayload(charaData, objectKind);

    private nint ResolveOwnedObjectAddressForRetry(nint playerAddress, ObjectKind objectKind)
        => _customizationCoordinator.ResolveOwnedObjectAddressForRetry(playerAddress, objectKind);

    private void ProcessPendingOwnedObjectCustomizationRetry(long nowTick)
        => _customizationCoordinator.ProcessPendingOwnedObjectCustomizationRetry(nowTick);

    private Task<bool> ApplyCustomizationDataAsync(Guid applicationId, KeyValuePair<ObjectKind, HashSet<PlayerChanges>> changes, CharacterData charaData, bool allowPlayerRedraw, bool forceLightweightMetadataReapply, bool awaitPlayerGlamourerApply, bool waitForPlayerGlamourerDrawSettle, ApplyFlag glamourerApplyFlags, CancellationToken token)
        => _customizationCoordinator.ApplyCustomizationDataAsync(applicationId, changes, charaData, allowPlayerRedraw, forceLightweightMetadataReapply, awaitPlayerGlamourerApply, waitForPlayerGlamourerDrawSettle, glamourerApplyFlags, token);

    private Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken, nint addressOverride = 0, IReadOnlyDictionary<ObjectKind, Guid?>? customizeIdSnapshot = null)
        => _customizationCoordinator.RevertCustomizationDataAsync(objectKind, name, applicationId, cancelToken, addressOverride, customizeIdSnapshot);

    internal bool IsPairSyncBusyForPayload(string payloadFingerprint) => _syncWorker?.IsBusyForPayload(payloadFingerprint) == true;

    private void CancelPairSyncWork(bool clearDesired = false) => _syncWorker?.CancelActiveWork(clearDesired);
    private void ResetPairSyncPipelineState()
    {
        ClearPairSyncAssetPlanCache();
        _syncWorker?.ResetPipelineState();
    }

    private Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken token, bool authoritativeCommit = true, bool suppressNonLifecycleRedraw = false)
        => _applyExecutionCoordinator.ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, assetPlan, downloadedAny, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, token, authoritativeCommit, suppressNonLifecycleRedraw);
}
