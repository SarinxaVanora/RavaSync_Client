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
    private readonly ObjectIndexCleanupService _objectIndexCleanupService;
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
    private int _manualRepairRunning = 0;
    private int? _lastAssignedObjectIndex = null;
    private nint _lastAssignedPlayerAddress = nint.Zero;
    private DateTime _lastAssignedCollectionAssignUtc = DateTime.MinValue;
    private DateTime _nextTempCollectionRetryNotBeforeUtc = DateTime.MinValue;
    private long _lastApplyCompletedTick;
    private static int ComputeNormalApplyConcurrency()
    {
        var logical = Environment.ProcessorCount;

        // Room entry already funnels the truly framework-sensitive Penumbra IPC through
        // IpcCallerPenumbra's single paced gate. Keep pair commits parallel enough that
        // cached/no-download room applies do not feel like a long one-by-one queue.
        if (logical <= 8) return 2;
        if (logical <= 16) return 4;
        return 6;
    }

    private static readonly int NormalApplyConcurrency = ComputeNormalApplyConcurrency();
    private static readonly int StormApplyConcurrency = Math.Max(1, NormalApplyConcurrency / 2);

    private static readonly SemaphoreSlim NormalApplySemaphore = new(NormalApplyConcurrency, NormalApplyConcurrency);
    private static readonly SemaphoreSlim StormApplySemaphore = new(StormApplyConcurrency, StormApplyConcurrency);

    // Smooth redraw spikes globally (room-entry bursts) without hard-stalling the entire room.
    // Per-actor redraw coalescing still prevents duplicate redraws for the same object.
    private static readonly SemaphoreSlim GlobalRedrawSemaphore = new(2, 2);
    private static readonly ConcurrentDictionary<int, byte> RedrawObjectIndicesInFlight = new();

    private static readonly SemaphoreSlim GlobalPostApplyRepairSemaphore = new(2, 2);
    private string? _lastAttemptedDataHash;
    private string? _lastAppliedTempModsFingerprint;
    private Dictionary<string, string>? _lastAppliedTempModsSnapshot;
    private string? _activeTempFilesModName;
    private int _activeTempFilesModPriority;
    private CancellationTokenSource? _deferredTempFilesModCleanupCts;
    private volatile bool _disposeRestoreAlreadyQueued;
    private string? _lastAppliedTransientSupportFingerprint;
    private string? _lastAppliedManipulationFingerprint = null;
    private readonly ConcurrentDictionary<Guid, byte> _lifecycleRedrawApplications = new();
    private long _suppressClassJobRedrawUntilTick = -1;

    private nint _visibleReplayCandidateAddress = nint.Zero;
    private long _visibleReplayCandidateSinceTick = -1;
    private int _visibleReplayStableFrames = 0;
    private long _lastInitialApplyDispatchTick = -1;
    private const int VisibleReplayStableFramesRequired = 3;
    private const int VisibleReplaySettleMs = 175;
    private const int VisibleReplayDispatchCooldownMs = 500;

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
    private const int StableVisibilityUpdateIntervalMs = 125;
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
        IHostApplicationLifetime lifetime,FileCacheManager fileDbManager, MareMediator mediator,PlayerPerformanceService playerPerformanceService,ServerConfigurationManager serverConfigManager,ModPathResolver modPathResolver,ObjectIndexCleanupService objectIndexCleanupService,PapSanitisationService papSanitizationService) 
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
        _objectIndexCleanupService = objectIndexCleanupService;
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
        LastAppliedDataBytes = -1;

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
                {
                    _initialApplyPending = true;
                    _lastAttemptedDataHash = null;
                    _redrawOnNextApplication = true;
                    ResetVisibleReplayReadiness();
                    _nextVisibilityWorkTick = 0;
                    _syncWorker?.Signal(PairSyncReason.BecameVisible);
                }

                string text = "User Visibility Changed, now: " + (_isVisible ? "Is Visible" : "Is not Visible");
                Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler),EventSeverity.Informational, text)));
                ScheduleRefreshUi();
            }
        }
    }

    public long LastAppliedDataBytes { get; private set; }
    public Pair Pair { get; private set; }

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
            && _dalamudUtil.AddressMatchesPlayerIdent(Pair.Ident, _lastAssignedPlayerAddress))
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

        return _dalamudUtil.AddressMatchesPlayerIdent(Pair.Ident, address);
    }

    private FileDownloadManager GetOrCreateDownloadManager()
    {
        return _downloadManager ??= _fileDownloadManagerFactory.Create();
    }

    private void DisposeDownloadManager()
    {
        var manager = Interlocked.Exchange(ref _downloadManager, null);
        if (manager == null)
            return;

        try
        {
            manager.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, "Error disposing download manager for {pair}", Pair.UserData.AliasOrUID);
        }
    }

    private void CleanupOldAssignedIndexIfNeeded(int? objectIndex, Guid applicationId)
    {
        if (!Pair.AutoPausedByOtherSync && objectIndex.HasValue && objectIndex.Value >= 0)
        {
            _ = _objectIndexCleanupService.CleanupIfNotOwnedByIdentAsync(objectIndex.Value, Pair.Ident, applicationId);
        }
    }

    public nint PlayerCharacter => ResolveStablePlayerAddress();
    public unsafe uint PlayerCharacterId => PlayerCharacter == nint.Zero
        ? uint.MaxValue
        : ((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)PlayerCharacter)->EntityId;
    public string? PlayerName { get; private set; }
    public string PlayerNameHash => Pair.Ident;

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
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) =>
        {
            Logger.LogDebug("Zone switch start: tearing down live RavaSync state for {pair}", Pair.UserData.AliasOrUID);
            ResetToUninitializedState(revertLiveCustomizationState: false);
        });

        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (_) =>
        {
            Logger.LogDebug("Zone switch end: ensuring live RavaSync state is torn down for {pair}", Pair.UserData.AliasOrUID);
            ResetToUninitializedState(revertLiveCustomizationState: false);
        });

        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) =>
        {
            _penumbraCollection = Guid.Empty;
            _penumbraCollectionTask = null;

            ResetAppliedModTrackingState();
            ResetCollectionBindingState();
            ResetOwnedObjectRetryState();

            if (!IsVisible && _charaHandler != null)
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

            Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
            Pair.SetCurrentDownloadStatus(null);
            Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
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
    }

    private void FrameworkUpdate()
    {
        FlushScheduledRefreshUi();
        _visibilityCoordinator.FrameworkUpdate();
    }

    private Task InitializeAsync(string name) => _visibilityCoordinator.InitializeAsync(name);

    private void ResetToUninitializedState(bool revertLiveCustomizationState = true) => _visibilityCoordinator.ResetToUninitializedState(revertLiveCustomizationState);

    private Task<(bool Bound, bool Reassigned)> EnsurePenumbraCollectionBindingAsync(Guid applicationId)
        => _penumbraCoordinator.EnsurePenumbraCollectionBindingAsync(applicationId);

    private Task EnsurePenumbraCollectionAsync() => _penumbraCoordinator.EnsurePenumbraCollectionAsync();

    private Task RemovePenumbraCollectionAsync(Guid applicationId) => _penumbraCoordinator.RemovePenumbraCollectionAsync(applicationId);

    private Task<bool> OnePassRedrawAsync(Guid applicationId, CancellationToken token, bool criticalRedraw = false)
        => _transientsCoordinator.OnePassRedrawAsync(applicationId, token, criticalRedraw);

    private void BroadcastLocalOtherSyncYieldState(bool yieldToOtherSync, string owner)
        => _otherSyncCoordinator.BroadcastLocalOtherSyncYieldState(yieldToOtherSync, owner);

    public void ReclaimFromOtherSync(bool requestApplyIfPossible, bool treatAsFirstVisible)
        => _otherSyncCoordinator.ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible);

    public void EnterPausedVanillaState()
    {
        CancelPairSyncWork();

        var applicationId = Guid.NewGuid();
        var name = PlayerName;
        var playerAddress = ResolveStablePlayerAddress(_charaHandler?.Address ?? nint.Zero);
        var objectKinds = (_cachedData == null
            ? Enumerable.Empty<ObjectKind>()
            : (_cachedData.FileReplacements?.Keys ?? Enumerable.Empty<ObjectKind>())
                .Concat(_cachedData.GlamourerData?.Keys ?? Enumerable.Empty<ObjectKind>())
                .Concat(_cachedData.CustomizePlusData?.Keys ?? Enumerable.Empty<ObjectKind>()))
            .Distinct()
            .ToArray();

        ResetCollectionBindingState();
        ResetAppliedModTrackingState();
        ResetOwnedObjectRetryState();
        ResetReapplyTrackingState();
        ResetVisibilityTracking();

        _initialApplyPending = false;
        _forceApplyMods = true;
        _redrawOnNextApplication = true;

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        Pair.SetCurrentDownloadStatus(null);
        Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
        Pair.ClearDisplayedPerformanceMetrics();
        ScheduleRefreshUi(immediate: true);

        if (_lifetime.ApplicationStopping.IsCancellationRequested)
            return;

        if (_dalamudUtil.IsZoning || _dalamudUtil.IsInCutscene)
            return;

        if (_charaHandler == null || string.IsNullOrEmpty(name))
            return;

        _disposeRestoreAlreadyQueued = true;

        _ = Task.Run(async () =>
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

                await RemovePenumbraCollectionAsync(applicationId).ConfigureAwait(false);

                foreach (var objectKind in objectKinds)
                {
                    try
                    {
                        await RevertCustomizationDataAsync(objectKind, name, applicationId, cts.Token, playerAddress).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (InvalidOperationException ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Pause revert skipped for {pair} {objectKind}; actor no longer matched",
                            applicationId, Pair.UserData.AliasOrUID, objectKind);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "[{applicationId}] Pause revert failed for {pair} {objectKind}",
                            applicationId, Pair.UserData.AliasOrUID, objectKind);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Logger.LogDebug("[{applicationId}] Pause revert timed out for {pair}", applicationId, Pair.UserData.AliasOrUID);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "[{applicationId}] Failed entering paused vanilla state for {pair}", applicationId, Pair.UserData.AliasOrUID);
            }
        });
    }

    private void HandleOtherSyncReleased(bool requestApplyIfPossible)
        => _otherSyncCoordinator.HandleOtherSyncReleased(requestApplyIfPossible);

    private void EnterYieldedState(string owner) => _otherSyncCoordinator.EnterYieldedState(owner);

    private Task<PairSyncCommitResult> DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
        => _downloadCoordinator.DownloadAndApplyCharacterAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, assetPlan, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, downloadToken);

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

    private Task<bool> ApplyCustomizationDataAsync(Guid applicationId, KeyValuePair<ObjectKind, HashSet<PlayerChanges>> changes, CharacterData charaData, bool allowPlayerRedraw, bool forceLightweightMetadataReapply, bool awaitPlayerGlamourerApply, CancellationToken token)
        => _customizationCoordinator.ApplyCustomizationDataAsync(applicationId, changes, charaData, allowPlayerRedraw, forceLightweightMetadataReapply, awaitPlayerGlamourerApply, token);

    private Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken, nint addressOverride = 0, IReadOnlyDictionary<ObjectKind, Guid?>? customizeIdSnapshot = null)
        => _customizationCoordinator.RevertCustomizationDataAsync(objectKind, name, applicationId, cancelToken, addressOverride, customizeIdSnapshot);

    private bool IsPairSyncBusyForPayload(string payloadFingerprint) => _syncWorker?.IsBusyForPayload(payloadFingerprint) == true;

    private void CancelPairSyncWork(bool clearDesired = false) => _syncWorker?.CancelActiveWork(clearDesired);
    private void ResetPairSyncPipelineState() => _syncWorker?.ResetPipelineState();

    private Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken token)
        => _applyExecutionCoordinator.ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, assetPlan, downloadedAny, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, token);
}
