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
    private abstract class CoordinatorBase
    {
        protected CoordinatorBase(PairHandler owner)
        {
            Owner = owner;
        }

        protected PairHandler Owner { get; }

        public override string ToString() => Owner.ToString();

        protected ILogger Logger => Owner.Logger;
        protected MareMediator Mediator => Owner.Mediator;
        protected Pair Pair => Owner.Pair;
        protected bool IsVisible { get => Owner.IsVisible; set => Owner.IsVisible = value; }
        protected long LastAppliedDataBytes { get => Owner.LastAppliedDataBytes; set => Owner.LastAppliedDataBytes = value; }
        protected string? PlayerName { get => Owner.PlayerName; set => Owner.PlayerName = value; }
        protected string PlayerNameHash => Owner.PlayerNameHash;
        protected nint PlayerCharacter => Owner.PlayerCharacter;
        protected uint PlayerCharacterId => Owner.PlayerCharacterId;

        protected DalamudUtilService _dalamudUtil => Owner._dalamudUtil;
        protected FileDownloadManager? _downloadManager => Owner._downloadManager;
        protected FileCacheManager _fileDbManager => Owner._fileDbManager;
        protected GameObjectHandlerFactory _gameObjectHandlerFactory => Owner._gameObjectHandlerFactory;
        protected IpcManager _ipcManager => Owner._ipcManager;
        protected IHostApplicationLifetime _lifetime => Owner._lifetime;
        protected PlayerPerformanceService _playerPerformanceService => Owner._playerPerformanceService;
        protected ServerConfigurationManager _serverConfigManager => Owner._serverConfigManager;
        protected PluginWarningNotificationService _pluginWarningNotificationManager => Owner._pluginWarningNotificationManager;
        protected ModPathResolver _modPathResolver => Owner._modPathResolver;
        protected ObjectIndexCleanupService _objectIndexCleanupService => Owner._objectIndexCleanupService;
        protected PapSanitisationService _papSanitisationService => Owner._papSanitisationService;

        protected CancellationTokenSource? _applicationCancellationTokenSource { get => Owner._applicationCancellationTokenSource; set => Owner._applicationCancellationTokenSource = value; }
        protected CancellationTokenSource? _downloadCancellationTokenSource { get => Owner._downloadCancellationTokenSource; set => Owner._downloadCancellationTokenSource = value; }
        protected Guid _applicationId { get => Owner._applicationId; set => Owner._applicationId = value; }
        protected Task? _applicationTask { get => Owner._applicationTask; set => Owner._applicationTask = value; }
        protected Task<PairSyncCommitResult>? _pairSyncApplicationTask { get => Owner._pairSyncApplicationTask; set => Owner._pairSyncApplicationTask = value; }
        protected Task? _pairDownloadTask { get => Owner._pairDownloadTask; set => Owner._pairDownloadTask = value; }
        protected CharacterData? _cachedData { get => Owner._cachedData; set => Owner._cachedData = value; }
        protected GameObjectHandler? _charaHandler { get => Owner._charaHandler; set => Owner._charaHandler = value; }
        protected Dictionary<ObjectKind, Guid?> _customizeIds => Owner._customizeIds;
        protected CombatData? _dataReceivedInDowntime { get => Owner._dataReceivedInDowntime; set => Owner._dataReceivedInDowntime = value; }
        protected bool _forceApplyMods { get => Owner._forceApplyMods; set => Owner._forceApplyMods = value; }
        protected Guid _penumbraCollection { get => Owner._penumbraCollection; set => Owner._penumbraCollection = value; }
        protected Task<Guid>? _penumbraCollectionTask { get => Owner._penumbraCollectionTask; set => Owner._penumbraCollectionTask = value; }
        protected Task? _initializeTask { get => Owner._initializeTask; set => Owner._initializeTask = value; }
        protected int _initializeStarted { get => Owner._initializeStarted; set => Owner._initializeStarted = value; }
        protected bool _redrawOnNextApplication { get => Owner._redrawOnNextApplication; set => Owner._redrawOnNextApplication = value; }
        protected bool _hasRetriedAfterMissingDownload { get => Owner._hasRetriedAfterMissingDownload; set => Owner._hasRetriedAfterMissingDownload = value; }
        protected bool _hasRetriedAfterMissingAtApply { get => Owner._hasRetriedAfterMissingAtApply; set => Owner._hasRetriedAfterMissingAtApply = value; }
        protected int _manualRepairRunning { get => Owner._manualRepairRunning; set => Owner._manualRepairRunning = value; }
        protected int? _lastAssignedObjectIndex { get => Owner._lastAssignedObjectIndex; set => Owner._lastAssignedObjectIndex = value; }
        protected nint _lastAssignedPlayerAddress { get => Owner._lastAssignedPlayerAddress; set => Owner._lastAssignedPlayerAddress = value; }
        protected DateTime _lastAssignedCollectionAssignUtc { get => Owner._lastAssignedCollectionAssignUtc; set => Owner._lastAssignedCollectionAssignUtc = value; }
        protected DateTime _nextTempCollectionRetryNotBeforeUtc { get => Owner._nextTempCollectionRetryNotBeforeUtc; set => Owner._nextTempCollectionRetryNotBeforeUtc = value; }
        protected long _lastApplyCompletedTick { get => Owner._lastApplyCompletedTick; set => Owner._lastApplyCompletedTick = value; }
        protected string? _lastAttemptedDataHash { get => Owner._lastAttemptedDataHash; set => Owner._lastAttemptedDataHash = value; }
        protected string? _lastAppliedTempModsFingerprint { get => Owner._lastAppliedTempModsFingerprint; set => Owner._lastAppliedTempModsFingerprint = value; }
        protected Dictionary<string, string>? _lastAppliedTempModsSnapshot { get => Owner._lastAppliedTempModsSnapshot; set => Owner._lastAppliedTempModsSnapshot = value; }
        protected string? _activeTempFilesModName { get => Owner._activeTempFilesModName; set => Owner._activeTempFilesModName = value; }
        protected int _activeTempFilesModPriority { get => Owner._activeTempFilesModPriority; set => Owner._activeTempFilesModPriority = value; }
        protected string? _lastAppliedTransientSupportFingerprint { get => Owner._lastAppliedTransientSupportFingerprint; set => Owner._lastAppliedTransientSupportFingerprint = value; }
        protected string? _lastAppliedManipulationFingerprint { get => Owner._lastAppliedManipulationFingerprint; set => Owner._lastAppliedManipulationFingerprint = value; }
        protected ConcurrentDictionary<Guid, byte> _lifecycleRedrawApplications => Owner._lifecycleRedrawApplications;
        protected long _suppressClassJobRedrawUntilTick { get => Owner._suppressClassJobRedrawUntilTick; set => Owner._suppressClassJobRedrawUntilTick = value; }
        protected object _missingCheckGate => Owner._missingCheckGate;
        protected string? _lastMissingCheckedHash { get => Owner._lastMissingCheckedHash; set => Owner._lastMissingCheckedHash = value; }
        protected long _lastMissingCheckedTick { get => Owner._lastMissingCheckedTick; set => Owner._lastMissingCheckedTick = value; }
        protected bool _lastMissingCheckHadMissing { get => Owner._lastMissingCheckHadMissing; set => Owner._lastMissingCheckHadMissing = value; }
        protected int _missingCheckRunning { get => Owner._missingCheckRunning; set => Owner._missingCheckRunning = value; }
        protected string? _pendingMissingCheckHash { get => Owner._pendingMissingCheckHash; set => Owner._pendingMissingCheckHash = value; }
        protected CharacterData? _pendingMissingCheckData { get => Owner._pendingMissingCheckData; set => Owner._pendingMissingCheckData = value; }
        protected Guid _pendingMissingCheckBase { get => Owner._pendingMissingCheckBase; set => Owner._pendingMissingCheckBase = value; }
        protected string? _lastPostApplyRepairHash { get => Owner._lastPostApplyRepairHash; set => Owner._lastPostApplyRepairHash = value; }
        protected long _lastPostApplyRepairTick { get => Owner._lastPostApplyRepairTick; set => Owner._lastPostApplyRepairTick = value; }
        protected object _postRepairGate => Owner._postRepairGate;
        protected CharacterData? _pendingPostApplyRepairData { get => Owner._pendingPostApplyRepairData; set => Owner._pendingPostApplyRepairData = value; }
        protected string? _pendingPostApplyRepairHash { get => Owner._pendingPostApplyRepairHash; set => Owner._pendingPostApplyRepairHash = value; }
        protected object _refreshUiGate => Owner._refreshUiGate;
        protected Dictionary<ObjectKind, string> _lastAppliedMoodlesData => Owner._lastAppliedMoodlesData;
        protected Dictionary<ObjectKind, string> _lastAppliedHonorificData => Owner._lastAppliedHonorificData;
        protected Dictionary<ObjectKind, string> _lastAppliedPetNamesData => Owner._lastAppliedPetNamesData;
        protected long _addressZeroSinceTick { get => Owner._addressZeroSinceTick; set => Owner._addressZeroSinceTick = value; }
        protected long _zoneRecoveryUntilTick { get => Owner._zoneRecoveryUntilTick; set => Owner._zoneRecoveryUntilTick = value; }
        protected long _identityDriftSinceTick { get => Owner._identityDriftSinceTick; set => Owner._identityDriftSinceTick = value; }
        protected long _nextVisibilityWorkTick { get => Owner._nextVisibilityWorkTick; set => Owner._nextVisibilityWorkTick = value; }
        protected bool _initialApplyPending { get => Owner._initialApplyPending; set => Owner._initialApplyPending = value; }
        protected long _otherSyncPollTick { get => Owner._otherSyncPollTick; set => Owner._otherSyncPollTick = value; }
        protected long _otherSyncReleaseCandidateSinceTick { get => Owner._otherSyncReleaseCandidateSinceTick; set => Owner._otherSyncReleaseCandidateSinceTick = value; }
        protected long _otherSyncAcquireCandidateSinceTick { get => Owner._otherSyncAcquireCandidateSinceTick; set => Owner._otherSyncAcquireCandidateSinceTick = value; }
        protected string _otherSyncAcquireCandidateOwner { get => Owner._otherSyncAcquireCandidateOwner; set => Owner._otherSyncAcquireCandidateOwner = value; }
        protected nint _lastKnownOwnershipAddr { get => Owner._lastKnownOwnershipAddr; set => Owner._lastKnownOwnershipAddr = value; }
        protected HashSet<ObjectKind> _pendingOwnedObjectCustomizationRetry => Owner._pendingOwnedObjectCustomizationRetry;
        protected long _nextOwnedObjectCustomizationRetryTick { get => Owner._nextOwnedObjectCustomizationRetryTick; set => Owner._nextOwnedObjectCustomizationRetryTick = value; }
        protected bool? _lastBroadcastYield { get => Owner._lastBroadcastYield; set => Owner._lastBroadcastYield = value; }
        protected string _lastBroadcastOwner { get => Owner._lastBroadcastOwner; set => Owner._lastBroadcastOwner = value; }
        protected bool _localOtherSyncDecisionYield { get => Owner._localOtherSyncDecisionYield; set => Owner._localOtherSyncDecisionYield = value; }
        protected string _localOtherSyncDecisionOwner { get => Owner._localOtherSyncDecisionOwner; set => Owner._localOtherSyncDecisionOwner = value; }

        protected long _lastInitialApplyDispatchTick { get => Owner._lastInitialApplyDispatchTick; set => Owner._lastInitialApplyDispatchTick = value; }

        protected static int VisibilityLossGraceMs => PairHandler.VisibilityLossGraceMs;
        protected static int ZoneVisibilityLossGraceMs => PairHandler.ZoneVisibilityLossGraceMs;
        protected static int VisibleReplayDispatchCooldownMs => PairHandler.VisibleReplayDispatchCooldownMs;
        protected static SemaphoreSlim NormalApplySemaphore => PairHandler.NormalApplySemaphore;
        protected static SemaphoreSlim StormApplySemaphore => PairHandler.StormApplySemaphore;
        protected static SemaphoreSlim GlobalRedrawSemaphore => PairHandler.GlobalRedrawSemaphore;
        protected static ConcurrentDictionary<int, byte> RedrawObjectIndicesInFlight => PairHandler.RedrawObjectIndicesInFlight;
        protected static SemaphoreSlim GlobalPostApplyRepairSemaphore => PairHandler.GlobalPostApplyRepairSemaphore;

        protected nint ResolveStablePlayerAddress(nint fallbackAddress = 0) => Owner.ResolveStablePlayerAddress(fallbackAddress);
        protected nint ResolveStrictVisiblePlayerAddress(nint fallbackAddress = 0) => Owner.ResolveStrictVisiblePlayerAddress(fallbackAddress);
        protected bool IsExpectedPlayerAddress(nint address) => Owner.IsExpectedPlayerAddress(address);
        protected void CleanupOldAssignedIndexIfNeeded(int? objectIndex, Guid applicationId) => Owner.CleanupOldAssignedIndexIfNeeded(objectIndex, applicationId);
        protected void ClearAppliedLightweightState() => Owner.ClearAppliedLightweightState();
        protected bool ShouldApplyLightweightMetadata(Dictionary<ObjectKind, string> cache, ObjectKind kind, string data) => Owner.ShouldApplyLightweightMetadata(cache, kind, data);
        protected void ResetCollectionBindingState() => Owner.ResetCollectionBindingState();
        protected void ResetAppliedModTrackingState() => Owner.ResetAppliedModTrackingState();
        protected void ResetOwnedObjectRetryState() => Owner.ResetOwnedObjectRetryState();
        protected void ResetOtherSyncCandidateState() => Owner.ResetOtherSyncCandidateState();
        protected void ResetReapplyTrackingState(bool resetRetries = true) => Owner.ResetReapplyTrackingState(resetRetries);
        protected void MarkInitialApplyRequired(bool redrawOnNextApplication = true) => Owner.MarkInitialApplyRequired(redrawOnNextApplication);
        protected void ResetVisibleReplayReadiness() => Owner.ResetVisibleReplayReadiness();
        protected bool IsVisibleReplayReady(nint address, long nowTick) => Owner.IsVisibleReplayReady(address, nowTick);
        protected void BeginVisibilityRecoveryWindow(long nowTick, bool isZoneTransition) => Owner.BeginVisibilityRecoveryWindow(nowTick, isZoneTransition);
        protected void ResetVisibilityTracking() => Owner.ResetVisibilityTracking();
        protected void ResetLiveApplicationState(bool resetRetryState = true) => Owner.ResetLiveApplicationState(resetRetryState);
        protected void CancelDownloadCancellationDeferred() => Owner.CancelDownloadCancellationDeferred();
        protected void SetUploading(bool isUploading = true) => Owner.SetUploading(isUploading);
        protected PairHandler.ApplyFrameworkSnapshot CaptureApplyFrameworkSnapshot() => Owner.CaptureApplyFrameworkSnapshot();
        protected PairHandler.ApplyPreparation PrepareApplyData(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization, PairHandler.ApplyFrameworkSnapshot snapshot) => Owner.PrepareApplyData(applicationBase, characterData, forceApplyCustomization, snapshot);
        protected bool IsApplyPreparationStillValid(PairHandler.ApplyFrameworkSnapshot snapshot) => Owner.IsApplyPreparationStillValid(snapshot);
        protected bool IsPairSyncBusyForPayload(string payloadFingerprint) => Owner.IsPairSyncBusyForPayload(payloadFingerprint);
        protected void CancelPairSyncWork(bool clearDesired = false) => Owner.CancelPairSyncWork(clearDesired);
        protected void ResetPairSyncPipelineState() => Owner.ResetPairSyncPipelineState();
        protected void QueueOwnedObjectCustomizationRetry(ObjectKind objectKind) => Owner.QueueOwnedObjectCustomizationRetry(objectKind);
        protected bool HasPendingOwnedObjectCustomizationPayload(CharacterData charaData, ObjectKind objectKind) => Owner.HasPendingOwnedObjectCustomizationPayload(charaData, objectKind);
        protected nint ResolveOwnedObjectAddressForRetry(nint playerAddress, ObjectKind objectKind) => Owner.ResolveOwnedObjectAddressForRetry(playerAddress, objectKind);
        protected void ProcessPendingOwnedObjectCustomizationRetry(long nowTick) => Owner.ProcessPendingOwnedObjectCustomizationRetry(nowTick);
        protected Task<bool> ApplyCustomizationDataAsync(Guid applicationId, KeyValuePair<ObjectKind, HashSet<PlayerChanges>> changes, CharacterData charaData, bool allowPlayerRedraw, bool forceLightweightMetadataReapply, bool awaitPlayerGlamourerApply, CancellationToken token) => Owner.ApplyCustomizationDataAsync(applicationId, changes, charaData, allowPlayerRedraw, forceLightweightMetadataReapply, awaitPlayerGlamourerApply, token);
        protected Task<bool> OnePassRedrawAsync(Guid applicationId, CancellationToken token, bool criticalRedraw = false) => Owner.OnePassRedrawAsync(applicationId, token, criticalRedraw);
        protected Task<(bool Bound, bool Reassigned)> EnsurePenumbraCollectionBindingAsync(Guid applicationId) => Owner.EnsurePenumbraCollectionBindingAsync(applicationId);
        protected Task EnsurePenumbraCollectionAsync() => Owner.EnsurePenumbraCollectionAsync();
        protected Task RemovePenumbraCollectionAsync(Guid applicationId) => Owner.RemovePenumbraCollectionAsync(applicationId);
        protected Task<PairSyncCommitResult> DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken) => Owner.DownloadAndApplyCharacterAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, assetPlan, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, downloadToken);
        protected Task<PairSyncCommitResult> ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, PairSyncAssetPlan assetPlan, bool downloadedAny, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken token) => Owner.ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, assetPlan, downloadedAny, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, token);
        protected bool HasAnyMissingCacheFiles(Guid applicationBase, CharacterData characterData) => Owner.HasAnyMissingCacheFiles(applicationBase, characterData);
        protected FileDownloadManager EnsureDownloadManager() => Owner.GetOrCreateDownloadManager();
        protected void DisposeDownloadManager() => Owner.DisposeDownloadManager();
        protected void BroadcastLocalOtherSyncYieldState(bool yieldToOtherSync, string owner) => Owner.BroadcastLocalOtherSyncYieldState(yieldToOtherSync, owner);
        protected Task InitializeAsync(string name) => Owner.InitializeAsync(name);
        protected Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken, nint addressOverride = 0, IReadOnlyDictionary<ObjectKind, Guid?>? customizeIdSnapshot = null) => Owner.RevertCustomizationDataAsync(objectKind, name, applicationId, cancelToken, addressOverride, customizeIdSnapshot);
        protected void RequestManualFileRepair() => Owner.RequestManualFileRepair();
        protected Task ManualVerifyAndRepairAsync(Guid applicationBase, CharacterData charaData, CancellationToken token, bool verifyFileHashes = true, bool publishEvents = true) => Owner.ManualVerifyAndRepairAsync(applicationBase, charaData, token, verifyFileHashes, publishEvents);
        protected bool TryGetRecentMissingCheck(string dataHash, out bool hadMissing) => Owner.TryGetRecentMissingCheck(dataHash, out hadMissing);
        protected void ScheduleMissingCheck(Guid applicationBase, CharacterData characterData) => Owner.ScheduleMissingCheck(applicationBase, characterData);
        protected void RequestPostApplyRepair(CharacterData appliedData) => Owner.RequestPostApplyRepair(appliedData);
        protected void ReclaimFromOtherSync(bool requestApplyIfPossible, bool treatAsFirstVisible) => Owner.ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible);
        protected void HandleOtherSyncReleased(bool requestApplyIfPossible) => Owner.HandleOtherSyncReleased(requestApplyIfPossible);
        protected void ResetToUninitializedState(bool revertLiveCustomizationState = true) => Owner.ResetToUninitializedState(revertLiveCustomizationState);
        protected void ScheduleRefreshUi(bool immediate = false) => Owner.ScheduleRefreshUi(immediate);
        protected void EnterYieldedState(string owner) => Owner.EnterYieldedState(owner);
    }
}
