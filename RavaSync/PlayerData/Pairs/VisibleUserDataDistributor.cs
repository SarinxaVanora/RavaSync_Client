using RavaSync.API.Data;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.Mesh;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using RavaSync.Interop.Ipc;
using Microsoft.Extensions.Logging;
using RavaSync.PlayerData.Services;

namespace RavaSync.PlayerData.Pairs;

public class VisibleUserDataDistributor : DisposableMediatorSubscriberBase
{
    private sealed record LocalOtherSyncClaim(bool Active, string Owner);
    private readonly ApiController _apiController;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileUploadManager _fileTransferManager;
    private readonly MissingFileMeshService _missingFileMeshService;
    private readonly PairManager _pairManager;
    private readonly IpcManager _ipcManager;
    private readonly CharacterAnalyzer _characterAnalyzer;
    private readonly CharacterRavaSidecarUtility _characterRavaSidecarUtility;
    private readonly LocalActiveSyncIndicatorService _localActiveSyncIndicatorService;
    private CharacterData? _lastCreatedData;
    private string _lastCreatedDataPayloadFingerprint = string.Empty;
    private CharacterData? _uploadingCharacterData = null;
    private Task<(CharacterData Data, bool Success, string? Error)>? _fileUploadTask = null;
    private readonly Dictionary<string, UserData> _usersToPushDataTo = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _queuedPushRevisionByUid = new(StringComparer.Ordinal);
    private readonly HashSet<UserData> _previouslyVisiblePlayersSet = [];
    private readonly HashSet<UserData> _currentVisiblePlayersSet = [];
    private readonly List<UserData> _newVisibleUsersBuffer = [];
    private readonly List<UserData> _noLongerVisibleUsersBuffer = [];
    private readonly List<nint> _visiblePlayerAddressesBuffer = [];
    private readonly SemaphoreSlim _pushDataSemaphore = new(1, 1);
    private int _pushTaskQueuedOrRunning;
    private readonly object _pushQueueLock = new();
    private readonly CancellationTokenSource _runtimeCts = new();
    private long _queuedPushRevisionCounter;
    private string _lastHeelsData = string.Empty;
    private long _pendingHeelsApplyTick = -1;
    private string _lastUploadBarrierKey = string.Empty;
    private HashSet<string> _lastUploadBarrierHashes = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _authorizedRecipientUids = new(StringComparer.Ordinal);
    private string? _pendingHeelsData;
    private bool _hasPendingHeelsUpdate;

    private string _lastHonorificData = string.Empty;
    private string _lastMoodlesData = string.Empty;
    private string _lastPetNamesData = string.Empty;

    private string? _pendingHonorificData;
    private bool _hasPendingHonorificUpdate;

    private string? _pendingMoodlesData;
    private bool _hasPendingMoodlesUpdate;

    private string? _pendingPetNamesData;
    private bool _hasPendingPetNamesUpdate;
    private bool _forceBarrierPending;
    private bool _forceOutboundPending;
    private bool _bypassDuplicateSuppressionPending;
    private long _pendingMoodlesApplyTick = -1;
    private long _nextLocalPlayerAddressRefreshTick;
    private long _nextVisibilityScanTick;
    private nint _localPlayerAddress;
    private string _lastSidecarBroadcastSignature = string.Empty;
    private DateTime _lastSidecarBroadcastUtc = DateTime.MinValue;
    private DateTime _lastOutboundCharacterPushUtc = DateTime.MinValue;
    private readonly Dictionary<string, string> _lastOutboundFullSignatureByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _lastOutboundAppearanceSignatureByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastOutboundSendTickByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastOutboundSidecarOnlyTickByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _lastOutboundOtherSyncSignatureByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, LocalOtherSyncClaim> _localOtherSyncClaimByUid = new(StringComparer.Ordinal);
    private const int DisplayedMetricsPreSendWaitMs = 250;
    private const int OutboundPushCoalesceDelayMs = 125;
    private const int MaxOutboundPushCoalesceWaitMs = 500;
    private const int DuplicateOutboundSuppressWindowMs = 10_000;
    private const int SidecarOnlyOutboundMinIntervalMs = 60_000;
    private static readonly TimeSpan SidecarRefreshMinInterval = TimeSpan.FromSeconds(60);

    public VisibleUserDataDistributor(ILogger<VisibleUserDataDistributor> logger, ApiController apiController, DalamudUtilService dalamudUtil,
        PairManager pairManager, MareMediator mediator, FileUploadManager fileTransferManager, MissingFileMeshService missingFileMeshService, IpcManager ipcManager, CharacterAnalyzer characterAnalyzer, CharacterRavaSidecarUtility characterRavaSidecarUtility, LocalActiveSyncIndicatorService localActiveSyncIndicatorService) : base(logger, mediator)
    {
        _apiController = apiController;
        _dalamudUtil = dalamudUtil;
        _pairManager = pairManager;
        _fileTransferManager = fileTransferManager;
        _missingFileMeshService = missingFileMeshService;
        _ipcManager = ipcManager;
        _characterAnalyzer = characterAnalyzer;
        _characterRavaSidecarUtility = characterRavaSidecarUtility;
        _localActiveSyncIndicatorService = localActiveSyncIndicatorService;
        Mediator.Subscribe<FrameworkUpdateMessage>(this, (_) =>
        {
            var nowTick = Environment.TickCount64;
            if (_localPlayerAddress == nint.Zero || nowTick >= _nextLocalPlayerAddressRefreshTick)
            {
                try
                {
                    _localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                }
                catch
                {
                    _localPlayerAddress = nint.Zero;
                }

                _nextLocalPlayerAddressRefreshTick = nowTick + 1000;
            }

            FrameworkOnUpdate(nowTick);
        });
        Mediator.Subscribe<CharacterDataCreatedMessage>(this, (msg) =>
        {
            var newData = msg.CharacterData;
            var newPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(newData);

            if (_lastCreatedData == null || !string.Equals(newPayloadFingerprint, _lastCreatedDataPayloadFingerprint, StringComparison.Ordinal))
            {
                var firstPayload = _lastCreatedData == null;
                var oldBarrierKey = _lastCreatedData != null ? BuildUploadBarrierKey(_lastCreatedData) : string.Empty;
                var newBarrierKey = BuildUploadBarrierKey(newData);
                var barrierKeyChanged = !string.Equals(oldBarrierKey, newBarrierKey, StringComparison.Ordinal);

                _lastCreatedData = newData;
                _lastCreatedDataPayloadFingerprint = newPayloadFingerprint;
                _lastHeelsData = newData.HeelsData ?? string.Empty;
                _lastHonorificData = newData.HonorificData ?? string.Empty;
                _lastMoodlesData = newData.MoodlesData ?? string.Empty;
                _lastPetNamesData = newData.PetNamesData ?? string.Empty;

                // Keep recipient authorization when the payload hash set changes.
                // Existing hashes remain shared; the push path can now authorize only newly-added hashes.
                // Visibility/reconnect still forces a full barrier when required.
                if (barrierKeyChanged && Logger.IsEnabled(LogLevel.Trace))
                {
                    Logger.LogTrace(
                        "Local payload hash set changed for {hash}; retaining prior upload/share authorizations and letting the barrier check only new hashes when safe",
                        newData.DataHash.Value);
                }

                Logger.LogTrace("Storing new data hash {hash}/{payload}", newData.DataHash.Value, newPayloadFingerprint);
                _lastSidecarBroadcastSignature = string.Empty;
                PushToAllVisibleUsers(forced: firstPayload || barrierKeyChanged, forceOutbound: msg.ForceOutbound);
            }
            else
            {
                Logger.LogTrace("Data hash {hash}/{payload} equal to stored data", newData.DataHash.Value, newPayloadFingerprint);

                if (msg.ForceOutbound && Logger.IsEnabled(LogLevel.Trace))
                {
                    Logger.LogTrace(
                        "Suppressing force-outbound push for unchanged local payload; reason={reason}",
                        msg.Reason);
                }
            }
        });

        Mediator.Subscribe<CharacterDataAnalyzedMessage>(this, (_) =>
        {
            var currentHash = _lastCreatedData?.DataHash.Value ?? string.Empty;
            if (string.IsNullOrWhiteSpace(currentHash))
                return;

            if (!string.Equals(_characterAnalyzer.LatestAnalyzedDataHash, currentHash, StringComparison.Ordinal))
                return;

            if (!_characterAnalyzer.TryGetDisplayedHeaderMetrics(currentHash, out var displayedVramBytes, out var displayedTriangles))
                return;

            var currentSignature = BuildSidecarSignature(currentHash, displayedVramBytes, displayedTriangles);
            if (string.Equals(_lastSidecarBroadcastSignature, currentSignature, StringComparison.Ordinal))
                return;

            var nowUtc = DateTime.UtcNow;
            if (nowUtc - _lastSidecarBroadcastUtc < SidecarRefreshMinInterval)
                return;

            // A full outbound character push already waits briefly for metrics and carries the
            // sidecar when it is ready. If metrics finish a few frames later, do not immediately
            // send a second character-data packet just for the UI stats; let the normal refresh
            // window handle it.
            if (nowUtc - _lastOutboundCharacterPushUtc < SidecarRefreshMinInterval)
                return;

            var queuedCount = QueueVisibleUsersForPush();

            if (queuedCount == 0)
                return;

            Logger.LogDebug("Scheduling sidecar-backed perf refresh for {count} visible players", queuedCount);
            PushCharacterData(includeSyncManifest: false);
        });

        Mediator.Subscribe<LocalActiveSyncIndicatorChangedMessage>(this, msg =>
        {
            if (!_apiController.IsConnected || _lastCreatedData == null)
                return;

            var queuedCount = QueueVisibleUsersForPush();
            if (queuedCount == 0)
                return;

            Logger.LogDebug("Scheduling active sound indicator refresh for {count} visible players; playing={playing}", queuedCount, msg.IsPlayingSound);
            PushCharacterData(forceOutbound: true, includeSyncManifest: false, bypassDuplicateSuppression: true);
        });

        //Mediator.Subscribe<ConnectedMessage>(this, (_) => PushToAllVisibleUsers(forced: true));

        Mediator.Subscribe<ConnectedMessage>(this, (_) =>
        {
            lock (_pushQueueLock)
            {
                _lastOutboundFullSignatureByUid.Clear();
                _lastOutboundAppearanceSignatureByUid.Clear();
                _lastOutboundSendTickByUid.Clear();
                _lastOutboundSidecarOnlyTickByUid.Clear();
                _lastOutboundOtherSyncSignatureByUid.Clear();
            }

            var queuedCount = QueueVisibleUsersForPush(forceBarrier: true);

            if (_lastCreatedData != null && queuedCount > 0)
            {
                Logger.LogDebug("Forcing outbound character data refresh after connection established for {count} visible players", queuedCount);
                PushCharacterData(forced: true, forceOutbound: true, bypassDuplicateSuppression: true);
            }
        });

        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) => ResetZoneVisibilityTracking());
        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (_) => ResetZoneVisibilityTracking());
        Mediator.Subscribe<DisconnectedMessage>(this, (_) =>
        {
            _pairManager.ClearDisplayedPerformanceMetricsForAllPairs();
            _previouslyVisiblePlayersSet.Clear();
            _currentVisiblePlayersSet.Clear();
            lock (_pushQueueLock)
            {
                _usersToPushDataTo.Clear();
                _queuedPushRevisionByUid.Clear();
                _forceBarrierPending = false;
                _forceOutboundPending = false;
                _bypassDuplicateSuppressionPending = false;
            }
            _newVisibleUsersBuffer.Clear();
            _noLongerVisibleUsersBuffer.Clear();
            _visiblePlayerAddressesBuffer.Clear();
            _ipcManager.OtherSync.UpdateTrackedVisibleAddresses(null);
            _nextVisibilityScanTick = 0;
            _nextLocalPlayerAddressRefreshTick = 0;
            _localPlayerAddress = nint.Zero;
            _uploadingCharacterData = null;
            _fileUploadTask = null;
            _lastUploadBarrierKey = string.Empty;
            _lastUploadBarrierHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _authorizedRecipientUids.Clear();
            // Keep the last built local payload across a transport disconnect so reconnect can repush it to visible recipients.
            _pendingHeelsData = null;
            _hasPendingHeelsUpdate = false;
            _pendingHonorificData = null;
            _hasPendingHonorificUpdate = false;
            _pendingMoodlesData = null;
            _hasPendingMoodlesUpdate = false;
            _pendingPetNamesData = null;
            _hasPendingPetNamesUpdate = false;

            _pendingHeelsApplyTick = -1;
            _pendingMoodlesApplyTick = -1;
            _lastSidecarBroadcastSignature = string.Empty;
            _lastSidecarBroadcastUtc = DateTime.MinValue;
            _lastOutboundCharacterPushUtc = DateTime.MinValue;
            _lastOutboundFullSignatureByUid.Clear();
            _lastOutboundAppearanceSignatureByUid.Clear();
            _lastOutboundSendTickByUid.Clear();
            _lastOutboundSidecarOnlyTickByUid.Clear();
            _lastOutboundOtherSyncSignatureByUid.Clear();
            _localOtherSyncClaimByUid.Clear();
        });

        Mediator.Subscribe<HeelsOffsetMessage>(this, (msg) =>
        {
            HandleHeelsOffsetChanged(msg.Offset);
        });
        Mediator.Subscribe<HonorificMessage>(this, (msg) =>
        {
            HandleHonorificChanged(msg.NewHonorificTitle);
        });

        Mediator.Subscribe<MoodlesMessage>(this, (msg) =>
        {
            if (!IsFromLocalPlayer(msg.Address))
                return;

            HandleMoodlesChanged(msg.MoodlesData);
        });

        Mediator.Subscribe<PetNamesMessage>(this, (msg) =>
        {
            HandlePetNamesChanged(msg.PetNicknamesData);
        });

        Mediator.Subscribe<LocalOtherSyncYieldStateChangedMessage>(this, (msg) =>
        {
            HandleLocalOtherSyncStateChanged(msg.AffectedUid, msg.YieldToOtherSync, msg.Owner);
        });

        Mediator.Subscribe<OtherSyncCurrentStateChangedMessage>(this, (msg) =>
        {
            HandleOtherSyncCurrentStateChanged(msg.Reason);
        });

    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _runtimeCts.Cancel();
            _runtimeCts.Dispose();
        }

        base.Dispose(disposing);
    }


    private void ResetZoneVisibilityTracking()
    {
        _pairManager.ClearDisplayedPerformanceMetricsForAllPairs();
        _previouslyVisiblePlayersSet.Clear();
        _currentVisiblePlayersSet.Clear();
        _newVisibleUsersBuffer.Clear();
        _noLongerVisibleUsersBuffer.Clear();
        _visiblePlayerAddressesBuffer.Clear();
        _ipcManager.OtherSync.UpdateTrackedVisibleAddresses(null);
        _nextVisibilityScanTick = 0;
        _nextLocalPlayerAddressRefreshTick = 0;
        _localPlayerAddress = nint.Zero;
        _lastSidecarBroadcastSignature = string.Empty;
        _lastSidecarBroadcastUtc = DateTime.MinValue;
    }

    private bool IsFromLocalPlayer(nint address)
    {
        if (address == nint.Zero)
            return false;

        var localPlayerAddress = _localPlayerAddress;
        if (localPlayerAddress == nint.Zero)
        {
            try
            {
                localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                _localPlayerAddress = localPlayerAddress;
            }
            catch
            {
                return false;
            }
        }

        return localPlayerAddress == address;
    }
    private void PushToAllVisibleUsers(bool forced = false, bool forceOutbound = false)
    {
        var queuedCount = QueueVisibleUsersForPush();

        if (queuedCount > 0)
        {
            Logger.LogDebug("Pushing data {hash} for {count} visible players", _lastCreatedData?.DataHash.Value ?? "UNKNOWN", queuedCount);
            PushCharacterData(forced, forceOutbound);
        }
    }

    private void HandleLocalOtherSyncStateChanged(string affectedUid, bool yieldToOtherSync, string owner)
    {
        if (string.IsNullOrWhiteSpace(affectedUid))
            return;

        owner ??= string.Empty;
        owner = yieldToOtherSync ? owner.Trim() : string.Empty;

        lock (_pushQueueLock)
        {
            if (yieldToOtherSync && !string.IsNullOrWhiteSpace(owner))
                _localOtherSyncClaimByUid[affectedUid] = new LocalOtherSyncClaim(true, owner);
            else
                _localOtherSyncClaimByUid.Remove(affectedUid);
        }

        if (!_apiController.IsConnected || _lastCreatedData == null)
            return;

        var pair = _pairManager.GetPairByUID(affectedUid);
        if (pair == null || !pair.IsVisible)
            return;

        QueueUsersForPush([pair.UserData]);
        PushCharacterData(forceOutbound: true, bypassDuplicateSuppression: true);
    }

    private void HandleOtherSyncCurrentStateChanged(string reason)
    {
        if (!_apiController.IsConnected || _lastCreatedData == null)
            return;

        var changedUsers = new List<UserData>();

        _pairManager.ForEachVisiblePair(pair =>
        {
            var uid = pair.UserData?.UID ?? string.Empty;
            if (string.IsNullOrWhiteSpace(uid))
                return;

            var currentSignature = BuildCurrentOtherSyncOutboundSignatureForTarget(uid);
            lock (_pushQueueLock)
            {
                if (_lastOutboundOtherSyncSignatureByUid.TryGetValue(uid, out var lastSignature)
                    && string.Equals(lastSignature, currentSignature, StringComparison.Ordinal))
                {
                    return;
                }
            }

            changedUsers.Add(pair.UserData);
        });

        if (changedUsers.Count == 0)
            return;

        Logger.LogDebug("Scheduling OtherSync current-state user data refresh for {count} visible player(s); reason={reason}", changedUsers.Count, reason);
        QueueUsersForPush(changedUsers);
        PushCharacterData(forceOutbound: true, bypassDuplicateSuppression: true);
    }

    private void HandleHeelsOffsetChanged(string offset)
    {
        if (!_apiController.IsConnected)
            return;

        offset ??= string.Empty;

        if (string.Equals(offset, _lastHeelsData, StringComparison.Ordinal))
            return;

        _pendingHeelsData = offset;
        _hasPendingHeelsUpdate = true;
        _pendingHeelsApplyTick = Environment.TickCount64 + 125;
    }

    private void HandleHonorificChanged(string honorificData)
    {
        if (!_apiController.IsConnected)
            return;

        honorificData ??= string.Empty;

        if (string.Equals(honorificData, _lastHonorificData, StringComparison.Ordinal))
            return;

        _pendingHonorificData = honorificData;
        _hasPendingHonorificUpdate = true;
    }

    private void HandleMoodlesChanged(string moodlesData)
    {
        if (!_apiController.IsConnected)
            return;

        moodlesData ??= string.Empty;

        _pendingMoodlesData = moodlesData;
        _hasPendingMoodlesUpdate = true;
        _pendingMoodlesApplyTick = Environment.TickCount64 + 125;
    }

    private void HandlePetNamesChanged(string petNamesData)
    {
        if (!_apiController.IsConnected)
            return;

        petNamesData ??= string.Empty;

        if (string.Equals(petNamesData, _lastPetNamesData, StringComparison.Ordinal))
            return;

        _pendingPetNamesData = petNamesData;
        _hasPendingPetNamesUpdate = true;
    }

    private static string BuildUploadBarrierKey(CharacterData data)
    {
        if (data.FileReplacements.Count == 0)
            return string.Empty;

        var hashes = CollectPayloadFileHashes(data)
            .OrderBy(h => h, StringComparer.Ordinal);

        return string.Join('|', hashes);
    }

    private static HashSet<string> CollectPayloadFileHashes(CharacterData data)
    {
        if (data.FileReplacements.Count == 0)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        return data.FileReplacements
            .SelectMany(kvp => kvp.Value)
            .Where(fr => string.IsNullOrWhiteSpace(fr.FileSwapPath) && !string.IsNullOrWhiteSpace(fr.Hash))
            .Select(fr => fr.Hash.Trim().ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private HashSet<string> GetAddedPayloadHashes(HashSet<string> currentPayloadHashes, bool forceFullBarrier)
    {
        if (forceFullBarrier || _lastUploadBarrierHashes.Count == 0)
            return new HashSet<string>(currentPayloadHashes, StringComparer.OrdinalIgnoreCase);

        return currentPayloadHashes
            .Except(_lastUploadBarrierHashes, StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private void RecordUploadBarrierState(string barrierKey, HashSet<string> payloadHashes)
    {
        _lastUploadBarrierKey = barrierKey;
        _lastUploadBarrierHashes = new HashSet<string>(payloadHashes, StringComparer.OrdinalIgnoreCase);
    }

    private Dictionary<string, string> ResolveTargetMeshSessions(IEnumerable<UserData> targetUsers)
    {
        var requiredUids = CollectRecipientUids(targetUsers);
        var sessions = new Dictionary<string, string>(StringComparer.Ordinal);

        if (requiredUids.Count == 0)
            return sessions;

        _pairManager.ForEachVisiblePair(pair =>
        {
            var uid = pair.UserData?.UID;
            if (string.IsNullOrWhiteSpace(uid) || !requiredUids.Contains(uid))
                return;

            if (string.IsNullOrWhiteSpace(pair.Ident))
                return;

            var sessionId = RavaSessionId.FromIdent(pair.Ident);
            if (!string.IsNullOrWhiteSpace(sessionId))
                sessions[uid] = sessionId;
        });

        return sessions;
    }

    private static string BuildSidecarSignature(string? dataHash, long vramBytes, long triangles)
        => $"{dataHash}|{vramBytes}|{triangles}";

    private string BuildFullOutboundSignature(CharacterData data)
        => PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(data);

    private string BuildAppearanceOutboundSignature(CharacterData data)
    {
        var clone = data.DeepClone();
        _characterRavaSidecarUtility.StripSidecar(clone);
        return PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(clone);
    }

    private List<UserData> FilterUsersForOutboundSend(CharacterData dataToSend, List<UserData> targetUsers, bool forceBarrier, bool forceOutbound, bool bypassDuplicateSuppression, IReadOnlyDictionary<string, CharacterData>? dataByUid = null)
    {
        if (targetUsers.Count == 0)
            return targetUsers;

        var nowTick = Environment.TickCount64;
        var filtered = new List<UserData>(targetUsers.Count);

        lock (_pushQueueLock)
        {
            foreach (var user in targetUsers)
            {
                if (!IsCurrentlyVisibleOutboundRecipient(user))
                    continue;

                var uid = user.UID;
                if (string.IsNullOrWhiteSpace(uid))
                    continue;

                var perUserData = GetTargetedCharacterData(dataByUid, uid, dataToSend);
                var perUserFullSignature = BuildFullOutboundSignature(perUserData);
                var perUserAppearanceSignature = BuildAppearanceOutboundSignature(perUserData);

                _lastOutboundFullSignatureByUid.TryGetValue(uid, out var lastFullSignature);
                _lastOutboundAppearanceSignatureByUid.TryGetValue(uid, out var lastAppearanceSignature);
                _lastOutboundSendTickByUid.TryGetValue(uid, out var lastSendTick);
                _lastOutboundSidecarOnlyTickByUid.TryGetValue(uid, out var lastSidecarOnlyTick);

                var sameFullPayload = string.Equals(perUserFullSignature, lastFullSignature, StringComparison.Ordinal);
                var sameAppearancePayload = string.Equals(perUserAppearanceSignature, lastAppearanceSignature, StringComparison.Ordinal);
                var elapsedSinceLastSend = unchecked(nowTick - lastSendTick);
                var elapsedSinceSidecarOnly = unchecked(nowTick - lastSidecarOnlyTick);

                // Always suppress exact duplicate payloads inside the short outbound window.
                // Visibility gain and reconnect still send once because their per-recipient state is
                // cleared when visibility/connection drops; the bypass flag is only allowed to skip
                // slower sidecar-only throttles, not to double-send the same final payload.
                if (sameFullPayload
                    && elapsedSinceLastSend >= 0
                    && elapsedSinceLastSend < DuplicateOutboundSuppressWindowMs
                    && !forceBarrier)
                {
                    continue;
                }

                var sidecarOnlyPayload = sameAppearancePayload && !sameFullPayload;
                if (sidecarOnlyPayload
                    && elapsedSinceSidecarOnly >= 0
                    && elapsedSinceSidecarOnly < SidecarOnlyOutboundMinIntervalMs
                    && !forceBarrier
                    && !forceOutbound
                    && !bypassDuplicateSuppression)
                {
                    continue;
                }

                filtered.Add(user);
            }
        }

        if (filtered.Count == 0 && Logger.IsEnabled(LogLevel.Debug))
        {
            Logger.LogDebug(
                "Suppressed outbound duplicate/sidecar-only character data push for {count} recipients",
                targetUsers.Count);
        }

        return filtered;
    }

    private void RecordOutboundSend(CharacterData sentData, IEnumerable<UserData> sentUsers, IReadOnlyDictionary<string, CharacterData>? dataByUid = null)
    {
        var nowTick = Environment.TickCount64;
        var recordedAnyRecipient = false;
        lock (_pushQueueLock)
        {
            foreach (var user in sentUsers)
            {
                var uid = user.UID;
                if (string.IsNullOrWhiteSpace(uid))
                    continue;

                var perUserData = GetTargetedCharacterData(dataByUid, uid, sentData);
                var perUserFullSignature = BuildFullOutboundSignature(perUserData);
                var perUserAppearanceSignature = BuildAppearanceOutboundSignature(perUserData);

                _lastOutboundFullSignatureByUid.TryGetValue(uid, out var lastFullSignature);
                _lastOutboundAppearanceSignatureByUid.TryGetValue(uid, out var lastAppearanceSignature);

                var sameAppearancePayload = string.Equals(perUserAppearanceSignature, lastAppearanceSignature, StringComparison.Ordinal);
                var sameFullPayload = string.Equals(perUserFullSignature, lastFullSignature, StringComparison.Ordinal);
                var sidecarOnlyPayload = sameAppearancePayload && !sameFullPayload;

                _lastOutboundFullSignatureByUid[uid] = perUserFullSignature;
                _lastOutboundAppearanceSignatureByUid[uid] = perUserAppearanceSignature;
                _lastOutboundSendTickByUid[uid] = nowTick;

                if (sidecarOnlyPayload)
                    _lastOutboundSidecarOnlyTickByUid[uid] = nowTick;

                _lastOutboundOtherSyncSignatureByUid[uid] = BuildOtherSyncOutboundSignatureFromData(perUserData, uid);
                recordedAnyRecipient = true;
            }
        }

        if (recordedAnyRecipient)
            _lastOutboundCharacterPushUtc = DateTime.UtcNow;
    }

    private async Task<bool> PushCharacterDataServerFallbackAsync(CharacterData data, List<UserData> targetUsers, IReadOnlyDictionary<string, CharacterData>? dataByUid = null)
    {
        targetUsers = FilterUsersToCurrentlyVisibleRecipients(targetUsers);
        if (targetUsers.Count == 0)
            return true;

        if (dataByUid == null || dataByUid.Count == 0)
            return await _apiController.PushCharacterData(data.DeepClone(), targetUsers).ConfigureAwait(false);

        var success = true;
        foreach (var user in targetUsers)
        {
            if (!IsCurrentlyVisibleOutboundRecipient(user))
                continue;

            var uid = user.UID ?? string.Empty;
            var perUserData = GetTargetedCharacterData(dataByUid, uid, data);
            success &= await _apiController.PushCharacterData(perUserData.DeepClone(), [user]).ConfigureAwait(false);
        }

        return success;
    }

    private async Task<bool> CompleteMeshCharacterPushOrFallbackAsync(CharacterData data, List<UserData> targetUsers, MissingFileMeshService.MeshCharacterPushOfferResult? meshOffer, IReadOnlyDictionary<string, string> targetMeshSessions, IReadOnlyDictionary<string, CharacterData>? dataByUid = null)
    {
        targetUsers = FilterUsersToCurrentlyVisibleRecipients(targetUsers);
        if (targetUsers.Count == 0)
            return true;

        var currentVisibleTargetUids = CollectRecipientUids(targetUsers);
        HashSet<string> meshReadyAcked = new(StringComparer.Ordinal);
        HashSet<string> meshAccepted = meshOffer?.AcceptedUids
            .Where(uid => !string.IsNullOrWhiteSpace(uid) && currentVisibleTargetUids.Contains(uid))
            .ToHashSet(StringComparer.Ordinal)
            ?? new HashSet<string>(StringComparer.Ordinal);

        if (meshAccepted.Count > 0)
        {
            meshReadyAcked = await _missingFileMeshService
                .SignalCharacterDataReadyAsync(targetMeshSessions, meshOffer!.RequestId, meshAccepted, data, _runtimeCts.Token, dataByUid)
                .ConfigureAwait(false);

            var missingReadyAck = meshAccepted.Except(meshReadyAcked, StringComparer.Ordinal).ToList();
            if (missingReadyAck.Count > 0 && Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug(
                    "Mesh character push ready ACK was not received from {count} accepted recipient(s); not server-falling back because the offer was accepted and ready was retried",
                    missingReadyAck.Count);
            }
        }

        var fallbackUsers = targetUsers
            .Where(u => string.IsNullOrWhiteSpace(u.UID) || !meshAccepted.Contains(u.UID))
            .ToList();

        if (fallbackUsers.Count == 0)
            return true;

        // Server fallback is only for recipients that never accepted the Mesh offer at all
        // (old client/no Mesh/no offer response). If an offer was accepted, do not also hit
        // UserPushData just because the final ready ACK was slow or lost; that would create
        // a new duplicate-push source on top of any pre-existing push coalescing issues.
        return await PushCharacterDataServerFallbackAsync(data, fallbackUsers, dataByUid).ConfigureAwait(false);
    }

    private static void RestoreMeshRoutedPluginPayloads(CharacterData destination, CharacterData source)
    {
        // FileReplacement data still goes through the upload/share barrier, but rich plugin state
        // can safely ride mesh. Preserve it after the server-facing upload path has sanitized its copy.
        destination.GlamourerData = source.GlamourerData?.ToDictionary(k => k.Key, v => v.Value) ?? [];
        destination.CustomizePlusData = source.CustomizePlusData?.ToDictionary(k => k.Key, v => v.Value) ?? [];
        destination.ManipulationData = source.ManipulationData ?? string.Empty;
        destination.HeelsData = source.HeelsData ?? string.Empty;
        destination.HonorificData = source.HonorificData ?? string.Empty;
        destination.MoodlesData = source.MoodlesData ?? string.Empty;
        destination.PetNamesData = source.PetNamesData ?? string.Empty;
    }

    private void ForgetOutboundStateForUsers(IEnumerable<UserData> users)
    {
        lock (_pushQueueLock)
        {
            foreach (var user in users)
            {
                var uid = user.UID;
                if (string.IsNullOrWhiteSpace(uid))
                    continue;

                _lastOutboundFullSignatureByUid.Remove(uid);
                _lastOutboundAppearanceSignatureByUid.Remove(uid);
                _lastOutboundSendTickByUid.Remove(uid);
                _lastOutboundSidecarOnlyTickByUid.Remove(uid);
                _lastOutboundOtherSyncSignatureByUid.Remove(uid);
                _authorizedRecipientUids.Remove(uid);
                _usersToPushDataTo.Remove(uid);
                _queuedPushRevisionByUid.Remove(uid);
            }
        }
    }

    private int QueueVisibleUsersForPush(bool forceBarrier = false)
    {
        var visibleUsers = new List<UserData>();
        _pairManager.ForEachVisibleUser(user => visibleUsers.Add(user));
        return QueueUsersForPush(visibleUsers, forceBarrier);
    }

    private int QueueUsersForPush(IEnumerable<UserData> users, bool forceBarrier = false)
    {
        lock (_pushQueueLock)
        {
            PruneQueuedUsersToCurrentlyVisibleLocked();

            var revision = ++_queuedPushRevisionCounter;
            foreach (var user in users)
            {
                if (!IsCurrentlyVisibleOutboundRecipient(user))
                    continue;

                _usersToPushDataTo[user.UID] = user;
                _queuedPushRevisionByUid[user.UID] = revision;
            }

            if (forceBarrier && _usersToPushDataTo.Count > 0)
                _forceBarrierPending = true;

            return _usersToPushDataTo.Count;
        }
    }

    private void PruneQueuedUsersToCurrentlyVisibleLocked()
    {
        if (_usersToPushDataTo.Count == 0)
            return;

        foreach (var uid in _usersToPushDataTo.Keys.ToList())
        {
            if (!IsCurrentlyVisibleOutboundRecipient(uid))
            {
                _usersToPushDataTo.Remove(uid);
                _queuedPushRevisionByUid.Remove(uid);
            }
        }
    }

    private bool IsCurrentlyVisibleOutboundRecipient(UserData? user)
        => user != null && IsCurrentlyVisibleOutboundRecipient(user.UID);

    private bool IsCurrentlyVisibleOutboundRecipient(string? uid)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return false;

        var pair = _pairManager.GetPairByUID(uid);
        return pair?.IsVisible == true;
    }

    private List<UserData> SnapshotQueuedVisibleUsersLocked()
    {
        PruneQueuedUsersToCurrentlyVisibleLocked();
        return _usersToPushDataTo.Values.Where(IsCurrentlyVisibleOutboundRecipient).ToList();
    }

    private List<UserData> FilterUsersToCurrentlyVisibleRecipients(IEnumerable<UserData> users)
        => users.Where(IsCurrentlyVisibleOutboundRecipient).ToList();

    private static HashSet<string> CollectRecipientUids(IEnumerable<UserData> users)
    {
        return users
            .Select(u => u.UID)
            .Where(uid => !string.IsNullOrWhiteSpace(uid))
            .ToHashSet(StringComparer.Ordinal);
    }

    private Dictionary<string, CharacterData> BuildTargetedCharacterDataByUid(CharacterData baseData, IEnumerable<UserData> targetUsers)
    {
        var result = new Dictionary<string, CharacterData>(StringComparer.Ordinal);

        foreach (var user in targetUsers)
        {
            var uid = user.UID ?? string.Empty;
            if (string.IsNullOrWhiteSpace(uid) || result.ContainsKey(uid))
                continue;

            var targetData = baseData.DeepClone();
            var owner = ResolveLocalOtherSyncOwnerForTarget(uid);
            var active = !string.IsNullOrWhiteSpace(owner);
            _characterRavaSidecarUtility.TryEmbedOtherSync(targetData, uid, active, owner);
            result[uid] = targetData;
        }

        return result;
    }

    private string ResolveLocalOtherSyncOwnerForTarget(string uid)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return string.Empty;

        var pair = _pairManager.GetPairByUID(uid);
        if (pair == null || !pair.IsVisible || pair.OverrideOtherSync)
            return string.Empty;

        LocalOtherSyncClaim? storedClaim = null;
        lock (_pushQueueLock)
        {
            _localOtherSyncClaimByUid.TryGetValue(uid, out storedClaim);
        }

        var address = pair.PlayerCharacter;
        var preferFresh = pair.AutoPausedByOtherSync || pair.RemoteOtherSyncOverrideActive;

        if (storedClaim is { Active: true } && !string.IsNullOrWhiteSpace(storedClaim.Owner))
        {
            if (!_ipcManager.OtherSync.IsOwnerAvailable(storedClaim.Owner))
            {
                lock (_pushQueueLock)
                    _localOtherSyncClaimByUid.Remove(uid);
            }
            else if (address == nint.Zero)
            {
                // Once a pair has yielded/manual-paused through the shared vanilla teardown path,
                // the live Rava handler may intentionally be detached. The stored claim is still
                // the last real pair-loop handshake decision and is refreshed/released by
                // OtherSyncCurrentStateChangedMessage, so keep sending it until the local poller
                // proves otherwise.
                return storedClaim.Owner;
            }
            else if (_ipcManager.OtherSync.IsOwnerHandlingAddress(storedClaim.Owner, address, preferFresh: true))
            {
                return storedClaim.Owner;
            }
            else
            {
                lock (_pushQueueLock)
                    _localOtherSyncClaimByUid.Remove(uid);
            }
        }

        if (address == nint.Zero)
            return string.Empty;

        if (!_ipcManager.OtherSync.TryGetOwningOtherSync(address, out var owner, preferFresh: preferFresh))
            return string.Empty;

        return _ipcManager.OtherSync.IsOwnerAvailable(owner)
            && _ipcManager.OtherSync.IsOwnerHandlingAddress(owner, address, preferFresh: preferFresh)
            ? owner
            : string.Empty;
    }

    private string BuildCurrentOtherSyncOutboundSignatureForTarget(string uid)
        => BuildOtherSyncOutboundSignature(uid, ResolveLocalOtherSyncOwnerForTarget(uid));

    private static string BuildOtherSyncOutboundSignature(string uid, string? owner)
    {
        owner ??= string.Empty;
        owner = owner.Trim();
        var active = !string.IsNullOrWhiteSpace(owner);
        return $"{uid}|{(active ? "1" : "0")}|{(active ? owner : string.Empty)}";
    }

    private string BuildOtherSyncOutboundSignatureFromData(CharacterData data, string uid)
    {
        var clone = data.DeepClone();
        if (!_characterRavaSidecarUtility.TryExtractOtherSync(clone, uid, out var payload) || payload?.a != true)
            return BuildOtherSyncOutboundSignature(uid, string.Empty);

        return BuildOtherSyncOutboundSignature(uid, payload.o);
    }

    private static CharacterData GetTargetedCharacterData(IReadOnlyDictionary<string, CharacterData>? dataByUid, string uid, CharacterData fallback)
    {
        if (dataByUid != null && !string.IsNullOrWhiteSpace(uid) && dataByUid.TryGetValue(uid, out var data) && data != null)
            return data;

        return fallback;
    }

    private void RemoveQueuedUsers(IEnumerable<UserData> users, long sentRevision)
    {
        lock (_pushQueueLock)
        {
            foreach (var user in users)
            {
                if (string.IsNullOrWhiteSpace(user.UID))
                    continue;

                if (_queuedPushRevisionByUid.TryGetValue(user.UID, out var queuedRevision) && queuedRevision <= sentRevision)
                {
                    _queuedPushRevisionByUid.Remove(user.UID);
                    _usersToPushDataTo.Remove(user.UID);
                }
            }
        }
    }

    private void FrameworkOnUpdate(long nowTick)
    {
        if (!_dalamudUtil.GetIsPlayerPresent() || !_apiController.IsConnected) return;

        ProcessPendingHeelsUpdate(nowTick);
        ProcessPendingHonorificUpdate();
        ProcessPendingMoodlesUpdate(nowTick);
        ProcessPendingPetNamesUpdate();

        if (nowTick < _nextVisibilityScanTick)
            return;

        _nextVisibilityScanTick = nowTick + 100;

        var currentVisibleUsers = _currentVisiblePlayersSet;
        currentVisibleUsers.Clear();

        var newVisibleUsers = _newVisibleUsersBuffer;
        newVisibleUsers.Clear();

        var noLongerVisibleUsers = _noLongerVisibleUsersBuffer;
        noLongerVisibleUsers.Clear();

        var visiblePlayerAddresses = _visiblePlayerAddressesBuffer;
        visiblePlayerAddresses.Clear();
        var trackOtherSyncAddresses = _ipcManager.OtherSync.ShouldPollOwnership();

        _pairManager.ForEachVisiblePair(pair =>
        {
            var user = pair.UserData;
            currentVisibleUsers.Add(user);
            if (!_previouslyVisiblePlayersSet.Contains(user))
                newVisibleUsers.Add(user);

            if (trackOtherSyncAddresses)
            {
                var address = pair.PlayerCharacter;
                if (address != nint.Zero)
                    visiblePlayerAddresses.Add(address);
            }
        });

        foreach (var user in _previouslyVisiblePlayersSet)
        {
            if (!currentVisibleUsers.Contains(user))
                noLongerVisibleUsers.Add(user);
        }

        if (noLongerVisibleUsers.Count > 0)
        {
            _pairManager.ClearDisplayedPerformanceMetrics(noLongerVisibleUsers);
            ForgetOutboundStateForUsers(noLongerVisibleUsers);
        }

        _ipcManager.OtherSync.UpdateTrackedVisibleAddresses(trackOtherSyncAddresses ? visiblePlayerAddresses : null);

        _previouslyVisiblePlayersSet.Clear();
        _previouslyVisiblePlayersSet.UnionWith(currentVisibleUsers);

        if (newVisibleUsers.Count == 0) return;

        if (Logger.IsEnabled(LogLevel.Debug))
        {
            Logger.LogDebug("Scheduling character data push of {data} to {users}",
                _lastCreatedData?.DataHash.Value ?? string.Empty,
                string.Join(", ", newVisibleUsers.Select(k => k.AliasOrUID)));
        }

        QueueUsersForPush(newVisibleUsers);

        // A visibility gain is a recipient-state change, not a local payload change.
        // Send the current payload even if it matches what this UID saw shortly before.
        PushCharacterData(forceOutbound: true, bypassDuplicateSuppression: true);

    }

    private void PushCharacterData(bool forced = false, bool forceOutbound = false, bool includeSyncManifest = true, bool bypassDuplicateSuppression = false)
    {
        if (forced || forceOutbound || bypassDuplicateSuppression)
        {
            lock (_pushQueueLock)
            {
                if (forced)
                    _forceBarrierPending = true;

                if (forceOutbound)
                    _forceOutboundPending = true;

                if (bypassDuplicateSuppression)
                    _bypassDuplicateSuppressionPending = true;
            }
        }

        if (Interlocked.CompareExchange(ref _pushTaskQueuedOrRunning, 1, 0) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await _pushDataSemaphore.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
                try
                {
                    await WaitForOutboundPushQueueToSettleAsync(_runtimeCts.Token).ConfigureAwait(false);

                List<UserData> targetUsers;
                List<UserData> originalTargetUsers;
                CharacterData dataToSend;
                bool forceBarrier;
                bool forceOutboundSend;
                bool bypassDuplicateSuppression;
                long sendRevision;

                lock (_pushQueueLock)
                {
                    if (_lastCreatedData == null || _usersToPushDataTo.Count == 0)
                        return;

                    targetUsers = SnapshotQueuedVisibleUsersLocked();
                    originalTargetUsers = targetUsers;
                    if (targetUsers.Count == 0)
                        return;

                    dataToSend = _lastCreatedData.DeepClone();
                    forceBarrier = _forceBarrierPending;
                    forceOutboundSend = _forceOutboundPending;
                    bypassDuplicateSuppression = _bypassDuplicateSuppressionPending;
                    _forceBarrierPending = false;
                    _forceOutboundPending = false;
                    _bypassDuplicateSuppressionPending = false;
                    sendRevision = _queuedPushRevisionCounter;
                }

                var dataHash = dataToSend.DataHash.Value;
                if (!_characterAnalyzer.TryGetDisplayedHeaderMetrics(dataHash, out var displayedVramBytes, out var displayedTriangles))
                {
                    try
                    {
                        await _characterAnalyzer.WaitForAggregateMetricsAsync(dataHash, DisplayedMetricsPreSendWaitMs, _runtimeCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (!_runtimeCts.IsCancellationRequested)
                    {
                    }

                    _characterAnalyzer.TryGetDisplayedHeaderMetrics(dataHash, out displayedVramBytes, out displayedTriangles);
                }

                if (includeSyncManifest && _characterRavaSidecarUtility.TryEmbedSyncManifest(dataToSend, out var syncManifest) && Logger.IsEnabled(LogLevel.Trace))
                {
                    Logger.LogTrace(
                        "Embedded sync manifest for outbound data {hash}: {total} assets, {critical} critical, manifest {manifestFingerprint}",
                        dataHash,
                        syncManifest?.total ?? 0,
                        syncManifest?.critical ?? 0,
                        syncManifest?.mf ?? string.Empty);
                }

                if ((displayedVramBytes > 0 || displayedTriangles > 0)
                    && _characterRavaSidecarUtility.TryEmbedPerformance(dataToSend, displayedVramBytes, displayedTriangles))
                {
                    _lastSidecarBroadcastSignature = BuildSidecarSignature(dataHash, displayedVramBytes, displayedTriangles);
                    _lastSidecarBroadcastUtc = DateTime.UtcNow;
                }

                _characterRavaSidecarUtility.TryEmbedActiveSoundIndicator(dataToSend, _localActiveSyncIndicatorService.IsPlayingSound);

                targetUsers = FilterUsersToCurrentlyVisibleRecipients(targetUsers);
                if (targetUsers.Count == 0)
                {
                    RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    return;
                }

                var outboundDataByUid = BuildTargetedCharacterDataByUid(dataToSend, targetUsers);

                targetUsers = FilterUsersForOutboundSend(dataToSend, targetUsers, forceBarrier, forceOutboundSend, bypassDuplicateSuppression, outboundDataByUid);
                if (targetUsers.Count == 0)
                {
                    RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    return;
                }

                targetUsers = FilterUsersToCurrentlyVisibleRecipients(targetUsers);
                if (targetUsers.Count == 0)
                {
                    RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    return;
                }

                outboundDataByUid = BuildTargetedCharacterDataByUid(dataToSend, targetUsers);

                var targetRecipientUids = CollectRecipientUids(targetUsers);
                var targetMeshSessions = ResolveTargetMeshSessions(targetUsers);
                var barrierKey = BuildUploadBarrierKey(dataToSend);
                var payloadFileHashes = CollectPayloadFileHashes(dataToSend);

                var barrierKeyChanged = !string.Equals(_lastUploadBarrierKey, barrierKey, StringComparison.Ordinal);
                var hasUnauthorizedRecipients = !targetRecipientUids.IsSubsetOf(_authorizedRecipientUids);
                var fullBarrierRequired = forceBarrier || hasUnauthorizedRecipients || string.IsNullOrEmpty(_lastUploadBarrierKey);
                var addedPayloadHashes = GetAddedPayloadHashes(payloadFileHashes, fullBarrierRequired);
                var removedOnlyHashChange = barrierKeyChanged && addedPayloadHashes.Count == 0;

                if (Logger.IsEnabled(LogLevel.Information))
                    Logger.LogInformation("Pushing your user data to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                MissingFileMeshService.MeshCharacterPushOfferResult? meshOffer = null;
                if (targetMeshSessions.Count > 0 && targetRecipientUids.Count > 0)
                {
                    meshOffer = await _missingFileMeshService
                        .OfferCharacterDataForUsersAsync(targetMeshSessions, dataToSend, targetUsers, payloadFileHashes, _runtimeCts.Token, outboundDataByUid)
                        .ConfigureAwait(false);
                }

                var meshAcceptedAllRecipients = meshOffer != null && targetRecipientUids.IsSubsetOf(meshOffer.AcceptedUids);

                IReadOnlyCollection<string>? scopedBarrierHashes = null;
                var mustRunUploadBarrier = false;
                if (meshAcceptedAllRecipients)
                {
                    scopedBarrierHashes = meshOffer!.MissingHashes;
                    mustRunUploadBarrier = scopedBarrierHashes.Count > 0;
                }
                else if (fullBarrierRequired)
                {
                    scopedBarrierHashes = null;
                    mustRunUploadBarrier = payloadFileHashes.Count > 0;
                }
                else if (barrierKeyChanged && addedPayloadHashes.Count > 0)
                {
                    scopedBarrierHashes = addedPayloadHashes;
                    mustRunUploadBarrier = true;
                }

                if (Logger.IsEnabled(LogLevel.Debug) && barrierKeyChanged)
                {
                    Logger.LogDebug(
                        "Outbound hash-set diff for {hash}: added={added}, removedOnly={removedOnly}, fullBarrier={fullBarrier}, meshAcceptedAll={meshAll}, meshMissing={meshMissing}",
                        dataToSend.DataHash.Value,
                        addedPayloadHashes.Count,
                        removedOnlyHashChange,
                        fullBarrierRequired,
                        meshAcceptedAllRecipients,
                        meshOffer?.MissingHashes.Count ?? -1);
                }

                if (!mustRunUploadBarrier)
                {
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("Completing metadata-only user data push to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                    outboundDataByUid = BuildTargetedCharacterDataByUid(dataToSend, targetUsers);

                    if (await CompleteMeshCharacterPushOrFallbackAsync(dataToSend, targetUsers, meshOffer, targetMeshSessions, outboundDataByUid).ConfigureAwait(false))
                    {
                        RecordOutboundSend(dataToSend, targetUsers, outboundDataByUid);

                        if (barrierKeyChanged || string.IsNullOrEmpty(_lastUploadBarrierKey))
                            RecordUploadBarrierState(barrierKey, payloadFileHashes);

                        _authorizedRecipientUids.UnionWith(targetRecipientUids);
                        RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    }
                    return;
                }

                if (forceOutboundSend && Logger.IsEnabled(LogLevel.Debug))
                {
                    Logger.LogDebug(
                        "Delaying forced outbound character data for {users} until upload/share barrier is complete",
                        string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));
                }

                var meshSourceData = dataToSend.DeepClone();
                _uploadingCharacterData = dataToSend;

                Logger.LogDebug(
                    "Starting UploadTask for {hash}, barrierKeyChanged: {keyChanged}, unauthorizedRecipients: {unauth}, forced: {frc}, meshAcceptedAll: {meshAll}, meshMissing: {meshMissing}, scopedHashes: {scopedHashes}",
                    dataToSend.DataHash.Value, barrierKeyChanged, hasUnauthorizedRecipients, forceBarrier, meshAcceptedAllRecipients, meshOffer?.MissingHashes.Count ?? -1, scopedBarrierHashes?.Count ?? -1);

                _fileUploadTask = _fileTransferManager.UploadFiles(meshSourceData.DeepClone(), targetUsers, scopedBarrierHashes);

                if (_fileUploadTask == null)
                {
                    _uploadingCharacterData = null;
                    return;
                }

                var uploadResult = await _fileUploadTask.ConfigureAwait(false);
                if (!uploadResult.Success)
                {
                    Logger.LogWarning("Upload/share failed for {hash}: {error}", _uploadingCharacterData?.DataHash.Value ?? "UNKNOWN", uploadResult.Error ?? "Unknown error");
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                    return;
                }

                dataToSend = uploadResult.Data;
                RestoreMeshRoutedPluginPayloads(dataToSend, meshSourceData);

                targetUsers = FilterUsersToCurrentlyVisibleRecipients(targetUsers);
                if (targetUsers.Count == 0)
                {
                    RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                    return;
                }

                if (Logger.IsEnabled(LogLevel.Debug))
                    Logger.LogDebug("Upload/share barrier complete; releasing user data push to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                outboundDataByUid = BuildTargetedCharacterDataByUid(dataToSend, targetUsers);

                if (!await CompleteMeshCharacterPushOrFallbackAsync(dataToSend, targetUsers, meshOffer, targetMeshSessions, outboundDataByUid).ConfigureAwait(false))
                {
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                    return;
                }

                RecordOutboundSend(dataToSend, targetUsers, outboundDataByUid);

                if (barrierKeyChanged || string.IsNullOrEmpty(_lastUploadBarrierKey))
                    RecordUploadBarrierState(barrierKey, payloadFileHashes);

                _authorizedRecipientUids.UnionWith(targetRecipientUids);

                RemoveQueuedUsers(originalTargetUsers, sendRevision);
                _uploadingCharacterData = null;
                _fileUploadTask = null;
            }
                finally
                {
                    _pushDataSemaphore.Release();
                }
            }
            finally
            {
                Volatile.Write(ref _pushTaskQueuedOrRunning, 0);

                bool hasQueuedUsers;
                lock (_pushQueueLock)
                    hasQueuedUsers = _lastCreatedData != null && _usersToPushDataTo.Count > 0;

                if (hasQueuedUsers && !_runtimeCts.IsCancellationRequested)
                    PushCharacterData();
            }
        });
    }

    private async Task WaitForOutboundPushQueueToSettleAsync(CancellationToken token)
    {
        var waitedMs = 0;

        while (waitedMs < MaxOutboundPushCoalesceWaitMs)
        {
            long observedRevision;
            lock (_pushQueueLock)
                observedRevision = _queuedPushRevisionCounter;

            await Task.Delay(OutboundPushCoalesceDelayMs, token).ConfigureAwait(false);
            waitedMs += OutboundPushCoalesceDelayMs;

            lock (_pushQueueLock)
            {
                if (observedRevision == _queuedPushRevisionCounter)
                    return;
            }
        }
    }

    private void ProcessPendingHeelsUpdate(long nowTick)
    {
        if (!_hasPendingHeelsUpdate)
            return;

        if (_pendingHeelsApplyTick >= 0 && nowTick < _pendingHeelsApplyTick)
            return;

        _hasPendingHeelsUpdate = false;
        _pendingHeelsApplyTick = -1;

        if (!_apiController.IsConnected || !_dalamudUtil.GetIsPlayerPresent())
            return;

        if (_lastCreatedData == null)
            return;

        var newHeelsData = _pendingHeelsData ?? string.Empty;

        if (string.Equals(newHeelsData, _lastHeelsData, StringComparison.Ordinal))
            return;

        var patched = _lastCreatedData.DeepClone();
        patched.HeelsData = newHeelsData;

        var patchedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(patched);
        if (string.Equals(patchedPayloadFingerprint, _lastCreatedDataPayloadFingerprint, StringComparison.Ordinal))
            return;

        _lastCreatedData = patched;
        _lastCreatedDataPayloadFingerprint = patchedPayloadFingerprint;
        _lastHeelsData = newHeelsData;

        var queuedCount = QueueVisibleUsersForPush();

        if (queuedCount == 0)
            return;

        Logger.LogDebug("Fast-path heels update for {count} visible players", queuedCount);
        PushCharacterData();
    }

    private void ProcessPendingHonorificUpdate()
    {
        if (!_hasPendingHonorificUpdate)
            return;

        _hasPendingHonorificUpdate = false;

        ApplyMetadataUpdate(
            _pendingHonorificData ?? string.Empty,
            d => d.HonorificData,
            (d, v) => d.HonorificData = v,
            v => _lastHonorificData = v,
            "honorific");
    }

    private void ProcessPendingMoodlesUpdate(long nowTick)
    {
        if (!_hasPendingMoodlesUpdate)
            return;

        if (_pendingMoodlesApplyTick >= 0 && nowTick < _pendingMoodlesApplyTick)
            return;

        _hasPendingMoodlesUpdate = false;
        _pendingMoodlesApplyTick = -1;

        ApplyMetadataUpdate(
            _pendingMoodlesData ?? string.Empty,
            d => d.MoodlesData,
            (d, v) => d.MoodlesData = v,
            v => _lastMoodlesData = v,
            "moodles");
    }

    private void ProcessPendingPetNamesUpdate()
    {
        if (!_hasPendingPetNamesUpdate)
            return;

        _hasPendingPetNamesUpdate = false;

        ApplyMetadataUpdate(
            _pendingPetNamesData ?? string.Empty,
            d => d.PetNamesData,
            (d, v) => d.PetNamesData = v,
            v => _lastPetNamesData = v,
            "pet names");
    }

    private void ApplyMetadataUpdate(string newValue, Func<CharacterData, string> currentSelector, Action<CharacterData, string> applyValue, Action<string> storeValue, string logName)
    {
        if (!_apiController.IsConnected || !_dalamudUtil.GetIsPlayerPresent())
            return;

        if (_lastCreatedData == null)
            return;

        newValue ??= string.Empty;

        var currentValue = currentSelector(_lastCreatedData) ?? string.Empty;
        if (string.Equals(newValue, currentValue, StringComparison.Ordinal))
            return;

        var patched = _lastCreatedData.DeepClone();
        applyValue(patched, newValue);

        var patchedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(patched);
        if (string.Equals(patchedPayloadFingerprint, _lastCreatedDataPayloadFingerprint, StringComparison.Ordinal))
            return;

        _lastCreatedData = patched;
        _lastCreatedDataPayloadFingerprint = patchedPayloadFingerprint;
        storeValue(newValue);

        var queuedCount = QueueVisibleUsersForPush();

        if (queuedCount == 0)
            return;

        Logger.LogDebug("Fast-path {kind} update for {count} visible players", logName, queuedCount);
        PushCharacterData();
    }
}