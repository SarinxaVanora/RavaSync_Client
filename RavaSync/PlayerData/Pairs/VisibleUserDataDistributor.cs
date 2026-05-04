using RavaSync.API.Data;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using RavaSync.Interop.Ipc;
using Microsoft.Extensions.Logging;
using RavaSync.PlayerData.Services;

namespace RavaSync.PlayerData.Pairs;

public class VisibleUserDataDistributor : DisposableMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileUploadManager _fileTransferManager;
    private readonly PairManager _pairManager;
    private readonly IpcManager _ipcManager;
    private readonly CharacterAnalyzer _characterAnalyzer;
    private readonly CharacterRavaSidecarUtility _characterRavaSidecarUtility;
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
    private readonly object _pushQueueLock = new();
    private readonly CancellationTokenSource _runtimeCts = new();
    private long _queuedPushRevisionCounter;
    private string _lastHeelsData = string.Empty;
    private long _pendingHeelsApplyTick = -1;
    private string _lastUploadBarrierKey = string.Empty;
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
    private long _pendingMoodlesApplyTick = -1;
    private long _nextLocalPlayerAddressRefreshTick;
    private long _nextVisibilityScanTick;
    private nint _localPlayerAddress;
    private string _lastSidecarBroadcastSignature = string.Empty;
    private DateTime _lastSidecarBroadcastUtc = DateTime.MinValue;
    private readonly Dictionary<string, string> _lastOutboundFullSignatureByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _lastOutboundAppearanceSignatureByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastOutboundSendTickByUid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastOutboundSidecarOnlyTickByUid = new(StringComparer.Ordinal);
    private const int DisplayedMetricsPreSendWaitMs = 1500;
    private const int DuplicateOutboundSuppressWindowMs = 10_000;
    private const int SidecarOnlyOutboundMinIntervalMs = 60_000;
    private static readonly TimeSpan SidecarRefreshMinInterval = TimeSpan.FromSeconds(60);

    public VisibleUserDataDistributor(ILogger<VisibleUserDataDistributor> logger, ApiController apiController, DalamudUtilService dalamudUtil,
        PairManager pairManager, MareMediator mediator, FileUploadManager fileTransferManager, IpcManager ipcManager, CharacterAnalyzer characterAnalyzer, CharacterRavaSidecarUtility characterRavaSidecarUtility) : base(logger, mediator)
    {
        _apiController = apiController;
        _dalamudUtil = dalamudUtil;
        _pairManager = pairManager;
        _fileTransferManager = fileTransferManager;
        _ipcManager = ipcManager;
        _characterAnalyzer = characterAnalyzer;
        _characterRavaSidecarUtility = characterRavaSidecarUtility;
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

                if (barrierKeyChanged)
                {
                    _lastUploadBarrierKey = string.Empty;
                    _authorizedRecipientUids.Clear();
                }

                Logger.LogTrace("Storing new data hash {hash}/{payload}", newData.DataHash.Value, newPayloadFingerprint);
                _lastSidecarBroadcastSignature = string.Empty;
                PushToAllVisibleUsers(forced: firstPayload || barrierKeyChanged, forceOutbound: msg.ForceOutbound);
            }
            else
            {
                Logger.LogTrace("Data hash {hash}/{payload} equal to stored data", newData.DataHash.Value, newPayloadFingerprint);

                if (msg.ForceOutbound)
                {
                    _lastCreatedData = newData;
                    _lastCreatedDataPayloadFingerprint = newPayloadFingerprint;
                    PushToAllVisibleUsers(forceOutbound: true);
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

            if (DateTime.UtcNow - _lastSidecarBroadcastUtc < SidecarRefreshMinInterval)
                return;

            var queuedCount = QueueVisibleUsersForPush();

            if (queuedCount == 0)
                return;

            Logger.LogDebug("Scheduling sidecar-backed perf refresh for {count} visible players", queuedCount);
            PushCharacterData(includeSyncManifest: false);
        });

        //Mediator.Subscribe<ConnectedMessage>(this, (_) => PushToAllVisibleUsers(forced: true));

        Mediator.Subscribe<ConnectedMessage>(this, (_) =>
        {
            QueueVisibleUsersForPush(forceBarrier: true);
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
            _authorizedRecipientUids.Clear();
            _lastCreatedData = null;
            _lastCreatedDataPayloadFingerprint = string.Empty;
            _lastHeelsData = string.Empty;
            _lastHonorificData = string.Empty;
            _lastMoodlesData = string.Empty;
            _lastPetNamesData = string.Empty;
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
            _lastOutboundFullSignatureByUid.Clear();
            _lastOutboundAppearanceSignatureByUid.Clear();
            _lastOutboundSendTickByUid.Clear();
            _lastOutboundSidecarOnlyTickByUid.Clear();
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

        var hashes = data.FileReplacements
            .SelectMany(kvp => kvp.Value)
            .Where(fr => string.IsNullOrWhiteSpace(fr.FileSwapPath) && !string.IsNullOrWhiteSpace(fr.Hash))
            .Select(fr => fr.Hash)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(h => h, StringComparer.Ordinal);

        return string.Join('|', hashes);
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

    private List<UserData> FilterUsersForOutboundSend(CharacterData dataToSend, List<UserData> targetUsers, bool forceBarrier, bool forceOutbound)
    {
        if (targetUsers.Count == 0)
            return targetUsers;

        var nowTick = Environment.TickCount64;
        var fullSignature = BuildFullOutboundSignature(dataToSend);
        var appearanceSignature = BuildAppearanceOutboundSignature(dataToSend);
        var filtered = new List<UserData>(targetUsers.Count);

        lock (_pushQueueLock)
        {
            foreach (var user in targetUsers)
            {
                var uid = user.UID;
                if (string.IsNullOrWhiteSpace(uid))
                    continue;

                _lastOutboundFullSignatureByUid.TryGetValue(uid, out var lastFullSignature);
                _lastOutboundAppearanceSignatureByUid.TryGetValue(uid, out var lastAppearanceSignature);
                _lastOutboundSendTickByUid.TryGetValue(uid, out var lastSendTick);
                _lastOutboundSidecarOnlyTickByUid.TryGetValue(uid, out var lastSidecarOnlyTick);

                var sameFullPayload = string.Equals(fullSignature, lastFullSignature, StringComparison.Ordinal);
                var sameAppearancePayload = string.Equals(appearanceSignature, lastAppearanceSignature, StringComparison.Ordinal);
                var elapsedSinceLastSend = unchecked(nowTick - lastSendTick);
                var elapsedSinceSidecarOnly = unchecked(nowTick - lastSidecarOnlyTick);

                if (!forceOutbound && sameFullPayload && elapsedSinceLastSend >= 0 && elapsedSinceLastSend < DuplicateOutboundSuppressWindowMs)
                    continue;

                var sidecarOnlyPayload = sameAppearancePayload && !sameFullPayload;
                if (sidecarOnlyPayload
                    && elapsedSinceSidecarOnly >= 0
                    && elapsedSinceSidecarOnly < SidecarOnlyOutboundMinIntervalMs
                    && !forceBarrier
                    && !forceOutbound)
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

    private void RecordOutboundSend(CharacterData sentData, IEnumerable<UserData> sentUsers)
    {
        var nowTick = Environment.TickCount64;
        var fullSignature = BuildFullOutboundSignature(sentData);
        var appearanceSignature = BuildAppearanceOutboundSignature(sentData);

        lock (_pushQueueLock)
        {
            foreach (var user in sentUsers)
            {
                var uid = user.UID;
                if (string.IsNullOrWhiteSpace(uid))
                    continue;

                _lastOutboundFullSignatureByUid.TryGetValue(uid, out var lastFullSignature);
                _lastOutboundAppearanceSignatureByUid.TryGetValue(uid, out var lastAppearanceSignature);

                var sameAppearancePayload = string.Equals(appearanceSignature, lastAppearanceSignature, StringComparison.Ordinal);
                var sameFullPayload = string.Equals(fullSignature, lastFullSignature, StringComparison.Ordinal);
                var sidecarOnlyPayload = sameAppearancePayload && !sameFullPayload;

                _lastOutboundFullSignatureByUid[uid] = fullSignature;
                _lastOutboundAppearanceSignatureByUid[uid] = appearanceSignature;
                _lastOutboundSendTickByUid[uid] = nowTick;

                if (sidecarOnlyPayload)
                    _lastOutboundSidecarOnlyTickByUid[uid] = nowTick;
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
            var revision = ++_queuedPushRevisionCounter;
            foreach (var user in users)
            {
                if (string.IsNullOrWhiteSpace(user.UID))
                    continue;

                _usersToPushDataTo[user.UID] = user;
                _queuedPushRevisionByUid[user.UID] = revision;
            }

            if (forceBarrier)
                _forceBarrierPending = true;

            return _usersToPushDataTo.Count;
        }
    }

    private List<UserData> SnapshotQueuedUsers()
    {
        return _usersToPushDataTo.Values.ToList();
    }

    private static HashSet<string> CollectRecipientUids(IEnumerable<UserData> users)
    {
        return users
            .Select(u => u.UID)
            .Where(uid => !string.IsNullOrWhiteSpace(uid))
            .ToHashSet(StringComparer.Ordinal);
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

        _pairManager.ForEachVisiblePair(pair =>
        {
            var user = pair.UserData;
            currentVisibleUsers.Add(user);
            if (!_previouslyVisiblePlayersSet.Contains(user))
                newVisibleUsers.Add(user);

            var address = pair.PlayerCharacter;
            if (address != nint.Zero)
                visiblePlayerAddresses.Add(address);
        });

        foreach (var user in _previouslyVisiblePlayersSet)
        {
            if (!currentVisibleUsers.Contains(user))
                noLongerVisibleUsers.Add(user);
        }

        if (noLongerVisibleUsers.Count > 0)
            _pairManager.ClearDisplayedPerformanceMetrics(noLongerVisibleUsers);

        _ipcManager.OtherSync.UpdateTrackedVisibleAddresses(visiblePlayerAddresses);

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

        PushCharacterData();

    }

    private void PushCharacterData(bool forced = false, bool forceOutbound = false, bool includeSyncManifest = true)
    {
        if (forced || forceOutbound)
        {
            lock (_pushQueueLock)
            {
                if (forced)
                    _forceBarrierPending = true;

                if (forceOutbound)
                    _forceOutboundPending = true;
            }
        }

        _ = Task.Run(async () =>
        {
            await _pushDataSemaphore.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
            try
            {
                List<UserData> targetUsers;
                List<UserData> originalTargetUsers;
                CharacterData dataToSend;
                bool forceBarrier;
                bool forceOutboundSend;
                long sendRevision;

                lock (_pushQueueLock)
                {
                    if (_lastCreatedData == null || _usersToPushDataTo.Count == 0)
                        return;

                    targetUsers = SnapshotQueuedUsers();
                    originalTargetUsers = targetUsers;
                    if (targetUsers.Count == 0)
                        return;

                    dataToSend = _lastCreatedData.DeepClone();
                    forceBarrier = _forceBarrierPending;
                    forceOutboundSend = _forceOutboundPending;
                    _forceBarrierPending = false;
                    _forceOutboundPending = false;
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

                targetUsers = FilterUsersForOutboundSend(dataToSend, targetUsers, forceBarrier, forceOutboundSend);
                if (targetUsers.Count == 0)
                {
                    RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    return;
                }

                var targetRecipientUids = CollectRecipientUids(targetUsers);
                var barrierKey = BuildUploadBarrierKey(dataToSend);

                var barrierKeyChanged = !string.Equals(_lastUploadBarrierKey, barrierKey, StringComparison.Ordinal);
                var hasUnauthorizedRecipients = !targetRecipientUids.IsSubsetOf(_authorizedRecipientUids);

                var mustRunUploadBarrier =
                    forceBarrier
                    || barrierKeyChanged
                    || hasUnauthorizedRecipients;

                if (!mustRunUploadBarrier)
                {
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("Sending metadata-only appearance update to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                    if (await _apiController.PushCharacterData(dataToSend, targetUsers).ConfigureAwait(false))
                    {
                        RecordOutboundSend(dataToSend, targetUsers);
                        RemoveQueuedUsers(originalTargetUsers, sendRevision);
                    }
                    return;
                }

                _uploadingCharacterData = dataToSend;

                Logger.LogDebug(
                    "Starting UploadTask for {hash}, barrierKeyChanged: {keyChanged}, unauthorizedRecipients: {unauth}, forced: {frc}",
                    dataToSend.DataHash.Value, barrierKeyChanged, hasUnauthorizedRecipients, forceBarrier);

                _fileUploadTask = _fileTransferManager.UploadFiles(_uploadingCharacterData, targetUsers);

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

                if (Logger.IsEnabled(LogLevel.Debug))
                    Logger.LogDebug("Sending your appearance to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                if (!await _apiController.PushCharacterData(dataToSend, targetUsers).ConfigureAwait(false))
                {
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                    return;
                }

                RecordOutboundSend(dataToSend, targetUsers);

                if (barrierKeyChanged)
                {
                    _lastUploadBarrierKey = barrierKey;
                    _authorizedRecipientUids.Clear();
                }

                _authorizedRecipientUids.UnionWith(targetRecipientUids);

                RemoveQueuedUsers(originalTargetUsers, sendRevision);
                _uploadingCharacterData = null;
                _fileUploadTask = null;
            }
            finally
            {
                _pushDataSemaphore.Release();
            }
        });
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