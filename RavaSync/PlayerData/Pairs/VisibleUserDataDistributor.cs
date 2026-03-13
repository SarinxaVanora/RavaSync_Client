using RavaSync.API.Data;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using Microsoft.Extensions.Logging;

namespace RavaSync.PlayerData.Pairs;

public class VisibleUserDataDistributor : DisposableMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileUploadManager _fileTransferManager;
    private readonly PairManager _pairManager;
    private CharacterData? _lastCreatedData;
    private CharacterData? _uploadingCharacterData = null;
    private readonly List<UserData> _previouslyVisiblePlayers = [];
    private Task<(CharacterData Data, bool Success, string? Error)>? _fileUploadTask = null;
    private readonly HashSet<UserData> _usersToPushDataTo = [];
    private readonly HashSet<UserData> _previouslyVisiblePlayersSet = [];
    private readonly List<UserData> _newVisibleUsersBuffer = [];
    private readonly SemaphoreSlim _pushDataSemaphore = new(1, 1);
    private readonly CancellationTokenSource _runtimeCts = new();
    private string _lastHeelsData = string.Empty;
    private CancellationTokenSource? _heelsDebounceCts;
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

    private CancellationTokenSource? _moodlesDebounceCts;


    public VisibleUserDataDistributor(ILogger<VisibleUserDataDistributor> logger, ApiController apiController, DalamudUtilService dalamudUtil,
        PairManager pairManager, MareMediator mediator, FileUploadManager fileTransferManager) : base(logger, mediator)
    {
        _apiController = apiController;
        _dalamudUtil = dalamudUtil;
        _pairManager = pairManager;
        _fileTransferManager = fileTransferManager;
        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, (_) => FrameworkOnUpdate());
        Mediator.Subscribe<CharacterDataCreatedMessage>(this, (msg) =>
        {
            var newData = msg.CharacterData;
            if (_lastCreatedData == null || (!string.Equals(newData.DataHash.Value, _lastCreatedData.DataHash.Value, StringComparison.Ordinal)))
            {
                var oldBarrierKey = _lastCreatedData != null ? BuildUploadBarrierKey(_lastCreatedData) : string.Empty;
                var newBarrierKey = BuildUploadBarrierKey(newData);

                _lastCreatedData = newData;
                _lastHeelsData = newData.HeelsData ?? string.Empty;
                _lastHonorificData = newData.HonorificData ?? string.Empty;
                _lastMoodlesData = newData.MoodlesData ?? string.Empty;
                _lastPetNamesData = newData.PetNamesData ?? string.Empty;

                if (!string.Equals(oldBarrierKey, newBarrierKey, StringComparison.Ordinal))
                {
                    _lastUploadBarrierKey = string.Empty;
                    _authorizedRecipientUids.Clear();
                }

                Logger.LogTrace("Storing new data hash {hash}", newData.DataHash.Value);
                PushToAllVisibleUsers(forced: true);
            }
            else
            {
                Logger.LogTrace("Data hash {hash} equal to stored data", newData.DataHash.Value);
            }
        });

        Mediator.Subscribe<ConnectedMessage>(this, (_) => PushToAllVisibleUsers(forced: true));
        Mediator.Subscribe<DisconnectedMessage>(this, (_) =>
        {
            _previouslyVisiblePlayers.Clear();
            _previouslyVisiblePlayersSet.Clear();
            _usersToPushDataTo.Clear();
            _newVisibleUsersBuffer.Clear();
            _uploadingCharacterData = null;
            _fileUploadTask = null;
            _lastUploadBarrierKey = string.Empty;
            _authorizedRecipientUids.Clear();

            try
            {
                _heelsDebounceCts?.Cancel();
                _heelsDebounceCts?.Dispose();
            }
            catch
            {
                // ignore
            }

            _heelsDebounceCts = null;

            try
            {
                _moodlesDebounceCts?.Cancel();
                _moodlesDebounceCts?.Dispose();
            }
            catch
            {
                // ignore
            }

            _moodlesDebounceCts = null;
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
            try
            {
                _heelsDebounceCts?.Cancel();
                _heelsDebounceCts?.Dispose();
            }
            catch
            {
                // ignore
            }

            try
            {
                _moodlesDebounceCts?.Cancel();
                _moodlesDebounceCts?.Dispose();
            }
            catch
            {
                // ignore
            }

            _runtimeCts.Cancel();
            _runtimeCts.Dispose();
        }

        base.Dispose(disposing);
    }

    private void PushToAllVisibleUsers(bool forced = false)
    {
        foreach (var user in _pairManager.GetVisibleUsers())
        {
            _usersToPushDataTo.Add(user);
        }

        if (_usersToPushDataTo.Count > 0)
        {
            Logger.LogDebug("Pushing data {hash} for {count} visible players", _lastCreatedData?.DataHash.Value ?? "UNKNOWN", _usersToPushDataTo.Count);
            PushCharacterData(forced);
        }
    }

    private void HandleHeelsOffsetChanged(string offset)
    {
        if (!_apiController.IsConnected)
            return;

        offset ??= string.Empty;

        if (string.Equals(offset, _lastHeelsData, StringComparison.Ordinal))
            return;

        try
        {
            _heelsDebounceCts?.Cancel();
            _heelsDebounceCts?.Dispose();
        }
        catch
        {
            // ignore
        }

        _heelsDebounceCts = new CancellationTokenSource();
        var token = _heelsDebounceCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(125, token).ConfigureAwait(false);
                _pendingHeelsData = offset;
                _hasPendingHeelsUpdate = true;
            }
            catch (OperationCanceledException)
            {
                // superseded by a newer heels update
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to queue fast-path heels update");
            }
        });
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

        try
        {
            _moodlesDebounceCts?.Cancel();
            _moodlesDebounceCts?.Dispose();
        }
        catch
        {
            // ignore
        }

        _moodlesDebounceCts = new CancellationTokenSource();
        var token = _moodlesDebounceCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(125, token).ConfigureAwait(false);
                _pendingMoodlesData = moodlesData;
                _hasPendingMoodlesUpdate = true;
            }
            catch (OperationCanceledException)
            {
                // superseded
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to queue fast-path moodles update");
            }
        });
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

    private static HashSet<string> CollectRecipientUids(IEnumerable<UserData> users)
    {
        return users
            .Select(u => u.UID)
            .Where(uid => !string.IsNullOrWhiteSpace(uid))
            .ToHashSet(StringComparer.Ordinal);
    }

    private void RemoveQueuedUsers(IEnumerable<UserData> users)
    {
        foreach (var user in users)
            _usersToPushDataTo.Remove(user);
    }

    private void FrameworkOnUpdate()
    {
        if (!_dalamudUtil.GetIsPlayerPresent() || !_apiController.IsConnected) return;

        ProcessPendingHeelsUpdate();
        ProcessPendingHonorificUpdate();
        ProcessPendingMoodlesUpdate();
        ProcessPendingPetNamesUpdate();

        var allVisibleUsers = _pairManager.GetVisibleUsers();

        var newVisibleUsers = _newVisibleUsersBuffer;
        newVisibleUsers.Clear();

        for (int i = 0; i < allVisibleUsers.Count; i++)
        {
            var u = allVisibleUsers[i];
            if (!_previouslyVisiblePlayersSet.Contains(u))
                newVisibleUsers.Add(u);
        }

        _previouslyVisiblePlayers.Clear();
        _previouslyVisiblePlayers.AddRange(allVisibleUsers);

        _previouslyVisiblePlayersSet.Clear();
        for (int i = 0; i < allVisibleUsers.Count; i++)
            _previouslyVisiblePlayersSet.Add(allVisibleUsers[i]);

        if (newVisibleUsers.Count == 0) return;

        if (Logger.IsEnabled(LogLevel.Debug))
        {
            Logger.LogDebug("Scheduling character data push of {data} to {users}",
                _lastCreatedData?.DataHash.Value ?? string.Empty,
                string.Join(", ", newVisibleUsers.Select(k => k.AliasOrUID)));
        }

        for (int i = 0; i < newVisibleUsers.Count; i++)
            _usersToPushDataTo.Add(newVisibleUsers[i]);

        PushCharacterData();

    }

    private void PushCharacterData(bool forced = false)
    {
        if (_lastCreatedData == null || _usersToPushDataTo.Count == 0) return;

        var targetUsers = _usersToPushDataTo.ToList();
        if (targetUsers.Count == 0) return;

        var dataToSend = _lastCreatedData.DeepClone();

        _ = Task.Run(async () =>
        {
            var targetRecipientUids = CollectRecipientUids(targetUsers);
            var barrierKey = BuildUploadBarrierKey(dataToSend);

            var barrierKeyChanged = !string.Equals(_lastUploadBarrierKey, barrierKey, StringComparison.Ordinal);
            var hasUnauthorizedRecipients = !targetRecipientUids.IsSubsetOf(_authorizedRecipientUids);

            var mustRunUploadBarrier =
                forced
                || barrierKeyChanged
                || hasUnauthorizedRecipients;

            if (!mustRunUploadBarrier)
            {
                await _pushDataSemaphore.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
                try
                {
                    if (targetUsers.Count == 0) return;
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("Sending metadata-only appearance update to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                    await _apiController.PushCharacterData(dataToSend, targetUsers).ConfigureAwait(false);
                    RemoveQueuedUsers(targetUsers);
                }
                finally
                {
                    _pushDataSemaphore.Release();
                }

                return;
            }

            _uploadingCharacterData = dataToSend;

            Logger.LogDebug(
                "Starting UploadTask for {hash}, barrierKeyChanged: {keyChanged}, unauthorizedRecipients: {unauth}, forced: {frc}",
                dataToSend.DataHash.Value, barrierKeyChanged, hasUnauthorizedRecipients, forced);

            _fileUploadTask = _fileTransferManager.UploadFiles(_uploadingCharacterData, targetUsers);

            if (_fileUploadTask != null)
            {
                var uploadResult = await _fileUploadTask.ConfigureAwait(false);
                if (!uploadResult.Success)
                {
                    Logger.LogWarning("Upload/share failed for {hash}: {error}", _uploadingCharacterData?.DataHash.Value ?? "UNKNOWN", uploadResult.Error ?? "Unknown error");
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                    return;
                }

                dataToSend = uploadResult.Data;

                await _pushDataSemaphore.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
                try
                {
                    if (targetUsers.Count == 0) return;
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("Sending your appearance to {users}", string.Join(", ", targetUsers.Select(k => k.AliasOrUID)));

                    await _apiController.PushCharacterData(dataToSend, targetUsers).ConfigureAwait(false);

                    if (barrierKeyChanged)
                    {
                        _lastUploadBarrierKey = barrierKey;
                        _authorizedRecipientUids.Clear();
                    }

                    _authorizedRecipientUids.UnionWith(targetRecipientUids);

                    RemoveQueuedUsers(targetUsers);
                    _uploadingCharacterData = null;
                    _fileUploadTask = null;
                }
                finally
                {
                    _pushDataSemaphore.Release();
                }
            }
        });
    }

    private void ProcessPendingHeelsUpdate()
    {
        if (!_hasPendingHeelsUpdate)
            return;

        _hasPendingHeelsUpdate = false;

        if (!_apiController.IsConnected || !_dalamudUtil.GetIsPlayerPresent())
            return;

        if (_lastCreatedData == null)
            return;

        var newHeelsData = _pendingHeelsData ?? string.Empty;

        if (string.Equals(newHeelsData, _lastHeelsData, StringComparison.Ordinal))
            return;

        var patched = _lastCreatedData.DeepClone();
        patched.HeelsData = newHeelsData;

        if (string.Equals(patched.DataHash.Value, _lastCreatedData.DataHash.Value, StringComparison.Ordinal))
            return;

        _lastCreatedData = patched;
        _lastHeelsData = newHeelsData;

        foreach (var user in _pairManager.GetVisibleUsers())
            _usersToPushDataTo.Add(user);

        if (_usersToPushDataTo.Count == 0)
            return;

        Logger.LogDebug("Fast-path heels update for {count} visible players", _usersToPushDataTo.Count);
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

    private void ProcessPendingMoodlesUpdate()
    {
        if (!_hasPendingMoodlesUpdate)
            return;

        _hasPendingMoodlesUpdate = false;

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

        if (string.Equals(patched.DataHash.Value, _lastCreatedData.DataHash.Value, StringComparison.Ordinal))
            return;

        _lastCreatedData = patched;
        storeValue(newValue);

        foreach (var user in _pairManager.GetVisibleUsers())
            _usersToPushDataTo.Add(user);

        if (_usersToPushDataTo.Count == 0)
            return;

        Logger.LogDebug("Fast-path {kind} update for {count} visible players", logName, _usersToPushDataTo.Count);
        PushCharacterData();
    }
}