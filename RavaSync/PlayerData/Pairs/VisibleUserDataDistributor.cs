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
                _lastCreatedData = newData;
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

    private void FrameworkOnUpdate()
    {
        if (!_dalamudUtil.GetIsPlayerPresent() || !_apiController.IsConnected) return;

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

        _ = Task.Run(async () =>
        {
            forced |= _uploadingCharacterData?.DataHash != _lastCreatedData.DataHash;

            if (_fileUploadTask == null || (_fileUploadTask?.IsCompleted ?? false) || forced)
            {
                _uploadingCharacterData = _lastCreatedData.DeepClone();
                Logger.LogDebug("Starting UploadTask for {hash}, Reason: TaskIsNull: {task}, TaskIsCompleted: {taskCpl}, Forced: {frc}",
                    _lastCreatedData.DataHash, _fileUploadTask == null, _fileUploadTask?.IsCompleted ?? false, forced);
                _fileUploadTask = _fileTransferManager.UploadFiles(_uploadingCharacterData, [.. _usersToPushDataTo]);
            }

            if (_fileUploadTask != null)
            {
                var uploadResult = await _fileUploadTask.ConfigureAwait(false);
                if (!uploadResult.Success)
                {
                    Logger.LogWarning("Upload/share failed for {hash}: {error}", _uploadingCharacterData?.DataHash.Value ?? "UNKNOWN", uploadResult.Error ?? "Unknown error");
                    return;
                }

                var dataToSend = uploadResult.Data;
                await _pushDataSemaphore.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
                try
                {
                    if (_usersToPushDataTo.Count == 0) return;
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("Pushing {data} to {users}", dataToSend.DataHash, string.Join(", ", _usersToPushDataTo.Select(k => k.AliasOrUID)));

                    await _apiController.PushCharacterData(dataToSend, [.. _usersToPushDataTo]).ConfigureAwait(false);
                    _usersToPushDataTo.Clear();

                }
                finally
                {
                    _pushDataSemaphore.Release();
                }
            }
        });
    }
}