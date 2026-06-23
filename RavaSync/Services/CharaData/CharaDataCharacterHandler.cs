using RavaSync.API.Data.Enum;
using RavaSync.Interop.Ipc;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services.CharaData.Models;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace RavaSync.Services;

public sealed class CharaDataCharacterHandler : DisposableMediatorSubscriberBase
{
    private readonly GameObjectHandlerFactory _gameObjectHandlerFactory;
    private readonly DalamudUtilService _dalamudUtilService;
    private readonly IpcManager _ipcManager;
    private readonly HashSet<HandledCharaDataEntry> _handledCharaData = [];
    private readonly ConcurrentDictionary<string, byte> _pendingCutsceneReverts = new(StringComparer.Ordinal);

    public IEnumerable<HandledCharaDataEntry> HandledCharaData => _handledCharaData;

    public CharaDataCharacterHandler(ILogger<CharaDataCharacterHandler> logger, MareMediator mediator,
        GameObjectHandlerFactory gameObjectHandlerFactory, DalamudUtilService dalamudUtilService,
        IpcManager ipcManager)
        : base(logger, mediator)
    {
        _gameObjectHandlerFactory = gameObjectHandlerFactory;
        _dalamudUtilService = dalamudUtilService;
        _ipcManager = ipcManager;
        mediator.Subscribe<GposeEndMessage>(this, (f) =>
        {
            foreach (var chara in _handledCharaData.ToList())
            {
                _ = RevertHandledChara(chara);
            }
        });

        mediator.Subscribe<CutsceneFrameworkUpdateMessage>(this, (_) => HandleCutsceneFrameworkUpdate());
    }

    private void HandleCutsceneFrameworkUpdate()
    {
        if (!_dalamudUtilService.IsInGpose) return;

        foreach (var entry in _handledCharaData.ToList())
        {
            var chara = _dalamudUtilService.GetGposeCharacterFromObjectTableByName(entry.Name, onlyGposeCharacters: true);
            if (chara is not null) continue;
            if (!_handledCharaData.Remove(entry)) continue;
            if (!_pendingCutsceneReverts.TryAdd(entry.Name, 0)) continue;

            _ = Task.Run(async () =>
            {
                try
                {
                    await RevertChara(entry).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Best-effort cutscene revert failed for {name}", entry.Name);
                }
                finally
                {
                    _pendingCutsceneReverts.TryRemove(entry.Name, out _);
                }
            });
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        foreach (var chara in _handledCharaData.ToList())
        {
            _ = RevertHandledChara(chara);
        }
    }

    public Task RevertChara(string name, Guid? cPlusId)
        => RevertChara(new HandledCharaDataEntry(name, false, cPlusId, null!, null));

    public async Task RevertChara(HandledCharaDataEntry handled)
    {
        Guid applicationId = Guid.NewGuid();
        await _ipcManager.Glamourer.RevertByNameAsync(Logger, handled.Name, applicationId).ConfigureAwait(false);
        if (handled.CustomizePlus != null)
        {
            await _ipcManager.CustomizePlus.RevertByIdAsync(handled.CustomizePlus).ConfigureAwait(false);
        }

        using var handler = await _gameObjectHandlerFactory.Create(ObjectKind.Player,
            () => _dalamudUtilService.GetGposeCharacterFromObjectTableByName(handled.Name, _dalamudUtilService.IsInGpose)?.Address ?? IntPtr.Zero, false)
            .ConfigureAwait(false);

        bool hadLiveHandler = handler.Address != nint.Zero;
        if (hadLiveHandler)
        {
            var (idx, addr) = await _dalamudUtilService.RunOnFrameworkThread(() =>
            {
                var obj = handler.GetGameObject();
                return (obj?.ObjectIndex ?? -1, obj?.Address ?? nint.Zero);
            }).ConfigureAwait(false);

            if (idx >= 0)
            {
                await ClearPenumbraTemporarySlotAsync(handled, idx, addr, applicationId).ConfigureAwait(false);
                await _ipcManager.Penumbra.RedrawAsync(Logger, handler, applicationId, CancellationToken.None).ConfigureAwait(false);
            }
        }
        else if (handled.IsSelf)
        {
            var (idx, addr) = await _dalamudUtilService.RunOnFrameworkThread(() =>
            {
                var player = _dalamudUtilService.GetPlayerCharacter();
                return (player?.ObjectIndex ?? -1, player?.Address ?? nint.Zero);
            }).ConfigureAwait(false);

            if (idx >= 0)
                await ClearPenumbraTemporarySlotAsync(handled, idx, addr, applicationId).ConfigureAwait(false);
        }

        if (handled.PenumbraCollection is { } collection && collection != Guid.Empty)
            await _ipcManager.Penumbra.RemoveTemporaryCollectionAsync(Logger, applicationId, collection).ConfigureAwait(false);
    }

    private async Task ClearPenumbraTemporarySlotAsync(HandledCharaDataEntry handled, int idx, nint address, Guid applicationId)
    {
        if (handled.PenumbraCollection is not { } collection || collection == Guid.Empty || idx < 0)
            return;

        if (handled.IsSelf)
        {
            Logger.LogDebug("Skipping Penumbra temporary slot clear for local player {name}; local sender state must not use RavaSync temporary collections", handled.Name);
            return;
        }

        await _ipcManager.Penumbra.AssignEmptyCollectionToVerifiedCharacterAsync(Logger, idx, string.Empty, address, handled.Name).ConfigureAwait(false);
    }

    public async Task<bool> RevertHandledChara(string name)
    {
        var handled = _handledCharaData.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.Ordinal));
        if (handled == null) return false;
        _handledCharaData.Remove(handled);
        await RevertChara(handled).ConfigureAwait(false);
        return true;
    }

    public async Task RevertHandledChara(HandledCharaDataEntry? handled)
    {
        if (handled == null) return;
        _handledCharaData.Remove(handled);
        await RevertChara(handled).ConfigureAwait(false);
    }

    internal void AddHandledChara(HandledCharaDataEntry handledCharaDataEntry)
    {
        _handledCharaData.Add(handledCharaDataEntry);
    }

    public void UpdateHandledData(IReadOnlyDictionary<string, CharaDataMetaInfoExtendedDto?> newData)
    {
        foreach (var handledData in _handledCharaData)
        {
            if (newData.TryGetValue(handledData.MetaInfo.FullId, out var metaInfo) && metaInfo != null)
                handledData.MetaInfo = metaInfo;
        }
    }


    public async Task<GameObjectHandler?> TryCreateGameObjectHandler(string name, bool gPoseOnly = false)
    {
        var handler = await _gameObjectHandlerFactory.Create(ObjectKind.Player,
            () => _dalamudUtilService.GetGposeCharacterFromObjectTableByName(name, gPoseOnly && _dalamudUtilService.IsInGpose)?.Address ?? IntPtr.Zero, false)
            .ConfigureAwait(false);
        if (handler.Address == nint.Zero) return null;
        return handler;
    }

    public async Task<GameObjectHandler?> TryCreateGameObjectHandler(int index)
    {
        var handler = await _gameObjectHandlerFactory.Create(ObjectKind.Player,
            () => _dalamudUtilService.GetCharacterFromObjectTableByIndex(index)?.Address ?? IntPtr.Zero, false)
            .ConfigureAwait(false);
        if (handler.Address == nint.Zero) return null;
        return handler;
    }
}
