using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Game.ClientState.Objects.Types;
using Dalamud.Plugin;
using Dalamud.Plugin.Ipc;
using Dalamud.Utility;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerCustomize : IIpcCaller, IDisposable
{
    private readonly ICallGateSubscriber<(int, int)> _customizePlusApiVersion;
    private readonly ICallGateSubscriber<ushort, (int, Guid?)> _customizePlusGetActiveProfile;
    private readonly ICallGateSubscriber<Guid, (int, string?)> _customizePlusGetProfileById;
    private readonly ICallGateSubscriber<ushort, Guid, object> _customizePlusOnScaleUpdate;
    private readonly ICallGateSubscriber<ushort, int> _customizePlusRevertCharacter;
    private readonly ICallGateSubscriber<ushort, string, (int, Guid?)> _customizePlusSetBodyScaleToCharacter;
    private readonly ICallGateSubscriber<Guid, int> _customizePlusDeleteByUniqueId;
    private readonly ILogger<IpcCallerCustomize> _logger;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareMediator _mareMediator;

    public IpcCallerCustomize(ILogger<IpcCallerCustomize> logger, IDalamudPluginInterface dalamudPluginInterface,
        DalamudUtilService dalamudUtil, MareMediator mareMediator)
    {
        _customizePlusApiVersion = dalamudPluginInterface.GetIpcSubscriber<(int, int)>("CustomizePlus.General.GetApiVersion");
        _customizePlusGetActiveProfile = dalamudPluginInterface.GetIpcSubscriber<ushort, (int, Guid?)>("CustomizePlus.Profile.GetActiveProfileIdOnCharacter");
        _customizePlusGetProfileById = dalamudPluginInterface.GetIpcSubscriber<Guid, (int, string?)>("CustomizePlus.Profile.GetByUniqueId");
        _customizePlusRevertCharacter = dalamudPluginInterface.GetIpcSubscriber<ushort, int>("CustomizePlus.Profile.DeleteTemporaryProfileOnCharacter");
        _customizePlusSetBodyScaleToCharacter = dalamudPluginInterface.GetIpcSubscriber<ushort, string, (int, Guid?)>("CustomizePlus.Profile.SetTemporaryProfileOnCharacter");
        _customizePlusOnScaleUpdate = dalamudPluginInterface.GetIpcSubscriber<ushort, Guid, object>("CustomizePlus.Profile.OnUpdate");
        _customizePlusDeleteByUniqueId = dalamudPluginInterface.GetIpcSubscriber<Guid, int>("CustomizePlus.Profile.DeleteTemporaryProfileByUniqueId");

        _customizePlusOnScaleUpdate.Subscribe(OnCustomizePlusScaleChange);
        _logger = logger;
        _dalamudUtil = dalamudUtil;
        _mareMediator = mareMediator;

        CheckAPI();
    }

    public bool APIAvailable { get; private set; } = false;

    public async Task RevertAsync(nint character)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(_logger, "CustomizePlus.RevertCharacter", TimeSpan.FromSeconds(2), async ct =>
        {
            await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var gameObj = _dalamudUtil.CreateGameObject(character);
                if (gameObj is ICharacter c)
                {
                    _logger.LogTrace("CustomizePlus reverting for {chara}", c.Address.ToString("X"));
                    _customizePlusRevertCharacter!.InvokeFunc(c.ObjectIndex);
                }
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task<Guid?> SetBodyScaleAsync(nint character, string scale)
    {
        if (!APIAvailable) return null;

        var (ok, result) = await SafeIpc.TryCall<Guid?>(_logger, "CustomizePlus.SetBodyScaleToCharacter", TimeSpan.FromSeconds(2), async ct =>
        {
            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var gameObj = _dalamudUtil.CreateGameObject(character);
                if (gameObj is ICharacter c)
                {
                    _logger.LogTrace("CustomizePlus applying for {chara}", c.Address.ToString("X"));
                    if (scale.IsNullOrEmpty())
                    {
                        _customizePlusRevertCharacter!.InvokeFunc(c.ObjectIndex);
                        return (Guid?)null;
                    }

                    string decodedScale = Encoding.UTF8.GetString(Convert.FromBase64String(scale));
                    var res = _customizePlusSetBodyScaleToCharacter!.InvokeFunc(c.ObjectIndex, decodedScale);
                    return res.Item2;
                }

                return (Guid?)null;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return ok ? result : null;
    }

    public async Task RevertByIdAsync(Guid? profileId)
    {
        if (!APIAvailable || profileId == null) return;

        await SafeIpc.TryRun(_logger, "CustomizePlus.DeleteTemporaryProfileByUniqueId", TimeSpan.FromSeconds(2), async ct =>
        {
            await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                _ = _customizePlusDeleteByUniqueId.InvokeFunc(profileId.Value);
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task<string?> GetScaleAsync(nint character)
    {
        if (!APIAvailable) return string.Empty;

        var (ok, raw) = await SafeIpc.TryCall<string>(_logger, "CustomizePlus.GetScaleBase64", TimeSpan.FromSeconds(2), async ct =>
        {
            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var gameObj = _dalamudUtil.CreateGameObject(character);
                if (gameObj is ICharacter c)
                {
                    var res = _customizePlusGetActiveProfile.InvokeFunc(c.ObjectIndex);
                    _logger.LogTrace("CustomizePlus GetActiveProfile returned {err}", res.Item1);
                    if (res.Item1 != 0 || res.Item2 == null) return string.Empty;
                    return _customizePlusGetProfileById.InvokeFunc(res.Item2.Value).Item2 ?? string.Empty;
                }

                return string.Empty;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        if (!ok || string.IsNullOrEmpty(raw)) return string.Empty;
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(raw));
    }

    public void CheckAPI()
    {
        try
        {
            var version = _customizePlusApiVersion.InvokeFunc();
            APIAvailable = (version.Item1 == 6 && version.Item2 >= 0);
        }
        catch
        {
            APIAvailable = false;
        }
    }

    private void OnCustomizePlusScaleChange(ushort c, Guid g)
    {
        var obj = _dalamudUtil.GetCharacterFromObjectTableByIndex(c);
        _mareMediator.Publish(new CustomizePlusMessage(obj?.Address ?? null));
    }

    public void Dispose()
    {
        _customizePlusOnScaleUpdate.Unsubscribe(OnCustomizePlusScaleChange);
    }
}
