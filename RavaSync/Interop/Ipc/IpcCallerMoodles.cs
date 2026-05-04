using Dalamud.Plugin;
using Dalamud.Plugin.Ipc;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerMoodles : IIpcCaller
{
    private readonly ICallGateSubscriber<int> _moodlesApiVersion;
    private readonly ICallGateSubscriber<nint, object> _moodlesOnChange;
    private readonly ICallGateSubscriber<nint, string> _moodlesGetStatus;
    private readonly ICallGateSubscriber<nint, string, object> _moodlesSetStatus;
    private readonly ICallGateSubscriber<nint, object> _moodlesRevertStatus;
    private readonly ILogger<IpcCallerMoodles> _logger;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareMediator _mareMediator;

    public IpcCallerMoodles(ILogger<IpcCallerMoodles> logger, IDalamudPluginInterface pi, DalamudUtilService dalamudUtil,
        MareMediator mareMediator)
    {
        _logger = logger;
        _dalamudUtil = dalamudUtil;
        _mareMediator = mareMediator;

        _moodlesApiVersion = pi.GetIpcSubscriber<int>("Moodles.Version");
        _moodlesOnChange = pi.GetIpcSubscriber<nint, object>("Moodles.StatusManagerModified");
        _moodlesGetStatus = pi.GetIpcSubscriber<nint, string>("Moodles.GetStatusManagerByPtrV2");
        _moodlesSetStatus = pi.GetIpcSubscriber<nint, string, object>("Moodles.SetStatusManagerByPtrV2");
        _moodlesRevertStatus = pi.GetIpcSubscriber<nint, object>("Moodles.ClearStatusManagerByPtrV2");

        _moodlesOnChange.Subscribe(OnMoodlesChange);

        CheckAPI();
    }

    private void OnMoodlesChange(nint address)
    {
        if (address == nint.Zero)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                var localPlayerAddress = await _dalamudUtil.RunOnFrameworkThread(_dalamudUtil.GetPlayerPtr).ConfigureAwait(false);
                if (localPlayerAddress == nint.Zero || localPlayerAddress != address)
                    return;

                var status = _moodlesGetStatus.InvokeFunc(address) ?? string.Empty;
                _mareMediator.Publish(new MoodlesMessage(address, status));
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Could not obtain local Moodles status during change event");

                try
                {
                    var localPlayerAddress = await _dalamudUtil.RunOnFrameworkThread(_dalamudUtil.GetPlayerPtr).ConfigureAwait(false);
                    if (localPlayerAddress != nint.Zero && localPlayerAddress == address)
                    {
                        _mareMediator.Publish(new MoodlesMessage(address, string.Empty));
                    }
                }
                catch
                {
                    // ignore secondary failure
                }
            }
        });
    }

    public bool APIAvailable { get; private set; } = false;

    public void CheckAPI()
    {
        try
        {
            APIAvailable = _moodlesApiVersion.InvokeFunc() >= 4;
        }
        catch
        {
            APIAvailable = false;
        }
    }

    public void Dispose()
    {
        _moodlesOnChange.Unsubscribe(OnMoodlesChange);
    }

    public async Task<string?> GetStatusAsync(nint address)
    {
        if (!APIAvailable) return null;

        try
        {
            return await _dalamudUtil.RunOnFrameworkThread(() => _moodlesGetStatus.InvokeFunc(address)).ConfigureAwait(false);

        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not Get Moodles Status");
            return null;
        }
    }

    public async Task SetStatusAsync(nint pointer, string status)
    {
        if (!APIAvailable) return;
        try
        {
            await _dalamudUtil.RunOnFrameworkThread(() => _moodlesSetStatus.InvokeAction(pointer, status)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not Set Moodles Status");
        }
    }

    public async Task RevertStatusAsync(nint pointer)
    {
        if (!APIAvailable) return;
        try
        {
            await _dalamudUtil.RunOnFrameworkThread(() => _moodlesRevertStatus.InvokeAction(pointer)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Could not Set Moodles Status");
        }
    }
}
