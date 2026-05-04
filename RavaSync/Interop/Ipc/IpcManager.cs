using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Interop.Ipc;

public sealed partial class IpcManager : DisposableMediatorSubscriberBase
{
    private static readonly TimeSpan PeriodicApiStateCheckInterval = TimeSpan.FromSeconds(1);
    private long _nextPeriodicApiStateCheckTick;
    private int _periodicApiStateCheckRunning;
    private int _periodicApiStateCheckSlot;
    public IpcManager(ILogger<IpcManager> logger, MareMediator mediator,
        IpcCallerPenumbra penumbraIpc, IpcCallerGlamourer glamourerIpc, IpcCallerCustomize customizeIpc, IpcCallerHeels heelsIpc,
        IpcCallerHonorific honorificIpc, IpcCallerMoodles moodlesIpc, IpcCallerPetNames ipcCallerPetNames, IpcCallerBrio ipcCallerBrio,
        IpcCallerOtherSync otherSyncIpc) : base(logger, mediator)
    {
        CustomizePlus = customizeIpc;
        Heels = heelsIpc;
        Glamourer = glamourerIpc;
        Penumbra = penumbraIpc;
        Honorific = honorificIpc;
        Moodles = moodlesIpc;
        PetNames = ipcCallerPetNames;
        Brio = ipcCallerBrio;
        OtherSync = otherSyncIpc;
        if (Initialized)
        {
            Mediator.Publish(new PenumbraInitializedMessage());
        }

        Interlocked.Exchange(ref _nextPeriodicApiStateCheckTick,
            Environment.TickCount64 + (long)PeriodicApiStateCheckInterval.TotalMilliseconds);

        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, (_) => PeriodicApiStateCheck());
    }

    public bool Initialized => Penumbra.APIAvailable && Glamourer.APIAvailable;

    public IpcCallerCustomize CustomizePlus { get; init; }
    public IpcCallerHonorific Honorific { get; init; }
    public IpcCallerHeels Heels { get; init; }
    public IpcCallerGlamourer Glamourer { get; }
    public IpcCallerPenumbra Penumbra { get; }
    public IpcCallerMoodles Moodles { get; }
    public IpcCallerPetNames PetNames { get; }
    public IpcCallerBrio Brio { get; }
    public IpcCallerOtherSync OtherSync { get; }

    private void PeriodicApiStateCheck()
    {
        var nowTick = Environment.TickCount64;
        if (nowTick < Interlocked.Read(ref _nextPeriodicApiStateCheckTick))
            return;

        if (Interlocked.Exchange(ref _periodicApiStateCheckRunning, 1) != 0)
            return;

        Interlocked.Exchange(ref _nextPeriodicApiStateCheckTick, nowTick + (long)PeriodicApiStateCheckInterval.TotalMilliseconds);

        var slot = _periodicApiStateCheckSlot;
        _periodicApiStateCheckSlot = (_periodicApiStateCheckSlot + 1) % 9;

        _ = Task.Run(() =>
        {
            try
            {
                switch (slot)
                {
                    case 0:
                        Penumbra.CheckAPI();
                        Penumbra.CheckModDirectory();
                        break;
                    case 1:
                        Glamourer.CheckAPI();
                        break;
                    case 2:
                        Heels.CheckAPI();
                        break;
                    case 3:
                        CustomizePlus.CheckAPI();
                        break;
                    case 4:
                        Honorific.CheckAPI();
                        break;
                    case 5:
                        Moodles.CheckAPI();
                        break;
                    case 6:
                        PetNames.CheckAPI();
                        break;
                    case 7:
                        Brio.CheckAPI();
                        break;
                    default:
                        OtherSync.CheckAPI();
                        break;
                }
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Periodic IPC API state check failed");
            }
            finally
            {
                Interlocked.Exchange(ref _periodicApiStateCheckRunning, 0);
            }
        });
    }
}