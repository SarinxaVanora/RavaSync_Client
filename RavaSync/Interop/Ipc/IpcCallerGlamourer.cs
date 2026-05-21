using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Game.ClientState.Objects.Types;
using Dalamud.Plugin;
using Glamourer.Api.Enums;
using Glamourer.Api.Helpers;
using Glamourer.Api.IpcSubscribers;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerGlamourer : DisposableMediatorSubscriberBase, IIpcCaller
{
    private readonly ILogger<IpcCallerGlamourer> _logger;
    private readonly IDalamudPluginInterface _pi;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareMediator _mareMediator;
    private readonly RedrawManager _redrawManager;

    private readonly ApiVersion _glamourerApiVersions;
    private readonly ApplyState? _glamourerApplyAll;
    private readonly ReapplyState _glamourerReapply;
    private readonly GetStateBase64? _glamourerGetAllCustomization;
    private readonly RevertState _glamourerRevert;
    private readonly RevertStateName _glamourerRevertByName;
    private readonly UnlockState _glamourerUnlock;
    private readonly UnlockStateName _glamourerUnlockByName;
    private readonly EventSubscriber<nint>? _glamourerStateChanged;

    private bool _shownGlamourerUnavailable = false;
    private readonly uint LockCode = 0x6D617265;

    private static readonly SemaphoreSlim GlamourerFrameworkIpcGate = new(1, 1);
    private static long _nextGlamourerFrameworkIpcTick;
    private const int GlamourerFrameworkIpcSpacingMs = 16;

    public IpcCallerGlamourer(ILogger<IpcCallerGlamourer> logger, IDalamudPluginInterface pi, DalamudUtilService dalamudUtil, MareMediator mareMediator,
        RedrawManager redrawManager) : base(logger, mareMediator)
    {
        _glamourerApiVersions = new ApiVersion(pi);
        _glamourerGetAllCustomization = new GetStateBase64(pi);
        _glamourerApplyAll = new ApplyState(pi);
        _glamourerReapply = new ReapplyState(pi);
        _glamourerRevert = new RevertState(pi);
        _glamourerRevertByName = new RevertStateName(pi);
        _glamourerUnlock = new UnlockState(pi);
        _glamourerUnlockByName = new UnlockStateName(pi);

        _logger = logger;
        _pi = pi;
        _dalamudUtil = dalamudUtil;
        _mareMediator = mareMediator;
        _redrawManager = redrawManager;
        CheckAPI();

        _glamourerStateChanged = StateChanged.Subscriber(pi, GlamourerChanged);
        _glamourerStateChanged.Enable();
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _redrawManager.Cancel();
        _glamourerStateChanged?.Dispose();
    }

    public bool APIAvailable { get; private set; }

    public void CheckAPI()
    {
        bool apiAvailable = false;
        try
        {
            bool versionValid = (_pi.InstalledPlugins
                .FirstOrDefault(p => string.Equals(p.InternalName, "Glamourer", StringComparison.OrdinalIgnoreCase))
                ?.Version ?? new Version(0, 0, 0, 0)) >= new Version(1, 3, 0, 10);
            try
            {
                var version = _glamourerApiVersions.Invoke();
                if (version is { Major: 1, Minor: >= 1 } && versionValid)
                {
                    apiAvailable = true;
                }
            }
            catch
            {
                // ignore
            }
            _shownGlamourerUnavailable = _shownGlamourerUnavailable && !apiAvailable;

            APIAvailable = apiAvailable;
        }
        catch
        {
            APIAvailable = apiAvailable;
        }
        finally
        {
            if (!apiAvailable && !_shownGlamourerUnavailable)
            {
                _shownGlamourerUnavailable = true;
                _mareMediator.Publish(new NotificationMessage("Glamourer inactive", "Your Glamourer installation is not active or out of date. Update Glamourer to continue to use RavaSync. If you just updated Glamourer, ignore this message.",
                    NotificationType.Error));
            }
        }
    }

    private async Task<T> RunPacedGlamourerFrameworkIpcAsync<T>(ILogger logger, string operationName, Func<T> action, CancellationToken token, int warnAfterMs = 60)
    {
        await GlamourerFrameworkIpcGate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var delayMs = Volatile.Read(ref _nextGlamourerFrameworkIpcTick) - Environment.TickCount64;
            if (delayMs > 0 && delayMs < 1000)
                await Task.Delay((int)delayMs, token).ConfigureAwait(false);

            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var frameworkStopwatch = Stopwatch.StartNew();
                try
                {
                    return action();
                }
                finally
                {
                    frameworkStopwatch.Stop();
                    if (frameworkStopwatch.ElapsedMilliseconds >= warnAfterMs)
                        logger.LogWarning("[Glamourer IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, frameworkStopwatch.ElapsedMilliseconds);
                    else
                        logger.LogTrace("[Glamourer IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, frameworkStopwatch.ElapsedMilliseconds);
                }
            }).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _nextGlamourerFrameworkIpcTick, Environment.TickCount64 + GlamourerFrameworkIpcSpacingMs);
            GlamourerFrameworkIpcGate.Release();
        }
    }

    public async Task ApplyAllAsync(ILogger logger, GameObjectHandler handler, string? customization, Guid applicationId, CancellationToken token, bool fireAndForget = false, ApplyFlag? flags = null, bool waitForDrawSettle = true)
    {
        if (!APIAvailable || string.IsNullOrEmpty(customization) || _dalamudUtil.IsZoning) return;

        async Task ApplyCoreAsync()
        {
            await SafeIpc.TryRun(Logger, "Glamourer.ApplyAll", TimeSpan.FromSeconds(2), async ct =>
            {
                var applyFlags = flags ?? ApplyFlagEx.StateDefault;
                var isLightweightApply = applyFlags != ApplyFlagEx.StateDefault;
                await RunPacedGlamourerFrameworkIpcAsync(logger, $"Glamourer.ApplyState({applyFlags})", () =>
                {
                    try
                    {
                        logger.LogDebug("[{appid}] Calling on IPC: GlamourerApplyAll flags={flags}", applicationId, applyFlags);

                        var gameObj = handler.GetGameObject();
                        if (gameObj is not ICharacter chara)
                            return 0;

                        _glamourerApplyAll!.Invoke(customization, chara.ObjectIndex, LockCode, applyFlags);
                        return 0;
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "[{appid}] Failed to apply Glamourer data", applicationId);
                        return 0;
                    }
                }, ct).ConfigureAwait(false);

                await _dalamudUtil.RunOnFrameworkThread(() => 0).ConfigureAwait(false);

                if (waitForDrawSettle && !isLightweightApply && handler.Address != nint.Zero && handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                {
                    await _dalamudUtil.WaitWhileCharacterIsDrawing(logger, handler, applicationId, 15000, ct).ConfigureAwait(false);
                }
                else if (handler.Address != nint.Zero && handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                {
                    logger.LogTrace("[{appid}] Glamourer apply flags={flags} left {name} drawing; not blocking the RavaSync apply lane", applicationId, applyFlags, handler.Name);
                }
            }).ConfigureAwait(false);
        }

        if (fireAndForget)
        {
            _ = ApplyCoreAsync();
            return;
        }

        await ApplyCoreAsync().ConfigureAwait(false);
    }

    public async Task ReapplyDirectAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning)
            return;

        token.ThrowIfCancellationRequested();

        await RunPacedGlamourerFrameworkIpcAsync(logger, "Glamourer.ReapplyState(Once)", () =>
        {
            if (handler.GetGameObject() is not ICharacter chara)
                return 0;

            try
            {
                logger.LogDebug("[{appid}] Calling on IPC: GlamourerReapplyDirect", applicationId);
                _glamourerReapply.Invoke(chara.ObjectIndex, LockCode, ApplyFlag.Once);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{appid}] Error during GlamourerReapplyDirect", applicationId);
            }

            return 0;
        }, token).ConfigureAwait(false);
    }



    public async Task<string> GetCharacterCustomizationAsync(IntPtr character)
    {
        if (!APIAvailable) return string.Empty;

        var (ok, result) = await SafeIpc.TryCall<string>(Logger, "Glamourer.GetStateBase64", TimeSpan.FromSeconds(2), async ct =>
        {
            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var gameObj = _dalamudUtil.CreateGameObject(character);
                if (gameObj is ICharacter c)
                {
                    return _glamourerGetAllCustomization!.Invoke(c.ObjectIndex).Item2 ?? string.Empty;
                }
                return string.Empty;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return ok ? (result ?? string.Empty) : string.Empty;
    }

    public async Task RevertAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token, bool fireAndForget = false)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning) return;

        var task = _redrawManager.PenumbraRedrawInternalAsync(logger, handler, applicationId, chara =>
        {
            try
            {
                logger.LogDebug("[{appid}] Calling On IPC: GlamourerUnlock", applicationId);
                _glamourerUnlock.Invoke(chara.ObjectIndex, LockCode);

                logger.LogDebug("[{appid}] Calling On IPC: GlamourerRevert", applicationId);
                _glamourerRevert.Invoke(chara.ObjectIndex, LockCode);

                logger.LogDebug("[{appid}] Requesting Penumbra Redraw via mediator", applicationId);
                _mareMediator.Publish(new PenumbraRedrawCharacterMessage(chara));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{appid}] Error during GlamourerRevert", applicationId);
            }
        }, token);

        if (fireAndForget)
            return;

        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("[{appid}] Glamourer revert cancelled for {name}", applicationId, handler.Name);
        }
    }


    public async Task RevertByNameAsync(ILogger logger, string name, Guid applicationId)
    {
        if ((!APIAvailable) || _dalamudUtil.IsZoning) return;

        // Guard the framework-thread work with SafeIpc (timeout + errors)
        await SafeIpc.TryRun(Logger, "Glamourer.RevertByNameAsync", TimeSpan.FromSeconds(2), async ct =>
        {
            await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                RevertByName(logger, name, applicationId);
                return 0; // any dummy value to satisfy RunOnFrameworkThread<T>
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public void RevertByName(ILogger logger, string name, Guid applicationId)
    {
        if ((!APIAvailable) || _dalamudUtil.IsZoning) return;

        try
        {
            logger.LogDebug("[{appid}] Calling On IPC: GlamourerRevertByName", applicationId);
            _glamourerRevertByName.Invoke(name, LockCode);
            logger.LogDebug("[{appid}] Calling On IPC: GlamourerUnlockName", applicationId);
            _glamourerUnlockByName.Invoke(name, LockCode);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during Glamourer RevertByName");
        }
    }

    public async Task RevertByObjectIndexAsync(ILogger logger, int objectIndex, Guid applicationId)
    {
        if ((!APIAvailable) || _dalamudUtil.IsZoning) return;

        await SafeIpc.TryRun(Logger, "Glamourer.RevertByObjectIndex", TimeSpan.FromSeconds(2), async ct =>
        {
            await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                try
                {
                    logger.LogDebug("[{appid}] Glamourer cleanup for idx {idx}: Unlock + Revert", applicationId, objectIndex);
                    _glamourerUnlock.Invoke(objectIndex, LockCode);
                    _glamourerRevert.Invoke(objectIndex, LockCode);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{appid}] Glamourer cleanup failed for idx {idx}", applicationId, objectIndex);
                }

                return 0;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    private void GlamourerChanged(nint address)
    {
        _mareMediator.Publish(new GlamourerChangedMessage(address));
    }
}
