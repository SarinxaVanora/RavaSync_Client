using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.Optimisation;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI;
using RavaSync.Utils;
using System;
using System.Reflection;

namespace RavaSync;

#pragma warning disable S125 // Sections of code should not be commented out
/*
                                                                    (..,,...,,,,,+/,                ,,.....,,+
                                                              ..,,+++/((###%%%&&%%#(+,,.,,,+++,,,,//,,#&@@@@%+.
                                                          ...+//////////(/,,,,++,.,(###((//////////,..  .,#@@%/./
                                                       ,..+/////////+///,.,. ,&@@@@,,/////////////+,..    ,(##+,.
                                                    ,,.+//////////++++++..     ./#%#,+/////////////+,....,/((,..,
                                                  +..////////////+++++++...  .../##(,,////////////////++,,,+/(((+,
                                                +,.+//////////////+++++++,.,,,/(((+.,////////////////////////((((#/,,
                                              /+.+//////////++++/++++++++++,,...,++///////////////////////////((((##,
                                             /,.////////+++++++++++++++++++++////////+++//////++/+++++//////////((((#(+,
                                           /+.+////////+++++++++++++++++++++++++++++++++++++++++++++++++++++/////((((##+
                                          +,.///////////////+++++++++++++++++++++++++++++++++++++++++++++++++++///((((%/
                                         /.,/////////////////+++++++++++++++++++++++++++++++++++++++++++++++++++///+/(#+
                                        +,./////////////////+++++++++++++++++++++++++++++++++++++++++++++++,,+++++///((,
                                       ...////////++/++++++++++++++++++++++++,,++++++++++++++++++++++++++++++++++++//(,,
                                       ..//+,+///++++++++++++++++++,,,,+++,,,,,,,,,,,,++++++++,,+++++++++++++++++++//,,+
                                      ..,++,.++++++++++++++++++++++,,,,,,,,,,,,,,,,,,,++++++++,,,,,,,,,,++++++++++...
                                      ..+++,.+++++++++++++++++++,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,++,..,.
                                     ..,++++,,+++++++++++,+,,,,,,,,,,..,+++++++++,,,,,,.....................,//+,+
                                 ....,+++++,.,+++++++++++,,,,,,,,.+///(((((((((((((///////////////////////(((+,,,
                          .....,++++++++++..,+++++++++++,,.,,,.////////(((((((((((((((////////////////////+,,/
                      .....,++++++++++++,..,,+++++++++,,.,../////////////////((((((((((//////////////////,,+
                   ...,,+++++++++++++,.,,.,,,+++++++++,.,/////////////////(((//++++++++++++++//+++++++++/,,
                ....,++++++++++++++,.,++.,++++++++++++.,+////////////////////+++++++++++++++++++++++++///,,..
              ...,++++++++++++++++..+++..+++++++++++++.,//////////////////////////++++++++++++///////++++......
            ...++++++++++++++++++..++++.,++,++++++++++.+///////////////////////////////////////////++++++..,,,..
          ...+++++++++++++++++++..+++++..,+,,+++++++++.+//////////////////////////////////////////+++++++...,,,,..
         ..++++++++++++++++++++..++++++..,+,,+++++++++.+//////////////////////////////////////++++++++++,....,,,,..
       ...+++//(//////+++++++++..++++++,.,+++++++++++++,..,....,,,+++///////////////////////++++++++++++..,,,,,,,,...
      ..,++/(((((//////+++++++,.,++++++,,.,,,+++++++++++++++++++++++,.++////////////////////+++++++++++.....,,,,,,,...
     ..,//#(((((///////+++++++..++++++++++,...,++,++++++++++++++++,...+++/////////////////////+,,,+++...  ....,,,,,,...
   ...+//(((((//////////++++++..+++++++++++++++,......,,,,++++++,,,..+++////////////////////////+,....     ...,,,,,,,...
   ..,//((((////////////++++++..++++++/+++++++++++++,,...,,........,+/+//////////////////////((((/+,..     ....,.,,,,..
  ...+/////////////////////+++..++++++/+///+++++++++++++++++++++///+/+////////////////////////(((((/+...   .......,,...
  ..++////+++//////////////++++.+++++++++///////++++++++////////////////////////////////////+++/(((((/+..    .....,,...
  .,++++++++///////////////++++..++++//////////////////////////////////////////////////////++++++/((((++..    ........
  .+++++++++////////////////++++,.+++/////////////////////////////////////////////////////+++++++++/((/++..
 .,++++++++//////////////////++++,.+++//////////////////////////////////////////////////+++++++++++++//+++..
 .++++++++//////////////////////+/,.,+++////((((////////////////////////////////////////++++++++++++++++++...
 .++++++++///////////////////////+++..++++//((((((((///////////////////////////////////++++++++++++++++++++ .
 .++++++///////////////////////////++,.,+++++/(((((((((/////////////////////////////+++++++++++++++++++++++,..
 .++++++////////////////////////////+++,.,+++++++/((((((((//////////////////////////++++++++++++++++++++++++..
 .+++++++///////////////////++////////++++,.,+++++++++///////////+////////////////+++++++++++++++++++++++++,..
 ..++++++++++//////////////////////+++++++..+...,+++++++++++++++/++++++++++++++++++++++++++++++++++++++++++,...
  ..++++++++++++///////////////+++++++,...,,,,,.,....,,,,+++++++++++++++++++++++++++++++++++++++++++++++,,,,...
  ...++++++++++++++++++++++++++,,,,...,,,,,,,,,..,,++,,,.,,,,,,,,,,,,,,,,,,+++++++++++++++++++++++++,,,,,,,,..
   ...+++++++++++++++,,,,,,,,....,,,,,,,,,,,,,,,..,,++++++,,,,,,,,,,,,,,,,+++++++++++++++++++++++++,,,,,,,,,..
     ...++++++++++++,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,...,++++++++++++++++++++++++++++++++++++++++++++,,,,,,,,,,...
       ,....,++++++++++++++,,,+++++++,,,,,,,,,,,,,,,,,.,++++++++++++++++++++++++++++++++++++++++++++,,,,,,,,..

*/
#pragma warning restore S125 // Sections of code should not be commented out

public class RavaPlugin : MediatorSubscriberBase, IHostedService
{
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareConfigService _mareConfigService;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private IServiceScope? _runtimeServiceScope;
    private Task? _launchTask;
    private CancellationTokenSource _launchCts = new();
    private CancellationTokenSource _warmupCts = new();
    private readonly object _launchTaskLock = new();
    private readonly SemaphoreSlim _runtimeScopeGate = new(1, 1);

    public RavaPlugin(ILogger<RavaPlugin> logger, MareConfigService mareConfigService,
        ServerConfigurationManager serverConfigurationManager,
        DalamudUtilService dalamudUtil,
        IServiceScopeFactory serviceScopeFactory, MareMediator mediator) : base(logger, mediator)
    {
        _mareConfigService = mareConfigService;
        _serverConfigurationManager = serverConfigurationManager;
        _dalamudUtil = dalamudUtil;
        _serviceScopeFactory = serviceScopeFactory;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        AppContext.SetSwitch("System.Net.SocketsHttpHandler.Http3Support", true);
        var version = Assembly.GetExecutingAssembly().GetName().Version!;
        Logger.LogInformation("Launching {name} {major}.{minor}.{build}", "RavaSync", version.Major, version.Minor, version.Build);
        Mediator.Publish(new EventMessage(new Services.Events.Event(nameof(RavaPlugin), Services.Events.EventSeverity.Informational,
            $"Starting RavaSync {version.Major}.{version.Minor}.{version.Build}")));

        Mediator.Subscribe<SwitchToMainUiMessage>(this, (_) => QueueLaunchCharacterManager());
        Mediator.Subscribe<DalamudLoginMessage>(this, (_) => DalamudUtilOnLogIn());
        Mediator.Subscribe<DalamudLogoutMessage>(this, (_) => DalamudUtilOnLogOut());

        Mediator.StartQueueProcessing();

        QueueLaunchCharacterManager();

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        UnsubscribeAll();

        CancelLaunchTask();
        CancelWarmup();

        var launchTask = _launchTask;
        if (launchTask != null)
        {
            try
            {
                await Task.WhenAny(launchTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken)).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Host shutdown cancellation;
            }
        }

        await DisposeRuntimeScopeAsync().ConfigureAwait(false);

        _launchCts.CancelDispose();
        _warmupCts.CancelDispose();

        Logger.LogDebug("Halting MarePlugin");
    }

    private void DalamudUtilOnLogIn()
    {
        Logger?.LogDebug("Client login");
        QueueLaunchCharacterManager();
    }

    private void DalamudUtilOnLogOut()
    {
        Logger?.LogDebug("Client logout");

        CancelLaunchTask();
        CancelWarmup();

        _ = DisposeRuntimeScopeAsync();
    }

    private void QueueLaunchCharacterManager()
    {
        lock (_launchTaskLock)
        {
            if (_launchTask is { IsCompleted: false })
                return;

            _launchCts.Dispose();
            _launchCts = new CancellationTokenSource();

            var token = _launchCts.Token;
            _launchTask = Task.Run(() => WaitForPlayerAndLaunchCharacterManager(token), token);
        }
    }

    private void CancelLaunchTask()
    {
        lock (_launchTaskLock)
        {
            if (!_launchCts.IsCancellationRequested)
                _launchCts.Cancel();
        }
    }

    private void CancelWarmup()
    {
        _warmupCts.Cancel();
        _warmupCts.Dispose();
        _warmupCts = new CancellationTokenSource();
    }

    private async Task WarmupHeavyServicesAfterSettleAsync(IServiceScope runtimeScope, CancellationToken token)
    {
        try
        {
            // Let Dalamud, Penumbra, Glamourer, file-cache startup, and first login work settle before
            // warming heavier GPU/optimisation services.
            await Task.Delay(TimeSpan.FromSeconds(3), token).ConfigureAwait(false);

            if (token.IsCancellationRequested || !ReferenceEquals(runtimeScope, _runtimeServiceScope))
                return;

            var textureOptimisationService = runtimeScope.ServiceProvider.GetRequiredService<TextureOptimisationService>();
            var meshOptimisationService = runtimeScope.ServiceProvider.GetRequiredService<MeshOptimisationService>();

            await textureOptimisationService.WarmupAsync(token).ConfigureAwait(false);

            if (token.IsCancellationRequested || !ReferenceEquals(runtimeScope, _runtimeServiceScope))
                return;

            await Task.Delay(500, token).ConfigureAwait(false);
            await meshOptimisationService.WarmupAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            Logger?.LogDebug(ex, "Optimisation warmup failed");
        }
    }

    private async Task WaitForPlayerAndLaunchCharacterManager(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested
                && !await _dalamudUtil.GetIsPlayerPresentAsync().ConfigureAwait(false))
            {
                await Task.Delay(100, token).ConfigureAwait(false);
            }

            token.ThrowIfCancellationRequested();

            Logger?.LogDebug("Launching Managers");

            await _runtimeScopeGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var oldScope = _runtimeServiceScope;
                _runtimeServiceScope = null;
                DisposeServiceScope(oldScope);

                token.ThrowIfCancellationRequested();

                var runtimeScope = _serviceScopeFactory.CreateScope();
                _runtimeServiceScope = runtimeScope;

                runtimeScope.ServiceProvider.GetRequiredService<UiService>();
                runtimeScope.ServiceProvider.GetRequiredService<CommandManagerService>();

                if (!_mareConfigService.Current.HasValidSetup() || !_serverConfigurationManager.HasValidConfig())
                {
                    Mediator.Publish(new SwitchToIntroUiMessage());
                    return;
                }

                if (!_mareConfigService.Current.SeenDiscoveryIntro)
                {
                    Mediator.Publish(new UiToggleMessage(typeof(DiscoveryIntroUi)));
                }

                runtimeScope.ServiceProvider.GetRequiredService<CacheCreationService>();
                runtimeScope.ServiceProvider.GetRequiredService<TransientResourceManager>();
                runtimeScope.ServiceProvider.GetRequiredService<VisibleUserDataDistributor>();
                runtimeScope.ServiceProvider.GetRequiredService<NotificationService>();

                CancelWarmup();
                _ = WarmupHeavyServicesAfterSettleAsync(runtimeScope, _warmupCts.Token);

            #if !DEBUG
                if (_mareConfigService.Current.LogLevel != LogLevel.Information)
                {
                    Mediator.Publish(new NotificationMessage("Abnormal Log Level",
                        $"Your log level is set to '{_mareConfigService.Current.LogLevel}' which is not recommended for normal usage. Set it to '{LogLevel.Information}' in \"RavaSync Settings -> Debug\" unless instructed otherwise.",
                        MareConfiguration.Models.NotificationType.Error, TimeSpan.FromSeconds(15000)));
                }
            #endif
            }
            finally
            {
                _runtimeScopeGate.Release();
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            Logger?.LogCritical(ex, "Error during launch of managers");
        }
    }

    private async Task DisposeRuntimeScopeAsync()
    {
        await _runtimeScopeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            var oldScope = _runtimeServiceScope;
            _runtimeServiceScope = null;
            DisposeServiceScope(oldScope);
        }
        finally
        {
            _runtimeScopeGate.Release();
        }
    }

    private static void DisposeServiceScope(IServiceScope? scope)
    {
        try
        {
            scope?.Dispose();
        }
        catch
        {
            // Best-effort shutdown. Never let scoped disposal crash plugin unload/logout.
        }
    }
}