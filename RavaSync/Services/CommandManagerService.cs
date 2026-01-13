using Dalamud.Game.Command;
using Dalamud.Plugin.Services;
using RavaSync.API.Dto.Group;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI;
using RavaSync.WebAPI;
using System.Globalization;

namespace RavaSync.Services;

public sealed class CommandManagerService : IDisposable
{
    private const string _commandName = "/rava";

    private readonly ApiController _apiController;
    private readonly ICommandManager _commandManager;
    private readonly MareMediator _mediator;
    private readonly MareConfigService _mareConfigService;
    private readonly PerformanceCollectorService _performanceCollectorService;
    private readonly CacheMonitor _cacheMonitor;
    private readonly ServerConfigurationManager _serverConfigurationManager;

    // NEW: we need these to verify location + furnishings permission
    private readonly IGameGui _gameGui;
    private readonly DalamudUtilService _dalamudUtil;

    public CommandManagerService(ICommandManager commandManager, PerformanceCollectorService performanceCollectorService,
        ServerConfigurationManager serverConfigurationManager, CacheMonitor periodicFileScanner,
        ApiController apiController, MareMediator mediator, MareConfigService mareConfigService,
        IGameGui gameGui, DalamudUtilService dalamudUtil)
    {
        _commandManager = commandManager;
        _performanceCollectorService = performanceCollectorService;
        _serverConfigurationManager = serverConfigurationManager;
        _cacheMonitor = periodicFileScanner;
        _apiController = apiController;
        _mediator = mediator;
        _mareConfigService = mareConfigService;
        _gameGui = gameGui;
        _dalamudUtil = dalamudUtil;

        _commandManager.AddHandler(_commandName, new CommandInfo(OnCommand)
        {
            HelpMessage = "Opens the RavaSync UI" + Environment.NewLine + Environment.NewLine +
                "Additionally possible commands:" + Environment.NewLine +
                "\t /rava toggle - Disconnects from RavaSync, if connected. Connects to RavaSync, if disconnected" + Environment.NewLine +
                "\t /rava toggle on|off - Connects or disconnects to RavaSync respectively" + Environment.NewLine +
                "\t /rava gpose - Opens the RavaSync Character Data Hub window" + Environment.NewLine +
                "\t /rava analyze - Opens the RavaSync Character Data Analysis window" + Environment.NewLine +
                "\t /rava settings - Opens the RavaSync Settings window" + Environment.NewLine +
                "\t /rava linktoshell - Opens the Venue Registration pane for the current interior (requires Indoor Furnishings)" + Environment.NewLine +
                "\t /rava tools - Opens the Tools hub shortcuts window" + Environment.NewLine +
                "\t /rava games - Opens the 'Toy Box games' window"
        });
    }

    public void Dispose()
    {
        _commandManager.RemoveHandler(_commandName);
    }

    private void OnCommand(string command, string args)
    {
        var splitArgs = args.ToLowerInvariant().Trim().Split(" ", StringSplitOptions.RemoveEmptyEntries);

        if (splitArgs.Length == 0)
        {
            if (_mareConfigService.Current.HasValidSetup())
                _mediator.Publish(new UiToggleMessage(typeof(CompactUi)));
            else
                _mediator.Publish(new UiToggleMessage(typeof(IntroUi)));
            return;
        }

        if (!_mareConfigService.Current.HasValidSetup()) return;

        if (string.Equals(splitArgs[0], "toggle", StringComparison.OrdinalIgnoreCase))
        {
            if (_apiController.ServerState == WebAPI.SignalR.Utils.ServerState.Disconnecting)
            {
                _mediator.Publish(new NotificationMessage("RavaSync disconnecting", "Cannot use /toggle while RavaSync is still disconnecting",
                    NotificationType.Error));
            }

            if (_serverConfigurationManager.CurrentServer == null) return;
            var fullPause = splitArgs.Length > 1 ? splitArgs[1] switch
            {
                "on" => false,
                "off" => true,
                _ => !_serverConfigurationManager.CurrentServer.FullPause,
            } : !_serverConfigurationManager.CurrentServer.FullPause;

            if (fullPause != _serverConfigurationManager.CurrentServer.FullPause)
            {
                _serverConfigurationManager.CurrentServer.FullPause = fullPause;
                _serverConfigurationManager.Save();
                _ = _apiController.CreateConnectionsAsync();
            }
        }
        else if (string.Equals(splitArgs[0], "gpose", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.Publish(new UiToggleMessage(typeof(CharaDataHubUi)));
        }
        else if (string.Equals(splitArgs[0], "rescan", StringComparison.OrdinalIgnoreCase))
        {
            _cacheMonitor.InvokeScan();
        }
        else if (string.Equals(splitArgs[0], "perf", StringComparison.OrdinalIgnoreCase))
        {
            if (splitArgs.Length > 1 && int.TryParse(splitArgs[1], CultureInfo.InvariantCulture, out var limitBySeconds))
            {
                _performanceCollectorService.PrintPerformanceStats(limitBySeconds);
            }
            else
            {
                _performanceCollectorService.PrintPerformanceStats();
            }
        }
        else if (string.Equals(splitArgs[0], "medi", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.PrintSubscriberInfo();
        }
        else if (string.Equals(splitArgs[0], "analyze", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.Publish(new UiToggleMessage(typeof(DataAnalysisUi)));
        }
        else if (string.Equals(splitArgs[0], "settings", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.Publish(new UiToggleMessage(typeof(SettingsUi)));
        }
        else if (string.Equals(splitArgs[0], "linktoshell", StringComparison.OrdinalIgnoreCase))
        {
            if (!_dalamudUtil.TryGetRegisterableVenue(_gameGui, out var _, out var reason))
            {
                _mediator.Publish(new NotificationMessage("Cannot register here", reason, NotificationType.Error));
                return;
            }
            _mediator.Publish(new UiToggleMessage(typeof(VenueRegistrationUi)));
        }
        else if (string.Equals(splitArgs[0], "tools", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.Publish(new UiToggleMessage(typeof(ToolsHubUi)));
        }
        else if (string.Equals(splitArgs[0], "games", StringComparison.OrdinalIgnoreCase))
        {
            _mediator.Publish(new UiToggleMessage(typeof(ToyBoxUi)));
        }
    }
}
