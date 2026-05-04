using Dalamud.Game;
using Dalamud.Interface.ImGuiFileDialog;
using Dalamud.Interface.Windowing;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.Game;
using RavaSync.FileCache;
using RavaSync.Fonts;
using RavaSync.Interop;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services;
using RavaSync.Services.CharaData;
using RavaSync.Services.Events;
using RavaSync.Services.Gpu;
using RavaSync.Services.Optimisation;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Themes;
using RavaSync.UI;
using RavaSync.UI.Components;
using RavaSync.UI.Components.Popup;
using RavaSync.UI.Handlers;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NReco.Logging.File;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using RavaSync.Services.Discovery;
using RavaSync.FileCache;
using RavaSync.Utils;


namespace RavaSync;

public sealed class Plugin : IAsyncDalamudPlugin
{
    private readonly IHost _host;
    private readonly string _configDirectory;
    private readonly string _traceDir;
    private bool _hostStarted;

    public Plugin(IDalamudPluginInterface pluginInterface, ICommandManager commandManager, IDataManager gameData,
        IFramework framework, IObjectTable objectTable, IClientState clientState, ICondition condition, IChatGui chatGui,
        IGameGui gameGui, IDtrBar dtrBar, IPluginLog pluginLog, ITargetManager targetManager, INotificationManager notificationManager,
        ITextureProvider textureProvider, IContextMenu contextMenu, IGameInteropProvider gameInteropProvider, IGameConfig gameConfig,
        ISigScanner sigScanner, IPartyList partyList, INamePlateGui nameplate)
    {

        _configDirectory = pluginInterface.ConfigDirectory.FullName;
        _traceDir = Path.Join(_configDirectory, "tracelog");

        Directory.CreateDirectory(_configDirectory);
        Directory.CreateDirectory(_traceDir);

        _host = new HostBuilder()
        .UseContentRoot(_configDirectory)
        .ConfigureLogging(lb =>
        {
            lb.ClearProviders();
            lb.AddDalamudLogging(pluginLog, gameData.HasModifiedGameDataFiles);
            lb.AddFile(Path.Combine(_traceDir, $"mare-trace-{DateTime.Now:yyyy-MM-dd-HH-mm-ss}.log"), (opt) =>
            {
                opt.Append = true;
                opt.RollingFilesConvention = FileLoggerOptions.FileRollingConvention.Ascending;
                opt.MinLevel = LogLevel.Information;
                opt.FileSizeLimitBytes = 50 * 1024 * 1024;
            });
            lb.SetMinimumLevel(LogLevel.Information);
        })
        .ConfigureServices(collection =>
        {
            collection.AddSingleton(new WindowSystem("RavaSync"));
            collection.AddSingleton<FileDialogManager>();
            collection.AddSingleton(new Dalamud.Localization("RavaSync.Localization.", "", useEmbedded: true));

            collection.AddSingleton<IFramework>(framework);
            collection.AddSingleton<IGameInteropProvider>(gameInteropProvider);
            collection.AddSingleton<ISigScanner>(sigScanner);
            collection.AddSingleton<IObjectTable>(objectTable);
            collection.AddSingleton<IPartyList>(partyList);
            collection.AddSingleton<IGameGui>(gameGui);
            collection.AddSingleton<ICommandManager>(commandManager);
            collection.AddSingleton<IChatGui>(chatGui);
            collection.AddSingleton(clientState);
            collection.AddSingleton<MareMediator>();
            collection.AddSingleton<IRavaMesh, RavaMesh>();
            collection.AddSingleton<RavaDiscoveryService>();
            collection.AddSingleton<FileCacheManager>();
            collection.AddSingleton<ServerConfigurationManager>();
            collection.AddSingleton<ApiController>();
            collection.AddSingleton<PerformanceCollectorService>();
            collection.AddSingleton<HubFactory>();
            collection.AddSingleton<FileUploadManager>();
            collection.AddSingleton<FileTransferOrchestrator>();
            collection.AddSingleton<RavaPlugin>();
            collection.AddSingleton<MareProfileManager>();
            collection.AddSingleton<GameObjectHandlerFactory>();
            collection.AddSingleton<FileDownloadManagerFactory>();
            collection.AddSingleton<SafetyGate>();
            collection.AddSingleton<PairHandlerFactory>();
            collection.AddSingleton<PairFactory>();
            collection.AddSingleton<XivDataAnalyzer>();
            collection.AddSingleton<PapSanitisationService>();
            collection.AddSingleton<LocalPapSafetyModService>();
            collection.AddSingleton<CharacterAnalyzer>();
            collection.AddSingleton<CharacterRavaSidecarUtility>();
            collection.AddSingleton<GpuCapabilityService>();
            collection.AddSingleton<GpuTelemetry>();
            collection.AddSingleton<GpuResourcePool>();
            collection.AddSingleton<GpuDeviceService>();
            collection.AddSingleton<D3D11SharedDeviceService>();
            collection.AddSingleton<D3D11ShaderBytecodeCache>();
            collection.AddSingleton<D3D11ComputeService>();
            collection.AddSingleton<D3D11MeshAnalysisService>();
            collection.AddSingleton<D3D11TextureCompressionService>();
            collection.AddSingleton<OptimisationPolicyService>();
            collection.AddSingleton<TextureOptimisationService>();
            collection.AddSingleton<MeshOptimisationService>();
            collection.AddSingleton<TokenProvider>();
            collection.AddSingleton<PluginWarningNotificationService>();
            collection.AddSingleton<FileCompactor>();
            collection.AddSingleton<TagHandler>();
            collection.AddSingleton<IdDisplayHandler>();
            collection.AddSingleton<PlayerPerformanceService>();
            collection.AddSingleton<ThresholdPerfMeshRelayService>();
            collection.AddSingleton<MissingFileMeshService>();
            collection.AddSingleton<TransientResourceManager>();
            collection.AddSingleton<ToyBox>();
            collection.AddSingleton<ModPathResolver>();
            collection.AddSingleton<ObjectIndexCleanupService>();


            collection.AddSingleton<CharaDataManager>();
            collection.AddSingleton<CharaDataFileHandler>();
            collection.AddSingleton<CharaDataCharacterHandler>();
            collection.AddSingleton<CharaDataNearbyManager>();
            collection.AddSingleton<CharaDataGposeTogetherManager>();

            collection.AddSingleton(s => new VfxSpawnManager(s.GetRequiredService<ILogger<VfxSpawnManager>>(),
                gameInteropProvider, s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new BlockedCharacterHandler(s.GetRequiredService<ILogger<BlockedCharacterHandler>>(), gameInteropProvider));
            collection.AddSingleton((s) => new IpcProvider(s.GetRequiredService<ILogger<IpcProvider>>(),
                pluginInterface,
                s.GetRequiredService<CharaDataManager>(),
                s.GetRequiredService<MareMediator>()));
            collection.AddSingleton<SelectPairForTagUi>();
            collection.AddSingleton((s) => new EventAggregator(pluginInterface.ConfigDirectory.FullName,
                s.GetRequiredService<ILogger<EventAggregator>>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new DalamudUtilService(s.GetRequiredService<ILogger<DalamudUtilService>>(),
                clientState, objectTable, framework, gameGui, condition, gameData, targetManager, gameConfig,
                s.GetRequiredService<BlockedCharacterHandler>(), s.GetRequiredService<MareMediator>(), s.GetRequiredService<PerformanceCollectorService>(),
                s.GetRequiredService<MareConfigService>(),partyList));
            collection.AddSingleton((s) => new DtrEntry(s.GetRequiredService<ILogger<DtrEntry>>(), dtrBar, s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<MareMediator>(), s.GetRequiredService<PairManager>(), s.GetRequiredService<ApiController>(), s.GetRequiredService<UiSharedService>()));
            collection.AddSingleton(s => new PairManager(s.GetRequiredService<ILogger<PairManager>>(), s.GetRequiredService<PairFactory>(),
                s.GetRequiredService<MareConfigService>(), s.GetRequiredService<MareMediator>(), contextMenu, s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<IpcManager>(),
                s.GetRequiredService<CharacterRavaSidecarUtility>(), s.GetRequiredService<PlayerPerformanceService>()));
            collection.AddSingleton<RedrawManager>();
            collection.AddSingleton((s) => new IpcCallerPenumbra(s.GetRequiredService<ILogger<IpcCallerPenumbra>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>(), s.GetRequiredService<RedrawManager>()));
            collection.AddSingleton((s) => new IpcCallerGlamourer(s.GetRequiredService<ILogger<IpcCallerGlamourer>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>(), s.GetRequiredService<RedrawManager>()));
            collection.AddSingleton((s) => new IpcCallerCustomize(s.GetRequiredService<ILogger<IpcCallerCustomize>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcCallerHeels(s.GetRequiredService<ILogger<IpcCallerHeels>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcCallerHonorific(s.GetRequiredService<ILogger<IpcCallerHonorific>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcCallerMoodles(s.GetRequiredService<ILogger<IpcCallerMoodles>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcCallerPetNames(s.GetRequiredService<ILogger<IpcCallerPetNames>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcCallerBrio(s.GetRequiredService<ILogger<IpcCallerBrio>>(), pluginInterface,
                s.GetRequiredService<DalamudUtilService>()));
            collection.AddSingleton((s) => new IpcCallerOtherSync(s.GetRequiredService<ILogger<IpcCallerOtherSync>>(), pluginInterface,
                s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new IpcManager(s.GetRequiredService<ILogger<IpcManager>>(),
                s.GetRequiredService<MareMediator>(), s.GetRequiredService<IpcCallerPenumbra>(), s.GetRequiredService<IpcCallerGlamourer>(),
                s.GetRequiredService<IpcCallerCustomize>(), s.GetRequiredService<IpcCallerHeels>(), s.GetRequiredService<IpcCallerHonorific>(),
                s.GetRequiredService<IpcCallerMoodles>(), s.GetRequiredService<IpcCallerPetNames>(), s.GetRequiredService<IpcCallerBrio>(),
                s.GetRequiredService<IpcCallerOtherSync>()));
            collection.AddSingleton((s) => new NotificationService(s.GetRequiredService<ILogger<NotificationService>>(),
                s.GetRequiredService<MareMediator>(), s.GetRequiredService<DalamudUtilService>(),
                notificationManager, chatGui, s.GetRequiredService<MareConfigService>()));
            collection.AddSingleton<CrashRecoveryService>();
            collection.AddSingleton<StorageMaintenanceService>();
            collection.AddSingleton(s => new PcpExportGuard(s.GetRequiredService<ILogger<PcpExportGuard>>(),pluginInterface,
                s.GetRequiredService<PairManager>(),s.GetRequiredService<IpcCallerPenumbra>(),s.GetRequiredService<DalamudUtilService>(), s.GetRequiredService<MareMediator>()));
            collection.AddSingleton((s) => new PairRequestService(
                s.GetRequiredService<ILogger<PairRequestService>>(),
                s.GetRequiredService<MareMediator>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<IRavaMesh>()));

            collection.AddSingleton(s => new PairRequestContextMenuService(
                s.GetRequiredService<ILogger<PairRequestContextMenuService>>(),
                s.GetRequiredService<MareMediator>(),
                contextMenu,                                   
                s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<PairManager>(),
                s.GetRequiredService<RavaDiscoveryService>()));

            collection.AddSingleton(s => new FriendshapedMarkerService(
                s.GetRequiredService<ILogger<FriendshapedMarkerService>>(),
                s.GetRequiredService<MareMediator>(),
                nameplate,
                s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<PairManager>(),
                s.GetRequiredService<RavaDiscoveryService>()));

            collection.AddSingleton((s) =>
            {
                var handler = new SocketsHttpHandler
                {
                    PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                    PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                    MaxConnectionsPerServer = 256,
                    EnableMultipleHttp2Connections = true,
                    AutomaticDecompression = DecompressionMethods.None,
                    Expect100ContinueTimeout = TimeSpan.Zero,
                    UseCookies = false,
                    ConnectTimeout = TimeSpan.FromSeconds(10),

                    KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                    KeepAlivePingDelay = TimeSpan.FromSeconds(20),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(10),

                    AllowAutoRedirect = false

                };

                var httpClient = new HttpClient(handler, disposeHandler: true)
                {
                    Timeout = Timeout.InfiniteTimeSpan
                };

                httpClient.DefaultRequestVersion = HttpVersion.Version20;
                httpClient.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher;
                httpClient.DefaultRequestHeaders.ExpectContinue = false;

                var ver = Assembly.GetExecutingAssembly().GetName().Version;
                httpClient.DefaultRequestHeaders.UserAgent.Add(
                    new ProductInfoHeaderValue("RavaSync", $"{ver!.Major}.{ver!.Minor}.{ver!.Build}"));

                return httpClient;
            });

            collection.AddSingleton((s) => new MareConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new ServerConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new NotesConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new ServerTagConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new TransientConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new XivDataStorageService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new PlayerPerformanceConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton((s) => new CharaDataConfigService(pluginInterface.ConfigDirectory.FullName));
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<MareConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<ServerConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<NotesConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<ServerTagConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<TransientConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<XivDataStorageService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<PlayerPerformanceConfigService>());
            collection.AddSingleton<IConfigService<IMareConfiguration>>(s => s.GetRequiredService<CharaDataConfigService>());
            collection.AddSingleton<ConfigurationMigrator>();
            collection.AddSingleton<ConfigurationSaveService>();
            collection.AddSingleton<IThemeManager>(s =>
            {
                var pluginDir = pluginInterface.AssemblyLocation.Directory?.FullName
                                ?? Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
                                ?? AppContext.BaseDirectory;

                var tm = new ThemeManager(pluginInterface.ConfigDirectory.FullName, pluginDir);
                tm.LoadAll();

                var cfg = s.GetRequiredService<MareConfigService>().Current;

                if (string.IsNullOrWhiteSpace(cfg.SelectedThemeId))
                {
                    cfg.SelectedThemeId = ThemeManager.NoneId;
                    s.GetRequiredService<MareConfigService>().Save();
                }

                if (!tm.TryApply(cfg.SelectedThemeId))
                    tm.TryApply(ThemeManager.NoneId);

                return tm;
            });

            collection.AddSingleton<IFontManager>(s =>
            {
                return new FontManager(pluginInterface, defaultSizePx: 16f);
            });

            collection.AddSingleton<HubFactory>();

            collection.AddSingleton<IFriendResolver, FriendResolver>();
            collection.AddSingleton<ScopeAutoPauseService>();

            collection.AddSingleton((s) => new VenueInviteService(
                s.GetRequiredService<ILogger<VenueInviteService>>(),
                s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<IFramework>(),
                gameGui,
                s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<MareMediator>(), 
                s.GetRequiredService<PairManager>()
                ));

            collection.AddSingleton((s) => new VenueRegistrationService(
                s.GetRequiredService<ILogger<VenueRegistrationService>>(),
                s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<IFramework>(),
                gameGui,
                s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<MareMediator>(), 
                s.GetRequiredService<PairManager>()
                ));


            collection.AddScoped<DrawEntityFactory>();
            collection.AddScoped<CacheMonitor>();
            collection.AddScoped<UiFactory>();
            collection.AddScoped<SelectTagForPairUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, SettingsUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, CompactUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, IntroUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, DownloadUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, MinimisedRestoreUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, TournamentHpUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, PopoutProfileUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, DataAnalysisUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, JoinSyncshellUI>();
            collection.AddScoped<WindowMediatorSubscriberBase, CreateSyncshellUI>();
            collection.AddScoped<WindowMediatorSubscriberBase, EventViewerUI>();
            collection.AddScoped<WindowMediatorSubscriberBase, CharaDataHubUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, VenueRegistrationUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, VenueJoinUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, PairRequestUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, ToolsHubUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, ToyBoxUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, DiscoveryIntroUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, VanityUi>();
            collection.AddScoped<WindowMediatorSubscriberBase, DiscoverySettingsUi>();


            collection.AddScoped<WindowMediatorSubscriberBase, EditProfileUi>((s) => new EditProfileUi(s.GetRequiredService<ILogger<EditProfileUi>>(),
                s.GetRequiredService<MareMediator>(), s.GetRequiredService<ApiController>(), s.GetRequiredService<UiSharedService>(), s.GetRequiredService<FileDialogManager>(),
                s.GetRequiredService<MareProfileManager>(), s.GetRequiredService<PerformanceCollectorService>()));
            collection.AddScoped<WindowMediatorSubscriberBase, PopupHandler>();
            collection.AddScoped<IPopupHandler, BanUserPopupHandler>();
            collection.AddScoped<IPopupHandler, CensusPopupHandler>();
            collection.AddScoped<CacheCreationService>();
            collection.AddScoped<PlayerDataFactory>();
            collection.AddScoped<VisibleUserDataDistributor>();
            collection.AddScoped((s) => new UiService(s.GetRequiredService<ILogger<UiService>>(), pluginInterface.UiBuilder, s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<WindowSystem>(), s.GetServices<WindowMediatorSubscriberBase>(),
                s.GetRequiredService<UiFactory>(),
                s.GetRequiredService<FileDialogManager>(), s.GetRequiredService<MareMediator>()));
            collection.AddScoped((s) => new CommandManagerService(commandManager, s.GetRequiredService<PerformanceCollectorService>(),
                s.GetRequiredService<ServerConfigurationManager>(), s.GetRequiredService<CacheMonitor>(), s.GetRequiredService<ApiController>(),
                s.GetRequiredService<MareMediator>(), s.GetRequiredService<MareConfigService>(), gameGui, s.GetRequiredService<DalamudUtilService>(),
                s.GetRequiredService<StorageMaintenanceService>()));
            collection.AddScoped((s) => new UiSharedService(
                s.GetRequiredService<ILogger<UiSharedService>>(),
                s.GetRequiredService<IpcManager>(),
                s.GetRequiredService<ApiController>(),
                s.GetRequiredService<CacheMonitor>(),
                s.GetRequiredService<FileDialogManager>(),
                s.GetRequiredService<MareConfigService>(),
                s.GetRequiredService<DalamudUtilService>(),
                pluginInterface,
                textureProvider,
                s.GetRequiredService<Dalamud.Localization>(),
                s.GetRequiredService<ServerConfigurationManager>(),
                s.GetRequiredService<TokenProvider>(),
                s.GetRequiredService<MareMediator>(),
                s.GetRequiredService<IThemeManager>(),
                s.GetRequiredService<IFontManager>()
            ));

            collection.AddHostedService(p => p.GetRequiredService<ConfigurationSaveService>());
            collection.AddHostedService(p => p.GetRequiredService<MareMediator>());
            collection.AddHostedService(p => p.GetRequiredService<NotificationService>());
            collection.AddHostedService(p => p.GetRequiredService<CrashRecoveryService>());
            collection.AddHostedService(p => p.GetRequiredService<ConfigurationMigrator>());
            collection.AddHostedService(p => p.GetRequiredService<FileCacheManager>());
            collection.AddHostedService(p => p.GetRequiredService<PapSanitisationService>());
            collection.AddHostedService(p => p.GetRequiredService<DalamudUtilService>());
            collection.AddHostedService(p => p.GetRequiredService<PerformanceCollectorService>());
            collection.AddHostedService(p => p.GetRequiredService<DtrEntry>());
            collection.AddHostedService(p => p.GetRequiredService<EventAggregator>());
            collection.AddHostedService(p => p.GetRequiredService<IpcProvider>());
            collection.AddHostedService(p => p.GetRequiredService<RavaPlugin>());
            collection.AddHostedService(p => p.GetRequiredService<RavaDiscoveryService>());
            collection.AddHostedService(p => p.GetRequiredService<ToyBox>());


        })
        .Build();
    }

    public async Task LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        CdnPrewarmHelper.Initialize(_configDirectory);

        _ = Task.Run(() => DeleteOldTraceFilesBestEffortAsync(_traceDir), CancellationToken.None);

        // These are intentionally activated before hosted services start because they register
        // long-lived behaviours, mediator subscribers, context menus, or plugin-scoped handlers.
        _ = _host.Services.GetRequiredService<VenueInviteService>();
        _ = _host.Services.GetRequiredService<VenueRegistrationService>();
        _ = _host.Services.GetRequiredService<PcpExportGuard>();
        _ = _host.Services.GetRequiredService<PairRequestService>();
        _ = _host.Services.GetRequiredService<PairRequestContextMenuService>();
        _ = _host.Services.GetRequiredService<FriendshapedMarkerService>();
        _ = _host.Services.GetRequiredService<ThresholdPerfMeshRelayService>();
        _ = _host.Services.GetRequiredService<MissingFileMeshService>();
        _ = _host.Services.GetRequiredService<ScopeAutoPauseService>();

        await _host.StartAsync(cancellationToken).ConfigureAwait(false);
        _hostStarted = true;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_hostStarted)
            {
                await _host.StopAsync(CancellationToken.None).ConfigureAwait(false);
                _hostStarted = false;
            }
        }
        finally
        {
            if (_host is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                _host.Dispose();
            }
        }
    }

    private static async Task DeleteOldTraceFilesBestEffortAsync(string traceDir)
    {
        try
        {
            if (!Directory.Exists(traceDir))
                return;

            var oldFiles = Directory.EnumerateFiles(traceDir)
                .Select(f => new FileInfo(f))
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .Skip(9)
                .ToArray();

            foreach (var file in oldFiles)
            {
                for (var attempt = 0; attempt < 5; attempt++)
                {
                    try
                    {
                        file.Delete();
                        break;
                    }
                    catch
                    {
                        await Task.Delay(500).ConfigureAwait(false);
                    }
                }

                await Task.Yield();
            }
        }
        catch
        {
            // Best-effort cleanup only. Never fail plugin load because old logs were locked.
        }
    }
}