using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.GameFonts;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using Dalamud.Utility;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Comparer;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.Fonts;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Themes;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using RavaSync.WebAPI.SignalR.Utils;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Numerics;
using System.Text;
using System.Text.Json;

namespace RavaSync.UI;

public class SettingsUi : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly CacheMonitor _cacheMonitor;
    private readonly MareConfigService _configService;
    private readonly ConcurrentDictionary<GameObjectHandler, Dictionary<string, FileDownloadStatus>> _currentDownloads = new();
    private readonly DalamudUtilService _dalamudUtilService;
    private readonly HttpClient _httpClient;
    private readonly FileCacheManager _fileCacheManager;
    private readonly FileCompactor _fileCompactor;
    private readonly FileUploadManager _fileTransferManager;
    private readonly FileTransferOrchestrator _fileTransferOrchestrator;
    private readonly IpcManager _ipcManager;
    private readonly PairManager _pairManager;
    private readonly PerformanceCollectorService _performanceCollector;
    private readonly PlayerPerformanceConfigService _playerPerformanceConfigService;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly UiSharedService _uiShared;
    private readonly IProgress<(int, int, FileCacheEntity)> _validationProgress;
    private (int, int, FileCacheEntity) _currentProgress;
    private bool _deleteAccountPopupModalShown = false;
    private bool _deleteFilesPopupModalShown = false;
    private string _lastTab = string.Empty;
    private bool? _notesSuccessfullyApplied = null;
    private bool _overwriteExistingLabels = true;
    private bool _readClearCache = false;
    private int _selectedEntry = -1;
    private string _uidToAddForIgnore = string.Empty;
    private CancellationTokenSource? _validationCts;
    private Task<List<FileCacheEntity>>? _validationTask;
    private bool _wasOpen = false;
    private string _newUserAlias = string.Empty;
    private string _vanityStatusMessage = string.Empty;
    private Vector4 _vanityStatusColor = ImGuiColors.ParsedGreen;
    private readonly IThemeManager _themeManager;
    private readonly IFontManager _fontManager;
    private int _themeSelectedIndex = -1;
    private Themes.Theme? _editingTheme;
    private List<string> _availableGameFonts = new();
    private string _renameBuffer = string.Empty;

    private readonly Dictionary<string, string> _languages = new(StringComparer.Ordinal)
    {
        { "English", "en" },
        { "Deutsch", "de" },
        { "Français", "fr" },
        { "Español", "es" },
        { "Italiano", "it" },
        { "العربية", "ar" },
        { "日本語", "ja" },
        { "中文", "zh" },
        { "한국어", "ko" },
        { "Русский", "ru" },
        { "Pirate", "pirate" },
        { "Olde English", "olde" },
    };
    private int _languageIndex = 0;


    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public SettingsUi(ILogger<SettingsUi> logger,
        UiSharedService uiShared, MareConfigService configService,
        PairManager pairManager,
        ServerConfigurationManager serverConfigurationManager,
        PlayerPerformanceConfigService playerPerformanceConfigService,
        MareMediator mediator, PerformanceCollectorService performanceCollector,
        FileUploadManager fileTransferManager,
        FileTransferOrchestrator fileTransferOrchestrator,
        FileCacheManager fileCacheManager,
        FileCompactor fileCompactor, ApiController apiController,
        IpcManager ipcManager, CacheMonitor cacheMonitor,
        DalamudUtilService dalamudUtilService, HttpClient httpClient, IThemeManager themeManager, IFontManager fontManager) : base(logger, mediator, "RavaSync Settings", performanceCollector)
    {
        _configService = configService;
        _pairManager = pairManager;
        _serverConfigurationManager = serverConfigurationManager;
        _playerPerformanceConfigService = playerPerformanceConfigService;
        _performanceCollector = performanceCollector;
        _fileTransferManager = fileTransferManager;
        _fileTransferOrchestrator = fileTransferOrchestrator;
        _fileCacheManager = fileCacheManager;
        _apiController = apiController;
        _ipcManager = ipcManager;
        _cacheMonitor = cacheMonitor;
        _dalamudUtilService = dalamudUtilService;
        _httpClient = httpClient;
        _fileCompactor = fileCompactor;
        _uiShared = uiShared;
        _themeManager = themeManager;
        _fontManager = fontManager;
        AllowClickthrough = false;
        AllowPinning = false;
        _validationProgress = new Progress<(int, int, FileCacheEntity)>(v => _currentProgress = v);

        var savedLang = _configService.Current.LanguageCode;
        if (string.IsNullOrWhiteSpace(savedLang)) savedLang = "en";
        var lidx = _languages.Values.ToList().FindIndex(v => string.Equals(v, savedLang, StringComparison.OrdinalIgnoreCase));
        _languageIndex = lidx >= 0 ? lidx : 0;

        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new Vector2(800, 400),
            MaximumSize = new Vector2(800, 2000),
        };
        //Set rounded, feels nicer
        _themeManager.ThemeChanged += t =>
        {
            ImGui.GetStyle().FrameRounding = t.Effects.RoundedUi ? 8f : 0f;
        };

        Mediator.Subscribe<OpenSettingsUiMessage>(this, (_) => Toggle());
        Mediator.Subscribe<SwitchToIntroUiMessage>(this, (_) => IsOpen = false);
        Mediator.Subscribe<CutsceneStartMessage>(this, (_) => UiSharedService_GposeStart());
        Mediator.Subscribe<CutsceneEndMessage>(this, (_) => UiSharedService_GposeEnd());
        Mediator.Subscribe<CharacterDataCreatedMessage>(this, (msg) => LastCreatedCharacterData = msg.CharacterData);
        Mediator.Subscribe<DownloadStartedMessage>(this, (msg) => _currentDownloads[msg.DownloadId] = msg.DownloadStatus);
        Mediator.Subscribe<DownloadFinishedMessage>(this, (msg) => _currentDownloads.TryRemove(msg.DownloadId, out _));
    }

    public CharacterData? LastCreatedCharacterData { private get; set; }
    private ApiController ApiController => _uiShared.ApiController;

    public override void OnOpen()
    {
        _uiShared.ResetOAuthTasksState();
        _speedTestCts = new();
    }

    public override void OnClose()
    {
        _uiShared.EditTrackerPosition = false;
        _uidToAddForIgnore = string.Empty;
        _secretKeysConversionCts = _secretKeysConversionCts.CancelRecreate();
        _downloadServersTask = null;
        _speedTestTask = null;
        _speedTestCts?.Cancel();
        _speedTestCts?.Dispose();
        _speedTestCts = null;

        base.OnClose();
    }

    protected override void DrawInternal()
    {
        _ = _uiShared.DrawOtherPluginState();
        DrawSettingsContent();
    }

    private bool InputDtrColors(string label, ref DtrEntry.Colors colors)
    {
        using var id = ImRaii.PushId(label);
        var innerSpacing = ImGui.GetStyle().ItemInnerSpacing.X;
        var foregroundColor = ConvertColor(colors.Foreground);
        var glowColor = ConvertColor(colors.Glow);

        var ret = ImGui.ColorEdit3("###foreground", ref foregroundColor, ImGuiColorEditFlags.NoInputs | ImGuiColorEditFlags.NoLabel | ImGuiColorEditFlags.Uint8);
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip(_uiShared.L("UI.SettingsUi.5cc01e7c", "Foreground Color - Set to pure black (#000000) to use the default color"));

        ImGui.SameLine(0.0f, innerSpacing);
        ret |= ImGui.ColorEdit3("###glow", ref glowColor, ImGuiColorEditFlags.NoInputs | ImGuiColorEditFlags.NoLabel | ImGuiColorEditFlags.Uint8);
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip(_uiShared.L("UI.SettingsUi.e31591ad", "Glow Color - Set to pure black (#000000) to use the default color"));

        ImGui.SameLine(0.0f, innerSpacing);
        ImGui.TextUnformatted(label);

        if (ret)
            colors = new(ConvertBackColor(foregroundColor), ConvertBackColor(glowColor));

        return ret;

        static Vector3 ConvertColor(uint color)
            => unchecked(new((byte)color / 255.0f, (byte)(color >> 8) / 255.0f, (byte)(color >> 16) / 255.0f));

        static uint ConvertBackColor(Vector3 color)
            => byte.CreateSaturating(color.X * 255.0f) | ((uint)byte.CreateSaturating(color.Y * 255.0f) << 8) | ((uint)byte.CreateSaturating(color.Z * 255.0f) << 16);
    }

    private void DrawBlockedTransfers()
    {
        _lastTab = "BlockedTransfers";
        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.98ba8eec", "Files that you attempted to upload or download that were forbidden to be transferred by their creators will appear here. ") +
                             "If you see file paths from your drive here, then those files were not allowed to be uploaded. If you see hashes, those files were not allowed to be downloaded. " +
                             "Ask your paired friend to send you the mod in question through other means, acquire the mod yourself or pester the mod creator to allow it to be sent over RavaSync.",
            ImGuiColors.DalamudGrey);

        if (ImGui.BeginTable(_uiShared.L("UI.SettingsUi.87f3bf49", "TransfersTable"), 2, ImGuiTableFlags.SizingStretchProp))
        {
            ImGui.TableSetupColumn(
                $"Hash/Filename");
            ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.c0a5ac1b", "Forbidden by"));

            ImGui.TableHeadersRow();

            foreach (var item in _fileTransferOrchestrator.ForbiddenTransfers)
            {
                ImGui.TableNextColumn();
                if (item is UploadFileTransfer transfer)
                {
                    ImGui.TextUnformatted(transfer.LocalFile);
                }
                else
                {
                    ImGui.TextUnformatted(item.Hash);
                }
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(item.ForbiddenBy);
            }
            ImGui.EndTable();
        }
    }

    private void DrawCurrentTransfers()
    {
        _lastTab = "Transfers";
        _uiShared.BigText("Transfer Settings");

        int maxParallelDownloads = _configService.Current.ParallelDownloads;
        int maxParallelUploads = _configService.Current.ParallelUploads;
        bool useAlternativeUpload = _configService.Current.UseAlternativeFileUpload;
        int downloadSpeedLimit = _configService.Current.DownloadSpeedLimitInBytes;

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.7a28f585", "Global Download Speed Limit"));
        ImGui.SameLine();
        ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
        if (ImGui.InputInt("###speedlimit", ref downloadSpeedLimit))
        {
            _configService.Current.DownloadSpeedLimitInBytes = downloadSpeedLimit;
            _configService.Save();
            Mediator.Publish(new DownloadLimitChangedMessage());
        }
        ImGui.SameLine();
        ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
        _uiShared.DrawCombo("###speed", [DownloadSpeeds.Bps, DownloadSpeeds.KBps, DownloadSpeeds.MBps],
            (s) => s switch
            {
                DownloadSpeeds.Bps => "Byte/s",
                DownloadSpeeds.KBps => "KB/s",
                DownloadSpeeds.MBps => "MB/s",
                _ => throw new NotSupportedException()
            }, (s) =>
            {
                _configService.Current.DownloadSpeedType = s;
                _configService.Save();
                Mediator.Publish(new DownloadLimitChangedMessage());
            }, _configService.Current.DownloadSpeedType);
        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.e63c3e99", "0 = No limit/infinite"));

        if (ImGui.SliderInt(_uiShared.L("UI.SettingsUi.1a9197dd", "Maximum Parallel Downloads"), ref maxParallelDownloads, 0, 12))
        {
            _configService.Current.ParallelDownloads = maxParallelDownloads;
            _configService.Save();
        }

        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.TextDisabled(_uiShared.L("UI.SettingsUi.0e7c1b4a", "(0 = Auto)"));

        if (ImGui.SliderInt(_uiShared.L("UI.SettingsUi.5e527747", "Maximum Parallel Uploads"), ref maxParallelUploads, 0, 12))
        {
            _configService.Current.ParallelUploads = maxParallelUploads;
            _configService.Save();
        }

        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.TextDisabled(_uiShared.L("UI.SettingsUi.0e7c1b4a", "(0 = Auto)"));

        ImGui.Separator();
        _uiShared.BigText("Transfer UI");

        bool showTransferWindow = _configService.Current.ShowTransferWindow;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.101ac3ed", "Show separate transfer window"), ref showTransferWindow))
        {
            _configService.Current.ShowTransferWindow = showTransferWindow;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L(
            "UI.SettingsUi.TransferWindow.Help",
            "The download window will show the current progress of outstanding downloads.\n\n" +
            "What do W/Q/P/D stand for?\n" +
            "W = Waiting for Slot (see Maximum Parallel Downloads)\n" +
            "Q = Queued on Server, waiting for queue ready signal\n" +
            "P = Processing download (aka downloading)\n" +
            "D = Decompressing download"));

        if (!_configService.Current.ShowTransferWindow) ImGui.BeginDisabled();
        ImGui.Indent();
        bool editTransferWindowPosition = _uiShared.EditTrackerPosition;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.b8fa2338", "Edit Transfer Window position"), ref editTransferWindowPosition))
        {
            _uiShared.EditTrackerPosition = editTransferWindowPosition;
        }
        ImGui.Unindent();
        if (!_configService.Current.ShowTransferWindow) ImGui.EndDisabled();

        bool showGlobalTransferBars = _configService.Current.ShowGlobalTransferBars;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.0069fc2b", "Show global transfer bars"), ref showGlobalTransferBars))
        {
            _configService.Current.ShowGlobalTransferBars = showGlobalTransferBars;
            _configService.Save();
        }

        ImGui.SameLine();

        var isEditingGlobal = _configService.Current.EditGlobalTransferOverlay;
        var editLabel = isEditingGlobal
            ? _uiShared.L("UI.SettingsUi.GlobalBars.Edit.Done", "Done editing global bars") + "##editGlobalTransfers"
            : _uiShared.L("UI.SettingsUi.GlobalBars.Edit.Start", "Edit global bars location") + "##editGlobalTransfers";
        if (ImGui.SmallButton(editLabel))
        {
            _configService.Current.EditGlobalTransferOverlay = !isEditingGlobal;
            _configService.Save();
        }

        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.f397600d", "Shows a single download bar and a single upload bar when transfers are active."));

        bool showUploadProgress = _configService.Current.ShowUploadProgress;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.cf813deb", "Show upload progress bar"), ref showUploadProgress))
        {
            _configService.Current.ShowUploadProgress = showUploadProgress;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.UploadProgress.Help", "Shows a global upload bar when uploads are active.\nUse this if you want individual download bars but still want to see your own upload progress"));

        bool showTransferText = _configService.Current.showTransferText;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.6344c308", "Show transfers in main UI"), ref showTransferText))
        {
            _configService.Current.showTransferText = showTransferText;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.8d585dd8", "This will show downloads / uploads in progress as text above your pairs list"));

        bool showTransferBars = _configService.Current.ShowTransferBars;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.9575bb11", "Show transfer bars rendered below players"), ref showTransferBars))
        {
            _configService.Current.ShowTransferBars = showTransferBars;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.12ab1786", "This will render a progress bar during the download at the feet of the player you are downloading from."));

        if (!showTransferBars) ImGui.BeginDisabled();

        ImGui.Indent();
        bool transferBarShowText = _configService.Current.TransferBarsShowText;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.dd38ab4a", "Show Download Text"), ref transferBarShowText))
        {
            _configService.Current.TransferBarsShowText = transferBarShowText;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.f714f031", "Shows download text (amount of MiB downloaded) in the transfer bars"));
        int transferBarWidth = _configService.Current.TransferBarsWidth;
        if (ImGui.SliderInt(_uiShared.L("UI.SettingsUi.5e9e0b2c", "Transfer Bar Width"), ref transferBarWidth, 10, 500))
        {
            _configService.Current.TransferBarsWidth = transferBarWidth;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.ee00620c", "Width of the displayed transfer bars (will never be less wide than the displayed text)"));
        int transferBarHeight = _configService.Current.TransferBarsHeight;
        if (ImGui.SliderInt(_uiShared.L("UI.SettingsUi.916795f0", "Transfer Bar Height"), ref transferBarHeight, 2, 50))
        {
            _configService.Current.TransferBarsHeight = transferBarHeight;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.116df63f", "Height of the displayed transfer bars (will never be less tall than the displayed text)"));
        ImGui.Unindent();

        if (!showTransferBars) ImGui.EndDisabled();

        bool showUploading = _configService.Current.ShowUploading;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.44ec0bc8", "Show 'Uploading' text below players that are currently uploading"), ref showUploading))
        {
            _configService.Current.ShowUploading = showUploading;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.e336b219", "This will render an 'Uploading' text at the feet of the player that is in progress of uploading data."));

        if (!showUploading) ImGui.BeginDisabled();
        ImGui.Indent();
        bool showUploadingBigText = _configService.Current.ShowUploadingBigText;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.a4d54600", "Large font for 'Uploading' text"), ref showUploadingBigText))
        {
            _configService.Current.ShowUploadingBigText = showUploadingBigText;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.3ae6024b", "This will render an 'Uploading' text in a larger font."));
        ImGui.Unindent();
        if (!showUploading) ImGui.EndDisabled();


        if (_apiController.IsConnected)
        {
            ImGuiHelpers.ScaledDummy(5);
            ImGui.Separator();
            ImGuiHelpers.ScaledDummy(10);
            using var tree = ImRaii.TreeNode("Speed Test to Servers");
            if (tree)
            {
                if (_downloadServersTask == null || ((_downloadServersTask?.IsCompleted ?? false) && (!_downloadServersTask?.IsCompletedSuccessfully ?? false)))
                {
                    if (_uiShared.IconTextButton(FontAwesomeIcon.GroupArrowsRotate, _uiShared.L("UI.SettingsUi.79d90b01", "Update Download Server List")))
                    {
                        _downloadServersTask = GetDownloadServerList();
                    }
                }
                if (_downloadServersTask != null && _downloadServersTask.IsCompleted && !_downloadServersTask.IsCompletedSuccessfully)
                {
                    UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.49c6d1dc", "Failed to get download servers from service, see /xllog for more information"), ImGuiColors.DalamudRed);
                }
                if (_downloadServersTask != null && _downloadServersTask.IsCompleted && _downloadServersTask.IsCompletedSuccessfully)
                {
                    if (_speedTestTask == null || _speedTestTask.IsCompleted)
                    {
                        if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowRight, _uiShared.L("UI.SettingsUi.999d1555", "Start Speedtest")))
                        {
                            _speedTestTask = RunSpeedTest(_downloadServersTask.Result!, _speedTestCts?.Token ?? CancellationToken.None);
                        }
                    }
                    else if (!_speedTestTask.IsCompleted)
                    {
                        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.c4d53e09", "Running Speedtest to File Servers..."), ImGuiColors.DalamudYellow);
                        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.ae3ed8e0", "Please be patient, depending on usage and load this can take a while."), ImGuiColors.DalamudYellow);
                        if (_uiShared.IconTextButton(FontAwesomeIcon.Ban, _uiShared.L("UI.SettingsUi.0ae156b6", "Cancel speedtest")))
                        {
                            _speedTestCts?.Cancel();
                            _speedTestCts?.Dispose();
                            _speedTestCts = new();
                        }
                    }
                    if (_speedTestTask != null && _speedTestTask.IsCompleted)
                    {
                        if (_speedTestTask.Result != null && _speedTestTask.Result.Count != 0)
                        {
                            foreach (var result in _speedTestTask.Result)
                            {
                                UiSharedService.TextWrapped(result);
                            }
                        }
                        else
                        {
                            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.3fd78cce", "Speedtest completed with no results"), ImGuiColors.DalamudYellow);
                        }
                    }
                }
            }
            ImGuiHelpers.ScaledDummy(10);
        }

        ImGui.Separator();
        _uiShared.BigText("Current Transfers");

        if (ImGui.BeginTabBar(_uiShared.L("UI.SettingsUi.0abab5b8", "TransfersTabBar")))
        {
            if (ApiController.ServerState is ServerState.Connected && ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.812412a0", "Transfers")))
            {
                ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.227cc640", "Uploads"));
                if (ImGui.BeginTable(_uiShared.L("UI.SettingsUi.e295d0ac", "UploadsTable"), 3))
                {
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.2c3cafa4", "File"));
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.80c49489", "Uploaded"));
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.b7152342", "Size"));
                    ImGui.TableHeadersRow();
                    foreach (var transfer in _fileTransferManager.CurrentUploads.ToArray())
                    {
                        var color = UiSharedService.UploadColor((transfer.Transferred, transfer.Total));
                        var col = ImRaii.PushColor(ImGuiCol.Text, color);
                        ImGui.TableNextColumn();
                        ImGui.TextUnformatted(transfer.Hash);
                        ImGui.TableNextColumn();
                        ImGui.TextUnformatted(UiSharedService.ByteToString(transfer.Transferred));
                        ImGui.TableNextColumn();
                        ImGui.TextUnformatted(UiSharedService.ByteToString(transfer.Total));
                        col.Dispose();
                        ImGui.TableNextRow();
                    }

                    ImGui.EndTable();
                }
                ImGui.Separator();
                ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.a862c2b2", "Downloads"));
                if (ImGui.BeginTable(_uiShared.L("UI.SettingsUi.4a1bbd90", "DownloadsTable"), 4))
                {
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.9f8a2389", "User"));
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.cb0cb170", "Server"));
                    ImGui.TableSetupColumn(_uiShared.L("UI.CharaDataHubUiMcdOnline.6ce6c512", "Files"));
                    ImGui.TableSetupColumn(_uiShared.L("UI.SettingsUi.a479c9c3", "Download"));
                    ImGui.TableHeadersRow();

                    foreach (var transfer in _currentDownloads.ToArray())
                    {
                        var userName = transfer.Key.Name;
                        foreach (var entry in transfer.Value)
                        {
                            var color = UiSharedService.UploadColor((entry.Value.TransferredBytes, entry.Value.TotalBytes));
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(userName);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(entry.Key);
                            var col = ImRaii.PushColor(ImGuiCol.Text, color);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(entry.Value.TransferredFiles + "/" + entry.Value.TotalFiles);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(UiSharedService.ByteToString(entry.Value.TransferredBytes) + "/" + UiSharedService.ByteToString(entry.Value.TotalBytes));
                            ImGui.TableNextColumn();
                            col.Dispose();
                            ImGui.TableNextRow();
                        }
                    }

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.2654a438", "Blocked Transfers")))
            {
                DrawBlockedTransfers();
                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }
    }

    private Task<List<string>?>? _downloadServersTask = null;
    private Task<List<string>?>? _speedTestTask = null;
    private CancellationTokenSource? _speedTestCts;

    private async Task<List<string>?> RunSpeedTest(List<string> servers, CancellationToken token)
    {
        List<string> speedTestResults = new();
        foreach (var server in servers)
        {
            HttpResponseMessage? result = null;
            Stopwatch? st = null;
            try
            {
                result = await _fileTransferOrchestrator.SendRequestAsync(HttpMethod.Get, new Uri(new Uri(server), "speedtest/run"), token, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
                result.EnsureSuccessStatusCode();
                using CancellationTokenSource speedtestTimeCts = new();
                speedtestTimeCts.CancelAfter(TimeSpan.FromSeconds(10));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(speedtestTimeCts.Token, token);
                long readBytes = 0;
                st = Stopwatch.StartNew();
                try
                {
                    var stream = await result.Content.ReadAsStreamAsync(linkedCts.Token).ConfigureAwait(false);
                    byte[] buffer = new byte[8192];
                    while (!speedtestTimeCts.Token.IsCancellationRequested)
                    {
                        var currentBytes = await stream.ReadAsync(buffer, linkedCts.Token).ConfigureAwait(false);
                        if (currentBytes == 0)
                            break;
                        readBytes += currentBytes;
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Speedtest to {server} cancelled", server);
                }
                st.Stop();
                _logger.LogInformation("Downloaded {bytes} from {server} in {time}", UiSharedService.ByteToString(readBytes), server, st.Elapsed);
                var bps = (long)((readBytes) / st.Elapsed.TotalSeconds);
                speedTestResults.Add($"{server}: ~{UiSharedService.ByteToString(bps)}/s");
            }
            catch (HttpRequestException ex)
            {
                if (result != null)
                {
                    var res = await result!.Content.ReadAsStringAsync().ConfigureAwait(false);
                    speedTestResults.Add($"{server}: {ex.Message} - {res}");
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Speedtest on {server} cancelled", server);
                speedTestResults.Add($"{server}: Cancelled by user");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Some exception");
            }
            finally
            {
                st?.Stop();
            }
        }
        return speedTestResults;
    }

    private async Task<List<string>?> GetDownloadServerList()
    {
        try
        {
            var result = await _fileTransferOrchestrator.SendRequestAsync(HttpMethod.Get, new Uri(_fileTransferOrchestrator.FilesCdnUri!, "/files/downloadServers"), CancellationToken.None).ConfigureAwait(false);
            result.EnsureSuccessStatusCode();
            return await JsonSerializer.DeserializeAsync<List<string>>(await result.Content.ReadAsStreamAsync().ConfigureAwait(false)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get download server list");
            throw;
        }
    }

    private void DrawDebug()
    {
        _lastTab = "Debug";

        _uiShared.BigText("Debug");
#if DEBUG
        if (LastCreatedCharacterData != null && ImGui.TreeNode(_uiShared.L("UI.SettingsUi.558eacf1", "Last created character data")))
        {
            foreach (var l in JsonSerializer.Serialize(LastCreatedCharacterData, new JsonSerializerOptions() { WriteIndented = true }).Split('\n'))
            {
                ImGui.TextUnformatted($"{l}");
            }

            ImGui.TreePop();
        }
#endif
        if (_uiShared.IconTextButton(FontAwesomeIcon.Copy, _uiShared.L("UI.SettingsUi.f4c205e5", "[DEBUG] Copy Last created Character Data to clipboard")))
        {
            if (LastCreatedCharacterData != null)
            {
                ImGui.SetClipboardText(JsonSerializer.Serialize(LastCreatedCharacterData, new JsonSerializerOptions() { WriteIndented = true }));
            }
            else
            {
                ImGui.SetClipboardText(_uiShared.L("UI.SettingsUi.3134ddad", "ERROR: No created character data, cannot copy."));
            }
        }
        UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.02fba0bb", "Use this when reporting mods being rejected from the server."));

        _uiShared.DrawCombo("Log Level", Enum.GetValues<LogLevel>(), (l) => l.ToString(), (l) =>
        {
            _configService.Current.LogLevel = l;
            _configService.Save();
        }, _configService.Current.LogLevel);

        bool logPerformance = _configService.Current.LogPerformance;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.e506ba35", "Log Performance Counters"), ref logPerformance))
        {
            _configService.Current.LogPerformance = logPerformance;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.b0ac16b3", "Enabling this can incur a (slight) performance impact. Enabling this for extended periods of time is not recommended."));

        using (ImRaii.Disabled(!logPerformance))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.StickyNote, _uiShared.L("UI.SettingsUi.9d3b929c", "Print Performance Stats to /xllog")))
            {
                _performanceCollector.PrintPerformanceStats();
            }
            ImGui.SameLine();
            if (_uiShared.IconTextButton(FontAwesomeIcon.StickyNote, _uiShared.L("UI.SettingsUi.b32ecb8a", "Print Performance Stats (last 60s) to /xllog")))
            {
                _performanceCollector.PrintPerformanceStats(60);
            }
        }

        bool stopWhining = _configService.Current.DebugStopWhining;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.4a27ebc9", "Do not notify for modified game files or enabled LOD"), ref stopWhining))
        {
            _configService.Current.DebugStopWhining = stopWhining;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.a66a582f", "Having modified game files will still mark your logs with UNSUPPORTED and you will not receive support, message shown or not.") + UiSharedService.TooltipSeparator
            + "Keeping LOD enabled can lead to more crashes. Use at your own risk.");
    }

    private void DrawFileStorageSettings()
    {
        _lastTab = "FileCache";

        _uiShared.BigText("Export MCDF");

        ImGuiHelpers.ScaledDummy(10);

        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.df2b286b", "Exporting MCDF has moved."), ImGuiColors.DalamudYellow);
        ImGuiHelpers.ScaledDummy(5);
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.5E116F99", "It is now found in the Main UI under \"Your User Menu\" ("));
        ImGui.SameLine();
        _uiShared.IconText(FontAwesomeIcon.UserCog);
        ImGui.SameLine();
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.323BBFEE", ") -> \"Character Data Hub\"."));
        if (_uiShared.IconTextButton(FontAwesomeIcon.Running, _uiShared.L("UI.SettingsUi.13a3810c", "Open RavaSync Character Data Hub")))
        {
            Mediator.Publish(new UiToggleMessage(typeof(CharaDataHubUi)));
        }
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.1081763b", "Note: this entry will be removed in the near future. Please use the Main UI to open the Character Data Hub."));
        ImGuiHelpers.ScaledDummy(5);
        ImGui.Separator();

        _uiShared.BigText("Storage");

        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.d842e127", "RavaSync Stores downloaded files from paired people permanently. This is to improve loading performance and requiring less downloads. ") +
            _uiShared.L("UI.SettingsUi.Storage.SelfClearing", "The storage governs itself by clearing data beyond the set storage size. Please set the storage size accordingly. It is not necessary to manually clear the storage."));

        _uiShared.DrawFileScanState();
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.7f459f61", "Monitoring Penumbra Folder: ") + (_cacheMonitor.PenumbraWatcher?.Path ?? _uiShared.L("UI.SettingsUi.93c63aad", "Not monitoring")));
        if (string.IsNullOrEmpty(_cacheMonitor.PenumbraWatcher?.Path))
        {
            ImGui.SameLine();
            using var id = ImRaii.PushId("penumbraMonitor");
            if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowsToCircle, _uiShared.L("UI.SettingsUi.5da43fcc", "Try to reinitialize Monitor")))
            {
                _cacheMonitor.StartPenumbraWatcher(_ipcManager.Penumbra.ModDirectory);
            }
        }

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.6f70504a", "Monitoring RavaSync Storage Folder: ") + (_cacheMonitor.MareWatcher?.Path ?? _uiShared.L("UI.SettingsUi.93c63aad", "Not monitoring")));
        if (string.IsNullOrEmpty(_cacheMonitor.MareWatcher?.Path))
        {
            ImGui.SameLine();
            using var id = ImRaii.PushId("mareMonitor");
            if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowsToCircle, _uiShared.L("UI.SettingsUi.5da43fcc", "Try to reinitialize Monitor")))
            {
                _cacheMonitor.StartMareWatcher(_configService.Current.CacheFolder);
            }
        }
        if (_cacheMonitor.MareWatcher == null || _cacheMonitor.PenumbraWatcher == null)
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Play, _uiShared.L("UI.SettingsUi.b5d740f9", "Resume Monitoring")))
            {
                _cacheMonitor.StartMareWatcher(_configService.Current.CacheFolder);
                _cacheMonitor.StartPenumbraWatcher(_ipcManager.Penumbra.ModDirectory);
                _cacheMonitor.InvokeScan();
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.25900a9b", "Attempts to resume monitoring for both Penumbra and RavaSync Storage. ")
                + "Resuming the monitoring will also force a full scan to run." + Environment.NewLine
                + "If the button remains present after clicking it, consult /xllog for errors");
        }
        else
        {
            using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
            {
                if (_uiShared.IconTextButton(FontAwesomeIcon.Stop, _uiShared.L("UI.SettingsUi.7831d508", "Stop Monitoring")))
                {
                    _cacheMonitor.StopMonitoring();
                }
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.f89f03a3", "Stops the monitoring for both Penumbra and RavaSync Storage. ")
                + "Do not stop the monitoring, unless you plan to move the Penumbra and RavaSync Storage folders, to ensure correct functionality of RavaSync." + Environment.NewLine
                + "If you stop the monitoring to move folders around, resume it after you are finished moving the files."
                + UiSharedService.TooltipSeparator + _uiShared.L("UI.SettingsUi.24a2da65", "Hold CTRL to enable this button"));
        }

        _uiShared.DrawCacheDirectorySetting();
        ImGui.AlignTextToFramePadding();
        if (_cacheMonitor.FileCacheSize >= 0)
            ImGui.TextUnformatted(string.Format(_uiShared.L("UI.SettingsUi.b32b1f34", "Currently utilized local storage: {0}"), UiSharedService.ByteToString(_cacheMonitor.FileCacheSize)));
        else
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.0fbae47f", "Currently utilized local storage: Calculating..."));
        ImGui.TextUnformatted(string.Format(_uiShared.L("UI.SettingsUi.bf683b03", "Remaining space free on drive: {0}"), UiSharedService.ByteToString(_cacheMonitor.FileCacheDriveFree)));
        bool useFileCompactor = _configService.Current.UseCompactor;
        bool isLinux = _dalamudUtilService.IsWine;
        if (!useFileCompactor && !isLinux)
        {
            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.59e24048", "Hint: To free up space when using RavaSync consider enabling the File Compactor"), ImGuiColors.DalamudYellow);
        }
        if (isLinux || !_cacheMonitor.StorageisNTFS) ImGui.BeginDisabled();
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.caca6213", "Use file compactor"), ref useFileCompactor))
        {
            _configService.Current.UseCompactor = useFileCompactor;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.1d4a97fa", "The file compactor can massively reduce your saved files. It might incur a minor penalty on loading files on a slow CPU.") + Environment.NewLine
            + "It is recommended to leave it enabled to save on space.");
        ImGui.SameLine();
        if (!_fileCompactor.MassCompactRunning)
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.FileArchive, _uiShared.L("UI.SettingsUi.2e32b4b3", "Compact all files in storage")))
            {
                _ = Task.Run(() =>
                {
                    _fileCompactor.CompactStorage(compress: true);
                    _cacheMonitor.RecalculateFileCacheSize(CancellationToken.None);
                });
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.fb25d80d", "This will run compression on all files in your current RavaSync Storage.") + Environment.NewLine
                + "You do not need to run this manually if you keep the file compactor enabled.");
            ImGui.SameLine();
            if (_uiShared.IconTextButton(FontAwesomeIcon.File, _uiShared.L("UI.SettingsUi.02956d67", "Decompact all files in storage")))
            {
                _ = Task.Run(() =>
                {
                    _fileCompactor.CompactStorage(compress: false);
                    _cacheMonitor.RecalculateFileCacheSize(CancellationToken.None);
                });
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.b1794a3c", "This will run decompression on all files in your current RavaSync Storage."));
        }
        else
        {
            UiSharedService.ColorText($"File compactor currently running ({_fileCompactor.Progress})", ImGuiColors.DalamudYellow);
        }
        if (isLinux || !_cacheMonitor.StorageisNTFS)
        {
            ImGui.EndDisabled();
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.4d798fc5", "The file compactor is only available on Windows and NTFS drives."));
        }
        ImGuiHelpers.ScaledDummy(new Vector2(10, 10));

        ImGui.Separator();
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.fd6ad6de", "File Storage validation can make sure that all files in your local RavaSync Storage are valid. ") +
            _uiShared.L("UI.SettingsUi.StorageValidation.Warn", "Run the validation before you clear the Storage for no reason.") + Environment.NewLine +
            _uiShared.L("UI.SettingsUi.StorageValidation.Intensive", "This operation, depending on how many files you have in your storage, can take a while and will be CPU and drive intensive."));
        using (ImRaii.Disabled(_validationTask != null && !_validationTask.IsCompleted))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Check, _uiShared.L("UI.SettingsUi.1499a5d0", "Start File Storage Validation")))
            {
                _validationCts?.Cancel();
                _validationCts?.Dispose();
                _validationCts = new();
                var token = _validationCts.Token;
                _validationTask = Task.Run(() => _fileCacheManager.ValidateLocalIntegrity(_validationProgress, token));
            }
        }
        if (_validationTask != null && !_validationTask.IsCompleted)
        {
            ImGui.SameLine();
            if (_uiShared.IconTextButton(FontAwesomeIcon.Times, _uiShared.L("UI.SettingsUi.27402d36", "Cancel")))
            {
                _validationCts?.Cancel();
            }
        }

        if (_validationTask != null)
        {
            using (ImRaii.PushIndent(20f))
            {
                if (_validationTask.IsCompleted)
                {
                    UiSharedService.TextWrapped(string.Format(_uiShared.L("UI.SettingsUi.c838e56a", "The storage validation has completed and removed {0} invalid files from storage."), _validationTask.Result.Count));
                }
                else
                {
                    UiSharedService.TextWrapped(string.Format(_uiShared.L("UI.SettingsUi.3a76f1c7", "Storage validation is running: {0}/{1}"), _currentProgress.Item1, _currentProgress.Item2));

                    var currentItem = _currentProgress.Item3?.ResolvedFilepath;
                    UiSharedService.TextWrapped(string.Format(_uiShared.L("UI.SettingsUi.e67b1f0f", "Current item: {0}"), (!string.IsNullOrEmpty(currentItem) ? currentItem : _uiShared.L("UI.SettingsUi.64e02234", "(starting...)"))));
                }

            }
        }
        ImGui.Separator();

        ImGuiHelpers.ScaledDummy(new Vector2(10, 10));
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.d9bb05dd", "To clear the local storage accept the following disclaimer"));
        ImGui.Indent();
        ImGui.Checkbox("##readClearCache", ref _readClearCache);
        ImGui.SameLine();
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.6847ff7d", "I understand that: ") + Environment.NewLine + _uiShared.L("UI.SettingsUi.fe67c5fb", "- By clearing the local storage I put the file servers of my connected service under extra strain by having to redownload all data.")
            + Environment.NewLine + _uiShared.L("UI.SettingsUi.ClearStorage.Disclaimer.NotFixSync", "- This is not a step to try to fix sync issues.")
            + Environment.NewLine + _uiShared.L("UI.SettingsUi.ClearStorage.Disclaimer.WorseUnderLoad", "- This can make the situation of not getting other players data worse in situations of heavy file server load."));
        if (!_readClearCache)
            ImGui.BeginDisabled();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.79ee78eb", "Clear local storage")) && UiSharedService.CtrlPressed() && _readClearCache)
        {
            _ = Task.Run(() =>
            {
                foreach (var file in Directory.GetFiles(_configService.Current.CacheFolder))
                {
                    File.Delete(file);
                }
            });
        }
        UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.b56e57a1", "You normally do not need to do this. THIS IS NOT SOMETHING YOU SHOULD BE DOING TO TRY TO FIX SYNC ISSUES.") + Environment.NewLine
            + _uiShared.L("UI.SettingsUi.ClearStorage.Tooltip.Redownload", "This will solely remove all downloaded data from all players and will require you to re-download everything again.") + Environment.NewLine
            + _uiShared.L("UI.SettingsUi.ClearStorage.Tooltip.SelfClearing", "Rava's storage is self-clearing and will not surpass the limit you have set it to.") + Environment.NewLine
            + _uiShared.L("UI.SettingsUi.ClearStorage.Tooltip.HoldCtrl", "If you still think you need to do this hold CTRL while pressing the button."));
        if (!_readClearCache)
            ImGui.EndDisabled();
        ImGui.Unindent();
    }

    private void DrawGeneral()
    {
        if (!string.Equals(_lastTab, "General", StringComparison.OrdinalIgnoreCase))
        {
            _notesSuccessfullyApplied = null;
        }

        _lastTab = "General";
        //UiSharedService.FontText(_uiShared.L("UI.SettingsUi.b718f8c3", "Experimental"), _uiShared.UidFont);
        //ImGui.Separator();

        _uiShared.BigText("Notes");
        if (_uiShared.IconTextButton(FontAwesomeIcon.StickyNote, _uiShared.L("UI.SettingsUi.3a0eda9c", "Export all your user notes to clipboard")))
        {
            ImGui.SetClipboardText(UiSharedService.GetNotes(_pairManager.DirectPairs.UnionBy(_pairManager.GroupPairs.SelectMany(p => p.Value), p => p.UserData, UserDataComparer.Instance).ToList()));
        }
        if (_uiShared.IconTextButton(FontAwesomeIcon.FileImport, _uiShared.L("UI.SettingsUi.44a8ccc6", "Import notes from clipboard")))
        {
            _notesSuccessfullyApplied = null;
            var notes = ImGui.GetClipboardText();
            _notesSuccessfullyApplied = _uiShared.ApplyNotesFromClipboard(notes, _overwriteExistingLabels);
        }

        ImGui.SameLine();
        ImGui.Checkbox(_uiShared.L("UI.SettingsUi.244640eb", "Overwrite existing notes"), ref _overwriteExistingLabels);
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.def1de08", "If this option is selected all already existing notes for UIDs will be overwritten by the imported notes."));
        if (_notesSuccessfullyApplied.HasValue && _notesSuccessfullyApplied.Value)
        {
            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.03e00a78", "User Notes successfully imported"), ImGuiColors.HealerGreen);
        }
        else if (_notesSuccessfullyApplied.HasValue && !_notesSuccessfullyApplied.Value)
        {
            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.781d6318", "Attempt to import notes from clipboard failed. Check formatting and try again"), ImGuiColors.DalamudRed);
        }

        var openPopupOnAddition = _configService.Current.OpenPopupOnAdd;

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.ab641fe6", "Open Notes Popup on user addition"), ref openPopupOnAddition))
        {
            _configService.Current.OpenPopupOnAdd = openPopupOnAddition;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.5e8e28ed", "This will open a popup that allows you to set the notes for a user after successfully adding them to your individual pairs."));

        var autoPopulateNotes = _configService.Current.AutoPopulateEmptyNotesFromCharaName;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.1bf1bee2", "Automatically populate notes using player names"), ref autoPopulateNotes))
        {
            _configService.Current.AutoPopulateEmptyNotesFromCharaName = autoPopulateNotes;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.d0f50360", "This will automatically populate user notes using the first encountered player name if the note was not set prior"));

        ImGui.Separator();
        _uiShared.BigText("UI");
        ImGui.Separator();
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.Language", "Language"));
        ImGui.SameLine();
        ImGui.SetNextItemWidth(220 * ImGuiHelpers.GlobalScale);
        if (ImGui.Combo("##ravaLanguage", ref _languageIndex, _languages.Keys.ToArray(), _languages.Count))
        {
            var code = _languages.ElementAt(_languageIndex).Value;
            _configService.Current.LanguageCode = code;
            _configService.Save();
            _uiShared.LoadLocalization(code);
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.Language.Help", "Select the language used for RavaSync's UI. Takes effect immediately."));

        var showNameInsteadOfNotes = _configService.Current.ShowCharacterNameInsteadOfNotesForVisible;
        var showVisibleSeparate = _configService.Current.ShowVisibleUsersSeparately;
        var showOfflineSeparate = _configService.Current.ShowOfflineUsersSeparately;
        var showProfiles = _configService.Current.ProfilesShow;
        var showNsfwProfiles = _configService.Current.ProfilesAllowNsfw;
        var profileDelay = _configService.Current.ProfileDelay;
        var profileOnRight = _configService.Current.ProfilePopoutRight;
        var enableRightClickMenu = _configService.Current.EnableRightClickMenus;
        var enableDtrEntry = _configService.Current.EnableDtrEntry;
        var showUidInDtrTooltip = _configService.Current.ShowUidInDtrTooltip;
        var preferNoteInDtrTooltip = _configService.Current.PreferNoteInDtrTooltip;
        var useColorsInDtr = _configService.Current.UseColorsInDtr;
        var dtrColorsDefault = _configService.Current.DtrColorsDefault;
        var dtrColorsNotConnected = _configService.Current.DtrColorsNotConnected;
        var dtrColorsPairsInRange = _configService.Current.DtrColorsPairsInRange;
        var preferNotesInsteadOfName = _configService.Current.PreferNotesOverNamesForVisible;
        var useFocusTarget = _configService.Current.UseFocusTarget;
        var groupUpSyncshells = _configService.Current.GroupUpSyncshells;
        var groupInVisible = _configService.Current.ShowSyncshellUsersInVisible;
        var syncshellOfflineSeparate = _configService.Current.ShowSyncshellOfflineUsersSeparately;
        var sortPairsByVRAM = _configService.Current.SortPairsByVRAM;


        bool showMinimizedRestoreIcon = _configService.Current.ShowMinimizedRestoreIcon;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.dd33a899", "Show minimised icon"), ref showMinimizedRestoreIcon))
        {
            _configService.Current.ShowMinimizedRestoreIcon = showMinimizedRestoreIcon;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.10a3ebc0", "If disabled, the draggable icon will not appear."));

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.b7c71630", "Enable Game Right Click Menu Entries"), ref enableRightClickMenu))
        {
            _configService.Current.EnableRightClickMenus = enableRightClickMenu;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.b61542a0", "This will add RavaSync related right click menu entries in the game UI on paired players."));

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.88028472", "Display status and visible pair count in Server Info Bar"), ref enableDtrEntry))
        {
            _configService.Current.EnableDtrEntry = enableDtrEntry;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.dc62313c", "This will add RavaSync connection status and visible pair count in the Server Info Bar.\nYou can further configure this through your Dalamud Settings."));

        using (ImRaii.Disabled(!enableDtrEntry))
        {
            using var indent = ImRaii.PushIndent();
            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.ea8b98ef", "Show visible character's UID in tooltip"), ref showUidInDtrTooltip))
            {
                _configService.Current.ShowUidInDtrTooltip = showUidInDtrTooltip;
                _configService.Save();
            }

            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.e58c92af", "Prefer notes over player names in tooltip"), ref preferNoteInDtrTooltip))
            {
                _configService.Current.PreferNoteInDtrTooltip = preferNoteInDtrTooltip;
                _configService.Save();
            }

            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.662846bc", "Color-code the Server Info Bar entry according to status"), ref useColorsInDtr))
            {
                _configService.Current.UseColorsInDtr = useColorsInDtr;
                _configService.Save();
            }

            using (ImRaii.Disabled(!useColorsInDtr))
            {
                using var indent2 = ImRaii.PushIndent();
                if (InputDtrColors(_uiShared.L("UI.SettingsUi.DtrColors.Default", "Default"), ref dtrColorsDefault))
                {
                    _configService.Current.DtrColorsDefault = dtrColorsDefault;
                    _configService.Save();
                }

                ImGui.SameLine();
                if (InputDtrColors(_uiShared.L("UI.SettingsUi.DtrColors.NotConnected", "Not Connected"), ref dtrColorsNotConnected))
                {
                    _configService.Current.DtrColorsNotConnected = dtrColorsNotConnected;
                    _configService.Save();
                }

                ImGui.SameLine();
                if (InputDtrColors(_uiShared.L("UI.SettingsUi.DtrColors.PairsInRange", "Pairs in Range"), ref dtrColorsPairsInRange))
                {
                    _configService.Current.DtrColorsPairsInRange = dtrColorsPairsInRange;
                    _configService.Save();
                }
            }
        }

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.de2f789c", "Show separate Visible group"), ref showVisibleSeparate))
        {
            _configService.Current.ShowVisibleUsersSeparately = showVisibleSeparate;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.49844597", "This will show all currently visible users in a special 'Visible' group in the main UI."));

        using (ImRaii.Disabled(!showVisibleSeparate))
        {
            using var indent = ImRaii.PushIndent();
            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.83abfafe", "Show Syncshell Users in Visible Group"), ref groupInVisible))
            {
                _configService.Current.ShowSyncshellUsersInVisible = groupInVisible;
                _configService.Save();
                Mediator.Publish(new RefreshUiMessage());
            }
        }

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.b0da7a6a", "Show separate Offline group"), ref showOfflineSeparate))
        {
            _configService.Current.ShowOfflineUsersSeparately = showOfflineSeparate;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.9e941a95", "This will show all currently offline users in a special 'Offline' group in the main UI."));

        using (ImRaii.Disabled(!showOfflineSeparate))
        {
            using var indent = ImRaii.PushIndent();
            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.1af5aabe", "Show separate Offline group for Syncshell users"), ref syncshellOfflineSeparate))
            {
                _configService.Current.ShowSyncshellOfflineUsersSeparately = syncshellOfflineSeparate;
                _configService.Save();
                Mediator.Publish(new RefreshUiMessage());
            }
        }

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.5ae90add", "Group up all syncshells in one folder"), ref groupUpSyncshells))
        {
            _configService.Current.GroupUpSyncshells = groupUpSyncshells;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.105db4f0", "This will group up all Syncshells in a special 'All Syncshells' folder in the main UI."));

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.ec0dcd26", "Sort pairs by VRAM (descending)"), ref sortPairsByVRAM))
        {
            _configService.Current.SortPairsByVRAM = sortPairsByVRAM;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.a386318d", "When enabled, the main pair list is ordered by approximate VRAM usage first."));

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.864393b5", "Show player name for visible players"), ref showNameInsteadOfNotes))
        {
            _configService.Current.ShowCharacterNameInsteadOfNotesForVisible = showNameInsteadOfNotes;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.08b5897c", "This will show the character name instead of custom set note when a character is visible"));

        ImGui.Indent();
        if (!_configService.Current.ShowCharacterNameInsteadOfNotesForVisible) ImGui.BeginDisabled();
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.4494cf86", "Prefer notes over player names for visible players"), ref preferNotesInsteadOfName))
        {
            _configService.Current.PreferNotesOverNamesForVisible = preferNotesInsteadOfName;
            _configService.Save();
            Mediator.Publish(new RefreshUiMessage());
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.1b397ef3", "If you set a note for a player it will be shown instead of the player name"));
        if (!_configService.Current.ShowCharacterNameInsteadOfNotesForVisible) ImGui.EndDisabled();
        ImGui.Unindent();

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.e3276aaf", "Set visible pairs as focus targets when clicking the eye"), ref useFocusTarget))
        {
            _configService.Current.UseFocusTarget = useFocusTarget;
            _configService.Save();
        }

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.21fca011", "Show RavaSync Profiles on Hover"), ref showProfiles))
        {
            Mediator.Publish(new ClearProfileDataMessage());
            _configService.Current.ProfilesShow = showProfiles;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.2d970429", "This will show the configured user profile after a set delay"));
        ImGui.Indent();
        if (!showProfiles) ImGui.BeginDisabled();
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.9c4555dc", "Popout profiles on the right"), ref profileOnRight))
        {
            _configService.Current.ProfilePopoutRight = profileOnRight;
            _configService.Save();
            Mediator.Publish(new CompactUiChange(Vector2.Zero, Vector2.Zero));
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.0128b9e7", "Will show profiles on the right side of the main UI"));
        if (ImGui.SliderFloat(_uiShared.L("UI.SettingsUi.3227bc4d", "Hover Delay"), ref profileDelay, 1, 10))
        {
            _configService.Current.ProfileDelay = profileDelay;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.80baef97", "Delay until the profile should be displayed"));
        if (!showProfiles) ImGui.EndDisabled();
        ImGui.Unindent();
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.6d5446f7", "Show profiles marked as NSFW"), ref showNsfwProfiles))
        {
            Mediator.Publish(new ClearProfileDataMessage());
            _configService.Current.ProfilesAllowNsfw = showNsfwProfiles;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.3ab22ded", "Will show profiles that have the NSFW tag enabled"));

        ImGui.Separator();

        var disableOptionalPluginWarnings = _configService.Current.DisableOptionalPluginWarnings;
        var onlineNotifs = _configService.Current.ShowOnlineNotifications;
        var onlineNotifsPairsOnly = _configService.Current.ShowOnlineNotificationsOnlyForIndividualPairs;
        var onlineNotifsNamedOnly = _configService.Current.ShowOnlineNotificationsOnlyForNamedPairs;
        _uiShared.BigText("Notifications");

        string LocNotif(NotificationLocation i) =>_uiShared.L($"UI.SettingsUi.NotificationLocation.{i}", i.ToString());

        _uiShared.DrawCombo(_uiShared.L("UI.SettingsUi.InfoNotif.Title", "Info Notification Display") + "##settingsUi",
            (NotificationLocation[])Enum.GetValues(typeof(NotificationLocation)),
            LocNotif,
            (i) =>
            {
                _configService.Current.InfoNotification = i;
                _configService.Save();
            }, _configService.Current.InfoNotification);

        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.InfoNotif.Help",
            "The location where \"Info\" notifications will display.\n" +
            "'Nowhere' will not show any Info notifications\n" +
            "'Chat' will print Info notifications in chat\n" +
            "'Toast' will show toast notifications in the bottom right corner\n" +
            "'Both' will show chat as well as the toast notification"));

        string LocWarn(NotificationLocation i) =>_uiShared.L($"UI.SettingsUi.NotificationLocation.{i}", i.ToString());

        _uiShared.DrawCombo(_uiShared.L("UI.SettingsUi.WarningNotif.Title", "Warning Notification Display") + "##settingsUi",
            (NotificationLocation[])Enum.GetValues(typeof(NotificationLocation)),
            LocWarn,
            (i) =>
            {
                _configService.Current.WarningNotification = i;
                _configService.Save();
            }, _configService.Current.WarningNotification);

        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.WarningNotif.Help",
            "The location where \"Warning\" notifications will display.\n" +
            "'Nowhere' will not show any Warning notifications\n" +
            "'Chat' will print Warning notifications in chat\n" +
            "'Toast' will show toast notifications in the bottom right corner\n" +
            "'Both' will show chat as well as the toast notification"));

        string LocError(NotificationLocation i) =>_uiShared.L($"UI.SettingsUi.NotificationLocation.{i}", i.ToString());

        _uiShared.DrawCombo(_uiShared.L("UI.SettingsUi.ErrorNotif.Title", "Error Notification Display") + "##settingsUi",
            (NotificationLocation[])Enum.GetValues(typeof(NotificationLocation)),
            LocError,
            (i) =>
            {
                _configService.Current.ErrorNotification = i;
                _configService.Save();
            }, _configService.Current.ErrorNotification);

        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.ErrorNotif.Help",
            "The location where \"Error\" notifications will display.\n" +
            "'Nowhere' will not show any Error notifications\n" +
            "'Chat' will print Error notifications in chat\n" +
            "'Toast' will show toast notifications in the bottom right corner\n" +
            "'Both' will show chat as well as the toast notification"));

        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.5327f64b", "Disable optional plugin warnings"), ref disableOptionalPluginWarnings))
        {
            _configService.Current.DisableOptionalPluginWarnings = disableOptionalPluginWarnings;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.8fa7b383", "Enabling this will not show any \"Warning\" labeled messages for missing optional plugins."));
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.c6bac2f4", "Enable online notifications"), ref onlineNotifs))
        {
            _configService.Current.ShowOnlineNotifications = onlineNotifs;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.ea319c1e", "Enabling this will show a small notification (type: Info) in the bottom right corner when pairs go online."));

        using var disabled = ImRaii.Disabled(!onlineNotifs);
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.bb79b004", "Notify only for individual pairs"), ref onlineNotifsPairsOnly))
        {
            _configService.Current.ShowOnlineNotificationsOnlyForIndividualPairs = onlineNotifsPairsOnly;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.737d9325", "Enabling this will only show online notifications (type: Info) for individual pairs."));
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.89794a67", "Notify only for named pairs"), ref onlineNotifsNamedOnly))
        {
            _configService.Current.ShowOnlineNotificationsOnlyForNamedPairs = onlineNotifsNamedOnly;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.baea8295", "Enabling this will only show online notifications (type: Info) for pairs where you have set an individual note."));
    }

    private void DrawPerformance()
    {
        _uiShared.BigText("Performance Settings");
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.88bb147d", "The configuration options here are to give you more informed warnings and automation when it comes to other performance-intensive synced players."));
        ImGui.Dummy(new Vector2(10));
        ImGui.Separator();
        ImGui.Dummy(new Vector2(10));
        bool showPerformanceIndicator = _playerPerformanceConfigService.Current.ShowPerformanceIndicator;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.32ca13f6", "Show performance indicator"), ref showPerformanceIndicator))
        {
            _playerPerformanceConfigService.Current.ShowPerformanceIndicator = showPerformanceIndicator;
            _playerPerformanceConfigService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.4b4fce66", "Will show a performance indicator when players exceed defined thresholds in Mares UI.") + Environment.NewLine + _uiShared.L("UI.SettingsUi.daaee07d", "Will use warning thresholds."));
        bool warnOnExceedingThresholds = _playerPerformanceConfigService.Current.WarnOnExceedingThresholds;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.d09ce6d2", "Warn on loading in players exceeding performance thresholds"), ref warnOnExceedingThresholds))
        {
            _playerPerformanceConfigService.Current.WarnOnExceedingThresholds = warnOnExceedingThresholds;
            _playerPerformanceConfigService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.71d96641", "RavaSync will print a warning in chat once per session of meeting those people. Will not warn on players with preferred permissions."));
        using (ImRaii.Disabled(!warnOnExceedingThresholds && !showPerformanceIndicator))
        {
            using var indent = ImRaii.PushIndent();
            var warnOnPref = _playerPerformanceConfigService.Current.WarnOnPreferredPermissionsExceedingThresholds;
            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.2be952f7", "Warn/Indicate also on players with preferred permissions"), ref warnOnPref))
            {
                _playerPerformanceConfigService.Current.WarnOnPreferredPermissionsExceedingThresholds = warnOnPref;
                _playerPerformanceConfigService.Save();
            }
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.2f21969d", "RavaSync will also print warnings and show performance indicator for players where you enabled preferred permissions. If warning in general is disabled, this will not produce any warnings."));
        }
        using (ImRaii.Disabled(!showPerformanceIndicator && !warnOnExceedingThresholds))
        {
            var vram = _playerPerformanceConfigService.Current.VRAMSizeWarningThresholdMiB;
            var tris = _playerPerformanceConfigService.Current.TrisWarningThresholdThousands;
            ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputInt(_uiShared.L("UI.SettingsUi.c3faeff3", "Warning VRAM threshold"), ref vram))
            {
                _playerPerformanceConfigService.Current.VRAMSizeWarningThresholdMiB = vram;
                _playerPerformanceConfigService.Save();
            }
            ImGui.SameLine();
            ImGui.Text(_uiShared.L("UI.SettingsUi.5374a9fd", "(MiB)"));
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.f2e9bf3e", "Limit in MiB of approximate VRAM usage to trigger warning or performance indicator on UI.") + UiSharedService.TooltipSeparator
                + "Default: 375 MiB");
            ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputInt(_uiShared.L("UI.SettingsUi.ffa85420", "Warning Triangle threshold"), ref tris))
            {
                _playerPerformanceConfigService.Current.TrisWarningThresholdThousands = tris;
                _playerPerformanceConfigService.Save();
            }
            ImGui.SameLine();
            ImGui.Text(_uiShared.L("UI.SettingsUi.24b3ca4d", "(thousand triangles)"));
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.f5a0323a", "Limit in approximate used triangles from mods to trigger warning or performance indicator on UI.") + UiSharedService.TooltipSeparator
                + "Default: 165 thousand");
        }
        ImGui.Dummy(new Vector2(10));
        bool autoPause = _playerPerformanceConfigService.Current.AutoPausePlayersExceedingThresholds;
        bool autoPauseEveryone = _playerPerformanceConfigService.Current.AutoPausePlayersWithPreferredPermissionsExceedingThresholds;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.cc56184b", "Automatically pause players exceeding thresholds"), ref autoPause))
        {
            _playerPerformanceConfigService.Current.AutoPausePlayersExceedingThresholds = autoPause;
            _playerPerformanceConfigService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.3f41de24", "When enabled, it will automatically pause all players without preferred permissions that exceed the thresholds defined below.") + Environment.NewLine
            + "Will print a warning in chat when a player got paused automatically."
            + UiSharedService.TooltipSeparator + _uiShared.L("UI.SettingsUi.a077ec42", "Warning: this will not automatically unpause those people again, you will have to do this manually."));
        using (ImRaii.Disabled(!autoPause))
        {
            using var indent = ImRaii.PushIndent();
            if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.a3dc739a", "Automatically pause also players with preferred permissions"), ref autoPauseEveryone))
            {
                _playerPerformanceConfigService.Current.AutoPausePlayersWithPreferredPermissionsExceedingThresholds = autoPauseEveryone;
                _playerPerformanceConfigService.Save();
            }
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.b8212ca2", "When enabled, will automatically pause all players regardless of preferred permissions that exceed thresholds defined below.") + UiSharedService.TooltipSeparator +
                "Warning: this will not automatically unpause those people again, you will have to do this manually.");
            var vramAuto = _playerPerformanceConfigService.Current.VRAMSizeAutoPauseThresholdMiB;
            var trisAuto = _playerPerformanceConfigService.Current.TrisAutoPauseThresholdThousands;
            ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputInt(_uiShared.L("UI.SettingsUi.c377d390", "Auto Pause VRAM threshold"), ref vramAuto))
            {
                _playerPerformanceConfigService.Current.VRAMSizeAutoPauseThresholdMiB = vramAuto;
                _playerPerformanceConfigService.Save();
            }
            ImGui.SameLine();
            ImGui.Text(_uiShared.L("UI.SettingsUi.5374a9fd", "(MiB)"));
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.3cd70fff", "When a loading in player and their VRAM usage exceeds this amount, automatically pauses the synced player.") + UiSharedService.TooltipSeparator
                + "Default: 550 MiB");
            ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputInt(_uiShared.L("UI.SettingsUi.33d59b53", "Auto Pause Triangle threshold"), ref trisAuto))
            {
                _playerPerformanceConfigService.Current.TrisAutoPauseThresholdThousands = trisAuto;
                _playerPerformanceConfigService.Save();
            }
            ImGui.SameLine();
            ImGui.Text(_uiShared.L("UI.SettingsUi.24b3ca4d", "(thousand triangles)"));
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.92cd7b62", "When a loading in player and their triangle count exceeds this amount, automatically pauses the synced player.") + UiSharedService.TooltipSeparator
                + "Default: 250 thousand");

            // --- Syncshell pooled VRAM cap (MiB; 0 = disabled) ---
            var syncshellCapMiB = _playerPerformanceConfigService.Current.SyncshellVramCapMiB;
            ImGui.SetNextItemWidth(100 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputInt(_uiShared.L("UI.SettingsUi.44ec7f3a", "Syncshell VRAM cap"), ref syncshellCapMiB))
            {
                if (syncshellCapMiB < 0) syncshellCapMiB = 0; // 0 disables
                _playerPerformanceConfigService.Current.SyncshellVramCapMiB = syncshellCapMiB;
                _playerPerformanceConfigService.Save();
            }
            ImGui.SameLine();
            ImGui.Text(_uiShared.L("UI.SettingsUi.5608ba29", "(MiB) 1024 = 1 GiB"));
            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.bd16431e", "Total pooled VRAM limit for all non-direct Syncshell users. ")
                + "Set to 0 to disable (default). Direct pairs are always exempt.");


        }
        ImGui.Dummy(new Vector2(10));
        _uiShared.BigText("Whitelisted UIDs");
        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.59605516", "The entries in the list below will be ignored for all warnings and auto pause operations."));
        ImGui.Dummy(new Vector2(10));
        ImGui.SetNextItemWidth(200 * ImGuiHelpers.GlobalScale);
        ImGui.InputText("##ignoreuid", ref _uidToAddForIgnore, 20);
        ImGui.SameLine();
        using (ImRaii.Disabled(string.IsNullOrEmpty(_uidToAddForIgnore)))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Plus, _uiShared.L("UI.SettingsUi.2f596b09", "Add UID/Vanity ID to whitelist")))
            {
                if (!_playerPerformanceConfigService.Current.UIDsToIgnore.Contains(_uidToAddForIgnore, StringComparer.Ordinal))
                {
                    _playerPerformanceConfigService.Current.UIDsToIgnore.Add(_uidToAddForIgnore);
                    _playerPerformanceConfigService.Save();
                }
                _uidToAddForIgnore = string.Empty;
            }
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.24b33430", "Hint: UIDs are case sensitive."));
        var playerList = _playerPerformanceConfigService.Current.UIDsToIgnore;
        ImGui.SetNextItemWidth(200 * ImGuiHelpers.GlobalScale);
        using (var lb = ImRaii.ListBox(string.Concat(_uiShared.L("UI.SettingsUi.888e0bd1", "UID whitelist"), "##888e0bd1")))
        {
            if (lb)
            {
                for (int i = 0; i < playerList.Count; i++)
                {
                    bool shouldBeSelected = _selectedEntry == i;
                    if (ImGui.Selectable(playerList[i] + "##" + i, shouldBeSelected))
                    {
                        _selectedEntry = i;
                    }
                }
            }
        }
        using (ImRaii.Disabled(_selectedEntry == -1))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.4c6b1172", "Delete selected UID")))
            {
                _playerPerformanceConfigService.Current.UIDsToIgnore.RemoveAt(_selectedEntry);
                _selectedEntry = -1;
                _playerPerformanceConfigService.Save();
            }
        }
    }

    private void DrawServerConfiguration()
    {
        _lastTab = "Service Settings";
        if (ApiController.ServerAlive)
        {
            _uiShared.BigText(_uiShared.L("UI.SettingsUi.ServiceActions.Title", "Service Actions"));
            ImGuiHelpers.ScaledDummy(new Vector2(5, 5));
            if (ImGui.Button(_uiShared.L("UI.SettingsUi.2bc27379", "Delete all my files")))
            {
                _deleteFilesPopupModalShown = true;
                ImGui.OpenPopup(_uiShared.L("UI.SettingsUi.3b9c4233", "Delete all your files?"));
            }

            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.1cfd05d5", "Completely deletes all your uploaded files on the service."));

            if (ImGui.BeginPopupModal(_uiShared.L("UI.SettingsUi.3b9c4233", "Delete all your files?"), ref _deleteFilesPopupModalShown, UiSharedService.PopupWindowFlags))
            {
                UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.62BA1D18", "All your own uploaded files on the service will be deleted.\nThis operation cannot be undone."));
                ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.8de6e73b", "Are you sure you want to continue?"));
                ImGui.Separator();
                ImGui.Spacing();

                var buttonSize = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X -
                                 ImGui.GetStyle().ItemSpacing.X) / 2;

                if (ImGui.Button(_uiShared.L("UI.SettingsUi.554393a2", "Delete everything"), new Vector2(buttonSize, 0)))
                {
                    _ = Task.Run(_fileTransferManager.DeleteAllFiles);
                    _deleteFilesPopupModalShown = false;
                }

                ImGui.SameLine();

                if (ImGui.Button($"{_uiShared.L("UI.VenueRegistrationUi.77dfd213", "Cancel")}##cancelDelete", new Vector2(buttonSize, 0)))
                {
                    _deleteFilesPopupModalShown = false;
                }

                UiSharedService.SetScaledWindowSize(325);
                ImGui.EndPopup();
            }
            ImGui.SameLine();
            if (ImGui.Button(_uiShared.L("UI.SettingsUi.1753c206", "Delete account")))
            {
                _deleteAccountPopupModalShown = true;
                ImGui.OpenPopup(_uiShared.L("UI.SettingsUi.35293dc4", "Delete your account?"));
            }

            _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.5a8bd401", "Completely deletes your account and all uploaded files to the service."));

            if (ImGui.BeginPopupModal(_uiShared.L("UI.SettingsUi.35293dc4", "Delete your account?"), ref _deleteAccountPopupModalShown, UiSharedService.PopupWindowFlags))
            {
                UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.872bf5ca", "Your account and all associated files and data on the service will be deleted."));
                UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.acf9dbf9", "Your UID will be removed from all pairing lists."));
                ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.8de6e73b", "Are you sure you want to continue?"));
                ImGui.Separator();
                ImGui.Spacing();

                var buttonSize = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X -
                                  ImGui.GetStyle().ItemSpacing.X) / 2;

                if (ImGui.Button(_uiShared.L("UI.SettingsUi.1753c206", "Delete account"), new Vector2(buttonSize, 0)))
                {
                    _ = Task.Run(ApiController.UserDelete);
                    _deleteAccountPopupModalShown = false;
                    Mediator.Publish(new SwitchToIntroUiMessage());
                }

                ImGui.SameLine();

                if (ImGui.Button($"{_uiShared.L("UI.VenueRegistrationUi.77dfd213", "Cancel")}##cancelDelete", new Vector2(buttonSize, 0)))
                {
                    _deleteAccountPopupModalShown = false;
                }

                UiSharedService.SetScaledWindowSize(325);
                ImGui.EndPopup();
            }
            ImGui.Separator();
        }

        _uiShared.BigText(_uiShared.L("UI.SettingsUi.ServiceCharacterSettings.Title", "Service & Character Settings"));
        ImGuiHelpers.ScaledDummy(new Vector2(5, 5));
        var sendCensus = _serverConfigurationManager.SendCensusData;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.47b68200", "Send Statistical Census Data"), ref sendCensus))
        {
            _serverConfigurationManager.SendCensusData = sendCensus;
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.39ba6f20", "This will allow sending census data to the currently connected service.") + UiSharedService.TooltipSeparator
            + "Census data contains:" + Environment.NewLine
            + "- Current World" + Environment.NewLine
            + "- Current Gender" + Environment.NewLine
            + "- Current Race" + Environment.NewLine
            + _uiShared.L("UI.SettingsUi.aed90f23", "- Current Clan (this is not your Free Company, this is e.g. Keeper or Seeker for Miqo'te)") + UiSharedService.TooltipSeparator
            + _uiShared.L("UI.SettingsUi.803d5a31", "The census data is only saved temporarily and will be removed from the server on disconnect. It is stored temporarily associated with your UID while you are connected.") + UiSharedService.TooltipSeparator
            + "If you do not wish to participate in the statistical census, untick this box and reconnect to the server.");
        ImGuiHelpers.ScaledDummy(new Vector2(10, 10));

        var idx = _uiShared.DrawServiceSelection();
        if (_lastSelectedServerIndex != idx)
        {
            _uiShared.ResetOAuthTasksState();
            _secretKeysConversionCts = _secretKeysConversionCts.CancelRecreate();
            _secretKeysConversionTask = null;
            _lastSelectedServerIndex = idx;
        }

        ImGuiHelpers.ScaledDummy(new Vector2(10, 10));

        var selectedServer = _serverConfigurationManager.GetServerByIndex(idx);
        if (selectedServer == _serverConfigurationManager.CurrentServer)
        {
            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.43ae3183", "For any changes to be applied to the current service you need to reconnect to the service."), ImGuiColors.DalamudYellow);
        }

        bool useOauth = selectedServer.UseOAuth2;

        if (ImGui.BeginTabBar(_uiShared.L("UI.SettingsUi.7a5b95fb", "serverTabBar")))
        {
            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.360ae43b", "Character Management")))
            {
                if (selectedServer.SecretKeys.Any() || useOauth)
                {
                    UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.f4a771d5", "Characters listed here will automatically connect to the selected RavaSync Service with the settings as provided below.") +
                        " " + _uiShared.L("UI.SettingsUi.CharacterManagement.Help.AddCurrent", "Make sure to enter the character names correctly or use the 'Add current character' button at the bottom."), ImGuiColors.DalamudYellow);
                    int i = 0;
                    _uiShared.DrawUpdateOAuthUIDsButton(selectedServer);

                    if (selectedServer.UseOAuth2 && !string.IsNullOrEmpty(selectedServer.OAuthToken))
                    {
                        bool hasSetSecretKeysButNoUid = selectedServer.Authentications.Exists(u => u.SecretKeyIdx != -1 && string.IsNullOrEmpty(u.UID));
                        if (hasSetSecretKeysButNoUid)
                        {
                            ImGui.Dummy(new(5f, 5f));
                            UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.9dde6de2", "Some entries have been detected that have previously been assigned secret keys but not UIDs. ") +
                                "Press this button below to attempt to convert those entries.");
                            using (ImRaii.Disabled(_secretKeysConversionTask != null && !_secretKeysConversionTask.IsCompleted))
                            {
                                if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowsLeftRight, _uiShared.L("UI.SettingsUi.67f77283", "Try to Convert Secret Keys to UIDs")))
                                {
                                    _secretKeysConversionTask = ConvertSecretKeysToUIDs(selectedServer, _secretKeysConversionCts.Token);
                                }
                            }
                            if (_secretKeysConversionTask != null && !_secretKeysConversionTask.IsCompleted)
                            {
                                UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.c35ae74e", "Converting Secret Keys to UIDs"), ImGuiColors.DalamudYellow);
                            }
                            if (_secretKeysConversionTask != null && _secretKeysConversionTask.IsCompletedSuccessfully)
                            {
                                Vector4? textColor = null;
                                if (_secretKeysConversionTask.Result.PartialSuccess)
                                {
                                    textColor = ImGuiColors.DalamudYellow;
                                }
                                if (!_secretKeysConversionTask.Result.Success)
                                {
                                    textColor = ImGuiColors.DalamudRed;
                                }
                                string text = $"Conversion has completed: {_secretKeysConversionTask.Result.Result}";
                                if (textColor == null)
                                {
                                    UiSharedService.TextWrapped(text);
                                }
                                else
                                {
                                    UiSharedService.ColorTextWrapped(text, textColor!.Value);
                                }
                                if (!_secretKeysConversionTask.Result.Success || _secretKeysConversionTask.Result.PartialSuccess)
                                {
                                    UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.26abbb5a", "In case of conversion failures, please set the UIDs for the failed conversions manually."));
                                }
                            }
                        }
                    }
                    ImGui.Separator();

                    // === Account Vanity (Alias) ===
                    _uiShared.BigText(_uiShared.L("UI.SettingsUi.AccountVanity.Title", "Set Account Vanity"), ImGuiColors.ParsedGreen);
                    UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.2587f7ae", "Set a friendly ID that others can use instead of your UID."));

                    ImGui.InputTextWithHint("##alias_input", _uiShared.L("UI.SettingsUi.ee2a2426", "new-vanity (5–15 chars: A–Z, 0–9, _ or -)"), ref _newUserAlias, 32);
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.38aae622", "Allowed: letters, numbers, underscore and hyphen, 5–15 characters."));

                    // Action button
                    using (ImRaii.Disabled(string.IsNullOrWhiteSpace(_newUserAlias)))
                    {
                        if (_uiShared.IconTextButton(FontAwesomeIcon.Tag, _uiShared.L("UI.SettingsUi.3911187a", "Set Vanity")))
                        {
                            _vanityStatusMessage = "Submitting…";
                            _vanityStatusColor = ImGuiColors.DalamudYellow;

                            // Fire and forget to avoid blocking the UI thread
                            _ = Task.Run(async () =>
                            {
                                try
                                {
                                    // This calls the new Hub method added on the server.
                                    var err = await _apiController.UserSetAlias(_newUserAlias).ConfigureAwait(false);

                                    if (string.IsNullOrEmpty(err))
                                    {
                                        _vanityStatusMessage = "Vanity updated.";
                                        _vanityStatusColor = ImGuiColors.ParsedGreen;

                                        // Clear the input so it’s obvious it went through
                                        _newUserAlias = string.Empty;
                                        //Reconnect to apply
                                        await _apiController.CreateConnectionsAsync().ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        _vanityStatusMessage = err;
                                        _vanityStatusColor = ImGuiColors.DalamudRed;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _vanityStatusMessage = "Failed to set Vanity: " + ex.Message;
                                    _vanityStatusColor = ImGuiColors.DalamudRed;
                                }
                            });
                        }
                    }

                    // Status line (inline feedback)
                    if (!string.IsNullOrEmpty(_vanityStatusMessage))
                    {
                        UiSharedService.ColorTextWrapped(_vanityStatusMessage, _vanityStatusColor);
                    }

                    ImGuiHelpers.ScaledDummy(5);
                    ImGui.Separator();
                    ImGuiHelpers.ScaledDummy(5);
                    // === End Account Vanity ===

                    string youName = _dalamudUtilService.GetPlayerName();
                    uint youWorld = _dalamudUtilService.GetHomeWorldId();
                    ulong youCid = _dalamudUtilService.GetCID();
                    if (!selectedServer.Authentications.Exists(a => string.Equals(a.CharacterName, youName, StringComparison.Ordinal) && a.WorldId == youWorld))
                    {
                        _uiShared.BigText(_uiShared.L("UI.SettingsUi.YourCharacterNotConfigured.Title", "Your Character is not Configured"), ImGuiColors.DalamudRed);
                        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.ee62041a", "You have currently no character configured that corresponds to your current name and world."), ImGuiColors.DalamudRed);
                        var authWithCid = selectedServer.Authentications.Find(f => f.LastSeenCID == youCid);
                        if (authWithCid != null)
                        {
                            ImGuiHelpers.ScaledDummy(5);
                            UiSharedService.ColorText(_uiShared.L("UI.SettingsUi.4124012e", "A potential rename/world change from this character was detected:"), ImGuiColors.DalamudYellow);
                            using (ImRaii.PushIndent(10f))
                                UiSharedService.ColorText(_uiShared.L("UI.SettingsUi.32b12c6d", "Entry: ") + authWithCid.CharacterName + " - " + _dalamudUtilService.WorldData.Value[(ushort)authWithCid.WorldId], ImGuiColors.ParsedGreen);
                            UiSharedService.ColorText(_uiShared.L("UI.SettingsUi.78d68368", "Press the button below to adjust that entry to your current character:"), ImGuiColors.DalamudYellow);
                            using (ImRaii.PushIndent(10f))
                                UiSharedService.ColorText(_uiShared.L("UI.SettingsUi.920a9360", "Current: ") + youName + " - " + _dalamudUtilService.WorldData.Value[(ushort)youWorld], ImGuiColors.ParsedGreen);
                            ImGuiHelpers.ScaledDummy(5);
                            if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowRight, _uiShared.L("UI.SettingsUi.90ddd7cc", "Update Entry to Current Character")))
                            {
                                authWithCid.CharacterName = youName;
                                authWithCid.WorldId = youWorld;
                                _serverConfigurationManager.Save();
                            }
                        }
                        ImGuiHelpers.ScaledDummy(5);
                        ImGui.Separator();
                        ImGuiHelpers.ScaledDummy(5);
                    }
                    foreach (var item in selectedServer.Authentications.ToList())
                    {
                        using var charaId = ImRaii.PushId("selectedChara" + i);

                        var worldIdx = (ushort)item.WorldId;
                        var data = _uiShared.WorldData.OrderBy(u => u.Value, StringComparer.Ordinal).ToDictionary(k => k.Key, k => k.Value);
                        if (!data.TryGetValue(worldIdx, out string? worldPreview))
                        {
                            worldPreview = data.First().Value;
                        }

                        Dictionary<int, SecretKey> keys = [];

                        if (!useOauth)
                        {
                            var secretKeyIdx = item.SecretKeyIdx;
                            keys = selectedServer.SecretKeys;
                            if (!keys.TryGetValue(secretKeyIdx, out var secretKey))
                            {
                                secretKey = new();
                            }
                        }

                        bool thisIsYou = false;
                        if (string.Equals(youName, item.CharacterName, StringComparison.OrdinalIgnoreCase)
                            && youWorld == worldIdx)
                        {
                            thisIsYou = true;
                        }
                        bool misManaged = false;
                        if (selectedServer.UseOAuth2 && !string.IsNullOrEmpty(selectedServer.OAuthToken) && string.IsNullOrEmpty(item.UID))
                        {
                            misManaged = true;
                        }
                        if (!selectedServer.UseOAuth2 && item.SecretKeyIdx == -1)
                        {
                            misManaged = true;
                        }
                        Vector4 color = ImGuiColors.ParsedGreen;
                        string text = thisIsYou ? _uiShared.L("UI.SettingsUi.CurrentCharacter.Title", "Your Current Character") : string.Empty;
                        if (misManaged)
                        {
                            text += " [" + _uiShared.L("UI.SettingsUi.Label.Mismanaged", "MISMANAGED") + " (" + (selectedServer.UseOAuth2 ? _uiShared.L("UI.SettingsUi.Label.NoUidSet", "No UID Set") : _uiShared.L("UI.SettingsUi.Label.NoSecretKeySet", "No Secret Key Set")) + ")]";
                            color = ImGuiColors.DalamudRed;
                        }
                        if (selectedServer.Authentications.Where(e => e != item).Any(e => string.Equals(e.CharacterName, item.CharacterName, StringComparison.Ordinal)
                            && e.WorldId == item.WorldId))
                        {
                            text += " [" + _uiShared.L("UI.SettingsUi.Label.Duplicate", "DUPLICATE") + "]";
                            color = ImGuiColors.DalamudRed;
                        }

                        if (!string.IsNullOrEmpty(text))
                        {
                            text = text.Trim();
                            _uiShared.BigText(text, color);
                        }

                        var charaName = item.CharacterName;
                        if (ImGui.InputText(_uiShared.L("UI.SettingsUi.d11da964", "Character Name"), ref charaName, 64))
                        {
                            item.CharacterName = charaName;
                            _serverConfigurationManager.Save();
                        }

                        _uiShared.DrawCombo("World##" + item.CharacterName + i, data, (w) => w.Value,
                            (w) =>
                            {
                                if (item.WorldId != w.Key)
                                {
                                    item.WorldId = w.Key;
                                    _serverConfigurationManager.Save();
                                }
                            }, EqualityComparer<KeyValuePair<ushort, string>>.Default.Equals(data.FirstOrDefault(f => f.Key == worldIdx), default) ? data.First() : data.First(f => f.Key == worldIdx));

                        if (!useOauth)
                        {
                            _uiShared.DrawCombo("Secret Key###" + item.CharacterName + i, keys, (w) => w.Value.FriendlyName,
                                (w) =>
                                {
                                    if (w.Key != item.SecretKeyIdx)
                                    {
                                        item.SecretKeyIdx = w.Key;
                                        _serverConfigurationManager.Save();
                                        // immediately reconnect so the new secret is active in this session
                                        _ = Task.Run(() => _apiController.CreateConnectionsAsync());
                                    }
                                }, EqualityComparer<KeyValuePair<int, SecretKey>>.Default.Equals(keys.FirstOrDefault(f => f.Key == item.SecretKeyIdx), default) ? keys.First() : keys.First(f => f.Key == item.SecretKeyIdx));
                        }
                        else
                        {
                            _uiShared.DrawUIDComboForAuthentication(i, item, selectedServer.ServerUri, _logger);
                        }
                        bool isAutoLogin = item.AutoLogin;
                        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.861c5ccc", "Automatically login to RavaSync"), ref isAutoLogin))
                        {
                            item.AutoLogin = isAutoLogin;
                            _serverConfigurationManager.Save();
                        }
                        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.ca151f98", "When enabled and logging into this character in XIV, RavaSync will automatically connect to the current service."));
                        if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.815a2475", "Delete Character")) && UiSharedService.CtrlPressed())
                            _serverConfigurationManager.RemoveCharacterFromServer(idx, item);
                        UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.d7921dac", "Hold CTRL to delete this entry."));

                        i++;
                        if (item != selectedServer.Authentications.ToList()[^1])
                        {
                            ImGuiHelpers.ScaledDummy(5);
                            ImGui.Separator();
                            ImGuiHelpers.ScaledDummy(5);
                        }
                    }

                    if (selectedServer.Authentications.Any())
                        ImGui.Separator();

                    if (!selectedServer.Authentications.Exists(c => string.Equals(c.CharacterName, youName, StringComparison.Ordinal)
                        && c.WorldId == youWorld))
                    {
                        if (_uiShared.IconTextButton(FontAwesomeIcon.User, _uiShared.L("UI.SettingsUi.0bf53ccf", "Add current character")))
                        {
                            _serverConfigurationManager.AddCurrentCharacterToServer(idx);
                        }
                        ImGui.SameLine();
                    }

                    if (_uiShared.IconTextButton(FontAwesomeIcon.Plus, _uiShared.L("UI.SettingsUi.ed3d0560", "Add new character")))
                    {
                        _serverConfigurationManager.AddEmptyCharacterToServer(idx);
                    }
                }
                else
                {
                    UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.e6eedca2", "You need to add a Secret Key first before adding Characters."), ImGuiColors.DalamudYellow);
                }

                ImGui.EndTabItem();
            }

            if (!useOauth && ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.61948fd8", "Secret Key Management")))
            {
                foreach (var item in selectedServer.SecretKeys.ToList())
                {
                    using var id = ImRaii.PushId("key" + item.Key);
                    var friendlyName = item.Value.FriendlyName;
                    if (ImGui.InputText(_uiShared.L("UI.SettingsUi.2fe1166f", "Secret Key Display Name"), ref friendlyName, 255))
                    {
                        item.Value.FriendlyName = friendlyName;
                        _serverConfigurationManager.Save();
                    }
                    var key = item.Value.Key;
                    if (ImGui.InputText(_uiShared.L("UI.SettingsUi.2969a186", "Secret Key"), ref key, 64))
                    {
                        item.Value.Key = key;
                        _serverConfigurationManager.Save();
                    }
                    if (!selectedServer.Authentications.Exists(p => p.SecretKeyIdx == item.Key))
                    {
                        if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.df7def09", "Delete Secret Key")) && UiSharedService.CtrlPressed())
                        {
                            selectedServer.SecretKeys.Remove(item.Key);
                            _serverConfigurationManager.Save();
                        }
                        UiSharedService.AttachToolTip(_uiShared.L("UI.SettingsUi.332c39a5", "Hold CTRL to delete this secret key entry"));
                    }
                    else
                    {
                        UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.e60ed27e", "This key is in use and cannot be deleted"), ImGuiColors.DalamudYellow);
                    }

                    if (item.Key != selectedServer.SecretKeys.Keys.LastOrDefault())
                        ImGui.Separator();
                }

                ImGui.Separator();
                if (_uiShared.IconTextButton(FontAwesomeIcon.Plus, _uiShared.L("UI.SettingsUi.c2cb3b4d", "Add new Secret Key")))
                {
                    selectedServer.SecretKeys.Add(selectedServer.SecretKeys.Any() ? selectedServer.SecretKeys.Max(p => p.Key) + 1 : 0, new SecretKey()
                    {
                        FriendlyName = "New Secret Key",
                    });
                    _serverConfigurationManager.Save();
                }

                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.ac91855f", "Service Configuration")))
            {
                var serverName = selectedServer.ServerName;
                var serverUri = selectedServer.ServerUri;
                var isMain = string.Equals(serverName, ApiController.MainServer, StringComparison.OrdinalIgnoreCase);
                var flags = isMain ? ImGuiInputTextFlags.ReadOnly : ImGuiInputTextFlags.None;

                if (ImGui.InputText(_uiShared.L("UI.SettingsUi.556057bb", "Service URI"), ref serverUri, 255, flags))
                {
                    selectedServer.ServerUri = serverUri;
                }
                if (isMain)
                {
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.7585599b", "You cannot edit the URI of the main service."));
                }

                if (ImGui.InputText(_uiShared.L("UI.SettingsUi.5a6be7a0", "Service Name"), ref serverName, 255, flags))
                {
                    selectedServer.ServerName = serverName;
                    _serverConfigurationManager.Save();
                }
                if (isMain)
                {
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.cb0672d5", "You cannot edit the name of the main service."));
                }

                ImGui.SetNextItemWidth(200);
                var serverTransport = _serverConfigurationManager.GetTransport();
                _uiShared.DrawCombo("Server Transport Type", Enum.GetValues<HttpTransportType>().Where(t => t != HttpTransportType.None),
                    (v) => v.ToString(),
                    onSelected: (t) => _serverConfigurationManager.SetTransportType(t),
                    serverTransport);
                _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.dfd574c6", "You normally do not need to change this, if you don't know what this is or what it's for, keep it to WebSockets.") + Environment.NewLine
                    + _uiShared.L("UI.SettingsUi.f6c5c553", "If you run into connection issues with e.g. VPNs, try ServerSentEvents first before trying out LongPolling.") + UiSharedService.TooltipSeparator
                    + "Note: if the server does not support a specific Transport Type it will fall through to the next automatically: WebSockets > ServerSentEvents > LongPolling");

                if (_dalamudUtilService.IsWine)
                {
                    bool forceWebSockets = selectedServer.ForceWebSockets;
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.9fc81ae2", "[wine only] Force WebSockets"), ref forceWebSockets))
                    {
                        selectedServer.ForceWebSockets = forceWebSockets;
                        _serverConfigurationManager.Save();
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.50f8e5ad", "On wine, RavaSync will automatically fall back to ServerSentEvents/LongPolling, even if WebSockets is selected. ")
                        + "WebSockets are known to crash XIV entirely on wine 8.5 shipped with Dalamud. "
                        + "Only enable this if you are not running wine 8.5." + Environment.NewLine
                        + "Note: If the issue gets resolved at some point this option will be removed.");
                }

                ImGuiHelpers.ScaledDummy(5);

                if (!isMain && selectedServer != _serverConfigurationManager.CurrentServer)
                {
                    ImGui.Separator();
                    if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.dbb0a6a0", "Delete Service")) && UiSharedService.CtrlPressed())
                    {
                        _serverConfigurationManager.DeleteServer(selectedServer);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.492d2cb3", "Hold CTRL to delete this service"));
                }

                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.2405c5d6", "Permission Settings")))
            {
                _uiShared.BigText("Default Permission Settings");
                if (selectedServer == _serverConfigurationManager.CurrentServer && _apiController.IsConnected)
                {
                    UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.c8576184", "Note: The default permissions settings here are not applied retroactively to existing pairs or joined Syncshells."));
                    UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.f5fcf42c", "Note: The default permissions settings here are sent and stored on the connected service."));
                    ImGuiHelpers.ScaledDummy(5f);
                    var perms = _apiController.DefaultPermissions!;
                    bool individualIsSticky = perms.IndividualIsSticky;
                    bool disableIndividualSounds = perms.DisableIndividualSounds;
                    bool disableIndividualAnimations = perms.DisableIndividualAnimations;
                    bool disableIndividualVFX = perms.DisableIndividualVFX;
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.27d1eadc", "Individually set permissions become preferred permissions"), ref individualIsSticky))
                    {
                        perms.IndividualIsSticky = individualIsSticky;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.22458c5b", "The preferred attribute means that the permissions to that user will never change through any of your permission changes to Syncshells ") +
                        "(i.e. if you have paused one specific user in a Syncshell and they become preferred permissions, then pause and unpause the same Syncshell, the user will remain paused - " +
                        "if a user does not have preferred permissions, it will follow the permissions of the Syncshell and be unpaused)." + Environment.NewLine + Environment.NewLine +
                        "This setting means:" + Environment.NewLine +
                        "  - All new individual pairs get their permissions defaulted to preferred permissions." + Environment.NewLine +
                        "  - All individually set permissions for any pair will also automatically become preferred permissions. This includes pairs in Syncshells." + Environment.NewLine + Environment.NewLine +
                        "It is possible to remove or set the preferred permission state for any pair at any time." + Environment.NewLine + Environment.NewLine +
                        "If unsure, leave this setting off.");
                    ImGuiHelpers.ScaledDummy(3f);

                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.224fb16a", "Disable individual pair sounds"), ref disableIndividualSounds))
                    {
                        perms.DisableIndividualSounds = disableIndividualSounds;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.fc19e364", "This setting will disable sound sync for all new individual pairs."));
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.c9ed862a", "Disable individual pair animations"), ref disableIndividualAnimations))
                    {
                        perms.DisableIndividualAnimations = disableIndividualAnimations;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.6f805c7a", "This setting will disable animation sync for all new individual pairs."));
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.97ae0490", "Disable individual pair VFX"), ref disableIndividualVFX))
                    {
                        perms.DisableIndividualVFX = disableIndividualVFX;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.f6bd3308", "This setting will disable VFX sync for all new individual pairs."));
                    ImGuiHelpers.ScaledDummy(5f);
                    bool disableGroundSounds = perms.DisableGroupSounds;
                    bool disableGroupAnimations = perms.DisableGroupAnimations;
                    bool disableGroupVFX = perms.DisableGroupVFX;
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.d2b89e63", "Disable Syncshell pair sounds"), ref disableGroundSounds))
                    {
                        perms.DisableGroupSounds = disableGroundSounds;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.9a3bb894", "This setting will disable sound sync for all non-sticky pairs in newly joined syncshells."));
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.e91bb6e0", "Disable Syncshell pair animations"), ref disableGroupAnimations))
                    {
                        perms.DisableGroupAnimations = disableGroupAnimations;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.011bdd69", "This setting will disable animation sync for all non-sticky pairs in newly joined syncshells."));
                    if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.ab693f17", "Disable Syncshell pair VFX"), ref disableGroupVFX))
                    {
                        perms.DisableGroupVFX = disableGroupVFX;
                        _ = _apiController.UserUpdateDefaultPermissions(perms);
                    }
                    _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.66f7929d", "This setting will disable VFX sync for all non-sticky pairs in newly joined syncshells."));
                }
                else
                {
                    UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.9eea36f7", "Default Permission Settings unavailable for this service. ") +
                        "You need to connect to this service to change the default permissions since they are stored on the service.", ImGuiColors.DalamudYellow);
                }

                ImGui.EndTabItem();
            }
            ImGui.EndTabBar();
        }
    }

    private int _lastSelectedServerIndex = -1;
    private Task<(bool Success, bool PartialSuccess, string Result)>? _secretKeysConversionTask = null;
    private CancellationTokenSource _secretKeysConversionCts = new CancellationTokenSource();

    private async Task<(bool Success, bool partialSuccess, string Result)> ConvertSecretKeysToUIDs(ServerStorage serverStorage, CancellationToken token)
    {
        List<Authentication> failedConversions = serverStorage.Authentications.Where(u => u.SecretKeyIdx == -1 && string.IsNullOrEmpty(u.UID)).ToList();
        List<Authentication> conversionsToAttempt = serverStorage.Authentications.Where(u => u.SecretKeyIdx != -1 && string.IsNullOrEmpty(u.UID)).ToList();
        List<Authentication> successfulConversions = [];
        Dictionary<string, List<Authentication>> secretKeyMapping = new(StringComparer.Ordinal);
        foreach (var authEntry in conversionsToAttempt)
        {
            if (!serverStorage.SecretKeys.TryGetValue(authEntry.SecretKeyIdx, out var secretKey))
            {
                failedConversions.Add(authEntry);
                continue;
            }

            if (!secretKeyMapping.TryGetValue(secretKey.Key, out List<Authentication>? authList))
            {
                secretKeyMapping[secretKey.Key] = authList = [];
            }

            authList.Add(authEntry);
        }

        if (secretKeyMapping.Count == 0)
        {
            return (false, false, $"Failed to convert {failedConversions.Count} entries: " + string.Join(", ", failedConversions.Select(k => k.CharacterName)));
        }

        var baseUri = serverStorage.ServerUri.Replace("wss://", "https://").Replace("ws://", "http://");
        var oauthCheckUri = MareAuth.GetUIDsBasedOnSecretKeyFullPath(new Uri(baseUri));
        var requestContent = JsonContent.Create(secretKeyMapping.Select(k => k.Key).ToList());
        HttpRequestMessage requestMessage = new(HttpMethod.Post, oauthCheckUri);
        requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", serverStorage.OAuthToken);
        requestMessage.Content = requestContent;

        using var response = await _httpClient.SendAsync(requestMessage, token).ConfigureAwait(false);
        Dictionary<string, string>? secretKeyUidMapping = await JsonSerializer.DeserializeAsync<Dictionary<string, string>>
            (await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false), cancellationToken: token).ConfigureAwait(false);
        if (secretKeyUidMapping == null)
        {
            return (false, false, $"Failed to parse the server response. Failed to convert all entries.");
        }

        foreach (var entry in secretKeyMapping)
        {
            if (!secretKeyUidMapping.TryGetValue(entry.Key, out var assignedUid) || string.IsNullOrEmpty(assignedUid))
            {
                failedConversions.AddRange(entry.Value);
                continue;
            }

            foreach (var auth in entry.Value)
            {
                auth.UID = assignedUid;
                successfulConversions.Add(auth);
            }
        }

        if (successfulConversions.Count > 0)
            _serverConfigurationManager.Save();

        StringBuilder sb = new();
        sb.Append("Conversion complete." + Environment.NewLine);
        sb.Append($"Successfully converted {successfulConversions.Count} entries." + Environment.NewLine);
        if (failedConversions.Count > 0)
        {
            sb.Append($"Failed to convert {failedConversions.Count} entries, assign those manually: ");
            sb.Append(string.Join(", ", failedConversions.Select(k => k.CharacterName)));
        }

        return (true, failedConversions.Count != 0, sb.ToString());
    }

    private void DrawSettingsContent()
    {
        if (_apiController.ServerState is ServerState.Connected)
        {
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.23a2e5ea", "Service ") + _serverConfigurationManager.CurrentServer!.ServerName + ":");
            ImGui.SameLine();
            ImGui.TextColored(ImGuiColors.ParsedGreen, _uiShared.L("UI.SettingsUi.e03c6c57", "Available"));
            ImGui.SameLine();
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.28ed3a79", "("));
            ImGui.SameLine();
            ImGui.TextColored(ImGuiColors.ParsedGreen, _apiController.OnlineUsers.ToString(CultureInfo.InvariantCulture));
            ImGui.SameLine();
            ImGui.TextUnformatted(_uiShared.L("UI.CompactUILegacy.3dfd96d8", "Users Online"));
            ImGui.SameLine();
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.e7064f0b", ")"));
        }

        ImGui.AlignTextToFramePadding();
        ImGui.SameLine();
        ImGui.Separator();
        if (ImGui.BeginTabBar(_uiShared.L("UI.SettingsUi.39eff0e0", "mainTabBar")))
        {
            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.9239ee2c", "General")))
            {
                DrawGeneral();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.bcb3bf82", "Discovery Settings")))
            {
                DrawDiscoverySettings();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.41def7a0", "Appearance")))
            {
                DrawAppearance();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.63c90455", "Performance")))
            {
                DrawPerformance();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.9e092dda", "Storage")))
            {
                DrawFileStorageSettings();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.812412a0", "Transfers")))
            {
                DrawCurrentTransfers();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.d8d91d3c", "Service Settings")))
            {
                DrawServerConfiguration();
                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem(_uiShared.L("UI.SettingsUi.bd604d99", "Debug")))
            {
                DrawDebug();
                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }
    }

    private void UiSharedService_GposeEnd()
    {
        IsOpen = _wasOpen;
    }

    private void UiSharedService_GposeStart()
    {
        _wasOpen = IsOpen;
        IsOpen = false;
    }

    private void DrawAppearance()
    {
        _lastTab = "Appearance";
        _uiShared.BigText("Appearance");

        // Ensure game font list is populated (kept for other screens that may use it)
        if (_availableGameFonts.Count == 0)
        {
            _availableGameFonts = Enum.GetNames(typeof(GameFontFamilyAndSize))
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        var themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();
        if (themes.Count == 0)
        {
            UiSharedService.ColorTextWrapped(_uiShared.L("UI.SettingsUi.79a85121", "No themes found."), ImGuiColors.DalamudRed);
            return;
        }

        // Initial selection
        if (_themeSelectedIndex < 0)
        {
            var curId = _configService.Current.SelectedThemeId;
            if (string.Equals(curId, ThemeManager.NoneId, StringComparison.OrdinalIgnoreCase))
            {
                _themeSelectedIndex = -1;
                _renameBuffer = string.Empty; // keep rename box empty for "no theme"
            }
            else
            {
                _themeSelectedIndex = Math.Max(0, themes.FindIndex(t =>
                    string.Equals(t.Id, curId, StringComparison.OrdinalIgnoreCase)));
                if (_themeSelectedIndex < 0) _themeSelectedIndex = 0;

                // prime rename buffer with the selected theme name (if custom)
                var sel = themes[_themeSelectedIndex];
                _renameBuffer = _themeManager.IsCustom(sel.Id) ? (sel.Name ?? string.Empty) : string.Empty;
            }
        }

        // Theme dropdown (with "No theme" entry)
        var currentLabel = _themeSelectedIndex >= 0 ? themes[_themeSelectedIndex].Name : _uiShared.L("UI.SettingsUi.Theme.NoneLabel", "No theme (default UI)");
        ImGui.SetNextItemWidth(240f * ImGuiHelpers.GlobalScale);
        if (ImGui.BeginCombo(_uiShared.L("UI.SettingsUi.a797e309", "Theme"), currentLabel))
        {
            // --- No theme option ---
            bool selNone = _themeSelectedIndex == -1;
            if (ImGui.Selectable(_uiShared.L("UI.SettingsUi.e0786e01", "No theme (default UI)"), selNone))
            {
                _themeSelectedIndex = -1;
                _configService.Current.SelectedThemeId = ThemeManager.NoneId;
                _configService.Save();
                _themeManager.TryApply(ThemeManager.NoneId);
                _editingTheme = null;
                _renameBuffer = string.Empty; // keep input stable
            }
            if (selNone) ImGui.SetItemDefaultFocus();

            ImGui.Separator();

            // --- Normal theme entries ---
            for (int i = 0; i < themes.Count; i++)
            {
                bool sel = i == _themeSelectedIndex;
                if (ImGui.Selectable(themes[i].Name, sel))
                {
                    _themeSelectedIndex = i;
                    var id = themes[i].Id;

                    _configService.Current.SelectedThemeId = id;
                    _configService.Save();
                    _themeManager.TryApply(id);
                    ApplyThemeTypography(themes[i]);

                    // only allow editing if it's a custom theme
                    _editingTheme = _themeManager.IsCustom(id)
                        ? JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(themes[i]))!
                        : null;

                    // sync rename buffer with selected theme name (custom only)
                    _renameBuffer = _editingTheme?.Name ?? string.Empty;
                }
                if (sel) ImGui.SetItemDefaultFocus();
            }
            ImGui.EndCombo();
        }

        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Undo, _uiShared.L("UI.SettingsUi.d8fbae1a", "Reapply")))
        {
            var idToReapply = _themeSelectedIndex >= 0 ? themes[_themeSelectedIndex].Id : ThemeManager.NoneId;
            _themeManager.TryApply(idToReapply);
            if (_themeSelectedIndex >= 0)
                ApplyThemeTypography(themes[_themeSelectedIndex]);
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.331ec535", "Re-applies the selected theme in case another window changed colors."));

        // --- New Theme (from default UI) button ---
        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Plus, _uiShared.L("UI.SettingsUi.329e19a0", "New Theme")))
        {
            // Capture current ImGui style to a Colors block
            static string Hex(Vector4 v)
            {
                byte r = (byte)(Math.Clamp(v.X, 0, 1) * 255f);
                byte g = (byte)(Math.Clamp(v.Y, 0, 1) * 255f);
                byte b = (byte)(Math.Clamp(v.Z, 0, 1) * 255f);
                return $"#{r:X2}{g:X2}{b:X2}";
            }

            var style = ImGui.GetStyle();
            var seeded = new Themes.Theme
            {
                Id = "custom_" + DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Name = "New Theme",
                Colors = new Themes.Colors
                {
                    Text = Hex(style.Colors[(int)ImGuiCol.Text]),
                    MutedText = Hex(style.Colors[(int)ImGuiCol.TextDisabled]),
                    Background = Hex(style.Colors[(int)ImGuiCol.WindowBg]),
                    BackgroundAlt = Hex(style.Colors[(int)ImGuiCol.ChildBg]),
                    Surface = Hex(style.Colors[(int)ImGuiCol.FrameBg]),
                    Border = Hex(style.Colors[(int)ImGuiCol.Border]),
                    Highlight = Hex(style.Colors[(int)ImGuiCol.HeaderHovered]),
                    Primary = Hex(style.Colors[(int)ImGuiCol.Button]),
                    Accent = Hex(style.Colors[(int)ImGuiCol.ButtonActive]),
                    Success = "#3ECF8E",
                    Warning = "#FFB020",
                    Danger = "#E57373",
                    Info = "#4DA3FF",
                },
                Typography = new Themes.Typography
                {
                    DisplayFont = _fontManager.Current?.Name ?? "",
                    BaseSize = _fontManager.GetUserSizePx(),
                },
                Effects = new Themes.Effects { RoundedUi = style.FrameRounding > 0f }
            };

            _themeManager.SaveCustom(seeded);

            // refresh + select + apply
            themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();
            _themeSelectedIndex = themes.FindIndex(t => t.Id == seeded.Id);
            if (_themeSelectedIndex < 0) _themeSelectedIndex = 0;

            _configService.Current.SelectedThemeId = seeded.Id;
            _configService.Save();
            _themeManager.TryApply(seeded.Id);

            // editor points to saved version (deep copy for editing)
            var saved = _themeManager.GetById(seeded.Id)!;
            _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(saved))!;
            _renameBuffer = _editingTheme.Name ?? string.Empty; // sync buffer
        }

        ImGui.Separator();

        // Quick toggles (rounded)
        var curLive = _themeManager.Current;
        bool rounded = curLive.Effects.RoundedUi;
        if (ImGui.Checkbox(_uiShared.L("UI.SettingsUi.46830724", "Rounded UI"), ref rounded))
        {
            curLive.Effects.RoundedUi = rounded;              // live apply
            ImGui.GetStyle().FrameRounding = rounded ? 8f : 0f;
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.c98ed1a4", "Toggles soft corners on widgets."));

        ImGui.Separator();

        // Handle "No theme" explicitly, then bail (read-only default UI)
        if (_themeSelectedIndex == -1)
        {
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.c2a77d57", "Default UI (no theme)"));
            return;
        }

        var selectedTheme = themes[_themeSelectedIndex];
        bool isBuiltIn = _themeManager.IsBuiltIn(selectedTheme.Id);

        // ---------------- BUILT-IN (READ-ONLY) ----------------
        if (isBuiltIn)
        {
            // Current font info (global UI font)
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.49eb783c", "Text (read-only)"));
            var curFontName = _fontManager.Current?.Name ?? "(default)";
            var sizePx = _fontManager.GetUserSizePx();
            using (ImRaii.PushIndent())
            {
                ImGui.TextDisabled($"Font: {curFontName}");
                ImGui.TextDisabled($"Size: {sizePx:0.0}px");
                var h = _fontManager.GetCurrentHandle();
                if (h is not null)
                {
                    using (h.Push())
                        ImGui.TextDisabled(_uiShared.L("UI.SettingsUi.CA3C065D", "Preview: The quick brown fox jumps over the lazy dog â 0123456789"));
                }
            }

            ImGui.Separator();

            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.065da83a", "Theme palette (read-only)"));
            using (ImRaii.PushIndent())
            {
                void Swatch(string label, string hex)
                {
                    var col = UiSharedService.Vector4FromColorString(hex);
                    ImGui.ColorButton(label, col, ImGuiColorEditFlags.NoTooltip | ImGuiColorEditFlags.NoDragDrop);
                    ImGui.SameLine(); ImGui.TextUnformatted($"{label}: {hex}");
                }

                // Show only the practical colors users actually see
                Swatch(_uiShared.L("UI.SettingsUi.Theme.Label.BackgroundWindow", "Background (Window)"), selectedTheme.Colors.Background);
                Swatch(_uiShared.L("UI.SettingsUi.Theme.Label.TabsControls", "Tabs and controls"), selectedTheme.Colors.Surface);
                Swatch("Text (Primary)", selectedTheme.Colors.Text);
                Swatch("hints and previews", selectedTheme.Colors.MutedText);
                Swatch(_uiShared.L("UI.SettingsUi.bd2d600b", "Buttons"), selectedTheme.Colors.Primary);
                Swatch(_uiShared.L("UI.SettingsUi.Theme.Label.HighlightSelection", "Highlight (Selection)"), selectedTheme.Colors.Highlight);
                Swatch(_uiShared.L("UI.SettingsUi.Theme.Label.Border", "Border"), selectedTheme.Colors.Border);
            }

            ImGui.Separator();
            if (_uiShared.IconTextButton(FontAwesomeIcon.Clone, _uiShared.L("UI.SettingsUi.aa4e6823", "Duplicate as Custom")))
            {
                var clone = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(selectedTheme))!;
                clone.Id = $"{selectedTheme.Id}_copy";
                clone.Name = $"{selectedTheme.Name} (Copy)";

                _themeManager.SaveCustom(clone);

                // refresh + select + apply
                themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();
                _themeSelectedIndex = themes.FindIndex(t => t.Id == clone.Id);
                if (_themeSelectedIndex < 0) _themeSelectedIndex = 0;

                _configService.Current.SelectedThemeId = clone.Id;
                _configService.Save();
                _themeManager.TryApply(clone.Id);

                // editor points to saved version (deep copy for editing)
                var saved = _themeManager.GetById(clone.Id)!;
                _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(saved))!;
                _renameBuffer = _editingTheme.Name ?? string.Empty; // sync buffer
            }

            ImGui.Separator();
            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.eb206747", "Built-in themes cannot be edited directly. Duplicate or create a custom one to modify."));
            return; // Stop here for built-in
        }

        // ---------------- CUSTOM THEME (EDITABLE) ----------------
        if (_editingTheme == null)
            _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(selectedTheme))!; // safety

        // ---- Rename (custom only) ----
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.d645a920", "Theme name"));
        using (ImRaii.PushIndent())
        {
            if (_renameBuffer == string.Empty)
                _renameBuffer = _editingTheme!.Name ?? string.Empty;

            ImGui.SetNextItemWidth(260f * ImGuiHelpers.GlobalScale);
            var pressedEnter = ImGui.InputText("##rename_theme_name", ref _renameBuffer, 64, ImGuiInputTextFlags.EnterReturnsTrue);

            ImGui.SameLine();
            var clickedRename = _uiShared.IconTextButton(FontAwesomeIcon.Edit, _uiShared.L("UI.SettingsUi.e47eebbc", "Rename"));

            if ((clickedRename || pressedEnter)
                && !string.IsNullOrWhiteSpace(_renameBuffer)
                && !_themeManager.IsBuiltIn(_editingTheme.Id))
            {
                var oldId = _editingTheme.Id;
                var reqName = _renameBuffer.Trim();

                if (_themeManager.RenameCustom(oldId, reqName))
                {
                    // refresh & select the renamed theme
                    themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();

                    var renamed = themes.FirstOrDefault(t =>
                                       t.Name.Equals(reqName, StringComparison.OrdinalIgnoreCase))
                                  ?? themes.FirstOrDefault(t => t.Id.Equals(oldId, StringComparison.OrdinalIgnoreCase))
                                  ?? themes.First(); // safety

                    _configService.Current.SelectedThemeId = renamed.Id;
                    _configService.Save();
                    _themeManager.TryApply(renamed.Id);

                    _themeSelectedIndex = themes.FindIndex(t => t.Id == renamed.Id);
                    if (_themeSelectedIndex < 0) _themeSelectedIndex = 0;

                    // rebind editor to the saved version and sync buffer to canonical name
                    _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(
                        JsonSerializer.Serialize(_themeManager.GetById(renamed.Id)!))!;
                    _renameBuffer = _editingTheme.Name ?? reqName;

                    // AUTOSAVE
                    ScheduleAutoSave(TimeSpan.FromMilliseconds(50));
                }
            }
        }

        ImGui.Separator();

        // Typography (editable for custom)
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.c3328c39", "Text"));
        using (ImRaii.PushIndent())
        {
            _fontManager.DrawDropdown("Interface Font", 320f);

            ImGuiHelpers.ScaledDummy(6);

            ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.4d33ea02", "Custom fonts"));

            if (_uiShared.IconTextButton(FontAwesomeIcon.FileImport, _uiShared.L("UI.SettingsUi.ba81ef14", "Import .ttf/.otf")))
            {
                _uiShared.FileDialogManager.OpenFileDialog(_uiShared.L("UI.SettingsUi.b8f84eb9", "Select font file"), ".ttf,.otf", (success, paths) =>
                {
                    if (!success) return;
                    var path = paths.FirstOrDefault();
                    if (string.IsNullOrWhiteSpace(path)) return;

                    try
                    {
                        if (_fontManager.TryImportFont(path, out var importedId) && !string.IsNullOrWhiteSpace(importedId))
                        {
                            _fontManager.LoadAll();
                            _fontManager.TryApply(importedId);

                            _configService.Current.FontId = importedId;
                            _configService.Save();

                            Mediator.Publish(new NotificationMessage(
                                "Font imported",
                                $"Imported: {importedId}",
                                NotificationType.Info));
                        }
                        else
                        {
                            Mediator.Publish(new NotificationMessage(
                                "Font import failed",
                                "Please pick a valid .ttf/.otf file.",
                                NotificationType.Warning));
                        }
                    }
                    catch (Exception ex)
                    {
                        Mediator.Publish(new NotificationMessage(
                            "Font import failed",
                            "There was a problem importing the font. Check logs.",
                            NotificationType.Warning));
                    }
                }, 1, Directory.Exists(_fontManager.UserFontsDirectory) ? _fontManager.UserFontsDirectory : null);

                ImGui.SameLine();
                if (_uiShared.IconTextButton(FontAwesomeIcon.FolderOpen, _uiShared.L("UI.SettingsUi.03085684", "Open fonts folder")))
                {
                    try
                    {
                        Process.Start(new ProcessStartInfo
                        {
                            FileName = _fontManager.UserFontsDirectory,
                            UseShellExecute = true
                        });
                    }
                    catch
                    {
                    }
                }
            }

            // keep editing theme in sync with current selection (by Name)
            if (_editingTheme is not null && !_themeManager.IsBuiltIn(_editingTheme.Id))
            {
                var curFontName = _fontManager.Current?.Name;
                if (!string.IsNullOrWhiteSpace(curFontName))
                {
                    _editingTheme.Typography ??= new Themes.Typography();
                    _editingTheme.Typography.DisplayFont = curFontName;

                    if (_themeManager.Current?.Typography is not null)
                        _themeManager.Current.Typography.DisplayFont = curFontName;

                    ScheduleAutoSave();
                }
            }

            // Size (preset dropdown)
            var sizes = _fontManager.GetSupportedSizesPx();
            float curSz = _fontManager.GetUserSizePx();
            ImGui.SetNextItemWidth(220f * ImGuiHelpers.GlobalScale);
            if (ImGui.BeginCombo(_uiShared.L("UI.SettingsUi.b7152342", "Size"), $"{curSz:0.#} px"))
            {
                for (int i = 0; i < sizes.Count; i++)
                {
                    bool isSel = Math.Abs(curSz - sizes[i]) < 0.001f;
                    if (ImGui.Selectable($"{sizes[i]:0.#} px", isSel))
                    {
                        _fontManager.SetUserSizePx(sizes[i]);
                        _configService.Current.FontSizePx = sizes[i];
                        _configService.Save();
                        curSz = sizes[i];

                        if (_editingTheme is not null && !_themeManager.IsBuiltIn(_editingTheme.Id))
                        {
                            _editingTheme.Typography ??= new Themes.Typography();
                            _editingTheme.Typography.BaseSize = sizes[i];

                            if (_themeManager.Current?.Typography is not null)
                                _themeManager.Current.Typography.BaseSize = sizes[i];

                            ScheduleAutoSave();
                        }
                    }
                    if (isSel) ImGui.SetItemDefaultFocus();
                }
                ImGui.EndCombo();
            }

            var h = _fontManager.GetCurrentHandle();
            if (h is not null)
            {
                using (h.Push())
                    ImGui.TextDisabled(_uiShared.L("UI.SettingsUi.CA3C065D", "Preview: The quick brown fox jumps over the lazy dog â 0123456789"));
            }
        }

        ImGui.Separator();

        // Live-edit colors (only the ones users see), with instant preview
        ImGui.TextUnformatted(_uiShared.L("UI.SettingsUi.eb4e9125", "Colours"));
        using (ImRaii.PushIndent())
        {
            string Edit(string label, string hex, Action<string> assignBoth)
            {
                var col = UiSharedService.Vector4FromColorString(hex);
                ImGui.SetNextItemWidth(240f * ImGuiHelpers.GlobalScale);
                if (ImGui.ColorEdit4(label, ref col, ImGuiColorEditFlags.DisplayHex | ImGuiColorEditFlags.AlphaBar))
                {
                    var newHex = UiSharedService.ColorStringFromVector4(col);
                    assignBoth(newHex);
                    ScheduleAutoSave();
                }
                return UiSharedService.ColorStringFromVector4(col);
            }

            // keep BackgroundAlt as requested
            _editingTheme.Colors.Background = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.BackgroundWindow", "Background (Window)"), _editingTheme.Colors.Background, v =>
            {
                _editingTheme.Colors.BackgroundAlt = v;
                _themeManager.Current.Colors.BackgroundAlt = v; // live
            });
            _editingTheme.Colors.Primary = Edit(_uiShared.L("UI.SettingsUi.bd2d600b", "Buttons"), _editingTheme.Colors.Primary, v =>
            {
                _editingTheme.Colors.Primary = v;
                _themeManager.Current.Colors.Primary = v;
            });
            _editingTheme.Colors.Highlight = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.HighlightSelection", "Highlight (Selection)"), _editingTheme.Colors.Highlight, v =>
            {
                _editingTheme.Colors.Highlight = v;
                _themeManager.Current.Colors.Highlight = v;
            });
            _editingTheme.Colors.Border = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.Border", "Border"), _editingTheme.Colors.Border, v =>
            {
                _editingTheme.Colors.Border = v;
                _themeManager.Current.Colors.Border = v;
            });
            _editingTheme.Colors.Surface = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.TabsControls", "Tabs and controls"), _editingTheme.Colors.Surface, v =>
            {
                _editingTheme.Colors.Surface = v;
                _themeManager.Current.Colors.Surface = v;
            });
            _editingTheme.Colors.Text = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.PrimaryText", "Primary text"), _editingTheme.Colors.Text, v =>
            {
                _editingTheme.Colors.Text = v;
                _themeManager.Current.Colors.Text = v;
            });
            _editingTheme.Colors.MutedText = Edit(_uiShared.L("UI.SettingsUi.Theme.Label.HintsPreviews", "Hints and previews"), _editingTheme.Colors.MutedText, v =>
            {
                _editingTheme.Colors.MutedText = v;
                _themeManager.Current.Colors.MutedText = v;
            });
        }

        ImGui.Separator();


        // Actions: Save, Revert, Delete
        if (_uiShared.IconTextButton(FontAwesomeIcon.Save, _uiShared.L("UI.SettingsUi.e65d11e6", "Save")))
        {
            if (string.IsNullOrWhiteSpace(_editingTheme.Id))
                _editingTheme.Id = (_editingTheme.Name ?? "custom").Trim().ToLowerInvariant().Replace(' ', '_');

            _themeManager.SaveCustom(_editingTheme);

            // Refresh list & re-select
            themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();
            _themeSelectedIndex = themes.FindIndex(t => t.Id.Equals(_editingTheme.Id, StringComparison.OrdinalIgnoreCase));
            if (_themeSelectedIndex < 0) _themeSelectedIndex = 0;

            _configService.Current.SelectedThemeId = _editingTheme.Id;
            _configService.Save();
            _themeManager.TryApply(_editingTheme.Id);

            // Rebind editor to saved version
            var saved = _themeManager.GetById(_editingTheme.Id);
            if (saved != null)
            {
                _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(saved))!;
                _renameBuffer = _editingTheme.Name ?? _renameBuffer; // keep input in sync
            }
        }

        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Undo, _uiShared.L("UI.SettingsUi.d56d1310", "Revert")))
        {
            var reloaded = _themeManager.GetById(_editingTheme!.Id);
            _editingTheme = reloaded != null
                ? JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(reloaded))!
                : _editingTheme;

            // also revert live (cancel preview)
            if (reloaded != null)
            {
                _themeManager.TryApply(reloaded.Id);
                _renameBuffer = reloaded.Name ?? _renameBuffer; // sync buffer to canonical name
            }
        }

        ImGui.SameLine();
        using (ImRaii.Disabled(!_themeManager.IsCustom(_editingTheme!.Id) || !UiSharedService.CtrlPressed()))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, _uiShared.L("UI.SettingsUi.79e3ac91", "Delete (hold CTRL)")))
            {
                _themeManager.DeleteCustom(_editingTheme!.Id);

                themes = _themeManager.Custom.Concat(_themeManager.BuiltIn).ToList();
                _themeSelectedIndex = Math.Clamp(_themeSelectedIndex, 0, Math.Max(0, themes.Count - 1));

                var fallbackId = themes.Count > 0 ? themes[_themeSelectedIndex].Id : "avernus";
                _configService.Current.SelectedThemeId = fallbackId;
                _configService.Save();
                _themeManager.TryApply(fallbackId);

                _editingTheme = _themeManager.IsCustom(fallbackId)
                    ? JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(_themeManager.GetById(fallbackId)!))!
                    : null;

                _renameBuffer = _editingTheme?.Name ?? string.Empty; // reset buffer to new selection
            }
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.SettingsUi.39ac769f", "Hold CTRL and click Delete to delete this theme."));
    }

    void DrawDiscoverySettings()
    {
        if (!string.Equals(_lastTab, "Discovery Settings", StringComparison.OrdinalIgnoreCase))
        {
            _notesSuccessfullyApplied = null;
        }

        _lastTab = "Discovery Settings";

        _uiShared.BigText("Discovery & Presence");

        UiSharedService.TextWrapped(_uiShared.L("UI.SettingsUi.3ebc5831", "Discovery is entirely opt-in. When you turn it on, other RavaSync users who have also opted in can see that you're on RavaSync (with a little ♥) and can right-click you to send a pair request — and you can do the same to them. If you leave it off, you're effectively invisible: no hearts, no right-click pair option, and people will need your UID to pair with you."));

        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(5);

        // --- Master opt-in flag ---
        bool discoveryPresence = _configService.Current.EnableRavaDiscoveryPresence;
        if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.e6c3b6f6", "Show RavaSync presence to other RavaSync users?"), ref discoveryPresence))
        {
            _configService.Current.EnableRavaDiscoveryPresence = discoveryPresence;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L(
                                           "UI.DiscoverySettingsUi.Presence.Help",
                                           "When enabled: you and other RavaSync users who also opted in can see each other's ♥ and use the right-click \"Send pair request\" option. " + 
                                           "When disabled: you don't participate in discovery at all and stay hidden."));

        ImGuiHelpers.ScaledDummy(5);

        // --- Right-click pair request menu toggle ---
        bool enablePairRequestMenu = _configService.Current.EnableSendPairRequestContextMenu;
        using (ImRaii.Disabled(!discoveryPresence))
        {
            if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.EE0E6297", "Show \"Send pair request\" in right-click menu"), ref enablePairRequestMenu))
            {
                _configService.Current.EnableSendPairRequestContextMenu = enablePairRequestMenu;
                _configService.Save();
            }
        }
        _uiShared.DrawHelpText(_uiShared.L(
            "UI.DiscoverySettingsUi.SendPairRequest.Help",
            "Adds a \"Send pair request\" option to the right-click menu for RavaSync users that you're not already paired with. Requires discovery to be enabled."));

        ImGuiHelpers.ScaledDummy(5);

        // --- Auto-decline incoming requests ---
        bool autoDecline = _configService.Current.AutoDeclineIncomingPairRequests;
        if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.3bb79ae6", "Automatically decline incoming pair requests"), ref autoDecline))
        {
            _configService.Current.AutoDeclineIncomingPairRequests = autoDecline;
            _configService.Save();
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.DiscoverySettingsUi.AutoDecline.Help", "Exactly what it says on the tin. If you don't want randoms sending you pair requests, turn this on and they'll be politely told no."));
ImGuiHelpers.ScaledDummy(5);

        // --- Heart cosmetic toggle ---
        bool showHeart = _configService.Current.ShowFriendshapedHeart;
        using (ImRaii.Disabled(!discoveryPresence))
        {
            if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.FFF66944", "Show â¥ on RavaSync users not yet paired"), ref showHeart))
            {
                _configService.Current.ShowFriendshapedHeart = showHeart;
                _configService.Save();
            }
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.DiscoverySettingsUi.ShowHeart.Help", "Controls the little ♥ on nameplates for other RavaSync users that you're not paired with yet. Purely cosmetic and only relevant when discovery is turned on."));
}


    private void ApplyThemeTypography(Themes.Theme theme)
    {
        if (theme == null) return;
        var typo = theme.Typography;
        if (typo == null) return;

        var uiFont = typo.DisplayFont;
        if (!string.IsNullOrWhiteSpace(uiFont))
        {
            var face = _fontManager.Available.FirstOrDefault(f =>
                   uiFont.Equals(f.Name, StringComparison.OrdinalIgnoreCase)
                || uiFont.Equals(f.Id, StringComparison.OrdinalIgnoreCase)
                || uiFont.Equals(Path.GetFileNameWithoutExtension(f.Id), StringComparison.OrdinalIgnoreCase));

            if (face != null)
            {
                _fontManager.TryApply(face.Id);
                _configService.Current.FontId = face.Id;
            }
        }

        float? sizeFromTheme = null;

        try
        {
            sizeFromTheme = (float)typo.BaseSize;
        }
        catch { /* schema differences are fine; just skip size */ }

        if (sizeFromTheme is float px && px > 0)
        {
            _fontManager.SetUserSizePx(px); // snaps to nearest prebaked size, instant handle swap
            _configService.Current.FontSizePx = _fontManager.GetUserSizePx();
        }

        _configService.Save();
    }

    private CancellationTokenSource? _autoSaveCts;

    private void ScheduleAutoSave(TimeSpan? delay = null)
    {
        delay ??= TimeSpan.FromMilliseconds(400);
        _autoSaveCts?.Cancel();
        _autoSaveCts = new CancellationTokenSource();
        var token = _autoSaveCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay.Value, token);
                if (!token.IsCancellationRequested)
                    AutoSaveNow();
            }
            catch (TaskCanceledException) { /* expected on rapid changes */ }
        }, token);
    }

    private void AutoSaveNow()
    {
        if (_editingTheme is null) return;
        if (_themeManager.IsBuiltIn(_editingTheme.Id)) return;

        if (string.IsNullOrWhiteSpace(_editingTheme.Id))
            _editingTheme.Id = (_editingTheme.Name ?? "custom").Trim().ToLowerInvariant().Replace(' ', '_');

        _themeManager.SaveCustom(_editingTheme);

        var saved = _themeManager.GetById(_editingTheme.Id);
        if (saved != null)
        {
            _editingTheme = JsonSerializer.Deserialize<Themes.Theme>(JsonSerializer.Serialize(saved))!;
        }
    }


}