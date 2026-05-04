using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Interface.Windowing;
using Dalamud.Plugin.Services;
using Dalamud.Utility;
using Microsoft.Extensions.Logging;
using Penumbra.Api.Enums;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.Optimisation;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI.Components;
using RavaSync.UI.Handlers;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using RavaSync.WebAPI.SignalR.Utils;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Runtime.InteropServices;

namespace RavaSync.UI;

public class CompactUi : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly MareConfigService _configService;
    private readonly ConcurrentDictionary<GameObjectHandler, DownloadStatusAggregate> _currentDownloads = new();
    private readonly DrawEntityFactory _drawEntityFactory;
    private readonly FileUploadManager _fileTransferManager;
    private readonly PairManager _pairManager;
    private readonly SelectTagForPairUi _selectGroupForPairUi;
    private readonly SelectPairForTagUi _selectPairsForGroupUi;
    private readonly IpcManager _ipcManager;
    private readonly ServerConfigurationManager _serverManager;
    private readonly TopTabMenu _tabMenu;
    private readonly TagHandler _tagHandler;
    private readonly UiSharedService _uiSharedService;
    private readonly CharacterAnalyzer _characterAnalyzer;
    private readonly PlayerPerformanceConfigService _playerPerformanceConfigService;
    private readonly PlayerPerformanceService _playerPerformanceService;
    private readonly TransientConfigService _transientConfigService;
    private readonly TextureOptimisationService _textureOptimizationService;
    private readonly MeshOptimisationService _meshOptimizationService;
    private readonly OptimisationPolicyService _optimisationPolicyService;
    private readonly TransientResourceManager _transientResourceManager;
    private readonly IFramework _framework;
    private List<IDrawFolder> _drawFolders;
    private Pair? _lastAddedUser;
    private string _lastAddedUserComment = string.Empty;
    private Vector2 _lastPosition = Vector2.One;
    private Vector2 _lastSize = Vector2.One;
    private bool _showModalForUserAddition;
    private bool _wasOpen;
    private float _windowContentWidth;
    private Vector2? _restoreMainUiPos;
    private bool _restoreMainUiOpenToLeft;
    private bool _restoreUncollapse;
    private bool _suppressMinimizedRestoreIcon;
    private bool _wasCollapsed;
    private volatile bool _drawFoldersRefreshPending;
    private volatile bool _drawFoldersRefreshDeferredUntilVisible;
    private long _nextDrawFoldersRefreshUtcTicks;
    private volatile bool _headerStatsRefreshPending = true;
    private long _nextHeaderStatsRefreshUtcTicks;
    private HeaderStatsSnapshot _headerStatsSnapshot;


    private enum PairViewTab
    {
        DirectPairs,
        Shells,
        Visible
    }

    private readonly record struct DownloadStatusAggregate(long TotalBytes, long TransferredBytes, int TotalFiles, int TransferredFiles)
    {
        public bool HasAny => TotalFiles > 0 || TotalBytes > 0 || TransferredFiles > 0 || TransferredBytes > 0;
    }

    private readonly record struct HeaderStatsSnapshot(
        long MyVramBytes,
        long MyTriangles,
        bool HasAnalysis,
        long PairsVramBytes,
        long ShellsVramBytes,
        long TotalVramBytes);

    private PairViewTab _currentPairViewTab = PairViewTab.DirectPairs;
    private string _pairSearch = string.Empty;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public CompactUi(ILogger<CompactUi> logger, UiSharedService uiShared, MareConfigService configService, ApiController apiController, PairManager pairManager,
        ServerConfigurationManager serverManager, MareMediator mediator, FileUploadManager fileTransferManager,
        TagHandler tagHandler, DrawEntityFactory drawEntityFactory, SelectTagForPairUi selectTagForPairUi, SelectPairForTagUi selectPairForTagUi,
        PerformanceCollectorService performanceCollectorService, IpcManager ipcManager, PlayerPerformanceConfigService playerPerformanceConfigService, PlayerPerformanceService playerPerformanceService,
        CharacterAnalyzer characterAnalyzer, TransientConfigService transientConfigService, TransientResourceManager transientResourceManager,
        TextureOptimisationService textureOptimizationService, MeshOptimisationService meshOptimizationService, OptimisationPolicyService optimisationPolicyService, IFramework framework)
            : base(logger, mediator, "###RavaSyncMainUI", performanceCollectorService)
    {
        _uiSharedService = uiShared;
        _configService = configService;
        _apiController = apiController;
        _pairManager = pairManager;
        _serverManager = serverManager;
        _fileTransferManager = fileTransferManager;
        _tagHandler = tagHandler;
        _drawEntityFactory = drawEntityFactory;
        _selectGroupForPairUi = selectTagForPairUi;
        _selectPairsForGroupUi = selectPairForTagUi;
        _ipcManager = ipcManager;
        _playerPerformanceConfigService = playerPerformanceConfigService;
        _playerPerformanceService = playerPerformanceService;
        _characterAnalyzer = characterAnalyzer;
        _transientConfigService = transientConfigService;
        _transientResourceManager = transientResourceManager;
        _textureOptimizationService = textureOptimizationService;
        _meshOptimizationService = meshOptimizationService;
        _optimisationPolicyService = optimisationPolicyService;
        _framework = framework;
        _tabMenu = new TopTabMenu(Mediator, _apiController, _pairManager, _uiSharedService);
        _bc7Progress.ProgressChanged += (_, e) =>
        {
            _bc7CurFile = e.fileName;
            _bc7CurIndex = e.index;
        };

        AllowPinning = false;
        AllowClickthrough = false;
        TitleBarButtons = new()
        {
            new TitleBarButton()
            {
                Icon = FontAwesomeIcon.Cog,
                Click = (msg) =>
                {
                    Mediator.Publish(new UiToggleMessage(typeof(SettingsUi)));
                },
                IconOffset = new(2,1),
                ShowTooltip = () =>
                {
                    ImGui.BeginTooltip();
                    ImGui.Text(_uiSharedService.L("UI.CompactUI.57ed29f5", "Open RavaSync Settings"));
                    ImGui.EndTooltip();
                }
            },
            new TitleBarButton()
            {
                Icon = FontAwesomeIcon.Book,
                Click = (msg) =>
                {
                    Mediator.Publish(new UiToggleMessage(typeof(EventViewerUI)));
                },
                IconOffset = new(2,1),
                ShowTooltip = () =>
                {
                    ImGui.BeginTooltip();
                    ImGui.Text(_uiSharedService.L("UI.CompactUI.eb711ab9", "Open RavaSync Event Viewer"));
                    ImGui.EndTooltip();
                }
            },
            new TitleBarButton()
            {
                Icon = GetTitleBarConnectionIcon(),
                Click = (msg) =>
                {
                    if (_apiController.ServerState is not (ServerState.Reconnecting or ServerState.Disconnecting))
                    {
                        ToggleServerConnectionFromTitleBar();
                    }
                },
                IconOffset = new(2,1),
                ShowTooltip = () =>
                {
                    ImGui.BeginTooltip();
                    ImGui.Text(GetTitleBarConnectionTooltip());
                    ImGui.EndTooltip();
                }
            },
        };

        _drawFolders = GetDrawFolders().ToList();

#if DEBUG
        string dev = "Dev Build";
        var ver = Assembly.GetExecutingAssembly().GetName().Version!;
        WindowName = $"RavaSync {dev} ({ver.Major}.{ver.Minor}.{ver.Build})###RavaSyncMainUI";
        Toggle();
#else
        var ver = Assembly.GetExecutingAssembly().GetName().Version;
        WindowName = "RavaSync " + ver.Major + "." + ver.Minor + "." + ver.Build + "###RavaSyncMainUI";
#endif
        IsOpen = _configService.Current.HasValidSetup() && _configService.Current.CompactUiLastOpen;

        Mediator.Subscribe<SwitchToMainUiMessage>(this, (_) => IsOpen = true);
        Mediator.Subscribe<SwitchToIntroUiMessage>(this, (_) => IsOpen = false);
        Mediator.Subscribe<CutsceneStartMessage>(this, (_) => UiSharedService_GposeStart());
        Mediator.Subscribe<CutsceneEndMessage>(this, (_) => UiSharedService_GposeEnd());
        Mediator.Subscribe<DownloadStartedMessage>(this, (msg) =>
        {
            if (msg.DownloadId == null) return;
            _currentDownloads[msg.DownloadId] = SummarizeDownloadStatus(msg.DownloadStatus);
        });
        Mediator.Subscribe<DownloadFinishedMessage>(this, (msg) =>
        {
            if (msg.DownloadId == null) return;
            _currentDownloads.TryRemove(msg.DownloadId, out _);
        });
        Mediator.Subscribe<RefreshUiMessage>(this, (msg) =>
        {
            ScheduleDrawFoldersRefresh();
            ScheduleHeaderStatsRefresh();
        });
        Mediator.Subscribe<CharacterDataCreatedMessage>(this, (_) =>
        {
            ScheduleReduceMySizeSnapshotRebuild(immediate: true);
            ScheduleHeaderStatsRefresh(immediate: true);
            PumpReduceMySizeBackgroundWork(ignoreThrottle: true);
        });
        Mediator.Subscribe<CharacterDataAnalyzedMessage>(this, (_) =>
        {
            ScheduleReduceMySizeSnapshotRebuild();
            ScheduleMeshEstimateQueueBuild();
            _lastSeenMeshEstimateGeneration = -1;
            Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, 0L);
            ScheduleHeaderStatsRefresh();
            PumpReduceMySizeBackgroundWork(ignoreThrottle: true);
        });

        Mediator.Subscribe<RestoreCompactUiStateMessage>(this, (_) =>
        {
            if (!_configService.Current.HasValidSetup())
                return;

            if (_configService.Current.CompactUiLastMinimized && _configService.Current.ShowMinimizedRestoreIcon)
            {
                _suppressMinimizedRestoreIcon = false;
                IsOpen = false;
                Mediator.Publish(new MainUiMinimizedMessage());
                return;
            }

            IsOpen = true;
        });

        Mediator.Subscribe<RestoreMainUiAtPositionMessage>(this, (msg) =>
        {
            _restoreMainUiPos = msg.Position;
            _restoreMainUiOpenToLeft = msg.OpenToLeft;
            _restoreUncollapse = true;
            IsOpen = true;
        });

        Flags |= ImGuiWindowFlags.NoDocking | ImGuiWindowFlags.NoCollapse;

        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new Vector2(385, 500),
            MaximumSize = new Vector2(385, 2100),
        };

        _framework.Update += OnFrameworkUpdate;
        ScheduleReduceMySizeSnapshotRebuild(initialBuild: true);
        PumpReduceMySizeBackgroundWork(ignoreThrottle: true);
    }

    private void PersistCompactUiState(bool open, bool minimized)
    {
        if (_configService.Current.CompactUiLastOpen == open
            && _configService.Current.CompactUiLastMinimized == minimized)
        {
            return;
        }

        _configService.Current.CompactUiLastOpen = open;
        _configService.Current.CompactUiLastMinimized = minimized;
        _configService.Save();
    }

    public override void OnOpen()
    {
        PersistCompactUiState(open: true, minimized: false);
        _suppressMinimizedRestoreIcon = false;

        if (_configService.Current.ShowMinimizedRestoreIcon)
            Mediator.Publish(new MainUiRestoredMessage());

        if (_drawFoldersRefreshDeferredUntilVisible || _drawFoldersRefreshPending)
            ScheduleDrawFoldersRefresh(immediate: true);

        ScheduleHeaderStatsRefresh(immediate: true);
        PumpReduceMySizeBackgroundWork(ignoreThrottle: true);

        base.OnOpen();
    }

    public override void OnClose()
    {
        if (!_suppressMinimizedRestoreIcon && _configService.Current.ShowMinimizedRestoreIcon)
        {
            PersistCompactUiState(open: false, minimized: true);
            Mediator.Publish(new MainUiMinimizedAtPositionMessage(_lastPosition));
            Mediator.Publish(new MainUiMinimizedMessage());
        }
        else
        {
            PersistCompactUiState(open: false, minimized: false);
        }

        base.OnClose();
    }

    protected override void DrawInternal()
    {
        _windowContentWidth = UiSharedService.GetWindowContentRegionWidth();
        UpdateCompactWindowTitle();
        RefreshDrawFoldersIfNeeded();
        RefreshHeaderStatsIfNeeded();

        if (_configService.Current.ShowMinimizedRestoreIcon)
            Flags |= ImGuiWindowFlags.NoCollapse;
        else
            Flags &= ~ImGuiWindowFlags.NoCollapse;

        if (_restoreMainUiPos != null)
        {
            var vp = ImGui.GetMainViewport();
            var pos1 = _restoreMainUiPos.Value;
            var restoreWidth = ImGui.GetWindowSize().X;
            if (restoreWidth <= 0f)
                restoreWidth = 385f * ImGuiHelpers.GlobalScale;
            if (_restoreMainUiOpenToLeft)
                pos1.X -= MathF.Max(0f, restoreWidth - (44f * ImGuiHelpers.GlobalScale));
            var min = vp.WorkPos;
            var max = vp.WorkPos + vp.WorkSize - new Vector2(60f, 60f);
            pos1 = Vector2.Clamp(pos1, min, max);
            ImGui.SetWindowPos(pos1, ImGuiCond.Always);
            _restoreMainUiPos = null;
            _restoreMainUiOpenToLeft = false;
        }

        if (_restoreUncollapse)
        {
            ImGui.SetWindowCollapsed(false, ImGuiCond.Always);
            ImGui.SetWindowFocus();
            _restoreUncollapse = false;
        }

        if (!_apiController.IsCurrentVersion)
        {
            var ver = _apiController.CurrentClientVersion;
            var unsupported = "UNSUPPORTED VERSION";

            using (_uiSharedService.UidFont.Push())
            {
                var uidTextSize = ImGui.CalcTextSize(unsupported);
                ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMax().X + ImGui.GetWindowContentRegionMin().X) / 2 - uidTextSize.X / 2);
                ImGui.AlignTextToFramePadding();
                ImGui.TextColored(ImGuiColors.DalamudRed, unsupported);
            }
            UiSharedService.ColorTextWrapped(string.Format(_uiSharedService.L("UI.CompactUI.d6f399f2", "Your RavaSync installation is out of date, the current version is {0}.{1}.{2}. It is highly recommended to keep RavaSync up to date. Open /xlplugins and update the plugin."), ver.Major, ver.Minor, ver.Build), ImGuiColors.DalamudRed);
        }

        if (!_ipcManager.Initialized)
        {
            var unsupported = "MISSING ESSENTIAL PLUGINS";

            using (_uiSharedService.UidFont.Push())
            {
                var uidTextSize = ImGui.CalcTextSize(unsupported);
                ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMax().X + ImGui.GetWindowContentRegionMin().X) / 2 - uidTextSize.X / 2);
                ImGui.AlignTextToFramePadding();
                ImGui.TextColored(ImGuiColors.DalamudRed, unsupported);
            }
            var penumAvailable = _ipcManager.Penumbra.APIAvailable;
            var glamAvailable = _ipcManager.Glamourer.APIAvailable;

            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.CompactUI.b7212e0b", "One or more Plugins essential for RavaSync operation are unavailable. Enable or update following plugins:"), ImGuiColors.DalamudRed);
            using var indent = ImRaii.PushIndent(10f);
            if (!penumAvailable)
            {
                UiSharedService.TextWrapped(_uiSharedService.L("UI.CompactUI.2b89c404", "Penumbra"));
                _uiSharedService.BooleanToColoredIcon(penumAvailable, true);
            }
            if (!glamAvailable)
            {
                UiSharedService.TextWrapped(_uiSharedService.L("UI.CharaDataHubUiMcdOnline.90d38268", "Glamourer"));
                _uiSharedService.BooleanToColoredIcon(glamAvailable, true);
            }
            ImGui.Separator();
        }

        using (ImRaii.PushId("header")) DrawUIDHeader();
        ImGui.Separator();

        if (_apiController.ServerState is ServerState.Connected)
        {
            var style = ImGui.GetStyle();
            float scale = ImGuiHelpers.GlobalScale;

            using (ImRaii.PushStyle(ImGuiStyleVar.FramePadding, new Vector2(style.FramePadding.X, 1f * scale)))
            using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, new Vector2(style.ItemSpacing.X, 2f * scale)))
            using (ImRaii.PushId("global-topmenu"))
            {
                _tabMenu.Draw();
            }

            ImGui.Separator();

            if (_configService.Current.showTransferText)
            {
                using (ImRaii.PushId("transfers")) DrawTransfers();
                ImGui.Separator();
            }

            DrawPairViewHeader();
            ImGui.Separator();

            using (ImRaii.PushId("pairlist")) DrawPairs();

            using (ImRaii.PushId("group-user-popup")) _selectPairsForGroupUi.Draw(_pairManager.DirectPairs);
            using (ImRaii.PushId("grouping-popup")) _selectGroupForPairUi.Draw();
        }

        if (_configService.Current.OpenPopupOnAdd && _pairManager.LastAddedUser != null)
        {
            _lastAddedUser = _pairManager.LastAddedUser;
            _pairManager.LastAddedUser = null;
            ImGui.OpenPopup(_uiSharedService.L("UI.CompactUI.94bdb3d2", "Set Notes for New User"));
            _showModalForUserAddition = true;
            _lastAddedUserComment = string.Empty;
        }

        if (ImGui.BeginPopupModal(_uiSharedService.L("UI.CompactUI.94bdb3d2", "Set Notes for New User"), ref _showModalForUserAddition, UiSharedService.PopupWindowFlags))
        {
            if (_lastAddedUser == null)
            {
                _showModalForUserAddition = false;
            }
            else
            {
                UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.CompactUI.be2f451b", "You have successfully added {0}. Set a local note for the user in the field below:"), _lastAddedUser.UserData.AliasOrUID));
                ImGui.InputTextWithHint("##noteforuser", string.Format(_uiSharedService.L("UI.CompactUI.53eef159", "Note for {0}"), _lastAddedUser.UserData.AliasOrUID), ref _lastAddedUserComment, 100);
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, _uiSharedService.L("UI.CompactUI.a3f4a501", "Save Note")))
                {
                    _serverManager.SetNoteForUid(_lastAddedUser.UserData.UID, _lastAddedUserComment);
                    _lastAddedUser = null;
                    _lastAddedUserComment = string.Empty;
                    _showModalForUserAddition = false;
                }
            }
            UiSharedService.SetScaledWindowSize(275);
            ImGui.EndPopup();
        }

        if (_bc7Task != null && !_bc7Task.IsCompleted)
        {
            if (_bc7ShowModal && !_bc7ModalOpen)
            {
                ImGui.OpenPopup(_uiSharedService.L("UI.CompactUI.fa170e36", "Optimisation in Progress"));
                _bc7ModalOpen = true;
            }

            if (ImGui.BeginPopupModal(_uiSharedService.L("UI.CompactUI.fa170e36", "Optimisation in Progress")))
            {
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.b8ef613e", "Optimisation in progress: {0}/{1}"), _bc7CurIndex, _bc7Total));
                UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.CompactUI.12a59426", "Current file: {0}"), _bc7CurFile));
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StopCircle, _uiSharedService.L("UI.CompactUI.08eb8f15", "Cancel optimisation")))
                {
                    _bc7Cts.Cancel();
                }
                UiSharedService.SetScaledWindowSize(500);
                ImGui.EndPopup();
            }
            else
            {
                _bc7ModalOpen = false;
            }
        }
        else if (_bc7Task != null && _bc7Task.IsCompleted && _bc7Total > 0)
        {
            _bc7Task = null;
            _bc7Set.Clear();
            _bc7CurFile = string.Empty;
            _bc7CurIndex = 0;
            _bc7Total = 0;
            _bc7ShowModal = false;
            _bc7ModalOpen = false;

            _ = _characterAnalyzer.ComputeAnalysis(print: false, recalculate: true);
            ScheduleReduceMySizeSnapshotRebuild(immediate: true);
        }

        var pos = ImGui.GetWindowPos();
        var size = ImGui.GetWindowSize();
        if (_lastSize != size || _lastPosition != pos)
        {
            _lastSize = size;
            _lastPosition = pos;
            Mediator.Publish(new CompactUiChange(_lastSize, _lastPosition));
        }
    }

    private void DrawPairs()
    {
        var style = ImGui.GetStyle();

        float availableHeight = ImGui.GetWindowContentRegionMax().Y
                                - ImGui.GetCursorPosY()
                                - style.WindowPadding.Y;
        if (availableHeight < 1f) availableHeight = 1f;

        ImGui.BeginChild(_uiSharedService.L("UI.CompactUI.fe6db99f", "pairlist-main"), new Vector2(_windowContentWidth, 0), border: false);

        foreach (var item in _drawFolders)
        {
            item.Draw();
        }

        ImGui.EndChild();
    }

    private void DrawPairViewHeader()
    {
        using var id = ImRaii.PushId("PairViewHeader");

        var style = ImGui.GetStyle();
        float contentWidth = UiSharedService.GetWindowContentRegionWidth();

        bool tabChanged = false;

        void DrawTab(string label, PairViewTab tab)
        {
            bool selected = _currentPairViewTab == tab;

            if (selected)
                ImGui.PushStyleColor(ImGuiCol.Button, ImGuiColors.DalamudGrey3);

            if (ImGui.Button(label))
            {
                if (_currentPairViewTab != tab)
                {
                    _currentPairViewTab = tab;
                    tabChanged = true;
                }
            }

            if (selected)
                ImGui.PopStyleColor();
        }

        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Pairs", "Pairs"), PairViewTab.DirectPairs);
        ImGui.SameLine();
        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Shells", "Shells"), PairViewTab.Shells);
        ImGui.SameLine();
        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Visible", "Visible"), PairViewTab.Visible);

        ImGui.SameLine();

        float searchWidth = contentWidth - ImGui.GetCursorPosX() - style.WindowPadding.X;
        if (searchWidth < 150f * ImGuiHelpers.GlobalScale)
            searchWidth = 150f * ImGuiHelpers.GlobalScale;

        ImGui.SetNextItemWidth(searchWidth);
        bool searchChanged = ImGui.InputTextWithHint("##pairSearch",
            _uiSharedService.L("UI.CompactUI.d01c78fb", "Search pairs…"), ref _pairSearch, 128,
            ImGuiInputTextFlags.None);

        if (tabChanged || searchChanged)
        {
            ScheduleDrawFoldersRefresh(immediate: true);
            RefreshDrawFoldersIfNeeded();
        }
    }

    private void DrawTransfers()
    {
        var currentUploads = _fileTransferManager.GetCurrentUploadsSnapshot();
        ImGui.AlignTextToFramePadding();
        _uiSharedService.IconText(FontAwesomeIcon.Upload);
        ImGui.SameLine(35 * ImGuiHelpers.GlobalScale);

        if (currentUploads.Any())
        {
            var totalUploads = currentUploads.Count();

            var doneUploads = currentUploads.Count(c => c.IsTransferred);
            var totalUploaded = currentUploads.Sum(c => c.Transferred);
            var totalToUpload = currentUploads.Sum(c => c.Total);

            ImGui.TextUnformatted($"{doneUploads}/{totalUploads}");
            var uploadText = $"({UiSharedService.ByteToString(totalUploaded)}/{UiSharedService.ByteToString(totalToUpload)})";
            var textSize = ImGui.CalcTextSize(uploadText);
            ImGui.SameLine(_windowContentWidth - textSize.X);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(uploadText);
        }
        else
        {
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.9a2c8241", "No uploads in progress"));
        }

        long totalToDownload = 0;
        long totalDownloaded = 0;
        int totalDownloads = 0;
        int doneDownloads = 0;
        bool anyDownloads = false;

        foreach (var aggregate in _currentDownloads.Values)
        {
            if (!aggregate.HasAny)
                continue;

            anyDownloads = true;
            totalDownloads += aggregate.TotalFiles;
            doneDownloads += aggregate.TransferredFiles;
            totalDownloaded += aggregate.TransferredBytes;
            totalToDownload += aggregate.TotalBytes;
        }

        ImGui.AlignTextToFramePadding();
        _uiSharedService.IconText(FontAwesomeIcon.Download);
        ImGui.SameLine(35 * ImGuiHelpers.GlobalScale);

        if (anyDownloads)
        {
            ImGui.TextUnformatted($"{doneDownloads}/{totalDownloads}");
            var downloadText =
                $"({UiSharedService.ByteToString(totalDownloaded)}/{UiSharedService.ByteToString(totalToDownload)})";
            var textSize = ImGui.CalcTextSize(downloadText);
            ImGui.SameLine(_windowContentWidth - textSize.X);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(downloadText);
        }
        else
        {
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.34fbbc21", "No downloads in progress"));
        }
    }

    static string NoDot00(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;

        var space = s.IndexOf(' ');
        if (space <= 0) return s;

        var num = s[..space];
        var unit = s[space..];

        if (num.EndsWith(".00", StringComparison.Ordinal))
            return num[..^3] + unit;

        return s;
    }

    static string FormatTrianglesThousands(long triangles)
    {
        if (triangles <= 0) return "0K";
        long roundedThousands = (long)Math.Round(triangles / 1000d, MidpointRounding.AwayFromZero);
        return $"{roundedThousands:N0}K";
    }

    private void DrawUIDHeader()
    {
        var contentW = UiSharedService.GetWindowContentRegionWidth();
        var scale = ImGuiHelpers.GlobalScale;

        bool isConnected = _apiController.ServerState is ServerState.Connected;
        var snapshot = _reduceMySizeSnapshot;
        var headerStats = _headerStatsSnapshot;
        long myVramBytes = headerStats.MyVramBytes;
        long myTriangles = headerStats.MyTriangles;
        bool hasAnalysis = headerStats.HasAnalysis;

        var eligibleSet = snapshot.EligibleTextures;
        var eligibleMeshSet = snapshot.EligibleMeshes;
        var (estSaved, estCount, _) = snapshot.TextureEstimate;
        long estimatedMeshTriangles = snapshot.EstimatedMeshTriangles;
        int estimatedMeshFiles = snapshot.EstimatedMeshFiles;

        const long MiB = 1024L * 1024L;
        static long RoundToWholeMiBBytes(long bytes)
        {
            if (bytes <= 0) return 0;
            return (long)(Math.Round(bytes / (double)MiB) * MiB);
        }

        estSaved = RoundToWholeMiBBytes(estSaved);
        var analysis = hasAnalysis ? _characterAnalyzer.LastAnalysis : null;

        bool isSnapshotBuilding = _textureEstimateState == ReduceEstimateState.Building;
        bool isTextureEstimateBusy = isSnapshotBuilding
            || _textureCleanupEstimateRefreshPending
            || (_textureCleanupEstimateTask != null && !_textureCleanupEstimateTask.IsCompleted);
        bool isTextureEstimateBuilding = isTextureEstimateBusy && estSaved <= 0 && estCount <= 0;
        bool isMeshEstimateBusy = _meshEstimateQueueBuildPending
            || (_meshEstimateQueueBuildTask != null && !_meshEstimateQueueBuildTask.IsCompleted)
            || _meshOptimizationService.PendingEstimateCount > 0;
        bool isMeshEstimateBuilding = isMeshEstimateBusy
            || (isSnapshotBuilding && estimatedMeshTriangles <= 0 && estimatedMeshFiles <= 0);
        bool hasTextureSavings = ((_textureEstimateState == ReduceEstimateState.Ready) || isTextureEstimateBusy) && estSaved > 0 && estCount > 0;
        bool hasStrictMeshSavings = estimatedMeshTriangles > 0 && estimatedMeshFiles > 0;
        bool hasEligibleMeshTargets = snapshot.HasEligibleMeshes && eligibleMeshSet is { Count: > 0 };
        bool hasMeshSavings = ((_textureEstimateState == ReduceEstimateState.Ready) || isMeshEstimateBusy || isSnapshotBuilding) && hasStrictMeshSavings;

        var reduceVramLabel = BuildReduceVramButtonLabel(estSaved, isTextureEstimateBuilding);
        var reduceTrisLabel = BuildReduceTrisButtonLabel(estimatedMeshTriangles, isMeshEstimateBuilding);

        bool canClickVram = hasTextureSavings && (_bc7Task == null || _bc7Task.IsCompleted);
        bool canClickTris = hasMeshSavings && (_bc7Task == null || _bc7Task.IsCompleted);

        long pairsVramBytes = headerStats.PairsVramBytes;
        long shellsVramBytes = headerStats.ShellsVramBytes;
        long totalVramBytes = headerStats.TotalVramBytes;

        myVramBytes = RoundToWholeMiBBytes(myVramBytes);
        pairsVramBytes = RoundToWholeMiBBytes(pairsVramBytes);
        shellsVramBytes = RoundToWholeMiBBytes(shellsVramBytes);
        totalVramBytes = RoundToWholeMiBBytes(totalVramBytes);

        Vector4 shellsColor;
        if (_playerPerformanceConfigService.Current.SyncshellVramCapMiB > 0)
        {
            var shellsCapBytes = (long)_playerPerformanceConfigService.Current.SyncshellVramCapMiB * 1024 * 1024;
            float usage = shellsCapBytes > 0 ? (float)shellsVramBytes / shellsCapBytes : 0f;

            if (usage < 0.5f) shellsColor = ImGuiColors.HealerGreen;
            else if (usage < 0.8f) shellsColor = ImGuiColors.TankBlue;
            else if (usage < 1f) shellsColor = ImGuiColors.DalamudYellow;
            else shellsColor = ImGuiColors.DPSRed;
        }
        else
        {
            shellsColor = ImGuiColors.DalamudWhite;
        }

        {
            var dl = ImGui.GetWindowDrawList();
            var panelStart = ImGui.GetCursorScreenPos();

            ImGui.BeginGroup();

            var name = isConnected ? (_apiController.DisplayName ?? string.Empty) : _uiSharedService.L("UI.CompactUI.OfflineHeader", "OFFLINE");
            var uid = _apiController.UID ?? string.Empty;

            bool needsVanity = isConnected && !string.IsNullOrEmpty(uid) && string.Equals(name, uid, StringComparison.Ordinal);

            float nameH;
            using (_uiSharedService.UidFont.Push())
                nameH = ImGui.GetTextLineHeight();

            float uidH = string.IsNullOrEmpty(uid) ? 0f : ImGui.CalcTextSize(uid).Y;
            float firstLineH = needsVanity ? ImGui.GetFrameHeight() : nameH;
            float leftH = firstLineH + (uidH > 0f ? (ImGui.GetStyle().ItemSpacing.Y + uidH) : 0f);

            var vramVal = (analysis is null || analysis.Count == 0)
                ? "—"
                : NoDot00(UiSharedService.ByteToString(myVramBytes, addSuffix: true));

            var triVal = (analysis is null || analysis.Count == 0 || myTriangles <= 0)
                ? "—"
                : FormatTrianglesThousands(myTriangles);

            float labelH = ImGui.GetTextLineHeight();
            float buttonH = ImGui.GetFrameHeight();
            float rowH = MathF.Max(labelH, buttonH);

            float rightTopPad = 1f * scale;
            float rightInnerGap = 2f * scale;
            float rightRowSpacing = ImGui.GetStyle().ItemSpacing.Y;
            float rightH = rightTopPad + rowH + rightRowSpacing + rightInnerGap + rowH + rightRowSpacing;

            float vCenterPad = MathF.Max(0f, (rightH - leftH) * 0.5f);

            Vector2 nameSz;
            using (_uiSharedService.UidFont.Push())
                nameSz = ImGui.CalcTextSize(name);

            var uidSz = ImGui.CalcTextSize(uid);

            float desiredLeft = MathF.Max(nameSz.X, uidSz.X) + 18f * scale;
            float maxLeft = MathF.Max(1f, contentW * 0.36f);
            float leftW = MathF.Min(desiredLeft, maxLeft);

            using (ImRaii.PushStyle(ImGuiStyleVar.CellPadding, new Vector2(1f * scale, 2f * scale)))
            using (var hdr = ImRaii.Table("##hdrMain", 2, ImGuiTableFlags.SizingStretchProp))
            {
                if (hdr)
                {
                    ImGui.TableSetupColumn(_uiSharedService.L("UI.CompactUI.07c342be", "l"), ImGuiTableColumnFlags.WidthFixed, leftW);
                    ImGui.TableSetupColumn(_uiSharedService.L("UI.CompactUI.4dc7c9ec", "r"), ImGuiTableColumnFlags.WidthStretch);

                    ImGui.TableNextRow();
                    ImGui.TableSetColumnIndex(0);
                    ImGui.BeginGroup();

                    if (vCenterPad > 0f)
                        ImGui.Dummy(new Vector2(0, vCenterPad));

                    float colW0 = ImGui.GetColumnWidth();
                    float padXLeft = 4f * scale;

                    if (needsVanity)
                    {
                        const string vanityLabel = "Set Vanity";
                        var style = ImGui.GetStyle();
                        var txt = ImGui.CalcTextSize(vanityLabel);
                        float vanityW = txt.X + (style.FramePadding.X * 2f) + (22f * scale);

                        float innerW = MathF.Max(0f, colW0 - (padXLeft * 2f));
                        float useW = MathF.Min(vanityW, innerW);

                        var cur = ImGui.GetCursorScreenPos();
                        float x = cur.X + padXLeft + MathF.Max(0f, (innerW - useW) * 0.5f);
                        SetCursorScreenPosX(x);

                        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Users, vanityLabel, useW))
                            Mediator.Publish(new UiToggleMessage(typeof(VanityUi)));

                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.CompactUI.fac312eb", "Setup Vanity (custom ID) here! It's free!"));
                    }
                    else
                    {
                        bool clickedName;
                        using (_uiSharedService.UidFont.Push())
                        {
                            clickedName = DrawCenteredTextAutoFit(
                                "##name_line",
                                name,
                                ImGuiColors.DalamudViolet,
                                colW0,
                                padXLeft,
                                minScale: 0.70f);
                        }

                        if (clickedName)
                            ImGui.SetClipboardText(name);

                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.CompactUI.8ef07790", "Click to copy"));
                    }

                    if (!string.IsNullOrEmpty(uid))
                    {
                        bool clickedUid = DrawCenteredTextAutoFit(
                            "##uid_line",
                            uid,
                            ImGuiColors.ParsedBlue,
                            colW0,
                            padXLeft,
                            minScale: 0.80f);

                        if (clickedUid)
                            ImGui.SetClipboardText(uid);

                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.CompactUI.8ef07790", "Click to copy"));
                    }

                    ImGui.EndGroup();
                    ImGui.TableSetColumnIndex(1);
                    ImGui.BeginGroup();

                    ImGui.Dummy(new Vector2(0, 1f * scale));
                    float colW = ImGui.GetColumnWidth();
                    float rowLeftPad = 6f * scale;
                    float rowRightPad = 8f * scale;
                    float innerWR = MathF.Max(0f, colW - rowLeftPad - rowRightPad);

                    string FitRowLabel(string label, float width)
                    {
                        if (width <= 0f)
                            return label;

                        var fitted = label;
                        while (fitted.Length > 1 && ImGui.CalcTextSize(fitted).X > width)
                        {
                            fitted = fitted[..^1];
                        }

                        if (fitted.Length < label.Length && fitted.Length > 1)
                            fitted = fitted[..Math.Max(1, fitted.Length - 1)] + "…";

                        return fitted;
                    }

                    bool IconTextButtonAutoFit(string id, FontAwesomeIcon icon, string text, float width, float minTextScale = 0.84f)
                    {
                        var style = ImGui.GetStyle();
                        float h = ImGui.GetFrameHeight();

                        ImGui.PushID(id);
                        bool pressed = ImGui.Button(string.Empty, new Vector2(width, h));

                        var dl = ImGui.GetWindowDrawList();
                        var rMin = ImGui.GetItemRectMin();
                        var rMax = ImGui.GetItemRectMax();
                        dl.PushClipRect(rMin, rMax, true);

                        float gap = 3f * ImGuiHelpers.GlobalScale;
                        string iconStr = icon.ToIconString();
                        Vector2 iconSz;
                        using (_uiSharedService.IconFont.Push())
                            iconSz = ImGui.CalcTextSize(iconStr);

                        float padX = style.FramePadding.X;
                        float padW = padX * 2f;
                        float baseTextW = ImGui.CalcTextSize(text).X;
                        float maxTextW = MathF.Max(0f, width - (iconSz.X + gap + padW));
                        float tScale = (baseTextW > 0f && maxTextW > 0f) ? MathF.Min(1f, maxTextW / baseTextW) : 1f;
                        tScale = MathF.Max(minTextScale, tScale);

                        var font = ImGui.GetFont();
                        float textFontSize = ImGui.GetFontSize() * tScale;
                        float scaledTextW = baseTextW * tScale;
                        float groupW = iconSz.X + gap + scaledTextW;
                        float groupStartX = rMin.X + MathF.Max(padX, (width - groupW) * 0.5f);
                        float iconFontSize = ImGui.GetFontSize();
                        float iconX = groupStartX;
                        float iconY = rMin.Y + (h - iconFontSize) * 0.5f;

                        using (_uiSharedService.IconFont.Push())
                            dl.AddText(new Vector2(iconX, iconY), ImGui.GetColorU32(ImGuiCol.Text), iconStr);

                        float textX = iconX + iconSz.X + gap;
                        float textY = rMin.Y + (h - textFontSize) * 0.5f;
                        dl.AddText(font, textFontSize, new Vector2(textX, textY), ImGui.GetColorU32(ImGuiCol.Text), text);

                        dl.PopClipRect();
                        ImGui.PopID();
                        return pressed;
                    }

                    static void SetCursorScreenPosX(float targetScreenX)
                    {
                        var curScreen = ImGui.GetCursorScreenPos();
                        var curLocalX = ImGui.GetCursorPosX();
                        var delta = targetScreenX - curScreen.X;
                        ImGui.SetCursorPosX(curLocalX + delta);
                    }

                    void DrawReduceRow(string rowId, string statLabel, string buttonLabel, bool canClick, Action onClick, Action drawTooltip)
                    {
                        var style = ImGui.GetStyle();
                        float rowGap = 6f * scale;
                        float textWidth = ImGui.CalcTextSize(buttonLabel).X;
                        float iconWidth;
                        using (_uiSharedService.IconFont.Push())
                            iconWidth = ImGui.CalcTextSize(FontAwesomeIcon.Recycle.ToIconString()).X;

                        float desiredButtonWidth = textWidth + iconWidth + (style.FramePadding.X * 2f) + (12f * scale);
                        float buttonWidth = MathF.Min(164f * scale, MathF.Max(90f * scale, MathF.Max(desiredButtonWidth, innerWR * 0.38f)));
                        float labelWidth = MathF.Max(0f, innerWR - buttonWidth - rowGap);

                        var rowStart = ImGui.GetCursorScreenPos();
                        float buttonX = rowStart.X + MathF.Max(0f, innerWR - buttonWidth);

                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(FitRowLabel(statLabel, labelWidth));

                        ImGui.SetCursorScreenPos(new Vector2(buttonX, rowStart.Y));
                        using (ImRaii.Disabled(!canClick))
                        {
                            if (IconTextButtonAutoFit(rowId, FontAwesomeIcon.Recycle, buttonLabel, buttonWidth))
                                onClick();
                        }

                        if (ImGui.IsItemHovered())
                            drawTooltip();

                        if (ImGui.IsItemClicked(ImGuiMouseButton.Right))
                        {
                            if (string.Equals(rowId, "reduce_vram_row", StringComparison.Ordinal))
                                Mediator.Publish(new OpenDataAnalysisOptimisationTabMessage(DataAnalysisOptimisationTab.Textures));
                            else if (string.Equals(rowId, "reduce_tris_row", StringComparison.Ordinal))
                                Mediator.Publish(new OpenDataAnalysisOptimisationTabMessage(DataAnalysisOptimisationTab.Meshes));
                        }

                        float nextRowY = rowStart.Y + MathF.Max(ImGui.GetTextLineHeight(), ImGui.GetFrameHeight()) + style.ItemSpacing.Y;
                        ImGui.SetCursorScreenPos(new Vector2(rowStart.X, nextRowY));
                    }

                    ImGui.PushStyleVar(ImGuiStyleVar.FrameRounding, 8f * scale);
                    ImGui.PushStyleVar(ImGuiStyleVar.FramePadding, new Vector2(10f, 6f) * scale);

                    var rowsStart = ImGui.GetCursorScreenPos();
                    SetCursorScreenPosX(rowsStart.X + rowLeftPad);
                    DrawReduceRow(
                        "reduce_vram_row",
                        $"VRAM: {vramVal}",
                        reduceVramLabel,
                        canClickVram,
                        () => BeginReduceVram(eligibleSet!, snapshot.TextureTargets),
                        () => DrawReduceVramTooltip(analysis, myVramBytes, estSaved, estCount, isTextureEstimateBuilding));

                    ImGui.Dummy(new Vector2(0, 2f * scale));
                    SetCursorScreenPosX(rowsStart.X + rowLeftPad);
                    DrawReduceRow(
                        "reduce_tris_row",
                        $"Tris: {triVal}",
                        reduceTrisLabel,
                        canClickTris,
                        () => BeginReduceTris(eligibleMeshSet!),
                        () => DrawReduceTrisTooltip(analysis, myTriangles, estimatedMeshTriangles, estimatedMeshFiles, isMeshEstimateBuilding, hasEligibleMeshTargets));

                    ImGui.PopStyleVar(2);

                    ImGui.EndGroup();
                }
            }

            ImGui.EndGroup();

            var panelEnd = ImGui.GetCursorScreenPos();
            var w = contentW;
            var h = MathF.Max(0f, panelEnd.Y - panelStart.Y);

            var bg = ImGui.GetColorU32(new Vector4(0.40f, 0.20f, 0.60f, 0.10f));
            var border = ImGui.GetColorU32(new Vector4(0.60f, 0.35f, 0.90f, 0.18f));

            dl.AddRectFilled(panelStart, panelStart + new Vector2(w, h), bg, 10f * scale);
            dl.AddRect(panelStart, panelStart + new Vector2(w, h), border, 10f * scale);
            ImGui.Dummy(new Vector2(0, 0.5f * scale));
        }

        if (!isConnected)
        {
            var statusText = GetUidText();
            var detailText = GetServerError();
            var statusColor = GetUidColor();

            var statusSize = ImGui.CalcTextSize(statusText);
            ImGui.SetCursorPosX(MathF.Max(0f, (contentW - statusSize.X) * 0.5f));
            ImGui.PushStyleColor(ImGuiCol.Text, statusColor);
            ImGui.TextUnformatted(statusText);
            ImGui.PopStyleColor();

            if (!string.IsNullOrEmpty(detailText) && !string.Equals(detailText, statusText, StringComparison.Ordinal))
            {
                float wrapWidth = MathF.Max(120f * scale, contentW - (24f * scale));
                var words = detailText.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var wrappedLines = new List<string>();
                var currentLine = string.Empty;

                foreach (var word in words)
                {
                    var candidate = string.IsNullOrEmpty(currentLine) ? word : currentLine + " " + word;
                    if (ImGui.CalcTextSize(candidate).X <= wrapWidth || string.IsNullOrEmpty(currentLine))
                    {
                        currentLine = candidate;
                    }
                    else
                    {
                        wrappedLines.Add(currentLine);
                        currentLine = word;
                    }
                }

                if (!string.IsNullOrEmpty(currentLine))
                    wrappedLines.Add(currentLine);

                ImGui.PushStyleColor(ImGuiCol.Text, statusColor);

                foreach (var line in wrappedLines)
                {
                    var lineSize = ImGui.CalcTextSize(line);
                    ImGui.SetCursorPosX(MathF.Max(0f, (contentW - lineSize.X) * 0.5f));
                    ImGui.TextUnformatted(line);
                }

                ImGui.PopStyleColor();
            }

            ImGui.Dummy(new Vector2(0, 2f * scale));
            return;
        }

        {
            var dl = ImGui.GetWindowDrawList();
            var start = ImGui.GetCursorScreenPos();

            ImGui.BeginGroup();

            float padX = 12f * scale;
            var title = "VRAM use";
            var titleSz = ImGui.CalcTextSize(title);
            ImGui.SetCursorPosX(MathF.Max(0f, (contentW - titleSz.X) * 0.5f));
            ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey2);
            ImGui.TextUnformatted(title);
            ImGui.PopStyleColor();

            var pairsText = string.Format(
                _uiSharedService.L("UI.CompactUI.e6a5dfed", "Pairs {0}"),
                NoDot00(UiSharedService.ByteToString(pairsVramBytes, addSuffix: true)));

            var shellsText =
                _uiSharedService.L("UI.CompactUI.418f28e3", "Shells ")
                + NoDot00(UiSharedService.ByteToString(shellsVramBytes, addSuffix: true));

            var totalText = "Total " + NoDot00(UiSharedService.ByteToString(totalVramBytes, addSuffix: true));
            var line = $"{pairsText}  |  {shellsText}  |  {totalText}";

            DrawCenteredTextAutoFit(
                "##vram_single_line",
                line,
                ImGuiColors.DalamudWhite,
                regionW: contentW,
                padX: padX,
                minScale: 0.70f);

            ImGui.EndGroup();

            var end = ImGui.GetCursorScreenPos();
            var w = contentW;
            var h = MathF.Max(0f, end.Y - start.Y);

            var bg = ImGui.GetColorU32(new Vector4(0.40f, 0.20f, 0.60f, 0.10f));
            var border = ImGui.GetColorU32(new Vector4(0.60f, 0.35f, 0.90f, 0.18f));

            dl.AddRectFilled(start, start + new Vector2(w, h), bg, 10f * scale);
            dl.AddRect(start, start + new Vector2(w, h), border, 10f * scale);
        }

        ImGui.Separator();
        DrawScopeToggles();
    }

    private IEnumerable<IDrawFolder> GetDrawFolders()
    {
        List<IDrawFolder> drawFolders = [];

        bool isDirectTab = _currentPairViewTab == PairViewTab.DirectPairs;
        bool isShellTab = _currentPairViewTab == PairViewTab.Shells;

        var allPairsWithGroups = _pairManager.PairsWithGroups;

        var allPairs = allPairsWithGroups
            .Where(entry =>
            {
                return _currentPairViewTab switch
                {
                    PairViewTab.DirectPairs => entry.Key.IsDirectlyPaired,
                    PairViewTab.Shells => !entry.Key.IsDirectlyPaired,
                    _ => true
                };
            })
            .ToDictionary(k => k.Key, k => k.Value);

        var hasSearch = !string.IsNullOrWhiteSpace(_pairSearch);
        var term = hasSearch ? _pairSearch.Trim() : string.Empty;

        var filteredPairs = hasSearch
            ? allPairs
                .Where(p =>
                    p.Key.UserData.AliasOrUID.Contains(term, StringComparison.OrdinalIgnoreCase) ||
                    (p.Key.GetNote()?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false) ||
                    (p.Key.PlayerName?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false))
                .ToDictionary(k => k.Key, k => k.Value)
            : allPairs;

        string? AlphabeticalSort(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => (_configService.Current.ShowCharacterNameInsteadOfNotesForVisible && !string.IsNullOrEmpty(u.Key.PlayerName)
                    ? (_configService.Current.PreferNotesOverNamesForVisible ? u.Key.GetNote() : u.Key.PlayerName)
                    : (u.Key.GetNote() ?? u.Key.UserData.AliasOrUID));

        bool FilterThresholdAutoPaused(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => _playerPerformanceService.IsThresholdAutoPaused(u.Key);

        bool FilterThresholdAutoPausedForFolder(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => _playerPerformanceService.IsThresholdAutoPaused(u.Key)
                || _playerPerformanceService.IsRememberedThresholdAutoPaused(u.Key);

        bool FilterNotOtherSync(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => !u.Key.AutoPausedByOtherSync;

        bool FilterNormalPair(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNotOtherSync(u) && !FilterThresholdAutoPaused(u);

        bool FilterOnlineOrPausedSelf(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
                ? ((u.Key.IsOnline || (u.Key.UserPair.OwnPermissions.IsPaused() && !u.Key.AutoPausedByOtherSync)) && FilterNormalPair(u))
                : (u.Key.IsOnline
                   || (!u.Key.IsOnline && !_configService.Current.ShowOfflineUsersSeparately)
                   || (u.Key.UserPair.OwnPermissions.IsPaused() && !u.Key.AutoPausedByOtherSync))
                  && FilterNormalPair(u);

        Dictionary<Pair, List<GroupFullInfoDto>> BasicSortedDictionary(IEnumerable<KeyValuePair<Pair, List<GroupFullInfoDto>>> u)
            => (_configService.Current.SortPairsByVRAM
                    ? u.OrderByDescending(u => System.Math.Max(u.Key.LastAppliedApproximateVRAMBytes, 0))
                        .ThenByDescending(u => u.Key.IsVisible)
                        .ThenByDescending(u => u.Key.IsOnline)
                    : u.OrderByDescending(u => u.Key.IsVisible)
                        .ThenByDescending(u => u.Key.IsOnline))
                .ThenBy(AlphabeticalSort, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(u => u.Key, u => u.Value);

        ImmutableList<Pair> ImmutablePairList(IEnumerable<KeyValuePair<Pair, List<GroupFullInfoDto>>> u)
            => u.Select(k => k.Key).ToImmutableList();

        bool FilterVisibleUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNormalPair(u)
                && u.Key.IsVisible
                && (_configService.Current.ShowSyncshellUsersInVisible || !(!_configService.Current.ShowSyncshellUsersInVisible && !u.Key.IsDirectlyPaired));

        bool FilterTagusers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, string tag)
            => FilterNormalPair(u)
                && u.Key.IsDirectlyPaired
                && !u.Key.IsOneSidedPair
                && _tagHandler.HasTag(u.Key.UserData.UID, tag);

        bool FilterGroupUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, GroupFullInfoDto group)
            => FilterNormalPair(u) && u.Value.Exists(g => string.Equals(g.GID, group.GID, StringComparison.Ordinal));

        bool FilterNotTaggedUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNormalPair(u)
                && u.Key.IsDirectlyPaired
                && !u.Key.IsOneSidedPair
                && !_tagHandler.HasAnyTag(u.Key.UserData.UID);

        bool FilterOfflineUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
                ? (FilterNormalPair(u) && !u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused())
                : ((u.Key.IsDirectlyPaired && _configService.Current.ShowSyncshellOfflineUsersSeparately)
                   || !_configService.Current.ShowSyncshellOfflineUsersSeparately)
                  && (!u.Key.IsOneSidedPair || u.Value.Any())
                  && !u.Key.IsOnline
                  && !u.Key.UserPair.OwnPermissions.IsPaused()
                  && FilterNormalPair(u);

        bool FilterOfflineSyncshellUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNormalPair(u)
                && (!u.Key.IsDirectlyPaired && !u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused());
        Func<KeyValuePair<Pair, List<GroupFullInfoDto>>, bool> FilterOtherSyncAny =
            u => u.Key.AutoPausedByOtherSync;

        Func<KeyValuePair<Pair, List<GroupFullInfoDto>>, bool> FilterOtherSyncLightless =
            u => u.Key.AutoPausedByOtherSync
                 && string.Equals(u.Key.AutoPausedByOtherSyncName, "Lightless", StringComparison.OrdinalIgnoreCase);

        Func<KeyValuePair<Pair, List<GroupFullInfoDto>>, bool> FilterOtherSyncSnowcloak =
            u => u.Key.AutoPausedByOtherSync
                 && string.Equals(u.Key.AutoPausedByOtherSyncName, "Snowcloak", StringComparison.OrdinalIgnoreCase);

        Func<KeyValuePair<Pair, List<GroupFullInfoDto>>, bool> FilterOtherSyncOther =
            u => u.Key.AutoPausedByOtherSync
                 && !string.Equals(u.Key.AutoPausedByOtherSyncName, "Lightless", StringComparison.OrdinalIgnoreCase)
                 && !string.Equals(u.Key.AutoPausedByOtherSyncName, "Snowcloak", StringComparison.OrdinalIgnoreCase);

        if (_currentPairViewTab == PairViewTab.Visible)
        {
            var allVisiblePairs = ImmutablePairList(allPairs.Where(FilterVisibleUsers));
            var filteredVisiblePairs = BasicSortedDictionary(filteredPairs.Where(FilterVisibleUsers));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                TagHandler.CustomVisibleTag,
                filteredVisiblePairs,
                allVisiblePairs));
            if (allPairs.Any(FilterOtherSyncAny))
            {
                var allOtherSyncOtherPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncOther));
                var filteredOtherSyncOtherPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncOther));
                var otherFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncTag, filteredOtherSyncOtherPairs, allOtherSyncOtherPairs);

                var allOtherSyncLightlessPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncLightless));
                var filteredOtherSyncLightlessPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncLightless));
                var lightlessFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncLightlessTag, filteredOtherSyncLightlessPairs, allOtherSyncLightlessPairs);

                var allOtherSyncSnowcloakPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncSnowcloak));
                var filteredOtherSyncSnowcloakPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncSnowcloak));
                var snowcloakFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncSnowcloakTag, filteredOtherSyncSnowcloakPairs, allOtherSyncSnowcloakPairs);

                drawFolders.Add(new DrawGroupedOtherSyncFolder(
                    TagHandler.CustomOtherSyncRootTag,
                    _uiSharedService.L("UI.DrawFolderTag.Name.OtherSync", "Handled by other sync"),
                    new IDrawFolder[] { otherFolder, lightlessFolder, snowcloakFolder },
                    _tagHandler));
            }

            return drawFolders;
        }

        if (_configService.Current.ShowVisibleUsersSeparately)
        {
            var allVisiblePairs = ImmutablePairList(allPairs.Where(FilterVisibleUsers));
            var filteredVisiblePairs = BasicSortedDictionary(filteredPairs.Where(FilterVisibleUsers));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                TagHandler.CustomVisibleTag,
                filteredVisiblePairs,
                allVisiblePairs));
        }

        if (allPairs.Any(FilterOtherSyncAny))
        {
            var allOtherSyncOtherPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncOther));
            var filteredOtherSyncOtherPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncOther));
            var otherFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncTag, filteredOtherSyncOtherPairs, allOtherSyncOtherPairs);

            var allOtherSyncLightlessPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncLightless));
            var filteredOtherSyncLightlessPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncLightless));
            var lightlessFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncLightlessTag, filteredOtherSyncLightlessPairs, allOtherSyncLightlessPairs);

            var allOtherSyncSnowcloakPairs = ImmutablePairList(allPairs.Where(FilterOtherSyncSnowcloak));
            var filteredOtherSyncSnowcloakPairs = BasicSortedDictionary(filteredPairs.Where(FilterOtherSyncSnowcloak));
            var snowcloakFolder = _drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOtherSyncSnowcloakTag, filteredOtherSyncSnowcloakPairs, allOtherSyncSnowcloakPairs);

            drawFolders.Add(new DrawGroupedOtherSyncFolder(
                TagHandler.CustomOtherSyncRootTag,
                _uiSharedService.L("UI.DrawFolderTag.Name.OtherSync", "Handled by other sync"),
                new IDrawFolder[] { otherFolder, lightlessFolder, snowcloakFolder },
                _tagHandler));
        }

        if (isDirectTab)
        {
            var allDirectTabAutoPausedPairs = allPairs
                .Where(FilterThresholdAutoPausedForFolder)
                .ToDictionary(k => k.Key, k => k.Value);

            if (allDirectTabAutoPausedPairs.Count > 0)
            {
                var filteredDirectTabAutoPausedPairs = hasSearch
                    ? allDirectTabAutoPausedPairs
                        .Where(p =>
                            p.Key.UserData.AliasOrUID.Contains(term, StringComparison.OrdinalIgnoreCase) ||
                            (p.Key.GetNote()?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false) ||
                            (p.Key.PlayerName?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false))
                        .ToDictionary(k => k.Key, k => k.Value)
                    : allDirectTabAutoPausedPairs;

                var allAutoPausedPairs = ImmutablePairList(allDirectTabAutoPausedPairs);
                var filteredAutoPausedPairs = BasicSortedDictionary(filteredDirectTabAutoPausedPairs);
                drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                    TagHandler.CustomAutoPausedTag,
                    filteredAutoPausedPairs,
                    allAutoPausedPairs));
            }
        }


        if (isShellTab)
        {
            var allShellTabAutoPausedPairs = allPairsWithGroups
                .Where(FilterThresholdAutoPausedForFolder)
                .ToDictionary(k => k.Key, k => k.Value);

            if (allShellTabAutoPausedPairs.Count > 0)
            {
                var filteredShellTabAutoPausedPairs = hasSearch
                    ? allShellTabAutoPausedPairs
                        .Where(p =>
                            p.Key.UserData.AliasOrUID.Contains(term, StringComparison.OrdinalIgnoreCase) ||
                            (p.Key.GetNote()?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false) ||
                            (p.Key.PlayerName?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false))
                        .ToDictionary(k => k.Key, k => k.Value)
                    : allShellTabAutoPausedPairs;

                var allAutoPausedPairs = ImmutablePairList(allShellTabAutoPausedPairs);
                var filteredAutoPausedPairs = BasicSortedDictionary(filteredShellTabAutoPausedPairs);
                drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                    TagHandler.CustomAutoPausedTag,
                    filteredAutoPausedPairs,
                    allAutoPausedPairs));
            }
        }

        if (!isDirectTab)
        {
            List<IDrawFolder> groupFolders = new();
            foreach (var group in _pairManager.GroupPairs.Select(g => g.Key).OrderBy(g => g.GroupAliasOrGID, StringComparer.OrdinalIgnoreCase))
            {
                var allGroupPairs = ImmutablePairList(allPairs
                    .Where(u => FilterGroupUsers(u, group)));

                var filteredGroupPairs = filteredPairs
                    .Where(u => FilterGroupUsers(u, group) && FilterOnlineOrPausedSelf(u))
                    .OrderByDescending(u => u.Key.IsOnline)
                    .ThenBy(u =>
                    {
                        if (string.Equals(u.Key.UserData.UID, group.OwnerUID, StringComparison.Ordinal)) return 0;
                        if (group.GroupPairUserInfos.TryGetValue(u.Key.UserData.UID, out var info))
                        {
                            if (info.IsModerator()) return 1;
                            if (info.IsPinned()) return 2;
                        }
                        return u.Key.IsVisible ? 3 : 4;
                    })
                    .ThenBy(AlphabeticalSort, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(k => k.Key, k => k.Value);

                groupFolders.Add(_drawEntityFactory.CreateDrawGroupFolder(group, filteredGroupPairs, allGroupPairs));
            }

            if (_configService.Current.GroupUpSyncshells)
                drawFolders.Add(new DrawGroupedGroupFolder(groupFolders, _tagHandler, _uiSharedService));
            else
                drawFolders.AddRange(groupFolders);
        }

        var tags = _tagHandler.GetAllTagsSorted();
        foreach (var tag in tags)
        {
            var allTagPairs = ImmutablePairList(allPairs
                .Where(u => FilterTagusers(u, tag)));
            var filteredTagPairs = BasicSortedDictionary(filteredPairs
                .Where(u => FilterTagusers(u, tag) && FilterOnlineOrPausedSelf(u)));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(tag, filteredTagPairs, allTagPairs));
        }

        var allOnlineNotTaggedPairs = ImmutablePairList(allPairs
            .Where(FilterNotTaggedUsers));
        var onlineNotTaggedPairs = BasicSortedDictionary(filteredPairs
            .Where(u => FilterNotTaggedUsers(u) && FilterOnlineOrPausedSelf(u)));

        drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder((_configService.Current.ShowOfflineUsersSeparately ? TagHandler.CustomOnlineTag : TagHandler.CustomAllTag),
            onlineNotTaggedPairs, allOnlineNotTaggedPairs));

        if (_configService.Current.ShowOfflineUsersSeparately)
        {
            var allOfflinePairs = ImmutablePairList(allPairs
                .Where(FilterOfflineUsers));
            var filteredOfflinePairs = BasicSortedDictionary(filteredPairs
                .Where(FilterOfflineUsers));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOfflineTag, filteredOfflinePairs, allOfflinePairs));
            if (_configService.Current.ShowSyncshellOfflineUsersSeparately && !isDirectTab)
            {
                var allOfflineSyncshellUsers = ImmutablePairList(allPairs
                    .Where(FilterOfflineSyncshellUsers));
                var filteredOfflineSyncshellUsers = BasicSortedDictionary(filteredPairs
                    .Where(FilterOfflineSyncshellUsers));

                drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomOfflineSyncshellTag,
                    filteredOfflineSyncshellUsers,
                    allOfflineSyncshellUsers));
            }
        }

        drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(TagHandler.CustomUnpairedTag,
            BasicSortedDictionary(filteredPairs.Where(u => u.Key.IsOneSidedPair)),
            ImmutablePairList(allPairs.Where(u => u.Key.IsOneSidedPair))));

        return drawFolders;
    }

    private string GetServerError()
    {
        return _apiController.ServerState switch
        {
            ServerState.Connecting => "Attempting to connect to the server.",
            ServerState.Reconnecting => "Connection to server interrupted, attempting to reconnect to the server.",
            ServerState.Disconnected => "You are currently disconnected from the RavaSync server.",
            ServerState.Disconnecting => "Disconnecting from the server",
            ServerState.Unauthorized => "Server Response: " + _apiController.AuthFailureMessage,
            ServerState.Offline => "Your selected RavaSync server is currently offline.",
            ServerState.VersionMisMatch =>
                "Your plugin or the server you are connecting to is out of date. Please update your plugin now. If you already did so, contact the server provider to update their server to the latest version.",
            ServerState.RateLimited => "You are rate limited for (re)connecting too often. Disconnect, wait 10 minutes and try again.",
            ServerState.Connected => string.Empty,
            ServerState.NoSecretKey => "You have no secret key set for this current character. Open Settings -> Service Settings and set a secret key for the current character. You can reuse the same secret key for multiple characters.",
            ServerState.MultiChara => "Your Character Configuration has multiple characters configured with same name and world. You will not be able to connect until you fix this issue. Remove the duplicates from the configuration in Settings -> Service Settings -> Character Management and reconnect manually after.",
            ServerState.OAuthMisconfigured => "OAuth2 is enabled...If you see this, go to C:\\Users\\YourName\\AppData\\Roaming\\XIVLauncher\\pluginConfigs\\RavaSync, open server.Json and set UseOAuth2 to false.",
            ServerState.OAuthLoginTokenStale => "Your OAuth2 login token is stale and cannot be used to renew. Go to the Settings -> Service Settings and unlink then relink your OAuth2 configuration.",
            ServerState.NoAutoLogon => "This character has automatic login into RavaSync disabled. Press the connect button to connect to RavaSync.",
            _ => string.Empty
        };
    }

    private Vector4 GetUidColor()
    {
        return _apiController.ServerState switch
        {
            ServerState.Connecting => ImGuiColors.DalamudYellow,
            ServerState.Reconnecting => ImGuiColors.DalamudRed,
            ServerState.Connected => ImGuiColors.ParsedGreen,
            ServerState.Disconnected => ImGuiColors.DalamudYellow,
            ServerState.Disconnecting => ImGuiColors.DalamudYellow,
            ServerState.Unauthorized => ImGuiColors.DalamudRed,
            ServerState.VersionMisMatch => ImGuiColors.DalamudRed,
            ServerState.Offline => ImGuiColors.DalamudRed,
            ServerState.RateLimited => ImGuiColors.DalamudYellow,
            ServerState.NoSecretKey => ImGuiColors.DalamudYellow,
            ServerState.MultiChara => ImGuiColors.DalamudYellow,
            ServerState.OAuthMisconfigured => ImGuiColors.DalamudRed,
            ServerState.OAuthLoginTokenStale => ImGuiColors.DalamudRed,
            ServerState.NoAutoLogon => ImGuiColors.DalamudYellow,
            _ => ImGuiColors.DalamudRed
        };
    }

    private string GetUidText()
    {
        return _apiController.ServerState switch
        {
            ServerState.Reconnecting => "Reconnecting",
            ServerState.Connecting => "Connecting",
            ServerState.Disconnected => "Disconnected",
            ServerState.Disconnecting => "Disconnecting",
            ServerState.Unauthorized => "Unauthorized",
            ServerState.VersionMisMatch => "Version mismatch",
            ServerState.Offline => "Unavailable",
            ServerState.RateLimited => "Rate Limited",
            ServerState.NoSecretKey => "No Secret Key",
            ServerState.MultiChara => "Duplicate Characters",
            ServerState.OAuthMisconfigured => "Misconfigured OAuth2",
            ServerState.OAuthLoginTokenStale => "Stale OAuth2",
            ServerState.NoAutoLogon => "Auto Login disabled",
            ServerState.Connected => _apiController.DisplayName,
            _ => string.Empty
        };
    }

    private void UiSharedService_GposeEnd()
    {
        _suppressMinimizedRestoreIcon = false;
        IsOpen = _wasOpen;
    }

    private void UiSharedService_GposeStart()
    {
        _suppressMinimizedRestoreIcon = true;
        _wasOpen = IsOpen;
        IsOpen = false;
    }

    private readonly Progress<(string fileName, int index)> _bc7Progress = new();
    private CancellationTokenSource _bc7Cts = new();
    private Task? _bc7Task;
    private string _bc7CurFile = string.Empty;
    private int _bc7CurIndex = 0;
    private int _bc7Total = 0;
    private bool _bc7ShowModal = false;
    private bool _bc7ModalOpen = false;
    private readonly Dictionary<string, string[]> _bc7Set = new(StringComparer.Ordinal);

    private sealed record TextureCleanupEstimateCandidate(string PrimaryPath, string HintPath, string Format, long SourceSize, string[] RelatedModels);

    private sealed record ReduceMySizeSnapshot
    {
        public bool HasEligibleTextures { get; init; }
        public bool HasEligibleMeshes { get; init; }
        public Dictionary<string, string[]> EligibleTextures { get; init; } = new(StringComparer.Ordinal);
        public Dictionary<string, string> TextureTargets { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, string[]> MeshEstimateCandidates { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, string[]> EligibleMeshes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, long> EligibleMeshTriangles { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        public List<(string fmt, long size)> TextureItems { get; init; } = new();
        public Dictionary<string, TextureCleanupEstimateCandidate> PendingTextureCleanupCandidates { get; init; } = new(StringComparer.Ordinal);
        public (long saved, int count, Dictionary<string, (int files, long before, long after)> byFmt) TextureEstimate { get; init; }
        public long EstimatedMeshTriangles { get; init; }
        public int EstimatedMeshFiles { get; init; }
    }

    private enum ReduceEstimateState
    {
        Empty,
        Building,
        Ready,
    }

    private ReduceMySizeSnapshot _reduceMySizeSnapshot = new();
    private Task? _snapshotBuildTask;
    private Task? _meshEstimateSummaryTask;
    private Task? _textureCleanupEstimateTask;
    private Task? _meshEstimateQueueBuildTask;
    private readonly object _textureCleanupEstimateCacheLock = new();
    private readonly Dictionary<string, long> _textureCleanupEstimateCache = new(StringComparer.Ordinal);
    private int _snapshotBuildVersion;
    private int _meshEstimateQueueBuildVersion;
    private volatile bool _snapshotBuildPending;
    private volatile bool _meshEstimateSummaryRefreshPending;
    private volatile bool _textureCleanupEstimateRefreshPending;
    private volatile bool _meshEstimateQueueBuildPending;
    private long _nextSnapshotBuildUtcTicks;
    private long _nextMeshEstimateRefreshUtcTicks;
    private long _nextTextureCleanupEstimateUtcTicks;
    private long _nextMeshEstimateQueueBuildUtcTicks;
    private long _nextReduceMySizePumpUtcTicks;
    private volatile ReduceEstimateState _textureEstimateState = ReduceEstimateState.Empty;
    private long _lastSeenMeshEstimateGeneration = -1;

    private static DownloadStatusAggregate SummarizeDownloadStatus(Dictionary<string, FileDownloadStatus>? status)
    {
        if (status is null || status.Count == 0)
            return default;

        long totalBytes = 0;
        long transferredBytes = 0;
        int totalFiles = 0;
        int transferredFiles = 0;

        foreach (var entry in status.Values)
        {
            if (entry == null)
                continue;

            totalBytes += Math.Max(0, entry.TotalBytes);
            transferredBytes += Math.Max(0, entry.TransferredBytes);
            totalFiles += Math.Max(0, entry.TotalFiles);
            transferredFiles += Math.Max(0, entry.TransferredFiles);
        }

        return new DownloadStatusAggregate(totalBytes, transferredBytes, totalFiles, transferredFiles);
    }

    private void ScheduleDrawFoldersRefresh(bool immediate = false)
    {
        _drawFoldersRefreshPending = true;
        if (!IsOpen)
            _drawFoldersRefreshDeferredUntilVisible = true;

        int delayMs = immediate ? 0 : 90;
        Interlocked.Exchange(ref _nextDrawFoldersRefreshUtcTicks, DateTime.UtcNow.AddMilliseconds(delayMs).Ticks);
    }

    private void RefreshDrawFoldersIfNeeded()
    {
        if (!_drawFoldersRefreshPending)
            return;

        long dueTicks = Interlocked.Read(ref _nextDrawFoldersRefreshUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        _drawFolders = GetDrawFolders().ToList();
        _drawFoldersRefreshPending = false;
        _drawFoldersRefreshDeferredUntilVisible = false;
    }

    private void ScheduleHeaderStatsRefresh(bool immediate = false)
    {
        _headerStatsRefreshPending = true;
        int delayMs = immediate ? 0 : 150;
        Interlocked.Exchange(ref _nextHeaderStatsRefreshUtcTicks, DateTime.UtcNow.AddMilliseconds(delayMs).Ticks);
    }

    private void RefreshHeaderStatsIfNeeded()
    {
        if (!_headerStatsRefreshPending)
            return;

        long dueTicks = Interlocked.Read(ref _nextHeaderStatsRefreshUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        var liveStats = GetLiveHeaderStats();
        long pairsVramBytes = 0;
        long shellsVramBytes = 0;

        Pair[] pairsSnapshot;
        try
        {
            pairsSnapshot = _pairManager.PairsWithGroups.Keys.ToArray();
        }
        catch
        {
            pairsSnapshot = Array.Empty<Pair>();
        }

        foreach (var pair in pairsSnapshot)
        {
            if (!pair.IsVisible || pair.UserPair.OwnPermissions.IsPaused() || pair.AutoPausedByCap)
                continue;

            var bytes = Math.Max(0, pair.LastAppliedApproximateVRAMBytes);
            if (pair.IsDirectlyPaired)
                pairsVramBytes += bytes;
            else
                shellsVramBytes += bytes;
        }

        _headerStatsSnapshot = new HeaderStatsSnapshot(
            liveStats.vramBytes,
            liveStats.triangles,
            liveStats.hasAnalysis,
            pairsVramBytes,
            shellsVramBytes,
            pairsVramBytes + shellsVramBytes);

        _headerStatsRefreshPending = false;
    }

    private static long TryReadLong(object obj, params string[] names)
    {
        var t = obj.GetType();
        foreach (var n in names)
        {
            var p = t.GetProperty(n, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (p != null)
            {
                var v = p.GetValue(obj);
                if (v is int i) return i;
                if (v is long l) return l;
            }

            var f = t.GetField(n, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (f != null)
            {
                var v = f.GetValue(obj);
                if (v is int i2) return i2;
                if (v is long l2) return l2;
            }
        }
        return 0;
    }

    private static long TryReadTriangleCount(CharacterAnalyzer.FileDataEntry e)
        => TryReadLong(e,
            "TriangleCount", "Triangles", "TriCount", "Tris",
            "NumTriangles", "TriangleCnt");

    private (long vramBytes, long triangles, bool hasAnalysis) GetLiveHeaderStats()
    {
        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis is null || analysis.Count == 0)
            return (0, 0, false);

        long myVramBytes = 0;
        long myTriangles = 0;
        var seenHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        KeyValuePair<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>[] snapshot;
        try
        {
            snapshot = analysis.ToArray();
        }
        catch
        {
            return (0, 0, false);
        }

        foreach (var kv in snapshot)
        {
            CharacterAnalyzer.FileDataEntry[] entries;
            try
            {
                entries = kv.Value.Values.ToArray();
            }
            catch
            {
                continue;
            }

            foreach (var e in entries)
            {
                if (e == null)
                    continue;

                if (!string.IsNullOrEmpty(e.Hash) && seenHashes.Add(e.Hash))
                    myVramBytes += Math.Max(0, e.VramBytes);

                if (!string.IsNullOrEmpty(e.Hash)
                    && (string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                        || (e.GamePaths?.Any(path => path.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false))
                    && seenModelHashes.Add(e.Hash))
                {
                    myTriangles += TryReadTriangleCount(e);
                }
            }
        }

        return (myVramBytes, myTriangles, true);
    }

    private const int ReduceMySizePumpIntervalMs = 100;
    private const int WindowsThreadModeBackgroundBegin = 0x00010000;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetThreadPriority(IntPtr hThread, int nPriority);

    private void OnFrameworkUpdate(IFramework _)
    {
        PumpReduceMySizeBackgroundWork();
    }

    private void PumpReduceMySizeBackgroundWork(bool ignoreThrottle = false)
    {
        if (!ignoreThrottle)
        {
            long nowTicks = DateTime.UtcNow.Ticks;
            long dueTicks = Interlocked.Read(ref _nextReduceMySizePumpUtcTicks);
            if (dueTicks > nowTicks)
                return;

            Interlocked.Exchange(ref _nextReduceMySizePumpUtcTicks, DateTime.UtcNow.AddMilliseconds(ReduceMySizePumpIntervalMs).Ticks);
        }

        RefreshReduceMySizeSnapshotIfNeeded();
    }

    private void RefreshReduceMySizeSnapshotIfNeeded()
    {
        if (_snapshotBuildTask != null && _snapshotBuildTask.IsCompleted)
            _snapshotBuildTask = null;

        if (_meshEstimateSummaryTask != null && _meshEstimateSummaryTask.IsCompleted)
            _meshEstimateSummaryTask = null;

        if (_textureCleanupEstimateTask != null && _textureCleanupEstimateTask.IsCompleted)
            _textureCleanupEstimateTask = null;

        if (_meshEstimateQueueBuildTask != null && _meshEstimateQueueBuildTask.IsCompleted)
            _meshEstimateQueueBuildTask = null;

        TryStartScheduledMeshEstimateQueueBuild();
        TryStartScheduledReduceMySizeSnapshotBuild();
        RefreshMeshEstimateSummaryIfNeeded();
        TryStartScheduledMeshEstimateSummaryRefresh();
        TryStartScheduledTextureCleanupEstimateRefresh();
    }

    private void ScheduleReduceMySizeSnapshotRebuild(bool immediate = false, bool initialBuild = false)
    {
        var analysis = _characterAnalyzer.LastAnalysis;
        Interlocked.Increment(ref _snapshotBuildVersion);

        if (analysis is null || analysis.Count == 0)
        {
            _snapshotBuildPending = false;
            _reduceMySizeSnapshot = new ReduceMySizeSnapshot();
            _textureEstimateState = ReduceEstimateState.Empty;
            _lastSeenMeshEstimateGeneration = -1;
            _meshEstimateSummaryRefreshPending = false;
            _textureCleanupEstimateRefreshPending = false;
            Interlocked.Exchange(ref _nextSnapshotBuildUtcTicks, 0L);
            Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, 0L);
            Interlocked.Exchange(ref _nextTextureCleanupEstimateUtcTicks, 0L);
            return;
        }

        _snapshotBuildPending = true;
        _textureEstimateState = ReduceEstimateState.Building;
        _textureCleanupEstimateRefreshPending = false;
        Interlocked.Exchange(ref _nextTextureCleanupEstimateUtcTicks, 0L);
        int delayMs = initialBuild ? 0 : (immediate ? 0 : 650);
        Interlocked.Exchange(ref _nextSnapshotBuildUtcTicks, DateTime.UtcNow.AddMilliseconds(delayMs).Ticks);
    }

    private void TryStartScheduledReduceMySizeSnapshotBuild()
    {
        if (!_snapshotBuildPending)
            return;

        if (_snapshotBuildTask != null && !_snapshotBuildTask.IsCompleted)
            return;

        long dueTicks = Interlocked.Read(ref _nextSnapshotBuildUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        var analysis = _characterAnalyzer.LastAnalysis;
        var buildVersion = Volatile.Read(ref _snapshotBuildVersion);
        _snapshotBuildPending = false;

        if (analysis is null || analysis.Count == 0)
        {
            _reduceMySizeSnapshot = new ReduceMySizeSnapshot();
            _textureEstimateState = ReduceEstimateState.Empty;
            _meshEstimateSummaryRefreshPending = false;
            _textureCleanupEstimateRefreshPending = false;
            _snapshotBuildTask = null;
            return;
        }

        _textureEstimateState = ReduceEstimateState.Building;
        _snapshotBuildTask = Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundEstimatePriority();
            try
            {
                var snapshot = BuildReduceMySizeSnapshot(analysis);
                if (buildVersion != Volatile.Read(ref _snapshotBuildVersion))
                    return;

                _reduceMySizeSnapshot = snapshot;
                _textureEstimateState = ReduceEstimateState.Ready;
                _lastSeenMeshEstimateGeneration = _meshOptimizationService.EstimateGeneration;
                _meshEstimateSummaryRefreshPending = false;
                _textureCleanupEstimateRefreshPending = snapshot.PendingTextureCleanupCandidates.Count > 0;
                if (_textureCleanupEstimateRefreshPending)
                    Interlocked.Exchange(ref _nextTextureCleanupEstimateUtcTicks, DateTime.UtcNow.AddMilliseconds(450).Ticks);
                else
                    Interlocked.Exchange(ref _nextTextureCleanupEstimateUtcTicks, 0L);
                Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, 0L);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to rebuild reduce-my-size snapshot in the background.");
                if (buildVersion == Volatile.Read(ref _snapshotBuildVersion))
                    _textureEstimateState = ReduceEstimateState.Empty;
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    private void RefreshMeshEstimateSummaryIfNeeded()
    {
        long generation = _meshOptimizationService.EstimateGeneration;
        var snapshot = _reduceMySizeSnapshot;
        bool shouldRefresh = snapshot.EligibleMeshTriangles.Count > 0
            || _meshEstimateQueueBuildPending
            || (_meshEstimateQueueBuildTask != null && !_meshEstimateQueueBuildTask.IsCompleted)
            || _meshOptimizationService.PendingEstimateCount > 0;
        if (!shouldRefresh)
        {
            _lastSeenMeshEstimateGeneration = generation;
            _meshEstimateSummaryRefreshPending = false;
            return;
        }

        if (generation == _lastSeenMeshEstimateGeneration && !_meshEstimateSummaryRefreshPending)
            return;

        _meshEstimateSummaryRefreshPending = true;
        if (generation != _lastSeenMeshEstimateGeneration)
        {
            _lastSeenMeshEstimateGeneration = generation;
            Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, DateTime.UtcNow.AddMilliseconds(GetMeshEstimateRefreshDelayMs()).Ticks);
        }
    }

    private void TryStartScheduledMeshEstimateSummaryRefresh()
    {
        if (!_meshEstimateSummaryRefreshPending)
            return;

        if (_snapshotBuildPending || (_snapshotBuildTask != null && !_snapshotBuildTask.IsCompleted))
            return;

        if (_meshEstimateSummaryTask != null && !_meshEstimateSummaryTask.IsCompleted)
            return;

        long dueTicks = Interlocked.Read(ref _nextMeshEstimateRefreshUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        var snapshot = _reduceMySizeSnapshot;
        if (snapshot.EligibleMeshTriangles.Count == 0)
        {
            _meshEstimateSummaryRefreshPending = false;
            return;
        }

        long observedGeneration = _lastSeenMeshEstimateGeneration;
        int snapshotBuildVersion = Volatile.Read(ref _snapshotBuildVersion);
        _meshEstimateSummaryRefreshPending = false;

        _meshEstimateSummaryTask = Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundEstimatePriority();

            long estimatedMeshTriangles = 0;
            int estimatedMeshFiles = 0;
            var actionableMeshes = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in snapshot.MeshEstimateCandidates)
            {
                if (_meshOptimizationService.TryGetCachedEstimateFast(kv.Key, out var estimate) && estimate.HasSavings)
                {
                    actionableMeshes[kv.Key] = kv.Value;
                    estimatedMeshTriangles += estimate.SavedTriangles;
                    estimatedMeshFiles++;
                }
            }

            if (snapshotBuildVersion == Volatile.Read(ref _snapshotBuildVersion)
                && (estimatedMeshTriangles != snapshot.EstimatedMeshTriangles
                    || estimatedMeshFiles != snapshot.EstimatedMeshFiles
                    || actionableMeshes.Count != snapshot.EligibleMeshes.Count))
            {
                _reduceMySizeSnapshot = snapshot with
                {
                    HasEligibleMeshes = actionableMeshes.Count > 0,
                    EligibleMeshes = actionableMeshes,
                    EstimatedMeshTriangles = estimatedMeshTriangles,
                    EstimatedMeshFiles = estimatedMeshFiles,
                };
            }

            if (_meshOptimizationService.EstimateGeneration != observedGeneration)
            {
                _meshEstimateSummaryRefreshPending = true;
                Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, DateTime.UtcNow.AddMilliseconds(GetMeshEstimateRefreshDelayMs()).Ticks);
            }
        }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
    }

    private void TryStartScheduledTextureCleanupEstimateRefresh()
    {
        if (!_textureCleanupEstimateRefreshPending)
            return;

        if (_snapshotBuildPending || (_snapshotBuildTask != null && !_snapshotBuildTask.IsCompleted))
            return;

        if (_textureCleanupEstimateTask != null && !_textureCleanupEstimateTask.IsCompleted)
            return;

        long dueTicks = Interlocked.Read(ref _nextTextureCleanupEstimateUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        var snapshot = _reduceMySizeSnapshot;
        if (snapshot.PendingTextureCleanupCandidates.Count == 0)
        {
            _textureCleanupEstimateRefreshPending = false;
            return;
        }

        int snapshotBuildVersion = Volatile.Read(ref _snapshotBuildVersion);
        _textureCleanupEstimateRefreshPending = false;

        _textureCleanupEstimateTask = Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundEstimatePriority();

            var textureItems = new List<(string fmt, long size)>(snapshot.TextureItems);
            var remaining = new Dictionary<string, TextureCleanupEstimateCandidate>(snapshot.PendingTextureCleanupCandidates, StringComparer.Ordinal);

            TextureCleanupEstimateCandidate? firstUncachedCandidate = null;
            foreach (var candidate in snapshot.PendingTextureCleanupCandidates.Values
                         .OrderByDescending(static c => c.SourceSize)
                         .ThenBy(static c => c.PrimaryPath, StringComparer.OrdinalIgnoreCase))
            {
                if (TryGetCachedTextureCleanupEstimate(candidate, out var cachedEstimate))
                {
                    remaining.Remove(candidate.PrimaryPath);
                    if (cachedEstimate > 0)
                        textureItems.Add(($"CLEAN:{candidate.Format}", cachedEstimate));
                    continue;
                }

                firstUncachedCandidate = candidate;
                break;
            }

            if (firstUncachedCandidate != null)
            {
                long cleanupEstimate = 0;
                if (TextureOptimisationService.TryEstimateCleanupSavings(firstUncachedCandidate.RelatedModels, firstUncachedCandidate.HintPath, firstUncachedCandidate.SourceSize, out var estimatedSavedBytes, out _, out _))
                    cleanupEstimate = Math.Max(0, estimatedSavedBytes);

                CacheTextureCleanupEstimate(firstUncachedCandidate, cleanupEstimate);
                remaining.Remove(firstUncachedCandidate.PrimaryPath);
                if (cleanupEstimate > 0)
                    textureItems.Add(($"CLEAN:{firstUncachedCandidate.Format}", cleanupEstimate));
            }

            if (snapshotBuildVersion == Volatile.Read(ref _snapshotBuildVersion))
            {
                _reduceMySizeSnapshot = snapshot with
                {
                    TextureItems = textureItems,
                    PendingTextureCleanupCandidates = remaining,
                    TextureEstimate = EstimateBc7Savings(textureItems),
                };
            }

            if (remaining.Count > 0 && snapshotBuildVersion == Volatile.Read(ref _snapshotBuildVersion))
            {
                _textureCleanupEstimateRefreshPending = true;
                Interlocked.Exchange(ref _nextTextureCleanupEstimateUtcTicks, DateTime.UtcNow.AddMilliseconds(250).Ticks);
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    private static void TrySetCurrentThreadToBackgroundEstimatePriority()
    {
        try
        {
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
        }
        catch
        {
        }

        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                _ = SetThreadPriority(GetCurrentThread(), WindowsThreadModeBackgroundBegin);
        }
        catch
        {
        }
    }

    private static int GetMeshEstimateRefreshDelayMs()
        => 500;

    private void ScheduleMeshEstimateQueueBuild(bool immediate = false)
    {
        var analysis = _characterAnalyzer.LastAnalysis;
        Interlocked.Increment(ref _meshEstimateQueueBuildVersion);

        if (analysis is null || analysis.Count == 0)
        {
            _meshEstimateQueueBuildPending = false;
            Interlocked.Exchange(ref _nextMeshEstimateQueueBuildUtcTicks, 0L);
            return;
        }

        _meshEstimateQueueBuildPending = true;
        int delayMs = immediate ? 0 : 700;
        Interlocked.Exchange(ref _nextMeshEstimateQueueBuildUtcTicks, DateTime.UtcNow.AddMilliseconds(delayMs).Ticks);
    }

    private void TryStartScheduledMeshEstimateQueueBuild()
    {
        if (!_meshEstimateQueueBuildPending)
            return;

        if (_snapshotBuildPending || (_snapshotBuildTask != null && !_snapshotBuildTask.IsCompleted))
            return;

        if (_meshEstimateQueueBuildTask != null && !_meshEstimateQueueBuildTask.IsCompleted)
            return;

        long dueTicks = Interlocked.Read(ref _nextMeshEstimateQueueBuildUtcTicks);
        if (dueTicks > 0 && DateTime.UtcNow.Ticks < dueTicks)
            return;

        var snapshot = _reduceMySizeSnapshot;
        if (snapshot.EligibleMeshTriangles.Count == 0)
        {
            _meshEstimateQueueBuildPending = false;
            return;
        }

        int buildVersion = Volatile.Read(ref _meshEstimateQueueBuildVersion);
        _meshEstimateQueueBuildPending = false;

        _meshEstimateQueueBuildTask = Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundEstimatePriority();
            QueueMeshEstimates(snapshot.EligibleMeshTriangles);

            if (buildVersion != Volatile.Read(ref _meshEstimateQueueBuildVersion))
            {
                _meshEstimateQueueBuildPending = true;
                Interlocked.Exchange(ref _nextMeshEstimateQueueBuildUtcTicks, DateTime.UtcNow.AddMilliseconds(700).Ticks);
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    private void QueueMeshEstimates(IReadOnlyDictionary<string, long> eligibleMeshTriangles)
    {
        if (eligibleMeshTriangles == null || eligibleMeshTriangles.Count == 0)
            return;

        _meshOptimizationService.QueueEstimates(eligibleMeshTriangles
            .Where(static kv => !string.IsNullOrWhiteSpace(kv.Key))
            .Select(static kv => (PrimaryPath: kv.Key, CurrentTriangles: Math.Max(0, kv.Value)))
            .OrderByDescending(static r => r.CurrentTriangles)
            .ThenBy(static r => r.PrimaryPath, StringComparer.OrdinalIgnoreCase)
            .Select(static r => (r.PrimaryPath, r.CurrentTriangles)));
    }

    private static string BuildTextureCleanupEstimateCacheKey(TextureCleanupEstimateCandidate candidate)
    {
        return string.Join("|", new[]
        {
            candidate.PrimaryPath ?? string.Empty,
            candidate.HintPath ?? string.Empty,
            candidate.Format ?? string.Empty,
            candidate.SourceSize.ToString(CultureInfo.InvariantCulture),
            string.Join(";", candidate.RelatedModels.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase)),
        });
    }

    private bool TryGetCachedTextureCleanupEstimate(TextureCleanupEstimateCandidate candidate, out long estimatedSavedBytes)
    {
        var key = BuildTextureCleanupEstimateCacheKey(candidate);
        lock (_textureCleanupEstimateCacheLock)
        {
            return _textureCleanupEstimateCache.TryGetValue(key, out estimatedSavedBytes);
        }
    }

    private void CacheTextureCleanupEstimate(TextureCleanupEstimateCandidate candidate, long estimatedSavedBytes)
    {
        var key = BuildTextureCleanupEstimateCacheKey(candidate);
        lock (_textureCleanupEstimateCacheLock)
        {
            _textureCleanupEstimateCache[key] = Math.Max(0, estimatedSavedBytes);
        }
    }

    private ReduceMySizeSnapshot BuildReduceMySizeSnapshot(Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? analysis)
    {
        var eligibleTextures = new Dictionary<string, string[]>(StringComparer.Ordinal);
        var eligibleMeshes = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        var meshEstimateCandidates = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        var eligibleMeshTriangles = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var textureItems = new List<(string fmt, long size)>();
        var pendingTextureCleanupCandidates = new Dictionary<string, TextureCleanupEstimateCandidate>(StringComparer.Ordinal);
        long estimatedMeshTriangles = 0;
        int estimatedMeshFiles = 0;

        if (analysis is null || analysis.Count == 0)
        {
            return new ReduceMySizeSnapshot
            {
                HasEligibleTextures = false,
                HasEligibleMeshes = false,
                EligibleTextures = eligibleTextures,
                TextureTargets = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                MeshEstimateCandidates = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase),
                EligibleMeshes = eligibleMeshes,
                EligibleMeshTriangles = eligibleMeshTriangles,
                TextureItems = textureItems,
                PendingTextureCleanupCandidates = pendingTextureCleanupCandidates,
                TextureEstimate = EstimateBc7Savings(textureItems),
                EstimatedMeshTriangles = 0,
                EstimatedMeshFiles = 0,
            };
        }

        var textureEntries = new List<CharacterAnalyzer.FileDataEntry>();
        var textureEntriesByPrimary = new Dictionary<string, CharacterAnalyzer.FileDataEntry>(StringComparer.OrdinalIgnoreCase);
        var textureTargets = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var optimisationSnapshot = _optimisationPolicyService.Analyze(analysis);
        var allowedTexturePrimaries = new HashSet<string>(
            optimisationSnapshot.TextureCandidates
                .Select(static c => c.FilePaths?.FirstOrDefault())
                .Where(static p => !string.IsNullOrWhiteSpace(p))
                .Select(static p => p!),
            StringComparer.OrdinalIgnoreCase);

        KeyValuePair<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>[] snapshot;
        try
        {
            snapshot = analysis.ToArray();
        }
        catch
        {
            return _reduceMySizeSnapshot;
        }

        foreach (var kv in snapshot)
        {
            CharacterAnalyzer.FileDataEntry[] entries;
            try
            {
                entries = kv.Value.Values.ToArray();
            }
            catch
            {
                continue;
            }

            foreach (var e in entries)
            {
                if (e == null) continue;

                if (e.FilePaths is { Count: > 0 })
                {
                    bool isModel = string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                        || (e.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false);
                    if (isModel)
                    {
                        var primaryMesh = e.FilePaths[0];
                        long meshTriangles = Math.Max(0, e.Triangles);
                        if (_meshOptimizationService.ShouldOfferReduction(primaryMesh, meshTriangles))
                        {
                            var alternates = e.FilePaths.Skip(1).ToArray();
                            meshEstimateCandidates[primaryMesh] = alternates;
                            eligibleMeshTriangles[primaryMesh] = meshTriangles;

                            if (_meshOptimizationService.TryGetCachedEstimateFast(primaryMesh, out var meshEstimate) && meshEstimate.HasSavings)
                            {
                                eligibleMeshes[primaryMesh] = alternates;
                                estimatedMeshTriangles += meshEstimate.SavedTriangles;
                                estimatedMeshFiles++;
                            }
                        }
                    }
                }

                if (!string.Equals(e.FileType, "tex", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (e.FilePaths is null || e.FilePaths.Count == 0)
                    continue;

                string primaryTexture = e.FilePaths[0];
                if (string.IsNullOrWhiteSpace(primaryTexture) || !allowedTexturePrimaries.Contains(primaryTexture))
                    continue;

                textureEntries.Add(e);
                textureEntriesByPrimary.TryAdd(primaryTexture, e);
            }
        }

        var meshEntries = snapshot
            .SelectMany(static kv => kv.Value.Values)
            .Where(static e => e?.FilePaths is { Count: > 0 }
                && (string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                    || (e.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false)))
            .ToArray();

        var relatedModelsByTexture = BuildTextureModelReferenceMap(textureEntries, meshEntries);

        foreach (var textureCandidate in optimisationSnapshot.TextureCandidates)
        {
            if (textureCandidate.FilePaths is null || textureCandidate.FilePaths.Count == 0)
                continue;

            var primary = textureCandidate.FilePaths[0];
            if (string.IsNullOrWhiteSpace(primary))
                continue;

            if (!textureEntriesByPrimary.TryGetValue(primary, out var entry))
                continue;

            if (!Enum.TryParse<TextureCompressionPlanner.Target>(textureCandidate.SuggestedTarget, true, out var target)
                || target == TextureCompressionPlanner.Target.None)
                continue;

            bool alreadyTarget = string.Equals(entry.Format.Value, target.ToString(), StringComparison.OrdinalIgnoreCase);
            long formatSavings = 0;
            bool hasFormatSavings = false;
            if (!alreadyTarget)
            {
                formatSavings = EstimateFormatSavingsBytes(entry.Format.Value, target, entry.OriginalSize);
                hasFormatSavings = formatSavings > 0;
            }

            long cleanupEstimate = 0;
            bool hasCleanupEstimate = false;
            string? hint = (entry.GamePaths != null && entry.GamePaths.Count > 0) ? entry.GamePaths[0] : primary;
            if (relatedModelsByTexture.TryGetValue(primary, out var relatedModels) && relatedModels.Length > 0)
            {
                var cleanupCandidate = new TextureCleanupEstimateCandidate(
                    primary,
                    hint ?? primary,
                    entry.Format.Value,
                    entry.OriginalSize,
                    relatedModels
                        .Where(static p => !string.IsNullOrWhiteSpace(p))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(static p => p, StringComparer.OrdinalIgnoreCase)
                        .ToArray());

                if (cleanupCandidate.RelatedModels.Length > 0)
                {
                    if (TryGetCachedTextureCleanupEstimate(cleanupCandidate, out cleanupEstimate))
                    {
                        hasCleanupEstimate = cleanupEstimate > 0;
                    }
                    else
                    {
                        pendingTextureCleanupCandidates[primary] = cleanupCandidate;
                    }
                }
            }

            if (!hasFormatSavings && !hasCleanupEstimate && !pendingTextureCleanupCandidates.ContainsKey(primary))
                continue;

            if (!eligibleTextures.ContainsKey(primary))
                eligibleTextures[primary] = entry.FilePaths.Skip(1).ToArray();

            textureTargets[primary] = target.ToString();

            if (hasFormatSavings)
                textureItems.Add(($"{target}:{entry.Format.Value}", entry.OriginalSize));

            if (hasCleanupEstimate)
                textureItems.Add(($"CLEAN:{entry.Format.Value}", cleanupEstimate));
        }

        return new ReduceMySizeSnapshot
        {
            HasEligibleTextures = eligibleTextures.Count > 0,
            HasEligibleMeshes = eligibleMeshes.Count > 0,
            EligibleTextures = eligibleTextures,
            TextureTargets = textureTargets,
            MeshEstimateCandidates = meshEstimateCandidates,
            EligibleMeshes = eligibleMeshes,
            EligibleMeshTriangles = eligibleMeshTriangles,
            TextureItems = textureItems,
            PendingTextureCleanupCandidates = pendingTextureCleanupCandidates,
            TextureEstimate = EstimateBc7Savings(textureItems),
            EstimatedMeshTriangles = estimatedMeshTriangles,
            EstimatedMeshFiles = estimatedMeshFiles,
        };
    }

    private IEnumerable<string> GetAllMeshPrimariesFromAnalysis()
    {
        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis is null || analysis.Count == 0)
            yield break;

        KeyValuePair<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>[] snapshot;
        try
        {
            snapshot = analysis.ToArray();
        }
        catch
        {
            yield break;
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in snapshot)
        {
            CharacterAnalyzer.FileDataEntry[] entries;
            try
            {
                entries = kv.Value.Values.ToArray();
            }
            catch
            {
                continue;
            }

            foreach (var e in entries)
            {
                if (e?.FilePaths is not { Count: > 0 })
                    continue;

                bool isModel = string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                    || (e.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false);
                if (!isModel)
                    continue;

                var primary = e.FilePaths[0];
                if (string.IsNullOrWhiteSpace(primary) || !seen.Add(primary))
                    continue;

                yield return primary;
            }
        }
    }

    private string BuildReduceVramButtonLabel(long estimatedSavedBytes, bool isEstimateBuilding)
    {
        if (estimatedSavedBytes > 0)
        {
            long roundedMiB = Math.Max(1L, (long)Math.Round(estimatedSavedBytes / 1048576d));
            return string.Format(_uiSharedService.L("UI.CompactUI.ReduceVram.Est", "Reduce (~{0})"), $"{roundedMiB} MiB");
        }

        return isEstimateBuilding
            ? _uiSharedService.L("UI.CompactUI.ReduceSimple.Calculating", "Reduce (...)")
            : _uiSharedService.L("UI.CompactUI.ReduceSimple", "Reduce");
    }

    private string BuildReduceTrisButtonLabel(long estimatedSavedTriangles, bool isEstimateBuilding)
        => estimatedSavedTriangles > 0
            ? string.Format(_uiSharedService.L("UI.CompactUI.ReduceTris.Est", "Reduce (~{0})"), FormatTrianglesThousands(estimatedSavedTriangles))
            : isEstimateBuilding
                ? _uiSharedService.L("UI.CompactUI.ReduceSimple.Calculating", "Reduce (...)")
                : _uiSharedService.L("UI.CompactUI.ReduceSimple", "Reduce");


    private void DrawReduceVramTooltip(Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? analysis, long currentVramBytes, long estimatedSavedBytes, int estimatedFiles, bool isEstimateBuilding)
    {
        ImGui.BeginTooltip();
        if (analysis is null || analysis.Count == 0)
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.f19f2322", "Run Character Analysis to compute your footprint."));
        }
        else if (isEstimateBuilding && !(estimatedSavedBytes > 0 && estimatedFiles > 0))
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceVram.CalculatingTooltip", "Calculating VRAM optimisation savings..."));
        }
        else if (estimatedSavedBytes > 0 && estimatedFiles > 0)
        {
            long estimatedAfterBytes = Math.Max(0, currentVramBytes - estimatedSavedBytes);
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceVram.Header", "Estimated VRAM reduction:"));
            ImGui.Separator();
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.3bdf36a7", "Files affected: {0}"), estimatedFiles));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.235a7422", "Before: {0}"), UiSharedService.ByteToString(currentVramBytes, addSuffix: true)));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.b3286612", "After:  ~{0}"), UiSharedService.ByteToString(estimatedAfterBytes, addSuffix: true)));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.efb6de86", "Saved:  ~{0}"), UiSharedService.ByteToString(estimatedSavedBytes, addSuffix: true)));

            if (isEstimateBuilding)
            {
                ImGui.Spacing();
                ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceVram.RefreshingTooltip", "Refreshing VRAM estimate in the background..."));
            }
        }
        else
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceVram.None", "Nothing found to reduce for VRAM."));
        }

        ImGui.EndTooltip();
    }

    private void DrawReduceTrisTooltip(Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? analysis, long currentTriangles, long estimatedSavedTriangles, int estimatedMeshFiles, bool isEstimateBuilding, bool hasEligibleMeshes)
    {
        ImGui.BeginTooltip();
        if (analysis is null || analysis.Count == 0)
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.f19f2322", "Run Character Analysis to compute your footprint."));
        }
        else if (isEstimateBuilding && !(estimatedSavedTriangles > 0 && estimatedMeshFiles > 0))
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceTris.CalculatingTooltip", "Calculating triangle optimisation savings..."));
        }
        else if (estimatedSavedTriangles > 0 && estimatedMeshFiles > 0)
        {
            long estimatedTrianglesAfterTotal = Math.Max(0, currentTriangles - estimatedSavedTriangles);
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceTris.Header", "Estimated triangle reduction:"));
            ImGui.Separator();
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.MeshFiles", "Meshes affected: {0}"), estimatedMeshFiles));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.MeshBefore", "Triangles before: {0}"), FormatTrianglesThousands(currentTriangles)));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.MeshAfter", "Triangles after:  ~{0}"), FormatTrianglesThousands(estimatedTrianglesAfterTotal)));
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.MeshSaved", "Triangles saved: ~{0}"), FormatTrianglesThousands(estimatedSavedTriangles)));

            if (isEstimateBuilding)
            {
                ImGui.Spacing();
                ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceTris.RefreshingTooltip", "Refreshing triangle estimate in the background..."));
            }
        }
        else if (hasEligibleMeshes)
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceTris.Eligible", "Meshes are eligible for cleanup. Estimated savings are not available yet, but you can still run Reduce."));
        }
        else
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ReduceTris.None", "Nothing found to reduce for triangles."));
        }

        ImGui.EndTooltip();
    }

    private void BeginReduceVram(Dictionary<string, string[]> textureSet, Dictionary<string, string> textureTargets)
    {
        BeginReduceMySize(textureSet, textureTargets, new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase));
    }

    private void BeginReduceTris(Dictionary<string, string[]> meshSet)
    {
        BeginReduceMySize(new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), meshSet);
    }

    private void BeginReduceMySize(Dictionary<string, string[]> textureSet, Dictionary<string, string> textureTargets, Dictionary<string, string[]> meshSet)
    {
        _bc7Set.Clear();
        foreach (var kv in textureSet) _bc7Set[kv.Key] = kv.Value;

        var meshWork = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in meshSet)
        {
            if (_meshOptimizationService.TryGetCachedEstimateFast(kv.Key, out var estimate) && estimate.HasSavings)
                meshWork[kv.Key] = kv.Value;
        }

        Interlocked.Increment(ref _snapshotBuildVersion);
        _snapshotBuildPending = false;
        _reduceMySizeSnapshot = new ReduceMySizeSnapshot();
        _textureEstimateState = ReduceEstimateState.Empty;
        _lastSeenMeshEstimateGeneration = _meshOptimizationService.EstimateGeneration;
        Interlocked.Exchange(ref _nextSnapshotBuildUtcTicks, 0L);
        Interlocked.Exchange(ref _nextMeshEstimateRefreshUtcTicks, 0L);

        _bc7Total = _bc7Set.Count + meshWork.Count;
        _bc7CurFile = string.Empty;
        _bc7CurIndex = 0;

        if (_bc7Total <= 0)
        {
            _bc7Task = null;
            _bc7ShowModal = false;
            _bc7ModalOpen = false;
            ScheduleReduceMySizeSnapshotRebuild(immediate: true);
            return;
        }

        _bc7Cts.Cancel();
        _bc7Cts.Dispose();
        _bc7Cts = new();

        var targetByPrimary = new Dictionary<string, string>(textureTargets ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
        var relatedModelsByTexture = BuildTextureModelReferenceMapFromAnalysis(_bc7Set.Keys);
        _bc7Task = Task.Run(async () =>
        {
            int completed = 0;
            if (_bc7Set.Count > 0)
            {
                var textureProgress = new Progress<(string fileName, int index)>(e =>
                {
                    _bc7CurFile = e.fileName;
                    _bc7CurIndex = completed + e.index;
                });

                await _textureOptimizationService.RunPlannedOptimizationAsync(
                    _logger,
                    _bc7Set,
                    targetByPrimary,
                    textureProgress,
                    _bc7Cts.Token,
                    relatedModelsByTexture).ConfigureAwait(false);

                completed += _bc7Set.Count;
            }

            if (meshWork.Count > 0)
            {
                var meshProgress = new Progress<(string fileName, int index)>(e =>
                {
                    _bc7CurFile = e.fileName;
                    _bc7CurIndex = completed + e.index;
                });

                await _meshOptimizationService.RunPlannedOptimisationAsync(
                    _logger,
                    meshWork,
                    meshProgress,
                    _bc7Cts.Token).ConfigureAwait(false);
            }
        }, _bc7Cts.Token);

        _bc7ShowModal = true;
    }

    private Dictionary<string, string[]> BuildTextureModelReferenceMapFromAnalysis(IEnumerable<string> texturePrimaries)
    {
        var primaries = (texturePrimaries ?? Array.Empty<string>())
            .Where(static p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (primaries.Count == 0)
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis is null || analysis.Count == 0)
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        CharacterAnalyzer.FileDataEntry[] entries;
        try
        {
            entries = analysis.SelectMany(static kv => kv.Value.Values).Where(static e => e != null).ToArray();
        }
        catch
        {
            return BuildTextureModelReferenceMap(primaries, GetAllMeshPrimariesFromAnalysis());
        }

        var textures = entries.Where(e => e?.FilePaths is { Count: > 0 } && primaries.Contains(e.FilePaths[0])).ToArray();
        var meshes = entries.Where(static e => e?.FilePaths is { Count: > 0 }
            && (string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                || (e.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false))).ToArray();
        return BuildTextureModelReferenceMap(textures, meshes);
    }

    private static Dictionary<string, string[]> BuildTextureModelReferenceMap(IEnumerable<CharacterAnalyzer.FileDataEntry> textureEntries, IEnumerable<CharacterAnalyzer.FileDataEntry> meshEntries)
    {
        var textures = (textureEntries ?? Array.Empty<CharacterAnalyzer.FileDataEntry>())
            .Where(static e => e?.FilePaths is { Count: > 0 })
            .GroupBy(static e => e.FilePaths[0], StringComparer.OrdinalIgnoreCase)
            .Select(static g => g.First())
            .ToArray();
        var meshes = (meshEntries ?? Array.Empty<CharacterAnalyzer.FileDataEntry>())
            .Where(static e => e?.FilePaths is { Count: > 0 })
            .GroupBy(static e => e.FilePaths[0], StringComparer.OrdinalIgnoreCase)
            .Select(static g => g.First())
            .ToArray();

        var result = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        if (textures.Length == 0 || meshes.Length == 0)
            return result;

        var meshByIdentity = meshes
            .SelectMany(mesh => GetAnalysisIdentityKeys(mesh).Select(key => new { key, path = mesh.FilePaths[0] }))
            .GroupBy(static x => x.key, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.Select(static x => x.path).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(), StringComparer.OrdinalIgnoreCase);

        var fallbackMeshPrimaries = meshes.Select(static e => e.FilePaths[0]).ToArray();
        foreach (var texture in textures)
        {
            var primary = texture.FilePaths[0];
            var keys = GetAnalysisIdentityKeys(texture).ToArray();
            var matched = keys
                .Where(meshByIdentity.ContainsKey)
                .SelectMany(key => meshByIdentity[key])
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            if (matched.Length == 0)
            {
                var fallback = BuildTextureModelReferenceMap(new[] { primary }, fallbackMeshPrimaries);
                if (fallback.TryGetValue(primary, out var fallbackModels) && fallbackModels.Length > 0)
                    result[primary] = fallbackModels;
                continue;
            }

            result[primary] = matched;
        }

        return result;
    }

    private static IEnumerable<string> GetAnalysisIdentityKeys(CharacterAnalyzer.FileDataEntry entry)
    {
        if (entry == null)
            yield break;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var rawPath in entry.GamePaths ?? Enumerable.Empty<string>())
        {
            var normalized = NormalizeGamePath(rawPath);
            if (string.IsNullOrWhiteSpace(normalized))
                continue;

            if (TryBuildAnalysisIdentityKey(normalized, out var identity) && seen.Add(identity))
                yield return identity;

            var equipmentCode = ExtractEquipmentCode(normalized);
            if (!string.IsNullOrWhiteSpace(equipmentCode) && seen.Add(equipmentCode))
                yield return equipmentCode;
        }

        if (entry.FilePaths is { Count: > 0 })
        {
            var primary = entry.FilePaths[0].Replace('\\', '/');
            if (TryBuildAnalysisIdentityKey(primary, out var fileIdentity) && seen.Add(fileIdentity))
                yield return fileIdentity;
        }
    }

    private static bool TryBuildAnalysisIdentityKey(string value, out string key)
    {
        key = string.Empty;
        var equipmentCode = ExtractEquipmentCode(value);
        if (string.IsNullOrWhiteSpace(equipmentCode))
            return false;

        var slot = ExtractModelSlot(value) ?? ExtractTextureSlot(value);
        if (string.IsNullOrWhiteSpace(slot))
            return false;

        key = equipmentCode + ':' + slot;
        return true;
    }

    private static string NormalizeGamePath(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Replace('\\', '/');

    private static string? ExtractEquipmentCode(string value)
    {
        var m = System.Text.RegularExpressions.Regex.Match(value ?? string.Empty, @"(?i)e\d{4}");
        return m.Success ? m.Value.ToLowerInvariant() : null;
    }

    private static string? ExtractModelSlot(string value)
    {
        foreach (var slot in new[] { "_top", "_dwn", "_glv", "_sho", "_leg", "_met", "_ear", "_nek", "_rir", "_ril", "_wrs", "_fac" })
        {
            if (value.Contains(slot, StringComparison.OrdinalIgnoreCase))
                return slot;
        }
        return null;
    }

    private static string? ExtractTextureSlot(string value)
    {
        foreach (var slot in new[] { "_top_", "_dwn_", "_glv_", "_sho_", "_leg_", "_met_", "_ear_", "_nek_", "_rir_", "_ril_", "_wrs_", "_fac_" })
        {
            if (value.Contains(slot, StringComparison.OrdinalIgnoreCase))
                return slot.TrimEnd('_');
        }
        return null;
    }

    private static Dictionary<string, string[]> BuildTextureModelReferenceMap(IEnumerable<string> texturePrimaries, IEnumerable<string> meshPrimaries)
    {
        var textures = (texturePrimaries ?? Array.Empty<string>()).Where(static p => !string.IsNullOrWhiteSpace(p)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var meshes = (meshPrimaries ?? Array.Empty<string>()).Where(static p => !string.IsNullOrWhiteSpace(p)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var result = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        if (textures.Length == 0 || meshes.Length == 0)
            return result;

        foreach (var texture in textures)
        {
            string textureDir = Path.GetDirectoryName(texture) ?? string.Empty;
            var ranked = meshes
                .Select(mesh => new { Path = mesh, Score = ComputeTextureModelAffinityScore(texture, textureDir, mesh) })
                .Where(static x => x.Score > 0)
                .OrderByDescending(static x => x.Score)
                .ThenBy(static x => x.Path.Length)
                .Take(8)
                .Select(static x => x.Path)
                .ToArray();

            if (ranked.Length > 0)
                result[texture] = ranked;
        }

        return result;
    }

    private static int ComputeTextureModelAffinityScore(string texturePath, string textureDir, string meshPath)
    {
        string meshDir = Path.GetDirectoryName(meshPath) ?? string.Empty;
        int sharedDepth = ComputeSharedDirectoryDepth(textureDir, meshDir);
        if (sharedDepth <= 0)
            return 0;

        string textureName = Path.GetFileNameWithoutExtension(texturePath) ?? string.Empty;
        string meshName = Path.GetFileNameWithoutExtension(meshPath) ?? string.Empty;
        string textureNorm = texturePath.Replace('\\', '/');
        string meshNorm = meshPath.Replace('\\', '/');
        int score = sharedDepth * 100;

        static string? ExtractEquipmentCode(string value)
        {
            var m = System.Text.RegularExpressions.Regex.Match(value, @"(?i)e\d{4}");
            return m.Success ? m.Value : null;
        }

        static string? ExtractModelSlot(string value)
        {
            foreach (var slot in new[] { "_top", "_dwn", "_glv", "_sho", "_leg", "_met", "_ear", "_nek", "_rir", "_ril", "_wrs" })
            {
                if (value.Contains(slot, StringComparison.OrdinalIgnoreCase))
                    return slot;
            }
            return null;
        }

        var textureCode = ExtractEquipmentCode(textureNorm);
        var meshCode = ExtractEquipmentCode(meshNorm);
        if (!string.IsNullOrWhiteSpace(textureCode) && string.Equals(textureCode, meshCode, StringComparison.OrdinalIgnoreCase))
            score += 140;

        var textureSlot = ExtractModelSlot(textureNorm);
        var meshSlot = ExtractModelSlot(meshNorm);
        if (!string.IsNullOrWhiteSpace(textureSlot) && string.Equals(textureSlot, meshSlot, StringComparison.OrdinalIgnoreCase))
            score += 90;

        if (!string.IsNullOrWhiteSpace(textureName) && !string.IsNullOrWhiteSpace(meshName))
        {
            if (string.Equals(textureName, meshName, StringComparison.OrdinalIgnoreCase))
                score += 60;
            else
            {
                var textureTokens = textureName.Split(new[] { '_', '-', '.' }, StringSplitOptions.RemoveEmptyEntries);
                var meshTokens = new HashSet<string>(meshName.Split(new[] { '_', '-', '.' }, StringSplitOptions.RemoveEmptyEntries), StringComparer.OrdinalIgnoreCase);
                score += textureTokens.Count(t => meshTokens.Contains(t)) * 10;
            }
        }

        return score;
    }

    private static int ComputeSharedDirectoryDepth(string a, string b)
    {
        if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b))
            return 0;

        var aParts = a.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        var bParts = b.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        int depth = 0;
        int count = Math.Min(aParts.Length, bParts.Length);
        for (int i = 0; i < count; i++)
        {
            if (!string.Equals(aParts[i], bParts[i], StringComparison.OrdinalIgnoreCase))
                break;
            depth++;
        }

        return depth;
    }

    private static int GetTargetBits(TextureCompressionPlanner.Target target)
        => target switch
        {
            TextureCompressionPlanner.Target.BC1 => 4,
            TextureCompressionPlanner.Target.BC3 => 8,
            TextureCompressionPlanner.Target.BC7 => 8,
            _ => int.MaxValue,
        };

    private static int GetSourceBits(string? format)
    {
        if (string.IsNullOrWhiteSpace(format))
            return 0;

        return format.Trim().ToUpperInvariant() switch
        {
            "A8R8G8B8" or "R8G8B8A8" or "RGBA8" or "ARGB8" or "X8R8G8B8" => 32,
            "DXT1" or "BC1" or "BC4" => 4,
            "DXT3" or "DXT5" or "BC2" or "BC3" or "BC5" or "BC7" => 8,
            _ => 0,
        };
    }

    private static long EstimateFormatSavingsBytes(string? sourceFormat, TextureCompressionPlanner.Target target, long originalSize)
    {
        if (originalSize <= 0)
            return 0;

        int srcBits = GetSourceBits(sourceFormat);
        int dstBits = GetTargetBits(target);

        if (srcBits <= 0 || dstBits == int.MaxValue)
            return 0;

        long estAfter = (long)Math.Round(originalSize * (dstBits / (double)srcBits));
        return Math.Max(0, originalSize - estAfter);
    }

    private (long saved, int count, Dictionary<string, (int files, long before, long after)> byFmt)
    EstimateBc7Savings(List<(string fmt, long size)> items)
    {
        long before = 0, after = 0;
        var byFmt = new Dictionary<string, (int files, long b, long a)>(StringComparer.OrdinalIgnoreCase);

        foreach (var (fmtPacked, size) in items)
        {
            var parts = (fmtPacked ?? "").Split(':', 2, StringSplitOptions.RemoveEmptyEntries);
            string tgt = parts.Length == 2 ? parts[0] : "BC7";
            string src = parts.Length == 2 ? parts[1] : parts[0];

            if (string.Equals(tgt, "CLEAN", StringComparison.OrdinalIgnoreCase))
            {
                before += size;
                after += 0;
                var cleanupKey = $"Cleanup ({src})";
                if (!byFmt.TryGetValue(cleanupKey, out var cleanAgg)) cleanAgg = (0, 0, 0);
                cleanAgg.files += 1;
                cleanAgg.b += size;
                cleanAgg.a += 0;
                byFmt[cleanupKey] = cleanAgg;
                continue;
            }

            int srcBpp;
            var s = src.Trim().ToUpperInvariant();
            if (s is "A8R8G8B8" or "R8G8B8A8" or "RGBA8" or "ARGB8" or "X8R8G8B8") srcBpp = 32;
            else if (s is "DXT1" or "BC1" or "BC4") srcBpp = 4;
            else if (s is "DXT3" or "DXT5" or "BC2" or "BC3" or "BC5" or "BC7") srcBpp = 8;
            else srcBpp = 8;

            int tgtBpp;
            var t = tgt.Trim().ToUpperInvariant();
            if (t is "BC1" or "BC4") tgtBpp = 4;
            else tgtBpp = 8;

            double factor = (double)tgtBpp / srcBpp;
            long est = (long)Math.Round(size * factor);

            before += size;
            after += est;

            var formatKey = $"{src}→{tgt}";
            if (!byFmt.TryGetValue(formatKey, out var agg)) agg = (0, 0, 0);
            agg.files += 1;
            agg.b += size;
            agg.a += est;
            byFmt[formatKey] = agg;
        }

        return (before - after, items.Count, byFmt);
    }

    private void DrawScopeToggles()
    {
        var transient = _transientConfigService.Current;
        var selected = (ScopeMode)transient.SelectedScopeMode;

        using (ImRaii.PushId("ScopeToggles"))
        {
            float contentWidth = ImGui.GetContentRegionAvail().X;
            float baseX = ImGui.GetCursorPosX();

            var style = ImGui.GetStyle();

            ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey2);
            var l = _uiSharedService.L("UI.CompactUI.Scope.Header", "Limit Sync to:");
            var lsz = ImGui.CalcTextSize(l);
            ImGui.SetCursorPosX(MathF.Max(0f, (UiSharedService.GetWindowContentRegionWidth() - lsz.X) * 0.5f));
            ImGui.TextUnformatted(l);
            ImGui.PopStyleColor();

            string[] names =
            [
                _uiSharedService.L("UI.CompactUI.Scope.PairsShells", "Pairs & Shells"),
                _uiSharedService.L("UI.CompactUI.Scope.Friends", "Friends"),
                _uiSharedService.L("UI.CompactUI.Scope.PartyAlliance", "Party & Alli"),
            ];
            ScopeMode[] modes = [ScopeMode.Everyone, ScopeMode.Friends, ScopeMode.Alliance];

            float scale = ImGuiHelpers.GlobalScale;
            float spacingX = 10f * scale;
            float minSpacingX = 2f * scale;
            float innerSpacingX = style.ItemInnerSpacing.X;
            float minInnerSpacingX = 2f * scale;
            float circleW = ImGui.GetFrameHeight();

            float CalcRowW(float sX, float isX)
            {
                float w = 0f;
                for (int i = 0; i < names.Length; i++)
                {
                    float tW = ImGui.CalcTextSize(names[i]).X;
                    float itemW = circleW + isX + tW;
                    w += itemW;
                    if (i != names.Length - 1) w += sX;
                }
                return w;
            }

            float rowW = CalcRowW(spacingX, innerSpacingX);
            int guard = 0;
            while (rowW > contentWidth && guard++ < 50)
            {
                bool changed = false;
                if (spacingX > minSpacingX) { spacingX -= 1f * scale; changed = true; }
                else if (innerSpacingX > minInnerSpacingX) { innerSpacingX -= 1f * scale; changed = true; }
                if (!changed) break;
                rowW = CalcRowW(spacingX, innerSpacingX);
            }

            ImGui.PushStyleVar(ImGuiStyleVar.ItemSpacing, new Vector2(spacingX, style.ItemSpacing.Y));
            ImGui.PushStyleVar(ImGuiStyleVar.ItemInnerSpacing, new Vector2(innerSpacingX, style.ItemInnerSpacing.Y));
            ImGui.PushStyleVar(ImGuiStyleVar.FramePadding, new Vector2(style.FramePadding.X, 2f * scale));

            ImGui.SetCursorPosX(baseX + MathF.Max(0f, (contentWidth - rowW) * 0.5f));

            for (int i = 0; i < names.Length; i++)
            {
                if (i != 0) ImGui.SameLine();

                bool isSet = selected == modes[i];
                if (ImGui.RadioButton(names[i], isSet) && !isSet)
                    selected = modes[i];
            }

            ImGui.PopStyleVar(3);

            if ((ScopeMode)transient.SelectedScopeMode != selected)
            {
                transient.SelectedScopeMode = (int)selected;
                _transientConfigService.Save();
                Mediator.Publish(new ScopeModeChangedMessage(selected));
            }
        }
    }

    private static bool DrawCenteredTextAutoFit(string id, string text, Vector4 color, float regionW, float padX, float minScale)
    {
        var dl = ImGui.GetWindowDrawList();
        var start = ImGui.GetCursorScreenPos();

        float lineH = ImGui.GetTextLineHeight();
        ImGui.InvisibleButton(id, new Vector2(regionW, lineH));

        if (string.IsNullOrEmpty(text) || regionW <= 1f)
            return ImGui.IsItemClicked();

        float innerW = MathF.Max(0f, regionW - (padX * 2f));
        if (innerW <= 1f)
            return ImGui.IsItemClicked();

        float baseW = ImGui.CalcTextSize(text).X;
        float scale = (baseW > 0f) ? MathF.Min(1f, innerW / baseW) : 1f;
        scale = MathF.Max(minScale, scale);

        var font = ImGui.GetFont();
        float fontSize = ImGui.GetFontSize() * scale;

        float drawW = baseW * scale;
        float x = start.X + padX + MathF.Max(0f, (innerW - drawW) * 0.5f);
        float y = start.Y + (lineH - fontSize) * 0.5f;

        var rMin = start;
        var rMax = start + new Vector2(regionW, lineH);

        dl.PushClipRect(rMin, rMax, true);
        dl.AddText(font, fontSize, new Vector2(x, y), ImGui.GetColorU32(color), text);
        dl.PopClipRect();

        return ImGui.IsItemClicked();
    }

    private void UpdateCompactWindowTitle()
    {
        var ver = Assembly.GetExecutingAssembly().GetName().Version;
        var versionText = $"RavaSync {ver.Major}.{ver.Minor}.{ver.Build}";
        if (_apiController.ServerState is ServerState.Connected)
        {
            var users = _apiController.OnlineUsers.ToString(CultureInfo.InvariantCulture);
            WindowName = $"{versionText} ♥ {users} users online###RavaSyncMainUI";
            return;
        }

        WindowName = $"{versionText}###RavaSyncMainUI";
    }

    private void ToggleServerConnectionFromTitleBar()
    {
        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;

        if (isConnectingOrConnected && !_serverManager.CurrentServer.FullPause)
        {
            _serverManager.CurrentServer.FullPause = true;
            _serverManager.Save();
        }
        else if (!isConnectingOrConnected && _serverManager.CurrentServer.FullPause)
        {
            _serverManager.CurrentServer.FullPause = false;
            _serverManager.Save();
        }

        _ = _apiController.CreateConnectionsAsync();
    }

    private FontAwesomeIcon GetTitleBarConnectionIcon()
    {
        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;
        return isConnectingOrConnected ? FontAwesomeIcon.Unlink : FontAwesomeIcon.Link;
    }

    private string GetTitleBarConnectionTooltip()
    {
        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;
        return isConnectingOrConnected
            ? "Disconnect from " + _serverManager.CurrentServer.ServerName
            : "Connect to " + _serverManager.CurrentServer.ServerName;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _framework.Update -= OnFrameworkUpdate;
        }

        base.Dispose(disposing);
    }
}
