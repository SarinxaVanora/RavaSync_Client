using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
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



namespace RavaSync.UI;

public class CompactUi : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly MareConfigService _configService;
    private readonly ConcurrentDictionary<GameObjectHandler, Dictionary<string, FileDownloadStatus>> _currentDownloads = new();
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
    private readonly TransientConfigService _transientConfigService;
    private readonly TransientResourceManager _transientResourceManager;
    private List<IDrawFolder> _drawFolders;
    private Pair? _lastAddedUser;
    private string _lastAddedUserComment = string.Empty;
    private Vector2 _lastPosition = Vector2.One;
    private Vector2 _lastSize = Vector2.One;
    private bool _showModalForUserAddition;
    private bool _wasOpen;
    private float _windowContentWidth;
    private Vector2? _restoreMainUiPos;
    private bool _restoreUncollapse;
    private bool _suppressMinimizedRestoreIcon;
    private bool _wasCollapsed;


    private enum PairViewTab
    {
        DirectPairs,
        Shells,
        Visible
    }

    private PairViewTab _currentPairViewTab = PairViewTab.DirectPairs;
    private string _pairSearch = string.Empty;


    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();


    public CompactUi(ILogger<CompactUi> logger, UiSharedService uiShared, MareConfigService configService, ApiController apiController, PairManager pairManager,
        ServerConfigurationManager serverManager, MareMediator mediator, FileUploadManager fileTransferManager,
        TagHandler tagHandler, DrawEntityFactory drawEntityFactory, SelectTagForPairUi selectTagForPairUi, SelectPairForTagUi selectPairForTagUi,
        PerformanceCollectorService performanceCollectorService, IpcManager ipcManager, PlayerPerformanceConfigService playerPerformanceConfigService,
        CharacterAnalyzer characterAnalyzer, TransientConfigService transientConfigService, TransientResourceManager transientResourceManager)
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
        _characterAnalyzer = characterAnalyzer;
        _transientConfigService = transientConfigService;
        _transientResourceManager = transientResourceManager;
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
        Mediator.Subscribe<SwitchToMainUiMessage>(this, (_) => IsOpen = true);
        Mediator.Subscribe<SwitchToIntroUiMessage>(this, (_) => IsOpen = false);
        Mediator.Subscribe<CutsceneStartMessage>(this, (_) => UiSharedService_GposeStart());
        Mediator.Subscribe<CutsceneEndMessage>(this, (_) => UiSharedService_GposeEnd());
        Mediator.Subscribe<DownloadStartedMessage>(this, (msg) => _currentDownloads[msg.DownloadId] = msg.DownloadStatus);
        Mediator.Subscribe<DownloadFinishedMessage>(this, (msg) => _currentDownloads.TryRemove(msg.DownloadId, out _));
        Mediator.Subscribe<RefreshUiMessage>(this, (msg) => _drawFolders = GetDrawFolders().ToList());

        Mediator.Subscribe<RestoreMainUiAtPositionMessage>(this, (msg) =>
        {
            _restoreMainUiPos = msg.Position;
            _restoreUncollapse = true;
            IsOpen = true;
        });

        Flags |= ImGuiWindowFlags.NoDocking | ImGuiWindowFlags.NoCollapse;

        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new Vector2(385, 500),
            MaximumSize = new Vector2(385, 2100),
        };
    }


    public override void OnOpen()
    {
        _suppressMinimizedRestoreIcon = false;

        if (_configService.Current.ShowMinimizedRestoreIcon)
            Mediator.Publish(new MainUiRestoredMessage());

        base.OnOpen();
    }


    public override void OnClose()
    {
        if (!_suppressMinimizedRestoreIcon && _configService.Current.ShowMinimizedRestoreIcon)
        {
            Mediator.Publish(new MainUiMinimizedAtPositionMessage(_lastPosition));
            Mediator.Publish(new MainUiMinimizedMessage());
        }

        base.OnClose();
    }



    protected override void DrawInternal()
    {
        _windowContentWidth = UiSharedService.GetWindowContentRegionWidth();
        UpdateCompactWindowTitle();

        if (_configService.Current.ShowMinimizedRestoreIcon)
            Flags |= ImGuiWindowFlags.NoCollapse;
        else
            Flags &= ~ImGuiWindowFlags.NoCollapse;

        if (_restoreMainUiPos != null)
        {
            var vp = ImGui.GetMainViewport();
            var pos1 = _restoreMainUiPos.Value;

            // Keep a little margin.
            var min = vp.WorkPos;
            var max = vp.WorkPos + vp.WorkSize - new Vector2(60f, 60f);

            pos1 = Vector2.Clamp(pos1, min, max);
            ImGui.SetWindowPos(pos1, ImGuiCond.Always);

            _restoreMainUiPos = null;
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

        // ---- Front-UI BC7 conversion modal (independent of Analysis window) ----
        if (_bc7Task != null && !_bc7Task.IsCompleted)
        {
            if (_bc7ShowModal && !_bc7ModalOpen)
            {
                ImGui.OpenPopup(_uiSharedService.L("UI.CompactUI.fa170e36", "BC7 Conversion in Progress"));
                _bc7ModalOpen = true;
            }

            if (ImGui.BeginPopupModal(_uiSharedService.L("UI.CompactUI.fa170e36", "BC7 Conversion in Progress")))
            {
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.b8ef613e", "BC7 Conversion in progress: {0}/{1}"), _bc7CurIndex, _bc7Total));
                UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.CompactUI.12a59426", "Current file: {0}"), _bc7CurFile));
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StopCircle, _uiSharedService.L("UI.CompactUI.08eb8f15", "Cancel conversion")))
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
        else if (_bc7Task != null && _bc7Task.IsCompleted && _bc7Set.Count > 0)
        {
            _bc7Task = null;
            _bc7Set.Clear();
            _bc7ShowModal = false;
            _bc7ModalOpen = false;
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

        // Tabs: Direct | Shells | Visible
        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Pairs", "Pairs"), PairViewTab.DirectPairs);
        ImGui.SameLine();
        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Shells", "Shells"), PairViewTab.Shells);
        ImGui.SameLine();
        DrawTab(_uiSharedService.L("UI.CompactUI.Tab.Visible", "Visible"), PairViewTab.Visible);

        // Search box, right-aligned on the same row
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
            _drawFolders = GetDrawFolders().ToList();
        }
    }

    private void DrawTransfers()
    {
        var currentUploads = _fileTransferManager.CurrentUploads.ToList();
        ImGui.AlignTextToFramePadding();
        _uiSharedService.IconText(FontAwesomeIcon.Upload);
        ImGui.SameLine(35 * ImGuiHelpers.GlobalScale);

        if (currentUploads.Any())
        {
            var totalUploads = currentUploads.Count;

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

        foreach (var kv in _currentDownloads)
        {
            foreach (var s in kv.Value.Values)
            {
                anyDownloads = true;
                totalDownloads += s.TotalFiles;
                doneDownloads += s.TransferredFiles;
                totalDownloaded += s.TransferredBytes;
                totalToDownload += s.TotalBytes;
            }
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

    private void DrawUIDHeader()
    {
        var contentW = UiSharedService.GetWindowContentRegionWidth();
        var scale = ImGuiHelpers.GlobalScale;

        bool isConnected = _apiController.ServerState is ServerState.Connected;

        RefreshHeaderCacheIfNeeded();

        long myVramBytes = _cachedMyVramBytes;
        long myTriangles = _cachedMyTriangles;

        bool hasEligible = _cachedHasEligibleBc7;
        var eligibleSet = _cachedEligibleBc7Set;
        var (estSaved, estCount, _) = _cachedBc7Est;

        estSaved = RoundToWholeMiBBytes(estSaved);

        var analysis = _characterAnalyzer.LastAnalysis;

        var btnLabel = estSaved > 0
            ? string.Format(_uiSharedService.L("UI.CompactUI.ReduceMySize.Est", "Reduce my size (~{0})"),
                UiSharedService.ByteToString(estSaved, addSuffix: true))
            : _uiSharedService.L("UI.CompactUI.ReduceMySize", "Reduce my size");

        bool canClick = hasEligible && (_bc7Task == null || _bc7Task.IsCompleted);

        long pairsVramBytes = _pairManager.PairsWithGroups.Keys
            .Where(p => p.IsDirectlyPaired && p.IsVisible && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
            .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

        long shellsVramBytes = _pairManager.PairsWithGroups.Keys
            .Where(p => !p.IsDirectlyPaired && p.IsVisible && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
            .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

        long totalVramBytes = pairsVramBytes + shellsVramBytes;

        const long MiB = 1024L * 1024L;

        static long RoundToWholeMiBBytes(long bytes)
        {
            if (bytes <= 0) return 0;
            return (long)(Math.Round(bytes / (double)MiB) * MiB);
        }

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

        // ---------- Header panel ----------
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
                : myTriangles.ToString("N0", CultureInfo.InvariantCulture);

            var statsLine = $"VRAM: {vramVal}  |  Tris: {triVal}";
            float statsH = ImGui.CalcTextSize(statsLine).Y;
            float buttonH = ImGui.GetFrameHeight();
            float rightH = (2f * scale) + statsH + (3f * scale) + buttonH;

            float vCenterPad = MathF.Max(0f, (rightH - leftH) * 0.5f);

            vCenterPad = MathF.Max(0f, vCenterPad + (4f * scale));

            Vector2 nameSz;
            using (_uiSharedService.UidFont.Push())
                nameSz = ImGui.CalcTextSize(name);

            var uidSz = ImGui.CalcTextSize(uid);


            float minLeft = 128f * scale;
            float maxLeft = contentW * 0.41f;
            float leftW = Math.Clamp(MathF.Max(nameSz.X, uidSz.X) + 8f * scale, minLeft, maxLeft);

            using (ImRaii.PushStyle(ImGuiStyleVar.CellPadding, new Vector2(1f * scale, 2f * scale)))
            using (var hdr = ImRaii.Table("##hdrMain", 2, ImGuiTableFlags.SizingStretchProp))
            {
                if (hdr)
                {
                    ImGui.TableSetupColumn(_uiSharedService.L("UI.CompactUI.07c342be", "l"), ImGuiTableColumnFlags.WidthFixed, leftW);
                    ImGui.TableSetupColumn(_uiSharedService.L("UI.CompactUI.4dc7c9ec", "r"), ImGuiTableColumnFlags.WidthStretch);

                    ImGui.TableNextRow();

                    // LEFT
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

                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.CompactUI.fac312eb", "Setup Vanity (custom ID) here!"));
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

                    // RIGHT
                    ImGui.TableSetColumnIndex(1);
                    ImGui.BeginGroup();

                    ImGui.Dummy(new Vector2(0, 2f * scale));

                    float colW = ImGui.GetColumnWidth();

                    float rightPad = 6f * scale;
                    float innerWR = MathF.Max(0f, colW - (rightPad * 2f));

                    float minStatsScale = 0.80f;

                    {
                        var vramSeg = $"VRAM: {vramVal}";
                        var sepSeg = "  |  ";
                        var trisSeg = $"Tris: {triVal}";

                        float vramW = ImGui.CalcTextSize(vramSeg).X;
                        float sepW = ImGui.CalcTextSize(sepSeg).X;
                        float trisW = ImGui.CalcTextSize(trisSeg).X;

                        float totalW = vramW + sepW + trisW;

                        float scaleStats = (totalW > 0f && innerWR > 0f) ? MathF.Min(1f, innerWR / totalW) : 1f;
                        scaleStats = MathF.Max(minStatsScale, scaleStats);

                        float fontSize = ImGui.GetFontSize() * scaleStats;
                        float lineH = ImGui.GetTextLineHeight();
                        float drawW = totalW * scaleStats;

                        var start = ImGui.GetCursorScreenPos();

                        ImGui.InvisibleButton("##stats_line", new Vector2(colW, lineH));

                        float x = start.X + rightPad + MathF.Max(0f, (innerWR - drawW) * 0.5f);
                        float y = start.Y + (lineH - fontSize) * 0.5f;

                        var dlR = ImGui.GetWindowDrawList();
                        var rMin = start;
                        var rMax = start + new Vector2(colW, lineH);

                        dlR.PushClipRect(rMin, rMax, true);

                        var font = ImGui.GetFont();

                        dlR.AddText(font, fontSize, new Vector2(x, y), ImGui.GetColorU32(ImGuiCol.Text), vramSeg);
                        x += vramW * scaleStats;

                        dlR.AddText(font, fontSize, new Vector2(x, y), ImGui.GetColorU32(ImGuiColors.DalamudGrey2), sepSeg);
                        x += sepW * scaleStats;

                        dlR.AddText(font, fontSize, new Vector2(x, y), ImGui.GetColorU32(ImGuiCol.Text), trisSeg);

                        dlR.PopClipRect();
                    }

                    ImGui.Dummy(new Vector2(0, 1f * scale));

                    bool IconTextButtonAutoFit(FontAwesomeIcon icon, string text, float width, float minTextScale = 0.80f)
                    {
                        var style = ImGui.GetStyle();
                        float h = ImGui.GetFrameHeight();

                        ImGui.PushID(text);
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

                    ImGui.PushStyleVar(ImGuiStyleVar.FrameRounding, 8f * scale);
                    ImGui.PushStyleVar(ImGuiStyleVar.FramePadding, new Vector2(10f, 6f) * scale);

                    //var naturalBtnW = _uiSharedService.GetIconTextButtonSize(FontAwesomeIcon.Recycle, btnLabel);
                    //float useBtnW = MathF.Min(naturalBtnW, innerWR);

                    //var btnStart = ImGui.GetCursorScreenPos();
                    //float btnTargetX = btnStart.X + rightPad + MathF.Max(0f, (innerWR - useBtnW) * 0.5f);
                    //SetCursorScreenPosX(btnTargetX);

                    float useBtnW = innerWR;

                    var btnStart = ImGui.GetCursorScreenPos();
                    float btnTargetX = btnStart.X + rightPad;
                    SetCursorScreenPosX(btnTargetX);

                    using (ImRaii.Disabled(!canClick))
                    {
                        if (IconTextButtonAutoFit(FontAwesomeIcon.Recycle, btnLabel, useBtnW, minTextScale: 0.80f))
                            BeginBc7Conversion(eligibleSet!);
                    }

                    ImGui.PopStyleVar(2);

                    if (ImGui.IsItemHovered())
                    {
                        ImGui.BeginTooltip();
                        if (analysis is null || analysis.Count == 0)
                        {
                            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.f19f2322", "Run Character Analysis to compute your footprint."));
                        }
                        else if (estSaved > 0)
                        {
                            long estAfter = myVramBytes - estSaved;
                            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.ad414934", "Estimated reduction:"));
                            ImGui.Separator();
                            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.3bdf36a7", "Files affected: {0}"), estCount));
                            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.235a7422", "Before: {0}"), UiSharedService.ByteToString(myVramBytes, addSuffix: true)));
                            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.b3286612", "After:  ~{0}"), UiSharedService.ByteToString(estAfter, addSuffix: true)));
                            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.CompactUI.efb6de86", "Saved:  ~{0}"), UiSharedService.ByteToString(estSaved, addSuffix: true)));
                        }
                        else
                        {
                            ImGui.TextUnformatted(_uiSharedService.L("UI.CompactUI.eab7436e", "Nothing found to compress."));
                        }
                        ImGui.EndTooltip();
                    }

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

        // ---------- VRAM panel ----------
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

        var allPairs = _pairManager.PairsWithGroups
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

        bool FilterNotOtherSync(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => !u.Key.AutoPausedByOtherSync;

        bool FilterOnlineOrPausedSelf(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
                ? ((u.Key.IsOnline || (u.Key.UserPair.OwnPermissions.IsPaused() && !u.Key.AutoPausedByOtherSync)) && FilterNotOtherSync(u))
                : (u.Key.IsOnline
                   || (!u.Key.IsOnline && !_configService.Current.ShowOfflineUsersSeparately)
                   || (u.Key.UserPair.OwnPermissions.IsPaused() && !u.Key.AutoPausedByOtherSync))
                  && FilterNotOtherSync(u);

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
            => FilterNotOtherSync(u)
                && u.Key.IsVisible
                && (_configService.Current.ShowSyncshellUsersInVisible || !(!_configService.Current.ShowSyncshellUsersInVisible && !u.Key.IsDirectlyPaired));

        bool FilterTagusers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, string tag)
            => FilterNotOtherSync(u)
                && u.Key.IsDirectlyPaired
                && !u.Key.IsOneSidedPair
                && _tagHandler.HasTag(u.Key.UserData.UID, tag);

        bool FilterGroupUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, GroupFullInfoDto group)
            => FilterNotOtherSync(u) && u.Value.Exists(g => string.Equals(g.GID, group.GID, StringComparison.Ordinal));

        bool FilterNotTaggedUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNotOtherSync(u)
                && u.Key.IsDirectlyPaired
                && !u.Key.IsOneSidedPair
                && !_tagHandler.HasAnyTag(u.Key.UserData.UID);

        bool FilterOfflineUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
                ? (FilterNotOtherSync(u) && !u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused())
                : ((u.Key.IsDirectlyPaired && _configService.Current.ShowSyncshellOfflineUsersSeparately)
                   || !_configService.Current.ShowSyncshellOfflineUsersSeparately)
                  && (!u.Key.IsOneSidedPair || u.Value.Any())
                  && !u.Key.IsOnline
                  && !u.Key.UserPair.OwnPermissions.IsPaused()
                  && FilterNotOtherSync(u);

        bool FilterOfflineSyncshellUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => FilterNotOtherSync(u)
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

    // ---- Cached header computation (prevents frame hitch) ----
    private object? _lastAnalysisRef;
    private long _cachedMyVramBytes;
    private long _cachedMyTriangles;
    private bool _cachedHasEligibleBc7;
    private Dictionary<string, string[]> _cachedEligibleBc7Set = new(StringComparer.Ordinal);
    private List<(string fmt, long size)> _cachedBc7Items = new();
    private (long saved, int count, Dictionary<string, (int files, long before, long after)> byFmt) _cachedBc7Est;
    private long _nextHeaderCacheRefreshMs;

    private bool TryBuildEligibleBc7Set(out Dictionary<string, string[]> set, out List<(string fmt, long size)> sourceItems)
    {
        set = new(StringComparer.Ordinal);
        sourceItems = new();

        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis is null || analysis.Count == 0) return false;

        foreach (var byObject in analysis.Values)
        {
            foreach (var e in byObject.Values)
            {
                if (!string.Equals(e.FileType, "tex", StringComparison.Ordinal)) continue;

                if (string.Equals(e.Format.Value, "BC7", StringComparison.Ordinal)) continue;

                var primary = e.FilePaths[0];
                if (!set.ContainsKey(primary))
                {
                    set[primary] = e.FilePaths.Skip(1).ToArray();
                    sourceItems.Add((e.Format.Value, e.OriginalSize));
                }
            }
        }

        return set.Count > 0;
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
    {
        return TryReadLong(e,
            "TriangleCount", "Triangles", "TriCount", "Tris",
            "NumTriangles", "TriangleCnt");
    }

    private void RefreshHeaderCacheIfNeeded()
    {
        long nowMs = Environment.TickCount64;
        var analysis = _characterAnalyzer.LastAnalysis;

        bool analysisChanged = !ReferenceEquals(_lastAnalysisRef, analysis);

        if (!analysisChanged && nowMs < _nextHeaderCacheRefreshMs)
            return;

        _nextHeaderCacheRefreshMs = nowMs + 500; // 0.5s throttle
        _lastAnalysisRef = analysis;

        _cachedMyVramBytes = 0;
        _cachedMyTriangles = 0;

        _cachedHasEligibleBc7 = false;
        _cachedEligibleBc7Set.Clear();
        _cachedBc7Items.Clear();
        _cachedBc7Est = default;

        if (analysis is null || analysis.Count == 0)
            return;

        var seenHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        KeyValuePair<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>[] snapshot;
        try
        {
            snapshot = analysis.ToArray();
        }
        catch
        {
            // If analysis mutates mid-enum, just skip this refresh.
            return;
        }

        foreach (var kv in snapshot)
        {
            CharacterAnalyzer.FileDataEntry[] entries;
            try
            {
                // Defensive: inner dictionary can mutate while we're enumerating.
                entries = kv.Value.Values.ToArray();
            }
            catch
            {
                continue;
            }

            var gamePathToEntry = new Dictionary<string, CharacterAnalyzer.FileDataEntry>(StringComparer.OrdinalIgnoreCase);

            foreach (var e in entries)
            {
                if (e == null) continue;

                static string NormalizeGp(string p)
                {
                    if (string.IsNullOrWhiteSpace(p)) return string.Empty;
                    return p.ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase);
                }

                if (e.GamePaths != null)
                {
                    foreach (var gpRaw in e.GamePaths)
                    {
                        if (string.IsNullOrWhiteSpace(gpRaw)) continue;

                        var gp = NormalizeGp(gpRaw);
                        if (string.IsNullOrWhiteSpace(gp)) continue;

                        if (!gamePathToEntry.ContainsKey(gp))
                            gamePathToEntry[gp] = e;
                    }
                }

                // Unique-hash VRAM
                if (!string.IsNullOrEmpty(e.Hash) && seenHashes.Add(e.Hash))
                {
                    _cachedMyVramBytes += Math.Max(0, e.VramBytes);
                }

                // Unique model triangles
                if (!string.IsNullOrEmpty(e.Hash)
                    && (string.Equals(e.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                        || (e.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false)))
                {
                    if (seenModelHashes.Add(e.Hash))
                        _cachedMyTriangles += TryReadTriangleCount(e);
                }

                if (!string.Equals(e.FileType, "tex", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (e.FilePaths is null || e.FilePaths.Count == 0)
                    continue;

                if (!TextureCompressionPlanner.TryParseFormat(e.Format.Value, out var srcFmt))
                    continue;

                // best hint: game path (content-type heuristics), else file path
                string? hint = (e.GamePaths != null && e.GamePaths.Count > 0) ? e.GamePaths[0] : e.FilePaths[0];

                if (!TextureCompressionPlanner.TryChooseTarget(hint, srcFmt, out var target) || target == TextureCompressionPlanner.Target.None)
                    continue;

                // If already in target, skip
                if (string.Equals(e.Format.Value, target.ToString(), StringComparison.OrdinalIgnoreCase))
                    continue;

                var primary = e.FilePaths[0];
                if (!_cachedEligibleBc7Set.ContainsKey(primary))
                {
                    _cachedEligibleBc7Set[primary] = e.FilePaths.Skip(1).ToArray();
                    _cachedBc7Items.Add(($"{target}:{e.Format.Value}", e.OriginalSize));
                }
            }
        }

        _cachedHasEligibleBc7 = _cachedEligibleBc7Set.Count > 0;
        _cachedBc7Est = EstimateBc7Savings(_cachedBc7Items);
    }

    private void BeginBc7Conversion(Dictionary<string, string[]> set)
    {
        _bc7Set.Clear();
        foreach (var kv in set) _bc7Set[kv.Key] = kv.Value;

        _bc7Total = _bc7Set.Count;
        _bc7CurFile = string.Empty;
        _bc7CurIndex = 0;

        _bc7Cts.Cancel();
        _bc7Cts.Dispose();
        _bc7Cts = new();

        var plan = new Dictionary<TextureType, Dictionary<string, string[]>>();

        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis != null)
        {
            var primaryToEntry = new Dictionary<string, CharacterAnalyzer.FileDataEntry>(StringComparer.OrdinalIgnoreCase);
            try
            {
                foreach (var byObj in analysis.Values)
                {
                    foreach (var e in byObj.Values)
                    {
                        if (e == null) continue;
                        if (!string.Equals(e.FileType, "tex", StringComparison.OrdinalIgnoreCase)) continue;
                        if (e.FilePaths == null || e.FilePaths.Count == 0) continue;

                        var primary = e.FilePaths[0];
                        if (!primaryToEntry.ContainsKey(primary))
                            primaryToEntry[primary] = e;
                    }
                }
            }
            catch
            {
                primaryToEntry.Clear();
            }

            foreach (var kv in _bc7Set)
            {
                var primary = kv.Key;
                var dups = kv.Value ?? Array.Empty<string>();

                TextureType targetType = TextureType.Bc7Tex;

                if (!plan.TryGetValue(targetType, out var bucket))
                    plan[targetType] = bucket = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

                bucket[primary] = dups;
            }
        }
        else
        {
            // No analysis? Treat it as BC7-only (old behaviour)
            var bucket = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in _bc7Set)
                bucket[kv.Key] = kv.Value ?? Array.Empty<string>();

            plan[TextureType.Bc7Tex] = bucket;
        }

        _bc7Task = _ipcManager.Penumbra.ConvertTextureFiles(
            _logger, plan, _bc7Progress, _bc7Cts.Token);

        _bc7ShowModal = true;
    }


    private (long saved, int count, Dictionary<string, (int files, long before, long after)> byFmt)
    EstimateBc7Savings(List<(string fmt, long size)> items)
    {
        long before = 0, after = 0;
        var byFmt = new Dictionary<string, (int files, long b, long a)>(StringComparer.OrdinalIgnoreCase);

        foreach (var (fmtPacked, size) in items)
        {
            // fmtPacked is either "SRC" (legacy) or "TARGET:SRC"
            var parts = (fmtPacked ?? "").Split(':', 2, StringSplitOptions.RemoveEmptyEntries);
            string tgt = parts.Length == 2 ? parts[0] : "BC7";
            string src = parts.Length == 2 ? parts[1] : parts[0];

            // source bpp
            int srcBpp;
            var s = src.Trim().ToUpperInvariant();
            if (s is "A8R8G8B8" or "R8G8B8A8" or "RGBA8" or "ARGB8" or "X8R8G8B8") srcBpp = 32;
            else if (s is "DXT1" or "BC1" or "BC4") srcBpp = 4;
            else if (s is "DXT3" or "DXT5" or "BC2" or "BC3" or "BC5" or "BC7") srcBpp = 8;
            else srcBpp = 8;

            // target bpp
            int tgtBpp;
            var t = tgt.Trim().ToUpperInvariant();
            if (t is "BC1" or "BC4") tgtBpp = 4;
            else tgtBpp = 8; // BC3/BC5/BC7

            // estimate by bpp ratio
            double factor = (double)tgtBpp / srcBpp;
            long est = (long)Math.Round(size * factor);

            before += size;
            after += est;

            var key = $"{src}→{tgt}";
            if (!byFmt.TryGetValue(key, out var agg)) agg = (0, 0, 0);
            agg.files += 1;
            agg.b += size;
            agg.a += est;
            byFmt[key] = agg;
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

    private static bool DrawCenteredTextAutoFit(string id,string text,Vector4 color,float regionW,float padX,float minScale)
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
}