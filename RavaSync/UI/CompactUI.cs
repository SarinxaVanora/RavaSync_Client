using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Utility;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
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
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using RavaSync.MareConfiguration.Configurations;
using Dalamud.Plugin.Services;



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
    private readonly ICommandManager _commandManager;
    private List<IDrawFolder> _drawFolders;
    private Pair? _lastAddedUser;
    private string _lastAddedUserComment = string.Empty;
    private Vector2 _lastPosition = Vector2.One;
    private Vector2 _lastSize = Vector2.One;
    private bool _showModalForUserAddition;
    private float _transferPartHeight;
    private bool _wasOpen;
    private float _windowContentWidth;
    private Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? _cachedAnalysis;

    private enum PairViewTab
    {
        DirectPairs,
        Shells,
        Visible
    }

    private PairViewTab _currentPairViewTab = PairViewTab.DirectPairs;
    private string _pairSearch = string.Empty;
    private bool _toolsFlyoutOpen = false;


    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();


    public CompactUi(ILogger<CompactUi> logger, UiSharedService uiShared, MareConfigService configService, ApiController apiController, PairManager pairManager,
        ServerConfigurationManager serverManager, MareMediator mediator, FileUploadManager fileTransferManager,
        TagHandler tagHandler, DrawEntityFactory drawEntityFactory, SelectTagForPairUi selectTagForPairUi, SelectPairForTagUi selectPairForTagUi,
        PerformanceCollectorService performanceCollectorService, IpcManager ipcManager, PlayerPerformanceConfigService playerPerformanceConfigService, CharacterAnalyzer characterAnalyzer, TransientConfigService transientConfigService, ICommandManager commandManager)
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
        _commandManager = commandManager;
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
                    ImGui.Text("Open RavaSync Settings");
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
                    ImGui.Text("Open RavaSync Event Viewer");
                    ImGui.EndTooltip();
                }
            }
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

        Flags |= ImGuiWindowFlags.NoDocking;

        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new Vector2(420, 500),
            MaximumSize = new Vector2(420, 2100),
        };
    }

    protected override void DrawInternal()
    {
        _windowContentWidth = UiSharedService.GetWindowContentRegionWidth();
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
            UiSharedService.ColorTextWrapped($"Your RavaSync installation is out of date, the current version is {ver.Major}.{ver.Minor}.{ver.Build}. " +
                $"It is highly recommended to keep RavaSync up to date. Open /xlplugins and update the plugin.", ImGuiColors.DalamudRed);
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

            UiSharedService.ColorTextWrapped($"One or more Plugins essential for RavaSync operation are unavailable. Enable or update following plugins:", ImGuiColors.DalamudRed);
            using var indent = ImRaii.PushIndent(10f);
            if (!penumAvailable)
            {
                UiSharedService.TextWrapped("Penumbra");
                _uiSharedService.BooleanToColoredIcon(penumAvailable, true);
            }
            if (!glamAvailable)
            {
                UiSharedService.TextWrapped("Glamourer");
                _uiSharedService.BooleanToColoredIcon(glamAvailable, true);
            }
            ImGui.Separator();
        }

        using (ImRaii.PushId("header")) DrawUIDHeader();
        ImGui.Separator();
        using (ImRaii.PushId("serverstatus")) DrawServerStatus();
        ImGui.Separator();

        if (_apiController.ServerState is ServerState.Connected)
        {
            // Keep the global top menu (settings / etc.)
            using (ImRaii.PushId("global-topmenu")) _tabMenu.Draw();
            ImGui.Separator();

            // Transfers pane at the top
            using (ImRaii.PushId("transfers")) DrawTransfers();
            ImGui.Separator();

            // Tabs (Direct / Shells / Visible) + always-present search box
            DrawPairViewHeader();
            ImGui.Separator();

            // Main pair list
            using (ImRaii.PushId("pairlist")) DrawPairs();

            // Popups
            using (ImRaii.PushId("group-user-popup")) _selectPairsForGroupUi.Draw(_pairManager.DirectPairs);
            using (ImRaii.PushId("grouping-popup")) _selectGroupForPairUi.Draw();
        }

        if (_configService.Current.OpenPopupOnAdd && _pairManager.LastAddedUser != null)
        {
            _lastAddedUser = _pairManager.LastAddedUser;
            _pairManager.LastAddedUser = null;
            ImGui.OpenPopup("Set Notes for New User");
            _showModalForUserAddition = true;
            _lastAddedUserComment = string.Empty;
        }

        if (ImGui.BeginPopupModal("Set Notes for New User", ref _showModalForUserAddition, UiSharedService.PopupWindowFlags))
        {
            if (_lastAddedUser == null)
            {
                _showModalForUserAddition = false;
            }
            else
            {
                UiSharedService.TextWrapped($"You have successfully added {_lastAddedUser.UserData.AliasOrUID}. Set a local note for the user in the field below:");
                ImGui.InputTextWithHint("##noteforuser", $"Note for {_lastAddedUser.UserData.AliasOrUID}", ref _lastAddedUserComment, 100);
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, "Save Note"))
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
                ImGui.OpenPopup("BC7 Conversion in Progress");
                _bc7ModalOpen = true;
            }

            if (ImGui.BeginPopupModal("BC7 Conversion in Progress"))
            {
                ImGui.TextUnformatted($"BC7 Conversion in progress: {_bc7CurIndex}/{_bc7Total}");
                UiSharedService.TextWrapped($"Current file: {_bc7CurFile}");
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StopCircle, "Cancel conversion"))
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

        // Main pair list – full width
        ImGui.BeginChild("pairlist-main", new Vector2(_windowContentWidth, 0), border: false);

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
        DrawTab("Direct pairs", PairViewTab.DirectPairs);
        ImGui.SameLine();
        DrawTab("Shells", PairViewTab.Shells);
        ImGui.SameLine();
        DrawTab("Visible", PairViewTab.Visible);

        // Search box, right-aligned on the same row
        ImGui.SameLine();

        float searchWidth = contentWidth - ImGui.GetCursorPosX() - style.WindowPadding.X;
        if (searchWidth < 150f * ImGuiHelpers.GlobalScale)
            searchWidth = 150f * ImGuiHelpers.GlobalScale;

        ImGui.SetNextItemWidth(searchWidth);
        bool searchChanged = ImGui.InputTextWithHint("##pairSearch",
            "Search pairs…", ref _pairSearch, 128,
            ImGuiInputTextFlags.None);

        if (tabChanged || searchChanged)
        {
            _drawFolders = GetDrawFolders().ToList();
        }
    }

    private void DrawServerStatus()
    {
        var buttonSize = _uiSharedService.GetIconButtonSize(FontAwesomeIcon.Link);
        var userCount = _apiController.OnlineUsers.ToString(CultureInfo.InvariantCulture);
        var userSize = ImGui.CalcTextSize(userCount);
        var textSize = ImGui.CalcTextSize("Users Online");
#if DEBUG
        string shardConnection = $"Shard: {_apiController.ServerInfo.ShardName}";
#else
        string shardConnection = string.Equals(_apiController.ServerInfo.ShardName, "Main", StringComparison.OrdinalIgnoreCase) ? string.Empty : $"Shard: {_apiController.ServerInfo.ShardName}";
#endif
        var shardTextSize = ImGui.CalcTextSize(shardConnection);
        var printShard = !string.IsNullOrEmpty(_apiController.ServerInfo.ShardName) && shardConnection != string.Empty;

        if (_apiController.ServerState is ServerState.Connected)
        {
            ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMin().X + UiSharedService.GetWindowContentRegionWidth()) / 2 - (userSize.X + textSize.X) / 2 - ImGui.GetStyle().ItemSpacing.X / 2);
            if (!printShard) ImGui.AlignTextToFramePadding();
            ImGui.TextColored(ImGuiColors.ParsedGreen, userCount);
            ImGui.SameLine();
            if (!printShard) ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Users Online");
        }
        else
        {
            ImGui.AlignTextToFramePadding();
            ImGui.TextColored(ImGuiColors.DalamudRed, "Not connected to any server");
        }

        if (printShard)
        {
            ImGui.SetCursorPosY(ImGui.GetCursorPosY() - ImGui.GetStyle().ItemSpacing.Y);
            ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMin().X + UiSharedService.GetWindowContentRegionWidth()) / 2 - shardTextSize.X / 2);
            ImGui.TextUnformatted(shardConnection);
        }

        ImGui.SameLine();
        if (printShard)
        {
            ImGui.SetCursorPosY(ImGui.GetCursorPosY() - ((userSize.Y + textSize.Y) / 2 + shardTextSize.Y) / 2 - ImGui.GetStyle().ItemSpacing.Y + buttonSize.Y / 2);
        }
        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;
        var color = UiSharedService.GetBoolColor(!isConnectingOrConnected);
        var connectedIcon = isConnectingOrConnected ? FontAwesomeIcon.Unlink : FontAwesomeIcon.Link;

        ImGui.SameLine(ImGui.GetWindowContentRegionMin().X + UiSharedService.GetWindowContentRegionWidth() - buttonSize.X);
        if (printShard)
        {
            ImGui.SetCursorPosY(ImGui.GetCursorPosY() - ((userSize.Y + textSize.Y) / 2 + shardTextSize.Y) / 2 - ImGui.GetStyle().ItemSpacing.Y + buttonSize.Y / 2);
        }

        if (_apiController.ServerState is not (ServerState.Reconnecting or ServerState.Disconnecting))
        {
            using (ImRaii.PushColor(ImGuiCol.Text, color))
            {
                if (_uiSharedService.IconButton(connectedIcon))
                {
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
            }

            UiSharedService.AttachToolTip(isConnectingOrConnected ? "Disconnect from " + _serverManager.CurrentServer.ServerName : "Connect to " + _serverManager.CurrentServer.ServerName);
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
            ImGui.TextUnformatted("No uploads in progress");
        }

        var currentDownloads = _currentDownloads.SelectMany(d => d.Value.Values).ToList();
        ImGui.AlignTextToFramePadding();
        _uiSharedService.IconText(FontAwesomeIcon.Download);
        ImGui.SameLine(35 * ImGuiHelpers.GlobalScale);

        if (currentDownloads.Any())
        {
            var totalDownloads = currentDownloads.Sum(c => c.TotalFiles);
            var doneDownloads = currentDownloads.Sum(c => c.TransferredFiles);
            var totalDownloaded = currentDownloads.Sum(c => c.TransferredBytes);
            var totalToDownload = currentDownloads.Sum(c => c.TotalBytes);

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
            ImGui.TextUnformatted("No downloads in progress");
        }
    }

    private void DrawUIDHeader()
    {
        var uidText = GetUidText();

        using (_uiSharedService.UidFont.Push())
        {
            var uidTextSize = ImGui.CalcTextSize(uidText);
            ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2 - (uidTextSize.X / 2));
            ImGui.TextColored(GetUidColor(), uidText);
        }

        if (_apiController.ServerState is ServerState.Connected)
        {
            if (ImGui.IsItemClicked())
            {
                ImGui.SetClipboardText(_apiController.DisplayName);
            }
            UiSharedService.AttachToolTip("Click to copy");

            if (!string.Equals(_apiController.DisplayName, _apiController.UID, StringComparison.Ordinal))
            {
                var origTextSize = ImGui.CalcTextSize(_apiController.UID);
                ImGui.SetCursorPosX((ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2 - (origTextSize.X / 2));
                ImGui.TextColored(GetUidColor(), _apiController.UID);
                if (ImGui.IsItemClicked())
                {
                    ImGui.SetClipboardText(_apiController.UID);
                }
                UiSharedService.AttachToolTip("Click to copy");
            }

            Vector4 color;
            // --- Current logged-in player's VRAM footprint (from Character Analysis) ---
            long myVramBytes = 0;

            // take a safe snapshot to avoid null / concurrent mutation issues
            var analysis = _characterAnalyzer.LastAnalysis;
            if (analysis is not null && analysis.Count > 0)
            {
                // shallow snapshot to avoid "collection was modified" during draw
                var snapshot = analysis.ToArray();
                foreach (var kv in snapshot) // kv.Value : Dictionary<string, CharacterAnalyzer.FileDataEntry>
                {
                    myVramBytes += kv.Value.Values.Sum(e => e.OriginalSize);
                }
            }

            // Add a little vertical spacing before the line
            ImGui.Spacing();

            // Compose and center the line
            string mySize = $"Your current size: {UiSharedService.ByteToString(myVramBytes, addSuffix: true)}";
            var myTextSize = ImGui.CalcTextSize(mySize);
            float centerX2 = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2f - (myTextSize.X / 2f);
            ImGui.SetCursorPosX(centerX2);

            ImGui.TextColored(ImGuiColors.DalamudWhite, mySize);

            // One-click BC7 conversion button (enabled only when eligible non-BC7 textures exist)
            Dictionary<string, string[]>? eligibleSet;
            List<(string fmt, long size)>? items;
            bool hasEligible = TryBuildEligibleBc7Set(out eligibleSet!, out items!);

            // Estimate on the same items the converter will touch
            var (estSaved, estCount, byFmt) = EstimateBc7Savings(items);

            var btnLabel = estSaved > 0
                ? $"Reduce my size (~{UiSharedService.ByteToString(estSaved, addSuffix: true)})"
                : "Reduce my size";

            // center button under the line
            var btnSz = ImGui.CalcTextSize(btnLabel);
            float centerBtnX = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2f
                             - (btnSz.X + ImGui.GetStyle().FramePadding.X * 2f) / 2f;
            ImGui.SetCursorPosX(centerBtnX);

            // Only enable if we have anything to convert and no active job
            bool canClick = hasEligible && (_bc7Task == null || _bc7Task.IsCompleted);
            using (ImRaii.Disabled(!canClick))
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Recycle, btnLabel))
                {
                    BeginBc7Conversion(eligibleSet!);
                }
            }

            //why the estimate is what it is
            if (ImGui.IsItemHovered())
            {
                ImGui.BeginTooltip();

                if (estSaved > 0)
                {
                    long estAfter = myVramBytes - estSaved;
                    ImGui.TextUnformatted("Estimated reduction:");
                    ImGui.Separator();
                    ImGui.TextUnformatted($"Files affected: {estCount}");
                    ImGui.TextUnformatted($"Before: {UiSharedService.ByteToString(myVramBytes, addSuffix: true)}");
                    ImGui.TextUnformatted($"After:  ~{UiSharedService.ByteToString(estAfter, addSuffix: true)}");
                    ImGui.TextUnformatted($"Saved:  ~{UiSharedService.ByteToString(estSaved, addSuffix: true)}");
                }
                else
                {
                    ImGui.TextUnformatted("Nothing found to compress.");
                }

                ImGui.EndTooltip();
            }


            //setup Vram Use output
            long pairsVramBytes = _pairManager.PairsWithGroups.Keys
                .Where(p =>
                    p.IsDirectlyPaired
                    && p.IsVisible
                    && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
                .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

            long shellsVramBytes = _pairManager.PairsWithGroups.Keys
                .Where(p =>
                    !p.IsDirectlyPaired
                    && p.IsVisible
                    && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
                .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

            long totalVramBytes = pairsVramBytes + shellsVramBytes;

            // Pick color for shells based on Syncshell cap
            Vector4 shellsColor;
            if (_playerPerformanceConfigService.Current.SyncshellVramCapMiB > 0)
            {
                var capBytes = (long)_playerPerformanceConfigService.Current.SyncshellVramCapMiB * 1024 * 1024;
                float usagePercent = (float)shellsVramBytes / capBytes;

                if (usagePercent < 0.5f)
                    shellsColor = ImGuiColors.HealerGreen; // safe
                else if (usagePercent < 0.8f)
                    shellsColor = ImGuiColors.TankBlue;    // medium
                else if (usagePercent < 1f)
                    shellsColor = ImGuiColors.DalamudYellow;   // warning
                else
                    shellsColor = ImGuiColors.DPSRed;      // over cap
            }
            else
            {
                shellsColor = ImGuiColors.DalamudWhite; // no cap set
            }

            // Build the mixed-color breakdown
            string pairsText = $"Pairs: {UiSharedService.ByteToString(pairsVramBytes, addSuffix: true)}";
            string shellsText = $"Shells: {UiSharedService.ByteToString(shellsVramBytes, addSuffix: true)}";

            string prefix = "VRAM use - ";
            var prefixSize = ImGui.CalcTextSize(prefix);
            var pairsSize = ImGui.CalcTextSize(pairsText + "  ");
            var shellsSize = ImGui.CalcTextSize(shellsText);

            float breakdownWidth = prefixSize.X + pairsSize.X + shellsSize.X;
            float centerXBreakdown = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2f - (breakdownWidth / 2f);
            ImGui.SetCursorPosX(centerXBreakdown);

            // Render with mixed colors
            ImGui.TextColored(ImGuiColors.DalamudWhite, prefix);
            ImGui.SameLine(0, 0);
            ImGui.TextColored(ImGuiColors.DalamudWhite, pairsText + "  ");
            ImGui.SameLine(0, 0);
            ImGui.TextColored(shellsColor, shellsText);

            // Second line: total, always white, centered
            string totalText = $"Total: {UiSharedService.ByteToString(totalVramBytes, addSuffix: true)}";
            var totalSize = ImGui.CalcTextSize(totalText);
            float centerXTotal = (ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X) / 2f - (totalSize.X / 2f);
            ImGui.SetCursorPosX(centerXTotal);
            ImGui.TextColored(ImGuiColors.DalamudWhite, totalText);

            ImGui.Spacing();
            ImGui.Separator();
            DrawScopeToggles();
        }
        else
        {
            UiSharedService.ColorTextWrapped(GetServerError(), GetUidColor());
        }
    }
    private bool PairMatchesCurrentTab(Pair pair)
    {
        return _currentPairViewTab switch
        {
            PairViewTab.DirectPairs => pair.IsDirectlyPaired,
            PairViewTab.Shells => !pair.IsDirectlyPaired,
            PairViewTab.Visible => pair.IsVisible,
            _ => true
        };
    }

    private IEnumerable<IDrawFolder> GetDrawFolders()
    {
        List<IDrawFolder> drawFolders = [];

        bool isDirectTab = _currentPairViewTab == PairViewTab.DirectPairs;
        bool isShellTab = _currentPairViewTab == PairViewTab.Shells;


        var allPairs = _pairManager.PairsWithGroups
            // First, filter based on the current view tab
            .Where(entry =>
            {
                return _currentPairViewTab switch
                {
                    PairViewTab.DirectPairs => entry.Key.IsDirectlyPaired,   // no shells
                    PairViewTab.Shells => !entry.Key.IsDirectlyPaired,  // shells only
                    _ => true                          // Visible / anything else
                };
            })
            .ToDictionary(k => k.Key, k => k.Value);

        // Then apply the always-present search box
        var filteredPairs = allPairs
            .Where(p =>
            {
                if (_pairSearch.IsNullOrEmpty()) return true;

                var term = _pairSearch;
                return p.Key.UserData.AliasOrUID.Contains(term, StringComparison.OrdinalIgnoreCase) ||
                       (p.Key.GetNote()?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false) ||
                       (p.Key.PlayerName?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false);
            })
            .ToDictionary(k => k.Key, k => k.Value);


        string? AlphabeticalSort(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => (_configService.Current.ShowCharacterNameInsteadOfNotesForVisible && !string.IsNullOrEmpty(u.Key.PlayerName)
                    ? (_configService.Current.PreferNotesOverNamesForVisible ? u.Key.GetNote() : u.Key.PlayerName)
                    : (u.Key.GetNote() ?? u.Key.UserData.AliasOrUID));
        bool FilterOnlineOrPausedSelf(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
            ? (u.Key.IsOnline || u.Key.UserPair.OwnPermissions.IsPaused())
            : (u.Key.IsOnline
               || (!u.Key.IsOnline && !_configService.Current.ShowOfflineUsersSeparately)
               || u.Key.UserPair.OwnPermissions.IsPaused());

        //bool FilterOnlineOrPausedSelf(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
        //    => (u.Key.IsOnline || (!u.Key.IsOnline && !_configService.Current.ShowOfflineUsersSeparately)
        //            || u.Key.UserPair.OwnPermissions.IsPaused());
        //Dictionary<Pair, List<GroupFullInfoDto>> BasicSortedDictionary(IEnumerable<KeyValuePair<Pair, List<GroupFullInfoDto>>> u)
        //    => u.OrderByDescending(u => u.Key.IsVisible)
        //        .ThenByDescending(u => u.Key.IsOnline)
        //        .ThenBy(AlphabeticalSort, StringComparer.OrdinalIgnoreCase)
        //        .ToDictionary(u => u.Key, u => u.Value);
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
            => u.Key.IsVisible
                && (_configService.Current.ShowSyncshellUsersInVisible || !(!_configService.Current.ShowSyncshellUsersInVisible && !u.Key.IsDirectlyPaired));
        bool FilterTagusers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, string tag)
            => u.Key.IsDirectlyPaired && !u.Key.IsOneSidedPair && _tagHandler.HasTag(u.Key.UserData.UID, tag);
        bool FilterGroupUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u, GroupFullInfoDto group)
            => u.Value.Exists(g => string.Equals(g.GID, group.GID, StringComparison.Ordinal));
        bool FilterNotTaggedUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => u.Key.IsDirectlyPaired && !u.Key.IsOneSidedPair && !_tagHandler.HasAnyTag(u.Key.UserData.UID);
        //bool FilterOfflineUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
        //    => ((u.Key.IsDirectlyPaired && _configService.Current.ShowSyncshellOfflineUsersSeparately)
        //        || !_configService.Current.ShowSyncshellOfflineUsersSeparately)
        //        && (!u.Key.IsOneSidedPair || u.Value.Any()) && !u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused();

        bool FilterOfflineUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => isDirectTab
            ? (!u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused())
            : ((u.Key.IsDirectlyPaired && _configService.Current.ShowSyncshellOfflineUsersSeparately)
            || !_configService.Current.ShowSyncshellOfflineUsersSeparately)
            && (!u.Key.IsOneSidedPair || u.Value.Any())
            && !u.Key.IsOnline
            && !u.Key.UserPair.OwnPermissions.IsPaused();
        bool FilterOfflineSyncshellUsers(KeyValuePair<Pair, List<GroupFullInfoDto>> u)
            => (!u.Key.IsDirectlyPaired && !u.Key.IsOnline && !u.Key.UserPair.OwnPermissions.IsPaused());


        if (_currentPairViewTab == PairViewTab.Visible)
        {
            var allVisiblePairs = ImmutablePairList(allPairs.Where(FilterVisibleUsers));
            var filteredVisiblePairs = BasicSortedDictionary(filteredPairs.Where(FilterVisibleUsers));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                TagHandler.CustomVisibleTag,
                filteredVisiblePairs,
                allVisiblePairs));

            return drawFolders;
        }

        // Normal behaviour when *not* on the Visible tab
        if (_configService.Current.ShowVisibleUsersSeparately)
        {
            var allVisiblePairs = ImmutablePairList(allPairs.Where(FilterVisibleUsers));
            var filteredVisiblePairs = BasicSortedDictionary(filteredPairs.Where(FilterVisibleUsers));

            drawFolders.Add(_drawEntityFactory.CreateDrawTagFolder(
                TagHandler.CustomVisibleTag,
                filteredVisiblePairs,
                allVisiblePairs));
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
        IsOpen = _wasOpen;
    }

    private void UiSharedService_GposeStart()
    {
        _wasOpen = IsOpen;
        IsOpen = false;
    }


    // --- BC7 one-click conversion state
    private readonly Progress<(string fileName, int index)> _bc7Progress = new();
    private CancellationTokenSource _bc7Cts = new();
    private Task? _bc7Task;
    private string _bc7CurFile = string.Empty;
    private int _bc7CurIndex = 0;
    private int _bc7Total = 0;
    private bool _bc7ShowModal = false;
    private bool _bc7ModalOpen = false;
    private readonly Dictionary<string, string[]> _bc7Set = new(StringComparer.Ordinal);

    // Build mapping of non-BC7 textures from the latest CharacterAnalyzer snapshot.
    // Key = primary file path, Value = duplicate paths to also convert.
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

                // converter includes everything that isn't already BC7
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

        _bc7Task = _ipcManager.Penumbra.ConvertTextureFiles(
            _logger, _bc7Set, _bc7Progress, _bc7Cts.Token);

        // open our own modal; Analysis window stays closed
        _bc7ShowModal = true;
    }

    // --- Format → bits-per-pixel mapping for size estimation
    private static bool TryGetBppForFormat(string fmt, out int bpp)
    {
        fmt = fmt?.Trim()?.ToUpperInvariant() ?? "";

        // uncompressed 32 bpp common aliases
        if (fmt is "A8R8G8B8" or "R8G8B8A8" or "RGBA8" or "ARGB8" or "X8R8G8B8") { bpp = 32; return true; }

        // block-compressed 4 bpp
        if (fmt is "DXT1" or "BC1" or "BC4") { bpp = 4; return true; }

        // block-compressed 8 bpp
        if (fmt is "DXT3" or "DXT5" or "BC2" or "BC3" or "BC5" or "BC7") { bpp = 8; return true; }

        // HDR etc (treat as 8 bpp for conservative estimate if unknown)
        bpp = 8; // safe default so we don't claim savings incorrectly
        return false;
    }

    // Estimate using the exact items we’ll convert.
    // Returns (before, after, deltaSaved, count, perFormat)
    private (long saved, int count, Dictionary<string, (int files, long before, long after)> byFmt)
        EstimateBc7Savings(List<(string fmt, long size)> items)
    {
        long before = 0, after = 0;
        var byFmt = new Dictionary<string, (int files, long b, long a)>(StringComparer.OrdinalIgnoreCase);
        const int bc7Bpp = 8;

        foreach (var (fmt, size) in items)
        {
            TryGetBppForFormat(fmt, out var srcBpp); // returns 8 by default if unknown
                                                     // scale by bpp ratio; this matches what the converter does in spirit
            double factor = (double)bc7Bpp / srcBpp;
            long est = (long)Math.Round(size * factor);

            before += size;
            after += est;

            if (!byFmt.TryGetValue(fmt, out var agg)) agg = (0, 0, 0);
            agg.files += 1;
            agg.b += size;
            agg.a += est;
            byFmt[fmt] = agg;
        }

        return (before - after, items.Count, byFmt);
    }
    private void DrawScopeToggles()
    {
        var transient = _transientConfigService.Current;
        var selected = (ScopeMode)transient.SelectedScopeMode;

        using (ImRaii.PushId("ScopeToggles"))
        {
            // --- Measurements
            float contentWidth = UiSharedService.GetWindowContentRegionWidth();
            var style = ImGui.GetStyle();

            // Header (centered)
            const string header = "Limit Sync to:";
            var headerSize = ImGui.CalcTextSize(header);
            ImGui.SetCursorPosX((contentWidth - headerSize.X) * 0.5f);
            ImGui.TextUnformatted(header);

            // Small vertical gap under header
            ImGui.Dummy(new Vector2(0, 2f * ImGuiHelpers.GlobalScale));

            // Fixed cell width for consistency
            float cellWidth = 100f * ImGuiHelpers.GlobalScale;
            float spacingX = style.ItemSpacing.X;
            float rowWidth = (cellWidth * 3f) + (spacingX * 2f);

            // Center the entire row
            ImGui.SetCursorPosX((contentWidth - rowWidth) * 0.5f);

            // Helper to draw a single toggle cell
            void DrawToggle(string label, ScopeMode mode)
            {
                bool isSet = (selected == mode);
                if (ImGui.RadioButton(label, isSet) && !isSet)
                    selected = mode;
            }

            // Row: Everyone | Friends | Party | Alliance
            float startX = ImGui.GetCursorPosX();
            DrawToggle("Everyone", ScopeMode.Everyone);

            ImGui.SameLine(startX + cellWidth - 0.5f);
            DrawToggle("Friends", ScopeMode.Friends);

            ImGui.SameLine(startX + (cellWidth * 1.85f));
            DrawToggle("Party", ScopeMode.Party);

            ImGui.SameLine(startX + (cellWidth * 2.55f));
            DrawToggle("Alliance", ScopeMode.Alliance);

            // Small padding below
            ImGui.Spacing();

            // Apply selection change
            if ((ScopeMode)transient.SelectedScopeMode != selected)
            {
                transient.SelectedScopeMode = (int)selected;
                _transientConfigService.Save();
                Mediator.Publish(new ScopeModeChangedMessage(selected));
            }
        }
    }





}