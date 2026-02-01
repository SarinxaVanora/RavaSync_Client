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
    private List<IDrawFolder> _drawFolders;
    private Pair? _lastAddedUser;
    private string _lastAddedUserComment = string.Empty;
    private Vector2 _lastPosition = Vector2.One;
    private Vector2 _lastSize = Vector2.One;
    private bool _showModalForUserAddition;
    private bool _wasOpen;
    private float _windowContentWidth;

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
        PerformanceCollectorService performanceCollectorService, IpcManager ipcManager, PlayerPerformanceConfigService playerPerformanceConfigService, CharacterAnalyzer characterAnalyzer, TransientConfigService transientConfigService)
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
            using (ImRaii.PushId("global-topmenu")) _tabMenu.Draw();
            ImGui.Separator();

            using (ImRaii.PushId("transfers")) DrawTransfers();
            ImGui.Separator();

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
        var contentW = UiSharedService.GetWindowContentRegionWidth();
        var scale = ImGuiHelpers.GlobalScale;

        if (_apiController.ServerState is not ServerState.Connected)
        {
            var text = GetServerError();
            if (!string.IsNullOrEmpty(text))
                UiSharedService.ColorTextWrapped(text, GetUidColor());
            else
                UiSharedService.ColorTextWrapped(GetUidText(), GetUidColor());
            return;
        }

        long myVramBytes = 0;
        long myTriangles = 0;

        var analysis = _characterAnalyzer.LastAnalysis;
        if (analysis is not null && analysis.Count > 0)
        {
            var snapshot = analysis.ToArray();
            foreach (var kv in snapshot)
            {
                foreach (var e in kv.Value.Values)
                {
                    myVramBytes += e.OriginalSize;
                    myTriangles += TryReadTriangleCount(e);
                }
            }
        }

        Dictionary<string, string[]>? eligibleSet;
        List<(string fmt, long size)>? items;
        bool hasEligible = TryBuildEligibleBc7Set(out eligibleSet!, out items!);
        var (estSaved, estCount, _) = EstimateBc7Savings(items);

        var btnLabel = estSaved > 0
            ? $"Reduce my size (~{UiSharedService.ByteToString(estSaved, addSuffix: true)})"
            : "Reduce my size";

        bool canClick = hasEligible && (_bc7Task == null || _bc7Task.IsCompleted);

        long pairsVramBytes = _pairManager.PairsWithGroups.Keys
            .Where(p => p.IsDirectlyPaired && p.IsVisible && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
            .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

        long shellsVramBytes = _pairManager.PairsWithGroups.Keys
            .Where(p => !p.IsDirectlyPaired && p.IsVisible && !(p.UserPair.OwnPermissions.IsPaused() || p.AutoPausedByCap))
            .Sum(p => System.Math.Max(0, p.LastAppliedApproximateVRAMBytes));

        long totalVramBytes = pairsVramBytes + shellsVramBytes;

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

            var name = _apiController.DisplayName ?? string.Empty;
            var uid = _apiController.UID ?? string.Empty;

            bool needsVanity = !string.IsNullOrEmpty(uid) && string.Equals(name, uid, StringComparison.Ordinal);

            float nameH;
            using (_uiSharedService.UidFont.Push())
                nameH = ImGui.GetTextLineHeight();

            float uidH = string.IsNullOrEmpty(uid) ? 0f : ImGui.CalcTextSize(uid).Y;

            float firstLineH = needsVanity ? ImGui.GetFrameHeight() : nameH;
            float leftH = firstLineH + (uidH > 0f ? (ImGui.GetStyle().ItemSpacing.Y + uidH) : 0f);


            var vramVal = (analysis is null || analysis.Count == 0)
                ? "—"
                : UiSharedService.ByteToString(myVramBytes, addSuffix: true);

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


            float minLeft = 160f * scale;
            float maxLeft = contentW * 0.50f;
            float leftW = Math.Clamp(MathF.Max(nameSz.X, uidSz.X) + 18f * scale, minLeft, maxLeft);

            using (var hdr = ImRaii.Table("##hdrMain", 2, ImGuiTableFlags.SizingStretchProp))
            {
                if (hdr)
                {
                    ImGui.TableSetupColumn("l", ImGuiTableColumnFlags.WidthFixed, leftW);
                    ImGui.TableSetupColumn("r", ImGuiTableColumnFlags.WidthStretch);

                    ImGui.TableNextRow();

                    // LEFT
                    ImGui.TableSetColumnIndex(0);
                    ImGui.BeginGroup();

                    if (vCenterPad > 0f)
                        ImGui.Dummy(new Vector2(0, vCenterPad));

                    float colW0 = ImGui.GetColumnWidth();
                    float padXLeft = 10f * scale;

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

                        UiSharedService.AttachToolTip("Setup Vanity (custom ID) here!");
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

                        UiSharedService.AttachToolTip("Click to copy");
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

                        UiSharedService.AttachToolTip("Click to copy");
                    }

                    ImGui.EndGroup();

                    // RIGHT
                    ImGui.TableSetColumnIndex(1);
                    ImGui.BeginGroup();

                    ImGui.Dummy(new Vector2(0, 2f * scale));

                    float colW = ImGui.GetColumnWidth();

                    float rightPad = 12f * scale;
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
                        float iconX = rMin.X + padX;
                        float iconFontSize = ImGui.GetFontSize();
                        float iconY = rMin.Y + (h - iconFontSize) * 0.5f;

                        using (_uiSharedService.IconFont.Push())
                            dl.AddText(new Vector2(iconX, iconY), ImGui.GetColorU32(ImGuiCol.Text), iconStr);

                        float textX = iconX + iconSz.X + gap;
                        float padW = padX * 2f;
                        float maxTextW = MathF.Max(0f, width - (iconSz.X + gap + padW));

                        float baseTextW = ImGui.CalcTextSize(text).X;
                        float tScale = (baseTextW > 0f && maxTextW > 0f) ? MathF.Min(1f, maxTextW / baseTextW) : 1f;
                        tScale = MathF.Max(minTextScale, tScale);

                        var font = ImGui.GetFont();
                        float textFontSize = ImGui.GetFontSize() * tScale;
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

                    var naturalBtnW = _uiSharedService.GetIconTextButtonSize(FontAwesomeIcon.Recycle, btnLabel);
                    float useBtnW = MathF.Min(naturalBtnW, innerWR);

                    var btnStart = ImGui.GetCursorScreenPos();
                    float btnTargetX = btnStart.X + rightPad + MathF.Max(0f, (innerWR - useBtnW) * 0.5f);
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
                            ImGui.TextUnformatted("Run Character Analysis to compute your footprint.");
                        }
                        else if (estSaved > 0)
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

        // ---------- VRAM panel ----------
        {
            var dl = ImGui.GetWindowDrawList();
            var start = ImGui.GetCursorScreenPos();

            ImGui.BeginGroup();

            float padX = 12f * scale;
            float rightPadX = 12f * scale; 

            var title = "VRAM use";
            var titleSz = ImGui.CalcTextSize(title);
            ImGui.SetCursorPosX(MathF.Max(0f, (contentW - titleSz.X) * 0.5f));
            ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey2);
            ImGui.TextUnformatted(title);
            ImGui.PopStyleColor();


            var totalText = $"Total {UiSharedService.ByteToString(totalVramBytes, addSuffix: true)}";
            var totalSz = ImGui.CalcTextSize(totalText);

            var contentMinX = ImGui.GetWindowContentRegionMin().X;
            var contentMaxX = ImGui.GetWindowContentRegionMax().X;

            // LEFT
            ImGui.SetCursorPosX(contentMinX + padX);
            ImGui.AlignTextToFramePadding();

            ImGui.TextUnformatted($"Pairs {UiSharedService.ByteToString(pairsVramBytes, addSuffix: true)}");
            ImGui.SameLine(0, 0);
            ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey2);
            ImGui.TextUnformatted("  |  ");
            ImGui.PopStyleColor();
            ImGui.SameLine(0, 0);
            ImGui.TextUnformatted("Shells ");
            ImGui.SameLine(0, 0);
            ImGui.PushStyleColor(ImGuiCol.Text, shellsColor);
            ImGui.TextUnformatted(UiSharedService.ByteToString(shellsVramBytes, addSuffix: true));
            ImGui.PopStyleColor();

            // RIGHT
            ImGui.SameLine();
            ImGui.SetCursorPosX(MathF.Max(contentMinX + padX, contentMaxX - padX - totalSz.X));
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(totalText);


            ImGui.EndGroup();

            var end = ImGui.GetCursorScreenPos();
            var w = contentW;
            var h = MathF.Max(0f, end.Y - start.Y);

            var bg = ImGui.GetColorU32(new Vector4(0.40f, 0.20f, 0.60f, 0.10f));
            var border = ImGui.GetColorU32(new Vector4(0.60f, 0.35f, 0.90f, 0.18f));

            dl.AddRectFilled(start, start + new Vector2(w, h), bg, 10f * scale);
            dl.AddRect(start, start + new Vector2(w, h), border, 10f * scale);

            //ImGui.Dummy(new Vector2(0, 1f * scale));
        }

        ImGui.Separator();
        ImGui.Dummy(new Vector2(0, 0.5f * scale));

        DrawScopeToggles();
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
                    PairViewTab.DirectPairs => entry.Key.IsDirectlyPaired,   
                    PairViewTab.Shells => !entry.Key.IsDirectlyPaired,  
                    _ => true                        
                };
            })
            .ToDictionary(k => k.Key, k => k.Value);

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


    private readonly Progress<(string fileName, int index)> _bc7Progress = new();
    private CancellationTokenSource _bc7Cts = new();
    private Task? _bc7Task;
    private string _bc7CurFile = string.Empty;
    private int _bc7CurIndex = 0;
    private int _bc7Total = 0;
    private bool _bc7ShowModal = false;
    private bool _bc7ModalOpen = false;
    private readonly Dictionary<string, string[]> _bc7Set = new(StringComparer.Ordinal);

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

        _bc7ShowModal = true;
    }

    private static bool TryGetBppForFormat(string fmt, out int bpp)
    {
        fmt = fmt?.Trim()?.ToUpperInvariant() ?? "";

        if (fmt is "A8R8G8B8" or "R8G8B8A8" or "RGBA8" or "ARGB8" or "X8R8G8B8") { bpp = 32; return true; }

        if (fmt is "DXT1" or "BC1" or "BC4") { bpp = 4; return true; }

        if (fmt is "DXT3" or "DXT5" or "BC2" or "BC3" or "BC5" or "BC7") { bpp = 8; return true; }

        bpp = 8; // safe default so we don't claim savings incorrectly
        return false;
    }


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
            float contentWidth = ImGui.GetContentRegionAvail().X;
            float baseX = ImGui.GetCursorPosX();

            var style = ImGui.GetStyle();

            ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey2);
            const string l = "Limit Sync to:";
            var lsz = ImGui.CalcTextSize(l);
            ImGui.SetCursorPosX(MathF.Max(0f, (UiSharedService.GetWindowContentRegionWidth() - lsz.X) * 0.5f));
            ImGui.TextUnformatted(l);
            ImGui.PopStyleColor();

            //ImGui.Dummy(new Vector2(0, 0.5f * ImGuiHelpers.GlobalScale));

            string[] names = ["Pairs & Shells", "Friends", "Party", "Alliance"];
            ScopeMode[] modes = [ScopeMode.Everyone, ScopeMode.Friends, ScopeMode.Party, ScopeMode.Alliance];

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

}