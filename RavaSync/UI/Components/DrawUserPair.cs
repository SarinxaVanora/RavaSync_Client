using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.User;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI.Handlers;
using RavaSync.WebAPI;

namespace RavaSync.UI.Components;

public class DrawUserPair
{
    protected readonly ApiController _apiController;
    protected readonly IdDisplayHandler _displayHandler;
    protected readonly MareMediator _mediator;
    protected readonly List<GroupFullInfoDto> _syncedGroups;
    private readonly GroupFullInfoDto? _currentGroup;
    protected Pair _pair;
    private readonly string _id;
    private readonly SelectTagForPairUi _selectTagForPairUi;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly UiSharedService _uiSharedService;
    private readonly PlayerPerformanceConfigService _performanceConfigService;
    private readonly CharaDataManager _charaDataManager;
    private float _menuWidth = -1;
    private bool _wasHovered = false;

    public DrawUserPair(string id, Pair entry, List<GroupFullInfoDto> syncedGroups,
        GroupFullInfoDto? currentGroup,
        ApiController apiController, IdDisplayHandler uIDDisplayHandler,
        MareMediator mareMediator, SelectTagForPairUi selectTagForPairUi,
        ServerConfigurationManager serverConfigurationManager,
        UiSharedService uiSharedService, PlayerPerformanceConfigService performanceConfigService,
        CharaDataManager charaDataManager)
    {
        _id = id;
        _pair = entry;
        _syncedGroups = syncedGroups;
        _currentGroup = currentGroup;
        _apiController = apiController;
        _displayHandler = uIDDisplayHandler;
        _mediator = mareMediator;
        _selectTagForPairUi = selectTagForPairUi;
        _serverConfigurationManager = serverConfigurationManager;
        _uiSharedService = uiSharedService;
        _performanceConfigService = performanceConfigService;
        _charaDataManager = charaDataManager;
    }

    public Pair Pair => _pair;
    public UserFullPairDto UserPair => _pair.UserPair!;

    public void DrawPairedClient()
    {
        using var id = ImRaii.PushId(GetType() + _id);
        var color = ImRaii.PushColor(ImGuiCol.ChildBg, ImGui.GetColorU32(ImGuiCol.FrameBgHovered), _wasHovered);
        float twoLineHeight = ImGui.GetTextLineHeight() * 2f + ImGui.GetStyle().ItemSpacing.Y;

        using (ImRaii.Child(GetType() + _id,
            new System.Numerics.Vector2(UiSharedService.GetWindowContentRegionWidth() - ImGui.GetCursorPosX(),
            twoLineHeight), false))
        {
            DrawLeftSide();
            ImGui.SameLine();
            var posX = ImGui.GetCursorPosX();
            var rightSide = DrawRightSide();
            DrawName(posX, rightSide);
        }
        _wasHovered = ImGui.IsItemHovered();
        color.Dispose();
    }

    private void DrawCommonClientMenu()
    {
        if (!_pair.IsPaused)
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.User, _uiSharedService.L("UI.DrawUserPair.9fffd1be", "Open Profile"), _menuWidth, true))
            {
                _displayHandler.OpenProfile(_pair);
                ImGui.CloseCurrentPopup();
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.166fdc0e", "Opens the profile for this user in a new window"));
        }
        if (_pair.IsVisible)
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Sync, _uiSharedService.L("UI.DrawUserPair.abf8f695", "Reload last data"), _menuWidth, true))
            {
                _pair.ApplyLastReceivedData(forced: true);
                ImGui.CloseCurrentPopup();
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.4cdcceee", "This reapplies the last received character data to this character"));
        }

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, _uiSharedService.L("UI.DrawUserPair.2a49efc9", "Cycle pause state"), _menuWidth, true))
        {
            _ = _apiController.CyclePauseAsync(_pair.UserData);
            ImGui.CloseCurrentPopup();
        }
        ImGui.Separator();

        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.0703d4ea", "Pair Permission Functions"));
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.WindowMaximize, _uiSharedService.L("UI.DrawUserPair.92862868", "Open Permissions Window"), _menuWidth, true))
        {
            _mediator.Publish(new OpenPermissionWindow(_pair));
            ImGui.CloseCurrentPopup();
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.a2f60a17", "Opens the Permissions Window which allows you to manage multiple permissions at once."));

        var isSticky = _pair.UserPair!.OwnPermissions.IsSticky();
        string stickyText = isSticky ? "Disable Preferred Permissions" : "Enable Preferred Permissions";
        var stickyIcon = isSticky ? FontAwesomeIcon.ArrowCircleDown : FontAwesomeIcon.ArrowCircleUp;
        if (_uiSharedService.IconTextButton(stickyIcon, stickyText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetSticky(!isSticky);
            _ = _apiController.UserSetPairPermissions(new(_pair.UserData, permissions));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.8b66f03a", "Preferred permissions means that this pair will not") + Environment.NewLine + _uiSharedService.L("UI.DrawUserPair.4b593833", " be affected by any syncshell permission changes through you."));

        string individualText = Environment.NewLine + Environment.NewLine + "Note: changing this permission will turn the permissions for this"
            + Environment.NewLine + "user to preferred permissions. You can change this behavior"
            + Environment.NewLine + "in the permission settings.";
        bool individual = !_pair.IsDirectlyPaired && _apiController.DefaultPermissions!.IndividualIsSticky;

        var isDisableSounds = _pair.UserPair!.OwnPermissions.IsDisableSounds();
        string disableSoundsText = isDisableSounds ? "Enable sound sync" : "Disable sound sync";
        var disableSoundsIcon = isDisableSounds ? FontAwesomeIcon.VolumeUp : FontAwesomeIcon.VolumeMute;
        if (_uiSharedService.IconTextButton(disableSoundsIcon, disableSoundsText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetDisableSounds(!isDisableSounds);
            _ = _apiController.UserSetPairPermissions(new UserPermissionsDto(_pair.UserData, permissions));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.4af9f6ea", "Changes sound sync permissions with this user.") + (individual ? individualText : string.Empty));

        var isDisableAnims = _pair.UserPair!.OwnPermissions.IsDisableAnimations();
        string disableAnimsText = isDisableAnims ? "Enable animation sync" : "Disable animation sync";
        var disableAnimsIcon = isDisableAnims ? FontAwesomeIcon.Running : FontAwesomeIcon.Stop;
        if (_uiSharedService.IconTextButton(disableAnimsIcon, disableAnimsText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetDisableAnimations(!isDisableAnims);
            _ = _apiController.UserSetPairPermissions(new UserPermissionsDto(_pair.UserData, permissions));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.c7595230", "Changes animation sync permissions with this user.") + (individual ? individualText : string.Empty));

        var isDisableVFX = _pair.UserPair!.OwnPermissions.IsDisableVFX();
        string disableVFXText = isDisableVFX ? "Enable VFX sync" : "Disable VFX sync";
        var disableVFXIcon = isDisableVFX ? FontAwesomeIcon.Sun : FontAwesomeIcon.Circle;
        if (_uiSharedService.IconTextButton(disableVFXIcon, disableVFXText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetDisableVFX(!isDisableVFX);
            _ = _apiController.UserSetPairPermissions(new UserPermissionsDto(_pair.UserData, permissions));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.d9f7dfb0", "Changes VFX sync permissions with this user.") + (individual ? individualText : string.Empty));

        var isDisableCustomizePlus = _pair.UserPair!.OwnPermissions.IsDisableCustomizePlus();
        string disableCustomizePlusText = isDisableCustomizePlus ? "Enable Customize+ sync" : "Disable Customize+ sync";
        var disableCustomizePlusIcon = isDisableCustomizePlus ? FontAwesomeIcon.Palette : FontAwesomeIcon.Ban;
        if (_uiSharedService.IconTextButton(disableCustomizePlusIcon, disableCustomizePlusText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetDisableCustomizePlus(!isDisableCustomizePlus);
            _ = _apiController.UserSetPairPermissions(new UserPermissionsDto(_pair.UserData, permissions));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.c8cd2dba", "Changes Customize+ sync permissions with this user.") + (individual ? individualText : string.Empty));

        bool isDisableMetadata = !_pair.IsMetadataEnabled;
        string disableMetadataText = isDisableMetadata ? "Enable height metadata" : "Disable height metadata";
        var disableMetadataIcon = isDisableMetadata ? FontAwesomeIcon.ArrowsAltV : FontAwesomeIcon.Ban;

        if (_uiSharedService.IconTextButton(disableMetadataIcon, disableMetadataText, _menuWidth, true))
        {
            var permissions = _pair.UserPair.OwnPermissions;
            permissions.SetDisableMetaData(!isDisableMetadata);
            _ = _apiController.UserSetPairPermissions(new UserPermissionsDto(_pair.UserData, permissions));
            _pair.ToggleMetadataAndReapply();
        }

        UiSharedService.AttachToolTip(
            _uiSharedService.L("UI.DrawUserPair.d41d8cd9", "")
            + (individual ? individualText : string.Empty));
    }

    private void DrawIndividualMenu()
    {
        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.ff2e3c51", "Individual Pair Functions"));
        var entryUID = _pair.UserData.AliasOrUID;

        if (_pair.IndividualPairStatus != API.Data.Enum.IndividualPairStatus.None)
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Folder, _uiSharedService.L("UI.DrawUserPair.8964a2c1", "Pair Groups"), _menuWidth, true))
            {
                _selectTagForPairUi.Open(_pair);
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.81221a62", "Choose pair groups for ") + entryUID);
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.DrawUserPair.08eef782", "Unpair Permanently"), _menuWidth, true) && UiSharedService.CtrlPressed())
            {
                _ = _apiController.UserRemovePair(new(_pair.UserData));
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.75efc64d", "Hold CTRL and click to unpair permanently from ") + entryUID);
        }
        else
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Plus, _uiSharedService.L("UI.DrawUserPair.13c04156", "Send pair request"), _menuWidth, true))
            {
                var targetIdent = _pair.Ident;
                var targetName =_pair.PlayerName ?? _pair.UserData.Alias ?? _pair.UserData.UID ;

                _mediator.Publish(new DirectPairRequestMessage(targetIdent, targetName));
            }

            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.61bc92a1", "Send a pair request to ") + entryUID);
        }
    }

    private void DrawLeftSide()
    {
        var pair = _pair;
        var userData = pair.UserData;
        var aliasOrUid = userData.AliasOrUID;
        var userPair = pair.UserPair;

        string userPairText = string.Empty;

        ImGui.AlignTextToFramePadding();

        if (pair.IsPaused)
        {
            using var _ = ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.DalamudYellow);
            _uiSharedService.IconText(FontAwesomeIcon.PauseCircle);
            userPairText = aliasOrUid + " is paused";
        }
        else if (!pair.IsOnline)
        {
            using var _ = ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.DalamudRed);
            _uiSharedService.IconText(pair.IndividualPairStatus == API.Data.Enum.IndividualPairStatus.OneSided
                ? FontAwesomeIcon.ArrowsLeftRight
                : (pair.IndividualPairStatus == API.Data.Enum.IndividualPairStatus.Bidirectional
                    ? FontAwesomeIcon.User : FontAwesomeIcon.Users));
            userPairText = aliasOrUid + " is offline";
        }
        else if (pair.IsVisible)
        {
            var icon = FontAwesomeIcon.Eye;
            var color = ImGuiColors.ParsedGreen;
            var stateText = "Visible";

            if (pair.IsUploading)
            {
                icon = FontAwesomeIcon.Upload;
                color = ImGuiColors.DalamudYellow;
                stateText = "Visible - Uploading";
            }
            else
            {
                var dl = pair.CurrentDownloadStatus;

                try
                {
                    if (dl != null && dl.Count > 0)
                    {
                        bool hasDownloading = false;
                        bool hasLoading = false;

                        foreach (var s in dl.Values)
                        {
                            switch (s.DownloadStatus)
                            {
                                case RavaSync.WebAPI.Files.Models.DownloadStatus.Downloading:
                                case RavaSync.WebAPI.Files.Models.DownloadStatus.WaitingForQueue:
                                case RavaSync.WebAPI.Files.Models.DownloadStatus.WaitingForSlot:
                                    hasDownloading = true;
                                    break;

                                case RavaSync.WebAPI.Files.Models.DownloadStatus.Initializing:
                                case RavaSync.WebAPI.Files.Models.DownloadStatus.Decompressing:
                                    hasLoading = true;
                                    break;
                            }

                            if (hasDownloading)
                                break;
                        }

                        if (hasDownloading)
                        {
                            icon = FontAwesomeIcon.Download;
                            color = ImGuiColors.ParsedBlue;
                            stateText = "Visible - Downloading";
                        }
                        else if (hasLoading)
                        {
                            icon = FontAwesomeIcon.Sync;
                            color = ImGuiColors.DalamudViolet;
                            stateText = "Visible - Loading files";
                        }
                    }
                }
                catch
                {
                    // ignore
                }
            }

            _uiSharedService.IconText(icon, color);

            userPairText = $"{aliasOrUid} is {stateText}: {pair.PlayerName}"
                + Environment.NewLine
                + "Click to target this player";

            if (ImGui.IsItemClicked())
            {
                _mediator.Publish(new TargetPairMessage(pair));
            }
        }
        else
        {
            using var _ = ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.HealerGreen);
            _uiSharedService.IconText(pair.IndividualPairStatus == API.Data.Enum.IndividualPairStatus.Bidirectional
                ? FontAwesomeIcon.User : FontAwesomeIcon.Users);
            userPairText = aliasOrUid + " is online";
        }

        if (ImGui.IsItemHovered())
        {
            if (pair.IndividualPairStatus == API.Data.Enum.IndividualPairStatus.OneSided)
            {
                userPairText += UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DrawUserPair.9c391340", "User has not added you back");
            }
            else if (pair.IndividualPairStatus == API.Data.Enum.IndividualPairStatus.Bidirectional)
            {
                userPairText += UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DrawUserPair.952cfa97", "You are directly Paired");
            }

            if (pair.LastAppliedDataBytes >= 0)
            {
                userPairText += UiSharedService.TooltipSeparator;
                userPairText += ((!pair.IsPaired) ? "(Last) " : string.Empty) + "Mods Info" + Environment.NewLine;
                userPairText += "Files Size: " + UiSharedService.ByteToString(pair.LastAppliedDataBytes, true);

                if (pair.LastAppliedApproximateVRAMBytes >= 0)
                {
                    userPairText += Environment.NewLine + "Approx. VRAM Usage: " + UiSharedService.ByteToString(pair.LastAppliedApproximateVRAMBytes, true);
                }

                if (pair.LastAppliedDataTris >= 0)
                {
                    userPairText += Environment.NewLine + "Approx. Triangle Count (excl. Vanilla): "
                        + (pair.LastAppliedDataTris > 1000 ? (pair.LastAppliedDataTris / 1000d).ToString("0.0'k'") : pair.LastAppliedDataTris);
                }
            }

            if (_syncedGroups.Count > 0)
            {
                userPairText += UiSharedService.TooltipSeparator;

                for (int i = 0; i < _syncedGroups.Count; i++)
                {
                    var g = _syncedGroups[i];
                    var groupNote = _serverConfigurationManager.GetNoteForGid(g.GID);
                    var groupString = string.IsNullOrEmpty(groupNote) ? g.GroupAliasOrGID : $"{groupNote} ({g.GroupAliasOrGID})";

                    if (i > 0)
                        userPairText += Environment.NewLine;

                    userPairText += "Paired through " + groupString;
                }
            }

            UiSharedService.AttachToolTip(userPairText);
        }

        if (_performanceConfigService.Current.ShowPerformanceIndicator
            && !_performanceConfigService.Current.UIDsToIgnore
                .Exists(uid => string.Equals(uid, UserPair.User.Alias, StringComparison.Ordinal) || string.Equals(uid, UserPair.User.UID, StringComparison.Ordinal))
            && ((_performanceConfigService.Current.VRAMSizeWarningThresholdMiB > 0 && _performanceConfigService.Current.VRAMSizeWarningThresholdMiB * 1024 * 1024 < pair.LastAppliedApproximateVRAMBytes)
                || (_performanceConfigService.Current.TrisWarningThresholdThousands > 0 && _performanceConfigService.Current.TrisWarningThresholdThousands * 1000 < pair.LastAppliedDataTris))
            && (!userPair!.OwnPermissions.IsSticky()
                || _performanceConfigService.Current.WarnOnPreferredPermissionsExceedingThresholds))
        {
            ImGui.SameLine();

            _uiSharedService.IconText(FontAwesomeIcon.ExclamationTriangle, ImGuiColors.DalamudYellow);

            if (ImGui.IsItemHovered())
            {
                string userWarningText = _uiSharedService.L("UI.DrawUserPair.1f7beef6", "WARNING: This user exceeds one or more of your defined thresholds:") + UiSharedService.TooltipSeparator;
                bool shownVram = false;

                if (_performanceConfigService.Current.VRAMSizeWarningThresholdMiB > 0
                    && _performanceConfigService.Current.VRAMSizeWarningThresholdMiB * 1024 * 1024 < pair.LastAppliedApproximateVRAMBytes)
                {
                    shownVram = true;
                    userWarningText += $"Approx. VRAM Usage: Used: {UiSharedService.ByteToString(pair.LastAppliedApproximateVRAMBytes)}, Threshold: {_performanceConfigService.Current.VRAMSizeWarningThresholdMiB} MiB";
                }

                if (_performanceConfigService.Current.TrisWarningThresholdThousands > 0
                    && _performanceConfigService.Current.TrisWarningThresholdThousands * 1024 < pair.LastAppliedDataTris)
                {
                    if (shownVram) userWarningText += Environment.NewLine;
                    userWarningText += $"Approx. Triangle count: Used: {pair.LastAppliedDataTris}, Threshold: {_performanceConfigService.Current.TrisWarningThresholdThousands * 1000}";
                }

                UiSharedService.AttachToolTip(userWarningText);
            }
        }

        ImGui.SameLine();
    }

    private void DrawName(float leftSide, float rightSide)
    {
        _displayHandler.DrawPairText(_id, _pair, leftSide, () => rightSide - leftSide);
    }

    private void DrawPairedClientMenu()
    {
        DrawIndividualMenu();

        if (_syncedGroups.Count > 0) ImGui.Separator();
        foreach (var entry in _syncedGroups)
        {
            bool selfIsOwner = string.Equals(_apiController.UID, entry.Owner.UID, StringComparison.Ordinal);
            bool selfIsModerator = entry.GroupUserInfo.IsModerator();
            bool userIsModerator = entry.GroupPairUserInfos.TryGetValue(_pair.UserData.UID, out var modinfo) && modinfo.IsModerator();
            bool userIsPinned = entry.GroupPairUserInfos.TryGetValue(_pair.UserData.UID, out var info) && info.IsPinned();
            if (selfIsOwner || selfIsModerator)
            {
                var groupNote = _serverConfigurationManager.GetNoteForGid(entry.GID);
                var groupString = string.IsNullOrEmpty(groupNote) ? entry.GroupAliasOrGID : $"{groupNote} ({entry.GroupAliasOrGID})";

                if (ImGui.BeginMenu(groupString + _uiSharedService.L("UI.DrawUserPair.05ec46dd", " Moderation Functions")))
                {
                    DrawSyncshellMenu(entry, selfIsOwner, selfIsModerator, userIsPinned, userIsModerator);
                    ImGui.EndMenu();
                }
            }
        }
    }

    private float DrawRightSide()
    {
        var pair = _pair;
        var userPair = pair.UserPair!;
        var ownPerms = userPair.OwnPermissions;
        var otherPerms = userPair.OtherPermissions;

        var pauseIcon = ownPerms.IsPaused() ? FontAwesomeIcon.Play : FontAwesomeIcon.Pause;
        var pauseButtonSize = _uiSharedService.GetIconButtonSize(pauseIcon);
        var barButtonSize = _uiSharedService.GetIconButtonSize(FontAwesomeIcon.EllipsisV);
        var spacingX = ImGui.GetStyle().ItemSpacing.X;
        var windowEndX = ImGui.GetWindowContentRegionMin().X + UiSharedService.GetWindowContentRegionWidth();
        float currentRightSide = windowEndX - barButtonSize.X;

        ImGui.SameLine(currentRightSide);
        ImGui.AlignTextToFramePadding();
        if (_uiSharedService.IconButton(FontAwesomeIcon.EllipsisV))
        {
            ImGui.OpenPopup(_uiSharedService.L("UI.DrawFolderBase.d24cd894", "User Flyout Menu"));
        }

        currentRightSide -= (pauseButtonSize.X + spacingX);
        ImGui.SameLine(currentRightSide);
        if (_uiSharedService.IconButton(pauseIcon))
        {
            var perm = ownPerms;

            if (UiSharedService.CtrlPressed() && !perm.IsPaused())
            {
                perm.SetSticky(true);
            }
            perm.SetPaused(!perm.IsPaused());
            _ = _apiController.UserSetPairPermissions(new(_pair.UserData, perm));
        }
        UiSharedService.AttachToolTip(!ownPerms.IsPaused()
            ? ("Pause pairing with " + _pair.UserData.AliasOrUID
                + (ownPerms.IsSticky()
                    ? string.Empty
                    : UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DrawUserPair.9c09dbc9", "Hold CTRL to enable preferred permissions while pausing.") + Environment.NewLine + _uiSharedService.L("UI.DrawUserPair.0967fa69", "This will leave this pair paused even if unpausing syncshells including this pair.")))
            : "Resume pairing with " + _pair.UserData.AliasOrUID);

        if (_pair.IsPaired)
        {
            var individualSoundsDisabled = ownPerms.IsDisableSounds() || otherPerms.IsDisableSounds();
            var individualAnimDisabled = ownPerms.IsDisableAnimations() || otherPerms.IsDisableAnimations();
            var individualVFXDisabled = ownPerms.IsDisableVFX() || otherPerms.IsDisableVFX();
            var individualIsSticky = ownPerms.IsSticky();
            var individualIcon = individualIsSticky ? FontAwesomeIcon.ArrowCircleUp : FontAwesomeIcon.InfoCircle;

            if (individualAnimDisabled || individualSoundsDisabled || individualVFXDisabled || individualIsSticky)
            {
                currentRightSide -= (_uiSharedService.GetIconSize(individualIcon).X + spacingX);

                ImGui.SameLine(currentRightSide);
                using (ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.DalamudYellow, individualAnimDisabled || individualSoundsDisabled || individualVFXDisabled))
                    _uiSharedService.IconText(individualIcon);
                if (ImGui.IsItemHovered())
                {
                    ImGui.BeginTooltip();

                    ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.ad62308d", "Individual User permissions"));
                    ImGui.Separator();

                    if (individualIsSticky)
                    {
                        _uiSharedService.IconText(individualIcon);
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.f8335ab7", "Preferred permissions enabled"));
                        if (individualAnimDisabled || individualSoundsDisabled || individualVFXDisabled)
                            ImGui.Separator();
                    }

                    if (individualSoundsDisabled)
                    {
                        var userSoundsText = "Sound sync";
                        _uiSharedService.IconText(FontAwesomeIcon.VolumeOff);
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(userSoundsText);
                        ImGui.NewLine();
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.905cb326", "You"));
                        _uiSharedService.BooleanToColoredIcon(ownPerms.IsDisableSounds());
                        ImGui.SameLine();
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.10506865", "They"));
                        _uiSharedService.BooleanToColoredIcon(otherPerms.IsDisableSounds());
                    }

                    if (individualAnimDisabled)
                    {
                        var userAnimText = "Animation sync";
                        _uiSharedService.IconText(FontAwesomeIcon.Stop);
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(userAnimText);
                        ImGui.NewLine();
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.905cb326", "You"));
                        _uiSharedService.BooleanToColoredIcon(ownPerms.IsDisableAnimations());
                        ImGui.SameLine();
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.10506865", "They"));
                        _uiSharedService.BooleanToColoredIcon(otherPerms.IsDisableAnimations());
                    }

                    if (individualVFXDisabled)
                    {
                        var userVFXText = "VFX sync";
                        _uiSharedService.IconText(FontAwesomeIcon.Circle);
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(userVFXText);
                        ImGui.NewLine();
                        ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.905cb326", "You"));
                        _uiSharedService.BooleanToColoredIcon(ownPerms.IsDisableVFX());
                        ImGui.SameLine();
                        ImGui.AlignTextToFramePadding();
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.10506865", "They"));
                        _uiSharedService.BooleanToColoredIcon(otherPerms.IsDisableVFX());
                    }

                    ImGui.EndTooltip();
                }
            }
        }

        if (_charaDataManager.SharedWithYouData.TryGetValue(_pair.UserData, out var sharedData))
        {
            currentRightSide -= (_uiSharedService.GetIconSize(FontAwesomeIcon.Running).X + (spacingX / 2f));
            ImGui.SameLine(currentRightSide);
            _uiSharedService.IconText(FontAwesomeIcon.Running);
            UiSharedService.AttachToolTip($"This user has shared {sharedData.Count} Character Data Sets with you." + UiSharedService.TooltipSeparator
                + "Click to open the Character Data Hub and show the entries.");
            if (ImGui.IsItemClicked(ImGuiMouseButton.Left))
            {
                _mediator.Publish(new OpenCharaDataHubWithFilterMessage(_pair.UserData));
            }
        }

        if (_currentGroup != null)
        {
            var icon = FontAwesomeIcon.None;
            var text = string.Empty;
            if (string.Equals(_currentGroup.OwnerUID, _pair.UserData.UID, StringComparison.Ordinal))
            {
                icon = FontAwesomeIcon.Crown;
                text = "User is owner of this syncshell";
            }
            else if (_currentGroup.GroupPairUserInfos.TryGetValue(_pair.UserData.UID, out var userinfo))
            {
                if (userinfo.IsModerator())
                {
                    icon = FontAwesomeIcon.UserShield;
                    text = "User is moderator in this syncshell";
                }
                else if (userinfo.IsPinned())
                {
                    icon = FontAwesomeIcon.Thumbtack;
                    text = "User is pinned in this syncshell";
                }
            }

            if (!string.IsNullOrEmpty(text))
            {
                currentRightSide -= (_uiSharedService.GetIconSize(icon).X + spacingX);
                ImGui.SameLine(currentRightSide);
                _uiSharedService.IconText(icon);
                UiSharedService.AttachToolTip(text);
            }
        }

        if (ImGui.BeginPopup(_uiSharedService.L("UI.DrawFolderBase.d24cd894", "User Flyout Menu")))
        {
            using (ImRaii.PushId($"buttons-{_pair.UserData.UID}"))
            {
                ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.6e42c82b", "Common Pair Functions"));
                DrawCommonClientMenu();
                ImGui.Separator();
                DrawPairedClientMenu();
                if (_menuWidth <= 0)
                {
                    _menuWidth = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;
                }
            }

            ImGui.EndPopup();
        }

        return currentRightSide - spacingX;
    }

    private void DrawSyncshellMenu(GroupFullInfoDto group, bool selfIsOwner, bool selfIsModerator, bool userIsPinned, bool userIsModerator)
    {
        if (selfIsOwner || ((selfIsModerator) && (!userIsModerator)))
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.cc6fe07f", "Syncshell Moderator Functions"));
            var pinText = userIsPinned ? "Unpin user" : "Pin user";
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Thumbtack, pinText, _menuWidth, true))
            {
                ImGui.CloseCurrentPopup();
                if (!group.GroupPairUserInfos.TryGetValue(_pair.UserData.UID, out var userinfo))
                {
                    userinfo = API.Data.Enum.GroupPairUserInfo.IsPinned;
                }
                else
                {
                    userinfo.SetPinned(!userinfo.IsPinned());
                }
                _ = _apiController.GroupSetUserInfo(new GroupPairUserInfoDto(group.Group, _pair.UserData, userinfo));
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.d74206b9", "Pin this user to the Syncshell. Pinned users will not be deleted in case of a manually initiated Syncshell clean"));

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.DrawUserPair.4b9559cc", "Remove user"), _menuWidth, true) && UiSharedService.CtrlPressed())
            {
                ImGui.CloseCurrentPopup();
                _ = _apiController.GroupRemoveUser(new(group.Group, _pair.UserData));
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.21d847d9", "Hold CTRL and click to remove user ") + (_pair.UserData.AliasOrUID) + _uiSharedService.L("UI.DrawUserPair.4777c958", " from Syncshell"));

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.UserSlash, _uiSharedService.L("UI.DrawUserPair.ad99afbf", "Ban User"), _menuWidth, true))
            {
                _mediator.Publish(new OpenBanUserPopupMessage(_pair, group));
                ImGui.CloseCurrentPopup();
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.5b38742b", "Ban user from this Syncshell"));

            ImGui.Separator();
        }

        if (selfIsOwner)
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawUserPair.f3626cc3", "Syncshell Owner Functions"));
            string modText = userIsModerator ? "Demod user" : "Mod user";
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.UserShield, modText, _menuWidth, true) && UiSharedService.CtrlPressed())
            {
                ImGui.CloseCurrentPopup();
                if (!group.GroupPairUserInfos.TryGetValue(_pair.UserData.UID, out var userinfo))
                {
                    userinfo = API.Data.Enum.GroupPairUserInfo.IsModerator;
                }
                else
                {
                    userinfo.SetModerator(!userinfo.IsModerator());
                }

                _ = _apiController.GroupSetUserInfo(new GroupPairUserInfoDto(group.Group, _pair.UserData, userinfo));
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.4e244bf8", "Hold CTRL to change the moderator status for ") + (_pair.UserData.AliasOrUID) + Environment.NewLine +
                "Moderators can kick, ban/unban, pin/unpin users and clear the Syncshell.");

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Crown, _uiSharedService.L("UI.DrawUserPair.8a35105c", "Transfer Ownership"), _menuWidth, true) && UiSharedService.CtrlPressed() && UiSharedService.ShiftPressed())
            {
                ImGui.CloseCurrentPopup();
                _ = _apiController.GroupChangeOwnership(new(group.Group, _pair.UserData));
            }
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawUserPair.3824bf2c", "Hold CTRL and SHIFT and click to transfer ownership of this Syncshell to ")
                + (_pair.UserData.AliasOrUID) + Environment.NewLine + "WARNING: This action is irreversible.");
        }
    }
}