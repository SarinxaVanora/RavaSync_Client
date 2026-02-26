using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Numerics;

namespace RavaSync.UI.Components.Popup;

public class SyncshellAdminUI : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly bool _isModerator = false;
    private readonly bool _isOwner = false;
    private readonly List<string> _oneTimeInvites = [];
    private readonly PairManager _pairManager;
    private readonly UiSharedService _uiSharedService;
    private List<BannedGroupUserDto> _bannedUsers = [];
    private int _multiInvites;
    private string _newPassword;
    private bool _pwChangeSuccess;
    private Task<int>? _pruneTestTask;
    private Task<int>? _pruneTask;
    private int _pruneDays = 14;
    private string _newGroupAlias = string.Empty;
    private string _groupAliasStatus = string.Empty;
    private Vector4 _groupAliasStatusColor = ImGuiColors.ParsedGreen;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public SyncshellAdminUI(ILogger<SyncshellAdminUI> logger, MareMediator mediator, ApiController apiController,
        UiSharedService uiSharedService, PairManager pairManager, GroupFullInfoDto groupFullInfo, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "Syncshell Admin Panel (" + groupFullInfo.GroupAliasOrGID + ")", performanceCollectorService)
    {
        GroupFullInfo = groupFullInfo;
        _apiController = apiController;
        _uiSharedService = uiSharedService;
        _pairManager = pairManager;
        _isOwner = string.Equals(GroupFullInfo.OwnerUID, _apiController.UID, StringComparison.Ordinal);
        _isModerator = GroupFullInfo.GroupUserInfo.IsModerator();
        _newPassword = string.Empty;
        _multiInvites = 30;
        _pwChangeSuccess = true;
        IsOpen = true;
        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new(700, 500),
            MaximumSize = new(700, 2000),
        };
    }

    public GroupFullInfoDto GroupFullInfo { get; private set; }

    protected override void DrawInternal()
    {
        if (!_isModerator && !_isOwner) return;

        GroupFullInfo = _pairManager.Groups[GroupFullInfo.Group];

        using var id = ImRaii.PushId("syncshell_admin_" + GroupFullInfo.GID);

        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted(GroupFullInfo.GroupAliasOrGID + _uiSharedService.L("UI.SyncshellAdminUI.6fa48424", " Administrative Panel"));

        ImGui.Separator();
        var perm = GroupFullInfo.GroupPermissions;

        using var tabbar = ImRaii.TabBar("syncshell_tab_" + GroupFullInfo.GID);

        if (tabbar)
        {
            var inviteTab = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.SyncshellAdminUI.213b8684", "Invites"), "##213b8684"));
            if (inviteTab)
            {
                bool isInvitesDisabled = perm.IsDisableInvites();

                if (_uiSharedService.IconTextButton(isInvitesDisabled ? FontAwesomeIcon.Unlock : FontAwesomeIcon.Lock,
                    isInvitesDisabled ? "Unlock Syncshell" : "Lock Syncshell"))
                {
                    perm.SetDisableInvites(!isInvitesDisabled);
                    _ = _apiController.GroupChangeGroupPermissionState(new(GroupFullInfo.Group, perm));
                }

                ImGuiHelpers.ScaledDummy(2f);

                UiSharedService.TextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.e6ad04b2", "One-time invites work as single-use passwords. Use those if you do not want to distribute your Syncshell password."));
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Envelope, _uiSharedService.L("UI.SyncshellAdminUI.3ee82191", "Single one-time invite")))
                {
                    ImGui.SetClipboardText(_apiController.GroupCreateTempInvite(new(GroupFullInfo.Group), 1).Result.FirstOrDefault() ?? string.Empty);
                }
                UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.1a2e88b3", "Creates a single-use password for joining the syncshell which is valid for 24h and copies it to the clipboard."));
                ImGui.InputInt("##amountofinvites", ref _multiInvites);
                ImGui.SameLine();
                using (ImRaii.Disabled(_multiInvites <= 1 || _multiInvites > 100))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Envelope, _uiSharedService.L("UI.SyncshellAdminUI.1c666044", "Generate ") + _multiInvites + _uiSharedService.L("UI.SyncshellAdminUI.df6a1dab", " one-time invites")))
                    {
                        _oneTimeInvites.AddRange(_apiController.GroupCreateTempInvite(new(GroupFullInfo.Group), _multiInvites).Result);
                    }
                }

                if (_oneTimeInvites.Any())
                {
                    var invites = string.Join(Environment.NewLine, _oneTimeInvites);
                    ImGui.InputTextMultiline(_uiSharedService.L("UI.SyncshellAdminUI.aa23b54e", "Generated Multi Invites"), ref invites, 5000, new(0, 0), ImGuiInputTextFlags.ReadOnly);
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Copy, _uiSharedService.L("UI.SyncshellAdminUI.e0866e84", "Copy Invites to clipboard")))
                    {
                        ImGui.SetClipboardText(invites);
                    }
                }
            }
            inviteTab.Dispose();

            var mgmtTab = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.SyncshellAdminUI.92726ab5", "User Management"), "##92726ab5"));
            if (mgmtTab)
            {
                var userNode = ImRaii.TreeNode("User List & Administration");
                if (userNode)
                {
                    if (!_pairManager.GroupPairs.TryGetValue(GroupFullInfo, out var pairs))
                    {
                        UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.8af40305", "No users found in this Syncshell"), ImGuiColors.DalamudYellow);
                    }
                    else
                    {
                        using var table = ImRaii.Table("userList#" + GroupFullInfo.Group.GID, 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingStretchProp | ImGuiTableFlags.ScrollY);
                        if (table)
                        {
                            ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.0a4d9256", "Alias/UID/Note"), ImGuiTableColumnFlags.None, 3);
                            ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.da241013", "Online/Name"), ImGuiTableColumnFlags.None, 2);
                            ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.5d728758", "Flags"), ImGuiTableColumnFlags.None, 1);
                            ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.c3cd636a", "Actions"), ImGuiTableColumnFlags.None, 2);
                            ImGui.TableHeadersRow();

                            var groupedPairs = new Dictionary<Pair, GroupPairUserInfo?>(pairs.Select(p => new KeyValuePair<Pair, GroupPairUserInfo?>(p,
                                GroupFullInfo.GroupPairUserInfos.TryGetValue(p.UserData.UID, out GroupPairUserInfo value) ? value : null)));

                            foreach (var pair in groupedPairs.OrderBy(p =>
                            {
                                if (p.Value == null) return 10;
                                if (p.Value.Value.IsModerator()) return 0;
                                if (p.Value.Value.IsPinned()) return 1;
                                return 10;
                            }).ThenBy(p => p.Key.GetNote() ?? p.Key.UserData.AliasOrUID, StringComparer.OrdinalIgnoreCase))
                            {
                                using var tableId = ImRaii.PushId("userTable_" + pair.Key.UserData.UID);

                                ImGui.TableNextColumn(); // alias/uid/note
                                var note = pair.Key.GetNote();
                                var text = note == null ? pair.Key.UserData.AliasOrUID : note + " (" + pair.Key.UserData.AliasOrUID + ")";
                                ImGui.AlignTextToFramePadding();
                                ImGui.TextUnformatted(text);

                                ImGui.TableNextColumn(); // online/name
                                string onlineText = pair.Key.IsOnline ? "Online" : "Offline";
                                if (!string.IsNullOrEmpty(pair.Key.PlayerName))
                                {
                                    onlineText += " (" + pair.Key.PlayerName + ")";
                                }
                                var boolcolor = UiSharedService.GetBoolColor(pair.Key.IsOnline);
                                ImGui.AlignTextToFramePadding();
                                UiSharedService.ColorText(onlineText, boolcolor);

                                ImGui.TableNextColumn(); // special flags
                                if (pair.Value != null && (pair.Value.Value.IsModerator() || pair.Value.Value.IsPinned()))
                                {
                                    if (pair.Value.Value.IsModerator())
                                    {
                                        _uiSharedService.IconText(FontAwesomeIcon.UserShield);
                                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.ad3b15c3", "Moderator"));
                                    }
                                    if (pair.Value.Value.IsPinned())
                                    {
                                        _uiSharedService.IconText(FontAwesomeIcon.Thumbtack);
                                        UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.f9312169", "Pinned"));
                                    }
                                }
                                else
                                {
                                    _uiSharedService.IconText(FontAwesomeIcon.None);
                                }

                                ImGui.TableNextColumn(); // actions
                                if (_isOwner)
                                {
                                    if (_uiSharedService.IconButton(FontAwesomeIcon.UserShield))
                                    {
                                        GroupPairUserInfo userInfo = pair.Value ?? GroupPairUserInfo.None;

                                        userInfo.SetModerator(!userInfo.IsModerator());

                                        _ = _apiController.GroupSetUserInfo(new GroupPairUserInfoDto(GroupFullInfo.Group, pair.Key.UserData, userInfo));
                                    }
                                    UiSharedService.AttachToolTip(pair.Value != null && pair.Value.Value.IsModerator() ? _uiSharedService.L("UI.SyncshellAdminUI.ac62a352", "Demod user") : _uiSharedService.L("UI.SyncshellAdminUI.0a337c37", "Mod user"));
                                    ImGui.SameLine();
                                }

                                if (_isOwner || (pair.Value == null || (pair.Value != null && !pair.Value.Value.IsModerator())))
                                {
                                    if (_uiSharedService.IconButton(FontAwesomeIcon.Thumbtack))
                                    {
                                        GroupPairUserInfo userInfo = pair.Value ?? GroupPairUserInfo.None;

                                        userInfo.SetPinned(!userInfo.IsPinned());

                                        _ = _apiController.GroupSetUserInfo(new GroupPairUserInfoDto(GroupFullInfo.Group, pair.Key.UserData, userInfo));
                                    }
                                    UiSharedService.AttachToolTip(pair.Value != null && pair.Value.Value.IsPinned() ? _uiSharedService.L("UI.SyncshellAdminUI.c18887e4", "Unpin user") : _uiSharedService.L("UI.SyncshellAdminUI.b2fe96d2", "Pin user"));
                                    ImGui.SameLine();

                                    using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
                                    {
                                        if (_uiSharedService.IconButton(FontAwesomeIcon.Trash))
                                        {
                                            _ = _apiController.GroupRemoveUser(new GroupPairDto(GroupFullInfo.Group, pair.Key.UserData));
                                        }
                                    }
                                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.3b02f453", "Remove user from Syncshell")
                                        + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.SyncshellAdminUI.96778cef", "Hold CTRL to enable this button"));

                                    ImGui.SameLine();
                                    using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
                                    {
                                        if (_uiSharedService.IconButton(FontAwesomeIcon.Ban))
                                        {
                                            Mediator.Publish(new OpenBanUserPopupMessage(pair.Key, GroupFullInfo));
                                        }
                                    }
                                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.16a77efd", "Ban user from Syncshell")
                                        + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.SyncshellAdminUI.96778cef", "Hold CTRL to enable this button"));
                                }
                            }
                        }
                    }
                }
                userNode.Dispose();
                var clearNode = ImRaii.TreeNode("Mass Cleanup");
                if (clearNode)
                {
                    using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
                    {
                        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Broom, _uiSharedService.L("UI.SyncshellAdminUI.7efd5c88", "Clear Syncshell")))
                        {
                            _ = _apiController.GroupClear(new(GroupFullInfo.Group));
                        }
                    }
                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.c1888a9c", "This will remove all non-pinned, non-moderator users from the Syncshell.")
                        + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.SyncshellAdminUI.96778cef", "Hold CTRL to enable this button"));

                    ImGuiHelpers.ScaledDummy(2f);
                    ImGui.Separator();
                    ImGuiHelpers.ScaledDummy(2f);

                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Unlink, _uiSharedService.L("UI.SyncshellAdminUI.14557053", "Check for Inactive Users")))
                    {
                        _pruneTestTask = _apiController.GroupPrune(new(GroupFullInfo.Group), _pruneDays, execute: false);
                        _pruneTask = null;
                    }
                    UiSharedService.AttachToolTip($"This will start the prune process for this Syncshell of inactive RavaSync users that have not logged in in the past {_pruneDays} days."
                        + Environment.NewLine + "You will be able to review the amount of inactive users before executing the prune."
                        + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.SyncshellAdminUI.19e9045e", "Note: this check excludes pinned users and moderators of this Syncshell."));
                    ImGui.SameLine();
                    ImGui.SetNextItemWidth(150);
                    _uiSharedService.DrawCombo("Days of inactivity", [7, 14, 30, 90], (count) =>
                    {
                        return count + " days";
                    },
                    (selected) =>
                    {
                        _pruneDays = selected;
                        _pruneTestTask = null;
                        _pruneTask = null;
                    },
                    _pruneDays);

                    if (_pruneTestTask != null)
                    {
                        if (!_pruneTestTask.IsCompleted)
                        {
                            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.8dc46bbd", "Calculating inactive users..."), ImGuiColors.DalamudYellow);
                        }
                        else
                        {
                            ImGui.AlignTextToFramePadding();
                            UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.SyncshellAdminUI.b17ee261", "Found {0} user(s) that have not logged into RavaSync in the past {1} days."), _pruneTestTask.Result, _pruneDays));
                            if (_pruneTestTask.Result > 0)
                            {
                                using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
                                {
                                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Broom, _uiSharedService.L("UI.SyncshellAdminUI.141cafb6", "Prune Inactive Users")))
                                    {
                                        _pruneTask = _apiController.GroupPrune(new(GroupFullInfo.Group), _pruneDays, execute: true);
                                        _pruneTestTask = null;
                                    }
                                }
                                UiSharedService.AttachToolTip($"Pruning will remove {_pruneTestTask?.Result ?? 0} inactive user(s)."
                                    + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.SyncshellAdminUI.96778cef", "Hold CTRL to enable this button"));
                            }
                        }
                    }
                    if (_pruneTask != null)
                    {
                        if (!_pruneTask.IsCompleted)
                        {
                            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.5aa48da3", "Pruning Syncshell..."), ImGuiColors.DalamudYellow);
                        }
                        else
                        {
                            UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.SyncshellAdminUI.0fcd5cf8", "Syncshell was pruned and {0} inactive user(s) have been removed."), _pruneTask.Result));
                        }
                    }
                }
                clearNode.Dispose();

                var banNode = ImRaii.TreeNode("User Bans");
                if (banNode)
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Retweet, _uiSharedService.L("UI.SyncshellAdminUI.48978b57", "Refresh Banlist from Server")))
                    {
                        _bannedUsers = _apiController.GroupGetBannedUsers(new GroupDto(GroupFullInfo.Group)).Result;
                    }

                    if (ImGui.BeginTable("bannedusertable" + GroupFullInfo.GID, 6, ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingStretchProp | ImGuiTableFlags.ScrollY))
                    {
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.EventViewerUI.d946adf5", "UID"), ImGuiTableColumnFlags.None, 1);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.04259816", "Alias"), ImGuiTableColumnFlags.None, 1);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.cfadbd7a", "By"), ImGuiTableColumnFlags.None, 1);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.eb9a4bc1", "Date"), ImGuiTableColumnFlags.None, 2);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.f219cc06", "Reason"), ImGuiTableColumnFlags.None, 3);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.SyncshellAdminUI.c3cd636a", "Actions"), ImGuiTableColumnFlags.None, 1);

                        ImGui.TableHeadersRow();

                        foreach (var bannedUser in _bannedUsers.ToList())
                        {
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(bannedUser.UID);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(bannedUser.UserAlias ?? string.Empty);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(bannedUser.BannedBy);
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(bannedUser.BannedOn.ToLocalTime().ToString(CultureInfo.CurrentCulture));
                            ImGui.TableNextColumn();
                            UiSharedService.TextWrapped(bannedUser.Reason);
                            ImGui.TableNextColumn();
                            using var _ = ImRaii.PushId(bannedUser.UID);
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, _uiSharedService.L("UI.SyncshellAdminUI.f1e92f70", "Unban")))
                            {
                                _apiController.GroupUnbanUser(bannedUser);
                                _bannedUsers.RemoveAll(b => string.Equals(b.UID, bannedUser.UID, StringComparison.Ordinal));
                            }
                        }

                        ImGui.EndTable();
                    }
                }
                banNode.Dispose();
            }
            mgmtTab.Dispose();

            var permissionTab = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.SyncshellAdminUI.d08ccf52", "Permissions"), "##d08ccf52"));
            if (permissionTab)
            {
                bool isDisableAnimations = perm.IsPreferDisableAnimations();
                bool isDisableSounds = perm.IsPreferDisableSounds();
                bool isDisableVfx = perm.IsPreferDisableVFX();

                ImGui.AlignTextToFramePadding();
                ImGui.Text(_uiSharedService.L("UI.SyncshellAdminUI.268f3d9a", "Suggest Sound Sync"));
                _uiSharedService.BooleanToColoredIcon(!isDisableSounds);
                ImGui.SameLine(230);
                if (_uiSharedService.IconTextButton(isDisableSounds ? FontAwesomeIcon.VolumeUp : FontAwesomeIcon.VolumeMute,
                    isDisableSounds ? "Suggest to enable sound sync" : "Suggest to disable sound sync"))
                {
                    perm.SetPreferDisableSounds(!perm.IsPreferDisableSounds());
                    _ = _apiController.GroupChangeGroupPermissionState(new(GroupFullInfo.Group, perm));
                }

                ImGui.AlignTextToFramePadding();
                ImGui.Text(_uiSharedService.L("UI.SyncshellAdminUI.92167980", "Suggest Animation Sync"));
                _uiSharedService.BooleanToColoredIcon(!isDisableAnimations);
                ImGui.SameLine(230);
                if (_uiSharedService.IconTextButton(isDisableAnimations ? FontAwesomeIcon.Running : FontAwesomeIcon.Stop,
                    isDisableAnimations ? "Suggest to enable animation sync" : "Suggest to disable animation sync"))
                {
                    perm.SetPreferDisableAnimations(!perm.IsPreferDisableAnimations());
                    _ = _apiController.GroupChangeGroupPermissionState(new(GroupFullInfo.Group, perm));
                }

                ImGui.AlignTextToFramePadding();
                ImGui.Text(_uiSharedService.L("UI.SyncshellAdminUI.8bc7a876", "Suggest VFX Sync"));
                _uiSharedService.BooleanToColoredIcon(!isDisableVfx);
                ImGui.SameLine(230);
                if (_uiSharedService.IconTextButton(isDisableVfx ? FontAwesomeIcon.Sun : FontAwesomeIcon.Circle,
                    isDisableVfx ? "Suggest to enable vfx sync" : "Suggest to disable vfx sync"))
                {
                    perm.SetPreferDisableVFX(!perm.IsPreferDisableVFX());
                    _ = _apiController.GroupChangeGroupPermissionState(new(GroupFullInfo.Group, perm));
                }

                UiSharedService.TextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.802a1a61", "Note: those suggested permissions will be shown to users on joining the Syncshell."));
            }
            permissionTab.Dispose();

            if (_isOwner)
            {
                var ownerTab = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.SyncshellAdminUI.087928b9", "Owner Settings"), "##087928b9"));
                if (ownerTab)
                {
                    ImGui.AlignTextToFramePadding();

                    // --- Group Vanity (Alias) ---
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.SyncshellAdminUI.961723eb", "Set Shell Vanity"));
                    var avail = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;

                    // show current alias
                    ImGui.SameLine();
                    UiSharedService.ColorText("(" + (GroupFullInfo.GroupAliasOrGID ?? "-") + ")", ImGuiColors.ParsedGreen);

                    // input sizing
                    var setBtnSize = _uiSharedService.GetIconTextButtonSize(FontAwesomeIcon.Tag, _uiSharedService.L("UI.SyncshellAdminUI.dddffd34", "Set Alias"));
                    ImGui.SetNextItemWidth(MathF.Max(150, avail - setBtnSize - ImGui.CalcTextSize(_uiSharedService.L("UI.SyncshellAdminUI.5c6df2aa", "Group Vanity (Alias)")).X - ImGui.GetStyle().ItemSpacing.X * 3));

                    // input + button
                    ImGui.InputTextWithHint("##group_alias_input", _uiSharedService.L("UI.SyncshellAdminUI.8c7efeb7", "new-group-vanity (5–15 chars: A–Z, 0–9, _ or -)"), ref _newGroupAlias, 32);
                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.2BA2B842", "Allowed characters: letters, numbers, underscore, hyphen. Length: 5â15."));

                    ImGui.SameLine();
                    using (ImRaii.Disabled(string.IsNullOrWhiteSpace(_newGroupAlias)))
                    {
                        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Tag, _uiSharedService.L("UI.SyncshellAdminUI.749e5753", "Set Vanity")))
                        {
                            _groupAliasStatus = "Submitting…";
                            _groupAliasStatusColor = ImGuiColors.DalamudYellow;

                            // fire-and-forget so the UI stays responsive
                            _ = Task.Run(async () =>
                            {
                                try
                                {
                                    // Calls Hub - GroupSetAlias(GroupDto, string)
                                    var err = await _apiController.GroupSetAlias(new GroupDto(GroupFullInfo.Group), _newGroupAlias).ConfigureAwait(false);

                                    if (string.IsNullOrEmpty(err))
                                    {
                                        _groupAliasStatus = "Shell Vanity updated.";
                                        _groupAliasStatusColor = ImGuiColors.ParsedGreen;
                                        _newGroupAlias = string.Empty;

                                        // Refresh local GroupFullInfo so the new alias shows immediately in the window title and UI
                                        // PairManager holds the canonical instance; just re-pull the entry.
                                        GroupFullInfo = _pairManager.Groups[GroupFullInfo.Group];
                                    }
                                    else
                                    {
                                        _groupAliasStatus = err;
                                        _groupAliasStatusColor = ImGuiColors.DalamudRed;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _groupAliasStatus = "Failed to set Vanity: " + ex.Message;
                                    _groupAliasStatusColor = ImGuiColors.DalamudRed;
                                }
                            });
                        }
                    }

                    // inline feedback
                    if (!string.IsNullOrEmpty(_groupAliasStatus))
                    {
                        UiSharedService.ColorTextWrapped(_groupAliasStatus, _groupAliasStatusColor);
                    }

                    ImGuiHelpers.ScaledDummy(5);
                    ImGui.Separator();
                    ImGuiHelpers.ScaledDummy(5);
                    // --- End Group Vanity (Alias) ---


                    ImGui.TextUnformatted(_uiSharedService.L("UI.SyncshellAdminUI.4894cb39", "New Password"));
                    var availableWidth = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;
                    var buttonSize = _uiSharedService.GetIconTextButtonSize(FontAwesomeIcon.Passport, _uiSharedService.L("UI.SyncshellAdminUI.22de90ff", "Change Password"));
                    var textSize = ImGui.CalcTextSize(_uiSharedService.L("UI.SyncshellAdminUI.4894cb39", "New Password")).X;
                    var spacing = ImGui.GetStyle().ItemSpacing.X;

                    ImGui.SameLine();
                    ImGui.SetNextItemWidth(availableWidth - buttonSize - textSize - spacing * 2);
                    ImGui.InputTextWithHint("##changepw", _uiSharedService.L("UI.SyncshellAdminUI.41249a92", "Min 10 characters"), ref _newPassword, 50);
                    ImGui.SameLine();
                    using (ImRaii.Disabled(_newPassword.Length < 10))
                    {
                        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Passport, _uiSharedService.L("UI.SyncshellAdminUI.22de90ff", "Change Password")))
                        {
                            _pwChangeSuccess = _apiController.GroupChangePassword(new GroupPasswordDto(GroupFullInfo.Group, _newPassword)).Result;
                            _newPassword = string.Empty;
                        }
                    }
                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.f11a9f81", "Password requires to be at least 10 characters long. This action is irreversible."));

                    if (!_pwChangeSuccess)
                    {
                        UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.SyncshellAdminUI.04cbd349", "Failed to change the password. Password requires to be at least 10 characters long."), ImGuiColors.DalamudYellow);
                    }

                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.SyncshellAdminUI.0ea6e339", "Delete Syncshell")) && UiSharedService.CtrlPressed() && UiSharedService.ShiftPressed())
                    {
                        IsOpen = false;
                        _ = _apiController.GroupDelete(new(GroupFullInfo.Group));
                    }
                    UiSharedService.AttachToolTip(_uiSharedService.L("UI.SyncshellAdminUI.77582d26", "Hold CTRL and Shift and click to delete this Syncshell.") + Environment.NewLine + _uiSharedService.L("UI.SyncshellAdminUI.55f06eb5", "WARNING: this action is irreversible."));
                }
                ownerTab.Dispose();
            }
        }
    }

    public override void OnClose()
    {
        Mediator.Publish(new RemoveWindowMessage(this));
    }
}