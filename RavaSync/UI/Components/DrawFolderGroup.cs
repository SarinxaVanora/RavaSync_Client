using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.UI.Handlers;
using RavaSync.WebAPI;
using System;
using System.Collections.Immutable;

namespace RavaSync.UI.Components;

public class DrawFolderGroup : DrawFolderBase
{
    private readonly ApiController _apiController;
    private readonly GroupFullInfoDto _groupFullInfoDto;
    private readonly IdDisplayHandler _idDisplayHandler;
    private readonly MareMediator _mareMediator;
    private readonly PairManager _pairManager;

    public DrawFolderGroup(string id, GroupFullInfoDto groupFullInfoDto, ApiController apiController,
        IImmutableList<DrawUserPair> drawPairs, IImmutableList<Pair> allPairs, TagHandler tagHandler, IdDisplayHandler idDisplayHandler,
        MareMediator mareMediator, UiSharedService uiSharedService, PairManager pairManager) :
        base(id, drawPairs, allPairs, tagHandler, uiSharedService)
    {
        _groupFullInfoDto = groupFullInfoDto;
        _apiController = apiController;
        _idDisplayHandler = idDisplayHandler;
        _mareMediator = mareMediator;
        _pairManager = pairManager;

    }

    protected override bool RenderIfEmpty => true;
    protected override bool RenderMenu => true;
    private bool IsModerator => IsOwner || _groupFullInfoDto.GroupUserInfo.IsModerator();
    private bool IsOwner => string.Equals(_groupFullInfoDto.OwnerUID, _apiController.UID, StringComparison.Ordinal);
    private bool IsPinned => _groupFullInfoDto.GroupUserInfo.IsPinned();

    protected override float DrawIcon()
    {
        ImGui.AlignTextToFramePadding();

        _uiSharedService.IconText(_groupFullInfoDto.GroupPermissions.IsDisableInvites() ? FontAwesomeIcon.Lock : FontAwesomeIcon.Users);
        if (_groupFullInfoDto.GroupPermissions.IsDisableInvites())
        {
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.c464ea45", "Syncshell ") + _groupFullInfoDto.GroupAliasOrGID + _uiSharedService.L("UI.DrawFolderGroup.3a818311", " is closed for invites"));
        }

        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = ImGui.GetStyle().ItemSpacing.X / 2f }))
        {
            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();

            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.81541726", "[") + OnlinePairs.ToString() + "]");
        }
        UiSharedService.AttachToolTip(OnlinePairs + _uiSharedService.L("UI.DrawFolderGroup.c5e2e978", " online") + Environment.NewLine + TotalPairs + _uiSharedService.L("UI.DrawFolderGroup.f274d6e3", " total"));

        ImGui.SameLine();
        if (IsOwner)
        {
            ImGui.AlignTextToFramePadding();
            _uiSharedService.IconText(FontAwesomeIcon.Crown);
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.a0675189", "You are the owner of ") + _groupFullInfoDto.GroupAliasOrGID);
        }
        else if (IsModerator)
        {
            ImGui.AlignTextToFramePadding();
            _uiSharedService.IconText(FontAwesomeIcon.UserShield);
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.fcaf1fcb", "You are a moderator in ") + _groupFullInfoDto.GroupAliasOrGID);
        }
        else if (IsPinned)
        {
            ImGui.AlignTextToFramePadding();
            _uiSharedService.IconText(FontAwesomeIcon.Thumbtack);
            UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.c64349f8", "You are pinned in ") + _groupFullInfoDto.GroupAliasOrGID);
        }
        ImGui.SameLine();
        return ImGui.GetCursorPosX();
    }

    protected override void DrawMenu(float menuWidth)
    {
        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.2d085077", "Syncshell Menu (") + _groupFullInfoDto.GroupAliasOrGID + ")");
        ImGui.Separator();

        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.95a17938", "General Syncshell Actions"));
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Copy, _uiSharedService.L("UI.DrawFolderGroup.e3da891c", "Copy ID"), menuWidth, true))
        {
            ImGui.CloseCurrentPopup();
            ImGui.SetClipboardText(_groupFullInfoDto.GroupAliasOrGID);
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.89bce326", "Copy Syncshell ID to Clipboard"));

        ImGui.Separator();

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.StickyNote, _uiSharedService.L("UI.DrawFolderGroup.1014840b", "Copy Notes"), menuWidth, true))
        {
            ImGui.CloseCurrentPopup();
            ImGui.SetClipboardText(UiSharedService.GetNotes(DrawPairs.Select(k => k.Pair).ToList()));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.c5d266e9", "Copies all your notes for all users in this Syncshell to the clipboard.") + Environment.NewLine + _uiSharedService.L("UI.DrawFolderGroup.7dfe0e7d", "They can be imported via Settings -> General -> Notes -> Import notes from clipboard"));

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.ArrowCircleLeft, _uiSharedService.L("UI.DrawFolderGroup.9845944e", "Leave Syncshell"), menuWidth, true) && UiSharedService.CtrlPressed())
        {
            _ = _apiController.GroupLeave(_groupFullInfoDto);
            ImGui.CloseCurrentPopup();
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderGroup.3ec5833a", "Hold CTRL and click to leave this Syncshell") + (!string.Equals(_groupFullInfoDto.OwnerUID, _apiController.UID, StringComparison.Ordinal)
            ? string.Empty : Environment.NewLine + "WARNING: This action is irreversible" + Environment.NewLine + "Leaving an owned Syncshell will transfer the ownership to a random person in the Syncshell."));

        ImGui.Separator();
        ImGui.TextUnformatted(_uiSharedService.L("UI.SettingsUi.2405c5d6", "Permission Settings"));
        var perm = _groupFullInfoDto.GroupUserPermissions;
        bool disableSounds = perm.IsDisableSounds();
        bool disableAnims = perm.IsDisableAnimations();
        bool disableVfx = perm.IsDisableVFX();

        if ((_groupFullInfoDto.GroupPermissions.IsPreferDisableAnimations() != disableAnims
            || _groupFullInfoDto.GroupPermissions.IsPreferDisableSounds() != disableSounds
            || _groupFullInfoDto.GroupPermissions.IsPreferDisableVFX() != disableVfx)
            && _uiSharedService.IconTextButton(FontAwesomeIcon.Check, _uiSharedService.L("UI.DrawFolderGroup.d940f59c", "Align with suggested permissions"), menuWidth, true))
        {
            perm.SetDisableVFX(_groupFullInfoDto.GroupPermissions.IsPreferDisableVFX());
            perm.SetDisableSounds(_groupFullInfoDto.GroupPermissions.IsPreferDisableSounds());
            perm.SetDisableAnimations(_groupFullInfoDto.GroupPermissions.IsPreferDisableAnimations());
            _ = _apiController.GroupChangeIndividualPermissionState(new(_groupFullInfoDto.Group, new(_apiController.UID), perm));
            ImGui.CloseCurrentPopup();
        }

        if (_uiSharedService.IconTextButton(disableSounds ? FontAwesomeIcon.VolumeUp : FontAwesomeIcon.VolumeOff, disableSounds ? _uiSharedService.L("UI.DrawFolderGroup.6cec63f4", "Enable Sound Sync") : _uiSharedService.L("UI.DrawFolderGroup.689f94f0", "Disable Sound Sync"), menuWidth, true))
        {
            perm.SetDisableSounds(!disableSounds);
            _ = _apiController.GroupChangeIndividualPermissionState(new(_groupFullInfoDto.Group, new(_apiController.UID), perm));
            ImGui.CloseCurrentPopup();
        }

        if (_uiSharedService.IconTextButton(disableAnims ? FontAwesomeIcon.Running : FontAwesomeIcon.Stop, disableAnims ? _uiSharedService.L("UI.DrawFolderGroup.1741f849", "Enable Animation Sync") : _uiSharedService.L("UI.DrawFolderGroup.8a64088a", "Disable Animation Sync"), menuWidth, true))
        {
            perm.SetDisableAnimations(!disableAnims);
            _ = _apiController.GroupChangeIndividualPermissionState(new(_groupFullInfoDto.Group, new(_apiController.UID), perm));
            ImGui.CloseCurrentPopup();
        }

        if (_uiSharedService.IconTextButton(disableVfx ? FontAwesomeIcon.Sun : FontAwesomeIcon.Circle, disableVfx ? _uiSharedService.L("UI.DrawFolderGroup.02309596", "Enable VFX Sync") : _uiSharedService.L("UI.DrawFolderGroup.f9e2bc5b", "Disable VFX Sync"), menuWidth, true))
        {
            perm.SetDisableVFX(!disableVfx);
            _ = _apiController.GroupChangeIndividualPermissionState(new(_groupFullInfoDto.Group, new(_apiController.UID), perm));
            ImGui.CloseCurrentPopup();
        }

        if (IsModerator || IsOwner)
        {
            ImGui.Separator();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.7c87d1bf", "Syncshell Admin Functions"));
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Cog, _uiSharedService.L("UI.DrawFolderGroup.e0652012", "Open Admin Panel"), menuWidth, true))
            {
                ImGui.CloseCurrentPopup();
                _mareMediator.Publish(new OpenSyncshellAdminPanel(_groupFullInfoDto));
            }
        }
    }

    protected override void DrawName(float width)
    {
        _idDisplayHandler.DrawGroupText(_id, _groupFullInfoDto, ImGui.GetCursorPosX(), () => width);
    }

    protected override float DrawRightSide(float currentRightSideX)
    {
        var spacingX = ImGui.GetStyle().ItemSpacing.X;

        FontAwesomeIcon pauseIcon = _groupFullInfoDto.GroupUserPermissions.IsPaused() ? FontAwesomeIcon.Play : FontAwesomeIcon.Pause;
        var pauseButtonSize = _uiSharedService.GetIconButtonSize(pauseIcon);

        var userCogButtonSize = _uiSharedService.GetIconSize(FontAwesomeIcon.UsersCog);

        var individualSoundsDisabled = _groupFullInfoDto.GroupUserPermissions.IsDisableSounds();
        var individualAnimDisabled = _groupFullInfoDto.GroupUserPermissions.IsDisableAnimations();
        var individualVFXDisabled = _groupFullInfoDto.GroupUserPermissions.IsDisableVFX();

        var infoIconPosDist = currentRightSideX - pauseButtonSize.X - spacingX;

        ImGui.SameLine(infoIconPosDist - userCogButtonSize.X);

        ImGui.AlignTextToFramePadding();

        _uiSharedService.IconText(FontAwesomeIcon.UsersCog, (_groupFullInfoDto.GroupPermissions.IsPreferDisableAnimations() != individualAnimDisabled
            || _groupFullInfoDto.GroupPermissions.IsPreferDisableSounds() != individualSoundsDisabled
            || _groupFullInfoDto.GroupPermissions.IsPreferDisableVFX() != individualVFXDisabled) ? ImGuiColors.DalamudYellow : null);
        if (ImGui.IsItemHovered())
        {
            ImGui.BeginTooltip();

            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.45b46eea", "Syncshell Permissions"));
            ImGuiHelpers.ScaledDummy(2f);

            _uiSharedService.BooleanToColoredIcon(!individualSoundsDisabled, inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.abfb2bed", "Sound Sync"));

            _uiSharedService.BooleanToColoredIcon(!individualAnimDisabled, inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.92e2cb98", "Animation Sync"));

            _uiSharedService.BooleanToColoredIcon(!individualVFXDisabled, inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.67c3393c", "VFX Sync"));

            ImGui.Separator();

            ImGuiHelpers.ScaledDummy(2f);
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.3378b73f", "Suggested Permissions"));
            ImGuiHelpers.ScaledDummy(2f);

            _uiSharedService.BooleanToColoredIcon(!_groupFullInfoDto.GroupPermissions.IsPreferDisableSounds(), inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.abfb2bed", "Sound Sync"));

            _uiSharedService.BooleanToColoredIcon(!_groupFullInfoDto.GroupPermissions.IsPreferDisableAnimations(), inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.92e2cb98", "Animation Sync"));

            _uiSharedService.BooleanToColoredIcon(!_groupFullInfoDto.GroupPermissions.IsPreferDisableVFX(), inline: false);
            ImGui.SameLine(40 * ImGuiHelpers.GlobalScale);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderGroup.67c3393c", "VFX Sync"));

            ImGui.EndTooltip();
        }

        ImGui.SameLine();
        if (_uiSharedService.IconButton(pauseIcon))
        {
            var perm = _groupFullInfoDto.GroupUserPermissions;
            perm.SetPaused(!perm.IsPaused());
            _ = _apiController.GroupChangeIndividualPermissionState(new GroupPairUserPermissionDto(_groupFullInfoDto.Group, new(_apiController.UID), perm));
        }

        return currentRightSideX;
    }

}

