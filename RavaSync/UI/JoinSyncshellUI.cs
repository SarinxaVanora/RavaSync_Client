using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto;
using RavaSync.API.Dto.Group;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;

namespace RavaSync.UI;

internal class JoinSyncshellUI : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly UiSharedService _uiSharedService;
    private string _desiredSyncshellToJoin = string.Empty;
    private GroupJoinInfoDto? _groupJoinInfo = null;
    private DefaultPermissionsDto _ownPermissions = null!;
    private string _previousPassword = string.Empty;
    private string _syncshellPassword = string.Empty;
    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public JoinSyncshellUI(ILogger<JoinSyncshellUI> logger, MareMediator mediator,
        UiSharedService uiSharedService, ApiController apiController, PerformanceCollectorService performanceCollectorService) 
        : base(logger, mediator, "Join existing Syncshell###RavaSyncJoinSyncshell", performanceCollectorService)
    {
        _uiSharedService = uiSharedService;
        _apiController = apiController;
        SizeConstraints = new()
        {
            MinimumSize = new(700, 400),
            MaximumSize = new(700, 400)
        };

        Mediator.Subscribe<DisconnectedMessage>(this, (_) => IsOpen = false);

        Flags = ImGuiWindowFlags.NoCollapse | ImGuiWindowFlags.NoResize;
    }

    public override void OnOpen()
    {
        _desiredSyncshellToJoin = string.Empty;
        _syncshellPassword = string.Empty;
        _previousPassword = string.Empty;
        _groupJoinInfo = null;
        _ownPermissions = _apiController.DefaultPermissions.DeepClone()!;
    }

    protected override void DrawInternal()
    {
        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted(_groupJoinInfo == null || !_groupJoinInfo.Success ? _uiSharedService.L("UI.JoinSyncshellUI.682d6f9a", "Join Syncshell") : _uiSharedService.L("UI.JoinSyncshellUI.89109084", "Finalize join Syncshell ") + _groupJoinInfo.GroupAliasOrGID);
        ImGui.Separator();

        if (_groupJoinInfo == null || !_groupJoinInfo.Success)
        {
            UiSharedService.TextWrapped(string.Format(_uiSharedService.L("UI.JoinSyncshellUI.5af1f55d", "Here you can join existing Syncshells.\nPlease keep in mind that you cannot join more than {0} syncshells on this server.\nJoining a Syncshell will pair you implicitly with all existing users in the Syncshell.\nAll permissions to all users in the Syncshell will be set to the preferred Syncshell permissions on joining, excluding prior set preferred permissions."), _apiController.ServerInfo.MaxGroupsJoinedByUser));
            ImGui.Separator();
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.ca57735f", "Note: Syncshell ID and Password are case sensitive. MSS- is part of Syncshell IDs, unless using Vanity IDs."));

            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.e7eb13c6", "Syncshell ID"));
            ImGui.SameLine(200);
            ImGui.InputTextWithHint("##syncshellId", _uiSharedService.L("UI.JoinSyncshellUI.af334a24", "Full Syncshell ID"), ref _desiredSyncshellToJoin, 20);

            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.0e1d1d1c", "Syncshell Password"));
            ImGui.SameLine(200);
            ImGui.InputTextWithHint("##syncshellpw", _uiSharedService.L("UI.JoinSyncshellUI.f08ec9d2", "Password"), ref _syncshellPassword, 50, ImGuiInputTextFlags.Password);
            using (ImRaii.Disabled(string.IsNullOrEmpty(_desiredSyncshellToJoin) || string.IsNullOrEmpty(_syncshellPassword)))
            {
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Plus, _uiSharedService.L("UI.JoinSyncshellUI.682d6f9a", "Join Syncshell")))
                {
                    _groupJoinInfo = _apiController.GroupJoin(new GroupPasswordDto(new API.Data.GroupData(_desiredSyncshellToJoin), _syncshellPassword)).Result;
                    _previousPassword = _syncshellPassword;
                    _syncshellPassword = string.Empty;
                }
            }
            if (_groupJoinInfo != null && !_groupJoinInfo.Success)
            {
                UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.JoinSyncshellUI.7b2a7065", "Failed to join the Syncshell. This is due to one of following reasons:\n- The Syncshell does not exist or the password is incorrect\n- You are already in that Syncshell or are banned from that Syncshell\n- The Syncshell is at capacity or has invites disabled\n"), ImGuiColors.DalamudYellow);
            }
        }
        else
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.22dcef49", "You are about to join the Syncshell ") + _groupJoinInfo.GroupAliasOrGID + _uiSharedService.L("UI.JoinSyncshellUI.d20eeb48", " by ") + _groupJoinInfo.OwnerAliasOrUID);
            ImGuiHelpers.ScaledDummy(2f);
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.834e3e0d", "This Syncshell staff has set the following suggested Syncshell permissions:"));
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.e658c42d", "- Sounds "));
            _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableSounds());
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.269f0654", "- Animations"));
            _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations());
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.b3942667", "- VFX"));
            _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableVFX());

            if (_groupJoinInfo.GroupPermissions.IsPreferDisableSounds() != _ownPermissions.DisableGroupSounds
                || _groupJoinInfo.GroupPermissions.IsPreferDisableVFX() != _ownPermissions.DisableGroupVFX
                || _groupJoinInfo.GroupPermissions.IsPreferDisableAnimations() != _ownPermissions.DisableGroupAnimations)
            {
                ImGuiHelpers.ScaledDummy(2f);
                UiSharedService.ColorText(_uiSharedService.L("UI.JoinSyncshellUI.5f08c0ab", "Your current preferred default Syncshell permissions deviate from the suggested permissions:"), ImGuiColors.DalamudYellow);
                if (_groupJoinInfo.GroupPermissions.IsPreferDisableSounds() != _ownPermissions.DisableGroupSounds)
                {
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.cc79adf2", "- Sounds"));
                    _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupSounds);
                    ImGui.SameLine(200);
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.7fcd8d44", "Suggested"));
                    _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableSounds());
                    ImGui.SameLine();
                    using var id = ImRaii.PushId("suggestedSounds");
                    if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.JoinSyncshellUI.4a99fb58", "Apply suggested")))
                    {
                        _ownPermissions.DisableGroupSounds = _groupJoinInfo.GroupPermissions.IsPreferDisableSounds();
                    }
                }
                if (_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations() != _ownPermissions.DisableGroupAnimations)
                {
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.269f0654", "- Animations"));
                    _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupAnimations);
                    ImGui.SameLine(200);
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.7fcd8d44", "Suggested"));
                    _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations());
                    ImGui.SameLine();
                    using var id = ImRaii.PushId("suggestedAnims");
                    if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.JoinSyncshellUI.4a99fb58", "Apply suggested")))
                    {
                        _ownPermissions.DisableGroupAnimations = _groupJoinInfo.GroupPermissions.IsPreferDisableAnimations();
                    }
                }
                if (_groupJoinInfo.GroupPermissions.IsPreferDisableVFX() != _ownPermissions.DisableGroupVFX)
                {
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.b3942667", "- VFX"));
                    _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupVFX);
                    ImGui.SameLine(200);
                    ImGui.AlignTextToFramePadding();
                    ImGui.TextUnformatted(_uiSharedService.L("UI.JoinSyncshellUI.7fcd8d44", "Suggested"));
                    _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableVFX());
                    ImGui.SameLine();
                    using var id = ImRaii.PushId("suggestedVfx");
                    if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.JoinSyncshellUI.4a99fb58", "Apply suggested")))
                    {
                        _ownPermissions.DisableGroupVFX = _groupJoinInfo.GroupPermissions.IsPreferDisableVFX();
                    }
                }
                UiSharedService.TextWrapped(_uiSharedService.L("UI.JoinSyncshellUI.a78db077", "Note: you do not need to apply the suggested Syncshell permissions, they are solely suggestions by the staff of the Syncshell."));
            }
            else
            {
                UiSharedService.TextWrapped(_uiSharedService.L("UI.JoinSyncshellUI.ac29fe42", "Your default syncshell permissions on joining are in line with the suggested Syncshell permissions through the owner."));
            }
            ImGuiHelpers.ScaledDummy(2f);
            if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Plus, _uiSharedService.L("UI.JoinSyncshellUI.f8f230da", "Finalize and join ") + _groupJoinInfo.GroupAliasOrGID))
            {
                GroupUserPreferredPermissions joinPermissions = GroupUserPreferredPermissions.NoneSet;
                joinPermissions.SetDisableSounds(_ownPermissions.DisableGroupSounds);
                joinPermissions.SetDisableAnimations(_ownPermissions.DisableGroupAnimations);
                joinPermissions.SetDisableVFX(_ownPermissions.DisableGroupVFX);
                _ = _apiController.GroupJoinFinalize(new GroupJoinDto(_groupJoinInfo.Group, _previousPassword, joinPermissions));
                IsOpen = false;
            }
        }
    }
}