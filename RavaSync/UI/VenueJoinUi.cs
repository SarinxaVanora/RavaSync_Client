using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto;
using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.Venue;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System.Numerics;
using System.Threading.Tasks;

namespace RavaSync.UI;

public sealed class VenueJoinUi : WindowMediatorSubscriberBase
{
    private const string DummyPassword = "JoinedViaAuto";

    private readonly MareMediator _mediator;
    private readonly MareConfigService _cfg;
    private readonly ApiController _api;
    private readonly UiSharedService _uiSharedService;
    private readonly PairManager _pairManager;

    private string _canonicalKey = string.Empty;
    private string _venueName = string.Empty;
    private string _shellGid = string.Empty;
    private bool _askJoinFirst;
    private bool _centerOnAppear;


    // state mirrored from JoinSyncshellUI
    private GroupJoinInfoDto? _groupJoinInfo = null;
    private DefaultPermissionsDto _ownPermissions = null!;
    private string _previousPassword = string.Empty;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public VenueJoinUi(
        ILogger<VenueJoinUi> logger,
        MareMediator mediator,
        MareConfigService cfg,
        ApiController api,
        UiSharedService uiSharedService,
        PerformanceCollectorService performanceCollectorService,
        PairManager pairManager)
        : base(logger, mediator, "RavaSync — Venue Invite", performanceCollectorService)
    {
        _mediator = mediator;
        _cfg = cfg;
        _api = api;
        _uiSharedService = uiSharedService;
        _pairManager = pairManager;

        Flags = ImGuiWindowFlags.NoCollapse | ImGuiWindowFlags.AlwaysAutoResize;
        Size = new Vector2(520, 170);
        SizeCondition = ImGuiCond.Appearing;

        Mediator.Subscribe<OpenVenueJoinUiMessage>(this, OnOpen);
    }

    private void OnOpen(OpenVenueJoinUiMessage msg)
    {
        // hydrate source data from the venue detector
        _canonicalKey = msg.CanonicalKey;
        _venueName = msg.VenueName;
        _shellGid = msg.ShellGid;

        _logger.LogInformation($"{_canonicalKey} | {_venueName} | {_shellGid}");

        // clone the exact JoinSyncshellUI init semantics
        _groupJoinInfo = null;
        _ownPermissions = _api.DefaultPermissions.DeepClone()!;
        _previousPassword = DummyPassword;
        _askJoinFirst = true;
        _centerOnAppear = true;
        IsOpen = true;
    }
    protected override void DrawInternal()
    {
        // center once on appear
        if (_centerOnAppear)
        {
            var vp = ImGui.GetMainViewport();
            Position = vp.GetCenter() - (Size / 2f);
            _centerOnAppear = false;
        }

        // HEADER
        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted(
                _askJoinFirst
                    ? $"Welcome to {_venueName}"
                    : (_groupJoinInfo == null || !_groupJoinInfo.Success
                        ? "Join Syncshell"
                        : $"Finalize join Syncshell {_groupJoinInfo.GroupAliasOrGID}")
            );
        ImGui.Separator();

        // ─────────────────────────────────────────────────────────────────────────────
        // STEP 0: Ask first — simple Yes/No
        // ─────────────────────────────────────────────────────────────────────────────
        if (_askJoinFirst)
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.VenueJoinUi.18c5a737", "Would you like to join this venue's public shell?"));
            ImGuiHelpers.ScaledDummy(6f);

            using (ImRaii.Group())
            {
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.PlusCircle, _uiSharedService.L("UI.VenueJoinUi.d05f5d3c", "Yes")))
                {
                    // Mirror JoinSyncshellUI "Join" click
                    try
                    {
                        _previousPassword = DummyPassword;
                        _groupJoinInfo = _api.VenueJoin(
                            new GroupPasswordDto(new RavaSync.API.Data.GroupData(_shellGid), _previousPassword)
                        ).Result;
                    }
                    catch
                    {
                        _groupJoinInfo = null;
                    }

                    _askJoinFirst = false; // advance to finalize/failure screens
                }

                ImGui.SameLine();

                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.TimesCircle, _uiSharedService.L("UI.VenueJoinUi.ca53aaf1", "No")))
                {
                    IsOpen = false;
                }
            }

            return;
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // STEP 1: Failure block
        // ─────────────────────────────────────────────────────────────────────────────
        if (_groupJoinInfo == null || !_groupJoinInfo.Success)
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.VenueJoinUi.4ad67727", "Failed to join the Syncshell. This is due to one of the following reasons:\n- You are already in that Syncshell or are banned from that Syncshell\n- The Syncshell is at capacity or has invites disabled\n- The Syncshell could not be found\n"));
            ImGuiHelpers.ScaledDummy(2f);
            using (ImRaii.Disabled(false))
            {
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.TimesCircle, _uiSharedService.L("UI.VenueJoinUi.18e44fdd", "Close")))
                    IsOpen = false;
            }
            return;
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // STEP 2: Finalize
        // ─────────────────────────────────────────────────────────────────────────────
        ImGui.TextUnformatted(_uiSharedService.L("UI.VenueJoinUi.203c2e3c", "You are about to join the Syncshell ") + _groupJoinInfo.GroupAliasOrGID + _uiSharedService.L("UI.VenueJoinUi.5287faf1", " by ") + _groupJoinInfo.OwnerAliasOrUID);
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
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.VenueJoinUi.68db1a22", "Apply suggested")))
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
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.VenueJoinUi.68db1a22", "Apply suggested")))
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
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.VenueJoinUi.68db1a22", "Apply suggested")))
                {
                    _ownPermissions.DisableGroupVFX = _groupJoinInfo.GroupPermissions.IsPreferDisableVFX();
                }
            }
            UiSharedService.TextWrapped(_uiSharedService.L("UI.VenueJoinUi.f4e3a220", "Note: you do not need to apply the suggested Syncshell permissions, they are solely suggestions by the staff."));
        }
        else
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.JoinSyncshellUI.ac29fe42", "Your default syncshell permissions on joining are in line with the suggested Syncshell permissions through the owner."));
        }

        ImGuiHelpers.ScaledDummy(2f);
        if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Plus, _uiSharedService.L("UI.VenueJoinUi.4b7b8469", "Finalize and join ") + _groupJoinInfo.GroupAliasOrGID))
        {
            GroupUserPreferredPermissions joinPermissions = GroupUserPreferredPermissions.NoneSet;
            joinPermissions.SetDisableSounds(_ownPermissions.DisableGroupSounds);
            joinPermissions.SetDisableAnimations(_ownPermissions.DisableGroupAnimations);
            joinPermissions.SetDisableVFX(_ownPermissions.DisableGroupVFX);

            _ = _api.VenueJoinFinalize(new GroupJoinDto(_groupJoinInfo.Group, _previousPassword, joinPermissions));

            IsOpen = false;
        }
    }

}
