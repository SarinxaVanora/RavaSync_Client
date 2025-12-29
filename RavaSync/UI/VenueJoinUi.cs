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
            UiSharedService.TextWrapped("Would you like to join this venue's public shell?");
            ImGuiHelpers.ScaledDummy(6f);

            using (ImRaii.Group())
            {
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.PlusCircle, "Yes"))
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

                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.TimesCircle, "No"))
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
            UiSharedService.TextWrapped(
                "Failed to join the Syncshell. This is due to one of the following reasons:" + Environment.NewLine +
                "- You are already in that Syncshell or are banned from that Syncshell" + Environment.NewLine +
                "- The Syncshell is at capacity or has invites disabled" + Environment.NewLine +
                "- The Syncshell could not be found" + Environment.NewLine);
            ImGuiHelpers.ScaledDummy(2f);
            using (ImRaii.Disabled(false))
            {
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.TimesCircle, "Close"))
                    IsOpen = false;
            }
            return;
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // STEP 2: Finalize
        // ─────────────────────────────────────────────────────────────────────────────
        ImGui.TextUnformatted("You are about to join the Syncshell " + _groupJoinInfo.GroupAliasOrGID + " by " + _groupJoinInfo.OwnerAliasOrUID);
        ImGuiHelpers.ScaledDummy(2f);
        ImGui.TextUnformatted("This Syncshell staff has set the following suggested Syncshell permissions:");
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("- Sounds ");
        _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableSounds());
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("- Animations");
        _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations());
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("- VFX");
        _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableVFX());

        if (_groupJoinInfo.GroupPermissions.IsPreferDisableSounds() != _ownPermissions.DisableGroupSounds
            || _groupJoinInfo.GroupPermissions.IsPreferDisableVFX() != _ownPermissions.DisableGroupVFX
            || _groupJoinInfo.GroupPermissions.IsPreferDisableAnimations() != _ownPermissions.DisableGroupAnimations)
        {
            ImGuiHelpers.ScaledDummy(2f);
            UiSharedService.ColorText("Your current preferred default Syncshell permissions deviate from the suggested permissions:", ImGuiColors.DalamudYellow);
            if (_groupJoinInfo.GroupPermissions.IsPreferDisableSounds() != _ownPermissions.DisableGroupSounds)
            {
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("- Sounds");
                _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupSounds);
                ImGui.SameLine(200);
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("Suggested");
                _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableSounds());
                ImGui.SameLine();
                using var id = ImRaii.PushId("suggestedSounds");
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, "Apply suggested"))
                {
                    _ownPermissions.DisableGroupSounds = _groupJoinInfo.GroupPermissions.IsPreferDisableSounds();
                }
            }
            if (_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations() != _ownPermissions.DisableGroupAnimations)
            {
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("- Animations");
                _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupAnimations);
                ImGui.SameLine(200);
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("Suggested");
                _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableAnimations());
                ImGui.SameLine();
                using var id = ImRaii.PushId("suggestedAnims");
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, "Apply suggested"))
                {
                    _ownPermissions.DisableGroupAnimations = _groupJoinInfo.GroupPermissions.IsPreferDisableAnimations();
                }
            }
            if (_groupJoinInfo.GroupPermissions.IsPreferDisableVFX() != _ownPermissions.DisableGroupVFX)
            {
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("- VFX");
                _uiSharedService.BooleanToColoredIcon(!_ownPermissions.DisableGroupVFX);
                ImGui.SameLine(200);
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("Suggested");
                _uiSharedService.BooleanToColoredIcon(!_groupJoinInfo.GroupPermissions.IsPreferDisableVFX());
                ImGui.SameLine();
                using var id = ImRaii.PushId("suggestedVfx");
                if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowRight, "Apply suggested"))
                {
                    _ownPermissions.DisableGroupVFX = _groupJoinInfo.GroupPermissions.IsPreferDisableVFX();
                }
            }
            UiSharedService.TextWrapped("Note: you do not need to apply the suggested Syncshell permissions, they are solely suggestions by the staff.");
        }
        else
        {
            UiSharedService.TextWrapped("Your default syncshell permissions on joining are in line with the suggested Syncshell permissions through the owner.");
        }

        ImGuiHelpers.ScaledDummy(2f);
        if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Plus, "Finalize and join " + _groupJoinInfo.GroupAliasOrGID))
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
