using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;

namespace RavaSync.UI;

public class PermissionWindowUI : WindowMediatorSubscriberBase
{
    public Pair Pair { get; init; }

    private readonly UiSharedService _uiSharedService;
    private readonly ApiController _apiController;
    private UserPermissions _ownPermissions;
    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public PermissionWindowUI(ILogger<PermissionWindowUI> logger, Pair pair, MareMediator mediator, UiSharedService uiSharedService,
        ApiController apiController, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "Permissions for " + pair.UserData.AliasOrUID + "###RavaSyncPermissions" + pair.UserData.UID, performanceCollectorService)
    {
        Pair = pair;
        _uiSharedService = uiSharedService;
        _apiController = apiController;
        _ownPermissions = pair.UserPair.OwnPermissions.DeepClone();
        Flags = ImGuiWindowFlags.NoScrollbar | ImGuiWindowFlags.NoResize;
        SizeConstraints = new()
        {
            MinimumSize = new(450, 100),
            MaximumSize = new(450, 500)
        };
        IsOpen = true;
    }

    protected override void DrawInternal()
    {
        var sticky = _ownPermissions.IsSticky();
        var paused = _ownPermissions.IsPaused();
        var disableSounds = _ownPermissions.IsDisableSounds();
        var disableAnimations = _ownPermissions.IsDisableAnimations();
        var disableVfx = _ownPermissions.IsDisableVFX();
        var disableCustomize = _ownPermissions.IsDisableCustomizePlus();
        var disableMetaData = _ownPermissions.IsDisableMetaData();
        var style = ImGui.GetStyle();
        var indentSize = ImGui.GetFrameHeight() + style.ItemSpacing.X;

        _uiSharedService.BigText(_uiSharedService.L("UI.PermissionWindowUI.d41d8cd9", "") + Pair.UserData.AliasOrUID);
        ImGuiHelpers.ScaledDummy(1f);

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.453d261e", "Preferred Permissions"), ref sticky))
        {
            _ownPermissions.SetSticky(sticky);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.ea4d805b", "Preferred Permissions, when enabled, will exclude this user from any permission changes on any syncshells you share with this user."));

        ImGuiHelpers.ScaledDummy(1f);


        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.458e48be", "Pause Sync"), ref paused))
        {
            _ownPermissions.SetPaused(paused);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.c462fb5d", "Pausing will completely cease any sync with this user.") + UiSharedService.TooltipSeparator
            + "Note: this is bidirectional, either user pausing will cease sync completely.");
        var otherPerms = Pair.UserPair.OtherPermissions;

        var otherIsPaused = otherPerms.IsPaused();
        var otherDisableSounds = otherPerms.IsDisableSounds();
        var otherDisableAnimations = otherPerms.IsDisableAnimations();
        var otherDisableVFX = otherPerms.IsDisableVFX();

        using (ImRaii.PushIndent(indentSize, false))
        {
            _uiSharedService.BooleanToColoredIcon(!otherIsPaused, false);
            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.Text(Pair.UserData.AliasOrUID + _uiSharedService.L("UI.PermissionWindowUI.976d28da", " has ") + (!otherIsPaused ? _uiSharedService.L("UI.PermissionWindowUI.dc1b7dcc", "not ") : string.Empty) + _uiSharedService.L("UI.PermissionWindowUI.a1ae2431", "paused you"));
        }

        ImGuiHelpers.ScaledDummy(0.5f);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(0.5f);

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.750f537e", "Disable Sounds"), ref disableSounds))
        {
            _ownPermissions.SetDisableSounds(disableSounds);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.9d3bd330", "Disabling sounds will remove all sounds synced with this user on both sides.") + UiSharedService.TooltipSeparator
            + "Note: this is bidirectional, either user disabling sound sync will stop sound sync on both sides.");
        using (ImRaii.PushIndent(indentSize, false))
        {
            _uiSharedService.BooleanToColoredIcon(!otherDisableSounds, false);
            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.Text(Pair.UserData.AliasOrUID + _uiSharedService.L("UI.PermissionWindowUI.976d28da", " has ") + (!otherDisableSounds ? _uiSharedService.L("UI.PermissionWindowUI.dc1b7dcc", "not ") : string.Empty) + _uiSharedService.L("UI.PermissionWindowUI.e62defc5", "disabled sound sync with you"));
        }

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.ba0e876d", "Disable Animations"), ref disableAnimations))
        {
            _ownPermissions.SetDisableAnimations(disableAnimations);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.72ac39b9", "Disabling sounds will remove all animations synced with this user on both sides.") + UiSharedService.TooltipSeparator
            + "Note: this is bidirectional, either user disabling animation sync will stop animation sync on both sides.");
        using (ImRaii.PushIndent(indentSize, false))
        {
            _uiSharedService.BooleanToColoredIcon(!otherDisableAnimations, false);
            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.Text(Pair.UserData.AliasOrUID + _uiSharedService.L("UI.PermissionWindowUI.976d28da", " has ") + (!otherDisableAnimations ? _uiSharedService.L("UI.PermissionWindowUI.dc1b7dcc", "not ") : string.Empty) + _uiSharedService.L("UI.PermissionWindowUI.9afbe430", "disabled animation sync with you"));
        }

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.b5659523", "Disable VFX"), ref disableVfx))
        {
            _ownPermissions.SetDisableVFX(disableVfx);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.1f632e7e", "Disabling sounds will remove all VFX synced with this user on both sides.") + UiSharedService.TooltipSeparator
            + "Note: this is bidirectional, either user disabling VFX sync will stop VFX sync on both sides.");
        using (ImRaii.PushIndent(indentSize, false))
        {
            _uiSharedService.BooleanToColoredIcon(!otherDisableVFX, false);
            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.Text(Pair.UserData.AliasOrUID + _uiSharedService.L("UI.PermissionWindowUI.976d28da", " has ") + (!otherDisableVFX ? _uiSharedService.L("UI.PermissionWindowUI.dc1b7dcc", "not ") : string.Empty) + _uiSharedService.L("UI.PermissionWindowUI.3a092b2b", "disabled VFX sync with you"));
        }

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.f6eb9eee", "Disable Customize+"), ref disableCustomize))
        {
            _ownPermissions.SetDisableCustomizePlus(disableCustomize);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.def2d628", "Not all that down with the thiccness? PP Large enough to satisfy an elephant?") + UiSharedService.TooltipSeparator 
            + "This will disable all that and more!");

        if (ImGui.Checkbox(_uiSharedService.L("UI.PermissionWindowUI.2ea5261a", "Disable Height Metadata"), ref disableMetaData))
        {
            _ownPermissions.SetDisableMetaData(disableMetaData);
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.PermissionWindowUI.d9598eed", "Too tall? Too Small? Not anymore. This disables any height edits."));


        ImGuiHelpers.ScaledDummy(0.5f);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(0.5f);

        bool hasChanges = _ownPermissions != Pair.UserPair.OwnPermissions;

        using (ImRaii.Disabled(!hasChanges))
            if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Save, _uiSharedService.L("UI.PermissionWindowUI.28599bfe", "Save")))
            {
                _ = _apiController.SetBulkPermissions(new(
                    new(StringComparer.Ordinal)
                    {
                        { Pair.UserData.UID, _ownPermissions }
                    },
                    new(StringComparer.Ordinal)
                ));
            }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.PermissionWindowUI.b33e9980", "Save and apply all changes"));

        var rightSideButtons = _uiSharedService.GetIconTextButtonSize(Dalamud.Interface.FontAwesomeIcon.Undo, _uiSharedService.L("UI.PermissionWindowUI.9bc94c90", "Revert")) +
            _uiSharedService.GetIconTextButtonSize(Dalamud.Interface.FontAwesomeIcon.ArrowsSpin, _uiSharedService.L("UI.PermissionWindowUI.0391efbb", "Reset to Default"));
        var availableWidth = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;

        ImGui.SameLine(availableWidth - rightSideButtons);

        using (ImRaii.Disabled(!hasChanges))
            if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.Undo, _uiSharedService.L("UI.PermissionWindowUI.9bc94c90", "Revert")))
            {
                _ownPermissions = Pair.UserPair.OwnPermissions.DeepClone();
            }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.PermissionWindowUI.661d908b", "Revert all changes"));

        ImGui.SameLine();
        if (_uiSharedService.IconTextButton(Dalamud.Interface.FontAwesomeIcon.ArrowsSpin, _uiSharedService.L("UI.PermissionWindowUI.0391efbb", "Reset to Default")))
        {
            var defaultPermissions = _apiController.DefaultPermissions!;
            _ownPermissions.SetSticky(Pair.IsDirectlyPaired || defaultPermissions.IndividualIsSticky);
            _ownPermissions.SetPaused(false);
            _ownPermissions.SetDisableVFX(Pair.IsDirectlyPaired ? defaultPermissions.DisableIndividualVFX : defaultPermissions.DisableGroupVFX);
            _ownPermissions.SetDisableSounds(Pair.IsDirectlyPaired ? defaultPermissions.DisableIndividualSounds : defaultPermissions.DisableGroupSounds);
            _ownPermissions.SetDisableAnimations(Pair.IsDirectlyPaired ? defaultPermissions.DisableIndividualAnimations : defaultPermissions.DisableGroupAnimations);
            _ = _apiController.SetBulkPermissions(new(
                new(StringComparer.Ordinal)
                {
                    { Pair.UserData.UID, _ownPermissions }
                },
                new(StringComparer.Ordinal)
            ));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.PermissionWindowUI.0865b17d", "This will set all permissions to your defined default permissions in the RavaSync Settings"));

        var ySize = ImGui.GetCursorPosY() + style.FramePadding.Y * ImGuiHelpers.GlobalScale + style.FrameBorderSize;
        ImGui.SetWindowSize(new(400, ySize));
    }

    public override void OnClose()
    {
        Mediator.Publish(new RemoveWindowMessage(this));
    }
}
