using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System;
using System.Numerics;

namespace RavaSync.UI;

public sealed class DiscoverySettingsUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiShared;
    private readonly MareConfigService _configService;

    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public DiscoverySettingsUi(
        ILogger<DiscoverySettingsUi> logger,
        MareMediator mediator,
        UiSharedService uiShared,
        MareConfigService configService,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — Discovery Settings", performanceCollectorService)
    {
        _uiShared = uiShared;
        _configService = configService;

        RespectCloseHotkey = true;
        SizeConstraints = new()
        {
            MinimumSize = new(500 * ImGuiHelpers.GlobalScale, 320 * ImGuiHelpers.GlobalScale),
            MaximumSize = new(800 * ImGuiHelpers.GlobalScale, 700 * ImGuiHelpers.GlobalScale),
        };
    }

    protected override void DrawInternal()
    {
        _uiShared.BigText("Discovery & Presence", ImGuiColors.DalamudViolet);

        UiSharedService.TextWrapped(_uiShared.L("UI.DiscoverySettingsUi.3ebc5831", "Discovery is entirely opt-in. When you turn it on, other RavaSync users who have also opted in can see that you're on RavaSync (with a little ♥) and can right-click you to send a pair request — and you can do the same to them. If you leave it off, you're effectively invisible: no hearts, no right-click pair option, and people will need your UID to pair with you."));

        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(5);

        bool discoveryPresence = _configService.Current.EnableRavaDiscoveryPresence;
        if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.e6c3b6f6", "Show RavaSync presence to other RavaSync users?"), ref discoveryPresence))
        {
            _configService.Current.EnableRavaDiscoveryPresence = discoveryPresence;
            _configService.Save();
        }
        _uiShared.DrawHelpText(
            "When enabled: you and other RavaSync users who also opted in can see each other's ♥ " +
            "and use the right-click \"Send pair request\" option. When disabled: you don't participate " +
            "in discovery at all and stay hidden."
        );

        ImGuiHelpers.ScaledDummy(5);

        bool enablePairRequestMenu = _configService.Current.EnableSendPairRequestContextMenu;
        using (ImRaii.Disabled(!discoveryPresence))
        {
            if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.EE0E6297", "Show \"Send pair request\" in right-click menu"), ref enablePairRequestMenu))
            {
                _configService.Current.EnableSendPairRequestContextMenu = enablePairRequestMenu;
                _configService.Save();
            }
        }
        _uiShared.DrawHelpText(
            "Adds a \"Send pair request\" option to the right-click menu for RavaSync users " +
            "that you're not already paired with. Requires discovery to be enabled."
        );

        ImGuiHelpers.ScaledDummy(5);

        bool autoDecline = _configService.Current.AutoDeclineIncomingPairRequests;
        if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.3bb79ae6", "Automatically decline incoming pair requests"), ref autoDecline))
        {
            _configService.Current.AutoDeclineIncomingPairRequests = autoDecline;
            _configService.Save();
        }
        _uiShared.DrawHelpText(
            "Exactly what it says on the tin. If you don't want randoms sending you pair requests, " +
            "turn this on and they'll be politely told no."
        );

        ImGuiHelpers.ScaledDummy(5);

        bool showHeart = _configService.Current.ShowFriendshapedHeart;
        using (ImRaii.Disabled(!discoveryPresence))
        {
            if (ImGui.Checkbox(_uiShared.L("UI.DiscoverySettingsUi.FFF66944", "Show â¥ on RavaSync users not yet paired"), ref showHeart))
            {
                _configService.Current.ShowFriendshapedHeart = showHeart;
                _configService.Save();
            }
        }
        _uiShared.DrawHelpText(
            "Controls the little ♥ on nameplates for other RavaSync users that you're not paired with yet. " +
            "Purely cosmetic and only relevant when discovery is turned on."
        );
    }
}
