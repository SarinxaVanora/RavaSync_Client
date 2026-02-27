using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.UI;

public sealed class DiscoveryIntroUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiShared;
    private readonly MareConfigService _configService;

    private bool _initialized = false;
    private bool _tempDiscoveryOptIn = false;

    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public DiscoveryIntroUi(
        ILogger<DiscoveryIntroUi> logger,
        UiSharedService uiShared,
        MareConfigService configService,
        MareMediator mediator,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — Discovery", performanceCollectorService)
    {
        _uiShared = uiShared;
        _configService = configService;

        RespectCloseHotkey = true;
        SizeConstraints = new()
        {
            MinimumSize = new(550 * ImGuiHelpers.GlobalScale, 320 * ImGuiHelpers.GlobalScale),
            MaximumSize = new(900 * ImGuiHelpers.GlobalScale, 800 * ImGuiHelpers.GlobalScale)
        };
    }

    protected override void DrawInternal()
    {
        if (_configService.Current.SeenDiscoveryIntro)
        {
            IsOpen = false;
            return;
        }

        if (!_initialized)
        {
            _tempDiscoveryOptIn = _configService.Current.EnableRavaDiscoveryPresence;
            _initialized = true;
        }

        _uiShared.BigText("Discovery & Presence");

        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(5);

        UiSharedService.ColorTextWrapped(_uiShared.L("UI.DiscoveryIntroUi.8fdf0d11", "Discovery is an entirely optional extra on top of normal pairing."),
            ImGuiColors.DalamudViolet);

        UiSharedService.TextWrapped(_uiShared.L("UI.DiscoveryIntroUi.cbceb58c", "When you opt in, other RavaSync users who have also opted in can see that you're on RavaSync. In practice, that means:\n• A little ♥ can appear on your nameplate for them (and theirs for you).\n• You can right-click each other and send pair requests directly.\n\nIf you keep this off, you stay invisible to discovery. No hearts, no extra right-click option, and people will need your UID to pair."));

        ImGuiHelpers.ScaledDummy(10);

        bool allowDiscovery = _tempDiscoveryOptIn;
        if (ImGui.Checkbox(_uiShared.L("UI.DiscoveryIntroUi.93c9039e", "Show my RavaSync presence to other RavaSync users"), ref allowDiscovery))
        {
            _tempDiscoveryOptIn = allowDiscovery;
        }
        _uiShared.DrawHelpText(_uiShared.L("UI.DiscoveryIntroUi.OptIn.Help", "Turn this on if you're happy for other RavaSync users around you to see that you're using RavaSync and send you pair requests."));
ImGuiHelpers.ScaledDummy(10);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(5);

        ImGui.TextWrapped(_uiShared.L("UI.DiscoveryIntroUi.2cab8f56", "You can change this again at any time in Settings - Discovery Settings."));

        ImGuiHelpers.ScaledDummy(10);

        using (var col = ImRaii.PushColor(ImGuiCol.Button, ImGuiColors.DalamudViolet))
        using (var col2 = ImRaii.PushColor(ImGuiCol.ButtonHovered, ImGuiColors.DalamudViolet))
        {
            if (ImGui.Button(_uiShared.L("UI.DiscoveryIntroUi.e9b450d1", "Done"), new(120 * ImGuiHelpers.GlobalScale, 0)))
            {
                _configService.Current.EnableRavaDiscoveryPresence = _tempDiscoveryOptIn;
                _configService.Current.SeenDiscoveryIntro = true;
                _configService.Save();
                IsOpen = false;
            }
        }

        ImGui.SameLine();
    }
}
