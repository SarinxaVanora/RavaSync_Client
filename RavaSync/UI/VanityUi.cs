using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System;
using System.Numerics;
using System.Threading.Tasks;

namespace RavaSync.UI;

public sealed class VanityUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiShared;
    private readonly ApiController _apiController;

    private string _newUserAlias = string.Empty;
    private string _vanityStatusMessage = string.Empty;
    private Vector4 _vanityStatusColor = ImGuiColors.DalamudWhite;

    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public VanityUi(
        ILogger<VanityUi> logger,
        UiSharedService uiShared,
        ApiController apiController,
        MareMediator mediator,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — Vanity", performanceCollectorService)
    {
        _uiShared = uiShared;
        _apiController = apiController;

        RespectCloseHotkey = true;
        SizeConstraints = new()
        {
            MinimumSize = new(450 * ImGuiHelpers.GlobalScale, 220 * ImGuiHelpers.GlobalScale),
            MaximumSize = new(650 * ImGuiHelpers.GlobalScale, 400 * ImGuiHelpers.GlobalScale),
        };
    }

    protected override void DrawInternal()
    {
        _uiShared.BigText("Set Account Vanity", ImGuiColors.ParsedGreen);
        UiSharedService.TextWrapped("Set a friendly ID that others can use instead of your UID.");

        ImGuiHelpers.ScaledDummy(5);

        ImGui.InputTextWithHint(
            "##alias_input",
            "new-vanity (5–15 chars: A–Z, 0–9, _ or -)",
            ref _newUserAlias,
            32);

        _uiShared.DrawHelpText("Allowed: letters, numbers, underscore and hyphen, 5–15 characters.");

        ImGuiHelpers.ScaledDummy(5);

        using (ImRaii.Disabled(string.IsNullOrWhiteSpace(_newUserAlias)))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Tag, "Set Vanity"))
            {
                _vanityStatusMessage = "Submitting…";
                _vanityStatusColor = ImGuiColors.DalamudYellow;

                var aliasToSubmit = _newUserAlias; 

                _ = Task.Run(async () =>
                {
                    try
                    {
                        var err = await _apiController.UserSetAlias(aliasToSubmit).ConfigureAwait(false);

                        if (string.IsNullOrEmpty(err))
                        {
                            _vanityStatusMessage = "Vanity updated.";
                            _vanityStatusColor = ImGuiColors.ParsedGreen;

                            _newUserAlias = string.Empty;

                            // Reconnect so the new vanity applies everywhere
                            await _apiController.CreateConnectionsAsync().ConfigureAwait(false);
                        }
                        else
                        {
                            _vanityStatusMessage = err;
                            _vanityStatusColor = ImGuiColors.DalamudRed;
                        }
                    }
                    catch (Exception ex)
                    {
                        _vanityStatusMessage = "Failed to set Vanity: " + ex.Message;
                        _vanityStatusColor = ImGuiColors.DalamudRed;
                    }
                });
            }
        }

        ImGuiHelpers.ScaledDummy(5);

        if (!string.IsNullOrEmpty(_vanityStatusMessage))
        {
            UiSharedService.ColorTextWrapped(_vanityStatusMessage, _vanityStatusColor);
        }

        ImGuiHelpers.ScaledDummy(5);
        ImGui.TextWrapped(
            "You can share this Vanity with friends instead of your full UID. " +
            "If the name is taken or invalid, you'll see an error above.");
    }
}
