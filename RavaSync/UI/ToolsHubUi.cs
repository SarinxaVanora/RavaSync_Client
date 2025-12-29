using System;
using System.Numerics;
using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Plugin.Services;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.UI;

public sealed class ToolsHubUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiSharedService;
    private readonly ICommandManager _commandManager;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public ToolsHubUi(
        ILogger<ToolsHubUi> logger,
        MareMediator mediator,
        UiSharedService uiSharedService,
        ICommandManager commandManager,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — Tools Hub", performanceCollectorService)
    {
        _uiSharedService = uiSharedService;
        _commandManager = commandManager;

        RespectCloseHotkey = true;
        Size = new Vector2(350 * ImGuiHelpers.GlobalScale, 550 * ImGuiHelpers.GlobalScale);
        SizeCondition = ImGuiCond.FirstUseEver;
    }

    protected override void DrawInternal()
    {
        ImGui.TextUnformatted("RavaSync tools");
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.Columns(2, "tools-top-cols", false);

        void ToolButton(FontAwesomeIcon icon, string label, string tooltip, Action? onClick = null)
        {
            if (_uiSharedService.IconTextButton(icon, label, -1))
                onClick?.Invoke();

            if (ImGui.IsItemHovered())
            {
                ImGui.BeginTooltip();
                ImGui.TextUnformatted(tooltip);
                ImGui.EndTooltip();
            }

            ImGui.Spacing();
        }

        ToolButton(FontAwesomeIcon.Running, "Character data hub",
            "Open Character Data Hub",
            () => Mediator.Publish(new UiToggleMessage(typeof(CharaDataHubUi))));

        ToolButton(FontAwesomeIcon.Search, "Character analysis",
            "Open Character Analysis",
            () => Mediator.Publish(new UiToggleMessage(typeof(DataAnalysisUi))));

        ToolButton(FontAwesomeIcon.UserCircle, "RavaSync profile",
            "Edit RavaSync Profile",
            () => Mediator.Publish(new UiToggleMessage(typeof(EditProfileUi))));


        ImGui.NextColumn();

        ToolButton(FontAwesomeIcon.Users, "Join venue shell",
            "Join the shell in your current location if one has been linked.",
            () => Mediator.Publish(new UiToggleMessage(typeof(VenueJoinUi))));
        
        ToolButton(FontAwesomeIcon.Users, "Set or edit Vanity",
            "Setup Vanity (custom ID) here!",
            () => Mediator.Publish(new UiToggleMessage(typeof(VanityUi))));
        
        ToolButton(FontAwesomeIcon.Users, "Discovery Settings",
            "Change settings relating to the Discovery Service.",
            () => Mediator.Publish(new UiToggleMessage(typeof(DiscoverySettingsUi))));

        ImGui.Columns(1);
        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.TextUnformatted("General shortcuts");
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.Columns(2, "tools-bottom-cols", false);

        void CommandTool(FontAwesomeIcon icon, string label, string tooltip, string command)
            => ToolButton(icon, label, tooltip, () => _commandManager.ProcessCommand(command));

        CommandTool(FontAwesomeIcon.Plug, "Plogons installer", "Open plogon install menu", "/xlplugins");
        CommandTool(FontAwesomeIcon.Cogs, "Plogon Settings", "Open plogon settings", "/xlsettings");
        CommandTool(FontAwesomeIcon.ClipboardList, "Plogon Logs", "Open plogon logs", "/xllog");
        CommandTool(FontAwesomeIcon.BookOpen, "Wordsmith", "Open a new scratchpad", "/scratchpad");
        CommandTool(FontAwesomeIcon.Eye, "Peeping Tom", "Open Peeping Tom", "/ptom");
        CommandTool(FontAwesomeIcon.MapPin, "Maplink", "Open MapLink", "/maplink");
        CommandTool(FontAwesomeIcon.MapMarked, "Map Party Assist", "Open Map Party Assist", "/mparty");
        CommandTool(FontAwesomeIcon.HouseFire, "Burning Down The house", "Open BDTH", "/bdth");
        CommandTool(FontAwesomeIcon.HouseUser, "(Re)MakePlace", "Open MakePlace", "/makeplace");
        CommandTool(FontAwesomeIcon.Dragon, "VFXEditor", "Open VFXEditor", "/vfxedit");


        ImGui.NextColumn();

        CommandTool(FontAwesomeIcon.Magic, "Penumbra", "Open Penumbra", "/penumbra");
        CommandTool(FontAwesomeIcon.Palette, "Glamourer", "Open Glamourer", "/glamourer");
        CommandTool(FontAwesomeIcon.UserEdit, "Customize+", "Open Customize+", "/customize");
        CommandTool(FontAwesomeIcon.ShoePrints, "Heels", "Open Heels", "/heels");
        CommandTool(FontAwesomeIcon.Smile, "Moodles", "Open Moodles", "/moodles");
        CommandTool(FontAwesomeIcon.Signature, "Honorific", "Open Honorific", "/honorific");
        CommandTool(FontAwesomeIcon.Paw, "Pet Nicknames", "Open Pet Nicknames", "/petname");

        ImGui.Columns(1);
    }


}
