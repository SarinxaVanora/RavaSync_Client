using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System.Numerics;

namespace RavaSync.UI;

public class CreateSyncshellUI : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly UiSharedService _uiSharedService;
    private bool _errorGroupCreate;
    private GroupJoinDto? _lastCreatedGroup;
    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public CreateSyncshellUI(ILogger<CreateSyncshellUI> logger, MareMediator mareMediator, ApiController apiController, UiSharedService uiSharedService,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mareMediator, "Create new Syncshell###RavaSyncCreateSyncshell", performanceCollectorService)
    {
        _apiController = apiController;
        _uiSharedService = uiSharedService;
        SizeConstraints = new()
        {
            MinimumSize = new(550, 330),
            MaximumSize = new(550, 330)
        };

        Flags = ImGuiWindowFlags.NoResize | ImGuiWindowFlags.NoCollapse;

        Mediator.Subscribe<DisconnectedMessage>(this, (_) => IsOpen = false);
    }

    protected override void DrawInternal()
    {
        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.c0509b76", "Create new Syncshell"));

        if (_lastCreatedGroup == null)
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Plus, _uiSharedService.L("UI.CreateSyncshellUI.b551dae1", "Create Syncshell")))
            {
                try
                {
                    _lastCreatedGroup = _apiController.GroupCreate().Result;
                }
                catch
                {
                    _lastCreatedGroup = null;
                    _errorGroupCreate = true;
                }
            }
            ImGui.SameLine();
        }

        ImGui.Separator();

        if (_lastCreatedGroup == null)
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.ac33783a", "Creating a new Syncshell will create it with your current preferred permissions for Syncshells as default suggested permissions.") + Environment.NewLine +
                "- You can own up to " + _apiController.ServerInfo.MaxGroupsCreatedByUser + " Syncshells on this server." + Environment.NewLine +
                "- You can join up to " + _apiController.ServerInfo.MaxGroupsJoinedByUser + " Syncshells on this server (including your own)" + Environment.NewLine +
                "- Syncshells on this server can have a maximum of " + _apiController.ServerInfo.MaxGroupUserCount + " users");
            ImGuiHelpers.ScaledDummy(2f);
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.1c5da19f", "Your current Syncshell preferred permissions are:"));
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.269f0654", "- Animations"));
            _uiSharedService.BooleanToColoredIcon(!_apiController.DefaultPermissions!.DisableGroupAnimations);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.cc79adf2", "- Sounds"));
            _uiSharedService.BooleanToColoredIcon(!_apiController.DefaultPermissions!.DisableGroupSounds);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.b3942667", "- VFX"));
            _uiSharedService.BooleanToColoredIcon(!_apiController.DefaultPermissions!.DisableGroupVFX);
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.31b74482", "(Those preferred permissions can be changed anytime after Syncshell creation, your defaults can be changed anytime in the RavaSync Settings)"));
        }
        else
        {
            _errorGroupCreate = false;
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.618e7416", "Syncshell ID: ") + _lastCreatedGroup.Group.GID);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted(_uiSharedService.L("UI.CreateSyncshellUI.189d5c27", "Syncshell Password: ") + _lastCreatedGroup.Password);
            ImGui.SameLine();
            if (_uiSharedService.IconButton(FontAwesomeIcon.Copy))
            {
                ImGui.SetClipboardText(_lastCreatedGroup.Password);
            }
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.15233466", "You can change the Syncshell password later at any time."));
            ImGui.Separator();
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.06438ab1", "These settings were set based on your preferred syncshell permissions:"));
            ImGuiHelpers.ScaledDummy(2f);
            ImGui.AlignTextToFramePadding();
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.b24bf3a7", "Suggest Animation sync:"));
            _uiSharedService.BooleanToColoredIcon(!_lastCreatedGroup.GroupUserPreferredPermissions.IsDisableAnimations());
            ImGui.AlignTextToFramePadding();
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.8a15dcf6", "Suggest Sounds sync:"));
            _uiSharedService.BooleanToColoredIcon(!_lastCreatedGroup.GroupUserPreferredPermissions.IsDisableSounds());
            ImGui.AlignTextToFramePadding();
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.31c3c3a5", "Suggest VFX sync:"));
            _uiSharedService.BooleanToColoredIcon(!_lastCreatedGroup.GroupUserPreferredPermissions.IsDisableVFX());
        }

        if (_errorGroupCreate)
        {
            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.CreateSyncshellUI.72024cbf", "Something went wrong during creation of a new Syncshell"), new Vector4(1, 0, 0, 1));
        }
    }

    public override void OnOpen()
    {
        _lastCreatedGroup = null;
    }
}