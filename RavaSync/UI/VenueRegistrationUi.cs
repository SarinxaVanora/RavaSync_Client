using System;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Plugin.Services;
using RavaSync.API.Data.Extensions;           
using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.Venue;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;

namespace RavaSync.UI;

public sealed class VenueRegistrationUi : WindowMediatorSubscriberBase
{
    private readonly MareMediator _mediator;
    private readonly IGameGui _gameGui;
    private readonly DalamudUtilService _util;
    private readonly ApiController _api;
    private readonly MareConfigService _cfg;
    private readonly UiSharedService _uiSharedService;

    private string _canonicalKey = string.Empty;
    private string _venueName = string.Empty;
    private GroupFullInfoDto[] _ownedShells = Array.Empty<GroupFullInfoDto>();
    private int _selectedShellIndex = 0;
    private string _status = string.Empty;
    private bool _neverAskAgain;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public VenueRegistrationUi(
        ILogger<VenueRegistrationUi> logger,
        MareMediator mediator,
        IGameGui gameGui,
        DalamudUtilService util,
        ApiController api,
        MareConfigService cfg,
        UiSharedService uiSharedService,
        PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — Venue Registration", performanceCollectorService)
    {
        _mediator = mediator;
        _gameGui = gameGui;
        _util = util;
        _api = api;
        _cfg = cfg;
        _uiSharedService = uiSharedService;

        Flags = ImGuiWindowFlags.NoCollapse | ImGuiWindowFlags.AlwaysAutoResize;
        Size = new Vector2(520, 240);
        SizeCondition = ImGuiCond.Appearing;

        _mediator.Subscribe<UiToggleMessage>(this, OnUiToggle);
    }

    private void OnUiToggle(UiToggleMessage msg)
    {
        if (msg.UiType != typeof(VenueRegistrationUi)) return;

        // IMPORTANT: The venue probe touches game memory (map/agents) and MUST run on the Framework thread
        _ = ToggleOnFrameworkAsync();
    }

    // Runs the registerable-venue probe on the Framework thread to satisfy EnsureIsOnFramework()
    private async Task ToggleOnFrameworkAsync()
    {
        try
        {
            var probe = await _util.RunOnFrameworkThread(() =>
            {
                var ok = _util.TryGetRegisterableVenue(_gameGui, out var addr, out var deny);
                return (ok, addr, deny);
            }).ConfigureAwait(false);

            if (!probe.ok)
            {
                _mediator.Publish(new NotificationMessage("Cannot register here", probe.deny, NotificationType.Error));
                IsOpen = false;
                return;
            }

            if (_cfg.Current.VenueAskSuppressKeys.Contains(probe.addr.CanonicalKey))
            {
                _mediator.Publish(new NotificationMessage("Suppressed", "This location is set to never ask again.", NotificationType.Warning));
                IsOpen = false;
                return;
            }

            _canonicalKey = probe.addr.CanonicalKey;
            _venueName = string.Empty;
            _status = "Loading...";
            _neverAskAgain = false;
            IsOpen = true;

            _ = LoadStateAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed during venue probe / toggle");
            _mediator.Publish(new NotificationMessage("Error", "Failed to open venue registration.", NotificationType.Error));
            IsOpen = false;
        }
    }

    private async Task LoadStateAsync()
    {
        try
        {
            var existing = await _api.VenueLookup(_canonicalKey).ConfigureAwait(false);
            if (existing != null)
            {
                _mediator.Publish(new NotificationMessage("Already linked", $"Already linked to: {existing.DisplayName}", NotificationType.Warning));
                IsOpen = false;
                return;
            }

            var groups = await _api.GroupsGetAll().ConfigureAwait(false);
            var myUid = _api.UID;

            // Owners OR moderators of shells get to link this venue.
            _ownedShells = groups
                .Where(g =>
                    g.Owner != null &&
                    (
                        // owner (case-insensitive UID compare)
                        string.Equals(g.Owner.UID, myUid, StringComparison.OrdinalIgnoreCase)
                        // or moderator via GroupUserInfo flag
                        || g.GroupUserInfo.IsModerator()
                    ))
                .ToArray();

            _status = _ownedShells.Length == 0
                ? "You don't manage any shells (owner/moderator)."
                : "Ready.";
            _selectedShellIndex = 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load venue registration state");
            _status = "Error loading state.";
        }
    }

    protected override void DrawInternal()
    {
        ImGui.TextWrapped("Link this plot to one of your Syncshells.");
        ImGui.Spacing();

        ImGui.TextDisabled($"Location Key: {_canonicalKey}");
        ImGui.Spacing();

        using (ImRaii.Disabled(_ownedShells.Length == 0))
        {
            ImGui.Text("Shell:");
            ImGui.SameLine(140);

            if (_ownedShells.Length == 0)
            {
                ImGui.TextDisabled("No eligible shells");
            }
            else
            {
                var currentLabel = _ownedShells[Math.Clamp(_selectedShellIndex, 0, _ownedShells.Length - 1)].Group.AliasOrGID;
                if (ImGui.BeginCombo("##shells", currentLabel))
                {
                    for (int i = 0; i < _ownedShells.Length; i++)
                    {
                        bool sel = i == _selectedShellIndex;
                        if (ImGui.Selectable(_ownedShells[i].Group.AliasOrGID, sel))
                            _selectedShellIndex = i;
                        if (sel) ImGui.SetItemDefaultFocus();
                    }
                    ImGui.EndCombo();
                }
            }

            ImGui.Text("Venue Name:");
            ImGui.SameLine(140);
            ImGui.SetNextItemWidth(300);
            ImGui.InputText("##venuename", ref _venueName, 128);
        }

        ImGui.Spacing();
        ImGui.Checkbox("Never ask again for this location", ref _neverAskAgain);

        if (!string.IsNullOrEmpty(_status))
        {
            ImGui.Spacing();
            UiSharedService.ColorText(_status, new Vector4(0.9f, 0.9f, 0.9f, 1f));
        }

        ImGui.Spacing();
        using (ImRaii.Group())
        {
            using (ImRaii.Disabled(_ownedShells.Length == 0 || string.IsNullOrWhiteSpace(_venueName)))
            {
                if (ImGui.Button("Register", new Vector2(120, 28)))
                    _ = DoRegisterAsync();
            }

            ImGui.SameLine();

            if (ImGui.Button("Cancel", new Vector2(120, 28)))
            {
                if (_neverAskAgain && !string.IsNullOrEmpty(_canonicalKey))
                {
                    _cfg.Current.VenueAskSuppressKeys.Add(_canonicalKey);
                    _cfg.Save();
                }
                IsOpen = false;
            }
        }
    }

    private async Task DoRegisterAsync()
    {
        try
        {
            var shell = _ownedShells[_selectedShellIndex];

            // property-record initializer (no positional ctor)
            var req = new RegisterVenueRequest
            {
                CanonicalKey = _canonicalKey,
                DisplayName = _venueName
            };

            var res = await _api.VenueRegister(shell.Group.GID, req).ConfigureAwait(false);

            if (res != null)
            {
                _mediator.Publish(new NotificationMessage("Registered",
                    $"Linked \"{_venueName}\" to shell {shell.Group.AliasOrGID}.",
                    NotificationType.Info));

                if (_neverAskAgain && !string.IsNullOrEmpty(_canonicalKey))
                {
                    _cfg.Current.VenueAskSuppressKeys.Add(_canonicalKey);
                    _cfg.Save();
                }
                IsOpen = false;
            }
            else
            {
                _mediator.Publish(new NotificationMessage("Failed",
                    "Could not register this venue right now.",
                    NotificationType.Error));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Venue register failed");
            _mediator.Publish(new NotificationMessage("Failed",
                "Unexpected error during registration.",
                NotificationType.Error));
        }
    }
}
