using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Dto.User;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System;
using System.Numerics;
using System.Threading.Tasks;

namespace RavaSync.UI;

public sealed class PairRequestUi : WindowMediatorSubscriberBase
{
    private readonly MareMediator _mediator;
    private readonly UiSharedService _uiShared;
    private readonly ApiController _api;
    private readonly MareConfigService _serverConfig;

    // Store the full request DTO
    private PairRequestDto? _currentRequest;

    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public PairRequestUi(
        ILogger<PairRequestUi> logger,
        MareMediator mediator,
        UiSharedService uiSharedService,
        ApiController api,
        PerformanceCollectorService performanceCollectorService,
        MareConfigService serverConfigService)
        : base(logger, mediator, "RavaSync — Pair Request", performanceCollectorService)
    {
        _mediator = mediator;
        _uiShared = uiSharedService;
        _api = api;
        _serverConfig = serverConfigService;
        Flags = ImGuiWindowFlags.NoCollapse | ImGuiWindowFlags.AlwaysAutoResize;
        Size = new Vector2(520, 200);
        SizeCondition = ImGuiCond.Appearing;

        mediator.Subscribe<PairRequestReceivedMessage>(this, OnPairRequestReceived);
    }

    private void OnPairRequestReceived(PairRequestReceivedMessage msg)
    {
        if (_serverConfig.Current.AutoDeclineIncomingPairRequests)
        {
            try
            {
                // Immediately decline
                _= RespondAsync(false);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed auto-declining pair request");
            }
        }

        // Normal flow:
        _currentRequest = msg.Request;
        IsOpen = true;
    }


    protected override void DrawInternal()
    {
        if (_currentRequest is null)
        {
            ImGui.TextWrapped(_uiShared.L("UI.PairRequestUi.ae8d20c2", "No active pair request."));
            return;
        }

        // Prefer in-game name; fall back to UID/AliasOrUID if needed
        var displayName = !string.IsNullOrWhiteSpace(_currentRequest.RequesterInGameName)
            ? _currentRequest.RequesterInGameName
            : _currentRequest.Requester.AliasOrUID;

        var style = ImGui.GetStyle();
        var oldItemSpacing = style.ItemSpacing;
        ImGui.PushStyleVar(ImGuiStyleVar.ItemSpacing, new Vector2(oldItemSpacing.X, 4f));

        ImGui.TextWrapped(string.Format(_uiShared.L("UI.PairRequestUi.a2b4f9cd", "{0} wants to pair with you."), displayName));

        ImGui.Spacing();
        ImGui.Text(_uiShared.L("UI.PairRequestUi.81dd371d", "Do you accept?"));

        ImGui.Spacing();

        using (ImRaii.Group())
        {
            if (ImGui.Button(_uiShared.L("UI.PairRequestUi.5397e058", "Yes"), new Vector2(90, 24)))
            {
                _ = RespondAsync(true);
            }

            ImGui.SameLine();

            if (ImGui.Button(_uiShared.L("UI.PairRequestUi.816c52fd", "No"), new Vector2(90, 24)))
            {
                _ = RespondAsync(false);
            }
        }

        ImGui.PopStyleVar();
    }


    private async Task RespondAsync(bool accept)
    {
        try
        {
            if (_currentRequest != null)
            {
                // ApiController.UserRespondPairRequest(string requesterUid, bool accepted)
                var requesterUid = _currentRequest.Requester.UID;
                await _api.UserRespondPairRequest(requesterUid, accept).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to respond to pair request");
            _mediator.Publish(new NotificationMessage(
                "Pair request",
                "Unexpected error responding to pair request.",
                NotificationType.Error));
        }
        finally
        {
            _currentRequest = null;
            IsOpen = false;
        }
    }
}
