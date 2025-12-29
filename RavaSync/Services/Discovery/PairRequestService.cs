using Lumina.Excel.Sheets;
using RavaSync.API.Data;
using RavaSync.API.Dto.User;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR;
using RavaSync.WebAPI.SignalR.Utils;
using Microsoft.Extensions.Logging;
using System;

namespace RavaSync.Services.Discovery;

public class PairRequestService : DisposableMediatorSubscriberBase
{
    private readonly ApiController _api;
    private readonly DalamudUtilService _util;
    private readonly IRavaMesh _mesh;
    private static string _charName = "";
    public PairRequestService(
        ILogger<PairRequestService> logger,
        MareMediator mediator,
        ApiController api,
        DalamudUtilService util,
        IRavaMesh mesh)
        : base(logger, mediator)
    {
        _api = api;
        _util = util;
        _mesh = mesh;

        Mediator.Subscribe<ContextMenuPairRequestMessage>(this, OnContextMenuPairRequest);
        Mediator.Subscribe<PairRequestResultMessage>(this, OnPairRequestResult);
        Mediator.Subscribe<DirectPairRequestMessage>(this, OnDirectPairRequest);
    }

    private async void OnContextMenuPairRequest(ContextMenuPairRequestMessage msg)
    {
        try
        {
            // Must be connected
            if (_api.ServerState != ServerState.Connected)
            {
                Mediator.Publish(new NotificationMessage(
                    "Pair request",
                    "You must be connected to RavaSync to send a pair request.",
                    NotificationType.Error));
                return;
            }


            var targetIdent = msg.TargetIdent;

            if (string.IsNullOrEmpty(targetIdent))
            {
                Mediator.Publish(new NotificationMessage(
                    "Pair request",
                    "Could not resolve this player.",
                    NotificationType.Error));
                return;
            }

            // Sender's display name
            var myName = await _util.GetPlayerNameAsync().ConfigureAwait(false);

            var requester = new UserData(_api.UID, _api.DisplayName);
            var dto = new PairRequestDto(requester, string.Empty, myName);

            var targetSessionId = RavaSessionId.FromIdent(targetIdent);

            // Send via mesh
            await _mesh.SendAsync(targetSessionId, new RavaPairRequest(dto)).ConfigureAwait(false);

            // stash name for result toast
            _charName = msg.charName;


            Mediator.Publish(new NotificationMessage(
                "Pair request",
                "Pair request sent.",
                NotificationType.Info));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to send pair request");
            Mediator.Publish(new NotificationMessage(
                "Pair request",
                "Unexpected error while sending pair request.",
                NotificationType.Error));
        }
    }

    private async void OnPairRequestResult(PairRequestResultMessage msg)
    {
        var dto = msg.Result;

        if (dto.Accepted)
        {
            try
            {
                await _api.UserAddPair(new UserDto(dto.Target)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to auto-add pair after accepted pair request");
            }
        }

        var status = dto.Accepted ? "accepted" : "declined";
        var targetName = _charName;

        Mediator.Publish(new NotificationMessage(
            "Pair request result",
            $"{targetName} has {status} your pair request.",
            dto.Accepted ? NotificationType.Info : NotificationType.Warning));
    }

    private async void OnDirectPairRequest(DirectPairRequestMessage msg)
    {
        try
        {
            // Must be connected
            if (_api.ServerState != ServerState.Connected)
            {
                Mediator.Publish(new NotificationMessage(
                    "Pair request",
                    "You must be connected to RavaSync to send a pair request.",
                    NotificationType.Error));
                return;
            }

            var targetIdent = msg.TargetIdent;

            // Sender's display name
            var myName = await _util.GetPlayerNameAsync().ConfigureAwait(false);

            await _api.UserSendPairRequest(targetIdent, myName).ConfigureAwait(false);

            // Stash the nice display name for the result toast
            _charName = msg.TargetName;

            Mediator.Publish(new NotificationMessage(
                "Pair request",
                "Pair request sent.",
                NotificationType.Info));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to send pair request (direct)");
            Mediator.Publish(new NotificationMessage(
                "Pair request",
                "Unexpected error while sending pair request.",
                NotificationType.Error));
        }
    }


}
