using RavaSync.API.Dto.User;
using RavaSync.API.SignalR;
using RavaSync.Services.Mediator;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;

namespace RavaSync.WebAPI
{
    public partial class ApiController
    {
        public Task UserSendPairRequest(string targetIdent, string requesterInGameName)
        {
            CheckConnection();
            return _mareHub!.SendAsync(nameof(UserSendPairRequest), targetIdent, requesterInGameName);
        }

        public Task UserRespondPairRequest(string requesterUid, bool accepted)
        {
            CheckConnection();
            return _mareHub!.SendAsync(nameof(UserRespondPairRequest), requesterUid, accepted);
        }

        public Task Client_UserReceivePairRequest(PairRequestDto dto)
        {
            Logger.LogTrace("Client_UserReceivePairRequest: {@dto}", dto);

            ExecuteSafely(() =>
            {
                Mediator.Publish(new PairRequestReceivedMessage(dto));
            });

            return Task.CompletedTask;
        }

        public Task Client_UserPairRequestResult(PairRequestResultDto dto)
        {
            Logger.LogTrace("Client_UserPairRequestResult: {@dto}", dto);

            ExecuteSafely(() =>
            {
                Mediator.Publish(new PairRequestResultMessage(dto));
            });

            return Task.CompletedTask;
        }

        public void OnUserReceivePairRequest(Action<PairRequestDto> act)
        {
            if (_initialized) return;
            _mareHub!.On(nameof(Client_UserReceivePairRequest), act);
        }

        public void OnUserPairRequestResult(Action<PairRequestResultDto> act)
        {
            if (_initialized) return;
            _mareHub!.On(nameof(Client_UserPairRequestResult), act);
        }

    }
}
