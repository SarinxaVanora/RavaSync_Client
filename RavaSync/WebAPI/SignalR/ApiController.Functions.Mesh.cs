using RavaSync.API.Dto;
using RavaSync.API.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;

namespace RavaSync.WebAPI
{
    public sealed partial class ApiController
    {
        public Task MeshRegister(string sessionId)
        {
            CheckConnection();
            return _mareHub!.SendAsync(nameof(MeshRegister), sessionId);
        }

        public Task MeshSend(MeshMessageDto message)
        {
            CheckConnection();
            return _mareHub!.SendAsync(nameof(MeshSend), message);
        }

        public Task Client_MeshMessage(MeshMessageDto dto)
        {
            Logger.LogTrace("Client_MeshMessage: {@dto}", dto);
            return Task.CompletedTask;
        }

        public void OnMeshMessage(Action<MeshMessageDto> act)
        {
            if (_mareHub == null) return;
            _mareHub.On(nameof(Client_MeshMessage), act);
        }
    }
}
