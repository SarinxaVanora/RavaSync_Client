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
            var hub = _mareHub;
            if (hub == null || hub.State != HubConnectionState.Connected)
                return Task.CompletedTask;

            return hub.SendAsync(nameof(MeshRegister), sessionId);
        }

        public Task MeshSend(MeshMessageDto message)
        {
            var hub = _mareHub;
            if (hub == null || hub.State != HubConnectionState.Connected)
                return Task.CompletedTask;

            return hub.SendAsync(nameof(MeshSend), message);
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
