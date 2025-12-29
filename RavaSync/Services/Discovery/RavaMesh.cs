using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto;
using RavaSync.API.Dto.User;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RavaSync.Services.Discovery;

public interface IRavaMeshMessage { }

public sealed record RavaHello(string FromSessionId, byte[] FromPeerKey) : IRavaMeshMessage;
public sealed record RavaHelloAck(string FromSessionId, byte[] FromPeerKey) : IRavaMeshMessage;
public sealed record RavaPairRequest(PairRequestDto Request) : IRavaMeshMessage;
public sealed record RavaGoodbye(string FromSessionId, byte[] FromPeerKey) : IRavaMeshMessage;
public sealed record RavaGame(string FromSessionId, byte[] Payload) : IRavaMeshMessage;

public interface IRavaMesh
{
    void Listen(string sessionId, Func<string, IRavaMeshMessage, Task> handler);
    void Unlisten(string sessionId);
    Task SendAsync(string sessionId, IRavaMeshMessage message);
}

public sealed class RavaMesh : IRavaMesh
{
    private readonly ILogger<RavaMesh> _logger;
    private readonly ApiController _api;
    private readonly MareMediator _mediator;

    private readonly ConcurrentDictionary<string, Func<string, IRavaMeshMessage, Task>> _handlers
        = new(StringComparer.Ordinal);

    private string? _currentSessionId;

    private long _hookedHubEpoch = -1;

    public RavaMesh(ILogger<RavaMesh> logger, ApiController api, MareMediator mediator)
    {
        _logger = logger;
        _api = api;
        _mediator = mediator;
    }

    public void Listen(string sessionId, Func<string, IRavaMeshMessage, Task> handler)
    {
        if (string.IsNullOrEmpty(sessionId)) return;

        _handlers[sessionId] = handler;
        _currentSessionId = sessionId;

        EnsureHubMeshHooked();

        _ = _api.MeshRegister(sessionId);
    }

    public void Unlisten(string sessionId)
    {
        if (string.IsNullOrEmpty(sessionId)) return;

        _handlers.TryRemove(sessionId, out _);

        if (string.Equals(_currentSessionId, sessionId, StringComparison.Ordinal))
            _currentSessionId = null;
    }

    public Task SendAsync(string sessionId, IRavaMeshMessage message)
    {
        if (string.IsNullOrEmpty(sessionId)) return Task.CompletedTask;

        var fromSession = _currentSessionId ?? string.Empty;

        MeshMessageDto dto = message switch
        {
            RavaHello hello => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: hello.FromSessionId,
                Type: MeshMessageType.Hello,
                PeerKey: hello.FromPeerKey,
                Payload: null
            ),

            RavaHelloAck ack => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: ack.FromSessionId,
                Type: MeshMessageType.HelloAck,
                PeerKey: ack.FromPeerKey,
                Payload: null
            ),

            RavaPairRequest pr => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: fromSession,
                Type: MeshMessageType.PairRequest,
                PeerKey: Array.Empty<byte>(),
                Payload: MessagePackSerializer.Serialize(pr.Request)
            ),

            RavaGoodbye bye => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: bye.FromSessionId,
                Type: MeshMessageType.Goodbye,
                PeerKey: Array.Empty<byte>(),
                Payload: null
            ),

            RavaGame game => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: fromSession,
                Type: MeshMessageType.Game,
                PeerKey: Array.Empty<byte>(),
                Payload: game.Payload
            ),

            _ => throw new ArgumentOutOfRangeException(nameof(message), $"Unknown mesh message type: {message.GetType().Name}")
        };

        return _api.MeshSend(dto);
    }

    private void EnsureHubMeshHooked()
    {
        var epoch = _api.HubEpoch;
        if (_hookedHubEpoch == epoch) return;

        _hookedHubEpoch = epoch;

        try
        {
            _api.OnMeshMessage(OnMeshMessageFromHub);

            if (!string.IsNullOrEmpty(_currentSessionId))
                _ = _api.MeshRegister(_currentSessionId);

            _logger.LogDebug("RavaMesh re-hooked to hub epoch {epoch}", epoch);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to hook mesh message handler to hub");
        }
    }

    private void OnMeshMessageFromHub(MeshMessageDto dto)
    {
        try
        {
            if (!_handlers.TryGetValue(dto.TargetSessionId, out var handler))
            {
                return;
            }

            IRavaMeshMessage? msg = dto.Type switch
            {
                MeshMessageType.Hello =>
                    new RavaHello(dto.FromSessionId, dto.PeerKey),

                MeshMessageType.HelloAck =>
                    new RavaHelloAck(dto.FromSessionId, dto.PeerKey),

                MeshMessageType.PairRequest when dto.Payload is not null =>
                    new RavaPairRequest(MessagePackSerializer.Deserialize<PairRequestDto>(dto.Payload)),

                MeshMessageType.Goodbye =>
                    new RavaGoodbye(dto.FromSessionId, dto.PeerKey),

                MeshMessageType.Game when dto.Payload is not null =>
                    new RavaGame(dto.FromSessionId, dto.Payload),

                _ => null
            };

            if (msg is null) return;

            _ = handler(dto.TargetSessionId, msg);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling mesh message from hub");
        }
    }
}
