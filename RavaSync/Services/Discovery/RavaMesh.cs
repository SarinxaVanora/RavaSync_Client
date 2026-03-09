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

public sealed record RavaYield(string FromSessionId, string FromUid, bool YieldToOtherSync, string Owner) : IRavaMeshMessage;

public sealed record RavaGame(string FromSessionId, byte[] Payload) : IRavaMeshMessage;

public sealed record RavaYieldReceivedMessage(string AffectedUid, bool YieldToOtherSync, string Owner, TimeSpan Ttl);

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

    private static readonly byte[] YieldMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'Y', (byte)'I', (byte)'E', (byte)'L', (byte)'D', 0 };

    [MessagePackObject]
    public sealed class YieldPayload
    {
        [Key(0)] public string FromUid { get; set; } = string.Empty;
        [Key(1)] public bool YieldToOtherSync { get; set; }
        [Key(2)] public string Owner { get; set; } = string.Empty;
    }

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

        _ = SafeMeshRegisterAsync(sessionId);
    }

    public void Unlisten(string sessionId)
    {
        if (string.IsNullOrEmpty(sessionId)) return;

        _handlers.TryRemove(sessionId, out _);

        if (string.Equals(_currentSessionId, sessionId, StringComparison.Ordinal))
            _currentSessionId = null;
    }

    public async Task SendAsync(string sessionId, IRavaMeshMessage message)
    {
        if (string.IsNullOrEmpty(sessionId)) return;

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

            RavaYield y => new MeshMessageDto(
                TargetSessionId: sessionId,
                FromSessionId: fromSession,
                Type: MeshMessageType.Game,
                PeerKey: Array.Empty<byte>(),
                Payload: BuildYieldPayload(y.FromUid, y.YieldToOtherSync, y.Owner)
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

        try
        {
            await _api.MeshSend(dto).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogDebug(ex, "Skipping mesh send to {sessionId}: hub not active", sessionId);
        }
    }


    private async Task SafeMeshRegisterAsync(string? sessionId)
    {
        if (string.IsNullOrEmpty(sessionId)) return;

        try
        {
            await _api.MeshRegister(sessionId).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogDebug(ex, "Skipping mesh register for {sessionId}: hub not active", sessionId);
        }
    }

    private static byte[] BuildYieldPayload(string fromUid, bool yield, string owner)
    {
        var payload = MessagePackSerializer.Serialize(new YieldPayload
        {
            FromUid = fromUid ?? string.Empty,
            YieldToOtherSync = yield,
            Owner = owner ?? string.Empty
        });

        var buf = new byte[YieldMagic.Length + payload.Length];
        Buffer.BlockCopy(YieldMagic, 0, buf, 0, YieldMagic.Length);
        Buffer.BlockCopy(payload, 0, buf, YieldMagic.Length, payload.Length);
        return buf;
    }

    private static bool TryParseYieldPayload(byte[] payload, out YieldPayload? yp)
    {
        yp = null;
        if (payload == null || payload.Length <= YieldMagic.Length) return false;

        for (int i = 0; i < YieldMagic.Length; i++)
            if (payload[i] != YieldMagic[i]) return false;

        try
        {
            yp = MessagePackSerializer.Deserialize<YieldPayload>(payload.AsSpan(YieldMagic.Length).ToArray());
            return yp != null;
        }
        catch
        {
            return false;
        }
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
                _ = SafeMeshRegisterAsync(_currentSessionId);

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
                return;

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

                MeshMessageType.Game when dto.Payload is not null && TryParseYieldPayload(dto.Payload, out var yp) && yp != null =>
                    new RavaYield(dto.FromSessionId, yp.FromUid, yp.YieldToOtherSync, yp.Owner),

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