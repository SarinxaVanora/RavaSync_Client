using MessagePack;

namespace RavaSync.API.Dto;

/// <summary>
/// Opaque mesh message used by RavaSync: Discovery.
/// Carries a tiny payload from one sessionId to another.
/// Server only sees these opaque fields; it never knows who they belong to.
/// </summary>
[MessagePackObject(keyAsPropertyName: true)]
public record MeshMessageDto(
    string TargetSessionId,
    string FromSessionId,
    MeshMessageType Type, byte[] PeerKey,
    byte[]? Payload = null
);

public enum MeshMessageType
{
    Hello = 0,
    HelloAck = 1,
    PairRequest = 2,
    Goodbye = 3,
    Game = 10
}