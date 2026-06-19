using MessagePack;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace RavaSync.Services.RavaCast;

public enum RavaCastOp
{
    Advertise = 0,
    Join = 1,
    Leave = 2,
    StateSnapshot = 3,
    RequestState = 4,
    ScreenClosed = 5,
    ConsentCookies = 6,
    DirectStreamStart = 7,
    DirectStreamStop = 8,
    DirectStreamViewerReady = 9,
    DirectStreamViewerLeft = 10,
    DirectStreamOffer = 11,
    DirectStreamAnswer = 12,
    DirectStreamIce = 13,
    DirectStreamStats = 14,
    DirectStreamError = 15,
    DirectStreamSignalChunk = 16
}

public enum RavaCastMode
{
    UrlShare = 0,
    DirectStream = 1
}

public enum RavaCastDirectStreamQuality
{
    Low720p30 = 0,
    Normal720p30 = 1,
    Smooth720p60 = 2
}

public sealed record RavaCastDirectStreamPreset(string Label, int Width, int Height, int Fps, int VideoBitrateKbps, int AudioBitrateKbps)
{
    public double UploadMbps => (VideoBitrateKbps + AudioBitrateKbps) / 1000.0;
}

public static class RavaCastDirectStreamPresets
{
    public static RavaCastDirectStreamPreset Get(RavaCastDirectStreamQuality quality) => quality switch
    {
        RavaCastDirectStreamQuality.Low720p30 => new RavaCastDirectStreamPreset("Low 720p30", 1280, 720, 30, 2500, 128),
        RavaCastDirectStreamQuality.Smooth720p60 => new RavaCastDirectStreamPreset("Smooth 720p60", 1280, 720, 60, 5000, 160),
        _ => new RavaCastDirectStreamPreset("Normal 720p30", 1280, 720, 30, 3500, 128)
    };
}

public sealed record RavaCastDirectStreamBackendStatus(bool PublisherActive, bool ReceiverActive, bool NativeMediaAvailable, string StatusText, string? Detail, int ConnectedPeerCount)
{
    public static RavaCastDirectStreamBackendStatus Idle(bool nativeMediaAvailable) => new(false, false, nativeMediaAvailable, nativeMediaAvailable ? "Direct Stream v2 bridge ready" : "Direct Stream v2 media layer missing", nativeMediaAvailable ? "Direct Stream v2 bridge is present. Transport uses libdatachannel with FFmpeg H.264 live video and Opus audio-over-datachannel." : "RavaCast.Media.Native.dll / RavaCast.Media.BridgeHost.exe are not bundled yet; Direct Stream cannot start until the bridge files are present.", 0);
}

public sealed class RavaCastDirectStreamSignalProducedEventArgs : EventArgs
{
    public RavaCastDirectStreamSignalProducedEventArgs(string peerId, string signalType, string payloadJson)
    {
        PeerId = peerId ?? string.Empty;
        SignalType = signalType ?? string.Empty;
        PayloadJson = payloadJson ?? string.Empty;
    }

    public string PeerId { get; }
    public string SignalType { get; }
    public string PayloadJson { get; }
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastEnvelope(Guid CastId, RavaCastOp Op, byte[]? Payload = null);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastPlanePayload(
    ushort TerritoryId,
    string ScreenName,
    float TopLeftX,
    float TopLeftY,
    float TopLeftZ,
    float TopRightX,
    float TopRightY,
    float TopRightZ,
    float BottomRightX,
    float BottomRightY,
    float BottomRightZ,
    float BottomLeftX,
    float BottomLeftY,
    float BottomLeftZ)
{
    public static RavaCastPlanePayload FromPlane(RavaCastPlane plane) => new(
        plane.TerritoryId,
        plane.ScreenName ?? string.Empty,
        plane.TopLeft.X, plane.TopLeft.Y, plane.TopLeft.Z,
        plane.TopRight.X, plane.TopRight.Y, plane.TopRight.Z,
        plane.BottomRight.X, plane.BottomRight.Y, plane.BottomRight.Z,
        plane.BottomLeft.X, plane.BottomLeft.Y, plane.BottomLeft.Z);

    public RavaCastPlane ToPlane() => new(
        TerritoryId,
        ScreenName ?? string.Empty,
        new Vector3(TopLeftX, TopLeftY, TopLeftZ),
        new Vector3(TopRightX, TopRightY, TopRightZ),
        new Vector3(BottomRightX, BottomRightY, BottomRightZ),
        new Vector3(BottomLeftX, BottomLeftY, BottomLeftZ));
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastStatePayload(
    string HostSessionId,
    string HostName,
    string CastName,
    string Url,
    string SourceDomain,
    string MediaTitle,
    bool IsPlaying,
    double PositionSeconds,
    double? DurationSeconds,
    long StateUnixMs,
    int JoinedCount,
    RavaCastPlanePayload Plane,
    string[] Queue,
    RavaCastCookiePayload[] ConsentCookies)
{
    public RavaCastMode Mode { get; init; } = RavaCastMode.UrlShare;
    public bool PasswordProtected { get; init; }
    public string PasswordSalt { get; init; } = string.Empty;
    public RavaCastDirectStreamQuality DirectStreamQuality { get; init; } = RavaCastDirectStreamQuality.Normal720p30;
    public string DirectStreamStatus { get; init; } = string.Empty;
    public string DirectStreamDetail { get; init; } = string.Empty;
    public bool DirectStreamNativeMediaAvailable { get; init; }
    public long NavigationRevision { get; init; }
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamStartPayload(string HostSessionId, RavaCastDirectStreamQuality Quality);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamStopPayload(string Reason);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamViewerPayload(string ViewerSessionId, string ViewerName);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamSignalPayload(string FromSessionId, string ToSessionId, string Type, string PayloadJson);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamSignalChunkPayload(string SignalId, string FromSessionId, string ToSessionId, string Type, int ChunkIndex, int ChunkCount, string PayloadPart);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamStatsPayload(string SessionId, string StatsJson);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastDirectStreamErrorPayload(string SessionId, string Message);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastCookiePayload(
    string Name,
    string Value,
    string Domain,
    string Path,
    long? ExpiresUnixMs,
    bool Secure,
    string SameSite);

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastJoinPayload(string ViewerSessionId, string ViewerName, bool JoinMuted, string? PasswordHash = null);

public sealed record RavaCastMediaSnapshot(string Url, string Title, bool IsPlaying, double PositionSeconds, double? DurationSeconds, DateTime StateUtc)
{
    public static RavaCastMediaSnapshot Empty(string url = "") => new(url ?? string.Empty, string.Empty, true, 0, null, DateTime.UtcNow);
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record RavaCastLeavePayload(string ViewerSessionId);

public sealed record RavaCastPlane(ushort TerritoryId, string ScreenName, Vector3 TopLeft, Vector3 TopRight, Vector3 BottomRight, Vector3 BottomLeft);

public sealed record RavaCastSummary(
    Guid CastId,
    string HostSessionId,
    string HostName,
    string CastName,
    string Url,
    string SourceDomain,
    string MediaTitle,
    bool IsPlaying,
    double PositionSeconds,
    double? DurationSeconds,
    DateTime StateUtc,
    int JoinedCount,
    RavaCastPlane Plane,
    IReadOnlyList<string> Queue,
    IReadOnlyList<RavaCastCookiePayload> ConsentCookies,
    long LastSeenTick)
{
    public RavaCastMode Mode { get; init; } = RavaCastMode.UrlShare;
    public bool PasswordProtected { get; init; }
    public string PasswordSalt { get; init; } = string.Empty;
    public RavaCastDirectStreamQuality DirectStreamQuality { get; init; } = RavaCastDirectStreamQuality.Normal720p30;
    public string DirectStreamStatus { get; init; } = string.Empty;
    public string DirectStreamDetail { get; init; } = string.Empty;
    public bool DirectStreamNativeMediaAvailable { get; init; }
    public long NavigationRevision { get; init; }
}


public sealed record RavaCastSessionView(
    Guid CastId,
    string HostSessionId,
    string HostName,
    string CastName,
    string Url,
    string SourceDomain,
    string MediaTitle,
    bool IsPlaying,
    double PositionSeconds,
    double? DurationSeconds,
    DateTime StateUtc,
    int JoinedCount,
    RavaCastPlane Plane,
    IReadOnlyList<string> Queue,
    IReadOnlyList<RavaCastCookiePayload> ConsentCookies,
    bool IsOwner,
    bool IsMuted,
    float Volume)
{
    public RavaCastMode Mode { get; init; } = RavaCastMode.UrlShare;
    public bool PasswordProtected { get; init; }
    public string PasswordSalt { get; init; } = string.Empty;
    public RavaCastDirectStreamQuality DirectStreamQuality { get; init; } = RavaCastDirectStreamQuality.Normal720p30;
    public string DirectStreamStatus { get; init; } = string.Empty;
    public string DirectStreamDetail { get; init; } = string.Empty;
    public bool DirectStreamNativeMediaAvailable { get; init; }
    public int DirectStreamConnectedPeers { get; init; }
}


public sealed record RavaCastRenderState(
    Guid CastId,
    string CastName,
    string SourceDomain,
    string MediaTitle,
    string Url,
    bool IsPlaying,
    double PositionSeconds,
    double? DurationSeconds,
    RavaCastPlane Plane,
    bool IsOwner);
