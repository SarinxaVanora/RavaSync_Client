using MessagePack;

namespace RavaSync.API.Dto.Venue;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record class VenueInfo
{
    public string ShellGid { get; init; } = string.Empty;
    public string DisplayName { get; init; } = string.Empty;
    public string CanonicalKey { get; init; } = string.Empty;
    public bool AutoInviteEnabled { get; init; }
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record class RegisterVenueRequest
{
    public string CanonicalKey { get; init; } = string.Empty;
    public string DisplayName { get; init; } = string.Empty;
}

[MessagePackObject(keyAsPropertyName: true)]
public sealed record class JoinByVenueRequest
{
    public string CanonicalKey { get; init; } = string.Empty;
}
