namespace RavaSync.Services.Mediator;

public sealed record OpenVenueJoinUiMessage : MessageBase
{
    public string CanonicalKey { get; }
    public string VenueName { get; }
    public string ShellGid { get; }

    public OpenVenueJoinUiMessage(string canonicalKey, string venueName, string shellGid)
    {
        CanonicalKey = canonicalKey;
        VenueName = venueName;
        ShellGid = shellGid;
    }
}

