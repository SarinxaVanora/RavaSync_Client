using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.Venue;
using Microsoft.AspNetCore.SignalR.Client;

namespace RavaSync.WebAPI;

public partial class ApiController
{
    public async Task<VenueInfo?> VenueLookup(string canonicalKey)
    {
        CheckConnection();
        return await _mareHub!.InvokeAsync<VenueInfo?>("VenueLookup", canonicalKey).ConfigureAwait(false);
    }

    public async Task<VenueInfo?> VenueRegister(string shellGid, RegisterVenueRequest req)
    {
        CheckConnection();
        return await _mareHub!.InvokeAsync<VenueInfo?>("VenueRegister", shellGid, req).ConfigureAwait(false);
    }

    public async Task<GroupJoinInfoDto> VenueJoin(GroupPasswordDto passwordedGroup)
    {
        CheckConnection();
        return await _mareHub!.InvokeAsync<GroupJoinInfoDto>(nameof(VenueJoin), passwordedGroup).ConfigureAwait(false);
    }

    public async Task<bool> VenueJoinFinalize(GroupJoinDto passwordedGroup)
    {
        CheckConnection();
        return await _mareHub!.InvokeAsync<bool>(nameof(VenueJoinFinalize), passwordedGroup).ConfigureAwait(false);
    }

}
