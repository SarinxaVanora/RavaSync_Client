using RavaSync.API.Data;
using MessagePack;

namespace RavaSync.API.Dto.User;

[MessagePackObject(keyAsPropertyName: true)]
public record PairRequestResultDto(
    UserData Requester,
    UserData Target,
    bool Accepted
);
