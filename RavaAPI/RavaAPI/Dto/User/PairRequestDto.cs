using RavaSync.API.Data;
using MessagePack;

namespace RavaSync.API.Dto.User;

[MessagePackObject(keyAsPropertyName: true)]
public record PairRequestDto(
    UserData Requester,
    string RequesterIdent,
    string RequesterInGameName
);
