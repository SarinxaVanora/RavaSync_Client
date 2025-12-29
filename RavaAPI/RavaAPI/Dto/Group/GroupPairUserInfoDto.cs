using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using MessagePack;

namespace RavaSync.API.Dto.Group;

[MessagePackObject(keyAsPropertyName: true)]
public record GroupPairUserInfoDto(GroupData Group, UserData User, GroupPairUserInfo GroupUserInfo) : GroupPairDto(Group, User);