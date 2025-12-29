using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using MessagePack;

namespace RavaSync.API.Dto.Group;

[MessagePackObject(keyAsPropertyName: true)]
public record GroupPairUserPermissionDto(GroupData Group, UserData User, GroupUserPreferredPermissions GroupPairPermissions) : GroupPairDto(Group, User);