using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using MessagePack;

namespace RavaSync.API.Dto.Group;

[MessagePackObject(keyAsPropertyName: true)]
public record GroupPermissionDto(GroupData Group, GroupPermissions Permissions) : GroupDto(Group);