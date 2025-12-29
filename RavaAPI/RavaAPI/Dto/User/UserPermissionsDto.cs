using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using MessagePack;

namespace RavaSync.API.Dto.User;

[MessagePackObject(keyAsPropertyName: true)]
public record UserPermissionsDto(UserData User, UserPermissions Permissions) : UserDto(User);
