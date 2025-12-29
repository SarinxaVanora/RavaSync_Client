using RavaSync.API.Data;
using MessagePack;

namespace RavaSync.API.Dto.User;

[MessagePackObject(keyAsPropertyName: true)]
public record OnlineUserIdentDto(UserData User, string Ident) : UserDto(User);