using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using MessagePack;

namespace RavaSync.API.Dto.User;

[MessagePackObject(keyAsPropertyName: true)]
public record UserIndividualPairStatusDto(UserData User, IndividualPairStatus IndividualPairStatus) : UserDto(User);