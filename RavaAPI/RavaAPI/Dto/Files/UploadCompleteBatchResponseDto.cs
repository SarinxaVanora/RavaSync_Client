using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadCompleteBatchResponseDto
{
    public List<UploadCompleteBatchFileResultDto> Files { get; init; } = new();
}
