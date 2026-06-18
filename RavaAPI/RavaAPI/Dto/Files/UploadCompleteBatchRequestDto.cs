using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadCompleteBatchRequestDto
{
    public List<UploadCompleteBatchFileDto> Files { get; init; } = new();
}
