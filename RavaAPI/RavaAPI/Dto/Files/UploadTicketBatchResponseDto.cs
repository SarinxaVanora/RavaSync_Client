using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketBatchResponseDto
{
    public List<UploadTicketBatchFileResponseDto> Files { get; init; } = new();
}
