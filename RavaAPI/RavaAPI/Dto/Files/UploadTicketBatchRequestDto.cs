using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketBatchRequestDto
{
    public List<UploadTicketBatchFileRequestDto> Files { get; init; } = new();
    public bool Force { get; init; }
}
