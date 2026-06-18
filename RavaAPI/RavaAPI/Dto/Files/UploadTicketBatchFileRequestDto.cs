using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketBatchFileRequestDto
{
    public string Hash { get; init; } = string.Empty;
    public long RawSize { get; init; }
    public bool ClientWouldMunge { get; init; }
    public int ClientParallelUploads { get; init; }
    public string PayloadEncoding { get; init; } = FilePayloadEncoding.LegacyLz4;
}
