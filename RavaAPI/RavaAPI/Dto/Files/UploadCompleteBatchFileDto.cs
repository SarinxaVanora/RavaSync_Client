using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadCompleteBatchFileDto
{
    public string Hash { get; init; } = string.Empty;
    public long RawSize { get; init; }
    public long CompressedSize { get; init; }
    public string? ContentMd5Base64 { get; init; }
    public string? ETag { get; init; }
    public bool WasDirect { get; init; } = true;
    public string PayloadEncoding { get; init; } = FilePayloadEncoding.LegacyLz4;
}
