using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadCompleteDto
{
    public long RawSize { get; init; }
    public long CompressedSize { get; init; }
    public string? ContentMd5Base64 { get; init; }
    public string? ETag { get; init; }
    public bool WasDirect { get; init; }
    public string PayloadEncoding { get; init; } = FilePayloadEncoding.LegacyLz4;

    public UploadCompleteDto() { }

    public UploadCompleteDto(long rawSize, long compressedSize, string? contentMd5Base64 = null, string? eTag = null, bool wasDirect = true, string? payloadEncoding = null)
    {
        RawSize = rawSize;
        CompressedSize = compressedSize;
        ContentMd5Base64 = contentMd5Base64;
        ETag = eTag;
        WasDirect = wasDirect;
        PayloadEncoding = string.IsNullOrWhiteSpace(payloadEncoding) ? FilePayloadEncoding.LegacyLz4 : payloadEncoding;
    }
}
