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

    public UploadCompleteDto() { }

    public UploadCompleteDto(long rawSize, long compressedSize, string? contentMd5Base64 = null, string? eTag = null, bool wasDirect = true)
    {
        RawSize = rawSize;
        CompressedSize = compressedSize;
        ContentMd5Base64 = contentMd5Base64;
        ETag = eTag;
        WasDirect = wasDirect;
    }
}
