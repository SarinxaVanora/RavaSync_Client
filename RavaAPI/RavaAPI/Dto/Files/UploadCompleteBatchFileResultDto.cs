using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadCompleteBatchFileResultDto
{
    public string Hash { get; init; } = string.Empty;
    public bool Success { get; init; }
    public int StatusCode { get; init; } = 200;
    public string Error { get; init; } = string.Empty;
}
