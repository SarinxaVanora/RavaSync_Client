using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketBatchFileResponseDto
{
    public string Hash { get; init; } = string.Empty;
    public bool UploadRequired { get; init; }
    public string Mode { get; init; } = string.Empty;
    public string UploadUrl { get; init; } = string.Empty;
    public DateTime ExpiresUtc { get; init; }
    public string ObjectKey { get; init; } = string.Empty;
    public long MaxUploadBytes { get; init; }
    public IReadOnlyDictionary<string, string>? RequiredHeaders { get; init; }
    public bool Success { get; init; } = true;
    public string Error { get; init; } = string.Empty;
}
