using MessagePack;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketRequestDto
{
    public long RawSize { get; init; }
    public bool ClientWouldMunge { get; init; }
    public int ClientParallelUploads { get; init; }
    public string PayloadEncoding { get; init; } = FilePayloadEncoding.LegacyLz4;

    public UploadTicketRequestDto() { }

    public UploadTicketRequestDto(long rawSize, bool clientWouldMunge, int clientParallelUploads = 0, string? payloadEncoding = null)
    {
        RawSize = rawSize;
        ClientWouldMunge = clientWouldMunge;
        ClientParallelUploads = clientParallelUploads;
        PayloadEncoding = string.IsNullOrWhiteSpace(payloadEncoding) ? FilePayloadEncoding.LegacyLz4 : payloadEncoding;
    }
}
