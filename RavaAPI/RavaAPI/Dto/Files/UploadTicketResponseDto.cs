using MessagePack;
using System;
using System.Collections.Generic;

namespace RavaSync.API.Dto.Files;

[MessagePackObject(keyAsPropertyName: true)]
public sealed record UploadTicketResponseDto
{
    public bool UploadRequired { get; init; }
    public string Mode { get; init; } = string.Empty;
    public string UploadUrl { get; init; } = string.Empty;
    public DateTime ExpiresUtc { get; init; }
    public string ObjectKey { get; init; } = string.Empty;
    public long MaxUploadBytes { get; init; }
    public IReadOnlyDictionary<string, string>? RequiredHeaders { get; init; }

    public UploadTicketResponseDto() { }

    public UploadTicketResponseDto(
        bool uploadRequired,
        string mode,
        string uploadUrl,
        DateTime expiresUtc,
        string objectKey,
        long maxUploadBytes,
        IReadOnlyDictionary<string, string>? requiredHeaders = null)
    {
        UploadRequired = uploadRequired;
        Mode = mode;
        UploadUrl = uploadUrl;
        ExpiresUtc = expiresUtc;
        ObjectKey = objectKey;
        MaxUploadBytes = maxUploadBytes;
        RequiredHeaders = requiredHeaders;
    }
}
