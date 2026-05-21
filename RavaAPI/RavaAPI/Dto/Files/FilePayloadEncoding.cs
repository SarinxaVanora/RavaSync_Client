namespace RavaSync.API.Dto.Files;

public static class FilePayloadEncoding
{
    public const string LegacyLz4 = "LegacyLz4";
    public const string RawV2 = "RawV2";

    public static bool IsRaw(string? value) => string.Equals(value, RawV2, System.StringComparison.OrdinalIgnoreCase);
    public static bool IsLegacyLz4(string? value) => string.IsNullOrWhiteSpace(value) || string.Equals(value, LegacyLz4, System.StringComparison.OrdinalIgnoreCase);
}
