using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using RavaSync.API.Data.Enum;
using ApiCharacterData = RavaSync.API.Data.CharacterData;
using ApiFileReplacementData = RavaSync.API.Data.FileReplacementData;

namespace RavaSync.PlayerData.Data;

public static partial class CharacterDataPushSanitizer
{
    private static readonly string[] ServerAllowedGamePathExtensions = [".mdl", ".tex", ".mtrl", ".tmb", ".tmb2", ".pap", ".avfx", ".atex", ".sklb", ".atch", ".eid", ".phy", ".phyb", ".pbd", ".scd", ".skp", ".shpk"];

    public readonly record struct Result(int RemovedReplacements, int RemovedGamePaths, int RemovedBuckets, int RemovedHonorificData)
    {
        public bool Changed => RemovedReplacements > 0 || RemovedGamePaths > 0 || RemovedBuckets > 0 || RemovedHonorificData > 0;
    }

    public static Result SanitizeForPush(ApiCharacterData? data)
    {
        if (data == null)
            return default;

        int removedReplacements = 0;
        int removedGamePaths = 0;
        int removedBuckets = 0;
        int removedHonorificData = 0;

        if (ContainsServerRejectedHonorificTitle(data.HonorificData))
        {
            data.HonorificData = string.Empty;
            removedHonorificData = 1;
        }

        if (data.FileReplacements != null && data.FileReplacements.Count > 0)
        {
            foreach (var objectKind in data.FileReplacements.Keys.ToArray())
            {
                if (!data.FileReplacements.TryGetValue(objectKind, out var replacements) || replacements == null)
                {
                    data.FileReplacements.Remove(objectKind);
                    removedBuckets++;
                    continue;
                }

                for (int i = replacements.Count - 1; i >= 0; i--)
                {
                    var replacement = replacements[i];
                    var originalGamePathCount = replacement?.GamePaths?.Length ?? 0;

                    if (!TrySanitizeReplacement(replacement, out var sanitizedGamePaths))
                    {
                        replacements.RemoveAt(i);
                        removedReplacements++;
                        continue;
                    }

                    if (originalGamePathCount != sanitizedGamePaths.Length)
                        removedGamePaths += Math.Max(0, originalGamePathCount - sanitizedGamePaths.Length);

                    replacement!.GamePaths = sanitizedGamePaths;
                }

                if (replacements.Count == 0)
                {
                    data.FileReplacements.Remove(objectKind);
                    removedBuckets++;
                }
            }
        }

        StripVanillaOwnedObjectMetadata(data);

        return new Result(removedReplacements, removedGamePaths, removedBuckets, removedHonorificData);
    }


    private static void StripVanillaOwnedObjectMetadata(ApiCharacterData data)
    {
        // Mounts/minions are already visible to the game when they are vanilla.
        // A plain Glamourer snapshot for that actor is descriptive state, not a RavaSync mod
        // payload; pushing it makes receivers apply/redraw perfectly vanilla objects. Keep the
        // mount/minion metadata only when there is an actual file/C+ payload for that actor.
        foreach (var objectKind in Enum.GetValues<ObjectKind>())
        {
            if (objectKind != ObjectKind.MinionOrMount)
                continue;

            var hasFilePayload = data.FileReplacements.TryGetValue(objectKind, out var replacements)
                && replacements != null
                && replacements.Count > 0;
            var hasCustomizePayload = data.CustomizePlusData.TryGetValue(objectKind, out var customize)
                && !string.IsNullOrWhiteSpace(customize);

            if (hasFilePayload || hasCustomizePayload)
                continue;

            data.GlamourerData.Remove(objectKind);
            data.CustomizePlusData.Remove(objectKind);
        }
    }

    public static bool IsServerAcceptedGamePath(string? gamePath)
        => IsServerAcceptedPath(gamePath, requireKnownTransferExtension: true);

    public static bool IsServerAcceptedFileSwapPath(string? gamePath)
        => string.IsNullOrWhiteSpace(gamePath) || IsServerAcceptedPath(gamePath, requireKnownTransferExtension: false);

    public static bool IsServerAcceptedHash(string? hash)
        => string.IsNullOrEmpty(hash) || ServerHashRegex().IsMatch(hash);

    public static string[] GetServerAcceptedGamePaths(IEnumerable<string>? gamePaths)
        => GetServerAcceptedModdedGamePaths(gamePaths, resolvedPath: null);

    public static string[] GetServerAcceptedModdedGamePaths(IEnumerable<string>? gamePaths, string? resolvedPath)
    {
        var normalizedResolvedPath = NormalizeGamePathForPush(resolvedPath);
        var resolvedPathLooksLikeGamePath = !string.IsNullOrWhiteSpace(normalizedResolvedPath)
            && IsServerAcceptedFileSwapPath(normalizedResolvedPath);

        if (resolvedPathLooksLikeGamePath && IsUiOrInterfaceGamePath(normalizedResolvedPath))
            return [];

        return (gamePaths ?? Array.Empty<string>())
            .Select(NormalizeGamePathForPush)
            .Where(IsServerAcceptedGamePath)
            .Where(path => !resolvedPathLooksLikeGamePath || !string.Equals(path, normalizedResolvedPath, StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public static bool IsUiOrInterfaceGamePath(string? gamePath)
    {
        var normalized = NormalizeGamePathForPush(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        // Penumbra can expose UI/icon resources through the same resolved-path APIs as real
        // character files. RavaSync should never advertise HUD/interface files as syncable
        // character state; those paths cannot be applied meaningfully to another actor and can
        // leave hashes in the upload/share barrier that are not part of the real appearance.
        return normalized.StartsWith("ui/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("addon/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("font/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ui/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/addon/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/font/", StringComparison.OrdinalIgnoreCase);
    }

    public static string NormalizeGamePathForPush(string? value)
    {
        var normalized = (value ?? string.Empty)
            .Replace('\\', '/')
            .Trim()
            .Trim('\0')
            .ToLowerInvariant();

        while (normalized.StartsWith("./", StringComparison.Ordinal))
            normalized = normalized[2..];

        return normalized;
    }


    private static bool ContainsServerRejectedHonorificTitle(string? honorificData)
    {
        if (string.IsNullOrEmpty(honorificData))
            return false;

        try
        {
            var honorificJson = Encoding.Default.GetString(Convert.FromBase64String(honorificData));
            var deserialized = JsonSerializer.Deserialize<JsonElement>(honorificJson);
            if (deserialized.ValueKind != JsonValueKind.Object || !deserialized.TryGetProperty("Title", out var honorificTitle))
                return false;

            var title = honorificTitle.GetString()?.Normalize(NormalizationForm.FormKD);
            return !string.IsNullOrEmpty(title) && ServerUrlRegex().IsMatch(title);
        }
        catch
        {
            return false;
        }
    }

    private static bool TrySanitizeReplacement(ApiFileReplacementData? replacement, out string[] gamePaths)
    {
        gamePaths = [];
        if (replacement == null)
            return false;

        if (!IsServerAcceptedHash(replacement.Hash))
            return false;

        var fileSwapPath = NormalizeGamePathForPush(replacement.FileSwapPath);
        if (!IsServerAcceptedFileSwapPath(fileSwapPath))
            return false;

        replacement.FileSwapPath = fileSwapPath;
        gamePaths = GetServerAcceptedModdedGamePaths(replacement.GamePaths, fileSwapPath);
        return gamePaths.Length > 0;
    }

    private static bool IsServerAcceptedPath(string? gamePath, bool requireKnownTransferExtension)
    {
        var normalized = NormalizeGamePathForPush(gamePath);
        if (string.IsNullOrWhiteSpace(normalized) || !ServerGamePathRegex().IsMatch(normalized))
            return false;

        if (IsUiOrInterfaceGamePath(normalized))
            return false;

        return !requireKnownTransferExtension
            || ServerAllowedGamePathExtensions.Any(e => normalized.EndsWith(e, StringComparison.OrdinalIgnoreCase));
    }

    [GeneratedRegex(@"^([a-z0-9_ '+&,\.\-\{\}]+\/)+([a-z0-9_ '+&,\.\-\{\}]+\.[a-z]{3,4})$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.ECMAScript)]
    private static partial Regex ServerGamePathRegex();

    [GeneratedRegex(@"^[A-Z0-9]{40}$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.ECMAScript)]
    private static partial Regex ServerHashRegex();

    [GeneratedRegex("^[-a-zA-Z0-9@:%._\\+~#=]{1,256}[\\.,][a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)$")]
    private static partial Regex ServerUrlRegex();
}
