using RavaSync.API.Data;
using RavaSync.WebAPI.Files.Models;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace RavaSync.PlayerData.Services;

public static class PairApplyUtilities
{
    public static bool IsAnimationCriticalGamePath(string gamePath)
    {
        gamePath = NormalizeGamePath(gamePath);
        if (string.IsNullOrEmpty(gamePath)) return false;

        return gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || IsSkeletonOrPhysicsCriticalGamePath(gamePath);
    }

    public static bool IsVfxCriticalGamePath(string gamePath)
        => IsTransientRedrawCriticalGamePath(gamePath);

    public static bool IsTransientRedrawCriticalGamePath(string gamePath)
    {
        gamePath = NormalizeGamePath(gamePath);
        if (string.IsNullOrEmpty(gamePath)) return false;

        if (gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase))
            return true;

        if (gamePath.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".skp", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (gamePath.EndsWith(".eid", StringComparison.OrdinalIgnoreCase))
            return IsDangerousTransientScope(gamePath);

        return gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || IsSkeletonOrPhysicsCriticalGamePath(gamePath);
    }

    public static bool IsSkeletonOrPhysicsCriticalGamePath(string gamePath)
    {
        gamePath = NormalizeGamePath(gamePath);
        if (string.IsNullOrEmpty(gamePath)) return false;

        if (gamePath.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase))
            return true;

        if (!gamePath.EndsWith(".phy", StringComparison.OrdinalIgnoreCase)
            && !gamePath.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
            && !gamePath.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return !IsEquipmentScopedGamePath(gamePath);
    }

    public static bool IsEquipmentScopedGamePath(string gamePath)
    {
        gamePath = NormalizeGamePath(gamePath);
        if (string.IsNullOrEmpty(gamePath)) return false;

        return gamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/equipment/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/weapon/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsDangerousTransientScope(string gamePath)
    {
        if (IsEquipmentScopedGamePath(gamePath))
            return false;

        return gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/animation/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/action/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase);
    }

    public static string NormalizeGamePath(string gamePath)
        => string.IsNullOrWhiteSpace(gamePath)
            ? string.Empty
            : gamePath.Replace('\\', '/').Trim();

    public static bool IsVfxPropSupportGamePath(string gamePath)
    {
        if (string.IsNullOrEmpty(gamePath)) return false;

        gamePath = gamePath.Trim();

        return gamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsVfxModelSupportGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        gamePath = gamePath.Replace('\\', '/').Trim();

        return gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || IsVfxPropSupportGamePath(gamePath);
    }

    public static string ComputeTempModsFingerprint(Dictionary<string, string> tempMods)
    {
        if (tempMods.Count == 0) return "EMPTY";

        using var sha1 = SHA1.Create();

        var bytePool = ArrayPool<byte>.Shared;
        var keyPool = ArrayPool<string>.Shared;

        var rentedKeys = keyPool.Rent(tempMods.Count);
        var keyCount = 0;

        try
        {
            foreach (var key in tempMods.Keys)
                rentedKeys[keyCount++] = key;

            Array.Sort(rentedKeys, 0, keyCount, StringComparer.Ordinal);

            for (int i = 0; i < keyCount; i++)
            {
                var key = rentedKeys[i] ?? string.Empty;
                var val = tempMods[key] ?? string.Empty;

                var keyByteCount = Encoding.UTF8.GetByteCount(key);
                var valByteCount = Encoding.UTF8.GetByteCount(val);
                var total = keyByteCount + 1 + valByteCount + 1;

                var buf = bytePool.Rent(total);
                try
                {
                    var offset = 0;

                    offset += Encoding.UTF8.GetBytes(key, 0, key.Length, buf, offset);
                    buf[offset++] = (byte)'\n';

                    offset += Encoding.UTF8.GetBytes(val, 0, val.Length, buf, offset);
                    buf[offset++] = (byte)'\n';

                    sha1.TransformBlock(buf, 0, offset, null, 0);
                }
                finally
                {
                    bytePool.Return(buf);
                }
            }

            sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            return Convert.ToHexString(sha1.Hash!);
        }
        finally
        {
            Array.Clear(rentedKeys, 0, keyCount);
            keyPool.Return(rentedKeys);
        }
    }


    public static string ComputePathSetFingerprint(IEnumerable<string> gamePaths)
    {
        var normalized = gamePaths
            .Where(static p => !string.IsNullOrWhiteSpace(p))
            .Select(static p => p.Trim().ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static p => p, StringComparer.Ordinal)
            .ToArray();

        if (normalized.Length == 0)
            return "EMPTY";

        using var sha1 = SHA1.Create();
        var bytePool = ArrayPool<byte>.Shared;

        foreach (var path in normalized)
        {
            var byteCount = Encoding.UTF8.GetByteCount(path) + 1;
            var buf = bytePool.Rent(byteCount);
            try
            {
                var offset = 0;
                offset += Encoding.UTF8.GetBytes(path, 0, path.Length, buf, offset);
                buf[offset++] = (byte)'\n';
                sha1.TransformBlock(buf, 0, offset, null, 0);
            }
            finally
            {
                bytePool.Return(buf);
            }
        }

        sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha1.Hash!);
    }

    public static string ComputeManipulationFingerprint(string? manipulationData)
        => manipulationData?.Trim() ?? string.Empty;

    public static string ComputeCharacterDataPayloadFingerprint(CharacterData? charaData)
    {
        if (charaData == null)
            return "NULL";

        using var sha1 = SHA1.Create();

        void Append(string? value)
        {
            value ??= string.Empty;
            var bytes = Encoding.UTF8.GetBytes(value);
            if (bytes.Length > 0)
                sha1.TransformBlock(bytes, 0, bytes.Length, null, 0);

            sha1.TransformBlock(new[] { (byte)'\n' }, 0, 1, null, 0);
        }

        Append("HASH");
        Append(charaData.DataHash.Value);

        Append("FILES");
        foreach (var objectFiles in charaData.FileReplacements.OrderBy(k => (int)k.Key))
        {
            Append(((int)objectFiles.Key).ToString(System.Globalization.CultureInfo.InvariantCulture));

            var orderedFiles = (objectFiles.Value ?? [])
                .OrderBy(f => f.Hash ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ThenBy(f => f.FileSwapPath ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ThenBy(f => string.Join("\u001F", (f.GamePaths ?? []).Where(p => !string.IsNullOrWhiteSpace(p)).OrderBy(p => p, StringComparer.OrdinalIgnoreCase)), StringComparer.OrdinalIgnoreCase);

            foreach (var file in orderedFiles)
            {
                Append("FILE");
                Append(file.Hash);
                Append(file.FileSwapPath);

                foreach (var gamePath in (file.GamePaths ?? [])
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
                {
                    Append(gamePath);
                }
            }
        }

        Append("GLAMOURER");
        foreach (var kvp in charaData.GlamourerData.OrderBy(k => (int)k.Key))
        {
            Append(((int)kvp.Key).ToString(System.Globalization.CultureInfo.InvariantCulture));
            Append(kvp.Value);
        }

        Append("CUSTOMIZE");
        foreach (var kvp in charaData.CustomizePlusData.OrderBy(k => (int)k.Key))
        {
            Append(((int)kvp.Key).ToString(System.Globalization.CultureInfo.InvariantCulture));
            Append(kvp.Value);
        }

        Append("MANIP");
        Append(charaData.ManipulationData);
        Append("HEELS");
        Append(charaData.HeelsData);
        Append("HONORIFIC");
        Append(charaData.HonorificData);
        Append("MOODLES");
        Append(charaData.MoodlesData);
        Append("PETNAMES");
        Append(charaData.PetNamesData);

        sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha1.Hash!);
    }

    public static Dictionary<string, string> BuildPenumbraTempMods(Dictionary<(string GamePath, string? Hash), string> moddedPaths, Action<string,string,string>? logConflict = null)
    {
        var output = new Dictionary<string, string>(moddedPaths.Count, StringComparer.OrdinalIgnoreCase);
        var winnerHashes = new Dictionary<string, string?>(moddedPaths.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in moddedPaths)
        {
            var gamePath = kvp.Key.GamePath?.Replace('\\', '/').Trim();
            if (string.IsNullOrWhiteSpace(gamePath)) continue;

            var path = kvp.Value;
            if (string.IsNullOrWhiteSpace(path)) continue;

            var incomingHash = kvp.Key.Hash;

            if (!output.TryGetValue(gamePath, out var existingPath))
            {
                output[gamePath] = path;
                winnerHashes[gamePath] = incomingHash;
                continue;
            }

            var existingHash = winnerHashes[gamePath];
            var existingIsSwap = string.IsNullOrEmpty(existingHash);
            var incomingIsSwap = string.IsNullOrEmpty(incomingHash);

            // File swaps are explicit Penumbra option results and must win over a
            // normal replacement for the same game path. This matters for options
            // such as texture/pattern dropdowns.
            if (!existingIsSwap && incomingIsSwap)
            {
                output[gamePath] = path;
                winnerHashes[gamePath] = incomingHash;
                logConflict?.Invoke(gamePath, existingPath, path);
                continue;
            }

            if (existingIsSwap && !incomingIsSwap)
                continue;

            // Same-kind collisions should not depend on dictionary/hash ordering.
            // ModPathResolver preserves payload order, so the later effective entry
            // wins just like Penumbra's final resolved option view.
            if (!string.Equals(existingPath, path, StringComparison.OrdinalIgnoreCase))
                logConflict?.Invoke(gamePath, existingPath, path);

            output[gamePath] = path;
            winnerHashes[gamePath] = incomingHash;
        }

        return output;
    }

    public static List<FileReplacementData> DeduplicateReplacementsByHash(List<FileReplacementData> input)
    {
        if (input.Count <= 1)
            return input;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<FileReplacementData>(input.Count);

        for (int i = 0; i < input.Count; i++)
        {
            var item = input[i];
            var hash = item.Hash;

            if (string.IsNullOrWhiteSpace(hash))
            {
                result.Add(item);
                continue;
            }

            if (seen.Add(hash))
                result.Add(item);
        }

        return result;
    }

    public static string GetFirstGamePathOrFallback(FileReplacementData replacement)
    {
        var gamePaths = replacement.GamePaths;
        if (gamePaths == null)
            return "nogamepath";

        if (gamePaths is IList<string> list)
        {
            for (int i = 0; i < list.Count; i++)
            {
                var p = list[i];
                if (!string.IsNullOrWhiteSpace(p))
                    return p;
            }

            return "nogamepath";
        }

        foreach (var p in gamePaths)
        {
            if (!string.IsNullOrWhiteSpace(p))
                return p;
        }

        return "nogamepath";
    }

    public static string BuildMissingDetails(List<FileReplacementData> replacements)
    {
        if (replacements.Count == 0)
            return string.Empty;

        var sb = new StringBuilder();

        for (int i = 0; i < replacements.Count; i++)
        {
            var r = replacements[i];
            if (i > 0)
                sb.Append(", ");

            sb.Append(r.Hash);
            sb.Append(':');
            sb.Append(GetFirstGamePathOrFallback(r));
        }

        return sb.ToString();
    }
}
