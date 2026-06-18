using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace RavaSync.Services.Optimisation;

public sealed class ScreenShakeSanitisationService
{
    private const string SanitiserVersion = "ss-avfx-v9-camera-quake-effectors";
    private static readonly byte[] AvfxMagic = Encoding.ASCII.GetBytes("XFVA");

    private readonly ILogger<ScreenShakeSanitisationService> _logger;
    private readonly FileCacheManager _fileCacheManager;
    private readonly ConcurrentDictionary<string, SanitiseResult> _resultCache = new(StringComparer.OrdinalIgnoreCase);

    public ScreenShakeSanitisationService(ILogger<ScreenShakeSanitisationService> logger, FileCacheManager fileCacheManager)
    {
        _logger = logger;
        _fileCacheManager = fileCacheManager;
    }

    public bool TryGetScreenShakeSafePath(string? gamePath, string? resolvedPath, string? hash, out string safePath)
    {
        safePath = resolvedPath ?? string.Empty;
        if (!IsAvfxCandidate(gamePath, resolvedPath))
            return false;

        if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
            return false;

        try
        {
            var fileInfo = new FileInfo(resolvedPath);
            var cacheKey = $"{SanitiserVersion}|{hash}|{NormalizeSlashes(gamePath)}|{resolvedPath}|{fileInfo.Length}|{fileInfo.LastWriteTimeUtc.Ticks}";
            var result = _resultCache.GetOrAdd(cacheKey, _ => BuildSanitisedResult(gamePath, resolvedPath, hash));
            if (!result.Changed || string.IsNullOrWhiteSpace(result.SafePath))
                return false;

            safePath = result.SafePath;
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to sanitise screen shake AVFX {gamePath} from {resolvedPath}", gamePath, resolvedPath);
            safePath = resolvedPath ?? string.Empty;
            return false;
        }
    }

    private SanitiseResult BuildSanitisedResult(string? gamePath, string resolvedPath, string? hash)
    {
        var sourceBytes = File.ReadAllBytes(resolvedPath);
        if (!LooksLikeAvfx(sourceBytes))
            return SanitiseResult.Unchanged;

        var sanitisedBytes = (byte[])sourceBytes.Clone();
        var changed = false;

        // Keep this completely content-driven: every AVFX is inspected, no path/name checks.
        // First remove actual AVFX CameraQuake effectors. AVFX block names are stored reversed,
        // so top-level Efct blocks appear as tcfE, Emit blocks as timE, and the emitter/timeline
        // effector index field appears as oNfE (EfNo). This catches Meteor Dote style packs where
        // the shake lives in the target-side AVFX effectors rather than in the player-side filename.
        var removedCameraShakeEffectors = DisableCameraShakeEffectors(sanitisedBytes);
        changed |= removedCameraShakeEffectors;

        // The DRK on/off comparison proved one older embedded-shake shape as oNfE 1 -> 0
        // inside a camera-control-looking block. Keep that narrow compatibility pass only when
        // no real CameraQuake effector was found. If the file already has Efct/CameraQuake data,
        // the typed effector pass above is safer than blindly remapping EfNo 1, because EfNo 1 can
        // also be a harmless radial-blur/visual effector in normal AVFX files.
        if (!removedCameraShakeEffectors)
            changed |= DisableProvenTimelineCameraShakeReferences(sanitisedBytes);

        // Some incoming AVFX are dedicated camera/control helpers rather than normal visual
        // effects. Do not drop them and do not corrupt the AVFX root counts. Instead, leave
        // the file structurally intact and switch its enabled blocks off. That preserves a
        // valid resource for Penumbra/game loading while making the helper inert.
        if (LooksLikeCameraControlOnlyAvfx(sourceBytes))
            changed |= DisableEnabledBlocks(sanitisedBytes);

        if (!changed)
            return SanitiseResult.Unchanged;

        var outputPath = BuildOutputPath(gamePath, resolvedPath, hash, sourceBytes);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        WriteAllBytesIfDifferent(outputPath, sanitisedBytes);

        _logger.LogTrace("Sanitised AVFX camera/screen-shake data for {gamePath}: {source} => {safe}", gamePath, resolvedPath, outputPath);
        return new SanitiseResult(true, outputPath);
    }

    public static bool IsAvfxCandidate(string? gamePath, string? resolvedPath = null)
    {
        var candidate = NormalizeSlashes(gamePath);
        if (string.Equals(Path.GetExtension(candidate), ".avfx", StringComparison.OrdinalIgnoreCase))
            return true;

        var resolved = NormalizeSlashes(resolvedPath);
        return string.Equals(Path.GetExtension(resolved), ".avfx", StringComparison.OrdinalIgnoreCase);
    }

    private string BuildOutputPath(string? gamePath, string resolvedPath, string? hash, byte[] sourceBytes)
    {
        var cacheRoot = _fileCacheManager.CacheFolder;
        if (string.IsNullOrWhiteSpace(cacheRoot))
            cacheRoot = Path.GetDirectoryName(resolvedPath) ?? Path.GetTempPath();

        var keyMaterial = $"{SanitiserVersion}|{hash}|{NormalizeSlashes(gamePath)}|{resolvedPath}|{sourceBytes.Length}|{SHA1OfBytes(sourceBytes)}";
        var keyBytes = SHA1.HashData(Encoding.UTF8.GetBytes(keyMaterial));
        var key = Convert.ToHexString(keyBytes).ToLowerInvariant();
        return Path.Combine(cacheRoot, "RavaSync_Sanitised", "ScreenShake", key + ".avfx");
    }

    private static bool LooksLikeAvfx(byte[] data)
    {
        if (data.Length < 16)
            return false;

        return data[0] == AvfxMagic[0]
            && data[1] == AvfxMagic[1]
            && data[2] == AvfxMagic[2]
            && data[3] == AvfxMagic[3];
    }

    private static bool LooksLikeCameraControlOnlyAvfx(byte[] data)
    {
        if (!ContainsTimelineCameraControl(data))
            return false;

        return TryReadRootCount(data, "nCrP", out var particleCount) && particleCount == 0
            && TryReadRootCount(data, "nCxT", out var textureCount) && textureCount == 0
            && TryReadRootCount(data, "nCdM", out var modelCount) && modelCount == 0;
    }

    private static bool ContainsTimelineCameraControl(byte[] data)
    {
        var tag = Encoding.ASCII.GetBytes("oNfE");
        var offset = 0;
        while ((offset = IndexOf(data, tag, offset, data.Length - 12)) >= 0)
        {
            if (LooksLikeTimelineCameraControl(data, offset))
                return true;

            offset += 4;
        }

        return false;
    }

    private static bool DisableCameraShakeEffectors(byte[] data)
    {
        if (!TryGetRootContentRange(data, out var rootStart, out var rootEnd))
            return false;

        var cameraShakeEffectors = new HashSet<int>();
        var effectorIndex = 0;
        foreach (var block in EnumerateBlocks(data, rootStart, rootEnd))
        {
            if (!TagEquals(data, block.Offset, "tcfE")) // Efct, reversed.
                continue;

            if (TryReadInt32Field(data, block.ContentStart, block.ContentEnd, "TVfE", out var effectorType, out _)
                && IsCameraShakeEffectorType(effectorType))
            {
                cameraShakeEffectors.Add(effectorIndex);
            }

            effectorIndex++;
        }

        if (cameraShakeEffectors.Count == 0)
            return false;

        var changed = false;

        // Detach emitters/timeline items from camera-quake effectors. EfNo is stored as oNfE.
        // Use -1, matching the game's existing "no effector" value seen throughout vanilla AVFX.
        var tag = Encoding.ASCII.GetBytes("oNfE");
        var offset = 0;
        while ((offset = IndexOf(data, tag, offset, data.Length - 12)) >= 0)
        {
            if (offset + 12 > data.Length)
                break;

            var size = BitConverter.ToInt32(data, offset + 4);
            if (size == 4)
            {
                var value = BitConverter.ToInt32(data, offset + 8);
                if (cameraShakeEffectors.Contains(value))
                {
                    WriteInt32(data, offset + 8, -1);
                    changed = true;
                }
            }

            offset += 4;
        }

        // Also make the effectors inert for any engine path that can touch them directly.
        effectorIndex = 0;
        foreach (var block in EnumerateBlocks(data, rootStart, rootEnd))
        {
            if (!TagEquals(data, block.Offset, "tcfE"))
                continue;

            if (cameraShakeEffectors.Contains(effectorIndex))
            {
                changed |= ClearNumericField(data, block.ContentStart, block.ContentEnd, "VOAb"); // bAOV
                changed |= ClearNumericField(data, block.ContentStart, block.ContentEnd, "mGAb"); // bAGm
            }

            effectorIndex++;
        }

        return changed;
    }

    private static bool IsCameraShakeEffectorType(int effectorType)
        => effectorType == 6  // CameraQuake_Variable
            || effectorType == 9; // CameraQuake

    private static bool DisableProvenTimelineCameraShakeReferences(byte[] data)
    {
        var changed = false;
        var tag = Encoding.ASCII.GetBytes("oNfE");
        var offset = 0;
        while ((offset = IndexOf(data, tag, offset, data.Length - 12)) >= 0)
        {
            if (offset + 12 > data.Length)
                break;

            var size = BitConverter.ToInt32(data, offset + 4);
            if (size == 4 && LooksLikeTimelineCameraControl(data, offset))
            {
                var value = BitConverter.ToInt32(data, offset + 8);
                if (value == 1)
                {
                    Array.Clear(data, offset + 8, 4);
                    changed = true;
                }
            }

            offset += 4;
        }

        return changed;
    }

    private static bool DisableAlwaysOnCameraTimelineReferences(byte[] data)
    {
        var changed = false;
        var tag = Encoding.ASCII.GetBytes("timE");
        var offset = 0;
        while ((offset = IndexOf(data, tag, offset, data.Length - 12)) >= 0)
        {
            if (offset + 8 > data.Length)
                break;

            var size = BitConverter.ToInt32(data, offset + 4);
            var blockEnd = offset + 8 + size;
            if (size <= 0 || blockEnd > data.Length)
            {
                offset += 4;
                continue;
            }

            if (LooksLikeCameraControlTimelineBlock(data, offset, blockEnd) && !ContainsSoundReference(data, offset, blockEnd))
                changed |= ClearAlwaysOnTimelineFlags(data, offset, blockEnd);

            offset = Math.Max(offset + 4, blockEnd);
        }

        return changed;
    }

    private static bool ClearAlwaysOnTimelineFlags(byte[] data, int start, int endExclusive)
    {
        var changed = false;
        var tag = Encoding.ASCII.GetBytes("oNfE");
        var offset = start;
        while ((offset = IndexOf(data, tag, offset, endExclusive - 12)) >= 0)
        {
            if (offset + 12 > endExclusive)
                break;

            var size = BitConverter.ToInt32(data, offset + 4);
            if (size == 4)
            {
                var value = BitConverter.ToInt32(data, offset + 8);
                if (value == -1)
                {
                    Array.Clear(data, offset + 8, 4);
                    changed = true;
                }
            }

            offset += 4;
        }

        return changed;
    }

    private static bool DisableEnabledBlocks(byte[] data)
    {
        var changed = false;
        var tag = Encoding.ASCII.GetBytes("anEb");
        var offset = 0;
        while ((offset = IndexOf(data, tag, offset, data.Length - 12)) >= 0)
        {
            if (offset + 12 > data.Length)
                break;

            var size = BitConverter.ToInt32(data, offset + 4);
            if (size == 4)
            {
                var value = BitConverter.ToInt32(data, offset + 8);
                if (value == 1)
                {
                    Array.Clear(data, offset + 8, 4);
                    changed = true;
                }
            }

            offset += 4;
        }

        return changed;
    }

    private static bool LooksLikeTimelineCameraControl(byte[] data, int offset)
    {
        var start = Math.Max(0, offset - 96);
        var end = Math.Min(data.Length, offset + 128);
        return ContainsAscii(data, start, end, "timE")
            && ContainsAscii(data, start, end, "nClC")
            && ContainsAscii(data, start, end, "DAb\0")
            && ContainsAscii(data, start, end, "TVE\0")
            && ContainsAscii(data, start, end, "TDBR")
            && ContainsAscii(data, start, end, "TOCC")
            && ContainsAscii(data, start, end, "TOR\0");
    }

    private static bool LooksLikeCameraControlTimelineBlock(byte[] data, int start, int endExclusive)
        => ContainsAscii(data, start, endExclusive, "nClC")
            && ContainsAscii(data, start, endExclusive, "DAb\0")
            && ContainsAscii(data, start, endExclusive, "TVE\0")
            && ContainsAscii(data, start, endExclusive, "TDBR")
            && ContainsAscii(data, start, endExclusive, "TOCC")
            && ContainsAscii(data, start, endExclusive, "TOR\0");

    private static bool ContainsSoundReference(byte[] data, int start, int endExclusive)
        => ContainsAscii(data, start, endExclusive, ".scd")
            || ContainsAscii(data, start, endExclusive, "sound/")
            || ContainsAscii(data, start, endExclusive, "sound\\");

    private static bool TryReadRootCount(byte[] data, string tag, out int value)
    {
        value = 0;
        var searchLength = Math.Min(data.Length - 12, 4096);
        if (searchLength <= 0)
            return false;

        var tagBytes = Encoding.ASCII.GetBytes(tag);
        var offset = IndexOf(data, tagBytes, 0, searchLength);
        if (offset < 0 || offset + 12 > data.Length)
            return false;

        var size = BitConverter.ToInt32(data, offset + 4);
        if (size != 4)
            return false;

        value = BitConverter.ToInt32(data, offset + 8);
        return true;
    }

    private static bool TryGetRootContentRange(byte[] data, out int start, out int endExclusive)
    {
        start = 0;
        endExclusive = 0;
        if (!LooksLikeAvfx(data) || data.Length < 8)
            return false;

        var size = BitConverter.ToInt32(data, 4);
        if (size <= 0 || 8 + size > data.Length)
            return false;

        start = 8;
        endExclusive = 8 + size;
        return true;
    }

    private static IEnumerable<BlockRange> EnumerateBlocks(byte[] data, int start, int endExclusive)
    {
        var offset = start;
        while (offset + 8 <= endExclusive)
        {
            var size = BitConverter.ToInt32(data, offset + 4);
            if (size < 0)
                yield break;

            var contentStart = offset + 8;
            var contentEnd = contentStart + size;
            if (contentEnd > endExclusive)
                yield break;

            yield return new BlockRange(offset, contentStart, contentEnd);

            var paddedSize = (size + 3) & ~3;
            var next = contentStart + paddedSize;
            if (next <= offset)
                yield break;

            offset = next;
        }
    }

    private static bool TryReadInt32Field(byte[] data, int start, int endExclusive, string tag, out int value, out int valueOffset)
    {
        value = 0;
        valueOffset = 0;
        foreach (var block in EnumerateBlocks(data, start, endExclusive))
        {
            if (!TagEquals(data, block.Offset, tag))
                continue;

            if (block.ContentEnd - block.ContentStart != 4)
                return false;

            value = BitConverter.ToInt32(data, block.ContentStart);
            valueOffset = block.ContentStart;
            return true;
        }

        return false;
    }

    private static bool ClearNumericField(byte[] data, int start, int endExclusive, string tag)
    {
        var changed = false;
        foreach (var block in EnumerateBlocks(data, start, endExclusive))
        {
            if (!TagEquals(data, block.Offset, tag))
                continue;

            var length = block.ContentEnd - block.ContentStart;
            if (length == 1)
            {
                if (data[block.ContentStart] != 0)
                {
                    data[block.ContentStart] = 0;
                    changed = true;
                }
            }
            else if (length == 4)
            {
                if (BitConverter.ToInt32(data, block.ContentStart) != 0)
                {
                    Array.Clear(data, block.ContentStart, 4);
                    changed = true;
                }
            }
        }

        return changed;
    }

    private static bool TagEquals(byte[] data, int offset, string tag)
    {
        if (offset < 0 || offset + 4 > data.Length || tag.Length != 4)
            return false;

        return data[offset] == (byte)tag[0]
            && data[offset + 1] == (byte)tag[1]
            && data[offset + 2] == (byte)tag[2]
            && data[offset + 3] == (byte)tag[3];
    }

    private static void WriteInt32(byte[] data, int offset, int value)
    {
        var bytes = BitConverter.GetBytes(value);
        Buffer.BlockCopy(bytes, 0, data, offset, 4);
    }

    private static bool ContainsAscii(byte[] data, int start, int endExclusive, string text)
    {
        var pattern = Encoding.ASCII.GetBytes(text);
        if (pattern.Length == 0 || endExclusive - start < pattern.Length)
            return false;

        return IndexOf(data, pattern, start, endExclusive - pattern.Length) >= 0;
    }

    private static void WriteAllBytesIfDifferent(string path, byte[] bytes)
    {
        if (File.Exists(path))
        {
            try
            {
                var existing = File.ReadAllBytes(path);
                if (existing.AsSpan().SequenceEqual(bytes))
                    return;
            }
            catch
            {
                // Fall through and rewrite.
            }
        }

        File.WriteAllBytes(path, bytes);
    }

    private static int IndexOf(byte[] data, byte[] pattern, int start, int maxStartInclusive)
    {
        if (pattern.Length == 0 || data.Length < pattern.Length)
            return -1;

        var last = Math.Min(maxStartInclusive, data.Length - pattern.Length);
        for (var i = Math.Max(0, start); i <= last; i++)
        {
            var matched = true;
            for (var j = 0; j < pattern.Length; j++)
            {
                if (data[i + j] == pattern[j])
                    continue;

                matched = false;
                break;
            }

            if (matched)
                return i;
        }

        return -1;
    }

    private static string SHA1OfBytes(byte[] data)
        => Convert.ToHexString(SHA1.HashData(data)).ToLowerInvariant();

    private static string NormalizeSlashes(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Replace('\\', '/').Trim();

    private readonly record struct BlockRange(int Offset, int ContentStart, int ContentEnd);

    private sealed record SanitiseResult(bool Changed, string SafePath)
    {
        public static SanitiseResult Unchanged { get; } = new(false, string.Empty);
    }
}
