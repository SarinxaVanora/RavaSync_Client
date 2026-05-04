using FFXIVClientStructs.Havok.Common.Base.Math.QsTransform;
using System.Collections.ObjectModel;
using System.Text.RegularExpressions;

namespace RavaSync.Services;

public static partial class XivSkeletonIdentity
{
    [GeneratedRegex(@"^c\d{4}$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex HumanSkeletonCodeRegex();

    [GeneratedRegex(@"^skl_c\d{4}b\d{4}$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex HumanBaseSkeletonRegex();

    [GeneratedRegex(@"^c\d{4}(?:f\d{4})?_\d+:mdl:(?<partial>[a-z0-9_]+)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex HumanModelPartialRegex();

    public static string NormalizeSkeletonKey(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.Replace('\\', '/').Trim();
        var slash = normalized.LastIndexOf('/');
        if (slash >= 0 && slash < normalized.Length - 1)
            normalized = normalized[(slash + 1)..];

        if (normalized.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase))
            normalized = normalized[..^5];

        var partialMatch = HumanModelPartialRegex().Match(normalized);
        if (partialMatch.Success)
        {
            var partialName = partialMatch.Groups["partial"].Value;
            if (!string.IsNullOrWhiteSpace(partialName))
                normalized = partialName;
        }

        return normalized.ToLowerInvariant();
    }

    public static string NormalizeHumanAnimationFamilyKey(string? value)
    {
        var normalized = NormalizeSkeletonKey(value);
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        if (HumanSkeletonCodeRegex().IsMatch(normalized))
            return "human-base";

        if (HumanBaseSkeletonRegex().IsMatch(normalized))
            return "human-base";

        if (string.Equals(normalized, "n_root", StringComparison.Ordinal))
            return "human-partial:n_root";

        if (normalized.StartsWith("j_", StringComparison.Ordinal))
            return "human-partial:" + normalized;

        return string.Empty;
    }

    public static bool IsHumanAnimationSkeleton(string? value)
        => !string.IsNullOrWhiteSpace(NormalizeHumanAnimationFamilyKey(value));

    public static bool IsHumanPlayerAnimationPapGamePath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var normalized = gamePath.Trim().Replace('\\', '/').ToLowerInvariant();
        return normalized.EndsWith(".pap", StringComparison.Ordinal)
            && normalized.StartsWith("chara/human/", StringComparison.Ordinal)
            && normalized.Contains("/animation/", StringComparison.Ordinal)
            && !normalized.Contains("/mt_", StringComparison.Ordinal)
            && !normalized.Contains("/ot_", StringComparison.Ordinal);
    }
}

public sealed class TargetSkeletonSnapshot
{
    public TargetSkeletonSnapshot(
        string resourcePath,
        string skeletonName,
        IReadOnlyList<string?> boneNamesByIndex,
        IReadOnlyDictionary<string, short> boneNameToIndex,
        IReadOnlyList<hkQsTransformf>? referencePoseByIndex = null)
    {
        ResourcePath = resourcePath ?? string.Empty;
        SkeletonName = skeletonName ?? string.Empty;
        BoneNamesByIndex = boneNamesByIndex ?? Array.Empty<string?>();
        BoneNameToIndex = boneNameToIndex ?? new ReadOnlyDictionary<string, short>(new Dictionary<string, short>(StringComparer.OrdinalIgnoreCase));
        ReferencePoseByIndex = referencePoseByIndex ?? Array.Empty<hkQsTransformf>();
        NormalizedSkeletonName = XivSkeletonIdentity.NormalizeSkeletonKey(SkeletonName);
        NormalizedResourceName = XivSkeletonIdentity.NormalizeSkeletonKey(ResourcePath);
        HumanAnimationFamilyKey = string.IsNullOrWhiteSpace(XivSkeletonIdentity.NormalizeHumanAnimationFamilyKey(SkeletonName))
            ? XivSkeletonIdentity.NormalizeHumanAnimationFamilyKey(ResourcePath)
            : XivSkeletonIdentity.NormalizeHumanAnimationFamilyKey(SkeletonName);
    }

    public string ResourcePath { get; }
    public string SkeletonName { get; }
    public string NormalizedSkeletonName { get; }
    public string NormalizedResourceName { get; }
    public string HumanAnimationFamilyKey { get; }
    public bool IsHumanAnimationSkeleton => !string.IsNullOrWhiteSpace(HumanAnimationFamilyKey);
    public IReadOnlyList<string?> BoneNamesByIndex { get; }
    public IReadOnlyDictionary<string, short> BoneNameToIndex { get; }
    public IReadOnlyList<hkQsTransformf> ReferencePoseByIndex { get; }
    public int BoneCount => BoneNamesByIndex.Count;
    public short MaxBoneIndex => BoneCount <= 0 ? (short)-1 : (short)(BoneCount - 1);
}

public enum PapRewriteStatus
{
    OriginalSafe = 0,
    Sanitized = 1,
    Blocked = 2,
    OriginalFallback = 3,
}

public sealed record PapRewriteResult(
    PapRewriteStatus Status,
    string OriginalHash,
    string EffectiveHash,
    string EffectivePath,
    int RemappedTrackCount,
    int DroppedTrackCount,
    int BindingCount,
    string Reason)
{
    public static PapRewriteResult Original(string originalHash, string effectivePath, int bindingCount, string reason)
        => new(PapRewriteStatus.OriginalSafe, originalHash, originalHash, effectivePath, 0, 0, bindingCount, reason);

    public static PapRewriteResult Sanitised(string originalHash, string effectiveHash, string effectivePath, int remappedTrackCount, int droppedTrackCount, int bindingCount, string reason)
        => new(PapRewriteStatus.Sanitized, originalHash, effectiveHash, effectivePath, remappedTrackCount, droppedTrackCount, bindingCount, reason);

    public static PapRewriteResult Blocked(string originalHash, string reason)
        => new(PapRewriteStatus.Blocked, originalHash, string.Empty, string.Empty, 0, 0, 0, reason);

    public static PapRewriteResult OriginalFallback(string originalHash, string effectivePath, int bindingCount, string reason)
        => new(PapRewriteStatus.OriginalFallback, originalHash, originalHash, effectivePath, 0, 0, bindingCount, reason);
}
