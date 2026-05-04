using Lumina.Data.Files;
using RavaSync.API.Data.Enum;
using RavaSync.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RavaSync.Services.Optimisation;

public sealed record OptimisationAnalysisSnapshot(IReadOnlyList<TextureOptimisationCandidate> TextureCandidates, IReadOnlyList<MeshOptimisationCandidate> MeshCandidates);

public enum OptimisationTier
{
    None = 0,
    Observe,
    Conservative,
    Aggressive,
}

public sealed class OptimisationPolicyService
{
    internal OptimisationAnalysisSnapshot Analyze(Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? analysis)
    {
        if (analysis == null || analysis.Count == 0)
            return new OptimisationAnalysisSnapshot(Array.Empty<TextureOptimisationCandidate>(), Array.Empty<MeshOptimisationCandidate>());

        var textures = new List<TextureOptimisationCandidate>();
        var meshes = new List<MeshOptimisationCandidate>();

        foreach (var objectEntry in analysis)
        {
            foreach (var fileEntry in objectEntry.Value.Values)
            {
                if (string.Equals(fileEntry.FileType, "tex", StringComparison.OrdinalIgnoreCase))
                {
                    var candidate = BuildTextureCandidate(objectEntry.Key, fileEntry);
                    if (candidate != null) textures.Add(candidate);
                }
                else if (string.Equals(fileEntry.FileType, "mdl", StringComparison.OrdinalIgnoreCase))
                {
                    var candidate = BuildMeshCandidate(objectEntry.Key, fileEntry);
                    if (candidate != null) meshes.Add(candidate);
                }
            }
        }

        return new OptimisationAnalysisSnapshot(
            textures
                .OrderByDescending(t => t.Tier)
                .ThenByDescending(t => t.VramBytes)
                .ThenByDescending(t => t.OriginalSize)
                .ToList(),
            meshes
                .OrderByDescending(m => m.Tier)
                .ThenByDescending(m => m.Triangles)
                .ThenByDescending(m => m.VramBytes)
                .ToList());
    }

    private static TextureOptimisationCandidate? BuildTextureCandidate(ObjectKind objectKind, CharacterAnalyzer.FileDataEntry fileEntry)
    {
        if (!TextureCompressionPlanner.TryParseFormat(fileEntry.Format.Value, out var parsed))
            return null;

        if (!TextureCompressionPlanner.TryChooseTarget(fileEntry.GamePaths.FirstOrDefault(), parsed, out var target))
            return null;

        if (target == TextureCompressionPlanner.Target.None)
            return null;

        if ((parsed == TexFile.TextureFormat.BC7 && target == TextureCompressionPlanner.Target.BC7)
            || (parsed == TexFile.TextureFormat.BC3 && target == TextureCompressionPlanner.Target.BC3)
            || (parsed == TexFile.TextureFormat.BC1 && target == TextureCompressionPlanner.Target.BC1))
            return null;

        var profile = TextureContentProfile.Analyze(fileEntry.GamePaths.FirstOrDefault() ?? fileEntry.FilePaths.FirstOrDefault());
        var biggestDimensionBias = fileEntry.VramBytes >= 16L * 1024 * 1024 || fileEntry.OriginalSize >= 8L * 1024 * 1024;
        var tier = (profile.PreserveHighDetail || biggestDimensionBias) ? OptimisationTier.Conservative : OptimisationTier.Observe;

        string rationale = "Worth a look for texture cleanup.";

        return new TextureOptimisationCandidate(
            objectKind,
            fileEntry.Hash,
            fileEntry.GamePaths,
            fileEntry.FilePaths,
            fileEntry.Format.Value,
            target.ToString(),
            fileEntry.OriginalSize,
            fileEntry.CompressedSize,
            fileEntry.VramBytes,
            tier,
            rationale);
    }

    private static MeshOptimisationCandidate? BuildMeshCandidate(ObjectKind objectKind, CharacterAnalyzer.FileDataEntry fileEntry)
    {
        if (fileEntry.FilePaths is null || fileEntry.FilePaths.Count == 0)
            return null;

        string primaryPath = fileEntry.GamePaths.FirstOrDefault() ?? fileEntry.FilePaths.FirstOrDefault() ?? string.Empty;
        if (!IsSupportedMeshCleanupPath(primaryPath))
            return null;

        var tier = fileEntry.Triangles >= 350_000 || fileEntry.VramBytes >= 24L * 1024 * 1024
            ? OptimisationTier.Aggressive
            : fileEntry.Triangles >= 180_000 || fileEntry.VramBytes >= 12L * 1024 * 1024
                ? OptimisationTier.Conservative
                : OptimisationTier.Observe;

        string rationale = "Worth a look for mesh cleanup.";

        return new MeshOptimisationCandidate(
            objectKind,
            fileEntry.Hash,
            fileEntry.GamePaths,
            fileEntry.FilePaths,
            fileEntry.Triangles,
            fileEntry.OriginalSize,
            fileEntry.CompressedSize,
            fileEntry.VramBytes,
            tier,
            rationale);
    }

    private static bool IsSupportedMeshCleanupPath(string primaryPath)
    {
        if (string.IsNullOrWhiteSpace(primaryPath))
            return false;

        string normalized = primaryPath.Replace('\\', '/').ToLowerInvariant();
        if (!normalized.EndsWith(".mdl", StringComparison.Ordinal))
            return false;

        bool looksLikeEquipment = normalized.Contains("/equipment/") || normalized.Contains("/accessory/");
        if (!looksLikeEquipment)
            return false;

        if (normalized.Contains("/hair/")
            || normalized.Contains("/face/")
            || normalized.Contains("/tail/")
            || normalized.Contains("/zear/"))
        {
            return false;
        }

        return true;
    }
}
