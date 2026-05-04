using Lumina.Data.Files;
using Microsoft.Extensions.Logging;
using Penumbra.Api.Enums;
using RavaSync.API.Data.Enum;
using RavaSync.Interop.Ipc;
using RavaSync.Services.Gpu;
using RavaSync.Utils;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using MdlFile = RavaSync.Interop.GameModel.MdlFile;

namespace RavaSync.Services.Optimisation;

public sealed record TextureOptimisationCandidate(ObjectKind ObjectKind,string Hash,IReadOnlyList<string> GamePaths,IReadOnlyList<string> FilePaths,string SourceFormat,string SuggestedTarget,
    long OriginalSize,long CompressedSize,long VramBytes,OptimisationTier Tier,string Rationale);


public sealed class TextureOptimisationService
{
    private readonly ILogger<TextureOptimisationService> _logger;
    private readonly GpuDeviceService _gpuDeviceService;
    private readonly D3D11ComputeService _d3d11ComputeService;
    private readonly D3D11TextureCompressionService _d3d11TextureCompressionService;
    private readonly IpcManager _ipcManager;
    private static readonly object UvCoverageCacheLock = new();
    private static readonly Dictionary<string, CachedUvCoverageEntry> UvCoverageCache = new(StringComparer.Ordinal);


    public TextureOptimisationService(ILogger<TextureOptimisationService> logger, GpuDeviceService gpuDeviceService, D3D11ComputeService d3d11ComputeService, D3D11TextureCompressionService d3d11TextureCompressionService, IpcManager ipcManager)
    {
        _logger = logger;
        _gpuDeviceService = gpuDeviceService;
        _d3d11ComputeService = d3d11ComputeService;
        _d3d11TextureCompressionService = d3d11TextureCompressionService;
        _ipcManager = ipcManager;
    }

    public async Task<bool> WarmupAsync(CancellationToken token)
    {
        using var job = _gpuDeviceService.TryBeginJob("TextureOptimization.Warmup", token, out var reason);
        if (job == null)
        {
            _logger.LogDebug("Skipping texture optimisation warmup: {reason}", reason);
            return false;
        }

        bool resizeReady = await _d3d11ComputeService.WarmupAsync(token).ConfigureAwait(false);
        token.ThrowIfCancellationRequested();
        await Task.Yield();
        bool compressionReady = await _d3d11TextureCompressionService.WarmupAsync(token).ConfigureAwait(false);
        if (!resizeReady || !compressionReady)
        {
            job.CompleteFailure($"D3D11 texture backend warmup failed. ResizeReady={resizeReady}, CompressionReady={compressionReady}");
            return false;
        }

        job.CompleteSuccess("D3D11 texture resize and compression backends are ready.");
        return true;
    }

    public async Task RunPlannedOptimizationAsync(ILogger callerLogger, Dictionary<string, string[]> textures, IReadOnlyDictionary<string, string>? targetsByPrimary, IProgress<(string fileName, int index)> progress, CancellationToken token, IReadOnlyDictionary<string, string[]>? relatedModelsByTexturePrimary = null)
    {
        ArgumentNullException.ThrowIfNull(callerLogger);
        ArgumentNullException.ThrowIfNull(textures);
        ArgumentNullException.ThrowIfNull(progress);

        if (textures.Count == 0)
        {
            callerLogger.LogDebug("Texture optimisation requested with an empty texture set.");
            return;
        }

        var workItems = BuildWorkItems(textures, targetsByPrimary, relatedModelsByTexturePrimary, callerLogger);
        if (workItems.Count == 0)
        {
            callerLogger.LogDebug("Texture optimisation produced no valid work items.");
            return;
        }

        using var orchestrationJob = _gpuDeviceService.TryBeginJob("TextureOptimization.AuthoritativeDriver", token, out var gpuReason);

        int currentIndex = 0;
        int directGpuCompleted = 0;
        int penumbraFallbacks = 0;
        var fallbackPlan = new Dictionary<TextureType, Dictionary<string, string[]>>();

        foreach (var item in workItems)
        {
            token.ThrowIfCancellationRequested();
            progress.Report((Path.GetFileName(item.PrimaryPath), ++currentIndex));

            string temporaryOutput = CreateTemporaryPath(item.PrimaryPath, item.TargetName);
            try
            {
                string compressionSource = item.PrimaryPath;
                string? temporaryResizeSource = null;
                try
                {
                    var prepared = await TryPrepareD3DSourceAsync(item, token).ConfigureAwait(false);
                    compressionSource = prepared.PreparedPath;
                    callerLogger.LogDebug("Texture optimisation prepare step for {primary}: {detail}", item.PrimaryPath, prepared.Detail);
                    if (prepared.PreparedPathKind == PreparedPathKind.GpuTemporary)
                        temporaryResizeSource = prepared.PreparedPath;

                    var profile = TextureContentProfile.Analyze(item.PrimaryPath);
                    bool gpuCompressed = await _d3d11TextureCompressionService.TryCompressTextureAsync(compressionSource, temporaryOutput, item.TargetTexFormat, profile.IsStrictNormalMap, token).ConfigureAwait(false);
                    if (!gpuCompressed)
                    {
                        callerLogger.LogInformation("Texture optimisation falling back to Penumbra for {primary}. Target={target}.", item.PrimaryPath, item.TargetName);
                        AddToPlan(fallbackPlan, item.TargetTextureType, item.PrimaryPath, item.AlternatePaths.ToArray());
                        penumbraFallbacks++;
                        continue;
                    }

                    ReplacePrimaryAndAlternates(temporaryOutput, item.PrimaryPath, item.AlternatePaths);
                    directGpuCompleted++;
                    callerLogger.LogInformation("Texture optimisation completed locally through D3D11 for {primary}. Target={target}.", item.PrimaryPath, item.TargetName);
                }
                finally
                {
                    if (!string.IsNullOrWhiteSpace(temporaryResizeSource))
                        TryDeleteQuietly(callerLogger, temporaryResizeSource);
                }
            }
            catch (Exception ex)
            {
                callerLogger.LogWarning(ex, "Texture optimisation failed locally for {primary}. Target={target}. Routing to Penumbra.", item.PrimaryPath, item.TargetName);
                AddToPlan(fallbackPlan, item.TargetTextureType, item.PrimaryPath, item.AlternatePaths.ToArray());
                penumbraFallbacks++;
            }
            finally
            {
                TryDeleteQuietly(callerLogger, temporaryOutput);
            }

        }

        if (fallbackPlan.Count > 0)
        {
            await _ipcManager.Penumbra.ConvertTextureFiles(callerLogger, fallbackPlan, progress, token).ConfigureAwait(false);
        }
        else if (directGpuCompleted > 0)
        {
            await _ipcManager.Penumbra.FinalizeTextureWriteAsync(nameof(TextureOptimisationService)).ConfigureAwait(false);
        }

        orchestrationJob?.CompleteSuccess($"Texture optimisation completed. DirectGpu={directGpuCompleted}, PenumbraFallbacks={penumbraFallbacks}, Total={workItems.Count}, GPUPath={(orchestrationJob != null ? "available" : $"unavailable ({gpuReason})")}");
    }

    private List<TextureWorkItem> BuildWorkItems(IReadOnlyDictionary<string, string[]> textures, IReadOnlyDictionary<string, string>? targetsByPrimary, IReadOnlyDictionary<string, string[]>? relatedModelsByTexturePrimary, ILogger callerLogger)
    {
        var result = new List<TextureWorkItem>(textures.Count);
        foreach (var kv in textures)
        {
            var primary = kv.Key;
            if (string.IsNullOrWhiteSpace(primary) || !File.Exists(primary))
                continue;

            string targetName = ResolveTarget(targetsByPrimary, primary, callerLogger);
            if (!TryMapTarget(targetName, out var textureType, out var texFormat))
                continue;

            result.Add(new TextureWorkItem(primary, kv.Value ?? Array.Empty<string>(), targetName, textureType, texFormat, relatedModelsByTexturePrimary != null && relatedModelsByTexturePrimary.TryGetValue(primary, out var models) ? models : Array.Empty<string>()));
        }

        return result;
    }

    private async Task<PreparedTexturePath> TryPrepareD3DSourceAsync(TextureWorkItem item, CancellationToken token)
    {
        if (!NativeTexCodec.TryLoadRgba32(item.PrimaryPath, out var sourceImage, out var sourceFormat, out var loadReason) || sourceImage == null)
            return PreparedTexturePath.Direct(item.PrimaryPath, item.AlternatePaths.ToArray(), $"Source was not suitable for local D3D staging; using original TEX as the local compression source. Reason={loadReason}");

        try
        {
            using var sourceScope = sourceImage;
            var profile = TextureContentProfile.Analyze(item.PrimaryPath);
            var workingImage = sourceImage.Clone();
            bool imageChanged = false;
            string cleanupReason = string.Empty;
            try
            {
                if (TryCleanTextureForCompression(item, workingImage, profile, out cleanupReason))
                    imageChanged = true;
            }
            catch (Exception ex)
            {
                cleanupReason = $"Cleanup skipped after exception: {ex.Message}";
            }

            int maxDimension = DetermineMaxDimension(item.PrimaryPath, item.TargetName, workingImage.Width, workingImage.Height);
            bool needsResize = maxDimension > 0 && (workingImage.Width > maxDimension || workingImage.Height > maxDimension);
            Image<Rgba32>? finalImage = workingImage;
            bool ownsFinalImage = true;

            if (needsResize)
            {
                var targetSize = ComputeScaledSize(workingImage.Width, workingImage.Height, maxDimension);
                Image<Rgba32>? gpuResized = profile.IsStrictNormalMap
                    ? await _d3d11ComputeService.TryResizeNormalMapAsync(workingImage, targetSize.Width, targetSize.Height, token).ConfigureAwait(false)
                    : await _d3d11ComputeService.TryResizeAsync(workingImage, targetSize.Width, targetSize.Height, token).ConfigureAwait(false);
                if (gpuResized == null)
                {
                    workingImage.Dispose();
                    return PreparedTexturePath.Direct(item.PrimaryPath, item.AlternatePaths.ToArray(), "D3D11 resize backend was unavailable; using original TEX as the local compression source.");
                }

                finalImage = gpuResized;
                workingImage.Dispose();
                imageChanged = true;
                cleanupReason = string.IsNullOrWhiteSpace(cleanupReason)
                    ? $"D3D prepared {sourceImage.Width}x{sourceImage.Height} -> {gpuResized.Width}x{gpuResized.Height} for final {item.TargetName} conversion."
                    : cleanupReason + $" D3D resized to {gpuResized.Width}x{gpuResized.Height}.";
            }

            if (!imageChanged)
            {
                finalImage?.Dispose();
                return PreparedTexturePath.Direct(item.PrimaryPath, item.AlternatePaths.ToArray(), "No D3D cleanup or resize pass was required; using original TEX as the local compression source.");
            }

            string tempPath = CreateTemporaryPath(item.PrimaryPath, item.TargetName);
            if (!NativeTexCodec.TrySaveUncompressed(tempPath, finalImage!, NormalizeUncompressedFormat(sourceFormat), out var saveReason))
            {
                finalImage?.Dispose();
                return PreparedTexturePath.Direct(item.PrimaryPath, item.AlternatePaths.ToArray(), $"Failed to persist D3D-preprocessed TEX; using original TEX as the local compression source. Reason={saveReason}");
            }

            if (ownsFinalImage)
                finalImage?.Dispose();

            var duplicateTargets = new string[item.AlternatePaths.Count + 1];
            duplicateTargets[0] = item.PrimaryPath;
            for (int i = 0; i < item.AlternatePaths.Count; i++)
                duplicateTargets[i + 1] = item.AlternatePaths[i];

            return PreparedTexturePath.GpuTemporary(tempPath, duplicateTargets, string.IsNullOrWhiteSpace(cleanupReason) ? $"D3D prepared source before final {item.TargetName} conversion." : cleanupReason);
        }
        catch (Exception ex)
        {
            return PreparedTexturePath.Direct(item.PrimaryPath, item.AlternatePaths.ToArray(), $"Local D3D staging threw an exception; using original TEX as the local compression source. Reason={ex.Message}");
        }
    }

    private static bool TryCleanTextureForCompression(TextureWorkItem item, Image<Rgba32> image, TextureContentProfile profile, out string reason)
    {
        reason = string.Empty;
        bool changed = false;

        if (!profile.IsStrictNormalMap && !profile.IsMaterialMask)
            changed |= ScrubTransparentPixels(image, profile);

        if (CanApplyUvAwareCleanup(profile) && item.RelatedModelPaths.Count > 0 && TryBuildBestUvCoverage(item.RelatedModelPaths, out var coverage, out var hadWrappedModels))
        {
            if (coverage.SampledTriangles > 0 && coverage.Occupancy < 0.97f)
            {
                int pad = Math.Max(12, Math.Min(image.Width, image.Height) / 48);
                if (TryApplyUvDeadSpaceCleanup(image, profile, coverage, pad))
                {
                    changed = true;
                    reason = $"UV-aware cleanup removed unsampled texture space. Coverage={coverage.Occupancy:P0}, Triangles={coverage.SampledTriangles}.";
                }
            }
            else if (hadWrappedModels)
            {
                reason = "UV-aware cleanup stayed conservative because some related meshes use out-of-range or tiled UVs.";
            }
        }
        else if (CanApplyUvAwareCleanup(profile) && item.RelatedModelPaths.Count > 0)
        {
            reason = "UV-aware cleanup could not build a safe coverage map from the related meshes.";
        }
        else if (!CanApplyUvAwareCleanup(profile) && item.RelatedModelPaths.Count > 0)
        {
            reason = "UV-aware cleanup skipped for UI or overlay-sensitive textures.";
        }

        return changed;
    }

    private static bool ScrubTransparentPixels(Image<Rgba32> image, TextureContentProfile profile)
    {
        bool changed = false;
        byte neutralR = profile.IsStrictNormalMap ? (byte)128 : (byte)0;
        byte neutralG = profile.IsStrictNormalMap ? (byte)128 : (byte)0;
        byte neutralB = profile.IsStrictNormalMap ? (byte)255 : (byte)0;
        byte neutralA = profile.IsDefinitelyOpaqueDiffuse ? (byte)255 : (byte)0;

        for (int y = 0; y < image.Height; y++)
        {
            for (int x = 0; x < image.Width; x++)
            {
                var px = image[x, y];
                if (profile.IsDefinitelyOpaqueDiffuse && px.A != 255)
                {
                    px.A = 255;
                    changed = true;
                }

                if (px.A <= 2)
                {
                    if (px.R != neutralR || px.G != neutralG || px.B != neutralB || px.A != neutralA)
                    {
                        px = new Rgba32(neutralR, neutralG, neutralB, neutralA);
                        changed = true;
                    }
                }

                image[x, y] = px;
            }
        }

        return changed;
    }

    internal static bool TryEstimateCleanupSavings(IReadOnlyList<string>? modelPaths, string? texturePath, long sourceSize, out long estimatedSavedBytes, out float occupancy, out int sampledTriangles)
    {
        estimatedSavedBytes = 0;
        occupancy = 1f;
        sampledTriangles = 0;

        if (sourceSize <= 0 || modelPaths == null || modelPaths.Count == 0)
            return false;

        var profile = TextureContentProfile.Analyze(texturePath);
        if (!CanApplyUvAwareCleanup(profile))
            return false;

        if (!TryGetCachedBestUvCoverage(modelPaths, out var coverage))
            return false;

        occupancy = coverage.Occupancy;
        sampledTriangles = coverage.SampledTriangles;
        if (sampledTriangles <= 0)
            return false;

        var deadSpace = Math.Clamp(1f - occupancy, 0f, 0.95f);
        if (deadSpace < 0.03f)
            return false;

        float reclaimFactor = profile.IsDefinitelyOpaqueDiffuse ? 0.55f : 0.35f;
        if (profile.IsStrictNormalMap || profile.IsMaterialMask)
            reclaimFactor = 0.28f;
        else if (profile.IsAlphaSensitive)
            reclaimFactor = 0.22f;

        estimatedSavedBytes = (long)Math.Round(sourceSize * deadSpace * reclaimFactor, MidpointRounding.AwayFromZero);
        return estimatedSavedBytes > 0;
    }

    private static bool TryGetCachedBestUvCoverage(IReadOnlyList<string> modelPaths, out UvCoverage coverage)
    {
        coverage = default;

        var normalized = modelPaths
            .Where(static p => !string.IsNullOrWhiteSpace(p) && File.Exists(p))
            .Select(static p => p.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static p => p, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (normalized.Length == 0)
            return false;

        string key = BuildUvCoverageCacheKey(normalized);
        lock (UvCoverageCacheLock)
        {
            if (UvCoverageCache.TryGetValue(key, out var cached) && cached.IsStillValidFor(normalized))
            {
                coverage = cached.Coverage;
                return true;
            }
        }

        if (!TryBuildBestUvCoverage(normalized, out coverage, out _))
            return false;

        lock (UvCoverageCacheLock)
        {
            UvCoverageCache[key] = CachedUvCoverageEntry.Create(normalized, coverage);
        }

        return true;
    }

    private static string BuildUvCoverageCacheKey(IReadOnlyList<string> modelPaths)
    {
        return string.Join("|", modelPaths.Select(static p => p.ToLowerInvariant()));
    }

    private static bool CanApplyUvAwareCleanup(TextureContentProfile profile)
    {
        return !profile.IsUiOrOverlaySensitive
            && !profile.IsStrictNormalMap
            && !profile.IsMaterialMask;
    }

    private static bool TryApplyUvDeadSpaceCleanup(Image<Rgba32> image, TextureContentProfile profile, UvCoverage coverage, int pixelPadding)
    {
        var replacement = profile.IsStrictNormalMap
            ? new Rgba32(128, 128, 255, 255)
            : profile.IsMaterialMask
                ? new Rgba32(0, 0, 0, 0)
                : profile.IsDefinitelyOpaqueDiffuse
                    ? new Rgba32(0, 0, 0, 255)
                    : new Rgba32(0, 0, 0, 0);

        int gridSize = coverage.GridSize;
        float pixelsPerCell = Math.Max(1f, Math.Min(image.Width, image.Height) / (float)gridSize);
        int dilation = Math.Max(1, (int)Math.Ceiling(pixelPadding / pixelsPerCell));
        var expanded = ExpandCoverage(coverage.UsedCells, gridSize, dilation);
        bool changed = false;
        int keptPixels = 0;
        int totalPixels = image.Width * image.Height;

        for (int y = 0; y < image.Height; y++)
        {
            int gy = Math.Clamp((int)((y + 0.5f) * gridSize / image.Height), 0, gridSize - 1);
            for (int x = 0; x < image.Width; x++)
            {
                int gx = Math.Clamp((int)((x + 0.5f) * gridSize / image.Width), 0, gridSize - 1);
                if (expanded[gx, gy])
                {
                    keptPixels++;
                    continue;
                }

                if (!image[x, y].Equals(replacement))
                {
                    image[x, y] = replacement;
                    changed = true;
                }
            }
        }

        return changed && keptPixels < totalPixels;
    }

    private static bool TryBuildBestUvCoverage(IReadOnlyList<string> modelPaths, out UvCoverage coverage, out bool hadWrappedModels)
    {
        coverage = default;
        hadWrappedModels = false;

        if (modelPaths == null || modelPaths.Count == 0)
            return false;

        if (TryBuildUvCoverage(modelPaths, out var combined) && !combined.WrapDetected)
        {
            coverage = combined;
            return true;
        }

        const int gridSize = 256;
        var merged = new bool[gridSize, gridSize];
        float minU = 1f, minV = 1f, maxU = 0f, maxV = 0f;
        int sampledTriangles = 0;
        bool anyCoverage = false;

        foreach (var modelPath in modelPaths)
        {
            if (!TryBuildUvCoverage(new[] { modelPath }, out var single))
                continue;

            if (single.WrapDetected)
            {
                hadWrappedModels = true;
                continue;
            }

            anyCoverage = true;
            sampledTriangles += single.SampledTriangles;
            minU = Math.Min(minU, single.Bounds.MinX);
            minV = Math.Min(minV, single.Bounds.MinY);
            maxU = Math.Max(maxU, single.Bounds.MaxX);
            maxV = Math.Max(maxV, single.Bounds.MaxY);

            for (int y = 0; y < gridSize; y++)
                for (int x = 0; x < gridSize; x++)
                    merged[x, y] |= single.UsedCells[x, y];
        }

        if (!anyCoverage || sampledTriangles <= 0)
            return false;

        int usedCells = 0;
        for (int y = 0; y < gridSize; y++)
            for (int x = 0; x < gridSize; x++)
                if (merged[x, y])
                    usedCells++;

        coverage = new UvCoverage(
            new UvBounds(Clamp01(minU), Clamp01(minV), Clamp01(maxU), Clamp01(maxV)),
            usedCells / (float)(gridSize * gridSize),
            hadWrappedModels,
            sampledTriangles,
            gridSize,
            merged);
        return coverage.Bounds.MaxX > coverage.Bounds.MinX && coverage.Bounds.MaxY > coverage.Bounds.MinY;
    }

    private static bool TryBuildUvCoverage(IReadOnlyList<string> modelPaths, out UvCoverage coverage)
    {
        const int gridSize = 256;
        var used = new bool[gridSize, gridSize];
        float minU = 1f, minV = 1f, maxU = 0f, maxV = 0f;
        bool wrapDetected = false;
        int sampledTriangles = 0;

        coverage = default;

        foreach (var modelPath in modelPaths)
        {
            if (string.IsNullOrWhiteSpace(modelPath) || !File.Exists(modelPath))
                continue;

            try
            {
                var mdl = new MdlFile(modelPath);
                byte[] bytes = File.ReadAllBytes(modelPath);
                int lodIndex = 0;
                if (mdl.Lods == null || mdl.Lods.Length == 0)
                    continue;

                foreach (var mesh in mdl.Meshes)
                {
                    if (!TryGetMeshTexCoords(mdl, lodIndex, mesh, bytes, out var uvs))
                        continue;
                    if (!TryGetMeshIndices(mdl, lodIndex, mesh, bytes, out var indices))
                        continue;

                    for (int i = 0; i + 2 < indices.Count; i += 3)
                    {
                        var uv0 = uvs[indices[i]];
                        var uv1 = uvs[indices[i + 1]];
                        var uv2 = uvs[indices[i + 2]];
                        if (!IsFinite(uv0) || !IsFinite(uv1) || !IsFinite(uv2))
                            continue;

                        if (IsOutsideStandardUv(uv0) || IsOutsideStandardUv(uv1) || IsOutsideStandardUv(uv2))
                        {
                            wrapDetected = true;
                            continue;
                        }

                        minU = Math.Min(minU, Math.Min(uv0.X, Math.Min(uv1.X, uv2.X)));
                        minV = Math.Min(minV, Math.Min(uv0.Y, Math.Min(uv1.Y, uv2.Y)));
                        maxU = Math.Max(maxU, Math.Max(uv0.X, Math.Max(uv1.X, uv2.X)));
                        maxV = Math.Max(maxV, Math.Max(uv0.Y, Math.Max(uv1.Y, uv2.Y)));
                        RasterizeTriangleCoverage(used, gridSize, uv0, uv1, uv2);
                        sampledTriangles++;
                    }
                }
            }
            catch
            {
            }
        }

        if (sampledTriangles == 0)
            return false;

        int usedCells = 0;
        for (int y = 0; y < gridSize; y++)
            for (int x = 0; x < gridSize; x++)
                if (used[x, y])
                    usedCells++;

        coverage = new UvCoverage(
            new UvBounds(Clamp01(minU), Clamp01(minV), Clamp01(maxU), Clamp01(maxV)),
            usedCells / (float)(gridSize * gridSize),
            wrapDetected,
            sampledTriangles,
            gridSize,
            used);
        return coverage.Bounds.MaxX > coverage.Bounds.MinX && coverage.Bounds.MaxY > coverage.Bounds.MinY;
    }

    private static void RasterizeTriangleCoverage(bool[,] used, int gridSize, Vector2 uv0, Vector2 uv1, Vector2 uv2)
    {
        int minX = Math.Max(0, (int)Math.Floor(Math.Min(uv0.X, Math.Min(uv1.X, uv2.X)) * gridSize));
        int minY = Math.Max(0, (int)Math.Floor(Math.Min(uv0.Y, Math.Min(uv1.Y, uv2.Y)) * gridSize));
        int maxX = Math.Min(gridSize - 1, (int)Math.Ceiling(Math.Max(uv0.X, Math.Max(uv1.X, uv2.X)) * gridSize));
        int maxY = Math.Min(gridSize - 1, (int)Math.Ceiling(Math.Max(uv0.Y, Math.Max(uv1.Y, uv2.Y)) * gridSize));
        if (minX > maxX || minY > maxY)
            return;

        float area = Edge(uv0, uv1, uv2);
        if (Math.Abs(area) < 1e-8f)
            return;

        for (int y = minY; y <= maxY; y++)
        {
            float py = (y + 0.5f) / gridSize;
            for (int x = minX; x <= maxX; x++)
            {
                float px = (x + 0.5f) / gridSize;
                var p = new Vector2(px, py);
                float w0 = Edge(uv1, uv2, p);
                float w1 = Edge(uv2, uv0, p);
                float w2 = Edge(uv0, uv1, p);
                if ((w0 >= 0f && w1 >= 0f && w2 >= 0f) || (w0 <= 0f && w1 <= 0f && w2 <= 0f))
                    used[x, y] = true;
            }
        }
    }

    private static float Edge(Vector2 a, Vector2 b, Vector2 c)
        => (c.X - a.X) * (b.Y - a.Y) - (c.Y - a.Y) * (b.X - a.X);

    private static bool[,] ExpandCoverage(bool[,] used, int gridSize, int radius)
    {
        if (radius <= 0)
            return used;

        var expanded = new bool[gridSize, gridSize];
        for (int y = 0; y < gridSize; y++)
        {
            for (int x = 0; x < gridSize; x++)
            {
                if (!used[x, y])
                    continue;

                int minX = Math.Max(0, x - radius);
                int maxX = Math.Min(gridSize - 1, x + radius);
                int minY = Math.Max(0, y - radius);
                int maxY = Math.Min(gridSize - 1, y + radius);
                for (int yy = minY; yy <= maxY; yy++)
                    for (int xx = minX; xx <= maxX; xx++)
                        expanded[xx, yy] = true;
            }
        }

        return expanded;
    }

    private static bool IsOutsideStandardUv(Vector2 uv)
        => uv.X < -0.01f || uv.X > 1.01f || uv.Y < -0.01f || uv.Y > 1.01f;

    private static bool IsFinite(Vector2 uv)
        => !(float.IsNaN(uv.X) || float.IsNaN(uv.Y) || float.IsInfinity(uv.X) || float.IsInfinity(uv.Y));

    private static float Clamp01(float value)
        => Math.Clamp(value, 0f, 1f);

    private static bool TryGetMeshTexCoords(MdlFile mdl, int lodIndex, object mesh, byte[] bytes, out Vector2[] texCoords)
    {
        texCoords = Array.Empty<Vector2>();
        if (!TryReadVertexCount(mesh, out uint vertexCount) || vertexCount == 0 || vertexCount > 200000)
            return false;
        if (!TryReadVertexDataOffset(mesh, out uint vertexDataOffset))
            return false;

        int declIndex = TryReadVertexDeclarationIndex(mesh, out var idx) ? idx : 0;
        if (declIndex < 0 || declIndex >= mdl.VertexDeclarations.Length)
            return false;

        var declaration = mdl.VertexDeclarations[declIndex];
        if (!TryResolveTexCoordElement(declaration, out var stream, out var elementOffset, out var stride, out var formatName))
            return false;
        if (stream >= 3)
            return false;

        long streamBase = mdl.DataSectionOffset + mdl.Lods[lodIndex].VertexDataOffset + mdl.VertexOffset[stream] + vertexDataOffset;
        long bytesRequired = streamBase + (long)vertexCount * stride;
        if (streamBase < 0 || bytesRequired > bytes.LongLength)
            return false;

        texCoords = new Vector2[vertexCount];
        for (int i = 0; i < vertexCount; i++)
        {
            int p = (int)(streamBase + i * stride + elementOffset);
            if (!TryReadTexCoord(bytes, p, formatName, out texCoords[i]))
                return false;
            texCoords[i].Y = 1f - texCoords[i].Y;
        }

        return true;
    }

    private static bool TryGetMeshIndices(MdlFile mdl, int lodIndex, object mesh, byte[] bytes, out List<ushort> indices)
    {
        indices = new List<ushort>();
        if (!TryGetUInt32(ReadMemberValue(mesh, "IndexCount"), out uint indexCount) || indexCount == 0 || indexCount > 900000)
            return false;
        if (!TryReadIndexDataOffset(mesh, out uint indexDataOffset))
            return false;

        long absoluteStart = mdl.DataSectionOffset + mdl.Lods[lodIndex].IndexDataOffset + mdl.IndexOffset[lodIndex] + indexDataOffset;
        long bytesRequired = absoluteStart + (long)indexCount * sizeof(ushort);
        if (absoluteStart < 0 || bytesRequired > bytes.LongLength)
            return false;

        indices = new List<ushort>((int)indexCount);
        for (int i = 0; i < indexCount; i++)
            indices.Add(BitConverter.ToUInt16(bytes, (int)absoluteStart + i * sizeof(ushort)));
        return true;
    }

    private static bool TryReadIndexDataOffset(object mesh, out uint indexDataOffset)
    {
        foreach (var name in new[] { "IndexDataOffset", "IndexOffset", "IndexBufferOffset" })
        {
            if (TryGetUInt32(ReadMemberValue(mesh, name), out indexDataOffset))
                return true;
        }

        indexDataOffset = 0;
        return false;
    }

    private static bool TryResolveTexCoordElement(MdlFile.VertexDeclarationStruct declaration, out int stream, out int elementOffset, out int stride, out string formatName)
    {
        stream = 0;
        elementOffset = 0;
        stride = 0;
        formatName = string.Empty;
        int[] streamStride = new int[4];
        object? texCoordElement = null;
        foreach (var elem in declaration.VertexElements)
        {
            int elemStream = TryReadInt(elem, "Stream") ?? 0;
            int offset = TryReadInt(elem, "Offset") ?? 0;
            string typeName = ReadMemberValue(elem, "Type")?.ToString() ?? string.Empty;
            int size = InferVertexElementSize(typeName);
            if (elemStream >= 0 && elemStream < streamStride.Length)
                streamStride[elemStream] = Math.Max(streamStride[elemStream], offset + size);

            string usage = ReadMemberValue(elem, "Usage")?.ToString() ?? ReadMemberValue(elem, "UsageType")?.ToString() ?? string.Empty;
            if (texCoordElement == null && (usage.Contains("TexCoord", StringComparison.OrdinalIgnoreCase) || usage.Contains("UV", StringComparison.OrdinalIgnoreCase) || usage.Contains("TextureCoordinate", StringComparison.OrdinalIgnoreCase)))
            {
                texCoordElement = elem;
                formatName = typeName;
            }
        }

        if (texCoordElement == null)
            return false;

        stream = TryReadInt(texCoordElement, "Stream") ?? 0;
        elementOffset = TryReadInt(texCoordElement, "Offset") ?? 0;
        if (stream < 0 || stream >= streamStride.Length)
            return false;
        stride = streamStride[stream];
        return stride > elementOffset;
    }

    private static bool TryReadTexCoord(byte[] bytes, int offset, string formatName, out Vector2 value)
    {
        value = default;
        if (offset < 0 || offset + 4 > bytes.Length)
            return false;

        if (formatName.Contains("Float2", StringComparison.OrdinalIgnoreCase))
        {
            if (offset + 8 > bytes.Length)
                return false;
            value = new Vector2(BitConverter.ToSingle(bytes, offset), BitConverter.ToSingle(bytes, offset + 4));
            return true;
        }

        if (formatName.Contains("Half2", StringComparison.OrdinalIgnoreCase))
        {
            if (offset + 4 > bytes.Length)
                return false;
            value = new Vector2(ConvertHalfToSingle(BitConverter.ToUInt16(bytes, offset)), ConvertHalfToSingle(BitConverter.ToUInt16(bytes, offset + 2)));
            return true;
        }

        if (formatName.Contains("Short2", StringComparison.OrdinalIgnoreCase))
        {
            if (offset + 4 > bytes.Length)
                return false;
            value = new Vector2(BitConverter.ToInt16(bytes, offset) / 32767f, BitConverter.ToInt16(bytes, offset + 2) / 32767f);
            return true;
        }

        return false;
    }

    private static float ConvertHalfToSingle(ushort value)
        => (float)(Half)BitConverter.UInt16BitsToHalf(value);

    private static bool TryReadVertexCount(object mesh, out uint vertexCount)
    {
        foreach (var name in new[] { "VertexCount", "NumVertices" })
        {
            if (TryGetUInt32(ReadMemberValue(mesh, name), out vertexCount))
                return true;
        }

        foreach (var member in mesh.GetType().GetMembers(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (!member.Name.Contains("vertex", StringComparison.OrdinalIgnoreCase) || !member.Name.Contains("count", StringComparison.OrdinalIgnoreCase))
                continue;
            if (TryGetUInt32(member switch { PropertyInfo p => p.GetValue(mesh), FieldInfo f => f.GetValue(mesh), _ => null }, out vertexCount))
                return true;
        }

        vertexCount = 0;
        return false;
    }

    private static bool TryReadVertexDataOffset(object mesh, out uint vertexDataOffset)
    {
        foreach (var name in new[] { "VertexDataOffset", "VertexOffset", "VertexBufferOffset", "VertexBufferOffset0" })
        {
            if (TryGetUInt32(ReadMemberValue(mesh, name), out vertexDataOffset))
                return true;
        }

        foreach (var member in mesh.GetType().GetMembers(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (!member.Name.Contains("vertex", StringComparison.OrdinalIgnoreCase) || !member.Name.Contains("offset", StringComparison.OrdinalIgnoreCase))
                continue;
            if (TryGetUInt32(member switch { PropertyInfo p => p.GetValue(mesh), FieldInfo f => f.GetValue(mesh), _ => null }, out vertexDataOffset))
                return true;
        }

        vertexDataOffset = 0;
        return false;
    }

    private static bool TryReadVertexDeclarationIndex(object mesh, out int declarationIndex)
    {
        foreach (var name in new[] { "VertexDeclarationIndex", "VertexDeclIndex", "DeclarationIndex" })
        {
            if (TryGetUInt32(ReadMemberValue(mesh, name), out var value))
            {
                declarationIndex = (int)value;
                return true;
            }
        }

        foreach (var member in mesh.GetType().GetMembers(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (!member.Name.Contains("declaration", StringComparison.OrdinalIgnoreCase) || !member.Name.Contains("index", StringComparison.OrdinalIgnoreCase))
                continue;
            if (TryGetUInt32(member switch { PropertyInfo p => p.GetValue(mesh), FieldInfo f => f.GetValue(mesh), _ => null }, out var value))
            {
                declarationIndex = (int)value;
                return true;
            }
        }

        declarationIndex = 0;
        return false;
    }

    private static object? ReadMemberValue(object target, string name)
    {
        var type = target.GetType();
        var prop = type.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (prop != null)
            return prop.GetValue(target);

        var field = type.GetField(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (field != null)
            return field.GetValue(target);

        return null;
    }

    private static bool TryGetUInt32(object? value, out uint converted)
    {
        switch (value)
        {
            case byte b:
                converted = b;
                return true;
            case ushort us:
                converted = us;
                return true;
            case uint ui:
                converted = ui;
                return true;
            case int i when i >= 0:
                converted = (uint)i;
                return true;
            case long l when l >= 0 && l <= uint.MaxValue:
                converted = (uint)l;
                return true;
            default:
                converted = 0u;
                return false;
        }
    }

    private static int? TryReadInt(object target, string name)
    {
        if (TryGetUInt32(ReadMemberValue(target, name), out var value))
            return (int)value;
        return null;
    }

    private static int InferVertexElementSize(string typeName)
    {
        if (typeName.Contains("Float4", StringComparison.OrdinalIgnoreCase)) return 16;
        if (typeName.Contains("Float3", StringComparison.OrdinalIgnoreCase)) return 12;
        if (typeName.Contains("Float2", StringComparison.OrdinalIgnoreCase)) return 8;
        if (typeName.Contains("Half4", StringComparison.OrdinalIgnoreCase)) return 8;
        if (typeName.Contains("Half2", StringComparison.OrdinalIgnoreCase)) return 4;
        if (typeName.Contains("Short4", StringComparison.OrdinalIgnoreCase)) return 8;
        if (typeName.Contains("Short2", StringComparison.OrdinalIgnoreCase)) return 4;
        if (typeName.Contains("Byte4", StringComparison.OrdinalIgnoreCase)) return 4;
        return 16;
    }

    private static void AddToPlan(Dictionary<TextureType, Dictionary<string, string[]>> plan, TextureType textureType, string preparedPath, string[] duplicateTargets)
    {
        if (!plan.TryGetValue(textureType, out var bucket))
            plan[textureType] = bucket = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        bucket[preparedPath] = duplicateTargets;
    }

    private static void ReplacePrimaryAndAlternates(string temporaryOutput, string primaryPath, IReadOnlyList<string> alternatePaths)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(primaryPath)!);
        File.Copy(temporaryOutput, primaryPath, overwrite: true);
        foreach (var duplicate in alternatePaths)
        {
            if (string.IsNullOrWhiteSpace(duplicate))
                continue;

            Directory.CreateDirectory(Path.GetDirectoryName(duplicate)!);
            File.Copy(temporaryOutput, duplicate, overwrite: true);
        }
    }

    private static void TryDeleteQuietly(ILogger callerLogger, string path)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex)
        {
            callerLogger.LogDebug(ex, "Failed to delete temporary texture file {path}", path);
        }
    }

    private static string CreateTemporaryPath(string primaryPath, string targetName)
    {
        string fileName = Path.GetFileNameWithoutExtension(primaryPath);
        string extension = Path.GetExtension(primaryPath);
        string unique = Guid.NewGuid().ToString("N", System.Globalization.CultureInfo.InvariantCulture);
        return Path.Combine(Path.GetTempPath(), "RavaSync", "TextureOptimization", $"{fileName}.{targetName}.{unique}{extension}");
    }

    private static TexFile.TextureFormat NormalizeUncompressedFormat(TexFile.TextureFormat sourceFormat)
    {
        if (sourceFormat == TexFile.TextureFormat.A8R8G8B8 || sourceFormat == TexFile.TextureFormat.B8G8R8A8)
            return sourceFormat;

        return TexFile.TextureFormat.B8G8R8A8;
    }

    private static (int Width, int Height) ComputeScaledSize(int width, int height, int maxDimension)
    {
        if (width <= maxDimension && height <= maxDimension)
            return (width, height);

        double scale = Math.Min((double)maxDimension / width, (double)maxDimension / height);
        int scaledWidth = Math.Max(1, (int)Math.Round(width * scale, MidpointRounding.AwayFromZero));
        int scaledHeight = Math.Max(1, (int)Math.Round(height * scale, MidpointRounding.AwayFromZero));
        return (scaledWidth, scaledHeight);
    }

    private static int DetermineMaxDimension(string primaryPath, string targetName, int width, int height)
    {
        int max = Math.Max(width, height);
        if (max <= 2048)
            return 0;

        var profile = TextureContentProfile.Analyze(primaryPath);
        if (profile.PreferredMaxDimension > 0)
            return profile.PreferredMaxDimension;

        return targetName.ToUpperInvariant() switch
        {
            "BC1" => 2048,
            "BC3" => 4096,
            "BC7" => 4096,
            _ => 0,
        };
    }

    private static string ResolveTarget(IReadOnlyDictionary<string, string>? targetsByPrimary, string primary, ILogger callerLogger)
    {
        string? preferred = null;
        if (targetsByPrimary != null && targetsByPrimary.TryGetValue(primary, out var target) && !string.IsNullOrWhiteSpace(target))
            preferred = target.Trim();

        var profile = TextureContentProfile.Analyze(primary);

        if (!NativeTexCodec.TryLoadRgba32(primary, out var image, out var format, out _ ) || image == null)
        {
            if (profile.IsStrictNormalMap)
                return TextureCompressionPlanner.Target.BC3.ToString();
            if (profile.IsMaterialMask)
                return TextureCompressionPlanner.Target.BC3.ToString();
            return preferred ?? TextureCompressionPlanner.Target.BC7.ToString();
        }

        using (image)
        {
            if (profile.IsStrictNormalMap)
                return TextureCompressionPlanner.Target.BC3.ToString();

            if (profile.IsMaterialMask)
                return TextureCompressionPlanner.Target.BC3.ToString();

            if (!string.IsNullOrWhiteSpace(preferred))
            {
                if (string.Equals(preferred, TextureCompressionPlanner.Target.BC1.ToString(), StringComparison.OrdinalIgnoreCase)
                    && (!IsProbablyOpaque(image) || profile.IsAlphaSensitive || profile.IsCharacterTexture || profile.IsEquipmentTexture))
                {
                    return TextureCompressionPlanner.Target.BC7.ToString();
                }

                if (string.Equals(preferred, TextureCompressionPlanner.Target.BC7.ToString(), StringComparison.OrdinalIgnoreCase)
                    && IsProbablyOpaque(image)
                    && !profile.IsAlphaSensitive
                    && !profile.IsCharacterTexture
                    && !profile.IsEquipmentTexture
                    && !profile.IsSkinOrBody)
                {
                    return TextureCompressionPlanner.Target.BC1.ToString();
                }

                return preferred;
            }

            if (TextureCompressionPlanner.TryChooseTarget(primary, format, out var planned))
            {
                if (planned == TextureCompressionPlanner.Target.BC7
                    && IsProbablyOpaque(image)
                    && !profile.IsAlphaSensitive
                    && !profile.IsCharacterTexture
                    && !profile.IsEquipmentTexture
                    && !profile.IsSkinOrBody)
                {
                    return TextureCompressionPlanner.Target.BC1.ToString();
                }

                return planned.ToString();
            }
        }

        callerLogger.LogDebug("Texture optimisation could not infer a target for {primary}; falling back to BC7.", primary);
        return TextureCompressionPlanner.Target.BC7.ToString();
    }

    private static bool IsProbablyOpaque(Image<Rgba32> image)
    {
        int strideX = Math.Max(1, image.Width / 64);
        int strideY = Math.Max(1, image.Height / 64);
        bool sawSample = false;
        for (int y = 0; y < image.Height; y += strideY)
        {
            for (int x = 0; x < image.Width; x += strideX)
            {
                sawSample = true;
                if (image[x, y].A < 250)
                    return false;
            }
        }

        return sawSample;
    }

    private static bool TryMapTarget(string target, out TextureType textureType, out TexFile.TextureFormat texFormat)
    {
        switch ((target ?? string.Empty).Trim().ToUpperInvariant())
        {
            case "BC1":
                textureType = TextureType.Bc1Tex;
                texFormat = TexFile.TextureFormat.BC1;
                return true;
            case "BC3":
                textureType = TextureType.Bc3Tex;
                texFormat = TexFile.TextureFormat.BC3;
                return true;
            case "BC7":
                textureType = TextureType.Bc7Tex;
                texFormat = TexFile.TextureFormat.BC7;
                return true;
            default:
                textureType = TextureType.Bc7Tex;
                texFormat = TexFile.TextureFormat.BC7;
                return false;
        }
    }

    private sealed record TextureWorkItem(string PrimaryPath, IReadOnlyList<string> AlternatePaths, string TargetName, TextureType TargetTextureType, TexFile.TextureFormat TargetTexFormat, IReadOnlyList<string> RelatedModelPaths);

    private readonly record struct UvBounds(float MinX, float MinY, float MaxX, float MaxY);

    private readonly record struct UvCoverage(UvBounds Bounds, float Occupancy, bool WrapDetected, int SampledTriangles, int GridSize, bool[,] UsedCells);

    private enum PreparedPathKind
    {
        Original,
        GpuTemporary,
    }

    private sealed record PreparedTexturePath(string PreparedPath, string[] DuplicateTargets, PreparedPathKind PreparedPathKind, string Detail)
    {
        public static PreparedTexturePath Direct(string preparedPath, string[] duplicateTargets, string detail) => new(preparedPath, duplicateTargets, PreparedPathKind.Original, detail);
        public static PreparedTexturePath GpuTemporary(string preparedPath, string[] duplicateTargets, string detail) => new(preparedPath, duplicateTargets, PreparedPathKind.GpuTemporary, detail);
    }

    private readonly record struct CachedUvCoverageEntry(UvCoverage Coverage, string[] Paths, long[] Lengths, long[] LastWriteUtcTicks)
    {
        public static CachedUvCoverageEntry Create(string[] modelPaths, UvCoverage coverage)
        {
            var lengths = new long[modelPaths.Length];
            var ticks = new long[modelPaths.Length];
            for (int i = 0; i < modelPaths.Length; i++)
            {
                try
                {
                    var info = new FileInfo(modelPaths[i]);
                    lengths[i] = info.Exists ? info.Length : -1;
                    ticks[i] = info.Exists ? info.LastWriteTimeUtc.Ticks : 0;
                }
                catch
                {
                    lengths[i] = -1;
                    ticks[i] = 0;
                }
            }

            return new CachedUvCoverageEntry(coverage, modelPaths, lengths, ticks);
        }

        public bool IsStillValidFor(string[] modelPaths)
        {
            if (modelPaths.Length != Paths.Length)
                return false;

            for (int i = 0; i < modelPaths.Length; i++)
            {
                if (!string.Equals(modelPaths[i], Paths[i], StringComparison.OrdinalIgnoreCase))
                    return false;

                try
                {
                    var info = new FileInfo(modelPaths[i]);
                    long len = info.Exists ? info.Length : -1;
                    long ticks = info.Exists ? info.LastWriteTimeUtc.Ticks : 0;
                    if (len != Lengths[i] || ticks != LastWriteUtcTicks[i])
                        return false;
                }
                catch
                {
                    return false;
                }
            }

            return true;
        }
    }

}
