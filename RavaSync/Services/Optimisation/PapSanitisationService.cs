using FFXIVClientStructs.Havok.Animation;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.Havok.Animation.Animation;
using FFXIVClientStructs.Havok.Animation.Rig;
using FFXIVClientStructs.Havok.Common.Base.Container.Array;
using FFXIVClientStructs.Havok.Common.Base.Math.QsTransform;
using FFXIVClientStructs.Havok.Common.Base.Object;
using FFXIVClientStructs.Havok.Common.Base.System.IO.OStream;
using FFXIVClientStructs.Havok.Common.Base.Types;
using FFXIVClientStructs.Havok.Common.Serialize.Resource;
using FFXIVClientStructs.Havok.Common.Serialize.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using RavaSync.PlayerData.Handlers;
using RavaSync.Utils;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using FFXIVClientStructs.Havok.Common.Base.Math.Vector;
using FFXIVClientStructs.Havok.Common.Base.Math.Quaternion;

namespace RavaSync.Services;

public sealed class PapSanitisationService : IHostedService
{
    private enum PapContainerReadStatus
    {
        Valid = 0,
        PassThroughOriginal = 1,
        Invalid = 2,
    }

    private const int MaxHavokBytes = 32 * 1024 * 1024;
    private const string HavokInterleavedConversionCtorSig = "48 89 5C 24 ?? 48 89 6C 24 ?? 56 57 41 55 41 56 41 57 48 83 EC ?? 0F 29 74 24";
    private const string SanitizerVersion = "pap-repackage-v15-target-dummy-skeleton";
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private readonly ILogger<PapSanitisationService> _logger;
    private readonly FileCacheManager _fileCacheManager;
    private static readonly SemaphoreSlim _nativeHavokGate = new(1, 1);

    private readonly XivDataAnalyzer _xivDataAnalyzer;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly HavokInterleavedConversionCtorDelegate? _havokInterleavedConversionCtor;
    private readonly ConcurrentDictionary<string, PapRewriteResult> _rewriteCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _rewriteLocks = new(StringComparer.Ordinal);
    private int _startupCleanupComplete;
    private const bool EnableRoundTripMaterializedPapVerification = false;

    public PapSanitisationService(ILogger<PapSanitisationService> logger, FileCacheManager fileCacheManager, XivDataAnalyzer xivDataAnalyzer, DalamudUtilService dalamudUtil, ISigScanner sigScanner)
    {
        _logger = logger;
        _fileCacheManager = fileCacheManager;
        _xivDataAnalyzer = xivDataAnalyzer;
        _dalamudUtil = dalamudUtil;
        _havokInterleavedConversionCtor = ResolveHavokInterleavedConversionCtor(sigScanner);

        if (_havokInterleavedConversionCtor == null)
            _logger.LogWarning("Sender-side compressed PAP rebuild support was unavailable because the Havok conversion constructor could not be resolved");
    }

    public IReadOnlyList<TargetSkeletonSnapshot> GetTargetSkeletonSnapshots(GameObjectHandler handler)
    {
        var snapshots = _xivDataAnalyzer.GetTargetSkeletonSnapshots(handler);
        if (snapshots.Count == 0)
            return snapshots;

        var filtered = snapshots
            .Where(static s => s.IsHumanAnimationSkeleton)
            .OrderByDescending(static s => s.BoneCount)
            .ToArray();

        return filtered.Length > 0 ? filtered : snapshots;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            PruneGeneratedPapFolder();
            Interlocked.Exchange(ref _startupCleanupComplete, 1);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to prune generated PAP folder on startup");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task<PapRewriteResult> RewritePapForTargetAsync(string originalHash, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(originalHash))
            return PapRewriteResult.Blocked(string.Empty, "PAP had no hash to sanitize");

        if (targetSkeletons == null || targetSkeletons.Count == 0)
            return PapRewriteResult.Blocked(originalHash, "Target skeleton snapshot was unavailable");

        EnsureStartupCleanup();

        string skeletonFingerprint = ComputeSkeletonFingerprint(targetSkeletons);
        string cacheKey = originalHash + "|" + skeletonFingerprint + "|" + SanitizerVersion;

        if (_rewriteCache.TryGetValue(cacheKey, out var cached) && IsReusable(cached))
            return cached;

        var gate = _rewriteLocks.GetOrAdd(cacheKey, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_rewriteCache.TryGetValue(cacheKey, out cached) && IsReusable(cached))
                return cached;

            var existingGenerated = TryGetExistingGeneratedVariant(originalHash, skeletonFingerprint);
            if (existingGenerated != null)
            {
                _rewriteCache[cacheKey] = existingGenerated;
                return existingGenerated;
            }

            var result = await RewritePapForTargetSerializedAsync(originalHash, targetSkeletons, skeletonFingerprint, token).ConfigureAwait(false);
            _rewriteCache[cacheKey] = result;
            return result;
        }
        finally
        {
            gate.Release();
        }
    }

    private void EnsureStartupCleanup()
    {
        if (Volatile.Read(ref _startupCleanupComplete) == 1)
            return;

        PruneGeneratedPapFolder();
        Interlocked.Exchange(ref _startupCleanupComplete, 1);
    }

    private async Task<PapRewriteResult> RewritePapForTargetSerializedAsync(string originalHash, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, string skeletonFingerprint, CancellationToken token)
    {
        var preparation = await Task.Run(() => PreparePapRewrite(originalHash), token).ConfigureAwait(false);
        if (preparation.ImmediateResult != null)
            return preparation.ImmediateResult;

        var prepared = preparation.Context;
        if (prepared == null)
            return PapRewriteResult.Blocked(originalHash, "PAP rewrite preparation did not produce a valid context");

        try
        {
            PapRewriteFrameworkResult frameworkResult;

            await _nativeHavokGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                _logger.LogDebug("Entering serialized PAP Havok rewrite for {hash}", originalHash);
                frameworkResult = await _dalamudUtil.RunOnFrameworkThread(() => RewritePapForTargetFramework(prepared, targetSkeletons)).ConfigureAwait(false);
            }
            finally
            {
                _nativeHavokGate.Release();
            }

            if (frameworkResult.TerminalResult != null)
                return frameworkResult.TerminalResult;

            var result = await Task.Run(() => FinalizePapRewrite(prepared, skeletonFingerprint, frameworkResult), token).ConfigureAwait(false);

            if (EnableRoundTripMaterializedPapVerification && result.Status == PapRewriteStatus.Sanitized)
            {
                await _nativeHavokGate.WaitAsync(token).ConfigureAwait(false);
                try
                {
                    var verification = await _dalamudUtil.RunOnFrameworkThread(() => VerifyMaterializedPapOnFrameworkThread(result.EffectivePath, targetSkeletons, result.BindingCount)).ConfigureAwait(false);
                    if (!verification.Success)
                    {
                        DeleteGeneratedPapArtifacts(result.EffectiveHash, result.EffectivePath);
                        return PapRewriteResult.Blocked(originalHash, verification.Failure);
                    }
                }
                finally
                {
                    _nativeHavokGate.Release();
                }
            }

            return result;
        }
        finally
        {
            CleanupPreparedPapRewrite(prepared);
        }
    }

    private void PruneGeneratedPapFolder()
    {
        var generatedFolder = GetGeneratedPapFolder();
        Directory.CreateDirectory(generatedFolder);

        int deletedTempCount = 0;
        int deletedOrphanedPapCount = 0;
        int deletedOrphanedSidecarCount = 0;

        foreach (var file in Directory.EnumerateFiles(generatedFolder, "*.tmp", SearchOption.TopDirectoryOnly))
        {
            try
            {
                File.Delete(file);
                deletedTempCount++;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not delete generated PAP temp artifact {file}", file);
            }
        }

        foreach (var sidecarPath in Directory.EnumerateFiles(generatedFolder, "*.pap.ravasync.json", SearchOption.TopDirectoryOnly))
        {
            try
            {
                GeneratedPapMetadata? metadata = null;
                try
                {
                    metadata = JsonSerializer.Deserialize<GeneratedPapMetadata>(File.ReadAllText(sidecarPath), _jsonOptions);
                }
                catch
                {
                    // Treat unreadable metadata as orphaned and remove it so it cannot force a rescan forever.
                }

                var generatedHash = metadata?.GeneratedHash;
                if (string.IsNullOrWhiteSpace(generatedHash))
                {
                    File.Delete(sidecarPath);
                    deletedOrphanedSidecarCount++;
                    continue;
                }

                var papPath = GetGeneratedPapPath(generatedHash);
                if (!File.Exists(papPath))
                {
                    File.Delete(sidecarPath);
                    deletedOrphanedSidecarCount++;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not validate generated PAP sidecar {file}", sidecarPath);
            }
        }

        foreach (var papPath in Directory.EnumerateFiles(generatedFolder, "*.pap", SearchOption.TopDirectoryOnly))
        {
            try
            {
                var hash = Path.GetFileNameWithoutExtension(papPath);
                var sidecarPath = GetGeneratedPapMetadataPath(hash);
                if (File.Exists(sidecarPath))
                    continue;

                File.Delete(papPath);
                deletedOrphanedPapCount++;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not validate generated PAP artifact {file}", papPath);
            }
        }

        if (deletedTempCount > 0 || deletedOrphanedPapCount > 0 || deletedOrphanedSidecarCount > 0)
        {
            _logger.LogDebug(
                "Pruned generated PAP folder ({tempCount} temp file(s), {papCount} orphaned PAP(s), {sidecarCount} orphaned sidecar file(s))",
                deletedTempCount,
                deletedOrphanedPapCount,
                deletedOrphanedSidecarCount);
        }
    }

    private PapRewriteResult? TryGetExistingGeneratedVariant(string originalHash, string skeletonFingerprint)
    {
        var folder = GetGeneratedPapFolder();
        if (!Directory.Exists(folder))
            return null;

        foreach (var sidecarPath in Directory.EnumerateFiles(folder, "*.pap.ravasync.json", SearchOption.TopDirectoryOnly))
        {
            GeneratedPapMetadata? metadata;
            try
            {
                metadata = JsonSerializer.Deserialize<GeneratedPapMetadata>(File.ReadAllText(sidecarPath), _jsonOptions);
            }
            catch
            {
                continue;
            }

            if (metadata == null)
                continue;

            if (!string.Equals(metadata.SanitizerVersion, SanitizerVersion, StringComparison.Ordinal)
                || !string.Equals(metadata.OriginalHash, originalHash, StringComparison.Ordinal)
                || !string.Equals(metadata.SkeletonFingerprint, skeletonFingerprint, StringComparison.Ordinal)
                || string.IsNullOrWhiteSpace(metadata.GeneratedHash))
            {
                continue;
            }

            var papPath = GetGeneratedPapPath(metadata.GeneratedHash);
            if (!File.Exists(papPath))
                continue;


            return PapRewriteResult.Sanitised(
                originalHash,
                metadata.GeneratedHash,
                papPath,
                metadata.RemappedTrackCount,
                metadata.DroppedTrackCount,
                metadata.BindingCount,
                metadata.Reason ?? "Reused previously generated PAP variant for the current session");
        }

        return null;
    }

    private static bool IsReusable(PapRewriteResult result)
    {
        return result.Status switch
        {
            PapRewriteStatus.Blocked => true,
            PapRewriteStatus.OriginalFallback => true,
            PapRewriteStatus.OriginalSafe => !string.IsNullOrWhiteSpace(result.EffectivePath) && File.Exists(result.EffectivePath),
            PapRewriteStatus.Sanitized => !string.IsNullOrWhiteSpace(result.EffectivePath) && File.Exists(result.EffectivePath),
            _ => false,
        };
    }

    private PapRewritePreparation PreparePapRewrite(string originalHash)
    {
        string sourcePath = string.Empty;
        string tempHkxPath = string.Empty;

        try
        {
            var cacheEntity = _fileCacheManager.GetFileCacheByHash(originalHash);
            if (cacheEntity == null || string.IsNullOrWhiteSpace(cacheEntity.ResolvedFilepath) || !File.Exists(cacheEntity.ResolvedFilepath))
                return new PapRewritePreparation(null, PapRewriteResult.Blocked(originalHash, "Original PAP was not available in cache"));

            sourcePath = cacheEntity.ResolvedFilepath;

            var readStatus = TryReadPapContainer(sourcePath, out var papContainer, out var readFailure);
            if (readStatus == PapContainerReadStatus.Invalid)
                return new PapRewritePreparation(null, PapRewriteResult.Blocked(originalHash, readFailure));

            if (readStatus == PapContainerReadStatus.PassThroughOriginal)
                return new PapRewritePreparation(null, PapRewriteResult.Original(originalHash, sourcePath, 0, readFailure));

            if (!papContainer.IsHumanPap)
                return new PapRewritePreparation(null, PapRewriteResult.Original(originalHash, sourcePath, 0, "Non-human PAP left untouched"));

            _logger.LogDebug("Prepared PAP safety rewrite for {hash} from {path} (headerSize={headerSize}, havokOffset={havokOffset}, footerOffset={footerOffset}, havokBytes={havokBytes})",
                originalHash,
                sourcePath,
                papContainer.HeaderSize,
                papContainer.HavokOffset,
                papContainer.FooterOffset,
                papContainer.HavokBytes.Length);

            tempHkxPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) + ".hkx";
            File.WriteAllBytes(tempHkxPath, papContainer.HavokBytes);

            return new PapRewritePreparation(new PapRewritePreparedContext(originalHash, sourcePath, papContainer, tempHkxPath), null);
        }
        catch (Exception ex)
        {
            CleanupPreparedPapRewrite(tempHkxPath);
            _logger.LogWarning(ex, "Failed to prepare PAP {hash} at {path} for sanitizer rewrite", originalHash, sourcePath);
            return new PapRewritePreparation(null, PapRewriteResult.OriginalFallback(originalHash, sourcePath, 0, "PAP sanitizer preparation failed, so the mod will be disabled for the current target skeleton"));
        }
    }

    private PapRewriteFrameworkResult RewritePapForTargetFramework(PapRewritePreparedContext prepared, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons)
    {
        var tempHkxPathAnsi = Marshal.StringToHGlobalAnsi(prepared.TempHkxPath);

        try
        {
            unsafe
            {
                var loadOptions = stackalloc hkSerializeUtil.LoadOptions[1];
                loadOptions->TypeInfoRegistry = hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry();
                loadOptions->ClassNameRegistry = hkBuiltinTypeRegistry.Instance()->GetClassNameRegistry();
                loadOptions->Flags = new hkFlags<hkSerializeUtil.LoadOptionBits, int>
                {
                    Storage = (int)hkSerializeUtil.LoadOptionBits.Default
                };

                var resource = hkSerializeUtil.LoadFromFile((byte*)tempHkxPathAnsi, null, loadOptions);
                if (resource == null)
                {
                    return PapRewriteFrameworkResult.FromTerminal(
                        PapRewriteResult.Original(prepared.OriginalHash, prepared.SourcePath, 0, "PAP Havok payload could not be loaded by the sanitizer, so the original PAP will be used unchanged for the current target skeleton"));
                }

                var rootLevelName = "hkRootLevelContainer"u8;
                fixed (byte* rootName = rootLevelName)
                {
                    var container = (hkRootLevelContainer*)resource->GetContentsPointer(rootName, hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry());
                    if (container == null)
                    {
                        return PapRewriteFrameworkResult.FromTerminal(
                            PapRewriteResult.Original(prepared.OriginalHash, prepared.SourcePath, 0, "Havok root level container was unavailable to the sanitizer, so the original PAP will be used unchanged for the current target skeleton"));
                    }

                    var animationName = "hkaAnimationContainer"u8;
                    fixed (byte* animName = animationName)
                    {
                        var animationContainer = (hkaAnimationContainer*)container->findObjectByName(animName, null);
                        if (animationContainer == null)
                        {
                            return PapRewriteFrameworkResult.FromTerminal(
                                PapRewriteResult.Original(prepared.OriginalHash, prepared.SourcePath, 0, "Animation container was not available to the sanitizer, so the original PAP will be used unchanged for the current target skeleton"));
                        }

                        if (TryDetectAlreadySafePap(animationContainer, targetSkeletons, prepared.SourcePath, out var fastBindingCount, out var fastReason))
                        {
                            _logger.LogDebug("PAP {hash} was proven safe by sender-side metadata scan and was left in original format", prepared.OriginalHash);
                            return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Original(prepared.OriginalHash, prepared.SourcePath, fastBindingCount, fastReason));
                        }

                        var analysis = AnalyzeAnimationBindings(animationContainer, targetSkeletons, prepared.SourcePath);
                        if (analysis.Blocked)
                            return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Blocked(prepared.OriginalHash, analysis.Reason));

                        if (!analysis.Changed)
                        {
                            _logger.LogDebug("PAP {hash} required no transform-track pruning for current skeleton", prepared.OriginalHash);
                            return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Original(prepared.OriginalHash, prepared.SourcePath, analysis.BindingCount, analysis.Reason));
                        }

                        var allocatedPatchBuffers = new List<nint>();
                        try
                        {
                            if (!ApplyBindingPatchesInMemory(animationContainer, analysis.BindingPatches, allocatedPatchBuffers, out var applyFailure))
                                return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Blocked(prepared.OriginalHash, applyFailure));

                            if (!TryValidatePatchedBindingsInMemory(animationContainer, targetSkeletons, analysis.BindingCount, out var inMemoryVerifyFailure))
                                return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Blocked(prepared.OriginalHash, inMemoryVerifyFailure));

                            if (!TrySaveResourceToHkx(resource, out var rewrittenHkx, out var saveFailure))
                                return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Blocked(prepared.OriginalHash, saveFailure));

                            if (!HasCompatibleHkxSignature(prepared.PapContainer.HavokBytes, rewrittenHkx, out var signatureFailure))
                                return PapRewriteFrameworkResult.FromTerminal(PapRewriteResult.Blocked(prepared.OriginalHash, signatureFailure));

                            return PapRewriteFrameworkResult.FromSanitized(rewrittenHkx, analysis.RemappedTracks, analysis.DroppedTracks, analysis.BindingCount, analysis.Reason);
                        }
                        finally
                        {
                            foreach (var buffer in allocatedPatchBuffers)
                            {
                                if (buffer != 0)
                                    Marshal.FreeHGlobal(buffer);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to sanitize PAP {hash} at {path}; leaving the original PAP untouched", prepared.OriginalHash, prepared.SourcePath);
            return PapRewriteFrameworkResult.FromTerminal(
                PapRewriteResult.OriginalFallback(prepared.OriginalHash, prepared.SourcePath, 0, "PAP sanitizer hit an internal error, so the mod will be disabled for the current target skeleton"));
        }
        finally
        {
            Marshal.FreeHGlobal(tempHkxPathAnsi);
        }
    }

    private PapRewriteResult FinalizePapRewrite(PapRewritePreparedContext prepared, string skeletonFingerprint, PapRewriteFrameworkResult frameworkResult)
    {
        if (frameworkResult.RewrittenHkx == null || frameworkResult.RewrittenHkx.Length == 0)
            return PapRewriteResult.Blocked(prepared.OriginalHash, "PAP sanitizer completed without producing a rewritten Havok payload");

        var rewrittenPap = BuildPapFromRewrittenHkx(prepared.PapContainer, frameworkResult.RewrittenHkx);
        if (!TryMaterializePapToCache(
                rewrittenPap,
                prepared.OriginalHash,
                skeletonFingerprint,
                frameworkResult.RemappedTracks,
                frameworkResult.DroppedTracks,
                frameworkResult.BindingCount,
                frameworkResult.Reason,
                out var rewrittenHash,
                out var rewrittenPath,
                out var cacheFailure))
        {
            return PapRewriteResult.Blocked(prepared.OriginalHash, cacheFailure);
        }

        _logger.LogDebug(
            "Sanitized PAP {hash}: remapped {remapped} track(s), detached {dropped} track(s), bindings {bindings}, output {newHash}",
            prepared.OriginalHash,
            frameworkResult.RemappedTracks,
            frameworkResult.DroppedTracks,
            frameworkResult.BindingCount,
            rewrittenHash);

        return PapRewriteResult.Sanitised(
            prepared.OriginalHash,
            rewrittenHash,
            rewrittenPath,
            frameworkResult.RemappedTracks,
            frameworkResult.DroppedTracks,
            frameworkResult.BindingCount,
            frameworkResult.Reason);
    }

    private static void CleanupPreparedPapRewrite(PapRewritePreparedContext? prepared)
        => CleanupPreparedPapRewrite(prepared?.TempHkxPath);

    private static void CleanupPreparedPapRewrite(string? tempHkxPath)
    {
        if (string.IsNullOrWhiteSpace(tempHkxPath))
            return;

        try { File.Delete(tempHkxPath); } catch { }
    }

    private (bool Success, string Failure) VerifyMaterializedPapOnFrameworkThread(string rewrittenPath, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, int expectedBindingCount)
    {
        return TryVerifyMaterializedPap(rewrittenPath, targetSkeletons, expectedBindingCount, out var failure)
            ? (true, string.Empty)
            : (false, failure);
    }

    public string GetTargetSkeletonFingerprint(IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons)
        => ComputeSkeletonFingerprint(targetSkeletons);

    public bool IsGeneratedPapVariantPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        try
        {
            var normalizedPath = Path.GetFullPath(path);
            var generatedFolder = Path.GetFullPath(GetGeneratedPapFolder());
            if (!generatedFolder.EndsWith(Path.DirectorySeparatorChar))
                generatedFolder += Path.DirectorySeparatorChar;

            return normalizedPath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
                && normalizedPath.StartsWith(generatedFolder, StringComparison.OrdinalIgnoreCase);
        }
        catch
        {
            return false;
        }
    }

    private static string ComputeSkeletonFingerprint(IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons)
    {
        using var sha1 = System.Security.Cryptography.SHA1.Create();
        using var ms = new MemoryStream();
        using var writer = new StreamWriter(ms, leaveOpen: true);

        foreach (var snapshot in targetSkeletons.OrderBy(static s => s.NormalizedSkeletonName, StringComparer.Ordinal))
        {
            writer.Write(snapshot.NormalizedSkeletonName);
            writer.Write('|');
            writer.Write(snapshot.NormalizedResourceName);
            writer.Write('|');
            writer.Write(snapshot.BoneCount);
            writer.Write('|');
            writer.Write(snapshot.ReferencePoseByIndex.Count);
            writer.Write('\n');

            for (int i = 0; i < snapshot.BoneNamesByIndex.Count; i++)
            {
                writer.Write(i);
                writer.Write(':');
                writer.Write(snapshot.BoneNamesByIndex[i] ?? string.Empty);
                writer.Write('\n');
            }

            writer.Flush();

            var referencePoseArray = snapshot.ReferencePoseByIndex as hkQsTransformf[] ?? snapshot.ReferencePoseByIndex.ToArray();
            var referencePoseBytes = MemoryMarshal.AsBytes(referencePoseArray.AsSpan());
            ms.Write(referencePoseBytes);
        }

        writer.Flush();
        ms.Position = 0;
        return Convert.ToHexString(sha1.ComputeHash(ms));
    }

    private sealed record PapContainerData(int HeaderSize, int HavokOffset, int FooterOffset, bool IsHumanPap, byte[] HeaderBytes, byte[] HavokBytes, byte[] FooterBytes);
    private sealed record PapRewritePreparation(PapRewritePreparedContext? Context, PapRewriteResult? ImmediateResult);
    private sealed record PapRewritePreparedContext(string OriginalHash, string SourcePath, PapContainerData PapContainer, string TempHkxPath);
    private sealed record PapRewriteFrameworkResult(PapRewriteResult? TerminalResult, byte[]? RewrittenHkx, int RemappedTracks, int DroppedTracks, int BindingCount, string Reason)
    {
        public static PapRewriteFrameworkResult FromTerminal(PapRewriteResult result)
            => new(result, null, 0, 0, result.BindingCount, result.Reason ?? string.Empty);

        public static PapRewriteFrameworkResult FromSanitized(byte[] rewrittenHkx, int remappedTracks, int droppedTracks, int bindingCount, string reason)
            => new(null, rewrittenHkx, remappedTracks, droppedTracks, bindingCount, reason);
    }
    private sealed record BindingPatchPlan(int BindingIndex,nint AnimationAddress,string TargetSkeletonKey,TargetSkeletonSnapshot TargetSkeleton,short[] OriginalTracks,short[] PatchedTracks,int[] KeptTrackIndices,int NewTrackCount);
    private readonly record struct RebuildCacheKey(nint AnimationAddress, string PlanKey, string TargetSkeletonKey);
    private sealed record AnalysisOutcome(bool Changed, bool Blocked, int RemappedTracks, int DroppedTracks, int BindingCount, string Reason, IReadOnlyList<BindingPatchPlan> BindingPatches);
    [StructLayout(LayoutKind.Sequential)]
    private struct HkaInterleavedUncompressedAnimation
    {
        public hkaAnimation Animation;
        public hkArray<hkQsTransformf> Transforms;
        public hkArray<float> Floats;
    }

    [StructLayout(LayoutKind.Explicit, Size = 0xC0)]
    private unsafe struct HkaPredictiveCompressedAnimation
    {
        [FieldOffset(0x00)] public hkaAnimation Animation;
        [FieldOffset(0xB0)] public hkaSkeleton* Skeleton;
    }

    [StructLayout(LayoutKind.Explicit, Size = 0x58)]
    private unsafe struct HkaQuantizedCompressedAnimation
    {
        [FieldOffset(0x00)] public hkaAnimation Animation;
        [FieldOffset(0x50)] public hkaSkeleton* Skeleton;
    }

    private unsafe delegate HkaInterleavedUncompressedAnimation* HavokInterleavedConversionCtorDelegate(HkaInterleavedUncompressedAnimation* destinationAnimation, hkaAnimation* sourceAnimation);

    private sealed record GeneratedPapMetadata(string SanitizerVersion,string OriginalHash,string GeneratedHash,string SkeletonFingerprint,int RemappedTrackCount,int DroppedTrackCount,int BindingCount,string? Reason,DateTimeOffset CreatedUtc);

    private static void AddTargetLookupEntry(Dictionary<string, TargetSkeletonSnapshot> output, string key, TargetSkeletonSnapshot snapshot)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (output.TryGetValue(key, out var existing) && existing.BoneCount >= snapshot.BoneCount)
            return;

        output[key] = snapshot;
    }

    private static void AddSourceSkeletonLookupEntry(Dictionary<string, IReadOnlyList<string?>> output, string key, IReadOnlyList<string?> boneNames)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (output.TryGetValue(key, out var existing) && existing.Count >= boneNames.Count)
            return;

        output[key] = boneNames;
    }

    private static string CreateTargetSkeletonCacheKey(TargetSkeletonSnapshot targetSkeleton)
    {
        if (targetSkeleton == null)
            return string.Empty;

        return targetSkeleton.NormalizedResourceName
            + "|"
            + targetSkeleton.NormalizedSkeletonName
            + "|"
            + targetSkeleton.BoneCount;
    }

    private unsafe bool TryDetectAlreadySafePap(hkaAnimationContainer* animationContainer, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, string sourcePath, out int bindingCount, out string reason)
    {
        bindingCount = 0;
        reason = "PAP already matched target skeleton";

        if (animationContainer == null)
            return false;

        bindingCount = animationContainer->Bindings.Length;
        if (bindingCount <= 0 || bindingCount > 256)
            return false;

        var targetLookup = BuildTargetLookup(targetSkeletons);
        var unionMaxIndex = targetSkeletons.Count == 0 ? -1 : targetSkeletons.Max(static s => s.BoneCount - 1);

        for (int bindingIndex = 0; bindingIndex < bindingCount; bindingIndex++)
        {
            var binding = animationContainer->Bindings[bindingIndex].ptr;
            if (binding == null)
                continue;

            var transformTracks = binding->TransformTrackToBoneIndices;
            if (transformTracks.Length <= 0 || transformTracks.Length > 8192)
                return false;

            short maxAnimIndex = -1;
            for (int trackIndex = 0; trackIndex < transformTracks.Length; trackIndex++)
            {
                var boneIndex = transformTracks[trackIndex];
                if (boneIndex > maxAnimIndex)
                    maxAnimIndex = boneIndex;
            }

            var originalSkeletonName = binding->OriginalSkeletonName.String ?? string.Empty;
            var normalizedOriginalSkeleton = XivSkeletonIdentity.NormalizeSkeletonKey(originalSkeletonName);
            var targetSkeleton = ResolveTargetSkeleton(originalSkeletonName, normalizedOriginalSkeleton, targetLookup, targetSkeletons, maxAnimIndex);
            if (targetSkeleton == null)
            {
                if (maxAnimIndex > unionMaxIndex)
                {
                    _logger.LogDebug(
                        "PAP {path} fast safety scan could not match binding {bindingIndex} to a target skeleton for referenced bone index {maxAnimIndex}",
                        sourcePath,
                        bindingIndex,
                        maxAnimIndex);
                }

                return false;
            }

            var animation = (hkaAnimation*)binding->Animation.ptr;
            if (animation == null)
                return false;

            if (animation->NumberOfTransformTracks > 0 && animation->NumberOfTransformTracks != transformTracks.Length)
                return false;

            if (animation->Type == hkaAnimation.AnimationType.InterleavedAnimation
                && !TryValidateInterleavedFrameLayout(animation, bindingIndex, out _))
            {
                return false;
            }

            if (!TryValidateBindingSideArrayCounts(binding, animation, bindingIndex, out _))
                return false;

            for (int trackIndex = 0; trackIndex < transformTracks.Length; trackIndex++)
            {
                short originalBoneIndex = transformTracks[trackIndex];
                if (originalBoneIndex >= 0 && originalBoneIndex >= targetSkeleton.BoneCount)
                    return false;
            }
        }

        reason = "PAP already matched target skeleton (sender-side fast scan)";
        return true;
    }

    private unsafe AnalysisOutcome AnalyzeAnimationBindings(hkaAnimationContainer* animationContainer, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, string sourcePath)
    {
        var bindingCount = animationContainer->Bindings.Length;
        if (bindingCount <= 0 || bindingCount > 256)
            return new AnalysisOutcome(false, false, 0, 0, 0, "PAP had no animation bindings", Array.Empty<BindingPatchPlan>());

        var targetLookup = BuildTargetLookup(targetSkeletons);
        var unionMaxIndex = targetSkeletons.Count == 0 ? -1 : targetSkeletons.Max(static s => s.BoneCount - 1);

        var bindingPatches = new List<BindingPatchPlan>(bindingCount);
        int remappedTracks = 0;
        int droppedTracks = 0;
        string reason = "PAP already matched target skeleton";

        for (int bindingIndex = 0; bindingIndex < bindingCount; bindingIndex++)
        {
            var binding = animationContainer->Bindings[bindingIndex].ptr;
            if (binding == null)
                continue;

            var transformTracks = binding->TransformTrackToBoneIndices;
            if (transformTracks.Length <= 0 || transformTracks.Length > 8192)
                continue;

            var originalTracks = new short[transformTracks.Length];
            short maxAnimIndex = -1;
            for (int trackIndex = 0; trackIndex < transformTracks.Length; trackIndex++)
            {
                var boneIndex = transformTracks[trackIndex];
                originalTracks[trackIndex] = boneIndex;
                if (boneIndex > maxAnimIndex)
                    maxAnimIndex = boneIndex;
            }

            var originalSkeletonName = binding->OriginalSkeletonName.String ?? string.Empty;
            var normalizedOriginalSkeleton = XivSkeletonIdentity.NormalizeSkeletonKey(originalSkeletonName);
            var targetSkeleton = ResolveTargetSkeleton(originalSkeletonName, normalizedOriginalSkeleton, targetLookup, targetSkeletons, maxAnimIndex);

            if (targetSkeleton == null)
            {
                if (maxAnimIndex > unionMaxIndex)
                {
                    var why = string.IsNullOrWhiteSpace(originalSkeletonName)
                        ? "Animation binding could not be matched to any target skeleton"
                        : $"Animation binding skeleton '{originalSkeletonName}' could not be matched to any target skeleton";

                    _logger.LogWarning(
                        "Blocking PAP {path}: binding {bindingIndex} could not be matched to a target skeleton and references bone index {maxAnimIndex} beyond current target max {targetMax}",
                        sourcePath,
                        bindingIndex,
                        maxAnimIndex,
                        unionMaxIndex);

                    return new AnalysisOutcome(false, true, 0, 0, bindingCount, why, Array.Empty<BindingPatchPlan>());
                }

                continue;
            }

            var animation = (hkaAnimation*)binding->Animation.ptr;
            if (animation == null)
            {
                var why = string.IsNullOrWhiteSpace(originalSkeletonName)
                    ? "Animation binding did not reference an animation object"
                    : $"Animation binding skeleton '{originalSkeletonName}' did not reference an animation object";
                return new AnalysisOutcome(false, true, 0, 0, bindingCount, why, Array.Empty<BindingPatchPlan>());
            }

            if (!TryValidateBindingSideArrayCounts(binding, animation, bindingIndex, out var sideArrayFailure))
                return new AnalysisOutcome(false, true, 0, 0, bindingCount, sideArrayFailure, Array.Empty<BindingPatchPlan>());

            if (animation->NumberOfTransformTracks > 0 && animation->NumberOfTransformTracks != originalTracks.Length)
            {
                _logger.LogWarning(
                    "Blocking PAP {path}: binding {bindingIndex} transform-track map length {bindingTrackCount} did not match animation track count {animationTrackCount}",
                    sourcePath,
                    bindingIndex,
                    originalTracks.Length,
                    animation->NumberOfTransformTracks);

                return new AnalysisOutcome(false, true, 0, 0, bindingCount,
                    $"Animation binding skeleton '{originalSkeletonName}' had mismatched track metadata", Array.Empty<BindingPatchPlan>());
            }

            var keptTracks = new List<short>(originalTracks.Length);
            var keptTrackIndices = new List<int>(originalTracks.Length);
            bool removedAnyTracks = false;
            for (int trackIndex = 0; trackIndex < originalTracks.Length; trackIndex++)
            {
                short originalBoneIndex = originalTracks[trackIndex];
                if (originalBoneIndex < 0 || originalBoneIndex < targetSkeleton.BoneCount)
                {
                    keptTracks.Add(originalBoneIndex);
                    keptTrackIndices.Add(trackIndex);
                    continue;
                }

                removedAnyTracks = true;
                droppedTracks++;
                reason = "PAP tracks targeting bones outside the current loaded skeleton were removed from the animation binding";
            }

            if (!removedAnyTracks)
                continue;

            if (keptTracks.Count <= 0)
            {
                _logger.LogWarning(
                    "Blocking PAP {path}: binding {bindingIndex} for skeleton {skeletonName} would lose every transform track against target skeleton {targetSkeleton}",
                    sourcePath,
                    bindingIndex,
                    string.IsNullOrWhiteSpace(originalSkeletonName) ? "<unknown>" : originalSkeletonName,
                    targetSkeleton.SkeletonName);

                return new AnalysisOutcome(false, true, 0, 0, bindingCount,
                    $"Animation binding skeleton '{originalSkeletonName}' had no safe transform tracks for the loaded skeleton", Array.Empty<BindingPatchPlan>());
            }

            if (animation->Type != hkaAnimation.AnimationType.InterleavedAnimation && !IsSupportedSenderSideRebuildAnimationType(animation->Type))
            {
                _logger.LogWarning(
                    "Blocking PAP {path}: binding {bindingIndex} for skeleton {skeletonName} required stripping, but Havok animation type {animationType} is not supported by the sender-side rebuild path",
                    sourcePath,
                    bindingIndex,
                    string.IsNullOrWhiteSpace(originalSkeletonName) ? "<unknown>" : originalSkeletonName,
                    animation->Type);

                return new AnalysisOutcome(false, true, 0, 0, bindingCount,
                    $"Animation binding skeleton '{originalSkeletonName}' required a sender-side rebuild for unsupported Havok animation type '{animation->Type}'", Array.Empty<BindingPatchPlan>());
            }

            var animationPtr = (nint)animation;
            var keptTrackIndexArray = keptTrackIndices.ToArray();

            remappedTracks += keptTrackIndexArray.Length;
            bindingPatches.Add(new BindingPatchPlan(
                bindingIndex,
                animationPtr,
                CreateTargetSkeletonCacheKey(targetSkeleton),
                targetSkeleton,
                originalTracks,
                keptTracks.ToArray(),
                keptTrackIndexArray,
                keptTrackIndexArray.Length));
        }

        bool changed = bindingPatches.Count > 0;
        return new AnalysisOutcome(changed, false, remappedTracks, droppedTracks, bindingCount, reason, bindingPatches);
    }

    private unsafe bool ApplyBindingPatchesInMemory(hkaAnimationContainer* animationContainer, IReadOnlyList<BindingPatchPlan> bindingPatches, List<nint> allocatedBuffers, out string failure)
    {
        failure = string.Empty;
        var rebuiltAnimationLookup = new Dictionary<RebuildCacheKey, nint>();

        foreach (var patch in bindingPatches)
        {
            if (patch.BindingIndex < 0 || patch.BindingIndex >= animationContainer->Bindings.Length)
                continue;

            var binding = animationContainer->Bindings[patch.BindingIndex].ptr;
            if (binding == null)
                continue;

            var rebuildCacheKey = new RebuildCacheKey(
                patch.AnimationAddress,
                CreateTrackPlanKey(patch.KeptTrackIndices),
                patch.TargetSkeletonKey);

            hkaAnimation* animation;
            if (rebuiltAnimationLookup.TryGetValue(rebuildCacheKey, out var rebuiltAnimationAddress))
            {
                animation = (hkaAnimation*)rebuiltAnimationAddress;
                binding->Animation.ptr = animation;
            }
            else
            {
                animation = (hkaAnimation*)binding->Animation.ptr;
                if (animation == null)
                {
                    failure = $"Animation binding {patch.BindingIndex} lost its animation reference during sender-side rebuild";
                    return false;
                }

                if (!TryRebuildAnimationPayload(animationContainer, binding, animation, patch, allocatedBuffers, out var rebuiltAnimation, out failure))
                    return false;

                animation = rebuiltAnimation;
                rebuiltAnimationLookup[rebuildCacheKey] = (nint)animation;
                binding->Animation.ptr = animation;
            }

            if (animation == null)
            {
                failure = $"Animation binding {patch.BindingIndex} did not produce a rebuilt animation object";
                return false;
            }

            if (!TryCompactBindingPartitionIndices(binding, patch.KeptTrackIndices, patch.OriginalTracks.Length, animation->NumberOfFloatTracks, allocatedBuffers, out failure))
                return false;

            binding->TransformTrackToBoneIndices = CreateOwnedArray<short>(patch.PatchedTracks, allocatedBuffers);
        }

        return true;
    }

    private unsafe bool TryRebuildAnimationPayload(hkaAnimationContainer* animationContainer, hkaAnimationBinding* binding, hkaAnimation* animation, BindingPatchPlan patch, List<nint> allocatedBuffers, out hkaAnimation* rebuiltAnimation, out string failure)
    {
        rebuiltAnimation = animation;
        failure = string.Empty;

        if (animation == null)
        {
            failure = "Animation was null during sender-side rebuild";
            return false;
        }

        if (animation->Type == hkaAnimation.AnimationType.InterleavedAnimation)
        {
            if (!TryCloneInterleavedAnimationPayload(animation, allocatedBuffers, out var clonedAnimation, out failure))
                return false;

            return TryRebuildInterleavedAnimationPayload(clonedAnimation, patch, allocatedBuffers, out rebuiltAnimation, out failure);
        }

        if (!IsSupportedSenderSideRebuildAnimationType(animation->Type))
        {
            failure = $"Unsupported Havok animation type '{animation->Type}' for sender-side transform rebuild";
            return false;
        }

        return TryRebuildCompressedAnimationPayload(animationContainer, binding, animation, patch, allocatedBuffers, out rebuiltAnimation, out failure);
    }

    private unsafe bool TryRebuildCompressedAnimationPayload(hkaAnimationContainer* animationContainer, hkaAnimationBinding* binding, hkaAnimation* sourceAnimation, BindingPatchPlan patch, List<nint> allocatedBuffers, out hkaAnimation* rebuiltAnimation, out string failure)
    {
        rebuiltAnimation = null;
        failure = string.Empty;

        if (_havokInterleavedConversionCtor == null)
        {
            failure = "Compressed animation rebuild was unavailable because the Havok conversion constructor could not be resolved";
            return false;
        }

        if (!IsSupportedSenderSideRebuildAnimationType(sourceAnimation->Type))
        {
            failure = $"Unsupported Havok animation type '{sourceAnimation->Type}' for sender-side compressed rebuild";
            return false;
        }

        hkaSkeleton* injectedSkeleton = null;
        if (!TryEnsureCompressedAnimationSkeleton(binding, sourceAnimation, patch.TargetSkeleton, allocatedBuffers, out injectedSkeleton, out failure))
            return false;

        var interleavedSize = Marshal.SizeOf<HkaInterleavedUncompressedAnimation>();
        var convertedInterleaved = (HkaInterleavedUncompressedAnimation*)Marshal.AllocHGlobal(interleavedSize);
        new Span<byte>(convertedInterleaved, interleavedSize).Clear();

        HkaInterleavedUncompressedAnimation* ctorResult;
        try
        {
            ctorResult = _havokInterleavedConversionCtor(convertedInterleaved, sourceAnimation);
        }
        catch (Exception ex)
        {
            Marshal.FreeHGlobal((nint)convertedInterleaved);
            failure = $"Compressed animation conversion to interleaved form failed: {ex.Message}";
            return false;
        }
        finally
        {
            if (injectedSkeleton != null)
                ClearInjectedCompressedAnimationSkeleton(sourceAnimation, injectedSkeleton);
        }

        if (ctorResult == null)
        {
            Marshal.FreeHGlobal((nint)convertedInterleaved);
            failure = "Compressed animation conversion produced no interleaved animation object";
            return false;
        }

        if (ctorResult->Animation.Type != hkaAnimation.AnimationType.InterleavedAnimation)
        {
            Marshal.FreeHGlobal((nint)ctorResult);
            failure = $"Compressed animation conversion produced unexpected Havok animation type '{ctorResult->Animation.Type}'";
            return false;
        }

        if (ctorResult->Animation.NumberOfTransformTracks <= 0 || ctorResult->Transforms.Length <= 0)
        {
            Marshal.FreeHGlobal((nint)ctorResult);
            failure = "Compressed animation conversion produced no transform data";
            return false;
        }

        if (ctorResult->Animation.NumberOfTransformTracks != patch.OriginalTracks.Length)
        {
            Marshal.FreeHGlobal((nint)ctorResult);
            failure = $"Compressed animation conversion changed transform track count unexpectedly ({ctorResult->Animation.NumberOfTransformTracks} vs expected {patch.OriginalTracks.Length})";
            return false;
        }

        if (!TryValidateInterleavedFrameLayout(&ctorResult->Animation, patch.BindingIndex, out failure))
        {
            Marshal.FreeHGlobal((nint)ctorResult);
            return false;
        }

        allocatedBuffers.Add((nint)ctorResult);

        rebuiltAnimation = &ctorResult->Animation;
        if (!TryRebuildInterleavedAnimationPayload(rebuiltAnimation, patch, allocatedBuffers, out rebuiltAnimation, out failure))
            return false;

        return true;
    }

    private static string CreateTrackPlanKey(ReadOnlySpan<int> keptTrackIndices)
    {
        if (keptTrackIndices.Length == 0)
            return string.Empty;

        var builder = new StringBuilder(checked(keptTrackIndices.Length * 6));
        for (int i = 0; i < keptTrackIndices.Length; i++)
        {
            if (i > 0)
                builder.Append(',');

            builder.Append(keptTrackIndices[i]);
        }

        return builder.ToString();
    }

    private static unsafe bool TryCloneInterleavedAnimationPayload(hkaAnimation* sourceAnimation, List<nint> allocatedBuffers, out hkaAnimation* clonedAnimation, out string failure)
    {
        clonedAnimation = null;
        failure = string.Empty;

        if (sourceAnimation == null)
        {
            failure = "Interleaved animation clone source was null";
            return false;
        }

        if (sourceAnimation->Type != hkaAnimation.AnimationType.InterleavedAnimation)
        {
            failure = $"Unsupported Havok animation type '{sourceAnimation->Type}' for sender-side interleaved clone";
            return false;
        }

        if (!TryValidateInterleavedFrameLayout(sourceAnimation, -1, out failure))
            return false;

        var sourceInterleaved = (HkaInterleavedUncompressedAnimation*)sourceAnimation;
        var cloneSize = Marshal.SizeOf<HkaInterleavedUncompressedAnimation>();
        var clonedInterleaved = (HkaInterleavedUncompressedAnimation*)Marshal.AllocHGlobal(cloneSize);
        new Span<byte>(clonedInterleaved, cloneSize).Clear();
        *clonedInterleaved = *sourceInterleaved;
        allocatedBuffers.Add((nint)clonedInterleaved);

        clonedInterleaved->Transforms = CreateOwnedArray<hkQsTransformf>(AsSpan(sourceInterleaved->Transforms), allocatedBuffers);
        clonedInterleaved->Floats = CreateOwnedArray<float>(AsSpan(sourceInterleaved->Floats), allocatedBuffers);
        clonedInterleaved->Animation.AnnotationTracks = CreateOwnedArray<hkaAnnotationTrack>(AsSpan(sourceAnimation->AnnotationTracks), allocatedBuffers);

        clonedAnimation = &clonedInterleaved->Animation;
        return true;
    }

    private static unsafe ReadOnlySpan<T> AsSpan<T>(hkArray<T> array) where T : unmanaged
    {
        if (array.Length <= 0 || array.Data == null)
            return ReadOnlySpan<T>.Empty;

        return new ReadOnlySpan<T>(array.Data, array.Length);
    }

    private static unsafe bool TryRebuildInterleavedAnimationPayload(hkaAnimation* animation, BindingPatchPlan patch, List<nint> allocatedBuffers, out hkaAnimation* rebuiltAnimation, out string failure)
    {
        rebuiltAnimation = animation;
        failure = string.Empty;

        if (animation->Type != hkaAnimation.AnimationType.InterleavedAnimation)
        {
            failure = $"Unsupported Havok animation type '{animation->Type}' for sender-side transform rebuild";
            return false;
        }

        var oldTrackCount = patch.OriginalTracks.Length;
        if (oldTrackCount <= 0)
        {
            failure = "Interleaved animation had no transform tracks";
            return false;
        }

        if (!TryValidateInterleavedFrameLayout(animation, patch.BindingIndex, out failure))
            return false;

        if (animation->NumberOfTransformTracks != oldTrackCount)
        {
            failure = $"Interleaved animation track count {animation->NumberOfTransformTracks} did not match expected track count {oldTrackCount}";
            return false;
        }

        var interleaved = (HkaInterleavedUncompressedAnimation*)animation;
        var transformCount = interleaved->Transforms.Length;

        if (patch.KeptTrackIndices.Length <= 0)
        {
            failure = "Track stripping removed every transform track";
            return false;
        }

        var frameCount = transformCount / oldTrackCount;
        var compactedTransformCount = checked(frameCount * patch.KeptTrackIndices.Length);
        var compactedTransforms = ArrayPool<hkQsTransformf>.Shared.Rent(compactedTransformCount);

        try
        {
            var compactedTransformIndex = 0;
            for (int frame = 0; frame < frameCount; frame++)
            {
                var frameOffset = frame * oldTrackCount;
                for (int i = 0; i < patch.KeptTrackIndices.Length; i++)
                    compactedTransforms[compactedTransformIndex++] = interleaved->Transforms[frameOffset + patch.KeptTrackIndices[i]];
            }

            if (!TryCompactAnnotationTracksForStrippedTransforms(animation, patch.KeptTrackIndices, oldTrackCount, allocatedBuffers, out failure))
                return false;

            interleaved->Transforms = CreateOwnedArray<hkQsTransformf>(compactedTransforms.AsSpan(0, compactedTransformCount), allocatedBuffers);
            animation->NumberOfTransformTracks = patch.NewTrackCount;
            rebuiltAnimation = animation;
            return true;
        }
        finally
        {
            ArrayPool<hkQsTransformf>.Shared.Return(compactedTransforms);
        }
    }

    private static unsafe bool TryValidateInterleavedFrameLayout(hkaAnimation* animation, int bindingIndex, out string failure)
    {
        failure = string.Empty;

        if (animation == null)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had a null interleaved animation"
                : "Interleaved animation pointer was null";
            return false;
        }

        if (animation->Type != hkaAnimation.AnimationType.InterleavedAnimation)
            return true;

        var interleaved = (HkaInterleavedUncompressedAnimation*)animation;
        if (animation->NumberOfTransformTracks <= 0)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had no transform tracks after interleaved rebuild"
                : "Interleaved animation had no transform tracks";
            return false;
        }

        if (interleaved->Transforms.Length <= 0 || interleaved->Transforms.Length % animation->NumberOfTransformTracks != 0)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had an invalid interleaved transform buffer length {interleaved->Transforms.Length} for track count {animation->NumberOfTransformTracks}"
                : $"Interleaved transform buffer length {interleaved->Transforms.Length} was not divisible by track count {animation->NumberOfTransformTracks}";
            return false;
        }

        var frameCount = interleaved->Transforms.Length / animation->NumberOfTransformTracks;
        if (frameCount <= 0)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had no animation frames after interleaved rebuild"
                : "Interleaved animation had no animation frames";
            return false;
        }

        if (animation->NumberOfFloatTracks <= 0)
        {
            if (interleaved->Floats.Length != 0)
            {
                failure = bindingIndex >= 0
                    ? $"Patched PAP binding {bindingIndex} kept float samples without any float tracks"
                    : "Interleaved animation had float samples without any float tracks";
                return false;
            }

            return true;
        }

        if (interleaved->Floats.Length <= 0 || interleaved->Floats.Length % animation->NumberOfFloatTracks != 0)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had an invalid float buffer length {interleaved->Floats.Length} for float track count {animation->NumberOfFloatTracks}"
                : $"Interleaved float buffer length {interleaved->Floats.Length} was not divisible by float track count {animation->NumberOfFloatTracks}";
            return false;
        }

        var floatFrameCount = interleaved->Floats.Length / animation->NumberOfFloatTracks;
        if (floatFrameCount != frameCount)
        {
            failure = bindingIndex >= 0
                ? $"Patched PAP binding {bindingIndex} had mismatched transform frame count {frameCount} and float frame count {floatFrameCount}"
                : $"Interleaved transform frame count {frameCount} did not match float frame count {floatFrameCount}";
            return false;
        }

        return true;
    }

    private static unsafe bool TryValidateBindingSideArrayCounts(hkaAnimationBinding* binding, hkaAnimation* animation, int bindingIndex, out string failure)
    {
        failure = string.Empty;

        if (binding == null || animation == null)
            return true;

        var expectedTransformTrackCount = animation->NumberOfTransformTracks;
        var expectedCombinedTrackCount = expectedTransformTrackCount + Math.Max(0, animation->NumberOfFloatTracks);

        if (animation->AnnotationTracks.Length > 0
            && animation->AnnotationTracks.Length != expectedTransformTrackCount
            && animation->AnnotationTracks.Length != expectedCombinedTrackCount)
        {
            failure = $"PAP binding {bindingIndex} had an invalid annotation track count {animation->AnnotationTracks.Length} for transform/float track counts {expectedTransformTrackCount}/{animation->NumberOfFloatTracks}";
            return false;
        }

        if (binding->PartitionIndices.Length > 0
            && binding->PartitionIndices.Length != expectedTransformTrackCount
            && binding->PartitionIndices.Length != expectedCombinedTrackCount)
        {
            failure = $"PAP binding {bindingIndex} had an invalid partition index count {binding->PartitionIndices.Length} for transform/float track counts {expectedTransformTrackCount}/{animation->NumberOfFloatTracks}";
            return false;
        }

        return true;
    }

    private static unsafe bool TryCompactAnnotationTracksForStrippedTransforms(hkaAnimation* animation, ReadOnlySpan<int> keptTrackIndices, int oldTrackCount, List<nint> allocatedBuffers, out string failure)
    {
        failure = string.Empty;

        var annotationTrackCount = animation->AnnotationTracks.Length;
        if (annotationTrackCount == 0)
            return true;

        if (annotationTrackCount != oldTrackCount && annotationTrackCount != oldTrackCount + animation->NumberOfFloatTracks)
        {
            failure = $"Annotation track count {annotationTrackCount} did not align with transform/float track counts ({oldTrackCount}, {animation->NumberOfFloatTracks})";
            return false;
        }

        var compactedAnnotationTrackCount = checked(keptTrackIndices.Length + Math.Max(0, annotationTrackCount - oldTrackCount));
        var compactedAnnotationTracks = ArrayPool<hkaAnnotationTrack>.Shared.Rent(compactedAnnotationTrackCount);

        try
        {
            var compactedIndex = 0;
            for (int i = 0; i < keptTrackIndices.Length; i++)
                compactedAnnotationTracks[compactedIndex++] = animation->AnnotationTracks[keptTrackIndices[i]];

            for (int i = oldTrackCount; i < annotationTrackCount; i++)
                compactedAnnotationTracks[compactedIndex++] = animation->AnnotationTracks[i];

            animation->AnnotationTracks = CreateOwnedArray<hkaAnnotationTrack>(compactedAnnotationTracks.AsSpan(0, compactedAnnotationTrackCount), allocatedBuffers);
            return true;
        }
        finally
        {
            ArrayPool<hkaAnnotationTrack>.Shared.Return(compactedAnnotationTracks);
        }
    }

    private static unsafe bool TryCompactBindingPartitionIndices(hkaAnimationBinding* binding, ReadOnlySpan<int> keptTrackIndices, int oldTrackCount, int floatTrackCount, List<nint> allocatedBuffers, out string failure)
    {
        failure = string.Empty;

        var partitionCount = binding->PartitionIndices.Length;
        if (partitionCount == 0)
            return true;

        if (partitionCount != oldTrackCount && partitionCount != oldTrackCount + floatTrackCount)
        {
            failure = $"Partition index count {partitionCount} did not align with transform/float track counts ({oldTrackCount}, {floatTrackCount})";
            return false;
        }

        var compactedPartitionCount = checked(keptTrackIndices.Length + Math.Max(0, partitionCount - oldTrackCount));
        var compactedPartitionIndices = ArrayPool<short>.Shared.Rent(compactedPartitionCount);

        try
        {
            var compactedIndex = 0;
            for (int i = 0; i < keptTrackIndices.Length; i++)
                compactedPartitionIndices[compactedIndex++] = binding->PartitionIndices[keptTrackIndices[i]];

            for (int i = oldTrackCount; i < partitionCount; i++)
                compactedPartitionIndices[compactedIndex++] = binding->PartitionIndices[i];

            binding->PartitionIndices = CreateOwnedArray<short>(compactedPartitionIndices.AsSpan(0, compactedPartitionCount), allocatedBuffers);
            return true;
        }
        finally
        {
            ArrayPool<short>.Shared.Return(compactedPartitionIndices);
        }
    }

    private static bool IsSupportedSenderSideRebuildAnimationType(hkaAnimation.AnimationType animationType)
    {
        return animationType is hkaAnimation.AnimationType.SplineCompressedAnimation
            or hkaAnimation.AnimationType.PredictiveCompressedAnimation
            or hkaAnimation.AnimationType.QuantizedCompressedAnimation;
    }

    private static HavokInterleavedConversionCtorDelegate? ResolveHavokInterleavedConversionCtor(ISigScanner sigScanner)
    {
        try
        {
            var ctorAddress = sigScanner.ScanText(HavokInterleavedConversionCtorSig);
            return Marshal.GetDelegateForFunctionPointer<HavokInterleavedConversionCtorDelegate>(ctorAddress);
        }
        catch
        {
            return null;
        }
    }

    private static bool RequiresDummySkeletonForCompressedConversion(hkaAnimation.AnimationType animationType)
    {
        return animationType is hkaAnimation.AnimationType.PredictiveCompressedAnimation
            or hkaAnimation.AnimationType.QuantizedCompressedAnimation;
    }

    private static unsafe bool TryEnsureCompressedAnimationSkeleton(hkaAnimationBinding* binding,hkaAnimation* animation,TargetSkeletonSnapshot targetSkeleton,List<nint> allocatedBuffers,out hkaSkeleton* injectedSkeleton,out string failure)
    {
        injectedSkeleton = null;
        failure = string.Empty;

        if (animation == null)
        {
            failure = "Compressed animation pointer was null";
            return false;
        }

        if (!RequiresDummySkeletonForCompressedConversion(animation->Type))
            return true;

        var skeletonField = GetCompressedAnimationSkeletonField(animation);
        if (skeletonField == null)
        {
            failure = $"Compressed animation type '{animation->Type}' did not expose a skeleton field for conversion";
            return false;
        }

        if (*skeletonField != null)
            return true;

        if (targetSkeleton == null || targetSkeleton.ReferencePoseByIndex.Count <= 0)
        {
            failure = $"Compressed animation rebuild could not inject a dummy target skeleton because '{targetSkeleton?.SkeletonName ?? "<unknown>"}' had no reference pose data";
            return false;
        }

        var requiredBoneCount = Math.Max(1, GetMaxReferencedBoneIndex(binding) + 1);
        injectedSkeleton = AllocateDummyCompressedAnimationSkeleton(targetSkeleton.ReferencePoseByIndex, requiredBoneCount, allocatedBuffers);
        if (injectedSkeleton == null)
        {
            failure = "Compressed animation rebuild could not allocate a dummy target skeleton for conversion";
            return false;
        }

        *skeletonField = injectedSkeleton;
        return true;
    }

    private static unsafe hkaSkeleton** GetCompressedAnimationSkeletonField(hkaAnimation* animation)
    {
        if (animation == null)
            return null;

        return animation->Type switch
        {
            hkaAnimation.AnimationType.PredictiveCompressedAnimation => &((HkaPredictiveCompressedAnimation*)animation)->Skeleton,
            hkaAnimation.AnimationType.QuantizedCompressedAnimation => &((HkaQuantizedCompressedAnimation*)animation)->Skeleton,
            _ => null,
        };
    }

    private static unsafe hkaSkeleton* AllocateDummyCompressedAnimationSkeleton(IReadOnlyList<hkQsTransformf> targetReferencePoseByIndex,int requiredBoneCount,List<nint> allocatedBuffers)
    {
        requiredBoneCount = Math.Max(1, requiredBoneCount);

        var skeleton = (hkaSkeleton*)Marshal.AllocHGlobal(sizeof(hkaSkeleton));
        new Span<byte>(skeleton, sizeof(hkaSkeleton)).Clear();
        allocatedBuffers.Add((nint)skeleton);

        var poseBuffer = (hkQsTransformf*)Marshal.AllocHGlobal(requiredBoneCount * sizeof(hkQsTransformf));
        new Span<byte>(poseBuffer, requiredBoneCount * sizeof(hkQsTransformf)).Clear();
        allocatedBuffers.Add((nint)poseBuffer);

        var identityTransform = new hkQsTransformf
        {
            Translation = new hkVector4f { X = 0f, Y = 0f, Z = 0f, W = 0f },
            Rotation = new hkQuaternionf { X = 0f, Y = 0f, Z = 0f, W = 1f },
            Scale = new hkVector4f { X = 1f, Y = 1f, Z = 1f, W = 0f },
        };

        var sharedCount = Math.Min(targetReferencePoseByIndex.Count, requiredBoneCount);
        for (int i = 0; i < sharedCount; i++)
            poseBuffer[i] = targetReferencePoseByIndex[i];

        for (int i = sharedCount; i < requiredBoneCount; i++)
            poseBuffer[i] = identityTransform;

        skeleton->ReferencePose = new hkArray<hkQsTransformf>
        {
            Data = poseBuffer,
            Length = requiredBoneCount,
            CapacityAndFlags = requiredBoneCount | unchecked((int)hkArray<hkQsTransformf>.hkArrayFlags.DontDeallocate),
        };

        return skeleton;
    }

    private static unsafe void ClearInjectedCompressedAnimationSkeleton(hkaAnimation* animation, hkaSkeleton* injectedSkeleton)
    {
        if (animation == null || injectedSkeleton == null)
            return;

        var skeletonField = GetCompressedAnimationSkeletonField(animation);
        if (skeletonField != null && *skeletonField == injectedSkeleton)
            *skeletonField = null;
    }

    private static unsafe int GetMaxReferencedBoneIndex(hkaAnimationBinding* binding)
    {
        if (binding == null)
            return -1;

        var transformTracks = binding->TransformTrackToBoneIndices;
        var maxReferencedIndex = -1;
        for (int trackIndex = 0; trackIndex < transformTracks.Length; trackIndex++)
        {
            var boneIndex = transformTracks[trackIndex];
            if (boneIndex > maxReferencedIndex)
                maxReferencedIndex = boneIndex;
        }

        return maxReferencedIndex;
    }


    private static unsafe bool HasSkeletonMarkerBone(hkaSkeleton* skeleton, string markerBoneName)
    {
        if (skeleton == null || string.IsNullOrWhiteSpace(markerBoneName))
            return false;

        for (int boneIndex = 0; boneIndex < skeleton->Bones.Length; boneIndex++)
        {
            var boneName = skeleton->Bones[boneIndex].Name.String;
            if (string.IsNullOrWhiteSpace(boneName))
                continue;

            if (string.Equals(XivSkeletonIdentity.NormalizeSkeletonKey(boneName), markerBoneName, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static unsafe hkArray<T> CreateOwnedArray<T>(ReadOnlySpan<T> data, List<nint> allocatedBuffers) where T : unmanaged
    {
        var count = data.Length;
        T* buffer = null;

        if (count > 0)
        {
            var bytes = checked(count * sizeof(T));
            buffer = (T*)Marshal.AllocHGlobal(bytes);
            data.CopyTo(new Span<T>(buffer, count));
            allocatedBuffers.Add((nint)buffer);
        }

        return new hkArray<T>
        {
            Data = buffer,
            Length = count,
            CapacityAndFlags = count | unchecked((int)hkArray<T>.hkArrayFlags.DontDeallocate),
        };
    }

    private static Dictionary<string, TargetSkeletonSnapshot> BuildTargetLookup(IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons)
    {
        var output = new Dictionary<string, TargetSkeletonSnapshot>(StringComparer.Ordinal);
        foreach (var skeleton in targetSkeletons.OrderByDescending(static s => s.BoneCount))
        {
            AddTargetLookupEntry(output, skeleton.NormalizedSkeletonName, skeleton);
            AddTargetLookupEntry(output, skeleton.NormalizedResourceName, skeleton);
            AddTargetLookupEntry(output, skeleton.HumanAnimationFamilyKey, skeleton);
        }

        return output;
    }

    private static TargetSkeletonSnapshot? ResolveTargetSkeleton(string originalSkeletonName, string normalizedOriginalSkeleton, IReadOnlyDictionary<string, TargetSkeletonSnapshot> targetLookup, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, int maxReferencedIndex)
    {
        if (!string.IsNullOrWhiteSpace(normalizedOriginalSkeleton) && targetLookup.TryGetValue(normalizedOriginalSkeleton, out var exact))
            return exact;

        var markerBoneName = ExtractBindingMarkerBoneName(originalSkeletonName);
        if (!string.IsNullOrWhiteSpace(markerBoneName))
        {
            var markerMatch = ChooseBestTargetSkeletonCandidate(
                targetSkeletons.Where(snapshot => snapshot.BoneNameToIndex.ContainsKey(markerBoneName)),
                maxReferencedIndex);
            if (markerMatch != null)
                return markerMatch;
        }

        var humanAnimationFamilyKey = XivSkeletonIdentity.NormalizeHumanAnimationFamilyKey(originalSkeletonName);
        if (!string.IsNullOrWhiteSpace(humanAnimationFamilyKey))
        {
            var familyMatch = ChooseBestTargetSkeletonCandidate(
                targetSkeletons.Where(snapshot => string.Equals(snapshot.HumanAnimationFamilyKey, humanAnimationFamilyKey, StringComparison.Ordinal)),
                maxReferencedIndex);
            if (familyMatch != null)
                return familyMatch;
        }

        if (!string.IsNullOrWhiteSpace(normalizedOriginalSkeleton))
        {
            var fuzzyMatch = ChooseBestTargetSkeletonCandidate(
                targetSkeletons.Where(target =>
                    target.NormalizedSkeletonName.Contains(normalizedOriginalSkeleton, StringComparison.Ordinal)
                    || normalizedOriginalSkeleton.Contains(target.NormalizedSkeletonName, StringComparison.Ordinal)
                    || target.NormalizedResourceName.Contains(normalizedOriginalSkeleton, StringComparison.Ordinal)
                    || normalizedOriginalSkeleton.Contains(target.NormalizedResourceName, StringComparison.Ordinal)),
                maxReferencedIndex);
            if (fuzzyMatch != null)
                return fuzzyMatch;
        }

        var preferredHumanAnimation = ChooseBestTargetSkeletonCandidate(
            targetSkeletons.Where(static s => s.IsHumanAnimationSkeleton),
            maxReferencedIndex);
        if (preferredHumanAnimation != null)
            return preferredHumanAnimation;

        if (targetSkeletons.Count == 1)
            return targetSkeletons[0];

        return null;
    }

    private static string ExtractBindingMarkerBoneName(string? skeletonName)
    {
        if (string.IsNullOrWhiteSpace(skeletonName))
            return string.Empty;

        var normalized = skeletonName.Replace('\\', '/').Trim();
        var lastColon = normalized.LastIndexOf(':');
        if (lastColon >= 0 && lastColon < normalized.Length - 1)
            return normalized[(lastColon + 1)..].Trim().ToLowerInvariant();

        return XivSkeletonIdentity.NormalizeSkeletonKey(normalized);
    }

    private static TargetSkeletonSnapshot? ChooseBestTargetSkeletonCandidate(IEnumerable<TargetSkeletonSnapshot> candidates, int maxReferencedIndex)
    {
        TargetSkeletonSnapshot? bestAdequate = null;
        TargetSkeletonSnapshot? bestFallback = null;

        foreach (var candidate in candidates)
        {
            if (candidate == null || candidate.BoneCount <= 0)
                continue;

            if (candidate.BoneCount > maxReferencedIndex)
            {
                if (bestAdequate == null || candidate.BoneCount < bestAdequate.BoneCount)
                    bestAdequate = candidate;
            }
            else
            {
                if (bestFallback == null || candidate.BoneCount > bestFallback.BoneCount)
                    bestFallback = candidate;
            }
        }

        return bestAdequate ?? bestFallback;
    }

    private static unsafe Dictionary<string, IReadOnlyList<string?>> BuildSourceSkeletonLookup(hkaAnimationContainer* animationContainer)
    {
        var output = new Dictionary<string, IReadOnlyList<string?>>(StringComparer.Ordinal);

        var skeletonCount = animationContainer->Skeletons.Length;
        if (skeletonCount <= 0 || skeletonCount > 64)
            return output;

        for (int skeletonIndex = 0; skeletonIndex < skeletonCount; skeletonIndex++)
        {
            var skeletonRef = animationContainer->Skeletons[skeletonIndex];
            var skeleton = skeletonRef.ptr;
            if (skeleton == null)
                continue;

            var skeletonName = skeleton->Name.String;
            if (string.IsNullOrWhiteSpace(skeletonName))
                continue;

            var normalizedSkeletonName = XivSkeletonIdentity.NormalizeSkeletonKey(skeletonName);
            if (string.IsNullOrWhiteSpace(normalizedSkeletonName))
                continue;

            var boneCount = skeleton->Bones.Length;
            if (boneCount <= 0 || boneCount > 4096)
                continue;

            var boneNames = new string?[boneCount];
            for (int boneIndex = 0; boneIndex < boneCount; boneIndex++)
            {
                var boneName = skeleton->Bones[boneIndex].Name.String;
                if (string.IsNullOrWhiteSpace(boneName))
                    continue;

                boneNames[boneIndex] = boneName;
            }

            AddSourceSkeletonLookupEntry(output, normalizedSkeletonName, boneNames);
            AddSourceSkeletonLookupEntry(output, XivSkeletonIdentity.NormalizeHumanAnimationFamilyKey(normalizedSkeletonName), boneNames);
        }

        return output;
    }

    private static PapContainerReadStatus TryReadPapContainer(string filePath, out PapContainerData papContainer, out string failure)
    {
        papContainer = default!;
        failure = string.Empty;

        byte[] bytes;
        try
        {
            bytes = File.ReadAllBytes(filePath);
        }
        catch (Exception ex)
        {
            failure = $"Could not read PAP from disk: {ex.Message}";
            return PapContainerReadStatus.Invalid;
        }

        if (bytes.Length < 64)
        {
            failure = "PAP file was too small to be valid";
            return PapContainerReadStatus.Invalid;
        }

        using var reader = new BinaryReader(new MemoryStream(bytes, writable: false));

        if (reader.ReadUInt32() != 0x20706170)
        {
            failure = "PAP magic was invalid";
            return PapContainerReadStatus.Invalid;
        }

        reader.ReadUInt16();
        reader.ReadUInt16();
        reader.ReadUInt16();
        reader.ReadUInt16();

        byte type = reader.ReadByte();
        reader.ReadByte();

        int headerSize = reader.ReadInt32();
        int havokOffset = reader.ReadInt32();
        int footerOffset = reader.ReadInt32();

        if (headerSize <= 0 || headerSize > havokOffset)
        {
            failure = "PAP header contained an invalid header size";
            return PapContainerReadStatus.Invalid;
        }

        if (havokOffset <= 0 || footerOffset <= 0 || footerOffset <= havokOffset)
        {
            failure = "PAP header contained invalid Havok offsets";
            return PapContainerReadStatus.Invalid;
        }

        if (havokOffset >= bytes.Length || footerOffset > bytes.Length)
        {
            failure = "PAP offsets pointed outside the file";
            return PapContainerReadStatus.Invalid;
        }

        int havokSize = footerOffset - havokOffset;
        if (havokSize <= 32 || havokSize > MaxHavokBytes)
        {
            failure = "PAP Havok section had an invalid size";
            return PapContainerReadStatus.Invalid;
        }

        var havokBytes = new byte[havokSize];
        Buffer.BlockCopy(bytes, havokOffset, havokBytes, 0, havokSize);

        if (!LooksLikeSupportedHavokPayload(havokBytes))
        {
            failure = "PAP Havok section format is not supported by the sanitizer, so the original PAP was left untouched";
            return PapContainerReadStatus.PassThroughOriginal;
        }

        var headerBytes = new byte[havokOffset];
        Buffer.BlockCopy(bytes, 0, headerBytes, 0, havokOffset);

        var footerBytes = new byte[bytes.Length - footerOffset];
        Buffer.BlockCopy(bytes, footerOffset, footerBytes, 0, footerBytes.Length);

        papContainer = new PapContainerData(headerSize, havokOffset, footerOffset, type == 0, headerBytes, havokBytes, footerBytes);
        return PapContainerReadStatus.Valid;
    }


    private static bool LooksLikeSupportedHavokPayload(byte[] havokBytes)
    {
        if (havokBytes.Length < 32)
            return false;

        ReadOnlySpan<byte> tagfileSignature = stackalloc byte[] { 0x1E, 0x0D, 0xB0, 0xCA, 0xCE, 0xFA, 0x11, 0xD0 };
        if (!havokBytes.AsSpan(0, tagfileSignature.Length).SequenceEqual(tagfileSignature))
            return false;

        var versionMarker = ExtractHkxVersionMarker(havokBytes);
        if (!versionMarker.StartsWith("hk_", StringComparison.Ordinal))
            return false;

        return ContainsAsciiToken(havokBytes, "hkRootLevelContainer")
            && ContainsAsciiToken(havokBytes, "hkaAnimationContainer");
    }

    private static bool ContainsAsciiToken(byte[] data, string token)
    {
        if (data.Length == 0 || string.IsNullOrEmpty(token))
            return false;

        return data.AsSpan().IndexOf(Encoding.ASCII.GetBytes(token)) >= 0;
    }

    private static byte[] BuildPapFromRewrittenHkx(PapContainerData originalPap, byte[] rewrittenHkx)
    {
        var output = new byte[originalPap.HeaderBytes.Length + rewrittenHkx.Length + originalPap.FooterBytes.Length];
        Buffer.BlockCopy(originalPap.HeaderBytes, 0, output, 0, originalPap.HeaderBytes.Length);
        Buffer.BlockCopy(rewrittenHkx, 0, output, originalPap.HeaderBytes.Length, rewrittenHkx.Length);
        Buffer.BlockCopy(originalPap.FooterBytes, 0, output, originalPap.HeaderBytes.Length + rewrittenHkx.Length, originalPap.FooterBytes.Length);

        BinaryPrimitives.WriteInt32LittleEndian(output.AsSpan(22, sizeof(int)), originalPap.HavokOffset + rewrittenHkx.Length);
        return output;
    }

    private bool TryMaterializePapToCache(byte[] papBytes, string originalHash, string skeletonFingerprint, int remappedTracks, int droppedTracks, int bindingCount, string reason, out string rewrittenHash, out string rewrittenPath, out string failure)
    {
        rewrittenHash = string.Empty;
        rewrittenPath = string.Empty;
        failure = string.Empty;

        Directory.CreateDirectory(GetGeneratedPapFolder());

        string tempPath = Path.Combine(GetGeneratedPapFolder(), Guid.NewGuid().ToString("N") + ".pap.tmp");
        try
        {
            File.WriteAllBytes(tempPath, papBytes);
            rewrittenHash = tempPath.GetFileHash();
            rewrittenPath = GetGeneratedPapPath(rewrittenHash);

            if (!File.Exists(rewrittenPath))
            {
                File.Move(tempPath, rewrittenPath);
            }
            else
            {
                File.Delete(tempPath);
            }

            var metadata = new GeneratedPapMetadata(
                SanitizerVersion,
                originalHash,
                rewrittenHash,
                skeletonFingerprint,
                remappedTracks,
                droppedTracks,
                bindingCount,
                reason,
                DateTimeOffset.UtcNow);

            File.WriteAllText(GetGeneratedPapMetadataPath(rewrittenHash), JsonSerializer.Serialize(metadata, _jsonOptions));

            var cacheEntry = _fileCacheManager.CreateCacheEntry(rewrittenPath);
            if (cacheEntry == null)
            {
                failure = "Sanitized PAP could not be registered in cache";
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            failure = ex.Message;
            return false;
        }
        finally
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch
            {
            }
        }
    }

    private unsafe bool TryVerifyMaterializedPap(string rewrittenPath, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, int expectedBindingCount, out string failure)
    {
        failure = string.Empty;

        var readStatus = TryReadPapContainer(rewrittenPath, out var papContainer, out var readFailure);
        if (readStatus != PapContainerReadStatus.Valid)
        {
            failure = readStatus == PapContainerReadStatus.PassThroughOriginal
                ? "Rewritten PAP verification produced an unsupported Havok payload"
                : readFailure;
            return false;
        }

        var tempHkxPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) + ".hkx";
        File.WriteAllBytes(tempHkxPath, papContainer.HavokBytes);
        var tempHkxPathAnsi = Marshal.StringToHGlobalAnsi(tempHkxPath);

        try
        {
            unsafe
            {
                var loadOptions = stackalloc hkSerializeUtil.LoadOptions[1];
                loadOptions->TypeInfoRegistry = hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry();
                loadOptions->ClassNameRegistry = hkBuiltinTypeRegistry.Instance()->GetClassNameRegistry();
                loadOptions->Flags = new hkFlags<hkSerializeUtil.LoadOptionBits, int>
                {
                    Storage = (int)hkSerializeUtil.LoadOptionBits.Default
                };

                var resource = hkSerializeUtil.LoadFromFile((byte*)tempHkxPathAnsi, null, loadOptions);
                if (resource == null)
                {
                    failure = "Rewritten PAP verification could not reload the emitted Havok payload";
                    return false;
                }

                var rootLevelName = "hkRootLevelContainer"u8;
                fixed (byte* rootName = rootLevelName)
                {
                    var container = (hkRootLevelContainer*)resource->GetContentsPointer(rootName, hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry());
                    if (container == null)
                    {
                        failure = "Rewritten PAP verification could not resolve the Havok root container";
                        return false;
                    }

                    var animationName = "hkaAnimationContainer"u8;
                    fixed (byte* animName = animationName)
                    {
                        var animationContainer = (hkaAnimationContainer*)container->findObjectByName(animName, null);
                        if (animationContainer == null)
                        {
                            failure = "Rewritten PAP verification could not resolve the animation container";
                            return false;
                        }

                        if (!TryValidatePatchedBindingsInMemory(animationContainer, targetSkeletons, expectedBindingCount, out failure))
                            return false;
                    }
                }
            }
        }
        finally
        {
            Marshal.FreeHGlobal(tempHkxPathAnsi);
            try { File.Delete(tempHkxPath); } catch { }
        }

        return true;
    }

    private void DeleteGeneratedPapArtifacts(string generatedHash, string generatedPath)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(generatedPath) && File.Exists(generatedPath))
                File.Delete(generatedPath);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not delete rejected generated PAP artifact {path}", generatedPath);
        }

        try
        {
            if (!string.IsNullOrWhiteSpace(generatedHash))
            {
                var metadataPath = GetGeneratedPapMetadataPath(generatedHash);
                if (File.Exists(metadataPath))
                    File.Delete(metadataPath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not delete rejected generated PAP sidecar for {hash}", generatedHash);
        }
    }

    private static unsafe bool TrySaveResourceToHkx(hkResource* resource, out byte[] rewrittenHkx, out string failure)
    {
        rewrittenHkx = Array.Empty<byte>();
        failure = string.Empty;

        string tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) + ".hkx";
        var tempFileAnsi = Marshal.StringToHGlobalAnsi(tempFile);

        try
        {
            var rootLevelName = "hkRootLevelContainer"u8;
            fixed (byte* rootName = rootLevelName)
            {
                var rootPointer = (hkRootLevelContainer*)resource->GetContentsPointer(rootName, hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry());
                if (rootPointer == null)
                {
                    failure = "Havok root pointer was unavailable during save";
                    return false;
                }

                var rootClass = hkBuiltinTypeRegistry.Instance()->GetClassNameRegistry()->GetClassByName(rootName);
                if (rootClass == null)
                {
                    failure = "Havok root class was unavailable during save";
                    return false;
                }

                hkOstream* outputStream = stackalloc hkOstream[1];
                outputStream->Ctor((byte*)tempFileAnsi);

                try
                {
                    hkResult* result = stackalloc hkResult[1];
                    var options = new hkSerializeUtil.SaveOptions
                    {
                        Flags = new hkFlags<hkSerializeUtil.SaveOptionBits, int>
                        {
                            Storage = (int)hkSerializeUtil.SaveOptionBits.Default
                        }
                    };

                    hkSerializeUtil.Save(result, rootPointer, rootClass, outputStream->StreamWriter.ptr, options);
                    if (result->Result != hkResult.hkResultEnum.Success)
                    {
                        failure = "Havok serializer returned failure";
                        return false;
                    }
                }
                finally
                {
                    outputStream->Dtor();
                }
            }

            rewrittenHkx = File.ReadAllBytes(tempFile);
            if (rewrittenHkx.Length == 0)
            {
                failure = "Havok serializer returned an empty HKX payload";
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            failure = ex.Message;
            return false;
        }
        finally
        {
            Marshal.FreeHGlobal(tempFileAnsi);
            try { File.Delete(tempFile); } catch { }
        }
    }


    private unsafe bool TryValidatePatchedBindingsInMemory(hkaAnimationContainer* animationContainer, IReadOnlyList<TargetSkeletonSnapshot> targetSkeletons, int expectedBindingCount, out string failure)
    {
        failure = string.Empty;

        if (animationContainer == null)
        {
            failure = "Animation container was null during in-memory verification";
            return false;
        }

        var actualBindingCount = animationContainer->Bindings.Length;
        if (expectedBindingCount > 0 && actualBindingCount != expectedBindingCount)
        {
            failure = $"Patched animation binding count changed unexpectedly ({expectedBindingCount} -> {actualBindingCount})";
            return false;
        }

        for (int bindingIndex = 0; bindingIndex < actualBindingCount; bindingIndex++)
        {
            var binding = animationContainer->Bindings[bindingIndex].ptr;
            if (binding == null)
                continue;

            var animation = (hkaAnimation*)binding->Animation.ptr;
            if (animation == null)
            {
                failure = $"Patched PAP binding {bindingIndex} lost its animation reference";
                return false;
            }

            if (animation->NumberOfTransformTracks != binding->TransformTrackToBoneIndices.Length)
            {
                failure = $"Patched PAP binding {bindingIndex} track map length {binding->TransformTrackToBoneIndices.Length} did not match animation track count {animation->NumberOfTransformTracks}";
                return false;
            }

            if (animation->Type == hkaAnimation.AnimationType.InterleavedAnimation
                && !TryValidateInterleavedFrameLayout(animation, bindingIndex, out failure))
            {
                return false;
            }

            if (!TryValidateBindingSideArrayCounts(binding, animation, bindingIndex, out failure))
                return false;
        }

        var postAnalysis = AnalyzeAnimationBindings(animationContainer, targetSkeletons, "<in-memory-patched-pap>");
        if (postAnalysis.Blocked)
        {
            failure = "Patched PAP still failed target-skeleton verification: " + postAnalysis.Reason;
            return false;
        }

        if (postAnalysis.Changed)
        {
            failure = "Patched PAP still required additional skeleton pruning before serialization";
            return false;
        }

        return true;
    }

    private static bool HasCompatibleHkxSignature(byte[] originalHkx, byte[] rewrittenHkx, out string failure)
    {
        failure = string.Empty;
        if (rewrittenHkx.Length < 32 || originalHkx.Length < 32)
        {
            failure = "Rewritten HKX payload was unexpectedly small";
            return false;
        }

        if (!originalHkx.AsSpan(0, 8).SequenceEqual(rewrittenHkx.AsSpan(0, 8)))
        {
            failure = "Rewritten HKX signature did not match the original Havok payload signature";
            return false;
        }

        if (!LooksLikeSupportedHavokPayload(rewrittenHkx))
        {
            failure = "Rewritten HKX payload no longer looked like a supported Havok tagfile payload";
            return false;
        }

        // The Havok serializer can legitimately normalize the tagfile version marker
        // when it re-emits the resource. Treating that as a hard failure would reject
        // otherwise valid rewritten payloads for no safety win.
        return true;
    }

    private static string ExtractHkxVersionMarker(byte[] hkx)
    {
        var max = Math.Min(hkx.Length, 64);
        for (int i = 0; i < max - 3; i++)
        {
            if (hkx[i] == (byte)'h' && hkx[i + 1] == (byte)'k' && hkx[i + 2] == (byte)'_')
            {
                int end = i + 3;
                while (end < max)
                {
                    var ch = hkx[end];
                    if (ch == 0 || ch < 0x20 || ch > 0x7E)
                        break;

                    end++;
                }

                return System.Text.Encoding.ASCII.GetString(hkx, i, end - i);
            }
        }

        return string.Empty;
    }

    private string GetGeneratedPapFolder()
        => Path.Combine(_fileCacheManager.CacheFolder, "_ravasync_pap_sanitizer");

    private string GetGeneratedPapPath(string hash)
        => Path.Combine(GetGeneratedPapFolder(), hash + ".pap");

    private string GetGeneratedPapMetadataPath(string hash)
        => Path.Combine(GetGeneratedPapFolder(), hash + ".pap.ravasync.json");
}
