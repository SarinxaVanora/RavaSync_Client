using Microsoft.Extensions.Logging;
using RavaSync.Interop.GameModel;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services.Gpu;
using System.Buffers;
using System.Buffers.Binary;
using System.Text.Json;
using System.Numerics;
using System.Runtime.InteropServices;
using static Lumina.Data.Parsing.MdlStructs;
using RavaSync.API.Data.Enum;

namespace RavaSync.Services.Optimisation;

public sealed record MeshOptimisationCandidate(ObjectKind ObjectKind, string Hash, IReadOnlyList<string> GamePaths, IReadOnlyList<string> FilePaths, long Triangles, long OriginalSize, long CompressedSize, long VramBytes, OptimisationTier Tier, string Rationale);

public sealed partial class MeshOptimisationService : IDisposable
{   
    private readonly ILogger<MeshOptimisationService> _logger;
    private readonly GpuDeviceService _gpuDeviceService;
    private readonly D3D11MeshAnalysisService _d3d11MeshAnalysisService;
    private readonly IpcManager _ipcManager;
    private readonly object _estimateCacheLock = new();
    private readonly object _historyLock = new();
    private readonly string _historyPath;
    private Dictionary<string, MeshOptimisationHistoryEntry> _history = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, MeshEstimateCacheEntry> _estimateCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, PendingEstimateRequest> _pendingEstimateRequests = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _pendingEstimateSignal = new(0);
    private readonly CancellationTokenSource _estimateWorkerCts = new();
    private readonly Task _estimateWorkerTask;
    private long _estimateGeneration;

    public readonly record struct MeshSavingsEstimate(long TrianglesBefore, long SavedTriangles, long TrianglesAfter, int ChangedMeshes, bool Success)
    {
        public bool HasSavings => Success && ChangedMeshes > 0 && SavedTriangles > 0;
    }

    private enum MeshCleanupMode : byte
    {
        Disabled = 0,
        IndexOnly = 1,
        FullCollapse = 2,
    }

    private enum ComponentDecimationMode : byte
    {
        SafeCloth = 0,
        BodyAware = 1,
        Pathological = 2,
    }

    private enum ComponentStopReason : byte
    {
        None = 0,
        TooSmall = 1,
        TooProtected = 2,
        TooNearBody = 3,
        BoundaryHeavy = 4,
        InteriorTooSparse = 5,
        TargetReached = 6,
        Stagnated = 7,
        NoEligibleCandidates = 8,
    }

    public MeshOptimisationService(ILogger<MeshOptimisationService> logger, GpuDeviceService gpuDeviceService, D3D11MeshAnalysisService d3d11MeshAnalysisService, IpcManager ipcManager, MareConfigService mareConfigService)
    {
        _logger = logger;
        _gpuDeviceService = gpuDeviceService;
        _d3d11MeshAnalysisService = d3d11MeshAnalysisService;
        _ipcManager = ipcManager;
        _historyPath = Path.Combine(mareConfigService.ConfigurationDirectory, "MeshCleanupHistory.json");
        LoadHistory();
        _estimateWorkerTask = Task.Factory.StartNew(() => ProcessEstimateQueueAsync(_estimateWorkerCts.Token), _estimateWorkerCts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    public long EstimateGeneration => Interlocked.Read(ref _estimateGeneration);

    public int PendingEstimateCount
    {
        get
        {
            lock (_estimateCacheLock)
            {
                return _pendingEstimateRequests.Count;
            }
        }
    }

    public bool ShouldOfferReduction(string primaryPath, long currentTriangles)
    {
        if (string.IsNullOrWhiteSpace(primaryPath) || !File.Exists(primaryPath))
            return false;

        if (!IsSupportedReductionPath(primaryPath))
            return false;

        lock (_historyLock)
        {
            if (TryNormaliseHistoryEntryLocked(primaryPath, currentTriangles, out bool alreadyOptimised))
                return !alreadyOptimised;
        }

        if (TryGetKnownEstimateActionability(primaryPath, out bool isActionable))
            return isActionable;

        return true;
    }

    private static bool IsSupportedReductionPath(string primaryPath)
    {
        if (string.IsNullOrWhiteSpace(primaryPath))
            return false;

        string normalized = primaryPath.Replace('\\', '/').ToLowerInvariant();
        if (!normalized.EndsWith(".mdl", StringComparison.Ordinal))
            return false;

        bool looksLikeGear = normalized.Contains("/equipment/") || normalized.Contains("/accessory/");
        if (!looksLikeGear)
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

    public async Task<bool> WarmupAsync(CancellationToken token)
    {
        using var job = _gpuDeviceService.TryBeginJob("MeshOptimisation.Warmup", token, out var reason);
        if (job == null)
        {
            _logger.LogDebug("Skipping mesh optimisation warmup: {reason}", reason);
            return false;
        }

        bool gpuReady = await _d3d11MeshAnalysisService.WarmupAsync(token).ConfigureAwait(false);
        job.CompleteSuccess(gpuReady
            ? "Strict submesh-aware mesh cleanup is available, with D3D11 GPU triangle analysis for large meshes."
            : "Strict submesh-aware mesh cleanup is available for models with readable metadata.");
        return true;
    }

    public bool TryGetCachedEstimate(string primaryPath, out MeshSavingsEstimate estimate)
    {
        estimate = default;

        if (!TryGetEstimateFileInfo(primaryPath, out var info))
            return false;

        return TryGetCachedEstimate(info, primaryPath, out estimate);
    }

    private bool TryGetCachedEstimate(FileInfo info, string primaryPath, out MeshSavingsEstimate estimate)
    {
        estimate = default;

        lock (_estimateCacheLock)
        {
            if (_estimateCache.TryGetValue(primaryPath, out var cached)
                && cached.AlgorithmVersion == EstimateCacheVersion
                && cached.Length == info.Length
                && cached.LastWriteTimeUtcTicks == info.LastWriteTimeUtc.Ticks)
            {
                estimate = new MeshSavingsEstimate(cached.TrianglesBefore, cached.RemovableTriangles, Math.Max(0, cached.TrianglesAfter), cached.ChangedMeshes, cached.Success);
                return true;
            }
        }

        lock (_historyLock)
        {
            if (_history.TryGetValue(primaryPath, out var entry)
                && entry.MatchesEstimateFingerprint(info)
                && entry.TryGetEstimate(out estimate))
            {
                lock (_estimateCacheLock)
                {
                    _estimateCache[primaryPath] = new MeshEstimateCacheEntry(
                        info.Length,
                        info.LastWriteTimeUtc.Ticks,
                        estimate.TrianglesBefore,
                        estimate.SavedTriangles,
                        estimate.TrianglesAfter,
                        estimate.ChangedMeshes,
                        estimate.Success,
                        EstimateCacheVersion,
                        entry.LastEstimateFailureReason ?? string.Empty);
                }

                return true;
            }
        }

        return false;
    }

    public bool TryGetCachedEstimateFast(string primaryPath, out MeshSavingsEstimate estimate)
    {
        estimate = default;
        if (string.IsNullOrWhiteSpace(primaryPath))
            return false;

        if (!TryGetEstimateFileInfo(primaryPath, out var info))
            return false;

        return TryGetCachedEstimate(info, primaryPath, out estimate);
    }

    public void QueueEstimate(string primaryPath, long currentTriangles = 0)
    {
        QueueEstimates([(primaryPath, currentTriangles)]);
    }

    public void QueueEstimates(IEnumerable<(string PrimaryPath, long CurrentTriangles)> requests)
    {
        if (requests == null)
            return;

        int queued = 0;
        lock (_estimateCacheLock)
        {
            foreach (var request in requests)
            {
                var primaryPath = request.PrimaryPath;
                if (string.IsNullOrWhiteSpace(primaryPath) || !File.Exists(primaryPath))
                    continue;

                if (!ShouldOfferReduction(primaryPath, request.CurrentTriangles))
                    continue;

                if (TryGetCachedEstimate(primaryPath, out _))
                    continue;

                var next = new PendingEstimateRequest(
                    primaryPath,
                    Math.Max(0, request.CurrentTriangles),
                    BuildEstimatePriority(primaryPath, request.CurrentTriangles),
                    DateTime.UtcNow.Ticks);

                if (_pendingEstimateRequests.TryGetValue(primaryPath, out var existing))
                {
                    if (existing.Priority >= next.Priority)
                        continue;
                }

                _pendingEstimateRequests[primaryPath] = next;
                queued++;
            }
        }

        for (int i = 0; i < queued; i++)
            _pendingEstimateSignal.Release();
    }


    public bool TryEstimateSavingsDetailed(string primaryPath, out MeshSavingsEstimate estimate)
        => TryEstimateSavingsDetailed(primaryPath, currentTriangles: 0, out estimate);

    public bool TryEstimateSavingsDetailed(string primaryPath, long currentTriangles, out MeshSavingsEstimate estimate)
    {
        estimate = default;

        if (!ShouldOfferReduction(primaryPath, currentTriangles))
        {
            if (TryGetSuppressedEstimateReason(primaryPath, currentTriangles, out var suppressedReason))
            {
                _logger.LogDebug("Mesh estimate not offered for {path}. Reason={reason}", primaryPath, suppressedReason);
            }

            return false;
        }

        if (!TryGetEstimateFileInfo(primaryPath, out var info))
        {
            _logger.LogDebug("Mesh estimate could not inspect {path}. Reason=Failed to stat model file.", primaryPath);
            return false;
        }

        if (TryGetCachedEstimate(info, primaryPath, out estimate))
        {
            if (!estimate.Success)
            {
                _logger.LogDebug("Mesh estimate returned no actionable cleanup for {path}. Reason={reason}", primaryPath,
                    TryGetLastEstimateFailureReason(primaryPath, out var cachedFailureReason) && !string.IsNullOrWhiteSpace(cachedFailureReason)
                        ? cachedFailureReason
                        : "Unknown estimate failure");
            }

            return estimate.Success;
        }

        var success = TryEstimateRemovableTriangles(primaryPath, out var removableTriangles, out var changedMeshes, out var trianglesBefore, out var failureReason);
        var trianglesAfter = Math.Max(0, trianglesBefore - removableTriangles);
        estimate = new MeshSavingsEstimate(trianglesBefore, removableTriangles, trianglesAfter, changedMeshes, success);

        lock (_estimateCacheLock)
        {
            _estimateCache[primaryPath] = new MeshEstimateCacheEntry(info.Length, info.LastWriteTimeUtc.Ticks, trianglesBefore, removableTriangles, trianglesAfter, changedMeshes, success, EstimateCacheVersion, failureReason ?? string.Empty);
        }

        UpdateEstimateHistory(primaryPath, info, estimate, failureReason);
        if (!success)
        {
            _logger.LogDebug("Mesh estimate returned no actionable cleanup for {path}. Reason={reason}", primaryPath, string.IsNullOrWhiteSpace(failureReason) ? "Unknown estimate failure" : failureReason);
        }

        Interlocked.Increment(ref _estimateGeneration);
        return success;
    }

    public void Dispose()
    {
        _estimateWorkerCts.Cancel();
        try
        {
            _pendingEstimateSignal.Release();
        }
        catch
        {
        }

        try
        {
            _estimateWorkerTask.Wait(TimeSpan.FromSeconds(1));
        }
        catch
        {
        }

        _estimateWorkerCts.Dispose();
        _pendingEstimateSignal.Dispose();
    }

    private async Task ProcessEstimateQueueAsync(CancellationToken token)
    {
        TrySetCurrentThreadToBackgroundEstimatePriority();

        while (!token.IsCancellationRequested)
        {
            try
            {
                await _pendingEstimateSignal.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            await WaitForEstimateQueueToSettleAsync(token).ConfigureAwait(false);

            while (TryDequeueNextEstimate(out var request))
            {
                try
                {
                    TryEstimateSavingsDetailed(request.PrimaryPath, request.CurrentTriangles, out _);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Background mesh estimate failed for {path}", request.PrimaryPath);
                }

                if (token.IsCancellationRequested)
                    break;

                if (BackgroundEstimatePauseMs > 0)
                {
                    try
                    {
                        await Task.Delay(BackgroundEstimatePauseMs, token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

        }
    }

    private bool TryDequeueNextEstimate(out PendingEstimateRequest request)
    {
        lock (_estimateCacheLock)
        {
            if (_pendingEstimateRequests.Count == 0)
            {
                request = default;
                return false;
            }

            request = _pendingEstimateRequests.Values
                .OrderByDescending(static v => v.Priority)
                .ThenBy(static v => v.QueuedUtcTicks)
                .First();

            _pendingEstimateRequests.Remove(request.PrimaryPath);
            return true;
        }
    }

    private async Task WaitForEstimateQueueToSettleAsync(CancellationToken token)
    {
        if (BackgroundEstimateSettleMs <= 0)
            return;

        long settleWindowTicks = TimeSpan.FromMilliseconds(BackgroundEstimateSettleMs).Ticks;
        while (!token.IsCancellationRequested)
        {
            long newestQueuedTicks;
            lock (_estimateCacheLock)
            {
                newestQueuedTicks = _pendingEstimateRequests.Count == 0
                    ? 0L
                    : _pendingEstimateRequests.Values.Max(static v => v.QueuedUtcTicks);
            }

            if (newestQueuedTicks <= 0)
                return;

            long ageTicks = DateTime.UtcNow.Ticks - newestQueuedTicks;
            if (ageTicks >= settleWindowTicks)
                return;

            int delayMs = (int)Math.Ceiling((settleWindowTicks - ageTicks) / (double)TimeSpan.TicksPerMillisecond);
            delayMs = Math.Clamp(delayMs, 10, BackgroundEstimateSettleMs);
            await Task.Delay(delayMs, token).ConfigureAwait(false);
        }
    }

    private const int WindowsThreadModeBackgroundBegin = 0x00010000;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetThreadPriority(IntPtr hThread, int nPriority);

    private static void TrySetCurrentThreadToBackgroundEstimatePriority()
    {
        try
        {
            Thread.CurrentThread.Priority = ThreadPriority.Lowest;
        }
        catch
        {
        }

        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                _ = SetThreadPriority(GetCurrentThread(), WindowsThreadModeBackgroundBegin);
        }
        catch
        {
        }
    }

    private static long BuildEstimatePriority(string primaryPath, long currentTriangles)
    {
        long trianglesBias = Math.Max(0, currentTriangles);
        long sizeBias = 0;
        try
        {
            sizeBias = new FileInfo(primaryPath).Length;
        }
        catch
        {
        }

        return checked((trianglesBias * 16L) + Math.Min(sizeBias, 32L * 1024L * 1024L));
    }

    public async Task RunPlannedOptimisationAsync(ILogger callerLogger,Dictionary<string, string[]> meshes,IProgress<(string fileName, int index)> progress,CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(callerLogger);
        ArgumentNullException.ThrowIfNull(meshes);
        ArgumentNullException.ThrowIfNull(progress);

        if (meshes.Count == 0)
        {
            callerLogger.LogDebug("Mesh optimisation requested with an empty mesh set.");
            return;
        }

        var workItems = BuildWorkItems(meshes);
        if (workItems.Count == 0)
        {
            callerLogger.LogDebug("Mesh optimisation produced no valid work items.");
            return;
        }

        var actionableWorkItems = new List<MeshWorkItem>(workItems.Count);
        foreach (var item in workItems)
        {
            token.ThrowIfCancellationRequested();

            if (!TryEstimateSavingsDetailed(item.PrimaryPath, out var estimate) || !estimate.HasSavings)
            {
                callerLogger.LogDebug("Mesh optimisation pre-check rejected {primary}: no current actionable savings.", item.PrimaryPath);
                continue;
            }

            actionableWorkItems.Add(item);
        }

        if (actionableWorkItems.Count == 0)
        {
            callerLogger.LogDebug("Mesh optimisation produced no currently actionable work items.");
            return;
        }

        workItems = actionableWorkItems;

        using var orchestrationJob = _gpuDeviceService.TryBeginJob("MeshOptimisation.AuthoritativeDriver", token, out var gpuReason);

        int currentIndex = 0;
        int completed = 0;
        int skipped = 0;
        long removedTriangles = 0;
        long removedBytes = 0;
        int changedMeshes = 0;

        foreach (var item in workItems)
        {
            token.ThrowIfCancellationRequested();
            progress.Report((Path.GetFileName(item.PrimaryPath), ++currentIndex));

            var result = await TryRunStrictCleanupAsync(item, token).ConfigureAwait(false);
            if (result.Succeeded)
            {
                completed++;
                removedTriangles += result.RemovedTriangles;
                removedBytes += result.RemovedBytes;
                changedMeshes += result.ChangedMeshes;
                RecordOptimisedMesh(item.PrimaryPath, result.TrianglesBefore, result.TrianglesAfter);
                InvalidateEstimateCacheFor(item);
                continue;
            }

            skipped++;
            callerLogger.LogInformation(
                "Mesh optimisation skipped {primary}. Reason={reason}",
                item.PrimaryPath,
                result.Detail);
        }

        if (completed > 0)
        {
            try
            {
                await _ipcManager.Penumbra.FinalizeTextureWriteAsync(nameof(MeshOptimisationService)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                callerLogger.LogWarning(ex, "Mesh optimisation redraw/finalisation failed after writing updated models.");
            }
        }

        orchestrationJob?.CompleteSuccess($"Mesh optimisation completed. UpdatedFiles={completed}, ChangedMeshes={changedMeshes}, RemovedTriangles={removedTriangles}, RemovedBytes={removedBytes}, Skipped={skipped}, GPUPath={(orchestrationJob != null ? "available" : $"unavailable ({gpuReason})")}");
    }


    private void InvalidateEstimateCacheFor(MeshWorkItem item)
    {
        lock (_estimateCacheLock)
        {
            _estimateCache.Remove(item.PrimaryPath);
            if (item.AlternatePaths is { Count: > 0 })
            {
                foreach (var alternate in item.AlternatePaths)
                {
                    if (!string.IsNullOrWhiteSpace(alternate))
                        _estimateCache.Remove(alternate);
                }
            }
        }

        ClearEstimateHistory(item.PrimaryPath);
        if (item.AlternatePaths is { Count: > 0 })
        {
            foreach (var alternate in item.AlternatePaths)
            {
                if (!string.IsNullOrWhiteSpace(alternate))
                    ClearEstimateHistory(alternate);
            }
        }

        Interlocked.Increment(ref _estimateGeneration);
    }

    private static List<MeshWorkItem> BuildWorkItems(IReadOnlyDictionary<string, string[]> meshes)
    {
        var result = new List<MeshWorkItem>(meshes.Count);
        foreach (var kv in meshes)
        {
            var primary = kv.Key;
            if (string.IsNullOrWhiteSpace(primary) || !File.Exists(primary))
                continue;

            if (!IsSupportedReductionPath(primary))
                continue;

            result.Add(new MeshWorkItem(primary, kv.Value ?? Array.Empty<string>()));
        }

        return result;
    }


    private const int MeshIndexCountFieldOffset = 4;
    private const int SubmeshIndexCountFieldOffset = 4;
    private const int FileHeaderVertexOffsetArrayOffset = 0x10;
    private const int FileHeaderIndexOffsetArrayOffset = 0x1C;
    private const int FileHeaderVertexBufferSizeArrayOffset = 0x28;
    private const int FileHeaderIndexBufferSizeArrayOffset = 0x34;

    private async Task<MeshExecutionResult> TryRunStrictCleanupAsync(MeshWorkItem item, CancellationToken token)
    {
        using var job = _gpuDeviceService.TryBeginJob("MeshOptimisation.Strict.SafeCleanup", token, out _);

        try
        {
            var mdl = new MdlFile(item.PrimaryPath);
            if (!TryValidateModelForRewrite(mdl, out var validationReason))
            {
                job?.CompleteFailure(validationReason);
                return MeshExecutionResult.Failure(validationReason);
            }

            byte[] bytes = await File.ReadAllBytesAsync(item.PrimaryPath, token).ConfigureAwait(false);
            if (!TryBuildCompactedModelBytes(bytes, mdl, token, out var rewrittenBytes, out var removedTriangles, out var removedBytes, out var changedMeshes, out var trianglesBefore, out var buildFailureReason))
            {
                var detail = string.IsNullOrWhiteSpace(buildFailureReason)
                    ? "No safely reclaimable triangles or compactable mesh data were found."
                    : buildFailureReason;
                job?.CompleteFailure(detail);
                return MeshExecutionResult.Failure(detail);
            }

            string tempPath = item.PrimaryPath + ".meshopt.tmp";
            try
            {
                await File.WriteAllBytesAsync(tempPath, rewrittenBytes, token).ConfigureAwait(false);
                if (!TryValidateSerializedModel(tempPath, out var serializedValidationReason))
                {
                    job?.CompleteFailure(serializedValidationReason);
                    return MeshExecutionResult.Failure(serializedValidationReason);
                }

                File.Copy(tempPath, item.PrimaryPath, overwrite: true);
                MirrorPrimaryToAlternates(item);
                job?.CompleteSuccess($"Removed {removedTriangles:N0} triangles, reclaimed {removedBytes:N0} bytes, updated {changedMeshes} mesh block(s).");
                return MeshExecutionResult.Success(removedTriangles, removedBytes, changedMeshes, trianglesBefore, Math.Max(0, trianglesBefore - removedTriangles), "Strict cleanup completed.");
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
        catch (Exception ex)
        {
            job?.CompleteFailure(ex.Message);
            return MeshExecutionResult.Failure(ex.Message);
        }
    }

    private bool TryBuildCompactedModelBytes(ReadOnlySpan<byte> bytes, MdlFile mdl, CancellationToken token, out byte[] rewrittenBytes, out long removedTriangles, out long removedBytes, out int changedMeshes, out long trianglesBefore, out string failureReason)
    {
        rewrittenBytes = Array.Empty<byte>();
        removedTriangles = 0;
        removedBytes = 0;
        changedMeshes = 0;
        trianglesBefore = 0;
        failureReason = string.Empty;

        if (mdl.LodCount <= 0 || mdl.Meshes.Length == 0)
        {
            failureReason = "Model does not expose any LOD mesh data to rebuild.";
            return false;
        }

        var updatedMeshes = (MeshStruct[])mdl.Meshes.Clone();
        var updatedSubMeshes = (SubmeshStruct[])mdl.SubMeshes.Clone();
        var updatedLods = (LodStruct[])mdl.Lods.Clone();
        var rebuiltData = new List<byte>(bytes.Length > mdl.DataSectionOffset ? checked((int)(bytes.Length - (int)mdl.DataSectionOffset)) : 0);

        for (int lodIndex = 0; lodIndex < mdl.LodCount; lodIndex++)
        {
            token.ThrowIfCancellationRequested();

            int meshIndex = mdl.Lods[lodIndex].MeshIndex;
            int meshCount = mdl.Lods[lodIndex].MeshCount;
            if (meshCount <= 0)
            {
                AppendOriginalLodData(bytes, mdl, lodIndex, rebuiltData, out var passthroughVertexOffset, out var passthroughIndexOffset, out var passthroughVertexSize, out var passthroughIndexSize);
                UpdateLodOffsets(ref updatedLods[lodIndex], mdl.DataSectionOffset, passthroughVertexOffset, passthroughIndexOffset, passthroughVertexSize, passthroughIndexSize);
                continue;
            }

            long lodIndexDataOffset = mdl.DataSectionOffset + mdl.Lods[lodIndex].IndexDataOffset;
            long lodVertexDataOffset = mdl.DataSectionOffset + mdl.Lods[lodIndex].VertexDataOffset;
            int meshEnd = Math.Min(updatedMeshes.Length, meshIndex + meshCount);
            var guardContext = BuildModelGuardContextForLod(bytes, mdl, lodIndex, lodIndexDataOffset, lodVertexDataOffset);
            var lodVertexBytes = new List<byte>(checked((int)mdl.Lods[lodIndex].VertexBufferSize));
            var lodIndices = new List<ushort>(checked((int)(mdl.Lods[lodIndex].IndexBufferSize / sizeof(ushort))));

            for (int m = meshIndex; m < meshEnd; m++)
            {
                token.ThrowIfCancellationRequested();

                var mesh = mdl.Meshes[m];
                uint indexCount = mesh.IndexCount;
                if (indexCount >= 3)
                    trianglesBefore += indexCount / 3;

                long absoluteIndexStart = lodIndexDataOffset + ((long)mesh.StartIndex * sizeof(ushort));
                long absoluteIndexEnd = absoluteIndexStart + ((long)indexCount * sizeof(ushort));
                if (absoluteIndexStart < 0 || absoluteIndexEnd > bytes.Length)
                {
                    failureReason = $"Mesh {m} index buffer range is invalid for rebuild.";
                    return false;
                }

                MeshCompactionPayload payload;
                MeshRewritePlan? rewritePlan = null;
                if (indexCount >= 3 && indexCount % 3 == 0
                    && TryPlanMeshRewrite(bytes, mdl, m, absoluteIndexStart, lodVertexDataOffset, guardContext, out var planned)
                    && planned.RewrittenIndices.Count >= 3)
                {
                    rewritePlan = planned;
                }

                if (rewritePlan is { } activeRewritePlan)
                {
                    if (!TryBuildCompactedMeshPayload(bytes, mdl, m, mesh, absoluteIndexStart, lodVertexDataOffset, activeRewritePlan, out payload, out var payloadFailureReason)
                        && !TryBuildOriginalMeshPayload(bytes, mdl, m, mesh, absoluteIndexStart, lodVertexDataOffset, out payload, out var passthroughFailureReason))
                    {
                        failureReason = $"Mesh {m} payload rebuild failed. Planned={payloadFailureReason}; Passthrough={passthroughFailureReason}";
                        return false;
                    }
                }
                else if (!TryBuildOriginalMeshPayload(bytes, mdl, m, mesh, absoluteIndexStart, lodVertexDataOffset, out payload, out var passthroughFailureReason))
                {
                    failureReason = $"Mesh {m} passthrough payload rebuild failed. Reason={passthroughFailureReason}";
                    return false;
                }

                if (payload.Changed)
                {
                    changedMeshes++;
                    removedTriangles += payload.RemovedTriangles;
                }

                var updatedMesh = updatedMeshes[m];
                int vertexBufferBase = lodVertexBytes.Count;
                int stream0Length = payload.Streams[0].Length;
                int stream1Length = payload.Streams[1].Length;
                int stream2Length = payload.Streams[2].Length;

                if (updatedMesh.VertexBufferOffset.Length > 0)
                    updatedMesh.VertexBufferOffset[0] = checked((uint)vertexBufferBase);
                if (updatedMesh.VertexBufferOffset.Length > 1)
                    updatedMesh.VertexBufferOffset[1] = checked((uint)(vertexBufferBase + stream0Length));
                if (updatedMesh.VertexBufferOffset.Length > 2)
                    updatedMesh.VertexBufferOffset[2] = checked((uint)(vertexBufferBase + stream0Length + stream1Length));
                updatedMesh.VertexCount = checked((ushort)payload.VertexCount);
                updatedMesh.StartIndex = checked((uint)lodIndices.Count);
                updatedMesh.IndexCount = checked((uint)payload.Indices.Length);

                updatedMeshes[m] = updatedMesh;

                lodVertexBytes.AddRange(payload.Streams[0]);
                lodVertexBytes.AddRange(payload.Streams[1]);
                lodVertexBytes.AddRange(payload.Streams[2]);
                lodIndices.AddRange(payload.Indices);

                int meshIndexBytes = lodIndices.Count * sizeof(ushort);
                int meshPaddingBytes = (16 - (meshIndexBytes % 16)) % 16;
                if (meshPaddingBytes > 0)
                {
                    int meshPaddingIndices = meshPaddingBytes / sizeof(ushort);
                    for (int pad = 0; pad < meshPaddingIndices; pad++)
                        lodIndices.Add(0);
                }

                for (int i = 0; i < payload.SubmeshInfos.Length; i++)
                {
                    var info = payload.SubmeshInfos[i];
                    var updatedSubMesh = updatedSubMeshes[info.SubmeshArrayIndex];
                    updatedSubMesh.IndexOffset = updatedMesh.StartIndex + info.RelativeIndexOffset;
                    updatedSubMesh.IndexCount = info.IndexCount;
                    updatedSubMeshes[info.SubmeshArrayIndex] = updatedSubMesh;
                }
            }

            if (!TryValidateRebuiltLodLayout(updatedMeshes, meshIndex, meshEnd, updatedSubMeshes, lodVertexBytes, lodIndices, out var lodValidationReason))
            {
                failureReason = $"LOD {lodIndex} rebuild validation failed. Reason={lodValidationReason}";
                return false;
            }

            uint rebuiltVertexOffset = checked((uint)rebuiltData.Count);
            AppendBytes(rebuiltData, lodVertexBytes);
            uint rebuiltIndexOffset = checked((uint)rebuiltData.Count);
            AppendIndicesAsBytes(rebuiltData, lodIndices);
            PadListToAlignment(rebuiltData, 16);

            UpdateLodOffsets(ref updatedLods[lodIndex], mdl.DataSectionOffset, rebuiltVertexOffset, rebuiltIndexOffset, checked((uint)lodVertexBytes.Count), checked((uint)(lodIndices.Count * sizeof(ushort))));
        }

        for (int lodIndex = mdl.LodCount; lodIndex < updatedLods.Length; lodIndex++)
        {
            updatedLods[lodIndex].VertexDataOffset = 0;
            updatedLods[lodIndex].IndexDataOffset = 0;
            updatedLods[lodIndex].VertexBufferSize = 0;
            updatedLods[lodIndex].IndexBufferSize = 0;
        }

        bool headerChanged = removedTriangles > 0;
        if (rebuiltData.Count >= 0)
        {
            long compactedLength = checked((long)mdl.DataSectionOffset) + rebuiltData.Count;
            removedBytes = Math.Max(0, bytes.Length - compactedLength);
            if (removedBytes > 0)
                headerChanged = true;
        }

        if (!headerChanged && changedMeshes <= 0)
        {
            failureReason = "No actionable mesh rewrites were produced.";
            return false;
        }

        rewrittenBytes = new byte[checked((int)mdl.DataSectionOffset) + rebuiltData.Count];
        bytes.Slice(0, (int)mdl.DataSectionOffset).CopyTo(rewrittenBytes);
        for (int i = 0; i < rebuiltData.Count; i++)
            rewrittenBytes[checked((int)mdl.DataSectionOffset) + i] = rebuiltData[i];

        for (int lodIndex = 0; lodIndex < updatedLods.Length; lodIndex++)
        {
            WriteUInt32ArrayValue(rewrittenBytes, FileHeaderVertexOffsetArrayOffset, lodIndex, (uint)updatedLods[lodIndex].VertexDataOffset);
            WriteUInt32ArrayValue(rewrittenBytes, FileHeaderIndexOffsetArrayOffset, lodIndex, (uint)updatedLods[lodIndex].IndexDataOffset);
            WriteUInt32ArrayValue(rewrittenBytes, FileHeaderVertexBufferSizeArrayOffset, lodIndex, (uint)updatedLods[lodIndex].VertexBufferSize);
            WriteUInt32ArrayValue(rewrittenBytes, FileHeaderIndexBufferSizeArrayOffset, lodIndex, (uint)updatedLods[lodIndex].IndexBufferSize);
            if (!TryWriteLodStructure(rewrittenBytes, mdl.LodOffsets[lodIndex], updatedLods[lodIndex]))
            {
                failureReason = $"Failed to write LOD {lodIndex} metadata.";
                return false;
            }
        }

        for (int meshIndex = 0; meshIndex < updatedMeshes.Length; meshIndex++)
        {
            if (!TryWriteMeshStructure(rewrittenBytes, mdl.MeshOffsets[meshIndex], updatedMeshes[meshIndex]))
            {
                failureReason = $"Failed to write mesh {meshIndex} metadata.";
                return false;
            }
        }

        for (int subMeshIndex = 0; subMeshIndex < updatedSubMeshes.Length; subMeshIndex++)
        {
            if (!TryWriteSubMeshStructure(rewrittenBytes, mdl.SubMeshOffsets[subMeshIndex], updatedSubMeshes[subMeshIndex]))
            {
                failureReason = $"Failed to write submesh {subMeshIndex} metadata.";
                return false;
            }
        }

        bool succeeded = changedMeshes > 0 || removedBytes > 0;
        if (!succeeded && string.IsNullOrWhiteSpace(failureReason))
            failureReason = "No rewritten mesh payloads produced output changes.";

        return succeeded;
    }

    private bool TryBuildOriginalMeshPayload(ReadOnlySpan<byte> bytes, MdlFile mdl, int meshIndex, MeshStruct mesh, long absoluteIndexStart, long absoluteVertexStart, out MeshCompactionPayload payload, out string failureReason)
    {
        payload = default;
        failureReason = string.Empty;

        if (!TryReadMeshIndices(bytes, absoluteIndexStart, mesh.IndexCount, out var sourceIndices))
        {
            failureReason = "Failed to read original mesh indices.";
            return false;
        }

        var streams = new byte[3][];
        for (int streamIndex = 0; streamIndex < 3; streamIndex++)
        {
            if (!TryCopyOriginalVertexStream(bytes, mesh, absoluteVertexStart, streamIndex, out streams[streamIndex]))
            {
                failureReason = $"Failed to copy original vertex stream {streamIndex}.";
                return false;
            }
        }

        if (!TryBuildSubmeshRewriteInfos(mdl, mesh, rewritePlan: null, out var submeshInfos))
        {
            failureReason = "Failed to rebuild passthrough submesh metadata.";
            return false;
        }

        payload = new MeshCompactionPayload(streams, sourceIndices, submeshInfos, mesh.VertexCount, 0, false);
        return true;
    }

    private static bool TryCopyOriginalVertexStream(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, out byte[] streamBytes)
    {
        streamBytes = [];
        if ((uint)streamIndex >= (uint)mesh.VertexBufferStride.Length || (uint)streamIndex >= (uint)mesh.VertexBufferOffset.Length)
            return false;

        int stride = mesh.VertexBufferStride[streamIndex];
        if (stride <= 0 || mesh.VertexCount <= 0)
            return true;

        long streamStart = absoluteVertexStart + mesh.VertexBufferOffset[streamIndex];
        long streamLength = (long)mesh.VertexCount * stride;
        long streamEnd = streamStart + streamLength;
        if (streamStart < 0 || streamEnd > bytes.Length)
            return false;

        streamBytes = bytes.Slice(checked((int)streamStart), checked((int)streamLength)).ToArray();
        return true;
    }

    private static bool[] GetUsedVertexStreamsForMesh(MdlFile mdl, MeshStruct mesh, int meshIndex)
    {
        var usedStreams = new bool[3];
        if (TryGetVertexDeclarationForMesh(mdl, mesh, meshIndex, out var declaration))
        {
            foreach (var element in declaration.VertexElements)
            {
                if ((uint)element.Stream < (uint)usedStreams.Length)
                    usedStreams[element.Stream] = true;
            }

            return usedStreams;
        }

        for (int streamIndex = 0; streamIndex < usedStreams.Length && streamIndex < mesh.VertexBufferStride.Length; streamIndex++)
        {
            usedStreams[streamIndex] = mesh.VertexBufferStride[streamIndex] > 0;
        }

        return usedStreams;
    }

    private bool TryBuildCompactedMeshPayload(ReadOnlySpan<byte> bytes, MdlFile mdl, int meshIndex, MeshStruct mesh, long absoluteIndexStart, long absoluteVertexStart, MeshRewritePlan? rewritePlan, out MeshCompactionPayload payload, out string failureReason)
    {
        payload = default;
        failureReason = string.Empty;

        if (rewritePlan is { HasReplacementPayload: true } replacementPlan)
        {
            if (replacementPlan.ReplacementVertexCount <= 0 || replacementPlan.ReplacementStreams == null || replacementPlan.ReplacementStreams.Length != 3)
            {
                failureReason = "Replacement payload did not contain valid stream data.";
                return false;
            }

            ushort[] replacementIndices = replacementPlan.RewrittenIndices.Count == 0 ? [] : replacementPlan.RewrittenIndices.ToArray();
            if (replacementIndices.Length == 0 || replacementIndices.Length % 3 != 0)
            {
                failureReason = "Replacement payload index buffer was empty or not triangle-aligned.";
                return false;
            }

            if (!TryBuildSubmeshRewriteInfos(mdl, mesh, replacementPlan, out var replacementInfos))
            {
                failureReason = "Failed to rebuild replacement submesh metadata.";
                return false;
            }

            payload = new MeshCompactionPayload(replacementPlan.ReplacementStreams, replacementIndices, replacementInfos, replacementPlan.ReplacementVertexCount, replacementPlan.RemovedTriangles, replacementPlan.RemovedTriangles > 0);
            return true;
        }

        ushort[] sourceIndices;
        if (rewritePlan is { } plan)
        {
            sourceIndices = plan.RewrittenIndices.Count == 0 ? [] : plan.RewrittenIndices.ToArray();
        }
        else if (!TryReadMeshIndices(bytes, absoluteIndexStart, mesh.IndexCount, out sourceIndices))
        {
            failureReason = "Failed to read source mesh indices.";
            return false;
        }

        if (sourceIndices.Length == 0 || sourceIndices.Length % 3 != 0)
        {
            failureReason = "Source mesh index payload was empty or not triangle-aligned.";
            return false;
        }

        if (!TryBuildCompactedVertexMap(mesh, sourceIndices, out var usedVertices, out var remappedIndices))
        {
            failureReason = "Failed to build rewritten vertex remap.";
            return false;
        }

        var streams = new byte[3][];
        var usedStreams = GetUsedVertexStreamsForMesh(mdl, mesh, meshIndex);
        for (int streamIndex = 0; streamIndex < 3; streamIndex++)
        {
            if (!usedStreams[streamIndex])
            {
                streams[streamIndex] = [];
                continue;
            }

            if (!TryCopyCompactedVertexStream(bytes, mesh, absoluteVertexStart, streamIndex, usedVertices, out streams[streamIndex]))
            {
                failureReason = $"Failed to copy compacted vertex stream {streamIndex}.";
                return false;
            }
        }

        if (!TryBuildSubmeshRewriteInfos(mdl, mesh, rewritePlan, out var submeshInfos))
        {
            failureReason = "Failed to rebuild rewritten submesh metadata.";
            return false;
        }

        bool changed = (rewritePlan?.RemovedTriangles ?? 0) > 0 || usedVertices.Length < mesh.VertexCount;
        payload = new MeshCompactionPayload(streams, remappedIndices, submeshInfos, usedVertices.Length, rewritePlan?.RemovedTriangles ?? 0, changed);
        return true;
    }

    private static bool TryReadMeshIndices(ReadOnlySpan<byte> bytes, long absoluteIndexStart, uint indexCount, out ushort[] indices)
    {
        indices = [];
        if (indexCount == 0)
            return true;

        long absoluteIndexEnd = absoluteIndexStart + ((long)indexCount * sizeof(ushort));
        if (absoluteIndexStart < 0 || absoluteIndexEnd > bytes.Length)
            return false;

        indices = new ushort[checked((int)indexCount)];
        int readOffset = checked((int)absoluteIndexStart);
        for (int i = 0; i < indices.Length; i++)
        {
            indices[i] = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(readOffset + (i * sizeof(ushort)), sizeof(ushort)));
        }

        return true;
    }

    private static bool TryBuildCompactedVertexMap(MeshStruct mesh, ushort[] sourceIndices, out ushort[] usedVertices, out ushort[] remappedIndices)
    {
        usedVertices = [];
        remappedIndices = [];
        if (mesh.VertexCount <= 0 || sourceIndices.Length == 0)
            return false;

        var firstSeen = new Dictionary<ushort, ushort>();
        var orderedVertices = new List<ushort>(Math.Min(mesh.VertexCount, sourceIndices.Length));
        remappedIndices = new ushort[sourceIndices.Length];

        for (int i = 0; i < sourceIndices.Length; i++)
        {
            ushort originalVertex = sourceIndices[i];
            if (originalVertex >= mesh.VertexCount)
                return false;

            if (!firstSeen.TryGetValue(originalVertex, out var remappedVertex))
            {
                if (orderedVertices.Count >= ushort.MaxValue)
                    return false;

                remappedVertex = checked((ushort)orderedVertices.Count);
                firstSeen.Add(originalVertex, remappedVertex);
                orderedVertices.Add(originalVertex);
            }

            remappedIndices[i] = remappedVertex;
        }

        usedVertices = orderedVertices.ToArray();
        return usedVertices.Length > 0;
    }

    private static bool TryCopyCompactedVertexStream(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, ushort[] usedVertices, out byte[] streamBytes)
    {
        streamBytes = [];
        if ((uint)streamIndex >= (uint)mesh.VertexBufferStride.Length || (uint)streamIndex >= (uint)mesh.VertexBufferOffset.Length)
            return false;

        int stride = mesh.VertexBufferStride[streamIndex];
        if (stride <= 0 || usedVertices.Length == 0)
            return true;

        long streamStart = absoluteVertexStart + mesh.VertexBufferOffset[streamIndex];
        long streamLength = (long)mesh.VertexCount * stride;
        long streamEnd = streamStart + streamLength;
        if (streamStart < 0 || streamEnd > bytes.Length)
            return false;

        streamBytes = new byte[usedVertices.Length * stride];
        int sourceBase = checked((int)streamStart);
        for (int i = 0; i < usedVertices.Length; i++)
        {
            int sourceOffset = sourceBase + (usedVertices[i] * stride);
            bytes.Slice(sourceOffset, stride).CopyTo(streamBytes.AsSpan(i * stride, stride));
        }

        return true;
    }

    private static bool TryBuildSubmeshRewriteInfos(MdlFile mdl, MeshStruct mesh, MeshRewritePlan? rewritePlan, out SubmeshRewriteInfo[] infos)
    {
        infos = [];
        if (mesh.SubMeshCount <= 0)
            return true;

        int submeshStart = mesh.SubMeshIndex;
        int submeshEnd = submeshStart + mesh.SubMeshCount;
        if (submeshStart < 0 || submeshEnd > mdl.SubMeshes.Length)
            return false;

        if (rewritePlan is { SubmeshUpdates.Count: > 0 } plan)
        {
            infos = new SubmeshRewriteInfo[plan.SubmeshUpdates.Count];
            for (int i = 0; i < plan.SubmeshUpdates.Count; i++)
            {
                var update = plan.SubmeshUpdates[i];
                if (update.SubmeshArrayIndex < submeshStart || update.SubmeshArrayIndex >= submeshEnd)
                    return false;
                if (update.IndexOffset < mesh.StartIndex)
                    return false;

                infos[i] = new SubmeshRewriteInfo(update.SubmeshArrayIndex, update.IndexOffset - mesh.StartIndex, update.IndexCount);
            }

            return true;
        }

        infos = new SubmeshRewriteInfo[mesh.SubMeshCount];
        for (int i = 0; i < mesh.SubMeshCount; i++)
        {
            int submeshArrayIndex = submeshStart + i;
            var submesh = mdl.SubMeshes[submeshArrayIndex];
            if (submesh.IndexOffset < mesh.StartIndex)
                return false;

            infos[i] = new SubmeshRewriteInfo(submeshArrayIndex, submesh.IndexOffset - mesh.StartIndex, submesh.IndexCount);
        }

        return true;
    }


    private static bool TryValidateRebuiltLodLayout(MeshStruct[] meshes, int meshStart, int meshEnd, SubmeshStruct[] subMeshes, List<byte> vertexBuffer, List<ushort> indexBuffer, out string reason)
    {
        reason = string.Empty;
        int totalIndexCount = indexBuffer.Count;
        if (meshStart < 0 || meshEnd < meshStart || meshEnd > meshes.Length)
        {
            reason = "Invalid mesh range.";
            return false;
        }

        for (int meshIndex = meshStart; meshIndex < meshEnd; meshIndex++)
        {
            var mesh = meshes[meshIndex];
            int vertexCount = mesh.VertexCount;
            int startIndex = checked((int)mesh.StartIndex);
            int indexCount = checked((int)mesh.IndexCount);
            if (startIndex < 0 || indexCount < 0 || startIndex + indexCount > totalIndexCount)
            {
                reason = $"Mesh {meshIndex}: index range out of bounds.";
                return false;
            }

            for (int stream = 0; stream < 3; stream++)
            {
                if ((uint)stream >= (uint)mesh.VertexBufferStride.Length || (uint)stream >= (uint)mesh.VertexBufferOffset.Length)
                    continue;

                int stride = mesh.VertexBufferStride[stream];
                if (stride <= 0 || vertexCount <= 0)
                    continue;

                int offset = checked((int)mesh.VertexBufferOffset[stream]);
                long size = (long)stride * vertexCount;
                if (offset < 0 || offset + size > vertexBuffer.Count)
                {
                    reason = $"Mesh {meshIndex}: vertex stream {stream} out of bounds.";
                    return false;
                }
            }

            for (int i = 0; i < indexCount; i++)
            {
                ushort idx = indexBuffer[startIndex + i];
                if (idx >= vertexCount)
                {
                    reason = $"Mesh {meshIndex}: index {idx} exceeds vertex count {vertexCount}.";
                    return false;
                }
            }

            int subMeshStart = mesh.SubMeshIndex;
            int subMeshEnd = subMeshStart + mesh.SubMeshCount;
            if (subMeshStart < 0 || subMeshEnd > subMeshes.Length)
            {
                reason = $"Mesh {meshIndex}: submesh range out of bounds.";
                return false;
            }

            int meshStartIndex = startIndex;
            int meshEndIndex = startIndex + indexCount;
            for (int subMeshIndex = subMeshStart; subMeshIndex < subMeshEnd; subMeshIndex++)
            {
                var subMesh = subMeshes[subMeshIndex];
                int subStart = checked((int)subMesh.IndexOffset);
                int subEnd = subStart + checked((int)subMesh.IndexCount);
                if (subStart < meshStartIndex || subEnd > meshEndIndex)
                {
                    reason = $"Mesh {meshIndex}: submesh {subMeshIndex} index range out of mesh bounds.";
                    return false;
                }
            }
        }

        return true;
    }

    private static void AppendOriginalLodData(ReadOnlySpan<byte> bytes, MdlFile mdl, int lodIndex, List<byte> destination, out uint vertexOffset, out uint indexOffset, out uint vertexSize, out uint indexSize)
    {
        vertexOffset = checked((uint)destination.Count);
        vertexSize = mdl.Lods[lodIndex].VertexBufferSize;
        indexSize = mdl.Lods[lodIndex].IndexBufferSize;

        long absoluteVertexStart = mdl.DataSectionOffset + mdl.Lods[lodIndex].VertexDataOffset;
        long absoluteVertexEnd = absoluteVertexStart + vertexSize;
        if (vertexSize > 0 && absoluteVertexStart >= 0 && absoluteVertexEnd <= bytes.Length)
            AppendSpan(destination, bytes.Slice((int)absoluteVertexStart, checked((int)vertexSize)));

        indexOffset = checked((uint)destination.Count);
        long absoluteIndexStart = mdl.DataSectionOffset + mdl.Lods[lodIndex].IndexDataOffset;
        long absoluteIndexEnd = absoluteIndexStart + indexSize;
        if (indexSize > 0 && absoluteIndexStart >= 0 && absoluteIndexEnd <= bytes.Length)
            AppendSpan(destination, bytes.Slice((int)absoluteIndexStart, checked((int)indexSize)));

        PadListToAlignment(destination, 16);
    }

    private static void UpdateLodOffsets(ref LodStruct lod, long dataSectionOffset, uint vertexOffset, uint indexOffset, uint vertexSize, uint indexSize)
    {
        // MdlFile reads these relative. The file wants the data-section base back on write.
        uint serializedBaseOffset = checked((uint)dataSectionOffset);
        lod.VertexDataOffset = checked(serializedBaseOffset + vertexOffset);
        lod.IndexDataOffset = checked(serializedBaseOffset + indexOffset);
        lod.VertexBufferSize = vertexSize;
        lod.IndexBufferSize = indexSize;
    }

    private static void AppendBytes(List<byte> destination, List<byte> source)
    {
        if (source.Count == 0)
            return;

        destination.AddRange(source);
    }

    private static void AppendIndicesAsBytes(List<byte> destination, List<ushort> indices)
    {
        for (int i = 0; i < indices.Count; i++)
        {
            ushort value = indices[i];
            destination.Add((byte)(value & 0xFF));
            destination.Add((byte)(value >> 8));
        }
    }

    private static void AppendSpan(List<byte> destination, ReadOnlySpan<byte> source)
    {
        for (int i = 0; i < source.Length; i++)
            destination.Add(source[i]);
    }

    private static void PadListToAlignment(List<byte> destination, int alignment)
    {
        if (alignment <= 1)
            return;

        int padding = (alignment - (destination.Count % alignment)) % alignment;
        for (int i = 0; i < padding; i++)
            destination.Add(0);
    }

    private static void WriteUInt32ArrayValue(byte[] buffer, int arrayOffset, int index, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(arrayOffset + (index * sizeof(uint)), sizeof(uint)), value);
    }

    private static bool TryWriteLodStructure(byte[] buffer, long offset, LodStruct lod)
    {
        const int size = 60;
        if (offset < 0 || offset + size > buffer.Length)
            return false;

        var span = buffer.AsSpan(checked((int)offset), size);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x00, 2), lod.MeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x02, 2), lod.MeshCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x04, 4), unchecked((uint)BitConverter.SingleToInt32Bits(lod.ModelLodRange)));
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x08, 4), unchecked((uint)BitConverter.SingleToInt32Bits(lod.TextureLodRange)));
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0C, 2), lod.WaterMeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0E, 2), lod.WaterMeshCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x10, 2), lod.ShadowMeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x12, 2), lod.ShadowMeshCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x14, 2), lod.TerrainShadowMeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x16, 2), lod.TerrainShadowMeshCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x18, 2), lod.VerticalFogMeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x1A, 2), lod.VerticalFogMeshCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x1C, 4), lod.EdgeGeometrySize);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x20, 4), lod.EdgeGeometryDataOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x24, 4), lod.PolygonCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x28, 4), lod.Unknown1);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x2C, 4), lod.VertexBufferSize);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x30, 4), lod.IndexBufferSize);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x34, 4), lod.VertexDataOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x38, 4), lod.IndexDataOffset);
        return true;
    }

    private static bool TryWriteMeshStructure(byte[] buffer, long offset, MeshStruct mesh)
    {
        // Write the mesh layout explicitly. Raw struct marshalling bit us already.

        const int size = 36;
        if (offset < 0 || offset + size > buffer.Length)
            return false;

        var span = buffer.AsSpan(checked((int)offset), size);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x00, 2), mesh.VertexCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x02, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x04, 4), mesh.IndexCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x08, 2), mesh.MaterialIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0A, 2), mesh.SubMeshIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0C, 2), mesh.SubMeshCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0E, 2), mesh.BoneTableIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x10, 4), mesh.StartIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x14, 4), GetArrayValue(mesh.VertexBufferOffset, 0));
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x18, 4), GetArrayValue(mesh.VertexBufferOffset, 1));
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x1C, 4), GetArrayValue(mesh.VertexBufferOffset, 2));
        span[0x20] = GetArrayValue(mesh.VertexBufferStride, 0);
        span[0x21] = GetArrayValue(mesh.VertexBufferStride, 1);
        span[0x22] = GetArrayValue(mesh.VertexBufferStride, 2);
        span[0x23] = mesh.VertexStreamCount;
        return true;
    }

    private static bool TryWriteSubMeshStructure(byte[] buffer, long offset, SubmeshStruct subMesh)
    {
        const int size = 16;
        if (offset < 0 || offset + size > buffer.Length)
            return false;

        var span = buffer.AsSpan(checked((int)offset), size);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x00, 4), subMesh.IndexOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x04, 4), subMesh.IndexCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x08, 4), subMesh.AttributeIndexMask);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0C, 2), subMesh.BoneStartIndex);
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x0E, 2), subMesh.BoneCount);
        return true;
    }

    private static uint GetArrayValue(uint[]? values, int index)
        => values is not null && (uint)index < (uint)values.Length ? values[index] : 0u;

    private static byte GetArrayValue(byte[]? values, int index)
        => values is not null && (uint)index < (uint)values.Length ? values[index] : (byte)0;

    private static bool TryValidateSerializedModel(string path, out string reason)
    {
        // Final sanity pass before we let the game see the rewritten mdl.

        reason = string.Empty;

        try
        {
            var mdl = new MdlFile(path);
            long fileLength = new FileInfo(path).Length;

            for (int lodIndex = 0; lodIndex < mdl.Lods.Length; lodIndex++)
            {
                bool activeLod = lodIndex < mdl.LodCount;
                var lod = mdl.Lods[lodIndex];

                if (!activeLod)
                {
                    if (lod.VertexDataOffset != 0 || lod.IndexDataOffset != 0 || lod.VertexBufferSize != 0 || lod.IndexBufferSize != 0
                        || mdl.VertexOffset[lodIndex] != 0 || mdl.IndexOffset[lodIndex] != 0 || mdl.VertexBufferSize[lodIndex] != 0 || mdl.IndexBufferSize[lodIndex] != 0)
                    {
                        reason = $"Serialized LOD {lodIndex} is inactive but still contains non-zero buffer metadata.";
                        return false;
                    }

                    continue;
                }

                if (mdl.VertexOffset[lodIndex] != lod.VertexDataOffset
                    || mdl.IndexOffset[lodIndex] != lod.IndexDataOffset
                    || mdl.VertexBufferSize[lodIndex] != lod.VertexBufferSize
                    || mdl.IndexBufferSize[lodIndex] != lod.IndexBufferSize)
                {
                    reason = $"Serialized LOD {lodIndex} header arrays do not match LOD struct values.";
                    return false;
                }

                long absoluteVertexStart = mdl.DataSectionOffset + lod.VertexDataOffset;
                long absoluteVertexEnd = absoluteVertexStart + lod.VertexBufferSize;
                if (lod.VertexBufferSize > 0 && (absoluteVertexStart < mdl.DataSectionOffset || absoluteVertexEnd > fileLength))
                {
                    reason = $"Serialized LOD {lodIndex} vertex buffer range is outside the file.";
                    return false;
                }

                long absoluteIndexStart = mdl.DataSectionOffset + lod.IndexDataOffset;
                long absoluteIndexEnd = absoluteIndexStart + lod.IndexBufferSize;
                if (lod.IndexBufferSize > 0 && (absoluteIndexStart < mdl.DataSectionOffset || absoluteIndexEnd > fileLength))
                {
                    reason = $"Serialized LOD {lodIndex} index buffer range is outside the file.";
                    return false;
                }

                int meshStart = lod.MeshIndex;
                int meshEnd = meshStart + lod.MeshCount;
                if (meshStart < 0 || meshEnd < meshStart || meshEnd > mdl.Meshes.Length)
                {
                    reason = $"Serialized LOD {lodIndex} mesh range is invalid.";
                    return false;
                }

                for (int meshIndex = meshStart; meshIndex < meshEnd; meshIndex++)
                {
                    var mesh = mdl.Meshes[meshIndex];
                    int vertexCount = mesh.VertexCount;
                    int startIndex = checked((int)mesh.StartIndex);
                    int indexCount = checked((int)mesh.IndexCount);
                    if (startIndex < 0 || indexCount < 0 || ((long)startIndex + indexCount) * sizeof(ushort) > lod.IndexBufferSize)
                    {
                        reason = $"Serialized mesh {meshIndex} index range exceeds LOD {lodIndex} index buffer size.";
                        return false;
                    }

                    for (int stream = 0; stream < 3; stream++)
                    {
                        if ((uint)stream >= (uint)mesh.VertexBufferStride.Length || (uint)stream >= (uint)mesh.VertexBufferOffset.Length)
                            continue;

                        int stride = mesh.VertexBufferStride[stream];
                        if (stride <= 0 || vertexCount <= 0)
                            continue;

                        long streamStart = mesh.VertexBufferOffset[stream];
                        long streamEnd = streamStart + ((long)stride * vertexCount);
                        if (streamStart < 0 || streamEnd > lod.VertexBufferSize)
                        {
                            reason = $"Serialized mesh {meshIndex} stream {stream} exceeds LOD {lodIndex} vertex buffer size.";
                            return false;
                        }
                    }

                    int subMeshStart = mesh.SubMeshIndex;
                    int subMeshEnd = subMeshStart + mesh.SubMeshCount;
                    if (subMeshStart < 0 || subMeshEnd > mdl.SubMeshes.Length)
                    {
                        reason = $"Serialized mesh {meshIndex} submesh range is invalid.";
                        return false;
                    }

                    int meshStartIndex = startIndex;
                    int meshEndIndex = startIndex + indexCount;
                    for (int subMeshIndex = subMeshStart; subMeshIndex < subMeshEnd; subMeshIndex++)
                    {
                        var subMesh = mdl.SubMeshes[subMeshIndex];
                        int subStart = checked((int)subMesh.IndexOffset);
                        int subEnd = subStart + checked((int)subMesh.IndexCount);
                        if (subStart < meshStartIndex || subEnd > meshEndIndex)
                        {
                            reason = $"Serialized submesh {subMeshIndex} exceeds mesh {meshIndex} index bounds.";
                            return false;
                        }
                    }
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            reason = $"Serialized model validation failed: {ex.Message}";
            return false;
        }
    }

    private bool TryEstimateRemovableTriangles(string path, out long removableTriangles, out int changedMeshes, out long trianglesBefore, out string failureReason)
    {
        removableTriangles = 0;
        changedMeshes = 0;
        trianglesBefore = 0;
        failureReason = string.Empty;

        try
        {
            var mdl = new MdlFile(path);
            if (!TryValidateModelForRewrite(mdl, out var validationFailureReason))
            {
                failureReason = string.IsNullOrWhiteSpace(validationFailureReason)
                    ? "Model failed rewrite validation."
                    : validationFailureReason;
                return false;
            }

            if (!TryReadAllBytesPooled(path, out var buffer, out var bufferLength))
            {
                failureReason = "Failed to read model bytes for estimate.";
                return false;
            }

            try
            {
                var bytes = buffer.AsSpan(0, bufferLength);
                if (!TryBuildCompactedModelBytes(
                        bytes,
                        mdl,
                        CancellationToken.None,
                        out _,
                        out removableTriangles,
                        out _,
                        out changedMeshes,
                        out trianglesBefore,
                        out var buildFailureReason))
                {
                    removableTriangles = 0;
                    changedMeshes = 0;
                    failureReason = string.IsNullOrWhiteSpace(buildFailureReason)
                        ? "Model rebuild failed during estimate."
                        : buildFailureReason;
                    return false;
                }

                if (removableTriangles <= 0 || changedMeshes <= 0)
                {
                    failureReason = $"Model rebuild succeeded but produced no actionable reduction. RemovedTriangles={removableTriangles}, ChangedMeshes={changedMeshes}";
                    return false;
                }

                failureReason = string.Empty;
                return true;
            }
            finally
            {
                ReturnPooledBuffer(buffer);
            }
        }
        catch (Exception ex)
        {
            failureReason = ex.Message;
            return false;
        }
    }

    private bool TryGetSuppressedEstimateReason(string primaryPath, long currentTriangles, out string failureReason)
    {
        failureReason = string.Empty;

        if (string.IsNullOrWhiteSpace(primaryPath))
        {
            failureReason = "Primary path is empty.";
            return true;
        }

        if (!IsSupportedReductionPath(primaryPath))
        {
            failureReason = "Path is not an eligible gear model.";
            return true;
        }

        lock (_historyLock)
        {
            if (TryNormaliseHistoryEntryLocked(primaryPath, currentTriangles, out bool alreadyOptimised) && alreadyOptimised)
            {
                failureReason = "Model already appears optimised for the current triangle count.";
                return true;
            }
        }

        return false;
    }

    public bool TryGetLastEstimateFailureReason(string primaryPath, out string failureReason)
    {
        failureReason = string.Empty;
        if (string.IsNullOrWhiteSpace(primaryPath))
            return false;

        if (!TryGetEstimateFileInfo(primaryPath, out var info))
        {
            failureReason = "Failed to stat model file.";
            return true;
        }

        lock (_estimateCacheLock)
        {
            if (_estimateCache.TryGetValue(primaryPath, out var cached)
                && cached.AlgorithmVersion == EstimateCacheVersion
                && cached.Length == info.Length
                && cached.LastWriteTimeUtcTicks == info.LastWriteTimeUtc.Ticks
                && !cached.Success
                && !string.IsNullOrWhiteSpace(cached.FailureReason))
            {
                failureReason = cached.FailureReason;
                return true;
            }
        }

        lock (_historyLock)
        {
            if (_history.TryGetValue(primaryPath, out var entry)
                && entry.MatchesEstimateFingerprint(info)
                && entry.HasLastEstimate
                && !entry.LastEstimateSuccess
                && !string.IsNullOrWhiteSpace(entry.LastEstimateFailureReason))
            {
                failureReason = entry.LastEstimateFailureReason;
                return true;
            }
        }

        if (TryGetSuppressedEstimateReason(primaryPath, currentTriangles: 0, out var suppressedReason))
        {
            failureReason = suppressedReason;
            return true;
        }

        return false;
    }

    private bool TryGetKnownEstimateActionability(string primaryPath, out bool isActionable)
    {
        isActionable = false;
        if (string.IsNullOrWhiteSpace(primaryPath))
            return false;

        if (!TryGetEstimateFileInfo(primaryPath, out var info))
            return false;

        lock (_estimateCacheLock)
        {
            if (_estimateCache.TryGetValue(primaryPath, out var cached)
                && cached.AlgorithmVersion == EstimateCacheVersion
                && cached.Length == info.Length
                && cached.LastWriteTimeUtcTicks == info.LastWriteTimeUtc.Ticks)
            {
                isActionable = cached.Success && cached.ChangedMeshes > 0 && cached.RemovableTriangles > 0;
                return true;
            }
        }

        lock (_historyLock)
        {
            if (_history.TryGetValue(primaryPath, out var entry)
                && entry.MatchesEstimateFingerprint(info)
                && entry.HasLastEstimate)
            {
                isActionable = entry.LastEstimateSuccess && entry.LastEstimateChangedMeshes > 0 && entry.LastEstimateSavedTriangles > 0;
                return true;
            }
        }

        return false;
    }


    private bool TryNormaliseHistoryEntryLocked(string primaryPath, long currentTriangles, out bool alreadyOptimised)
    {
        alreadyOptimised = false;
        if (!_history.TryGetValue(primaryPath, out var entry))
            return false;

        if (currentTriangles <= 0)
        {
            try
            {
                currentTriangles = CountTriangles(primaryPath);
            }
            catch
            {
                currentTriangles = 0;
            }
        }

        if (currentTriangles > 0)
        {
            if (entry.ReducedTriangles > 0 && currentTriangles <= entry.ReducedTriangles)
            {
                alreadyOptimised = true;
                return true;
            }

            if (currentTriangles == entry.OriginalTriangles)
                return true;
        }

        _history.Remove(primaryPath);
        return false;
    }

    private static bool TryGetEstimateFileInfo(string primaryPath, out FileInfo info)
    {
        info = null!;
        if (string.IsNullOrWhiteSpace(primaryPath) || !File.Exists(primaryPath))
            return false;

        try
        {
            info = new FileInfo(primaryPath);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void UpdateEstimateHistory(string primaryPath, FileInfo info, MeshSavingsEstimate estimate, string? failureReason)
    {
        lock (_historyLock)
        {
            if (!_history.TryGetValue(primaryPath, out var entry))
            {
                entry = new MeshOptimisationHistoryEntry();
                _history[primaryPath] = entry;
            }

            entry.LastSeenLength = info.Length;
            entry.LastSeenWriteUtcTicks = info.LastWriteTimeUtc.Ticks;
            entry.LastEstimateTrianglesBefore = estimate.TrianglesBefore;
            entry.LastEstimateSavedTriangles = estimate.SavedTriangles;
            entry.LastEstimateTrianglesAfter = estimate.TrianglesAfter;
            entry.LastEstimateChangedMeshes = estimate.ChangedMeshes;
            entry.LastEstimateSuccess = estimate.Success;
            entry.LastEstimateFailureReason = string.IsNullOrWhiteSpace(failureReason) ? null : failureReason;
            entry.HasLastEstimate = true;
            entry.LastEstimateUtc = DateTime.UtcNow;
            entry.EstimateAlgorithmVersion = EstimateCacheVersion;
        }
    }

    private void ClearEstimateHistory(string primaryPath)
    {
        if (string.IsNullOrWhiteSpace(primaryPath))
            return;

        lock (_historyLock)
        {
            if (!_history.TryGetValue(primaryPath, out var entry))
                return;

            entry.LastSeenLength = null;
            entry.LastSeenWriteUtcTicks = null;
            entry.HasLastEstimate = false;
            entry.LastEstimateUtc = null;
            entry.LastEstimateFailureReason = null;
            entry.EstimateAlgorithmVersion = 0;
        }
    }

    private static bool TryReadAllBytesPooled(string path, out byte[] buffer, out int length)
    {
        buffer = Array.Empty<byte>();
        length = 0;

        try
        {
            var info = new FileInfo(path);
            if (!info.Exists || info.Length <= 0 || info.Length > int.MaxValue)
                return false;

            length = checked((int)info.Length);
            buffer = ArrayPool<byte>.Shared.Rent(length);

            using var stream = new FileStream(path, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                BufferSize = EstimateReadBufferSize,
                Options = FileOptions.SequentialScan,
            });

            int offset = 0;
            while (offset < length)
            {
                int read = stream.Read(buffer, offset, Math.Min(EstimateReadBufferSize, length - offset));
                if (read <= 0)
                {
                    ReturnPooledBuffer(buffer);
                    buffer = Array.Empty<byte>();
                    length = 0;
                    return false;
                }

                offset += read;
            }

            return true;
        }
        catch
        {
            ReturnPooledBuffer(buffer);
            buffer = Array.Empty<byte>();
            length = 0;
            return false;
        }
    }

    private static void ReturnPooledBuffer(byte[]? buffer)
    {
        if (buffer == null || buffer.Length == 0)
            return;

        try
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        catch
        {
        }
    }

    private static void YieldEstimateInnerLoop(int processed, int interval)
    {
        if (interval <= 0 || processed <= 0 || processed % interval != 0)
            return;

        Thread.Yield();
    }


    private static bool TryValidateModelForRewrite(MdlFile mdl, out string reason)
    {
        if (mdl.LodCount <= 0 || mdl.Meshes.Length == 0)
        {
            reason = "Model did not expose any readable mesh metadata.";
            return false;
        }

        if (mdl.LodCount != 1)
        {
            reason = "Only single-LOD gear models are currently supported for safe cleanup.";
            return false;
        }

        if (mdl.ShapeCount > 0 || mdl.ShapeMeshCount > 0 || mdl.ShapeValueCount > 0)
        {
            reason = "Models with shape or morph data are skipped for safe cleanup.";
            return false;
        }

        if (mdl.LodOffsets.Length < mdl.LodCount || mdl.Lods.Length < mdl.LodCount)
        {
            reason = "Model did not expose enough LOD metadata for safe cleanup.";
            return false;
        }

        if (mdl.MeshStructSize < MeshIndexCountFieldOffset + sizeof(uint) || mdl.MeshOffsets.Length != mdl.Meshes.Length)
        {
            reason = "Model did not expose enough mesh table metadata for safe cleanup.";
            return false;
        }

        if (mdl.TotalSubmeshCount > 0)
        {
            if (mdl.SubMeshes.Length != mdl.TotalSubmeshCount
                || mdl.SubMeshOffsets.Length != mdl.TotalSubmeshCount
                || mdl.SubMeshStructSize < SubmeshIndexCountFieldOffset + sizeof(uint))
            {
                reason = "Model uses submeshes but did not expose enough submesh metadata for safe cleanup.";
                return false;
            }
        }

        if (mdl.AttributeCount > 0 && mdl.SubMeshes.Length == 0)
        {
            reason = "Model exposes attributes without submesh metadata, so attribute-linked visibility cannot be preserved safely.";
            return false;
        }

        reason = string.Empty;
        return true;
    }

    private bool TryPlanMeshRewrite(ReadOnlySpan<byte> bytes, RavaSync.Interop.GameModel.MdlFile mdl, int meshIndex, long absoluteStart, long absoluteVertexStart, ModelGuardContext guardContext, out MeshRewritePlan plan)
    {
        return TryPlanMeshRewriteWithPredictableCore(bytes, mdl, meshIndex, absoluteStart, absoluteVertexStart, guardContext, out plan);
    }

    private static bool TryGetSubmeshRangeWithinMesh(uint meshStartIndex, uint meshIndexCount, SubmeshStruct submesh, out uint submeshOffset, out uint submeshIndexCount)
    {
        submeshOffset = 0;
        submeshIndexCount = submesh.IndexCount;

        if (submesh.IndexOffset < meshStartIndex)
            return false;

        uint localOffset = submesh.IndexOffset - meshStartIndex;
        if (localOffset > meshIndexCount)
            return false;

        if (submeshIndexCount > meshIndexCount - localOffset)
            return false;

        submeshOffset = localOffset;
        return true;
    }

    private ModelGuardContext BuildModelGuardContextForLod(ReadOnlySpan<byte> bytes, MdlFile mdl, int lodIndex, long lodIndexDataOffset, long lodVertexDataOffset)
    {
        if (!PredictableAvoidBodyIntersection)
            return new ModelGuardContext(null);

        _ = TryBuildBodySurfaceGuideForLod(bytes, mdl, lodIndex, lodIndexDataOffset, lodVertexDataOffset, out var guide);
        return new ModelGuardContext(guide);
    }

    private bool TryBuildBodySurfaceGuideForLod(ReadOnlySpan<byte> bytes, MdlFile mdl, int lodIndex, long lodIndexDataOffset, long lodVertexDataOffset, out BodySurfaceGuide? guide)
    {
        guide = null;

        if (lodIndex < 0 || lodIndex >= mdl.LodCount)
            return false;

        int meshIndex = mdl.Lods[lodIndex].MeshIndex;
        int meshCount = mdl.Lods[lodIndex].MeshCount;
        if (meshCount <= 0)
            return false;

        int meshEnd = Math.Min(mdl.Meshes.Length, meshIndex + meshCount);
        var positions = new List<Vector3>();
        var triangles = new List<BodyTriangle>();
        double edgeLengthSum = 0d;
        long edgeLengthCount = 0;

        for (int m = meshIndex; m < meshEnd; m++)
        {
            var mesh = mdl.Meshes[m];
            bool isBodyMaterialMesh = IsProtectedBodyMesh(mdl, mesh);
            if (mesh.IndexCount < 3 || mesh.IndexCount % 3 != 0 || !isBodyMaterialMesh)
                continue;

            long absoluteStart = lodIndexDataOffset + ((long)mesh.StartIndex * sizeof(ushort));
            long absoluteEnd = absoluteStart + ((long)mesh.IndexCount * sizeof(ushort));
            if (absoluteStart < 0 || absoluteEnd > bytes.Length)
                continue;

            Vector3[] meshPositions;
            List<TriangleIndices> meshTriangles;
            double meshEdgeLengthSum;
            long meshEdgeLengthCount;
            if (!TryBuildBodyProxyGuideMesh(bytes, mdl, m, mesh, lodVertexDataOffset, lodIndexDataOffset, out meshPositions, out meshTriangles, out meshEdgeLengthSum, out meshEdgeLengthCount))
            {
                continue;
            }

            int vertexBase = positions.Count;
            positions.AddRange(meshPositions);

            foreach (var triangle in meshTriangles)
            {
                int a = vertexBase + triangle.A;
                int b = vertexBase + triangle.B;
                int c = vertexBase + triangle.C;
                if ((uint)a >= (uint)positions.Count || (uint)b >= (uint)positions.Count || (uint)c >= (uint)positions.Count)
                    continue;

                var p0 = positions[a];
                var p1 = positions[b];
                var p2 = positions[c];
                var min = Vector3.Min(p0, Vector3.Min(p1, p2));
                var max = Vector3.Max(p0, Vector3.Max(p1, p2));
                triangles.Add(new BodyTriangle(a, b, c, min, max));
            }

            edgeLengthSum += meshEdgeLengthSum;
            edgeLengthCount += meshEdgeLengthCount;
        }

        if (positions.Count == 0 || triangles.Count == 0 || edgeLengthCount <= 0)
            return false;

        float averageEdgeLength = (float)(edgeLengthSum / edgeLengthCount);
        if (!(averageEdgeLength > 0f))
            return false;

        float cellSize = MathF.Max(averageEdgeLength * BodyAdjacencyCellSizeFactor, BodyAdjacencyMinimumCellSize);
        var triangleCells = new Dictionary<BodyCellKey, List<int>>();
        for (int i = 0; i < triangles.Count; i++)
        {
            var triangle = triangles[i];
            var minCell = ToBodyCell(triangle.Min, cellSize);
            var maxCell = ToBodyCell(triangle.Max, cellSize);
            for (int x = minCell.X; x <= maxCell.X; x++)
            {
                for (int y = minCell.Y; y <= maxCell.Y; y++)
                {
                    for (int z = minCell.Z; z <= maxCell.Z; z++)
                    {
                        var key = new BodyCellKey(x, y, z);
                        if (!triangleCells.TryGetValue(key, out var list))
                        {
                            list = [];
                            triangleCells[key] = list;
                        }

                        list.Add(i);
                    }
                }
            }
        }

        guide = new BodySurfaceGuide(positions.ToArray(), triangles.ToArray(), triangleCells, averageEdgeLength, cellSize);
        return true;
    }

    private float ComputeBodyCollisionDistance(ReadOnlySpan<byte> bytes, long absoluteStart, uint indexCount, IReadOnlyList<Vector3> positions, BodySurfaceGuide bodySurface)
    {
        float referenceEdgeLength = bodySurface.AverageEdgeLength;
        if (TryComputeAverageEdgeLengthWithGpu(bytes, absoluteStart, indexCount, positions, out var gpuAverageEdgeLength) && gpuAverageEdgeLength > 0f)
            referenceEdgeLength = MathF.Max(referenceEdgeLength, gpuAverageEdgeLength);

        return Math.Clamp(referenceEdgeLength * BodyCollisionDistanceFactor, BodyAdjacencyMinimumDistance, BodyAdjacencyMaximumDistance);
    }

    private bool TryBuildBodyProxyGuideMesh(ReadOnlySpan<byte> bytes, MdlFile mdl, int meshIndex, MeshStruct mesh, long lodVertexDataOffset, long lodIndexDataOffset, out Vector3[] positions, out List<TriangleIndices> triangles, out double edgeLengthSum, out long edgeLengthCount)
    {
        positions = [];
        triangles = [];
        edgeLengthSum = 0d;
        edgeLengthCount = 0;

        long absoluteIndexStart = lodIndexDataOffset + ((long)mesh.StartIndex * sizeof(ushort));
        long absoluteIndexEnd = absoluteIndexStart + ((long)mesh.IndexCount * sizeof(ushort));
        if (absoluteIndexStart < 0 || absoluteIndexEnd > bytes.Length)
            return false;

        int submeshStart = mesh.SubMeshIndex;
        int submeshEnd = submeshStart + mesh.SubMeshCount;
        if (submeshStart < 0 || submeshEnd > mdl.SubMeshes.Length)
            return false;

        if (!TryGetVertexDeclarationForMesh(mdl, mesh, meshIndex, out var declaration))
            return false;

        if (!TryBuildPredictableVertexFormat(declaration, out var format, out _))
            return false;

        var meshSubMeshes = mdl.SubMeshes.Skip(submeshStart).Take(mesh.SubMeshCount).ToArray();
        if (!TryDecodePredictableMeshData(bytes, mdl, mesh, meshIndex, lodVertexDataOffset, absoluteIndexStart, format, meshSubMeshes, out var decoded, out var subMeshIndices, out _))
            return false;

        var proxyDecoded = decoded;
        var proxySubMeshIndices = subMeshIndices;
        int sourceTriangles = subMeshIndices.Sum(static sm => sm.Length / 3);
        bool allowProxyReduction = sourceTriangles >= PredictableBodyGuideProxyTriangleThreshold;
        int targetTriangles = allowProxyReduction
            ? Math.Max(1, (int)MathF.Floor(sourceTriangles * MathF.Max(PredictableTargetRatio, PredictableBodyProxyTargetRatioMin)))
            : 0;
        if (targetTriangles > 0
            && targetTriangles < sourceTriangles
            && TryDecimateWithPredictableCore(decoded, subMeshIndices, format, targetTriangles, null, out var decimated, out var decimatedSubMeshes, out var removedTriangles)
            && removedTriangles > 0)
        {
            proxyDecoded = decimated;
            proxySubMeshIndices = decimatedSubMeshes;
        }

        positions = new Vector3[proxyDecoded.Positions.Length];
        for (int i = 0; i < proxyDecoded.Positions.Length; i++)
            positions[i] = ToNumerics(proxyDecoded.Positions[i]);

        return TryReadBodyGuideTriangles(proxySubMeshIndices, positions, out triangles, out edgeLengthSum, out edgeLengthCount);
    }

    private static bool TryBuildPreprocessedSubmeshRewriteInfos(int submeshArrayStart, SubmeshStruct[] submeshes, out SubmeshRewriteInfo[] infos)
    {
        infos = new SubmeshRewriteInfo[submeshes.Length];
        for (int i = 0; i < submeshes.Length; i++)
        {
            infos[i] = new SubmeshRewriteInfo(submeshArrayStart + i, submeshes[i].IndexOffset, submeshes[i].IndexCount);
        }

        return true;
    }

    private static bool TryConvertIndicesToUInt16(int[] source, out ushort[] indices)
    {
        indices = [];
        if (source.Length == 0)
            return true;

        indices = new ushort[source.Length];
        for (int i = 0; i < source.Length; i++)
        {
            int value = source[i];
            if ((uint)value > ushort.MaxValue)
                return false;

            indices[i] = (ushort)value;
        }

        return true;
    }

    private static bool TryReadBodyGuideTriangles(int[][] subMeshIndices, IReadOnlyList<Vector3> positions, out List<TriangleIndices> triangles, out double edgeLengthSum, out long edgeLengthCount)
    {
        triangles = [];
        edgeLengthSum = 0d;
        edgeLengthCount = 0;

        for (int subMeshIndex = 0; subMeshIndex < subMeshIndices.Length; subMeshIndex++)
        {
            var indices = subMeshIndices[subMeshIndex];
            for (int i = 0; i + 2 < indices.Length; i += 3)
            {
                int a = indices[i];
                int b = indices[i + 1];
                int c = indices[i + 2];

                if (a == b || b == c || a == c
                    || (uint)a >= (uint)positions.Count
                    || (uint)b >= (uint)positions.Count
                    || (uint)c >= (uint)positions.Count)
                {
                    continue;
                }

                var triangle = new TriangleIndices((ushort)a, (ushort)b, (ushort)c);
                if (!TryComputeTriangleMetrics(triangle, positions, out _, out var doubleArea) || doubleArea <= 1e-8f)
                    continue;

                triangles.Add(triangle);
                edgeLengthSum += Vector3.Distance(positions[a], positions[b]);
                edgeLengthSum += Vector3.Distance(positions[b], positions[c]);
                edgeLengthSum += Vector3.Distance(positions[c], positions[a]);
                edgeLengthCount += 3;
            }
        }

        return triangles.Count > 0;
    }

    private static bool TryReadBodyGuideTriangles(ReadOnlySpan<byte> bytes, long absoluteStart, uint indexCount, IReadOnlyList<Vector3> positions, out List<TriangleIndices> triangles, out double edgeLengthSum, out long edgeLengthCount)
    {
        triangles = new List<TriangleIndices>(checked((int)(indexCount / 3)));
        edgeLengthSum = 0d;
        edgeLengthCount = 0;

        long byteEnd = absoluteStart + ((long)indexCount * sizeof(ushort));
        if (absoluteStart < 0 || byteEnd > bytes.Length)
            return false;

        int readOffset = checked((int)absoluteStart);
        for (uint i = 0; i + 2 < indexCount; i += 3)
        {
            ushort a = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(readOffset + checked((int)(i * sizeof(ushort))), sizeof(ushort)));
            ushort b = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(readOffset + checked((int)((i + 1) * sizeof(ushort))), sizeof(ushort)));
            ushort c = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(readOffset + checked((int)((i + 2) * sizeof(ushort))), sizeof(ushort)));

            if (a == b || b == c || a == c
                || (uint)a >= (uint)positions.Count
                || (uint)b >= (uint)positions.Count
                || (uint)c >= (uint)positions.Count)
            {
                continue;
            }

            var triangle = new TriangleIndices(a, b, c);
            if (!TryComputeTriangleMetrics(triangle, positions, out _, out var doubleArea) || doubleArea <= 1e-8f)
                continue;

            triangles.Add(triangle);
            edgeLengthSum += Vector3.Distance(positions[a], positions[b]);
            edgeLengthSum += Vector3.Distance(positions[b], positions[c]);
            edgeLengthSum += Vector3.Distance(positions[c], positions[a]);
            edgeLengthCount += 3;
        }

        return triangles.Count > 0;
    }

    private static float ComputeBodySurfaceDistanceSq(BodySurfaceGuide guide, Vector3 point, float maxDistanceSq)
    {
        if (guide.Triangles.Length == 0 || guide.TriangleCells.Count == 0 || maxDistanceSq <= 0f)
            return float.PositiveInfinity;

        float maxDistance = MathF.Sqrt(maxDistanceSq);
        int radius = Math.Max(1, (int)MathF.Ceiling(maxDistance / guide.CellSize));
        var cell = ToBodyCell(point, guide.CellSize);
        float minDistanceSq = float.PositiveInfinity;

        for (int x = -radius; x <= radius; x++)
        {
            for (int y = -radius; y <= radius; y++)
            {
                for (int z = -radius; z <= radius; z++)
                {
                    var key = new BodyCellKey(cell.X + x, cell.Y + y, cell.Z + z);
                    if (!guide.TriangleCells.TryGetValue(key, out var candidates))
                        continue;

                    foreach (int triangleIndex in candidates)
                    {
                        if ((uint)triangleIndex >= (uint)guide.Triangles.Length)
                            continue;

                        var triangle = guide.Triangles[triangleIndex];
                        if (DistancePointToAabbSq(point, triangle.Min, triangle.Max) > maxDistanceSq)
                            continue;

                        float distanceSq = PointTriangleDistanceSq(point, guide.Positions[triangle.A], guide.Positions[triangle.B], guide.Positions[triangle.C]);
                        if (distanceSq < minDistanceSq)
                            minDistanceSq = distanceSq;
                    }
                }
            }
        }

        return minDistanceSq;
    }

    private static float DistancePointToAabbSq(Vector3 point, Vector3 min, Vector3 max)
    {
        float dx = point.X < min.X ? min.X - point.X : point.X > max.X ? point.X - max.X : 0f;
        float dy = point.Y < min.Y ? min.Y - point.Y : point.Y > max.Y ? point.Y - max.Y : 0f;
        float dz = point.Z < min.Z ? min.Z - point.Z : point.Z > max.Z ? point.Z - max.Z : 0f;
        return (dx * dx) + (dy * dy) + (dz * dz);
    }

    private static float PointTriangleDistanceSq(Vector3 point, Vector3 a, Vector3 b, Vector3 c)
    {
        var ab = b - a;
        var ac = c - a;
        var ap = point - a;

        float d1 = Vector3.Dot(ab, ap);
        float d2 = Vector3.Dot(ac, ap);
        if (d1 <= 0f && d2 <= 0f)
            return Vector3.DistanceSquared(point, a);

        var bp = point - b;
        float d3 = Vector3.Dot(ab, bp);
        float d4 = Vector3.Dot(ac, bp);
        if (d3 >= 0f && d4 <= d3)
            return Vector3.DistanceSquared(point, b);

        float vc = (d1 * d4) - (d3 * d2);
        if (vc <= 0f && d1 >= 0f && d3 <= 0f)
        {
            float v = d1 / (d1 - d3);
            var projection = a + (v * ab);
            return Vector3.DistanceSquared(point, projection);
        }

        var cp = point - c;
        float d5 = Vector3.Dot(ab, cp);
        float d6 = Vector3.Dot(ac, cp);
        if (d6 >= 0f && d5 <= d6)
            return Vector3.DistanceSquared(point, c);

        float vb = (d5 * d2) - (d1 * d6);
        if (vb <= 0f && d2 >= 0f && d6 <= 0f)
        {
            float w = d2 / (d2 - d6);
            var projection = a + (w * ac);
            return Vector3.DistanceSquared(point, projection);
        }

        float va = (d3 * d6) - (d5 * d4);
        if (va <= 0f && (d4 - d3) >= 0f && (d5 - d6) >= 0f)
        {
            var bc = c - b;
            float w = (d4 - d3) / ((d4 - d3) + (d5 - d6));
            var projection = b + (w * bc);
            return Vector3.DistanceSquared(point, projection);
        }

        var normal = Vector3.Cross(ab, ac);
        float normalLengthSq = normal.LengthSquared();
        if (normalLengthSq <= 1e-12f)
            return float.PositiveInfinity;

        float distance = Vector3.Dot(point - a, normal);
        return (distance * distance) / normalLengthSq;
    }

    private static BodyCellKey ToBodyCell(Vector3 value, float cellSize)
    {
        float cellSizeInv = cellSize > 0f ? 1f / cellSize : 1f;
        return new BodyCellKey(
            (int)MathF.Floor(value.X * cellSizeInv),
            (int)MathF.Floor(value.Y * cellSizeInv),
            (int)MathF.Floor(value.Z * cellSizeInv));
    }

    private static MeshCleanupMode ResolveMeshCleanupMode(MdlFile mdl, int meshIndex)
    {
        if ((uint)meshIndex >= (uint)mdl.Meshes.Length)
            return MeshCleanupMode.Disabled;

        if (IsTrueBodyModelPath(mdl.SourcePath))
            return MeshCleanupMode.Disabled;

        return MeshCleanupMode.FullCollapse;
    }

    private static bool IsProtectedBodyMesh(MdlFile mdl, MeshStruct mesh)
    {
        if (!TryGetMeshMaterialReference(mdl, mesh, out var materialReference))
            return false;

        if (!IsBodyMaterialReference(materialReference))
            return false;

        return IsTrueBodyModelPath(mdl.SourcePath) || IsGearOrAccessoryModelPath(mdl.SourcePath);
    }

    private static bool TryGetMeshMaterialReference(MdlFile mdl, MeshStruct mesh, out string materialReference)
    {
        materialReference = string.Empty;
        if ((uint)mesh.MaterialIndex >= (uint)mdl.MaterialStrings.Length)
            return false;

        materialReference = mdl.MaterialStrings[mesh.MaterialIndex] ?? string.Empty;
        return !string.IsNullOrWhiteSpace(materialReference);
    }

    private static string NormalizeMeshReference(string? value)
        => (value ?? string.Empty).Replace('\\', '/').ToLowerInvariant();

    private static bool IsGearOrAccessoryModelPath(string? value)
    {
        string normalized = NormalizeMeshReference(value);
        if (string.IsNullOrEmpty(normalized))
            return false;

        return normalized.Contains("/chara/equipment/", StringComparison.Ordinal)
            || normalized.Contains("/chara/accessory/", StringComparison.Ordinal);
    }

    private static bool IsTrueBodyModelPath(string? value)
    {
        string normalized = NormalizeMeshReference(value);
        if (string.IsNullOrEmpty(normalized))
            return false;

        return normalized.Contains("/chara/human/", StringComparison.Ordinal)
            && normalized.Contains("/obj/body/", StringComparison.Ordinal)
            && normalized.Contains("/model/", StringComparison.Ordinal);
    }

    private static bool MatchesKnownBodyMaterialReference(string normalized)
    {
        if (string.IsNullOrEmpty(normalized))
            return false;

        int nameStart = normalized.LastIndexOf('/');
        string fileName = nameStart >= 0 ? normalized[(nameStart + 1)..] : normalized;
        return fileName.Contains("b0001_bibo.mtrl", StringComparison.OrdinalIgnoreCase)
            || fileName.Contains("b0101_bibo.mtrl", StringComparison.OrdinalIgnoreCase)
            || fileName.Contains("b0001_a.mtrl", StringComparison.OrdinalIgnoreCase)
            || fileName.Contains("b0001_b.mtrl", StringComparison.OrdinalIgnoreCase)
            || fileName.Contains("b0101_a.mtrl", StringComparison.OrdinalIgnoreCase)
            || fileName.Contains("b0101_b.mtrl", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsBodyMaterialReference(string? value)
    {
        string normalized = NormalizeMeshReference(value);
        if (string.IsNullOrEmpty(normalized))
            return false;

        return MatchesKnownBodyMaterialReference(normalized);
    }

    private static bool TryReadVector3Channel(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, byte elementType, out Vector3[] values)
    {
        values = [];
        if (!TryValidateVertexStream(mesh, absoluteVertexStart, streamIndex, elementOffset, out int stride, out long streamStart, bytes.Length))
            return false;

        values = new Vector3[mesh.VertexCount];
        for (int i = 0; i < mesh.VertexCount; i++)
        {
            YieldEstimateInnerLoop(i + 1, EstimateVertexReadYieldEveryVertices);
            int valueOffset = checked((int)(streamStart + (i * stride) + elementOffset));
            if (!TryReadVector3(bytes, valueOffset, elementType, out var value))
                return false;

            values[i] = value;
        }

        return true;
    }

    private static bool TryReadVector2Channel(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, byte elementType, out Vector2[] values)
    {
        values = [];
        if (!TryValidateVertexStream(mesh, absoluteVertexStart, streamIndex, elementOffset, out int stride, out long streamStart, bytes.Length))
            return false;

        values = new Vector2[mesh.VertexCount];
        for (int i = 0; i < mesh.VertexCount; i++)
        {
            YieldEstimateInnerLoop(i + 1, EstimateVertexReadYieldEveryVertices);
            int valueOffset = checked((int)(streamStart + (i * stride) + elementOffset));
            if (!TryReadVector2(bytes, valueOffset, elementType, out var value))
                return false;

            values[i] = value;
        }

        return true;
    }


    private static bool TryReadBlendIndicesChannel(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, byte elementType, out MeshSkinningVertex[] indices)
    {
        indices = [];
        if (!TryValidateVertexStream(mesh, absoluteVertexStart, streamIndex, elementOffset, requiredSize: 4, out int stride, out long streamStart, bytes.Length))
            return false;

        indices = new MeshSkinningVertex[mesh.VertexCount];
        for (int i = 0; i < mesh.VertexCount; i++)
        {
            YieldEstimateInnerLoop(i + 1, EstimateVertexReadYieldEveryVertices);
            int valueOffset = checked((int)(streamStart + (i * stride) + elementOffset));
            if (!TryReadBlendIndices(bytes, valueOffset, elementType, out var value))
                return false;

            indices[i] = value;
        }

        return true;
    }

    private static bool TryReadBlendWeightsChannel(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, byte elementType, out Vector4[] weights)
    {
        weights = [];
        int requiredSize = GetBlendWeightReadSize(elementType);
        if (!TryValidateVertexStream(mesh, absoluteVertexStart, streamIndex, elementOffset, requiredSize, out int stride, out long streamStart, bytes.Length))
            return false;

        weights = new Vector4[mesh.VertexCount];
        for (int i = 0; i < mesh.VertexCount; i++)
        {
            YieldEstimateInnerLoop(i + 1, EstimateVertexReadYieldEveryVertices);
            int valueOffset = checked((int)(streamStart + (i * stride) + elementOffset));
            if (!TryReadBlendWeights(bytes, valueOffset, elementType, out var value))
                return false;

            weights[i] = value;
        }

        return true;
    }

    private static bool TryValidateVertexStream(MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, out int stride, out long streamStart, long totalLength)
        => TryValidateVertexStream(mesh, absoluteVertexStart, streamIndex, elementOffset, requiredSize: 1, out stride, out streamStart, totalLength);

    private static bool TryValidateVertexStream(MeshStruct mesh, long absoluteVertexStart, int streamIndex, int elementOffset, int requiredSize, out int stride, out long streamStart, long totalLength)
    {
        stride = 0;
        streamStart = 0;

        if ((uint)streamIndex >= (uint)mesh.VertexBufferStride.Length || (uint)streamIndex >= (uint)mesh.VertexBufferOffset.Length)
            return false;

        stride = mesh.VertexBufferStride[streamIndex];
        if (stride <= 0 || elementOffset < 0 || elementOffset >= stride)
            return false;

        if (requiredSize <= 0)
            requiredSize = 1;

        if (elementOffset > stride - requiredSize)
            return false;

        streamStart = absoluteVertexStart + mesh.VertexBufferOffset[streamIndex];
        long streamEnd = streamStart + ((long)mesh.VertexCount * stride);
        return streamStart >= 0 && streamEnd <= totalLength;
    }

    private static bool TryReadVector3(ReadOnlySpan<byte> bytes, int offset, byte elementType, out Vector3 value)
    {
        value = default;

        switch (elementType)
        {
            case Float3Type:
                if (offset + 12 > bytes.Length)
                    return false;

                value = new Vector3(
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 0, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 4, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 8, 4)));
                return true;
            case Float4Type:
                if (offset + 16 > bytes.Length)
                    return false;

                value = new Vector3(
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 0, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 4, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 8, 4)));
                return true;
            case Half4Type:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector3(
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 2, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 4, 2))));
                return true;
            case Half2Type:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector3(
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 2, 2))),
                    0f);
                return true;
            case UByte4NType:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector3(
                    DecodeSignedNormalizedByte(bytes[offset + 0]),
                    DecodeSignedNormalizedByte(bytes[offset + 1]),
                    DecodeSignedNormalizedByte(bytes[offset + 2]));
                return true;
            case Short4NType:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector3(
                    DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 2, 2))),
                    DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 4, 2))));
                return true;
            default:
                return false;
        }
    }

    private static bool TryReadVector2(ReadOnlySpan<byte> bytes, int offset, byte elementType, out Vector2 value)
    {
        value = default;

        switch (elementType)
        {
            case Float2Type:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector2(
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 0, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 4, 4)));
                return true;
            case Float3Type:
            case Float4Type:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector2(
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 0, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 4, 4)));
                return true;
            case Half2Type:
            case Half4Type:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector2(
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 2, 2))));
                return true;
            case Short2NType:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector2(
                    DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 2, 2))));
                return true;
            case UShort2NType:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector2(
                    DecodeUnsignedNormalizedShort(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    DecodeUnsignedNormalizedShort(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 2, 2))));
                return true;
            default:
                return false;
        }
    }


    private static bool TryReadBlendIndices(ReadOnlySpan<byte> bytes, int offset, byte elementType, out MeshSkinningVertex value)
    {
        value = default;
        if (offset + 4 > bytes.Length)
            return false;

        value = new MeshSkinningVertex(
            bytes[offset + 0],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            Vector4.Zero);
        return true;
    }

    private static bool TryReadBlendWeights(ReadOnlySpan<byte> bytes, int offset, byte elementType, out Vector4 value)
    {
        value = default;

        switch (elementType)
        {
            case Float4Type:
                if (offset + 16 > bytes.Length)
                    return false;

                value = new Vector4(
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 0, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 4, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 8, 4)),
                    BinaryPrimitives.ReadSingleLittleEndian(bytes.Slice(offset + 12, 4)));
                return true;
            case Half4Type:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector4(
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 0, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 2, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 4, 2))),
                    HalfToSingle(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset + 6, 2))));
                return true;
            case Short4NType:
                if (offset + 8 > bytes.Length)
                    return false;

                value = new Vector4(
                    MathF.Max(0f, DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 0, 2)))),
                    MathF.Max(0f, DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 2, 2)))),
                    MathF.Max(0f, DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 4, 2)))),
                    MathF.Max(0f, DecodeSignedNormalizedShort(BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(offset + 6, 2)))));
                return true;
            default:
                if (offset + 4 > bytes.Length)
                    return false;

                value = new Vector4(
                    bytes[offset + 0] / 255f,
                    bytes[offset + 1] / 255f,
                    bytes[offset + 2] / 255f,
                    bytes[offset + 3] / 255f);
                return true;
        }
    }

    private static int GetBlendWeightReadSize(byte elementType)
        => elementType switch
        {
            Float4Type => 16,
            Half4Type => 8,
            Short4NType => 8,
            _ => 4,
        };

    private static Vector4 NormaliseBlendWeights(Vector4 weights)
    {
        weights = new Vector4(
            MathF.Max(0f, weights.X),
            MathF.Max(0f, weights.Y),
            MathF.Max(0f, weights.Z),
            MathF.Max(0f, weights.W));

        float total = weights.X + weights.Y + weights.Z + weights.W;
        if (total <= 1e-6f)
            return Vector4.Zero;

        return weights / total;
    }

    private static float HalfToSingle(ushort value)
        => (float)BitConverter.UInt16BitsToHalf(value);

    private static float DecodeSignedNormalizedByte(byte value)
        => Math.Clamp(((value / 255f) * 2f) - 1f, -1f, 1f);

    private static float DecodeSignedNormalizedShort(short value)
        => Math.Clamp(value / 32767f, -1f, 1f);

    private static float DecodeUnsignedNormalizedShort(ushort value)
        => value / 65535f;

    private static bool TryResolveVertexElement(MdlFile mdl, MeshStruct mesh, int meshIndex, byte usage, byte usageIndex, out int streamIndex, out int elementOffset, out byte elementType)
    {
        streamIndex = 0;
        elementOffset = 0;
        elementType = 0;

        if (!TryGetVertexDeclarationForMesh(mdl, mesh, meshIndex, out var declaration))
            return false;

        foreach (var element in declaration.VertexElements)
        {
            if (element.Usage == usage && element.UsageIndex == usageIndex)
            {
                streamIndex = element.Stream;
                elementOffset = element.Offset;
                elementType = element.Type;
                return true;
            }
        }

        return false;
    }

    private static bool TryGetVertexDeclarationForMesh(MdlFile mdl, MeshStruct mesh, int meshIndex, out MdlFile.VertexDeclarationStruct declaration)
    {
        declaration = default;
        if (mdl.VertexDeclarations.Length == 0)
            return false;

        if (mdl.VertexDeclarations.Length == 1)
        {
            declaration = mdl.VertexDeclarations[0];
            return true;
        }

        if ((uint)meshIndex < (uint)mdl.VertexDeclarations.Length)
        {
            declaration = mdl.VertexDeclarations[meshIndex];
            return true;
        }

        var first = mdl.VertexDeclarations[0];
        if (mdl.VertexDeclarations.All(v => PositionElementEquivalent(v, first)))
        {
            declaration = first;
            return true;
        }

        return false;
    }

    private static bool PositionElementEquivalent(MdlFile.VertexDeclarationStruct left, MdlFile.VertexDeclarationStruct right)
    {
        var leftPosition = left.VertexElements.FirstOrDefault(static e => e.Usage == PositionUsage);
        var rightPosition = right.VertexElements.FirstOrDefault(static e => e.Usage == PositionUsage);
        return leftPosition.Stream == rightPosition.Stream
            && leftPosition.Offset == rightPosition.Offset
            && leftPosition.Type == rightPosition.Type;
    }

    private static bool TryComputeTriangleMetrics(TriangleIndices triangle, IReadOnlyList<Vector3> positions, out Vector3 normal, out float doubleArea)
    {
        normal = default;
        doubleArea = 0;

        if ((uint)triangle.A >= (uint)positions.Count
            || (uint)triangle.B >= (uint)positions.Count
            || (uint)triangle.C >= (uint)positions.Count)
        {
            return false;
        }

        var ab = positions[triangle.B] - positions[triangle.A];
        var ac = positions[triangle.C] - positions[triangle.A];
        var cross = Vector3.Cross(ab, ac);
        doubleArea = cross.Length();
        if (doubleArea <= 1e-8f)
            return false;

        normal = cross / doubleArea;
        return true;
    }


    private static bool TryGetMeshVertexStreams(ReadOnlySpan<byte> bytes, MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, out MeshVertexStreams streams)
    {
        streams = default;

        if (!TryGetMeshPositions(bytes, mdl, mesh, meshIndex, absoluteVertexStart, out var positions))
            return false;

        _ = TryGetMeshNormals(bytes, mdl, mesh, meshIndex, absoluteVertexStart, out var normals);
        _ = TryGetMeshPrimaryUvs(bytes, mdl, mesh, meshIndex, absoluteVertexStart, out var primaryUvs);
        _ = TryGetMeshSkinning(bytes, mdl, mesh, meshIndex, absoluteVertexStart, out var skinning);

        streams = new MeshVertexStreams(positions, normals, primaryUvs, skinning);
        return true;
    }

    private static bool TryGetMeshPositions(ReadOnlySpan<byte> bytes, MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, out Vector3[] positions)
    {
        positions = [];
        if (mesh.VertexCount <= 0)
            return false;

        if (!TryResolveVertexElement(mdl, mesh, meshIndex, PositionUsage, 0, out int streamIndex, out int elementOffset, out byte elementType))
            return false;

        return TryReadVector3Channel(bytes, mesh, absoluteVertexStart, streamIndex, elementOffset, elementType, out positions);
    }

    private static bool TryGetMeshNormals(ReadOnlySpan<byte> bytes, MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, out Vector3[] normals)
    {
        normals = [];
        if (mesh.VertexCount <= 0)
            return false;

        if (!TryResolveVertexElement(mdl, mesh, meshIndex, NormalUsage, 0, out int streamIndex, out int elementOffset, out byte elementType))
            return false;

        if (!TryReadVector3Channel(bytes, mesh, absoluteVertexStart, streamIndex, elementOffset, elementType, out normals))
            return false;

        for (int i = 0; i < normals.Length; i++)
        {
            if (normals[i].LengthSquared() > 1e-10f)
                normals[i] = Vector3.Normalize(normals[i]);
        }

        return true;
    }

    private static bool TryGetMeshPrimaryUvs(ReadOnlySpan<byte> bytes, MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, out Vector2[] primaryUvs)
    {
        primaryUvs = [];
        if (mesh.VertexCount <= 0)
            return false;

        if (!TryResolveVertexElement(mdl, mesh, meshIndex, TexCoordUsage, 0, out int streamIndex, out int elementOffset, out byte elementType))
            return false;

        return TryReadVector2Channel(bytes, mesh, absoluteVertexStart, streamIndex, elementOffset, elementType, out primaryUvs);
    }

    private static bool TryGetMeshSkinning(ReadOnlySpan<byte> bytes, MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, out MeshSkinningVertex[] skinning)
    {
        skinning = [];
        if (mesh.VertexCount <= 0)
            return false;

        if (!TryResolveVertexElement(mdl, mesh, meshIndex, BlendIndicesUsage, 0, out int indexStream, out int indexOffset, out byte indexType))
            return false;

        if (!TryResolveVertexElement(mdl, mesh, meshIndex, BlendWeightUsage, 0, out int weightStream, out int weightOffset, out byte weightType))
            return false;

        if (!TryReadBlendIndicesChannel(bytes, mesh, absoluteVertexStart, indexStream, indexOffset, indexType, out var blendIndices))
            return false;

        if (!TryReadBlendWeightsChannel(bytes, mesh, absoluteVertexStart, weightStream, weightOffset, weightType, out var blendWeights))
            return false;

        int count = Math.Min(blendIndices.Length, blendWeights.Length);
        if (count <= 0)
            return false;

        skinning = new MeshSkinningVertex[count];
        for (int i = 0; i < count; i++)
        {
            skinning[i] = new MeshSkinningVertex(blendIndices[i].Bone0, blendIndices[i].Bone1, blendIndices[i].Bone2, blendIndices[i].Bone3, NormaliseBlendWeights(blendWeights[i]));
        }

        return true;
    }

    private readonly record struct TriangleIndices(ushort A, ushort B, ushort C)
    {
        public bool IsDegenerate => A == B || B == C || A == C;

        public TriangleIndices Replace(ushort fromVertex, ushort toVertex)
            => new(A == fromVertex ? toVertex : A, B == fromVertex ? toVertex : B, C == fromVertex ? toVertex : C);
    }

    private readonly record struct QuantizedPositionKey(int X, int Y, int Z)
    {
        public static QuantizedPositionKey From(Vector3 value, float step)
        {
            if (step <= 0)
                step = 1e-4f;

            return new QuantizedPositionKey((int)MathF.Round(value.X / step), (int)MathF.Round(value.Y / step), (int)MathF.Round(value.Z / step));
        }
    }

    private readonly record struct EdgeKey(ushort A, ushort B)
    {
        public static EdgeKey Create(ushort a, ushort b)
            => a <= b ? new EdgeKey(a, b) : new EdgeKey(b, a);
    }

    private readonly record struct CollapsePhaseSettings(bool AllowDetailPass, bool RelaxedPass, int MaxIterations, int StagnationLimit, float BatchFraction, int MinRequestedRemoval, int MaxRequestedRemoval);
    private readonly record struct MicroCollapseCandidate(ushort KeepVertex, ushort DropVertex, IReadOnlyList<int> IncidentTriangles, float Score);
    private readonly record struct FastCollapseCandidate(ushort KeepVertex, ushort DropVertex, int EstimatedRemovedTriangles, float Score);
    private readonly record struct GpuCollapseSeedCandidate(ushort KeepVertex, ushort DropVertex, bool IsBoundaryEdge, int BoundaryPressure, int SharedTriangleCount, float LocalMedianEdgeLength, float ReferenceEdgeLength, ReductionRegionProfile RegionProfile, D3D11MeshAnalysisService.GpuCollapseCandidate GpuCandidate);
    private readonly record struct ReductionRegionProfile(int RegionId, int TriangleCount, float MedianEdgeLength, float MedianTriangleDoubleArea, float MaxSpan, float CompactnessRatio, float AverageTriangleNormalAlignment, bool IsSmallDetailSurface, bool IsTinyRoundedSurface);
    private readonly struct MeshVertexStreams
    {
        public MeshVertexStreams(Vector3[] positions, Vector3[]? normals, Vector2[]? primaryUvs, MeshSkinningVertex[]? skinning)
        {
            Positions = positions ?? [];
            Normals = normals;
            PrimaryUvs = primaryUvs;
            Skinning = skinning;
        }

        public Vector3[] Positions { get; }
        public Vector3[]? Normals { get; }
        public Vector2[]? PrimaryUvs { get; }
        public MeshSkinningVertex[]? Skinning { get; }
    }
    private readonly record struct MeshSkinningVertex(byte Bone0, byte Bone1, byte Bone2, byte Bone3, Vector4 Weights);
    private readonly record struct BoneWeightPair(byte Bone, float Weight);
    private sealed record ModelGuardContext(BodySurfaceGuide? BodySurface);
    private readonly record struct BodyCollisionGuard(BodySurfaceGuide Surface, float DistanceSq);
    private readonly record struct BodySurfaceGuide(Vector3[] Positions, BodyTriangle[] Triangles, Dictionary<BodyCellKey, List<int>> TriangleCells, float AverageEdgeLength, float CellSize);
    private readonly record struct BodyTriangle(int A, int B, int C, Vector3 Min, Vector3 Max);
    private readonly record struct BodyCellKey(int X, int Y, int Z);

    private const int EstimateCacheVersion = 14;
    private const byte PositionUsage = 0;
    private const byte BlendWeightUsage = 1;
    private const byte BlendIndicesUsage = 2;
    private const byte NormalUsage = 3;
    private const byte TexCoordUsage = 4;
    private const byte Float2Type = 1;
    private const byte Float3Type = 2;
    private const byte Float4Type = 3;
    private const byte UByte4NType = 8;
    private const byte Short2NType = 9;
    private const byte Short4NType = 10;
    private const byte UShort2NType = 16;
    private const float BodyCollisionDistanceFactor = 0.40f;
    private const float BodyAdjacencyCellSizeFactor = 1.0f;
    private const float BodyAdjacencyMinimumDistance = 1e-4f;
    private const float BodyAdjacencyMaximumDistance = 0.030f;
    private const float BodyAdjacencyMinimumCellSize = 1e-4f;
    private const float PositionTwinQuantizationStep = 1e-6f;
    private const int BackgroundEstimateSettleMs = 900;
    private const int BackgroundEstimatePauseMs = 180;
    private const int EstimateReadBufferSize = 256 * 1024;
    private const int EstimateVertexReadYieldEveryVertices = 2048;



    private static void MirrorPrimaryToAlternates(MeshWorkItem item)
    {
        foreach (var alternate in item.AlternatePaths)
        {
            if (string.IsNullOrWhiteSpace(alternate))
                continue;

            var directory = Path.GetDirectoryName(alternate);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.Copy(item.PrimaryPath, alternate, overwrite: true);
        }
    }

    private void LoadHistory()
    {
        try
        {
            if (!File.Exists(_historyPath))
                return;

            var raw = File.ReadAllText(_historyPath);
            if (string.IsNullOrWhiteSpace(raw))
                return;

            try
            {
                var loaded = JsonSerializer.Deserialize<Dictionary<string, MeshOptimisationHistoryEntry>>(raw);
                if (loaded != null)
                {
                    _history = new Dictionary<string, MeshOptimisationHistoryEntry>(loaded, StringComparer.OrdinalIgnoreCase);
                    return;
                }
            }
            catch
            {
            }

            var rebuilt = new Dictionary<string, MeshOptimisationHistoryEntry>(StringComparer.OrdinalIgnoreCase);
            using var reader = new StringReader(raw);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                MeshOptimisationHistoryRecord? record;
                try
                {
                    record = JsonSerializer.Deserialize<MeshOptimisationHistoryRecord>(line);
                }
                catch
                {
                    continue;
                }

                if (record == null || string.IsNullOrWhiteSpace(record.PrimaryPath))
                    continue;

                rebuilt[record.PrimaryPath] = new MeshOptimisationHistoryEntry
                {
                    OriginalTriangles = record.OriginalTriangles,
                    ReducedTriangles = record.ReducedTriangles,
                    UpdatedUtc = record.UpdatedUtc,
                };
            }

            _history = rebuilt;
        }
        catch
        {
            _history = new Dictionary<string, MeshOptimisationHistoryEntry>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private void RecordOptimisedMesh(string primaryPath, long trianglesBefore, long trianglesAfter)
    {
        if (string.IsNullOrWhiteSpace(primaryPath) || trianglesBefore <= 0 || trianglesAfter <= 0 || trianglesAfter >= trianglesBefore)
            return;

        var updatedUtc = DateTime.UtcNow;

        lock (_historyLock)
        {
            if (!_history.TryGetValue(primaryPath, out var entry))
            {
                entry = new MeshOptimisationHistoryEntry();
                _history[primaryPath] = entry;
            }

            entry.OriginalTriangles = trianglesBefore;
            entry.ReducedTriangles = trianglesAfter;
            entry.UpdatedUtc = updatedUtc;
            entry.LastSeenLength = null;
            entry.LastSeenWriteUtcTicks = null;
            entry.HasLastEstimate = false;
            entry.LastEstimateUtc = null;
        }

        AppendOptimisationHistory(primaryPath, trianglesBefore, trianglesAfter, updatedUtc);
    }

    private void AppendOptimisationHistory(string primaryPath, long trianglesBefore, long trianglesAfter, DateTime updatedUtc)
    {
        try
        {
            var dir = Path.GetDirectoryName(_historyPath);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);

            var line = JsonSerializer.Serialize(new MeshOptimisationHistoryRecord(primaryPath, trianglesBefore, trianglesAfter, updatedUtc));
            File.AppendAllText(_historyPath, line + Environment.NewLine);
        }
        catch
        {
        }
    }

    private static long CountTriangles(string path)
    {
        var mdl = new MdlFile(path);
        long total = 0;
        foreach (var mesh in mdl.Meshes)
        {
            if (mesh.IndexCount >= 3)
                total += mesh.IndexCount / 3;
        }

        return total;
    }

    private readonly record struct SubmeshMetadataUpdate(int SubmeshArrayIndex, uint IndexOffset, uint IndexCount);
    private readonly record struct SubmeshRewriteInfo(int SubmeshArrayIndex, uint RelativeIndexOffset, uint IndexCount);
    private sealed record MeshCompactionPayload(byte[][] Streams, ushort[] Indices, SubmeshRewriteInfo[] SubmeshInfos, int VertexCount, long RemovedTriangles, bool Changed);
    private sealed record MeshRewritePlan(IReadOnlyList<ushort> RewrittenIndices, long RemovedTriangles, IReadOnlyList<SubmeshMetadataUpdate> SubmeshUpdates, byte[][]? ReplacementStreams = null, int ReplacementVertexCount = 0)
    {
        public bool HasReplacementPayload => ReplacementStreams != null && ReplacementStreams.Length == 3 && ReplacementVertexCount > 0;
    }

    private sealed record MeshWorkItem(string PrimaryPath, IReadOnlyList<string> AlternatePaths);
    private sealed record MeshEstimateCacheEntry(long Length, long LastWriteTimeUtcTicks, long TrianglesBefore, long RemovableTriangles, long TrianglesAfter, int ChangedMeshes, bool Success, int AlgorithmVersion, string FailureReason);
    private readonly record struct PendingEstimateRequest(string PrimaryPath, long CurrentTriangles, long Priority, long QueuedUtcTicks);

    private readonly record struct MeshExecutionResult(bool Succeeded, long RemovedTriangles, long RemovedBytes, int ChangedMeshes, long TrianglesBefore, long TrianglesAfter, string Detail)
    {
        public static MeshExecutionResult Success(long removedTriangles, long removedBytes, int changedMeshes, long trianglesBefore, long trianglesAfter, string detail)
            => new(true, removedTriangles, removedBytes, changedMeshes, trianglesBefore, trianglesAfter, detail);

        public static MeshExecutionResult Failure(string detail)
            => new(false, 0, 0, 0, 0, 0, detail);
    }

    private readonly record struct ComponentDecimationAnalysis(ComponentDecimationMode Mode, ComponentStopReason StopReason, HashSet<ushort> ProtectedVertices, float ProtectedRatio, float NearBodyRatio, float BoundaryRatio, float InteriorEdgeRatio, int TargetTriangles);
    private readonly record struct ComponentDecimationOutcome(ComponentDecimationMode Mode, ComponentStopReason StopReason, int SourceTriangles, int TargetTriangles, int ResultTriangles, long RemovedTriangles);

    private sealed record MeshOptimisationHistoryRecord(string PrimaryPath, long OriginalTriangles, long ReducedTriangles, DateTime UpdatedUtc);

    private sealed class MeshOptimisationHistoryEntry
    {
        public long OriginalTriangles { get; set; }
        public long ReducedTriangles { get; set; }
        public DateTime UpdatedUtc { get; set; }
        public long? LastSeenLength { get; set; }
        public long? LastSeenWriteUtcTicks { get; set; }
        public long LastEstimateTrianglesBefore { get; set; }
        public long LastEstimateSavedTriangles { get; set; }
        public long LastEstimateTrianglesAfter { get; set; }
        public int LastEstimateChangedMeshes { get; set; }
        public bool LastEstimateSuccess { get; set; }
        public string? LastEstimateFailureReason { get; set; }
        public bool HasLastEstimate { get; set; }
        public DateTime? LastEstimateUtc { get; set; }
        public int EstimateAlgorithmVersion { get; set; }

        public bool MatchesEstimateFingerprint(FileInfo info)
            => EstimateAlgorithmVersion == EstimateCacheVersion
                && LastSeenLength.HasValue
                && LastSeenWriteUtcTicks.HasValue
                && LastSeenLength.Value == info.Length
                && LastSeenWriteUtcTicks.Value == info.LastWriteTimeUtc.Ticks;

        public bool TryGetEstimate(out MeshSavingsEstimate estimate)
        {
            if (!HasLastEstimate)
            {
                estimate = default;
                return false;
            }

            estimate = new MeshSavingsEstimate(
                LastEstimateTrianglesBefore,
                LastEstimateSavedTriangles,
                LastEstimateTrianglesAfter,
                LastEstimateChangedMeshes,
                LastEstimateSuccess);
            return true;
        }
    }
}
