using Lumina.Data.Files;
using System.Collections.Concurrent;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.Services.Mediator;
using RavaSync.UI;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;

namespace RavaSync.Services;

public sealed class CharacterAnalyzer : MediatorSubscriberBase, IDisposable
{
    private readonly FileCacheManager _fileCacheManager;
    private readonly TransientResourceManager _transientResourceManager;
    private readonly XivDataAnalyzer _xivDataAnalyzer;
    private CancellationTokenSource? _analysisCts;
    private CancellationTokenSource _baseAnalysisCts = new();
    private string _lastDataHash = string.Empty;

    private readonly ConcurrentDictionary<string, AggregateMetricsEntry> _aggregateMetricsByDataHash = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _aggregateMetricWaiters = new(StringComparer.Ordinal);
    private string _latestAnalyzedDataHash = string.Empty;

    public CharacterAnalyzer(ILogger<CharacterAnalyzer> logger, MareMediator mediator, FileCacheManager fileCacheManager, XivDataAnalyzer modelAnalyzer, TransientResourceManager transientResourceManager)
        : base(logger, mediator)
    {
        Mediator.Subscribe<CharacterDataCreatedMessage>(this, (msg) =>
        {
            var newHash = msg.CharacterData?.DataHash.Value ?? string.Empty;
            if (!string.Equals(newHash, _lastDataHash, StringComparison.Ordinal))
            {
                _latestAnalyzedDataHash = string.Empty;
                LastAnalysis.Clear();
            }

            _baseAnalysisCts = _baseAnalysisCts.CancelRecreate();
            var token = _baseAnalysisCts.Token;
            PrimeAggregateMetrics(newHash);
            _ = BaseAnalysis(msg.CharacterData, token);
        });
        _fileCacheManager = fileCacheManager;
        _transientResourceManager = transientResourceManager;
        _xivDataAnalyzer = modelAnalyzer;
    }

    public string LatestAnalyzedDataHash => _latestAnalyzedDataHash;

    public bool TryGetAggregateMetrics(string? dataHash, out long vramBytes, out long triangles)
    {
        vramBytes = 0;
        triangles = 0;

        if (string.IsNullOrWhiteSpace(dataHash))
            return false;

        if (!_aggregateMetricsByDataHash.TryGetValue(dataHash, out var entry))
            return false;

        vramBytes = entry.VramBytes;
        triangles = entry.Triangles;
        return true;
    }

    public bool TryGetDisplayedHeaderMetrics(string? dataHash, out long vramBytes, out long triangles)
    {
        vramBytes = 0;
        triangles = 0;

        if (string.IsNullOrWhiteSpace(dataHash))
            return false;

        if (!string.Equals(_latestAnalyzedDataHash, dataHash, StringComparison.Ordinal))
            return false;

        var analysis = LastAnalysis;
        if (analysis == null || analysis.Count == 0)
            return false;

        var seenHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        KeyValuePair<ObjectKind, Dictionary<string, FileDataEntry>>[] snapshot;
        try
        {
            snapshot = analysis.ToArray();
        }
        catch
        {
            return false;
        }

        foreach (var kv in snapshot)
        {
            FileDataEntry[] entries;
            try
            {
                entries = kv.Value.Values.ToArray();
            }
            catch
            {
                continue;
            }

            foreach (var entry in entries)
            {
                if (entry == null)
                    continue;

                if (!ShouldCountForDisplayedPerformance(kv.Key, entry))
                    continue;

                if (!string.IsNullOrEmpty(entry.Hash) && seenHashes.Add(entry.Hash))
                    vramBytes += Math.Max(0, entry.VramBytes);

                if (!string.IsNullOrEmpty(entry.Hash)
                    && (string.Equals(entry.FileType, "mdl", StringComparison.OrdinalIgnoreCase)
                        || (entry.GamePaths?.Any(path => path.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false))
                    && seenModelHashes.Add(entry.Hash))
                {
                    triangles += Math.Max(0, entry.Triangles);
                }
            }
        }

        return vramBytes > 0 || triangles > 0;
    }

    public void PrimeAggregateMetrics(string? dataHash)
    {
        if (string.IsNullOrWhiteSpace(dataHash))
            return;

        _ = GetOrCreateAggregateMetricWaiter(dataHash);
    }

    public async Task<bool> WaitForAggregateMetricsAsync(string? dataHash, int timeoutMs, CancellationToken cancellationToken)
    {
        if (TryGetAggregateMetrics(dataHash, out _, out _))
            return true;

        if (string.IsNullOrWhiteSpace(dataHash))
            return false;

        var waiter = GetOrCreateAggregateMetricWaiter(dataHash);

        if (waiter.Task.IsCompleted)
            return TryGetAggregateMetrics(dataHash, out _, out _);

        try
        {
            using var timeoutCts = timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : null;
            using var linkedCts = timeoutCts != null
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            await waiter.Task.WaitAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            if (cancellationToken.IsCancellationRequested)
                throw;
        }

        return TryGetAggregateMetrics(dataHash, out _, out _);
    }

    public int CurrentFile { get; internal set; }
    public bool IsAnalysisRunning => _analysisCts != null;
    public int TotalFiles { get; internal set; }
    internal Dictionary<ObjectKind, Dictionary<string, FileDataEntry>> LastAnalysis { get; } = [];

    public void CancelAnalyze()
    {
        _analysisCts?.CancelDispose();
        _analysisCts = null;
    }

    public async Task ComputeAnalysis(bool print = true, bool recalculate = false)
    {
        Logger.LogDebug("=== Calculating Character Analysis ===");

        _analysisCts = _analysisCts?.CancelRecreate() ?? new();

        var cancelToken = _analysisCts.Token;

        var allFiles = LastAnalysis.SelectMany(v => v.Value.Select(d => d.Value)).ToList();
        if (allFiles.Exists(c => !c.IsComputed || recalculate))
        {
            var remaining = allFiles.Where(c => !c.IsComputed || recalculate).ToList();
            TotalFiles = remaining.Count;
            CurrentFile = 1;
            Logger.LogDebug("=== Computing {amount} remaining files ===", remaining.Count);

            Mediator.Publish(new HaltScanMessage(nameof(CharacterAnalyzer)));
            try
            {
                foreach (var file in remaining)
                {
                    Logger.LogDebug("Computing file {file}", file.FilePaths[0]);
                    await file.ComputeSizes(_fileCacheManager, cancelToken).ConfigureAwait(false);
                    CurrentFile++;
                }

                _fileCacheManager.WriteOutFullCsv();

            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to analyze files");
            }
            finally
            {
                Mediator.Publish(new ResumeScanMessage(nameof(CharacterAnalyzer)));
            }
        }

        Mediator.Publish(new CharacterDataAnalyzedMessage());

        _analysisCts.CancelDispose();
        _analysisCts = null;

        if (print) PrintAnalysis();
    }

    public void Dispose()
    {
        _analysisCts.CancelDispose();
    }

    private async Task BaseAnalysis(CharacterData charaData, CancellationToken token)
    {
        var aggregateHash = charaData.DataHash.Value;
        var waiter = string.IsNullOrWhiteSpace(aggregateHash)
            ? null
            : GetOrCreateAggregateMetricWaiter(aggregateHash);

        try
        {
            if (string.Equals(aggregateHash, _lastDataHash, StringComparison.Ordinal))
            {
                waiter?.TrySetResult(true);
                return;
            }

            LastAnalysis.Clear();

            var semiTransientByKind = new Dictionary<ObjectKind, HashSet<string>>();

            foreach (var obj in charaData.FileReplacements)
            {
                if (!semiTransientByKind.TryGetValue(obj.Key, out var semiTransientPaths))
                    semiTransientByKind[obj.Key] = semiTransientPaths = _transientResourceManager.GetSemiTransientResources(obj.Key);

                Dictionary<string, FileDataEntry> data = new(StringComparer.OrdinalIgnoreCase);
                foreach (var fileEntry in obj.Value)
                {
                    token.ThrowIfCancellationRequested();

                    bool semiTransientOnly = fileEntry.GamePaths.Count() > 0 && semiTransientPaths.Count > 0;
                    if (semiTransientOnly)
                    {
                        foreach (var gamePath in fileEntry.GamePaths)
                        {
                            if (!semiTransientPaths.Contains(gamePath))
                            {
                                semiTransientOnly = false;
                                break;
                            }
                        }
                    }

                    if (semiTransientOnly)
                        continue;

                    var fileCacheEntries = _fileCacheManager
                        .GetAllFileCachesByHash(fileEntry.Hash, ignoreCacheEntries: true, validate: false)
                        .ToList();

                    if (fileCacheEntries.Count == 0)
                        continue;

                    var primaryCache = _fileCacheManager.GetFileCacheByHash(fileEntry.Hash);
                    var primaryPath = primaryCache?.ResolvedFilepath;
                    if (string.IsNullOrWhiteSpace(primaryPath) || !File.Exists(primaryPath))
                    {
                        primaryPath = fileCacheEntries
                            .Select(c => c.ResolvedFilepath)
                            .Where(p => !string.IsNullOrWhiteSpace(p) && File.Exists(p))
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .FirstOrDefault();
                    }

                    if (string.IsNullOrWhiteSpace(primaryPath))
                        continue;

                    var existingPaths = new List<string> { primaryPath };
                    var probePath = primaryPath;

                    string ext = "unk?";
                    try
                    {
                        var fi = new FileInfo(probePath);
                        ext = fi.Extension.Length > 1 ? fi.Extension[1..] : "unk?";
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Could not identify extension for {path}", probePath);
                    }

                    var countsForDisplayedPerformance = ShouldCountForDisplayedPerformance(obj.Key, fileEntry.GamePaths, ext);

                    var isModelEntry = countsForDisplayedPerformance
                        && (string.Equals(ext, "mdl", StringComparison.OrdinalIgnoreCase)
                            || (fileEntry.GamePaths?.Any(p => p.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)) ?? false));

                    var tris = isModelEntry
                        ? await _xivDataAnalyzer.GetTrianglesByHash(fileEntry.Hash).ConfigureAwait(false)
                        : 0;

                    long vramBytes = 0;
                    try
                    {
                        if (countsForDisplayedPerformance && string.Equals(ext, "tex", StringComparison.OrdinalIgnoreCase))
                        {
                            foreach (var p in existingPaths)
                            {
                                if (VramEstimator.TryEstimateTexVramBytes(p, out var b))
                                    vramBytes = Math.Max(vramBytes, b);
                            }
                        }
                        else if (countsForDisplayedPerformance && string.Equals(ext, "mdl", StringComparison.OrdinalIgnoreCase))
                        {
                            foreach (var p in existingPaths)
                            {
                                if (VramEstimator.TryEstimateMdlVramBytes(p, out var b))
                                    vramBytes = Math.Max(vramBytes, b);
                            }
                        }
                    }
                    catch
                    {
                        vramBytes = 0;
                    }

                    long size = 0;
                    long compSize = 0;
                    foreach (var entry in fileCacheEntries)
                    {
                        if (entry.Size > 0) size = Math.Max(size, entry.Size.Value);
                        if (entry.CompressedSize > 0) compSize = Math.Max(compSize, entry.CompressedSize.Value);
                    }

                    data[fileEntry.Hash] = new FileDataEntry(
                        fileEntry.Hash,
                        ext,
                        [.. fileEntry.GamePaths],
                        existingPaths,
                        size,
                        compSize,
                        tris,
                        vramBytes);
                }

                LastAnalysis[obj.Key] = data;
            }

            var aggregateByHash = new Dictionary<string, (long Triangles, long VramBytes)>(StringComparer.OrdinalIgnoreCase);
            foreach (var kindData in LastAnalysis)
            {
                foreach (var entry in kindData.Value.Values)
                {
                    if (!ShouldCountForDisplayedPerformance(kindData.Key, entry))
                        continue;

                    if (!aggregateByHash.TryGetValue(entry.Hash, out var existing))
                    {
                        aggregateByHash[entry.Hash] = (entry.Triangles, entry.VramBytes);
                    }
                    else
                    {
                        aggregateByHash[entry.Hash] = (Math.Max(existing.Triangles, entry.Triangles), Math.Max(existing.VramBytes, entry.VramBytes));
                    }
                }
            }

            var aggregateTriangles = aggregateByHash.Values.Sum(v => v.Triangles);
            var aggregateVram = aggregateByHash.Values.Sum(v => v.VramBytes);
            _aggregateMetricsByDataHash[aggregateHash] = new AggregateMetricsEntry(aggregateVram, aggregateTriangles);
            _latestAnalyzedDataHash = aggregateHash;

            waiter?.TrySetResult(true);
            Mediator.Publish(new CharacterDataAnalyzedMessage());

            _lastDataHash = aggregateHash;
        }
        catch (OperationCanceledException)
        {
            waiter?.TrySetCanceled(token);
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to compute aggregate metrics for {hash}", aggregateHash);
            waiter?.TrySetResult(false);
        }
    }

    private TaskCompletionSource<bool> GetOrCreateAggregateMetricWaiter(string dataHash)
    {
        while (true)
        {
            if (_aggregateMetricWaiters.TryGetValue(dataHash, out var existing))
            {
                if (!existing.Task.IsCanceled && !existing.Task.IsFaulted)
                    return existing;

                var replacement = CreateAggregateMetricWaiter();
                if (_aggregateMetricWaiters.TryUpdate(dataHash, replacement, existing))
                    return replacement;

                continue;
            }

            var created = CreateAggregateMetricWaiter();
            if (_aggregateMetricWaiters.TryAdd(dataHash, created))
                return created;
        }
    }

    private static TaskCompletionSource<bool> CreateAggregateMetricWaiter()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private void PrintAnalysis()
    {
        if (LastAnalysis.Count == 0) return;
        foreach (var kvp in LastAnalysis)
        {
            int fileCounter = 1;
            int totalFiles = kvp.Value.Count;
            Logger.LogInformation("=== Analysis for {obj} ===", kvp.Key);

            foreach (var entry in kvp.Value.OrderBy(b => b.Value.GamePaths.OrderBy(p => p, StringComparer.Ordinal).First(), StringComparer.Ordinal))
            {
                Logger.LogInformation("File {x}/{y}: {hash}", fileCounter++, totalFiles, entry.Key);
                foreach (var path in entry.Value.GamePaths)
                {
                    Logger.LogInformation("  Game Path: {path}", path);
                }
                if (entry.Value.FilePaths.Count > 1) Logger.LogInformation("  Multiple fitting files detected for {key}", entry.Key);
                foreach (var filePath in entry.Value.FilePaths)
                {
                    Logger.LogInformation("  File Path: {path}", filePath);
                }
                Logger.LogInformation("  Size: {size}, Compressed: {compressed}", UiSharedService.ByteToString(entry.Value.OriginalSize),
                    UiSharedService.ByteToString(entry.Value.CompressedSize));
            }
        }
        foreach (var kvp in LastAnalysis)
        {
            Logger.LogInformation("=== Detailed summary by file type for {obj} ===", kvp.Key);
            foreach (var entry in kvp.Value.Select(v => v.Value).GroupBy(v => v.FileType, StringComparer.Ordinal))
            {
                Logger.LogInformation("{ext} files: {count}, size extracted: {size}, size compressed: {sizeComp}", entry.Key, entry.Count(),
                    UiSharedService.ByteToString(entry.Sum(v => v.OriginalSize)), UiSharedService.ByteToString(entry.Sum(v => v.CompressedSize)));
            }
            Logger.LogInformation("=== Total summary for {obj} ===", kvp.Key);
            Logger.LogInformation("Total files: {count}, size extracted: {size}, size compressed: {sizeComp}", kvp.Value.Count,
            UiSharedService.ByteToString(kvp.Value.Sum(v => v.Value.OriginalSize)), UiSharedService.ByteToString(kvp.Value.Sum(v => v.Value.CompressedSize)));
        }

        Logger.LogInformation("=== Total summary for all currently present objects ===");
        Logger.LogInformation("Total files: {count}, size extracted: {size}, size compressed: {sizeComp}",
            LastAnalysis.Values.Sum(v => v.Values.Count),
            UiSharedService.ByteToString(LastAnalysis.Values.Sum(c => c.Values.Sum(v => v.OriginalSize))),
            UiSharedService.ByteToString(LastAnalysis.Values.Sum(c => c.Values.Sum(v => v.CompressedSize))));
        Logger.LogInformation("IMPORTANT NOTES:\n\r- For RavaSync up- and downloads only the compressed size is relevant.\n\r- An unusually high total files count beyond 200 and up will also increase your download time to others significantly.");
    }


    private static bool ShouldCountForDisplayedPerformance(ObjectKind objectKind, FileDataEntry? entry)
    {
        if (entry == null)
            return false;

        return ShouldCountForDisplayedPerformance(objectKind, entry.GamePaths, entry.FileType);
    }

    private static bool ShouldCountForDisplayedPerformance(ObjectKind objectKind, IEnumerable<string>? gamePaths, string? fileType)
    {
        if (objectKind != ObjectKind.Player)
            return false;

        if (gamePaths == null)
            return false;

        var normalizedFileType = (fileType ?? string.Empty).TrimStart('.');

        if (normalizedFileType.Equals("mdl", StringComparison.OrdinalIgnoreCase))
            return gamePaths.Any(p => IsDisplayedPerformanceGamePath(p, ".mdl"));

        if (normalizedFileType.Equals("tex", StringComparison.OrdinalIgnoreCase))
            return gamePaths.Any(p => IsDisplayedPerformanceGamePath(p, ".tex"));

        return false;
    }

    private static bool IsDisplayedPerformanceGamePath(string? gamePath, string requiredExtension)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var path = gamePath.ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase);
        var extension = Path.GetExtension(path);

        if (!extension.Equals(requiredExtension, StringComparison.OrdinalIgnoreCase))
            return false;

        return path.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record AggregateMetricsEntry(long VramBytes, long Triangles);

    internal sealed record FileDataEntry(string Hash, string FileType, List<string> GamePaths, List<string> FilePaths, long OriginalSize, long CompressedSize, long Triangles, long VramBytes)
    {
        public bool IsComputed => OriginalSize > 0 && CompressedSize > 0;
        public async Task ComputeSizes(FileCacheManager fileCacheManager, CancellationToken token)
        {
            var compressedsize = await fileCacheManager.GetCompressedFileData(Hash, token).ConfigureAwait(false);
            var normalSize = new FileInfo(FilePaths[0]).Length;
            var entries = fileCacheManager.GetAllFileCachesByHash(Hash, ignoreCacheEntries: true, validate: false);
            foreach (var entry in entries)
            {
                entry.Size = normalSize;
                entry.CompressedSize = compressedsize.Item2.LongLength;
            }

            OriginalSize = normalSize;
            CompressedSize = compressedsize.Item2.LongLength;
            VramBytes = RecalculateVramBytes(FileType, FilePaths);
            Format = CreateFormatReader(FileType, FilePaths);
        }
        public long OriginalSize { get; private set; } = OriginalSize;
        public long CompressedSize { get; private set; } = CompressedSize;
        public long Triangles { get; private set; } = Triangles;
        public long VramBytes { get; private set; } = VramBytes;

        public Lazy<string> Format { get; private set; } = CreateFormatReader(FileType, FilePaths);

        private static Lazy<string> CreateFormatReader(string fileType, List<string> filePaths) => new(() =>
        {
            switch (fileType)
            {
                case "tex":
                    {
                        try
                        {
                            using var stream = new FileStream(filePaths[0], FileMode.Open, FileAccess.Read, FileShare.Read);
                            using var reader = new BinaryReader(stream);
                            reader.BaseStream.Position = 4;
                            var format = (TexFile.TextureFormat)reader.ReadInt32();
                            return format.ToString();
                        }
                        catch
                        {
                            return "Unknown";
                        }
                    }
                default:
                    return string.Empty;
            }
        });

        private static long RecalculateVramBytes(string fileType, List<string> filePaths)
        {
            try
            {
                if (string.Equals(fileType, "tex", StringComparison.OrdinalIgnoreCase))
                {
                    long best = 0;
                    foreach (var path in filePaths)
                    {
                        if (!string.IsNullOrWhiteSpace(path) && VramEstimator.TryEstimateTexVramBytes(path, out var bytes))
                            best = Math.Max(best, bytes);
                    }

                    return best;
                }

                if (string.Equals(fileType, "mdl", StringComparison.OrdinalIgnoreCase))
                {
                    long best = 0;
                    foreach (var path in filePaths)
                    {
                        if (!string.IsNullOrWhiteSpace(path) && VramEstimator.TryEstimateMdlVramBytes(path, out var bytes))
                            best = Math.Max(best, bytes);
                    }

                    return best;
                }
            }
            catch
            {
            }

            return 0;
        }
    }
}