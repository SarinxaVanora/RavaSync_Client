using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.FileCache;
using RavaSync.WebAPI.Files.Models;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;

namespace RavaSync.PlayerData.Services;

public sealed class ModPathResolver
{
    private sealed class PreparedReplacementPlan
    {
        public required ImmutableArray<FileReplacementData> NoSwap { get; init; }
        public required ImmutableArray<FileReplacementData> Swaps { get; init; }
    }

    private static readonly ConcurrentDictionary<string, PreparedReplacementPlan> ReplacementPlanCache = new(StringComparer.Ordinal);
    private static int ComputeResolveConcurrency()
    {
        var logical = Environment.ProcessorCount;
        if (logical <= 8) return 3;
        if (logical <= 16) return 4;
        return 6;
    }

    private static readonly int GlobalResolveConcurrency = ComputeResolveConcurrency();
    private static readonly SemaphoreSlim GlobalResolveSemaphore = new(GlobalResolveConcurrency, GlobalResolveConcurrency);

    private readonly FileCacheManager _fileDbManager;
    private readonly ILogger<ModPathResolver> _logger;

    public ModPathResolver(ILogger<ModPathResolver> logger, FileCacheManager fileDbManager)
    {
        _logger = logger;
        _fileDbManager = fileDbManager;
    }

    public List<FileReplacementData> Calculate(Guid applicationBase, CharacterData charaData, out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token)
    {
        GlobalResolveSemaphore.Wait(token);
        try
        {
            return CalculateCore(applicationBase, charaData, out moddedDictionary, token);
        }
        finally
        {
            GlobalResolveSemaphore.Release();
        }
    }

    private static PreparedReplacementPlan BuildReplacementPlan(CharacterData charaData)
    {
        var noSwap = ImmutableArray.CreateBuilder<FileReplacementData>(256);
        var swaps = ImmutableArray.CreateBuilder<FileReplacementData>(64);

        foreach (var kv in charaData.FileReplacements)
        {
            var list = kv.Value;
            for (int i = 0; i < list.Count; i++)
            {
                var v = list[i];
                if (string.IsNullOrWhiteSpace(v.FileSwapPath))
                    noSwap.Add(v);
                else
                    swaps.Add(v);
            }
        }

        return new PreparedReplacementPlan
        {
            NoSwap = noSwap.Count == noSwap.Capacity ? noSwap.MoveToImmutable() : noSwap.ToImmutable(),
            Swaps = swaps.Count == swaps.Capacity ? swaps.MoveToImmutable() : swaps.ToImmutable()
        };
    }

    private PreparedReplacementPlan GetOrCreateReplacementPlan(CharacterData charaData)
    {
        // DataHash can remain stable across some Penumbra option/file-swap changes.
        // Cache the resolved replacement plan by the actual payload instead, otherwise
        // texture/pattern dropdown swaps can reuse an older plan and appear to do nothing
        // until another unrelated change invalidates state.
        var cacheKey = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(charaData);
        return ReplacementPlanCache.GetOrAdd(cacheKey, _ => BuildReplacementPlan(charaData));
    }

    private List<FileReplacementData> CalculateCore(Guid applicationBase, CharacterData charaData, out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token)
    {
        var st = Stopwatch.StartNew();

        var missingFiles = new List<FileReplacementData>();
        var outputDict = new Dictionary<(string GamePath, string? Hash), string>();
        var fileCacheMemo = new ConcurrentDictionary<string, FileCacheEntity?>(StringComparer.OrdinalIgnoreCase);

        int migrationChanges = 0;
        object migrationLock = new();
        void MarkMigrationChanged() => Interlocked.Exchange(ref migrationChanges, 1);

        moddedDictionary = new Dictionary<(string GamePath, string? Hash), string>();
        try
        {
            var plan = GetOrCreateReplacementPlan(charaData);
            var noSwap = plan.NoSwap;
            var swaps = plan.Swaps;

            for (int i = 0; i < noSwap.Length; i++)
            {
                token.ThrowIfCancellationRequested();
                ProcessNoSwapReplacement(applicationBase, noSwap[i], missingFiles, outputDict, fileCacheMemo, migrationLock, MarkMigrationChanged, token);
            }

            for (int i = 0; i < swaps.Length; i++)
            {
                var item = swaps[i];
                var gamePaths = item.GamePaths;
                if (gamePaths == null) continue;

                var swap = item.FileSwapPath!;
                if (Path.IsPathRooted(swap) || swap.Contains(":\\", StringComparison.Ordinal) || swap.StartsWith("\\", StringComparison.Ordinal))
                {
                    _logger.LogWarning("[BASE-{appBase}] Ignoring invalid FileSwapPath that looks like a filesystem path: {swap}", applicationBase, swap);
                    continue;
                }

                foreach (var gp in gamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gp)) continue;
                    if (string.Equals(gp, swap, StringComparison.OrdinalIgnoreCase)) continue;
                    outputDict[(gp, null)] = swap;
                }
            }

            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        catch (OperationCanceledException)
        {
            _logger.LogTrace("[BASE-{appBase}] Replacement calculation cancelled", applicationBase);
            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BASE-{appBase}] Something went wrong during calculation replacements", applicationBase);
            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        finally
        {
            if (migrationChanges == 1)
                _fileDbManager.WriteOutFullCsv();

            st.Stop();
            _logger.LogDebug("[BASE-{appBase}] ModdedPaths calculated in {time}ms, missing files: {count}, total files: {total}",
                applicationBase, st.ElapsedMilliseconds, missingFiles.Count, moddedDictionary.Count);
        }

        return missingFiles.ToList();
    }

    private void ProcessNoSwapReplacement(Guid applicationBase, FileReplacementData item, List<FileReplacementData> missingFiles, Dictionary<(string GamePath, string? Hash), string> outputDict, ConcurrentDictionary<string, FileCacheEntity?> fileCacheMemo, object migrationLock, Action markMigrationChanged, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        var hash = item.Hash;
        if (string.IsNullOrWhiteSpace(hash))
            return;

        FileCacheEntity? GetCachedFileEntry(string h)
            => fileCacheMemo.GetOrAdd(h, static (key, self) => self._fileDbManager.GetFileCacheByHash(key), this);

        var gamePathsObj = item.GamePaths;
        if (gamePathsObj == null)
            return;

        object gamePathsStable = gamePathsObj is IList<string> ? gamePathsObj : (object)gamePathsObj.ToList();

        static bool TryGetFirstNonEmptyGamePath(object gamePathsObj, out string first)
        {
            first = string.Empty;
            if (gamePathsObj is IEnumerable<string> enumerable)
            {
                foreach (var s in enumerable)
                {
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        first = s;
                        return true;
                    }
                }
            }
            return false;
        }

        static string GetExtensionOrDat(string gamePath)
        {
            var e = Path.GetExtension(gamePath);
            if (string.IsNullOrWhiteSpace(e) || e.Length <= 1) return "dat";
            return e.AsSpan(1).ToString();
        }

        var fileCache = GetCachedFileEntry(hash);
        if (fileCache == null || string.IsNullOrWhiteSpace(fileCache.ResolvedFilepath))
        {
            missingFiles.Add(item);
            return;
        }

        var resolved = fileCache.ResolvedFilepath;
        FileInfo fi;
        try
        {
            fi = new FileInfo(resolved);
            if (!fi.Exists || (fi.Length == 0 && (DateTime.UtcNow - fi.LastWriteTimeUtc) >= TimeSpan.FromSeconds(10)))
            {
                missingFiles.Add(item);
                return;
            }
        }
        catch
        {
            fi = new FileInfo(resolved);
        }

        if (string.IsNullOrEmpty(fi.Extension) && TryGetFirstNonEmptyGamePath(gamePathsStable, out var firstGp))
        {
            var targetExt = GetExtensionOrDat(firstGp);
            lock (migrationLock)
            {
                var cacheAgain = GetCachedFileEntry(hash);
                if (cacheAgain != null)
                {
                    var fi2 = new FileInfo(cacheAgain.ResolvedFilepath);
                    if (string.IsNullOrEmpty(fi2.Extension))
                    {
                        markMigrationChanged();
                        cacheAgain = _fileDbManager.MigrateFileHashToExtension(cacheAgain, targetExt);
                        fileCacheMemo[hash] = cacheAgain;
                        resolved = cacheAgain.ResolvedFilepath;
                    }
                    else
                    {
                        resolved = cacheAgain.ResolvedFilepath;
                    }
                }
            }
        }

        foreach (var gp in (IEnumerable<string>)gamePathsStable)
        {
            if (string.IsNullOrWhiteSpace(gp)) continue;
            outputDict[(gp, hash)] = resolved;
        }
    }
}
