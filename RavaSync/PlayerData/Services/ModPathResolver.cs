using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.Services.Optimisation;
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
    private readonly MareConfigService _configService;
    private readonly ScreenShakeSanitisationService _screenShakeSanitisationService;

    public ModPathResolver(ILogger<ModPathResolver> logger, FileCacheManager fileDbManager, MareConfigService configService, ScreenShakeSanitisationService screenShakeSanitisationService)
    {
        _logger = logger;
        _fileDbManager = fileDbManager;
        _configService = configService;
        _screenShakeSanitisationService = screenShakeSanitisationService;
    }

    public List<FileReplacementData> Calculate(Guid applicationBase, CharacterData charaData, out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token, bool? allowScreenShake = null)
    {
        GlobalResolveSemaphore.Wait(token);
        try
        {
            return CalculateCore(applicationBase, charaData, out moddedDictionary, token, allowScreenShake ?? _configService.Current.GlobalSyncScreenShake);
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

    private List<FileReplacementData> CalculateCore(Guid applicationBase, CharacterData charaData, out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token, bool allowScreenShake)
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
                ProcessNoSwapReplacement(applicationBase, noSwap[i], missingFiles, outputDict, fileCacheMemo, migrationLock, MarkMigrationChanged, token, allowScreenShake);
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
                    if (!ShouldApplyGlobalSyncGamePath(gp, swap)) continue;
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

    private void ProcessNoSwapReplacement(Guid applicationBase, FileReplacementData item, List<FileReplacementData> missingFiles, Dictionary<(string GamePath, string? Hash), string> outputDict, ConcurrentDictionary<string, FileCacheEntity?> fileCacheMemo, object migrationLock, Action markMigrationChanged, CancellationToken token, bool allowScreenShake)
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

        var gamePathsStable = gamePathsObj is IList<string> list ? list.Where(static p => !string.IsNullOrWhiteSpace(p)).ToList() : gamePathsObj.Where(static p => !string.IsNullOrWhiteSpace(p)).ToList();
        if (gamePathsStable.Count == 0)
            return;

        var effectiveGamePaths = gamePathsStable
            .Where(path => ShouldApplyGlobalSyncGamePath(path, null))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (effectiveGamePaths.Length == 0)
            return;

        static bool TryGetFirstNonEmptyGamePath(IEnumerable<string> gamePaths, out string first)
        {
            first = string.Empty;
            foreach (var s in gamePaths)
            {
                if (!string.IsNullOrWhiteSpace(s))
                {
                    first = s;
                    return true;
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
            missingFiles.Add(CloneReplacementWithGamePaths(item, effectiveGamePaths));
            return;
        }

        var resolved = fileCache.ResolvedFilepath;
        FileInfo fi;
        try
        {
            fi = new FileInfo(resolved);
            if (!fi.Exists || (fi.Length == 0 && (DateTime.UtcNow - fi.LastWriteTimeUtc) >= TimeSpan.FromSeconds(10)))
            {
                missingFiles.Add(CloneReplacementWithGamePaths(item, effectiveGamePaths));
                return;
            }
        }
        catch
        {
            fi = new FileInfo(resolved);
        }

        if (string.IsNullOrEmpty(fi.Extension) && TryGetFirstNonEmptyGamePath(effectiveGamePaths, out var firstGp))
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

        foreach (var gp in effectiveGamePaths)
        {
            if (string.IsNullOrWhiteSpace(gp))
                continue;

            var effectiveResolved = resolved;
            if (!allowScreenShake
                && ScreenShakeSanitisationService.IsAvfxCandidate(gp, resolved)
                && _screenShakeSanitisationService.TryGetScreenShakeSafePath(gp, resolved, hash, out var safePath)
                && !string.IsNullOrWhiteSpace(safePath))
            {
                effectiveResolved = safePath;
            }

            outputDict[(gp, hash)] = effectiveResolved;
        }
    }

    private static FileReplacementData CloneReplacementWithGamePaths(FileReplacementData source, string[] gamePaths)
    {
        return new FileReplacementData
        {
            GamePaths = gamePaths,
            Hash = source.Hash,
            FileSwapPath = source.FileSwapPath,
        };
    }

    private bool ShouldApplyGlobalSyncGamePath(string? gamePath, string? fileSwapPath)
    {
        var pathForKind = string.IsNullOrWhiteSpace(fileSwapPath) ? gamePath : fileSwapPath;
        var normalized = NormalizeGamePath(pathForKind);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        var ext = Path.GetExtension(normalized).ToLowerInvariant();
        if (!_configService.Current.GlobalSyncSounds && ext == ".scd")
            return false;

        if (!_configService.Current.GlobalSyncAnimations && (ext == ".pap" || ext == ".tmb" || ext == ".tmb2"))
            return false;

        if (!_configService.Current.GlobalSyncVfx && IsVfxGamePathExtension(ext))
            return false;

        return true;
    }

    private static bool IsVfxGamePathExtension(string extension)
        => extension is ".avfx" or ".atex" or ".shpk" or ".eid" or ".skp";

    private static string NormalizeGamePath(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Replace('\\', '/').Trim();

}
