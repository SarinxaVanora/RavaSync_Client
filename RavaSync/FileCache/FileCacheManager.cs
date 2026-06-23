using K4os.Compression.LZ4.Legacy;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading;

namespace RavaSync.FileCache;

public sealed partial class FileCacheManager : IHostedService
{
    public const string CachePrefix = "{cache}";
    public const string CsvSplit = "|";
    public const string PenumbraPrefix = "{penumbra}";

    private readonly MareConfigService _configService;
    private readonly MareMediator _mareMediator;
    private readonly string _csvPath;

    private readonly ConcurrentDictionary<string, List<FileCacheEntity>> _fileCaches = new(StringComparer.Ordinal);
    private readonly object _fileCachesLock = new();

    private readonly ConcurrentDictionary<string, FileCacheEntity> _prefixedPathIndex = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, object> _getCachesByPathLocks = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _getCachesByPathsSemaphore = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _missingHashProbeUntilUtcTicks = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan _missingHashProbeTtl = TimeSpan.FromSeconds(5);
    private readonly ConcurrentDictionary<string, (FileCacheEntity? Entity, string LastModifiedTicks, long Size)> _validatedFileCacheMemo = new(StringComparer.OrdinalIgnoreCase);
    private int _csvRewriteNeeded;

    private readonly object _fileWriteLock = new();
    private readonly object _startupLoadGate = new();
    private Task? _startupLoadTask;
    private readonly IpcManager _ipcManager;
    private readonly ILogger<FileCacheManager> _logger;

    public string CacheFolder => _configService.Current.CacheFolder;

    public FileCacheManager(
        ILogger<FileCacheManager> logger,
        IpcManager ipcManager,
        MareConfigService configService,
        MareMediator mareMediator)
    {
        _logger = logger;
        _ipcManager = ipcManager;
        _configService = configService;
        _mareMediator = mareMediator;
        _csvPath = Path.Combine(configService.ConfigurationDirectory, "FileCache.csv");
    }

    private string CsvBakPath => _csvPath + ".bak";

    private static string NormalizePathForIndex(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        return path
            .Replace('/', '\\')
            .Replace("\\\\", "\\", StringComparison.Ordinal);
    }

    public FileCacheEntity? CreateCacheEntry(string path)
    {
        FileInfo fi = new(path);
        if (!fi.Exists) return null;

        _logger.LogTrace("Creating cache entry for {path}", path);

        var fullName = fi.FullName.ToLowerInvariant();
        var cacheFolder = _configService.Current.CacheFolder.ToLowerInvariant();

        if (!fullName.Contains(cacheFolder, StringComparison.Ordinal))
            return null;

        string prefixedPath = fullName
            .Replace(cacheFolder, CachePrefix + "\\", StringComparison.Ordinal)
            .Replace("\\\\", "\\", StringComparison.Ordinal);

        var nameHash = Path.GetFileNameWithoutExtension(fi.Name);

        if (LooksLikeSha1(nameHash))
            return CreateFileCacheEntity(fi, prefixedPath, nameHash);

        return CreateFileCacheEntity(fi, prefixedPath);
    }


    public void RegisterDownloadedCacheFiles(IEnumerable<(string Hash, string FilePath)> files)
    {
        if (files == null) return;

        var csvLinesByPath = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var needsFullCsvRewrite = false;

        foreach (var item in files)
        {
            if (string.IsNullOrWhiteSpace(item.Hash) || string.IsNullOrWhiteSpace(item.FilePath))
                continue;

            FileInfo fi;
            try
            {
                fi = new FileInfo(item.FilePath);
                if (!fi.Exists)
                    continue;
            }
            catch
            {
                continue;
            }

            var entry = CreateKnownHashCacheEntity(fi, item.Hash);
            if (entry == null)
                continue;

            var alreadyKnown = false;
            lock (_fileCachesLock)
            {
                alreadyKnown = _fileCaches.TryGetValue(entry.Hash, out var existing)
                    && existing.Any(e => string.Equals(e.PrefixedFilePath, entry.PrefixedFilePath, StringComparison.OrdinalIgnoreCase));
            }

            var addNeedsRewrite = AddHashedFile(entry);
            needsFullCsvRewrite |= addNeedsRewrite;
            _missingHashProbeUntilUtcTicks.TryRemove(entry.Hash, out _);

            if (!alreadyKnown && !addNeedsRewrite)
                csvLinesByPath[GetCsvStorageKey(entry)] = entry.CsvEntry;
        }

        if (needsFullCsvRewrite || ConsumeCsvRewriteNeeded())
        {
            WriteOutFullCsv();
            return;
        }

        if (csvLinesByPath.Count == 0)
            return;

        try
        {
            lock (_fileWriteLock)
            {
                File.AppendAllLines(_csvPath, csvLinesByPath.Values);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to append batch cache entries; rewriting full cache CSV");
            WriteOutFullCsv();
        }
    }

    private FileCacheEntity? CreateKnownHashCacheEntity(FileInfo fileInfo, string hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return null;

        var fullName = fileInfo.FullName.ToLowerInvariant();
        var cacheFolder = _configService.Current.CacheFolder.ToLowerInvariant();

        if (!fullName.Contains(cacheFolder, StringComparison.Ordinal))
            return null;

        string prefixedPath = fullName
            .Replace(cacheFolder, CachePrefix + "\\", StringComparison.Ordinal)
            .Replace("\\\\", "\\", StringComparison.Ordinal);

        long length = 0;
        long lastWriteTicks = 0;

        try { length = fileInfo.Length; }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }

        try { lastWriteTicks = fileInfo.LastWriteTimeUtc.Ticks; }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }

        var entity = new FileCacheEntity(
            hash,
            prefixedPath,
            lastWriteTicks.ToString(CultureInfo.InvariantCulture),
            length);

        return ReplacePathPrefixes(entity);
    }

    private static bool LooksLikeSha1(string? s)
    {
        if (string.IsNullOrWhiteSpace(s) || s.Length != 40) return false;

        foreach (var c in s)
        {
            bool isHex =
                (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');

            if (!isHex) return false;
        }

        return true;
    }

    public FileCacheEntity? CreateFileEntry(string path)
    {
        FileInfo fi = new(path);
        if (!fi.Exists) return null;

        _logger.LogTrace("Creating file entry for {path}", path);

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrEmpty(modDirectory))
            return null;

        var fullName = fi.FullName;
        var normalizedModDirectory = Path.GetFullPath(modDirectory).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var modDirIndex = fullName.IndexOf(normalizedModDirectory, StringComparison.OrdinalIgnoreCase);
        if (modDirIndex < 0)
            return null;

        var suffixStart = modDirIndex + normalizedModDirectory.Length;
        var suffix = suffixStart < fullName.Length ? fullName[suffixStart..].TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) : string.Empty;
        var prefixedPath = string.IsNullOrEmpty(suffix)
            ? PenumbraPrefix
            : PenumbraPrefix + "\\" + suffix;
        prefixedPath = prefixedPath.Replace("\\\\", "\\", StringComparison.Ordinal);

        return CreateFileCacheEntity(fi, prefixedPath);
    }

    public List<FileCacheEntity> GetAllFileCaches()
    {
        lock (_fileCachesLock)
        {
            return _fileCaches.Values.SelectMany(v => v).ToList();
        }
    }

    public List<FileCacheEntity> GetAllFileCachesByHash(string hash, bool ignoreCacheEntries = false, bool validate = true)
    {
        List<FileCacheEntity> candidates;

        lock (_fileCachesLock)
        {
            if (!_fileCaches.TryGetValue(hash, out var fileCacheEntities))
                return [];

            candidates = fileCacheEntities
                .Where(c => !ignoreCacheEntries || !c.IsCacheEntry)
                .ToList();
        }

        if (!validate)
            return candidates;

        List<FileCacheEntity> output = [];
        foreach (var fileCache in candidates)
        {
            var validated = GetValidatedFileCache(fileCache);
            if (validated != null)
                output.Add(validated);
        }

        return output;
    }

    public Task<List<FileCacheEntity>> ValidateLocalIntegrity(IProgress<(int, int, FileCacheEntity)> progress, CancellationToken cancellationToken)
    {
        _mareMediator.Publish(new HaltScanMessage(nameof(ValidateLocalIntegrity)));

        try
        {
            _logger.LogInformation("Validating local storage");

            List<FileCacheEntity> cacheEntries;
            lock (_fileCachesLock)
            {
                cacheEntries = _fileCaches
                    .SelectMany(v => v.Value)
                    .Where(v => v.IsCacheEntry)
                    .ToList();
            }

            List<FileCacheEntity> brokenEntities = [];
            int i = 0;

            foreach (var fileCache in cacheEntries)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                _logger.LogInformation("Validating {file}", fileCache.ResolvedFilepath);

                progress.Report((i, cacheEntries.Count, fileCache));
                i++;

                if (!File.Exists(fileCache.ResolvedFilepath))
                {
                    brokenEntities.Add(fileCache);
                    continue;
                }

                long length = -1;
                try
                {
                    length = new FileInfo(fileCache.ResolvedFilepath).Length;
                }
                catch
                {
                    // if we can't stat it, let hashing path decide
                }

                if (length >= 0 && length < 16)
                {
                    _logger.LogInformation("File too small to be valid ({len} bytes): {file}", length, fileCache.ResolvedFilepath);
                    brokenEntities.Add(fileCache);
                    continue;
                }

                try
                {
                    var computedHash = Crypto.GetFileHash(fileCache.ResolvedFilepath);
                    if (!string.Equals(computedHash, fileCache.Hash, StringComparison.Ordinal))
                    {
                        _logger.LogInformation(
                            "Failed to validate {file}, got hash {hash}, expected {expected}",
                            fileCache.ResolvedFilepath, computedHash, fileCache.Hash);

                        brokenEntities.Add(fileCache);
                    }
                }
                catch (IOException ioEx)
                {
                    if (length == 0)
                    {
                        _logger.LogWarning(ioEx, "IO error during validation of {file} but length is 0; marking as broken", fileCache.ResolvedFilepath);
                        brokenEntities.Add(fileCache);
                    }
                    else
                    {
                        _logger.LogWarning(
                            ioEx,
                            "IO error during validation of {file}; treating as temporarily unavailable and keeping cache entry",
                            fileCache.ResolvedFilepath);
                    }

                    continue;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Unexpected error during validation of {file}, marking as broken",
                        fileCache.ResolvedFilepath);

                    brokenEntities.Add(fileCache);
                }
            }

            foreach (var brokenEntity in brokenEntities)
            {
                RemoveHashedFile(brokenEntity.Hash, brokenEntity.PrefixedFilePath);

                try
                {
                    File.Delete(brokenEntity.ResolvedFilepath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not delete {file}", brokenEntity.ResolvedFilepath);
                }
            }

            return Task.FromResult(brokenEntities);
        }
        finally
        {
            _mareMediator.Publish(new ResumeScanMessage(nameof(ValidateLocalIntegrity)));
        }
    }

    public string GetCacheFilePath(string hash, string extension)
    {
        return Path.Combine(_configService.Current.CacheFolder, hash + "." + extension);
    }

    public async Task<(string, byte[])> GetCompressedFileData(string fileHash, CancellationToken uploadToken)
    {
        var fileCache = GetFileCacheByHash(fileHash);
        if (fileCache == null)
            throw new FileNotFoundException($"Could not resolve file cache entry for hash {fileHash}");

        var filePath = fileCache.ResolvedFilepath;
        return (
            fileHash,
            LZ4Wrapper.WrapHC(
                await File.ReadAllBytesAsync(filePath, uploadToken).ConfigureAwait(false),
                0,
                (int)new FileInfo(filePath).Length));
    }

    public FileCacheEntity? GetFileCacheByHash(string hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return null;

        var lookupHash = LooksLikeSha1(hash) ? hash.Trim().ToUpperInvariant() : hash.Trim();
        List<FileCacheEntity>? candidates = null;

        lock (_fileCachesLock)
        {
            if (_fileCaches.TryGetValue(lookupHash, out var hashes))
            {
                candidates = hashes.ToList();
            }
            else if (!string.Equals(lookupHash, hash, StringComparison.Ordinal) && _fileCaches.TryGetValue(hash, out hashes))
            {
                candidates = hashes.ToList();
            }
        }

        if (candidates != null)
        {
            for (int pass = 0; pass < 2; pass++)
            {
                bool preferPenumbra = pass == 0;

                foreach (var candidate in candidates)
                {
                    bool isPenumbra = candidate.PrefixedFilePath.Contains(PenumbraPrefix, StringComparison.OrdinalIgnoreCase);
                    if (isPenumbra != preferPenumbra)
                        continue;

                    var validated = GetValidatedFileCache(candidate);
                    if (validated != null)
                        return validated;
                }
            }
        }

        var nowTicks = DateTime.UtcNow.Ticks;
        if (_missingHashProbeUntilUtcTicks.TryGetValue(lookupHash, out var untilTicks) && untilTicks > nowTicks)
            return null;

        var probed = TryRecoverCacheEntryByHashFileName(lookupHash);
        if (probed != null)
            return probed;

        _missingHashProbeUntilUtcTicks[lookupHash] = DateTime.UtcNow.Add(_missingHashProbeTtl).Ticks;
        return null;
    }

    private FileCacheEntity? TryRecoverCacheEntryByHashFileName(string hash)
    {
        if (!LooksLikeSha1(hash))
            return null;

        string cacheFolder;
        try
        {
            cacheFolder = _configService.Current.CacheFolder;
            if (string.IsNullOrWhiteSpace(cacheFolder) || !Directory.Exists(cacheFolder))
                return null;
        }
        catch
        {
            return null;
        }

        try
        {
            foreach (var path in Directory.EnumerateFiles(cacheFolder, hash + ".*", SearchOption.TopDirectoryOnly))
            {
                FileInfo fi;
                try
                {
                    fi = new FileInfo(path);
                    if (!fi.Exists || fi.Length <= 0)
                        continue;
                }
                catch
                {
                    continue;
                }

                var nameHash = Path.GetFileNameWithoutExtension(fi.Name);
                if (!string.Equals(nameHash, hash, StringComparison.OrdinalIgnoreCase))
                    continue;

                RegisterDownloadedCacheFiles([(hash, fi.FullName)]);

                List<FileCacheEntity>? recoveredCandidates = null;
                lock (_fileCachesLock)
                {
                    if (_fileCaches.TryGetValue(hash, out var hashes))
                        recoveredCandidates = hashes.ToList();
                }

                if (recoveredCandidates == null)
                    continue;

                foreach (var candidate in recoveredCandidates)
                {
                    if (!string.Equals(candidate.ResolvedFilepath, fi.FullName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var validated = GetValidatedFileCache(candidate);
                    if (validated != null)
                    {
                        _logger.LogDebug("Recovered missing cache index entry for hash {hash} from existing cache file {path}", hash, fi.FullName);
                        return validated;
                    }
                }
            }
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "Could not probe cache folder for hash {hash}", hash);
        }
        catch (UnauthorizedAccessException ex)
        {
            _logger.LogDebug(ex, "Could not probe cache folder for hash {hash}", hash);
        }

        return null;
    }

    private FileCacheEntity? GetFileCacheByPath(string path)
    {
        var prefixedPath = NormalizePathForIndex(path);

        var modDir = NormalizePathForIndex(_ipcManager.Penumbra.ModDirectory);
        if (!string.IsNullOrEmpty(modDir))
        {
            prefixedPath = prefixedPath.Replace(modDir, PenumbraPrefix + "\\", StringComparison.OrdinalIgnoreCase);
        }

        var cacheDir = NormalizePathForIndex(_configService.Current.CacheFolder);
        if (!string.IsNullOrEmpty(cacheDir))
        {
            prefixedPath = prefixedPath.Replace(cacheDir, CachePrefix + "\\", StringComparison.OrdinalIgnoreCase);
        }

        prefixedPath = NormalizePathForIndex(prefixedPath);

        if (_prefixedPathIndex.TryGetValue(prefixedPath, out var entry))
        {
            return GetValidatedFileCache(entry);
        }

        _logger.LogDebug("Found no entries for {path}", prefixedPath);

        return prefixedPath.StartsWith(CachePrefix, StringComparison.OrdinalIgnoreCase)
            ? CreateCacheEntry(path)
            : CreateFileEntry(path);
    }

    public Dictionary<string, FileCacheEntity?> GetFileCachesByPaths(string[] paths)
    {
        _getCachesByPathsSemaphore.Wait();

        try
        {
            var modDir = NormalizePathForIndex(_ipcManager.Penumbra.ModDirectory);
            var cacheDir = NormalizePathForIndex(_configService.Current.CacheFolder);

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, FileCacheEntity?>(StringComparer.OrdinalIgnoreCase);
            var processed = 0;

            foreach (var original in paths)
            {
                if (string.IsNullOrWhiteSpace(original)) continue;
                if (!seen.Add(original)) continue;

                if (++processed % 64 == 0)
                    Thread.Yield();

                var prefixed = NormalizePathForIndex(original);

                if (!string.IsNullOrEmpty(modDir))
                    prefixed = prefixed.Replace(modDir, PenumbraPrefix + "\\", StringComparison.OrdinalIgnoreCase);

                if (!string.IsNullOrEmpty(cacheDir))
                    prefixed = prefixed.Replace(cacheDir, CachePrefix + "\\", StringComparison.OrdinalIgnoreCase);

                prefixed = NormalizePathForIndex(prefixed);

                if (_prefixedPathIndex.TryGetValue(prefixed, out var entity))
                {
                    result[original] = GetValidatedFileCache(entity);
                }
                else
                {
                    result[original] = prefixed.Contains(CachePrefix, StringComparison.OrdinalIgnoreCase)
                        ? CreateCacheEntry(original)
                        : CreateFileEntry(original);
                }
            }

            return result;
        }
        finally
        {
            _getCachesByPathsSemaphore.Release();
        }
    }

    public Dictionary<string, string> GetFileHashesByPaths(string[] paths)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var hashesByPrefixedPath = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var processed = 0;

        foreach (var originalPath in paths)
        {
            if (string.IsNullOrWhiteSpace(originalPath))
            {
                result[originalPath] = string.Empty;
                continue;
            }

            if (++processed % 64 == 0)
                Thread.Yield();

            var prefixedPath = ToPrefixedPathForIndex(originalPath);
            if (string.IsNullOrWhiteSpace(prefixedPath))
            {
                result[originalPath] = string.Empty;
                continue;
            }

            if (hashesByPrefixedPath.TryGetValue(prefixedPath, out var existingHash))
            {
                result[originalPath] = existingHash;
                continue;
            }

            if (_prefixedPathIndex.TryGetValue(prefixedPath, out var indexedEntity))
            {
                var indexedHash = GetValidatedFileCache(indexedEntity)?.Hash ?? string.Empty;
                hashesByPrefixedPath[prefixedPath] = indexedHash;
                result[originalPath] = indexedHash;
                continue;
            }

            var pathLock = _getCachesByPathLocks.GetOrAdd(prefixedPath, static _ => new object());
            lock (pathLock)
            {
                if (_prefixedPathIndex.TryGetValue(prefixedPath, out indexedEntity))
                {
                    var lockedIndexedHash = GetValidatedFileCache(indexedEntity)?.Hash ?? string.Empty;
                    hashesByPrefixedPath[prefixedPath] = lockedIndexedHash;
                    result[originalPath] = lockedIndexedHash;
                    continue;
                }

                FileCacheEntity? created;
                if (prefixedPath.Contains(CachePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    created = CreateCacheEntry(originalPath);
                }
                else if (prefixedPath.Contains(PenumbraPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    created = CreateFileEntry(originalPath);
                }
                else
                {
                    created = CreateFileEntry(originalPath) ?? CreateCacheEntry(originalPath);
                }

                var createdHash = created?.Hash ?? string.Empty;
                hashesByPrefixedPath[prefixedPath] = createdHash;
                result[originalPath] = createdHash;
            }
        }

        return result;
    }

    public Task<Dictionary<string, FileCacheEntity?>> RefreshFileCachesByPathsAsync(IEnumerable<string>? paths, CancellationToken ct, bool writeCsv = false)
    {
        var pathArray = paths?
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray() ?? [];

        if (pathArray.Length == 0)
            return Task.FromResult(new Dictionary<string, FileCacheEntity?>(StringComparer.OrdinalIgnoreCase));

        return Task.Factory.StartNew(() =>
        {
            try
            {
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
            }
            catch
            {
                // Best effort only; not every runtime allows changing thread priority.
            }

            ct.ThrowIfCancellationRequested();
            var refreshed = RefreshFileCachesByPaths(pathArray, ct);

            if (writeCsv)
                WriteOutFullCsv();

            return refreshed;
        }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public Dictionary<string, FileCacheEntity?> RefreshFileCachesByPaths(IEnumerable<string>? paths, CancellationToken ct = default)
    {
        var pathArray = paths?
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray() ?? [];

        foreach (var path in pathArray)
        {
            ct.ThrowIfCancellationRequested();
            InvalidateValidatedFileCacheMemo(ToPrefixedPathForIndex(path));
        }

        return GetFileCachesByPaths(pathArray);
    }

    private string ToPrefixedPathForIndex(string path)
    {
        var prefixedPath = NormalizePathForIndex(path);

        var modDir = NormalizePathForIndex(_ipcManager.Penumbra.ModDirectory);
        if (!string.IsNullOrEmpty(modDir))
            prefixedPath = prefixedPath.Replace(modDir, PenumbraPrefix + "\\", StringComparison.OrdinalIgnoreCase);

        var cacheDir = NormalizePathForIndex(_configService.Current.CacheFolder);
        if (!string.IsNullOrEmpty(cacheDir))
            prefixedPath = prefixedPath.Replace(cacheDir, CachePrefix + "\\", StringComparison.OrdinalIgnoreCase);

        return NormalizePathForIndex(prefixedPath);
    }

    private void MarkCsvRewriteNeeded()
        => Interlocked.Exchange(ref _csvRewriteNeeded, 1);

    internal bool ConsumeCsvRewriteNeeded()
        => Interlocked.Exchange(ref _csvRewriteNeeded, 0) == 1;

    public void RemoveHashedFile(string hash, string prefixedFilePath)
    {
        InvalidateValidatedFileCacheMemo(prefixedFilePath);
        lock (_fileCachesLock)
        {
            if (_fileCaches.TryGetValue(hash, out var caches))
            {
                var removedCount = caches.RemoveAll(c =>
                    string.Equals(c.PrefixedFilePath, prefixedFilePath, StringComparison.OrdinalIgnoreCase));

                _logger.LogTrace(
                    "Removed from DB: {count} file(s) with hash {hash} and file cache {path}",
                    removedCount, hash, prefixedFilePath);

                if (removedCount > 0)
                    MarkCsvRewriteNeeded();

                if (caches.Count == 0)
                {
                    _fileCaches.TryRemove(hash, out _);
                }

                if (_prefixedPathIndex.TryGetValue(prefixedFilePath, out var indexed) &&
                    string.Equals(indexed.Hash, hash, StringComparison.OrdinalIgnoreCase))
                {
                    RebuildPrefixedPathIndexForPath(prefixedFilePath);
                }
            }
        }
    }

    public void UpdateHashedFile(FileCacheEntity fileCache, bool computeProperties = true)
    {
        InvalidateValidatedFileCacheMemo(fileCache.PrefixedFilePath);
        _logger.LogTrace("Updating hash for {path}", fileCache.ResolvedFilepath);

        var oldHash = fileCache.Hash;
        var prefixedPath = fileCache.PrefixedFilePath;

        if (computeProperties)
        {
            try
            {
                var fi = new FileInfo(fileCache.ResolvedFilepath);
                fileCache.Size = fi.Length;
                fileCache.CompressedSize = null;
                fileCache.Hash = Crypto.GetFileHash(fileCache.ResolvedFilepath);
                fileCache.LastModifiedDateTicks = fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture);
            }
            catch (IOException ioEx)
            {
                _logger.LogDebug(
                    ioEx,
                    "IO error while updating hash for {file}; keeping existing cache entry unchanged",
                    fileCache.ResolvedFilepath);

                return;
            }
        }

        RemoveHashedFile(oldHash, prefixedPath);
        AddHashedFile(fileCache);
    }

    private void UpdatePrefixedPathIndex(FileCacheEntity entity)
    {
        if (entity == null || string.IsNullOrEmpty(entity.PrefixedFilePath))
            return;

        _prefixedPathIndex.AddOrUpdate(entity.PrefixedFilePath, entity, (_, existing) =>
        {
            if (existing == null) return entity;

            if (long.TryParse(existing.LastModifiedDateTicks, NumberStyles.Integer, CultureInfo.InvariantCulture, out var exTicks) &&
                long.TryParse(entity.LastModifiedDateTicks, NumberStyles.Integer, CultureInfo.InvariantCulture, out var newTicks))
            {
                return newTicks >= exTicks ? entity : existing;
            }

            return entity;
        });
    }

    private static long GetCacheEntrySortTicks(FileCacheEntity? entity)
    {
        if (entity == null) return long.MinValue;
        return long.TryParse(entity.LastModifiedDateTicks, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks)
            ? ticks
            : long.MinValue;
    }

    private static bool IsBetterPathEntry(FileCacheEntity candidate, FileCacheEntity current)
    {
        var candidateTicks = GetCacheEntrySortTicks(candidate);
        var currentTicks = GetCacheEntrySortTicks(current);
        if (candidateTicks != currentTicks)
            return candidateTicks > currentTicks;

        var candidateHasSize = candidate.Size.HasValue && candidate.Size.Value >= 0;
        var currentHasSize = current.Size.HasValue && current.Size.Value >= 0;
        if (candidateHasSize != currentHasSize)
            return candidateHasSize;

        if (candidateHasSize && currentHasSize && candidate.Size!.Value != current.Size!.Value)
            return candidate.Size.Value > current.Size.Value;

        return string.Compare(candidate.Hash, current.Hash, StringComparison.OrdinalIgnoreCase) > 0;
    }

    private static string GetCsvStorageKey(FileCacheEntity entry)
        => entry.IsCacheEntry
            ? entry.Hash + "|" + entry.PrefixedFilePath
            : entry.PrefixedFilePath;

    private int RemoveOtherHashEntriesForPathUnsafe(FileCacheEntity fileCache)
    {
        if (fileCache == null || string.IsNullOrWhiteSpace(fileCache.PrefixedFilePath))
            return 0;

        // Cache entries may deliberately alias multiple requested hashes to one physical
        // downloaded file when the CDN/server route returns a different valid payload hash.
        // Do not collapse those aliases by path; hash lookup is the authority for downloads.
        if (fileCache.IsCacheEntry)
            return 0;

        var removed = 0;
        foreach (var kvp in _fileCaches.ToArray())
        {
            if (string.Equals(kvp.Key, fileCache.Hash, StringComparison.OrdinalIgnoreCase))
                continue;

            var entries = kvp.Value;
            if (entries == null)
                continue;

            removed += entries.RemoveAll(entry => string.Equals(entry.PrefixedFilePath, fileCache.PrefixedFilePath, StringComparison.OrdinalIgnoreCase));
            if (entries.Count == 0)
                _fileCaches.TryRemove(kvp.Key, out _);
        }

        return removed;
    }

    private void RebuildPrefixedPathIndexForPath(string prefixedPath)
    {
        if (string.IsNullOrWhiteSpace(prefixedPath))
            return;

        FileCacheEntity? best = null;
        long bestTicks = long.MinValue;

        lock (_fileCachesLock)
        {
            foreach (var e in _fileCaches.SelectMany(k => k.Value))
            {
                if (e == null) continue;
                if (!string.Equals(e.PrefixedFilePath, prefixedPath, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!long.TryParse(e.LastModifiedDateTicks, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks))
                    ticks = 0;

                if (best == null || ticks >= bestTicks)
                {
                    best = e;
                    bestTicks = ticks;
                }
            }
        }

        if (best != null) _prefixedPathIndex[prefixedPath] = best;
        else _prefixedPathIndex.TryRemove(prefixedPath, out _);
    }

    public (FileState State, FileCacheEntity FileCache) ValidateFileCacheEntity(FileCacheEntity fileCache)
    {
        fileCache = ReplacePathPrefixes(fileCache);
        FileInfo fi = new(fileCache.ResolvedFilepath);

        if (!fi.Exists)
        {
            return (FileState.RequireDeletion, fileCache);
        }

        if (!string.Equals(fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture), fileCache.LastModifiedDateTicks, StringComparison.Ordinal))
        {
            return (FileState.RequireUpdate, fileCache);
        }

        return (FileState.Valid, fileCache);
    }

    public void WriteOutFullCsv()
    {
        lock (_fileWriteLock)
        {
            List<FileCacheEntity> snapshot;
            lock (_fileCachesLock)
            {
                snapshot = CompactFileCachesByPathUnsafe()
                    .OrderBy(f => f.PrefixedFilePath, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

            Interlocked.Exchange(ref _csvRewriteNeeded, 0);

            StringBuilder sb = new();
            foreach (var entry in snapshot)
            {
                sb.AppendLine(entry.CsvEntry);
            }

            if (File.Exists(_csvPath))
            {
                File.Copy(_csvPath, CsvBakPath, overwrite: true);
            }

            try
            {
                File.WriteAllText(_csvPath, sb.ToString());
                File.Delete(CsvBakPath);
            }
            catch
            {
                File.WriteAllText(CsvBakPath, sb.ToString());
            }
        }
    }

    private List<FileCacheEntity> CompactFileCachesByPathUnsafe()
    {
        var entriesByStorageKey = new Dictionary<string, FileCacheEntity>(StringComparer.OrdinalIgnoreCase);
        var total = 0;

        foreach (var entry in _fileCaches.SelectMany(static kvp => kvp.Value))
        {
            if (entry == null || string.IsNullOrWhiteSpace(entry.PrefixedFilePath) || string.IsNullOrWhiteSpace(entry.Hash))
                continue;

            total++;
            var storageKey = GetCsvStorageKey(entry);
            if (!entriesByStorageKey.TryGetValue(storageKey, out var existing) || IsBetterPathEntry(entry, existing))
                entriesByStorageKey[storageKey] = entry;
        }

        if (entriesByStorageKey.Count != total)
        {
            _fileCaches.Clear();
            _prefixedPathIndex.Clear();

            foreach (var entry in entriesByStorageKey.Values)
            {
                if (!_fileCaches.TryGetValue(entry.Hash, out var entries) || entries is null)
                    _fileCaches[entry.Hash] = entries = [];

                entries.Add(entry);
                UpdatePrefixedPathIndex(entry);
            }
        }

        return entriesByStorageKey.Values.ToList();
    }

    internal FileCacheEntity MigrateFileHashToExtension(FileCacheEntity fileCache, string ext)
    {
        try
        {
            RemoveHashedFile(fileCache.Hash, fileCache.PrefixedFilePath);

            var extensionPath = fileCache.ResolvedFilepath.ToUpper(CultureInfo.InvariantCulture) + "." + ext;
            File.Move(fileCache.ResolvedFilepath, extensionPath, overwrite: true);

            var newHashedEntity = new FileCacheEntity(
                fileCache.Hash,
                fileCache.PrefixedFilePath + "." + ext,
                DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture));

            newHashedEntity.SetResolvedFilePath(extensionPath);
            AddHashedFile(newHashedEntity);

            _logger.LogTrace("Migrated from {oldPath} to {newPath}", fileCache.ResolvedFilepath, newHashedEntity.ResolvedFilepath);
            return newHashedEntity;
        }
        catch (Exception ex)
        {
            AddHashedFile(fileCache);
            _logger.LogWarning(ex, "Failed to migrate entity {entity}", fileCache.PrefixedFilePath);
            return fileCache;
        }
    }

    private bool AddHashedFile(FileCacheEntity fileCache, bool removeOtherHashEntriesForPath = true)
    {
        InvalidateValidatedFileCacheMemo(fileCache.PrefixedFilePath);
        var needsCsvRewrite = false;
        lock (_fileCachesLock)
        {
            if (removeOtherHashEntriesForPath)
            {
                var removedOtherHashes = RemoveOtherHashEntriesForPathUnsafe(fileCache);
                if (removedOtherHashes > 0)
                {
                    needsCsvRewrite = true;
                    _logger.LogTrace("Removed {count} stale hash entr{suffix} for path {path} while adding current hash {hash}", removedOtherHashes, removedOtherHashes == 1 ? "y" : "ies", fileCache.PrefixedFilePath, fileCache.Hash);
                }
            }

            if (!_fileCaches.TryGetValue(fileCache.Hash, out var entries) || entries is null)
            {
                _fileCaches[fileCache.Hash] = entries = [];
            }

            var existingIndex = entries.FindIndex(u => string.Equals(u.PrefixedFilePath, fileCache.PrefixedFilePath, StringComparison.OrdinalIgnoreCase));
            if (existingIndex >= 0)
            {
                needsCsvRewrite = true;
                entries[existingIndex] = fileCache;
                UpdatePrefixedPathIndex(fileCache);
            }
            else
            {
                entries.Add(fileCache);
                UpdatePrefixedPathIndex(fileCache);
            }
        }

        if (needsCsvRewrite)
            MarkCsvRewriteNeeded();

        return needsCsvRewrite;
    }

    private FileCacheEntity? CreateFileCacheEntity(FileInfo fileInfo, string prefixedPath, string? hash = null)
    {
        if (string.IsNullOrWhiteSpace(hash))
        {
            try
            {
                hash = Crypto.GetFileHash(fileInfo.FullName);
            }
            catch (IOException)
            {
                hash = Path.GetFileNameWithoutExtension(fileInfo.Name);
            }
            catch (UnauthorizedAccessException)
            {
                hash = Path.GetFileNameWithoutExtension(fileInfo.Name);
            }
        }

        if (string.IsNullOrWhiteSpace(hash))
            return null;

        long length = 0;
        long lastWriteTicks = 0;

        try { length = fileInfo.Length; }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }

        try { lastWriteTicks = fileInfo.LastWriteTimeUtc.Ticks; }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }

        var entity = new FileCacheEntity(
            hash,
            prefixedPath,
            lastWriteTicks.ToString(CultureInfo.InvariantCulture),
            length);

        entity = ReplacePathPrefixes(entity);

        try
        {
            var addNeedsRewrite = AddHashedFile(entity);

            if (addNeedsRewrite)
            {
                WriteOutFullCsv();
            }
            else
            {
                lock (_fileWriteLock)
                {
                    File.AppendAllLines(_csvPath, new[] { entity.CsvEntry });
                }
            }
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }

        var result = GetFileCacheByPath(fileInfo.FullName);
        _logger.LogTrace("Creating cache entity for {name} success: {success}", fileInfo.FullName, result != null);
        return result;
    }

    private void InvalidateValidatedFileCacheMemo(string? prefixedFilePath)
    {
        if (!string.IsNullOrWhiteSpace(prefixedFilePath))
            _validatedFileCacheMemo.TryRemove(prefixedFilePath, out _);
    }

    private FileCacheEntity? GetValidatedFileCache(FileCacheEntity fileCache)
    {
        var resultingFileCache = ReplacePathPrefixes(fileCache);
        var prefixedPath = resultingFileCache.PrefixedFilePath;

        FileInfo file;
        try
        {
            file = new FileInfo(resultingFileCache.ResolvedFilepath);
        }
        catch
        {
            if (!string.IsNullOrWhiteSpace(prefixedPath))
                _validatedFileCacheMemo.TryRemove(prefixedPath, out _);

            RemoveHashedFile(resultingFileCache.Hash, resultingFileCache.PrefixedFilePath);
            return null;
        }

        if (!file.Exists)
        {
            if (!string.IsNullOrWhiteSpace(prefixedPath))
                _validatedFileCacheMemo.TryRemove(prefixedPath, out _);

            RemoveHashedFile(resultingFileCache.Hash, resultingFileCache.PrefixedFilePath);
            return null;
        }

        var currentLastWriteTicks = file.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture);
        var currentSize = file.Length;

        if (!string.IsNullOrWhiteSpace(prefixedPath)
            && _validatedFileCacheMemo.TryGetValue(prefixedPath, out var cached)
            && cached.Size == currentSize
            && string.Equals(cached.LastModifiedTicks, currentLastWriteTicks, StringComparison.Ordinal)
            && string.Equals(resultingFileCache.LastModifiedDateTicks, currentLastWriteTicks, StringComparison.Ordinal))
        {
            return cached.Entity;
        }

        FileCacheEntity? validated;
        if (!string.Equals(currentLastWriteTicks, resultingFileCache.LastModifiedDateTicks, StringComparison.Ordinal)
            || (resultingFileCache.Size.HasValue && resultingFileCache.Size.Value >= 0 && resultingFileCache.Size.Value != currentSize))
        {
            UpdateHashedFile(resultingFileCache);
            validated = resultingFileCache;
        }
        else
        {
            validated = resultingFileCache;
        }

        if (!string.IsNullOrWhiteSpace(prefixedPath))
            _validatedFileCacheMemo[prefixedPath] = (validated, currentLastWriteTicks, currentSize);

        return validated;
    }

    private FileCacheEntity ReplacePathPrefixes(FileCacheEntity fileCache)
    {
        if (fileCache.PrefixedFilePath.StartsWith(PenumbraPrefix, StringComparison.OrdinalIgnoreCase))
        {
            fileCache.SetResolvedFilePath(
                fileCache.PrefixedFilePath.Replace(PenumbraPrefix, _ipcManager.Penumbra.ModDirectory, StringComparison.Ordinal));
        }
        else if (fileCache.PrefixedFilePath.StartsWith(CachePrefix, StringComparison.OrdinalIgnoreCase))
        {
            fileCache.SetResolvedFilePath(
                fileCache.PrefixedFilePath.Replace(CachePrefix, _configService.Current.CacheFolder, StringComparison.Ordinal));
        }

        return fileCache;
    }

    private FileCacheEntity? Validate(FileCacheEntity fileCache)
    {
        var file = new FileInfo(fileCache.ResolvedFilepath);

        if (!file.Exists)
        {
            RemoveHashedFile(fileCache.Hash, fileCache.PrefixedFilePath);
            return null;
        }

        if (!string.Equals(file.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture), fileCache.LastModifiedDateTicks, StringComparison.Ordinal))
        {
            UpdateHashedFile(fileCache);
        }

        return fileCache;
    }

    private void EnsureStartupLoadStarted(CancellationToken cancellationToken)
    {
        lock (_startupLoadGate)
        {
            if (_startupLoadTask != null)
                return;

            _startupLoadTask = Task.Factory.StartNew(() => LoadInitialCacheSnapshot(), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }

    private void LoadInitialCacheSnapshot()
    {
        try
        {
            Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
        }
        catch
        {
        }

        lock (_fileWriteLock)
        {
            try
            {
                _logger.LogInformation("Checking for {bakPath}", CsvBakPath);

                if (File.Exists(CsvBakPath))
                {
                    _logger.LogInformation("{bakPath} found, moving to {csvPath}", CsvBakPath, _csvPath);
                    File.Move(CsvBakPath, _csvPath, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to move BAK to ORG, deleting BAK");
                try
                {
                    if (File.Exists(CsvBakPath))
                        File.Delete(CsvBakPath);
                }
                catch (Exception ex1)
                {
                    _logger.LogWarning(ex1, "Could not delete bak file");
                }
            }
        }

        if (!File.Exists(_csvPath))
            return;

        if (!_ipcManager.Penumbra.APIAvailable || string.IsNullOrEmpty(_ipcManager.Penumbra.ModDirectory))
        {
            _logger.LogDebug("Loading local file cache snapshot while Penumbra is unavailable or has no mod directory; suppressing redundant user notification.");
        }

        _logger.LogInformation("{csvPath} found, parsing", _csvPath);

        bool success = false;
        string[] entries = [];
        int attempts = 0;

        while (!success && attempts < 10)
        {
            try
            {
                _logger.LogInformation("Attempting to read {csvPath}", _csvPath);
                entries = File.ReadAllLines(_csvPath);
                success = true;
            }
            catch (Exception ex)
            {
                attempts++;
                _logger.LogWarning(ex, "Could not open {file}, trying again", _csvPath);
                Thread.Sleep(100);
            }
        }

        if (!entries.Any())
        {
            _logger.LogWarning("Could not load entries from {path}, continuing with empty file cache", _csvPath);
        }

        _logger.LogInformation("Found {amount} files in {path}", entries.Length, _csvPath);

        Dictionary<string, FileCacheEntity> entriesByStorageKey = new(StringComparer.OrdinalIgnoreCase);
        List<string> duplicateSamples = [];
        int duplicateCount = 0;
        foreach (var entry in entries)
        {
            var splittedEntry = entry.Split(CsvSplit, 5, StringSplitOptions.None);

            try
            {
                var hash = splittedEntry[0];
                if (hash.Length != 40)
                    throw new InvalidOperationException("Expected Hash length of 40, received " + hash.Length);

                var path = splittedEntry[1];
                var time = splittedEntry[2];

                long size = -1;
                long compressed = -1;

                if (splittedEntry.Length > 3)
                {
                    if (long.TryParse(splittedEntry[3], CultureInfo.InvariantCulture, out long result))
                    {
                        size = result;
                    }

                    if (splittedEntry.Length > 4 &&
                        long.TryParse(splittedEntry[4], CultureInfo.InvariantCulture, out long resultCompressed))
                    {
                        compressed = resultCompressed;
                    }
                }

                var fileCache = ReplacePathPrefixes(new FileCacheEntity(hash, path, time, size, compressed));
                var storageKey = GetCsvStorageKey(fileCache);
                if (entriesByStorageKey.TryGetValue(storageKey, out var existing))
                {
                    duplicateCount++;
                    if (duplicateSamples.Count < 10)
                        duplicateSamples.Add(fileCache.PrefixedFilePath);

                    if (IsBetterPathEntry(fileCache, existing))
                        entriesByStorageKey[storageKey] = fileCache;

                    continue;
                }

                entriesByStorageKey[storageKey] = fileCache;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to initialize entry {entry}, ignoring", entry);
            }
        }

        foreach (var fileCache in entriesByStorageKey.Values)
            AddHashedFile(fileCache, removeOtherHashEntriesForPath: false);

        if (duplicateCount > 0)
        {
            _logger.LogWarning(
                "File cache snapshot contained {duplicateCount} duplicate path entries. Sample: {duplicateSamples}. Kept the newest/current entry for each path and rewrote compacted CSV.",
                duplicateCount,
                duplicateSamples.Count == 0 ? "(none)" : string.Join(", ", duplicateSamples));
        }

        if (entriesByStorageKey.Count != entries.Length)
        {
            WriteOutFullCsv();
        }
    }


    public async Task WaitForInitialCacheLoadAsync(CancellationToken cancellationToken)
    {
        EnsureStartupLoadStarted(cancellationToken);

        Task? startupLoadTask;
        lock (_startupLoadGate)
        {
            startupLoadTask = _startupLoadTask;
        }

        if (startupLoadTask == null || startupLoadTask.IsCompleted)
        {
            if (startupLoadTask != null)
                await startupLoadTask.ConfigureAwait(false);

            return;
        }

        var cancellationCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true), cancellationCompletion);

        var completed = await Task.WhenAny(startupLoadTask, cancellationCompletion.Task).ConfigureAwait(false);
        if (!ReferenceEquals(completed, startupLoadTask))
            cancellationToken.ThrowIfCancellationRequested();

        await startupLoadTask.ConfigureAwait(false);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting FileCacheManager");
        EnsureStartupLoadStarted(cancellationToken);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var startupLoadTask = _startupLoadTask;
        if (startupLoadTask != null)
        {
            try
            {
                await startupLoadTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // ignored during shutdown
            }
        }

        WriteOutFullCsv();
    }
}
