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
    private readonly SemaphoreSlim _getCachesByPathsSemaphore = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _missingHashProbeUntilUtcTicks = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan _missingHashProbeTtl = TimeSpan.FromSeconds(5);
    private readonly ConcurrentDictionary<string, (FileCacheEntity? Entity, string LastModifiedTicks, long ExpiresUtcTicks)> _validatedFileCacheMemo = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan _validatedFileCacheMemoTtl = TimeSpan.FromMilliseconds(750);

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

        var csvLines = new List<string>();

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

            AddHashedFile(entry);
            _missingHashProbeUntilUtcTicks.TryRemove(entry.Hash, out _);

            if (!alreadyKnown)
                csvLines.Add(entry.CsvEntry);
        }

        if (csvLines.Count == 0)
            return;

        try
        {
            lock (_fileWriteLock)
            {
                File.AppendAllLines(_csvPath, csvLines);
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

        var fullName = fi.FullName.ToLowerInvariant();
        var modDirLower = modDirectory.ToLowerInvariant();

        if (!fullName.Contains(modDirLower, StringComparison.Ordinal))
            return null;

        string prefixedPath = fullName
            .Replace(modDirLower, PenumbraPrefix + "\\", StringComparison.Ordinal)
            .Replace("\\\\", "\\", StringComparison.Ordinal);

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
        List<FileCacheEntity>? candidates = null;

        lock (_fileCachesLock)
        {
            if (_fileCaches.TryGetValue(hash, out var hashes))
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
        if (_missingHashProbeUntilUtcTicks.TryGetValue(hash, out var untilTicks) && untilTicks > nowTicks)
            return null;

        _missingHashProbeUntilUtcTicks[hash] = DateTime.UtcNow.Add(_missingHashProbeTtl).Ticks;
        return null;
    }

    private FileCacheEntity? GetFileCacheByPath(string path)
    {
        var prefixedPath = path.Replace("/", "\\", StringComparison.OrdinalIgnoreCase);

        var modDir = _ipcManager.Penumbra.ModDirectory;
        if (!string.IsNullOrEmpty(modDir))
        {
            prefixedPath = prefixedPath.Replace(modDir, PenumbraPrefix + "\\", StringComparison.OrdinalIgnoreCase);
        }

        var cacheDir = _configService.Current.CacheFolder;
        if (!string.IsNullOrEmpty(cacheDir))
        {
            prefixedPath = prefixedPath.Replace(cacheDir, CachePrefix + "\\", StringComparison.OrdinalIgnoreCase);
        }

        prefixedPath = prefixedPath.Replace("\\\\", "\\", StringComparison.Ordinal);

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
            var modDir = _ipcManager.Penumbra.ModDirectory ?? string.Empty;
            var cacheDir = _configService.Current.CacheFolder ?? string.Empty;

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, FileCacheEntity?>(StringComparer.OrdinalIgnoreCase);

            foreach (var original in paths)
            {
                if (string.IsNullOrWhiteSpace(original)) continue;
                if (!seen.Add(original)) continue;

                var prefixed = original.Replace("/", "\\", StringComparison.OrdinalIgnoreCase);

                if (!string.IsNullOrEmpty(modDir))
                    prefixed = prefixed.Replace(modDir, PenumbraPrefix + "\\", StringComparison.OrdinalIgnoreCase);

                if (!string.IsNullOrEmpty(cacheDir))
                    prefixed = prefixed.Replace(cacheDir, CachePrefix + "\\", StringComparison.OrdinalIgnoreCase);

                prefixed = prefixed.Replace("\\\\", "\\", StringComparison.Ordinal);

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
                snapshot = _fileCaches
                    .SelectMany(k => k.Value)
                    .OrderBy(f => f.PrefixedFilePath, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

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

    private void AddHashedFile(FileCacheEntity fileCache)
    {
        InvalidateValidatedFileCacheMemo(fileCache.PrefixedFilePath);
        lock (_fileCachesLock)
        {
            if (!_fileCaches.TryGetValue(fileCache.Hash, out var entries) || entries is null)
            {
                _fileCaches[fileCache.Hash] = entries = [];
            }

            var existingIndex = entries.FindIndex(u => string.Equals(u.PrefixedFilePath, fileCache.PrefixedFilePath, StringComparison.OrdinalIgnoreCase));
            if (existingIndex >= 0)
            {
                entries[existingIndex] = fileCache;
                UpdatePrefixedPathIndex(fileCache);
            }
            else
            {
                entries.Add(fileCache);
                UpdatePrefixedPathIndex(fileCache);
            }
        }
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
            AddHashedFile(entity);

            lock (_fileWriteLock)
            {
                File.AppendAllLines(_csvPath, new[] { entity.CsvEntry });
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
        var nowTicks = DateTime.UtcNow.Ticks;

        if (!string.IsNullOrWhiteSpace(prefixedPath)
            && _validatedFileCacheMemo.TryGetValue(prefixedPath, out var cached)
            && cached.ExpiresUtcTicks > nowTicks
            && string.Equals(cached.LastModifiedTicks, resultingFileCache.LastModifiedDateTicks, StringComparison.Ordinal))
        {
            return cached.Entity;
        }

        var validated = Validate(resultingFileCache);

        if (!string.IsNullOrWhiteSpace(prefixedPath))
        {
            _validatedFileCacheMemo[prefixedPath] = (
                validated,
                resultingFileCache.LastModifiedDateTicks,
                DateTime.UtcNow.Add(_validatedFileCacheMemoTtl).Ticks);
        }

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

        HashSet<string> processedFiles = new(StringComparer.OrdinalIgnoreCase);
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

                if (!processedFiles.Add(path))
                {
                    duplicateCount++;
                    if (duplicateSamples.Count < 10)
                        duplicateSamples.Add(path);
                    continue;
                }

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

                AddHashedFile(ReplacePathPrefixes(new FileCacheEntity(hash, path, time, size, compressed)));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to initialize entry {entry}, ignoring", entry);
            }
        }

        if (duplicateCount > 0)
        {
            _logger.LogWarning(
                "File cache snapshot contained {duplicateCount} duplicate path entries. Sample: {duplicateSamples}. Rewriting compacted CSV.",
                duplicateCount,
                duplicateSamples.Count == 0 ? "(none)" : string.Join(", ", duplicateSamples));
        }

        if (processedFiles.Count != entries.Length)
        {
            WriteOutFullCsv();
        }
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