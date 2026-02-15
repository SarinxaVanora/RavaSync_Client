using K4os.Compression.LZ4.Legacy;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
    private readonly ConcurrentDictionary<string, FileCacheEntity> _prefixedPathIndex = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _getCachesByPathsSemaphore = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _missingHashProbeUntilUtcTicks = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan _missingHashProbeTtl = TimeSpan.FromSeconds(5);
    private readonly object _fileWriteLock = new();
    private readonly IpcManager _ipcManager;
    private readonly ILogger<FileCacheManager> _logger;
    public string CacheFolder => _configService.Current.CacheFolder;

    public FileCacheManager(ILogger<FileCacheManager> logger, IpcManager ipcManager, MareConfigService configService, MareMediator mareMediator)
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
        if (!fullName.Contains(_configService.Current.CacheFolder.ToLowerInvariant(), StringComparison.Ordinal)) return null;
        string prefixedPath = fullName.Replace(_configService.Current.CacheFolder.ToLowerInvariant(), CachePrefix + "\\", StringComparison.Ordinal).Replace("\\\\", "\\", StringComparison.Ordinal);

        var nameHash = Path.GetFileNameWithoutExtension(fi.Name);

        if (LooksLikeSha1(nameHash))
            return CreateFileCacheEntity(fi, prefixedPath, nameHash);

        return CreateFileCacheEntity(fi, prefixedPath);
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
        var fullName = fi.FullName.ToLowerInvariant();
        if (!fullName.Contains(_ipcManager.Penumbra.ModDirectory!.ToLowerInvariant(), StringComparison.Ordinal)) return null;
        string prefixedPath = fullName.Replace(_ipcManager.Penumbra.ModDirectory!.ToLowerInvariant(), PenumbraPrefix + "\\", StringComparison.Ordinal).Replace("\\\\", "\\", StringComparison.Ordinal);
        return CreateFileCacheEntity(fi, prefixedPath);
    }

    public List<FileCacheEntity> GetAllFileCaches() => _fileCaches.Values.SelectMany(v => v).ToList();

    public List<FileCacheEntity> GetAllFileCachesByHash(string hash, bool ignoreCacheEntries = false, bool validate = true)
    {
        List<FileCacheEntity> output = [];
        if (_fileCaches.TryGetValue(hash, out var fileCacheEntities))
        {
            foreach (var fileCache in fileCacheEntities.Where(c => ignoreCacheEntries ? !c.IsCacheEntry : true).ToList())
            {
                if (!validate) output.Add(fileCache);
                else
                {
                    var validated = GetValidatedFileCache(fileCache);
                    if (validated != null) output.Add(validated);
                }
            }
        }

        return output;
    }

    public Task<List<FileCacheEntity>> ValidateLocalIntegrity(IProgress<(int, int, FileCacheEntity)> progress,CancellationToken cancellationToken)
    {
        _mareMediator.Publish(new HaltScanMessage(nameof(ValidateLocalIntegrity)));
        _logger.LogInformation("Validating local storage");

        var cacheEntries = _fileCaches
            .SelectMany(v => v.Value)
            .Where(v => v.IsCacheEntry)
            .ToList();

        List<FileCacheEntity> brokenEntities = new();
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
                // File genuinely missing on disk
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

        _mareMediator.Publish(new ResumeScanMessage(nameof(ValidateLocalIntegrity)));
        return Task.FromResult(brokenEntities);
    }


    public string GetCacheFilePath(string hash, string extension)
    {
        return Path.Combine(_configService.Current.CacheFolder, hash + "." + extension);
    }

    public async Task<(string, byte[])> GetCompressedFileData(string fileHash, CancellationToken uploadToken)
    {
        var fileCache = GetFileCacheByHash(fileHash)!.ResolvedFilepath;
        return (fileHash, LZ4Wrapper.WrapHC(await File.ReadAllBytesAsync(fileCache, uploadToken).ConfigureAwait(false), 0,
            (int)new FileInfo(fileCache).Length));
    }


    public FileCacheEntity? GetFileCacheByHash(string hash)
    {
        if (_fileCaches.TryGetValue(hash, out var hashes))
        {
            foreach (var candidate in hashes
                         .OrderBy(p => p.PrefixedFilePath.Contains(PenumbraPrefix, StringComparison.OrdinalIgnoreCase) ? 0 : 1))
            {
                var validated = GetValidatedFileCache(candidate);
                if (validated != null)
                    return validated;
            }
        }

        // Negative cache: avoid repeated expensive filesystem scans when many hashes are missing
        var nowTicks = DateTime.UtcNow.Ticks;
        if (_missingHashProbeUntilUtcTicks.TryGetValue(hash, out var untilTicks) && untilTicks > nowTicks)
            return null;

        //try
        //{
        //    var cacheFolder = _configService.Current.CacheFolder;
        //    if (!string.IsNullOrEmpty(cacheFolder) && Directory.Exists(cacheFolder))
        //    {
        //        var first = Directory.EnumerateFiles(cacheFolder, hash + ".*", SearchOption.AllDirectories).FirstOrDefault();
        //        if (!string.IsNullOrEmpty(first))
        //        {
        //            // Found: clear miss cache
        //            _missingHashProbeUntilUtcTicks.TryRemove(hash, out _);

        //            var fi = new FileInfo(first);

        //            try
        //            {
        //                // Can throw if something has an exclusive lock
        //                var computed = Crypto.GetFileHash(fi.FullName);

        //                // Wrong content? Kill the stray and treat as missing
        //                if (!string.Equals(computed, hash, StringComparison.OrdinalIgnoreCase))
        //                {
        //                    _logger.LogWarning(
        //                        "Cache probe: file {file} has hash {actual} but expected {expected}, deleting",
        //                        fi.FullName, computed, hash);

        //                    try { File.Delete(fi.FullName); } catch { /* ignore */ }

        //                    // mark miss briefly so we don't thrash disk
        //                    _missingHashProbeUntilUtcTicks[hash] = DateTime.UtcNow.Add(_missingHashProbeTtl).Ticks;
        //                    return null;
        //                }
        //            }
        //            catch (IOException ioEx)
        //            {
        //                // The file definitely exists but is temporarily locked (AV, Penumbra, etc.).
        //                // Treat it as present and valid, using the expected hash.
        //                _logger.LogDebug(
        //                    ioEx,
        //                    "IO lock while hashing {file} for {hash}; treating file as existing and valid",
        //                    fi.FullName, hash);

        //                var cacheRootLower = cacheFolder.ToLowerInvariant();
        //                var fullNameLower = fi.FullName.ToLowerInvariant();

        //                if (!fullNameLower.Contains(cacheRootLower, StringComparison.Ordinal))
        //                    return null;

        //                var prefixedPath = fullNameLower
        //                    .Replace(cacheRootLower, CachePrefix + "\\", StringComparison.Ordinal)
        //                    .Replace("\\\\", "\\", StringComparison.Ordinal);

        //                var entity = new FileCacheEntity(
        //                    hash,
        //                    prefixedPath,
        //                    fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture),
        //                    fi.Length);

        //                entity = ReplacePathPrefixes(entity);
        //                AddHashedFile(entity);

        //                // Keep CSV in sync for maint tools
        //                lock (_fileWriteLock)
        //                {
        //                    File.AppendAllLines(_csvPath, new[] { entity.CsvEntry });
        //                }

        //                return entity;
        //            }

        //            // If we got here, hashing succeeded and matched the expected hash.
        //            var cacheRootLower2 = cacheFolder.ToLowerInvariant();
        //            var fullNameLower2 = fi.FullName.ToLowerInvariant();

        //            if (!fullNameLower2.Contains(cacheRootLower2, StringComparison.Ordinal))
        //                return null;

        //            string prefixedPath2 = fullNameLower2
        //                .Replace(cacheRootLower2, CachePrefix + "\\", StringComparison.Ordinal)
        //                .Replace("\\\\", "\\", StringComparison.Ordinal);

        //            return CreateFileCacheEntity(fi, prefixedPath2, hash);
        //        }
        //    }
        //}
        //catch (Exception ex)
        //{
        //    _logger.LogWarning(ex, "Error while probing cache folder for hash {hash}", hash);
        //}

        _missingHashProbeUntilUtcTicks[hash] = DateTime.UtcNow.Add(_missingHashProbeTtl).Ticks;
        return null;
    }




    private FileCacheEntity? GetFileCacheByPath(string path)
    {
        var cleanedPath = path.Replace("/", "\\", StringComparison.OrdinalIgnoreCase).ToLowerInvariant()
            .Replace(_ipcManager.Penumbra.ModDirectory!.ToLowerInvariant(), "", StringComparison.OrdinalIgnoreCase);
        var entry = _fileCaches.SelectMany(v => v.Value).FirstOrDefault(f => f.ResolvedFilepath.EndsWith(cleanedPath, StringComparison.OrdinalIgnoreCase));

        if (entry == null)
        {
            _logger.LogDebug("Found no entries for {path}", cleanedPath);
            return CreateFileEntry(path);
        }

        var validatedCacheEntry = GetValidatedFileCache(entry);

        return validatedCacheEntry;
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

                var prefixed = original
                    .Replace("/", "\\", StringComparison.OrdinalIgnoreCase);

                if (!string.IsNullOrEmpty(modDir))
                    prefixed = prefixed.Replace(modDir, PenumbraPrefix + '\\', StringComparison.OrdinalIgnoreCase);

                if (!string.IsNullOrEmpty(cacheDir))
                    prefixed = prefixed.Replace(cacheDir, CachePrefix + '\\', StringComparison.OrdinalIgnoreCase);

                prefixed = prefixed.Replace("\\\\", "\\", StringComparison.Ordinal);

                if (_prefixedPathIndex.TryGetValue(prefixed, out var entity))
                {
                    result[original] = GetValidatedFileCache(entity);
                }
                else
                {
                    result[original] = !prefixed.Contains(CachePrefix, StringComparison.Ordinal)
                        ? CreateFileEntry(original)
                        : CreateCacheEntry(original);
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
        if (_fileCaches.TryGetValue(hash, out var caches))
        {
            var removedCount = caches.RemoveAll(c => string.Equals(c.PrefixedFilePath, prefixedFilePath, StringComparison.OrdinalIgnoreCase));
            _logger.LogTrace("Removed from DB: {count} file(s) with hash {hash} and file cache {path}", removedCount, hash, prefixedFilePath);

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


    public void UpdateHashedFile(FileCacheEntity fileCache, bool computeProperties = true)
    {
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
                // "File in use" or similar – don’t treat this as broken.
                // Keep the existing entry and bail out.
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

            // If ticks are weird/missing, prefer the newer arrival.
            return entity;
        });
    }

    private void RebuildPrefixedPathIndexForPath(string prefixedPath)
    {
        if (string.IsNullOrWhiteSpace(prefixedPath))
            return;

        FileCacheEntity? best = null;
        long bestTicks = long.MinValue;

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
            StringBuilder sb = new();
            foreach (var entry in _fileCaches.SelectMany(k => k.Value).OrderBy(f => f.PrefixedFilePath, StringComparer.OrdinalIgnoreCase))
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
            var newHashedEntity = new FileCacheEntity(fileCache.Hash, fileCache.PrefixedFilePath + "." + ext, DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture));
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
        if (!_fileCaches.TryGetValue(fileCache.Hash, out var entries) || entries is null)
        {
            _fileCaches[fileCache.Hash] = entries = [];
        }

        if (!entries.Exists(u => string.Equals(u.PrefixedFilePath, fileCache.PrefixedFilePath, StringComparison.OrdinalIgnoreCase)))
        {
            entries.Add(fileCache);
            UpdatePrefixedPathIndex(fileCache);
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
                // file is locked/in-flight; treat as present
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
        catch (IOException) { /* locked -> treat as pass */ }
        catch (UnauthorizedAccessException) { }

        try { lastWriteTicks = fileInfo.LastWriteTimeUtc.Ticks; }
        catch (IOException) { /* locked -> treat as pass */ }
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
        _logger.LogTrace("Creating cache entity for {name} success: {success}", fileInfo.FullName, (result != null));
        return result;
    }


    private FileCacheEntity? GetValidatedFileCache(FileCacheEntity fileCache)
    {
        var resultingFileCache = ReplacePathPrefixes(fileCache);
        //_logger.LogTrace("Validating {path}", fileCache.PrefixedFilePath);
        resultingFileCache = Validate(resultingFileCache);
        return resultingFileCache;
    }

    private FileCacheEntity ReplacePathPrefixes(FileCacheEntity fileCache)
    {
        if (fileCache.PrefixedFilePath.StartsWith(PenumbraPrefix, StringComparison.OrdinalIgnoreCase))
        {
            fileCache.SetResolvedFilePath(fileCache.PrefixedFilePath.Replace(PenumbraPrefix, _ipcManager.Penumbra.ModDirectory, StringComparison.Ordinal));
        }
        else if (fileCache.PrefixedFilePath.StartsWith(CachePrefix, StringComparison.OrdinalIgnoreCase))
        {
            fileCache.SetResolvedFilePath(fileCache.PrefixedFilePath.Replace(CachePrefix, _configService.Current.CacheFolder, StringComparison.Ordinal));
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

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting FileCacheManager");

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

        if (File.Exists(_csvPath))
        {
            if (!_ipcManager.Penumbra.APIAvailable || string.IsNullOrEmpty(_ipcManager.Penumbra.ModDirectory))
            {
                _mareMediator.Publish(new NotificationMessage("Penumbra not connected",
                    "Could not load local file cache data. Penumbra is not connected or not properly set up. Please enable and/or configure Penumbra properly to use RavaSync. After, reload RavaSync in the Plugin installer.",
                    MareConfiguration.Models.NotificationType.Error));
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

            Dictionary<string, bool> processedFiles = new(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in entries)
            {
                var splittedEntry = entry.Split(CsvSplit, StringSplitOptions.None);
                try
                {
                    var hash = splittedEntry[0];
                    if (hash.Length != 40) throw new InvalidOperationException("Expected Hash length of 40, received " + hash.Length);
                    var path = splittedEntry[1];
                    var time = splittedEntry[2];

                    if (processedFiles.ContainsKey(path))
                    {
                        _logger.LogWarning("Already processed {file}, ignoring", path);
                        continue;
                    }

                    processedFiles.Add(path, value: true);

                    long size = -1;
                    long compressed = -1;
                    if (splittedEntry.Length > 3)
                    {
                        if (long.TryParse(splittedEntry[3], CultureInfo.InvariantCulture, out long result))
                        {
                            size = result;
                        }
                        if (long.TryParse(splittedEntry[4], CultureInfo.InvariantCulture, out long resultCompressed))
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

            if (processedFiles.Count != entries.Length)
            {
                WriteOutFullCsv();
            }
        }

       // _ = Task.Run(() => RepairCacheFromFilesystem(cancellationToken));

        _logger.LogInformation("Started FileCacheManager");

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        WriteOutFullCsv();
        return Task.CompletedTask;
    }
}