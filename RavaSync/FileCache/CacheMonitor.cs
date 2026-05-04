using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace RavaSync.FileCache;

public sealed class CacheMonitor : DisposableMediatorSubscriberBase
{
    private readonly MareConfigService _configService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileCompactor _fileCompactor;
    private readonly FileCacheManager _fileDbManager;
    private readonly IpcManager _ipcManager;
    private readonly PerformanceCollectorService _performanceCollector;
    private long _currentFileProgress = 0;
    private CancellationTokenSource _scanCancellationTokenSource = new();
    private readonly CancellationTokenSource _periodicCalculationTokenSource = new();
    private readonly object _csvFlushGate = new();
    private CancellationTokenSource? _csvFlushCts;
    public static readonly IImmutableList<string> AllowedFileExtensions = [".mdl", ".tex", ".mtrl", ".tmb", ".pap", ".avfx", ".atex", ".sklb", ".eid", ".phy", ".phyb", ".pbd", ".scd", ".skp", ".shpk"];
    private const string AutomaticTrimScanLock = "CacheMonitor.AutomaticTrim";
    private const int AutomaticTrimDeleteRetryCount = 5;
    private static readonly TimeSpan AutomaticTrimDeleteRetryDelay = TimeSpan.FromMilliseconds(150);

    public CacheMonitor(ILogger<CacheMonitor> logger, IpcManager ipcManager, MareConfigService configService,
        FileCacheManager fileDbManager, MareMediator mediator, PerformanceCollectorService performanceCollector, DalamudUtilService dalamudUtil,
        FileCompactor fileCompactor) : base(logger, mediator)
    {
        _ipcManager = ipcManager;
        _configService = configService;
        _fileDbManager = fileDbManager;
        _performanceCollector = performanceCollector;
        _dalamudUtil = dalamudUtil;
        _fileCompactor = fileCompactor;
        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) =>
        {
            StartPenumbraWatcher(_ipcManager.Penumbra.ModDirectory);
            StartMareWatcher(configService.Current.CacheFolder);
            InvokeScan();
        });
        Mediator.Subscribe<HaltScanMessage>(this, (msg) => HaltScan(msg.Source));
        Mediator.Subscribe<ResumeScanMessage>(this, (msg) => ResumeScan(msg.Source));
        Mediator.Subscribe<DalamudLoginMessage>(this, (_) =>
        {
            StartMareWatcher(configService.Current.CacheFolder);
            StartPenumbraWatcher(_ipcManager.Penumbra.ModDirectory);
            InvokeScan();
        });
        Mediator.Subscribe<PenumbraDirectoryChangedMessage>(this, (msg) =>
        {
            StartPenumbraWatcher(msg.ModDirectory);
            InvokeScan();
        });
        if (_ipcManager.Penumbra.APIAvailable && !string.IsNullOrEmpty(_ipcManager.Penumbra.ModDirectory))
        {
            StartPenumbraWatcher(_ipcManager.Penumbra.ModDirectory);
        }
        if (configService.Current.HasValidSetup())
        {
            StartMareWatcher(configService.Current.CacheFolder);
            InvokeScan();
        }

        var token = _periodicCalculationTokenSource.Token;
        _ = Task.Run(async () =>
        {
            Logger.LogInformation("Starting Periodic Storage Directory Calculation Task");
            var token = _periodicCalculationTokenSource.Token;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    while (_dalamudUtil.IsOnFrameworkThread && !token.IsCancellationRequested)
                    {
                        await Task.Delay(1).ConfigureAwait(false);
                    }

                    RecalculateFileCacheSize(token);
                }
                catch
                {
                    // ignore
                }
                await Task.Delay(TimeSpan.FromMinutes(1), token).ConfigureAwait(false);
            }
        }, token);
    }

    public long CurrentFileProgress => _currentFileProgress;
    public long FileCacheSize { get; set; }
    public long FileCacheDriveFree { get; set; }
    public ConcurrentDictionary<string, int> HaltScanLocks { get; set; } = new(StringComparer.Ordinal);
    public bool IsScanRunning => CurrentFileProgress > 0 || TotalFiles > 0;
    public long TotalFiles { get; private set; }
    public long TotalFilesStorage { get; private set; }

    public void HaltScan(string source)
    {
        if (!HaltScanLocks.ContainsKey(source)) HaltScanLocks[source] = 0;
        HaltScanLocks[source]++;
    }

    record WatcherChange(WatcherChangeTypes ChangeType, string? OldPath = null);
    private readonly Dictionary<string, WatcherChange> _watcherChanges = new Dictionary<string, WatcherChange>(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, WatcherChange> _mareChanges = new Dictionary<string, WatcherChange>(StringComparer.OrdinalIgnoreCase);

    public void StopMonitoring()
    {
        Logger.LogInformation("Stopping monitoring of Penumbra and RavaSync Storage folders");
        MareWatcher?.Dispose();
        PenumbraWatcher?.Dispose();
        MareWatcher = null;
        PenumbraWatcher = null;
    }

    public bool StorageisNTFS { get; private set; } = false;

    public void StartMareWatcher(string? marePath)
    {
        MareWatcher?.Dispose();
        if (string.IsNullOrEmpty(marePath) || !Directory.Exists(marePath))
        {
            MareWatcher = null;
            Logger.LogWarning("RavaSync file path is not set, cannot start the FSW for RavaSync.");
            return;
        }

        DriveInfo di = new(new DirectoryInfo(_configService.Current.CacheFolder).Root.FullName);
        StorageisNTFS = string.Equals("NTFS", di.DriveFormat, StringComparison.OrdinalIgnoreCase);
        Logger.LogInformation("RavaSync Storage is on NTFS drive: {isNtfs}", StorageisNTFS);

        Logger.LogDebug("Initializing RavaSync FSW on {path}", marePath);
        MareWatcher = new()
        {
            Path = marePath,
            InternalBufferSize = 8388608,
            NotifyFilter = NotifyFilters.CreationTime
                | NotifyFilters.LastWrite
                | NotifyFilters.FileName
                | NotifyFilters.DirectoryName
                | NotifyFilters.Size,
            Filter = "*.*",
            IncludeSubdirectories = false,
        };

        MareWatcher.Deleted += MareWatcher_FileChanged;
        MareWatcher.Created += MareWatcher_FileChanged;
        MareWatcher.EnableRaisingEvents = true;
    }

    private void MareWatcher_FileChanged(object sender, FileSystemEventArgs e)
    {
        Logger.LogTrace("RavaSync FSW: FileChanged: {change} => {path}", e.ChangeType, e.FullPath);

        if (!AllowedFileExtensions.Any(ext => e.FullPath.EndsWith(ext, StringComparison.OrdinalIgnoreCase))) return;

        lock (_watcherChanges)
        {
            _mareChanges[e.FullPath] = new(e.ChangeType);
        }

        _ = MareWatcherExecution();
    }

    public void StartPenumbraWatcher(string? penumbraPath)
    {
        PenumbraWatcher?.Dispose();
        if (string.IsNullOrEmpty(penumbraPath))
        {
            PenumbraWatcher = null;
            Logger.LogWarning("Penumbra is not connected or the path is not set, cannot start FSW for Penumbra.");
            return;
        }

        Logger.LogDebug("Initializing Penumbra FSW on {path}", penumbraPath);
        PenumbraWatcher = new()
        {
            Path = penumbraPath,
            InternalBufferSize = 8388608,
            NotifyFilter = NotifyFilters.CreationTime
                | NotifyFilters.LastWrite
                | NotifyFilters.FileName
                | NotifyFilters.DirectoryName
                | NotifyFilters.Size,
            Filter = "*.*",
            IncludeSubdirectories = true
        };

        PenumbraWatcher.Deleted += Fs_Changed;
        PenumbraWatcher.Created += Fs_Changed;
        PenumbraWatcher.Changed += Fs_Changed;
        PenumbraWatcher.Renamed += Fs_Renamed;
        PenumbraWatcher.EnableRaisingEvents = true;
    }

    private void Fs_Changed(object sender, FileSystemEventArgs e)
    {
        if (Directory.Exists(e.FullPath)) return;
        if (!AllowedFileExtensions.Any(ext => e.FullPath.EndsWith(ext, StringComparison.OrdinalIgnoreCase))) return;

        if (e.ChangeType is not (WatcherChangeTypes.Changed or WatcherChangeTypes.Deleted or WatcherChangeTypes.Created))
            return;

        lock (_watcherChanges)
        {
            _watcherChanges[e.FullPath] = new(e.ChangeType);
        }

        Logger.LogTrace("FSW {event}: {path}", e.ChangeType, e.FullPath);

        _ = PenumbraWatcherExecution();
    }

    private void Fs_Renamed(object sender, RenamedEventArgs e)
    {
        if (Directory.Exists(e.FullPath))
        {
            var directoryFiles = Directory.GetFiles(e.FullPath, "*.*", SearchOption.AllDirectories);
            lock (_watcherChanges)
            {
                foreach (var file in directoryFiles)
                {
                    if (!AllowedFileExtensions.Any(ext => file.EndsWith(ext, StringComparison.OrdinalIgnoreCase))) continue;
                    var oldPath = file.Replace(e.FullPath, e.OldFullPath, StringComparison.OrdinalIgnoreCase);

                    _watcherChanges.Remove(oldPath);
                    _watcherChanges[file] = new(WatcherChangeTypes.Renamed, oldPath);
                    Logger.LogTrace("FSW Renamed: {path} -> {new}", oldPath, file);

                }
            }
        }
        else
        {
            if (!AllowedFileExtensions.Any(ext => e.FullPath.EndsWith(ext, StringComparison.OrdinalIgnoreCase))) return;

            lock (_watcherChanges)
            {
                _watcherChanges.Remove(e.OldFullPath);
                _watcherChanges[e.FullPath] = new(WatcherChangeTypes.Renamed, e.OldFullPath);
            }

            Logger.LogTrace("FSW Renamed: {path} -> {new}", e.OldFullPath, e.FullPath);
        }

        _ = PenumbraWatcherExecution();
    }

    private CancellationTokenSource _penumbraFswCts = new();
    private CancellationTokenSource _mareFswCts = new();
    public FileSystemWatcher? PenumbraWatcher { get; private set; }
    public FileSystemWatcher? MareWatcher { get; private set; }

    private async Task MareWatcherExecution()
    {
        _mareFswCts = _mareFswCts.CancelRecreate();
        var token = _mareFswCts.Token;
        var delay = TimeSpan.FromSeconds(5);
        Dictionary<string, WatcherChange> changes;
        lock (_mareChanges)
            changes = _mareChanges.ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
        try
        {
            do
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
            } while (HaltScanLocks.Any(f => f.Value > 0));
        }
        catch (TaskCanceledException)
        {
            return;
        }

        lock (_mareChanges)
        {
            foreach (var key in changes.Keys)
            {
                _mareChanges.Remove(key);
            }
        }

        HandleChanges(changes, fromPenumbraWatcher: false);
    }

    private void HandleChanges(Dictionary<string, WatcherChange> changes, bool fromPenumbraWatcher)
    {
        string[] allChanges;

        lock (_fileDbManager)
        {
            var deletedEntries = changes.Where(c => c.Value.ChangeType == WatcherChangeTypes.Deleted).Select(c => c.Key);
            var renamedEntries = changes.Where(c => c.Value.ChangeType == WatcherChangeTypes.Renamed);
            var remainingEntries = changes.Where(c => c.Value.ChangeType != WatcherChangeTypes.Deleted).Select(c => c.Key);

            foreach (var entry in deletedEntries)
            {
                Logger.LogDebug("FSW Change: Deletion - {val}", entry);
            }

            foreach (var entry in renamedEntries)
            {
                Logger.LogDebug("FSW Change: Renamed - {oldVal} => {val}", entry.Value.OldPath, entry.Key);
            }

            foreach (var entry in remainingEntries)
            {
                Logger.LogDebug("FSW Change: Creation or Change - {val}", entry);
            }

            allChanges = deletedEntries
                .Concat(renamedEntries.Select(c => c.Value.OldPath!))
                .Concat(renamedEntries.Select(c => c.Key))
                .Concat(remainingEntries)
                .ToArray();

            _ = _fileDbManager.GetFileCachesByPaths(allChanges);
        }

        ScheduleCsvFlush();

        if (fromPenumbraWatcher)
        {
            var changedPaths = allChanges
                .Where(path => !string.IsNullOrWhiteSpace(path))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            if (changedPaths.Length > 0)
            {
                Logger.LogDebug("Penumbra file cache changed for {count} file(s); scheduling owned object cache rebuild", changedPaths.Length);
                Mediator.Publish(new PenumbraFileCacheChangedMessage(changedPaths));
            }
        }
    }

    private void ScheduleCsvFlush()
    {
        lock (_csvFlushGate)
        {
            _csvFlushCts?.CancelDispose();
            _csvFlushCts = new CancellationTokenSource();
            var token = _csvFlushCts.Token;

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), token).ConfigureAwait(false);

                    if (!token.IsCancellationRequested)
                    {
                        _fileDbManager.WriteOutFullCsv();
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    Logger.LogTrace(ex, "Coalesced file-cache CSV flush failed");
                }
            }, token);
        }
    }

    private async Task PenumbraWatcherExecution()
    {
        _penumbraFswCts = _penumbraFswCts.CancelRecreate();
        var token = _penumbraFswCts.Token;
        Dictionary<string, WatcherChange> changes;
        lock (_watcherChanges)
            changes = _watcherChanges.ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
        var delay = TimeSpan.FromSeconds(10);
        try
        {
            do
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
            } while (HaltScanLocks.Any(f => f.Value > 0));
        }
        catch (TaskCanceledException)
        {
            return;
        }

        lock (_watcherChanges)
        {
            foreach (var key in changes.Keys)
            {
                _watcherChanges.Remove(key);
            }
        }

        HandleChanges(changes, fromPenumbraWatcher: true);
    }

    public void InvokeScan()
    {
        TotalFiles = 0;
        _currentFileProgress = 0;
        _scanCancellationTokenSource = _scanCancellationTokenSource?.CancelRecreate() ?? new CancellationTokenSource();
        var token = _scanCancellationTokenSource.Token;

        _ = Task.Run(async () =>
        {
            Logger.LogDebug("Starting Full File Scan");
            TotalFiles = 0;
            _currentFileProgress = 0;

            while (_dalamudUtil.IsOnFrameworkThread && !token.IsCancellationRequested)
            {
                Logger.LogWarning("Scanner is on framework, waiting for leaving thread before continuing");
                await Task.Delay(250, token).ConfigureAwait(false);
            }

            try
            {
                await Task.Factory.StartNew(() =>
                {
                    if (_performanceCollector.Enabled)
                        _performanceCollector.LogPerformance(this, $"FullFileScan", () => FullFileScan(token));
                    else
                        FullFileScan(token);
                }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during Full File Scan");
            }
            finally
            {
                TotalFiles = 0;
                _currentFileProgress = 0;
            }
        }, token);
    }

    public void RecalculateFileCacheSize(CancellationToken token)
    {
        if (string.IsNullOrEmpty(_configService.Current.CacheFolder)
            || !Directory.Exists(_configService.Current.CacheFolder))
        {
            FileCacheSize = 0;
            return;
        }

        // Per-file safety cap: nothing in cache should realistically be bigger than this
        const long MaxSingleFileBytes = 400L * 1024 * 1024; // 400 MiB

        FileCacheSize = -1;

        try
        {
            var di = new DriveInfo(new DirectoryInfo(_configService.Current.CacheFolder).Root.FullName);
            FileCacheDriveFree = di.AvailableFreeSpace;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Could not determine drive size for Storage Folder {folder}",
                _configService.Current.CacheFolder);
        }

        // Oldest-first list of files
        var files = Directory.EnumerateFiles(_configService.Current.CacheFolder)
            .Select(f => new FileInfo(f))
            .OrderBy(f => f.LastAccessTime)
            .ToList();

        // Recalculate total size with simple, safe logic
        long total = 0;
        foreach (var fi in files)
        {
            token.ThrowIfCancellationRequested();

            long size;
            try
            {
                size = _fileCompactor.GetFileSizeOnDisk(fi, StorageisNTFS);
            }
            catch
            {
                size = 0;
            }

            // Clamp insane values and fall back to logical length
            if (size < 0 || size > MaxSingleFileBytes)
            {
                try
                {
                    size = fi.Length;
                }
                catch
                {
                    size = 0;
                }
            }

            if (size < 0) size = 0;

            total += size;
        }

        FileCacheSize = total;

        var maxCacheInBytes = (long)(_configService.Current.MaxLocalCacheInGiB * 1024d * 1024d * 1024d);
        if (FileCacheSize < maxCacheInBytes) return;

        var maxCacheBuffer = (long)(maxCacheInBytes * 0.05d); // 5% buffer
        var targetSize = maxCacheInBytes - maxCacheBuffer;

        // Trim until we’re back under (limit - buffer) or out of files
        HaltScan(AutomaticTrimScanLock);
        try
        {
            while (FileCacheSize > targetSize && files.Count > 0)
            {
                token.ThrowIfCancellationRequested();

                var oldestFile = files[0];

                long sizeToSubtract;
                try
                {
                    sizeToSubtract = _fileCompactor.GetFileSizeOnDisk(oldestFile, StorageisNTFS);
                }
                catch
                {
                    try
                    {
                        sizeToSubtract = oldestFile.Length;
                    }
                    catch
                    {
                        sizeToSubtract = 0;
                    }
                }

                if (sizeToSubtract < 0 || sizeToSubtract > MaxSingleFileBytes)
                {
                    try
                    {
                        sizeToSubtract = oldestFile.Length;
                    }
                    catch
                    {
                        sizeToSubtract = 0;
                    }
                }

                if (sizeToSubtract < 0) sizeToSubtract = 0;

                if (TryDeleteCacheFileWithRetries(oldestFile.FullName, token))
                {
                    FileCacheSize -= sizeToSubtract;
                    if (FileCacheSize < 0) FileCacheSize = 0;
                }
                else
                {
                    Logger.LogWarning(
                        "Failed to delete {file} while trimming cache after coordinated retries; it is likely still in use",
                        oldestFile.FullName);
                }

                files.RemoveAt(0);
            }
        }
        finally
        {
            ResumeScan(AutomaticTrimScanLock);
        }
    }


    private bool TryDeleteCacheFileWithRetries(string path, CancellationToken token)
    {
        for (var attempt = 0; attempt < AutomaticTrimDeleteRetryCount; attempt++)
        {
            token.ThrowIfCancellationRequested();

            try
            {
                if (!File.Exists(path))
                    return true;

                try
                {
                    var attributes = File.GetAttributes(path);
                    if ((attributes & FileAttributes.ReadOnly) != 0)
                    {
                        File.SetAttributes(path, attributes & ~FileAttributes.ReadOnly);
                    }
                }
                catch
                {
                    // best effort only
                }

                File.Delete(path);
                return !File.Exists(path);
            }
            catch (IOException) when (attempt < AutomaticTrimDeleteRetryCount - 1)
            {
            }
            catch (UnauthorizedAccessException) when (attempt < AutomaticTrimDeleteRetryCount - 1)
            {
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }

            if (attempt < AutomaticTrimDeleteRetryCount - 1)
            {
                try
                {
                    if (token.WaitHandle.WaitOne(AutomaticTrimDeleteRetryDelay))
                        throw new OperationCanceledException(token);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
            }
        }

        return !File.Exists(path);
    }


    public void ResetLocks()
    {
        HaltScanLocks.Clear();
    }

    public void ResumeScan(string source)
    {
        if (!HaltScanLocks.ContainsKey(source)) HaltScanLocks[source] = 0;

        HaltScanLocks[source]--;
        if (HaltScanLocks[source] < 0) HaltScanLocks[source] = 0;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _scanCancellationTokenSource?.Cancel();
        PenumbraWatcher?.Dispose();
        MareWatcher?.Dispose();
        _penumbraFswCts?.CancelDispose();
        _mareFswCts?.CancelDispose();
        lock (_csvFlushGate)
        {
            _csvFlushCts?.CancelDispose();
            _csvFlushCts = null;
        }
        _periodicCalculationTokenSource?.CancelDispose();
    }

    private void FullFileScan(CancellationToken ct)
    {
        TotalFiles = 1;
        var penumbraDir = _ipcManager.Penumbra.ModDirectory;
        bool penDirExists = true;
        bool cacheDirExists = true;

        if (string.IsNullOrEmpty(penumbraDir) || !Directory.Exists(penumbraDir))
        {
            penDirExists = false;
            Logger.LogWarning("Penumbra directory is not set or does not exist.");
        }

        if (string.IsNullOrEmpty(_configService.Current.CacheFolder) || !Directory.Exists(_configService.Current.CacheFolder))
        {
            cacheDirExists = false;
            Logger.LogWarning("RavaSync Cache directory is not set or does not exist.");
        }

        if (!penDirExists || !cacheDirExists)
            return;

        var previousThreadPriority = Thread.CurrentThread.Priority;
        Thread.CurrentThread.Priority = ThreadPriority.Lowest;

        Dictionary<string, bool> allScannedFiles = new(StringComparer.OrdinalIgnoreCase);
        List<FileCacheEntity> entitiesToRemove = [];
        List<FileCacheEntity> entitiesToUpdate = [];

        try
        {
            Logger.LogDebug("Lazily scanning files from {penumbra} and {storage}", penumbraDir, _configService.Current.CacheFolder);

            var discoveredFiles = 0;

            foreach (var folder in EnumerateTopLevelDirectoriesSafely(penumbraDir!, ct))
            {
                ct.ThrowIfCancellationRequested();

                foreach (var file in EnumerateFilesRecursiveSafely(folder, ct))
                {
                    ct.ThrowIfCancellationRequested();

                    if (!IsAllowedPenumbraScanPath(file))
                        continue;

                    if (allScannedFiles.TryAdd(file, false))
                    {
                        discoveredFiles++;

                        if (discoveredFiles % 500 == 0)
                        {
                            TotalFiles = discoveredFiles;

                            if (ct.WaitHandle.WaitOne(1))
                                return;

                            Thread.Yield();
                        }
                    }
                }

                // Give disk/CPU a tiny breather between mod folders.
                if (ct.WaitHandle.WaitOne(10))
                    return;
            }

            try
            {
                foreach (var cacheFile in Directory.EnumerateFiles(_configService.Current.CacheFolder, "*.*", SearchOption.TopDirectoryOnly))
                {
                    ct.ThrowIfCancellationRequested();

                    if (!IsCacheHashFileCandidate(cacheFile))
                        continue;

                    if (allScannedFiles.TryAdd(cacheFile, false))
                    {
                        discoveredFiles++;

                        if (discoveredFiles % 500 == 0)
                        {
                            TotalFiles = discoveredFiles;

                            if (ct.WaitHandle.WaitOne(1))
                                return;

                            Thread.Yield();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Could not enumerate RavaSync cache folder {path}", _configService.Current.CacheFolder);
            }

            if (ct.IsCancellationRequested)
                return;

            TotalFiles = allScannedFiles.Count;

            if (ct.WaitHandle.WaitOne(1))
                return;

            Thread.Yield();

            // Lower cap than before. Full scans should be gentle, not a CPU/disk bar fight.
            var threadCount = Math.Clamp(Math.Max(1, Environment.ProcessorCount / 4), 1, 4);

            object sync = new();
            Thread[] workerThreads = new Thread[threadCount];

            ConcurrentQueue<FileCacheEntity> fileCaches = new(_fileDbManager.GetAllFileCaches());

            TotalFilesStorage = fileCaches.Count;

            for (int i = 0; i < threadCount; i++)
            {
                Logger.LogTrace("Creating Thread {i}", i);
                workerThreads[i] = new((tcounter) =>
                {
                    var threadNr = (int)tcounter!;
                    Logger.LogTrace("Spawning Worker Thread {i}", threadNr);

                    while (!ct.IsCancellationRequested && fileCaches.TryDequeue(out var workload))
                    {
                        try
                        {
                            if (ct.IsCancellationRequested)
                                return;

                            if (!_ipcManager.Penumbra.APIAvailable)
                            {
                                Logger.LogWarning("Penumbra not available");
                                return;
                            }

                            var validatedCacheResult = _fileDbManager.ValidateFileCacheEntity(workload);
                            if (validatedCacheResult.State != FileState.RequireDeletion)
                            {
                                lock (sync)
                                {
                                    allScannedFiles[validatedCacheResult.FileCache.ResolvedFilepath] = true;
                                }
                            }

                            if (validatedCacheResult.State == FileState.RequireUpdate)
                            {
                                Logger.LogTrace("To update: {path}", validatedCacheResult.FileCache.ResolvedFilepath);
                                lock (sync)
                                {
                                    entitiesToUpdate.Add(validatedCacheResult.FileCache);
                                }
                            }
                            else if (validatedCacheResult.State == FileState.RequireDeletion)
                            {
                                Logger.LogTrace("To delete: {path}", validatedCacheResult.FileCache.ResolvedFilepath);
                                lock (sync)
                                {
                                    entitiesToRemove.Add(validatedCacheResult.FileCache);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "Failed validating {path}", workload.ResolvedFilepath);
                        }

                        Interlocked.Increment(ref _currentFileProgress);
                    }

                    Logger.LogTrace("Ending Worker Thread {i}", threadNr);
                })
                {
                    Priority = ThreadPriority.Lowest,
                    IsBackground = true
                };

                workerThreads[i].Start(i);
            }

            while (!ct.IsCancellationRequested && workerThreads.Any(u => u.IsAlive))
            {
                if (ct.WaitHandle.WaitOne(250))
                    return;
            }

            if (ct.IsCancellationRequested)
                return;

            Logger.LogTrace("Threads exited");

            if (!_ipcManager.Penumbra.APIAvailable)
            {
                Logger.LogWarning("Penumbra not available");
                return;
            }

            if (entitiesToUpdate.Any() || entitiesToRemove.Any())
            {
                foreach (var entity in entitiesToUpdate)
                {
                    ct.ThrowIfCancellationRequested();
                    _fileDbManager.UpdateHashedFile(entity);
                }

                foreach (var entity in entitiesToRemove)
                {
                    ct.ThrowIfCancellationRequested();
                    _fileDbManager.RemoveHashedFile(entity.Hash, entity.PrefixedFilePath);
                }

                _fileDbManager.WriteOutFullCsv();
            }

            Logger.LogTrace("Scanner validated existing db files");

            if (!_ipcManager.Penumbra.APIAvailable)
            {
                Logger.LogWarning("Penumbra not available");
                return;
            }

            if (ct.IsCancellationRequested)
                return;

            var newFiles = allScannedFiles
                .Where(c => !c.Value)
                .Select(c => c.Key)
                .ToArray();

            if (newFiles.Length > 0)
            {
                Parallel.ForEach(newFiles,
                    new ParallelOptions()
                    {
                        MaxDegreeOfParallelism = threadCount,
                        CancellationToken = ct
                    },
                    (cachePath) =>
                    {
                        if (ct.IsCancellationRequested)
                            return;

                        if (!_ipcManager.Penumbra.APIAvailable)
                        {
                            Logger.LogWarning("Penumbra not available");
                            return;
                        }

                        try
                        {
                            var entry = _fileDbManager.CreateFileEntry(cachePath);
                            if (entry == null)
                                _ = _fileDbManager.CreateCacheEntry(cachePath);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "Failed adding {file}", cachePath);
                        }

                        Interlocked.Increment(ref _currentFileProgress);
                    });

                Logger.LogTrace("Scanner added {notScanned} new files to db", newFiles.Length);
            }

            Logger.LogDebug("Scan complete");
            TotalFiles = 0;
            _currentFileProgress = 0;
            entitiesToRemove.Clear();
            allScannedFiles.Clear();

            if (!_configService.Current.InitialScanComplete)
            {
                _configService.Current.InitialScanComplete = true;
                _configService.Save();
                StartMareWatcher(_configService.Current.CacheFolder);
                StartPenumbraWatcher(penumbraDir);
            }
        }
        finally
        {
            Thread.CurrentThread.Priority = previousThreadPriority;
        }
    }

    private IEnumerable<string> EnumerateTopLevelDirectoriesSafely(string root, CancellationToken ct)
    {
        string[] directories;

        try
        {
            directories = Directory.GetDirectories(root, "*", SearchOption.TopDirectoryOnly);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Could not enumerate top-level directories in {path}", root);
            yield break;
        }

        foreach (var directory in directories)
        {
            ct.ThrowIfCancellationRequested();
            yield return directory;
        }
    }

    private IEnumerable<string> EnumerateFilesRecursiveSafely(string root, CancellationToken ct)
    {
        Stack<string> pendingDirectories = new();
        pendingDirectories.Push(root);

        while (pendingDirectories.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            var currentDirectory = pendingDirectories.Pop();

            string[] files;
            try
            {
                files = Directory.GetFiles(currentDirectory, "*.*", SearchOption.TopDirectoryOnly);
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Could not enumerate files in {path}", currentDirectory);
                files = [];
            }

            foreach (var file in files)
            {
                ct.ThrowIfCancellationRequested();
                yield return file;
            }

            string[] childDirectories;
            try
            {
                childDirectories = Directory.GetDirectories(currentDirectory, "*", SearchOption.TopDirectoryOnly);
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Could not enumerate child directories in {path}", currentDirectory);
                continue;
            }

            foreach (var childDirectory in childDirectories)
            {
                ct.ThrowIfCancellationRequested();
                pendingDirectories.Push(childDirectory);
            }
        }
    }

    private static bool IsAllowedPenumbraScanPath(string file)
    {
        return AllowedFileExtensions.Any(e => file.EndsWith(e, StringComparison.OrdinalIgnoreCase))
            && !file.Contains(@"\bg\", StringComparison.OrdinalIgnoreCase)
            && !file.Contains(@"\bgcommon\", StringComparison.OrdinalIgnoreCase)
            && !file.Contains(@"\ui\", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsCacheHashFileCandidate(string file)
    {
        var fileName = Path.GetFileName(file);
        if (string.IsNullOrWhiteSpace(fileName))
            return false;

        if (fileName.Length == 40)
            return true;

        return (fileName.Split('.').FirstOrDefault()?.Length ?? 0) == 40;
    }
}