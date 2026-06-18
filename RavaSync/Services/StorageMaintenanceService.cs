using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;

namespace RavaSync.Services;

public sealed class StorageMaintenanceService
{
    private const string ClearCacheScanLock = "SettingsUi.ClearLocalStorage";

    private readonly ILogger<StorageMaintenanceService> _logger;
    private readonly MareMediator _mediator;
    private readonly MareConfigService _configService;
    private readonly CacheMonitor _cacheMonitor;
    private readonly FileCacheManager _fileCacheManager;
    private volatile StorageValidationProgressSnapshot _validationProgress = StorageValidationProgressSnapshot.Idle;
    private volatile StorageClearCacheProgressSnapshot _clearCacheProgress = StorageClearCacheProgressSnapshot.Idle;

    public StorageValidationProgressSnapshot ValidationProgress => _validationProgress;
    public StorageClearCacheProgressSnapshot ClearCacheProgress => _clearCacheProgress;

    public StorageMaintenanceService(
        ILogger<StorageMaintenanceService> logger,
        MareMediator mediator,
        MareConfigService configService,
        CacheMonitor cacheMonitor,
        FileCacheManager fileCacheManager)
    {
        _logger = logger;
        _mediator = mediator;
        _configService = configService;
        _cacheMonitor = cacheMonitor;
        _fileCacheManager = fileCacheManager;
    }

    public Task<List<FileCacheEntity>> ValidateLocalStorageAsync(CancellationToken cancellationToken = default)
        => ValidateLocalStorageAsync(null, cancellationToken);

    public async Task<List<FileCacheEntity>> ValidateLocalStorageAsync(IProgress<(int, int, FileCacheEntity)>? externalProgress, CancellationToken cancellationToken = default)
    {
        _validationProgress = new StorageValidationProgressSnapshot(true, 0, 0, string.Empty, 0);

        var progress = new DelegatingProgress<(int Current, int Total, FileCacheEntity Entity)>(value =>
        {
            var currentFile = value.Entity?.ResolvedFilepath ?? string.Empty;
            var currentFileName = Path.GetFileName(currentFile);
            if (string.IsNullOrWhiteSpace(currentFileName))
                currentFileName = currentFile;

            _validationProgress = new StorageValidationProgressSnapshot(
                true,
                Math.Max(0, value.Current),
                Math.Max(0, value.Total),
                NormalizeProgressDisplayText(currentFileName),
                _validationProgress.RemovedFiles);

            externalProgress?.Report((value.Current, value.Total, value.Entity));
        });

        try
        {
            var broken = await Task.Run(() => _fileCacheManager.ValidateLocalIntegrity(progress, cancellationToken), cancellationToken).ConfigureAwait(false);
            _validationProgress = new StorageValidationProgressSnapshot(false, _validationProgress.TotalFiles, _validationProgress.TotalFiles, string.Empty, broken.Count);
            return broken;
        }
        catch
        {
            _validationProgress = StorageValidationProgressSnapshot.Idle;
            throw;
        }
    }

    public async Task ClearLocalStorageAsync(CancellationToken cancellationToken = default)
    {
        _clearCacheProgress = new StorageClearCacheProgressSnapshot(true, 0, 0, string.Empty, 0);

        var cacheFolder = _configService.Current.CacheFolder;
        if (string.IsNullOrWhiteSpace(cacheFolder) || !Directory.Exists(cacheFolder))
        {
            _clearCacheProgress = StorageClearCacheProgressSnapshot.Idle;
            return;
        }

        _mediator.Publish(new HaltScanMessage(ClearCacheScanLock));

        var deletedFiles = 0;
        try
        {
            var files = Directory.GetFiles(cacheFolder);
            _clearCacheProgress = new StorageClearCacheProgressSnapshot(true, 0, files.Length, string.Empty, 0);

            for (var i = 0; i < files.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var file = files[i];
                var currentFileName = Path.GetFileName(file);
                if (string.IsNullOrWhiteSpace(currentFileName))
                    currentFileName = file;

                _clearCacheProgress = new StorageClearCacheProgressSnapshot(true, i, files.Length, NormalizeProgressDisplayText(currentFileName), deletedFiles);
                File.Delete(file);
                deletedFiles++;
                _clearCacheProgress = new StorageClearCacheProgressSnapshot(true, i, files.Length, NormalizeProgressDisplayText(currentFileName), deletedFiles);
            }

            _clearCacheProgress = new StorageClearCacheProgressSnapshot(false, files.Length, files.Length, string.Empty, deletedFiles);
        }
        catch
        {
            _clearCacheProgress = StorageClearCacheProgressSnapshot.Idle;
            throw;
        }
        finally
        {
            _mediator.Publish(new ResumeScanMessage(ClearCacheScanLock));
            _cacheMonitor.InvokeScan();
        }

        _logger.LogInformation("Cleared local storage in {cacheFolder}", cacheFolder);
        await Task.CompletedTask;
    }

    private static string NormalizeProgressDisplayText(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.Replace('\\', '/').Trim();
        return normalized.Length <= 96 ? normalized : "…" + normalized[^95..];
    }

    private sealed class DelegatingProgress<T> : IProgress<T>
    {
        private readonly Action<T> _handler;

        public DelegatingProgress(Action<T> handler)
        {
            _handler = handler;
        }

        public void Report(T value) => _handler(value);
    }

    public sealed record class StorageValidationProgressSnapshot(bool IsRunning, int CurrentFile, int TotalFiles, string CurrentFileName, int RemovedFiles)
    {
        public static StorageValidationProgressSnapshot Idle { get; } = new(false, 0, 0, string.Empty, 0);
        public float Progress => TotalFiles <= 0 ? 0f : Math.Clamp((CurrentFile + 1) / (float)TotalFiles, 0f, 1f);
    }

    public sealed record class StorageClearCacheProgressSnapshot(bool IsRunning, int CurrentFile, int TotalFiles, string CurrentFileName, int DeletedFiles)
    {
        public static StorageClearCacheProgressSnapshot Idle { get; } = new(false, 0, 0, string.Empty, 0);
        public float Progress => TotalFiles <= 0 ? 0f : Math.Clamp((CurrentFile + 1) / (float)TotalFiles, 0f, 1f);
    }
}
