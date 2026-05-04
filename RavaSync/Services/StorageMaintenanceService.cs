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

    public async Task<List<FileCacheEntity>> ValidateLocalStorageAsync(CancellationToken cancellationToken = default)
    {
        var progress = new Progress<(int, int, FileCacheEntity)>(_ => { });
        return await Task.Run(() => _fileCacheManager.ValidateLocalIntegrity(progress, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    public async Task ClearLocalStorageAsync(CancellationToken cancellationToken = default)
    {
        var cacheFolder = _configService.Current.CacheFolder;
        if (string.IsNullOrWhiteSpace(cacheFolder) || !Directory.Exists(cacheFolder))
            return;

        _mediator.Publish(new HaltScanMessage(ClearCacheScanLock));

        try
        {
            foreach (var file in Directory.GetFiles(cacheFolder))
            {
                cancellationToken.ThrowIfCancellationRequested();
                File.Delete(file);
            }
        }
        finally
        {
            _mediator.Publish(new ResumeScanMessage(ClearCacheScanLock));
            _cacheMonitor.InvokeScan();
        }

        _logger.LogInformation("Cleared local storage in {cacheFolder}", cacheFolder);
        await Task.CompletedTask;
    }
}
