using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files;

namespace RavaSync.Services;

public sealed class CrashRecoveryService : IHostedService, IDisposable
{
    private const string SentinelFileName = "_ravasync_session.lock";

    private readonly ILogger<CrashRecoveryService> _logger;
    private readonly MareConfigService _cfg;
    private readonly MareMediator _mediator;
    private readonly DelayedActivatorService _delayedActivator;

    private string _sentinelPath = string.Empty;
    private int _sentinelWritten = 0;

    public CrashRecoveryService(
        ILogger<CrashRecoveryService> logger,
        MareConfigService cfg,
        MareMediator mediator,
        DelayedActivatorService delayedActivator)
    {
        _logger = logger;
        _cfg = cfg;
        _mediator = mediator;
        _delayedActivator = delayedActivator;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _sentinelPath = Path.Combine(_cfg.ConfigurationDirectory, SentinelFileName);

        var uncleanShutdownDetected = File.Exists(_sentinelPath);

        TryWriteSentinel();

        if (uncleanShutdownDetected)
        {
            var cacheFolder = _cfg.Current.CacheFolder;
            var quarantineFolder = _delayedActivator.QuarantineRoot;

            var (tinyDeleted, tinyFailed) = CleanupTinyFiles(16, cacheFolder, quarantineFolder);
            var (tmpDeleted, tmpFailed) = CleanupTempArtifacts(cacheFolder, quarantineFolder);

            _mediator.Publish(new NotificationMessage(
                "RavaSync Recovery",
                $"Previous session ended unexpectedly. Removed {tinyDeleted} tiny file(s) and {tmpDeleted} temp file(s){((tinyFailed + tmpFailed) > 0 ? $" (failed: {tinyFailed + tmpFailed})" : string.Empty)}.",
                NotificationType.Warning,
                TimeSpan.FromSeconds(8)));
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        TryDeleteSentinel();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        TryDeleteSentinel();
        GC.SuppressFinalize(this);
    }

    private void TryWriteSentinel()
    {
        if (Interlocked.Exchange(ref _sentinelWritten, 1) == 1) return;

        try
        {
            Directory.CreateDirectory(_cfg.ConfigurationDirectory);
            File.WriteAllText(_sentinelPath, $"UTC={DateTime.UtcNow:O}\nPID={Environment.ProcessId}\n");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to write crash recovery sentinel at {path}", _sentinelPath);
        }
    }

    private void TryDeleteSentinel()
    {
        if (string.IsNullOrWhiteSpace(_sentinelPath)) return;

        try
        {
            if (File.Exists(_sentinelPath))
                File.Delete(_sentinelPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete crash recovery sentinel at {path}", _sentinelPath);
        }
    }

    private (int Deleted, int Failed) CleanupTinyFiles(int minValidSizeBytes = 16, params string[] roots)
    {
        int deleted = 0;
        int failed = 0;

        foreach (var root in roots)
        {
            if (string.IsNullOrWhiteSpace(root)) continue;
            if (!Directory.Exists(root)) continue;

            try
            {
                foreach (var path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
                {
                    long length;
                    try
                    {
                        length = new FileInfo(path).Length;
                    }
                    catch
                    {
                        continue;
                    }

                    if (length >= minValidSizeBytes) continue;

                    try
                    {
                        File.Delete(path);
                        deleted++;
                    }
                    catch
                    {
                        failed++;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed while scanning for tiny files in {root}", root);
            }
        }

        if (deleted > 0 || failed > 0)
            _logger.LogWarning("Crash recovery cleanup: deleted {deleted} tiny file(s) (<{min} bytes), failed {failed}", deleted, minValidSizeBytes, failed);

        return (deleted, failed);
    }

    private (int Deleted, int Failed) CleanupTempArtifacts(params string[] roots)
    {
        int deleted = 0;
        int failed = 0;

        foreach (var root in roots)
        {
            if (string.IsNullOrWhiteSpace(root)) continue;
            if (!Directory.Exists(root)) continue;

            try
            {
                foreach (var path in Directory.EnumerateFiles(root, "*.tmp", SearchOption.AllDirectories))
                {
                    try
                    {
                        File.Delete(path);
                        deleted++;
                    }
                    catch
                    {
                        failed++;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed while scanning for temp artifacts in {root}", root);
            }
        }

        if (deleted > 0 || failed > 0)
            _logger.LogWarning("Crash recovery cleanup: deleted {deleted} temp artifact(s) (*.tmp), failed {failed}", deleted, failed);

        return (deleted, failed);
    }
}