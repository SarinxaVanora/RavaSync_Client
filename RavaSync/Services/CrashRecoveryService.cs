using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files;
using RavaSync.Utils;

namespace RavaSync.Services;

public sealed class CrashRecoveryService : IHostedService, IDisposable
{
    private const string SentinelFileName = "_ravasync_session.lock";

    private readonly ILogger<CrashRecoveryService> _logger;
    private readonly MareConfigService _cfg;
    private readonly MareMediator _mediator;

    private string _sentinelPath = string.Empty;
    private int _sentinelWritten = 0;

    public CrashRecoveryService(ILogger<CrashRecoveryService> logger,MareConfigService cfg,MareMediator mediator)
    {
        _logger = logger;
        _cfg = cfg;
        _mediator = mediator;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _sentinelPath = Path.Combine(_cfg.ConfigurationDirectory, SentinelFileName);

        var uncleanShutdownDetected = File.Exists(_sentinelPath);

        TryWriteSentinel();

        if (uncleanShutdownDetected)
        {
            var cacheFolder = _cfg.Current.CacheFolder;

            var (tinyDeleted, tinyFailed) = CleanupTinyFiles(16, cacheFolder);
            var (tmpDeleted, tmpFailed) = CleanupTempArtifacts(cacheFolder);
            var (todayScanned, todayDeleted, todayFailed) = CleanupInvalidFilesDownloadedToday(cacheFolder);

            _mediator.Publish(new NotificationMessage(
                "RavaSync Recovery",
                $"Previous session ended unexpectedly. Removed {tinyDeleted} tiny file(s), {tmpDeleted} temp file(s), and {todayDeleted} invalid file(s) from today's downloads after scanning {todayScanned} file(s){((tinyFailed + tmpFailed + todayFailed) > 0 ? $" (failed: {tinyFailed + tmpFailed + todayFailed})" : string.Empty)}.",
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

    private (int Scanned, int Deleted, int Failed) CleanupInvalidFilesDownloadedToday(params string[] roots)
    {
        int scanned = 0;
        int deleted = 0;
        int failed = 0;
        var today = DateTime.Now.Date;

        foreach (var root in roots)
        {
            if (string.IsNullOrWhiteSpace(root)) continue;
            if (!Directory.Exists(root)) continue;

            try
            {
                foreach (var path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
                {
                    FileInfo fileInfo;
                    try
                    {
                        fileInfo = new FileInfo(path);
                    }
                    catch
                    {
                        continue;
                    }

                    if (!WasTouchedToday(fileInfo, today))
                        continue;

                    if (string.Equals(fileInfo.Extension, ".tmp", StringComparison.OrdinalIgnoreCase))
                        continue;

                    scanned++;

                    try
                    {
                        if (fileInfo.Length < 16)
                        {
                            File.Delete(path);
                            deleted++;
                            continue;
                        }

                        var expectedHash = Path.GetFileNameWithoutExtension(fileInfo.Name);
                        if (!LooksLikeSha1(expectedHash))
                            continue;

                        var computedHash = Crypto.GetFileHash(path);
                        if (!string.Equals(expectedHash, computedHash, StringComparison.OrdinalIgnoreCase))
                        {
                            File.Delete(path);
                            deleted++;
                        }
                    }
                    catch
                    {
                        failed++;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed while scanning today's downloads in {root}", root);
            }
        }

        if (scanned > 0 || deleted > 0 || failed > 0)
            _logger.LogWarning("Crash recovery cleanup: scanned {scanned} file(s) downloaded today, deleted {deleted} invalid file(s), failed {failed}", scanned, deleted, failed);

        return (scanned, deleted, failed);
    }

    private static bool WasTouchedToday(FileInfo fileInfo, DateTime localToday)
    {
        return fileInfo.CreationTime.Date == localToday || fileInfo.LastWriteTime.Date == localToday;
    }

    private static bool LooksLikeSha1(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length != 40)
            return false;

        foreach (var c in value)
        {
            var isHex = (c >= '0' && c <= '9')
                || (c >= 'a' && c <= 'f')
                || (c >= 'A' && c <= 'F');

            if (!isHex)
                return false;
        }

        return true;
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