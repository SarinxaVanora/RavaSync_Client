using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;
using RavaSync.MareConfiguration.Models;

namespace RavaSync.MareConfiguration;

public class ConfigurationMigrator(ILogger<ConfigurationMigrator> logger,TransientConfigService transientConfigService,
    ServerConfigService serverConfigService,MareConfigService mareConfigService,MareMediator mediator) : IHostedService

{
    private readonly ILogger<ConfigurationMigrator> _logger = logger;

    public void Migrate()
    {
        MigrateCacheFolderToSubdir(mareConfigService, mediator);

        if (mareConfigService.Current.Version < 3)
        {
            _logger.LogInformation("Migrating Rava Config V{old} => V3", mareConfigService.Current.Version);

            mareConfigService.Current.ParallelDownloads = 0;
            mareConfigService.Current.ParallelUploads = 0;
            mareConfigService.Current.ShowTransferBars = false;
            mareConfigService.Current.Version = 3;
            mareConfigService.Save();
        }

        if (transientConfigService.Current.Version == 0)
        {
            _logger.LogInformation("Migrating Transient Config V0 => V1");
            transientConfigService.Current.TransientConfigs.Clear();
            transientConfigService.Current.Version = 1;
            transientConfigService.Save();
        }

        if (serverConfigService.Current.Version == 1)
        {
            _logger.LogInformation("Migrating Server Config V1 => V2");
            var centralServer = serverConfigService.Current.ServerStorage.Find(f =>
                f.ServerName.Equals("Ravalyn's Domain", StringComparison.Ordinal));
            if (centralServer != null)
            {
                centralServer.ServerName = ApiController.MainServer;
            }
            serverConfigService.Current.Version = 2;
            serverConfigService.Save();
        }
    }


    private const string CacheSubdirName = "RavaFiles";

    private void MigrateCacheFolderToSubdir(MareConfigService cfgSvc, MareMediator mediator)
    {
        try
        {
            var cfg = cfgSvc.Current;
            if (cfg.CacheFolderSubdirMigrationDone) return;

            var cacheFolder = (cfg.CacheFolder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(cacheFolder))
            {
                cfg.CacheFolderSubdirMigrationDone = true;
                cfgSvc.Save();
                return;
            }

            cacheFolder = Path.GetFullPath(cacheFolder);
            cacheFolder = Path.TrimEndingDirectorySeparator(cacheFolder);

            // If already pointing at subdir, just mark done.
            if (string.Equals(Path.GetFileName(cacheFolder), CacheSubdirName, StringComparison.OrdinalIgnoreCase))
            {
                cfg.CacheFolder = cacheFolder;
                cfg.CacheFolderSubdirMigrationDone = true;
                cfgSvc.Save();
                return;
            }

            var target = Path.Combine(cacheFolder, CacheSubdirName);
            Directory.CreateDirectory(target);

            int moved = 0;

            if (Directory.Exists(cacheFolder))
            {
                foreach (var file in Directory.EnumerateFiles(cacheFolder, "*", SearchOption.TopDirectoryOnly))
                {
                    if (!LooksLikeSha1CacheFile(file)) continue;

                    var dest = Path.Combine(target, Path.GetFileName(file));
                    try
                    {
                        if (File.Exists(dest))
                        {
                            long srcLen = 0, dstLen = 0;
                            try { srcLen = new FileInfo(file).Length; } catch { }
                            try { dstLen = new FileInfo(dest).Length; } catch { }

                            if (dstLen <= 0 && srcLen > 0)
                            {
                                try { File.Delete(dest); } catch { }
                                File.Move(file, dest);
                                moved++;
                            }
                            else
                            {
                                try { File.Delete(file); } catch { }
                            }
                        }
                        else
                        {
                            File.Move(file, dest);
                            moved++;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Cache migration: failed to move {src} -> {dst}", file, dest);
                    }
                }
            }

            cfg.CacheFolder = target;
            cfg.CacheFolderSubdirMigrationDone = true;
            cfgSvc.Save();

            var msg = moved > 0
                ? $"RavaSync moved your cache into a dedicated folder to avoid conflicts.\nMoved {moved} file(s) to:\n{target}"
                : $"RavaSync now uses a dedicated cache subfolder to avoid conflicts:\n{target}";

            mediator.Publish(new NotificationMessage("RavaSync Cache Migration", msg, NotificationType.Info));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cache migration failed; leaving cache folder unchanged.");
        }
    }

    private static bool LooksLikeSha1CacheFile(string fullPath)
    {
        try
        {
            var name = Path.GetFileNameWithoutExtension(fullPath);
            if (string.IsNullOrEmpty(name) || name.Length != 40) return false;

            for (int i = 0; i < name.Length; i++)
            {
                var c = name[i];
                bool isHex = (c >= '0' && c <= '9') ||
                             (c >= 'a' && c <= 'f') ||
                             (c >= 'A' && c <= 'F');
                if (!isHex) return false;
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Migrate();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
