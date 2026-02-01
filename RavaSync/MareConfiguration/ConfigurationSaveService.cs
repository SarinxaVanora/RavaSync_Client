using RavaSync.MareConfiguration.Configurations;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RavaSync.MareConfiguration;

public class ConfigurationSaveService : IHostedService
{
    private readonly HashSet<object> _configsToSave = [];
    private readonly ILogger<ConfigurationSaveService> _logger;
    private readonly SemaphoreSlim _configSaveSemaphore = new(1, 1);
    private readonly CancellationTokenSource _configSaveCheckCts = new();
    public const string BackupFolder = "config_backup";
    private readonly MethodInfo _saveMethod;

    public ConfigurationSaveService(ILogger<ConfigurationSaveService> logger, IEnumerable<IConfigService<IMareConfiguration>> configs)
    {
        foreach (var config in configs)
        {
            config.ConfigSave += OnConfigurationSave;
        }
        _logger = logger;
#pragma warning disable S3011 // Reflection should not be used to increase accessibility of classes, methods, or fields
        _saveMethod = GetType().GetMethod(nameof(SaveConfig), BindingFlags.Instance | BindingFlags.NonPublic)!;
#pragma warning restore S3011 // Reflection should not be used to increase accessibility of classes, methods, or fields
    }

    private void OnConfigurationSave(object? sender, EventArgs e)
    {
        _configSaveSemaphore.Wait();
        _configsToSave.Add(sender!);
        _configSaveSemaphore.Release();
    }

    private async Task PeriodicSaveCheck(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await SaveConfigs().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during SaveConfigs");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
        }
    }

    private async Task SaveConfigs()
    {
        if (_configsToSave.Count == 0) return;

        await _configSaveSemaphore.WaitAsync().ConfigureAwait(false);
        var configList = _configsToSave.ToList();
        _configsToSave.Clear();
        _configSaveSemaphore.Release();

        foreach (var config in configList)
        {
            var expectedType = config.GetType().BaseType!.GetGenericArguments()[0];
            var save = _saveMethod.MakeGenericMethod(expectedType);
            await ((Task)save.Invoke(this, [config])!).ConfigureAwait(false);
        }
    }

    private async Task SaveConfig<T>(IConfigService<T> config) where T : IMareConfiguration
    {
        _logger.LogTrace("Saving {configName}", config.ConfigurationName);

        var configDir = Path.GetDirectoryName(config.ConfigurationPath);
        if (string.IsNullOrWhiteSpace(configDir))
            configDir = config.ConfigurationPath.Replace(config.ConfigurationName, string.Empty);

        try
        {
            if (!string.IsNullOrWhiteSpace(configDir) && !Directory.Exists(configDir))
                Directory.CreateDirectory(configDir);

            if (File.Exists(config.ConfigurationPath))
            {
                var configBackupFolder = Path.Join(configDir, BackupFolder);
                if (!Directory.Exists(configBackupFolder))
                    Directory.CreateDirectory(configBackupFolder);

                var configNameSplit = config.ConfigurationName.Split(".");
                var existingConfigs = Directory.EnumerateFiles(configBackupFolder, configNameSplit[0] + "*")
                    .Select(c => new FileInfo(c))
                    .OrderByDescending(c => c.LastWriteTimeUtc)
                    .ToList();

                if (existingConfigs.Skip(10).Any())
                {
                    foreach (var oldBak in existingConfigs.Skip(10).ToList())
                        oldBak.Delete();
                }

                string backupPath = Path.Combine(
                    configBackupFolder,
                    configNameSplit[0] + "." + DateTime.Now.ToString("yyyyMMddHHmmss") + "." + configNameSplit[1]);

                _logger.LogTrace("Backing up current config to {backupPath}", backupPath);
                File.Copy(config.ConfigurationPath, backupPath, overwrite: true);

                FileInfo fi = new(backupPath);
                fi.LastWriteTimeUtc = DateTime.UtcNow;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not create backup for {config}", config.ConfigurationPath);
        }

        var temp = config.ConfigurationPath + ".tmp";
        try
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
            options.Converters.Add(new SafeFloatConverter());
            options.Converters.Add(new SafeDoubleConverter());

            var json = JsonSerializer.Serialize(config.Current, typeof(T), options);

            await File.WriteAllTextAsync(temp, json).ConfigureAwait(false);
            File.Move(temp, config.ConfigurationPath, true);

            config.UpdateLastWriteTime();
        }
        catch (Exception ex)
        {
            try
            {
                if (File.Exists(temp))
                    File.Delete(temp);
            }
            catch
            {
            }

            _logger.LogWarning(ex, "Error during config save of {config}", config.ConfigurationName);
        }
    }



    sealed class SafeFloatConverter : JsonConverter<float>
    {
        public override float Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.GetSingle();

        public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
        {
            if (float.IsNaN(value) || float.IsInfinity(value)) writer.WriteNumberValue(0f);
            else writer.WriteNumberValue(value);
        }
    }

    sealed class SafeDoubleConverter : JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.GetDouble();

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            if (double.IsNaN(value) || double.IsInfinity(value)) writer.WriteNumberValue(0d);
            else writer.WriteNumberValue(value);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = Task.Run(() => PeriodicSaveCheck(_configSaveCheckCts.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _configSaveCheckCts.CancelAsync().ConfigureAwait(false);
        _configSaveCheckCts.Dispose();

        await SaveConfigs().ConfigureAwait(false);
    }
}
