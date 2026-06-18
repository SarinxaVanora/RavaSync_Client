using Dalamud.Interface;
using Dalamud.Plugin;
using Microsoft.Extensions.Logging;
using RavaSync.Utils;
using System.Reflection;

namespace RavaSync.Services;

public sealed class PluginUpdateGate
{
    private readonly IDalamudPluginInterface _pluginInterface;
    private readonly ILogger<PluginUpdateGate> _logger;
    private readonly SemaphoreSlim _checkGate = new(1, 1);
    private bool _checked;
    private object? _update;
    private Version? _reportedUpdateVersion;


    public PluginUpdateGate(IDalamudPluginInterface pluginInterface, ILogger<PluginUpdateGate> logger)
    {
        _pluginInterface = pluginInterface;
        _logger = logger;
    }

    public bool HasBlockingUpdate => _update != null;
    public string PluginSearchText => string.IsNullOrWhiteSpace(_pluginInterface.InternalName) ? "RavaSync" : _pluginInterface.InternalName;

    public string CurrentVersionText => PluginVersion.CurrentText;
    public string ReportedUpdateVersionText => PluginVersion.Format(_reportedUpdateVersion);

    public async Task<bool> CheckForUpdateAsync(CancellationToken cancellationToken = default, bool forceRefresh = false)
    {
        if (!forceRefresh && _checked)
            return HasBlockingUpdate;

        await _checkGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!forceRefresh && _checked)
                return HasBlockingUpdate;

            try
            {
                _update = await _pluginInterface.CheckForUpdateAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
                _reportedUpdateVersion = TryGetUpdateVersion(_update);

                if (_update != null && IsReportedUpdateAlreadyInstalled(_reportedUpdateVersion))
                {
                    _logger.LogInformation("Dalamud reported a RavaSync update, but the loaded version {CurrentVersion} is already at or above the reported update version {UpdateVersion}. Continuing normal startup.",
                        PluginVersion.CurrentText, PluginVersion.Format(_reportedUpdateVersion));
                    _update = null;
                }

                _checked = true;

                if (_update != null)
                    _logger.LogWarning("RavaSync update detected. Blocking normal startup until the user updates through Dalamud. Loaded={CurrentVersion}, Available={UpdateVersion}",
                        PluginVersion.CurrentText, PluginVersion.Format(_reportedUpdateVersion));
                else
                    _logger.LogDebug("No blocking RavaSync update detected by Dalamud. Loaded={CurrentVersion}, Reported={UpdateVersion}",
                        PluginVersion.CurrentText, PluginVersion.Format(_reportedUpdateVersion));

                return HasBlockingUpdate;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _checked = true;
                _update = null;
                _reportedUpdateVersion = null;
                _logger.LogWarning(ex, "Could not check whether RavaSync has a Dalamud update. Continuing normal startup.");
                return false;
            }
        }
        finally
        {
            _checkGate.Release();
        }
    }

    public void InvalidateCachedResult()
    {
        _checked = false;
    }

    public bool OpenUpdater()
    {
        var searchText = PluginSearchText;

        try
        {
            InvalidateCachedResult();

            if (_pluginInterface.OpenPluginInstallerTo(PluginInstallerOpenKind.UpdateablePlugins, searchText))
                return true;

            if (_pluginInterface.OpenPluginInstallerTo(PluginInstallerOpenKind.InstalledPlugins, searchText))
                return true;

            return _pluginInterface.OpenPluginInstallerTo(PluginInstallerOpenKind.AllPlugins, searchText);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to open Dalamud plugin installer for RavaSync update.");
            return false;
        }
    }

    private static bool IsReportedUpdateAlreadyInstalled(Version? reportedUpdateVersion)
    {
        return reportedUpdateVersion != null && PluginVersion.Current.CompareTo(reportedUpdateVersion) >= 0;
    }

    private static Version? TryGetUpdateVersion(object? update)
    {
        if (update == null)
            return null;

        var updateType = update.GetType();
        foreach (var propertyName in new[]
        {
            "AssemblyVersion",
            "TestingAssemblyVersion",
            "Version",
            "PluginVersion",
            "ManifestVersion"
        })
        {
            var property = updateType.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
            if (property == null || !property.CanRead)
                continue;

            try
            {
                if (TryCoerceVersion(property.GetValue(update), out var version))
                    return version;
            }
            catch
            {
                // Best-effort only. Dalamud has changed these manifest/update shapes before.
            }
        }

        return null;
    }

    private static bool TryCoerceVersion(object? value, out Version version)
    {
        version = new Version(0, 0, 0, 0);

        if (value is Version directVersion)
        {
            version = directVersion;
            return true;
        }

        if (value is string text)
            return TryParseVersionPrefix(text, out version);

        return value != null && TryParseVersionPrefix(value.ToString(), out version);
    }

    private static bool TryParseVersionPrefix(string? value, out Version version)
    {
        version = new Version(0, 0, 0, 0);

        if (string.IsNullOrWhiteSpace(value))
            return false;

        var end = 0;
        while (end < value.Length && (char.IsDigit(value[end]) || value[end] == '.'))
            end++;

        if (end == 0)
            return false;

        return Version.TryParse(value[..end].TrimEnd('.'), out version);
    }
}
