using Dalamud.Plugin;
using Dalamud.Plugin.Ipc;
using Microsoft.Extensions.Logging;
using RavaSync.Services.Mediator;
using System.Text.Json;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerOtherSync : IIpcCaller
{
    private readonly ILogger<IpcCallerOtherSync> _logger;
    private readonly IDalamudPluginInterface _pi;

    private readonly ICallGateSubscriber<List<nint>> _lightlessGetHandledAddresses;
    private readonly ICallGateSubscriber<List<nint>> _snowcloakGetHandledAddresses;
    private readonly ICallGateSubscriber<List<nint>> _ravaGetHandledAddresses;

    private readonly object _cacheGate = new();
    private HashSet<nint> _cached = new();
    private long _lastRefreshTick;

    private long _lastApiCheckTick;
    private const int ApiCheckIntervalMs = 3000;

    private long _lastLightlessConfigProbeTick;
    private const int LightlessConfigProbeIntervalMs = 1000;
    private string? _lightlessConfigDir;
    private bool? _lightlessFullPauseCached;

    private long _lastSnowcloakConfigProbeTick;
    private const int SnowcloakConfigProbeIntervalMs = 1000;
    private string? _snowcloakConfigDir;
    private bool? _snowcloakFullPauseCached;

    private bool _lightlessAvailable;
    private bool _snowcloakAvailable;
    private bool _ravaAvailable;

    public IpcCallerOtherSync(ILogger<IpcCallerOtherSync> logger, IDalamudPluginInterface pi, MareMediator mediator)
    {
        _logger = logger;
        _pi = pi;

        _lightlessGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("LightlessSync.GetHandledAddresses");
        _snowcloakGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("Snowcloak.GetHandledAddresses");
        _ravaGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("RavaSync.GetHandledAddresses");

        CheckAPI();
    }

    public bool APIAvailable { get; private set; }

    public void CheckAPI()
    {
        _lightlessAvailable = false;
        _snowcloakAvailable = false;
        _ravaAvailable = false;

        try { _ = _lightlessGetHandledAddresses.InvokeFunc(); _lightlessAvailable = true; } catch { _lightlessAvailable = false; }
        try { _ = _snowcloakGetHandledAddresses.InvokeFunc(); _snowcloakAvailable = true; } catch { _snowcloakAvailable = false; }
        try { _ = _ravaGetHandledAddresses.InvokeFunc(); _ravaAvailable = true; } catch { _ravaAvailable = false; }

        APIAvailable = _lightlessAvailable || _snowcloakAvailable;
    }

    private void EnsureApiAvailable()
    {
        if (APIAvailable) return;

        var now = Environment.TickCount64;

        lock (_cacheGate)
        {
            if ((now - _lastApiCheckTick) < ApiCheckIntervalMs)
                return;

            _lastApiCheckTick = now;
        }

        CheckAPI();
    }

    public bool IsAddressHandledByOtherSync(nint address)
    {
        if (address == nint.Zero) return false;

        EnsureApiAvailable();
        if (!APIAvailable) return false;

        RefreshCacheIfNeeded();

        lock (_cacheGate)
        {
            return _cached.Contains(address);
        }
    }

    public bool TryGetOwningOtherSync(nint address, out string owner)
    {
        owner = string.Empty;

        if (address == nint.Zero) return false;

        EnsureApiAvailable();
        if (!APIAvailable) return false;

        RefreshCacheIfNeeded();

        if (_lightlessAvailable)
        {
            try
            {
                var list = _lightlessGetHandledAddresses.InvokeFunc();
                if (list != null && list.Contains(address))
                {
                    var llPause = TryGetLightlessFullPause(onlyIfListNonEmpty: true, listCountHint: list.Count);

                    if (llPause != false)
                        return false;

                    owner = "Lightless";
                    return true;
                }
            }
            catch
            {
                _lightlessAvailable = false;
                APIAvailable = _lightlessAvailable || _snowcloakAvailable;
            }
        }

        if (_snowcloakAvailable)
        {
            try
            {
                var list = _snowcloakGetHandledAddresses.InvokeFunc();
                if (list != null && list.Contains(address))
                {
                    var scPause = TryGetSnowcloakFullPause(onlyIfListNonEmpty: true, listCountHint: list.Count);

                    if (scPause != false)
                        return false;

                    owner = "Snowcloak";
                    return true;
                }
            }
            catch
            {
                _snowcloakAvailable = false;
                APIAvailable = _lightlessAvailable || _snowcloakAvailable;
            }
        }

        lock (_cacheGate)
        {
            if (_cached.Contains(address))
            {
                owner = "Other";
                return true;
            }
        }

        return false;
    }

    private void RefreshCacheIfNeeded()
    {
        var now = Environment.TickCount64;

        lock (_cacheGate)
        {
            if ((now - _lastRefreshTick) < 1000)
                return;

            _lastRefreshTick = now;
        }

        EnsureApiAvailable();
        if (!APIAvailable)
        {
            lock (_cacheGate)
                _cached = new HashSet<nint>();

            return;
        }

        var set = new HashSet<nint>();

        List<nint>? lightlessList = null;
        List<nint>? snowcloakList = null;
        List<nint>? ravaList = null;

        if (_lightlessAvailable)
        {
            try
            {
                lightlessList = _lightlessGetHandledAddresses.InvokeFunc();

                if (lightlessList != null && lightlessList.Count > 0)
                {
                    var llPause = TryGetLightlessFullPause(onlyIfListNonEmpty: true, listCountHint: lightlessList.Count);

                    if (llPause == false)
                    {
                        foreach (var a in lightlessList)
                            if (a != nint.Zero) set.Add(a);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query LightlessSync.GetHandledAddresses");
                _lightlessAvailable = false;
                APIAvailable = _lightlessAvailable || _snowcloakAvailable;
            }
        }

        if (_snowcloakAvailable)
        {
            try
            {
                snowcloakList = _snowcloakGetHandledAddresses.InvokeFunc();

                if (snowcloakList != null && snowcloakList.Count > 0)
                {
                    var scPause = TryGetSnowcloakFullPause(onlyIfListNonEmpty: true, listCountHint: snowcloakList.Count);

                    if (scPause == false)
                    {
                        foreach (var a in snowcloakList)
                            if (a != nint.Zero) set.Add(a);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query Snowcloak.GetHandledAddresses");
                _snowcloakAvailable = false;
                APIAvailable = _lightlessAvailable || _snowcloakAvailable;
            }
        }

        if (_ravaAvailable)
        {
            try
            {
                ravaList = _ravaGetHandledAddresses.InvokeFunc();
                if (ravaList != null)
                {
                    foreach (var a in ravaList)
                        if (a != nint.Zero) set.Remove(a);
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query RavaSync.GetHandledAddresses (self-guard)");
            }
        }

        lock (_cacheGate)
        {
            _cached = set;
        }
    }

    private bool? TryGetLightlessFullPause(bool onlyIfListNonEmpty, int listCountHint)
    {
        if (!_lightlessAvailable) return null;
        if (onlyIfListNonEmpty && listCountHint <= 0) return _lightlessFullPauseCached;

        var now = Environment.TickCount64;
        if ((now - _lastLightlessConfigProbeTick) < LightlessConfigProbeIntervalMs)
            return _lightlessFullPauseCached;

        _lastLightlessConfigProbeTick = now;

        try
        {
            _lightlessConfigDir ??= LocatePluginConfigDirByServerJsonHint(
                preferredNames: new[] { "LightlessSync", "LightlessClient", "Lightless-Sync", "Lightless" });

            if (string.IsNullOrWhiteSpace(_lightlessConfigDir))
                return _lightlessFullPauseCached = null;

            var serverPath = Path.Combine(_lightlessConfigDir, "server.json");
            if (!File.Exists(serverPath))
                return _lightlessFullPauseCached = null;

            return _lightlessFullPauseCached = TryParseFullPause(serverPath);
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Failed reading Lightless server.json for FullPause heuristic");
            return _lightlessFullPauseCached = null;
        }
    }


    private bool? TryGetSnowcloakFullPause(bool onlyIfListNonEmpty, int listCountHint)
    {
        if (!_snowcloakAvailable) return null;
        if (onlyIfListNonEmpty && listCountHint <= 0) return _snowcloakFullPauseCached;

        var now = Environment.TickCount64;
        if ((now - _lastSnowcloakConfigProbeTick) < SnowcloakConfigProbeIntervalMs)
            return _snowcloakFullPauseCached;

        _lastSnowcloakConfigProbeTick = now;

        try
        {
            _snowcloakConfigDir ??= LocatePluginConfigDirByServerJsonHint(
                preferredNames: new[] { "Snowcloak", "SnowcloakSync", "SnowcloakClient" });

            if (string.IsNullOrWhiteSpace(_snowcloakConfigDir))
                return _snowcloakFullPauseCached = null;

            var serverPath = Path.Combine(_snowcloakConfigDir, "server.json");
            if (!File.Exists(serverPath))
                return _snowcloakFullPauseCached = null;

            return _snowcloakFullPauseCached = TryParseFullPause(serverPath);
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Failed reading Snowcloak server.json for FullPause heuristic");
            return _snowcloakFullPauseCached = null;
        }
    }

    private bool? TryParseFullPause(string serverJsonPath)
    {
        var json = File.ReadAllText(serverJsonPath);

        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return null;

        if (doc.RootElement.TryGetProperty("ServerStorage", out var storage)
            && storage.ValueKind == JsonValueKind.Array
            && storage.GetArrayLength() > 0)
        {
            var first = storage[0];
            if (first.ValueKind == JsonValueKind.Object
                && first.TryGetProperty("FullPause", out var fp)
                && (fp.ValueKind == JsonValueKind.True || fp.ValueKind == JsonValueKind.False))
            {
                return fp.GetBoolean();
            }
        }

        return null;
    }

    private string? LocatePluginConfigDirByServerJsonHint(string[] preferredNames)
    {
        try
        {
            var ravaDir = _pi.GetPluginConfigDirectory();
            if (string.IsNullOrWhiteSpace(ravaDir))
                return null;

            var pluginConfigsDir = Directory.GetParent(ravaDir)?.FullName;
            if (string.IsNullOrWhiteSpace(pluginConfigsDir) || !Directory.Exists(pluginConfigsDir))
                return null;

            foreach (var name in preferredNames)
            {
                var p = Path.Combine(pluginConfigsDir, name);
                if (Directory.Exists(p) && File.Exists(Path.Combine(p, "server.json")))
                    return p;
            }

            foreach (var dir in Directory.GetDirectories(pluginConfigsDir))
            {
                var serverPath = Path.Combine(dir, "server.json");
                if (!File.Exists(serverPath)) continue;

                try
                {
                    var txt = File.ReadAllText(serverPath);
                    if (txt.Contains("\"FullPause\"", StringComparison.OrdinalIgnoreCase)
                        && txt.Contains("\"ServerUri\"", StringComparison.OrdinalIgnoreCase))
                        return dir;
                }
                catch
                {
                    // ignore
                }
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    public void Dispose()
    {
        // no-op (no subscriptions)
    }
}