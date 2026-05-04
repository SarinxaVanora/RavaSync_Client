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
    private readonly ICallGateSubscriber<List<nint>> _playerSyncGetHandledAddresses;

    private readonly object _cacheGate = new();
    private HashSet<nint> _cached = new();
    private Dictionary<nint, string> _cachedOwners = new();
    private HashSet<nint> _trackedVisibleAddresses = new();

    private volatile bool _apiAvailable;
    private volatile bool _disposed;
    private volatile bool _hasBuiltCache;

    private long _lastRefreshTick;
    private long _lastApiCheckTick;
    private long _lastRefreshRequestTick;
    private int _refreshQueued;
    private int _apiCheckQueued;
    private int _refreshRequestedWhileQueued;

    private const int ApiCheckIntervalMs = 3000;
    private const int SlowRefreshIntervalMs = 1000;
    private const int FastRefreshIntervalMs = 250;

    private long _lastLightlessConfigProbeTick;
    private const int LightlessConfigProbeIntervalMs = 5000;
    private string? _lightlessConfigDir;
    private bool? _lightlessFullPauseCached;
    private string? _lightlessServerJsonPath;
    private DateTime _lightlessServerJsonLastWriteUtc = DateTime.MinValue;

    private long _lastSnowcloakConfigProbeTick;
    private const int SnowcloakConfigProbeIntervalMs = 5000;
    private string? _snowcloakConfigDir;
    private bool? _snowcloakFullPauseCached;
    private string? _snowcloakServerJsonPath;
    private DateTime _snowcloakServerJsonLastWriteUtc = DateTime.MinValue;

    private long _lastPlayerSyncConfigProbeTick;
    private const int PlayerSyncConfigProbeIntervalMs = 5000;
    private string? _playerSyncConfigDir;
    private bool? _playerSyncFullPauseCached;
    private string? _playerSyncServerJsonPath;
    private DateTime _playerSyncServerJsonLastWriteUtc = DateTime.MinValue;

    private bool _lightlessEnabled;
    private bool _snowcloakEnabled;
    private bool _playerSyncEnabled;

    private bool _lightlessAvailable;
    private bool _snowcloakAvailable;
    private bool _playerSyncAvailable;

    public IpcCallerOtherSync(ILogger<IpcCallerOtherSync> logger, IDalamudPluginInterface pi, MareMediator mediator)
    {
        _logger = logger;
        _pi = pi;

        _lightlessGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("LightlessSync.GetHandledAddresses");
        _snowcloakGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("Snowcloak.GetHandledAddresses");
        _playerSyncGetHandledAddresses = pi.GetIpcSubscriber<List<nint>>("PlayerSync.GetHandledAddresses");

        CheckAPI();
        RefreshCacheCore();
    }

    public bool APIAvailable => _apiAvailable;

    private bool HasAnyEnabledService => _lightlessEnabled || _snowcloakEnabled || _playerSyncEnabled;

    public void UpdateTrackedVisibleAddresses(IEnumerable<nint>? addresses)
    {
        if (_disposed)
            return;

        var next = new HashSet<nint>();
        if (addresses != null)
        {
            foreach (var address in addresses)
            {
                if (address != nint.Zero)
                    next.Add(address);
            }
        }

        var shouldForceRefresh = false;

        lock (_cacheGate)
        {
            if (_trackedVisibleAddresses.SetEquals(next))
                return;

            shouldForceRefresh = next.Count > 0 && !_trackedVisibleAddresses.IsSupersetOf(next);
            _trackedVisibleAddresses = next;

            if (_trackedVisibleAddresses.Count == 0)
            {
                _cached = new HashSet<nint>();
                _cachedOwners = new Dictionary<nint, string>();
                _lastRefreshTick = Environment.TickCount64;
                _hasBuiltCache = true;
                return;
            }

            if (_cached.Count == 0 && _cachedOwners.Count == 0)
            {
                if (shouldForceRefresh)
                    _lastRefreshTick = 0;

                return;
            }

            _cached.RemoveWhere(address => !_trackedVisibleAddresses.Contains(address));

            if (_cachedOwners.Count > 0)
            {
                var filteredOwners = new Dictionary<nint, string>(_cachedOwners.Count);
                foreach (var kvp in _cachedOwners)
                {
                    if (_trackedVisibleAddresses.Contains(kvp.Key))
                        filteredOwners[kvp.Key] = kvp.Value;
                }

                _cachedOwners = filteredOwners;
            }

            if (shouldForceRefresh)
                _lastRefreshTick = 0;
            else
                _lastRefreshTick = Environment.TickCount64;
        }

        if (shouldForceRefresh)
        {
            EnsureApiAvailable();
            if (APIAvailable)
                QueueCacheRefresh(force: true);
        }
    }

    public void CheckAPI()
    {
        _lightlessEnabled = IsServiceEnabledByConfig(ServiceKind.Lightless);
        _snowcloakEnabled = IsServiceEnabledByConfig(ServiceKind.Snowcloak);
        _playerSyncEnabled = IsServiceEnabledByConfig(ServiceKind.PlayerSync);

        //_logger.LogInformation("Service States. Light: {0} Snow: {1} PS: {2}", _lightlessAvailable, _snowcloakEnabled, _playerSyncEnabled);

        _lightlessAvailable = false;
        _snowcloakAvailable = false;
        _playerSyncAvailable = false;

        if (_lightlessEnabled)
        {
            try { _ = _lightlessGetHandledAddresses.InvokeFunc(); _lightlessAvailable = true; } catch { _lightlessAvailable = false; }
        }

        if (_snowcloakEnabled)
        {
            try { _ = _snowcloakGetHandledAddresses.InvokeFunc(); _snowcloakAvailable = true; } catch { _snowcloakAvailable = false; }
        }

        if (_playerSyncEnabled)
        {
            try { _ = _playerSyncGetHandledAddresses.InvokeFunc(); _playerSyncAvailable = true; } catch { _playerSyncAvailable = false; }
        }

        _apiAvailable = (_lightlessEnabled && _lightlessAvailable)
            || (_snowcloakEnabled && _snowcloakAvailable)
            || (_playerSyncEnabled && _playerSyncAvailable);

        if (!HasAnyEnabledService || !_apiAvailable)
            ClearCachedOwnership();
    }

    private void EnsureApiAvailable()
    {
        if (_disposed || APIAvailable || !HasAnyEnabledService)
            return;

        var now = Environment.TickCount64;

        lock (_cacheGate)
        {
            if ((now - _lastApiCheckTick) < ApiCheckIntervalMs)
                return;

            _lastApiCheckTick = now;
        }

        QueueApiCheck();
    }

    public bool IsAddressHandledByOtherSync(nint address, bool preferFresh = false)
    {
        if (address == nint.Zero)
            return false;

        EnsureApiAvailable();
        RequestCacheRefresh(preferFresh);

        lock (_cacheGate)
        {
            return _cached.Contains(address);
        }
    }

    public bool IsOwnerAvailable(string? owner)
    {
        EnsureApiAvailable();
        return IsOwnerEnabled(owner);
    }

    public bool IsOwnerEnabled(string? owner)
    {
        var normalizedOwner = NormalizeOwner(owner);
        if (string.IsNullOrEmpty(normalizedOwner))
            return false;

        if (string.Equals(normalizedOwner, "RavaSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalizedOwner, "OtherSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalizedOwner, "Other", StringComparison.OrdinalIgnoreCase))
            return false;

        return normalizedOwner switch
        {
            "Lightless" => _lightlessEnabled && _lightlessAvailable,
            "Snowcloak" => _snowcloakEnabled && _snowcloakAvailable,
            "PlayerSync" => _playerSyncEnabled && _playerSyncAvailable,
            _ => false,
        };
    }

    public bool IsOwnerHandlingAddress(string? owner, nint address, bool preferFresh = false)
    {
        if (address == nint.Zero)
            return false;

        var normalizedOwner = NormalizeOwner(owner);
        if (string.IsNullOrEmpty(normalizedOwner) || !IsOwnerEnabled(normalizedOwner))
            return false;

        if (preferFresh)
        {
            return TryGetOwningOtherSyncDirect(address, out var directOwner)
                && string.Equals(NormalizeOwner(directOwner), normalizedOwner, StringComparison.OrdinalIgnoreCase);
        }

        EnsureApiAvailable();
        RequestCacheRefresh(preferFresh);

        lock (_cacheGate)
        {
            return _cachedOwners.TryGetValue(address, out var cachedOwner)
                && string.Equals(NormalizeOwner(cachedOwner), normalizedOwner, StringComparison.OrdinalIgnoreCase);
        }
    }

    public bool TryGetOwningOtherSync(nint address, out string owner, bool preferFresh = false)
    {
        owner = string.Empty;

        if (address == nint.Zero)
            return false;

        if (preferFresh)
            return TryGetOwningOtherSyncDirect(address, out owner);

        EnsureApiAvailable();
        RequestCacheRefresh(preferFresh);

        lock (_cacheGate)
        {
            if (_cachedOwners.TryGetValue(address, out owner))
                return true;

            if (_cached.Contains(address))
            {
                owner = "Other";
                return true;
            }
        }

        owner = string.Empty;
        return false;
    }

    private void RequestCacheRefresh(bool preferFresh = false)
    {
        if (_disposed)
            return;

        if (!APIAvailable)
            return;

        var now = Environment.TickCount64;
        var refreshIntervalMs = preferFresh ? FastRefreshIntervalMs : SlowRefreshIntervalMs;

        if (_hasBuiltCache && (now - Interlocked.Read(ref _lastRefreshTick)) < refreshIntervalMs)
            return;

        lock (_cacheGate)
        {
            if ((now - _lastRefreshRequestTick) < refreshIntervalMs)
                return;

            _lastRefreshRequestTick = now;
        }

        QueueCacheRefresh();
    }

    private void QueueApiCheck()
    {
        if (_disposed)
            return;

        if (Interlocked.Exchange(ref _apiCheckQueued, 1) != 0)
            return;

        _ = Task.Run(() =>
        {
            try
            {
                if (_disposed)
                    return;

                CheckAPI();
                if (APIAvailable)
                    QueueCacheRefresh(force: true);
                else
                    ClearCachedOwnership();
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed queued OtherSync API check");
            }
            finally
            {
                Interlocked.Exchange(ref _apiCheckQueued, 0);
            }
        });
    }

    private void QueueCacheRefresh(bool force = false)
    {
        if (_disposed)
            return;

        if (force)
            Interlocked.Exchange(ref _refreshRequestedWhileQueued, 1);

        if (Interlocked.CompareExchange(ref _refreshQueued, 1, 0) != 0)
            return;

        _ = Task.Run(() =>
        {
            try
            {
                if (_disposed)
                    return;

                do
                {
                    Interlocked.Exchange(ref _refreshRequestedWhileQueued, 0);
                    RefreshCacheCore();
                }
                while (!_disposed && APIAvailable && Interlocked.CompareExchange(ref _refreshRequestedWhileQueued, 0, 0) != 0);
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed queued OtherSync cache refresh");
            }
            finally
            {
                Interlocked.Exchange(ref _refreshQueued, 0);

                if (!_disposed && APIAvailable && Interlocked.Exchange(ref _refreshRequestedWhileQueued, 0) != 0)
                    QueueCacheRefresh(force: true);
            }
        });
    }

    private void RefreshCacheCore()
    {
        if (_disposed)
            return;

        if (!APIAvailable)
        {
            ClearCachedOwnership();
            return;
        }

        HashSet<nint> trackedVisibleAddresses;
        lock (_cacheGate)
        {
            trackedVisibleAddresses = _trackedVisibleAddresses.Count > 0
                ? new HashSet<nint>(_trackedVisibleAddresses)
                : new HashSet<nint>();
        }

        if (trackedVisibleAddresses.Count == 0)
        {
            ClearCachedOwnership();
            return;
        }

        var set = new HashSet<nint>();
        var owners = new Dictionary<nint, string>();

        RefreshOwnerCacheForService(
            enabled: _lightlessEnabled,
            available: ref _lightlessAvailable,
            getHandledAddresses: _lightlessGetHandledAddresses,
            trackedVisibleAddresses: trackedVisibleAddresses,
            set: set,
            owners: owners,
            ownerName: "Lightless",
            failureMessage: "Failed to query LightlessSync.GetHandledAddresses");

        RefreshOwnerCacheForService(
            enabled: _snowcloakEnabled,
            available: ref _snowcloakAvailable,
            getHandledAddresses: _snowcloakGetHandledAddresses,
            trackedVisibleAddresses: trackedVisibleAddresses,
            set: set,
            owners: owners,
            ownerName: "Snowcloak",
            failureMessage: "Failed to query Snowcloak.GetHandledAddresses");

        RefreshOwnerCacheForService(
            enabled: _playerSyncEnabled,
            available: ref _playerSyncAvailable,
            getHandledAddresses: _playerSyncGetHandledAddresses,
            trackedVisibleAddresses: trackedVisibleAddresses,
            set: set,
            owners: owners,
            ownerName: "PlayerSync",
            failureMessage: "Failed to query PlayerSync.GetHandledAddresses");

        _apiAvailable = (_lightlessEnabled && _lightlessAvailable)
            || (_snowcloakEnabled && _snowcloakAvailable)
            || (_playerSyncEnabled && _playerSyncAvailable);

        lock (_cacheGate)
        {
            _cached = set;
            _cachedOwners = owners;
            _lastRefreshTick = Environment.TickCount64;
            _hasBuiltCache = true;
        }
    }

    private void RefreshOwnerCacheForService(bool enabled,ref bool available,ICallGateSubscriber<List<nint>> getHandledAddresses,HashSet<nint> trackedVisibleAddresses,HashSet<nint> set,Dictionary<nint, string> owners,string ownerName,string failureMessage)
    {
        if (!enabled || !available)
            return;

        try
        {
            var handledAddresses = getHandledAddresses.InvokeFunc();
            if (handledAddresses == null || handledAddresses.Count == 0)
                return;

            foreach (var address in handledAddresses)
            {
                if (address == nint.Zero)
                    continue;

                if (!trackedVisibleAddresses.Contains(address))
                    continue;

                set.Add(address);
                owners[address] = ownerName;
            }
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, failureMessage);
            available = false;
        }
    }

    private void ClearCachedOwnership()
    {
        lock (_cacheGate)
        {
            _cached = new HashSet<nint>();
            _cachedOwners = new Dictionary<nint, string>();
            _lastRefreshTick = Environment.TickCount64;
            _hasBuiltCache = true;
        }
    }

    private bool TryGetOwningOtherSyncDirect(nint address, out string owner)
    {
        owner = string.Empty;

        if (address == nint.Zero)
            return false;

        if (_lightlessEnabled && _lightlessAvailable)
        {
            try
            {
                var handledAddresses = _lightlessGetHandledAddresses.InvokeFunc();
                if (handledAddresses != null && handledAddresses.Contains(address))
                {
                    owner = "Lightless";

                    lock (_cacheGate)
                    {
                        _cached.Add(address);
                        _cachedOwners[address] = owner;
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query LightlessSync.GetHandledAddresses");
                _lightlessAvailable = false;
            }
        }

        if (_snowcloakEnabled && _snowcloakAvailable)
        {
            try
            {
                var handledAddresses = _snowcloakGetHandledAddresses.InvokeFunc();
                if (handledAddresses != null && handledAddresses.Contains(address))
                {
                    owner = "Snowcloak";

                    lock (_cacheGate)
                    {
                        _cached.Add(address);
                        _cachedOwners[address] = owner;
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query Snowcloak.GetHandledAddresses");
                _snowcloakAvailable = false;
            }
        }

        if (_playerSyncEnabled && _playerSyncAvailable)
        {
            try
            {
                var handledAddresses = _playerSyncGetHandledAddresses.InvokeFunc();
                if (handledAddresses != null && handledAddresses.Contains(address))
                {
                    owner = "PlayerSync";

                    lock (_cacheGate)
                    {
                        _cached.Add(address);
                        _cachedOwners[address] = owner;
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogTrace(ex, "Failed to query PlayerSync.GetHandledAddresses");
                _playerSyncAvailable = false;
            }
        }

        _apiAvailable = (_lightlessEnabled && _lightlessAvailable)
            || (_snowcloakEnabled && _snowcloakAvailable)
            || (_playerSyncEnabled && _playerSyncAvailable);

        lock (_cacheGate)
        {
            _cached.Remove(address);
            _cachedOwners.Remove(address);
        }

        return false;
    }

    private bool IsServiceEnabledByConfig(ServiceKind kind)
        => GetFullPause(kind) == false;

    private bool? GetFullPause(ServiceKind kind)
        => kind switch
        {
            ServiceKind.Lightless => TryRefreshFullPauseCache(
                preferredNames: ["LightlessSync", "LightlessClient", "Lightless-Sync", "Lightless"],
                ref _lightlessConfigDir,
                ref _lightlessServerJsonPath,
                ref _lightlessFullPauseCached,
                ref _lightlessServerJsonLastWriteUtc,
                ref _lastLightlessConfigProbeTick,
                LightlessConfigProbeIntervalMs),
            ServiceKind.Snowcloak => TryRefreshFullPauseCache(
                preferredNames: ["Snowcloak", "SnowcloakSync", "SnowcloakClient"],
                ref _snowcloakConfigDir,
                ref _snowcloakServerJsonPath,
                ref _snowcloakFullPauseCached,
                ref _snowcloakServerJsonLastWriteUtc,
                ref _lastSnowcloakConfigProbeTick,
                SnowcloakConfigProbeIntervalMs),
            ServiceKind.PlayerSync => TryRefreshFullPauseCache(
                preferredNames: ["MareSempiterne", "PlayerSync"],
                ref _playerSyncConfigDir,
                ref _playerSyncServerJsonPath,
                ref _playerSyncFullPauseCached,
                ref _playerSyncServerJsonLastWriteUtc,
                ref _lastPlayerSyncConfigProbeTick,
                PlayerSyncConfigProbeIntervalMs),
            _ => null,
        };

    private static string NormalizeOwner(string? owner)
    {
        owner = owner?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(owner))
            return string.Empty;

        if (string.Equals(owner, "LightlessSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(owner, "LightlessClient", StringComparison.OrdinalIgnoreCase)
            || string.Equals(owner, "Lightless-Sync", StringComparison.OrdinalIgnoreCase))
            return "Lightless";

        if (string.Equals(owner, "SnowcloakSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(owner, "SnowcloakClient", StringComparison.OrdinalIgnoreCase))
            return "Snowcloak";

        if (string.Equals(owner, "MareSynchronos", StringComparison.OrdinalIgnoreCase)
            || string.Equals(owner, "MareSempiterne", StringComparison.OrdinalIgnoreCase))
            return "PlayerSync";

        return owner;
    }

    private bool? TryRefreshFullPauseCache(string[] preferredNames,ref string? configDir,ref string? serverJsonPath,ref bool? cachedValue,ref DateTime lastWriteUtc,ref long lastProbeTick,int probeIntervalMs)
    {
        var now = Environment.TickCount64;
        if ((now - lastProbeTick) < probeIntervalMs)
            return cachedValue;

        lastProbeTick = now;

        try
        {
            configDir ??= LocatePluginConfigDir(preferredNames);
            if (string.IsNullOrWhiteSpace(configDir))
            {
                serverJsonPath = null;
                lastWriteUtc = DateTime.MinValue;
                return cachedValue = null;
            }

            serverJsonPath ??= Path.Combine(configDir, "server.json");
            if (!File.Exists(serverJsonPath))
            {
                serverJsonPath = null;
                lastWriteUtc = DateTime.MinValue;
                return cachedValue = null;
            }

            var fileLastWriteUtc = File.GetLastWriteTimeUtc(serverJsonPath);
            if (cachedValue.HasValue && fileLastWriteUtc == lastWriteUtc)
                return cachedValue;

            lastWriteUtc = fileLastWriteUtc;
            return cachedValue = TryParseFullPause(serverJsonPath);
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Failed reading {ServerJsonPath} for FullPause state", serverJsonPath ?? "server.json");
            return cachedValue = null;
        }
    }

    private bool? TryParseFullPause(string serverJsonPath)
    {
        var json = File.ReadAllText(serverJsonPath);

        using var doc = JsonDocument.Parse(json);
        return TryFindFullPause(doc.RootElement);
    }

    private static bool? TryFindFullPause(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (string.Equals(property.Name, "FullPause", StringComparison.OrdinalIgnoreCase)
                        && (property.Value.ValueKind == JsonValueKind.True || property.Value.ValueKind == JsonValueKind.False))
                    {
                        return property.Value.GetBoolean();
                    }

                    var nested = TryFindFullPause(property.Value);
                    if (nested.HasValue)
                        return nested;
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    var nested = TryFindFullPause(item);
                    if (nested.HasValue)
                        return nested;
                }
                break;
        }

        return null;
    }

    private string? LocatePluginConfigDir(string[] preferredNames)
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
                var candidate = Path.Combine(pluginConfigsDir, name);
                if (Directory.Exists(candidate) && File.Exists(Path.Combine(candidate, "server.json")))
                    return candidate;
            }

            foreach (var dir in Directory.GetDirectories(pluginConfigsDir))
            {
                var dirName = Path.GetFileName(dir);
                if (string.IsNullOrWhiteSpace(dirName))
                    continue;

                if (!File.Exists(Path.Combine(dir, "server.json")))
                    continue;

                foreach (var preferredName in preferredNames)
                {
                    if (dirName.Contains(preferredName, StringComparison.OrdinalIgnoreCase))
                        return dir;
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
        _disposed = true;
    }

    private enum ServiceKind
    {
        Lightless,
        Snowcloak,
        PlayerSync,
    }
}
