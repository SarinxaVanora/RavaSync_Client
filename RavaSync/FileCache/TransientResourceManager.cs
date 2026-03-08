using RavaSync.API.Data.Enum;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace RavaSync.FileCache;

public sealed class TransientResourceManager : DisposableMediatorSubscriberBase
{
    private readonly object _cacheAdditionLock = new();
    private readonly object _playerRelatedPointersLock = new();
    private readonly object _transientResourcesLock = new();
    private DateTime _nextHandledPathsClearUtc = DateTime.MinValue;
    private static readonly TimeSpan _handledPathsClearInterval = TimeSpan.FromMilliseconds(250);
    private const int _handledPathsMaxSizeBeforeClear = 4096;

    private readonly HashSet<string> _cachedHandledPaths = new(StringComparer.Ordinal);
    private readonly TransientConfigService _configurationService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly string[] _handledFileTypes = ["tmb", "pap", "avfx", "atex", "sklb", "eid", "phyb", "scd", "skp", "shpk"];
    private readonly string[] _handledRecordingFileTypes = ["tex", "mdl", "mtrl"];
    private readonly HashSet<GameObjectHandler> _playerRelatedPointers = [];
    private Dictionary<nint, ObjectKind> _cachedFrameAddresses = new();
    private ConcurrentDictionary<ObjectKind, HashSet<string>>? _semiTransientResources = null;
    private uint _lastClassJobId = uint.MaxValue;
    public bool IsTransientRecording { get; private set; } = false;

    private readonly ConcurrentDictionary<ObjectKind, int> _transientSendScheduled = new();
    private readonly ConcurrentDictionary<ObjectKind, int> _transientDirty = new();
    private static readonly TimeSpan _transientSendDelay = TimeSpan.FromMilliseconds(750);
    private HashSet<string> _semiTransientAll = new(StringComparer.OrdinalIgnoreCase);
    private CancellationTokenSource _penumbraSettingsChangedDebounceCts = new();
    private readonly ConcurrentQueue<(string? EmoteKey, string TriggerPath, nint OwnerAddress, string? FilePrefix, string? Collection)> _autoRecordTriggerQueue = new();
    private volatile bool _inCombatOrPerformingSnapshot = false;
    private volatile string _playerPersistentDataKey = string.Empty;

    // -------------------- AUTO VFX EMOTE RECORDING --------------------
    private int _autoRecordRunning = 0;
    private bool _autoRecordWarnedDisabled = false;
    private DateTime _autoRecordCooldownUntilUtc = DateTime.MinValue;

    private static readonly TimeSpan _autoRecordDuration = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan _autoRecordCooldown = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan _autoRecordMinDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan _autoRecordIdleStop = TimeSpan.FromMilliseconds(1500);

    private nint _recordingOwnerAddress = nint.Zero;
    private string? _recordingFilePrefix = null;
    private string? _recordingCollection = null;

    private long _autoRecordLastActivityTicks = 0;

    private readonly object _autoRecordedKeyLock = new();
    private HashSet<string>? _autoRecordedEmoteKeysCache;

    private readonly FileCacheManager _fileCacheManager;
    private readonly ConcurrentDictionary<string, string> _transientHashByGamePath = new(StringComparer.OrdinalIgnoreCase);

    public bool HasPendingTransients(ObjectKind kind)
    {
        lock (_transientResourcesLock)
        {
            return TransientResources.TryGetValue(kind, out var set) && set.Count > 0;
        }
    }

    public HashSet<string> GetKnownTransientHashes(ObjectKind kind)
    {
        if (kind != ObjectKind.Player)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        return _transientHashByGamePath.Values
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public TransientResourceManager(ILogger<TransientResourceManager> logger, TransientConfigService configurationService,
            DalamudUtilService dalamudUtil, MareMediator mediator, FileCacheManager fileCacheManager) : base(logger, mediator)
    {
        _configurationService = configurationService;
        _dalamudUtil = dalamudUtil;
        _fileCacheManager = fileCacheManager;

        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, Manager_PenumbraResourceLoadEvent);
        Mediator.Subscribe<PenumbraModSettingChangedMessage>(this, (_) => Manager_PenumbraModSettingChanged());
        Mediator.Subscribe<PriorityFrameworkUpdateMessage>(this, (_) => DalamudUtil_FrameworkUpdate());
        Mediator.Subscribe<PrimeTransientPathsMessage>(this, (msg) => PrimeTransientPaths(msg.Address, msg.Kind, msg.GamePaths));
        Mediator.Subscribe<GameObjectHandlerCreatedMessage>(this, (msg) =>
        {
            if (!msg.OwnedObject) return;
            lock (_playerRelatedPointersLock)
            {
                _playerRelatedPointers.Add(msg.GameObjectHandler);
            }
        });
        Mediator.Subscribe<GameObjectHandlerDestroyedMessage>(this, (msg) =>
        {
            if (!msg.OwnedObject) return;
            lock (_playerRelatedPointersLock)
            {
                _playerRelatedPointers.Remove(msg.GameObjectHandler);
            }
        });
    }

    private TransientConfig.TransientPlayerConfig PlayerConfig
    {
        get
        {
            var key = PlayerPersistentDataKey;
            if (string.IsNullOrWhiteSpace(key))
            {
                return new TransientConfig.TransientPlayerConfig();
            }

            if (!_configurationService.Current.TransientConfigs.TryGetValue(key, out var transientConfig))
            {
                _configurationService.Current.TransientConfigs[key] = transientConfig = new();
            }
            return transientConfig;
        }
    }

    private string PlayerPersistentDataKey => _playerPersistentDataKey;
    private ConcurrentDictionary<ObjectKind, HashSet<string>> SemiTransientResources
    {
        get
        {
            lock (_transientResourcesLock)
            {
                if (_semiTransientResources == null)
                {
                    var next = new ConcurrentDictionary<ObjectKind, HashSet<string>>();

                    PlayerConfig.JobSpecificCache.TryGetValue(_dalamudUtil.ClassJobId, out var jobSpecificData);
                    next[ObjectKind.Player] = PlayerConfig.GlobalPersistentCache
                        .Concat(jobSpecificData ?? [])
                        .ToHashSet(StringComparer.Ordinal);

                    PlayerConfig.JobSpecificPetCache.TryGetValue(_dalamudUtil.ClassJobId, out var petSpecificData);
                    next[ObjectKind.Pet] = [.. petSpecificData ?? []];

                    _semiTransientResources = next;
                }

                return _semiTransientResources;
            }
        }
    }
    private ConcurrentDictionary<ObjectKind, HashSet<string>> TransientResources { get; } = new();

    public void CleanUpSemiTransientResources(ObjectKind objectKind, List<FileReplacement>? fileReplacement = null)
    {
        int removedPaths = 0;

        lock (_transientResourcesLock)
        {
            if (!SemiTransientResources.TryGetValue(objectKind, out HashSet<string>? value))
                return;

            if (fileReplacement == null)
            {
                value.Clear();
                return;
            }

            foreach (var replacement in fileReplacement.Where(p => !p.HasFileReplacement).SelectMany(p => p.GamePaths).ToList())
            {
                removedPaths += PlayerConfig.RemovePath(replacement, objectKind);
                value.Remove(replacement);
            }
        }

        if (removedPaths > 0)
        {
            Logger.LogTrace("Removed {amount} of SemiTransient paths during CleanUp, Saving from {name}", removedPaths, nameof(CleanUpSemiTransientResources));
            _configurationService.Save();
            RebuildSemiTransientAll();
        }
    }

    public HashSet<string> GetSemiTransientResources(ObjectKind objectKind)
    {
        lock (_transientResourcesLock)
        {
            if (SemiTransientResources.TryGetValue(objectKind, out var result) && result != null)
                return new HashSet<string>(result, StringComparer.Ordinal);

            return new HashSet<string>(StringComparer.Ordinal);
        }
    }

    public void PersistTransientResources(ObjectKind objectKind)
    {
        List<string> transientResources;
        List<string> newlyAddedGamePaths;

        lock (_transientResourcesLock)
        {
            if (!SemiTransientResources.TryGetValue(objectKind, out HashSet<string>? semiTransientResources))
            {
                SemiTransientResources[objectKind] = semiTransientResources = new(StringComparer.Ordinal);
            }

            if (!TransientResources.TryGetValue(objectKind, out var resources))
            {
                return;
            }

            transientResources = resources.ToList();
            Logger.LogDebug("Persisting {count} transient resources", transientResources.Count);

            newlyAddedGamePaths = resources
                .Except(semiTransientResources, StringComparer.Ordinal)
                .ToList();

            foreach (var gamePath in transientResources)
            {
                semiTransientResources.Add(gamePath);
            }

            resources.Clear();
        }

        bool saveConfig = false;

        if (objectKind == ObjectKind.Player && newlyAddedGamePaths.Count != 0)
        {
            saveConfig = true;
            foreach (var item in newlyAddedGamePaths.Where(f => !string.IsNullOrEmpty(f)))
            {
                PlayerConfig.AddOrElevate(_dalamudUtil.ClassJobId, item);
            }
        }
        else if (objectKind == ObjectKind.Pet && newlyAddedGamePaths.Count != 0)
        {
            saveConfig = true;

            if (!PlayerConfig.JobSpecificPetCache.TryGetValue(_dalamudUtil.ClassJobId, out var petPerma))
            {
                PlayerConfig.JobSpecificPetCache[_dalamudUtil.ClassJobId] = petPerma = [];
            }

            foreach (var item in newlyAddedGamePaths.Where(f => !string.IsNullOrEmpty(f)))
            {
                petPerma.Add(item);
            }
        }

        if (saveConfig)
        {
            Logger.LogTrace("Saving transient.json from {method}", nameof(PersistTransientResources));
            _configurationService.Save();
        }

        RebuildSemiTransientAll();
    }

    public void RemoveTransientResource(ObjectKind objectKind, string path)
    {
        bool removed = false;

        lock (_transientResourcesLock)
        {
            if (SemiTransientResources.TryGetValue(objectKind, out var resources))
            {
                removed = resources.RemoveWhere(f => string.Equals(path, f, StringComparison.Ordinal)) > 0;
            }
        }

        if (removed && objectKind == ObjectKind.Player)
        {
            PlayerConfig.RemovePath(path, objectKind);
            Logger.LogTrace("Saving transient.json from {method}", nameof(RemoveTransientResource));
            _configurationService.Save();
            RebuildSemiTransientAll();
        }
    }

    internal bool AddTransientResource(ObjectKind objectKind, string item)
    {
        lock (_transientResourcesLock)
        {
            if (SemiTransientResources.TryGetValue(objectKind, out var semiTransient)
                && semiTransient != null
                && semiTransient.Contains(item))
            {
                return false;
            }

            if (!TransientResources.TryGetValue(objectKind, out HashSet<string>? transientResource))
            {
                transientResource = new HashSet<string>(StringComparer.Ordinal);
                TransientResources[objectKind] = transientResource;
            }

            return transientResource.Add(item.ToLowerInvariant());
        }
    }

    private void PrimeTransientPaths(IntPtr actorAddress, ObjectKind kind, IReadOnlyCollection<string> gamePaths)
    {
        if (actorAddress == IntPtr.Zero) return;
        if (gamePaths == null || gamePaths.Count == 0) return;

        bool addedAny = false;

        lock (_transientResourcesLock)
        {
            if (!TransientResources.TryGetValue(kind, out var transientSet) || transientSet == null)
                TransientResources[kind] = transientSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var p in gamePaths)
            {
                var gp = NormalizePath(p);
                if (string.IsNullOrWhiteSpace(gp)) continue;

                if (!EndsWithAny(gp, _handledFileTypes)) continue;
                if (_semiTransientAll.Contains(gp)) continue;

                if (transientSet.Add(gp))
                    addedAny = true;
            }
        }

        if (addedAny)
            Mediator.Publish(new TransientResourceChangedMessage(actorAddress));
    }

    internal void ClearTransientPaths(ObjectKind objectKind, List<string> list)
    {
        int recordingOnlyRemoved = list.RemoveAll(entry =>
            _handledRecordingFileTypes.Any(ext => entry.EndsWith(ext, StringComparison.OrdinalIgnoreCase)));

        if (recordingOnlyRemoved > 0)
        {
            Logger.LogTrace("Ignored {0} game paths when clearing transients", recordingOnlyRemoved);
        }

        if (list.Count == 0)
            return;

        var removeSet = list.ToHashSet(StringComparer.OrdinalIgnoreCase);

        bool reloadSemiTransient = false;

        lock (_transientResourcesLock)
        {
            if (TransientResources.TryGetValue(objectKind, out var set))
            {
                foreach (var file in set.Where(removeSet.Contains))
                {
                    Logger.LogTrace("Removing From Transient: {file}", file);
                }

                int removed = set.RemoveWhere(removeSet.Contains);
                Logger.LogDebug("Removed {removed} previously existing transient paths", removed);
            }

            if (objectKind == ObjectKind.Player && SemiTransientResources.TryGetValue(objectKind, out var semiset))
            {
                foreach (var file in semiset.Where(removeSet.Contains))
                {
                    Logger.LogTrace("Removing From SemiTransient: {file}", file);
                    PlayerConfig.RemovePath(file, objectKind);
                }

                int removed = semiset.RemoveWhere(removeSet.Contains);
                Logger.LogDebug("Removed {removed} previously existing semi transient paths", removed);

                if (removed > 0)
                {
                    reloadSemiTransient = true;
                    Logger.LogTrace("Saving transient.json from {method}", nameof(ClearTransientPaths));
                    _configurationService.Save();
                }
            }

            if (reloadSemiTransient)
            {
                _semiTransientResources = null;
                _semiTransientAll = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        lock (_transientResourcesLock)
        {
            TransientResources.Clear();
            SemiTransientResources.Clear();
        }

        lock (_playerRelatedPointersLock)
        {
            _playerRelatedPointers.Clear();
        }
    }

    private void DalamudUtil_FrameworkUpdate()
    {
        _cachedFrameAddresses.Clear();

        var playerPointers = GetPlayerRelatedPointerSnapshot();

        foreach (var p in playerPointers)
        {
            if (p.Address != nint.Zero)
                _cachedFrameAddresses[p.Address] = p.ObjectKind;
        }

        _inCombatOrPerformingSnapshot = _dalamudUtil.IsInCombatOrPerforming;

        if (string.IsNullOrEmpty(_playerPersistentDataKey))
        {
            try
            {
                var name = _dalamudUtil.GetPlayerName();
                var world = _dalamudUtil.GetHomeWorldId();

                if (!string.IsNullOrEmpty(name) && world != 0)
                    _playerPersistentDataKey = name + "_" + world;
            }
            catch
            {
                // swallow; 
            }
        }

        var now = DateTime.UtcNow;
        if (now >= _nextHandledPathsClearUtc || _cachedHandledPaths.Count > _handledPathsMaxSizeBeforeClear)
        {
            _nextHandledPathsClearUtc = now.Add(_handledPathsClearInterval);

            lock (_cacheAdditionLock)
            {
                _cachedHandledPaths.Clear();
            }
        }

        ProcessQueuedAutoRecordTriggers();

        if (_lastClassJobId != _dalamudUtil.ClassJobId)
        {
            _lastClassJobId = _dalamudUtil.ClassJobId;

            PlayerConfig.JobSpecificCache.TryGetValue(_dalamudUtil.ClassJobId, out var jobSpecificData);

            var playerSet = new HashSet<string>(PlayerConfig.GlobalPersistentCache, StringComparer.OrdinalIgnoreCase);
            if (jobSpecificData != null)
            {
                foreach (var s in jobSpecificData)
                    playerSet.Add(s);
            }

            PlayerConfig.JobSpecificPetCache.TryGetValue(_dalamudUtil.ClassJobId, out var petSpecificData);
            var petSet = petSpecificData != null ? new HashSet<string>(petSpecificData, StringComparer.Ordinal) : [];

            lock (_transientResourcesLock)
            {
                SemiTransientResources[ObjectKind.Player] = playerSet;
                SemiTransientResources[ObjectKind.Pet] = petSet;
            }

            RebuildSemiTransientAll();
        }

        lock (_transientResourcesLock)
        {
            if (TransientResources.Count > 0 && _cachedFrameAddresses.Count > 0)
            {
                var presentKinds = new HashSet<ObjectKind>();
                foreach (var kv in _cachedFrameAddresses)
                    presentKinds.Add(kv.Value);

                var toRemove = new List<ObjectKind>();
                foreach (var kv in TransientResources)
                {
                    if (!presentKinds.Contains(kv.Key))
                        toRemove.Add(kv.Key);
                }

                foreach (var k in toRemove)
                {
                    TransientResources.Remove(k, out _);
                    Logger.LogDebug("Object not present anymore: {kind}", k.ToString());
                }
            }
            else if (TransientResources.Count > 0 && _cachedFrameAddresses.Count == 0)
            {
                TransientResources.Clear();
            }
        }
    }

    private void Manager_PenumbraModSettingChanged()
    {
        _penumbraSettingsChangedDebounceCts.Cancel();
        _penumbraSettingsChangedDebounceCts.Dispose();
        _penumbraSettingsChangedDebounceCts = new();

        var token = _penumbraSettingsChangedDebounceCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(250, token).ConfigureAwait(false);

                Logger.LogDebug("Penumbra Mod Settings changed, verifying SemiTransientResources");

                var playerPointers = GetPlayerRelatedPointerSnapshot();

                foreach (var item in playerPointers)
                {
                    Mediator.Publish(new TransientResourceChangedMessage(item.Address));
                }
            }
            catch (OperationCanceledException)
            {
            }
        }, token);
    }

    public void RebuildSemiTransientResources()
    {
        lock (_transientResourcesLock)
        {
            _semiTransientResources = null;
            _semiTransientAll = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private void RebuildSemiTransientAll()
    {
        var next = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        lock (_transientResourcesLock)
        {
            var resources = _semiTransientResources;
            if (resources == null)
            {
                _semiTransientAll = next;
                return;
            }

            foreach (var kv in resources)
            {
                var set = kv.Value;
                if (set == null) continue;

                foreach (var p in set)
                {
                    if (!string.IsNullOrEmpty(p))
                        next.Add(p);
                }
            }

            _semiTransientAll = next;
        }
    }

    private static bool EndsWithAny(string path, string[] exts)
    {
        for (int i = 0; i < exts.Length; i++)
        {
            if (path.EndsWith(exts[i], StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private GameObjectHandler[] GetPlayerRelatedPointerSnapshot()
    {
        lock (_playerRelatedPointersLock)
        {
            return _playerRelatedPointers.ToArray();
        }
    }

    private void Manager_PenumbraResourceLoadEvent(PenumbraResourceLoadMessage msg)
    {
        var gamePathRaw = msg.GamePath ?? string.Empty;
        var gameObjectAddress = msg.GameObject;
        var filePathRaw = msg.FilePath ?? string.Empty;

        var replacedGamePath = NormalizePath(gamePathRaw);
        if (string.IsNullOrWhiteSpace(replacedGamePath))
            return;

        lock (_cacheAdditionLock)
        {
            if (_cachedHandledPaths.Contains(replacedGamePath)) return;
            _cachedHandledPaths.Add(replacedGamePath);
        }

        string? collection = null;
        var filePath = filePathRaw;

        if (!string.IsNullOrEmpty(filePath) && filePath.StartsWith("|", StringComparison.OrdinalIgnoreCase))
        {
            var parts = filePath.Split("|");
            if (parts.Length >= 3)
            {
                collection = parts[1];
                filePath = parts[2];
            }
        }

        var filePathForLookup = filePath;
        filePath = NormalizePath(filePath);

        try
        {
            if (!string.IsNullOrWhiteSpace(filePathForLookup))
            {
                var map = _fileCacheManager.GetFileCachesByPaths(new[] { filePathForLookup });
                if (map.TryGetValue(filePathForLookup, out var ent) && ent != null && !string.IsNullOrWhiteSpace(ent.Hash))
                {
                    _transientHashByGamePath[replacedGamePath] = ent.Hash;
                }
            }
        }
        catch
        {
            // never break transient tracking
        }

        if (IsTransientRecording && string.IsNullOrWhiteSpace(filePath))
            return;

        if (string.Equals(filePath, replacedGamePath, StringComparison.OrdinalIgnoreCase))
            return;

        bool isHandled =
            EndsWithAny(replacedGamePath, _handledFileTypes)
            || (IsTransientRecording && EndsWithAny(replacedGamePath, _handledRecordingFileTypes));

        if (!isHandled)
            return;

        ObjectKind objectKind = ObjectKind.Player;
        var hasMappedKind = _cachedFrameAddresses.TryGetValue(gameObjectAddress, out objectKind);

        if (!hasMappedKind && !IsTransientRecording)
            return;

        if (IsTransientRecording)
        {
            if (!ShouldAutoRecordVfxOnly(replacedGamePath))
                return;

            if (_recordingOwnerAddress == nint.Zero)
                return;

            if (hasMappedKind)
            {
                if (gameObjectAddress != _recordingOwnerAddress)
                    return;
            }
            else
            {
                if (gameObjectAddress != nint.Zero)
                    return;

                if (string.IsNullOrWhiteSpace(_recordingCollection) ||
                    !string.Equals(collection, _recordingCollection, StringComparison.OrdinalIgnoreCase))
                    return;

                if (string.IsNullOrWhiteSpace(_recordingFilePrefix) ||
                    string.IsNullOrWhiteSpace(filePath) ||
                    !StartsWithNormalized(filePath, _recordingFilePrefix))
                    return;
            }

            Interlocked.Exchange(ref _autoRecordLastActivityTicks, DateTime.UtcNow.Ticks);
        }

        HashSet<string> transientResources;
        lock (_transientResourcesLock)
        {
            if (!TransientResources.TryGetValue(objectKind, out transientResources!))
            {
                transientResources = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                TransientResources[objectKind] = transientResources;
            }
        }

        var playerPointers = GetPlayerRelatedPointerSnapshot();

        GameObjectHandler? owner = null;

        // During recording, we always attach to the session owner
        if (IsTransientRecording && _recordingOwnerAddress != nint.Zero)
        {
            foreach (var ptr in playerPointers)
            {
                if (ptr.Address == _recordingOwnerAddress)
                {
                    owner = ptr;
                    break;
                }
            }
        }
        else
        {
            foreach (var ptr in playerPointers)
            {
                if (ptr.Address == gameObjectAddress)
                {
                    owner = ptr;
                    break;
                }
            }
        }

        bool alreadyTransient = false;
        bool transientContains;
        bool semiTransientContains;

        lock (_transientResourcesLock)
        {
            transientContains = transientResources.Contains(replacedGamePath);
            semiTransientContains = _semiTransientAll.Contains(replacedGamePath);
        }

        if (!IsTransientRecording
            && hasMappedKind
            && objectKind == ObjectKind.Player
            && !_inCombatOrPerformingSnapshot
            && ShouldAutoRecordVfxOnly(replacedGamePath)
            && !transientContains
            && !semiTransientContains)
        {
            GameObjectHandler? ownerForTrigger = null;
            foreach (var ptr in playerPointers)
            {
                if (ptr.Address == gameObjectAddress)
                {
                    ownerForTrigger = ptr;
                    break;
                }
            }

            if (ownerForTrigger != null)
            {
                var key = IsEmoteKeyPath(replacedGamePath) ? replacedGamePath : null;
                var prefix = GetFilePrefix(filePath);
                QueueAutoRecordTrigger(key, replacedGamePath, ownerForTrigger.Address, prefix, collection);
            }
        }

        if (transientContains || semiTransientContains)
        {
            if (!IsTransientRecording)
            {
                Logger.LogTrace(
                    "Not adding {replacedPath} => {filePath}, Reason: Transient: {contains}, SemiTransient: {contains2}",
                    replacedGamePath, filePath, transientContains, semiTransientContains);
            }

            alreadyTransient = true;
        }
        else
        {
            if (!IsTransientRecording)
            {
                bool isAdded;
                lock (_transientResourcesLock)
                {
                    isAdded = transientResources.Add(replacedGamePath);
                }

                if (isAdded)
                {
                    Logger.LogDebug(
                        "Adding {replacedGamePath} for {gameObject} ({filePath})",
                        replacedGamePath,
                        owner?.ToString() ?? gameObjectAddress.ToString("X"),
                        filePath);

                    SendTransients(gameObjectAddress, objectKind);
                }
            }
        }

        if (owner != null && IsTransientRecording)
        {
            _recordedTransients.Add(new TransientRecord(owner, replacedGamePath, filePath, alreadyTransient)
            {
                AddTransient = !alreadyTransient
            });
        }
    }

    private void SendTransients(nint gameObject, ObjectKind objectKind)
    {
        _transientDirty[objectKind] = 1;

        if (_transientSendScheduled.TryGetValue(objectKind, out var scheduled) && scheduled == 1)
            return;

        _transientSendScheduled[objectKind] = 1;

        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    _transientDirty[objectKind] = 0;

                    await Task.Delay(_transientSendDelay).ConfigureAwait(false);

                    bool hasValues;
                    lock (_transientResourcesLock)
                    {
                        hasValues = TransientResources.TryGetValue(objectKind, out var values) && values.Any();
                    }

                    if (hasValues)
                    {
                        Logger.LogTrace("Sending Transients for {kind}", objectKind);
                        Mediator.Publish(new TransientResourceChangedMessage(gameObject));
                    }

                    if (!_transientDirty.TryGetValue(objectKind, out var dirty) || dirty == 0)
                        break;
                }
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Transient send task failed for {kind}", objectKind);
            }
            finally
            {
                _transientSendScheduled[objectKind] = 0;
            }
        });
    }

    public void StartRecording(CancellationToken token)
    {
        if (IsTransientRecording) return;
        _recordedTransients = new ConcurrentBag<TransientRecord>();
        IsTransientRecording = true;
        RecordTimeRemaining.Value = TimeSpan.FromSeconds(150);
        _ = Task.Run(async () =>
        {
            try
            {
                while (RecordTimeRemaining.Value > TimeSpan.Zero && !token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), token).ConfigureAwait(false);
                    RecordTimeRemaining.Value = RecordTimeRemaining.Value.Subtract(TimeSpan.FromSeconds(1));
                }
            }
            finally
            {
                IsTransientRecording = false;
            }
        });
    }

    public async Task WaitForRecording(CancellationToken token)
    {
        while (IsTransientRecording)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), token).ConfigureAwait(false);
        }
    }

    internal void SaveRecording()
    {
        HashSet<nint> addedTransients = [];
        var snapshot = _recordedTransients;
        _recordedTransients = new ConcurrentBag<TransientRecord>();

        lock (_transientResourcesLock)
        {
            foreach (var item in snapshot)
            {
                if (!item.AddTransient || item.AlreadyTransient) continue;

                if (!TransientResources.TryGetValue(item.Owner.ObjectKind, out var transient))
                {
                    TransientResources[item.Owner.ObjectKind] = transient = [];
                }

                Logger.LogTrace("Adding recorded: {gamePath} => {filePath}", item.GamePath, item.FilePath);

                transient.Add(item.GamePath);
                addedTransients.Add(item.Owner.Address);
            }
        }

        foreach (var item in addedTransients)
        {
            Mediator.Publish(new TransientResourceChangedMessage(item));
        }
    }

    // -------------------- PATH HELPERS --------------------

    private static string NormalizePath(string p)
    {
        if (string.IsNullOrWhiteSpace(p)) return string.Empty;
        return p.ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsEmoteKeyPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        return gamePath.StartsWith("emote/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/emote/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsVfxRelatedResourcePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        return gamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/bgcommon/vfx/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsPropRelatedResourcePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        return gamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsClothesOrBodyPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        return gamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase);
    }

    private bool ShouldAutoRecordVfxOnly(string normalizedGamePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedGamePath)) return false;
        if (IsClothesOrBodyPath(normalizedGamePath)) return false;

        if (normalizedGamePath.Contains("/emote/", StringComparison.OrdinalIgnoreCase))
            return true;

        return IsVfxRelatedResourcePath(normalizedGamePath)
            || IsPropRelatedResourcePath(normalizedGamePath);
    }

    private HashSet<string> GetAutoRecordedEmoteKeySet()
    {
        lock (_autoRecordedKeyLock)
        {
            if (_autoRecordedEmoteKeysCache != null) return _autoRecordedEmoteKeysCache;

            _autoRecordedEmoteKeysCache = new HashSet<string>(
                PlayerConfig.AutoRecordedEmoteKeys.Select(NormalizePath),
                StringComparer.OrdinalIgnoreCase);

            return _autoRecordedEmoteKeysCache;
        }
    }

    private void AddAutoRecordedEmoteKey(string keyNormalized)
    {
        lock (_autoRecordedKeyLock)
        {
            PlayerConfig.AutoRecordedEmoteKeys.Add(keyNormalized);
            _autoRecordedEmoteKeysCache ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _autoRecordedEmoteKeysCache.Add(keyNormalized);
        }
    }

    private void TryStartAutoRecordSession(string? emoteKeyNormalized, string triggerPathNormalized, nint ownerAddress, string? filePrefix, string? collection)
    {
        if (!PlayerConfig.AutoRecordEmotes)
        {
            if (!_autoRecordWarnedDisabled)
            {
                _autoRecordWarnedDisabled = true;
                Logger.LogWarning("Auto-record emotes is disabled in transient config.");
            }
            return;
        }

        if (DateTime.UtcNow < _autoRecordCooldownUntilUtc)
            return;

        var normalizedKey = string.IsNullOrWhiteSpace(emoteKeyNormalized) ? null : NormalizePath(emoteKeyNormalized);
        if (!string.IsNullOrEmpty(normalizedKey))
        {
            var done = GetAutoRecordedEmoteKeySet();
            if (done.Contains(normalizedKey))
                return;
        }

        if (Interlocked.CompareExchange(ref _autoRecordRunning, 1, 0) != 0)
            return;

        _autoRecordCooldownUntilUtc = DateTime.UtcNow + _autoRecordCooldown;
        Interlocked.Exchange(ref _autoRecordLastActivityTicks, DateTime.UtcNow.Ticks);

        _recordingOwnerAddress = ownerAddress;
        _recordingFilePrefix = string.IsNullOrWhiteSpace(filePrefix) ? null : NormalizePath(filePrefix).TrimEnd('/') + "/";
        _recordingCollection = string.IsNullOrWhiteSpace(collection) ? null : collection;

        Logger.LogInformation("Auto-record starting (key={key}, trigger={trigger})", normalizedKey ?? "<none>", triggerPathNormalized);

        var cts = new CancellationTokenSource(_autoRecordDuration);
        StartRecording(cts.Token);

        _ = Task.Run(async () =>
        {
            using (cts)
            {
                try
                {
                    var startedUtc = DateTime.UtcNow;

                    while (!cts.IsCancellationRequested)
                    {
                        await Task.Delay(150).ConfigureAwait(false);

                        var elapsed = DateTime.UtcNow - startedUtc;
                        if (elapsed < _autoRecordMinDuration)
                            continue;

                        var lastTicks = Interlocked.Read(ref _autoRecordLastActivityTicks);
                        if (lastTicks == 0)
                            continue;

                        var lastUtc = new DateTime(lastTicks, DateTimeKind.Utc);
                        if (DateTime.UtcNow - lastUtc >= _autoRecordIdleStop)
                        {
                            cts.Cancel();
                            break;
                        }
                    }

                    try { cts.Cancel(); } catch { /* ignore */ }
                    await WaitForRecording(CancellationToken.None).ConfigureAwait(false);

                    var recorded = RecordedTransients.Count;
                    SaveRecording();
                    PersistTransientResources(ObjectKind.Player);

                    if (!string.IsNullOrEmpty(normalizedKey))
                    {
                        AddAutoRecordedEmoteKey(normalizedKey);
                        _configurationService.Save();
                    }

                    Logger.LogInformation("Auto-record finished (key={key}, recorded={count})", normalizedKey ?? "<none>", recorded);
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex, "Auto-record emote task failed (key={key}, trigger={trigger})", normalizedKey ?? "<none>", triggerPathNormalized);
                }
                finally
                {
                    _recordingOwnerAddress = nint.Zero;
                    _recordingFilePrefix = null;
                    _recordingCollection = null;
                    Interlocked.Exchange(ref _autoRecordRunning, 0);
                }
            }
        });
    }

    private void QueueAutoRecordTrigger(string? emoteKeyNormalized, string triggerPathNormalized, nint ownerAddress, string? filePrefix, string? collection)
    {
        _autoRecordTriggerQueue.Enqueue((emoteKeyNormalized, triggerPathNormalized, ownerAddress, filePrefix, collection));
    }

    private void ProcessQueuedAutoRecordTriggers()
    {
        if (_autoRecordTriggerQueue.IsEmpty) return;

        (string? EmoteKey, string TriggerPath, nint OwnerAddress, string? FilePrefix, string? Collection) last = default;
        while (_autoRecordTriggerQueue.TryDequeue(out var item))
        {
            last = item;
        }

        if (string.IsNullOrWhiteSpace(last.TriggerPath)) return;
        if (last.OwnerAddress == nint.Zero) return;

        TryStartAutoRecordSession(last.EmoteKey, last.TriggerPath, last.OwnerAddress, last.FilePrefix, last.Collection);
    }

    private static string? GetFilePrefix(string normalizedFilePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedFilePath)) return null;
        var p = NormalizePath(normalizedFilePath);
        var lastSlash = p.LastIndexOf('/');
        if (lastSlash <= 0) return null;
        return p.Substring(0, lastSlash + 1);
    }

    private static bool StartsWithNormalized(string value, string prefix)
    {
        if (string.IsNullOrWhiteSpace(value) || string.IsNullOrWhiteSpace(prefix)) return false;
        return NormalizePath(value).StartsWith(NormalizePath(prefix), StringComparison.OrdinalIgnoreCase);
    }



    private volatile ConcurrentBag<TransientRecord> _recordedTransients = new();
    public IReadOnlyCollection<TransientRecord> RecordedTransients => _recordedTransients;

    public ValueProgress<TimeSpan> RecordTimeRemaining { get; } = new();

    public record TransientRecord(GameObjectHandler Owner, string GamePath, string FilePath, bool AlreadyTransient)
    {
        public bool AddTransient { get; set; }
    }
}