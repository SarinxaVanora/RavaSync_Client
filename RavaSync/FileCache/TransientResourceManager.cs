using Lumina.Data.Parsing.Uld;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data.Enum;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using System.Collections.Concurrent;
using System.Text.Json;

namespace RavaSync.FileCache;

public sealed class TransientResourceManager : DisposableMediatorSubscriberBase
{
    private static readonly string[] _manifestGamePathRoots = ["chara/", "vfx/", "bgcommon/", "sound/", "ui/", "shader/"];
    private static readonly Dictionary<string, uint> _manifestJobTokenToClassJobId = new(StringComparer.OrdinalIgnoreCase)
    {
        ["pld"] = 19, ["paladin"] = 19, ["gladiator"] = 19, ["gla"] = 19,
        ["mnk"] = 20, ["monk"] = 20, ["pugilist"] = 20, ["pgl"] = 20,
        ["war"] = 21, ["warrior"] = 21, ["marauder"] = 21, ["mrd"] = 21, ["2ax"] = 21,
        ["drg"] = 22, ["dragoon"] = 22, ["lancer"] = 22, ["lnc"] = 22, ["2sp"] = 22,
        ["brd"] = 23, ["bard"] = 23, ["archer"] = 23, ["arc"] = 23, ["2bw"] = 23,
        ["whm"] = 24, ["whitemage"] = 24, ["conjurer"] = 24, ["cnj"] = 24,
        ["blm"] = 25, ["blackmage"] = 25, ["thaumaturge"] = 25, ["thm"] = 25,
        ["smn"] = 27, ["summoner"] = 27,
        ["sch"] = 28, ["scholar"] = 28,
        ["nin"] = 30, ["ninja"] = 30, ["rogue"] = 30, ["rog"] = 30,
        ["mch"] = 31, ["machinist"] = 31, ["2gn"] = 31,
        ["drk"] = 32, ["drkr"] = 32, ["darkknight"] = 32, ["2sw"] = 32,
        ["ast"] = 33, ["astrologian"] = 33, ["2gl"] = 33,
        ["sam"] = 34, ["samurai"] = 34, ["2kt"] = 34,
        ["rdm"] = 35, ["redmage"] = 35, ["2rp"] = 35,
        ["blu"] = 36, ["bluemage"] = 36,
        ["gnb"] = 37, ["gunbreaker"] = 37, ["2gb"] = 37,
        ["dnc"] = 38, ["dancer"] = 38,
        ["rpr"] = 39, ["rrp"] = 39, ["reaper"] = 39, ["riaper"] = 39, ["2km"] = 39,
        ["sge"] = 40, ["sage"] = 40, ["2ff"] = 40,
        ["vpr"] = 41, ["viper"] = 41, ["bld"] = 41, ["bld2"] = 41,
        ["pct"] = 42, ["pictomancer"] = 42, ["brs"] = 42, ["plt"] = 42,
    };
    private readonly object _cacheAdditionLock = new();
    private readonly object _playerRelatedPointersLock = new();
    private readonly object _transientResourcesLock = new();
    private DateTime _nextHandledPathsClearUtc = DateTime.MinValue;
    private static readonly TimeSpan _handledPathsClearInterval = TimeSpan.FromMilliseconds(250);
    private const int _handledPathsMaxSizeBeforeClear = 4096;

    private readonly HashSet<string> _cachedHandledPaths = new(StringComparer.Ordinal);
    private readonly TransientConfigService _configurationService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly string[] _handledFileTypes = ["tmb", "pap", "avfx", "atex", "sklb", "eid", "phy", "phyb", "pbd", "scd", "skp", "shpk"];
    private readonly string[] _handledRecordingFileTypes = ["tex", "mdl", "mtrl"];
    private readonly HashSet<GameObjectHandler> _playerRelatedPointers = [];
    private Dictionary<nint, ObjectKind> _cachedFrameAddresses = new();
    private long _lastTransientCleanupTick;
    private const int TransientCleanupIntervalMs = 500;
    private readonly HashSet<ObjectKind> _presentKindsBuffer = [];
    private readonly List<ObjectKind> _transientKindsToRemoveBuffer = [];
    private ConcurrentDictionary<ObjectKind, HashSet<string>>? _semiTransientResources = null;
    private uint _lastClassJobId = uint.MaxValue;
    public bool IsTransientRecording { get; private set; } = false;

    private readonly ConcurrentDictionary<ObjectKind, int> _transientSendScheduled = new();
    private readonly ConcurrentDictionary<ObjectKind, int> _transientDirty = new();
    private static readonly TimeSpan _transientSendDelay = TimeSpan.FromMilliseconds(750);
    private HashSet<string> _semiTransientAll = new(StringComparer.OrdinalIgnoreCase);
    private DateTime _lastManifestPrimeUtc = DateTime.MinValue;
    private DateTime _lastPenumbraManifestPrimeRequestUtc = DateTime.MinValue;
    private CancellationTokenSource _manifestPrimeDebounceCts = new();
    private CancellationTokenSource _penumbraSettingsChangedDebounceCts = new();
    private int _manifestPrimeRunning = 0;
    private readonly ConcurrentQueue<(string? EmoteKey, string TriggerPath, nint OwnerAddress, string? FilePrefix, string? ModRoot, string? Collection)> _autoRecordTriggerQueue = new();
    private volatile bool _inCombatOrPerformingSnapshot = false;
    private volatile bool _inCombatSnapshot = false;
    private volatile string _playerPersistentDataKey = string.Empty;

    // -------------------- AUTO VFX EMOTE RECORDING --------------------
    private int _autoRecordRunning = 0;
    private bool _autoRecordWarnedDisabled = false;
    private DateTime _autoRecordCooldownUntilUtc = DateTime.MinValue;
    private readonly ConcurrentDictionary<string, DateTime> _zeroResultAutoRecordSuppressedUntilByTrigger = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan _zeroResultAutoRecordSuppression = TimeSpan.FromMinutes(30);

    private static readonly TimeSpan _autoRecordDuration = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan _autoRecordCooldown = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan _autoRecordMinDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan _autoRecordIdleStop = TimeSpan.FromMilliseconds(1500);

    private nint _recordingOwnerAddress = nint.Zero;
    private string? _recordingFilePrefix = null;
    private string? _recordingModRoot = null;
    private string? _recordingCollection = null;

    private long _autoRecordLastActivityTicks = 0;

    private readonly object _autoRecordedKeyLock = new();
    private HashSet<string>? _autoRecordedEmoteKeysCache;

    private readonly FileCacheManager _fileCacheManager;
    private readonly IpcManager _ipcManager;
    private readonly SemaphoreSlim _localCollectionLookupLock = new(1, 1);
    private Guid _cachedLocalPlayerCollectionId = Guid.Empty;
    private DateTime _cachedLocalPlayerCollectionIdUntilUtc = DateTime.MinValue;
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
            DalamudUtilService dalamudUtil, MareMediator mediator, FileCacheManager fileCacheManager, IpcManager ipcManager) : base(logger, mediator)
    {
        _configurationService = configurationService;
        _dalamudUtil = dalamudUtil;
        _fileCacheManager = fileCacheManager;
        _ipcManager = ipcManager;

        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, Manager_PenumbraResourceLoadEvent);
        Mediator.Subscribe<PenumbraModSettingChangedMessage>(this, Manager_PenumbraModSettingChanged);
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
        HashSet<string> result;

        lock (_transientResourcesLock)
        {
            result = SemiTransientResources.TryGetValue(objectKind, out var existing) && existing != null
                ? new HashSet<string>(existing, StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal);
        }

        return result;
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
        var normalizedItem = NormalizePath(item);

        if (string.IsNullOrWhiteSpace(normalizedItem)
            || IsKnownAmbientAutoRecordNoise(normalizedItem)
            || !ShouldImportManifestGamePath(normalizedItem))
        {
            Logger.LogTrace("Ignoring invalid/noisy transient add for {kind}: {path}", objectKind, item);
            return false;
        }

        lock (_transientResourcesLock)
        {
            if (SemiTransientResources.TryGetValue(objectKind, out var semiTransient)
                && semiTransient != null
                && semiTransient.Contains(normalizedItem))
            {
                return false;
            }

            if (!TransientResources.TryGetValue(objectKind, out HashSet<string>? transientResource))
            {
                transientResource = new HashSet<string>(StringComparer.Ordinal);
                TransientResources[objectKind] = transientResource;
            }

            return transientResource.Add(normalizedItem);
        }
    }

    private void PrimeTransientPaths(IntPtr actorAddress, ObjectKind kind, IReadOnlyCollection<string> gamePaths)
    {
        if (actorAddress == IntPtr.Zero) return;
        if (gamePaths == null || gamePaths.Count == 0) return;

        if (!IsOwnedTrackedAddress(actorAddress))
        {
            Logger.LogTrace("Ignoring prime transient paths for non-owned actor {address:X}", actorAddress);
            return;
        }

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

        _manifestPrimeDebounceCts.Cancel();
        _manifestPrimeDebounceCts.Dispose();
        _penumbraSettingsChangedDebounceCts.Cancel();
        _penumbraSettingsChangedDebounceCts.Dispose();

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
        _inCombatSnapshot = _dalamudUtil.IsInCombat;

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
                // swallow
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

        var nowTick = Environment.TickCount64;
        if ((nowTick - _lastTransientCleanupTick) < TransientCleanupIntervalMs)
            return;

        _lastTransientCleanupTick = nowTick;

        var transientLockTaken = false;
        try
        {
            Monitor.TryEnter(_transientResourcesLock, ref transientLockTaken);
            if (!transientLockTaken)
                return;

            if (TransientResources.Count > 0 && _cachedFrameAddresses.Count > 0)
            {
                _presentKindsBuffer.Clear();
                foreach (var kv in _cachedFrameAddresses)
                    _presentKindsBuffer.Add(kv.Value);

                _transientKindsToRemoveBuffer.Clear();
                foreach (var kv in TransientResources)
                {
                    if (!_presentKindsBuffer.Contains(kv.Key))
                        _transientKindsToRemoveBuffer.Add(kv.Key);
                }

                foreach (var k in _transientKindsToRemoveBuffer)
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
        finally
        {
            if (transientLockTaken)
                Monitor.Exit(_transientResourcesLock);
        }
    }

    private void Manager_PenumbraModSettingChanged(PenumbraModSettingChangedMessage msg)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                if (!await IsPenumbraModSettingChangeForLocalPlayerCollectionAsync(msg, CancellationToken.None).ConfigureAwait(false))
                    return;

                ClearAutoRecordedEmoteKeysForOptionChange();

                _penumbraSettingsChangedDebounceCts.Cancel();
                _penumbraSettingsChangedDebounceCts.Dispose();
                _penumbraSettingsChangedDebounceCts = new();

                var token = _penumbraSettingsChangedDebounceCts.Token;

                var now = DateTime.UtcNow;
                if (now - _lastPenumbraManifestPrimeRequestUtc >= TimeSpan.FromSeconds(15))
                {
                    _lastPenumbraManifestPrimeRequestUtc = now;
                    ClearManifestTransientCache();
                    ScheduleManifestPrime("PenumbraModSettingChanged");
                }

                await Task.Delay(250, token).ConfigureAwait(false);

                Logger.LogDebug("Local Penumbra Mod Settings changed, verifying SemiTransientResources");

                var playerPointers = GetPlayerRelatedPointerSnapshot();

                foreach (var item in playerPointers)
                {
                    Mediator.Publish(new TransientResourceChangedMessage(item.Address));
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Failed to classify Penumbra mod-setting change for transient refresh; allowing refresh to preserve local transient state correctness");
                var playerPointers = GetPlayerRelatedPointerSnapshot();
                foreach (var item in playerPointers)
                    Mediator.Publish(new TransientResourceChangedMessage(item.Address));
            }
        });
    }

    private async Task<bool> IsPenumbraModSettingChangeForLocalPlayerCollectionAsync(PenumbraModSettingChangedMessage msg, CancellationToken token)
    {
        if (msg.CollectionId == Guid.Empty)
            return true;

        var now = DateTime.UtcNow;
        var cachedCollectionId = _cachedLocalPlayerCollectionId;
        if (cachedCollectionId != Guid.Empty && now <= _cachedLocalPlayerCollectionIdUntilUtc)
        {
            var isLocalCached = cachedCollectionId == msg.CollectionId;
            if (!isLocalCached && Logger.IsEnabled(LogLevel.Trace))
            {
                Logger.LogTrace("Ignoring Penumbra mod-setting change for transient refresh because collection {collectionId} is not local player collection {localCollectionId}; mod {modName} change {change}",
                    msg.CollectionId, cachedCollectionId, msg.ModName, msg.Change);
            }

            return isLocalCached;
        }

        await _localCollectionLookupLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            now = DateTime.UtcNow;
            cachedCollectionId = _cachedLocalPlayerCollectionId;
            if (cachedCollectionId != Guid.Empty && now <= _cachedLocalPlayerCollectionIdUntilUtc)
            {
                var isLocalCached = cachedCollectionId == msg.CollectionId;
                if (!isLocalCached && Logger.IsEnabled(LogLevel.Trace))
                {
                    Logger.LogTrace("Ignoring Penumbra mod-setting change for transient refresh because collection {collectionId} is not local player collection {localCollectionId}; mod {modName} change {change}",
                        msg.CollectionId, cachedCollectionId, msg.ModName, msg.Change);
                }

                return isLocalCached;
            }

            var localPlayer = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var ptr = _dalamudUtil.GetPlayerPtr();
                if (ptr == nint.Zero)
                    return (Address: nint.Zero, ObjectIndex: -1);

                var obj = _dalamudUtil.CreateGameObject(ptr);
                return (Address: ptr, ObjectIndex: obj?.ObjectIndex ?? -1);
            }).ConfigureAwait(false);

            if (localPlayer.Address == nint.Zero || localPlayer.ObjectIndex < 0)
                return true;

            var collectionState = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(Logger, localPlayer.ObjectIndex).ConfigureAwait(false);
            if (collectionState?.CollectionId is not { } localCollectionId || localCollectionId == Guid.Empty)
                return true;

            _cachedLocalPlayerCollectionId = localCollectionId;
            _cachedLocalPlayerCollectionIdUntilUtc = now + TimeSpan.FromMilliseconds(250);

            var isLocal = localCollectionId == msg.CollectionId;
            if (!isLocal && Logger.IsEnabled(LogLevel.Trace))
            {
                Logger.LogTrace("Ignoring Penumbra mod-setting change for transient refresh because collection {collectionId} is not local player collection {localCollectionId}; mod {modName} change {change}",
                    msg.CollectionId, localCollectionId, msg.ModName, msg.Change);
            }

            return isLocal;
        }
        finally
        {
            _localCollectionLookupLock.Release();
        }
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
            if (resources != null)
            {
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
            }
        }

        _semiTransientAll = next;
    }

    public async Task EnsureManifestPrimeAsync(string reason, CancellationToken token)
    {
        if (!_ipcManager.Initialized) return;
        if (string.IsNullOrWhiteSpace(PlayerPersistentDataKey))
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
                // Best-effort only; manifest prime can try again next payload build.
            }
        }

        if (string.IsNullOrWhiteSpace(PlayerPersistentDataKey)) return;
        if (string.IsNullOrWhiteSpace(_ipcManager.Penumbra.ModDirectory)) return;

        // Character-data creation is the path that feeds the sync payload. Do not rely only
        // on the debounced background prime here, because selected idle VFX/model options
        // can otherwise miss the first send and only become correct after the sender repeats
        // the animation/resource load.
        if (DateTime.UtcNow - _lastManifestPrimeUtc < TimeSpan.FromSeconds(2))
            return;

        if (Interlocked.CompareExchange(ref _manifestPrimeRunning, 1, 0) != 0)
        {
            await WaitForRunningManifestPrimeAsync(reason, token).ConfigureAwait(false);
            return;
        }

        try
        {
            await PrimeActiveManifestTransientsAsync(reason, token).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _manifestPrimeRunning, 0);
        }
    }

    private async Task WaitForRunningManifestPrimeAsync(string reason, CancellationToken token)
    {
        var deadline = Environment.TickCount64 + 2500;

        while (Volatile.Read(ref _manifestPrimeRunning) != 0)
        {
            token.ThrowIfCancellationRequested();

            if (Environment.TickCount64 >= deadline)
            {
                Logger.LogTrace("Timed out waiting for running manifest transient prime during {reason}", reason);
                return;
            }

            await Task.Delay(50, token).ConfigureAwait(false);
        }
    }

    public void ScheduleManifestPrime(string reason)
    {
        if (!_ipcManager.Initialized) return;
        if (string.IsNullOrWhiteSpace(PlayerPersistentDataKey)) return;
        if (string.IsNullOrWhiteSpace(_ipcManager.Penumbra.ModDirectory)) return;

        if (string.Equals(reason, "PlayerDataFactory.CreateCharacterData", StringComparison.Ordinal)
            && DateTime.UtcNow - _lastManifestPrimeUtc < TimeSpan.FromSeconds(3))
        {
            return;
        }

        _manifestPrimeDebounceCts.Cancel();
        _manifestPrimeDebounceCts.Dispose();
        _manifestPrimeDebounceCts = new();

        var token = _manifestPrimeDebounceCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(350, token).ConfigureAwait(false);
                await WaitForManifestPrimeQuietWindowAsync(reason, token).ConfigureAwait(false);

                if (Interlocked.CompareExchange(ref _manifestPrimeRunning, 1, 0) != 0)
                {
                    Logger.LogTrace("Skipping manifest transient prime for {reason}: another prime is already running", reason);
                    return;
                }

                try
                {
                    await PrimeActiveManifestTransientsAsync(reason, token).ConfigureAwait(false);
                }
                finally
                {
                    Volatile.Write(ref _manifestPrimeRunning, 0);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Manifest transient prime failed for {reason}", reason);
            }
        }, token);
    }

    private async Task WaitForManifestPrimeQuietWindowAsync(string reason, CancellationToken token)
    {
        const int maxWaitMs = 8000;
        var waitedMs = 0;

        while (!token.IsCancellationRequested && waitedMs < maxWaitMs)
        {
            var busy = _inCombatOrPerformingSnapshot
                || IsTransientRecording
                || Volatile.Read(ref _autoRecordRunning) != 0
                || SyncStorm.IsActive;

            if (!busy)
                return;

            if (waitedMs == 0)
                Logger.LogTrace("Deferring manifest transient prime for {reason} until the client is quieter", reason);

            await Task.Delay(500, token).ConfigureAwait(false);
            waitedMs += 500;
        }
    }

    private void ClearManifestTransientCache()
    {
        _lastManifestPrimeUtc = DateTime.MinValue;
    }

    private async Task PrimeActiveManifestTransientsAsync(string reason, CancellationToken token)
    {
        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return;

        (nint Address, int ObjectIndex) localPlayer = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            var ptr = _dalamudUtil.GetPlayerPtr();
            if (ptr == nint.Zero)
                return (Address: nint.Zero, ObjectIndex: -1);

            var obj = _dalamudUtil.CreateGameObject(ptr);
            return (Address: ptr, ObjectIndex: obj?.ObjectIndex ?? -1);
        }).ConfigureAwait(false);

        if (localPlayer.Address == nint.Zero || localPlayer.ObjectIndex < 0)
            return;

        var currentJobId = _dalamudUtil.ClassJobId;

        var collectionState = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(Logger, localPlayer.ObjectIndex).ConfigureAwait(false);

        if (collectionState == null)
        {
            Logger.LogTrace("Skipping manifest transient prime for {reason}: collection state unavailable", reason);
            return;
        }

        var manifestGroups = new List<ManifestTransientImportGroup>();

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (!mod.Value.Enabled)
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            ImportManifestEntries(modPath, mod.Value.Settings, manifestGroups, token);
        }

        int addedGlobal = 0;
        int addedJobSpecific = 0;

        lock (_transientResourcesLock)
        {
            var playerConfig = PlayerConfig;

            var sanitizedGlobal = SanitiseSeededTransientList(playerConfig.GlobalPersistentCache);
            var globalList = playerConfig.GlobalPersistentCache;
            var globalSeen = new HashSet<string>(
                globalList.Where(s => !string.IsNullOrWhiteSpace(s)),
                StringComparer.OrdinalIgnoreCase);

            int sanitizedJobs = 0;
            int reclassifiedJobs = 0;
            int sanitizedPets = 0;

            var jobLists = new Dictionary<uint, List<string>>();
            var jobSeenById = new Dictionary<uint, HashSet<string>>();
            foreach (var kvp in playerConfig.JobSpecificCache.ToList())
            {
                var list = kvp.Value ?? [];
                playerConfig.JobSpecificCache[kvp.Key] = list;
                jobLists[kvp.Key] = list;
                jobSeenById[kvp.Key] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            sanitizedJobs += SanitiseAndReclassifySeededJobLists(globalList, globalSeen, playerConfig.JobSpecificCache, jobLists, jobSeenById, out reclassifiedJobs);

            foreach (var kvp in playerConfig.JobSpecificPetCache.ToList())
            {
                var list = kvp.Value ?? [];
                playerConfig.JobSpecificPetCache[kvp.Key] = list;
                sanitizedPets += SanitiseSeededTransientList(list);
            }

            if (sanitizedGlobal > 0 || sanitizedJobs > 0 || sanitizedPets > 0 || reclassifiedJobs > 0)
                Logger.LogDebug("Sanitized seeded transient cache for current player ({global} global, {jobs} job-specific, {pets} pet, {moved} reclassified)", sanitizedGlobal, sanitizedJobs, sanitizedPets, reclassifiedJobs);

            var groupResolvedJobsById = new Dictionary<ManifestTransientImportGroup, HashSet<uint>>();
            var folderResolvedJobsByKey = new Dictionary<string, HashSet<uint>>(StringComparer.OrdinalIgnoreCase);
            var importScopeResolvedJobsByKey = new Dictionary<string, HashSet<uint>>(StringComparer.OrdinalIgnoreCase);
            var modResolvedJobsByKey = new Dictionary<string, HashSet<uint>>(StringComparer.OrdinalIgnoreCase);

            foreach (var group in manifestGroups)
            {
                var groupResolvedJobs = DetermineManifestImportTargetJobs(group.GamePaths);
                groupResolvedJobsById[group] = groupResolvedJobs;

                if (groupResolvedJobs.Count == 0)
                    continue;

                if (!string.IsNullOrWhiteSpace(group.FolderScopeKey))
                {
                    if (!folderResolvedJobsByKey.TryGetValue(group.FolderScopeKey, out var folderResolvedJobs))
                    {
                        folderResolvedJobs = new HashSet<uint>();
                        folderResolvedJobsByKey[group.FolderScopeKey] = folderResolvedJobs;
                    }

                    foreach (var resolvedJob in groupResolvedJobs)
                        folderResolvedJobs.Add(resolvedJob);
                }

                if (!string.IsNullOrWhiteSpace(group.ImportScopeKey))
                {
                    if (!importScopeResolvedJobsByKey.TryGetValue(group.ImportScopeKey, out var importScopeResolvedJobs))
                    {
                        importScopeResolvedJobs = new HashSet<uint>();
                        importScopeResolvedJobsByKey[group.ImportScopeKey] = importScopeResolvedJobs;
                    }

                    foreach (var resolvedJob in groupResolvedJobs)
                        importScopeResolvedJobs.Add(resolvedJob);
                }

                if (!string.IsNullOrWhiteSpace(group.ModScopeKey))
                {
                    if (!modResolvedJobsByKey.TryGetValue(group.ModScopeKey, out var modResolvedJobs))
                    {
                        modResolvedJobs = new HashSet<uint>();
                        modResolvedJobsByKey[group.ModScopeKey] = modResolvedJobs;
                    }

                    foreach (var resolvedJob in groupResolvedJobs)
                        modResolvedJobs.Add(resolvedJob);
                }
            }

            foreach (var group in manifestGroups)
            {
                groupResolvedJobsById.TryGetValue(group, out var groupResolvedJobs);

                uint inheritedJobId = DetermineInheritedManifestJobId(group,groupResolvedJobs,folderResolvedJobsByKey,importScopeResolvedJobsByKey,modResolvedJobsByKey);

                foreach (var gamePath in group.GamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    if (TryResolveManifestClassJobId(gamePath, out var resolvedJobId))
                    {
                        AddSeededPathToJob(playerConfig.JobSpecificCache, jobLists, jobSeenById, resolvedJobId, gamePath, ref addedJobSpecific);
                        continue;
                    }

                    if (inheritedJobId != 0 && !ShouldForceGlobalManifestPath(gamePath))
                    {
                        AddSeededPathToJob(playerConfig.JobSpecificCache, jobLists, jobSeenById, inheritedJobId, gamePath, ref addedJobSpecific);
                        continue;
                    }

                    if (globalSeen.Add(gamePath))
                    {
                        globalList.Add(gamePath);
                        addedGlobal++;
                    }
                }
            }

            if (addedGlobal > 0 || addedJobSpecific > 0)
                _semiTransientResources = null;
        }

        _lastManifestPrimeUtc = DateTime.UtcNow;

        var totalAdded = addedGlobal + addedJobSpecific;
        if (totalAdded <= 0)
        {
            Logger.LogTrace("Manifest transient prime found no new transient config paths for {reason}", reason);
            return;
        }

        Logger.LogDebug(
            "Manifest transient prime imported {count} transient config paths for {reason} ({global} global, {job} job-specific)",
            totalAdded, reason, addedGlobal, addedJobSpecific);

        Logger.LogTrace("Saving transient.json from {method}", nameof(PrimeActiveManifestTransientsAsync));
        _configurationService.Save();
        RebuildSemiTransientAll();
    }

    private int SanitiseSeededTransientList(List<string> values)
    {
        if (values == null || values.Count == 0)
            return 0;

        int changed = 0;
        var next = new List<string>(values.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var value in values)
        {
            if (!TryNormalizeManifestGamePath(value, out var normalized))
            {
                changed++;
                continue;
            }

            if (!ShouldImportManifestGamePath(normalized))
            {
                changed++;
                continue;
            }

            if (!seen.Add(normalized))
            {
                changed++;
                continue;
            }

            if (!string.Equals(value, normalized, StringComparison.Ordinal))
                changed++;

            next.Add(normalized);
        }

        if (changed > 0)
        {
            values.Clear();
            values.AddRange(next);
        }

        return changed;
    }

    private int SanitiseAndReclassifySeededJobLists(
        List<string> globalList,
        HashSet<string> globalSeen,
        Dictionary<uint, List<string>> playerJobSpecificCache,
        Dictionary<uint, List<string>> jobLists,
        Dictionary<uint, HashSet<string>> jobSeenById,
        out int reclassified)
    {
        reclassified = 0;
        int changed = 0;

        foreach (var kvp in playerJobSpecificCache.ToList())
        {
            var jobId = kvp.Key;
            var sourceList = kvp.Value ?? [];
            playerJobSpecificCache[jobId] = sourceList;

            int listChanged = 0;
            var next = new List<string>(sourceList.Count);
            var localSeen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var value in sourceList)
            {
                if (!TryNormalizeManifestGamePath(value, out var normalized))
                {
                    changed++;
                    listChanged++;
                    continue;
                }

                if (!ShouldImportManifestGamePath(normalized))
                {
                    changed++;
                    listChanged++;
                    continue;
                }

                if (TryResolveManifestClassJobId(normalized, out var resolvedJobId) && resolvedJobId != jobId)
                {
                    AddSeededPathToJob(playerJobSpecificCache, jobLists, jobSeenById, resolvedJobId, normalized, ref reclassified);
                    changed++;
                    listChanged++;
                    continue;
                }

                if (ShouldForceGlobalManifestPath(normalized))
                {
                    if (globalSeen.Add(normalized))
                        globalList.Add(normalized);

                    changed++;
                    listChanged++;
                    continue;
                }

                if (!localSeen.Add(normalized))
                {
                    changed++;
                    listChanged++;
                    continue;
                }

                if (!string.Equals(value, normalized, StringComparison.Ordinal))
                {
                    changed++;
                    listChanged++;
                }

                next.Add(normalized);
            }

            if (listChanged > 0 || next.Count != sourceList.Count)
            {
                sourceList.Clear();
                sourceList.AddRange(next);
            }

            jobLists[jobId] = sourceList;
            jobSeenById[jobId] = new HashSet<string>(sourceList, StringComparer.OrdinalIgnoreCase);
        }

        return changed;
    }

    private static void AddSeededPathToJob(
        Dictionary<uint, List<string>> playerJobSpecificCache,
        Dictionary<uint, List<string>> jobLists,
        Dictionary<uint, HashSet<string>> jobSeenById,
        uint jobId,
        string gamePath,
        ref int addedCount)
    {
        if (!jobLists.TryGetValue(jobId, out var targetJobList) || targetJobList == null)
        {
            playerJobSpecificCache[jobId] = targetJobList = [];
            jobLists[jobId] = targetJobList;
        }

        if (!jobSeenById.TryGetValue(jobId, out var targetJobSeen) || targetJobSeen == null)
        {
            targetJobSeen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            jobSeenById[jobId] = targetJobSeen;
        }

        if (targetJobSeen.Add(gamePath))
        {
            targetJobList.Add(gamePath);
            addedCount++;
        }
    }
    private static uint DetermineInheritedManifestJobId(ManifestTransientImportGroup group,HashSet<uint>? groupResolvedJobs,IReadOnlyDictionary<string, HashSet<uint>> folderResolvedJobsByKey,
        IReadOnlyDictionary<string, HashSet<uint>> importScopeResolvedJobsByKey,IReadOnlyDictionary<string, HashSet<uint>> modResolvedJobsByKey)
    {
        if (TryGetSingleResolvedManifestJob(groupResolvedJobs, out var inheritedJobId))
            return inheritedJobId;

        if (!string.IsNullOrWhiteSpace(group.FolderScopeKey)
            && folderResolvedJobsByKey.TryGetValue(group.FolderScopeKey, out var folderResolvedJobs)
            && TryGetSingleResolvedManifestJob(folderResolvedJobs, out inheritedJobId))
        {
            return inheritedJobId;
        }

        if (!string.IsNullOrWhiteSpace(group.ImportScopeKey)
            && importScopeResolvedJobsByKey.TryGetValue(group.ImportScopeKey, out var importScopeResolvedJobs)
            && TryGetSingleResolvedManifestJob(importScopeResolvedJobs, out inheritedJobId))
        {
            return inheritedJobId;
        }

        if (!string.IsNullOrWhiteSpace(group.ModScopeKey)
            && modResolvedJobsByKey.TryGetValue(group.ModScopeKey, out var modResolvedJobs)
            && TryGetSingleResolvedManifestJob(modResolvedJobs, out inheritedJobId))
        {
            return inheritedJobId;
        }

        return 0;
    }

    private static bool TryGetSingleResolvedManifestJob(IEnumerable<uint>? resolvedJobs, out uint jobId)
    {
        jobId = 0;
        if (resolvedJobs == null)
            return false;

        using var enumerator = resolvedJobs.GetEnumerator();
        if (!enumerator.MoveNext())
            return false;

        var first = enumerator.Current;
        if (enumerator.MoveNext())
            return false;

        jobId = first;
        return true;
    }

    private static bool TryNormalizeManifestGamePath(string rawPath, out string normalizedGamePath)
    {
        normalizedGamePath = string.Empty;
        if (string.IsNullOrWhiteSpace(rawPath))
            return false;

        var normalized = NormalizePath(rawPath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        string? matchedRoot = _manifestGamePathRoots.FirstOrDefault(root => normalized.StartsWith(root, StringComparison.OrdinalIgnoreCase));
        if (string.IsNullOrWhiteSpace(matchedRoot))
            return false;

        foreach (var root in _manifestGamePathRoots)
        {
            var idx = normalized.IndexOf(root, matchedRoot.Length, StringComparison.OrdinalIgnoreCase);
            if (idx >= 0)
                return false;
        }

        normalizedGamePath = normalized;
        return true;
    }

    private static string ResolveModDirectory(string modDirectoryRoot, string modKey)
    {
        if (string.IsNullOrWhiteSpace(modKey)) return modDirectoryRoot;
        if (Path.IsPathRooted(modKey)) return Path.GetFullPath(modKey);
        return Path.GetFullPath(Path.Combine(modDirectoryRoot, modKey));
    }

    private void ImportManifestEntries(string modPath, Dictionary<string, List<string>> selectedSettings,
        List<ManifestTransientImportGroup> manifestGroups, CancellationToken token)
    {
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
            ImportManifestJson(modPath, defaultManifest, null, manifestGroups, token);

        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            ImportManifestJson(modPath, groupManifest, selectedSettings, manifestGroups, token);
        }
    }

    private void ImportManifestJson(string modPath, string manifestPath, Dictionary<string, List<string>>? selectedSettings,
        List<ManifestTransientImportGroup> manifestGroups, CancellationToken token)
    {
        using var stream = File.OpenRead(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        if (string.Equals(Path.GetFileName(manifestPath), "default_mod.json", StringComparison.OrdinalIgnoreCase))
        {
            ImportManifestMappings(modPath, root, manifestGroups, NormalizePath(manifestPath) + "|default");
            return;
        }

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        var selectedOptions = GetSelectedOptionNames(root, selectedSettings);
        if (selectedOptions.Count == 0)
            return;

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();

            var optionName = option.TryGetProperty("Name", out var optionNameElement)
                ? optionNameElement.GetString() ?? string.Empty
                : string.Empty;

            if (!selectedOptions.Contains(optionName))
                continue;

            ImportManifestMappings(modPath, option, manifestGroups, NormalizePath(manifestPath) + "|" + NormalizePath(optionName));
        }
    }

    private static HashSet<string> GetSelectedOptionNames(JsonElement groupRoot, Dictionary<string, List<string>>? selectedSettings)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var groupName = groupRoot.TryGetProperty("Name", out var groupNameElement)
            ? groupNameElement.GetString() ?? string.Empty
            : string.Empty;

        if (!string.IsNullOrWhiteSpace(groupName) && selectedSettings != null)
        {
            List<string>? selected = null;

            if (!selectedSettings.TryGetValue(groupName, out selected))
            {
                var normalizedGroupName = NormalizePath(groupName);

                foreach (var kvp in selectedSettings)
                {
                    if (string.Equals(NormalizePath(kvp.Key), normalizedGroupName, StringComparison.OrdinalIgnoreCase))
                    {
                        selected = kvp.Value;
                        break;
                    }
                }
            }

            if (selected != null)
            {
                foreach (var item in selected.Where(s => !string.IsNullOrWhiteSpace(s)))
                    output.Add(item);

                if (output.Count > 0)
                    return output;
            }
        }

        if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return output;

        var optionNames = new List<string>();

        foreach (var option in options.EnumerateArray())
        {
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var name = optionNameElement.GetString();
            if (!string.IsNullOrWhiteSpace(name))
                optionNames.Add(name);
        }

        if (optionNames.Count == 0)
            return output;

        if (!groupRoot.TryGetProperty("DefaultSettings", out var defaultSettings)
            || defaultSettings.ValueKind != JsonValueKind.Number
            || !defaultSettings.TryGetInt32(out var defaultValue))
        {
            return output;
        }

        var groupType = groupRoot.TryGetProperty("Type", out var typeElement)
            ? typeElement.GetString() ?? string.Empty
            : string.Empty;

        if (string.Equals(groupType, "Single", StringComparison.OrdinalIgnoreCase))
        {
            // Penumbra Single groups use DefaultSettings as a zero-based option index.
            // DefaultSettings = 0
            if (defaultValue >= 0 && defaultValue < optionNames.Count)
                output.Add(optionNames[defaultValue]);

            return output;
        }

        if (string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase))
        {
            if (defaultValue <= 0)
                return output;

            for (var i = 0; i < optionNames.Count; i++)
            {
                if ((defaultValue & (1 << i)) != 0)
                    output.Add(optionNames[i]);
            }

            return output;
        }
        if (defaultValue >= 0 && defaultValue < optionNames.Count)
        {
            output.Add(optionNames[defaultValue]);
            return output;
        }

        if (defaultValue > 0)
        {
            for (var i = 0; i < optionNames.Count; i++)
            {
                if ((defaultValue & (1 << i)) != 0)
                    output.Add(optionNames[i]);
            }
        }

        return output;
    }

    private void ImportManifestMappings(string modPath, JsonElement element, List<ManifestTransientImportGroup> manifestGroups, string importScopeKey)
    {
        var modScopeKey = NormalizePath(modPath);
        var groupsByScopeKey = new Dictionary<string, ManifestTransientImportGroup>(StringComparer.OrdinalIgnoreCase);

        if (element.TryGetProperty("Files", out var files) && files.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in files.EnumerateObject())
            {
                if (!TryNormalizeManifestGamePath(property.Name, out var gamePath)) continue;
                if (!ShouldImportManifestGamePath(gamePath)) continue;

                var resolvedFilePath = ResolveManifestFilePath(modPath, property.Value.GetString());
                if (string.IsNullOrWhiteSpace(resolvedFilePath)) continue;
                if (!File.Exists(resolvedFilePath)) continue;

                var folderScopeKey = BuildManifestFolderScopeKey(modScopeKey, property.Value.GetString());
                var scopeKey = string.IsNullOrWhiteSpace(folderScopeKey) ? modScopeKey : folderScopeKey;
                if (!groupsByScopeKey.TryGetValue(scopeKey, out var group))
                {
                    group = new ManifestTransientImportGroup
                    {
                        ModScopeKey = modScopeKey,
                        FolderScopeKey = folderScopeKey,
                        ImportScopeKey = importScopeKey,
                    };
                    groupsByScopeKey[scopeKey] = group;
                }

                group.GamePaths.Add(gamePath);
            }
        }

        if (element.TryGetProperty("FileSwaps", out var fileSwaps) && fileSwaps.ValueKind == JsonValueKind.Object)
        {
            var fileSwapScopeKey = modScopeKey + "|fileswaps";
            if (!groupsByScopeKey.TryGetValue(fileSwapScopeKey, out var fileSwapGroup))
            {
                fileSwapGroup = new ManifestTransientImportGroup
                {
                    ModScopeKey = modScopeKey,
                    FolderScopeKey = string.Empty,
                    ImportScopeKey = importScopeKey,
                };
                groupsByScopeKey[fileSwapScopeKey] = fileSwapGroup;
            }

            foreach (var property in fileSwaps.EnumerateObject())
            {
                if (!TryNormalizeManifestGamePath(property.Name, out var gamePath)) continue;
                if (!ShouldImportManifestGamePath(gamePath)) continue;

                var replacementGamePath = NormalizePath(property.Value.GetString() ?? string.Empty);
                if (string.IsNullOrWhiteSpace(replacementGamePath)) continue;

                fileSwapGroup.GamePaths.Add(gamePath);

                // File-swap options often point an idle VFX/action resource at a selected
                // prop/model path. Include that selected target as well when it is in our
                // transient-safe scope, otherwise the first receiver apply can have the swap
                // but miss the model/support files until the sender repeats the animation.
                if (TryNormalizeManifestGamePath(replacementGamePath, out var replacementManifestGamePath)
                    && ShouldImportManifestGamePath(replacementManifestGamePath))
                {
                    fileSwapGroup.GamePaths.Add(replacementManifestGamePath);
                }
            }
        }

        foreach (var group in groupsByScopeKey.Values)
        {
            if (group.GamePaths.Count > 0)
                manifestGroups.Add(group);
        }
    }

    private static string ResolveManifestFilePath(string modPath, string? manifestValue)
    {
        if (string.IsNullOrWhiteSpace(manifestValue)) return string.Empty;

        var normalized = manifestValue.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar);
        var combined = Path.IsPathRooted(normalized)
            ? normalized
            : Path.Combine(modPath, normalized);

        return Path.GetFullPath(combined).ToLowerInvariant();
    }

    private static string BuildManifestFolderScopeKey(string modScopeKey, string? manifestValue)
    {
        if (string.IsNullOrWhiteSpace(modScopeKey) || string.IsNullOrWhiteSpace(manifestValue))
            return string.Empty;

        var normalized = NormalizePath(manifestValue);
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        var lastSlash = normalized.LastIndexOf('/');
        if (lastSlash <= 0)
            return string.Empty;

        return modScopeKey + "|" + normalized[..lastSlash];
    }

    private bool ShouldImportManifestGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        if (!_manifestGamePathRoots.Any(root => gamePath.StartsWith(root, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (!EndsWithAny(gamePath, _handledFileTypes))
            return false;

        if (IsKnownAmbientAutoRecordNoise(gamePath))
            return false;

        if (gamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            && !gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase))
            return false;

        if (gamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase))
            return false;

        if (gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase))
            return false;

        if (gamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase))
            return false;

        if (IsEmoteKeyPath(gamePath))
            return true;

        if (IsVfxRelatedResourcePath(gamePath))
            return true;

        return gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase);
    }

    private static HashSet<uint> DetermineManifestImportTargetJobs(IEnumerable<string> gamePaths)
    {
        var result = new HashSet<uint>();

        foreach (var gamePath in gamePaths)
        {
            if (TryResolveManifestClassJobId(gamePath, out var classJobId))
                result.Add(classJobId);
        }

        return result;
    }

    private static bool TryResolveManifestClassJobId(string gamePath, out uint classJobId)
    {
        classJobId = 0;
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        if (ShouldForceGlobalManifestPath(gamePath))
            return false;

        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        foreach (System.Text.RegularExpressions.Match match in System.Text.RegularExpressions.Regex.Matches(
            normalized,
            @"(?<![a-z0-9])(2ax|2sp|2bw|2gn|2sw|2gl|2kt|2rp|2gb|2km|2ff|bld2?|brs|plt)(?=[a-z0-9_/-]|$)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase))
        {
            var codeToken = match.Groups[1].Value;
            if (_manifestJobTokenToClassJobId.TryGetValue(codeToken, out classJobId))
                return true;
        }

        foreach (var token in EnumerateManifestClassificationTokens(normalized))
        {
            if (_manifestJobTokenToClassJobId.TryGetValue(token, out classJobId))
                return true;
        }

        return false;
    }

    private static bool ShouldForceGlobalManifestPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return true;

        return gamePath.Contains("/rol_common/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/bt_common/emote/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/bt_common/emote_sp/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/bt_common/resident/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/event_base/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("ui/", StringComparison.OrdinalIgnoreCase);
    }

    private static IEnumerable<string> EnumerateManifestClassificationTokens(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            yield break;

        foreach (var token in System.Text.RegularExpressions.Regex.Split(normalized, @"[^a-z0-9]+"))
        {
            if (!string.IsNullOrWhiteSpace(token))
                yield return token;
        }
    }

    private sealed class ManifestTransientImportGroup
    {
        public string ModScopeKey { get; set; } = string.Empty;
        public string FolderScopeKey { get; set; } = string.Empty;
        public string ImportScopeKey { get; set; } = string.Empty;
        public HashSet<string> GamePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
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

    private bool IsOwnedTrackedAddress(IntPtr actorAddress)
    {
        if (actorAddress == IntPtr.Zero)
            return false;

        foreach (var ptr in GetPlayerRelatedPointerSnapshot())
        {
            if (ptr.Address == actorAddress)
                return true;
        }

        return false;
    }

    private void Manager_PenumbraResourceLoadEvent(PenumbraResourceLoadMessage msg)
    {
        var gamePathRaw = msg.GamePath ?? string.Empty;
        var gameObjectAddress = msg.GameObject;
        var filePathRaw = msg.FilePath ?? string.Empty;

        var replacedGamePath = NormalizePath(gamePathRaw);
        if (string.IsNullOrWhiteSpace(replacedGamePath))
            return;

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

        bool hasResolvedReplacement = false;
        bool isGamePathSwap = false;

        try
        {
            if (!string.IsNullOrWhiteSpace(filePathForLookup))
            {
                var map = _fileCacheManager.GetFileCachesByPaths(new[] { filePathForLookup });
                if (map.TryGetValue(filePathForLookup, out var ent) && ent != null)
                {
                    hasResolvedReplacement =
                        !string.IsNullOrWhiteSpace(ent.ResolvedFilepath) &&
                        File.Exists(ent.ResolvedFilepath);

                    if (hasResolvedReplacement && !string.IsNullOrWhiteSpace(ent.Hash))
                    {
                        _transientHashByGamePath[replacedGamePath] = ent.Hash;
                    }
                }
            }
        }
        catch
        {
            // never break transient tracking
        }

        if (!string.IsNullOrWhiteSpace(filePath)
            && TryNormalizeManifestGamePath(filePath, out var swappedGamePath)
            && ShouldImportManifestGamePath(swappedGamePath)
            && !string.Equals(swappedGamePath, replacedGamePath, StringComparison.OrdinalIgnoreCase))
        {
            isGamePathSwap = true;
        }

        if (string.IsNullOrWhiteSpace(filePath))
            return;

        if (string.Equals(filePath, replacedGamePath, StringComparison.OrdinalIgnoreCase))
            return;

        ObjectKind preObjectKind = ObjectKind.Player;
        var preHasMappedKind = _cachedFrameAddresses.TryGetValue(gameObjectAddress, out preObjectKind);

        var prefix = GetFilePrefix(filePath);
        var modRoot = FindContainingModRoot(filePath);
        var hasLocalModScopedReplacement = IsLocalModScopedReplacementFile(filePath);

        if (!IsTransientRecording
            && preHasMappedKind
            && preObjectKind == ObjectKind.Player
            && !_inCombatSnapshot
            && ShouldTriggerAutoRecordSession(replacedGamePath, hasResolvedReplacement, isGamePathSwap, hasLocalModScopedReplacement))
        {
            GameObjectHandler? ownerForTrigger = null;
            foreach (var ptr in GetPlayerRelatedPointerSnapshot())
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

                Logger.LogDebug(
                    "Auto-record trigger detected: key={key}, trigger={trigger}, file={file}, prefix={prefix}, modRoot={modRoot}, collection={collection}, resolved={resolved}, swap={swap}, localModScoped={localModScoped}",
                    key ?? "<none>",
                    replacedGamePath,
                    filePath,
                    prefix ?? "<none>",
                    modRoot ?? "<none>",
                    collection ?? "<none>",
                    hasResolvedReplacement,
                    isGamePathSwap,
                    hasLocalModScopedReplacement);

                QueueAutoRecordTrigger(key, replacedGamePath, ownerForTrigger.Address, prefix, modRoot, collection);
            }
        }

        if (!IsTransientRecording && !hasResolvedReplacement && !isGamePathSwap)
        {
            Logger.LogTrace(
                "Ignoring transient candidate {gamePath} because {filePath} was neither a resolved replacement nor a fileswap",
                replacedGamePath, filePathForLookup);

            return;
        }

        lock (_cacheAdditionLock)
        {
            if (!IsTransientRecording)
            {
                if (_cachedHandledPaths.Contains(replacedGamePath)) return;
                _cachedHandledPaths.Add(replacedGamePath);
            }
        }

        bool isHandled =
            EndsWithAny(replacedGamePath, _handledFileTypes)
            || (IsTransientRecording && EndsWithAny(replacedGamePath, _handledRecordingFileTypes));

        if (!isHandled)
            return;

        ObjectKind objectKind = preObjectKind;
        var hasMappedKind = preHasMappedKind;

        if (!hasMappedKind && !IsTransientRecording)
            return;

        if (IsTransientRecording)
        {
            if (!ShouldAutoRecordVfxOnly(replacedGamePath))
                return;

            var isScopedAutoRecording = _recordingOwnerAddress != nint.Zero;

            if (isScopedAutoRecording)
            {
                if (hasMappedKind)
                {
                    if (gameObjectAddress != _recordingOwnerAddress)
                        return;
                }
                else
                {
                    if (gameObjectAddress != nint.Zero)
                        return;

                    var hasRecordingCollection = !string.IsNullOrWhiteSpace(_recordingCollection);
                    var hasEventCollection = !string.IsNullOrWhiteSpace(collection);

                    if (hasRecordingCollection
                        && hasEventCollection
                        && !string.Equals(collection, _recordingCollection, StringComparison.OrdinalIgnoreCase))
                    {
                        return;
                    }

                    var matchesRecordingCollection =
                        hasRecordingCollection
                        && hasEventCollection
                        && string.Equals(collection, _recordingCollection, StringComparison.OrdinalIgnoreCase);

                    var matchesRecordingPrefix =
                        !string.IsNullOrWhiteSpace(_recordingFilePrefix)
                        && !string.IsNullOrWhiteSpace(filePath)
                        && StartsWithNormalized(filePath, _recordingFilePrefix);

                    var matchesRecordingModRoot =
                        !string.IsNullOrWhiteSpace(_recordingModRoot)
                        && !string.IsNullOrWhiteSpace(filePath)
                        && StartsWithNormalized(filePath, _recordingModRoot);

                    var hasFileScope =
                        !string.IsNullOrWhiteSpace(_recordingFilePrefix)
                        || !string.IsNullOrWhiteSpace(_recordingModRoot);

                    if (hasFileScope)
                    {
                        if (!matchesRecordingPrefix && !matchesRecordingModRoot)
                            return;
                    }
                    else if (!matchesRecordingCollection)
                    {
                        return;
                    }
                }
            }
            else
            {
                if (!hasMappedKind)
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
            var addTransient = !alreadyTransient;

            _recordedTransients.Add(new TransientRecord(owner, replacedGamePath, filePath, alreadyTransient)
            {
                AddTransient = addTransient
            });

            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug(
                    "Auto-record captured: gamePath={gamePath}, file={file}, add={add}, alreadyTransient={already}, owner={owner}",
                    replacedGamePath,
                    filePath,
                    addTransient,
                    alreadyTransient,
                    owner);
            }
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

                var normalizedGamePath = NormalizePath(item.GamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath)
                    || IsKnownAmbientAutoRecordNoise(normalizedGamePath)
                    || !ShouldImportManifestGamePath(normalizedGamePath))
                {
                    Logger.LogTrace("Ignoring recorded invalid/noisy transient path {gamePath}", item.GamePath);
                    continue;
                }

                if (!TransientResources.TryGetValue(item.Owner.ObjectKind, out var transient))
                {
                    TransientResources[item.Owner.ObjectKind] = transient = [];
                }

                Logger.LogTrace("Adding recorded: {gamePath} => {filePath}", normalizedGamePath, item.FilePath);

                transient.Add(normalizedGamePath);
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
            || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase);
    }

    private bool ShouldTriggerAutoRecordSession(string normalizedGamePath, bool hasResolvedReplacement, bool isGamePathSwap, bool hasLocalModScopedReplacement)
    {
        if (!ShouldAutoRecordVfxOnly(normalizedGamePath))
            return false;

        if (EndsWithAny(normalizedGamePath, _handledFileTypes))
            return true;

        if ((hasResolvedReplacement || isGamePathSwap || hasLocalModScopedReplacement)
            && EndsWithAny(normalizedGamePath, _handledRecordingFileTypes)
            && (IsVfxRelatedResourcePath(normalizedGamePath) || IsPropRelatedResourcePath(normalizedGamePath)))
        {
            return true;
        }

        return false;
    }

    private bool ShouldAutoRecordVfxOnly(string normalizedGamePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedGamePath)) return false;
        if (IsKnownAmbientAutoRecordNoise(normalizedGamePath)) return false;

        if (IsEmoteKeyPath(normalizedGamePath))
            return true;

        if (IsSoundEffectPath(normalizedGamePath))
            return false;

        if (IsClothesOrBodyPath(normalizedGamePath)) return false;

        return IsVfxRelatedResourcePath(normalizedGamePath)
            || IsPropRelatedResourcePath(normalizedGamePath);
    }

    private static bool IsSoundEffectPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath)) return false;

        return gamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            && gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsKnownAmbientAutoRecordNoise(string normalizedGamePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedGamePath))
            return false;

        var path = NormalizePath(normalizedGamePath);

        return path.Contains("/eff/wh_breath", StringComparison.OrdinalIgnoreCase)
            || path.Contains("wh_breath", StringComparison.OrdinalIgnoreCase)

            || path.StartsWith("sound/foot/", StringComparison.OrdinalIgnoreCase)
            || path.Contains("/sound/foot/", StringComparison.OrdinalIgnoreCase);
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
    private void ClearAutoRecordedEmoteKeysForOptionChange()
    {
        lock (_autoRecordedKeyLock)
        {
            if (PlayerConfig.AutoRecordedEmoteKeys.Count == 0 && _autoRecordedEmoteKeysCache == null)
                return;

            PlayerConfig.AutoRecordedEmoteKeys.Clear();
            _autoRecordedEmoteKeysCache = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        Logger.LogDebug("Cleared auto-recorded emote key cache because local Penumbra settings changed");
        _configurationService.Save();
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

    private void TryStartAutoRecordSession(string? emoteKeyNormalized, string triggerPathNormalized, nint ownerAddress, string? filePrefix, string? modRoot, string? collection)
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
        var triggerKey = NormalizePath(triggerPathNormalized);

        if (normalizedKey == null && IsKnownAmbientAutoRecordNoise(triggerKey))
        {
            Logger.LogTrace("Ignoring ambient/noisy keyless auto-record trigger {trigger}", triggerKey);
            return;
        }

        if (normalizedKey == null && !string.IsNullOrWhiteSpace(triggerKey)
            && _zeroResultAutoRecordSuppressedUntilByTrigger.TryGetValue(triggerKey, out var suppressedUntil))
        {
            if (DateTime.UtcNow < suppressedUntil)
                return;

            _zeroResultAutoRecordSuppressedUntilByTrigger.TryRemove(triggerKey, out _);
        }

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
        _recordingModRoot = string.IsNullOrWhiteSpace(modRoot) ? null : NormalizePath(modRoot).TrimEnd('/') + "/";
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
                    else if (recorded == 0 && !string.IsNullOrWhiteSpace(triggerKey))
                    {
                        var suppressUntil = DateTime.UtcNow + _zeroResultAutoRecordSuppression;
                        _zeroResultAutoRecordSuppressedUntilByTrigger[triggerKey] = suppressUntil;

                        Logger.LogDebug(
                            "Suppressing keyless zero-result auto-record trigger {trigger} until {until}",
                            triggerKey,
                            suppressUntil);
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
                    _recordingModRoot = null;
                    _recordingCollection = null;
                    Interlocked.Exchange(ref _autoRecordRunning, 0);
                }
            }
        });
    }

    private void QueueAutoRecordTrigger(string? emoteKeyNormalized, string triggerPathNormalized, nint ownerAddress, string? filePrefix, string? modRoot, string? collection)
    {
        TryStartAutoRecordSession(emoteKeyNormalized, triggerPathNormalized, ownerAddress, filePrefix, modRoot, collection);
    }

    private void ProcessQueuedAutoRecordTriggers()
    {
        if (_autoRecordTriggerQueue.IsEmpty) return;

        (string? EmoteKey, string TriggerPath, nint OwnerAddress, string? FilePrefix, string? ModRoot, string? Collection) last = default;
        while (_autoRecordTriggerQueue.TryDequeue(out var item))
        {
            last = item;
        }

        if (string.IsNullOrWhiteSpace(last.TriggerPath)) return;
        if (last.OwnerAddress == nint.Zero) return;

        TryStartAutoRecordSession(last.EmoteKey, last.TriggerPath, last.OwnerAddress, last.FilePrefix, last.ModRoot, last.Collection);
    }


    private static bool IsLocalModScopedReplacementFile(string normalizedFilePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedFilePath))
            return false;

        try
        {
            var normalized = NormalizePath(normalizedFilePath);

            // Game paths are not local replacement files.
            if (TryNormalizeManifestGamePath(normalized, out _))
                return false;

            var diskPath = normalized.Replace('/', Path.DirectorySeparatorChar);

            if (!Path.IsPathRooted(diskPath))
                return false;

            return File.Exists(diskPath);
        }
        catch
        {
            return false;
        }
    }

    private static string? FindContainingModRoot(string normalizedFilePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedFilePath))
            return null;

        try
        {
            var path = NormalizePath(normalizedFilePath).Replace('/', Path.DirectorySeparatorChar);
            var dir = File.Exists(path) ? Path.GetDirectoryName(path) : Path.GetDirectoryName(path);

            for (var i = 0; i < 12 && !string.IsNullOrWhiteSpace(dir); i++)
            {
                if (File.Exists(Path.Combine(dir, "meta.json")))
                    return NormalizePath(dir).TrimEnd('/') + "/";

                dir = Path.GetDirectoryName(dir);
            }
        }
        catch
        {
            // Best effort only. Prefix matching still works without mod-root detection.
        }

        return null;
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