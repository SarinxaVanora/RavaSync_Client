using Dalamud.Plugin.Services;
using Lumina.Excel.Sheets;
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
using System.Text;
using System.Text.Json;

namespace RavaSync.FileCache;

public sealed class TransientResourceManager : DisposableMediatorSubscriberBase
{
    private static readonly string[] _manifestGamePathRoots = ["chara/", "vfx/", "bgcommon/", "sound/", "ui/", "shader/"];
    private static readonly JsonDocumentOptions _manifestJsonOptions = new()
    {
        AllowTrailingCommas = true,
        CommentHandling = JsonCommentHandling.Skip,
        MaxDepth = 128
    };

    private static readonly Dictionary<string, uint> _manifestJobTokenToClassJobId = new(StringComparer.OrdinalIgnoreCase)
    {
        ["pld"] = 19, ["paladin"] = 19, ["gladiator"] = 19, ["gla"] = 19, ["swd"] = 19, ["sld"] = 19, ["swd_knight"] = 19, ["swd_sld"] = 19, ["swd_emp"] = 19,
        ["mnk"] = 20, ["monk"] = 20, ["pugilist"] = 20, ["pgl"] = 20, ["clw"] = 20, ["clw_monk"] = 20, ["clw_clw"] = 20,
        ["war"] = 21, ["warrior"] = 21, ["marauder"] = 21, ["mrd"] = 21, ["2ax"] = 21, ["2ax_warrior"] = 21, ["2ax_emp"] = 21,
        ["drg"] = 22, ["dragoon"] = 22, ["lancer"] = 22, ["lnc"] = 22, ["2sp"] = 22, ["2sp_dragoon"] = 22, ["2sp_emp"] = 22, ["2sp_sld"] = 22,
        ["brd"] = 23, ["bard"] = 23, ["archer"] = 23, ["arc"] = 23, ["2bw"] = 23, ["2bw_bard"] = 23, ["2bw_emp"] = 23,
        ["whm"] = 24, ["whitemage"] = 24, ["conjurer"] = 24, ["cnj"] = 24, ["cnj_white"] = 24, ["stf"] = 24, ["stf_sld"] = 24, ["stf_emp"] = 24, ["2st"] = 24, ["2st_emp"] = 24,
        ["blm"] = 25, ["blackmage"] = 25, ["thaumaturge"] = 25, ["thm"] = 25, ["thm_black"] = 25, ["jst"] = 25, ["jst_sld"] = 25, ["jst_emp"] = 25, ["2js"] = 25, ["2js_emp"] = 25,
        ["smn"] = 27, ["summoner"] = 27,
        ["sch"] = 28, ["scholar"] = 28,
        ["nin"] = 30, ["ninja"] = 30, ["rogue"] = 30, ["rog"] = 30, ["dgr"] = 30, ["dgr_ninja"] = 30, ["dgr_dgr"] = 30,
        ["mch"] = 31, ["machinist"] = 31, ["2gn"] = 31, ["2gn_machin"] = 31, ["2gn_machinist"] = 31, ["2gn_emp"] = 31,
        ["drk"] = 32, ["drkr"] = 32, ["darkknight"] = 32, ["2sw"] = 32, ["2sw_dark"] = 32, ["2sw_emp"] = 32,
        ["ast"] = 33, ["astrologian"] = 33, ["2gl"] = 33, ["2gl_astro"] = 33, ["2gl_emp"] = 33,
        ["sam"] = 34, ["samurai"] = 34, ["2kt"] = 34, ["2kt_samrai"] = 34, ["2kt_samurai"] = 34, ["2kt_emp"] = 34,
        ["rdm"] = 35, ["redmage"] = 35, ["2rp"] = 35, ["2rp_redmage"] = 35, ["2rp_emp"] = 35,
        ["blu"] = 36, ["bluemage"] = 36, ["rod_aoz"] = 36,
        ["gnb"] = 37, ["gunbreaker"] = 37, ["2gb"] = 37, ["2gb_bgb"] = 37, ["2gb_emp"] = 37,
        ["dnc"] = 38, ["dancer"] = 38, ["chk"] = 38, ["chk_rdc"] = 38, ["chk_chk"] = 38,
        ["rpr"] = 39, ["rrp"] = 39, ["reaper"] = 39, ["riaper"] = 39, ["2km"] = 39, ["2km_reaper"] = 39, ["2km_riaper"] = 39, ["2km_emp"] = 39,
        ["sge"] = 40, ["sage"] = 40, ["2ff"] = 40, ["2ff_sage"] = 40, ["2ff_emp"] = 40,
        ["vpr"] = 41, ["viper"] = 41, ["bld"] = 41, ["bld2"] = 41, ["bld_blademaster"] = 41, ["bld_bld"] = 41,
        ["pct"] = 42, ["pictomancer"] = 42, ["brs"] = 42, ["plt"] = 42, ["brs_pictomancer"] = 42, ["brs_plt"] = 42,
        ["bst"] = 43, ["beastmaster"] = 43,
    };
    private static readonly Dictionary<string, uint[]> _manifestSharedJobTokenToClassJobIds = new(StringComparer.OrdinalIgnoreCase)
    {
        ["2bk"] = [27, 28], ["2bk_emp"] = [27, 28], ["swl_summon"] = [27, 28],
    };
    private static readonly Dictionary<uint, uint[]> _runtimeClassJobScopeAliases = new()
    {
        // Only base classes fan out to their promoted jobs. Promoted jobs intentionally do not pull
        // the base-class scope back in, otherwise normal job rebuilds can drag unrelated transient
        // paths into every character build and cause repeated tiny rebuild/publish churn.
        [1] = [1, 19],       // Gladiator -> Paladin
        [2] = [2, 20],       // Pugilist -> Monk
        [3] = [3, 21],       // Marauder -> Warrior
        [4] = [4, 22],       // Lancer -> Dragoon
        [5] = [5, 23],       // Archer -> Bard
        [6] = [6, 24],       // Conjurer -> White Mage
        [7] = [7, 25],       // Thaumaturge -> Black Mage
        [26] = [26, 27, 28], // Arcanist -> Summoner / Scholar
        [29] = [29, 30],     // Rogue -> Ninja
    };
    private const string _manifestDirectJobTokenPattern = "swd_knight|swd_sld|swd_emp|clw_monk|clw_clw|2ax_warrior|2ax_emp|2sp_dragoon|2sp_emp|2sp_sld|2bw_bard|2bw_emp|cnj_white|stf_sld|stf_emp|2st_emp|thm_black|jst_sld|jst_emp|2js_emp|swl_summon|2bk_emp|dgr_ninja|dgr_dgr|2gn_machinist|2gn_machin|2gn_emp|2sw_dark|2sw_emp|2gl_astro|2gl_emp|2kt_samurai|2kt_samrai|2kt_emp|2rp_redmage|2rp_emp|rod_aoz|2gb_bgb|2gb_emp|chk_rdc|chk_chk|2km_reaper|2km_riaper|2km_emp|2ff_sage|2ff_emp|bld_blademaster|bld_bld|bld2?|brs_pictomancer|brs_plt|2ax|2sp|2bw|2gn|2sw|2gl|2kt|2rp|2gb|2km|2ff|2bk|brs|plt|chk|clw|swd|sld|stf|2st|jst|2js|dgr";
    private readonly object _cacheAdditionLock = new();
    private readonly object _playerRelatedPointersLock = new();
    private readonly object _semiTransientResourcesLock = new();
    private readonly object _transientResourcesLock = new();
    private readonly object _recordedTransientsLock = new();
    private readonly object _sendTransientLock = new();
    private readonly object _playerConfigIdentityLock = new();
    private readonly object _playerOwnedResourceBaselineLock = new();
    private readonly object _targetedManifestRefreshLock = new();

    private readonly TransientConfigService _configurationService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IpcManager _ipcManager;
    private readonly IDataManager _gameData;
    private readonly object _modelCharaObjectKindIndexLock = new();
    private ModelCharaObjectKindIndex? _modelCharaObjectKindIndex;

    private readonly string[] _handledFileTypes = ["tmb", "tmb2", "pap", "avfx", "atex", "sklb", "eid", "phy", "phyb", "pbd", "scd", "skp", "shpk", "kdb"];
    private readonly string[] _handledRecordingFileTypes = ["tex", "mdl", "mtrl"];
    private static readonly string[] _manifestSupportFileTypes = ["mdl", "mtrl", "tex"];
    private readonly string[] _autoRecordTriggerFileTypes = ["tmb", "tmb2", "pap", "avfx", "atex", "scd"];
    private const string StartupPrimeManifestRuleVersion = "startup-manifest-prime-v17-explicit-inactive-options";
    private static readonly TimeSpan _autoRecordQuietWindow = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan _autoRecordMaximumWindow = TimeSpan.FromMilliseconds(1700);
    private static readonly TimeSpan _autoRecordRecentBaselineLifetime = TimeSpan.FromMilliseconds(2500);
    private static readonly TimeSpan _autoRecordSnapshotProbeDelay = TimeSpan.FromMilliseconds(150);
    private const int AutoRecordSnapshotProbeCount = 6;
    private static readonly TimeSpan _playerOwnedResourceBaselineRefreshInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan _appearanceChangeAutoRecordSuppressWindow = TimeSpan.FromMilliseconds(2500);
    private const int StartupPrimeApplyBatchSize = 96;
    private const int StartupPrimeManifestYieldEvery = 6;
    private static readonly TimeSpan StartupPrimeModYieldDelay = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan StartupPrimeBatchYieldDelay = TimeSpan.FromMilliseconds(35);
    private static readonly TimeSpan TargetedManifestRefreshSettleDelay = TimeSpan.FromMilliseconds(750);

    private readonly HashSet<string> _cachedHandledPaths = new(StringComparer.Ordinal);
    private readonly HashSet<GameObjectHandler> _playerRelatedPointers = [];
    private Dictionary<nint, ObjectKind> _cachedFrameAddresses = new();
    private Dictionary<nint, GameObjectHandler> _cachedFrameOwners = new();
    private ConcurrentDictionary<ObjectKind, HashSet<string>>? _semiTransientResources;
    private readonly ConcurrentDictionary<ObjectKind, HashSet<string>> _transientResources = new();
    private readonly ConcurrentDictionary<ObjectKind, HashSet<string>> _pendingAutoRecordedSupportResources = new();
    private readonly ConcurrentDictionary<nint, ConcurrentDictionary<string, long>> _recentOwnedResourceLoadsByAddress = new();
    private readonly ConcurrentDictionary<string, string> _transientHashByGamePath = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _transientFilePathByGamePath = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<string, string> _cachedPlayerOwnedResourceFileByGamePath = new(StringComparer.OrdinalIgnoreCase);
    private string _cachedPlayerPersistentDataKey = string.Empty;

    private CancellationTokenSource _sendTransientCts = new();
    private int _playerOwnedResourceBaselineRefreshRunning;
    private long _nextPlayerOwnedResourceBaselineRefreshTick;
    private long _suppressAutoRecordUntilTick;
    private volatile ConcurrentBag<TransientRecord> _recordedTransients = new();
    private readonly object _autoRecordLock = new();
    private CancellationTokenSource _autoRecordCts = new();
    private volatile ConcurrentBag<TransientRecord> _autoRecordedTransients = new();
    private nint _autoRecordAddress;
    private ObjectKind _autoRecordObjectKind;
    private DateTimeOffset _autoRecordStartedAt;
    private HashSet<string> _autoRecordBaselinePaths = new(StringComparer.OrdinalIgnoreCase);
    private bool _autoRecordHasPlayerOwnedBaseline;
    private string _autoRecordTriggerPath = string.Empty;
    private int _isAutoRecording;
    private uint _lastClassJobId = uint.MaxValue;
    private int _isTransientRecording;
    private int _startupTransientPrimeQueued;
    private int _startupTransientPrimeCompleted;
    private CancellationTokenSource _startupTransientPrimeCts = new();
    private CancellationTokenSource _targetedManifestRefreshCts = new();
    private readonly Dictionary<string, PenumbraModSettingChangedMessage> _pendingTargetedManifestRefreshes = new(StringComparer.OrdinalIgnoreCase);
    private volatile TransientManifestPrimeProgressSnapshot _manifestPrimeProgress = TransientManifestPrimeProgressSnapshot.Idle;
    private int _manifestPrimeImportedPathCount;
    private int _manifestPrimePrunedPathCount;

    public TransientManifestPrimeProgressSnapshot ManifestPrimeProgress => _manifestPrimeProgress;
    public bool IsTransientRecording => Volatile.Read(ref _isTransientRecording) != 0;
    public ValueProgress<TimeSpan> RecordTimeRemaining { get; } = new();
    public IReadOnlyCollection<TransientRecord> RecordedTransients => _recordedTransients;

    public TransientResourceManager(ILogger<TransientResourceManager> logger, TransientConfigService configurationService,
        DalamudUtilService dalamudUtil, MareMediator mediator, FileCacheManager fileCacheManager, IpcManager ipcManager, IDataManager gameData) : base(logger, mediator)
    {
        _configurationService = configurationService;
        _dalamudUtil = dalamudUtil;
        _ipcManager = ipcManager;
        _gameData = gameData;

        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, Manager_PenumbraResourceLoadEvent);
        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) => QueueStartupTransientManifestPrime("PenumbraInitialized"));
        Mediator.Subscribe<PenumbraDirectoryChangedMessage>(this, (_) => QueueStartupTransientManifestPrime("PenumbraDirectoryChanged"));
        Mediator.Subscribe<PenumbraModSettingChangedMessage>(this, (msg) =>
        {
            SuppressAutoRecordingForAppearanceChange("PenumbraModSettingChanged");
            QueueTargetedTransientManifestRefresh(msg);
        });
        Mediator.Subscribe<ClassJobChangedMessage>(this, (_) => SuppressAutoRecordingForAppearanceChange("ClassJobChanged"));
        Mediator.Subscribe<GlamourerChangedMessage>(this, (msg) =>
        {
            if (msg.Address != nint.Zero
                && _cachedFrameAddresses.TryGetValue(msg.Address, out var kind)
                && kind == ObjectKind.Player)
            {
                SuppressAutoRecordingForAppearanceChange("GlamourerChanged");
            }
        });
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

        QueueStartupTransientManifestPrime("TransientResourceManagerStartup");
    }

    private string PlayerPersistentDataKey
    {
        get
        {
            if (TryGetCachedPlayerPersistentDataKey(out var key))
                return key;

            if (_dalamudUtil.IsOnFrameworkThread)
            {
                RefreshPlayerPersistentDataKeyFromFramework();
                if (TryGetCachedPlayerPersistentDataKey(out key))
                    return key;
            }

            return string.Empty;
        }
    }

    private TransientConfig.TransientPlayerConfig PlayerConfig
    {
        get
        {
            var key = PlayerPersistentDataKey;
            if (string.IsNullOrWhiteSpace(key))
            {
                Logger.LogTrace("Transient player config requested before local player identity was cached; using detached empty config");
                return new();
            }

            if (!_configurationService.Current.TransientConfigs.TryGetValue(key, out var transientConfig))
            {
                _configurationService.Current.TransientConfigs[key] = transientConfig = new();
            }

            return transientConfig;
        }
    }

    private bool TryGetCachedPlayerPersistentDataKey(out string key)
    {
        lock (_playerConfigIdentityLock)
        {
            key = _cachedPlayerPersistentDataKey;
            return !string.IsNullOrWhiteSpace(key);
        }
    }

    private void RefreshPlayerPersistentDataKeyFromFramework()
    {
        if (!_dalamudUtil.IsOnFrameworkThread)
            return;

        try
        {
            var playerName = _dalamudUtil.GetPlayerName();
            var homeWorldId = _dalamudUtil.GetHomeWorldId();
            if (string.IsNullOrWhiteSpace(playerName) || string.Equals(playerName, "--", StringComparison.Ordinal) || homeWorldId == 0)
                return;

            var nextKey = playerName + "_" + homeWorldId;
            var changed = false;
            lock (_playerConfigIdentityLock)
            {
                if (!string.Equals(_cachedPlayerPersistentDataKey, nextKey, StringComparison.Ordinal))
                {
                    _cachedPlayerPersistentDataKey = nextKey;
                    changed = true;
                }
            }

            if (changed)
            {
                lock (_semiTransientResourcesLock)
                {
                    _semiTransientResources = null;
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, "Failed to refresh transient player config identity");
        }
    }

    private ConcurrentDictionary<ObjectKind, HashSet<string>> SemiTransientResources
    {
        get
        {
            lock (_semiTransientResourcesLock)
            {
                return EnsureSemiTransientResourcesUnsafe();
            }
        }
    }

    public bool HasPendingTransients(ObjectKind kind)
    {
        lock (_transientResourcesLock)
        {
            return _transientResources.TryGetValue(kind, out var set) && set.Count > 0;
        }
    }

    public HashSet<string> GetSemiTransientResources(ObjectKind objectKind)
    {
        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            return semiTransientResources.TryGetValue(objectKind, out var values)
                ? new HashSet<string>(values, StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }

    public HashSet<string> GetActiveTransientResources(ObjectKind objectKind)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var semiValues))
                result.UnionWith(semiValues);
        }

        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var transientValues))
                result.UnionWith(transientValues);

            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingValues))
                result.UnionWith(pendingValues);
        }

        return result;
    }

    public HashSet<string> GetPendingTransientResources(ObjectKind objectKind)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var transientValues))
                result.UnionWith(transientValues);

            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingValues))
                result.UnionWith(pendingValues);
        }

        return result;
    }

    public HashSet<string> PrepareTransientResourcesForBuild(ObjectKind objectKind)
    {
        if (HasPendingTransients(objectKind))
            PersistTransientResources(objectKind);

        return GetActiveTransientResources(objectKind);
    }

    public void PersistTransientResources(ObjectKind objectKind)
    {
        List<string> transientResources;
        lock (_transientResourcesLock)
        {
            if (!_transientResources.TryGetValue(objectKind, out var resources) || resources.Count == 0)
                return;

            transientResources = resources.ToList();
            resources.Clear();
        }

        if (transientResources.Count == 0)
            return;

        HashSet<string> newlyAddedGamePaths;
        lock (_semiTransientResourcesLock)
        {
            var semiTransientResourcesByKind = EnsureSemiTransientResourcesUnsafe();
            if (!semiTransientResourcesByKind.TryGetValue(objectKind, out var semiTransientResources) || semiTransientResources == null)
            {
                semiTransientResourcesByKind[objectKind] = semiTransientResources = new(StringComparer.OrdinalIgnoreCase);
            }

            newlyAddedGamePaths = transientResources
                .Where(path => !semiTransientResources.Contains(path))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var gamePath in transientResources)
                semiTransientResources.Add(gamePath);
        }

        if (newlyAddedGamePaths.Count == 0)
            return;

        foreach (var gamePath in newlyAddedGamePaths)
        {
            if (objectKind == ObjectKind.Player)
            {
                PersistPlayerTransientPath(gamePath, TryGetKnownResolvedFilePath(gamePath));
            }
            else if (objectKind == ObjectKind.Pet)
            {
                PlayerConfig.SetPetPathScope(_dalamudUtil.ClassJobId, gamePath);
            }
            else if (objectKind == ObjectKind.MinionOrMount || objectKind == ObjectKind.Companion)
            {
                PlayerConfig.SetObjectPathScope(objectKind, gamePath);
            }
        }

        SaveTransientConfigNow(nameof(PersistTransientResources));
    }

    public void CleanUpSemiTransientResources(ObjectKind objectKind, List<FileReplacement>? fileReplacement = null)
    {
        if (fileReplacement == null)
        {
            lock (_semiTransientResourcesLock)
            {
                var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
                if (semiTransientResources.TryGetValue(objectKind, out var value))
                    value.Clear();
            }
            return;
        }

        var missingGamePaths = fileReplacement
            .Where(replacement => !replacement.HasFileReplacement)
            .SelectMany(replacement => replacement.GamePaths)
            .Select(NormalizePath)
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (missingGamePaths.Count == 0)
            return;

        var removed = 0;
        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var value))
                removed = value.RemoveWhere(missingGamePaths.Contains);
        }

        if (removed <= 0)
            return;

        foreach (var path in missingGamePaths)
            PlayerConfig.RemovePath(path, objectKind);

        SaveTransientConfigNow(nameof(CleanUpSemiTransientResources));
    }

    public void RemoveTransientResource(ObjectKind objectKind, string path)
    {
        var normalizedPath = NormalizePath(path);
        if (string.IsNullOrWhiteSpace(normalizedPath))
            return;

        var removed = false;
        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var resources))
                removed = resources.Remove(normalizedPath);
        }

        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var transient))
                removed |= transient.Remove(normalizedPath);

            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingSupport))
                removed |= pendingSupport.Remove(normalizedPath);
        }

        if (!removed)
            return;

        PlayerConfig.RemovePath(normalizedPath, objectKind);
        SaveTransientConfigNow(nameof(RemoveTransientResource));
    }

    internal bool AddTransientResource(ObjectKind objectKind, string item)
    {
        var normalizedItem = NormalizePath(item);
        if (string.IsNullOrWhiteSpace(normalizedItem))
            return false;

        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var semiTransient) && semiTransient.Contains(normalizedItem))
                return false;
        }

        lock (_transientResourcesLock)
        {
            if (!_transientResources.TryGetValue(objectKind, out var transientResource))
                _transientResources[objectKind] = transientResource = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            return transientResource.Add(normalizedItem);
        }
    }

    private bool AddPendingAutoRecordedSupportResource(ObjectKind objectKind, string item)
    {
        var normalizedItem = NormalizePath(item);
        if (string.IsNullOrWhiteSpace(normalizedItem))
            return false;

        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var semiTransient) && semiTransient.Contains(normalizedItem))
                return false;
        }

        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var transientResource) && transientResource.Contains(normalizedItem))
                return false;

            if (!_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingSupport))
                _pendingAutoRecordedSupportResources[objectKind] = pendingSupport = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            return pendingSupport.Add(normalizedItem);
        }
    }

    private void RemovePendingAutoRecordedSupportResources(ObjectKind objectKind, IEnumerable<string> gamePaths)
    {
        var removeSet = gamePaths
            .Select(NormalizePath)
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (removeSet.Count == 0)
            return;

        lock (_transientResourcesLock)
        {
            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingSupport))
                pendingSupport.RemoveWhere(removeSet.Contains);
        }
    }

    internal bool RegisterKnownTransientFilePath(string gamePath, string? resolvedFilePath)
    {
        var normalizedGamePath = NormalizePath(gamePath);
        var normalizedResolvedFilePath = NormalizeResolvedFilePath(resolvedFilePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalizedGamePath) || string.IsNullOrWhiteSpace(normalizedResolvedFilePath))
            return false;

        if (_transientFilePathByGamePath.TryGetValue(normalizedGamePath, out var existingResolvedFilePath)
            && string.Equals(existingResolvedFilePath, normalizedResolvedFilePath, StringComparison.Ordinal))
        {
            return false;
        }

        _transientFilePathByGamePath[normalizedGamePath] = normalizedResolvedFilePath;
        return true;
    }

    private string? TryGetKnownResolvedFilePath(string gamePath)
    {
        var normalizedGamePath = NormalizePath(gamePath);
        return _transientFilePathByGamePath.TryGetValue(normalizedGamePath, out var value) ? value : null;
    }

    private void BeginManifestPrimeProgress(bool targeted, int totalMods, string reason)
    {
        Interlocked.Exchange(ref _manifestPrimeImportedPathCount, 0);
        Interlocked.Exchange(ref _manifestPrimePrunedPathCount, 0);
        UpdateManifestPrimeProgress(targeted, "Preparing", reason, 0, Math.Max(0, totalMods));
    }

    private void UpdateManifestPrimeProgress(bool targeted, string phase, string currentMod, int scannedMods, int totalMods)
    {
        _manifestPrimeProgress = new TransientManifestPrimeProgressSnapshot(
            true,
            targeted,
            phase ?? string.Empty,
            NormalizeManifestProgressDisplayText(currentMod),
            Math.Max(0, scannedMods),
            Math.Max(0, totalMods),
            Math.Max(0, Volatile.Read(ref _manifestPrimeImportedPathCount)),
            Math.Max(0, Volatile.Read(ref _manifestPrimePrunedPathCount)));
    }

    private void FinishManifestPrimeProgress(bool targeted, string phase, string currentMod, int scannedMods, int totalMods)
    {
        _manifestPrimeProgress = new TransientManifestPrimeProgressSnapshot(
            false,
            targeted,
            phase ?? string.Empty,
            NormalizeManifestProgressDisplayText(currentMod),
            Math.Max(0, scannedMods),
            Math.Max(0, totalMods),
            Math.Max(0, Volatile.Read(ref _manifestPrimeImportedPathCount)),
            Math.Max(0, Volatile.Read(ref _manifestPrimePrunedPathCount)));
    }

    private static string NormalizeManifestProgressDisplayText(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.Replace('\\', '/').Trim();
        return normalized.Length <= 96 ? normalized : "…" + normalized[^95..];
    }

    public Dictionary<string, string> GetKnownResolvedFilePaths(ObjectKind objectKind, IEnumerable<string>? gamePaths = null)
    {
        var requested = gamePaths?
            .Select(NormalizePath)
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (requested != null && requested.Count == 0)
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var active = requested ?? GetActiveTransientResources(objectKind);
        var output = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var gamePath in active)
        {
            var normalizedGamePath = NormalizePath(gamePath);
            if (string.IsNullOrWhiteSpace(normalizedGamePath))
                continue;

            if (!_transientFilePathByGamePath.TryGetValue(normalizedGamePath, out var resolvedPath)
                && !PlayerConfig.TryGetPersistentResolvedFilePath(normalizedGamePath, out resolvedPath))
            {
                continue;
            }

            var normalizedResolvedPath = NormalizeResolvedFilePath(resolvedPath);
            if (string.IsNullOrWhiteSpace(normalizedResolvedPath)
                || string.Equals(normalizedResolvedPath, normalizedGamePath, StringComparison.OrdinalIgnoreCase)
                || !File.Exists(normalizedResolvedPath))
            {
                continue;
            }

            _transientFilePathByGamePath[normalizedGamePath] = normalizedResolvedPath;
            output[normalizedGamePath] = normalizedResolvedPath;
        }

        return output;
    }

    private void QueueStartupTransientManifestPrime(string reason)
    {
        if (Volatile.Read(ref _startupTransientPrimeCompleted) != 0)
            return;

        if (Interlocked.CompareExchange(ref _startupTransientPrimeQueued, 1, 0) != 0)
            return;

        CancellationToken token;
        lock (_sendTransientLock)
        {
            if (_startupTransientPrimeCts.IsCancellationRequested)
            {
                _startupTransientPrimeCts.Dispose();
                _startupTransientPrimeCts = new CancellationTokenSource();
            }

            token = _startupTransientPrimeCts.Token;
        }

        BeginManifestPrimeProgress(targeted: false, totalMods: 0, reason: reason);
        UpdateManifestPrimeProgress(targeted: false, "Waiting for Penumbra/local player", reason, 0, 0);

        _ = Task.Run(async () =>
        {
            try
            {
                // Wait for login, Penumbra and the local player's effective collection to settle.
                for (var attempt = 0; attempt < 12 && !token.IsCancellationRequested; attempt++)
                {
                    await Task.Delay(attempt == 0 ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(5), token).ConfigureAwait(false);

                    if (await TryPrimeStartupTransientManifestAsync(reason, token).ConfigureAwait(false))
                    {
                        Volatile.Write(ref _startupTransientPrimeCompleted, 1);
                        return;
                    }
                }

                FinishManifestPrimeProgress(targeted: false, "Skipped", "Penumbra/local player was not ready", 0, 0);
                Logger.LogTrace("Startup transient manifest prime skipped for {reason}; Penumbra/local player did not become ready before retry limit", reason);
            }
            catch (OperationCanceledException)
            {
                FinishManifestPrimeProgress(targeted: false, "Cancelled", string.Empty, 0, 0);
                // shutting down or replaced by a newer startup request
            }
            catch (Exception ex)
            {
                FinishManifestPrimeProgress(targeted: false, "Failed", string.Empty, 0, 0);
                Logger.LogDebug(ex, "Startup transient manifest prime failed for {reason}", reason);
            }
            finally
            {
                Volatile.Write(ref _startupTransientPrimeQueued, 0);
            }
        }, token);
    }

    private void QueueTargetedTransientManifestRefresh(PenumbraModSettingChangedMessage msg)
    {
        var modName = NormalizePath(msg.ModName ?? string.Empty);
        if (string.IsNullOrWhiteSpace(modName))
            return;

        CancellationToken token;
        lock (_targetedManifestRefreshLock)
        {
            var key = (msg.CollectionId == Guid.Empty ? "local" : msg.CollectionId.ToString("N")) + "|" + modName;
            _pendingTargetedManifestRefreshes[key] = msg;

            try
            {
                _targetedManifestRefreshCts.Cancel();
                _targetedManifestRefreshCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // shutdown/dispose race
            }

            _targetedManifestRefreshCts = new CancellationTokenSource();
            token = _targetedManifestRefreshCts.Token;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TargetedManifestRefreshSettleDelay, token).ConfigureAwait(false);

                List<PenumbraModSettingChangedMessage> requests;
                lock (_targetedManifestRefreshLock)
                {
                    requests = _pendingTargetedManifestRefreshes.Values.ToList();
                    _pendingTargetedManifestRefreshes.Clear();
                }

                if (requests.Count == 0)
                    return;

                await TryRefreshTargetedTransientManifestPacksAsync(requests, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                FinishManifestPrimeProgress(targeted: true, "Cancelled", string.Empty, 0, 0);
                // a newer Penumbra settings change replaced this targeted sweep
            }
            catch (Exception ex)
            {
                FinishManifestPrimeProgress(targeted: true, "Failed", string.Empty, 0, 0);
                Logger.LogDebug(ex, "Targeted transient manifest refresh failed after Penumbra mod-setting change for {modName}", msg.ModName);
            }
        }, token);
    }

    private async Task TryRefreshTargetedTransientManifestPacksAsync(IReadOnlyCollection<PenumbraModSettingChangedMessage> requests, CancellationToken token)
    {
        if (requests.Count == 0 || !_ipcManager.Penumbra.APIAvailable)
            return;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return;

        var localPlayer = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            RefreshPlayerPersistentDataKeyFromFramework();

            var ptr = _dalamudUtil.GetPlayerPtr();
            if (ptr == nint.Zero)
                return (Address: nint.Zero, ObjectIndex: -1);

            var obj = _dalamudUtil.CreateGameObject(ptr);
            return (Address: ptr, ObjectIndex: obj?.ObjectIndex ?? -1);
        }).ConfigureAwait(false);

        if (token.IsCancellationRequested || localPlayer.Address == nint.Zero || localPlayer.ObjectIndex < 0)
            return;

        var collectionState = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(Logger, localPlayer.ObjectIndex).ConfigureAwait(false);
        if (collectionState == null)
            return;

        var effectiveRequests = requests
            .Where(request => IsPenumbraModSettingRequestRelevantToLocalCollection(request, collectionState.Mods, collectionState.CollectionId))
            .ToList();

        if (effectiveRequests.Count == 0)
            return;

        var useFullEffectiveCollectionRefresh = effectiveRequests.Any(IsLikelyWholeModStateRefreshRequest)
            || effectiveRequests.Any(request => !TryResolveCollectionModForTargetedManifestRefresh(collectionState.Mods, request.ModName, out _, out _));
        var targetedMods = BuildTargetedManifestRefreshModList(collectionState.Mods, effectiveRequests, useFullEffectiveCollectionRefresh);
        if (targetedMods.Count == 0)
            return;

        BeginManifestPrimeProgress(targeted: true, totalMods: targetedMods.Count, reason: useFullEffectiveCollectionRefresh ? "Penumbra mod enable/disable" : "Penumbra mod setting change");

        var knownGamePaths = BuildStartupPrimeKnownGamePathsSnapshot();
        var importBatch = new List<ManifestTransientImportEntry>(StartupPrimeApplyBatchSize * 2);
        var manifestIndex = new StartupManifestPrimeSelectionIndex();
        var appliedManifestPriorityByGamePath = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var importedAny = false;
        var scannedMods = 0;

        foreach (var mod in targetedMods)
        {
            token.ThrowIfCancellationRequested();
            UpdateManifestPrimeProgress(targeted: true, useFullEffectiveCollectionRefresh ? "Refreshing effective collection" : "Scanning changed mod", mod.ModKey, scannedMods, targetedMods.Count);

            var modPath = ResolveModDirectory(modDirectory, mod.ModKey);
            if (!Directory.Exists(modPath))
            {
                scannedMods++;
                UpdateManifestPrimeProgress(targeted: true, "Skipping missing mod directory", mod.ModKey, scannedMods, targetedMods.Count);
                Logger.LogTrace("Skipping targeted transient manifest refresh for {mod}; directory did not exist at {path}", mod.ModKey, modPath);
                continue;
            }

            var importSelectedEntries = mod.ModState is { Enabled: true, Temporary: false };
            var settings = mod.ModState?.Settings ?? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var priority = mod.ModState?.Priority ?? 0;

            await ImportManifestEntriesAsync(modPath, settings, priority, importSelectedEntries, importBatch, knownGamePaths, manifestIndex, token).ConfigureAwait(false);
            scannedMods++;
            UpdateManifestPrimeProgress(targeted: true, useFullEffectiveCollectionRefresh ? "Refreshing effective collection" : "Scanning changed mod", mod.ModKey, scannedMods, targetedMods.Count);

            if (importBatch.Count >= StartupPrimeApplyBatchSize)
            {
                if (ApplyStartupManifestEntriesToTransientConfig(importBatch, useFullEffectiveCollectionRefresh ? "PenumbraModToggle:EffectiveCollectionManifest" : "PenumbraModSettingChanged:TargetedManifest", knownGamePaths, appliedManifestPriorityByGamePath))
                    importedAny = true;

                importBatch.Clear();
                await Task.Delay(StartupPrimeBatchYieldDelay, token).ConfigureAwait(false);
            }
        }

        if (importBatch.Count > 0)
        {
            if (ApplyStartupManifestEntriesToTransientConfig(importBatch, useFullEffectiveCollectionRefresh ? "PenumbraModToggle:EffectiveCollectionManifest" : "PenumbraModSettingChanged:TargetedManifest", knownGamePaths, appliedManifestPriorityByGamePath))
                importedAny = true;
        }

        var prunedExistingManifestEntries = PruneInvalidStartupManifestPaths(manifestIndex);
        if (!importedAny && !prunedExistingManifestEntries)
        {
            FinishManifestPrimeProgress(targeted: true, "No selected transient changes", string.Empty, scannedMods, targetedMods.Count);
            Logger.LogTrace("Targeted transient manifest refresh found no selected transient changes across {count} changed Penumbra mod(s)", scannedMods);
            return;
        }

        ClearStartupManifestPrimeFingerprint();
        SaveTransientConfigNow(nameof(TryRefreshTargetedTransientManifestPacksAsync));
        PublishLocalPlayerTransientManifestRefresh("PenumbraModSettingChanged:TransientManifest");
        FinishManifestPrimeProgress(targeted: true, "Completed", string.Empty, scannedMods, targetedMods.Count);

        Logger.LogDebug("Targeted transient manifest refresh completed for {count} changed Penumbra mod(s); imported={imported}, pruned={pruned}",
            scannedMods,
            importedAny,
            prunedExistingManifestEntries);
    }

    private static bool IsPenumbraModSettingRequestRelevantToLocalCollection(
        PenumbraModSettingChangedMessage request,
        Dictionary<string, IpcCallerPenumbra.PenumbraModSettingState> localEffectiveMods,
        Guid localEffectiveCollectionId)
    {
        if (request.CollectionId == Guid.Empty || request.CollectionId == localEffectiveCollectionId || request.Inherited)
            return true;
        return TryResolveCollectionModForTargetedManifestRefresh(localEffectiveMods, request.ModName, out _, out _);
    }

    private static bool IsLikelyWholeModStateRefreshRequest(PenumbraModSettingChangedMessage request)
    {
        var change = NormalizeManifestSelectionKey(request.Change ?? string.Empty);
        if (string.IsNullOrWhiteSpace(change))
            return false;

        return change.Contains("enabled", StringComparison.OrdinalIgnoreCase)
            || change.Contains("enable", StringComparison.OrdinalIgnoreCase)
            || change.Contains("disabled", StringComparison.OrdinalIgnoreCase)
            || change.Contains("disable", StringComparison.OrdinalIgnoreCase)
            || change.Contains("state", StringComparison.OrdinalIgnoreCase)
            || change.Contains("status", StringComparison.OrdinalIgnoreCase)
            || change.Contains("modstate", StringComparison.OrdinalIgnoreCase)
            || change.Contains("modenabled", StringComparison.OrdinalIgnoreCase)
            || change.Contains("moddisabled", StringComparison.OrdinalIgnoreCase)
            || change.Contains("priority", StringComparison.OrdinalIgnoreCase)
            || change.Contains("inherit", StringComparison.OrdinalIgnoreCase)
            || change.Contains("temporary", StringComparison.OrdinalIgnoreCase);
    }

    private static List<TargetedManifestRefreshMod> BuildTargetedManifestRefreshModList(
        Dictionary<string, IpcCallerPenumbra.PenumbraModSettingState> localEffectiveMods,
        IReadOnlyCollection<PenumbraModSettingChangedMessage> requests,
        bool fullEffectiveCollectionRefresh)
    {
        if (fullEffectiveCollectionRefresh)
        {
            return localEffectiveMods
                .OrderByDescending(mod => mod.Value.Priority)
                .ThenBy(mod => NormalizePath(mod.Key), StringComparer.OrdinalIgnoreCase)
                .Select(mod => new TargetedManifestRefreshMod(mod.Key, mod.Value))
                .ToList();
        }

        var output = new List<TargetedManifestRefreshMod>();
        var visitedMods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var request in requests)
        {
            if (!TryResolveCollectionModForTargetedManifestRefresh(localEffectiveMods, request.ModName, out var modKey, out var modState))
                modKey = request.ModName ?? string.Empty;

            var normalizedModKey = NormalizePath(modKey);
            if (string.IsNullOrWhiteSpace(normalizedModKey) || !visitedMods.Add(normalizedModKey))
                continue;

            output.Add(new TargetedManifestRefreshMod(modKey, modState));
        }

        return output;
    }

    private static bool TryResolveCollectionModForTargetedManifestRefresh(
        Dictionary<string, IpcCallerPenumbra.PenumbraModSettingState> mods,
        string? changedModName,
        out string modKey,
        out IpcCallerPenumbra.PenumbraModSettingState? modState)
    {
        modKey = string.Empty;
        modState = null;

        if (mods == null || mods.Count == 0 || string.IsNullOrWhiteSpace(changedModName))
            return false;

        if (mods.TryGetValue(changedModName, out var exactState))
        {
            modKey = changedModName;
            modState = exactState;
            return true;
        }

        var normalizedChanged = NormalizePath(changedModName);
        var changedLeaf = NormalizePath(Path.GetFileName(normalizedChanged));

        foreach (var kvp in mods)
        {
            var normalizedKey = NormalizePath(kvp.Key);
            if (string.Equals(normalizedKey, normalizedChanged, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizePath(Path.GetFileName(normalizedKey)), changedLeaf, StringComparison.OrdinalIgnoreCase))
            {
                modKey = kvp.Key;
                modState = kvp.Value;
                return true;
            }
        }

        return false;
    }

    private void ClearStartupManifestPrimeFingerprint()
    {
        var playerConfig = PlayerConfig;
        lock (playerConfig)
        {
            playerConfig.StartupManifestPrimeFingerprint = string.Empty;
        }
    }

    private void PublishLocalPlayerTransientManifestRefresh(string reason)
    {
        GameObjectHandler? playerHandler = null;
        lock (_playerRelatedPointersLock)
        {
            playerHandler = _playerRelatedPointers.FirstOrDefault(handler => handler.ObjectKind == ObjectKind.Player && handler.Address != nint.Zero);
        }

        if (playerHandler == null || playerHandler.Address == nint.Zero)
            return;

        Mediator.Publish(new ImmediatePlayerStatePublishMessage(playerHandler, reason));
    }

    private async Task RequestLocalPlayerRedrawAfterTransientManifestPrimeAsync(string reason, CancellationToken token)
    {
        if (!_ipcManager.Penumbra.APIAvailable || token.IsCancellationRequested)
            return;

        try
        {
            var playerAddress = await _dalamudUtil.GetPlayerPointerAsync().ConfigureAwait(false);
            if (playerAddress == nint.Zero || token.IsCancellationRequested)
                return;

            Mediator.Publish(new ArmRequestedPlayerPublishAfterRedrawMessage(playerAddress));
            Mediator.Publish(new PenumbraRedrawAddressMessage(playerAddress));
            Logger.LogTrace("Requested local player Penumbra redraw after transient manifest prime for {reason}", reason);
        }
        catch (OperationCanceledException)
        {
            // shutting down or replaced by a newer startup request
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Failed to request local player Penumbra redraw after transient manifest prime for {reason}", reason);
        }
    }

    private async Task<bool> TryPrimeStartupTransientManifestAsync(string reason, CancellationToken token)
    {
        if (!_ipcManager.Penumbra.APIAvailable)
            return false;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return false;

        var localPlayer = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            RefreshPlayerPersistentDataKeyFromFramework();

            var ptr = _dalamudUtil.GetPlayerPtr();
            if (ptr == nint.Zero)
                return (Address: nint.Zero, ObjectIndex: -1);

            var obj = _dalamudUtil.CreateGameObject(ptr);
            return (Address: ptr, ObjectIndex: obj?.ObjectIndex ?? -1);
        }).ConfigureAwait(false);

        if (token.IsCancellationRequested)
            return false;

        if (localPlayer.Address == nint.Zero || localPlayer.ObjectIndex < 0)
            return false;

        var collectionState = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(Logger, localPlayer.ObjectIndex).ConfigureAwait(false);
        if (collectionState == null)
            return false;

        var knownGamePaths = BuildStartupPrimeKnownGamePathsSnapshot();
        var startupPrimeFingerprint = BuildStartupManifestPrimeFingerprint(collectionState.Mods);
        var totalMods = collectionState.Mods.Count;
        if (knownGamePaths.Count > 0 && IsStartupManifestPrimeCurrent(startupPrimeFingerprint))
        {
            FinishManifestPrimeProgress(targeted: false, "Already up to date", string.Empty, totalMods, totalMods);
            await RequestLocalPlayerRedrawAfterTransientManifestPrimeAsync(reason, token).ConfigureAwait(false);
            Logger.LogTrace("Startup transient manifest prime skipped for {reason}; enabled collection state is already seeded", reason);
            return true;
        }

        BeginManifestPrimeProgress(targeted: false, totalMods: totalMods, reason: reason);

        var importBatch = new List<ManifestTransientImportEntry>(StartupPrimeApplyBatchSize * 2);
        var manifestIndex = new StartupManifestPrimeSelectionIndex();
        var appliedManifestPriorityByGamePath = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var scannedMods = 0;
        var scannedEnabledMods = 0;
        var importedAny = false;
        foreach (var mod in collectionState.Mods
            .OrderByDescending(m => m.Value.Enabled && !m.Value.Temporary)
            .ThenByDescending(m => m.Value.Priority))
        {
            token.ThrowIfCancellationRequested();
            UpdateManifestPrimeProgress(targeted: false, "Scanning Penumbra manifests", mod.Key, scannedMods, totalMods);

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
            {
                scannedMods++;
                UpdateManifestPrimeProgress(targeted: false, "Skipping missing mod directory", mod.Key, scannedMods, totalMods);
                continue;
            }

            var importSelectedEntries = mod.Value.Enabled && !mod.Value.Temporary;
            await ImportManifestEntriesAsync(modPath, mod.Value.Settings, mod.Value.Priority, importSelectedEntries, importBatch, knownGamePaths, manifestIndex, token).ConfigureAwait(false);
            scannedMods++;
            if (importSelectedEntries)
                scannedEnabledMods++;
            UpdateManifestPrimeProgress(targeted: false, "Scanning Penumbra manifests", mod.Key, scannedMods, totalMods);

            if (importBatch.Count >= StartupPrimeApplyBatchSize)
            {
                if (ApplyStartupManifestEntriesToTransientConfig(importBatch, reason, knownGamePaths, appliedManifestPriorityByGamePath))
                    importedAny = true;

                importBatch.Clear();
                await Task.Delay(StartupPrimeBatchYieldDelay, token).ConfigureAwait(false);
            }
            else if (scannedMods % StartupPrimeManifestYieldEvery == 0)
            {
                await Task.Delay(StartupPrimeModYieldDelay, token).ConfigureAwait(false);
            }
        }

        if (importBatch.Count > 0)
        {
            if (ApplyStartupManifestEntriesToTransientConfig(importBatch, reason, knownGamePaths, appliedManifestPriorityByGamePath))
                importedAny = true;
        }

        var prunedExistingManifestEntries = PruneInvalidStartupManifestPaths(manifestIndex);

        if (!importedAny)
        {
            var fingerprintChangedWithoutImports = SetStartupManifestPrimeFingerprint(startupPrimeFingerprint);
            if (fingerprintChangedWithoutImports || prunedExistingManifestEntries)
                SaveTransientConfigNow(nameof(TryPrimeStartupTransientManifestAsync));

            if (prunedExistingManifestEntries)
                PublishLocalPlayerTransientManifestRefresh("StartupTransientManifestPrime:TransientManifest");

            FinishManifestPrimeProgress(targeted: false, prunedExistingManifestEntries ? "Pruned stale transient entries" : "Already up to date", string.Empty, scannedMods, totalMods);
            await RequestLocalPlayerRedrawAfterTransientManifestPrimeAsync(reason, token).ConfigureAwait(false);
            Logger.LogTrace("Startup transient manifest prime found no new active manifest entries for {reason}", reason);
            return true;
        }

        var fingerprintChanged = SetStartupManifestPrimeFingerprint(startupPrimeFingerprint);
        SaveTransientConfigNow(nameof(TryPrimeStartupTransientManifestAsync));
        PublishLocalPlayerTransientManifestRefresh("StartupTransientManifestPrime:TransientManifest");
        FinishManifestPrimeProgress(targeted: false, "Completed", string.Empty, scannedMods, totalMods);
        await RequestLocalPlayerRedrawAfterTransientManifestPrimeAsync(reason, token).ConfigureAwait(false);

        Logger.LogDebug("Startup transient manifest prime completed for {reason}; scanned {mods} collection mods ({enabledMods} active), fingerprint changed={fingerprintChanged}", reason, scannedMods, scannedEnabledMods, fingerprintChanged);
        return true;
    }

    private bool PruneInvalidStartupManifestPaths(StartupManifestPrimeSelectionIndex manifestIndex)
    {
        var playerConfig = PlayerConfig;
        var removed = 0;

        lock (playerConfig)
        {
            removed += PruneInvalidStartupManifestGamePathsFromList(playerConfig.GlobalPersistentCache, manifestIndex);

            foreach (var kvp in playerConfig.JobSpecificCache.ToList())
            {
                removed += PruneInvalidStartupManifestGamePathsFromList(kvp.Value, manifestIndex);
                if (kvp.Value == null || kvp.Value.Count == 0)
                    playerConfig.JobSpecificCache.Remove(kvp.Key);
            }

            foreach (var kvp in playerConfig.JobSpecificPetCache.ToList())
            {
                removed += PruneInvalidStartupManifestGamePathsFromList(kvp.Value, manifestIndex);
                if (kvp.Value == null || kvp.Value.Count == 0)
                    playerConfig.JobSpecificPetCache.Remove(kvp.Key);
            }

            removed += PruneInvalidStartupManifestGamePathsFromList(playerConfig.MinionOrMountPersistentCache, manifestIndex);
            removed += PruneInvalidStartupManifestGamePathsFromList(playerConfig.CompanionPersistentCache, manifestIndex);

            removed += PruneInvalidStartupManifestResolvedPaths(playerConfig.PersistentResolvedFilePaths, manifestIndex);

            if (removed > 0)
                playerConfig.Canonicalize();
        }

        if (removed <= 0)
            return false;

        Interlocked.Add(ref _manifestPrimePrunedPathCount, removed);
        Logger.LogDebug("Pruned {count} stale startup manifest transient path(s) that are no longer valid, active, or selected", removed);
        return true;
    }

    private int PruneInvalidStartupManifestGamePathsFromList(List<string>? paths, StartupManifestPrimeSelectionIndex manifestIndex)
    {
        if (paths == null || paths.Count == 0)
            return 0;

        return paths.RemoveAll(path => ShouldPruneStartupManifestGamePath(path, manifestIndex));
    }

    private int PruneInvalidStartupManifestResolvedPaths(List<string>? paths, StartupManifestPrimeSelectionIndex manifestIndex)
    {
        if (paths == null || paths.Count == 0)
            return 0;

        return paths.RemoveAll(path => ShouldPruneStartupManifestResolvedPath(path, manifestIndex));
    }

    private bool ShouldPruneStartupManifestGamePath(string path, StartupManifestPrimeSelectionIndex manifestIndex)
    {
        var normalized = NormalizePath(path);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (IsManifestSupportPath(normalized) && !ShouldKeepPersistedManifestSupportPath(normalized))
            return true;

        return manifestIndex.ManifestGamePaths.Contains(normalized)
            && !manifestIndex.SelectedGamePaths.Contains(normalized);
    }

    private bool ShouldPruneStartupManifestResolvedPath(string path, StartupManifestPrimeSelectionIndex manifestIndex)
    {
        var normalized = NormalizePath(path);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (IsManifestSupportPath(normalized) && !ShouldKeepPersistedManifestSupportPath(normalized))
            return true;

        return manifestIndex.ManifestResolvedFilePaths.Contains(normalized)
            && !manifestIndex.SelectedResolvedFilePaths.Contains(normalized);
    }

    private bool ShouldKeepPersistedManifestSupportPath(string path)
    {
        if (TryInferLuminaPersistentObjectKind(path, out var objectKind) && objectKind != ObjectKind.Player)
            return ShouldImportObjectScopedManifestGamePath(path);

        return ShouldImportManifestSupportPath(path, string.Empty, string.Empty, scopeHasTransientTrigger: false);
    }

    private ObjectKind InferManifestEntryPersistentObjectKind(IEnumerable<ManifestTransientImportEntry> entries)
    {
        foreach (var entry in entries)
        {
            if (TryInferLuminaPersistentObjectKind(entry.GamePath, out var luminaObjectKind))
                return luminaObjectKind;
        }

        // Human animation/resource paths are always loaded by the player actor, even when the modpack
        // itself is a mount/minion pack. Mount rider/sitting animations therefore belong in the player
        // semi-transient state; putting them into MinionOrMount scope means the receiver applies them to
        // the vehicle actor instead of the rider and the custom seated pose never appears.
        foreach (var entry in entries)
        {
            if (IsPlayerActorManifestPath(entry.GamePath))
                return ObjectKind.Player;
        }

        foreach (var entry in entries)
        {
            if (IsLikelyMinionOrMountPersistentPath(entry.GamePath, entry.ResolvedFilePath, entry.ManifestValue))
                return ObjectKind.MinionOrMount;

            foreach (var scopeText in entry.ScopeTexts)
            {
                if (IsLikelyMinionOrMountPersistentPath(scopeText))
                    return ObjectKind.MinionOrMount;
            }
        }

        foreach (var entry in entries)
        {
            if (IsLikelyCompanionPersistentPath(entry.GamePath, entry.ResolvedFilePath, entry.ManifestValue))
                return ObjectKind.Companion;

            foreach (var scopeText in entry.ScopeTexts)
            {
                if (IsLikelyCompanionPersistentPath(scopeText))
                    return ObjectKind.Companion;
            }
        }

        return ObjectKind.Player;
    }

    private sealed class ModelCharaObjectKindIndex
    {
        public Dictionary<ModelCharaPathKey, ObjectKind> Exact { get; } = new();
        public Dictionary<ModelCharaModelKey, ObjectKind> ModelOnly { get; } = new();
    }

    private readonly record struct ModelCharaPathKey(string Root, uint Model, uint Base);
    private readonly record struct ModelCharaModelKey(string Root, uint Model);

    private bool TryInferLuminaPersistentObjectKind(string? gamePath, out ObjectKind objectKind)
    {
        objectKind = ObjectKind.Player;
        if (!TryExtractModelCharaPathIdentity(gamePath, out var root, out var model, out var baseId))
            return false;

        var index = EnsureModelCharaObjectKindIndex();
        if (baseId > 0 && index.Exact.TryGetValue(new ModelCharaPathKey(root, model, baseId), out objectKind))
            return true;

        if (index.ModelOnly.TryGetValue(new ModelCharaModelKey(root, model), out objectKind))
            return true;

        return false;
    }

    private ModelCharaObjectKindIndex EnsureModelCharaObjectKindIndex()
    {
        var cached = Volatile.Read(ref _modelCharaObjectKindIndex);
        if (cached != null)
            return cached;

        lock (_modelCharaObjectKindIndexLock)
        {
            if (_modelCharaObjectKindIndex != null)
                return _modelCharaObjectKindIndex;

            var index = new ModelCharaObjectKindIndex();
            try
            {
                var mountSheet = _gameData.GetExcelSheet<Mount>();
                if (mountSheet != null)
                {
                    foreach (var mount in mountSheet)
                        AddModelCharaObjectKind(index, mount.ModelChara.Value, ObjectKind.MinionOrMount);
                }

                // XIV's Companion sheet is the minion sheet, not RavaSync's battle-chocobo Companion object kind.
                var minionSheet = _gameData.GetExcelSheet<Companion>();
                if (minionSheet != null)
                {
                    foreach (var minion in minionSheet)
                        AddModelCharaObjectKind(index, minion.Model.Value, ObjectKind.MinionOrMount);
                }

                Logger.LogDebug("Built Lumina mount/minion ModelChara object-scope index ({exact} exact, {modelOnly} model-only)", index.Exact.Count, index.ModelOnly.Count);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Failed to build Lumina mount/minion ModelChara object-scope index; falling back to token-based transient scope inference");
            }

            _modelCharaObjectKindIndex = index;
            return index;
        }
    }

    private static void AddModelCharaObjectKind(ModelCharaObjectKindIndex index, ModelChara? modelChara, ObjectKind objectKind)
    {
        if (modelChara == null)
            return;

        var value = modelChara.Value;
        var model = Convert.ToUInt32(value.Model);
        var baseId = Convert.ToUInt32(value.Base);
        if (model == 0)
            return;

        foreach (var root in GetLikelyModelCharaPathRoots(value))
        {
            SetModelOnlyObjectKind(index.ModelOnly, new ModelCharaModelKey(root, model), objectKind);
            if (baseId > 0)
                SetExactObjectKind(index.Exact, new ModelCharaPathKey(root, model, baseId), objectKind);
        }
    }

    private static IEnumerable<string> GetLikelyModelCharaPathRoots(ModelChara modelChara)
    {
        var typeText = Convert.ToString(modelChara.Type) ?? string.Empty;
        if (typeText.Contains("monster", StringComparison.OrdinalIgnoreCase))
        {
            yield return "monster";
            yield break;
        }

        if (typeText.Contains("demihuman", StringComparison.OrdinalIgnoreCase))
        {
            yield return "demihuman";
            yield break;
        }

        // On some generated Lumina versions this is a numeric field rather than a friendly enum.
        // Mounts and minions overwhelmingly resolve through monster/demihuman paths; avoid human paths
        // because those are too easy to confuse with player body/gear paths.
        yield return "monster";
        yield return "demihuman";
    }

    private static void SetExactObjectKind(Dictionary<ModelCharaPathKey, ObjectKind> map, ModelCharaPathKey key, ObjectKind objectKind)
    {
        if (map.TryGetValue(key, out var existing) && existing != objectKind)
        {
            map.Remove(key);
            return;
        }

        map[key] = objectKind;
    }

    private static void SetModelOnlyObjectKind(Dictionary<ModelCharaModelKey, ObjectKind> map, ModelCharaModelKey key, ObjectKind objectKind)
    {
        if (map.TryGetValue(key, out var existing) && existing != objectKind)
        {
            map.Remove(key);
            return;
        }

        map[key] = objectKind;
    }

    private static bool TryExtractModelCharaPathIdentity(string? gamePath, out string root, out uint model, out uint baseId)
    {
        root = string.Empty;
        model = 0;
        baseId = 0;

        var normalized = NormalizePath(gamePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalized) || !normalized.StartsWith("chara/", StringComparison.OrdinalIgnoreCase))
            return false;

        var monsterMatch = System.Text.RegularExpressions.Regex.Match(normalized, @"^chara/monster/m(?<model>\d{4})(?:/|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (monsterMatch.Success)
        {
            root = "monster";
            model = uint.Parse(monsterMatch.Groups["model"].Value);
            baseId = ExtractModelCharaBaseId(normalized);
            return model > 0;
        }

        var demihumanMatch = System.Text.RegularExpressions.Regex.Match(normalized, @"^chara/demihuman/d(?<model>\d{4})(?:/|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (demihumanMatch.Success)
        {
            root = "demihuman";
            model = uint.Parse(demihumanMatch.Groups["model"].Value);
            baseId = ExtractModelCharaBaseId(normalized);
            return model > 0;
        }

        return false;
    }

    private static uint ExtractModelCharaBaseId(string normalizedPath)
    {
        var baseMatch = System.Text.RegularExpressions.Regex.Match(normalizedPath, @"(?:^|[/_])(?:b|e)(?<base>\d{4})(?=$|[/_.])", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return baseMatch.Success ? uint.Parse(baseMatch.Groups["base"].Value) : 0;
    }

    private static List<ManifestTransientImportEntry> BuildEffectivePriorityManifestEntries(IReadOnlyCollection<ManifestTransientImportEntry> manifestEntries, Dictionary<string, int> appliedManifestPriorityByGamePath)
    {
        var effectiveEntries = new List<ManifestTransientImportEntry>();
        var entriesByGamePath = manifestEntries
            .Where(entry => !string.IsNullOrWhiteSpace(entry.GamePath))
            .GroupBy(entry => NormalizePath(entry.GamePath), StringComparer.OrdinalIgnoreCase);

        foreach (var group in entriesByGamePath)
        {
            var gamePath = NormalizePath(group.Key);
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            var maxPriority = group.Max(entry => entry.Priority);
            if (appliedManifestPriorityByGamePath.TryGetValue(gamePath, out var alreadyAppliedPriority) && alreadyAppliedPriority > maxPriority)
                continue;

            if (!appliedManifestPriorityByGamePath.TryGetValue(gamePath, out alreadyAppliedPriority) || maxPriority > alreadyAppliedPriority)
                appliedManifestPriorityByGamePath[gamePath] = maxPriority;

            var usedFileIdentities = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in group.Where(entry => entry.Priority == maxPriority))
            {
                var fileIdentity = GetManifestImportFileIdentity(entry);
                if (!string.IsNullOrWhiteSpace(fileIdentity))
                {
                    var jobScopedIdentity = fileIdentity;
                    if (TryResolveManifestEntryClassJobIds(entry, out var jobIds) && jobIds.Count > 0)
                        jobScopedIdentity += "|jobs:" + string.Join(",", jobIds.OrderBy(id => id));

                    if (!usedFileIdentities.Add(jobScopedIdentity))
                        continue;
                }

                effectiveEntries.Add(entry);
            }
        }

        return effectiveEntries;
    }

    private static string FindManifestEntryGamePathForResolvedFilePath(IReadOnlyCollection<ManifestTransientImportEntry> entries, string resolvedFilePath)
    {
        if (string.IsNullOrWhiteSpace(resolvedFilePath))
            return string.Empty;

        var normalizedResolvedFilePath = NormalizeResolvedFilePath(resolvedFilePath);
        return entries.FirstOrDefault(entry => string.Equals(NormalizeResolvedFilePath(entry.ResolvedFilePath), normalizedResolvedFilePath, StringComparison.OrdinalIgnoreCase))?.GamePath ?? string.Empty;
    }

    private bool ApplyStartupManifestEntriesToTransientConfig(IReadOnlyCollection<ManifestTransientImportEntry> manifestEntries, string reason, HashSet<string> knownGamePaths, Dictionary<string, int> appliedManifestPriorityByGamePath)
    {
        var effectiveEntries = BuildEffectivePriorityManifestEntries(manifestEntries, appliedManifestPriorityByGamePath);
        var targetGlobalGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var targetJobGamePaths = new Dictionary<uint, HashSet<string>>();
        var targetJobsByGamePath = new Dictionary<string, HashSet<uint>>(StringComparer.OrdinalIgnoreCase);
        var targetObjectGamePaths = new Dictionary<ObjectKind, HashSet<string>>();
        var resolvedFilePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var resolvedFilePathMapUpdates = 0;

        foreach (var group in effectiveEntries.GroupBy(entry => NormalizePath(entry.GamePath), StringComparer.OrdinalIgnoreCase))
        {
            var gamePath = NormalizePath(group.Key);
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            var jobsForPath = new HashSet<uint>();
            foreach (var entry in group)
            {
                var resolvedPath = NormalizeResolvedFilePath(entry.ResolvedFilePath);
                if (!string.IsNullOrWhiteSpace(resolvedPath) && !resolvedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                {
                    resolvedFilePaths.Add(resolvedPath);
                    if (RegisterKnownTransientFilePath(gamePath, resolvedPath))
                        resolvedFilePathMapUpdates++;
                }

                if (TryResolveManifestEntryClassJobIds(entry, out var jobIds))
                {
                    foreach (var jobId in jobIds)
                        jobsForPath.Add(jobId);
                }
            }

            var persistentObjectKind = InferManifestEntryPersistentObjectKind(group);
            if (persistentObjectKind != ObjectKind.Player)
            {
                if (!targetObjectGamePaths.TryGetValue(persistentObjectKind, out var objectPaths))
                {
                    objectPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    targetObjectGamePaths[persistentObjectKind] = objectPaths;
                }

                objectPaths.Add(gamePath);
                continue;
            }

            if (jobsForPath.Count > 0)
            {
                targetJobsByGamePath[gamePath] = jobsForPath;
                foreach (var jobId in jobsForPath)
                {
                    if (!targetJobGamePaths.TryGetValue(jobId, out var jobPaths))
                    {
                        jobPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        targetJobGamePaths[jobId] = jobPaths;
                    }

                    jobPaths.Add(gamePath);
                }

                continue;
            }

            targetGlobalGamePaths.Add(gamePath);
        }

        var playerConfig = PlayerConfig;
        var addedGlobal = 0;
        var addedJob = 0;
        var addedObject = 0;
        var registeredResolved = 0;
        var movedScope = 0;

        lock (playerConfig)
        {
            foreach (var gamePath in targetGlobalGamePaths.OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
            {
                var changedThisPath = false;
                var removedObjectScoped = playerConfig.RemovePath(gamePath, ObjectKind.MinionOrMount)
                    + playerConfig.RemovePath(gamePath, ObjectKind.Companion);
                if (removedObjectScoped > 0)
                {
                    movedScope += removedObjectScoped;
                    changedThisPath = true;
                }

                if (!playerConfig.GlobalPersistentCache.Contains(gamePath, StringComparer.OrdinalIgnoreCase))
                {
                    playerConfig.GlobalPersistentCache.Add(gamePath);
                    addedGlobal++;
                    changedThisPath = true;
                }

                foreach (var jobCache in playerConfig.JobSpecificCache.Values)
                {
                    if (jobCache == null)
                        continue;

                    if (jobCache.RemoveAll(path => string.Equals(path, gamePath, StringComparison.OrdinalIgnoreCase)) > 0)
                        changedThisPath = true;
                }

                if (changedThisPath)
                    knownGamePaths.Add(gamePath);
            }

            foreach (var kvp in targetJobGamePaths.OrderBy(k => k.Key))
            {
                foreach (var gamePath in kvp.Value.OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
                {
                    if (targetGlobalGamePaths.Contains(gamePath))
                        continue;

                    var changedThisPath = false;
                    var removedObjectScoped = playerConfig.RemovePath(gamePath, ObjectKind.MinionOrMount)
                        + playerConfig.RemovePath(gamePath, ObjectKind.Companion);
                    if (removedObjectScoped > 0)
                    {
                        movedScope += removedObjectScoped;
                        changedThisPath = true;
                    }

                    if (playerConfig.GlobalPersistentCache.RemoveAll(path => string.Equals(path, gamePath, StringComparison.OrdinalIgnoreCase)) > 0)
                    {
                        movedScope++;
                        changedThisPath = true;
                    }

                    var targetJobsForThisPath = targetJobsByGamePath.TryGetValue(gamePath, out var scopedJobs)
                        ? scopedJobs
                        : new HashSet<uint> { kvp.Key };

                    foreach (var otherJobCache in playerConfig.JobSpecificCache.Where(cache => !targetJobsForThisPath.Contains(cache.Key)).Select(cache => cache.Value))
                    {
                        if (otherJobCache == null)
                            continue;

                        if (otherJobCache.RemoveAll(path => string.Equals(path, gamePath, StringComparison.OrdinalIgnoreCase)) > 0)
                        {
                            movedScope++;
                            changedThisPath = true;
                        }
                    }

                    if (!playerConfig.JobSpecificCache.TryGetValue(kvp.Key, out var existingJobList) || existingJobList == null)
                    {
                        playerConfig.JobSpecificCache[kvp.Key] = existingJobList = [];
                    }

                    if (!existingJobList.Contains(gamePath, StringComparer.OrdinalIgnoreCase))
                    {
                        existingJobList.Add(gamePath);
                        addedJob++;
                        changedThisPath = true;
                    }

                    if (changedThisPath)
                        knownGamePaths.Add(gamePath);
                }
            }

            foreach (var kvp in targetObjectGamePaths.OrderBy(k => k.Key))
            {
                foreach (var gamePath in kvp.Value.OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
                {
                    var beforePlayerScopedCount = playerConfig.GlobalPersistentCache.Count + playerConfig.JobSpecificCache.Values.Sum(list => list?.Count ?? 0);
                    var changedObjectScope = playerConfig.SetObjectPathScope(kvp.Key, gamePath);
                    var afterPlayerScopedCount = playerConfig.GlobalPersistentCache.Count + playerConfig.JobSpecificCache.Values.Sum(list => list?.Count ?? 0);

                    if (!changedObjectScope)
                        continue;

                    addedObject++;
                    knownGamePaths.Add(gamePath);
                    if (afterPlayerScopedCount < beforePlayerScopedCount)
                        movedScope += beforePlayerScopedCount - afterPlayerScopedCount;
                }
            }

            foreach (var entry in effectiveEntries)
            {
                var normalizedGamePath = NormalizePath(entry.GamePath);
                var normalizedResolved = NormalizeResolvedFilePath(entry.ResolvedFilePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath)
                    || string.IsNullOrWhiteSpace(normalizedResolved)
                    || normalizedResolved.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (playerConfig.RegisterPersistentResolvedFilePath(normalizedGamePath, normalizedResolved))
                    registeredResolved++;
            }

            if (addedGlobal > 0 || addedJob > 0 || addedObject > 0 || registeredResolved > 0 || movedScope > 0)
                playerConfig.Canonicalize();
        }

        if (addedGlobal == 0 && addedJob == 0 && addedObject == 0 && registeredResolved == 0 && movedScope == 0 && resolvedFilePathMapUpdates == 0)
        {
            Logger.LogTrace("Startup transient manifest prime found no new transient config paths for {reason}", reason);
            return false;
        }

        Interlocked.Add(ref _manifestPrimeImportedPathCount, addedGlobal + addedJob + addedObject);
        Logger.LogDebug("Startup transient manifest prime imported {count} paths for {reason} ({global} global, {job} job-specific, {objectScoped} object-scoped, {resolved} resolved file paths, {mapUpdates} resolved map updates, {moved} moved between scopes)",
            addedGlobal + addedJob + addedObject, reason, addedGlobal, addedJob, addedObject, registeredResolved, resolvedFilePathMapUpdates, movedScope);

        return true;
    }

    private bool IsStartupManifestPrimeCurrent(string fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
            return false;

        var playerConfig = PlayerConfig;
        lock (playerConfig)
        {
            return string.Equals(playerConfig.StartupManifestPrimeFingerprint, fingerprint, StringComparison.OrdinalIgnoreCase);
        }
    }

    private bool SetStartupManifestPrimeFingerprint(string fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
            return false;

        var playerConfig = PlayerConfig;
        lock (playerConfig)
        {
            if (string.Equals(playerConfig.StartupManifestPrimeFingerprint, fingerprint, StringComparison.OrdinalIgnoreCase))
                return false;

            playerConfig.StartupManifestPrimeFingerprint = fingerprint;
            return true;
        }
    }

    private static string BuildStartupManifestPrimeFingerprint(Dictionary<string, IpcCallerPenumbra.PenumbraModSettingState> mods)
    {
        if (mods == null || mods.Count == 0)
            return string.Empty;

        var rows = mods
            .Where(mod => mod.Value.Enabled && !mod.Value.Temporary)
            .OrderBy(mod => NormalizePath(mod.Key), StringComparer.OrdinalIgnoreCase)
            .Select(mod => NormalizePath(mod.Key) + "|" + mod.Value.Priority + "|" + BuildStartupManifestPrimeSettingsFingerprint(mod.Value.Settings))
            .ToList();

        if (rows.Count == 0)
            return string.Empty;

        var source = StartupPrimeManifestRuleVersion + "\n" + string.Join('\n', rows);
        return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(source)));
    }

    private static string BuildStartupManifestPrimeSettingsFingerprint(Dictionary<string, List<string>>? settings)
    {
        if (settings == null || settings.Count == 0)
            return string.Empty;

        return string.Join(";", settings
            .OrderBy(kvp => NormalizePath(kvp.Key), StringComparer.OrdinalIgnoreCase)
            .Select(kvp => NormalizePath(kvp.Key) + "=" + string.Join(",", (kvp.Value ?? [])
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .Select(NormalizePath)
                .OrderBy(v => v, StringComparer.OrdinalIgnoreCase))));
    }

    private HashSet<string> BuildStartupPrimeKnownGamePathsSnapshot()
    {
        var known = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var playerConfig = PlayerConfig;
            lock (playerConfig)
            {
                foreach (var path in playerConfig.GlobalPersistentCache)
                    AddKnownStartupPrimePath(known, path);

                foreach (var list in playerConfig.JobSpecificCache.Values)
                {
                    foreach (var path in list)
                        AddKnownStartupPrimePath(known, path);
                }

                foreach (var list in playerConfig.JobSpecificPetCache.Values)
                {
                    foreach (var path in list)
                        AddKnownStartupPrimePath(known, path);
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, "Failed to snapshot existing transient paths before startup prime");
        }

        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            foreach (var values in semiTransientResources.Values)
            {
                foreach (var path in values)
                    AddKnownStartupPrimePath(known, path);
            }
        }

        return known;
    }

    private static void AddKnownStartupPrimePath(HashSet<string> known, string? path)
    {
        var normalized = NormalizePath(path ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(normalized))
            known.Add(normalized);
    }

    private void PrimeTransientPaths(IntPtr actorAddress, ObjectKind kind, IReadOnlyCollection<string> gamePaths)
    {
        if (actorAddress == IntPtr.Zero || gamePaths == null || gamePaths.Count == 0)
            return;

        if (!IsOwnedTrackedAddress(actorAddress))
        {
            Logger.LogTrace("Ignoring transient prime for non-owned actor {address:X}", actorAddress);
            return;
        }

        var addedAny = false;
        foreach (var path in gamePaths)
        {
            var normalizedPath = NormalizePath(path);
            if (string.IsNullOrWhiteSpace(normalizedPath))
                continue;

            if (!EndsWithAny(normalizedPath, _handledFileTypes) && !EndsWithAny(normalizedPath, _handledRecordingFileTypes))
                continue;

            addedAny |= AddTransientResource(kind, normalizedPath);
        }

        if (addedAny)
            SendTransients(actorAddress, kind);
    }

    internal void ClearTransientPaths(ObjectKind objectKind, List<string> list)
    {
        if (list == null || list.Count == 0)
            return;

        list.RemoveAll(entry => EndsWithAny(NormalizePath(entry), _handledRecordingFileTypes)
            || (objectKind == ObjectKind.Pet && NormalizePath(entry).EndsWith("sklb", StringComparison.OrdinalIgnoreCase)));

        if (list.Count == 0)
            return;

        var removeSet = list.Select(NormalizePath).Where(path => !string.IsNullOrWhiteSpace(path)).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (removeSet.Count == 0)
            return;

        var removedTransient = false;
        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var set))
                removedTransient = set.RemoveWhere(removeSet.Contains) > 0;

            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingSupport))
                removedTransient |= pendingSupport.RemoveWhere(removeSet.Contains) > 0;
        }

        var removedSemiTransientPaths = new List<string>();
        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (objectKind == ObjectKind.Player && semiTransientResources.TryGetValue(objectKind, out var semiSet))
            {
                removedSemiTransientPaths = semiSet.Where(removeSet.Contains).ToList();
                if (removedSemiTransientPaths.Count > 0)
                    semiSet.RemoveWhere(removeSet.Contains);
            }
        }

        foreach (var file in removedSemiTransientPaths)
            PlayerConfig.RemovePath(file, objectKind);

        if (removedTransient || removedSemiTransientPaths.Count > 0)
            SaveTransientConfigNow(nameof(ClearTransientPaths));
    }

    public void RebuildSemiTransientResources()
    {
        lock (_semiTransientResourcesLock)
        {
            _semiTransientResources = null;
            EnsureSemiTransientResourcesUnsafe();
        }
    }

    private void DalamudUtil_FrameworkUpdate()
    {
        RefreshPlayerPersistentDataKeyFromFramework();

        if (_lastClassJobId != _dalamudUtil.ClassJobId)
            UpdateClassJobCache();

        Dictionary<nint, ObjectKind> addresses = [];
        Dictionary<nint, GameObjectHandler> owners = [];
        GameObjectHandler[] pointerSnapshot;
        lock (_playerRelatedPointersLock)
        {
            pointerSnapshot = _playerRelatedPointers.ToArray();
        }

        foreach (var ptr in pointerSnapshot)
        {
            if (ptr.Address == IntPtr.Zero)
                continue;

            addresses[ptr.Address] = ptr.ObjectKind;
            owners[ptr.Address] = ptr;
        }

        _cachedFrameAddresses = addresses;
        _cachedFrameOwners = owners;
        CleanupAbsentObjects(addresses);
        QueuePlayerOwnedResourceBaselineRefresh();

        lock (_cacheAdditionLock)
        {
            if (_cachedHandledPaths.Count > 4096)
                _cachedHandledPaths.Clear();
        }
    }

    private void UpdateClassJobCache()
    {
        _lastClassJobId = _dalamudUtil.ClassJobId;
        SuppressAutoRecordingForAppearanceChange("ClassJobChanged");
        lock (_semiTransientResourcesLock)
        {
            _semiTransientResources = null;
            EnsureSemiTransientResourcesUnsafe();
        }
    }

    private void QueuePlayerOwnedResourceBaselineRefresh()
    {
        if (!_ipcManager.Penumbra.APIAvailable || Volatile.Read(ref _isAutoRecording) != 0 || IsTransientRecording)
            return;

        var nowTick = Environment.TickCount64;
        if (nowTick < Interlocked.Read(ref _nextPlayerOwnedResourceBaselineRefreshTick))
            return;

        if (Interlocked.CompareExchange(ref _playerOwnedResourceBaselineRefreshRunning, 1, 0) != 0)
            return;

        Interlocked.Exchange(ref _nextPlayerOwnedResourceBaselineRefreshTick, nowTick + (long)_playerOwnedResourceBaselineRefreshInterval.TotalMilliseconds);

        _ = Task.Run(async () =>
        {
            try
            {
                var snapshot = await CapturePlayerOwnedResourceSnapshotAsync().ConfigureAwait(false);
                if (snapshot.Count == 0)
                    return;

                lock (_playerOwnedResourceBaselineLock)
                {
                    _cachedPlayerOwnedResourceFileByGamePath = snapshot;
                }
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Failed to refresh player-owned resource baseline");
            }
            finally
            {
                Interlocked.Exchange(ref _playerOwnedResourceBaselineRefreshRunning, 0);
            }
        });
    }

    private Dictionary<string, string> SnapshotCachedPlayerOwnedResourceBaseline()
    {
        lock (_playerOwnedResourceBaselineLock)
        {
            return new Dictionary<string, string>(_cachedPlayerOwnedResourceFileByGamePath, StringComparer.OrdinalIgnoreCase);
        }
    }

    private async Task<Dictionary<string, string>> CapturePlayerOwnedResourceSnapshotAsync()
    {
        var rawSnapshot = await _ipcManager.Penumbra.GetPlayerResourcePathsAsync(Logger).ConfigureAwait(false);
        return FlattenPlayerOwnedResourceSnapshot(rawSnapshot);
    }

    private Dictionary<string, string> FlattenPlayerOwnedResourceSnapshot(Dictionary<ushort, Dictionary<string, HashSet<string>>>? rawSnapshot)
    {
        var output = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (rawSnapshot == null || rawSnapshot.Count == 0)
            return output;

        foreach (var objectResources in rawSnapshot.Values)
        {
            if (objectResources == null || objectResources.Count == 0)
                continue;

            foreach (var kvp in objectResources)
            {
                var resolvedPath = NormalizePath(ExtractResolvedFilePath(kvp.Key ?? string.Empty));
                if (string.IsNullOrWhiteSpace(resolvedPath))
                    continue;

                if (kvp.Value == null)
                    continue;

                foreach (var rawGamePath in kvp.Value)
                {
                    var gamePath = NormalizePath(rawGamePath);
                    if (string.IsNullOrWhiteSpace(gamePath) || output.ContainsKey(gamePath))
                        continue;

                    output[gamePath] = resolvedPath;
                }
            }
        }

        return output;
    }

    private void SuppressAutoRecordingForAppearanceChange(string reason)
    {
        Interlocked.Exchange(ref _suppressAutoRecordUntilTick, Environment.TickCount64 + (long)_appearanceChangeAutoRecordSuppressWindow.TotalMilliseconds);

        lock (_autoRecordLock)
        {
            if (Volatile.Read(ref _isAutoRecording) != 0)
            {
                Volatile.Write(ref _isAutoRecording, 0);
                _autoRecordedTransients = new ConcurrentBag<TransientRecord>();
                _autoRecordAddress = nint.Zero;
                _autoRecordBaselinePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _autoRecordHasPlayerOwnedBaseline = false;
                _autoRecordTriggerPath = string.Empty;
            }
        }

        try
        {
            _autoRecordCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        Logger.LogTrace("Suppressed auto transient capture for {ms}ms after {reason}", _appearanceChangeAutoRecordSuppressWindow.TotalMilliseconds, reason);
    }

    private bool IsAutoRecordingSuppressed()
    {
        var suppressUntil = Interlocked.Read(ref _suppressAutoRecordUntilTick);
        return suppressUntil > 0 && Environment.TickCount64 < suppressUntil;
    }

    private void CleanupAbsentObjects(IReadOnlyDictionary<nint, ObjectKind> presentAddresses)
    {
        var presentKinds = presentAddresses.Values.ToHashSet();
        lock (_transientResourcesLock)
        {
            foreach (var kind in Enum.GetValues<ObjectKind>())
            {
                if (!presentKinds.Contains(kind) && _transientResources.TryRemove(kind, out _))
                    Logger.LogDebug("Object not present anymore: {kind}", kind);
            }
        }
    }

    private bool IsOwnedTrackedAddress(nint actorAddress)
    {
        if (actorAddress == nint.Zero)
            return false;

        return _cachedFrameAddresses.ContainsKey(actorAddress);
    }

    private void Manager_PenumbraResourceLoadEvent(PenumbraResourceLoadMessage msg)
    {
        var gameObjectAddress = msg.GameObject;
        var replacedGamePath = NormalizePath(msg.GamePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(replacedGamePath))
            return;

        var frameAddresses = _cachedFrameAddresses;
        if (!frameAddresses.TryGetValue(gameObjectAddress, out var objectKind))
            return;

        var filePath = ExtractResolvedFilePath(msg.FilePath ?? string.Empty);
        var normalizedFilePath = NormalizePath(filePath);
        if (string.IsNullOrWhiteSpace(normalizedFilePath))
            return;

        if (string.Equals(normalizedFilePath, replacedGamePath, StringComparison.OrdinalIgnoreCase))
            return;

        var isManagedTransientType = EndsWithAny(replacedGamePath, _handledFileTypes);
        var isManualRecordingObservedType = IsTransientRecording
            && (isManagedTransientType || EndsWithAny(replacedGamePath, _handledRecordingFileTypes));
        var isAutoRecordTrigger = !IsTransientRecording && objectKind == ObjectKind.Player && IsAutoRecordTriggerPath(replacedGamePath);

        if (isAutoRecordTrigger)
            ArmAutoRecording(gameObjectAddress, objectKind, replacedGamePath);

        var isAutoRecordingObservedType = !IsTransientRecording
            && IsAutoRecordingFor(gameObjectAddress, objectKind)
            && IsAutoRecordCapturablePath(replacedGamePath)
            && !IsAutoRecordBaselinePath(replacedGamePath);

        if (!isManualRecordingObservedType && !isAutoRecordingObservedType)
        {
            RememberRecentOwnedResourceLoad(gameObjectAddress, replacedGamePath);
            return;
        }

        var dedupeKey = $"{gameObjectAddress:X}|{replacedGamePath}|{normalizedFilePath}";
        lock (_cacheAdditionLock)
        {
            if (!IsTransientRecording && !_cachedHandledPaths.Add(dedupeKey))
            {
                RememberRecentOwnedResourceLoad(gameObjectAddress, replacedGamePath);
                return;
            }
        }

        RegisterKnownTransientFilePath(replacedGamePath, normalizedFilePath);
        Mediator.Publish(new ObservedSupportResourceMessage(objectKind, replacedGamePath, normalizedFilePath, isManagedTransientType));

        var semiTransientContains = GetSemiTransientResources(objectKind).Contains(replacedGamePath);
        var transientContains = false;
        var added = false;
        lock (_transientResourcesLock)
        {
            if (!_transientResources.TryGetValue(objectKind, out var transientResources))
                _transientResources[objectKind] = transientResources = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            transientContains = transientResources.Contains(replacedGamePath);
            if (!IsTransientRecording && isManagedTransientType && !isAutoRecordingObservedType && !transientContains && !semiTransientContains)
                added = transientResources.Add(replacedGamePath);
        }

        if (added)
        {
            Logger.LogDebug("Adding transient {gamePath} for {kind} ({filePath})", replacedGamePath, objectKind, normalizedFilePath);
            SendTransients(gameObjectAddress, objectKind, TimeSpan.FromMilliseconds(650));
        }

        var alreadyTransient = transientContains || semiTransientContains;

        if (IsTransientRecording)
        {
            if (!_cachedFrameOwners.TryGetValue(gameObjectAddress, out var owner))
                return;

            _recordedTransients.Add(new TransientRecord(owner, replacedGamePath, normalizedFilePath, alreadyTransient) { AddTransient = !alreadyTransient });
            RememberRecentOwnedResourceLoad(gameObjectAddress, replacedGamePath);
            return;
        }

        if (!isAutoRecordingObservedType)
        {
            RememberRecentOwnedResourceLoad(gameObjectAddress, replacedGamePath);
            return;
        }

        if (!_cachedFrameOwners.TryGetValue(gameObjectAddress, out var autoOwner))
            return;

        _autoRecordedTransients.Add(new TransientRecord(autoOwner, replacedGamePath, normalizedFilePath, alreadyTransient) { AddTransient = !alreadyTransient });

        var addedFastPendingSupport = AddPendingAutoRecordedSupportResource(objectKind, replacedGamePath);
        if (addedFastPendingSupport)
            Logger.LogTrace("Queued transient support candidate for {kind}: {gamePath}", objectKind, replacedGamePath);

        TouchAutoRecording(gameObjectAddress, objectKind);
        RememberRecentOwnedResourceLoad(gameObjectAddress, replacedGamePath);
    }

    private void RememberRecentOwnedResourceLoad(nint gameObjectAddress, string gamePath)
    {
        if (gameObjectAddress == nint.Zero)
            return;

        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return;

        var nowTick = Environment.TickCount64;
        var perActor = _recentOwnedResourceLoadsByAddress.GetOrAdd(gameObjectAddress, _ => new ConcurrentDictionary<string, long>(StringComparer.OrdinalIgnoreCase));
        perActor[normalized] = nowTick;

        if (perActor.Count > 256)
        {
            var cutoff = nowTick - (long)_autoRecordRecentBaselineLifetime.TotalMilliseconds;
            foreach (var kvp in perActor.ToArray())
            {
                if (unchecked(nowTick - kvp.Value) < 0 || kvp.Value < cutoff)
                    perActor.TryRemove(kvp.Key, out _);
            }
        }
    }

    private HashSet<string> SnapshotRecentOwnedResourceLoads(nint gameObjectAddress, string? triggerPath)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (gameObjectAddress == nint.Zero || !_recentOwnedResourceLoadsByAddress.TryGetValue(gameObjectAddress, out var perActor))
            return output;

        var nowTick = Environment.TickCount64;
        var cutoff = nowTick - (long)_autoRecordRecentBaselineLifetime.TotalMilliseconds;
        var normalizedTrigger = NormalizePath(triggerPath ?? string.Empty);

        foreach (var kvp in perActor.ToArray())
        {
            if (unchecked(nowTick - kvp.Value) < 0 || kvp.Value < cutoff)
            {
                perActor.TryRemove(kvp.Key, out _);
                continue;
            }

            if (!string.IsNullOrWhiteSpace(normalizedTrigger) && string.Equals(kvp.Key, normalizedTrigger, StringComparison.OrdinalIgnoreCase))
                continue;

            output.Add(kvp.Key);
        }

        return output;
    }

    private bool IsAutoRecordBaselinePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return true;

        lock (_autoRecordLock)
        {
            if (string.Equals(normalized, _autoRecordTriggerPath, StringComparison.OrdinalIgnoreCase))
                return false;

            return _autoRecordBaselinePaths.Contains(normalized);
        }
    }

    private void ArmAutoRecording(nint gameObjectAddress, ObjectKind objectKind, string triggerPath)
    {
        if (gameObjectAddress == nint.Zero || objectKind != ObjectKind.Player || IsAutoRecordingSuppressed())
            return;

        var startedNewWindow = false;
        var hadPlayerOwnedBaseline = false;
        lock (_autoRecordLock)
        {
            startedNewWindow = Volatile.Read(ref _isAutoRecording) == 0 || _autoRecordAddress != gameObjectAddress || _autoRecordObjectKind != objectKind;
            if (startedNewWindow)
            {
                var playerOwnedBaseline = SnapshotCachedPlayerOwnedResourceBaseline();

                _autoRecordedTransients = new ConcurrentBag<TransientRecord>();
                _autoRecordStartedAt = DateTimeOffset.UtcNow;
                _autoRecordBaselinePaths = playerOwnedBaseline.Count > 0
                    ? playerOwnedBaseline.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase)
                    : SnapshotRecentOwnedResourceLoads(gameObjectAddress, triggerPath);
                _autoRecordHasPlayerOwnedBaseline = playerOwnedBaseline.Count > 0;
                _autoRecordTriggerPath = NormalizePath(triggerPath);
                hadPlayerOwnedBaseline = _autoRecordHasPlayerOwnedBaseline;
            }
            else
            {
                hadPlayerOwnedBaseline = _autoRecordHasPlayerOwnedBaseline;
            }

            _autoRecordAddress = gameObjectAddress;
            _autoRecordObjectKind = objectKind;
            Volatile.Write(ref _isAutoRecording, 1);
        }

        ScheduleAutoRecordingClose(gameObjectAddress, objectKind);
        Logger.LogTrace("Auto transient recorder {state} by {triggerPath}; quiet={quiet}ms cap={cap}ms source={source}",
            startedNewWindow ? "armed" : "extended",
            triggerPath,
            _autoRecordQuietWindow.TotalMilliseconds,
            _autoRecordMaximumWindow.TotalMilliseconds,
            hadPlayerOwnedBaseline ? "player-owned snapshot" : "recent events");
    }

    private void TouchAutoRecording(nint gameObjectAddress, ObjectKind objectKind)
    {
        if (!IsAutoRecordingFor(gameObjectAddress, objectKind))
            return;

        ScheduleAutoRecordingClose(gameObjectAddress, objectKind);
    }

    private void ScheduleAutoRecordingClose(nint gameObjectAddress, ObjectKind objectKind)
    {
        CancellationToken token;
        TimeSpan delay;
        lock (_autoRecordLock)
        {
            if (Volatile.Read(ref _isAutoRecording) == 0 || _autoRecordAddress != gameObjectAddress || _autoRecordObjectKind != objectKind)
                return;

            var elapsed = DateTimeOffset.UtcNow - _autoRecordStartedAt;
            var remainingCap = _autoRecordMaximumWindow - elapsed;
            delay = remainingCap <= TimeSpan.Zero
                ? TimeSpan.Zero
                : remainingCap < _autoRecordQuietWindow ? remainingCap : _autoRecordQuietWindow;

            try
            {
                _autoRecordCts.Cancel();
                _autoRecordCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // disposal race while shutting down
            }

            _autoRecordCts = new CancellationTokenSource();
            token = _autoRecordCts.Token;
        }

        if (delay <= TimeSpan.Zero)
        {
            SaveAutoRecording(gameObjectAddress, objectKind);
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
                SaveAutoRecording(gameObjectAddress, objectKind);
            }
            catch (OperationCanceledException)
            {
                // capture window extended or manager disposed
            }
        }, token);
    }

    private bool IsAutoRecordingFor(nint gameObjectAddress, ObjectKind objectKind)
    {
        if (Volatile.Read(ref _isAutoRecording) == 0)
            return false;

        return _autoRecordAddress == gameObjectAddress && _autoRecordObjectKind == objectKind;
    }

    private void SaveAutoRecording(nint gameObjectAddress, ObjectKind objectKind)
    {
        TransientRecord[] eventSnapshot;
        HashSet<string> baselinePaths;
        bool hasPlayerOwnedBaseline;
        string triggerPath;
        GameObjectHandler? owner;

        lock (_autoRecordLock)
        {
            if (Volatile.Read(ref _isAutoRecording) == 0 || _autoRecordAddress != gameObjectAddress || _autoRecordObjectKind != objectKind)
                return;

            Volatile.Write(ref _isAutoRecording, 0);
            eventSnapshot = _autoRecordedTransients.ToArray();
            baselinePaths = new HashSet<string>(_autoRecordBaselinePaths, StringComparer.OrdinalIgnoreCase);
            hasPlayerOwnedBaseline = _autoRecordHasPlayerOwnedBaseline;
            triggerPath = _autoRecordTriggerPath;
            _cachedFrameOwners.TryGetValue(gameObjectAddress, out owner);

            _autoRecordedTransients = new ConcurrentBag<TransientRecord>();
            _autoRecordAddress = nint.Zero;
            _autoRecordBaselinePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _autoRecordHasPlayerOwnedBaseline = false;
            _autoRecordTriggerPath = string.Empty;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await SaveAutoRecordingAsync(gameObjectAddress, objectKind, owner, eventSnapshot, baselinePaths, hasPlayerOwnedBaseline, triggerPath).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Auto transient recorder failed to save support paths");
            }
        });
    }

    private async Task SaveAutoRecordingAsync(nint gameObjectAddress, ObjectKind objectKind, GameObjectHandler? owner, TransientRecord[] eventSnapshot, HashSet<string> baselinePaths, bool hasPlayerOwnedBaseline, string triggerPath)
    {
        var combined = eventSnapshot.ToList();

        if (owner != null && hasPlayerOwnedBaseline && !IsAutoRecordingSuppressed())
        {
            var snapshotRecords = await CapturePlayerOwnedSnapshotDiffRecordsWithProbeAsync(owner, baselinePaths, triggerPath).ConfigureAwait(false);
            if (snapshotRecords.Count > 0)
                combined.AddRange(snapshotRecords);
        }

        if (combined.Count == 0)
            return;

        var addedTransients = SaveRecordedTransientSnapshot(combined, publishImmediately: false);
        RemovePendingAutoRecordedSupportResources(objectKind, combined.Select(item => item.GamePath));

        if (addedTransients.Count == 0 && combined.Any(item => !item.AlreadyTransient))
        {
            if (owner != null)
                addedTransients.Add(owner);
        }

        if (addedTransients.Count == 0)
            return;

        foreach (var item in addedTransients)
        {
            if (item.Address != nint.Zero)
                SendTransients(item.Address, item.ObjectKind, TimeSpan.FromMilliseconds(250));
        }

        Logger.LogDebug("Auto transient recorder saved {count} owner bucket(s) with new support transient(s), event={eventCount}, snapshotBaseline={snapshotBaseline}",
            addedTransients.Count,
            eventSnapshot.Length,
            hasPlayerOwnedBaseline);
    }

    private async Task<List<TransientRecord>> CapturePlayerOwnedSnapshotDiffRecordsWithProbeAsync(GameObjectHandler owner, HashSet<string> baselinePaths, string triggerPath)
    {
        var records = new List<TransientRecord>();
        var seenPaths = new HashSet<string>(baselinePaths, StringComparer.OrdinalIgnoreCase);
        var sawNewPath = false;

        for (var attempt = 0; attempt < AutoRecordSnapshotProbeCount && !IsAutoRecordingSuppressed(); attempt++)
        {
            if (attempt > 0)
                await Task.Delay(_autoRecordSnapshotProbeDelay).ConfigureAwait(false);

            var passRecords = await CapturePlayerOwnedSnapshotDiffRecordsAsync(owner, seenPaths, triggerPath).ConfigureAwait(false);
            if (passRecords.Count == 0)
            {
                if (sawNewPath && attempt >= 2)
                    break;

                continue;
            }

            sawNewPath = true;
            foreach (var record in passRecords)
            {
                var normalized = NormalizePath(record.GamePath);
                if (string.IsNullOrWhiteSpace(normalized) || !seenPaths.Add(normalized))
                    continue;

                records.Add(record);
            }
        }

        return records;
    }

    private async Task<List<TransientRecord>> CapturePlayerOwnedSnapshotDiffRecordsAsync(GameObjectHandler owner, HashSet<string> baselinePaths, string triggerPath)
    {
        var records = new List<TransientRecord>();
        if (owner == null || owner.ObjectKind != ObjectKind.Player || baselinePaths.Count == 0)
            return records;

        var afterSnapshot = await CapturePlayerOwnedResourceSnapshotAsync().ConfigureAwait(false);
        if (afterSnapshot.Count == 0)
            return records;

        var normalizedTrigger = NormalizePath(triggerPath);
        foreach (var kvp in afterSnapshot.OrderBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase))
        {
            var gamePath = NormalizePath(kvp.Key);
            var resolvedPath = NormalizePath(kvp.Value);

            if (string.IsNullOrWhiteSpace(gamePath)
                || string.IsNullOrWhiteSpace(resolvedPath)
                || baselinePaths.Contains(gamePath)
                || string.Equals(gamePath, normalizedTrigger, StringComparison.OrdinalIgnoreCase)
                || string.Equals(gamePath, resolvedPath, StringComparison.OrdinalIgnoreCase)
                || !IsAutoRecordCapturablePath(gamePath)
                || !CharacterDataPushSanitizer.IsServerAcceptedGamePath(gamePath)
                || IsKnownTransientPath(owner.ObjectKind, gamePath)
                || !File.Exists(resolvedPath))
            {
                continue;
            }

            RegisterKnownTransientFilePath(gamePath, resolvedPath);
            records.Add(new TransientRecord(owner, gamePath, resolvedPath, false) { AddTransient = true });
            AddPendingAutoRecordedSupportResource(owner.ObjectKind, gamePath);
        }

        if (records.Count > 0)
            Logger.LogDebug("Captured {count} player-owned on-screen transient support resource(s) by snapshot diff", records.Count);

        lock (_playerOwnedResourceBaselineLock)
        {
            _cachedPlayerOwnedResourceFileByGamePath = afterSnapshot;
        }

        return records;
    }

    private bool IsKnownTransientPath(ObjectKind objectKind, string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return true;

        lock (_semiTransientResourcesLock)
        {
            var semiTransientResources = EnsureSemiTransientResourcesUnsafe();
            if (semiTransientResources.TryGetValue(objectKind, out var semiTransient) && semiTransient.Contains(normalized))
                return true;
        }

        lock (_transientResourcesLock)
        {
            if (_transientResources.TryGetValue(objectKind, out var transient) && transient.Contains(normalized))
                return true;

            if (_pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pending) && pending.Contains(normalized))
                return true;
        }

        return false;
    }

    private void SendTransients(nint gameObject, ObjectKind objectKind, TimeSpan? debounceOverride = null)
    {
        var debounce = debounceOverride ?? TimeSpan.FromSeconds(5);
        CancellationToken token;
        lock (_sendTransientLock)
        {
            try
            {
                _sendTransientCts.Cancel();
                _sendTransientCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // disposal race while shutting down
            }

            _sendTransientCts = new CancellationTokenSource();
            token = _sendTransientCts.Token;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(debounce, token).ConfigureAwait(false);
                lock (_transientResourcesLock)
                {
                    var hasTransientResources = _transientResources.TryGetValue(objectKind, out var resources) && resources.Count > 0;
                    var hasPendingSupportResources = _pendingAutoRecordedSupportResources.TryGetValue(objectKind, out var pendingSupport) && pendingSupport.Count > 0;
                    if (!hasTransientResources && !hasPendingSupportResources)
                        return;
                }

                Logger.LogTrace("Sending transients for {kind}", objectKind);
                Mediator.Publish(new TransientResourceChangedMessage(gameObject));
            }
            catch (OperationCanceledException)
            {
                // newer transient event replaced the debounce
            }
        }, token);
    }

    public void StartRecording(CancellationToken token)
    {
        if (Interlocked.CompareExchange(ref _isTransientRecording, 1, 0) != 0)
            return;

        _recordedTransients = new ConcurrentBag<TransientRecord>();
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
                Volatile.Write(ref _isTransientRecording, 0);
            }
        }, CancellationToken.None);
    }

    public async Task WaitForRecording(CancellationToken token)
    {
        while (IsTransientRecording)
            await Task.Delay(TimeSpan.FromSeconds(1), token).ConfigureAwait(false);
    }

    internal void SaveRecording()
    {
        var snapshot = _recordedTransients.ToArray();
        _recordedTransients = new ConcurrentBag<TransientRecord>();
        if (snapshot.Length == 0)
            return;

        var addedTransients = SaveRecordedTransientSnapshot(snapshot, publishImmediately: true);
        foreach (var item in addedTransients)
            Mediator.Publish(new TransientResourceChangedMessage(item.Address));
    }

    private HashSet<GameObjectHandler> SaveRecordedTransientSnapshot(IEnumerable<TransientRecord> records, bool publishImmediately)
    {
        HashSet<GameObjectHandler> addedTransients = [];
        var persistedResolvedFilePaths = 0;
        var persistedGamePaths = 0;
        lock (_transientResourcesLock)
        {
            foreach (var item in records)
            {
                if (!item.AddTransient || item.AlreadyTransient)
                    continue;

                var normalizedGamePath = NormalizePath(item.GamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    continue;

                RegisterKnownTransientFilePath(normalizedGamePath, item.FilePath);
                if (PlayerConfig.RegisterPersistentResolvedFilePath(normalizedGamePath, item.FilePath))
                    persistedResolvedFilePaths++;

                if (!publishImmediately)
                {
                    if (item.Owner.ObjectKind == ObjectKind.Player)
                    {
                        PersistPlayerTransientPath(normalizedGamePath, item.FilePath);
                        persistedGamePaths++;
                    }
                    else if (item.Owner.ObjectKind == ObjectKind.Pet)
                    {
                        PlayerConfig.SetPetPathScope(_dalamudUtil.ClassJobId, normalizedGamePath);
                        persistedGamePaths++;
                    }
                    else if (item.Owner.ObjectKind == ObjectKind.MinionOrMount || item.Owner.ObjectKind == ObjectKind.Companion)
                    {
                        PlayerConfig.SetObjectPathScope(item.Owner.ObjectKind, normalizedGamePath);
                        persistedGamePaths++;
                    }
                }

                if (!_transientResources.TryGetValue(item.Owner.ObjectKind, out var transient))
                    _transientResources[item.Owner.ObjectKind] = transient = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (transient.Add(normalizedGamePath))
                    addedTransients.Add(item.Owner);
            }
        }

        if ((persistedResolvedFilePaths > 0 || persistedGamePaths > 0) && !publishImmediately)
            SaveTransientConfigNow(nameof(SaveRecordedTransientSnapshot));

        return addedTransients;
    }


    private int MoveLuminaObjectScopedPathsOutOfPlayerCaches(TransientConfig.TransientPlayerConfig playerConfig)
    {
        var moved = 0;
        lock (playerConfig)
        {
            foreach (var gamePath in playerConfig.GlobalPersistentCache.ToArray())
            {
                if (!TryInferLuminaPersistentObjectKind(gamePath, out var objectKind) || objectKind == ObjectKind.Player)
                    continue;

                if (playerConfig.SetObjectPathScope(objectKind, gamePath))
                    moved++;
            }

            foreach (var kvp in playerConfig.JobSpecificCache.ToList())
            {
                var list = kvp.Value;
                if (list == null || list.Count == 0)
                    continue;

                foreach (var gamePath in list.ToArray())
                {
                    if (!TryInferLuminaPersistentObjectKind(gamePath, out var objectKind) || objectKind == ObjectKind.Player)
                        continue;

                    if (playerConfig.SetObjectPathScope(objectKind, gamePath))
                        moved++;
                }
            }
        }

        return moved;
    }

    private ConcurrentDictionary<ObjectKind, HashSet<string>> EnsureSemiTransientResourcesUnsafe()
    {
        if (_semiTransientResources != null)
            return _semiTransientResources;

        _semiTransientResources = new();
        var playerConfig = PlayerConfig;
        playerConfig.Canonicalize();
        var movedLuminaObjectScopedPaths = MoveLuminaObjectScopedPathsOutOfPlayerCaches(playerConfig);
        if (movedLuminaObjectScopedPaths > 0)
        {
            try
            {
                _configurationService.Save();
                Logger.LogDebug("Moved {count} Lumina-classified mount/minion transient path(s) out of player scopes", movedLuminaObjectScopedPaths);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Failed to save transient config after Lumina object-scope migration");
            }
        }

        var runtimeClassJobIds = GetRuntimeClassJobScopeIds(_dalamudUtil.ClassJobId);

        var playerPaths = playerConfig.GlobalPersistentCache.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var classJobId in runtimeClassJobIds)
        {
            if (playerConfig.JobSpecificCache.TryGetValue(classJobId, out var jobSpecificData))
                playerPaths.UnionWith(jobSpecificData);
        }
        _semiTransientResources[ObjectKind.Player] = playerPaths;

        var petPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var classJobId in runtimeClassJobIds)
        {
            if (playerConfig.JobSpecificPetCache.TryGetValue(classJobId, out var petSpecificData))
                petPaths.UnionWith(petSpecificData);
        }
        _semiTransientResources[ObjectKind.Pet] = petPaths;
        _semiTransientResources[ObjectKind.MinionOrMount] = playerConfig.MinionOrMountPersistentCache.ToHashSet(StringComparer.OrdinalIgnoreCase);
        _semiTransientResources[ObjectKind.Companion] = playerConfig.CompanionPersistentCache.ToHashSet(StringComparer.OrdinalIgnoreCase);

        return _semiTransientResources;
    }

    private static IReadOnlyList<uint> GetRuntimeClassJobScopeIds(uint classJobId)
    {
        return _runtimeClassJobScopeAliases.TryGetValue(classJobId, out var aliases)
            ? aliases
            : [classJobId];
    }

    private void PersistPlayerTransientPath(string gamePath, string? resolvedFilePath)
    {
        var normalizedGamePath = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalizedGamePath))
            return;

        var playerConfig = PlayerConfig;
        if (TryResolveRuntimeTransientClassJobId(normalizedGamePath, resolvedFilePath, out var classJobId))
        {
            playerConfig.AddOrElevate(classJobId, normalizedGamePath);
            Logger.LogTrace("Persisted transient path as job-specific scope {classJobId}: {gamePath}", classJobId, normalizedGamePath);
        }
        else
        {
            playerConfig.SetPathScope(null, normalizedGamePath);
            Logger.LogTrace("Persisted transient path as global scope: {gamePath}", normalizedGamePath);
        }

        if (!string.IsNullOrWhiteSpace(resolvedFilePath))
            playerConfig.RegisterPersistentResolvedFilePath(normalizedGamePath, resolvedFilePath);
    }

    private static bool TryResolveRuntimeTransientClassJobId(string gamePath, string? resolvedFilePath, out uint classJobId)
    {
        var normalizedGamePath = NormalizePath(gamePath);
        var normalizedResolvedFilePath = NormalizeResolvedFilePath(resolvedFilePath ?? string.Empty);

        var entry = new ManifestTransientImportEntry(
            normalizedGamePath,
            normalizedResolvedFilePath,
            normalizedResolvedFilePath,
            0,
            string.IsNullOrWhiteSpace(normalizedResolvedFilePath) ? [] : [normalizedResolvedFilePath]);

        return TryResolveManifestEntryClassJobId(entry, out classJobId);
    }

    private void SaveTransientConfigNow(string source)
    {
        try
        {
            PlayerConfig.Canonicalize();
            _configurationService.Save();
            lock (_semiTransientResourcesLock)
            {
                _semiTransientResources = null;
                EnsureSemiTransientResourcesUnsafe();
            }
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Failed to save transient config from {source}", source);
        }
    }

    private static string ResolveModDirectory(string modDirectoryRoot, string modKey)
    {
        if (string.IsNullOrWhiteSpace(modKey))
            return modDirectoryRoot;

        if (Path.IsPathRooted(modKey))
            return Path.GetFullPath(modKey);

        return Path.GetFullPath(Path.Combine(modDirectoryRoot, modKey));
    }

    private async Task ImportManifestEntriesAsync(string modPath, Dictionary<string, List<string>> selectedSettings, int priority, bool importSelectedEntries, List<ManifestTransientImportEntry> manifestEntries, HashSet<string> knownGamePaths, StartupManifestPrimeSelectionIndex manifestIndex, CancellationToken token)
    {
        var manifestCounter = 0;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            TryImportManifestJson(modPath, defaultManifest, null, priority, importSelectedEntries, manifestEntries, knownGamePaths, manifestIndex, token);
            manifestCounter++;
        }

        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            TryImportManifestJson(modPath, groupManifest, selectedSettings, priority, importSelectedEntries, manifestEntries, knownGamePaths, manifestIndex, token);
            manifestCounter++;

            if (manifestCounter % StartupPrimeManifestYieldEvery == 0)
                await Task.Delay(StartupPrimeModYieldDelay, token).ConfigureAwait(false);
        }
    }

    private void TryImportManifestJson(string modPath, string manifestPath, Dictionary<string, List<string>>? selectedSettings, int priority, bool importSelectedEntries, List<ManifestTransientImportEntry> manifestEntries, HashSet<string> knownGamePaths, StartupManifestPrimeSelectionIndex manifestIndex, CancellationToken token)
    {
        try
        {
            ImportManifestJson(modPath, manifestPath, selectedSettings, priority, importSelectedEntries, manifestEntries, knownGamePaths, manifestIndex, token);
        }
        catch (JsonException ex)
        {
            Logger.LogDebug(ex, "Skipping malformed Penumbra manifest {manifestPath}", manifestPath);
        }
        catch (IOException ex)
        {
            Logger.LogDebug(ex, "Skipping unreadable Penumbra manifest {manifestPath}", manifestPath);
        }
        catch (UnauthorizedAccessException ex)
        {
            Logger.LogDebug(ex, "Skipping inaccessible Penumbra manifest {manifestPath}", manifestPath);
        }
    }

    private void ImportManifestJson(string modPath, string manifestPath, Dictionary<string, List<string>>? selectedSettings, int priority, bool importSelectedEntries, List<ManifestTransientImportEntry> manifestEntries, HashSet<string> knownGamePaths, StartupManifestPrimeSelectionIndex manifestIndex, CancellationToken token)
    {
        using var stream = File.OpenRead(manifestPath);
        using var doc = JsonDocument.Parse(stream, _manifestJsonOptions);
        var root = doc.RootElement;

        if (string.Equals(Path.GetFileName(manifestPath), "default_mod.json", StringComparison.OrdinalIgnoreCase))
        {
            var hasGroupManifests = Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly).Any();
            ImportManifestMappings(modPath, root, priority, importSelectedEntries, importSelectedEntries, manifestEntries, NormalizePath(manifestPath) + "|default", knownGamePaths, manifestIndex, skipPhysicalPrefixedEntries: hasGroupManifests);
            return;
        }

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        var selectedOptions = GetSelectedOptionNames(root, selectedSettings, manifestPath, out var explicitSelectionFound);
        if (!explicitSelectionFound)
            selectedOptions.UnionWith(GetDefaultSelectedAuxiliaryOptionNames(root));

        RemoveInactiveManifestOptionNames(selectedOptions);

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();

            var optionName = option.TryGetProperty("Name", out var optionNameElement)
                ? optionNameElement.GetString() ?? string.Empty
                : string.Empty;

            var optionSelected = importSelectedEntries && selectedOptions.Contains(optionName);
            ImportManifestMappings(modPath, option, priority, optionSelected, optionSelected, manifestEntries, NormalizePath(manifestPath) + "|" + NormalizePath(optionName), knownGamePaths, manifestIndex);
        }
    }

    private static HashSet<string> GetSelectedOptionNames(JsonElement groupRoot, Dictionary<string, List<string>>? selectedSettings, string manifestPath, out bool explicitSelectionFound)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var optionNames = GetManifestOptionNames(groupRoot);
        var groupType = groupRoot.TryGetProperty("Type", out var typeElement)
            ? typeElement.GetString() ?? string.Empty
            : string.Empty;

        if (selectedSettings != null && TryGetSelectedSettingsForManifestGroup(groupRoot, manifestPath, selectedSettings, out var selected))
        {
            explicitSelectionFound = true;
            AddSelectedOptionNamesFromSettingValues(output, optionNames, groupType, selected);
            RemoveInactiveManifestOptionNames(output);
            return output;
        }

        explicitSelectionFound = false;
        AddDefaultSelectedOptionNames(output, optionNames, groupType, groupRoot);
        RemoveInactiveManifestOptionNames(output);
        return output;
    }

    private static void RemoveInactiveManifestOptionNames(HashSet<string> optionNames)
    {
        if (optionNames.Count == 0)
            return;

        optionNames.RemoveWhere(IsInactiveManifestOptionName);
    }

    private static bool IsInactiveManifestOptionName(string? optionName)
    {
        if (string.IsNullOrWhiteSpace(optionName))
            return false;

        return NormalizeManifestSelectionKey(optionName) is "off" or "none" or "no" or "false" or "null" or "disabled" or "disable" or "inactive" or "notactive";
    }

    private static List<string> GetManifestOptionNames(JsonElement groupRoot)
    {
        var optionNames = new List<string>();
        if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return optionNames;

        foreach (var option in options.EnumerateArray())
        {
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var name = optionNameElement.GetString();
            if (!string.IsNullOrWhiteSpace(name))
                optionNames.Add(name);
        }

        return optionNames;
    }

    private static HashSet<string> GetDefaultSelectedAuxiliaryOptionNames(JsonElement groupRoot)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var optionNames = GetManifestOptionNames(groupRoot);
        if (optionNames.Count == 0)
            return output;

        var groupType = groupRoot.TryGetProperty("Type", out var typeElement)
            ? typeElement.GetString() ?? string.Empty
            : string.Empty;

        if (!string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase))
            return output;

        var defaultSelected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddDefaultSelectedOptionNames(defaultSelected, optionNames, groupType, groupRoot);
        if (defaultSelected.Count == 0)
            return output;

        if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return output;

        foreach (var option in options.EnumerateArray())
        {
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var optionName = optionNameElement.GetString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(optionName) || !defaultSelected.Contains(optionName))
                continue;

            if (IsManifestAuxiliaryTimelineOption(option))
                output.Add(optionName);
        }

        return output;
    }

    private static bool IsManifestAuxiliaryTimelineOption(JsonElement option)
    {
        if (!option.TryGetProperty("Files", out var files) || files.ValueKind != JsonValueKind.Object)
            return false;

        var hasAuxiliaryTimelineOrEffect = false;
        foreach (var property in files.EnumerateObject())
        {
            if (!TryNormalizeManifestGamePath(property.Name, out var gamePath))
                continue;

            if (IsActionAuxiliaryTimelinePath(gamePath))
            {
                hasAuxiliaryTimelineOrEffect = true;
                continue;
            }

            // If the option also carries animation/model payloads it is not just a small
            // timeline/effect dependency; leave normal selection handling in charge.
            if (gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return hasAuxiliaryTimelineOrEffect;
    }

    private static bool IsActionAuxiliaryTimelinePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized) || !normalized.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase))
            return false;

        return normalized.EndsWith("_z.tmb", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("_z.tmb2", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetSelectedSettingsForManifestGroup(JsonElement groupRoot, string manifestPath, Dictionary<string, List<string>> selectedSettings, out List<string>? selected)
    {
        selected = null;
        if (selectedSettings.Count == 0)
            return false;

        var aliases = BuildManifestGroupSelectionAliases(groupRoot, manifestPath);
        foreach (var alias in aliases.RawAliases)
        {
            if (selectedSettings.TryGetValue(alias, out selected))
                return true;
        }

        foreach (var kvp in selectedSettings)
        {
            var key = kvp.Key ?? string.Empty;
            if (aliases.NormalizedAliases.Contains(NormalizePath(key)) || aliases.LooseAliases.Contains(NormalizeManifestSelectionKey(key)))
            {
                selected = kvp.Value;
                return true;
            }
        }

        return false;
    }

    private static ManifestGroupSelectionAliases BuildManifestGroupSelectionAliases(JsonElement groupRoot, string manifestPath)
    {
        var rawAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var normalizedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var looseAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void AddAlias(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            var trimmed = value.Trim();
            rawAliases.Add(trimmed);
            normalizedAliases.Add(NormalizePath(trimmed));
            looseAliases.Add(NormalizeManifestSelectionKey(trimmed));
        }

        var groupName = groupRoot.TryGetProperty("Name", out var groupNameElement)
            ? groupNameElement.GetString() ?? string.Empty
            : string.Empty;
        AddAlias(groupName);

        var manifestFileName = Path.GetFileName(manifestPath);
        var manifestStem = Path.GetFileNameWithoutExtension(manifestPath);
        AddAlias(manifestFileName);
        AddAlias(manifestStem);

        var normalizedStem = NormalizePath(manifestStem);
        var match = System.Text.RegularExpressions.Regex.Match(normalizedStem, @"^group[_\- ]*(\d+)(?:[_\- ]+(.*))?$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (match.Success)
        {
            var numberText = match.Groups[1].Value;
            AddAlias(numberText);
            if (int.TryParse(numberText, out var number))
            {
                AddAlias(number.ToString());
                AddAlias($"group_{number}");
                AddAlias($"group {number}");
            }

            AddAlias($"group_{numberText}");
            AddAlias($"group {numberText}");

            var suffix = match.Groups.Count > 2 ? match.Groups[2].Value : string.Empty;
            if (!string.IsNullOrWhiteSpace(suffix))
            {
                AddAlias(suffix);
                AddAlias(suffix.Replace('_', ' ').Replace('-', ' '));
                AddAlias($"{numberText}_{suffix}");
                AddAlias($"{numberText} {suffix.Replace('_', ' ').Replace('-', ' ')}");

                if (int.TryParse(numberText, out var numericSuffixNumber))
                {
                    AddAlias($"{numericSuffixNumber}_{suffix}");
                    AddAlias($"{numericSuffixNumber} {suffix.Replace('_', ' ').Replace('-', ' ')}");
                }
            }
        }

        return new ManifestGroupSelectionAliases(rawAliases, normalizedAliases, looseAliases);
    }

    private static string NormalizeManifestSelectionKey(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = NormalizePath(value);
        return System.Text.RegularExpressions.Regex.Replace(normalized, @"[^a-z0-9]+", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            .ToLowerInvariant();
    }

    private static void AddDefaultSelectedOptionNames(HashSet<string> output, IReadOnlyList<string> optionNames, string groupType, JsonElement groupRoot)
    {
        if (optionNames.Count == 0)
            return;

        if (!groupRoot.TryGetProperty("DefaultSettings", out var defaultSettings)
            || defaultSettings.ValueKind != JsonValueKind.Number
            || !defaultSettings.TryGetInt32(out var defaultValue))
            return;

        if (string.Equals(groupType, "Single", StringComparison.OrdinalIgnoreCase))
        {
            if (defaultValue >= 0 && defaultValue < optionNames.Count)
                output.Add(optionNames[defaultValue]);
            return;
        }

        if (string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase))
        {
            if (defaultValue <= 0)
                return;

            for (var i = 0; i < optionNames.Count; i++)
            {
                if ((defaultValue & (1 << i)) != 0)
                    output.Add(optionNames[i]);
            }

            return;
        }

        if (defaultValue >= 0 && defaultValue < optionNames.Count)
        {
            output.Add(optionNames[defaultValue]);
            return;
        }

        if (defaultValue > 0)
        {
            for (var i = 0; i < optionNames.Count; i++)
            {
                if ((defaultValue & (1 << i)) != 0)
                    output.Add(optionNames[i]);
            }
        }
    }

    private static void AddSelectedOptionNamesFromSettingValues(HashSet<string> output, IReadOnlyList<string> optionNames, string groupType, IEnumerable<string>? selectedValues)
    {
        if (selectedValues == null || optionNames.Count == 0)
            return;

        var values = EnumerateManifestSelectedSettingValues(selectedValues).ToList();
        if (values.Count == 0)
            return;

        if (TryAddBooleanSelectedOptionNames(output, optionNames, values))
            return;

        var isMultiGroup = string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase);
        foreach (var value in values)
        {
            if (TryAddSelectedOptionNameByAlias(output, optionNames, value))
                continue;

            var assignment = SplitManifestSelectionAssignment(value);
            if (!string.IsNullOrWhiteSpace(assignment.Name) && TryParseManifestSelectionBoolean(assignment.Value, out var enabled))
            {
                if (enabled)
                    TryAddSelectedOptionNameByAlias(output, optionNames, assignment.Name);

                continue;
            }

            if (!int.TryParse(value, out var numericValue))
                continue;

            if (isMultiGroup && values.Count == 1 && numericValue > 0)
            {
                var addedFromBitmask = false;
                for (var i = 0; i < optionNames.Count && i < 31; i++)
                {
                    if ((numericValue & (1 << i)) == 0)
                        continue;

                    output.Add(optionNames[i]);
                    addedFromBitmask = true;
                }

                if (addedFromBitmask)
                    continue;
            }

            if (numericValue >= 0 && numericValue < optionNames.Count)
            {
                output.Add(optionNames[numericValue]);
                continue;
            }

            if (numericValue > 0 && isMultiGroup)
            {
                for (var i = 0; i < optionNames.Count && i < 31; i++)
                {
                    if ((numericValue & (1 << i)) != 0)
                        output.Add(optionNames[i]);
                }
            }
        }
    }

    private static IEnumerable<string> EnumerateManifestSelectedSettingValues(IEnumerable<string> selectedValues)
    {
        var emitted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var rawValue in selectedValues)
        {
            if (string.IsNullOrWhiteSpace(rawValue))
                continue;

            foreach (var expanded in ExpandManifestSelectedSettingValue(rawValue))
            {
                var value = expanded.Trim().Trim('"', '\'', '[', ']');
                if (!string.IsNullOrWhiteSpace(value) && emitted.Add(value))
                    yield return value;
            }
        }
    }

    private static IEnumerable<string> ExpandManifestSelectedSettingValue(string rawValue)
    {
        var value = rawValue.Trim();
        if (string.IsNullOrWhiteSpace(value))
            yield break;

        yield return value;

        var trimmed = value.Trim().Trim('[', ']');
        foreach (var part in trimmed.Split(['\0', '\r', '\n', ';', ',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!string.IsNullOrWhiteSpace(part))
                yield return part;
        }
    }

    private static bool TryAddBooleanSelectedOptionNames(HashSet<string> output, IReadOnlyList<string> optionNames, IReadOnlyList<string> values)
    {
        if (values.Count != optionNames.Count)
            return false;

        var parsed = new bool[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            if (!TryParseManifestSelectionBoolean(values[i], out parsed[i]))
                return false;
        }

        for (var i = 0; i < parsed.Length; i++)
        {
            if (parsed[i])
                output.Add(optionNames[i]);
        }

        return true;
    }

    private static bool TryParseManifestSelectionBoolean(string? value, out bool result)
    {
        result = false;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalized = NormalizeManifestSelectionKey(value);
        if (normalized is "true" or "enabled" or "enable" or "on" or "yes" or "y")
        {
            result = true;
            return true;
        }

        if (normalized is "false" or "disabled" or "disable" or "off" or "no" or "n" or "none" or "null")
        {
            result = false;
            return true;
        }

        return false;
    }

    private static (string Name, string Value) SplitManifestSelectionAssignment(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return (string.Empty, string.Empty);

        var separatorIndex = value.IndexOf('=');
        if (separatorIndex < 0)
            separatorIndex = value.IndexOf(':');

        if (separatorIndex <= 0 || separatorIndex >= value.Length - 1)
            return (string.Empty, string.Empty);

        return (value[..separatorIndex].Trim().Trim('"', '\''), value[(separatorIndex + 1)..].Trim().Trim('"', '\''));
    }

    private static bool TryAddSelectedOptionNameByAlias(HashSet<string> output, IReadOnlyList<string> optionNames, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalizedValue = NormalizePath(value);
        var looseValue = NormalizeManifestSelectionKey(value);
        foreach (var optionName in optionNames)
        {
            if (string.Equals(optionName, value, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizePath(optionName), normalizedValue, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizeManifestSelectionKey(optionName), looseValue, StringComparison.OrdinalIgnoreCase))
            {
                output.Add(optionName);
                return true;
            }
        }

        return false;
    }

    private void ImportManifestMappings(string modPath, JsonElement element, int priority, bool addSelectedEntries, bool selectedForIndex, List<ManifestTransientImportEntry> manifestEntries, string importScopeKey, HashSet<string> knownGamePaths, StartupManifestPrimeSelectionIndex manifestIndex, bool skipPhysicalPrefixedEntries = false)
    {
        var modScopeKey = NormalizePath(modPath);
        var candidates = new List<ManifestTransientMappingCandidate>();

        if (element.TryGetProperty("Files", out var files) && files.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in files.EnumerateObject())
            {
                var rawManifestKey = NormalizePath(property.Name);
                if (skipPhysicalPrefixedEntries && !StartsWithManifestGamePathRoot(rawManifestKey))
                {
                    TrackSkippedPhysicalPrefixedDefaultManifestMapping(modPath, property, manifestIndex);
                    continue;
                }

                if (!TryNormalizeManifestGamePath(property.Name, out var gamePath))
                    continue;

                var manifestValue = property.Value.GetString();
                var resolvedFilePath = string.Empty;
                if (addSelectedEntries || selectedForIndex)
                {
                    resolvedFilePath = ResolveManifestFilePath(modPath, manifestValue);
                    if (string.IsNullOrWhiteSpace(resolvedFilePath) || !File.Exists(resolvedFilePath))
                    {
                        var keyResolvedFilePath = ResolveManifestFilePath(modPath, property.Name);
                        if (!string.IsNullOrWhiteSpace(keyResolvedFilePath) && File.Exists(keyResolvedFilePath))
                            resolvedFilePath = keyResolvedFilePath;
                    }

                    if (string.IsNullOrWhiteSpace(resolvedFilePath) || !File.Exists(resolvedFilePath))
                        continue;
                }

                candidates.Add(new ManifestTransientMappingCandidate(gamePath, resolvedFilePath, manifestValue ?? string.Empty,
                    [modScopeKey, BuildManifestFolderScopeKey(modScopeKey, manifestValue), importScopeKey, resolvedFilePath, manifestValue ?? string.Empty],
                    knownGamePaths.Contains(gamePath)));
            }
        }

        if (element.TryGetProperty("FileSwaps", out var fileSwaps) && fileSwaps.ValueKind == JsonValueKind.Object)
        {
            var fileSwapScopeKey = modScopeKey + "|fileswaps";
            foreach (var property in fileSwaps.EnumerateObject())
            {
                if (!TryNormalizeManifestGamePath(property.Name, out var gamePath))
                    continue;

                var replacementGamePath = NormalizePath(property.Value.GetString() ?? string.Empty);
                if (string.IsNullOrWhiteSpace(replacementGamePath))
                    continue;

                candidates.Add(new ManifestTransientMappingCandidate(gamePath, "fileswap|source|" + gamePath + "|" + replacementGamePath,
                    property.Value.GetString() ?? string.Empty, [modScopeKey, fileSwapScopeKey, importScopeKey, property.Name, property.Value.GetString() ?? string.Empty],
                    knownGamePaths.Contains(gamePath)));

                if (TryNormalizeManifestGamePath(replacementGamePath, out var replacementManifestGamePath))
                {
                    candidates.Add(new ManifestTransientMappingCandidate(replacementManifestGamePath, "fileswap|target|" + gamePath + "|" + replacementGamePath,
                        property.Value.GetString() ?? string.Empty, [modScopeKey, fileSwapScopeKey, importScopeKey, property.Name, property.Value.GetString() ?? string.Empty],
                        knownGamePaths.Contains(replacementManifestGamePath)));
                }
            }
        }

        if (candidates.Count == 0)
            return;

        var scopeHasTransientTrigger = candidates.Any(candidate => IsManifestSupportScopeTriggerPath(candidate.GamePath));
        var eligibleCandidates = candidates
            .Where(candidate => ShouldImportManifestGamePath(candidate.GamePath, candidate.ResolvedFilePath, candidate.ManifestValue, scopeHasTransientTrigger, candidate.ScopeTexts))
            .ToList();

        if (eligibleCandidates.Count == 0)
            return;

        if (!addSelectedEntries)
        {
            foreach (var candidate in eligibleCandidates)
                TrackStartupManifestCandidate(manifestIndex, candidate, selectedForIndex);

            return;
        }

        var inheritedScopeClassJobId = TryResolveManifestScopeClassJobId(eligibleCandidates, out var scopeClassJobId)
            ? scopeClassJobId
            : (uint?)null;

        foreach (var candidate in eligibleCandidates)
        {
            TrackStartupManifestCandidate(manifestIndex, candidate, selectedForIndex);

            var candidateInheritedScopeClassJobId = ShouldUseManifestScopeClassJobFallback(candidate.GamePath, inheritedScopeClassJobId)
                ? inheritedScopeClassJobId
                : null;

            manifestEntries.Add(new ManifestTransientImportEntry(candidate.GamePath, candidate.ResolvedFilePath, candidate.ManifestValue,
                priority, candidate.ScopeTexts, candidateInheritedScopeClassJobId));
        }
    }

    private static void TrackSkippedPhysicalPrefixedDefaultManifestMapping(string modPath, JsonProperty property, StartupManifestPrimeSelectionIndex manifestIndex)
    {
        if (!TryNormalizeManifestGamePath(property.Name, out var gamePath))
            return;

        var manifestValue = property.Value.GetString();
        var resolvedFilePath = ResolveManifestFilePath(modPath, manifestValue);
        if (string.IsNullOrWhiteSpace(resolvedFilePath) || !File.Exists(resolvedFilePath))
        {
            var keyResolvedFilePath = ResolveManifestFilePath(modPath, property.Name);
            if (!string.IsNullOrWhiteSpace(keyResolvedFilePath) && File.Exists(keyResolvedFilePath))
                resolvedFilePath = keyResolvedFilePath;
        }

        if (string.IsNullOrWhiteSpace(resolvedFilePath) || !File.Exists(resolvedFilePath))
            return;

        TrackStartupManifestCandidate(manifestIndex, new ManifestTransientMappingCandidate(gamePath, resolvedFilePath, manifestValue ?? string.Empty, [], false), selected: false);
    }

    private static void TrackStartupManifestCandidate(StartupManifestPrimeSelectionIndex manifestIndex, ManifestTransientMappingCandidate candidate, bool selected)
    {
        var gamePath = NormalizePath(candidate.GamePath);
        if (!string.IsNullOrWhiteSpace(gamePath))
        {
            manifestIndex.ManifestGamePaths.Add(gamePath);
            if (selected)
                manifestIndex.SelectedGamePaths.Add(gamePath);
        }

        var resolvedPath = NormalizeResolvedFilePath(candidate.ResolvedFilePath);
        if (!string.IsNullOrWhiteSpace(resolvedPath) && !resolvedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
        {
            manifestIndex.ManifestResolvedFilePaths.Add(resolvedPath);
            if (selected)
                manifestIndex.SelectedResolvedFilePaths.Add(resolvedPath);
        }
    }

    private static string ResolveManifestFilePath(string modPath, string? manifestValue)
    {
        if (string.IsNullOrWhiteSpace(manifestValue))
            return string.Empty;

        var normalized = manifestValue.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar);
        var combined = Path.IsPathRooted(normalized) ? normalized : Path.Combine(modPath, normalized);
        var fullPath = Path.GetFullPath(combined);
        if (File.Exists(fullPath))
            return NormalizeResolvedFilePath(fullPath);

        var caseResolvedPath = ResolveExistingFilePathCaseInsensitive(fullPath);
        return string.IsNullOrWhiteSpace(caseResolvedPath)
            ? NormalizeResolvedFilePath(fullPath)
            : NormalizeResolvedFilePath(caseResolvedPath);
    }

    private static string ResolveExistingFilePathCaseInsensitive(string fullPath)
    {
        if (string.IsNullOrWhiteSpace(fullPath))
            return string.Empty;

        try
        {
            if (File.Exists(fullPath))
                return Path.GetFullPath(fullPath);

            var root = Path.GetPathRoot(fullPath);
            if (string.IsNullOrWhiteSpace(root))
                return string.Empty;

            var current = root;
            var remainder = fullPath[root.Length..];
            foreach (var segment in remainder.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries))
            {
                if (string.IsNullOrWhiteSpace(segment) || !Directory.Exists(current))
                    return string.Empty;

                var match = Directory.EnumerateFileSystemEntries(current)
                    .FirstOrDefault(entry => string.Equals(Path.GetFileName(entry), segment, StringComparison.OrdinalIgnoreCase));
                if (string.IsNullOrWhiteSpace(match))
                    return string.Empty;

                current = match;
            }

            return File.Exists(current) ? current : string.Empty;
        }
        catch
        {
            return string.Empty;
        }
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

    private bool ShouldImportManifestGamePath(string gamePath, string resolvedFilePath = "", string manifestValue = "", bool scopeHasTransientTrigger = false, IEnumerable<string>? scopeTexts = null)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        if (!_manifestGamePathRoots.Any(root => gamePath.StartsWith(root, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (TryInferLuminaPersistentObjectKind(gamePath, out var objectKind) && objectKind != ObjectKind.Player)
            return ShouldImportObjectScopedManifestGamePath(gamePath);

        if (EndsWithAny(gamePath, _manifestSupportFileTypes))
            return ShouldImportManifestSupportPath(gamePath, resolvedFilePath, manifestValue, scopeHasTransientTrigger);

        if (!EndsWithAny(gamePath, _handledFileTypes))
            return false;

        if (EndsWithAny(gamePath, _handledRecordingFileTypes))
            return false;

        if (gamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase))
            return gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase);

        if (IsVfxRelatedResourcePath(gamePath))
            return true;

        if (IsWeaponPlacementSupportPath(gamePath))
            return true;

        if (gamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase))
            return gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
                || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase);

        if (IsHumanMountRiderAnimationPath(gamePath))
            return true;

        if (IsHumanAnimationPapPath(gamePath)
            && (IsLikelyMinionOrMountPersistentPath(resolvedFilePath, manifestValue)
                || ContainsPersistentObjectToken(scopeTexts ?? Array.Empty<string>(), "mount", "mounts", "minion", "minions")))
        {
            return true;
        }

        if (!gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            && !gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            && !gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (gamePath.Contains("/skeleton/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/mount.", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (IsJobBattleResidentAnimationPath(gamePath))
            return true;

        if (gamePath.Contains("/resident/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/idle.", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/move_", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return IsEmoteKeyPath(gamePath)
            || IsMusicKeyPath(gamePath)
            || gamePath.Contains("/ability/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/ws/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/limitbreak/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/event_base/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/nonresident/", StringComparison.OrdinalIgnoreCase);
    }

    private bool ShouldImportObjectScopedManifestGamePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (!normalized.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            && !normalized.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (EndsWithAny(normalized, _manifestSupportFileTypes))
            return true;

        if (!EndsWithAny(normalized, _handledFileTypes))
            return false;

        // Lumina has already proven this model belongs to a mount/minion actor. For those actors the
        // structural resources are part of the actor identity, not disposable player animation noise.
        // Keeping these fixes vehicle-style mounts where the visible model arrives but the receiver keeps
        // the vanilla skeleton/physics/animation rig, leaving wheels and attachments detached.
        return normalized.Contains("/skeleton/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/animation/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/vfx/", StringComparison.OrdinalIgnoreCase)
            || IsVfxRelatedResourcePath(normalized);
    }

    private static bool IsJobBattleResidentAnimationPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        return IsJobBattleResidentAnimationPathShape(normalized)
            && TryResolveManifestClassJobIds(normalized, out var classJobIds)
            && classJobIds.Count > 0;
    }

    private static bool IsHumanMountRiderAnimationPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (!IsHumanAnimationPapPath(normalized))
            return false;

        var lastSlash = normalized.LastIndexOf('/');
        var fileName = lastSlash >= 0 ? normalized[(lastSlash + 1)..] : normalized;
        var fileStem = fileName.EndsWith(".pap", StringComparison.OrdinalIgnoreCase) ? fileName[..^4] : fileName;

        return string.Equals(fileStem, "mount", StringComparison.OrdinalIgnoreCase)
            || fileStem.StartsWith("mount_", StringComparison.OrdinalIgnoreCase)
            || fileStem.StartsWith("mounted_", StringComparison.OrdinalIgnoreCase)
            || fileStem.StartsWith("ride_", StringComparison.OrdinalIgnoreCase)
            || fileStem.StartsWith("riding_", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/mount/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/mounted/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/riding/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ride/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsHumanAnimationPapPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        return !string.IsNullOrWhiteSpace(normalized)
            && normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            && normalized.Contains("/animation/", StringComparison.OrdinalIgnoreCase)
            && normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsPlayerActorManifestPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        return normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsWeaponPlacementSupportPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized) || !normalized.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase))
            return false;

        return System.Text.RegularExpressions.Regex.IsMatch(
                normalized,
                @"^chara/weapon/w\d{4}/animation/a\d{4}/wp_common/resident/weapon\.pap$",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            || System.Text.RegularExpressions.Regex.IsMatch(
                normalized,
                @"^chara/weapon/w\d{4}/skeleton/base/b0001/skl_w\d{4}b0001\.sklb$",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private static bool IsJobBattleResidentAnimationPathShape(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        return !string.IsNullOrWhiteSpace(normalized)
            && normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            && System.Text.RegularExpressions.Regex.IsMatch(
                normalized,
                @"^chara/human/c\d{4}/animation/a\d{4}/bt_[^/]+/resident/(idle|move(?:_[^/]+)?|sub)\.pap$",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private bool ShouldImportManifestSupportPath(string gamePath, string resolvedFilePath, string manifestValue, bool scopeHasTransientTrigger)
    {
        if (!IsManifestSupportPath(gamePath))
            return false;

        if (IsDeniedManifestSupportPath(gamePath, resolvedFilePath, manifestValue))
            return false;

        if (ContainsAnimationSupportBodyFolderSegment(gamePath))
            return true;

        if (IsSelectedEquipmentOrAccessoryManifestSupportPath(gamePath, resolvedFilePath, manifestValue, scopeHasTransientTrigger))
            return true;

        return false;
    }

    private bool IsManifestSupportPath(string? path)
    {
        return EndsWithAny(NormalizePath(path ?? string.Empty), _manifestSupportFileTypes);
    }

    private static bool IsManifestSupportPathStatic(string? path)
    {
        return EndsWithAny(NormalizePath(path ?? string.Empty), _manifestSupportFileTypes);
    }

    private static bool ContainsAnimationSupportBodyToken(string? path)
    {
        var normalized = NormalizePath(path ?? string.Empty);
        return !string.IsNullOrWhiteSpace(normalized) && normalized.Contains("b0001", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ContainsAnimationSupportBodyFolderSegment(params string?[] paths)
    {
        return paths.Any(path => ContainsPathSegment(path, "b0001"));
    }

    private static bool IsSelectedEquipmentOrAccessoryManifestSupportPath(string gamePath, string resolvedFilePath, string manifestValue, bool scopeHasTransientTrigger)
    {
        if (!scopeHasTransientTrigger)
            return false;

        return IsEquipmentOrAccessoryModelMaterialTexturePath(gamePath)
            || IsEquipmentOrAccessoryModelMaterialTexturePath(resolvedFilePath)
            || IsEquipmentOrAccessoryModelMaterialTexturePath(manifestValue);
    }

    private static bool IsEquipmentOrAccessoryModelMaterialTexturePath(string? path)
    {
        var normalized = NormalizePath(path ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return System.Text.RegularExpressions.Regex.IsMatch(
            normalized,
            @"(^|/)chara/(equipment|accessory)/[ea]\d{4}/(model|material|texture)/",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private static bool IsDeniedManifestSupportPath(params string?[] paths)
    {
        return IsMonsterManifestSupportPath(paths)
            || IsDemihumanManifestSupportPath(paths)
            || IsBaseHumanBodyOrSkinManifestSupportPath(paths)
            || IsLikelyMountOrMinionManifestSupportPath(paths);
    }

    private static bool IsMonsterManifestSupportPath(params string?[] paths)
    {
        return paths.Any(path => ContainsPathSegment(path, "monster"));
    }

    private static bool IsDemihumanManifestSupportPath(params string?[] paths)
    {
        return paths.Any(path => ContainsPathSegment(path, "demihuman"));
    }

    private static bool IsLikelyMinionOrMountPersistentPath(params string?[] paths)
        => ContainsPersistentObjectToken(paths, "mount", "mounts", "minion", "minions");

    private static bool IsLikelyCompanionPersistentPath(params string?[] paths)
        => ContainsPersistentObjectToken(paths, "companion", "companions", "chocobo", "buddy");

    private static bool ContainsPersistentObjectToken(IEnumerable<string?> paths, params string[] tokens)
    {
        foreach (var path in paths)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            if (string.IsNullOrWhiteSpace(normalized))
                continue;

            foreach (var token in tokens)
            {
                if (System.Text.RegularExpressions.Regex.IsMatch(
                    normalized,
                    $"(^|[^a-z0-9]){System.Text.RegularExpressions.Regex.Escape(token)}([^a-z0-9]|$)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool IsLikelyMountOrMinionManifestSupportPath(params string?[] paths)
    {
        foreach (var path in paths)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            if (string.IsNullOrWhiteSpace(normalized))
                continue;

            if (normalized.Contains("/mount/", StringComparison.OrdinalIgnoreCase)
                || normalized.Contains("/mounts/", StringComparison.OrdinalIgnoreCase)
                || normalized.Contains("/minion/", StringComparison.OrdinalIgnoreCase)
                || normalized.Contains("/minions/", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsBaseHumanBodyOrSkinManifestSupportPath(params string?[] paths)
    {
        foreach (var path in paths)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            if (string.IsNullOrWhiteSpace(normalized))
                continue;

            if (System.Text.RegularExpressions.Regex.IsMatch(normalized, @"(^|/)chara/human/c\d{4}/obj/body/b0001/(model|material|texture)/", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                return true;

            var fileName = Path.GetFileName(normalized);
            if (System.Text.RegularExpressions.Regex.IsMatch(fileName, @"^(mt_)?c\d{4}b0001([_.]|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                return true;
        }

        return false;
    }

    private static bool ContainsPathSegment(string? path, string segment)
    {
        if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(segment))
            return false;

        var normalized = NormalizePath(path);
        var normalizedSegment = NormalizePath(segment).Trim('/');
        if (string.IsNullOrWhiteSpace(normalized) || string.IsNullOrWhiteSpace(normalizedSegment))
            return false;

        return string.Equals(normalized, normalizedSegment, StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith(normalizedSegment + "/", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("/" + normalizedSegment, StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/" + normalizedSegment + "/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsManifestSupportScopeTriggerPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase);
    }

    private static bool StartsWithManifestGamePathRoot(string? path)
    {
        var normalized = NormalizePath(path ?? string.Empty);
        return !string.IsNullOrWhiteSpace(normalized)
            && _manifestGamePathRoots.Any(root => normalized.StartsWith(root, StringComparison.OrdinalIgnoreCase));
    }

    private static bool TryNormalizeManifestGamePath(string rawPath, out string normalizedGamePath)
    {
        normalizedGamePath = string.Empty;
        if (string.IsNullOrWhiteSpace(rawPath))
            return false;

        var normalized = NormalizePath(rawPath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (!_manifestGamePathRoots.Any(root => normalized.StartsWith(root, StringComparison.OrdinalIgnoreCase)))
            return false;

        normalizedGamePath = normalized;
        return true;
    }

    private static string GetManifestImportFileIdentity(ManifestTransientImportEntry entry)
    {
        if (!string.IsNullOrWhiteSpace(entry.ResolvedFilePath))
            return NormalizeResolvedFilePath(entry.ResolvedFilePath);

        if (!string.IsNullOrWhiteSpace(entry.ManifestValue))
            return NormalizePath(entry.ManifestValue);

        return string.Empty;
    }

    private static bool TryResolveManifestScopeClassJobId(IReadOnlyCollection<ManifestTransientMappingCandidate> candidates, out uint classJobId)
    {
        classJobId = 0;
        var candidatesByJob = new HashSet<uint>();

        foreach (var candidate in candidates)
        {
            if (!IsManifestScopeClassJobSourcePath(candidate.GamePath))
                continue;

            var entry = new ManifestTransientImportEntry(candidate.GamePath, candidate.ResolvedFilePath, candidate.ManifestValue, 0, candidate.ScopeTexts);
            if (TryResolveManifestEntryClassJobId(entry, out var candidateClassJobId))
                candidatesByJob.Add(candidateClassJobId);
        }

        if (candidatesByJob.Count != 1)
            return false;

        classJobId = candidatesByJob.First();
        return classJobId != 0;
    }

    private static bool IsManifestScopeClassJobSourcePath(string? gamePath)
    {
        var normalized = NormalizePath(gamePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalized) || IsManifestSupportPathStatic(normalized))
            return false;

        return IsVfxRelatedResourcePath(normalized)
            || normalized.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ability/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ws/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/limitbreak/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldUseManifestScopeClassJobFallback(string? gamePath, uint? inheritedScopeClassJobId)
    {
        if (!inheritedScopeClassJobId.HasValue || inheritedScopeClassJobId.Value == 0)
            return false;

        var normalized = NormalizePath(gamePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return IsVfxRelatedResourcePath(normalized);
    }

    private static bool TryResolveManifestEntryClassJobId(ManifestTransientImportEntry entry, out uint classJobId)
    {
        classJobId = 0;
        if (!TryResolveManifestEntryClassJobIds(entry, out var classJobIds) || classJobIds.Count != 1)
            return false;

        classJobId = classJobIds.First();
        return classJobId != 0;
    }

    private static bool TryResolveManifestEntryClassJobIds(ManifestTransientImportEntry entry, out HashSet<uint> classJobIds)
    {
        classJobIds = [];

        var gamePathCandidates = new HashSet<uint>();
        var physicalScopeCandidates = new HashSet<uint>();
        AddManifestClassJobCandidatesFromPhysicalScope(entry.ResolvedFilePath, physicalScopeCandidates);
        AddManifestClassJobCandidatesFromPhysicalScope(entry.ManifestValue, physicalScopeCandidates);

        foreach (var scopeText in entry.ScopeTexts)
            AddManifestClassJobCandidatesFromPhysicalScope(scopeText, physicalScopeCandidates);

        if (IsJobBattleResidentAnimationPathShape(entry.GamePath))
        {
            gamePathCandidates = new HashSet<uint>();
            AddManifestClassJobCandidatesFromText(entry.GamePath, gamePathCandidates);
            if (gamePathCandidates.Count == 1)
            {
                classJobIds = gamePathCandidates;
                return true;
            }

            if (gamePathCandidates.Count > 1)
            {
                var selectedSubset = physicalScopeCandidates.Where(gamePathCandidates.Contains).ToHashSet();
                classJobIds = selectedSubset.Count > 0 ? selectedSubset : gamePathCandidates;
                return classJobIds.Count > 0;
            }
        }

        if (physicalScopeCandidates.Count == 1)
        {
            classJobIds = physicalScopeCandidates;
            return true;
        }

        if (physicalScopeCandidates.Count > 1)
            return false;

        gamePathCandidates = new HashSet<uint>();
        if (ShouldInferManifestClassJobFromGamePath(entry.GamePath))
            AddManifestClassJobCandidatesFromText(entry.GamePath, gamePathCandidates);
        if (gamePathCandidates.Count == 1)
        {
            classJobIds = gamePathCandidates;
            return true;
        }

        if (entry.InheritedScopeClassJobId.HasValue
            && entry.InheritedScopeClassJobId.Value != 0
            && IsVfxRelatedResourcePath(entry.GamePath))
        {
            classJobIds.Add(entry.InheritedScopeClassJobId.Value);
            return true;
        }

        return false;
    }

    private static bool ShouldInferManifestClassJobFromGamePath(string? gamePath)
    {
        var normalized = NormalizePath(gamePath ?? string.Empty);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (normalized.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase))
            return false;

        return true;
    }

    private static void AddManifestClassJobCandidatesFromPhysicalScope(string? text, HashSet<uint> candidates)
    {
        if (string.IsNullOrWhiteSpace(text))
            return;

        var physicalScope = GetManifestPhysicalScopeText(text);
        if (string.IsNullOrWhiteSpace(physicalScope))
            return;

        AddManifestClassJobCandidatesFromText(physicalScope, candidates);
    }

    private static string GetManifestPhysicalScopeText(string text)
    {
        var normalized = NormalizePath(text);
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        var earliestRootIndex = -1;
        foreach (var root in _manifestGamePathRoots)
        {
            var rootNeedle = "/" + root;
            var idx = normalized.IndexOf(rootNeedle, StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
                continue;

            if (earliestRootIndex < 0 || idx < earliestRootIndex)
                earliestRootIndex = idx;
        }

        if (earliestRootIndex <= 0)
            return normalized;

        return normalized[..earliestRootIndex];
    }

    private static void AddManifestClassJobCandidatesFromText(string? text, HashSet<uint> candidates)
    {
        if (string.IsNullOrWhiteSpace(text))
            return;

        var normalized = NormalizePath(text);
        if (string.IsNullOrWhiteSpace(normalized))
            return;

        if (TryResolveManifestClassJobIds(normalized, out var directClassJobIds))
        {
            foreach (var directClassJobId in directClassJobIds)
                candidates.Add(directClassJobId);
        }

        foreach (var token in EnumerateManifestClassificationTokens(normalized))
        {
            if (_manifestJobTokenToClassJobId.TryGetValue(token, out var resolvedJobId))
                candidates.Add(resolvedJobId);

            if (_manifestSharedJobTokenToClassJobIds.TryGetValue(token, out var sharedJobIds))
            {
                foreach (var sharedJobId in sharedJobIds)
                    candidates.Add(sharedJobId);
            }
        }
    }


    private static bool TryResolveManifestEmbeddedClassJobId(string text, out uint classJobId)
    {
        classJobId = 0;
        var normalized = NormalizePath(text);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        var candidates = new HashSet<uint>();
        foreach (System.Text.RegularExpressions.Match match in System.Text.RegularExpressions.Regex.Matches(
            normalized,
            @"(?<![a-z0-9])(pld|mnk|drg|brd|whm|blm|smn|sch|nin|mch|drk|ast|sam|rdm|blu|gnb|dnc|rpr|sge|vpr|pct|bst)(?=(remake|rework|vfx|sfx|fx|sound|pack|overhaul|replace|replacement|revamp|edit|mod|skill|skills|action|actions|effect|effects|weapon|wep|job|class|ff|[0-9]|[_\-/ ]|$))",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase))
        {
            if (_manifestJobTokenToClassJobId.TryGetValue(match.Groups[1].Value, out var jobId))
                candidates.Add(jobId);
        }

        if (candidates.Count != 1)
            return false;

        classJobId = candidates.First();
        return classJobId != 0;
    }

    private static bool TryResolveManifestClassJobId(string gamePath, out uint classJobId)
    {
        classJobId = 0;
        if (!TryResolveManifestClassJobIds(gamePath, out var classJobIds) || classJobIds.Count != 1)
            return false;

        classJobId = classJobIds.First();
        return classJobId != 0;
    }

    private static bool TryResolveManifestClassJobIds(string gamePath, out HashSet<uint> classJobIds)
    {
        classJobIds = [];
        if (string.IsNullOrWhiteSpace(gamePath) || ShouldForceGlobalManifestPath(gamePath))
            return false;

        var normalized = NormalizePath(gamePath);
        foreach (System.Text.RegularExpressions.Match match in System.Text.RegularExpressions.Regex.Matches(
            normalized,
            $@"(?<![a-z0-9])({_manifestDirectJobTokenPattern})(?=[a-z0-9_/-]|$)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase))
        {
            var codeToken = match.Groups[1].Value;
            if (_manifestJobTokenToClassJobId.TryGetValue(codeToken, out var classJobId))
                classJobIds.Add(classJobId);

            if (_manifestSharedJobTokenToClassJobIds.TryGetValue(codeToken, out var sharedJobIds))
            {
                foreach (var sharedJobId in sharedJobIds)
                    classJobIds.Add(sharedJobId);
            }
        }

        foreach (var token in EnumerateManifestClassificationTokens(normalized))
        {
            if (_manifestJobTokenToClassJobId.TryGetValue(token, out var classJobId))
                classJobIds.Add(classJobId);

            if (_manifestSharedJobTokenToClassJobIds.TryGetValue(token, out var sharedJobIds))
            {
                foreach (var sharedJobId in sharedJobIds)
                    classJobIds.Add(sharedJobId);
            }
        }

        if (classJobIds.Count > 0)
            return true;

        if (TryResolveManifestEmbeddedClassJobId(normalized, out var embeddedClassJobId))
        {
            classJobIds.Add(embeddedClassJobId);
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

    private static bool IsEmoteKeyPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return gamePath.Contains("/emote/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/emote_sp/", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/human/action/emote/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsMusicKeyPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var normalized = NormalizePath(gamePath);
        return normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            && normalized.Contains("/bt_common/music/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsVfxRelatedResourcePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return gamePath.EndsWith("avfx", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith("atex", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record TargetedManifestRefreshMod(string ModKey, IpcCallerPenumbra.PenumbraModSettingState? ModState);

    private sealed record ManifestGroupSelectionAliases(HashSet<string> RawAliases, HashSet<string> NormalizedAliases, HashSet<string> LooseAliases);

    private sealed class StartupManifestPrimeSelectionIndex
    {
        public HashSet<string> ManifestGamePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> SelectedGamePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> ManifestResolvedFilePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> SelectedResolvedFilePaths { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class ManifestTransientMappingCandidate
    {
        public ManifestTransientMappingCandidate(string gamePath, string resolvedFilePath, string manifestValue, IEnumerable<string> scopeTexts, bool knownGamePath)
        {
            GamePath = NormalizePath(gamePath);
            ResolvedFilePath = NormalizeResolvedFilePath(resolvedFilePath);
            ManifestValue = NormalizePath(manifestValue);
            ScopeTexts = scopeTexts.Where(s => !string.IsNullOrWhiteSpace(s)).Select(NormalizePath).ToHashSet(StringComparer.OrdinalIgnoreCase);
            KnownGamePath = knownGamePath;
        }

        public string GamePath { get; }
        public string ResolvedFilePath { get; }
        public string ManifestValue { get; }
        public HashSet<string> ScopeTexts { get; }
        public bool KnownGamePath { get; }
    }

    private sealed class ManifestTransientImportEntry
    {
        public ManifestTransientImportEntry(string gamePath, string resolvedFilePath, string manifestValue, int priority, IEnumerable<string> scopeTexts, uint? inheritedScopeClassJobId = null)
        {
            GamePath = NormalizePath(gamePath);
            ResolvedFilePath = NormalizeResolvedFilePath(resolvedFilePath);
            ManifestValue = NormalizePath(manifestValue);
            Priority = priority;
            ScopeTexts = scopeTexts.Where(s => !string.IsNullOrWhiteSpace(s)).Select(NormalizePath).ToHashSet(StringComparer.OrdinalIgnoreCase);
            InheritedScopeClassJobId = inheritedScopeClassJobId;
        }

        public string GamePath { get; }
        public string ResolvedFilePath { get; }
        public string ManifestValue { get; }
        public int Priority { get; }
        public HashSet<string> ScopeTexts { get; }
        public uint? InheritedScopeClassJobId { get; }
    }

    private bool IsAutoRecordTriggerPath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (!EndsWithAny(normalized, _autoRecordTriggerFileTypes))
            return false;

        if (normalized.Contains("/skeleton/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/mount.", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (IsIdleOrMoveAnimationPath(normalized))
            return true;

        if (normalized.Contains("/resident/", StringComparison.OrdinalIgnoreCase))
            return false;

        if (normalized.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/vfx/", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (normalized.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase))
            return true;

        return normalized.Contains("/emote/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/emote_sp/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ability/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/ws/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/limitbreak/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/event_base/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/nonresident/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsIdleOrMoveAnimationPath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        if (!gamePath.Contains("/resident/", StringComparison.OrdinalIgnoreCase))
            return false;

        return gamePath.Contains("/idle.", StringComparison.OrdinalIgnoreCase)
            || gamePath.Contains("/move_", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsAutoRecordCapturablePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        var isManagedTransientType = EndsWithAny(normalized, _handledFileTypes);
        var isModelMaterialTexture = EndsWithAny(normalized, _handledRecordingFileTypes);
        if (!isManagedTransientType && !isModelMaterialTexture)
            return false;

        if (isModelMaterialTexture && IsLikelyAppearanceSlotModelMaterialTexturePath(normalized) && !ContainsAnimationSupportBodyToken(normalized))
            return false;

        return true;
    }

    private static bool IsLikelyAppearanceSlotModelMaterialTexturePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return normalized.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/hair/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/face/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/tail/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/ear/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/obj/body/", StringComparison.OrdinalIgnoreCase) && normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase);
    }

    private static string ExtractResolvedFilePath(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return string.Empty;

        if (!filePath.StartsWith("|", StringComparison.OrdinalIgnoreCase))
            return filePath;

        var parts = filePath.Split('|', StringSplitOptions.None);
        return parts.Length >= 3 ? parts[2] : string.Empty;
    }

    private static bool EndsWithAny(string gamePath, IEnumerable<string> extensions)
    {
        var normalizedPath = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalizedPath))
            return false;

        foreach (var ext in extensions)
        {
            var normalizedExt = NormalizePath(ext).TrimStart('.');
            if (string.IsNullOrWhiteSpace(normalizedExt))
                continue;

            if (normalizedPath.EndsWith("." + normalizedExt, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static string NormalizeResolvedFilePath(string? p)
    {
        if (string.IsNullOrWhiteSpace(p))
            return string.Empty;

        return p.Replace("\\", "/", StringComparison.OrdinalIgnoreCase).Trim();
    }

    private static string NormalizePath(string p)
    {
        if (string.IsNullOrWhiteSpace(p))
            return string.Empty;

        return p.ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase).Trim();
    }

    public static bool IsObservedSupportTrackedGamePath(string gamePath)
        => IsObservedSupportStateQualifiedGamePath(gamePath) || IsPropModelMaterialTexturePath(gamePath);

    public static bool IsObservedSupportStateQualifiedGamePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return normalized.EndsWith("tmb", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("tmb2", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("pap", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("avfx", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("atex", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("sklb", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("eid", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("phy", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("phyb", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("pbd", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("scd", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("skp", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("shpk", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("kdb", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsPropModelMaterialTexturePath(string gamePath)
    {
        var normalized = NormalizePath(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return normalized.EndsWith("mdl", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("mtrl", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("tex", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith("atex", StringComparison.OrdinalIgnoreCase);
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        lock (_sendTransientLock)
        {
            try
            {
                _sendTransientCts.Cancel();
                _sendTransientCts.Dispose();
                _autoRecordCts.Cancel();
                _autoRecordCts.Dispose();
                _startupTransientPrimeCts.Cancel();
                _startupTransientPrimeCts.Dispose();
                _targetedManifestRefreshCts.Cancel();
                _targetedManifestRefreshCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // already disposed
            }
        }
    }

    public sealed record class TransientManifestPrimeProgressSnapshot(
        bool IsRunning,
        bool IsTargeted,
        string Phase,
        string CurrentMod,
        int ScannedMods,
        int TotalMods,
        int ImportedPaths,
        int PrunedPaths)
    {
        public static TransientManifestPrimeProgressSnapshot Idle { get; } = new(false, false, string.Empty, string.Empty, 0, 0, 0, 0);
        public float Progress => TotalMods <= 0 ? 0f : Math.Clamp(ScannedMods / (float)TotalMods, 0f, 1f);
    }

    public record TransientRecord(GameObjectHandler Owner, string GamePath, string FilePath, bool AlreadyTransient)
    {
        public bool AddTransient { get; set; }
    }
}
