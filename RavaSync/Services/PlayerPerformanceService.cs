using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.UI;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files.Models;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text.Json;


namespace RavaSync.Services;

public class PlayerPerformanceService : MediatorSubscriberBase
{
    private readonly FileCacheManager _fileCacheManager;
    private readonly XivDataAnalyzer _xivDataAnalyzer;
    private readonly ILogger<PlayerPerformanceService> _logger;
    private readonly MareMediator _mediator;
    private readonly PlayerPerformanceConfigService _playerPerformanceConfigService;
    private readonly TransientResourceManager _transientResourceManager;
    private readonly IServiceProvider _services;
    private readonly ConcurrentDictionary<string, bool> _warnedForPlayers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, (long Bytes, long Stamp)> _textureVramByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (long Bytes, long Stamp)> _modelVramByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, UserData> _autoThresholdPausedUsers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, UserData> _autoCombatPausedUsers = new(StringComparer.Ordinal);
    private int _combatPauseActive;

    // pooled VRAM (bytes) per non-direct (Syncshell) UID
    private readonly ConcurrentDictionary<string, long> _syncshellVramByUid = new(StringComparer.Ordinal);
    private long _syncshellVramTotalBytes;

    private long GetTotalSyncshellVramBytes() => Interlocked.Read(ref _syncshellVramTotalBytes);

    private readonly ConcurrentDictionary<string, long> _autoCapPausedVramByUid = new();
    private readonly ConcurrentDictionary<string, UserData> _autoCapPausedUsers = new();
    private readonly ConcurrentDictionary<string, UserData> _knownUsersByUid = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StableMetricCacheEntry> _stableVramByDataHash = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StableMetricCacheEntry> _stableTrisByDataHash = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StableMetricCacheEntry> _receivedVramByDataHash = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StableMetricCacheEntry> _receivedTrisByDataHash = new(StringComparer.Ordinal);

    private readonly string _statePath =
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                 "RavaSync", "autoCapState.json");

    private int _stateDirty; // 0/1 via Interlocked
    private DateTime _lastSave = DateTime.MinValue;

    private int _stateSaveInFlight; // 0/1 via Interlocked
    private DateTime _nextStateSaveAttemptUtc = DateTime.MinValue;

    private static readonly TimeSpan AutoCapCheckInterval = TimeSpan.FromMilliseconds(75);
    private DateTime _nextAutoCapCheckUtc = DateTime.MinValue;

    private readonly ConcurrentDictionary<string, UserData> _pendingThresholdResumeOnConnect = new(StringComparer.Ordinal);
    private long _pendingThresholdResumeReadyTick;
    private long _pendingThresholdResumeDeadlineTick;

    private int _autoCapCandidatesDirty = 1;
    private long _lastObservedAutoCapBytes = -1;

    private long _nextStableMetricCachePruneTick;
    private const long StableMetricCachePruneIntervalMs = 30000;
    private const long StableMetricCacheTtlMs = 10 * 60 * 1000;

    public PlayerPerformanceService(ILogger<PlayerPerformanceService> logger, MareMediator mediator,
        PlayerPerformanceConfigService playerPerformanceConfigService, FileCacheManager fileCacheManager,
        XivDataAnalyzer xivDataAnalyzer, TransientResourceManager transientResourceManager,
        IServiceProvider services) : base(logger, mediator)
    {

        _logger = logger;
        _mediator = mediator;
        _playerPerformanceConfigService = playerPerformanceConfigService;
        _fileCacheManager = fileCacheManager;
        _xivDataAnalyzer = xivDataAnalyzer;
        _transientResourceManager = transientResourceManager;
        _services = services;

        _mediator.Subscribe<FrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        LoadAutoCapState();
        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => TryKickAutoCapStateSave());
        _mediator.Subscribe<CombatOrPerformanceStartMessage>(this, _ => HandleCombatAutoPauseStart());
        _mediator.Subscribe<CombatOrPerformanceEndMessage>(this, _ => HandleCombatAutoPauseEnd());
        _mediator.Subscribe<ConnectedMessage>(this, _ => HandleConnected());
        _mediator.Subscribe<DisconnectedMessage>(this, _ => ClearPendingThresholdResumeOnConnect());
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => ProcessPendingThresholdResumeOnConnect(Environment.TickCount64));
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => PruneRememberedThresholdPausedUsers());
        _mediator.Subscribe<ResumeMessage>(this, msg => HandleExplicitResume(msg.UserData));

    }

    public void StoreReceivedPerformanceMetrics(string? dataHash, long vramBytes, long triangles)
    {
        StoreStableMetricCache(_receivedVramByDataHash, dataHash, Math.Max(0, vramBytes));
        StoreStableMetricCache(_receivedTrisByDataHash, dataHash, Math.Max(0, triangles));
    }

    public void HandleIncomingPerformanceMetrics(Pair? pair, string? dataHash, long vramBytes, long triangles)
    {
        vramBytes = Math.Max(0, vramBytes);
        triangles = Math.Max(0, triangles);

        StoreReceivedPerformanceMetrics(dataHash, vramBytes, triangles);

        if (pair == null)
            return;

        var uid = pair.UserData?.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return;

        if (!(pair.UserPair?.OwnPermissions.IsPaused() ?? false))
        {
            if (_autoThresholdPausedUsers.ContainsKey(uid))
                RemoveThresholdPausedUser(uid);

            return;
        }

        if (!_autoThresholdPausedUsers.ContainsKey(uid))
            return;

        pair.LastAppliedApproximateVRAMBytes = vramBytes;
        pair.LastAppliedDataTris = triangles;
        _mediator.Publish(new RefreshUiMessage());

        if (pair.AutoPausedByCap)
            return;

        var config = _playerPerformanceConfigService.Current;
        if (IsIgnored(config, pair.UserData))
            return;

        bool isPrefPerm = pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);
        bool exceedsVramAutoPause = CheckForThreshold(
            config.AutoPausePlayersExceedingThresholds,
            config.VRAMSizeAutoPauseThresholdMiB * 1024L * 1024L,
            vramBytes,
            config.AutoPausePlayersWithPreferredPermissionsExceedingThresholds,
            isPrefPerm);

        bool exceedsTrisAutoPause = CheckForThreshold(
            config.AutoPausePlayersExceedingThresholds,
            config.TrisAutoPauseThresholdThousands * 1000L,
            triangles,
            config.AutoPausePlayersWithPreferredPermissionsExceedingThresholds,
            isPrefPerm);

        if (exceedsVramAutoPause || exceedsTrisAutoPause)
            return;

        RemoveThresholdPausedUser(uid);
        _mediator.Publish(new ResumeMessage(pair.UserData));

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Auto-unpaused {user} from received sidecar metrics - now under thresholds (VRAM={vram}, Tris={tris})",
                pair.UserData.AliasOrUID,
                UiSharedService.ByteToString(vramBytes, addSuffix: true),
                triangles);
        }
    }

    public bool IsThresholdAutoPaused(Pair? pair)
    {
        if (pair == null)
            return false;

        var uid = pair.UserData?.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return false;

        return pair.UserPair?.OwnPermissions.IsPaused() == true
            && _autoThresholdPausedUsers.ContainsKey(uid);
    }

    public bool IsRememberedThresholdAutoPaused(Pair? pair)
    {
        if (pair == null)
            return false;

        var uid = pair.UserData?.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return false;

        return _autoThresholdPausedUsers.ContainsKey(uid);
    }


    private void HandleExplicitResume(UserData? user)
    {
        if (user == null || string.IsNullOrWhiteSpace(user.UID))
            return;

        if (_autoThresholdPausedUsers.TryRemove(user.UID, out _))
        {
            MarkStateDirty();
            _mediator.Publish(new RefreshUiMessage());
        }
    }

    private void PruneRememberedThresholdPausedUsers()
    {
        if (_autoThresholdPausedUsers.IsEmpty)
            return;

        bool removedAny = false;
        foreach (var kv in _autoThresholdPausedUsers.ToArray())
        {
            var uid = kv.Key;
            if (string.IsNullOrWhiteSpace(uid))
                continue;

            var pair = _services.GetService<PairManager>()?.GetPairByUID(uid);
            if (pair == null)
                continue;

            if (!(pair.UserPair?.OwnPermissions.IsPaused() ?? false))
            {
                if (_autoThresholdPausedUsers.TryRemove(uid, out _))
                    removedAny = true;
            }
        }

        if (removedAny)
        {
            MarkStateDirty();
            _mediator.Publish(new RefreshUiMessage());
        }
    }

    public async Task<bool> CheckBothThresholds(PairHandler pairHandler, CharacterData charaData)
    {
        var config = _playerPerformanceConfigService.Current;

        var stableDataHash = charaData.DataHash.Value;
        HashSet<string>? moddedTextureHashes = null;
        HashSet<string>? moddedModelHashes = null;

        var hasStableVram = TryGetStableMetricCache(_receivedVramByDataHash, stableDataHash, out _) || TryGetStableMetricCache(_stableVramByDataHash, stableDataHash, out _);
        var hasStableTris = TryGetStableMetricCache(_receivedTrisByDataHash, stableDataHash, out _) || TryGetStableMetricCache(_stableTrisByDataHash, stableDataHash, out _);

        if (!hasStableVram || !hasStableTris)
        {
            moddedTextureHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            moddedModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            CollectVisiblePlayerStateHashes(charaData, moddedTextureHashes, moddedModelHashes);
        }

        bool notPausedAfterVram = ComputeAndAutoPauseOnVRAMUsageThresholds(pairHandler, charaData, [], moddedTextureHashes, moddedModelHashes, stableDataHash);
        if (!notPausedAfterVram) return false;

        bool notPausedAfterTris = await CheckTriangleUsageThresholds(pairHandler, charaData, moddedModelHashes, stableDataHash).ConfigureAwait(false);
        if (!notPausedAfterTris) return false;

        var vramUsage = pairHandler.Pair.LastAppliedApproximateVRAMBytes;
        var triUsage = pairHandler.Pair.LastAppliedDataTris;

        bool isPrefPerm = pairHandler.Pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);
        {
            var uid = pairHandler.Pair.UserData.UID;

            if (!pairHandler.Pair.UserPair.OwnPermissions.IsPaused())
            {
                RemoveThresholdPausedUser(uid);
            }
            else
            {
                if (!_autoThresholdPausedUsers.IsEmpty
                    && _autoThresholdPausedUsers.ContainsKey(uid)
                    && !pairHandler.Pair.AutoPausedByCap)
                {
                    var cfg = _playerPerformanceConfigService.Current;

                    bool exceedsVramAutoPause = CheckForThreshold(
                        cfg.AutoPausePlayersExceedingThresholds,
                        cfg.VRAMSizeAutoPauseThresholdMiB * 1024L * 1024L,
                        vramUsage,
                        cfg.AutoPausePlayersWithPreferredPermissionsExceedingThresholds,
                        isPrefPerm);

                    bool exceedsTrisAutoPause = CheckForThreshold(
                        cfg.AutoPausePlayersExceedingThresholds,
                        cfg.TrisAutoPauseThresholdThousands * 1000L,
                        triUsage,
                        cfg.AutoPausePlayersWithPreferredPermissionsExceedingThresholds,
                        isPrefPerm);

                    if (!exceedsVramAutoPause && !exceedsTrisAutoPause)
                    {
                        RemoveThresholdPausedUser(uid);

                        _mediator.Publish(new ResumeMessage(pairHandler.Pair.UserData));

                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("Auto-unpaused {user}  now under thresholds (VRAM={vram}, Tris={tris})",
                                pairHandler.Pair.UserData.AliasOrUID,
                                UiSharedService.ByteToString(vramUsage, addSuffix: true),
                                triUsage);
                        }
                    }
                }
            }
        }

        if (IsIgnored(config, pairHandler.Pair.UserData))
            return true;

        bool exceedsTris = CheckForThreshold(config.WarnOnExceedingThresholds, config.TrisWarningThresholdThousands * 1000,
            triUsage, config.WarnOnPreferredPermissionsExceedingThresholds, isPrefPerm);
        bool exceedsVram = CheckForThreshold(config.WarnOnExceedingThresholds, config.VRAMSizeWarningThresholdMiB * 1024 * 1024,
            vramUsage, config.WarnOnPreferredPermissionsExceedingThresholds, isPrefPerm);

        bool exceedsAny = exceedsTris || exceedsVram;
        string warnUid = pairHandler.Pair.UserData.UID;

        if (!exceedsAny)
        {
            _warnedForPlayers.TryRemove(warnUid, out _);
            return true;
        }

        if (_warnedForPlayers.TryGetValue(warnUid, out bool hadWarning) && hadWarning)
            return true;

        _warnedForPlayers[warnUid] = true;

        if (exceedsVram)
        {
            _mediator.Publish(new EventMessage(new Event(pairHandler.Pair.PlayerName, pairHandler.Pair.UserData, nameof(PlayerPerformanceService), EventSeverity.Warning,
                $"Exceeds VRAM threshold: ({UiSharedService.ByteToString(vramUsage, addSuffix: true)}/{config.VRAMSizeWarningThresholdMiB} MiB)")));
        }

        if (exceedsTris)
        {
            _mediator.Publish(new EventMessage(new Event(pairHandler.Pair.PlayerName, pairHandler.Pair.UserData, nameof(PlayerPerformanceService), EventSeverity.Warning,
                $"Exceeds triangle threshold: ({triUsage}/{config.TrisAutoPauseThresholdThousands * 1000} triangles)")));
        }

        if (exceedsTris || exceedsVram)
        {
            string warningText = string.Empty;
            if (exceedsTris && !exceedsVram)
            {
                warningText = $"Player {pairHandler.Pair.PlayerName} ({pairHandler.Pair.UserData.AliasOrUID}) exceeds your configured triangle warning threshold (" +
                    $"{triUsage}/{config.TrisWarningThresholdThousands * 1000} triangles).";
            }
            else if (!exceedsTris)
            {
                warningText = $"Player {pairHandler.Pair.PlayerName} ({pairHandler.Pair.UserData.AliasOrUID}) exceeds your configured VRAM warning threshold (" +
                    $"{UiSharedService.ByteToString(vramUsage, true)}/{config.VRAMSizeWarningThresholdMiB} MiB).";
            }
            else
            {
                warningText = $"Player {pairHandler.Pair.PlayerName} ({pairHandler.Pair.UserData.AliasOrUID}) exceeds both VRAM warning threshold (" +
                    $"{UiSharedService.ByteToString(vramUsage, true)}/{config.VRAMSizeWarningThresholdMiB} MiB) and " +
                    $"triangle warning threshold ({triUsage}/{config.TrisWarningThresholdThousands * 1000} triangles).";
            }

            _mediator.Publish(new NotificationMessage($"{pairHandler.Pair.PlayerName} ({pairHandler.Pair.UserData.AliasOrUID}) exceeds performance threshold(s)",
                warningText, MareConfiguration.Models.NotificationType.Warning));
        }

        return true;
    }
    private sealed record StableMetricCacheEntry(long Value, long LastAccessTick);

    private bool TryGetStableMetricCache(ConcurrentDictionary<string, StableMetricCacheEntry> cache, string? dataHash, out long value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(dataHash))
            return false;

        PruneStableMetricCachesIfNeeded();

        if (!cache.TryGetValue(dataHash, out var entry))
            return false;

        var refreshed = entry with { LastAccessTick = Environment.TickCount64 };
        cache.TryUpdate(dataHash, refreshed, entry);
        value = entry.Value;
        return true;
    }

    private void StoreStableMetricCache(ConcurrentDictionary<string, StableMetricCacheEntry> cache, string? dataHash, long value)
    {
        if (string.IsNullOrWhiteSpace(dataHash))
            return;

        PruneStableMetricCachesIfNeeded();
        cache[dataHash] = new StableMetricCacheEntry(value, Environment.TickCount64);
    }

    private void PruneStableMetricCachesIfNeeded()
    {
        var now = Environment.TickCount64;
        var next = Interlocked.Read(ref _nextStableMetricCachePruneTick);
        if (now < next)
            return;

        if (Interlocked.CompareExchange(ref _nextStableMetricCachePruneTick, now + StableMetricCachePruneIntervalMs, next) != next)
            return;

        PruneStableMetricCache(_stableVramByDataHash, now);
        PruneStableMetricCache(_stableTrisByDataHash, now);
        PruneStableMetricCache(_receivedVramByDataHash, now);
        PruneStableMetricCache(_receivedTrisByDataHash, now);
    }

    private static void PruneStableMetricCache(ConcurrentDictionary<string, StableMetricCacheEntry> cache, long now)
    {
        foreach (var kv in cache)
        {
            if (now - kv.Value.LastAccessTick <= StableMetricCacheTtlMs)
                continue;

            cache.TryRemove(kv.Key, out _);
        }
    }

    private void TryKickAutoCapStateSave()
    {
        if (Interlocked.CompareExchange(ref _stateDirty, 0, 0) == 0)
            return;

        var now = DateTime.UtcNow;
        if (now < _nextStateSaveAttemptUtc)
            return;

        if (Interlocked.Exchange(ref _stateSaveInFlight, 1) != 0)
            return;

        _ = SaveAutoCapStateAsync().ContinueWith(_ =>
        {
            Interlocked.Exchange(ref _stateSaveInFlight, 0);
        }, TaskScheduler.Default);
    }
    public async Task<bool> CheckTriangleUsageThresholds(PairHandler pairHandler, CharacterData charaData, HashSet<string>? precomputedModelHashes = null, string? stableDataHash = null)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long triUsage = 0;

        if (!TryGetStableMetricCache(_receivedTrisByDataHash, stableDataHash, out triUsage)
            && !TryGetStableMetricCache(_stableTrisByDataHash, stableDataHash, out triUsage))
        {
            var moddedModelHashes = precomputedModelHashes ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (precomputedModelHashes == null)
            {
                var tmpTex = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                CollectVisiblePlayerStateHashes(charaData, tmpTex, moddedModelHashes);
            }

            List<(string Hash, long Triangles)>? debugContributors = _logger.IsEnabled(LogLevel.Debug)
                ? new List<(string Hash, long Triangles)>()
                : null;

            foreach (var hash in moddedModelHashes)
            {
                var trianglesForHash = await _xivDataAnalyzer.GetTrianglesByHash(hash).ConfigureAwait(false);
                triUsage += trianglesForHash;

                if (debugContributors != null && trianglesForHash > 0)
                    debugContributors.Add((hash, trianglesForHash));
            }

            if (debugContributors != null)
            {
                foreach (var item in debugContributors
                    .OrderByDescending(v => v.Triangles)
                    .Take(20))
                {
                    _logger.LogDebug("Triangle contributor: {hash} => {triangles}", item.Hash, item.Triangles);
                }
            }

            StoreStableMetricCache(_stableTrisByDataHash, stableDataHash, triUsage);
        }

        pair.LastAppliedDataTris = triUsage;

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Calculated triangle usage for {p}", pairHandler);

        // no warning of any kind on ignored pairs
        if (IsIgnored(config, pair.UserData))
            return true;

        bool isPrefPerm = pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);

        // now check auto pause
        if (CheckForThreshold(config.AutoPausePlayersExceedingThresholds, config.TrisAutoPauseThresholdThousands * 1000,
            triUsage, config.AutoPausePlayersWithPreferredPermissionsExceedingThresholds, isPrefPerm))
        {
            _mediator.Publish(new NotificationMessage($"{pair.PlayerName} ({pair.UserData.AliasOrUID}) automatically paused",
                $"Player {pair.PlayerName} ({pair.UserData.AliasOrUID}) exceeded your configured triangle auto pause threshold (" +
                $"{triUsage}/{config.TrisAutoPauseThresholdThousands * 1000} triangles)" +
                $" and has been automatically paused.",
                MareConfiguration.Models.NotificationType.Warning));

            _mediator.Publish(new EventMessage(new Event(pair.PlayerName, pair.UserData, nameof(PlayerPerformanceService), EventSeverity.Warning,
                $"Exceeds triangle threshold: automatically paused ({triUsage}/{config.TrisAutoPauseThresholdThousands * 1000} triangles)")));

            AddThresholdPausedUser(pair.UserData);
            pair.EnterPausedVanillaState();
            _mediator.Publish(new PauseMessage(pair.UserData));

            return false;
        }

        return true;
    }

    public bool ComputeAndAutoPauseOnVRAMUsageThresholds(PairHandler pairHandler, CharacterData charaData, List<DownloadFileTransfer> toDownloadFiles, HashSet<string>? precomputedTextureHashes = null, HashSet<string>? precomputedModelHashes = null, string? stableDataHash = null)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long vramUsage = 0;
        var hasPendingDownloads = toDownloadFiles != null && toDownloadFiles.Count > 0;

        if (TryGetStableMetricCache(_receivedVramByDataHash, stableDataHash, out vramUsage))
        {
            pair.LastAppliedApproximateVRAMBytes = vramUsage;
        }
        else if (!hasPendingDownloads && TryGetStableMetricCache(_stableVramByDataHash, stableDataHash, out vramUsage))
        {
            pair.LastAppliedApproximateVRAMBytes = vramUsage;
        }
        else
        {
            Dictionary<string, long>? pendingDownloadSizes = null;
            if (hasPendingDownloads)
            {
                pendingDownloadSizes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                foreach (var f in toDownloadFiles)
                {
                    if (!pendingDownloadSizes.ContainsKey(f.Hash))
                        pendingDownloadSizes[f.Hash] = f.TotalRaw;
                }
            }

            HashSet<string> hashes;
            if (precomputedTextureHashes == null && precomputedModelHashes == null)
            {
                hashes = CollectVisiblePlayerStateHashesUnique(charaData);
            }
            else
            {
                hashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (precomputedTextureHashes != null) foreach (var h in precomputedTextureHashes) if (!string.IsNullOrWhiteSpace(h)) hashes.Add(h);
                if (precomputedModelHashes != null) foreach (var h in precomputedModelHashes) if (!string.IsNullOrWhiteSpace(h)) hashes.Add(h);

                if (hashes.Count == 0)
                    hashes = CollectVisiblePlayerStateHashesUnique(charaData);
            }

            foreach (var hash in hashes)
            {
                long bestBytesForHash = 0;

                if (pendingDownloadSizes != null && pendingDownloadSizes.TryGetValue(hash, out var pendingSize))
                {
                    vramUsage += Math.Max(0, pendingSize);
                    continue;
                }

                var fileEntries = _fileCacheManager
                    .GetAllFileCachesByHash(hash, ignoreCacheEntries: false, validate: true)
                    .Where(e => !string.IsNullOrWhiteSpace(e.ResolvedFilepath) && File.Exists(e.ResolvedFilepath))
                    .GroupBy(e => e.ResolvedFilepath, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.First());

                foreach (var fileEntry in fileEntries)
                {
                    var path = fileEntry.ResolvedFilepath;
                    var ext = Path.GetExtension(path);
                    long candidateBytes = 0;

                    var cacheKey = hash + "|" + path;

                    if (ext.Equals(".tex", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!TryGetCachedTexVram(cacheKey, path, out candidateBytes))
                            continue;
                    }
                    else if (ext.Equals(".mdl", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!TryGetCachedMdlVram(cacheKey, path, out candidateBytes))
                            continue;
                    }
                    else
                    {
                        continue;
                    }

                    if (candidateBytes > bestBytesForHash)
                        bestBytesForHash = candidateBytes;
                }

                vramUsage += Math.Max(0, bestBytesForHash);
            }

            if (!hasPendingDownloads)
            {
                StoreStableMetricCache(_stableVramByDataHash, stableDataHash, vramUsage);
            }

            pair.LastAppliedApproximateVRAMBytes = vramUsage;
        }

        _logger.LogDebug("Calculated VRAM usage for {p}", pairHandler);

        if (IsIgnored(config, pair.UserData))
            return true;

        bool isPrefPerm = pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);

        if (CheckForThreshold(config.AutoPausePlayersExceedingThresholds, config.VRAMSizeAutoPauseThresholdMiB * 1024 * 1024,
            vramUsage, config.AutoPausePlayersWithPreferredPermissionsExceedingThresholds, isPrefPerm))
        {
            _mediator.Publish(new NotificationMessage($"{pair.PlayerName} ({pair.UserData.AliasOrUID}) automatically paused",
                $"Player {pair.PlayerName} ({pair.UserData.AliasOrUID}) exceeded your configured VRAM auto pause threshold (" +
                $"{UiSharedService.ByteToString(vramUsage, addSuffix: true)}/{config.VRAMSizeAutoPauseThresholdMiB}MiB)" +
                $" and has been automatically paused.",
                MareConfiguration.Models.NotificationType.Warning));

            AddThresholdPausedUser(pair.UserData);
            pair.EnterPausedVanillaState();

            _mediator.Publish(new PauseMessage(pair.UserData));

            _mediator.Publish(new EventMessage(new Event(pair.PlayerName, pair.UserData, nameof(PlayerPerformanceService), EventSeverity.Warning,
                $"Exceeds VRAM threshold: automatically paused ({UiSharedService.ByteToString(vramUsage, addSuffix: true)}/{config.VRAMSizeAutoPauseThresholdMiB} MiB)")));

            return false;
        }

        if (pair.AutoPausedByCap && !pair.UserPair.OwnPermissions.IsPaused())
        {
            pair.AutoPausedByCap = false;
        }

        bool isEffectivelyPaused = pair.UserPair.OwnPermissions.IsPaused() || pair.AutoPausedByCap;
        bool contributesToPool = !pair.IsDirectlyPaired && pair.IsVisible && !isEffectivelyPaused;

        if (contributesToPool)
        {
            UpsertSyncshellVram(pair.UserData.UID, vramUsage);
            _knownUsersByUid.TryAdd(pair.UserData.UID, pair.UserData);
        }
        else
        {
            RemoveSyncshellVram(pair.UserData.UID);
        }

        int capMiB = _playerPerformanceConfigService.Current.SyncshellVramCapMiB;
        if (capMiB > 0)
        {
            long capBytes = (long)capMiB * 1024L * 1024L;
            long totalBytes = GetTotalSyncshellVramBytes();

            if (!pair.IsDirectlyPaired && contributesToPool && totalBytes > capBytes)
            {
                if (!pair.AutoPausedByCap)
                {
                    pair.AutoPausedByCap = true;
                    pair.EnterPausedVanillaState();
                    _mediator.Publish(new PauseMessage(pair.UserData));
                    _logger.LogInformation("Auto-paused {user}  pool {total}/{cap}",
                        pair.UserData.AliasOrUID,
                        UiSharedService.ByteToString(totalBytes, addSuffix: true),
                        UiSharedService.ByteToString(capBytes, addSuffix: true));
                    _autoCapPausedUsers[pair.UserData.UID] = pair.UserData;
                    _autoCapPausedVramByUid[pair.UserData.UID] = Math.Max(0, vramUsage);
                    RemoveSyncshellVram(pair.UserData.UID);
                    MarkAutoCapCandidatesDirty();
                    MarkStateDirty();
                }
            }
        }

        return true;
    }

    private void AutoPauseHandler()
    {
        // This runs on both FrameworkUpdate + DelayedFrameworkUpdate.
        // Throttle to prevent repeated expensive work and reduce hitching when player lists churn.
        var now = DateTime.UtcNow;
        if (now < _nextAutoCapCheckUtc) return;
        _nextAutoCapCheckUtc = now + AutoCapCheckInterval;

        int capMiB = _playerPerformanceConfigService.Current.SyncshellVramCapMiB;
        if (capMiB <= 0)
        {
            _lastObservedAutoCapBytes = 0;
            return;
        }

        long capBytes = (long)capMiB * 1024L * 1024L;

        // If cap changed, force a scan even if nothing else changed
        bool capChanged = capBytes != Interlocked.Read(ref _lastObservedAutoCapBytes);
        if (capChanged)
        {
            Interlocked.Exchange(ref _lastObservedAutoCapBytes, capBytes);
            MarkAutoCapCandidatesDirty();
        }

        // Nothing changed since last evaluation -> skip all dictionary scans
        if (Interlocked.Exchange(ref _autoCapCandidatesDirty, 0) == 0)
            return;

        long totalBytes = GetTotalSyncshellVramBytes();

        // Over cap: pause ONE highest-VRAM contributor not yet auto-capped (no sorting/allocations)
        if (totalBytes > capBytes)
        {
            string? nextUid = null;
            long nextVram = long.MinValue;

            foreach (var kv in _syncshellVramByUid)
            {
                var uid = kv.Key;
                var vram = kv.Value;

                if (_autoCapPausedUsers.ContainsKey(uid)) continue;

                if (vram > nextVram)
                {
                    nextVram = vram;
                    nextUid = uid;
                }
            }

            if (nextUid != null && _knownUsersByUid.TryGetValue(nextUid, out var user))
            {
                var last = _syncshellVramByUid.GetValueOrDefault(nextUid, 0);
                _autoCapPausedUsers[nextUid] = user;
                _autoCapPausedVramByUid[nextUid] = last;

                foreach (var pausedPair in EnumerateAllPairs())
                {
                    if (!string.Equals(pausedPair.UserData?.UID, nextUid, StringComparison.Ordinal))
                        continue;

                    pausedPair.AutoPausedByCap = true;
                    pausedPair.EnterPausedVanillaState();
                }

                _mediator.Publish(new PauseMessage(user));
                RemoveSyncshellVram(nextUid);
                MarkAutoCapCandidatesDirty();
                MarkStateDirty();

                _logger.LogInformation("Auto-paused {user} \x97 lastVRAM {last} \x97 pool {total}/{cap}",
                    user.AliasOrUID,
                    UiSharedService.ByteToString(last, true),
                    UiSharedService.ByteToString(totalBytes, true),
                    UiSharedService.ByteToString(capBytes, true));

                return; // exactly one action per tick
            }
        }

        // Under cap: resume ONE smallest-VRAM auto-capped UID (no sorting/allocations)
        if (totalBytes <= capBytes && !_autoCapPausedUsers.IsEmpty)
        {
            string? nextUid = null;
            long nextVram = long.MaxValue;

            foreach (var kv in _autoCapPausedVramByUid)
            {
                var uid = kv.Key;
                var vram = kv.Value;

                if (vram < nextVram)
                {
                    nextVram = vram;
                    nextUid = uid;
                }
            }

            if (nextUid is null) return;
            if (!_autoCapPausedUsers.TryGetValue(nextUid, out var user)) return;

            long lastKnown = _autoCapPausedVramByUid.GetValueOrDefault(nextUid, long.MaxValue);
            long estimateAfterResume = totalBytes + lastKnown;
            if (estimateAfterResume > capBytes) return;

            _autoCapPausedUsers.TryRemove(nextUid, out _);
            _autoCapPausedVramByUid.TryRemove(nextUid, out _);
            MarkAutoCapCandidatesDirty();

            _mediator.Publish(new ResumeMessage(user));

            _logger.LogInformation("Auto-unpaused {user} \x97 lastVRAM {last} \x97 pool {total}/{cap}",
                user.AliasOrUID,
                UiSharedService.ByteToString(lastKnown, true),
                UiSharedService.ByteToString(totalBytes, true),
                UiSharedService.ByteToString(capBytes, true));

            RemoveSyncshellVram(nextUid);
            MarkStateDirty();
        }
    }
    private void CollectVisiblePlayerStateHashes(CharacterData charaData, HashSet<string> moddedTextureHashes, HashSet<string> moddedModelHashes)
    {
        moddedTextureHashes.Clear();
        moddedModelHashes.Clear();

        if (!charaData.FileReplacements.TryGetValue(ObjectKind.Player, out var list) || list == null)
            return;

        var semiTransientByKind = new Dictionary<ObjectKind, HashSet<string>>();

        foreach (var replacement in list)
        {
            if (!ShouldCountForVisiblePlayerState(ObjectKind.Player, replacement, semiTransientByKind))
                continue;

            bool hasTex = false;
            bool hasMdl = false;

            foreach (var gamePath in replacement.GamePaths ?? Enumerable.Empty<string>())
            {
                if (!hasTex && IsVisiblePlayerTextureGamePath(gamePath))
                    hasTex = true;

                if (!hasMdl && IsVisiblePlayerModelGamePath(gamePath))
                    hasMdl = true;

                if (hasTex && hasMdl)
                    break;
            }

            if (hasTex) moddedTextureHashes.Add(replacement.Hash);
            if (hasMdl) moddedModelHashes.Add(replacement.Hash);
        }
    }


    private bool ShouldCountForVisiblePlayerState(ObjectKind objectKind, FileReplacementData? replacement, Dictionary<ObjectKind, HashSet<string>> semiTransientByKind)
    {
        if (replacement == null)
            return false;

        if (string.IsNullOrWhiteSpace(replacement.Hash))
            return false;

        if (!string.IsNullOrWhiteSpace(replacement.FileSwapPath))
            return false;

        if (IsSemiTransientOnly(objectKind, replacement.GamePaths, semiTransientByKind))
            return false;

        return replacement.GamePaths != null && replacement.GamePaths.Any(IsVisiblePlayerStateGamePath);
    }

    private static bool IsVisiblePlayerTextureGamePath(string? gamePath)
        => IsVisiblePlayerStateGamePath(gamePath, ".tex");

    private static bool IsVisiblePlayerModelGamePath(string? gamePath)
        => IsVisiblePlayerStateGamePath(gamePath, ".mdl");

    private static bool IsVisiblePlayerStateGamePath(string? gamePath)
        => IsVisiblePlayerStateGamePath(gamePath, requiredExtension: null);

    private static bool IsVisiblePlayerStateGamePath(string? gamePath, string? requiredExtension)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var path = NormalizePerformanceGamePath(gamePath);

        var extension = Path.GetExtension(path);
        if (string.IsNullOrWhiteSpace(extension))
            return false;

        if (requiredExtension != null && !extension.Equals(requiredExtension, StringComparison.OrdinalIgnoreCase))
            return false;

        if (!extension.Equals(".mdl", StringComparison.OrdinalIgnoreCase)
            && !extension.Equals(".tex", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return path.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizePerformanceGamePath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return string.Empty;

        return gamePath.ToLowerInvariant().Replace("\\", "/", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsSemiTransientOnly(ObjectKind objectKind, IEnumerable<string>? gamePaths, Dictionary<ObjectKind, HashSet<string>> semiTransientByKind)
    {
        if (gamePaths == null)
            return false;

        if (!semiTransientByKind.TryGetValue(objectKind, out var semiTransientPaths))
            semiTransientByKind[objectKind] = semiTransientPaths = _transientResourceManager.GetSemiTransientResources(objectKind);

        if (semiTransientPaths.Count == 0)
            return false;

        bool any = false;
        foreach (var gamePath in gamePaths)
        {
            any = true;

            var normalizedGamePath = NormalizePerformanceGamePath(gamePath);
            if (string.IsNullOrWhiteSpace(normalizedGamePath) || !semiTransientPaths.Contains(normalizedGamePath))
                return false;
        }

        return any;
    }

    private void MarkAutoCapCandidatesDirty()
    {
        Interlocked.Exchange(ref _autoCapCandidatesDirty, 1);
        _nextAutoCapCheckUtc = DateTime.MinValue;
    }
    private static bool CheckForThreshold(bool thresholdEnabled, long threshold, long value, bool checkForPrefPerm, bool isPrefPerm) =>
        thresholdEnabled && threshold > 0 && threshold < value && ((checkForPrefPerm && isPrefPerm) || !isPrefPerm);

    private sealed class AutoCapState
    {
        public int Version { get; set; } = 2;
        public DateTime SavedUtc { get; set; }
        public Dictionary<string, long> PausedVramBytes { get; set; } = new(StringComparer.Ordinal);
        public Dictionary<string, string> Aliases { get; set; } = new(StringComparer.Ordinal); // optional, nice for logs
        public Dictionary<string, string> ThresholdPausedAliases { get; set; } = new(StringComparer.Ordinal);
    }

    private void AddThresholdPausedUser(UserData user)
    {
        if (string.IsNullOrWhiteSpace(user?.UID))
            return;

        _autoThresholdPausedUsers[user.UID] = user;
        MarkStateDirty();
    }

    private void RemoveThresholdPausedUser(string? uid)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return;

        if (_autoThresholdPausedUsers.TryRemove(uid, out _))
            MarkStateDirty();
    }

    private static bool IsIgnored(PlayerPerformanceConfig config, UserData userData)
    {
        return config.UIDsToIgnore.Exists(uid =>
            string.Equals(uid, userData.Alias, StringComparison.Ordinal) ||
            string.Equals(uid, userData.UID, StringComparison.Ordinal));
    }

    private void MarkStateDirty() => Interlocked.Exchange(ref _stateDirty, 1);

    private void EnsureStateDir()
    {
        var dir = Path.GetDirectoryName(_statePath)!;
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
    }

    private async Task SaveAutoCapStateAsync()
    {
        if (Interlocked.CompareExchange(ref _stateDirty, 0, 0) == 0) return;

        // throttle to ~1 save / 2s
        var now = DateTime.UtcNow;
        var nextEligible = _lastSave + TimeSpan.FromSeconds(2);
        if (now < nextEligible)
        {
            _nextStateSaveAttemptUtc = nextEligible;
            return;
        }

        // consume dirty only when we are actually proceeding
        if (Interlocked.Exchange(ref _stateDirty, 0) == 0) return;

        try
        {
            EnsureStateDir();

            // snapshot to avoid racing ConcurrentDictionary during serialization
            var paused = _autoCapPausedVramByUid.ToArray();
            var aliases = _autoCapPausedUsers.ToArray();
            var thresholdAliases = _autoThresholdPausedUsers.ToArray();

            var state = new AutoCapState
            {
                Version = 2,
                SavedUtc = DateTime.UtcNow,
                PausedVramBytes = paused.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal),
                Aliases = aliases.ToDictionary(kv => kv.Key, kv => kv.Value.AliasOrUID, StringComparer.Ordinal),
                ThresholdPausedAliases = thresholdAliases.ToDictionary(kv => kv.Key, kv => kv.Value.AliasOrUID, StringComparer.Ordinal),
            };

            var json = JsonSerializer.Serialize(state, new JsonSerializerOptions
            {
                WriteIndented = false
            });

            var tmp = _statePath + ".tmp";
            await File.WriteAllTextAsync(tmp, json).ConfigureAwait(false);
            if (File.Exists(_statePath)) File.Delete(_statePath);
            File.Move(tmp, _statePath);

            _lastSave = DateTime.UtcNow;
            _nextStateSaveAttemptUtc = _lastSave + TimeSpan.FromSeconds(2);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save auto-cap state");
        }
    }

    private void UpsertSyncshellVram(string uid, long bytes)
    {
        bytes = Math.Max(0, bytes);

        while (true)
        {
            if (_syncshellVramByUid.TryGetValue(uid, out var existing))
            {
                // No actual change -> do nothing
                if (existing == bytes)
                    return;

                if (_syncshellVramByUid.TryUpdate(uid, bytes, existing))
                {
                    Interlocked.Add(ref _syncshellVramTotalBytes, bytes - existing);
                    MarkAutoCapCandidatesDirty();
                    return;
                }

                continue; // raced, retry
            }

            if (_syncshellVramByUid.TryAdd(uid, bytes))
            {
                Interlocked.Add(ref _syncshellVramTotalBytes, bytes);
                MarkAutoCapCandidatesDirty();
                return;
            }

            // raced, retry
        }
    }

    private void RemoveSyncshellVram(string uid)
    {
        if (_syncshellVramByUid.TryRemove(uid, out var removed))
        {
            Interlocked.Add(ref _syncshellVramTotalBytes, -removed);
            MarkAutoCapCandidatesDirty();
        }
    }

    private static long GetStamp(string path)
    {
        var fi = new FileInfo(path);
        unchecked
        {
            return (fi.LastWriteTimeUtc.Ticks << 1) ^ fi.Length;
        }
    }

    private bool TryGetCachedTexVram(string cacheKey, string path, out long bytes)
    {
        bytes = 0;
        long stamp;
        try { stamp = GetStamp(path); }
        catch { return false; }

        if (_textureVramByHash.TryGetValue(cacheKey, out var cached) && cached.Stamp == stamp)
        {
            bytes = cached.Bytes;
            return true;
        }

        if (!VramEstimator.TryEstimateTexVramBytes(path, out bytes))
            return false;

        _textureVramByHash[cacheKey] = (bytes, stamp);
        return true;
    }

    private bool TryGetCachedMdlVram(string cacheKey, string path, out long bytes)
    {
        bytes = 0;
        long stamp;
        try { stamp = GetStamp(path); }
        catch { return false; }

        if (_modelVramByHash.TryGetValue(cacheKey, out var cached) && cached.Stamp == stamp)
        {
            bytes = cached.Bytes;
            return true;
        }

        if (!VramEstimator.TryEstimateMdlVramBytes(path, out bytes))
            return false;

        _modelVramByHash[cacheKey] = (bytes, stamp);
        return true;
    }

    private HashSet<string> CollectVisiblePlayerStateHashesUnique(CharacterData charaData)
    {
        var textureHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var modelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        CollectVisiblePlayerStateHashes(charaData, textureHashes, modelHashes);

        textureHashes.UnionWith(modelHashes);
        return textureHashes;
    }

    private IEnumerable<Pair> EnumerateAllPairs()
    {
        PairManager? pm = null;
        try
        {
            pm = _services.GetService<PairManager>();
        }
        catch
        {
            // best effort
        }

        if (pm == null)
            yield break;

        var seenUids = new HashSet<string>(StringComparer.Ordinal);
        var pairs = new List<Pair>();

        pm.ForEachPair(pair =>
        {
            if (pair?.UserData?.UID is string uid && !string.IsNullOrWhiteSpace(uid))
            {
                if (seenUids.Add(uid))
                    pairs.Add(pair);

                return;
            }

            if (pair != null)
                pairs.Add(pair);
        });

        foreach (var pair in pairs)
            yield return pair;
    }

    private void ClearPendingThresholdResumeOnConnect()
    {
        _pendingThresholdResumeOnConnect.Clear();
        _pendingThresholdResumeReadyTick = 0;
        _pendingThresholdResumeDeadlineTick = 0;
    }

    private void ProcessPendingThresholdResumeOnConnect(long nowTick)
    {
        if (_pendingThresholdResumeOnConnect.IsEmpty)
            return;

        if (nowTick < _pendingThresholdResumeReadyTick)
            return;

        var pairManager = _services.GetService<PairManager>();
        if (pairManager == null)
            return;

        var forceDispatch = _pendingThresholdResumeDeadlineTick > 0 && nowTick >= _pendingThresholdResumeDeadlineTick;

        foreach (var kv in _pendingThresholdResumeOnConnect.ToArray())
        {
            var uid = kv.Key;
            var user = kv.Value;

            if (string.IsNullOrWhiteSpace(uid) || user == null)
            {
                _pendingThresholdResumeOnConnect.TryRemove(uid, out _);
                continue;
            }

            var pair = pairManager.GetPairByUID(uid);
            var pairReady = pair?.UserPair != null;

            if (!pairReady && !forceDispatch)
                continue;

            try
            {
                _mediator.Publish(new ResumeMessage(user));
            }
            catch
            {
                // best effort
            }
            finally
            {
                _pendingThresholdResumeOnConnect.TryRemove(uid, out _);
            }
        }

        if (_pendingThresholdResumeOnConnect.IsEmpty)
        {
            _pendingThresholdResumeReadyTick = 0;
            _pendingThresholdResumeDeadlineTick = 0;
            _mediator.Publish(new RefreshUiMessage());
        }
    }

    private void HandleConnected()
    {
        var cfg = _playerPerformanceConfigService.Current;
        if (!cfg.UnpauseThresholdAutoPausedPairsOnConnect)
        {
            ClearPendingThresholdResumeOnConnect();
            return;
        }

        var thresholdPausedUsers = _autoThresholdPausedUsers.ToArray();
        if (thresholdPausedUsers.Length == 0)
        {
            ClearPendingThresholdResumeOnConnect();
            return;
        }

        _pendingThresholdResumeOnConnect.Clear();
        foreach (var kv in thresholdPausedUsers)
        {
            if (!string.IsNullOrWhiteSpace(kv.Key))
                _pendingThresholdResumeOnConnect[kv.Key] = kv.Value;
        }

        var nowTick = Environment.TickCount64;
        _pendingThresholdResumeReadyTick = nowTick + 1500;
        _pendingThresholdResumeDeadlineTick = nowTick + 10000;

        _mediator.Publish(new RefreshUiMessage());

        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Queued auto-unpause for {count} remembered threshold-paused users on connect after state warmup", _pendingThresholdResumeOnConnect.Count);
    }

    private void HandleCombatAutoPauseStart()
    {
        var cfg = _playerPerformanceConfigService.Current;
        if (!cfg.AutoPauseWhileInCombat) return;

        if (Interlocked.Exchange(ref _combatPauseActive, 1) == 1)
            return;

        _autoCombatPausedUsers.Clear();

        foreach (var pair in EnumerateAllPairs())
        {
            try
            {
                if (!pair.IsVisible) continue;

                // don't interfere with other-sync ownership
                if (pair.AutoPausedByOtherSync && !pair.EffectiveOverrideOtherSync) continue;

                // already paused? don't touch, and don't record it
                if (pair.UserPair?.OwnPermissions.IsPaused() ?? false) continue;

                // if they are paused by cap right now, don't touch
                if (pair.AutoPausedByCap) continue;

                // ignore list should still apply
                if (IsIgnored(cfg, pair.UserData)) continue;

                _autoCombatPausedUsers[pair.UserData.UID] = pair.UserData;
                pair.EnterPausedVanillaState();
                _mediator.Publish(new PauseMessage(pair.UserData));
            }
            catch
            {
                // best effort
            }
        }

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Combat auto-pause: paused {count} visible users", _autoCombatPausedUsers.Count);
    }

    private void HandleCombatAutoPauseEnd()
    {
        if (Interlocked.Exchange(ref _combatPauseActive, 0) == 0)
            return;

        if (_autoCombatPausedUsers.IsEmpty)
            return;

        var liveByUid = new Dictionary<string, Pair>(StringComparer.Ordinal);
        foreach (var p in EnumerateAllPairs())
        {
            if (p?.UserData?.UID is string uid && !string.IsNullOrEmpty(uid))
                liveByUid[uid] = p;
        }

        foreach (var kv in _autoCombatPausedUsers)
        {
            var uid = kv.Key;
            var user = kv.Value;

            try
            {
                if (_autoThresholdPausedUsers.ContainsKey(uid)) continue;
                if (_autoCapPausedUsers.ContainsKey(uid)) continue;

                if (liveByUid.TryGetValue(uid, out var livePair))
                {
                    if (livePair.AutoPausedByCap) continue;
                    if (livePair.AutoPausedByOtherSync && !livePair.EffectiveOverrideOtherSync) continue;
                    if (!(livePair.UserPair?.OwnPermissions.IsPaused() ?? false)) continue;
                }

                _mediator.Publish(new ResumeMessage(user));
            }
            catch { }
        }

        _autoCombatPausedUsers.Clear();

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Combat auto-pause: resumed users and cleared list");
    }

    //Kaia, you fucking idiot. Why awas this making huge calls every tick? WHY. I fixed it, love, your own drunk ass.
    private void LoadAutoCapState()
    {
        try
        {
            if (!File.Exists(_statePath)) return;
            var json = File.ReadAllText(_statePath);
            var state = JsonSerializer.Deserialize<AutoCapState>(json);
            if (state is null || (state.Version != 1 && state.Version != 2)) return;

            // Merge: only add entries we're not already tracking
            foreach (var kv in state.PausedVramBytes)
            {
                if (!_autoCapPausedVramByUid.ContainsKey(kv.Key))
                    _autoCapPausedVramByUid[kv.Key] = kv.Value;
            }
            foreach (var kv in state.Aliases)
            {
                if (!_autoCapPausedUsers.ContainsKey(kv.Key))
                    _autoCapPausedUsers[kv.Key] = new UserData(kv.Key, kv.Value); // UID, alias
            }
            foreach (var kv in state.ThresholdPausedAliases)
            {
                if (!_autoThresholdPausedUsers.ContainsKey(kv.Key))
                    _autoThresholdPausedUsers[kv.Key] = new UserData(kv.Key, kv.Value); // UID, alias
            }

            _logger.LogInformation("Loaded persisted performance pause state: auto-cap={autoCapCount}, threshold={thresholdCount} from {when:u}",
                _autoCapPausedUsers.Count, _autoThresholdPausedUsers.Count, state.SavedUtc);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load auto-cap state");
        }
    }
}