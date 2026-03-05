using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Extensions;
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
    private readonly ConcurrentDictionary<string, bool> _warnedForPlayers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, (long Bytes, long Stamp)> _textureVramByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (long Bytes, long Stamp)> _modelVramByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, UserData> _autoThresholdPausedUsers = new(StringComparer.Ordinal);

    // pooled VRAM (bytes) per non-direct (Syncshell) UID
    private readonly ConcurrentDictionary<string, long> _syncshellVramByUid = new(StringComparer.Ordinal);
    private long _syncshellVramTotalBytes;

    private long GetTotalSyncshellVramBytes() => Interlocked.Read(ref _syncshellVramTotalBytes);

    private readonly ConcurrentDictionary<string, long> _autoCapPausedVramByUid = new();
    private readonly ConcurrentDictionary<string, UserData> _autoCapPausedUsers = new();
    private readonly ConcurrentDictionary<string, UserData> _knownUsersByUid = new(StringComparer.Ordinal);

    private readonly string _statePath =
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                 "RavaSync", "autoCapState.json");

    // debounce writes
    private int _stateDirty; // 0/1 via Interlocked
    private DateTime _lastSave = DateTime.MinValue;

    // avoid kicking save attempts every frame while throttled
    private int _stateSaveInFlight; // 0/1 via Interlocked
    private DateTime _nextStateSaveAttemptUtc = DateTime.MinValue;

    private static readonly TimeSpan AutoCapCheckInterval = TimeSpan.FromMilliseconds(500);
    private DateTime _nextAutoCapCheckUtc = DateTime.MinValue;

    // Only rescan auto-cap candidates when something relevant changed
    private int _autoCapCandidatesDirty = 1; // start dirty so first pass evaluates
    private long _lastObservedAutoCapBytes = -1;





    public PlayerPerformanceService(ILogger<PlayerPerformanceService> logger, MareMediator mediator,
        PlayerPerformanceConfigService playerPerformanceConfigService, FileCacheManager fileCacheManager,
        XivDataAnalyzer xivDataAnalyzer, TransientResourceManager transientResourceManager) : base(logger, mediator)
    {

        _logger = logger;
        _mediator = mediator;
        _playerPerformanceConfigService = playerPerformanceConfigService;
        _fileCacheManager = fileCacheManager;
        _xivDataAnalyzer = xivDataAnalyzer;
        _transientResourceManager = transientResourceManager;

        _mediator.Subscribe<FrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        LoadAutoCapState();
        Mediator.Subscribe<FrameworkUpdateMessage>(this, _ => TryKickAutoCapStateSave());
        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => TryKickAutoCapStateSave());

    }

    public async Task<bool> CheckBothThresholds(PairHandler pairHandler, CharacterData charaData)
    {
        var config = _playerPerformanceConfigService.Current;

        var moddedTextureHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var moddedModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        CollectReplacementHashesAllKinds(charaData, moddedTextureHashes, moddedModelHashes);

        bool notPausedAfterVram = ComputeAndAutoPauseOnVRAMUsageThresholds(pairHandler, charaData, [], moddedTextureHashes, moddedModelHashes);
        if (!notPausedAfterVram) return false;

        bool notPausedAfterTris = await CheckTriangleUsageThresholds(pairHandler, charaData, moddedModelHashes).ConfigureAwait(false);
        if (!notPausedAfterTris) return false;

        var vramUsage = pairHandler.Pair.LastAppliedApproximateVRAMBytes;
        var triUsage = pairHandler.Pair.LastAppliedDataTris;

        bool isPrefPerm = pairHandler.Pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);
        {
            var uid = pairHandler.Pair.UserData.UID;

            if (!pairHandler.Pair.UserPair.OwnPermissions.IsPaused())
            {
                _autoThresholdPausedUsers.TryRemove(uid, out _);
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
                        _autoThresholdPausedUsers.TryRemove(uid, out _);

                        _mediator.Publish(new ResumeMessage(pairHandler.Pair.UserData));

                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("Auto-unpaused {user} — now under thresholds (VRAM={vram}, Tris={tris})",
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
    public async Task<bool> CheckTriangleUsageThresholds(PairHandler pairHandler, CharacterData charaData, HashSet<string>? precomputedModelHashes = null)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long triUsage = 0;

        var moddedModelHashes = precomputedModelHashes ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (precomputedModelHashes == null)
        {
            var tmpTex = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            CollectReplacementHashesAllKinds(charaData, tmpTex, moddedModelHashes);
        }

        foreach (var hash in moddedModelHashes)
        {
            triUsage += await _xivDataAnalyzer.GetTrianglesByHash(hash).ConfigureAwait(false);
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

            _autoThresholdPausedUsers[pair.UserData.UID] = pair.UserData;
            _mediator.Publish(new PauseMessage(pair.UserData));

            return false;
        }

        return true;
    }

    public bool ComputeAndAutoPauseOnVRAMUsageThresholds(PairHandler pairHandler, CharacterData charaData, List<DownloadFileTransfer> toDownloadFiles, HashSet<string>? precomputedTextureHashes = null, HashSet<string>? precomputedModelHashes = null)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long vramUsage = 0;

        Dictionary<string, long>? pendingDownloadSizes = null;
        if (toDownloadFiles != null && toDownloadFiles.Count > 0)
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
            hashes = CollectReplacementHashesUnique(charaData);
        }
        else
        {
            hashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (precomputedTextureHashes != null) foreach (var h in precomputedTextureHashes) if (!string.IsNullOrWhiteSpace(h)) hashes.Add(h);
            if (precomputedModelHashes != null) foreach (var h in precomputedModelHashes) if (!string.IsNullOrWhiteSpace(h)) hashes.Add(h);

            if (hashes.Count == 0)
                hashes = CollectReplacementHashesUnique(charaData);
        }

        foreach (var hash in hashes)
        {
            long bytes = 0;

            if (pendingDownloadSizes != null && pendingDownloadSizes.TryGetValue(hash, out var pendingSize))
            {
                bytes = pendingSize;
                vramUsage += Math.Max(0, bytes);
                continue;
            }

            var fileEntry = _fileCacheManager.GetFileCacheByHash(hash);
            if (fileEntry == null) continue;
            if (!fileEntry.IsCacheEntry) continue;

            var path = fileEntry.ResolvedFilepath;
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) continue;

            var ext = Path.GetExtension(path);

            if (ext.Equals(".tex", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryGetCachedTexVram(hash, path, out bytes))
                    continue;
            }
            else if (ext.Equals(".mdl", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryGetCachedMdlVram(hash, path, out bytes))
                    continue;
            }
            else
            {
                continue;
            }

            vramUsage += Math.Max(0, bytes);
        }

        pair.LastAppliedApproximateVRAMBytes = vramUsage;

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

            _autoThresholdPausedUsers[pair.UserData.UID] = pair.UserData;

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
                    _mediator.Publish(new PauseMessage(pair.UserData));
                    _logger.LogInformation("Auto-paused {user} — pool {total}/{cap}",
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
    private static void CollectReplacementHashesAllKinds(CharacterData charaData,HashSet<string> moddedTextureHashes,HashSet<string> moddedModelHashes)
    {
        moddedTextureHashes.Clear();
        moddedModelHashes.Clear();

        foreach (var kv in charaData.FileReplacements)
        {
            var list = kv.Value;
            if (list == null) continue;

            foreach (var replacement in list)
            {
                if (replacement == null) continue;
                if (string.IsNullOrEmpty(replacement.Hash)) continue;

                if (!string.IsNullOrEmpty(replacement.FileSwapPath))
                    continue;

                bool hasTex = false;
                bool hasMdl = false;

                if (replacement.GamePaths != null)
                {
                    foreach (var gamePath in replacement.GamePaths)
                    {
                        if (!hasTex && gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase))
                            hasTex = true;

                        if (!hasMdl && gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase))
                            hasMdl = true;

                        if (hasTex && hasMdl)
                            break;
                    }
                }

                if (hasTex) moddedTextureHashes.Add(replacement.Hash);
                if (hasMdl) moddedModelHashes.Add(replacement.Hash);
            }
        }
    }
    private void MarkAutoCapCandidatesDirty() => Interlocked.Exchange(ref _autoCapCandidatesDirty, 1);
    private static bool CheckForThreshold(bool thresholdEnabled, long threshold, long value, bool checkForPrefPerm, bool isPrefPerm) =>
        thresholdEnabled && threshold > 0 && threshold < value && ((checkForPrefPerm && isPrefPerm) || !isPrefPerm);

    private sealed class AutoCapState
    {
        public int Version { get; set; } = 1;
        public DateTime SavedUtc { get; set; }
        public Dictionary<string, long> PausedVramBytes { get; set; } = new(StringComparer.Ordinal);
        public Dictionary<string, string> Aliases { get; set; } = new(StringComparer.Ordinal); // optional, nice for logs
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

            var state = new AutoCapState
            {
                Version = 1,
                SavedUtc = DateTime.UtcNow,
                PausedVramBytes = paused.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal),
                Aliases = aliases.ToDictionary(kv => kv.Key, kv => kv.Value.AliasOrUID, StringComparer.Ordinal),
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

    private bool TryGetCachedTexVram(string hash, string path, out long bytes)
    {
        bytes = 0;
        long stamp;
        try { stamp = GetStamp(path); }
        catch { return false; }

        if (_textureVramByHash.TryGetValue(hash, out var cached) && cached.Stamp == stamp)
        {
            bytes = cached.Bytes;
            return true;
        }

        if (!VramEstimator.TryEstimateTexVramBytes(path, out bytes))
            return false;

        _textureVramByHash[hash] = (bytes, stamp);
        return true;
    }
    private bool TryGetCachedMdlVram(string hash, string path, out long bytes)
    {
        bytes = 0;
        long stamp;
        try { stamp = GetStamp(path); }
        catch { return false; }

        if (_modelVramByHash.TryGetValue(hash, out var cached) && cached.Stamp == stamp)
        {
            bytes = cached.Bytes;
            return true;
        }

        if (!VramEstimator.TryEstimateMdlVramBytes(path, out bytes))
            return false;

        _modelVramByHash[hash] = (bytes, stamp);
        return true;
    }

    private static HashSet<string> CollectReplacementHashesUnique(CharacterData charaData)
    {
        var hashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var kv in charaData.FileReplacements)
        {
            var list = kv.Value;
            if (list == null) continue;

            foreach (var replacement in list)
            {
                if (replacement == null) continue;
                if (string.IsNullOrEmpty(replacement.Hash)) continue;

                if (!string.IsNullOrEmpty(replacement.FileSwapPath))
                    continue;

                hashes.Add(replacement.Hash);
            }
        }

        return hashes;
    }

    //Kaia, you fucking idiot. Why awas this making huge calls every tick? WHY. I fixed it, love, your own drunk ass.
    private void LoadAutoCapState()
    {
        try
        {
            if (!File.Exists(_statePath)) return;
            var json = File.ReadAllText(_statePath);
            var state = JsonSerializer.Deserialize<AutoCapState>(json);
            if (state is null || state.Version != 1) return;

            // Merge: only add entries we’re not already tracking
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

            _logger.LogInformation("Loaded auto-cap state: {count} entries from {when:u}",
                _autoCapPausedUsers.Count, state.SavedUtc);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load auto-cap state");
        }
    }
}