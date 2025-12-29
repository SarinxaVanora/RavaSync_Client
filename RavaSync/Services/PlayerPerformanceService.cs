using RavaSync.API.Data;
using RavaSync.API.Data.Extensions;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.UI;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files.Models;
using Microsoft.Extensions.Logging;
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
    private readonly Dictionary<string, bool> _warnedForPlayers = new(StringComparer.Ordinal);
    // pooled VRAM (bytes) per non-direct (Syncshell) UID
    private readonly ConcurrentDictionary<string, long> _syncshellVramByUid = new(StringComparer.Ordinal);

    private long GetTotalSyncshellVramBytes() => _syncshellVramByUid.Values.Sum();
    // Track who we auto-paused (by UID) and their last-known VRAM when we paused them
    private readonly ConcurrentDictionary<string, long> _autoCapPausedVramByUid = new();
    private readonly ConcurrentDictionary<string, UserData> _autoCapPausedUsers = new();
    private readonly ConcurrentDictionary<string, UserData> _knownUsersByUid = new(StringComparer.Ordinal);

    private readonly string _statePath =
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                 "RavaSync", "autoCapState.json");

    // debounce writes
    private int _stateDirty; // 0/1 via Interlocked
    private DateTime _lastSave = DateTime.MinValue;



    public PlayerPerformanceService(ILogger<PlayerPerformanceService> logger, MareMediator mediator,
        PlayerPerformanceConfigService playerPerformanceConfigService, FileCacheManager fileCacheManager,
        XivDataAnalyzer xivDataAnalyzer) : base(logger, mediator)
    {

        _logger = logger;
        _mediator = mediator;
        _playerPerformanceConfigService = playerPerformanceConfigService;
        _fileCacheManager = fileCacheManager;
        _xivDataAnalyzer = xivDataAnalyzer;

        _mediator.Subscribe<FrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => AutoPauseHandler());
        LoadAutoCapState();
        _mediator.Subscribe<FrameworkUpdateMessage>(this, async _ => await SaveAutoCapStateAsync().ConfigureAwait(false));
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, async _ => await SaveAutoCapStateAsync().ConfigureAwait(false));

    }

    public async Task<bool> CheckBothThresholds(PairHandler pairHandler, CharacterData charaData)
    {
        var config = _playerPerformanceConfigService.Current;
        bool notPausedAfterVram = ComputeAndAutoPauseOnVRAMUsageThresholds(pairHandler, charaData, []);
        if (!notPausedAfterVram) return false;
        bool notPausedAfterTris = await CheckTriangleUsageThresholds(pairHandler, charaData).ConfigureAwait(false);
        if (!notPausedAfterTris) return false;

        if (config.UIDsToIgnore
            .Exists(uid => string.Equals(uid, pairHandler.Pair.UserData.Alias, StringComparison.Ordinal) || string.Equals(uid, pairHandler.Pair.UserData.UID, StringComparison.Ordinal)))
            return true;


        var vramUsage = pairHandler.Pair.LastAppliedApproximateVRAMBytes;
        var triUsage = pairHandler.Pair.LastAppliedDataTris;

        bool isPrefPerm = pairHandler.Pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);

        bool exceedsTris = CheckForThreshold(config.WarnOnExceedingThresholds, config.TrisWarningThresholdThousands * 1000,
            triUsage, config.WarnOnPreferredPermissionsExceedingThresholds, isPrefPerm);
        bool exceedsVram = CheckForThreshold(config.WarnOnExceedingThresholds, config.VRAMSizeWarningThresholdMiB * 1024 * 1024,
            vramUsage, config.WarnOnPreferredPermissionsExceedingThresholds, isPrefPerm);

        if (_warnedForPlayers.TryGetValue(pairHandler.Pair.UserData.UID, out bool hadWarning) && hadWarning)
        {
            _warnedForPlayers[pairHandler.Pair.UserData.UID] = exceedsTris || exceedsVram;
            return true;
        }

        _warnedForPlayers[pairHandler.Pair.UserData.UID] = exceedsTris || exceedsVram;

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

    public async Task<bool> CheckTriangleUsageThresholds(PairHandler pairHandler, CharacterData charaData)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long triUsage = 0;

        if (!charaData.FileReplacements.TryGetValue(API.Data.Enum.ObjectKind.Player, out List<FileReplacementData>? playerReplacements))
        {
            pair.LastAppliedDataTris = 0;
            return true;
        }

        var moddedModelHashes = playerReplacements.Where(p => string.IsNullOrEmpty(p.FileSwapPath) && p.GamePaths.Any(g => g.EndsWith("mdl", StringComparison.OrdinalIgnoreCase)))
            .Select(p => p.Hash)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var hash in moddedModelHashes)
        {
            triUsage += await _xivDataAnalyzer.GetTrianglesByHash(hash).ConfigureAwait(false);
        }

        pair.LastAppliedDataTris = triUsage;

        _logger.LogDebug("Calculated VRAM usage for {p}", pairHandler);

        // no warning of any kind on ignored pairs
        if (config.UIDsToIgnore
            .Exists(uid => string.Equals(uid, pair.UserData.Alias, StringComparison.Ordinal) || string.Equals(uid, pair.UserData.UID, StringComparison.Ordinal)))
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

            _mediator.Publish(new PauseMessage(pair.UserData));

            return false;
        }

        return true;
    }

    public bool ComputeAndAutoPauseOnVRAMUsageThresholds(PairHandler pairHandler, CharacterData charaData, List<DownloadFileTransfer> toDownloadFiles)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long vramUsage = 0;

        if (!charaData.FileReplacements.TryGetValue(API.Data.Enum.ObjectKind.Player, out List<FileReplacementData>? playerReplacements))
        {
            pair.LastAppliedApproximateVRAMBytes = 0;
            return true;
        }

        var moddedTextureHashes = playerReplacements.Where(p => string.IsNullOrEmpty(p.FileSwapPath) && p.GamePaths.Any(g => g.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)))
            .Select(p => p.Hash)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var hash in moddedTextureHashes)
        {
            long fileSize = 0;

            var download = toDownloadFiles.Find(f => string.Equals(hash, f.Hash, StringComparison.OrdinalIgnoreCase));
            if (download != null)
            {
                fileSize = download.TotalRaw;
            }
            else
            {
                var fileEntry = _fileCacheManager.GetFileCacheByHash(hash);
                if (fileEntry == null) continue;

                if (fileEntry.Size == null)
                {
                    fileEntry.Size = new FileInfo(fileEntry.ResolvedFilepath).Length;
                    _fileCacheManager.UpdateHashedFile(fileEntry, computeProperties: true);
                }

                fileSize = fileEntry.Size.Value;
            }

            vramUsage += fileSize;
        }

        pair.LastAppliedApproximateVRAMBytes = vramUsage;

        _logger.LogDebug("Calculated VRAM usage for {p}", pairHandler);

        // no warning of any kind on ignored pairs
        if (config.UIDsToIgnore
            .Exists(uid => string.Equals(uid, pair.UserData.Alias, StringComparison.Ordinal) || string.Equals(uid, pair.UserData.UID, StringComparison.Ordinal)))
            return true;

        bool isPrefPerm = pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);

        // now check auto pause
        if (CheckForThreshold(config.AutoPausePlayersExceedingThresholds, config.VRAMSizeAutoPauseThresholdMiB * 1024 * 1024,
            vramUsage, config.AutoPausePlayersWithPreferredPermissionsExceedingThresholds, isPrefPerm))
        {
            _mediator.Publish(new NotificationMessage($"{pair.PlayerName} ({pair.UserData.AliasOrUID}) automatically paused",
                $"Player {pair.PlayerName} ({pair.UserData.AliasOrUID}) exceeded your configured VRAM auto pause threshold (" +
                $"{UiSharedService.ByteToString(vramUsage, addSuffix: true)}/{config.VRAMSizeAutoPauseThresholdMiB}MiB)" +
                $" and has been automatically paused.",
                MareConfiguration.Models.NotificationType.Warning));

            _mediator.Publish(new PauseMessage(pair.UserData));

            _mediator.Publish(new EventMessage(new Event(pair.PlayerName, pair.UserData, nameof(PlayerPerformanceService), EventSeverity.Warning,
                $"Exceeds VRAM threshold: automatically paused ({UiSharedService.ByteToString(vramUsage, addSuffix: true)}/{config.VRAMSizeAutoPauseThresholdMiB} MiB)")));

            return false;
        }

        // === Syncshell pooled VRAM cap enforcement (VISIBLE and UNPAUSED ONLY) ===

        // a pair contributes to the pool ONLY when:
        // - NOT a direct pair
        // - is currently VISIBLE
        // - NOT paused for any reason (manual or auto)
        if (pair.AutoPausedByCap && !pair.UserPair.OwnPermissions.IsPaused())
        {
            pair.AutoPausedByCap = false;
        }

        bool isEffectivelyPaused = pair.UserPair.OwnPermissions.IsPaused() || pair.AutoPausedByCap;
        bool contributesToPool = !pair.IsDirectlyPaired && pair.IsVisible && !isEffectivelyPaused;

        // keep the pool in sync with visibility/pause state
        if (contributesToPool)
        {
            _syncshellVramByUid[pair.UserData.UID] = Math.Max(0, vramUsage);
            _knownUsersByUid[pair.UserData.UID] = pair.UserData;
        }
        else
        {
            _syncshellVramByUid.TryRemove(pair.UserData.UID, out _);
        }

        // read cap (MiB ? bytes); 0 = disabled
        int capMiB = _playerPerformanceConfigService.Current.SyncshellVramCapMiB;
        if (capMiB > 0) //&& !pair.IsDirectlyPaired)
        {
            long capBytes = (long)capMiB * 1024L * 1024L;
            long totalBytes = GetTotalSyncshellVramBytes();

            // only enforce against users that would otherwise contribute (visible & unpaused)
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
                    _syncshellVramByUid.TryRemove(pair.UserData.UID, out _);
                    MarkStateDirty();
                }
            }
        }
        // === end pooled cap ===


        return true;
    }

    private void AutoPauseHandler()
    {
        int capMiB = _playerPerformanceConfigService.Current.SyncshellVramCapMiB;
        if (capMiB <= 0) return;

        long capBytes = (long)capMiB * 1024L * 1024L;
        long totalBytes = GetTotalSyncshellVramBytes();

        //Over cap - Pause ONE highest-VRAM contributor not yet auto-capped
        if (totalBytes > capBytes)
        {
            var nextUid = _syncshellVramByUid
                .OrderByDescending(kv => kv.Value)
                .Select(kv => kv.Key)
                .FirstOrDefault(uid => !_autoCapPausedUsers.ContainsKey(uid));

            if (nextUid != null && _knownUsersByUid.TryGetValue(nextUid, out var user))
            {
                // remember we auto-capped this UID (use last-known VRAM for smart resume later)
                var last = _syncshellVramByUid.GetValueOrDefault(nextUid, 0);
                _autoCapPausedUsers[nextUid] = user;
                _autoCapPausedVramByUid[nextUid] = last;

                // tell server to pause; per-pair local flag will sort itself next tick,
                // and server-side Paused=true is authoritative anyway
                _mediator.Publish(new PauseMessage(user));
                _syncshellVramByUid.TryRemove(nextUid, out _);
                MarkStateDirty();

                _logger.LogInformation("Auto-paused {user} — lastVRAM {last} — pool {total}/{cap}",
                    user.AliasOrUID,
                    UiSharedService.ByteToString(last, true),
                    UiSharedService.ByteToString(totalBytes, true),
                    UiSharedService.ByteToString(capBytes, true));

                return; // exactly one action per tick
            }
        }

        //Under cap - Resume ONE smallest-VRAM auto-capped UID
        if (totalBytes <= capBytes && !_autoCapPausedUsers.IsEmpty)
        {
            var nextUid = _autoCapPausedVramByUid
                .OrderBy(kv => kv.Value)
                .Select(kv => kv.Key)
                .FirstOrDefault();
            if (nextUid is null) return;

            if (!_autoCapPausedUsers.TryGetValue(nextUid, out var user)) return;

            long lastKnown = _autoCapPausedVramByUid.GetValueOrDefault(nextUid, long.MaxValue);
            long estimateAfterResume = totalBytes + lastKnown;
            if (estimateAfterResume > capBytes) return;

            _autoCapPausedUsers.TryRemove(nextUid, out _);
            _autoCapPausedVramByUid.TryRemove(nextUid, out _);

            _mediator.Publish(new ResumeMessage(user));

            _logger.LogInformation("Auto-unpaused {user} — lastVRAM {last} — pool {total}/{cap}",
                user.AliasOrUID,
                UiSharedService.ByteToString(lastKnown, true),
                UiSharedService.ByteToString(totalBytes, true),
                UiSharedService.ByteToString(capBytes, true));
            _syncshellVramByUid.TryRemove(nextUid, out _);
            MarkStateDirty();
        }
    }



    private static bool CheckForThreshold(bool thresholdEnabled, long threshold, long value, bool checkForPrefPerm, bool isPrefPerm) =>
        thresholdEnabled && threshold > 0 && threshold < value && ((checkForPrefPerm && isPrefPerm) || !isPrefPerm);

    //small class to build schema for AutoCap system state persistence
    private sealed class AutoCapState
    {
        public int Version { get; set; } = 1;
        public DateTime SavedUtc { get; set; }
        public Dictionary<string, long> PausedVramBytes { get; set; } = new(StringComparer.Ordinal);
        public Dictionary<string, string> Aliases { get; set; } = new(StringComparer.Ordinal); // optional, nice for logs
    }

    private void MarkStateDirty() => Interlocked.Exchange(ref _stateDirty, 1);

    private void EnsureStateDir()
    {
        var dir = Path.GetDirectoryName(_statePath)!;
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
    }

    private async Task SaveAutoCapStateAsync()
    {
        if (Interlocked.Exchange(ref _stateDirty, 0) == 0) return;
        // throttle to ~1 save / 2s
        if ((DateTime.UtcNow - _lastSave).TotalSeconds < 2) return;

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
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save auto-cap state");
        }
    }

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