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
    private readonly ConcurrentDictionary<string, bool> _warnedForPlayers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _textureSizeByHash = new(StringComparer.OrdinalIgnoreCase);

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

    private static readonly TimeSpan AutoPauseInterval = TimeSpan.FromMilliseconds(500);
    private DateTime _nextAutoPauseUtc = DateTime.MinValue;

    private static readonly TimeSpan AutoCapCheckInterval = TimeSpan.FromMilliseconds(500);
    private DateTime _nextAutoCapCheckUtc = DateTime.MinValue;

    // Only rescan auto-cap candidates when something relevant changed
    private int _autoCapCandidatesDirty = 1; // start dirty so first pass evaluates
    private long _lastObservedAutoCapBytes = -1;





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
        Mediator.Subscribe<FrameworkUpdateMessage>(this, _ => TryKickAutoCapStateSave());
        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => TryKickAutoCapStateSave());

    }

    public async Task<bool> CheckBothThresholds(PairHandler pairHandler, CharacterData charaData)
    {
        var config = _playerPerformanceConfigService.Current;

        HashSet<string>? moddedTextureHashes = null;
        HashSet<string>? moddedModelHashes = null;

        if (charaData.FileReplacements.TryGetValue(API.Data.Enum.ObjectKind.Player, out List<FileReplacementData>? playerReplacements))
        {
            moddedTextureHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            moddedModelHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            CollectPlayerReplacementHashes(playerReplacements, moddedTextureHashes, moddedModelHashes);
        }

        bool notPausedAfterVram = ComputeAndAutoPauseOnVRAMUsageThresholds(pairHandler, charaData, [], moddedTextureHashes);
        if (!notPausedAfterVram) return false;

        bool notPausedAfterTris = await CheckTriangleUsageThresholds(pairHandler, charaData, moddedModelHashes).ConfigureAwait(false);
        if (!notPausedAfterTris) return false;

        if (IsIgnored(config, pairHandler.Pair.UserData))
            return true;

        var vramUsage = pairHandler.Pair.LastAppliedApproximateVRAMBytes;
        var triUsage = pairHandler.Pair.LastAppliedDataTris;

        bool isPrefPerm = pairHandler.Pair.UserPair.OwnPermissions.HasFlag(API.Data.Enum.UserPermissions.Sticky);

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

        if (!charaData.FileReplacements.TryGetValue(API.Data.Enum.ObjectKind.Player, out List<FileReplacementData>? playerReplacements))
        {
            pair.LastAppliedDataTris = 0;
            return true;
        }

        var moddedModelHashes = precomputedModelHashes ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (precomputedModelHashes == null)
        {
            foreach (var replacement in playerReplacements)
            {
                if (!string.IsNullOrEmpty(replacement.FileSwapPath))
                    continue;

                bool hasModelPath = false;
                foreach (var gamePath in replacement.GamePaths)
                {
                    if (gamePath.EndsWith("mdl", StringComparison.OrdinalIgnoreCase))
                    {
                        hasModelPath = true;
                        break;
                    }
                }

                if (hasModelPath)
                    moddedModelHashes.Add(replacement.Hash);
            }
        }

        foreach (var hash in moddedModelHashes)
        {
            triUsage += await _xivDataAnalyzer.GetTrianglesByHash(hash).ConfigureAwait(false);
        }

        pair.LastAppliedDataTris = triUsage;

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Calculated VRAM usage for {p}", pairHandler);

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

            _mediator.Publish(new PauseMessage(pair.UserData));

            return false;
        }

        return true;
    }

    public bool ComputeAndAutoPauseOnVRAMUsageThresholds(PairHandler pairHandler, CharacterData charaData, List<DownloadFileTransfer> toDownloadFiles, HashSet<string>? precomputedTextureHashes = null)
    {
        var config = _playerPerformanceConfigService.Current;
        var pair = pairHandler.Pair;

        long vramUsage = 0;

        if (!charaData.FileReplacements.TryGetValue(API.Data.Enum.ObjectKind.Player, out List<FileReplacementData>? playerReplacements))
        {
            pair.LastAppliedApproximateVRAMBytes = 0;
            return true;
        }

        var moddedTextureHashes = precomputedTextureHashes ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (precomputedTextureHashes == null)
        {
            foreach (var replacement in playerReplacements)
            {
                if (!string.IsNullOrEmpty(replacement.FileSwapPath))
                    continue;

                bool hasTexturePath = false;
                foreach (var gamePath in replacement.GamePaths)
                {
                    if (gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase))
                    {
                        hasTexturePath = true;
                        break;
                    }
                }

                if (hasTexturePath)
                    moddedTextureHashes.Add(replacement.Hash);
            }
        }

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

        foreach (var hash in moddedTextureHashes)
        {
            long fileSize = 0;

            if (pendingDownloadSizes != null && pendingDownloadSizes.TryGetValue(hash, out var pendingSize))
            {
                fileSize = pendingSize;
            }
            else
            {
                if (_textureSizeByHash.TryGetValue(hash, out var cachedSize))
                {
                    fileSize = cachedSize;
                }
                else
                {
                    var fileEntry = _fileCacheManager.GetFileCacheByHash(hash);
                    if (fileEntry == null) continue;

                    if (fileEntry.Size == null)
                    {
                        try
                        {
                            var fi = new FileInfo(fileEntry.ResolvedFilepath);
                            fileEntry.Size = fi.Length;
                            fileEntry.LastModifiedDateTicks = fi.LastWriteTimeUtc.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);

                            _fileCacheManager.UpdateHashedFile(fileEntry, computeProperties: false);
                        }
                        catch (IOException)
                        {
                            continue;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            continue;
                        }
                    }

                    fileSize = fileEntry.Size.Value;
                    _textureSizeByHash.TryAdd(hash, fileSize);
                }

            }

            vramUsage += fileSize;
        }

        pair.LastAppliedApproximateVRAMBytes = vramUsage;

        _logger.LogDebug("Calculated VRAM usage for {p}", pairHandler);

        // no warning of any kind on ignored pairs
        if (IsIgnored(config, pair.UserData))
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
            UpsertSyncshellVram(pair.UserData.UID, vramUsage);
            _knownUsersByUid.TryAdd(pair.UserData.UID, pair.UserData);
        }
        else
        {
            RemoveSyncshellVram(pair.UserData.UID);
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
                    RemoveSyncshellVram(pair.UserData.UID);
                    MarkAutoCapCandidatesDirty();
                    MarkStateDirty();
                }
            }
        }
        // === end pooled cap ===


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
    private static void CollectPlayerReplacementHashes(List<FileReplacementData> playerReplacements,HashSet<string> moddedTextureHashes,HashSet<string> moddedModelHashes)
    {
        foreach (var replacement in playerReplacements)
        {
            if (!string.IsNullOrEmpty(replacement.FileSwapPath))
                continue;

            bool hasTexturePath = false;
            bool hasModelPath = false;

            foreach (var gamePath in replacement.GamePaths)
            {
                if (!hasTexturePath && gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase))
                    hasTexturePath = true;

                if (!hasModelPath && gamePath.EndsWith("mdl", StringComparison.OrdinalIgnoreCase))
                    hasModelPath = true;

                if (hasTexturePath && hasModelPath)
                    break;
            }

            if (hasTexturePath)
                moddedTextureHashes.Add(replacement.Hash);

            if (hasModelPath)
                moddedModelHashes.Add(replacement.Hash);
        }
    }
    private void MarkAutoCapCandidatesDirty() => Interlocked.Exchange(ref _autoCapCandidatesDirty, 1);
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