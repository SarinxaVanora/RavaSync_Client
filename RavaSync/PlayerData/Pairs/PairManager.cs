using Dalamud.Game.ClientState.Objects.Enums;
using Dalamud.Game.Gui.ContextMenu;
using Dalamud.Game.Text.SeStringHandling;
using Dalamud.Plugin.Services;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Comparer;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.User;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Factories;
using RavaSync.Services;
using RavaSync.Services.Discovery;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using System.Collections.Concurrent;
using System.Text.Json;

namespace RavaSync.PlayerData.Pairs;

public sealed class PairManager : DisposableMediatorSubscriberBase
{
    private readonly ConcurrentDictionary<UserData, Pair> _allClientPairs = new(UserDataComparer.Instance);
    private readonly ConcurrentDictionary<GroupData, GroupFullInfoDto> _allGroups = new(GroupDataComparer.Instance);
    private readonly MareConfigService _configurationService;
    private readonly IContextMenu _dalamudContextMenu;
    private readonly PairFactory _pairFactory;
    private readonly MareMediator _mediator;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IpcManager _ipcManager;
    private readonly FileUploadManager _fileUploadManager;
    private readonly CharacterRavaSidecarUtility _characterRavaSidecarUtility;
    private readonly PlayerPerformanceService _playerPerformanceService;
    private sealed record PendingOtherSyncLatch(bool YieldToOtherSync, string Owner);
    private readonly ConcurrentDictionary<string, PendingOtherSyncLatch> _pendingOtherSyncLatchByUid = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Pair> _pairsByUid = new(StringComparer.Ordinal);
    private readonly object _refreshUiGate = new();
    private long _lastOtherSyncCleanupTick;
    private string _ownUid = string.Empty;
    private bool _refreshUiPending;
    private long _refreshUiPublishTick;
    private Lazy<List<Pair>> _directPairsInternal;
    private Lazy<Dictionary<GroupFullInfoDto, List<Pair>>> _groupPairsInternal;
    private Lazy<Dictionary<Pair, List<GroupFullInfoDto>>> _pairsWithGroupsInternal;

    public PairManager(ILogger<PairManager> logger,PairFactory pairFactory,MareConfigService configurationService,MareMediator mediator,IContextMenu dalamudContextMenu,DalamudUtilService dalamudUtil,IpcManager ipcManager, FileUploadManager fileUploadManager, CharacterRavaSidecarUtility characterRavaSidecarUtility, PlayerPerformanceService playerPerformanceService) : base(logger, mediator)
    {
        _pairFactory = pairFactory;
        _configurationService = configurationService;
        _dalamudContextMenu = dalamudContextMenu;
        _mediator = mediator;
        _dalamudUtil = dalamudUtil;
        _ipcManager = ipcManager;
        _fileUploadManager = fileUploadManager;
        _characterRavaSidecarUtility = characterRavaSidecarUtility;
        _playerPerformanceService = playerPerformanceService;
        
        Mediator.Subscribe<ConnectedMessage>(this, msg =>
        {
            _ownUid = msg.Connection.User?.UID ?? string.Empty;
        });
        Mediator.Subscribe<DisconnectedMessage>(this, (_) =>
        {
            _ownUid = string.Empty;
            ClearPairs();
        });
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) =>
        {
            // Zone start is handled by each live PairHandler through the same targeted
            // IsVisible=false teardown path used for ordinary visibility loss. Do not
            // run a manager-level global wipe here: it has no pair lifecycle generation
            // guard, so if it executes late it can wipe freshly-applied actors in the
            // destination zone.
            Logger.LogDebug("Zone switch start: pair handlers own targeted vanilla teardown");
        });
        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (_) =>
        {
            // Zone end is a recovery/replay point, not a second teardown. Running a second
            // wipe/reset here races Glamourer/Penumbra reapply and leaves pairs stuck vanilla.
            Logger.LogDebug("Zone switch end: no global vanilla wipe; visible pair handlers will re-enter apply lifecycle");
        });
        Mediator.Subscribe<CutsceneEndMessage>(this, (_) => ReapplyPairData());
        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ =>
        {
            PeriodicOtherSyncCleanup();
            FlushScheduledRefreshUi();
        });
        _directPairsInternal = DirectPairsLazy();
        _groupPairsInternal = GroupPairsLazy();
        _pairsWithGroupsInternal = PairsWithGroupsLazy();

        _dalamudContextMenu.OnMenuOpened += DalamudContextMenuOnOnOpenGameObjectContextMenu;
    }
    public List<Pair> DirectPairs => _directPairsInternal.Value;

    public Dictionary<GroupFullInfoDto, List<Pair>> GroupPairs => _groupPairsInternal.Value;
    public Dictionary<GroupData, GroupFullInfoDto> Groups => _allGroups.ToDictionary(k => k.Key, k => k.Value, GroupDataComparer.Instance);
    public Pair? LastAddedUser { get; internal set; }
    public Dictionary<Pair, List<GroupFullInfoDto>> PairsWithGroups => _pairsWithGroupsInternal.Value;

    public void AddGroup(GroupFullInfoDto dto)
    {
        _allGroups[dto.Group] = dto;
        RecreateLazy();
    }

    public void AddGroupPair(GroupPairFullInfoDto dto)
    {
        if (!_allClientPairs.ContainsKey(dto.User))
        {
            var created = _pairFactory.Create(new UserFullPairDto(dto.User, API.Data.Enum.IndividualPairStatus.None,
                [dto.Group.GID], dto.SelfToOtherPermissions, dto.OtherToSelfPermissions));

            _allClientPairs[dto.User] = created;
            IndexPair(created);
            ApplyPendingOtherSyncLatchIfAny(created);
        }
        else
        {
            _allClientPairs[dto.User].UserPair.Groups.Add(dto.GID);
        }

        RecreateLazy();
    }

    public Pair? GetPairByUID(string uid)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return null;

        return _pairsByUid.TryGetValue(uid, out var pair) ? pair : null;
    }

    private void CleanupExpiredOtherSyncLatches()
    {
        // Remote OtherSync claims now arrive as pair-targeted current state inside
        // normal character userdata. Keep a received true claim pending until our
        // local IPC can verify the same owner for that pair; false/no-claim userdata
        // clears the pending state immediately.
        if (_pendingOtherSyncLatchByUid.IsEmpty)
            return;

        _ipcManager.OtherSync.ShouldPollOwnership();

        foreach (var pair in _allClientPairs.Values.ToArray())
            ApplyPendingOtherSyncLatchIfAny(pair);
    }

    private static bool SyncRelevantPermissionsChanged(RavaSync.API.Data.Enum.UserPermissions previousPermissions, RavaSync.API.Data.Enum.UserPermissions nextPermissions)
    {
        if (previousPermissions == nextPermissions)
            return false;

        return previousPermissions.IsPaused() != nextPermissions.IsPaused()
            || previousPermissions.IsDisableAnimations() != nextPermissions.IsDisableAnimations()
            || previousPermissions.IsDisableSounds() != nextPermissions.IsDisableSounds()
            || previousPermissions.IsDisableVFX() != nextPermissions.IsDisableVFX();
    }

    private static string NormalizeOtherSyncOwner(string? owner)
    {
        if (string.IsNullOrWhiteSpace(owner))
            return "OtherSync";

        var normalized = owner.Trim();

        if (string.Equals(normalized, "LightlessSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "LightlessClient", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "Lightless-Sync", StringComparison.OrdinalIgnoreCase))
            return "Lightless";

        if (string.Equals(normalized, "SnowcloakSync", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "SnowcloakClient", StringComparison.OrdinalIgnoreCase))
            return "Snowcloak";

        if (string.Equals(normalized, "MareSynchronos", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "MareSempiterne", StringComparison.OrdinalIgnoreCase))
            return "PlayerSync";

        return normalized;
    }

    private static bool CanTrackRemoteOtherSyncOwner(string? owner)
    {
        var normalizedOwner = NormalizeOtherSyncOwner(owner);
        return !string.IsNullOrWhiteSpace(normalizedOwner)
            && !string.Equals(normalizedOwner, "RavaSync", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(normalizedOwner, "OtherSync", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(normalizedOwner, "Other", StringComparison.OrdinalIgnoreCase);
    }

    private static bool CanStoreRemoteOtherSyncLatch(bool yieldToOtherSync, string? owner)
    {
        if (!yieldToOtherSync)
            return true;

        return CanTrackRemoteOtherSyncOwner(owner);
    }

    private bool CanApplyRemoteOtherSyncLatch(bool yieldToOtherSync, string? owner)
    {
        if (!yieldToOtherSync)
            return true;

        return CanTrackRemoteOtherSyncOwner(owner)
            && _ipcManager.OtherSync.IsOwnerAvailable(owner);
    }

    private static bool MatchesOtherSyncOwner(string currentOwner, string targetOwner)
        => string.Equals(currentOwner ?? string.Empty, targetOwner ?? string.Empty, StringComparison.OrdinalIgnoreCase);

    private void ApplyPendingOtherSyncLatchIfAny(Pair pair)
    {
        var uid = pair.UserData.UID;
        if (!_pendingOtherSyncLatchByUid.TryGetValue(uid, out var pending))
            return;

        if (!CanStoreRemoteOtherSyncLatch(pending.YieldToOtherSync, pending.Owner))
        {
            _pendingOtherSyncLatchByUid.TryRemove(uid, out _);

            pair.ExpireRemoteOtherSyncOverride(requestApplyIfPossible: true);
            return;
        }

        if (!CanApplyRemoteOtherSyncLatch(pending.YieldToOtherSync, pending.Owner))
            return;

        pair.ApplyRemoteOtherSyncOverride(pending.YieldToOtherSync, pending.Owner);
    }

    private void ApplyIncomingOtherSyncSidecar(Pair pair, string uid, bool hasOtherSync, CharacterRavaSidecarUtility.OtherSyncPayload? payload)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return;

        var active = hasOtherSync && payload?.a == true;
        var owner = active ? NormalizeOtherSyncOwner(payload?.o) : string.Empty;

        if (active && !CanTrackRemoteOtherSyncOwner(owner))
            active = false;

        if (!active)
        {
            _pendingOtherSyncLatchByUid.TryRemove(uid, out _);

            var wasYielded = pair.AutoPausedByOtherSync;
            pair.ExpireRemoteOtherSyncOverride(requestApplyIfPossible: true);

            if (wasYielded && !pair.AutoPausedByOtherSync)
                ScheduleRefreshUi();

            return;
        }

        _pendingOtherSyncLatchByUid[uid] = new PendingOtherSyncLatch(true, owner);
        ApplyPendingOtherSyncLatchIfAny(pair);
    }

    private void IndexPair(Pair pair)
    {
        var uid = pair.UserData.UID;
        if (!string.IsNullOrWhiteSpace(uid))
            _pairsByUid[uid] = pair;
    }

    private void UnindexPair(UserData user)
    {
        var uid = user.UID;
        if (!string.IsNullOrWhiteSpace(uid))
            _pairsByUid.TryRemove(uid, out _);
    }

    public void AddUserPair(UserFullPairDto dto)
    {
        if (!_allClientPairs.ContainsKey(dto.User))
        {
            var created = _pairFactory.Create(dto);
            _allClientPairs[dto.User] = created;
            IndexPair(created);

            ApplyPendingOtherSyncLatchIfAny(created);
        }
        else
        {
            var existingPair = _allClientPairs[dto.User];
            existingPair.UserPair.IndividualPairStatus = dto.IndividualPairStatus;
            existingPair.ApplyLastReceivedData();
        }

        RecreateLazy();
    }

    public void AddUserPair(UserPairDto dto, bool addToLastAddedUser = true)
    {
        if (!_allClientPairs.ContainsKey(dto.User))
        {
            var created = _pairFactory.Create(dto);
            _allClientPairs[dto.User] = created;
            IndexPair(created);

            ApplyPendingOtherSyncLatchIfAny(created);
        }
        else
        {
            addToLastAddedUser = false;
        }

        var existingPair = _allClientPairs[dto.User];
        existingPair.UserPair.IndividualPairStatus = dto.IndividualPairStatus;
        existingPair.UserPair.OwnPermissions = dto.OwnPermissions;
        existingPair.UserPair.OtherPermissions = dto.OtherPermissions;
        if (addToLastAddedUser)
            LastAddedUser = existingPair;
        existingPair.ApplyLastReceivedData();
        RecreateLazy();
    }

    public void ClearPairs()
    {
        Logger.LogDebug("Clearing all Pairs");

        var pairs = _allClientPairs.Values.ToArray();

        // Disconnect must be a slate-wipe, not a per-pair/index guessing game.
        // Queue one global Penumbra wipe first so every live RavaSync temporary collection is
        // unassigned/deleted without blocking the disconnect path or hitching the framework.
        QueueGlobalPenumbraVanillaWipe("disconnect");
        QueueGlobalLiveCustomizationVanillaWipe(pairs, "disconnect");

        DisposePairs(pairs);
        _allClientPairs.Clear();
        _pairsByUid.Clear();
        _allGroups.Clear();
        _pendingOtherSyncLatchByUid.Clear();
        RecreateLazy();
    }


    private void QueueGlobalPenumbraVanillaWipe(string reason)
    {
        try
        {
            _ipcManager.Penumbra.QueueRavaSyncGlobalTemporaryCollectionWipe(Logger, reason);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Failed queueing global Penumbra vanilla wipe for {reason}", reason);
        }
    }


    private void QueueGlobalLiveCustomizationVanillaWipe(IEnumerable<Pair> pairs, string reason)
    {
        var names = pairs
            .SelectMany(p => new[] { p.PlayerName ?? string.Empty, p.LastKnownPlayerName })
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var applicationId = Guid.NewGuid();
        _ = Task.Run(async () =>
        {
            try
            {
                Logger.LogDebug("[{applicationId}] Queueing global vanilla IPC-state wipe for {reason}: pairNames={count}", applicationId, reason, names.Length);

                foreach (var name in names)
                {
                    try
                    {
                        await _ipcManager.Glamourer.RevertByNameAsync(Logger, name!, applicationId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Ignoring global Glamourer name revert failure for {name} during {reason}", applicationId, name, reason);
                    }

                    await Task.Yield();
                }

                var livePlayers = await _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                    return _dalamudUtil.GetPlayerCharacterSnapshotsFromObjectTable()
                        .Where(p => p.ObjectIndex >= 0 && p.Address != nint.Zero && (localPlayerAddress == nint.Zero || p.Address != localPlayerAddress))
                        .ToArray();
                }).ConfigureAwait(false);

                foreach (var player in livePlayers)
                {
                    try
                    {
                        await _ipcManager.Glamourer.RevertByObjectIndexAsync(Logger, player.ObjectIndex, applicationId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Ignoring global Glamourer object-index revert failure for idx {idx} during {reason}", applicationId, player.ObjectIndex, reason);
                    }

                    if (player.ObjectIndex <= ushort.MaxValue)
                    {
                        try
                        {
                            await _ipcManager.CustomizePlus.RevertByObjectIndexAsync((ushort)player.ObjectIndex).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogDebug(ex, "[{applicationId}] Ignoring global Customize+ object-index revert failure for idx {idx} during {reason}", applicationId, player.ObjectIndex, reason);
                        }

                        try
                        {
                            await _ipcManager.Honorific.ClearTitleByObjectIndexAsync(player.ObjectIndex).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogDebug(ex, "[{applicationId}] Ignoring global Honorific object-index clear failure for idx {idx} during {reason}", applicationId, player.ObjectIndex, reason);
                        }

                        try
                        {
                            await _ipcManager.PetNames.ClearPlayerDataByObjectIndexAsync(player.ObjectIndex).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogDebug(ex, "[{applicationId}] Ignoring global PetNames object-index clear failure for idx {idx} during {reason}", applicationId, player.ObjectIndex, reason);
                        }
                    }

                    try
                    {
                        await _ipcManager.Heels.RestoreOffsetForPlayerAsync(player.Address).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Ignoring global Heels revert failure for {name}/{addr:X} during {reason}", applicationId, player.Name, player.Address, reason);
                    }

                    try
                    {
                        await _ipcManager.Moodles.RevertStatusAsync(player.Address).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "[{applicationId}] Ignoring global Moodles revert failure for {name}/{addr:X} during {reason}", applicationId, player.Name, player.Address, reason);
                    }

                    await Task.Yield();
                }

                Logger.LogDebug("[{applicationId}] Global vanilla IPC-state wipe queued for {reason}: livePlayers={count}", applicationId, reason, livePlayers.Length);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[{applicationId}] Global vanilla IPC-state wipe failed for {reason}", applicationId, reason);
            }
        });
    }

    public List<Pair> GetOnlineUserPairs()
    {
        var list = new List<Pair>(_allClientPairs.Count);
        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;
            if (!string.IsNullOrEmpty(pair.GetPlayerNameHash()))
                list.Add(pair);
        }
        return list;
    }

    public int GetVisibleUserCount()
    {
        var count = 0;
        foreach (var kvp in _allClientPairs)
        {
            if (kvp.Value.IsVisible)
                count++;
        }
        return count;
    }

    public void FillVisibleUsers(List<UserData> target)
    {
        ArgumentNullException.ThrowIfNull(target);

        target.Clear();
        if (target.Capacity < _allClientPairs.Count)
            target.Capacity = _allClientPairs.Count;

        foreach (var kvp in _allClientPairs)
        {
            if (kvp.Value.IsVisible)
                target.Add(kvp.Key);
        }
    }

    public void ForEachVisibleUser(Action<UserData> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        foreach (var kvp in _allClientPairs)
        {
            if (kvp.Value.IsVisible)
                action(kvp.Key);
        }
    }

    public void ForEachPair(Action<Pair> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        foreach (var kvp in _allClientPairs)
        {
            action(kvp.Value);
        }
    }

    public void ForEachVisiblePair(Action<Pair> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;
            if (pair.IsVisible)
                action(pair);
        }
    }

    public void ClearDisplayedPerformanceMetrics(IEnumerable<UserData> users)
    {
        ArgumentNullException.ThrowIfNull(users);

        var changed = false;
        foreach (var user in users)
        {
            if (user == null)
                continue;

            if (!_allClientPairs.TryGetValue(user, out var pair))
                continue;

            if (pair.LastAppliedApproximateVRAMBytes < 0 && pair.LastAppliedDataTris < 0)
                continue;

            pair.ClearDisplayedPerformanceMetrics();
            changed = true;
        }

        if (changed)
            ScheduleRefreshUi();
    }

    public void ClearDisplayedPerformanceMetricsForAllPairs()
    {
        var changed = false;
        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;
            if (pair.LastAppliedApproximateVRAMBytes < 0 && pair.LastAppliedDataTris < 0)
                continue;

            pair.ClearDisplayedPerformanceMetrics();
            changed = true;
        }

        if (changed)
            ScheduleRefreshUi();
    }

    public List<UserData> GetVisibleUsers()
    {
        var list = new List<UserData>(_allClientPairs.Count);
        FillVisibleUsers(list);
        return list;
    }


    public void MarkPairOffline(UserData user)
    {
        if (_allClientPairs.TryGetValue(user, out var pair))
        {
            Mediator.Publish(new ClearProfileDataMessage(pair.UserData));
            pair.MarkOffline();
        }

        RecreateLazy();
    }

    public void MarkPairOnline(OnlineUserIdentDto dto, bool sendNotif = true)
    {
        if (!_allClientPairs.ContainsKey(dto.User)) throw new InvalidOperationException("No user found for " + dto);

        Mediator.Publish(new ClearProfileDataMessage(dto.User));

        var pair = _allClientPairs[dto.User];
        if (pair.HasCachedPlayer)
        {
            // A fresh OnlineUserIdentDto can represent a newly recreated in-game actor
            // for the same UID. Let Pair decide whether the cached handler can stay or
            // must be recycled instead of treating "already cached" as authoritative.
            pair.CreateCachedPlayer(dto);
            RecreateLazy();
            return;
        }

        if (sendNotif && _configurationService.Current.ShowOnlineNotifications
            && (_configurationService.Current.ShowOnlineNotificationsOnlyForIndividualPairs && pair.IsDirectlyPaired && !pair.IsOneSidedPair
            || !_configurationService.Current.ShowOnlineNotificationsOnlyForIndividualPairs)
            && (_configurationService.Current.ShowOnlineNotificationsOnlyForNamedPairs && !string.IsNullOrEmpty(pair.GetNote())
            || !_configurationService.Current.ShowOnlineNotificationsOnlyForNamedPairs))
        {
            string? note = pair.GetNote();
            var msg = !string.IsNullOrEmpty(note)
                ? $"{note} ({pair.UserData.AliasOrUID}) is now online"
                : $"{pair.UserData.AliasOrUID} is now online";
            Mediator.Publish(new NotificationMessage("User online", msg, NotificationType.Info, TimeSpan.FromSeconds(5)));
        }

        pair.CreateCachedPlayer(dto);

        RecreateLazy();
    }

    public void ReceiveCharaData(OnlineUserCharaDataDto dto)
    {
        if (!_allClientPairs.TryGetValue(dto.User, out var pair))
            throw new InvalidOperationException("No user found for " + dto.User);

        var previousManifestFingerprint = pair.LastReceivedSyncManifest?.mf ?? string.Empty;
        var hasSyncManifest = _characterRavaSidecarUtility.TryExtractSyncManifest(dto.CharaData, out var syncManifest);
        var hasPerformance = _characterRavaSidecarUtility.TryExtractPerformance(dto.CharaData, out var sidecarVramBytes, out var sidecarTriangles);
        var hasOtherSync = _characterRavaSidecarUtility.TryExtractOtherSync(dto.CharaData, _ownUid, out var otherSync);
        var hasActiveSoundIndicator = _characterRavaSidecarUtility.TryExtractActiveSoundIndicator(dto.CharaData, out var activeSoundIndicator);

        if (hasPerformance)
            _playerPerformanceService.HandleIncomingPerformanceMetrics(pair, dto.CharaData?.DataHash.Value, sidecarVramBytes, sidecarTriangles);

        ApplyIncomingOtherSyncSidecar(pair, dto.User.UID, hasOtherSync, otherSync);
        if (hasActiveSoundIndicator)
            pair.ApplyRemoteActiveSoundIndicator(activeSoundIndicator);

        var merged = 0;
        if (hasSyncManifest)
        {
            merged = _characterRavaSidecarUtility.MergeManifestIntoCharacterData(dto.CharaData, syncManifest);
            pair.SetLastReceivedSyncManifest(syncManifest);
        }

        var currentManifestFingerprint = hasSyncManifest ? syncManifest?.mf ?? string.Empty : previousManifestFingerprint;
        var manifestUnchanged = string.Equals(previousManifestFingerprint, currentManifestFingerprint, StringComparison.Ordinal);

        if (pair.IsDuplicateIncomingPayload(dto.CharaData) && manifestUnchanged)
        {
            var clearedUpload = pair.ClearRemoteUploadStatus();
            if (Logger.IsEnabled(LogLevel.Trace))
            {
                Logger.LogTrace(
                    "Ignoring duplicate/sidecar-only character data for {uid}; perf={perf}, sound={sound}, manifest={manifest}, merged={merged}, clearedUpload={clearedUpload}",
                    dto.User.UID,
                    hasPerformance,
                    hasActiveSoundIndicator,
                    currentManifestFingerprint,
                    merged,
                    clearedUpload);
            }

            // A duplicate payload can still be the sender's final post-upload push.
            // Use that as the clean end-of-upload signal and as a lightweight self-heal
            // for cases where the visible actor missed its first apply and would
            // otherwise need a pause/unpause cycle.
            if (clearedUpload && pair.IsVisible)
                pair.ApplyLastReceivedData(forced: true);

            RecreateLazy();
            return;
        }

        if (!hasSyncManifest)
            pair.SetLastReceivedSyncManifest(null);

        if (hasSyncManifest && Logger.IsEnabled(LogLevel.Trace))
        {
            Logger.LogTrace(
                "Received sync manifest from {uid}: {total} assets, {critical} critical, {merged} merged into payload, manifest {manifestFingerprint}",
                dto.User.UID,
                syncManifest?.total ?? 0,
                syncManifest?.critical ?? 0,
                merged,
                currentManifestFingerprint);
        }

        Mediator.Publish(new EventMessage(new Event(pair.UserData, nameof(PairManager), EventSeverity.Informational, "Received Character Data")));
        pair.ApplyData(dto);
    }

    public void RemoveGroup(GroupData data)
    {
        _allGroups.TryRemove(data, out _);

        foreach (var item in _allClientPairs.ToList())
        {
            item.Value.UserPair.Groups.Remove(data.GID);

            if (!item.Value.HasAnyConnection())
            {
                item.Value.MarkOffline();
                _allClientPairs.TryRemove(item.Key, out _);
                UnindexPair(item.Key);
            }
        }

        RecreateLazy();
    }

    public void RemoveGroupPair(GroupPairDto dto)
    {
        if (_allClientPairs.TryGetValue(dto.User, out var pair))
        {
            pair.UserPair.Groups.Remove(dto.Group.GID);

            if (!pair.HasAnyConnection())
            {
                pair.MarkOffline();
                _allClientPairs.TryRemove(dto.User, out _);
                UnindexPair(dto.User);
            }
        }

        RecreateLazy();
    }

    public void RemoveUserPair(UserDto dto)
    {
        if (_allClientPairs.TryGetValue(dto.User, out var pair))
        {
            pair.UserPair.IndividualPairStatus = API.Data.Enum.IndividualPairStatus.None;

            if (!pair.HasAnyConnection())
            {
                pair.MarkOffline();
                _allClientPairs.TryRemove(dto.User, out _);
                UnindexPair(dto.User);
            }
        }

        RecreateLazy();
    }

    public void SetGroupInfo(GroupInfoDto dto)
    {
        _allGroups[dto.Group].Group = dto.Group;
        _allGroups[dto.Group].Owner = dto.Owner;
        _allGroups[dto.Group].GroupPermissions = dto.GroupPermissions;

        RecreateLazy();
    }

    public void UpdatePairPermissions(UserPermissionsDto dto)
    {
        if (!_allClientPairs.TryGetValue(dto.User, out var pair))
        {
            throw new InvalidOperationException("No such pair for " + dto);
        }

        if (pair.UserPair == null) throw new InvalidOperationException("No direct pair for " + dto);

        var previousPermissions = pair.UserPair.OtherPermissions;
        var permissionsChanged = SyncRelevantPermissionsChanged(previousPermissions, dto.Permissions);

        if (previousPermissions.IsPaused() != dto.Permissions.IsPaused())
        {
            Mediator.Publish(new ClearProfileDataMessage(dto.User));
        }

        pair.UserPair.OtherPermissions = dto.Permissions;

        Logger.LogTrace("Paused: {paused}, Anims: {anims}, Sounds: {sounds}, VFX: {vfx}",
            pair.UserPair.OtherPermissions.IsPaused(),
            pair.UserPair.OtherPermissions.IsDisableAnimations(),
            pair.UserPair.OtherPermissions.IsDisableSounds(),
            pair.UserPair.OtherPermissions.IsDisableVFX());

        if (permissionsChanged && !pair.IsPaused)
            pair.ApplyLastReceivedData();

        RecreateLazy();
    }

    public void UpdateSelfPairPermissions(UserPermissionsDto dto)
    {
        if (!_allClientPairs.TryGetValue(dto.User, out var pair))
        {
            throw new InvalidOperationException("No such pair for " + dto);
        }

        var previousPermissions = pair.UserPair.OwnPermissions;
        var permissionsChanged = SyncRelevantPermissionsChanged(previousPermissions, dto.Permissions);

        var pauseStateChanged = previousPermissions.IsPaused() != dto.Permissions.IsPaused();
        if (pauseStateChanged)
        {
            Mediator.Publish(new ClearProfileDataMessage(dto.User));
        }

        pair.UserPair.OwnPermissions = dto.Permissions;

        if (dto.Permissions.IsPaused())
        {
            pair.GoBackToVanillaState();
        }

        Logger.LogTrace("Paused: {paused}, Anims: {anims}, Sounds: {sounds}, VFX: {vfx}",
            pair.UserPair.OwnPermissions.IsPaused(),
            pair.UserPair.OwnPermissions.IsDisableAnimations(),
            pair.UserPair.OwnPermissions.IsDisableSounds(),
            pair.UserPair.OwnPermissions.IsDisableVFX());

        if (permissionsChanged && !pair.IsPaused)
            pair.ApplyLastReceivedData();

        RecreateLazy();
    }


    internal void ReceiveUploadStatus(UserDto dto)
    {
        var uid = dto.User?.UID;
        if (!string.IsNullOrWhiteSpace(uid) && _fileUploadManager.IsLocalOutboundUploadRecipient(uid))
        {
            Logger.LogTrace("Ignoring upload status echo for local outbound recipient {uid}", uid);
            return;
        }

        if (_allClientPairs.TryGetValue(dto.User, out var existingPair) && existingPair.IsVisible)
        {
            existingPair.SetIsUploading();
            RecreateLazy();
        }
    }

    internal void SetGroupPairStatusInfo(GroupPairUserInfoDto dto)
    {
        _allGroups[dto.Group].GroupPairUserInfos[dto.UID] = dto.GroupUserInfo;
        RecreateLazy();
    }

    internal void SetGroupPermissions(GroupPermissionDto dto)
    {
        _allGroups[dto.Group].GroupPermissions = dto.Permissions;
        RecreateLazy();
    }

    internal void SetGroupStatusInfo(GroupPairUserInfoDto dto)
    {
        _allGroups[dto.Group].GroupUserInfo = dto.GroupUserInfo;
        RecreateLazy();
    }

    internal void UpdateGroupPairPermissions(GroupPairUserPermissionDto dto)
    {
        _allGroups[dto.Group].GroupUserPermissions = dto.GroupPairPermissions;
        RecreateLazy();
    }

    internal void UpdateIndividualPairStatus(UserIndividualPairStatusDto dto)
    {
        if (_allClientPairs.TryGetValue(dto.User, out var pair))
        {
            pair.UserPair.IndividualPairStatus = dto.IndividualPairStatus;
            pair.ApplyLastReceivedData();
            RecreateLazy();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_refreshUiGate)
            {
                _refreshUiPending = false;
                _refreshUiPublishTick = 0;
            }
        }

        base.Dispose(disposing);

        _dalamudContextMenu.OnMenuOpened -= DalamudContextMenuOnOnOpenGameObjectContextMenu;

        DisposePairs();
    }

    private void DalamudContextMenuOnOnOpenGameObjectContextMenu(IMenuOpenedArgs args)
    {
        if (args.MenuType == ContextMenuType.Inventory) return;
        if (!_configurationService.Current.EnableRightClickMenus) return;

        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;
            if (!pair.IsVisible) continue;

            pair.AddContextMenu(args);
        }


    }


    private Lazy<List<Pair>> DirectPairsLazy() => new(() =>
    {
        var list = new List<Pair>(_allClientPairs.Count);
        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;
            if (pair.IndividualPairStatus != API.Data.Enum.IndividualPairStatus.None)
                list.Add(pair);
        }
        return list;
    });

    private void DisposePairs()
    {
        DisposePairs(_allClientPairs.Values.ToArray());
    }

    private void DisposePairs(IEnumerable<Pair> pairs)
    {
        Logger.LogDebug("Disposing all Pairs");

        foreach (var pair in pairs)
        {
            try
            {
                pair.MarkOffline(wait: false, queuePerPairTeardown: false);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Failed disposing pair {pair} during global pair clear", pair.UserData.AliasOrUID);
            }
        }

        RecreateLazy();
    }

    private Lazy<Dictionary<GroupFullInfoDto, List<Pair>>> GroupPairsLazy()
    {
        return new Lazy<Dictionary<GroupFullInfoDto, List<Pair>>>(() =>
        {
            Dictionary<GroupFullInfoDto, List<Pair>> outDict = [];

            foreach (var group in _allGroups)
                outDict[group.Value] = new List<Pair>();

            var gidToGroup = new Dictionary<string, GroupFullInfoDto>(StringComparer.Ordinal);
            foreach (var group in _allGroups)
                gidToGroup[group.Key.GID] = group.Value;

            foreach (var kvp in _allClientPairs)
            {
                var pair = kvp.Value;
                var gids = pair.UserPair.Groups;

                for (int i = 0; i < gids.Count; i++)
                {
                    var gid = gids[i];
                    if (gidToGroup.TryGetValue(gid, out var groupDto))
                    {
                        outDict[groupDto].Add(pair);
                    }
                }
            }

            return outDict;
        });
    }


    private Lazy<Dictionary<Pair, List<GroupFullInfoDto>>> PairsWithGroupsLazy()
    {
        return new Lazy<Dictionary<Pair, List<GroupFullInfoDto>>>(() =>
        {
            Dictionary<Pair, List<GroupFullInfoDto>> outDict = [];

            foreach (var kvp in _allClientPairs)
            {
                var pair = kvp.Value;
                var list = new List<GroupFullInfoDto>();

                foreach (var g in _allGroups)
                {
                    if (pair.UserPair.Groups.Contains(g.Key.GID, StringComparer.Ordinal))
                        list.Add(g.Value);
                }

                outDict[pair] = list;
            }

            return outDict;
        });
    }

    private void ScheduleRefreshUi(bool immediate = false)
    {
        lock (_refreshUiGate)
        {
            var dueTick = immediate ? Environment.TickCount64 : Environment.TickCount64 + 50;
            if (!_refreshUiPending || dueTick < _refreshUiPublishTick)
                _refreshUiPublishTick = dueTick;

            _refreshUiPending = true;
        }
    }

    private void FlushScheduledRefreshUi()
    {
        var publish = false;
        lock (_refreshUiGate)
        {
            if (!_refreshUiPending)
                return;

            if (Environment.TickCount64 < _refreshUiPublishTick)
                return;

            _refreshUiPending = false;
            _refreshUiPublishTick = 0;
            publish = true;
        }

        if (publish)
            Mediator.Publish(new RefreshUiMessage());
    }

    public void ReapplyAllPairData() => ReapplyPairData();

    public void ForceManipulationReapplyForAllPairs()
    {
        foreach (var pair in _allClientPairs.Select(k => k.Value))
        {
            pair.ForceManipulationReapply();
        }
    }

    private void ReapplyPairData()
    {
        foreach (var pair in _allClientPairs.Select(k => k.Value))
        {
            pair.ApplyLastReceivedData(forced: true);
        }
    }


    public bool IsIdentDirectlyPaired(string ident)
    {
        if (string.IsNullOrEmpty(ident)) return false;

        foreach (var kvp in _allClientPairs)
        {
            var pair = kvp.Value;

            if (!pair.IsDirectlyPaired) continue;

            if (string.Equals(pair.Ident, ident, System.StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private void PeriodicOtherSyncCleanup()
    {
        CleanupExpiredOtherSyncLatches();
    }

    private void RecreateLazy()
    {
        CleanupExpiredOtherSyncLatches();
        _directPairsInternal = DirectPairsLazy();
        _groupPairsInternal = GroupPairsLazy();
        _pairsWithGroupsInternal = PairsWithGroupsLazy();
        ScheduleRefreshUi();
    }
}