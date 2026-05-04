using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using RavaSync.Utils;
using RavaSync.Services.Optimisation;

namespace RavaSync.PlayerData.Services;

public sealed class CacheCreationService : DisposableMediatorSubscriberBase
{
    private readonly object _cacheCreateLockObj = new();
    private readonly HashSet<ObjectKind> _cachesToCreate = [];
    private readonly DalamudUtilService _dalamudUtil;
    private readonly PlayerDataFactory _characterDataFactory;
    private readonly LocalPapSafetyModService _localPapSafetyModService;
    private readonly HashSet<ObjectKind> _currentlyCreating = [];
    private readonly HashSet<ObjectKind> _debouncedObjectCache = [];
    private readonly Dictionary<ObjectKind, HashSet<string>> _debouncedReasons = [];
    private readonly Dictionary<ObjectKind, HashSet<string>> _activeReasons = [];
    private readonly CharacterData _playerData = new();
    private readonly Dictionary<ObjectKind, GameObjectHandler> _playerRelatedObjects = [];
    private readonly CancellationTokenSource _runtimeCts = new();
    private readonly SemaphoreSlim _immediatePublishLock = new(1, 1);
    private CancellationTokenSource _immediatePublishCts = new();
    private CancellationTokenSource _creationCts = new();
    private CancellationTokenSource _debounceCts = new();
    private CancellationTokenSource _immediatePlayerFollowUpCts = new();
    private CancellationTokenSource _coalescedImmediatePlayerPublishCts = new();
    private CancellationTokenSource _penumbraModSettingPublishCts = new();
    private bool _haltCharaDataCreation;
    private bool _isZoning = false;
    private DateTime _connectSettleUntilUtc = DateTime.MinValue;
    private readonly DateTime _serviceStartUtc = DateTime.UtcNow;
    private DateTime _lastPlayerAppearanceSignalUtc = DateTime.MinValue;
    private DateTime _lastImmediatePlayerPublishUtc = DateTime.MinValue;
    private DateTime _suppressFastPlayerBuildsUntilUtc = DateTime.MinValue;
    private readonly SemaphoreSlim _localCollectionLookupLock = new(1, 1);
    private Guid _cachedLocalPlayerCollectionId = Guid.Empty;
    private DateTime _cachedLocalPlayerCollectionIdUntilUtc = DateTime.MinValue;
    private bool _pendingImmediatePlayerFollowUp;
    private string? _pendingImmediatePlayerFollowUpReason;
    private static readonly TimeSpan PenumbraTransientFollowWindow = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan InitialPenumbraTransientSettleWindow = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan StartupImmediatePublishSettleWindow = TimeSpan.FromSeconds(4);
    private static readonly TimeSpan ConnectedImmediatePublishSettleWindow = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan FastPlayerAppearanceDelay = TimeSpan.FromMilliseconds(25);
    private static readonly TimeSpan DefaultBuildDelay = TimeSpan.FromMilliseconds(75);
    private static readonly TimeSpan ImmediatePlayerPublishCoalesceDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan ImmediatePlayerPublishCooldown = TimeSpan.FromMilliseconds(350);
    private static readonly TimeSpan ImmediatePlayerPublishSettleWindow = TimeSpan.FromMilliseconds(900);
    private static readonly TimeSpan PenumbraModSettingSettleDelay = TimeSpan.FromMilliseconds(1000);

    public CacheCreationService(ILogger<CacheCreationService> logger, MareMediator mediator, GameObjectHandlerFactory gameObjectHandlerFactory,
        PlayerDataFactory characterDataFactory, DalamudUtilService dalamudUtil, LocalPapSafetyModService localPapSafetyModService) : base(logger, mediator)
    {
        _characterDataFactory = characterDataFactory;
        _dalamudUtil = dalamudUtil;
        _localPapSafetyModService = localPapSafetyModService;

        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (msg) => _isZoning = true);
        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (msg) => _isZoning = false);

        Mediator.Subscribe<HaltCharaDataCreation>(this, (msg) =>
        {
            _haltCharaDataCreation = !msg.Resume;
        });

        Mediator.Subscribe<CreateCacheForObjectMessage>(this, (msg) =>
        {
            if (msg.ObjectToCreateFor.ObjectKind == ObjectKind.Player)
            {
                if (IsPlayerAppearanceSignalReason(msg.Reason))
                {
                    NotePlayerAppearanceSignal();

                    if (ShouldUseImmediatePlayerPublishOnly(msg.Reason))
                    {
                        QueueImmediatePlayerPublish(msg.ObjectToCreateFor, msg.Reason);
                        return;
                    }

                    if (ShouldSuppressFastPlayerBuildAfterRecentImmediatePublish(msg.Reason))
                    {
                        if (Logger.IsEnabled(LogLevel.Trace))
                            Logger.LogTrace("Suppressing fast player cache rebuild for {reason} because a recent immediate publish already captured the state", msg.Reason);

                        return;
                    }
                }
            }

            AddCacheToCreate(msg.ObjectToCreateFor.ObjectKind, msg.Reason);
        });

        Mediator.Subscribe<ImmediatePlayerStatePublishMessage>(this, (msg) =>
        {
            if (msg.ObjectToCreateFor.ObjectKind != ObjectKind.Player) return;
            if (_isZoning || _haltCharaDataCreation) return;

            QueueImmediatePlayerPublish(msg.ObjectToCreateFor, msg.Reason);
        });

        _playerRelatedObjects[ObjectKind.Player] = gameObjectHandlerFactory.CreateDeferred(ObjectKind.Player, dalamudUtil.GetPlayerPtr, isWatched: true);
        _playerRelatedObjects[ObjectKind.MinionOrMount] = gameObjectHandlerFactory.CreateDeferred(ObjectKind.MinionOrMount, () => dalamudUtil.GetMinionOrMountPtr(), isWatched: true);
        _playerRelatedObjects[ObjectKind.Pet] = gameObjectHandlerFactory.CreateDeferred(ObjectKind.Pet, () => dalamudUtil.GetPetPtr(), isWatched: true);
        _playerRelatedObjects[ObjectKind.Companion] = gameObjectHandlerFactory.CreateDeferred(ObjectKind.Companion, () => dalamudUtil.GetCompanionPtr(), isWatched: true);

        Mediator.Subscribe<ConnectedMessage>(this, (x) =>
        {
            if (_isZoning || _haltCharaDataCreation) return;

            NotePlayerAppearanceSignal();
            _connectSettleUntilUtc = DateTime.UtcNow + ConnectedImmediatePublishSettleWindow;

            var playerHandler = _playerRelatedObjects[ObjectKind.Player];
            if (playerHandler.Address != IntPtr.Zero)
            {
                ScheduleImmediatePlayerFollowUpBuild("Connected:InitialPlayerState", ConnectedImmediatePublishSettleWindow);
            }

            AddCacheToCreate(ObjectKind.Pet, "Connected:InitialState");
            AddCacheToCreate(ObjectKind.MinionOrMount, "Connected:InitialState");
            AddCacheToCreate(ObjectKind.Companion, "Connected:InitialState");
        });

        Mediator.Subscribe<ClassJobChangedMessage>(this, (msg) =>
        {
            if (msg.GameObjectHandler == _playerRelatedObjects[ObjectKind.Player])
            {
                NotePlayerAppearanceSignal();
            }
        });

        Mediator.Subscribe<ClearCacheForObjectMessage>(this, (msg) =>
        {
            if (msg.ObjectToCreateFor.ObjectKind == ObjectKind.Pet)
            {
                return;
            }

            AddCacheToCreate(msg.ObjectToCreateFor.ObjectKind, $"ClearCache:{msg.ObjectToCreateFor.ObjectKind}");
        });

        Mediator.Subscribe<CustomizePlusMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            foreach (var item in _playerRelatedObjects
                .Where(item => msg.Address == null
                || item.Value.Address == msg.Address).Select(k => k.Key))
            {
                if (item == ObjectKind.Player)
                {
                    NotePlayerAppearanceSignal();
                    continue;
                }

                AddCacheToCreate(item, $"CustomizePlus:{item}");
            }
        });

        Mediator.Subscribe<HeelsOffsetMessage>(this, (msg) =>
        {
            if (_isZoning) return;
        });

        Mediator.Subscribe<GlamourerChangedMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            var changedType = _playerRelatedObjects.FirstOrDefault(f => f.Value.Address == msg.Address);
            if (!default(KeyValuePair<ObjectKind, GameObjectHandler>).Equals(changedType))
            {
                if (changedType.Key == ObjectKind.Player)
                {
                    NotePlayerAppearanceSignal();

                    var playerHandler = changedType.Value;
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await Task.Delay(125, _runtimeCts.Token).ConfigureAwait(false);

                            if (_isZoning || _haltCharaDataCreation)
                                return;

                            Mediator.Publish(new ImmediatePlayerStatePublishMessage(playerHandler, "Glamourer:PlayerFallback"));
                        }
                        catch (OperationCanceledException)
                        {
                            // ignore
                        }
                    });

                    return;
                }

                AddCacheToCreate(changedType.Key, $"Glamourer:{changedType.Key}");
            }
        });

        Mediator.Subscribe<HonorificMessage>(this, (msg) =>
        {
            if (_isZoning) return;
        });

        Mediator.Subscribe<MoodlesMessage>(this, (msg) =>
        {
            if (_isZoning) return;
        });

        Mediator.Subscribe<PetNamesMessage>(this, (msg) =>
        {
            if (_isZoning) return;
        });

        Mediator.Subscribe<PenumbraModSettingChangedMessage>(this, (msg) =>
        {
            if (_isZoning || _haltCharaDataCreation) return;
            if (_localPapSafetyModService.IsManagedRuntimeModIdentifierForCurrentRoot(msg.ModName)) return;

            var playerHandler = _playerRelatedObjects[ObjectKind.Player];
            if (playerHandler.Address == IntPtr.Zero) return;

            NotePlayerAppearanceSignal();
            _localPapSafetyModService.InvalidateLocalPlayerCollectionSettingsCache();

            _penumbraModSettingPublishCts.Cancel();
            _penumbraModSettingPublishCts.Dispose();
            _penumbraModSettingPublishCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
            var penumbraToken = _penumbraModSettingPublishCts.Token;

            _ = Task.Run(async () =>
            {
                try
                {
                    if (!await IsPenumbraModSettingChangeForLocalPlayerCollectionAsync(msg, penumbraToken).ConfigureAwait(false))
                        return;

                    if (_localPapSafetyModService.ModMayContainHumanAnimationPapPayload(msg.ModName, penumbraToken))
                    {
                        await Task.Delay(PenumbraModSettingSettleDelay, penumbraToken).ConfigureAwait(false);

                        if (_isZoning || _haltCharaDataCreation || playerHandler.Address == IntPtr.Zero)
                            return;

                        await _characterDataFactory.RefreshLocalPlayerConvertedAnimationPackAsync(playerHandler, penumbraToken).ConfigureAwait(false);
                    }

                    if (_isZoning || _haltCharaDataCreation || playerHandler.Address == IntPtr.Zero)
                        return;

                    QueueImmediatePlayerPublish(playerHandler, "PenumbraModSettingChanged");
                }
                catch (OperationCanceledException)
                {
                    // ignore;
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Failed to process local Penumbra mod-setting change; falling back to the normal immediate publish queue");
                    if (!_isZoning && !_haltCharaDataCreation && playerHandler.Address != IntPtr.Zero)
                        QueueImmediatePlayerPublish(playerHandler, "PenumbraModSettingChanged");
                }
            }, penumbraToken);
        });

        Mediator.Subscribe<PenumbraFileCacheChangedMessage>(this, (msg) =>
        {
            if (_isZoning || _haltCharaDataCreation) return;
            if (msg.Paths != null && msg.Paths.Count > 0 && msg.Paths.All(_localPapSafetyModService.IsManagedRuntimePapPath)) return;

            _localPapSafetyModService.InvalidateSelectedAnimationSupportCache();

            var playerHandler = _playerRelatedObjects[ObjectKind.Player];
            if (playerHandler.Address == IntPtr.Zero) return;

            QueueImmediatePlayerPublish(playerHandler, "PenumbraFileCacheChanged");
        });

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (msg) => ProcessCacheCreation());
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _playerRelatedObjects.Values.ToList().ForEach(p => p.Dispose());
        _runtimeCts.Cancel();
        _runtimeCts.Dispose();
        _immediatePublishCts.Cancel();
        _immediatePublishCts.Dispose();
        _immediatePlayerFollowUpCts.Cancel();
        _immediatePlayerFollowUpCts.Dispose();
        _coalescedImmediatePlayerPublishCts.Cancel();
        _coalescedImmediatePlayerPublishCts.Dispose();
        _penumbraModSettingPublishCts.Cancel();
        _penumbraModSettingPublishCts.Dispose();
        _creationCts.Cancel();
        _creationCts.Dispose();
    }

    private void NotePlayerAppearanceSignal()
    {
        _lastPlayerAppearanceSignalUtc = DateTime.UtcNow;
    }

    private bool IsWithinStartupImmediatePublishSettleWindow()
    {
        return DateTime.UtcNow - _serviceStartUtc <= StartupImmediatePublishSettleWindow;
    }

    private bool TryGetImmediatePlayerPublishSettleDelay(string? reason, out TimeSpan delay)
    {
        delay = TimeSpan.Zero;

        if (string.IsNullOrWhiteSpace(reason))
            return false;

        if (!(IsFastPlayerAppearanceReason(reason)
            || reason.StartsWith("Startup:", StringComparison.Ordinal)
            || reason.StartsWith("Connected:", StringComparison.Ordinal)))
        {
            return false;
        }

        var now = DateTime.UtcNow;
        DateTime? settleUntilUtc = null;

        if (IsWithinStartupImmediatePublishSettleWindow())
            settleUntilUtc = _serviceStartUtc + StartupImmediatePublishSettleWindow;

        if (_connectSettleUntilUtc > now)
            settleUntilUtc = !settleUntilUtc.HasValue || _connectSettleUntilUtc > settleUntilUtc.Value
                ? _connectSettleUntilUtc
                : settleUntilUtc;

        if (!settleUntilUtc.HasValue || settleUntilUtc.Value <= now)
            return false;

        delay = settleUntilUtc.Value - now;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        return true;
    }

    private bool IsWithinPenumbraTransientFollowWindow()
    {
        return DateTime.UtcNow - _lastPlayerAppearanceSignalUtc <= PenumbraTransientFollowWindow;
    }

    private bool IsWithinInitialPenumbraTransientSettleWindow()
    {
        return DateTime.UtcNow - _serviceStartUtc <= InitialPenumbraTransientSettleWindow;
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
                Logger.LogTrace("Ignoring Penumbra mod-setting change for non-local collection {collectionId} mod {modName} change {change}; local collection is {localCollectionId}",
                    msg.CollectionId, msg.ModName, msg.Change, cachedCollectionId);
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
                    Logger.LogTrace("Ignoring Penumbra mod-setting change for non-local collection {collectionId} mod {modName} change {change}; local collection is {localCollectionId}",
                        msg.CollectionId, msg.ModName, msg.Change, cachedCollectionId);
                }

                return isLocalCached;
            }

            var collectionState = await _localPapSafetyModService.TryGetLocalPlayerCollectionSettingsAsync(token).ConfigureAwait(false);
            if (collectionState?.CollectionId is not { } localCollectionId || localCollectionId == Guid.Empty)
            {
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Could not resolve local Penumbra collection for mod-setting change {collectionId} mod {modName}; allowing publish", msg.CollectionId, msg.ModName);

                return true;
            }

            _cachedLocalPlayerCollectionId = localCollectionId;
            _cachedLocalPlayerCollectionIdUntilUtc = now + TimeSpan.FromMilliseconds(250);

            var isLocal = localCollectionId == msg.CollectionId;
            if (!isLocal && Logger.IsEnabled(LogLevel.Trace))
            {
                Logger.LogTrace("Ignoring Penumbra mod-setting change for non-local collection {collectionId} mod {modName} change {change}; local collection is {localCollectionId}",
                    msg.CollectionId, msg.ModName, msg.Change, localCollectionId);
            }

            return isLocal;
        }
        finally
        {
            _localCollectionLookupLock.Release();
        }
    }

    private static bool IsPlayerAppearanceSignalReason(string? reason)
    {
        if (string.IsNullOrEmpty(reason)) return false;

        return reason.StartsWith("GameObject:SemanticDiff", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("Glamourer:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }

    private static bool IsFastPlayerAppearanceReason(string? reason)
    {
        if (string.IsNullOrEmpty(reason))
            return false;

        return reason.StartsWith("Glamourer:", StringComparison.Ordinal)
            || reason.StartsWith("GameObject:SemanticDiff", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
            || string.Equals(reason, "PenumbraFileCacheChanged", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraEndRedraw", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraRedraw", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }

    private static bool ShouldUseImmediatePlayerPublishOnly(string? reason)
    {
        if (string.IsNullOrEmpty(reason))
            return false;

        return string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
            || string.Equals(reason, "PenumbraFileCacheChanged", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("Glamourer:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }

    private static bool IsPureFastPlayerAppearanceReasonSet(IReadOnlyCollection<string> reasons)
    {
        if (reasons.Count == 0) return false;

        foreach (var reason in reasons)
        {
            if (!IsFastPlayerAppearanceReason(reason))
                return false;
        }

        return true;
    }

    private bool HasRecentImmediatePlayerPublish(TimeSpan window)
    {
        return DateTime.UtcNow - _lastImmediatePlayerPublishUtc <= window;
    }

    private bool ShouldSuppressFastPlayerBuildAfterRecentImmediatePublish(string? reason)
    {
        if (string.IsNullOrEmpty(reason))
            return false;

        return IsFastPlayerAppearanceReason(reason)
            && HasRecentImmediatePlayerPublish(ImmediatePlayerPublishSettleWindow);
    }

    private void ScheduleImmediatePlayerFollowUpBuild(string reason, TimeSpan delay)
    {
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        _immediatePlayerFollowUpCts.Cancel();
        _immediatePlayerFollowUpCts.Dispose();
        _immediatePlayerFollowUpCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
        var token = _immediatePlayerFollowUpCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, token).ConfigureAwait(false);

                if (_isZoning || _haltCharaDataCreation)
                    return;

                AddCacheToCreate(ObjectKind.Player, $"ImmediateFollowUp:{reason}");
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
        }, token);
    }

    private static TimeSpan GetDebounceDelay(ObjectKind kind, string? reason)
    {
        if (kind == ObjectKind.Player && IsFastPlayerAppearanceReason(reason))
            return FastPlayerAppearanceDelay;

        return DefaultBuildDelay;
    }

    private TimeSpan GetBuildDelay(IReadOnlyCollection<ObjectKind> objectKinds)
    {
        if (objectKinds.Contains(ObjectKind.Player))
        {
            lock (_cacheCreateLockObj)
            {
                if (_activeReasons.TryGetValue(ObjectKind.Player, out var reasons))
                {
                    if (IsPureFastPlayerAppearanceReasonSet(reasons))
                        return FastPlayerAppearanceDelay;

                    if (IsPurePenumbraTransientCombo(reasons) && IsWithinPenumbraTransientFollowWindow())
                        return FastPlayerAppearanceDelay;
                }
            }
        }

        return DefaultBuildDelay;
    }

    private static bool IsPurePenumbraTransientCombo(IReadOnlyCollection<string> reasons)
    {
        if (reasons.Count == 0) return false;

        bool hasPenumbra = false;
        bool hasTransient = false;

        foreach (var reason in reasons)
        {
            if (reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal))
            {
                hasPenumbra = true;
                continue;
            }

            if (string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal))
            {
                hasTransient = true;
                continue;
            }

            return false;
        }

        return hasPenumbra && hasTransient;
    }

    private bool ShouldSkipPenumbraTransientOnlyBuild(ObjectKind objectKind, IReadOnlyCollection<string> reasons)
    {
        if (objectKind != ObjectKind.Player)
            return false;

        if (!IsPurePenumbraTransientCombo(reasons))
            return false;

        if (IsWithinPenumbraTransientFollowWindow())
            return false;

        if (IsWithinInitialPenumbraTransientSettleWindow())
            return true;

        return true;
    }

    private void SuppressFastPlayerBuildsFor(TimeSpan duration)
    {
        var suppressUntil = DateTime.UtcNow + duration;
        lock (_cacheCreateLockObj)
        {
            if (suppressUntil > _suppressFastPlayerBuildsUntilUtc)
                _suppressFastPlayerBuildsUntilUtc = suppressUntil;
        }
    }

    private bool ShouldSuppressFastPlayerBuildNow(string? reason)
    {
        if (!IsFastPlayerAppearanceReason(reason))
            return false;

        lock (_cacheCreateLockObj)
        {
            return DateTime.UtcNow <= _suppressFastPlayerBuildsUntilUtc;
        }
    }

    private void ScheduleCoalescedImmediatePlayerPublish(GameObjectHandler objectToCreateFor, string reason, TimeSpan delay)
    {
        if (objectToCreateFor.ObjectKind != ObjectKind.Player)
            return;

        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        NotePlayerAppearanceSignal();
        SuppressFastPlayerBuildsFor(delay + ImmediatePlayerPublishSettleWindow);

        _coalescedImmediatePlayerPublishCts.Cancel();
        _coalescedImmediatePlayerPublishCts.Dispose();
        _coalescedImmediatePlayerPublishCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
        var token = _coalescedImmediatePlayerPublishCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, token).ConfigureAwait(false);

                if (_isZoning || _haltCharaDataCreation)
                    return;

                await PublishPlayerStateImmediatelyAsync(objectToCreateFor, reason, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // coalesced publish was replaced by a newer one
            }
        }, token);
    }

    private void QueueImmediatePlayerPublish(GameObjectHandler objectToCreateFor, string reason)
    {
        if (objectToCreateFor.ObjectKind != ObjectKind.Player)
            return;

        NotePlayerAppearanceSignal();

        if (TryGetImmediatePlayerPublishSettleDelay(reason, out var settleDelay))
        {
            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, reason, settleDelay);
            return;
        }

        if (IsFastPlayerAppearanceReason(reason))
        {
            TimeSpan delay;
            lock (_cacheCreateLockObj)
            {
                var immediatePublishInFlight = _immediatePublishLock.CurrentCount == 0;
                if (immediatePublishInFlight)
                {
                    _pendingImmediatePlayerFollowUp = true;
                    _pendingImmediatePlayerFollowUpReason = reason;
                    return;
                }

                var timeSinceLastImmediatePublish = DateTime.UtcNow - _lastImmediatePlayerPublishUtc;
                delay = timeSinceLastImmediatePublish <= ImmediatePlayerPublishCooldown
                    ? ImmediatePlayerPublishCooldown - timeSinceLastImmediatePublish
                    : ImmediatePlayerPublishCoalesceDelay;
            }

            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, reason, delay);
            return;
        }

        var scheduleFollowUpBuild = false;
        var followUpDelay = TimeSpan.Zero;

        lock (_cacheCreateLockObj)
        {
            var immediatePublishInFlight = _immediatePublishLock.CurrentCount == 0;
            var timeSinceLastImmediatePublish = DateTime.UtcNow - _lastImmediatePlayerPublishUtc;
            var withinImmediateCooldown = timeSinceLastImmediatePublish <= ImmediatePlayerPublishCooldown;
            if (immediatePublishInFlight)
            {
                _pendingImmediatePlayerFollowUp = true;
                _pendingImmediatePlayerFollowUpReason = reason;
                return;
            }

            if (withinImmediateCooldown)
            {
                scheduleFollowUpBuild = true;
                followUpDelay = ImmediatePlayerPublishCooldown - timeSinceLastImmediatePublish;
            }
        }

        if (scheduleFollowUpBuild)
        {
            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, reason, followUpDelay);
            return;
        }

        _immediatePublishCts.Cancel();
        _immediatePublishCts.Dispose();
        _immediatePublishCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
        var token = _immediatePublishCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await PublishPlayerStateImmediatelyAsync(objectToCreateFor, reason, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }, token);
    }

    private async Task PublishPlayerStateImmediatelyAsync(GameObjectHandler objectToCreateFor, string reason, CancellationToken token, bool forceOutbound = false)
    {
        if (objectToCreateFor.ObjectKind != ObjectKind.Player)
            return;

        NotePlayerAppearanceSignal();

        var queueFollowUpBuild = false;
        string followUpReason = reason;

        await _immediatePublishLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var fragment = await BuildLocalPlayerFragmentAsync(objectToCreateFor, token).ConfigureAwait(false);
            _playerData.SetFragment(ObjectKind.Player, fragment);

            lock (_cacheCreateLockObj)
            {
                _cachesToCreate.Remove(ObjectKind.Player);
                _debouncedObjectCache.Remove(ObjectKind.Player);
                _debouncedReasons.Remove(ObjectKind.Player);

                if (!_currentlyCreating.Contains(ObjectKind.Player))
                    _activeReasons.Remove(ObjectKind.Player);
            }

            Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI(), forceOutbound, reason));
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Immediate player state publish failed for reason {reason}", reason);
        }
        finally
        {
            lock (_cacheCreateLockObj)
            {
                _lastImmediatePlayerPublishUtc = DateTime.UtcNow;
                var suppressUntil = _lastImmediatePlayerPublishUtc + ImmediatePlayerPublishSettleWindow;
                if (suppressUntil > _suppressFastPlayerBuildsUntilUtc)
                    _suppressFastPlayerBuildsUntilUtc = suppressUntil;

                queueFollowUpBuild = _pendingImmediatePlayerFollowUp;
                if (!string.IsNullOrWhiteSpace(_pendingImmediatePlayerFollowUpReason))
                    followUpReason = _pendingImmediatePlayerFollowUpReason!;

                _pendingImmediatePlayerFollowUp = false;
                _pendingImmediatePlayerFollowUpReason = null;
            }

            _immediatePublishLock.Release();
        }

        if (queueFollowUpBuild && !_isZoning && !_haltCharaDataCreation)
        {
            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, $"ImmediateFollowUp:{followUpReason}", ImmediatePlayerPublishCooldown);
        }
    }

    private void AddCacheToCreate(ObjectKind kind = ObjectKind.Player, string reason = "Unspecified")
    {
        if (kind == ObjectKind.Player && ShouldSuppressFastPlayerBuildNow(reason))
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Suppressing player cache rebuild for {reason} during immediate publish settle window", reason);

            return;
        }

        lock (_cacheCreateLockObj)
        {
            if (_currentlyCreating.Contains(kind))
            {
                if (!_activeReasons.TryGetValue(kind, out var currentReasons))
                {
                    currentReasons = [];
                    _activeReasons[kind] = currentReasons;
                }

                currentReasons.Add(reason);
                _cachesToCreate.Add(kind);
                return;
            }

            if (_cachesToCreate.Contains(kind))
            {
                if (!_activeReasons.TryGetValue(kind, out var queuedReasons))
                {
                    queuedReasons = [];
                    _activeReasons[kind] = queuedReasons;
                }

                queuedReasons.Add(reason);
                return;
            }

            if (_debouncedReasons.TryGetValue(kind, out var existingReasons) && existingReasons.Contains(reason))
            {
                return;
            }
        }

        _debounceCts.Cancel();
        _debounceCts.Dispose();
        _debounceCts = new();
        var token = _debounceCts.Token;

        lock (_cacheCreateLockObj)
        {
            _debouncedObjectCache.Add(kind);
            if (!_debouncedReasons.TryGetValue(kind, out var reasons))
            {
                reasons = [];
                _debouncedReasons[kind] = reasons;
            }

            reasons.Add(reason);
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(GetDebounceDelay(kind, reason), token).ConfigureAwait(false);

                lock (_cacheCreateLockObj)
                {
                    foreach (var item in _debouncedObjectCache)
                    {
                        _cachesToCreate.Add(item);
                        if (_debouncedReasons.TryGetValue(item, out var reasons) && reasons.Count > 0)
                        {
                            if (!_activeReasons.TryGetValue(item, out var active))
                            {
                                active = [];
                                _activeReasons[item] = active;
                            }

                            foreach (var reasonItem in reasons)
                                active.Add(reasonItem);
                        }
                    }

                    _debouncedObjectCache.Clear();
                    _debouncedReasons.Clear();
                }
            }
            catch (OperationCanceledException)
            {
                // debounce restarted; ignore
            }
        });
    }

    private async Task<CharacterDataFragmentPlayer?> BuildLocalPlayerFragmentAsync(GameObjectHandler playerHandler, CancellationToken token)
    {
        return await _characterDataFactory.BuildCharacterData(playerHandler, token).ConfigureAwait(false) as CharacterDataFragmentPlayer;
    }

    private void ProcessCacheCreation()
    {
        if (_isZoning || _haltCharaDataCreation) return;

        List<ObjectKind> pendingObjectKinds;
        lock (_cacheCreateLockObj)
        {
            if (_cachesToCreate.Count == 0) return;

            pendingObjectKinds = _cachesToCreate.ToList();
            if (pendingObjectKinds.All(_currentlyCreating.Contains)) return;
        }

        if (pendingObjectKinds.Any(kind =>
                _playerRelatedObjects.TryGetValue(kind, out var handler)
                && handler.CurrentDrawCondition is not (GameObjectHandler.DrawCondition.None
                    or GameObjectHandler.DrawCondition.DrawObjectZero
                    or GameObjectHandler.DrawCondition.ObjectZero)))
        {
            return;
        }

        _creationCts.Cancel();
        _creationCts.Dispose();
        _creationCts = new();

        List<ObjectKind> objectKindsToCreate;
        lock (_cacheCreateLockObj)
        {
            objectKindsToCreate = _cachesToCreate.ToList();
            foreach (var creationObj in objectKindsToCreate)
                _currentlyCreating.Add(creationObj);

            _cachesToCreate.Clear();
        }

        _ = Task.Run(async () =>
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_creationCts.Token, _runtimeCts.Token);

            var buildSucceeded = false;
            var builtAnyData = false;

            await Task.Delay(GetBuildDelay(objectKindsToCreate), linkedCts.Token).ConfigureAwait(false);

            try
            {
                Dictionary<ObjectKind, CharacterDataFragment?> createdData = [];
                List<ObjectKind> objectKindsSnapshot;
                lock (_cacheCreateLockObj)
                {
                    objectKindsSnapshot = [.. _currentlyCreating];
                }

                async Task<CharacterDataFragment?> BuildOneAsync(ObjectKind objectKind)
                {
                    HashSet<string> reasonSet;
                    lock (_cacheCreateLockObj)
                    {
                        reasonSet = _activeReasons.TryGetValue(objectKind, out var reasons) && reasons.Count > 0
                            ? [.. reasons]
                            : [];
                    }

                    if (ShouldSkipPenumbraTransientOnlyBuild(objectKind, reasonSet))
                    {
                        return null;
                    }

                    if (objectKind == ObjectKind.Player
                        && IsPureFastPlayerAppearanceReasonSet(reasonSet)
                        && (HasRecentImmediatePlayerPublish(ImmediatePlayerPublishSettleWindow)
                            || reasonSet.Any(ShouldSuppressFastPlayerBuildNow)))
                    {
                        return null;
                    }

                    var handler = _playerRelatedObjects[objectKind];
                    if (objectKind != ObjectKind.Player
                        && (handler.Address == IntPtr.Zero
                            || handler.CurrentDrawCondition is GameObjectHandler.DrawCondition.ObjectZero or GameObjectHandler.DrawCondition.DrawObjectZero))
                    {
                        return null;
                    }

                    if (objectKind == ObjectKind.Player)
                        return await BuildLocalPlayerFragmentAsync(handler, linkedCts.Token).ConfigureAwait(false);

                    return await _characterDataFactory.BuildCharacterData(handler, linkedCts.Token).ConfigureAwait(false);
                }

                var publishedEarlyPlayer = false;
                if (objectKindsSnapshot.Count == 1 && objectKindsSnapshot.Contains(ObjectKind.Player))
                {
                    var playerFragment = await BuildOneAsync(ObjectKind.Player).ConfigureAwait(false);
                    createdData[ObjectKind.Player] = playerFragment;
                    _playerData.SetFragment(ObjectKind.Player, playerFragment);
                    builtAnyData = true;
                    Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI()));
                    publishedEarlyPlayer = true;
                }
                else if (objectKindsSnapshot.Contains(ObjectKind.Player))
                {
                    createdData[ObjectKind.Player] = await BuildOneAsync(ObjectKind.Player).ConfigureAwait(false);
                }

                foreach (var objectKind in objectKindsSnapshot)
                {
                    if (objectKind == ObjectKind.Player)
                        continue;

                    createdData[objectKind] = await BuildOneAsync(objectKind).ConfigureAwait(false);
                }

                foreach (var kvp in createdData)
                {
                    if (kvp.Key == ObjectKind.Player && publishedEarlyPlayer)
                        continue;

                    _playerData.SetFragment(kvp.Key, kvp.Value);
                }

                if (createdData.Count > 0 && !publishedEarlyPlayer)
                {
                    builtAnyData = true;
                    Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI()));
                }
                else if (createdData.Any(kvp => kvp.Key != ObjectKind.Player))
                {
                    builtAnyData = true;
                    Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI()));
                }

                buildSucceeded = true;
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogCritical(ex, "Error during Cache Creation Processing");
            }
            finally
            {
                lock (_cacheCreateLockObj)
                {
                    var affectedKinds = _currentlyCreating.ToList();
                    _currentlyCreating.Clear();

                    if (!buildSucceeded)
                    {
                        foreach (var key in affectedKinds)
                            _cachesToCreate.Add(key);
                    }

                    foreach (var key in affectedKinds)
                    {
                        if ((buildSucceeded || builtAnyData) && !_cachesToCreate.Contains(key))
                        {
                            _activeReasons.Remove(key);
                        }
                    }
                }
            }
        });
    }
}