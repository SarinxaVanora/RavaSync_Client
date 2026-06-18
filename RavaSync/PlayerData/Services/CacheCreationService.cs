using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
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
    private readonly FileCacheManager _fileCacheManager;
    private readonly LocalPapSafetyModService _localPapSafetyModService;
    private readonly TransientResourceManager _transientResourceManager;
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
    private CancellationTokenSource _classJobTransientWarmupCts = new();
    private bool _haltCharaDataCreation;
    private bool _isZoning = false;
    private DateTime _connectSettleUntilUtc = DateTime.MinValue;
    private readonly DateTime _serviceStartUtc = DateTime.UtcNow;
    private DateTime _lastPlayerAppearanceSignalUtc = DateTime.MinValue;
    private DateTime _lastImmediatePlayerPublishUtc = DateTime.MinValue;
    private DateTime _suppressFastPlayerBuildsUntilUtc = DateTime.MinValue;
    private DateTime _lastTransientManifestDeltaPublishUtc = DateTime.MinValue;
    private readonly SemaphoreSlim _localCollectionLookupLock = new(1, 1);
    private Guid _cachedLocalPlayerCollectionId = Guid.Empty;
    private DateTime _cachedLocalPlayerCollectionIdUntilUtc = DateTime.MinValue;
    private bool _pendingImmediatePlayerFollowUp;
    private bool _pendingImmediatePlayerFollowUpForceOutbound;
    private string? _pendingImmediatePlayerFollowUpReason;
    private static readonly TimeSpan PenumbraTransientFollowWindow = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan InitialPenumbraTransientSettleWindow = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan StartupImmediatePublishSettleWindow = TimeSpan.FromSeconds(4);
    private static readonly TimeSpan ConnectedImmediatePublishSettleWindow = TimeSpan.Zero;
    private static readonly TimeSpan FastPlayerAppearanceDelay = TimeSpan.FromMilliseconds(150);
    private static readonly TimeSpan OwnedObjectBuildDelay = TimeSpan.FromMilliseconds(75);
    private static readonly TimeSpan OwnedObjectStormBuildDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan DefaultBuildDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan ImmediatePlayerPublishCoalesceDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan ImmediatePlayerPublishCooldown = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan ImmediatePlayerPublishSettleWindow = TimeSpan.FromMilliseconds(2000);
    private static readonly TimeSpan TransientResourceImmediateCoalesceDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan TransientResourceImmediateCooldown = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PenumbraModSettingSettleDelay = TimeSpan.FromMilliseconds(150);
    private static readonly TimeSpan TransientManifestImmediateCoalesceDelay = TimeSpan.Zero;
    private static readonly TimeSpan TransientManifestImmediateCooldown = TimeSpan.Zero;

    public CacheCreationService(ILogger<CacheCreationService> logger, MareMediator mediator, GameObjectHandlerFactory gameObjectHandlerFactory,
        PlayerDataFactory characterDataFactory, FileCacheManager fileCacheManager, DalamudUtilService dalamudUtil, LocalPapSafetyModService localPapSafetyModService,
        TransientResourceManager transientResourceManager) : base(logger, mediator)
    {
        _characterDataFactory = characterDataFactory;
        _fileCacheManager = fileCacheManager;
        _dalamudUtil = dalamudUtil;
        _localPapSafetyModService = localPapSafetyModService;
        _transientResourceManager = transientResourceManager;

        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (msg) => _isZoning = true);
        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (msg) => _isZoning = false);

        Mediator.Subscribe<HaltCharaDataCreation>(this, (msg) =>
        {
            _haltCharaDataCreation = !msg.Resume;
        });

        Mediator.Subscribe<CreateCacheForObjectMessage>(this, (msg) =>
        {
            if (!IsOwnedTrackedObjectHandler(msg.ObjectToCreateFor))
                return;

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
            if (!IsOwnedTrackedObjectHandler(msg.ObjectToCreateFor)) return;
            if (_isZoning || _haltCharaDataCreation) return;

            QueueImmediatePlayerPublish(msg.ObjectToCreateFor, msg.Reason);
        });

        Mediator.Subscribe<TransientManifestDeltaPublishMessage>(this, (msg) =>
        {
            if (_isZoning || _haltCharaDataCreation) return;
            QueueTransientManifestDeltaPublish(msg);
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
                _ = RunLinuxFriendlyBackgroundWork(async () =>
                {
                    try
                    {
                        await PublishPlayerStateImmediatelyAsync(playerHandler, "Connected:ImmediatePlayerState", _runtimeCts.Token, forceOutbound: true).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // reconnect publish was superseded or the plugin is shutting down
                    }
                }, _runtimeCts.Token);

                // Keep a tiny follow-up so late IPC/plugin settle can still correct the state,
                // but do not make the first reconnect push depend on that settled pass.
                ScheduleImmediatePlayerFollowUpBuild("Connected:SettledPlayerState", TimeSpan.FromSeconds(1));
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
                ScheduleClassJobTransientWarmup();
            }
        });

        Mediator.Subscribe<ClearCacheForObjectMessage>(this, (msg) =>
        {
            if (!IsOwnedTrackedObjectHandler(msg.ObjectToCreateFor))
                return;

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
            if (LocalPapSafetyModService.IsRavaSyncInternalTemporaryModIdentifier(msg.ModName)) return;
            if (_localPapSafetyModService.IsManagedRuntimeModIdentifierForCurrentRoot(msg.ModName)) return;

            var playerHandler = _playerRelatedObjects[ObjectKind.Player];
            if (playerHandler.Address == IntPtr.Zero) return;

            NotePlayerAppearanceSignal();
            _localPapSafetyModService.InvalidateLocalPlayerCollectionSettingsCache();

            _penumbraModSettingPublishCts.Cancel();
            _penumbraModSettingPublishCts.Dispose();
            _penumbraModSettingPublishCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
            var penumbraToken = _penumbraModSettingPublishCts.Token;

            _ = RunLinuxFriendlyBackgroundWork(async () =>
            {
                try
                {
                    if (!await IsPenumbraModSettingChangeForLocalPlayerCollectionAsync(msg, penumbraToken).ConfigureAwait(false))
                        return;

                    await Task.Delay(PenumbraModSettingSettleDelay, penumbraToken).ConfigureAwait(false);

                    if (_isZoning || _haltCharaDataCreation || playerHandler.Address == IntPtr.Zero)
                        return;

                    var runtimeChanged = false;
                    if (_localPapSafetyModService.ModMayContainHumanAnimationPapPayload(msg.ModName, penumbraToken))
                        runtimeChanged = await _characterDataFactory.RefreshLocalPlayerConvertedAnimationPackAsync(playerHandler, penumbraToken).ConfigureAwait(false);

                    if (runtimeChanged)
                    {
                        Logger.LogDebug("Local Penumbra mod-setting change for {mod} refreshed the converted animation runtime pack; redraw-triggered publish will carry the settled state", msg.ModName);
                        return;
                    }

                    QueueImmediatePlayerPublish(playerHandler, "PenumbraModSettingChanged:PlayerState");
                }
                catch (OperationCanceledException)
                {
                    // ignore;
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Failed to process local Penumbra mod-setting converted-animation refresh for {mod}", msg.ModName);
                }
            }, penumbraToken);
        });

        Mediator.Subscribe<PenumbraFileCacheChangedMessage>(this, (msg) =>
        {
            if (_isZoning || _haltCharaDataCreation) return;
            if (msg.Paths != null && msg.Paths.Count > 0 && msg.Paths.All(_localPapSafetyModService.IsManagedRuntimePapPath)) return;

            var forceLocalPublish = msg.ForceLocalPlayerPublish;
            if (!forceLocalPublish && !PenumbraFileCacheChangeTouchesCurrentLocalData(msg.Paths)) return;

            //_localPapSafetyModService.InvalidateSelectedAnimationSupportCache();

            var playerHandler = _playerRelatedObjects[ObjectKind.Player];
            if (playerHandler.Address == IntPtr.Zero) return;

            QueueFileChangedPlayerPublish(playerHandler, msg);
        });

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (msg) => ProcessCacheCreation());
    }

    private static Task RunLinuxFriendlyBackgroundWork(Func<Task> work, CancellationToken token = default)
    {
        return Task.Factory.StartNew(async () =>
        {
            TrySetCurrentThreadPriorityBelowNormal();
            await work().ConfigureAwait(false);
        }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    private static void TrySetCurrentThreadPriorityBelowNormal()
    {
        try
        {
            Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
        }
        catch
        {
            // Best-effort only; some runtimes ignore or reject priority changes.
        }
    }

    private void ScheduleClassJobTransientWarmup()
    {
        _classJobTransientWarmupCts.Cancel();
        _classJobTransientWarmupCts.Dispose();
        _classJobTransientWarmupCts = CancellationTokenSource.CreateLinkedTokenSource(_runtimeCts.Token);
        var token = _classJobTransientWarmupCts.Token;

        _ = RunLinuxFriendlyBackgroundWork(async () =>
        {
            try
            {
                await Task.Delay(25, token).ConfigureAwait(false);

                if (_isZoning || _haltCharaDataCreation)
                    return;

                var knownResolved = _transientResourceManager.GetKnownResolvedFilePaths(ObjectKind.Player, validateExists: false);
                if (knownResolved.Count == 0)
                    return;

                var resolvedPaths = knownResolved.Values
                    .Where(static path => !string.IsNullOrWhiteSpace(path))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                if (resolvedPaths.Length == 0)
                    return;

                token.ThrowIfCancellationRequested();
                _fileCacheManager.GetFileHashesByPaths(resolvedPaths);

                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Pre-warmed {count} local player transient hash(es) for the current class/job scope", resolvedPaths.Length);
            }
            catch (OperationCanceledException)
            {
                // newer class/job change or shutdown replaced this warmup
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Failed to pre-warm local player transient hashes for class/job change");
            }
        }, token);
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
        _classJobTransientWarmupCts.Cancel();
        _classJobTransientWarmupCts.Dispose();
        _creationCts.Cancel();
        _creationCts.Dispose();
    }

    private bool IsOwnedTrackedObjectHandler(GameObjectHandler? handler)
    {
        if (handler == null)
            return false;

        return _playerRelatedObjects.TryGetValue(handler.ObjectKind, out var trackedHandler)
            && ReferenceEquals(trackedHandler, handler);
    }

    private bool PenumbraFileCacheChangeTouchesCurrentLocalData(IReadOnlyCollection<string>? changedPaths)
    {
        if (changedPaths == null || changedPaths.Count == 0)
            return true;

        var changedFullPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var changedPath in changedPaths)
        {
            var normalized = NormalizeFullPathOrEmpty(changedPath);
            if (!string.IsNullOrWhiteSpace(normalized))
                changedFullPaths.Add(normalized);
        }

        if (changedFullPaths.Count == 0)
            return true;

        foreach (var replacementSet in _playerData.FileReplacements.Values)
        {
            if (replacementSet == null)
                continue;

            foreach (var replacement in replacementSet)
            {
                var resolvedPath = NormalizeFullPathOrEmpty(replacement.ResolvedPath);
                if (!string.IsNullOrWhiteSpace(resolvedPath) && changedFullPaths.Contains(resolvedPath))
                    return true;
            }
        }

        if (Logger.IsEnabled(LogLevel.Trace))
            Logger.LogTrace("Ignoring Penumbra file-cache change because none of the changed files are in current local outbound data: {count} changed path(s)", changedFullPaths.Count);

        return false;
    }

    private static string NormalizeFullPathOrEmpty(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        try
        {
            return Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        }
        catch
        {
            return path.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar).Trim();
        }
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
        var now = DateTime.UtcNow;
        var cachedCollectionId = _cachedLocalPlayerCollectionId;
        if (msg.CollectionId != Guid.Empty && cachedCollectionId != Guid.Empty && now <= _cachedLocalPlayerCollectionIdUntilUtc)
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

            if (msg.CollectionId == Guid.Empty)
            {
                if (string.IsNullOrWhiteSpace(msg.ModName))
                    return true;

                var modIsInEffectiveLocalCollection = IsModNamePresentInEffectiveLocalCollection(collectionState.Mods, msg.ModName);
                if (!modIsInEffectiveLocalCollection && Logger.IsEnabled(LogLevel.Trace))
                {
                    Logger.LogTrace("Ignoring collection-less Penumbra mod-setting change for {modName}; it is not present in the local player's effective collection {localCollectionId}",
                        msg.ModName, localCollectionId);
                }

                return modIsInEffectiveLocalCollection;
            }

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

    private static bool IsModNamePresentInEffectiveLocalCollection(Dictionary<string, RavaSync.Interop.Ipc.IpcCallerPenumbra.PenumbraModSettingState> mods, string? changedModName)
    {
        if (mods == null || mods.Count == 0 || string.IsNullOrWhiteSpace(changedModName))
            return false;

        if (mods.ContainsKey(changedModName))
            return true;

        var normalizedChanged = NormalizeCollectionModKey(changedModName);
        var changedLeaf = NormalizeCollectionModKey(Path.GetFileName(normalizedChanged));

        foreach (var modKey in mods.Keys)
        {
            var normalizedKey = NormalizeCollectionModKey(modKey);
            if (string.Equals(normalizedKey, normalizedChanged, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizeCollectionModKey(Path.GetFileName(normalizedKey)), changedLeaf, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeCollectionModKey(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : value.Replace('\\', '/').Trim().Trim('/');

    private static bool IsPlayerAppearanceSignalReason(string? reason)
    {
        return IsFastPlayerAppearanceReason(reason);
    }

    private static bool IsTransientResourceChangedReason(string? reason)
    {
        return string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || string.Equals(reason, "ImmediateFollowUp:GameObject:TransientResourceChanged", StringComparison.Ordinal);
    }

    private static TimeSpan GetImmediatePlayerPublishCoalesceDelay(string? reason)
    {
        if (IsTransientManifestRefreshReason(reason))
            return TransientManifestImmediateCoalesceDelay;

        return IsTransientResourceChangedReason(reason)
            ? TransientResourceImmediateCoalesceDelay
            : ImmediatePlayerPublishCoalesceDelay;
    }

    private static TimeSpan GetImmediatePlayerPublishCooldown(string? reason)
    {
        if (IsTransientManifestRefreshReason(reason))
            return TransientManifestImmediateCooldown;

        return IsTransientResourceChangedReason(reason)
            ? TransientResourceImmediateCooldown
            : ImmediatePlayerPublishCooldown;
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
            || reason.StartsWith("ReduceMySize:", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraEndRedraw", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraRedraw", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }

    private static bool ShouldUseImmediatePlayerPublishOnly(string? reason)
    {
        return IsFastPlayerAppearanceReason(reason);
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

    private void QueueFileChangedPlayerPublish(GameObjectHandler playerHandler, PenumbraFileCacheChangedMessage msg)
    {
        var paths = msg.Paths?
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray() ?? [];

        var reason = string.IsNullOrWhiteSpace(msg.Reason) ? "PenumbraFileCacheChanged" : msg.Reason;
        var forceOutbound = msg.ForceLocalPlayerPublish;

        _ = RunLinuxFriendlyBackgroundWork(async () =>
        {
            try
            {
                if (paths.Length > 0)
                    await _fileCacheManager.RefreshFileCachesByPathsAsync(paths, _runtimeCts.Token, writeCsv: forceOutbound).ConfigureAwait(false);

                if (_isZoning || _haltCharaDataCreation || playerHandler.Address == IntPtr.Zero)
                    return;

                QueueImmediatePlayerPublish(playerHandler, reason, forceOutbound: forceOutbound, bypassCoalesce: forceOutbound);
            }
            catch (OperationCanceledException)
            {
                // Plugin shutdown or superseded rebuild.
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Failed to refresh touched file-cache paths before local player publish for {reason}", reason);

                if (forceOutbound && !_isZoning && !_haltCharaDataCreation && playerHandler.Address != IntPtr.Zero)
                    QueueImmediatePlayerPublish(playerHandler, reason, forceOutbound: true, bypassCoalesce: true);
            }
        }, _runtimeCts.Token);
    }

    private static TimeSpan GetDebounceDelay(ObjectKind kind, string? reason)
    {
        if (SyncStorm.IsActive)
            return kind == ObjectKind.Player ? TimeSpan.FromMilliseconds(2500) : OwnedObjectStormBuildDelay;

        if (kind != ObjectKind.Player)
            return OwnedObjectBuildDelay;

        if (IsFastPlayerAppearanceReason(reason))
            return FastPlayerAppearanceDelay;

        return DefaultBuildDelay;
    }

    private TimeSpan GetBuildDelay(IReadOnlyCollection<ObjectKind> objectKinds)
    {
        if (!objectKinds.Contains(ObjectKind.Player))
            return SyncStorm.IsActive ? OwnedObjectStormBuildDelay : OwnedObjectBuildDelay;

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

        _ = RunLinuxFriendlyBackgroundWork(async () =>
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

    private void QueueImmediatePlayerPublish(GameObjectHandler objectToCreateFor, string reason, bool forceOutbound = false, bool bypassCoalesce = false)
    {
        if (objectToCreateFor.ObjectKind != ObjectKind.Player)
            return;

        if (ShouldSkipObservedTransientPlayerBuild(reason))
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Ignoring player transient-only immediate publish request for {reason}; transient manifest state is already authoritative", reason);

            return;
        }

        NotePlayerAppearanceSignal();

        if (!bypassCoalesce && TryGetImmediatePlayerPublishSettleDelay(reason, out var settleDelay))
        {
            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, reason, settleDelay);
            return;
        }

        if (!bypassCoalesce && IsFastPlayerAppearanceReason(reason))
        {
            TimeSpan delay;
            lock (_cacheCreateLockObj)
            {
                var immediatePublishInFlight = _immediatePublishLock.CurrentCount == 0;
                if (immediatePublishInFlight)
                {
                    if (IsTransientManifestRefreshReason(reason))
                    {
                        _pendingImmediatePlayerFollowUp = true;
                        _pendingImmediatePlayerFollowUpForceOutbound |= forceOutbound;
                        _pendingImmediatePlayerFollowUpReason = reason;
                        return;
                    }

                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("Ignoring fast player publish follow-up for {reason} because an immediate player publish is already in flight", reason);

                    return;
                }

                var cooldown = GetImmediatePlayerPublishCooldown(reason);
                var coalesceDelay = GetImmediatePlayerPublishCoalesceDelay(reason);

                var timeSinceLastImmediatePublish = DateTime.UtcNow - _lastImmediatePlayerPublishUtc;
                delay = timeSinceLastImmediatePublish <= cooldown
                    ? cooldown - timeSinceLastImmediatePublish
                    : coalesceDelay;
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
                _pendingImmediatePlayerFollowUpForceOutbound |= forceOutbound;
                _pendingImmediatePlayerFollowUpReason = reason;
                return;
            }

            if (!bypassCoalesce && withinImmediateCooldown)
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

        _ = RunLinuxFriendlyBackgroundWork(async () =>
        {
            try
            {
                await PublishPlayerStateImmediatelyAsync(objectToCreateFor, reason, token, forceOutbound).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }, token);
    }

    private void QueueTransientManifestDeltaPublish(TransientManifestDeltaPublishMessage msg)
    {
        _ = RunLinuxFriendlyBackgroundWork(async () =>
        {
            await _immediatePublishLock.WaitAsync(_runtimeCts.Token).ConfigureAwait(false);
            try
            {
                if (_isZoning || _haltCharaDataCreation)
                    return;

                var changed = ApplyTransientManifestDeltaToCurrentPlayerData(msg);
                if (!changed)
                    return;

                lock (_cacheCreateLockObj)
                {
                    _cachesToCreate.Remove(ObjectKind.Player);
                    _debouncedObjectCache.Remove(ObjectKind.Player);
                    _debouncedReasons.Remove(ObjectKind.Player);
                    _activeReasons.Remove(ObjectKind.Player);
                    _pendingImmediatePlayerFollowUp = false;
                    _pendingImmediatePlayerFollowUpForceOutbound = false;
                    _pendingImmediatePlayerFollowUpReason = null;
                }

                Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI(), false, msg.Reason));

                lock (_cacheCreateLockObj)
                {
                    _lastImmediatePlayerPublishUtc = DateTime.UtcNow;
                    _lastTransientManifestDeltaPublishUtc = _lastImmediatePlayerPublishUtc;
                    var suppressUntil = _lastImmediatePlayerPublishUtc + ImmediatePlayerPublishSettleWindow;
                    if (suppressUntil > _suppressFastPlayerBuildsUntilUtc)
                        _suppressFastPlayerBuildsUntilUtc = suppressUntil;
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to apply targeted transient manifest delta publish for reason {reason}", msg.Reason);
            }
            finally
            {
                _immediatePublishLock.Release();
            }
        }, _runtimeCts.Token);
    }

    private bool ApplyTransientManifestDeltaToCurrentPlayerData(TransientManifestDeltaPublishMessage msg)
    {
        var touchedGamePaths = new HashSet<string>(msg.RemoveGamePaths ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
        foreach (var replacementSet in msg.AddOrUpdateReplacementsByKind.Values)
        {
            foreach (var replacement in replacementSet)
            {
                foreach (var gamePath in replacement.GamePaths)
                    touchedGamePaths.Add(gamePath);
            }
        }

        var changed = false;
        if (touchedGamePaths.Count > 0)
        {
            foreach (var kind in _playerData.FileReplacements.Keys.ToArray())
            {
                if (RemoveGamePathsFromReplacementSet(_playerData.FileReplacements[kind], touchedGamePaths))
                    changed = true;

                if (_playerData.FileReplacements.TryGetValue(kind, out var remaining) && remaining.Count == 0)
                    _playerData.FileReplacements.Remove(kind);
            }
        }

        foreach (var kvp in msg.AddOrUpdateReplacementsByKind)
        {
            if (!_playerData.FileReplacements.TryGetValue(kvp.Key, out var replacements) || replacements == null)
            {
                replacements = new HashSet<FileReplacement>(FileReplacementComparer.Instance);
                _playerData.FileReplacements[kvp.Key] = replacements;
            }

            foreach (var replacement in kvp.Value)
            {
                var clone = CloneFileReplacement(replacement);
                if (replacements.Add(clone))
                    changed = true;
            }
        }

        return changed;
    }

    private static bool RemoveGamePathsFromReplacementSet(HashSet<FileReplacement> replacements, HashSet<string> gamePathsToRemove)
    {
        if (replacements.Count == 0 || gamePathsToRemove.Count == 0)
            return false;

        var changed = false;
        var next = new HashSet<FileReplacement>(FileReplacementComparer.Instance);
        foreach (var replacement in replacements)
        {
            var remainingGamePaths = replacement.GamePaths
                .Where(path => !gamePathsToRemove.Contains(path))
                .ToArray();

            if (remainingGamePaths.Length == replacement.GamePaths.Count)
            {
                next.Add(replacement);
                continue;
            }

            changed = true;
            if (remainingGamePaths.Length > 0)
                next.Add(new FileReplacement(remainingGamePaths, replacement.ResolvedPath) { Hash = replacement.Hash });
        }

        if (!changed)
            return false;

        replacements.Clear();
        foreach (var replacement in next)
            replacements.Add(replacement);

        return true;
    }

    private static FileReplacement CloneFileReplacement(FileReplacement replacement)
    {
        return new FileReplacement(replacement.GamePaths.ToArray(), replacement.ResolvedPath) { Hash = replacement.Hash };
    }

    private async Task PublishPlayerStateImmediatelyAsync(GameObjectHandler objectToCreateFor, string reason, CancellationToken token, bool forceOutbound = false)
    {
        if (objectToCreateFor.ObjectKind != ObjectKind.Player)
            return;

        if (string.Equals(reason, "PenumbraModSettingChanged:PlayerState", StringComparison.Ordinal)
            && DateTime.UtcNow - _lastTransientManifestDeltaPublishUtc <= ImmediatePlayerPublishSettleWindow)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Suppressing generic Penumbra player-state publish because a targeted transient manifest delta was just published");

            return;
        }

        NotePlayerAppearanceSignal();

        var queueFollowUpBuild = false;
        var publishedPlayerState = false;
        var followUpForceOutbound = false;
        string followUpReason = reason;

        await _immediatePublishLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (ShouldSkipObservedTransientPlayerBuild(reason))
                return;

            var fragment = await BuildLocalPlayerFragmentAsync(objectToCreateFor, reason, token).ConfigureAwait(false);
            var hasChanges = HasFragmentStateChanged(ObjectKind.Player, fragment);
            _playerData.SetFragment(ObjectKind.Player, fragment);
            SyncObservedSupportState(ObjectKind.Player, fragment);

            lock (_cacheCreateLockObj)
            {
                _cachesToCreate.Remove(ObjectKind.Player);
                _debouncedObjectCache.Remove(ObjectKind.Player);
                _debouncedReasons.Remove(ObjectKind.Player);

                if (!_currentlyCreating.Contains(ObjectKind.Player))
                    _activeReasons.Remove(ObjectKind.Player);
            }

            if (!hasChanges && !forceOutbound)
                return;

            var effectiveForceOutbound = forceOutbound;

            if (!hasChanges && forceOutbound && Logger.IsEnabled(LogLevel.Debug))
                Logger.LogDebug("Force-publishing unchanged local player state for reason {reason}", reason);

            Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI(), effectiveForceOutbound, reason));
            publishedPlayerState = true;
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
                if (publishedPlayerState)
                {
                    _lastImmediatePlayerPublishUtc = DateTime.UtcNow;
                    _lastTransientManifestDeltaPublishUtc = _lastImmediatePlayerPublishUtc;
                    var suppressUntil = _lastImmediatePlayerPublishUtc + ImmediatePlayerPublishSettleWindow;
                    if (suppressUntil > _suppressFastPlayerBuildsUntilUtc)
                        _suppressFastPlayerBuildsUntilUtc = suppressUntil;
                }

                queueFollowUpBuild = _pendingImmediatePlayerFollowUp;
                followUpForceOutbound = _pendingImmediatePlayerFollowUpForceOutbound;
                if (!string.IsNullOrWhiteSpace(_pendingImmediatePlayerFollowUpReason))
                    followUpReason = _pendingImmediatePlayerFollowUpReason!;

                _pendingImmediatePlayerFollowUp = false;
                _pendingImmediatePlayerFollowUpForceOutbound = false;
                _pendingImmediatePlayerFollowUpReason = null;
            }

            _immediatePublishLock.Release();
        }

        if (queueFollowUpBuild && !_isZoning && !_haltCharaDataCreation)
        {
            if (followUpForceOutbound)
            {
                QueueImmediatePlayerPublish(objectToCreateFor, followUpReason, forceOutbound: true, bypassCoalesce: true);
                return;
            }

            if (IsTransientManifestRefreshReason(followUpReason))
            {
                QueueImmediatePlayerPublish(objectToCreateFor, followUpReason, forceOutbound: followUpForceOutbound, bypassCoalesce: true);
                return;
            }

            if (IsFastPlayerAppearanceReason(followUpReason))
            {
                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("Suppressing fast immediate player follow-up for {reason}; the coalesced publish already captured the settled state", followUpReason);

                return;
            }

            var followUpReasonText = $"ImmediateFollowUp:{followUpReason}";
            ScheduleCoalescedImmediatePlayerPublish(objectToCreateFor, followUpReasonText, GetImmediatePlayerPublishCooldown(followUpReasonText));
        }
    }

    private void AddCacheToCreate(ObjectKind kind = ObjectKind.Player, string reason = "Unspecified")
    {
        if (kind == ObjectKind.Player && ShouldSkipObservedTransientPlayerBuild(reason))
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("Ignoring player transient-only cache rebuild request for {reason}; transient manifest state is already authoritative", reason);

            return;
        }

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

    private async Task<CharacterDataFragmentPlayer?> BuildLocalPlayerFragmentAsync(GameObjectHandler playerHandler, string? reason, CancellationToken token)
    {
        // Pure transient resource events are intentionally skipped before reaching this method.
        // A targeted transient-manifest refresh only changes the known transient file set, so reuse
        // the last verified static player snapshot when possible instead of re-querying Penumbra for
        // the whole local player appearance.
        if (IsTransientManifestOnlyPlayerReasonSet(reason) && TryCreatePlayerFragmentShellFromCurrentData(out var transientManifestShell))
        {
            if (await _characterDataFactory.TryBuildPlayerTransientOnlyFragmentFromSnapshotAsync(transientManifestShell, reason, token).ConfigureAwait(false))
                return transientManifestShell;
        }

        return await _characterDataFactory.BuildCharacterData(playerHandler, token, reason).ConfigureAwait(false) as CharacterDataFragmentPlayer;
    }

    private bool TryCreatePlayerFragmentShellFromCurrentData(out CharacterDataFragmentPlayer fragment)
    {
        fragment = new CharacterDataFragmentPlayer
        {
            CustomizePlusScale = _playerData.CustomizePlusScale.TryGetValue(ObjectKind.Player, out var customize) ? customize : string.Empty,
            GlamourerString = _playerData.GlamourerString.TryGetValue(ObjectKind.Player, out var glamourer) ? glamourer : string.Empty,
            ManipulationString = _playerData.ManipulationString,
            HeelsData = _playerData.HeelsData,
            HonorificData = _playerData.HonorificData,
            MoodlesData = _playerData.MoodlesData,
            PetNamesData = _playerData.PetNamesData,
        };

        return _playerData.FileReplacements.ContainsKey(ObjectKind.Player);
    }

    private static bool IsPureTransientOnlyPlayerReasonSet(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        var reasons = reason.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return reasons.Length > 0 && reasons.All(IsTransientResourceChangedReason);
    }

    private static bool IsTransientManifestOnlyPlayerReasonSet(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        var reasons = reason.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return reasons.Length > 0 && reasons.All(IsTransientManifestRefreshReason);
    }

    private static bool IsTransientManifestRefreshReason(string reason)
    {
        return string.Equals(reason, "PenumbraModSettingChanged:TransientManifest", StringComparison.Ordinal)
            || string.Equals(reason, "StartupTransientManifestPrime:TransientManifest", StringComparison.Ordinal);
    }

    private bool ShouldSkipObservedTransientPlayerBuild(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        return ShouldSkipObservedTransientPlayerBuild(new[] { reason });
    }

    private bool ShouldSkipObservedTransientPlayerBuild(IReadOnlyCollection<string> reasons)
    {
        if (reasons == null || reasons.Count == 0)
            return false;

        return reasons.All(IsTransientResourceChangedReason);
    }





    private void SyncObservedSupportState(ObjectKind objectKind, CharacterDataFragment? fragment)
    {
        // Transient support state is intentionally derived from owned observed resources only.
    }

    private bool HasFragmentStateChanged(ObjectKind objectKind, CharacterDataFragment? fragment)
    {
        _playerData.FileReplacements.TryGetValue(objectKind, out var currentReplacements);
        var nextReplacements = fragment?.FileReplacements ?? new HashSet<FileReplacement>(FileReplacementComparer.Instance);
        if (!AreFileReplacementSetsEqual(currentReplacements, nextReplacements))
            return true;

        _playerData.CustomizePlusScale.TryGetValue(objectKind, out var currentCustomize);
        var nextCustomize = fragment?.CustomizePlusScale ?? string.Empty;
        if (!string.Equals(currentCustomize ?? string.Empty, nextCustomize, StringComparison.Ordinal))
            return true;

        _playerData.GlamourerString.TryGetValue(objectKind, out var currentGlamourer);
        var nextGlamourer = fragment?.GlamourerString ?? string.Empty;
        if (!string.Equals(currentGlamourer ?? string.Empty, nextGlamourer, StringComparison.Ordinal))
            return true;

        if (objectKind != ObjectKind.Player)
            return false;

        var playerFragment = fragment as CharacterDataFragmentPlayer;
        if (!string.Equals(_playerData.ManipulationString, playerFragment?.ManipulationString ?? string.Empty, StringComparison.Ordinal))
            return true;
        if (!string.Equals(_playerData.HeelsData, playerFragment?.HeelsData ?? string.Empty, StringComparison.Ordinal))
            return true;
        if (!string.Equals(_playerData.HonorificData, playerFragment?.HonorificData ?? string.Empty, StringComparison.Ordinal))
            return true;
        if (!string.Equals(_playerData.MoodlesData, playerFragment?.MoodlesData ?? string.Empty, StringComparison.Ordinal))
            return true;
        if (!string.Equals(_playerData.PetNamesData, playerFragment?.PetNamesData ?? string.Empty, StringComparison.Ordinal))
            return true;

        return false;
    }

    private static bool AreFileReplacementSetsEqual(HashSet<FileReplacement>? currentReplacements, HashSet<FileReplacement> newReplacements)
    {
        if ((currentReplacements?.Count ?? 0) != newReplacements.Count)
            return false;

        if (currentReplacements == null)
            return newReplacements.Count == 0;

        return currentReplacements.SetEquals(newReplacements);
    }


    private bool ShouldPreserveInactiveOwnedObjectFragment(ObjectKind objectKind)
    {
        if (objectKind is not (ObjectKind.MinionOrMount or ObjectKind.Companion))
            return false;

        return HasMeaningfulOwnedObjectPayload(objectKind);
    }

    private bool HasMeaningfulOwnedObjectPayload(ObjectKind objectKind)
    {
        if (_playerData.FileReplacements.TryGetValue(objectKind, out var replacements)
            && replacements.Any(static replacement => replacement.HasFileReplacement || replacement.IsFileSwap))
        {
            return true;
        }

        if (_playerData.CustomizePlusScale.TryGetValue(objectKind, out var customizeScale)
            && !string.IsNullOrWhiteSpace(customizeScale))
        {
            return true;
        }

        if (_playerData.GlamourerString.TryGetValue(objectKind, out var glamourerString)
            && !string.IsNullOrWhiteSpace(glamourerString))
        {
            return true;
        }

        return false;
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

        _ = RunLinuxFriendlyBackgroundWork(async () =>
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

                async Task<(bool ShouldApply, CharacterDataFragment? Fragment, HashSet<string> ReasonSet)> BuildOneAsync(ObjectKind objectKind)
                {
                    HashSet<string> reasonSet;
                    lock (_cacheCreateLockObj)
                    {
                        reasonSet = _activeReasons.TryGetValue(objectKind, out var reasons) && reasons.Count > 0
                            ? [.. reasons]
                            : [];
                    }

                    if (ShouldSkipPenumbraTransientOnlyBuild(objectKind, reasonSet))
                        return (false, null, reasonSet);

                    if (objectKind == ObjectKind.Player && ShouldSkipObservedTransientPlayerBuild(reasonSet))
                        return (false, null, reasonSet);

                    if (objectKind == ObjectKind.Player
                        && IsPureFastPlayerAppearanceReasonSet(reasonSet)
                        && (HasRecentImmediatePlayerPublish(ImmediatePlayerPublishSettleWindow)
                            || reasonSet.Any(ShouldSuppressFastPlayerBuildNow)))
                    {
                        return (false, null, reasonSet);
                    }

                    var handler = _playerRelatedObjects[objectKind];
                    if (objectKind != ObjectKind.Player
                        && (handler.Address == IntPtr.Zero
                            || handler.CurrentDrawCondition is GameObjectHandler.DrawCondition.ObjectZero or GameObjectHandler.DrawCondition.DrawObjectZero))
                    {
                        if (ShouldPreserveInactiveOwnedObjectFragment(objectKind))
                        {
                            if (Logger.IsEnabled(LogLevel.Trace))
                                Logger.LogTrace("Preserving last known {objectKind} payload while the owned object is not fully spawned yet; avoiding a temporary vanilla/clear publish", objectKind);

                            return (false, null, reasonSet);
                        }

                        return (true, null, reasonSet);
                    }

                    if (objectKind == ObjectKind.Player)
                    {
                        var combinedReason = reasonSet.Count == 0 ? null : string.Join("|", reasonSet);
                        var playerFragment = await BuildLocalPlayerFragmentAsync(handler, combinedReason, linkedCts.Token).ConfigureAwait(false);
                        if (playerFragment == null)
                        {
                            Logger.LogWarning("Skipping local player cache update because the player fragment build returned null; preserving last known self state instead of publishing a partial/vanilla payload");
                            return (false, null, reasonSet);
                        }

                        return (true, playerFragment, reasonSet);
                    }

                    return (true, await _characterDataFactory.BuildCharacterData(handler, linkedCts.Token).ConfigureAwait(false), reasonSet);
                }

                var publishedEarlyPlayer = false;
                var changedKinds = new HashSet<ObjectKind>();
                var playerReasonSet = new HashSet<string>();

                if (objectKindsSnapshot.Count == 1 && objectKindsSnapshot.Contains(ObjectKind.Player))
                {
                    var (shouldApplyPlayer, playerFragment, playerReasons) = await BuildOneAsync(ObjectKind.Player).ConfigureAwait(false);
                    playerReasonSet = playerReasons;

                    if (shouldApplyPlayer)
                    {
                        createdData[ObjectKind.Player] = playerFragment;
                        var hasPlayerChanges = HasFragmentStateChanged(ObjectKind.Player, playerFragment);
                        _playerData.SetFragment(ObjectKind.Player, playerFragment);
                        SyncObservedSupportState(ObjectKind.Player, playerFragment);

                        if (hasPlayerChanges)
                        {
                            changedKinds.Add(ObjectKind.Player);
                            builtAnyData = true;

                            var combinedReason = playerReasonSet.Count == 0 ? string.Empty : string.Join("|", playerReasonSet);
                            var forceOutbound = false;

                            Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI(), forceOutbound, combinedReason));
                            publishedEarlyPlayer = true;
                        }
                    }
                }
                else if (objectKindsSnapshot.Contains(ObjectKind.Player))
                {
                    var (shouldApplyPlayer, playerFragment, playerReasons) = await BuildOneAsync(ObjectKind.Player).ConfigureAwait(false);
                    playerReasonSet = playerReasons;

                    if (shouldApplyPlayer)
                        createdData[ObjectKind.Player] = playerFragment;
                }

                foreach (var objectKind in objectKindsSnapshot)
                {
                    if (objectKind == ObjectKind.Player)
                        continue;

                    var (shouldApplyObject, objectFragment, _) = await BuildOneAsync(objectKind).ConfigureAwait(false);
                    if (shouldApplyObject)
                        createdData[objectKind] = objectFragment;
                }

                foreach (var kvp in createdData)
                {
                    if (kvp.Key == ObjectKind.Player && publishedEarlyPlayer)
                        continue;

                    var hasChanges = HasFragmentStateChanged(kvp.Key, kvp.Value);
                    _playerData.SetFragment(kvp.Key, kvp.Value);
                    SyncObservedSupportState(kvp.Key, kvp.Value);

                    if (hasChanges)
                        changedKinds.Add(kvp.Key);
                }

                if (changedKinds.Count > 0 && !publishedEarlyPlayer)
                {
                    builtAnyData = true;

                    var combinedReason = playerReasonSet.Count == 0 ? string.Empty : string.Join("|", playerReasonSet);
                    var forceOutbound = false;

                    Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI(), forceOutbound, combinedReason));
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