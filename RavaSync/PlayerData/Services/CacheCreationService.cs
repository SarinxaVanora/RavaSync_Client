using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;

namespace RavaSync.PlayerData.Services;

public sealed class CacheCreationService : DisposableMediatorSubscriberBase
{
    private readonly object _cacheCreateLockObj = new();
    private readonly HashSet<ObjectKind> _cachesToCreate = [];
    private readonly PlayerDataFactory _characterDataFactory;
    private readonly HashSet<ObjectKind> _currentlyCreating = [];
    private readonly HashSet<ObjectKind> _debouncedObjectCache = [];
    private readonly Dictionary<ObjectKind, HashSet<string>> _debouncedReasons = [];
    private readonly Dictionary<ObjectKind, HashSet<string>> _activeReasons = [];
    private readonly CharacterData _playerData = new();
    private readonly Dictionary<ObjectKind, GameObjectHandler> _playerRelatedObjects = [];
    private readonly CancellationTokenSource _runtimeCts = new();
    private CancellationTokenSource _creationCts = new();
    private CancellationTokenSource _debounceCts = new();
    private bool _haltCharaDataCreation;
    private bool _isZoning = false;
    private readonly DateTime _serviceStartUtc = DateTime.UtcNow;
    private DateTime _lastPlayerAppearanceSignalUtc = DateTime.MinValue;
    private static readonly TimeSpan PenumbraTransientFollowWindow = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan InitialPenumbraTransientSettleWindow = TimeSpan.FromSeconds(10);

    public CacheCreationService(ILogger<CacheCreationService> logger, MareMediator mediator, GameObjectHandlerFactory gameObjectHandlerFactory,
        PlayerDataFactory characterDataFactory, DalamudUtilService dalamudUtil) : base(logger, mediator)
    {
        _characterDataFactory = characterDataFactory;

        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (msg) => _isZoning = true);
        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (msg) => _isZoning = false);

        Mediator.Subscribe<HaltCharaDataCreation>(this, (msg) =>
        {
            _haltCharaDataCreation = !msg.Resume;
        });

        Mediator.Subscribe<CreateCacheForObjectMessage>(this, (msg) =>
        {
            if (msg.ObjectToCreateFor.ObjectKind == ObjectKind.Player && IsPlayerAppearanceSignalReason(msg.Reason))
            {
                NotePlayerAppearanceSignal();
            }
            AddCacheToCreate(msg.ObjectToCreateFor.ObjectKind, msg.Reason);
        });

        _playerRelatedObjects[ObjectKind.Player] = gameObjectHandlerFactory.Create(ObjectKind.Player, dalamudUtil.GetPlayerPtr, isWatched: true)
            .GetAwaiter().GetResult();
        _playerRelatedObjects[ObjectKind.MinionOrMount] = gameObjectHandlerFactory.Create(ObjectKind.MinionOrMount, () => dalamudUtil.GetMinionOrMountPtr(), isWatched: true)
            .GetAwaiter().GetResult();
        _playerRelatedObjects[ObjectKind.Pet] = gameObjectHandlerFactory.Create(ObjectKind.Pet, () => dalamudUtil.GetPetPtr(), isWatched: true)
            .GetAwaiter().GetResult();
        _playerRelatedObjects[ObjectKind.Companion] = gameObjectHandlerFactory.Create(ObjectKind.Companion, () => dalamudUtil.GetCompanionPtr(), isWatched: true)
            .GetAwaiter().GetResult();

        Mediator.Subscribe<ClassJobChangedMessage>(this, (msg) =>
        {
            if (msg.GameObjectHandler == _playerRelatedObjects[ObjectKind.Player])
            {
                NotePlayerAppearanceSignal();
                AddCacheToCreate(ObjectKind.Player, "ClassJobChanged:Player");
                AddCacheToCreate(ObjectKind.Pet, "ClassJobChanged:Pet");
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
                }
                AddCacheToCreate(item, $"CustomizePlus:{item}");
            }
        });

        Mediator.Subscribe<HeelsOffsetMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            AddCacheToCreate(ObjectKind.Player, "HeelsOffset");
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
                }
                AddCacheToCreate(changedType.Key, $"Glamourer:{changedType.Key}");
            }
        });

        Mediator.Subscribe<HonorificMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            if (!string.Equals(msg.NewHonorificTitle, _playerData.HonorificData, StringComparison.Ordinal))
            {
                AddCacheToCreate(ObjectKind.Player, "HonorificChanged");
            }
        });

        Mediator.Subscribe<MoodlesMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            var changedType = _playerRelatedObjects.FirstOrDefault(f => f.Value.Address == msg.Address);
            if (!default(KeyValuePair<ObjectKind, GameObjectHandler>).Equals(changedType) && changedType.Key == ObjectKind.Player)
            {
                AddCacheToCreate(ObjectKind.Player, "MoodlesChanged");
            }
        });

        Mediator.Subscribe<PetNamesMessage>(this, (msg) =>
        {
            if (_isZoning) return;
            if (!string.Equals(msg.PetNicknamesData, _playerData.PetNamesData, StringComparison.Ordinal))
            {
                AddCacheToCreate(ObjectKind.Player, "PetNamesChanged");
            }
        });

        Mediator.Subscribe<PenumbraModSettingChangedMessage>(this, (msg) =>
        {
            AddCacheToCreate(ObjectKind.Player, "PenumbraModSettingChanged");
            AddCacheToCreate(ObjectKind.Pet, "PenumbraModSettingChanged");
            AddCacheToCreate(ObjectKind.MinionOrMount, "PenumbraModSettingChanged");
            AddCacheToCreate(ObjectKind.Companion, "PenumbraModSettingChanged");
        });

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (msg) => ProcessCacheCreation());
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _playerRelatedObjects.Values.ToList().ForEach(p => p.Dispose());
        _runtimeCts.Cancel();
        _runtimeCts.Dispose();
        _creationCts.Cancel();
        _creationCts.Dispose();
    }

    private void NotePlayerAppearanceSignal()
    {
        _lastPlayerAppearanceSignalUtc = DateTime.UtcNow;
    }

    private bool IsWithinPenumbraTransientFollowWindow()
    {
        return DateTime.UtcNow - _lastPlayerAppearanceSignalUtc <= PenumbraTransientFollowWindow;
    }

    private bool IsWithinInitialPenumbraTransientSettleWindow()
    {
        return DateTime.UtcNow - _serviceStartUtc <= InitialPenumbraTransientSettleWindow;
    }

    private static bool IsPlayerAppearanceSignalReason(string? reason)
    {
        if (string.IsNullOrEmpty(reason)) return false;

        return reason.StartsWith("GameObject:SemanticDiff", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("Glamourer:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }

    private static bool IsPurePenumbraTransientCombo(IReadOnlyCollection<string> reasons)
    {
        if (reasons.Count == 0) return false;

        bool hasPenumbra = false;
        bool hasTransient = false;

        foreach (var reason in reasons)
        {
            if (string.Equals(reason, "PenumbraModSettingChanged", StringComparison.Ordinal))
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

    private bool ShouldSkipPenumbraTransientOnlyBuild(IReadOnlyCollection<string> reasons)
    {
        if (!IsPurePenumbraTransientCombo(reasons))
            return false;

        if (IsWithinInitialPenumbraTransientSettleWindow())
            return false;

        if (IsWithinPenumbraTransientFollowWindow())
            return false;

        return true;
    }

    private void AddCacheToCreate(ObjectKind kind = ObjectKind.Player, string reason = "Unspecified")
    {
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
                await Task.Delay(TimeSpan.FromSeconds(1), token).ConfigureAwait(false);

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

    private void ProcessCacheCreation()
    {
        if (_isZoning || _haltCharaDataCreation) return;

        if (_cachesToCreate.Count == 0) return;

        if (_playerRelatedObjects.Any(p => p.Value.CurrentDrawCondition is
            not (GameObjectHandler.DrawCondition.None or GameObjectHandler.DrawCondition.DrawObjectZero or GameObjectHandler.DrawCondition.ObjectZero)))
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

            await Task.Delay(TimeSpan.FromSeconds(1), linkedCts.Token).ConfigureAwait(false);

            try
            {
                Dictionary<ObjectKind, CharacterDataFragment?> createdData = [];
                foreach (var objectKind in _currentlyCreating)
                {
                    HashSet<string> reasonSet;
                    lock (_cacheCreateLockObj)
                    {
                        reasonSet = _activeReasons.TryGetValue(objectKind, out var reasons) && reasons.Count > 0
                            ? [.. reasons]
                            : [];
                    }

                    if (ShouldSkipPenumbraTransientOnlyBuild(reasonSet))
                    {
                        continue;
                    }

                    createdData[objectKind] = await _characterDataFactory.BuildCharacterData(_playerRelatedObjects[objectKind], linkedCts.Token).ConfigureAwait(false);
                }

                foreach (var kvp in createdData)
                {
                    _playerData.SetFragment(kvp.Key, kvp.Value);
                }

                if (createdData.Count > 0)
                {
                    Mediator.Publish(new CharacterDataCreatedMessage(_playerData.ToAPI()));
                }

                var createdKeys = createdData.Keys.ToList();
                _currentlyCreating.Clear();
                lock (_cacheCreateLockObj)
                {
                    foreach (var key in createdKeys)
                        _activeReasons.Remove(key);
                }
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
                    foreach (var key in _currentlyCreating.ToList())
                        _activeReasons.Remove(key);
                }
            }
        });
    }
}