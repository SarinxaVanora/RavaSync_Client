using FFXIVClientStructs.FFXIV.Client.Game.Character;
using FFXIVClientStructs.FFXIV.Client.Graphics.Scene;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using static FFXIVClientStructs.FFXIV.Client.Game.Character.DrawDataContainer;
using ObjectKind = RavaSync.API.Data.Enum.ObjectKind;

namespace RavaSync.PlayerData.Handlers;

public sealed class GameObjectHandler : DisposableMediatorSubscriberBase, IHighPriorityMediatorSubscriber
{
    private readonly DalamudUtilService _dalamudUtil;
    private readonly Func<IntPtr> _getAddress;
    private readonly bool _isOwnedObject;
    private readonly PerformanceCollectorService _performanceCollector;
    private byte _classJob = 0;
    private Task? _delayedZoningTask;
    private bool _haltProcessing = false;
    private bool _pendingTransientPublishAfterRedraw = false;
    private bool _pendingPlayerPublishAfterRequestedRedraw = false;
    private bool _pendingOwnedObjectStructuralPublishAfterDraw = false;
    private CancellationTokenSource _zoningCts = new();
    private long _nextUpdateTick;
    private bool _redrawPublishIssued;
    private bool _suppressNextSemanticDiffPublishAfterRedraw;
    private static int _nextHandlerPhaseSeed;
    private readonly int _updatePhaseJitterMs;
    private const int OwnedUpdateIntervalMs = 33;   // ~30Hz
    private const int OwnedStableUpdateIntervalMs = 66;
    private const int OtherUpdateIntervalMs = 100;  // remote actors are not local-state sensors
    private const int OtherStableUpdateIntervalMs = 500;
    private const int PostZoneSettleDelayMs = 3000;


    public GameObjectHandler(ILogger<GameObjectHandler> logger, PerformanceCollectorService performanceCollector,
        MareMediator mediator, DalamudUtilService dalamudUtil, ObjectKind objectKind, Func<IntPtr> getAddress, bool ownedObject = true) : base(logger, mediator)
    {
        _performanceCollector = performanceCollector;
        ObjectKind = objectKind;
        _dalamudUtil = dalamudUtil;
        _getAddress = () =>
        {
            _dalamudUtil.EnsureIsOnFramework();
            return getAddress.Invoke();
        };
        _isOwnedObject = ownedObject;
        _updatePhaseJitterMs = (int)((uint)Interlocked.Add(ref _nextHandlerPhaseSeed, 17) % 83);
        Name = string.Empty;

        if (ownedObject)
        {
            Mediator.Subscribe<TransientResourceChangedMessage>(this, (msg) =>
            {
                if (!(_delayedZoningTask?.IsCompleted ?? true)) return;
                if (msg.Address != Address) return;

                // Player transient resource events are already handled by transient.json / manifest refresh now.
                // Rebuilding and publishing the whole player payload here was the source of the expensive
                // "transient-only" builds during animation/VFX playback, and it is not needed for correctness.
                if (ObjectKind == ObjectKind.Player)
                {
                    if (Logger.IsEnabled(LogLevel.Trace))
                        Logger.LogTrace("[{this}] Ignoring player transient-resource build request; transient manifest state is authoritative", this);

                    return;
                }

                Mediator.Publish(new CreateCacheForObjectMessage(this, "GameObject:TransientResourceChanged"));
            });

            if (objectKind == ObjectKind.Player)
            {
                Mediator.Subscribe<ClassJobChangedMessage>(this, (msg) =>
                {
                    if (ReferenceEquals(msg.GameObjectHandler, this))
                        _pendingTransientPublishAfterRedraw = true;
                });

                Mediator.Subscribe<GlamourerChangedMessage>(this, (msg) =>
                {
                    if (msg.Address == Address)
                        _pendingTransientPublishAfterRedraw = true;
                });

                Mediator.Subscribe<CustomizePlusMessage>(this, (msg) =>
                {
                    if (msg.Address == null || msg.Address == Address)
                        _pendingTransientPublishAfterRedraw = true;
                });

            }
        }

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (_) => FrameworkUpdate());

        Mediator.Subscribe<DalamudLogoutMessage>(this, (_) =>
        {
            _haltProcessing = true;
            _pendingTransientPublishAfterRedraw = false;
            _pendingPlayerPublishAfterRequestedRedraw = false;
            _pendingOwnedObjectStructuralPublishAfterDraw = false;
            _redrawPublishIssued = false;
            _suppressNextSemanticDiffPublishAfterRedraw = false;
            Invalidate();
            _nextUpdateTick = 0;
        });

        Mediator.Subscribe<ZoneSwitchEndMessage>(this, (_) => ZoneSwitchEnd());
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) =>
        {
            _pendingTransientPublishAfterRedraw = false;
            _pendingPlayerPublishAfterRequestedRedraw = false;
            _pendingOwnedObjectStructuralPublishAfterDraw = false;
            _redrawPublishIssued = false;
            _suppressNextSemanticDiffPublishAfterRedraw = false;
            ZoneSwitchStart();
        });

        Mediator.Subscribe<CutsceneStartMessage>(this, (_) =>
        {
            _haltProcessing = false; //changed to false in efforts to stop issues with cutscene drawing
            _pendingTransientPublishAfterRedraw = false;
            _pendingPlayerPublishAfterRequestedRedraw = false;
            _pendingOwnedObjectStructuralPublishAfterDraw = false;
            _redrawPublishIssued = false;
            _suppressNextSemanticDiffPublishAfterRedraw = false;
        });
        Mediator.Subscribe<CutsceneEndMessage>(this, (_) =>
        {
            _haltProcessing = false;
            ZoneSwitchEnd();
        });
        Mediator.Subscribe<PenumbraStartRedrawMessage>(this, (msg) =>
        {
            if (msg.Address == Address)
            {
                _haltProcessing = true;
                _redrawPublishIssued = false;
                _suppressNextSemanticDiffPublishAfterRedraw = false;
            }
        });
        Mediator.Subscribe<PenumbraEndRedrawMessage>(this, (msg) =>
        {
            if (msg.Address == Address)
            {
                _haltProcessing = false;
                _nextUpdateTick = 0;

                PublishImmediatePlayerStateAfterRedraw("GameObject:PenumbraEndRedraw", includeRequestedRedrawOnly: false);
            }
        });

        Mediator.Subscribe<PenumbraRedrawMessage>(this, (msg) =>
        {
            if (msg.Address != Address) return;

            PublishImmediatePlayerStateAfterRedraw("GameObject:PenumbraRedraw", includeRequestedRedrawOnly: msg.WasRequested);
        });

        Mediator.Subscribe<ArmRequestedPlayerPublishAfterRedrawMessage>(this, (msg) =>
        {
            if (!_isOwnedObject || ObjectKind != ObjectKind.Player) return;
            if (msg.Address != Address) return;

            _pendingPlayerPublishAfterRequestedRedraw = true;
            _redrawPublishIssued = false;
            _nextUpdateTick = 0;
        });

        Mediator.Publish(new GameObjectHandlerCreatedMessage(this, _isOwnedObject));

        _ = _dalamudUtil.RunOnFrameworkThread(() =>
        {
            try
            {
                CheckAndUpdateObject();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Initial CheckAndUpdateObject failed for {this}", this);
            }
        });
    }

    public enum DrawCondition
    {
        None,
        ObjectZero,
        DrawObjectZero,
        RenderFlags,
        ModelInSlotLoaded,
        ModelFilesInSlotLoaded
    }

    public IntPtr Address { get; private set; }
    public IntPtr DrawObject => DrawObjectAddress;
    public DrawCondition CurrentDrawCondition { get; set; } = DrawCondition.None;
    public byte Gender { get; private set; }
    public string Name { get; private set; }
    public ObjectKind ObjectKind { get; }
    public byte RaceId { get; private set; }
    public byte TribeId { get; private set; }
    private byte[] CustomizeData { get; set; } = new byte[26];
    private IntPtr DrawObjectAddress { get; set; }
    private byte[] EquipSlotData { get; set; } = new byte[40];
    private ushort[] MainHandData { get; set; } = new ushort[3];
    private ushort[] OffHandData { get; set; } = new ushort[3];

    public async Task ActOnFrameworkAfterEnsureNoDrawAsync(Action<Dalamud.Game.ClientState.Objects.Types.ICharacter> act, CancellationToken token)
    {
        while (await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            // If we’re still drawing, keep looping.
            if (CurrentDrawCondition != DrawCondition.None) return true;

            var gameObj = _dalamudUtil.CreateGameObject(Address);
            if (gameObj is Dalamud.Game.ClientState.Objects.Types.ICharacter chara)
            {
                act.Invoke(chara);
            }

            // We’ve invoked the action and can break the loop.
            return false;
        }).ConfigureAwait(false))
        {
            await Task.Delay(50, token).ConfigureAwait(false);
        }
    }

    public void CompareNameAndThrow(string name)
    {
        if (!string.Equals(Name, name, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Player name not equal to requested name, pointer invalid");
        }
        if (Address == IntPtr.Zero)
        {
            throw new InvalidOperationException("Player pointer is zero, pointer invalid");
        }
    }

    public Dalamud.Game.ClientState.Objects.Types.IGameObject? GetGameObject()
    {
        return _dalamudUtil.CreateGameObject(Address);
    }

    public void Invalidate()
    {
        Address = IntPtr.Zero;
        DrawObjectAddress = IntPtr.Zero;
        CurrentDrawCondition = DrawCondition.ObjectZero;
        _pendingOwnedObjectStructuralPublishAfterDraw = false;
    }

    public async Task RefreshStateOnFrameworkAsync()
    {
        await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            try
            {
                CheckAndUpdateObject();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Error refreshing framework state for {this}", this);
            }
        }).ConfigureAwait(false);
    }

    public async Task<bool> IsBeingDrawnRunOnFrameworkAsync()
    {
        return await _dalamudUtil.RunOnFrameworkThread(IsBeingDrawn).ConfigureAwait(false);
    }

    public override string ToString()
    {
        var owned = _isOwnedObject ? "Self" : "Other";
        return $"{owned}/{ObjectKind}:{Name} ({Address:X},{DrawObjectAddress:X})";
    }

    private void PublishImmediatePlayerStateAfterRedraw(string reason, bool includeRequestedRedrawOnly)
    {
        if (!_isOwnedObject || ObjectKind != ObjectKind.Player)
            return;

        if (!(_delayedZoningTask?.IsCompleted ?? true))
            return;

        var hasPendingPublish = _pendingTransientPublishAfterRedraw
            || (includeRequestedRedrawOnly && _pendingPlayerPublishAfterRequestedRedraw);
        if (!hasPendingPublish)
            return;

        if (_redrawPublishIssued)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("[{this}] Suppressing duplicate redraw-triggered immediate player publish for {reason}", this, reason);

            return;
        }

        _redrawPublishIssued = true;
        _suppressNextSemanticDiffPublishAfterRedraw = true;
        _pendingTransientPublishAfterRedraw = false;
        _pendingPlayerPublishAfterRequestedRedraw = false;
        _nextUpdateTick = 0;
        Mediator.Publish(new ImmediatePlayerStatePublishMessage(this, reason));
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        Mediator.Publish(new GameObjectHandlerDestroyedMessage(this, _isOwnedObject));
    }

    private unsafe void CheckAndUpdateObject()
    {
        var prevAddr = Address;
        var prevDrawObj = DrawObjectAddress;

        Address = _getAddress();
        if (Address != IntPtr.Zero)
        {
            var drawObjAddr = (IntPtr)((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)Address)->DrawObject;
            DrawObjectAddress = drawObjAddr;
            CurrentDrawCondition = DrawCondition.None;
        }
        else
        {
            DrawObjectAddress = IntPtr.Zero;
            CurrentDrawCondition = DrawCondition.DrawObjectZero;
        }

        CurrentDrawCondition = IsBeingDrawnUnsafe();

        if (_haltProcessing) return;

        bool drawObjDiff = DrawObjectAddress != prevDrawObj;
        bool addrDiff = Address != prevAddr;

        if (Address != IntPtr.Zero && DrawObjectAddress != IntPtr.Zero)
        {
            var chara = (Character*)Address;
            var name = chara->GameObject.NameString;
            bool nameChange = !string.Equals(name, Name, StringComparison.Ordinal);
            if (nameChange)
            {
                Name = name;
            }

            // Remote/receiver handlers only need a valid actor pointer, name, draw object
            // and draw-state gate. They do not own local outbound state, so polling heavy
            // Human draw data (equipment/customize bytes) for every visible remote pair is
            // pure idle pressure and scales badly in modded rooms.
            if (!_isOwnedObject)
                return;

            if (ObjectKind != ObjectKind.Player && CurrentDrawCondition != DrawCondition.None)
            {
                if (addrDiff || drawObjDiff || nameChange)
                    _pendingOwnedObjectStructuralPublishAfterDraw = true;

                if (Logger.IsEnabled(LogLevel.Trace))
                    Logger.LogTrace("[{this}] Owned {objectKind} is still drawing ({condition}); deferring structural cache publish and skipping deep draw-data polling", this, ObjectKind, CurrentDrawCondition);

                return;
            }

            bool equipDiff = false;

            if (((DrawObject*)DrawObjectAddress)->Object.GetObjectType() == ObjectType.CharacterBase
                && ((CharacterBase*)DrawObjectAddress)->GetModelType() == CharacterBase.ModelType.Human)
            {
                var classJob = chara->CharacterData.ClassJob;
                if (classJob != _classJob)
                {
                    Logger.LogTrace("[{this}] classjob changed from {old} to {new}", this, _classJob, classJob);
                    _classJob = classJob;
                    Mediator.Publish(new ClassJobChangedMessage(this));
                }

                equipDiff = CompareAndUpdateEquipByteData((byte*)&((Human*)DrawObjectAddress)->Head);

                ref var mh = ref chara->DrawData.Weapon(WeaponSlot.MainHand);
                ref var oh = ref chara->DrawData.Weapon(WeaponSlot.OffHand);
                equipDiff |= CompareAndUpdateMainHand((Weapon*)mh.DrawObject);
                equipDiff |= CompareAndUpdateOffHand((Weapon*)oh.DrawObject);

                if (equipDiff)
                    Logger.LogTrace("Checking [{this}] equip data as human from draw obj, result: {diff}", this, equipDiff);
            }
            else
            {
                equipDiff = CompareAndUpdateEquipByteData((byte*)Unsafe.AsPointer(ref chara->DrawData.EquipmentModelIds[0]));
                if (equipDiff)
                    Logger.LogTrace("Checking [{this}] equip data from game obj, result: {diff}", this, equipDiff);
            }

            if (equipDiff && !_isOwnedObject)
            {
                Logger.LogTrace("[{this}] Changed", this);
                return;
            }

            bool customizeDiff = false;

            if (((DrawObject*)DrawObjectAddress)->Object.GetObjectType() == ObjectType.CharacterBase
                && ((CharacterBase*)DrawObjectAddress)->GetModelType() == CharacterBase.ModelType.Human)
            {
                var gender = ((Human*)DrawObjectAddress)->Customize.Sex;
                var raceId = ((Human*)DrawObjectAddress)->Customize.Race;
                var tribeId = ((Human*)DrawObjectAddress)->Customize.Tribe;

                if (_isOwnedObject && ObjectKind == ObjectKind.Player
                    && (gender != Gender || raceId != RaceId || tribeId != TribeId))
                {
                    Mediator.Publish(new CensusUpdateMessage(gender, raceId, tribeId));
                    Gender = gender;
                    RaceId = raceId;
                    TribeId = tribeId;
                }

                customizeDiff = CompareAndUpdateCustomizeData(((Human*)DrawObjectAddress)->Customize.Data);
                if (customizeDiff)
                    Logger.LogTrace("Checking [{this}] customize data as human from draw obj, result: {diff}", this, customizeDiff);
            }
            else
            {
                customizeDiff = CompareAndUpdateCustomizeData(chara->DrawData.CustomizeData.Data);
                if (customizeDiff)
                    Logger.LogTrace("Checking [{this}] customize data from game obj, result: {diff}", this, equipDiff);
            }

            if (_isOwnedObject)
            {
                var semanticDiff = equipDiff || customizeDiff || nameChange;
                var pendingOwnedObjectStructuralPublish = ObjectKind != ObjectKind.Player && _pendingOwnedObjectStructuralPublishAfterDraw;
                var nonPlayerStructuralDiff = ObjectKind != ObjectKind.Player && (addrDiff || drawObjDiff || pendingOwnedObjectStructuralPublish);

                if (semanticDiff || nonPlayerStructuralDiff)
                {
                    if (ObjectKind == ObjectKind.Player && (equipDiff || customizeDiff))
                    {
                        _pendingTransientPublishAfterRedraw = true;

                        if (_suppressNextSemanticDiffPublishAfterRedraw)
                        {
                            _suppressNextSemanticDiffPublishAfterRedraw = false;

                            if (Logger.IsEnabled(LogLevel.Trace))
                                Logger.LogTrace("[{this}] Suppressing next post-redraw semantic diff publish", this);

                            return;
                        }
                    }

                    var reason = semanticDiff
                        ? $"GameObject:SemanticDiff(equip={equipDiff},customize={customizeDiff},name={nameChange})"
                        : $"GameObject:StructuralDiff(addr={addrDiff},draw={drawObjDiff})";

                    if (pendingOwnedObjectStructuralPublish)
                        _pendingOwnedObjectStructuralPublishAfterDraw = false;

                    Logger.LogDebug("[{this}] Changed, Sending CreateCacheObjectMessage ({reason})", this, reason);
                    Mediator.Publish(new CreateCacheForObjectMessage(this, reason));
                }
                else if (addrDiff || drawObjDiff)
                {
                    Logger.LogTrace("[{this}] Suppressing CreateCacheObjectMessage for player pointer-only churn (addrDiff={addrDiff}, drawObjDiff={drawObjDiff})", this, addrDiff, drawObjDiff);
                }
            }
        }
        else if (addrDiff || drawObjDiff)
        {
            CurrentDrawCondition = DrawCondition.DrawObjectZero;
            _pendingOwnedObjectStructuralPublishAfterDraw = false;
            Logger.LogTrace("[{this}] Changed", this);
            if (_isOwnedObject && ObjectKind != ObjectKind.Player)
            {
                Mediator.Publish(new ClearCacheForObjectMessage(this));
            }
        }
    }

    private unsafe bool CompareAndUpdateCustomizeData(Span<byte> customizeData)
    {
        bool hasChanges = false;

        for (int i = 0; i < customizeData.Length; i++)
        {
            var data = customizeData[i];
            if (CustomizeData[i] != data)
            {
                CustomizeData[i] = data;
                hasChanges = true;
            }
        }

        return hasChanges;
    }

    private unsafe bool CompareAndUpdateEquipByteData(byte* equipSlotData)
    {
        bool hasChanges = false;
        for (int i = 0; i < EquipSlotData.Length; i++)
        {
            var data = equipSlotData[i];
            if (EquipSlotData[i] != data)
            {
                EquipSlotData[i] = data;
                hasChanges = true;
            }
        }

        return hasChanges;
    }

    private unsafe bool CompareAndUpdateMainHand(Weapon* weapon)
    {
        if ((nint)weapon == nint.Zero) return false;
        bool hasChanges = false;
        hasChanges |= weapon->ModelSetId != MainHandData[0];
        MainHandData[0] = weapon->ModelSetId;
        hasChanges |= weapon->Variant != MainHandData[1];
        MainHandData[1] = weapon->Variant;
        hasChanges |= weapon->SecondaryId != MainHandData[2];
        MainHandData[2] = weapon->SecondaryId;
        return hasChanges;
    }

    private unsafe bool CompareAndUpdateOffHand(Weapon* weapon)
    {
        if ((nint)weapon == nint.Zero) return false;
        bool hasChanges = false;
        hasChanges |= weapon->ModelSetId != OffHandData[0];
        OffHandData[0] = weapon->ModelSetId;
        hasChanges |= weapon->Variant != OffHandData[1];
        OffHandData[1] = weapon->Variant;
        hasChanges |= weapon->SecondaryId != OffHandData[2];
        OffHandData[2] = weapon->SecondaryId;
        return hasChanges;
    }

    private void FrameworkUpdate()
    {
        if (!_dalamudUtil.IsLoggedIn || _dalamudUtil.IsZoning)
        {
            _haltProcessing = true;
            Invalidate();
            return;
        }

        if (_haltProcessing) return;
        if (!_delayedZoningTask?.IsCompleted ?? false) return;

        var nowTick = Environment.TickCount64;
        var shouldUseFastCadence =
            _pendingTransientPublishAfterRedraw
            || CurrentDrawCondition != DrawCondition.None
            || Address == IntPtr.Zero
            || DrawObjectAddress == IntPtr.Zero
            || string.IsNullOrEmpty(Name);

        var intervalMs = shouldUseFastCadence
            ? (_isOwnedObject ? OwnedUpdateIntervalMs : OtherUpdateIntervalMs)
            : (_isOwnedObject ? OwnedStableUpdateIntervalMs : OtherStableUpdateIntervalMs);

        if (nowTick < Interlocked.Read(ref _nextUpdateTick)) return;

        var nextTick = nowTick + intervalMs;
        if (!_isOwnedObject)
            nextTick += shouldUseFastCadence ? Math.Min(_updatePhaseJitterMs, 25) : _updatePhaseJitterMs;

        Interlocked.Exchange(ref _nextUpdateTick, nextTick);

        try
        {
            if (_performanceCollector.Enabled)
            {
                _performanceCollector.LogPerformance(
                    this,
                    $"CheckAndUpdateObject>{(_isOwnedObject ? "Self" : "Other")}+{ObjectKind}/{(string.IsNullOrEmpty(Name) ? "Unk" : Name)}" + $"+{Address.ToString("X")}",
                    CheckAndUpdateObject);
            }
            else
            {
                CheckAndUpdateObject();
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error during FrameworkUpdate of {this}", this);
        }
    }


    private bool IsBeingDrawn()
    {
        if (_dalamudUtil.IsAnythingDrawing)
        {
            if (Logger.IsEnabled(LogLevel.Trace))
                Logger.LogTrace("[{this}] IsBeingDrawn, Global draw block", this);
            return true;
        }

        if (Logger.IsEnabled(LogLevel.Trace))
            Logger.LogTrace("[{this}] IsBeingDrawn, Condition: {cond}", this, CurrentDrawCondition);

        return CurrentDrawCondition != DrawCondition.None;
    }


    private unsafe DrawCondition IsBeingDrawnUnsafe()
    {
        if (Address == IntPtr.Zero) return DrawCondition.ObjectZero;
        if (DrawObjectAddress == IntPtr.Zero) return DrawCondition.DrawObjectZero;
        var flags = ((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)Address)->RenderFlags;
        var renderFlags = (((uint)flags) & 0x800u) != 0;
        if (renderFlags) return DrawCondition.RenderFlags;

        if (((DrawObject*)DrawObjectAddress)->Object.GetObjectType() == ObjectType.CharacterBase)
        {
            var characterBase = (CharacterBase*)DrawObjectAddress;

            var modelInSlotLoaded = characterBase->HasModelInSlotLoaded != 0;
            if (modelInSlotLoaded) return DrawCondition.ModelInSlotLoaded;

            var modelFilesInSlotLoaded = characterBase->HasModelFilesInSlotLoaded != 0;
            if (modelFilesInSlotLoaded) return DrawCondition.ModelFilesInSlotLoaded;
        }

        return DrawCondition.None;
    }

    private void ZoneSwitchEnd()
    {
        try
        {
            _zoningCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Zoning CTS cancel issue");
        }

        // Do not wait for the delayed task continuation before the next visibility pass.
        // Zone end is the positive signal that object discovery can resume immediately.
        _haltProcessing = false;
        _nextUpdateTick = 0;
    }

    private void ZoneSwitchStart()
    {
        try
        {
            _zoningCts?.Cancel();
        }
        catch
        {
            // ignore
        }

        _haltProcessing = true;
        Invalidate();

        _zoningCts = new();
        Logger.LogDebug("[{obj}] Starting Delay After Zoning", this);
        _delayedZoningTask = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(PostZoneSettleDelayMs, _zoningCts.Token).ConfigureAwait(false);
            }
            catch
            {
                // ignore cancelled
            }
            finally
            {
                _haltProcessing = false;
                _nextUpdateTick = 0;
                Logger.LogDebug("[{this}] Delay after zoning complete", this);
                _zoningCts.Dispose();
            }
        });
    }
}