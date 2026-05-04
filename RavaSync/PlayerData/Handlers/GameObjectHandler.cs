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
    private CancellationTokenSource _zoningCts = new();
    private long _nextUpdateTick;
    private bool _redrawPublishIssued;
    private bool _suppressNextSemanticDiffPublishAfterRedraw;
    private const int OwnedUpdateIntervalMs = 33;   // ~30Hz
    private const int OwnedStableUpdateIntervalMs = 66;
    private const int OtherUpdateIntervalMs = 100;  // 10Hz
    private const int OtherStableUpdateIntervalMs = 180;


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
        Name = string.Empty;

        if (ownedObject)
        {
            Mediator.Subscribe<TransientResourceChangedMessage>(this, (msg) =>
            {
                if (!(_delayedZoningTask?.IsCompleted ?? true)) return;
                if (msg.Address != Address) return;

                if (ObjectKind == ObjectKind.Player)
                    _pendingTransientPublishAfterRedraw = true;

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
            _redrawPublishIssued = false;
            _suppressNextSemanticDiffPublishAfterRedraw = false;
            ZoneSwitchStart();
        });

        Mediator.Subscribe<CutsceneStartMessage>(this, (_) =>
        {
            _haltProcessing = false; //changed to false in efforts to stop issues with cutscene drawing
            _pendingTransientPublishAfterRedraw = false;
            _pendingPlayerPublishAfterRequestedRedraw = false;
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
                if (semanticDiff)
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

                    Logger.LogDebug("[{this}] Changed, Sending CreateCacheObjectMessage", this);
                    Mediator.Publish(new CreateCacheForObjectMessage(this, $"GameObject:SemanticDiff(equip={equipDiff},customize={customizeDiff},name={nameChange})"));
                }
                else if (addrDiff || drawObjDiff)
                {
                    Logger.LogTrace("[{this}] Suppressing CreateCacheObjectMessage for pointer-only churn (addrDiff={addrDiff}, drawObjDiff={drawObjDiff})", this, addrDiff, drawObjDiff);
                }
            }
        }
        else if (addrDiff || drawObjDiff)
        {
            CurrentDrawCondition = DrawCondition.DrawObjectZero;
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
        Interlocked.Exchange(ref _nextUpdateTick, nowTick + intervalMs);

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

        if (ObjectKind == ObjectKind.Player)
        {
            var modelInSlotLoaded = (((CharacterBase*)DrawObjectAddress)->HasModelInSlotLoaded != 0);
            if (modelInSlotLoaded) return DrawCondition.ModelInSlotLoaded;
            var modelFilesInSlotLoaded = (((CharacterBase*)DrawObjectAddress)->HasModelFilesInSlotLoaded != 0);
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
                await Task.Delay(TimeSpan.FromSeconds(120), _zoningCts.Token).ConfigureAwait(false);
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