using Lumina;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using ObjectKind = RavaSync.API.Data.Enum.ObjectKind;

namespace RavaSync.PlayerData.Handlers;

public sealed class PairHandler : DisposableMediatorSubscriberBase
{
    private sealed record CombatData(Guid ApplicationId, CharacterData CharacterData, bool Forced);

    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileDownloadManager _downloadManager;
    private readonly FileCacheManager _fileDbManager;
    private readonly GameObjectHandlerFactory _gameObjectHandlerFactory;
    private readonly IpcManager _ipcManager;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly PlayerPerformanceService _playerPerformanceService;
    private readonly ServerConfigurationManager _serverConfigManager;
    private readonly PluginWarningNotificationService _pluginWarningNotificationManager;
    private CancellationTokenSource? _applicationCancellationTokenSource = new();
    private CancellationTokenSource? _downloadCancellationTokenSource = new();
    private Guid _applicationId;
    private Task? _applicationTask;
    private CharacterData? _cachedData = null;
    private GameObjectHandler? _charaHandler;
    private readonly Dictionary<ObjectKind, Guid?> _customizeIds = [];
    private CombatData? _dataReceivedInDowntime;
    private bool _forceApplyMods = false;
    private bool _isVisible;
    private Guid _penumbraCollection;
    private Task<Guid>? _penumbraCollectionTask;
    private CancellationTokenSource? _penumbraCollectionTeardownCts;
    private static readonly TimeSpan PenumbraCollectionTeardownDelay = TimeSpan.FromSeconds(15);
    private Task? _initializeTask;
    private int _initializeStarted;
    private bool _redrawOnNextApplication = false;
    private bool _hasRetriedAfterMissingDownload = false;
    private bool _hasRetriedAfterMissingAtApply = false;
    private int _manualRepairRunning = 0;
    private int? _lastAssignedObjectIndex = null;
    private DateTime _lastAssignedCollectionAssignUtc = DateTime.MinValue;
    private long _lastApplyCompletedTick;
    private static int ComputeNormalApplyConcurrency()
    {
        var logical = Environment.ProcessorCount;

        // Roughly: 16 threads => 4, 8 threads => 2, 24 threads => 5
        var normal = logical / 4;
        if (normal < 2) normal = 2;
        if (normal > 5) normal = 5;

        return normal;
    }

    private static readonly int NormalApplyConcurrency = ComputeNormalApplyConcurrency();
    private static readonly int StormApplyConcurrency = Math.Max(1, NormalApplyConcurrency / 2);

    private static readonly SemaphoreSlim NormalApplySemaphore = new(NormalApplyConcurrency, NormalApplyConcurrency);
    private static readonly SemaphoreSlim StormApplySemaphore = new(StormApplyConcurrency, StormApplyConcurrency);

    // Smooth redraw spikes globally (room-entry bursts) without hard-stalling all redraws.
    private static readonly SemaphoreSlim GlobalRedrawSemaphore = new(3, 3);
    private static readonly ConcurrentDictionary<int, byte> RedrawObjectIndicesInFlight = new();

    private static readonly SemaphoreSlim GlobalPostApplyRepairSemaphore = new(2, 2);
    private string? _lastAttemptedDataHash;
    private string? _lastAppliedTempModsFingerprint;
    private string? _lastAppliedManipulationFingerprint = null;

    private readonly object _missingCheckGate = new();
    private string? _lastMissingCheckedHash;
    private long _lastMissingCheckedTick;
    private bool _lastMissingCheckHadMissing;
    private int _missingCheckRunning;

    private string? _pendingMissingCheckHash;
    private CharacterData? _pendingMissingCheckData;
    private Guid _pendingMissingCheckBase;

    private string? _lastPostApplyRepairHash;
    private long _lastPostApplyRepairTick;

    private readonly object _postRepairGate = new();
    private CharacterData? _pendingPostApplyRepairData;
    private string? _pendingPostApplyRepairHash;

    private readonly object _applyCoalesceGate = new();
    private CancellationTokenSource? _applyCoalesceCts;
    private Task? _applyCoalesceTask;

    private Guid _applyQueuedBase;
    private CharacterData? _applyQueuedData;
    private bool _applyQueuedForce;

    private readonly object _pipelineGate = new();
    private string? _activePipelineHash;

    private long _addressZeroSinceTick = -1;
    private bool _initialApplyPending;
    private long _otherSyncPollTick;
    private long _otherSyncReleaseCandidateSinceTick = -1;
    private nint _lastKnownOwnershipAddr = nint.Zero;

    private bool? _lastBroadcastYield;
    private string _lastBroadcastOwner = string.Empty;


    public PairHandler(ILogger<PairHandler> logger, Pair pair,
        GameObjectHandlerFactory gameObjectHandlerFactory,
        IpcManager ipcManager, FileDownloadManager transferManager,
        PluginWarningNotificationService pluginWarningNotificationManager,
        DalamudUtilService dalamudUtil, IHostApplicationLifetime lifetime,
        FileCacheManager fileDbManager, MareMediator mediator,
        PlayerPerformanceService playerPerformanceService,
        ServerConfigurationManager serverConfigManager) : base(logger, mediator)
    {
        Pair = pair;
        _gameObjectHandlerFactory = gameObjectHandlerFactory;
        _ipcManager = ipcManager;
        _downloadManager = transferManager;
        _pluginWarningNotificationManager = pluginWarningNotificationManager;
        _dalamudUtil = dalamudUtil;
        _lifetime = lifetime;
        _fileDbManager = fileDbManager;
        _playerPerformanceService = playerPerformanceService;
        _serverConfigManager = serverConfigManager;
        _penumbraCollection = Guid.Empty;
        _penumbraCollectionTask = null;

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (_) => FrameworkUpdate());
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (zs) =>
        {
            _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
            _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();

            _charaHandler?.Invalidate();

            _addressZeroSinceTick = -1;
            _initialApplyPending = true;
            _otherSyncReleaseCandidateSinceTick = -1;
            _lastKnownOwnershipAddr = nint.Zero;

            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;
            _lastAppliedTempModsFingerprint = null;
            _lastAppliedManipulationFingerprint = null;

            CancelPendingPenumbraCollectionTeardown();
            _ = RemovePenumbraCollectionAsync(Guid.NewGuid());

            IsVisible = false;

            _lastBroadcastYield = null;
            _lastBroadcastOwner = string.Empty;
        });

        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) =>
        {
            CancelPendingPenumbraCollectionTeardown();
            _penumbraCollection = Guid.Empty;
            _penumbraCollectionTask = null;

            _lastAppliedTempModsFingerprint = null;
            _lastAppliedManipulationFingerprint = null;
            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;

            if (!IsVisible && _charaHandler != null)
            {
                PlayerName = string.Empty;
                _charaHandler.Dispose();
                _charaHandler = null;

                Interlocked.Exchange(ref _initializeStarted, 0);
                _initializeTask = null;
            }
        });
        Mediator.Subscribe<ClassJobChangedMessage>(this, (msg) =>
        {
            if (msg.GameObjectHandler == _charaHandler)
            {
                _redrawOnNextApplication = true;
            }
        });
        Mediator.Subscribe<DownloadStartedMessage>(this, (msg) =>
        {
            if (_charaHandler == null) return;
            if (msg.DownloadId != _charaHandler) return;

            Pair.SetCurrentDownloadStatus(SnapshotStatus(msg.DownloadStatus));
        });

        Mediator.Subscribe<DownloadFinishedMessage>(this, (msg) =>
        {
            if (_charaHandler == null) return;
            if (msg.DownloadId != _charaHandler) return;

            Pair.SetCurrentDownloadStatus(null);
            Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        });


        Mediator.Subscribe<CombatOrPerformanceEndMessage>(this, (msg) =>
        {
            if (IsVisible && _dataReceivedInDowntime != null)
            {
                ApplyCharacterData(_dataReceivedInDowntime.ApplicationId,
                    _dataReceivedInDowntime.CharacterData, _dataReceivedInDowntime.Forced);
                _dataReceivedInDowntime = null;
            }
        });

        Mediator.Subscribe<CombatOrPerformanceStartMessage>(this, _ =>
        {
            _dataReceivedInDowntime = null;
            _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
            _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();
        });

        LastAppliedDataBytes = -1;
    }

    public bool IsVisible
    {
        get => _isVisible;
        private set
        {
            if (_isVisible != value)
            {
                _isVisible = value;
                string text = "User Visibility Changed, now: " + (_isVisible ? "Is Visible" : "Is not Visible");
                Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler),
                    EventSeverity.Informational, text)));
                Mediator.Publish(new RefreshUiMessage());
            }
        }
    }

    public long LastAppliedDataBytes { get; private set; }
    public Pair Pair { get; private set; }
    public nint PlayerCharacter => _charaHandler?.Address ?? nint.Zero;
    public unsafe uint PlayerCharacterId => (_charaHandler?.Address ?? nint.Zero) == nint.Zero
        ? uint.MaxValue
        : ((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)_charaHandler!.Address)->EntityId;
    public string? PlayerName { get; private set; }
    public string PlayerNameHash => Pair.Ident;

    public void ApplyCharacterData(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization = false)
    {
        EnqueueApply(applicationBase, characterData, forceApplyCustomization);
    }

    private void EnqueueApply(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization)
    {
        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync) return;

        lock (_applyCoalesceGate)
        {
            _applyQueuedBase = applicationBase;
            _applyQueuedData = characterData;
            _applyQueuedForce = forceApplyCustomization;

            _applyCoalesceCts?.CancelDispose();
            _applyCoalesceCts = new CancellationTokenSource();

            var token = _applyCoalesceCts.Token;

            _applyCoalesceTask = Task.Run(async () =>
            {
                try
                {
                    if (SyncStorm.IsActive)
                    {
                        await Task.Delay(10, token).ConfigureAwait(false);
                    }

                    CharacterData? data;
                    Guid baseId;
                    bool forced;

                    lock (_applyCoalesceGate)
                    {
                        data = _applyQueuedData;
                        baseId = _applyQueuedBase;
                        forced = _applyQueuedForce;
                    }

                    if (data != null)
                    {
                        ApplyCharacterDataNow(baseId, data, forced);
                    }
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex, "Apply coalescer failed for {player}", PlayerName);
                }
            }, token);
        }
    }

    private void ApplyCharacterDataNow(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization = false)
    {
        if (_dalamudUtil.IsInCombatOrPerforming)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in combat or performing music, deferring application")));
            Logger.LogDebug("[BASE-{appBase}] Received data but player is in combat or performing", applicationBase);
            _dataReceivedInDowntime = new(applicationBase, characterData, forceApplyCustomization);
            SetUploading(isUploading: false);
            return;
        }

        if (_charaHandler == null || (PlayerCharacter == IntPtr.Zero))
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: Receiving Player is in an invalid state, deferring application")));
            Logger.LogDebug("[BASE-{appBase}] Received data but player was in invalid state, charaHandlerIsNull: {charaIsNull}, playerPointerIsNull: {ptrIsNull}",
                applicationBase, _charaHandler == null, PlayerCharacter == IntPtr.Zero);
            var diffData = characterData.CheckUpdatedData(applicationBase, _cachedData, Logger,
                this, forceApplyCustomization, forceApplyMods: false);

            bool hasDiffMods = false;
            foreach (var p in diffData)
            {
                if (p.Value.Contains(PlayerChanges.ModManip) || p.Value.Contains(PlayerChanges.ModFiles))
                {
                    hasDiffMods = true;
                    break;
                }
            }
            _forceApplyMods = hasDiffMods || _forceApplyMods || (PlayerCharacter == IntPtr.Zero && _cachedData == null);
            _cachedData = characterData;
            Logger.LogDebug("[BASE-{appBase}] Setting data: {hash}, forceApplyMods: {force}", applicationBase, _cachedData.DataHash.Value, _forceApplyMods);
            return;
        }

        SetUploading(isUploading: false);

        var newHash = characterData.DataHash.Value;
        var oldHash = _cachedData?.DataHash.Value ?? "NODATA";

        if (!string.Equals(newHash, _lastAttemptedDataHash, StringComparison.Ordinal))
        {
            _lastAttemptedDataHash = newHash;
            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;
        }

        Logger.LogDebug("[BASE-{appbase}] Applying data for {player}, forceApplyCustomization: {forced}, forceApplyMods: {forceMods}",
            applicationBase, this, forceApplyCustomization, _forceApplyMods);
        Logger.LogDebug("[BASE-{appbase}] Hash for data is {newHash}, current cache hash is {oldHash}",
            applicationBase, newHash, oldHash);

        bool sameHash = string.Equals(newHash, _cachedData?.DataHash.Value ?? string.Empty, StringComparison.Ordinal);


        if (sameHash && !forceApplyCustomization && !_redrawOnNextApplication)
        {
            if (TryGetRecentMissingCheck(newHash, out var hadMissing))
            {
                if (!hadMissing)
                {
                    Logger.LogDebug("[BASE-{appbase}] Data hash unchanged and recent missing-check passed, skipping re-apply", applicationBase);
                    return;
                }

                Logger.LogInformation("[BASE-{appbase}] Data hash unchanged but cached missing-check indicates missing; forcing repair", applicationBase);
            }
            else
            {
                ScheduleMissingCheck(applicationBase, characterData);
                Logger.LogDebug("[BASE-{appbase}] Data hash unchanged; scheduled missing-check and returning to avoid framework hitch", applicationBase);
                return;
            }
        }

        if (_dalamudUtil.IsInGpose || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
        {
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Warning,
                "Cannot apply character data: you are in GPose, a Cutscene or Penumbra/Glamourer is not available")));
            Logger.LogInformation("[BASE-{appbase}] Application of data for {player} while in cutscene/gpose or Penumbra/Glamourer unavailable, returning", applicationBase, this);
            return;
        }

        Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
            "Applying Character Data")));

        _forceApplyMods |= forceApplyCustomization;

        var charaDataToUpdate = characterData.CheckUpdatedData(applicationBase, _cachedData ?? new(), Logger, this, forceApplyCustomization, _forceApplyMods);

        if (_charaHandler != null && _forceApplyMods)
        {
            _forceApplyMods = false;
        }

        if (_redrawOnNextApplication)
        {
            if (!charaDataToUpdate.TryGetValue(ObjectKind.Player, out var player))
                charaDataToUpdate[ObjectKind.Player] = player = new HashSet<PlayerChanges>();

            player.Add(PlayerChanges.ForcedRedraw);
            _redrawOnNextApplication = false;
        }

        if (charaDataToUpdate.TryGetValue(ObjectKind.Player, out var playerChanges))
        {
            _pluginWarningNotificationManager.NotifyForMissingPlugins(Pair.UserData, PlayerName!, playerChanges);
        }

        Logger.LogDebug("[BASE-{appbase}] Downloading and applying character for {name}", applicationBase, this);

        DownloadAndApplyCharacter(applicationBase, characterData, charaDataToUpdate);
    }


    public override string ToString()
    {
        return Pair == null
            ? base.ToString() ?? string.Empty
            : Pair.UserData.AliasOrUID + ":" + PlayerName + ":" + (PlayerCharacter != nint.Zero ? "HasChar" : "NoChar");
    }

    internal void SetUploading(bool isUploading = true)
    {
        Logger.LogTrace("Setting {this} uploading {uploading}", this, isUploading);

        if (isUploading && Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
        {
            isUploading = false;
        }

        if (isUploading && Pair.RemoteOtherSyncOverrideActive && Pair.RemoteOtherSyncYield && !Pair.EffectiveOverrideOtherSync)
        {
            isUploading = false;
        }

        Pair?.SetUploadState(isUploading);

        if (_charaHandler != null)
        {
            try
            {
                var go = _charaHandler.GetGameObject();
                if (go == null) return;

                if (go.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Player)
                    return;

                var expected = _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident);
                if (expected != nint.Zero && expected != _charaHandler.Address)
                    return;

                Mediator.Publish(new PlayerUploadingMessage(_charaHandler, isUploading));
            }
            catch
            {
                // Best effort
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        SetUploading(isUploading: false);
        var name = PlayerName;
        Logger.LogDebug("Disposing {name} ({user})", name, Pair);
        try
        {
            Guid applicationId = Guid.NewGuid();
            _applicationCancellationTokenSource?.CancelDispose();
            _applicationCancellationTokenSource = null;
            _downloadCancellationTokenSource?.CancelDispose();
            _downloadCancellationTokenSource = null;
            _downloadManager.Dispose();
            _charaHandler?.Dispose();
            _charaHandler = null;

            if (!string.IsNullOrEmpty(name))
            {
                Mediator.Publish(new EventMessage(new Event(name, Pair.UserData, nameof(PairHandler), EventSeverity.Informational, "Disposing User")));
            }

            if (_lifetime.ApplicationStopping.IsCancellationRequested) return;

            if (_dalamudUtil is { IsZoning: false, IsInCutscene: false } && !string.IsNullOrEmpty(name))
            {
                Logger.LogTrace("[{applicationId}] Restoring state for {name} ({OnlineUser})", applicationId, name, Pair.UserPair);
                Logger.LogDebug("[{applicationId}] Removing Temp Collection for {name} ({user})", applicationId, name, Pair.UserPair);
                RemovePenumbraCollectionAsync(applicationId).GetAwaiter().GetResult();
                if (!IsVisible)
                {
                    Logger.LogDebug("[{applicationId}] Restoring Glamourer for {name} ({user})", applicationId, name, Pair.UserPair);
                    _ipcManager.Glamourer.RevertByNameAsync(Logger, name, applicationId).GetAwaiter().GetResult();
                }
                else
                {
                    using var cts = new CancellationTokenSource();
                    cts.CancelAfter(TimeSpan.FromSeconds(60));

                    Logger.LogInformation("[{applicationId}] CachedData is null {isNull}, contains things: {contains}", applicationId, _cachedData == null, _cachedData?.FileReplacements.Any() ?? false);

                    foreach (KeyValuePair<ObjectKind, List<FileReplacementData>> item in _cachedData?.FileReplacements ?? [])
                    {
                        try
                        {
                            RevertCustomizationDataAsync(item.Key, name, applicationId, cts.Token).GetAwaiter().GetResult();
                        }
                        catch (InvalidOperationException ex)
                        {
                            Logger.LogWarning(ex, "Failed disposing player (not present anymore?)");
                            break;
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogDebug("Disposal for {name} was cancelled (timeout or shutdown).", name);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error on disposal of {name}", name);
        }
        finally
        {
            PlayerName = null;
            _cachedData = null;
            _lastAppliedTempModsFingerprint = null;
            _lastAppliedManipulationFingerprint = null;
            Logger.LogDebug("Disposing {name} complete", name);
        }
    }

    private async Task<bool> ApplyCustomizationDataAsync(Guid applicationId, KeyValuePair<ObjectKind, HashSet<PlayerChanges>> changes, CharacterData charaData, bool allowPlayerRedraw, CancellationToken token)
    {
        if (PlayerCharacter == nint.Zero) return false;
        var ptr = PlayerCharacter;

        var handler = changes.Key switch
        {
            ObjectKind.Player => _charaHandler!,
            ObjectKind.Companion => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetCompanionPtr(ptr), isWatched: false).ConfigureAwait(false),
            ObjectKind.MinionOrMount => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetMinionOrMountPtr(ptr), isWatched: false).ConfigureAwait(false),
            ObjectKind.Pet => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetPetPtr(ptr), isWatched: false).ConfigureAwait(false),
            _ => throw new NotSupportedException("ObjectKind not supported: " + changes.Key)
        };

        try
        {
            if (handler.Address == nint.Zero) return false;

            Logger.LogDebug("[{applicationId}] Applying Customization Data for {handler}", applicationId, handler);
            if (changes.Key != ObjectKind.Player)
            {
                await _dalamudUtil.WaitWhileCharacterIsDrawing(Logger, handler, applicationId, 30000, token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
            }

            // Coalesce redraws for this handler into a single call at the end.
            bool needsRedraw = false;

            foreach (var change in changes.Value.OrderBy(p => (int)p))
            {
                Logger.LogDebug("[{applicationId}] Processing {change} for {handler}", applicationId, change, handler);

                switch (change)
                {
                    case PlayerChanges.Customize:
                        // Per-pair pause: if disabled, revert any previous C+ and skip applying new data
                        if (!Pair.IsCustomizePlusEnabled)
                        {
                            if (_customizeIds.TryGetValue(changes.Key, out var appliedId) && appliedId.HasValue)
                            {
                                await _ipcManager.CustomizePlus.RevertByIdAsync(appliedId.Value).ConfigureAwait(false);
                                _customizeIds.Remove(changes.Key);
                            }
                            break;
                        }

                        if (charaData.CustomizePlusData.TryGetValue(changes.Key, out var customizePlusData))
                        {
                            _customizeIds[changes.Key] = await _ipcManager.CustomizePlus
                                .SetBodyScaleAsync(handler.Address, customizePlusData)
                                .ConfigureAwait(false);
                        }
                        else if (_customizeIds.TryGetValue(changes.Key, out var customizeId) && customizeId.HasValue)
                        {
                            await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                            _customizeIds.Remove(changes.Key);
                        }
                        break;

                    case PlayerChanges.Heels:
                        await _ipcManager.Heels.SetOffsetForPlayerAsync(handler.Address, charaData.HeelsData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.Honorific:
                        await _ipcManager.Honorific.SetTitleAsync(handler.Address, charaData.HonorificData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.Glamourer:
                        if (charaData.GlamourerData.TryGetValue(changes.Key, out var glamourerData))
                        {
                            _ = _ipcManager.Glamourer.ApplyAllAsync(Logger, handler, glamourerData, applicationId, token, fireAndForget: true);
                        }
                        break;



                    case PlayerChanges.Moodles:
                        await _ipcManager.Moodles.SetStatusAsync(handler.Address, charaData.MoodlesData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.PetNames:
                        await _ipcManager.PetNames.SetPlayerData(handler.Address, charaData.PetNamesData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.ForcedRedraw:
                        needsRedraw = true;
                        break;

                    default:
                        break;
                }

                token.ThrowIfCancellationRequested();
            }

            if (needsRedraw)
            {
                if (changes.Key == ObjectKind.Player)
                    return true;

                await _ipcManager.Penumbra.RedrawAsync(Logger, handler, applicationId, token).ConfigureAwait(false);
            }

            return false;
        }
        finally
        {
            if (handler != _charaHandler) handler.Dispose();
        }
    }

    private async Task OnePassRedrawAsync(Guid applicationId, CancellationToken token)
    {
        if (_charaHandler == null || _charaHandler.Address == nint.Zero)
            return;

        int objIndex;
        try
        {
            objIndex = await _dalamudUtil
                .RunOnFrameworkThread(() => _charaHandler!.GetGameObject()!.ObjectIndex)
                .ConfigureAwait(false);
        }
        catch
        {
            return;
        }

        // If a redraw for this object is already in-flight, don't pile another one on top.
        if (!RedrawObjectIndicesInFlight.TryAdd(objIndex, 0))
        {
            Logger.LogTrace("[{applicationId}] Skipping redraw for idx {idx}: redraw already in flight", applicationId, objIndex);
            return;
        }

        var acquired = false;
        try
        {
            await GlobalRedrawSemaphore.WaitAsync(token).ConfigureAwait(false);
            acquired = true;

            // Tiny stagger to stop all redraws landing on the same frame.
            if (SyncStorm.IsActive)
            {
                var seed = ((PlayerNameHash?.GetHashCode(StringComparison.Ordinal) ?? 0) ^ objIndex) & 0x1F;
                await Task.Delay(10 + seed, token).ConfigureAwait(false); // 10–41ms
            }

            if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                return;

            await _ipcManager.Penumbra.RedrawAsync(Logger, _charaHandler, applicationId, token).ConfigureAwait(false);
        }
        finally
        {
            if (acquired)
                GlobalRedrawSemaphore.Release();

            RedrawObjectIndicesInFlight.TryRemove(objIndex, out _);
        }
    }

    private sealed class PreparedReplacementPlan
    {
        public required ImmutableArray<FileReplacementData> NoSwap { get; init; }
        public required ImmutableArray<FileReplacementData> Swaps { get; init; }
    }

    private static readonly ConcurrentDictionary<string, PreparedReplacementPlan> ReplacementPlanCache = new(StringComparer.Ordinal);

    private static PreparedReplacementPlan BuildReplacementPlan(CharacterData charaData)
    {
        var noSwap = ImmutableArray.CreateBuilder<FileReplacementData>(256);
        var swaps = ImmutableArray.CreateBuilder<FileReplacementData>(64);

        foreach (var kv in charaData.FileReplacements)
        {
            var list = kv.Value;
            for (int i = 0; i < list.Count; i++)
            {
                var v = list[i];
                if (string.IsNullOrWhiteSpace(v.FileSwapPath))
                    noSwap.Add(v);
                else
                    swaps.Add(v);
            }
        }

        return new PreparedReplacementPlan
        {
            NoSwap = noSwap.Count == noSwap.Capacity ? noSwap.MoveToImmutable() : noSwap.ToImmutable(),
            Swaps = swaps.Count == swaps.Capacity ? swaps.MoveToImmutable() : swaps.ToImmutable()
        };
    }

    private PreparedReplacementPlan GetOrCreateReplacementPlan(CharacterData charaData)
    {
        var hash = charaData.DataHash.Value;
        return ReplacementPlanCache.GetOrAdd(hash, _ => BuildReplacementPlan(charaData));
    }

    private void DownloadAndApplyCharacter(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (!updatedData.Any())
        {
            Logger.LogDebug("[BASE-{appBase}] Nothing to update for {obj}", applicationBase, this);
            return;
        }

        var pipelineHash = charaData.DataHash.Value;

        if (!TryBeginPipeline(pipelineHash))
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug(
                    "[BASE-{appBase}] Skipping duplicate apply pipeline for {player}, hash {hash} is already in flight",
                    applicationBase,
                    PlayerName,
                    pipelineHash);
            }

            return;
        }

        var handedOffToAsync = false;

        try
        {
            var updateModdedPaths = false;
            var updateManip = false;

            foreach (var set in updatedData.Values)
            {
                if (!updateModdedPaths && set.Contains(PlayerChanges.ModFiles))
                    updateModdedPaths = true;

                if (!updateManip && set.Contains(PlayerChanges.ModManip))
                    updateManip = true;

                if (updateModdedPaths && updateManip)
                    break;
            }

            _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate() ?? new CancellationTokenSource();

            CancellationToken downloadToken;
            try
            {
                downloadToken = _downloadCancellationTokenSource.Token;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            var task = DownloadAndApplyCharacterAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, downloadToken);
            handedOffToAsync = true;

            _ = task.ContinueWith(t =>
            {
                try
                {
                    _ = t.Exception;
                }
                catch
                {
                }
            }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
        }
        finally
        {
            if (!handedOffToAsync)
            {
                EndPipeline(pipelineHash);
            }
        }
    }


    private Task? _pairDownloadTask;

    private async Task DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData,
        bool updateModdedPaths, bool updateManip, CancellationToken downloadToken)
    {
        var pipelineHash = charaData.DataHash.Value;
        var handedOffToApplication = false;

        try
        {
            Dictionary<(string GamePath, string? Hash), string> moddedPaths = [];
            List<FileReplacementData> toDownloadReplacements = [];

            bool downloadedAny = false;

            if (updateModdedPaths)
            {
                List<FileReplacementData> RecalculateMissing()
                {
                    return TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, downloadToken);
                }

                int attempts = 0;
                toDownloadReplacements = RecalculateMissing();
                int previousMissingCount = toDownloadReplacements.Count;

                bool foundVfxLikeDownload = false;

                for (int i = 0; i < toDownloadReplacements.Count; i++)
                {
                    var replacement = toDownloadReplacements[i];
                    var gamePaths = replacement.GamePaths;
                    if (gamePaths == null) continue;

                    if (gamePaths is IList<string> gpList)
                    {
                        for (int j = 0; j < gpList.Count; j++)
                        {
                            var p = gpList[j];
                            if (string.IsNullOrWhiteSpace(p)) continue;

                            if (p.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase) ||
                                p.EndsWith(".atex", StringComparison.OrdinalIgnoreCase))
                            {
                                foundVfxLikeDownload = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        foreach (var p in gamePaths)
                        {
                            if (string.IsNullOrWhiteSpace(p)) continue;

                            if (p.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase) ||
                                p.EndsWith(".atex", StringComparison.OrdinalIgnoreCase))
                            {
                                foundVfxLikeDownload = true;
                                break;
                            }
                        }
                    }

                    if (foundVfxLikeDownload)
                        break;
                }

                if (foundVfxLikeDownload)
                {
                    _redrawOnNextApplication = true;
                }

                for (attempts = 1; attempts <= 10 && toDownloadReplacements.Count > 0 && !downloadToken.IsCancellationRequested; attempts++)
                {
                    if (_pairDownloadTask != null && !_pairDownloadTask.IsCompleted)
                    {
                        Logger.LogDebug("[BASE-{appBase}] Finishing prior running download task for player {name}, {kind}",
                            applicationBase, PlayerName, updatedData);
                        await _pairDownloadTask.ConfigureAwait(false);
                    }

                    toDownloadReplacements = DeduplicateReplacementsByHash(toDownloadReplacements);

                    if (attempts > 1 && Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[BASE-{appBase}] Retry {attempt}/10 - still missing {count}: {missing}",
                            applicationBase,
                            attempts,
                            toDownloadReplacements.Count,
                            BuildMissingDetails(toDownloadReplacements));
                    }

                    Mediator.Publish(new EventMessage(new Event(
                        PlayerName,
                        Pair.UserData,
                        nameof(PairHandler),
                        EventSeverity.Informational,
                        $"Starting download for {toDownloadReplacements.Count} files (attempt {attempts}/10)")));

                    var toDownloadFiles = await _downloadManager
                        .InitiateDownloadList(_charaHandler!, toDownloadReplacements, downloadToken)
                        .ConfigureAwait(false);

                    if (toDownloadFiles != null && toDownloadFiles.Count > 0)
                        downloadedAny = true;

                    if (toDownloadFiles == null || toDownloadFiles.Count == 0)
                    {
                        var recalculatedMissing = RecalculateMissing();

                        if (recalculatedMissing.Count == 0)
                        {
                            Logger.LogDebug(
                                "[BASE-{appBase}] Attempt {attempt}/10 queued 0 downloads because nothing is actually missing; continuing to apply",
                                applicationBase, attempts);

                            break;
                        }

                        Logger.LogWarning(
                            "[BASE-{appBase}] Attempt {attempt}/10 queued 0 downloads, and {count} hashes still appear missing after immediate recheck. Backing off then retrying.",
                            applicationBase, attempts, recalculatedMissing.Count);

                        _downloadManager.ClearDownload();
                        _pairDownloadTask = null;

                        var delayMs = Math.Min(4000, 500 * attempts);
                        await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);
                        if (downloadToken.IsCancellationRequested) return;

                        previousMissingCount = recalculatedMissing.Count;
                        toDownloadReplacements = recalculatedMissing;
                        continue;
                    }

                    if (!_playerPerformanceService
                            .ComputeAndAutoPauseOnVRAMUsageThresholds(this, charaData, toDownloadFiles))
                    {
                        _downloadManager.ClearDownload();
                        return;
                    }

                    _pairDownloadTask = _downloadManager.DownloadFiles(_charaHandler!, toDownloadReplacements, downloadToken);

                    try
                    {
                        await _pairDownloadTask.ConfigureAwait(false);

                        Pair.SetCurrentDownloadStatus(null);
                        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.LogTrace("[BASE-{appBase}] Download task cancelled during attempt {attempt}/10", applicationBase, attempts);

                        Pair.SetCurrentDownloadStatus(null);
                        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
                        return;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex,
                            "[BASE-{appBase}] Download task failed on attempt {attempt}/10 for {name}. Backing off then retrying.",
                            applicationBase, attempts, PlayerName);

                        Pair.SetCurrentDownloadStatus(null);
                        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);

                        _downloadManager.ClearDownload();
                        _pairDownloadTask = null;

                        var delayMs = Math.Min(15000, 2000 * attempts);
                        await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);
                        if (downloadToken.IsCancellationRequested) return;

                        toDownloadReplacements = RecalculateMissing();
                        previousMissingCount = toDownloadReplacements.Count;
                        continue;
                    }

                    if (downloadToken.IsCancellationRequested)
                    {
                        Logger.LogTrace("[BASE-{appBase}] Detected cancellation", applicationBase);
                        return;
                    }

                    toDownloadReplacements = RecalculateMissing();

                    if (toDownloadReplacements.Count > 0 && toDownloadReplacements.Count == previousMissingCount)
                    {
                        Logger.LogDebug(
                            "[BASE-{appBase}] Missing count unchanged after attempt {attempt}/10 for {player}: still {count}",
                            applicationBase, attempts, PlayerName, toDownloadReplacements.Count);
                    }

                    previousMissingCount = toDownloadReplacements.Count;

                    if (toDownloadReplacements.Count == 0)
                        break;
                }

                if (toDownloadReplacements.Count > 0)
                {
                    Logger.LogWarning(
                        "[BASE-{appBase}] Aborting application for player {name}: {count} files still missing after {attempts} attempts. Missing: {missing}",
                        applicationBase,
                        PlayerName,
                        toDownloadReplacements.Count,
                        attempts,
                        BuildMissingDetails(toDownloadReplacements));

                    Mediator.Publish(new EventMessage(new Event(
                        PlayerName,
                        Pair.UserData,
                        nameof(PairHandler),
                        EventSeverity.Warning,
                        $"RavaSync: {toDownloadReplacements.Count} files could not be downloaded; not applying partial appearance.")));

                    if (!_hasRetriedAfterMissingDownload)
                    {
                        _hasRetriedAfterMissingDownload = true;

                        Logger.LogInformation(
                            "[BASE-{appBase}] Self-heal: re-running sync for {name} after missing-files detected",
                            applicationBase,
                            PlayerName);

                        DownloadAndApplyCharacter(applicationBase, charaData, updatedData);
                    }

                    return;
                }

                if (!await _playerPerformanceService.CheckBothThresholds(this, charaData).ConfigureAwait(false))
                {
                    return;
                }
            }

            downloadToken.ThrowIfCancellationRequested();

            CancellationToken? appToken = null;
            try
            {
                appToken = _applicationCancellationTokenSource?.Token;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            var lastWaitLog = 0L;

            while ((!_applicationTask?.IsCompleted ?? false)
                   && !downloadToken.IsCancellationRequested
                   && (appToken == null || !appToken.Value.IsCancellationRequested))
            {
                var now = Environment.TickCount64;
                if (now - lastWaitLog >= 1000)
                {
                    lastWaitLog = now;
                    Logger.LogDebug("[BASE-{appBase}] Waiting for current data application (Id: {id}) for player ({handler}) to finish",
                        applicationBase, _applicationId, PlayerName);
                }

                await Task.Delay(15, downloadToken).ConfigureAwait(false);

                try
                {
                    appToken = _applicationCancellationTokenSource?.Token;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
            }

            if (downloadToken.IsCancellationRequested || (appToken?.IsCancellationRequested ?? false))
            {
                return;
            }

            var newCts = new CancellationTokenSource();
            var oldCts = Interlocked.Exchange(ref _applicationCancellationTokenSource, newCts);
            oldCts?.CancelDispose();

            CancellationToken token;
            try
            {
                token = newCts.Token;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            _applicationTask = ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, moddedPaths, downloadedAny, token);
            handedOffToApplication = true;

            _ = _applicationTask.ContinueWith(t =>
            {
                try
                {
                    _ = t.Exception;
                    Logger.LogWarning(t.Exception, "[BASE-{appBase}] Application task faulted for {player}", applicationBase, PlayerName);
                }
                catch
                {
                }
            }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
        }
        finally
        {
            if (!handedOffToApplication)
            {
                EndPipeline(pipelineHash);
            }
        }
    }

    private async Task ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, Dictionary<(string GamePath, string? Hash), string> moddedPaths, bool downloadedAny, CancellationToken token)
    {
        var pipelineHash = charaData.DataHash.Value;
        var acquired = false;
        SemaphoreSlim? applySemaphore = null;

        try
        {
            applySemaphore = SyncStorm.IsActive ? StormApplySemaphore : NormalApplySemaphore;
            //applySemaphore = NormalApplySemaphore;

            await applySemaphore.WaitAsync(token).ConfigureAwait(false);
            acquired = true;


            try
            {
                _applicationId = Guid.NewGuid();
                Logger.LogDebug("[BASE-{applicationBase}] Starting application task for {this}: {appId}",
                    applicationBase, this, _applicationId);

                if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                {
                    Logger.LogDebug("[{applicationId}] Cancelled: chara handler not valid at start", _applicationId);
                    return;
                }

                Logger.LogDebug("[{applicationId}] Waiting for initial draw for {handler}", _applicationId, _charaHandler);
                if (_charaHandler!.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                {
                    await _dalamudUtil
                        .WaitWhileCharacterIsDrawing(Logger, _charaHandler!, _applicationId, 30000, token)
                        .ConfigureAwait(false);
                }

                token.ThrowIfCancellationRequested();

                bool needsRedraw = false;
                bool collectionReassignedThisRun = false;

                if (updateModdedPaths)
                {
                    var objIndex = await _dalamudUtil
                                    .RunOnFrameworkThread(() => _charaHandler!.GetGameObject()!.ObjectIndex)
                                    .ConfigureAwait(false);

                    var nowUtc = DateTime.UtcNow;

                    var firstAssign = !_lastAssignedObjectIndex.HasValue;
                    var indexChanged = _lastAssignedObjectIndex != objIndex;
                    var stale = (nowUtc - _lastAssignedCollectionAssignUtc) > TimeSpan.FromSeconds(20);

                    var needsAssign = firstAssign || indexChanged || stale;

                    if (needsAssign)
                    {
                        _lastAssignedCollectionAssignUtc = nowUtc;

                        var oldIdx = _lastAssignedObjectIndex;

                        await EnsurePenumbraCollectionAsync().ConfigureAwait(false);

                        var ok = await _ipcManager.Penumbra
                            .AssignTemporaryCollectionAsync(Logger, _penumbraCollection, objIndex)
                            .ConfigureAwait(false);

                        if (!ok)
                        {
                            Logger.LogDebug("[{applicationId}] Could not claim Penumbra temp collection for idx {idx}; aborting apply to avoid partial state",
                                _applicationId, objIndex);
                            return;
                        }

                        _lastAssignedObjectIndex = objIndex;
                        collectionReassignedThisRun = indexChanged;

                        if (indexChanged && oldIdx.HasValue && oldIdx.Value >= 0)
                        {
                            var idxToClean = oldIdx.Value;

                            _ = Task.Run(async () =>
                            {
                                try
                                {
                                    await _ipcManager.Penumbra.AssignEmptyCollectionAsync(Logger, idxToClean).ConfigureAwait(false);
                                    await _ipcManager.Honorific.ClearTitleByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                    await _ipcManager.PetNames.ClearPlayerDataByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                    await _ipcManager.Heels.UnregisterByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                    await _ipcManager.CustomizePlus.RevertByObjectIndexAsync((ushort)idxToClean).ConfigureAwait(false);
                                    await _ipcManager.Glamourer.RevertByObjectIndexAsync(Logger, idxToClean, _applicationId).ConfigureAwait(false);
                                }
                                catch (Exception ex)
                                {
                                    Logger.LogDebug(ex, "[{applicationId}] Failed cleaning stale ObjectIndex {idx}", _applicationId, idxToClean);
                                }
                            });
                        }
                    }

                    var cacheRoot = _fileDbManager.CacheFolder;
                    var applyNowPaths = moddedPaths;

                    List<KeyValuePair<(string GamePath, string? Hash), string>>? missingAtApply = null;

                    foreach (var kvp in applyNowPaths)
                    {
                        var path = kvp.Value;

                        if (string.IsNullOrEmpty(path))
                        {
                            (missingAtApply ??= new()).Add(kvp);
                            continue;
                        }

                        if (path.StartsWith(cacheRoot, StringComparison.OrdinalIgnoreCase) && !File.Exists(path))
                        {
                            (missingAtApply ??= new()).Add(kvp);
                        }
                    }

                    if (missingAtApply != null && missingAtApply.Count > 0)
                    {
                        var details = string.Join(", ", missingAtApply.Select(x =>
                            string.IsNullOrEmpty(x.Key.Hash)
                                ? $"{x.Key.GamePath} {x.Value}"
                                : $"{x.Key.Hash} ({x.Key.GamePath})"));

                        Logger.LogWarning(
                            "[{applicationId}] Aborting pair apply for {player}: {count} non-optional files missing at apply time. {details}",
                            _applicationId,
                            Pair.UserData.AliasOrUID,
                            missingAtApply.Count,
                            details);

                        if (!_hasRetriedAfterMissingAtApply)
                        {
                            _hasRetriedAfterMissingAtApply = true;

                            Logger.LogInformation(
                                "[{applicationId}] Self-heal: re-running sync for {player} after apply-time missing files",
                                _applicationId,
                                Pair.UserData.AliasOrUID);

                            DownloadAndApplyCharacter(applicationBase, charaData, updatedData);
                        }

                        return;
                    }

                    bool containsAnimationCriticalPath = false;
                    bool containsVfxCriticalPath = false;
                    HashSet<string>? uniqueHashes = null;

                    foreach (var key in applyNowPaths.Keys)
                    {
                        var gp = key.GamePath;
                        if (!containsAnimationCriticalPath && IsAnimationCriticalGamePath(gp))
                            containsAnimationCriticalPath = true;

                        if (!containsVfxCriticalPath && IsVfxCriticalGamePath(gp))
                            containsVfxCriticalPath = true;

                        var hash = key.Hash;
                        if (!string.IsNullOrWhiteSpace(hash))
                            (uniqueHashes ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase)).Add(hash);
                    }

                    if ((containsAnimationCriticalPath || containsVfxCriticalPath) && _charaHandler != null && _charaHandler.Address != nint.Zero)
                    {
                        var prime = new List<string>(64);

                        foreach (var k in applyNowPaths.Keys)
                        {
                            var gp = k.GamePath;
                            if (string.IsNullOrWhiteSpace(gp)) continue;

                            if (IsAnimationCriticalGamePath(gp) || IsVfxCriticalGamePath(gp))
                                prime.Add(gp);
                        }

                        if (prime.Count > 0)
                        {
                            Mediator.Publish(new PrimeTransientPathsMessage((IntPtr)_charaHandler.Address, ObjectKind.Player, prime));
                        }
                    }

                    var tempMods = BuildPenumbraTempMods(applyNowPaths);
                    var fingerprint = ComputeTempModsFingerprint(tempMods);

                    var mustRedrawForContent = containsAnimationCriticalPath || containsVfxCriticalPath || _redrawOnNextApplication;

                    if (!string.Equals(fingerprint, _lastAppliedTempModsFingerprint, StringComparison.Ordinal))
                    {
                        await _ipcManager.Penumbra
                            .SetTemporaryModsAsync(
                                Logger,
                                _applicationId,
                                _penumbraCollection,
                                tempMods)
                            .ConfigureAwait(false);

                        _lastAppliedTempModsFingerprint = fingerprint;
                        needsRedraw |= mustRedrawForContent;

                        long totalBytes = 0;
                        bool any = false;

                        if (uniqueHashes != null)
                        {
                            foreach (var hash in uniqueHashes)
                            {
                                token.ThrowIfCancellationRequested();

                                try
                                {
                                    var cache = _fileDbManager.GetFileCacheByHash(hash);
                                    var sz = cache?.Size;

                                    if (sz.HasValue && sz.Value > 0)
                                    {
                                        totalBytes += sz.Value;
                                        any = true;
                                    }
                                }
                                catch
                                {
                                }
                            }
                        }

                        LastAppliedDataBytes = any ? totalBytes : -1;

                        if (downloadedAny || containsAnimationCriticalPath)
                        {
                            Logger.LogDebug("[{applicationId}] Mods applied; redraw will be enforced", _applicationId);
                        }
                    }
                    else
                    {
                        needsRedraw |= mustRedrawForContent;
                        Logger.LogDebug("[{applicationId}] TempMods unchanged; skipping SetTemporaryModsAsync, but redraw may still be required", _applicationId);
                    }
                }

                if (updateManip)
                {
                    var newManipFingerprint = Pair.IsMetadataEnabled
                        ? ComputeManipulationFingerprint(charaData.ManipulationData)
                        : string.Empty;

                    if (!string.Equals(newManipFingerprint, _lastAppliedManipulationFingerprint, StringComparison.Ordinal))
                    {
                        if (Pair.IsMetadataEnabled)
                        {
                            await _ipcManager.Penumbra
                                .SetManipulationDataAsync(Logger, _applicationId, _penumbraCollection, charaData.ManipulationData)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            await _ipcManager.Penumbra
                                .ClearManipulationDataAsync(Logger, _applicationId, _penumbraCollection)
                                .ConfigureAwait(false);
                        }

                        _lastAppliedManipulationFingerprint = newManipFingerprint;
                        needsRedraw = true;
                    }
                    else
                    {
                        Logger.LogDebug("[{applicationId}] Manipulation data unchanged; skipping manipulation apply/redraw", _applicationId);
                    }
                }

                token.ThrowIfCancellationRequested();

                var playerGlamourerUpdated =
                    updatedData.TryGetValue(ObjectKind.Player, out var playerUpdateSet)
                    && playerUpdateSet.Contains(PlayerChanges.Glamourer);

                var forcedRedrawRequested =
                    updatedData.TryGetValue(ObjectKind.Player, out var playerSetForForced)
                    && playerSetForForced.Contains(PlayerChanges.ForcedRedraw);


                //var allowPlayerRedraw = downloadedAny || updateManip || collectionReassignedThisRun || forcedRedrawRequested;
                var allowPlayerRedraw = updateManip || forcedRedrawRequested;

                foreach (var kind in updatedData)
                {
                    needsRedraw |= await ApplyCustomizationDataAsync(_applicationId, kind, charaData, allowPlayerRedraw, token).ConfigureAwait(false);
                    token.ThrowIfCancellationRequested();
                }

                if (needsRedraw && _charaHandler != null && _charaHandler.Address != nint.Zero)
                {
                    var mustRedrawEvenWithGlamourer = allowPlayerRedraw || forcedRedrawRequested;

                    if (playerGlamourerUpdated && !mustRedrawEvenWithGlamourer)
                    {
                        Logger.LogDebug("[{applicationId}] Skipping explicit redraw: Glamourer updated with no Penumbra/forced/collection changes", _applicationId);
                    }
                    else
                    {
                        await OnePassRedrawAsync(_applicationId, token).ConfigureAwait(false);
                    }
                }

                _cachedData = charaData;

                var shouldPostRepair =
                    downloadedAny
                    || updateManip
                    || (updateModdedPaths && _hasRetriedAfterMissingDownload)
                    || (updateModdedPaths && _hasRetriedAfterMissingAtApply);

                if (shouldPostRepair)
                    RequestPostApplyRepair(charaData);
                _lastApplyCompletedTick = Environment.TickCount64;
                Logger.LogDebug("[{applicationId}] Application finished", _applicationId);
            }
            catch (OperationCanceledException)
            {
                Logger.LogDebug("[{applicationId}] Application cancelled", _applicationId);
            }
            catch (Exception ex)
            {
                if (ex is AggregateException aggr && aggr.InnerExceptions.Any(e => e is ArgumentNullException))
                {
                    IsVisible = false;
                    _forceApplyMods = true;
                    _cachedData = charaData;
                    Logger.LogDebug("[{applicationId}] Cancelled, player turned null during application", _applicationId);
                }
                else
                {
                    Logger.LogWarning(ex, "[{applicationId}] Error during application", _applicationId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogDebug("[BASE-{applicationBase}] Application cancelled before semaphore acquired for {player}",
                applicationBase, PlayerName);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "[BASE-{applicationBase}] Unhandled error during apply for {player}",
                applicationBase, PlayerName);
        }
        finally
        {
            EndPipeline(pipelineHash);

            if (acquired)
            {
                applySemaphore?.Release();
            }
        }
    }

    private static bool IsAnimationCriticalGamePath(string gamePath)
    {
        if (string.IsNullOrEmpty(gamePath)) return false;

        gamePath = gamePath.Trim();

        return gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsVfxCriticalGamePath(string gamePath)
    {
        if (string.IsNullOrEmpty(gamePath)) return false;

        gamePath = gamePath.Trim();

        return gamePath.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".skp", StringComparison.OrdinalIgnoreCase);
    }

    private bool HasAnyMissingCacheFiles(Guid applicationBase, CharacterData characterData)
    {
        try
        {
            var seenHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var kv in characterData.FileReplacements)
            {
                var list = kv.Value;
                if (list == null) continue;

                for (int i = 0; i < list.Count; i++)
                {
                    var item = list[i];
                    if (!string.IsNullOrEmpty(item.FileSwapPath))
                        continue;

                    var hash = item.Hash;
                    if (string.IsNullOrWhiteSpace(hash))
                        continue;

                    if (!seenHashes.Add(hash))
                        continue;

                    var fileCache = _fileDbManager.GetFileCacheByHash(hash);
                    if (fileCache == null)
                    {
                        Logger.LogDebug(
                            "[BASE-{appBase}] Detected missing cache entry for hash {hash} during apply self-check",
                            applicationBase,
                            hash);
                        return true;
                    }

                    var resolvedPath = fileCache.ResolvedFilepath;
                    if (string.IsNullOrEmpty(resolvedPath) || !File.Exists(resolvedPath))
                    {
                        Logger.LogDebug(
                            "[BASE-{appBase}] Detected missing cache file on disk for hash {hash} (path: {path}) during apply self-check",
                            applicationBase,
                            hash,
                            resolvedPath);
                        return true;
                    }

                    try
                    {
                        var fi = new FileInfo(resolvedPath);
                        if (fi.Length == 0)
                        {
                            if ((DateTime.UtcNow - fi.LastWriteTimeUtc) < TimeSpan.FromSeconds(10))
                                continue;

                            return true;
                        }
                    }
                    catch (IOException)
                    {
                        continue;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        continue;
                    }
                    catch
                    {
                        Logger.LogDebug(
                            "[BASE-{appBase}] Detected unreadable cache file for hash {hash} (path: {path}) during apply self-check",
                            applicationBase,
                            hash,
                            resolvedPath);
                        return true;
                    }
                }
            }
        }
        catch (IOException ex)
        {
            Logger.LogDebug(ex, "[BASE-{appBase}] IO during HasAnyMissingCacheFiles; treating as pass", applicationBase);
            return false;
        }
        catch (UnauthorizedAccessException ex)
        {
            Logger.LogDebug(ex, "[BASE-{appBase}] Access during HasAnyMissingCacheFiles; treating as pass", applicationBase);
            return false;
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "[BASE-{appBase}] Error during HasAnyMissingCacheFiles check, falling back to re-apply", applicationBase);
            return true;
        }

        return false;
    }

    private void BroadcastLocalOtherSyncYieldState(bool yieldToOtherSync, string owner)
    {
        owner ??= string.Empty;

        if (_lastBroadcastYield.HasValue
            && _lastBroadcastYield.Value == yieldToOtherSync
            && string.Equals(_lastBroadcastOwner, owner, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _lastBroadcastYield = yieldToOtherSync;
        _lastBroadcastOwner = owner;

        Mediator.Publish(new LocalOtherSyncYieldStateChangedMessage(yieldToOtherSync, owner));
    }

    private void FrameworkUpdate()
    {
        var nowTick = Environment.TickCount64;

        bool ShouldPollNow(int intervalMs)
        {
            if ((nowTick - _otherSyncPollTick) < intervalMs) return false;
            _otherSyncPollTick = nowTick;
            return true;
        }

        bool ShouldReleaseOtherSyncLatch()
        {
            const int ReleaseGraceMs = 400;

            if (_otherSyncReleaseCandidateSinceTick < 0)
            {
                _otherSyncReleaseCandidateSinceTick = nowTick;
                return false;
            }

            return (nowTick - _otherSyncReleaseCandidateSinceTick) >= ReleaseGraceMs;
        }

        void ClearOtherSyncReleaseCandidate()
        {
            _otherSyncReleaseCandidateSinceTick = -1;
        }

        nint ResolveOwnershipAddress()
        {
            var liveAddr = _charaHandler?.Address ?? nint.Zero;
            if (liveAddr != nint.Zero)
            {
                _lastKnownOwnershipAddr = liveAddr;
                return liveAddr;
            }

            var canUseTableFallback =
                !string.IsNullOrEmpty(PlayerName)
                || Interlocked.CompareExchange(ref _initializeStarted, 0, 0) != 0
                || Pair.AutoPausedByOtherSync;

            if (canUseTableFallback)
            {
                var pc = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
                if (pc != default((string, nint)) && pc.Address != nint.Zero)
                {
                    _lastKnownOwnershipAddr = pc.Address;
                    return pc.Address;
                }
            }
            return nint.Zero;
        }

        bool PollAndActOtherSync(string context, int pollIntervalMs)
        {
            if (Pair.RemoteOtherSyncOverrideActive)
            {
                var effectivePoll = Math.Min(pollIntervalMs, 250);
                if (!ShouldPollNow(effectivePoll))
                    return true;

                var remoteOwner = string.IsNullOrWhiteSpace(Pair.RemoteOtherSyncOwner) ? "OtherSync" : Pair.RemoteOtherSyncOwner;

                if (Pair.RemoteOtherSyncYield)
                {
                    var addrToCheck2 = ResolveOwnershipAddress();
                    if (addrToCheck2 != nint.Zero && !_ipcManager.OtherSync.TryGetOwningOtherSync(addrToCheck2, out _))
                    {
                        if (!ShouldReleaseOtherSyncLatch())
                            return true;

                        Pair.ClearRemoteOtherSyncOverride();

                        if (Pair.AutoPausedByOtherSync)
                        {
                            BroadcastLocalOtherSyncYieldState(yieldToOtherSync: false, owner: string.Empty);
                            HandleOtherSyncReleased(requestApplyIfPossible: true);
                            Mediator.Publish(new RefreshUiMessage());
                        }

                        ClearOtherSyncReleaseCandidate();
                        return false;
                    }

                    ClearOtherSyncReleaseCandidate();
                    EnterYieldedState(remoteOwner);
                    return true;
                }
                else
                {
                    if (Pair.AutoPausedByOtherSync)
                    {
                        BroadcastLocalOtherSyncYieldState(yieldToOtherSync: false, owner: string.Empty);
                        HandleOtherSyncReleased(requestApplyIfPossible: true);
                        Mediator.Publish(new RefreshUiMessage());
                    }

                    ClearOtherSyncReleaseCandidate();
                    return false;
                }
            }

            if (Pair.EffectiveOverrideOtherSync)
            {
                if (Pair.AutoPausedByOtherSync)
                {
                    BroadcastLocalOtherSyncYieldState(yieldToOtherSync: false, owner: string.Empty);
                    HandleOtherSyncReleased(requestApplyIfPossible: true);
                    return true;
                }

                ClearOtherSyncReleaseCandidate();
                return false;
            }

            var latched = Pair.AutoPausedByOtherSync;

            if (!ShouldPollNow(pollIntervalMs))
                return latched;

            var addrToCheck = ResolveOwnershipAddress();
            if (addrToCheck == nint.Zero)
            {
                _lastKnownOwnershipAddr = nint.Zero;

                if (latched)
                {
                    if (!ShouldReleaseOtherSyncLatch())
                        return true;

                    BroadcastLocalOtherSyncYieldState(yieldToOtherSync: false, owner: string.Empty);
                    HandleOtherSyncReleased(requestApplyIfPossible: true);
                    ClearOtherSyncReleaseCandidate();
                    return true;
                }

                ClearOtherSyncReleaseCandidate();
                return false;
            }

            if (_ipcManager.OtherSync.TryGetOwningOtherSync(addrToCheck, out var owner))
            {
                ClearOtherSyncReleaseCandidate();
                EnterYieldedState(owner);
                return true;
            }

            if (latched)
            {
                if (!ShouldReleaseOtherSyncLatch())
                    return true;

                BroadcastLocalOtherSyncYieldState(yieldToOtherSync: false, owner: string.Empty);
                HandleOtherSyncReleased(requestApplyIfPossible: true);
                ClearOtherSyncReleaseCandidate();
                return true;
            }

            ClearOtherSyncReleaseCandidate();
            return false;
        }

        var pollMs = Pair.AutoPausedByOtherSync ? 250 : 1000;
        if (PollAndActOtherSync("global", pollIntervalMs: pollMs))
            return;

        if (string.IsNullOrEmpty(PlayerName))
        {
            var pc = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
            if (pc == default((string, nint))) return;

            if (Pair.EffectiveOverrideOtherSync && Pair.AutoPausedByOtherSync)
            {
                Pair.AutoPausedByOtherSync = false;
                Pair.AutoPausedByOtherSyncName = string.Empty;
                Mediator.Publish(new RefreshUiMessage());
            }

            if (Interlocked.CompareExchange(ref _initializeStarted, 1, 0) == 0)
            {
                _initializeTask = Task.Run(async () =>
                {
                    try
                    {
                        await InitializeAsync(pc.Name).ConfigureAwait(false);

                        Mediator.Publish(new EventMessage(new Event(PlayerName!, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
                            $"Initializing User For Character {pc.Name}")));
                    }
                    catch (Exception)
                    {
                        Interlocked.Exchange(ref _initializeStarted, 0);
                    }
                });
            }

            return;
        }

        var addr = _charaHandler?.Address ?? nint.Zero;

        if (addr != nint.Zero)
        {
            _addressZeroSinceTick = -1;

            if (Pair.EffectiveOverrideOtherSync && Pair.AutoPausedByOtherSync)
            {
                Pair.AutoPausedByOtherSync = false;
                Pair.AutoPausedByOtherSyncName = string.Empty;
                _initialApplyPending = true;
                Mediator.Publish(new RefreshUiMessage());
            }

            if (!IsVisible)
            {
                IsVisible = true;

                SyncStorm.RegisterVisibleNow();

                _lastAssignedObjectIndex = null;
                _lastAssignedCollectionAssignUtc = DateTime.MinValue;

                _initialApplyPending = true;
            }
        }
        else if (IsVisible)
        {
            if (Pair.AutoPausedByOtherSync)
                _initialApplyPending = true;

            if (_addressZeroSinceTick < 0)
                _addressZeroSinceTick = nowTick;

            if (nowTick - _addressZeroSinceTick >= 750)
            {
                var pc = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
                if (pc == default((string, nint)))
                {
                    var oldIdx = _lastAssignedObjectIndex;

                    IsVisible = false;
                    _lastKnownOwnershipAddr = nint.Zero;

                    _lastAssignedObjectIndex = null;
                    _lastAssignedCollectionAssignUtc = DateTime.MinValue;
                    _lastAppliedTempModsFingerprint = null;
                    _lastAppliedManipulationFingerprint = null;

                    _initialApplyPending = false;

                    SchedulePenumbraCollectionTeardown();

                    _charaHandler?.Invalidate();
                    _downloadCancellationTokenSource?.CancelDispose();
                    _downloadCancellationTokenSource = null;

                    if (!Pair.AutoPausedByOtherSync && oldIdx.HasValue && oldIdx.Value >= 0)
                    {
                        var idxToClean = oldIdx.Value;

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await _ipcManager.Penumbra.AssignEmptyCollectionAsync(Logger, idxToClean).ConfigureAwait(false);
                                await _ipcManager.Honorific.ClearTitleByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                await _ipcManager.PetNames.ClearPlayerDataByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                await _ipcManager.Heels.UnregisterByObjectIndexAsync(idxToClean).ConfigureAwait(false);
                                await _ipcManager.CustomizePlus.RevertByObjectIndexAsync((ushort)idxToClean).ConfigureAwait(false);
                                await _ipcManager.Glamourer.RevertByObjectIndexAsync(Logger, idxToClean, _applicationId).ConfigureAwait(false);
                            }
                            catch
                            {
                            }
                        });
                    }

                    return;
                }

                _addressZeroSinceTick = nowTick;
            }
        }

        if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync) return;

        if (!IsVisible || !_initialApplyPending) return;
        if (_charaHandler == null || _cachedData == null) return;
        if (_charaHandler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None) return;

        _initialApplyPending = false;

        var nowTick2 = Environment.TickCount64;
        var cachedHash = _cachedData?.DataHash.Value;

        if (!string.IsNullOrEmpty(cachedHash))
        {
            if (IsSamePipelineStillRunning(cachedHash))
                return;

            if (string.Equals(cachedHash, _lastAttemptedDataHash, StringComparison.Ordinal)
                && (nowTick2 - _lastApplyCompletedTick) < 2500)
            {
                return;
            }
        }

        var appData = Guid.NewGuid();
        _redrawOnNextApplication = true;

        _ = _dalamudUtil.RunOnFrameworkThread(() =>
        {
            ApplyCharacterData(appData, _cachedData!, forceApplyCustomization: true);
        });
    }

    private async Task InitializeAsync(string name)
    {
        CancelPendingPenumbraCollectionTeardown();

        PlayerName = name;

        _charaHandler = await _gameObjectHandlerFactory.Create(
            ObjectKind.Player,
            () => _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident),
            isWatched: false).ConfigureAwait(false);

        _serverConfigManager.AutoPopulateNoteForUid(Pair.UserData.UID, name);

        Mediator.Subscribe<HonorificReadyMessage>(this, async (_) =>
        {
            if (string.IsNullOrEmpty(_cachedData?.HonorificData)) return;
            Logger.LogTrace("Reapplying Honorific data for {this}", this);
            await _ipcManager.Honorific.SetTitleAsync(PlayerCharacter, _cachedData.HonorificData).ConfigureAwait(false);
        });

        Mediator.Subscribe<PetNamesReadyMessage>(this, async (_) =>
        {
            if (string.IsNullOrEmpty(_cachedData?.PetNamesData)) return;
            Logger.LogTrace("Reapplying Pet Names data for {this}", this);
            await _ipcManager.PetNames.SetPlayerData(PlayerCharacter, _cachedData.PetNamesData).ConfigureAwait(false);
        });


        if (Pair.LastReceivedCharacterData != null)
        {
            _cachedData = Pair.LastReceivedCharacterData.DeepClone();
            _lastAttemptedDataHash = null;
            _forceApplyMods = true;
            _redrawOnNextApplication = true;
            _initialApplyPending = true;
            
        }
    }

    private async Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken)
    {
        nint address = _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident);
        if (address == nint.Zero) return;

        Logger.LogDebug("[{applicationId}] Reverting all Customization for {alias}/{name} {objectKind}", applicationId, Pair.UserData.AliasOrUID, name, objectKind);

        if (!_customizeIds.TryGetValue(objectKind, out var customizeId) || !customizeId.HasValue)
            customizeId = null;
        else
            _customizeIds.Remove(objectKind);

        if (objectKind == ObjectKind.Player)
        {
            using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Player, () => address, isWatched: false).ConfigureAwait(false);
            tempHandler.CompareNameAndThrow(name);
            Logger.LogDebug("[{applicationId}] Restoring Customization and Equipment for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
            tempHandler.CompareNameAndThrow(name);
            Logger.LogDebug("[{applicationId}] Restoring Heels for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            await _ipcManager.Heels.RestoreOffsetForPlayerAsync(address).ConfigureAwait(false);
            tempHandler.CompareNameAndThrow(name);
            Logger.LogDebug("[{applicationId}] Restoring C+ for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            if (customizeId.HasValue)
                await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
            tempHandler.CompareNameAndThrow(name);
            Logger.LogDebug("[{applicationId}] Restoring Honorific for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            await _ipcManager.Honorific.ClearTitleAsync(address).ConfigureAwait(false);
            Logger.LogDebug("[{applicationId}] Restoring Moodles for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            await _ipcManager.Moodles.RevertStatusAsync(address).ConfigureAwait(false);
            Logger.LogDebug("[{applicationId}] Restoring Pet Nicknames for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
            await _ipcManager.PetNames.ClearPlayerData(address).ConfigureAwait(false);
        }
        else if (objectKind == ObjectKind.MinionOrMount)
        {
            var minionOrMount = await _dalamudUtil.GetMinionOrMountAsync(address).ConfigureAwait(false);
            if (minionOrMount != nint.Zero)
            {
                if (customizeId.HasValue)
                    await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.MinionOrMount, () => minionOrMount, isWatched: false).ConfigureAwait(false);
                _ = _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken, fireAndForget: true);
                await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);

            }
        }
        else if (objectKind == ObjectKind.Pet)
        {
            var pet = await _dalamudUtil.GetPetAsync(address).ConfigureAwait(false);
            if (pet != nint.Zero)
            {
                if (customizeId.HasValue)
                    await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Pet, () => pet, isWatched: false).ConfigureAwait(false);
                await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
            }
        }
        else if (objectKind == ObjectKind.Companion)
        {
            var companion = await _dalamudUtil.GetCompanionAsync(address).ConfigureAwait(false);
            if (companion != nint.Zero)
            {
                if (customizeId.HasValue)
                    await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                //using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Pet, () => companion, isWatched: false).ConfigureAwait(false);
                using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Companion, () => companion, isWatched: false).ConfigureAwait(false);
                await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
            }
        }
    }

    private void ProcessNoSwapReplacement(Guid applicationBase, FileReplacementData item, ConcurrentBag<FileReplacementData> missingFiles, ConcurrentDictionary<(string GamePath, string? Hash), string> outputDict, ConcurrentDictionary<string, FileCacheEntity?> fileCacheMemo, object migrationLock, Action markMigrationChanged, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        var hash = item.Hash;
        if (string.IsNullOrWhiteSpace(hash))
            return;

        FileCacheEntity? GetCachedFileEntry(string h)
        {
            return fileCacheMemo.GetOrAdd(h, static (key, self) => self._fileDbManager.GetFileCacheByHash(key), this);
        }

        var gamePathsObj = item.GamePaths;
        if (gamePathsObj == null)
            return;

        object gamePathsStable = gamePathsObj is IList<string> ? gamePathsObj : (object)gamePathsObj.ToList();

        static bool TryGetFirstNonEmptyGamePath(object gamePathsObj, out string first)
        {
            first = string.Empty;

            if (gamePathsObj is IList<string> list)
            {
                for (int i = 0; i < list.Count; i++)
                {
                    var s = list[i];
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        first = s;
                        return true;
                    }
                }
                return false;
            }

            if (gamePathsObj is IEnumerable<string> enumerable)
            {
                foreach (var s in enumerable)
                {
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        first = s;
                        return true;
                    }
                }
            }

            return false;
        }

        static string GetExtensionOrDat(string gamePath)
        {
            var e = Path.GetExtension(gamePath);
            if (string.IsNullOrWhiteSpace(e) || e.Length <= 1) return "dat";
            return e.AsSpan(1).ToString();
        }

        var fileCache = GetCachedFileEntry(hash);
        if (fileCache == null)
        {
            missingFiles.Add(item);
            return;
        }

        var resolved = fileCache.ResolvedFilepath;
        if (string.IsNullOrWhiteSpace(resolved))
        {
            missingFiles.Add(item);
            return;
        }

        FileInfo fi;
        try
        {
            fi = new FileInfo(resolved);

            if (!fi.Exists)
            {
                missingFiles.Add(item);
                return;
            }

            if (fi.Length == 0)
            {
                if ((DateTime.UtcNow - fi.LastWriteTimeUtc) >= TimeSpan.FromSeconds(10))
                {
                    missingFiles.Add(item);
                    return;
                }
            }
        }
        catch (IOException)
        {
            fi = new FileInfo(resolved);
        }
        catch (UnauthorizedAccessException)
        {
            fi = new FileInfo(resolved);
        }
        catch
        {
            missingFiles.Add(item);
            return;
        }

        if (string.IsNullOrEmpty(fi.Extension))
        {
            if (TryGetFirstNonEmptyGamePath(gamePathsStable, out var firstGp))
            {
                var targetExt = GetExtensionOrDat(firstGp);

                lock (migrationLock)
                {
                    var cacheAgain = GetCachedFileEntry(hash);
                    if (cacheAgain != null)
                    {
                        var fi2 = new FileInfo(cacheAgain.ResolvedFilepath);
                        if (string.IsNullOrEmpty(fi2.Extension))
                        {
                            markMigrationChanged();
                            cacheAgain = _fileDbManager.MigrateFileHashToExtension(cacheAgain, targetExt);
                            fileCacheMemo[hash] = cacheAgain;
                            resolved = cacheAgain.ResolvedFilepath;
                        }
                        else
                        {
                            resolved = cacheAgain.ResolvedFilepath;
                        }
                    }
                }
            }
        }

        if (gamePathsStable is IList<string> gpList2)
        {
            for (int i = 0; i < gpList2.Count; i++)
            {
                var gp = gpList2[i];
                if (string.IsNullOrWhiteSpace(gp)) continue;
                outputDict[(gp, hash)] = resolved;
            }
        }
        else
        {
            foreach (var gp in (IEnumerable<string>)gamePathsStable)
            {
                if (string.IsNullOrWhiteSpace(gp)) continue;
                outputDict[(gp, hash)] = resolved;
            }
        }
    }

    private List<FileReplacementData> TryCalculateModdedDictionary(Guid applicationBase, CharacterData charaData, out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token)
    {
        var st = Stopwatch.StartNew();

        var missingFiles = new ConcurrentBag<FileReplacementData>();
        var outputDict = new ConcurrentDictionary<(string GamePath, string? Hash), string>(Environment.ProcessorCount, 256);
        var fileCacheMemo = new ConcurrentDictionary<string, FileCacheEntity?>(StringComparer.OrdinalIgnoreCase);

        int migrationChanges = 0;
        object migrationLock = new();
        void MarkMigrationChanged() => Interlocked.Exchange(ref migrationChanges, 1);

        moddedDictionary = new Dictionary<(string GamePath, string? Hash), string>();
        try
        {
            var plan = GetOrCreateReplacementPlan(charaData);
            var noSwap = plan.NoSwap;
            var swaps = plan.Swaps;

            const int ParallelThreshold = 48;

            if (noSwap.Length < ParallelThreshold)
            {
                for (int i = 0; i < noSwap.Length; i++)
                { 
                    token.ThrowIfCancellationRequested();

                    ProcessNoSwapReplacement(
                        applicationBase,
                        noSwap[i],
                        missingFiles,
                        outputDict,
                        fileCacheMemo,
                        migrationLock,
                        MarkMigrationChanged,
                        token);
                }
            }
            else
            {
                Parallel.ForEach(
                    noSwap,
                    new ParallelOptions
                    {
                        CancellationToken = token,
                        MaxDegreeOfParallelism = 4
                    },
                    item =>
                    {
                        ProcessNoSwapReplacement(
                            applicationBase,
                            item,
                            missingFiles,
                            outputDict,
                            fileCacheMemo,
                            migrationLock,
                            MarkMigrationChanged,
                            token);
                    });
            }

            for (int i = 0; i < swaps.Length; i++)
            {
                var item = swaps[i];

                var gamePaths = item.GamePaths;
                if (gamePaths == null) continue;

                var swap = item.FileSwapPath!;
                if (Path.IsPathRooted(swap) ||
                    swap.Contains(":\\", StringComparison.Ordinal) ||
                    swap.StartsWith("\\", StringComparison.Ordinal))
                {
                    Logger.LogWarning("[BASE-{appBase}] Ignoring invalid FileSwapPath that looks like a filesystem path: {swap}", applicationBase, swap);
                    continue;
                }

                if (gamePaths is IList<string> gpList)
                {
                    for (int j = 0; j < gpList.Count; j++)
                    {
                        var gp = gpList[j];
                        if (string.IsNullOrWhiteSpace(gp)) continue;
                        if (string.Equals(gp, swap, StringComparison.OrdinalIgnoreCase)) continue;
                        outputDict[(gp, null)] = swap;
                    }
                }
                else
                {
                    foreach (var gp in gamePaths)
                    {
                        if (string.IsNullOrWhiteSpace(gp)) continue;
                        if (string.Equals(gp, swap, StringComparison.OrdinalIgnoreCase)) continue;
                        outputDict[(gp, null)] = swap;
                    }
                }
            }

            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        catch (OperationCanceledException)
        {
            Logger.LogTrace("[BASE-{appBase}] Replacement calculation cancelled", applicationBase);
            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "[BASE-{appBase}] Something went wrong during calculation replacements", applicationBase);
            moddedDictionary = outputDict.ToDictionary(k => k.Key, k => k.Value);
        }
        finally
        {
            if (migrationChanges == 1)
                _fileDbManager.WriteOutFullCsv();

            st.Stop();

            Logger.LogDebug(
                "[BASE-{appBase}] ModdedPaths calculated in {time}ms, missing files: {count}, total files: {total}",
                applicationBase, st.ElapsedMilliseconds, missingFiles.Count, moddedDictionary.Count);
        }

        return missingFiles.ToList();
    }

    private static string ComputeTempModsFingerprint(Dictionary<string, string> tempMods)
    {
        if (tempMods.Count == 0) return "EMPTY";

        using var sha1 = SHA1.Create();

        var bytePool = ArrayPool<byte>.Shared;
        var keyPool = ArrayPool<string>.Shared;

        var rentedKeys = keyPool.Rent(tempMods.Count);
        var keyCount = 0;

        try
        {
            foreach (var key in tempMods.Keys)
                rentedKeys[keyCount++] = key;

            Array.Sort(rentedKeys, 0, keyCount, StringComparer.Ordinal);

            for (int i = 0; i < keyCount; i++)
            {
                var key = rentedKeys[i] ?? string.Empty;
                var val = tempMods[key] ?? string.Empty;

                var keyByteCount = Encoding.UTF8.GetByteCount(key);
                var valByteCount = Encoding.UTF8.GetByteCount(val);
                var total = keyByteCount + 1 + valByteCount + 1;

                var buf = bytePool.Rent(total);
                try
                {
                    var offset = 0;

                    offset += Encoding.UTF8.GetBytes(key, 0, key.Length, buf, offset);
                    buf[offset++] = (byte)'\n';

                    offset += Encoding.UTF8.GetBytes(val, 0, val.Length, buf, offset);
                    buf[offset++] = (byte)'\n';

                    sha1.TransformBlock(buf, 0, offset, null, 0);
                }
                finally
                {
                    bytePool.Return(buf);
                }
            }

            sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            return Convert.ToHexString(sha1.Hash!);
        }
        finally
        {
            Array.Clear(rentedKeys, 0, keyCount);
            keyPool.Return(rentedKeys);
        }
    }
    
    private static string ComputeManipulationFingerprint(string? manipulationData)
    {
        return manipulationData?.Trim() ?? string.Empty;
    }

    private Dictionary<string, string> BuildPenumbraTempMods(Dictionary<(string GamePath, string? Hash), string> moddedPaths)
    {
        var output = new Dictionary<string, string>(moddedPaths.Count, StringComparer.Ordinal);
        var winnerHashes = new Dictionary<string, string?>(moddedPaths.Count, StringComparer.Ordinal);

        foreach (var kvp in moddedPaths)
        {
            var gamePath = kvp.Key.GamePath;
            if (string.IsNullOrWhiteSpace(gamePath)) continue;

            var path = kvp.Value;
            if (string.IsNullOrWhiteSpace(path)) continue;

            var incomingHash = kvp.Key.Hash;

            if (!output.TryGetValue(gamePath, out var existingPath))
            {
                output[gamePath] = path;
                winnerHashes[gamePath] = incomingHash;
                continue;
            }

            var existingHash = winnerHashes[gamePath];
            var existingIsSwap = string.IsNullOrEmpty(existingHash);
            var incomingIsSwap = string.IsNullOrEmpty(incomingHash);

            if (existingIsSwap && !incomingIsSwap)
            {
                // hashed beats swap
                output[gamePath] = path;
                winnerHashes[gamePath] = incomingHash;
                Logger.LogDebug("TempMods conflict for {gp}: swap replaced by hashed entry", gamePath);
                continue;
            }

            if (!existingIsSwap && incomingIsSwap)
            {
                // keep existing hashed
                Logger.LogDebug("TempMods conflict for {gp}: ignoring swap because hashed entry already exists", gamePath);
                continue;
            }

            // Both swap or both hashed: keep first deterministically, but log if different
            if (!string.Equals(existingPath, path, StringComparison.OrdinalIgnoreCase))
                Logger.LogDebug("TempMods conflict for {gp}: keeping {old}, ignoring {new}", gamePath, existingPath, path);
        }

        return output;
    }


    public void RequestManualFileRepair()
    {
        if (_cachedData == null)
        {
            Logger.LogInformation("Manual repair requested for {pair} but no cached data is present yet", Pair);
            Mediator.Publish(new EventMessage(new Event(
                PlayerName,
                Pair.UserData,
                nameof(PairHandler),
                EventSeverity.Warning,
                "RavaSync: Cannot verify files for this user yet (no cached data).")));
            return;
        }

        if (_charaHandler == null || PlayerCharacter == nint.Zero)
        {
            Logger.LogInformation("Manual repair requested for {pair} but character is not currently valid", Pair);
            Mediator.Publish(new EventMessage(new Event(
                PlayerName,
                Pair.UserData,
                nameof(PairHandler),
                EventSeverity.Warning,
                "RavaSync: Cannot verify files while this user is not visible/loaded.")));
            return;
        }

        // prevent overlap / spam click
        if (Interlocked.Exchange(ref _manualRepairRunning, 1) == 1)
        {
            Logger.LogInformation("Manual repair already running for {pair}, ignoring duplicate request", Pair);
            return;
        }

        var appBase = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            try
            {
                await ManualVerifyAndRepairAsync(appBase, _cachedData!.DeepClone(), cts.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Logger.LogInformation("[BASE-{appBase}] Manual verify/repair for {pair} was cancelled or timed out",
                    appBase, Pair.UserData.AliasOrUID);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "[BASE-{appBase}] Error during manual verify/repair for {pair}",
                    appBase, Pair.UserData.AliasOrUID);
            }
            finally
            {
                Interlocked.Exchange(ref _manualRepairRunning, 0);
            }
        });
    }

    private async Task ManualVerifyAndRepairAsync(Guid applicationBase, CharacterData charaData, CancellationToken token, bool verifyFileHashes = true, bool publishEvents = true)
    {
        if (publishEvents)
        {
            Logger.LogInformation("[BASE-{appBase}] Starting manual verify/repair for {pair}",applicationBase, Pair.UserData.AliasOrUID);
        }

        Dictionary<(string GamePath, string? Hash), string> moddedPaths;
        var missing = TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, token);

        var invalidHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pathByHash = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in moddedPaths)
        {
            var hash = kvp.Key.Hash;
            if (string.IsNullOrEmpty(hash)) continue;

            pathByHash.TryAdd(hash, kvp.Value);
        }

        foreach (var item in missing)
        {
            if (string.IsNullOrWhiteSpace(item.Hash))
                continue;

            invalidHashes.Add(item.Hash);
        }


        foreach (var (hash, path) in pathByHash)
        {
            if (string.IsNullOrWhiteSpace(hash)) continue;
            
            token.ThrowIfCancellationRequested();

            try
            {
                var fi = new FileInfo(path);
                if (!fi.Exists || fi.Length == 0)
                {
                    if (publishEvents)
                    {
                        Logger.LogDebug("[BASE-{appBase}] Manual validation: file for {hash} missing or zero-length at {path}",applicationBase, hash, path);
                    }
       
                    invalidHashes.Add(hash);
                    continue;
                }
                if (verifyFileHashes)
                {
                    var computed = Crypto.GetFileHash(fi.FullName);

                    if (!string.Equals(computed, hash, StringComparison.OrdinalIgnoreCase))
                    {
                        Logger.LogWarning("[BASE-{appBase}] hash mismatch for {hash} at {path}, computed {computed}", applicationBase, hash, fi.FullName, computed);
                        invalidHashes.Add(hash);
                    }
                }
            }
            catch (IOException)
            {

                continue;
            }
            catch (UnauthorizedAccessException)
            {
                continue;
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[BASE-{appBase}] Manual validation threw for hash {hash} at {path}", applicationBase, hash, path);
                invalidHashes.Add(hash);
            }

        }

        if (invalidHashes.Count == 0)
        {
            if (publishEvents)
            {
                Logger.LogInformation(
                    "[BASE-{appBase}] No missing or corrupt files detected for {pair}",
                    applicationBase, Pair.UserData.AliasOrUID);
            }
            else
            {
                Logger.LogDebug(
                    "[BASE-{appBase}] Background post-apply verification found no missing or corrupt files for {pair}",
                    applicationBase, Pair.UserData.AliasOrUID);
            }

            if (publishEvents)
            {
                Mediator.Publish(new EventMessage(new Event(
                    PlayerName,
                    Pair.UserData,
                    nameof(PairHandler),
                    EventSeverity.Informational,
                    "RavaSync: File verification complete — no issues detected for this user.")));
            }

            return;
        }

        foreach (var badHash in invalidHashes)
        {
            try
            {
                var entry = _fileDbManager.GetFileCacheByHash(badHash);
                if (entry == null) continue;

                try
                {
                    if (!string.IsNullOrEmpty(entry.ResolvedFilepath)
                        && File.Exists(entry.ResolvedFilepath))
                    {
                        Logger.LogWarning(
                            "[BASE-{appBase}] Manual repair: deleting invalid cache file for {hash} at {path}",
                            applicationBase, badHash, entry.ResolvedFilepath);
                        File.Delete(entry.ResolvedFilepath);
                    }
                }
                catch (Exception exDel)
                {
                    Logger.LogWarning(
                        exDel,
                        "[BASE-{appBase}] Manual repair: failed to delete invalid cache file for {hash} at {path}",
                        applicationBase, badHash, entry.ResolvedFilepath);
                }

                try
                {
                    _fileDbManager.RemoveHashedFile(entry.Hash, entry.PrefixedFilePath);
                }
                catch (Exception exDb)
                {
                    Logger.LogWarning(
                        exDb,
                        "[BASE-{appBase}] Manual repair: failed to remove cache DB entry for {hash}",
                        applicationBase, badHash);
                }
            }
            catch (Exception ex)
            {
                Logger.LogWarning(
                    ex,
                    "[BASE-{appBase}] Manual repair: error while cleaning up invalid cache for {hash}",
                    applicationBase, badHash);
            }
        }

        Logger.LogWarning(
            "[BASE-{appBase}] {count} missing/corrupt cache files detected for {pair}; starting repair download",
            applicationBase, invalidHashes.Count, Pair.UserData.AliasOrUID);

        if (publishEvents)
        {
            Mediator.Publish(new EventMessage(new Event(
                PlayerName,
                Pair.UserData,
                nameof(PairHandler),
                EventSeverity.Warning,
                $"RavaSync: Detected {invalidHashes.Count} missing/corrupt cache files; starting repair download.")));
        }

        await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            _downloadManager.ClearDownload();
            _pairDownloadTask = null;

            _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
            _downloadCancellationTokenSource ??= new CancellationTokenSource();

            _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();
            _applicationCancellationTokenSource ??= new CancellationTokenSource();

            _hasRetriedAfterMissingDownload = true;
            _hasRetriedAfterMissingAtApply = true;
            _forceApplyMods = true;

            ApplyCharacterData(applicationBase, charaData, forceApplyCustomization: true);
        }).ConfigureAwait(false);

    }

    private bool TryGetRecentMissingCheck(string dataHash, out bool hadMissing)
    {
        lock (_missingCheckGate)
        {
            // treat as fresh for 5 seconds
            if (string.Equals(_lastMissingCheckedHash, dataHash, StringComparison.Ordinal)
                && (Environment.TickCount64 - _lastMissingCheckedTick) < 5000)
            {
                hadMissing = _lastMissingCheckHadMissing;
                return true;
            }
        }

        hadMissing = false;
        return false;
    }

    private void ScheduleMissingCheck(Guid applicationBase, CharacterData characterData)
    {
        var hash = characterData.DataHash.Value;
        var dataCopy = characterData.DeepClone();

        // If a check is already running, coalesce to the latest request and return.
        if (Interlocked.CompareExchange(ref _missingCheckRunning, 1, 0) != 0)
        {
            lock (_missingCheckGate)
            {
                _pendingMissingCheckHash = hash;
                _pendingMissingCheckData = dataCopy;
                _pendingMissingCheckBase = applicationBase;
            }

            Logger.LogTrace("[BASE-{appBase}] Missing-check already running; queued latest hash {hash}", applicationBase, hash);
            return;
        }

        _ = Task.Run(() =>
        {
            try
            {
                Guid currentBase = applicationBase;
                string currentHash = hash;
                CharacterData currentData = dataCopy;

                while (true)
                {
                    try
                    {
                        var missing = HasAnyMissingCacheFiles(currentBase, currentData);

                        lock (_missingCheckGate)
                        {
                            _lastMissingCheckedHash = currentHash;
                            _lastMissingCheckedTick = Environment.TickCount64;
                            _lastMissingCheckHadMissing = missing;
                        }

                        if (missing)
                        {
                            var applyCopy = currentData.DeepClone();
                            _ = _dalamudUtil.RunOnFrameworkThread(() =>
                            {
                                ApplyCharacterData(Guid.NewGuid(), applyCopy, forceApplyCustomization: true);
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogTrace(ex, "[BASE-{appBase}] Missing-check failed for hash {hash}", currentBase, currentHash);
                    }

                    // Drain one queued request (latest wins). If none queued, we're done.
                    lock (_missingCheckGate)
                    {
                        if (_pendingMissingCheckData == null || string.IsNullOrEmpty(_pendingMissingCheckHash))
                            break;

                        currentBase = _pendingMissingCheckBase;
                        currentHash = _pendingMissingCheckHash;
                        currentData = _pendingMissingCheckData;

                        _pendingMissingCheckBase = Guid.Empty;
                        _pendingMissingCheckHash = null;
                        _pendingMissingCheckData = null;
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref _missingCheckRunning, 0);

                // Tiny race guard: if something queued right after we dropped the flag, kick another runner.
                lock (_missingCheckGate)
                {
                    if (_pendingMissingCheckData != null && !string.IsNullOrEmpty(_pendingMissingCheckHash))
                    {
                        var queuedBase = _pendingMissingCheckBase;
                        var queuedData = _pendingMissingCheckData;
                        _pendingMissingCheckBase = Guid.Empty;
                        _pendingMissingCheckHash = null;
                        _pendingMissingCheckData = null;

                        if (queuedData != null)
                            ScheduleMissingCheck(queuedBase, queuedData);
                    }
                }
            }
        });
    }

    private void RequestPostApplyRepair(CharacterData appliedData)
    {
        var hash = appliedData.DataHash.Value;
        var now = Environment.TickCount64;

        // Per-hash cooldown
        if (string.Equals(_lastPostApplyRepairHash, hash, StringComparison.Ordinal)
            && (now - _lastPostApplyRepairTick) < 30000)
            return;

        var dataCopy = appliedData.DeepClone();

        // If one is already active for this pair, coalesce to the latest request and return.
        if (Interlocked.CompareExchange(ref _manualRepairRunning, 1, 0) != 0)
        {
            lock (_postRepairGate)
            {
                _pendingPostApplyRepairHash = hash;
                _pendingPostApplyRepairData = dataCopy;
            }

            Logger.LogTrace("Post-apply repair already running for {pair}; queued latest hash {hash}", Pair, hash);
            return;
        }

        _lastPostApplyRepairHash = hash;
        _lastPostApplyRepairTick = now;

        _ = Task.Run(async () =>
        {
            try
            {
                CharacterData currentData = dataCopy;
                string currentHash = hash;

                while (true)
                {
                    var applicationBase = Guid.NewGuid();
                    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
                    var acquired = false;

                    try
                    {
                        await Task.Delay(1500, cts.Token).ConfigureAwait(false);

                        await GlobalPostApplyRepairSemaphore.WaitAsync(cts.Token).ConfigureAwait(false);
                        acquired = true;

                        await ManualVerifyAndRepairAsync(
                            applicationBase,
                            currentData,
                            cts.Token,
                            verifyFileHashes: false,
                            publishEvents: false).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "[BASE-{appBase}] Post-apply repair failed", applicationBase);
                    }
                    finally
                    {
                        if (acquired)
                            GlobalPostApplyRepairSemaphore.Release();
                    }

                    lock (_postRepairGate)
                    {
                        if (_pendingPostApplyRepairData == null || string.IsNullOrEmpty(_pendingPostApplyRepairHash))
                            break;

                        currentData = _pendingPostApplyRepairData;
                        currentHash = _pendingPostApplyRepairHash;

                        _pendingPostApplyRepairData = null;
                        _pendingPostApplyRepairHash = null;
                    }

                    _lastPostApplyRepairHash = currentHash;
                    _lastPostApplyRepairTick = Environment.TickCount64;
                }
            }
            finally
            {
                Interlocked.Exchange(ref _manualRepairRunning, 0);

                // Tiny race guard
                CharacterData? queuedData = null;
                string? queuedHash = null;

                lock (_postRepairGate)
                {
                    if (_pendingPostApplyRepairData != null && !string.IsNullOrEmpty(_pendingPostApplyRepairHash))
                    {
                        queuedData = _pendingPostApplyRepairData;
                        queuedHash = _pendingPostApplyRepairHash;
                        _pendingPostApplyRepairData = null;
                        _pendingPostApplyRepairHash = null;
                    }
                }

                if (queuedData != null && queuedHash != null)
                {
                    RequestPostApplyRepair(queuedData);
                }
            }
        });
    }
    public void ReclaimFromOtherSync(bool requestApplyIfPossible, bool treatAsFirstVisible)
    {
        Pair.AutoPausedByOtherSync = false;
        Pair.AutoPausedByOtherSyncName = string.Empty;

        CancelQueuedApplyWork();

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        Pair.SetCurrentDownloadStatus(null);

        if (treatAsFirstVisible)
        {
            ResetToUninitializedState();
            Mediator.Publish(new RefreshUiMessage());
            return;
        }

        if (_charaHandler != null && _charaHandler.Address != nint.Zero && !string.IsNullOrEmpty(PlayerName))
        {
            if (Pair.LastReceivedCharacterData != null)
                _cachedData = Pair.LastReceivedCharacterData.DeepClone();

            _lastAttemptedDataHash = null;
            _lastAppliedTempModsFingerprint = null;
            _lastAppliedManipulationFingerprint = null;

            _hasRetriedAfterMissingDownload = false;
            _hasRetriedAfterMissingAtApply = false;

            _forceApplyMods = true;
            _redrawOnNextApplication = true;
            
            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;
            _customizeIds.Clear();

            _addressZeroSinceTick = -1;
            _initialApplyPending = true;
            _otherSyncReleaseCandidateSinceTick = -1;
            _lastKnownOwnershipAddr = nint.Zero;

            if (requestApplyIfPossible)
            {
                if (!IsVisible)
                {
                    IsVisible = true;
                    SyncStorm.RegisterVisibleNow();
                }

                _ = _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    Pair.ApplyLastReceivedData(forced: true);
                });
            }

            Mediator.Publish(new RefreshUiMessage());
            return;
        }

        ResetToUninitializedState();
        Mediator.Publish(new RefreshUiMessage());
    }

    private void HandleOtherSyncReleased(bool requestApplyIfPossible)
        => ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible: true);

    private void ResetToUninitializedState()
    {
        CancelQueuedApplyWork();

        _cachedData ??= Pair.LastReceivedCharacterData?.DeepClone();

        _lastAttemptedDataHash = null;
        _lastAppliedTempModsFingerprint = null;
        _lastAppliedManipulationFingerprint = null;

        _hasRetriedAfterMissingDownload = false;
        _hasRetriedAfterMissingAtApply = false;

        _forceApplyMods = true;
        _redrawOnNextApplication = true;

        _lastAssignedObjectIndex = null;
        _lastAssignedCollectionAssignUtc = DateTime.MinValue;
        _customizeIds.Clear();

        CancelPendingPenumbraCollectionTeardown();
        _ = Task.Run(() => RemovePenumbraCollectionAsync(Guid.NewGuid()));

        _addressZeroSinceTick = -1;
        _initialApplyPending = true;
        _otherSyncReleaseCandidateSinceTick = -1;
        _lastKnownOwnershipAddr = nint.Zero;

        PlayerName = string.Empty;

        _charaHandler?.Dispose();
        _charaHandler = null;

        Interlocked.Exchange(ref _initializeStarted, 0);
        _initializeTask = null;

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        Pair.SetCurrentDownloadStatus(null);

        IsVisible = false;

        _lastBroadcastYield = null;
        _lastBroadcastOwner = string.Empty;
        
    }


    private void CancelPendingPenumbraCollectionTeardown()
    {
        _penumbraCollectionTeardownCts?.CancelDispose();
        _penumbraCollectionTeardownCts = null;
    }

    private void SchedulePenumbraCollectionTeardown()
    {
        CancelPendingPenumbraCollectionTeardown();

        if (_penumbraCollection == Guid.Empty && _penumbraCollectionTask == null)
            return;

        var cts = new CancellationTokenSource();
        _penumbraCollectionTeardownCts = cts;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(PenumbraCollectionTeardownDelay, cts.Token).ConfigureAwait(false);

                if (cts.IsCancellationRequested || IsVisible || _lifetime.ApplicationStopping.IsCancellationRequested)
                    return;

                await RemovePenumbraCollectionAsync(Guid.NewGuid()).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Delayed temp collection teardown failed for {pair}", Pair);
            }
            finally
            {
                if (ReferenceEquals(_penumbraCollectionTeardownCts, cts))
                    _penumbraCollectionTeardownCts = null;

                cts.Dispose();
            }
        });
    }

    private async Task EnsurePenumbraCollectionAsync()
    {
        CancelPendingPenumbraCollectionTeardown();

        if (_penumbraCollection != Guid.Empty)
            return;

        _penumbraCollectionTask ??= _ipcManager.Penumbra.CreateTemporaryCollectionAsync(Logger, Pair.UserData.UID);
        _penumbraCollection = await _penumbraCollectionTask.ConfigureAwait(false);
    }

    private async Task RemovePenumbraCollectionAsync(Guid applicationId)
    {
        CancelPendingPenumbraCollectionTeardown();

        var coll = _penumbraCollection;
        var collTask = _penumbraCollectionTask;

        _penumbraCollection = Guid.Empty;
        _penumbraCollectionTask = null;

        if (coll == Guid.Empty && collTask != null)
        {
            try
            {
                coll = await collTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Temp collection creation task faulted while tearing down for {pair}", Pair);
                return;
            }
        }

        if (coll == Guid.Empty)
            return;

        await _ipcManager.Penumbra.RemoveTemporaryCollectionAsync(Logger, applicationId, coll).ConfigureAwait(false);
    }

    private static List<FileReplacementData> DeduplicateReplacementsByHash(List<FileReplacementData> input)
    {
        if (input.Count <= 1)
            return input;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<FileReplacementData>(input.Count);

        for (int i = 0; i < input.Count; i++)
        {
            var item = input[i];
            var hash = item.Hash;

            if (string.IsNullOrWhiteSpace(hash))
            {
                result.Add(item);
                continue;
            }

            if (seen.Add(hash))
                result.Add(item);
        }

        return result;
    }

    private static string GetFirstGamePathOrFallback(FileReplacementData replacement)
    {
        var gamePaths = replacement.GamePaths;
        if (gamePaths == null)
            return "nogamepath";

        if (gamePaths is IList<string> list)
        {
            for (int i = 0; i < list.Count; i++)
            {
                var p = list[i];
                if (!string.IsNullOrWhiteSpace(p))
                    return p;
            }

            return "nogamepath";
        }

        foreach (var p in gamePaths)
        {
            if (!string.IsNullOrWhiteSpace(p))
                return p;
        }

        return "nogamepath";
    }

    private static string BuildMissingDetails(List<FileReplacementData> replacements)
    {
        if (replacements.Count == 0)
            return string.Empty;

        var sb = new StringBuilder();

        for (int i = 0; i < replacements.Count; i++)
        {
            var r = replacements[i];
            if (i > 0)
                sb.Append(", ");

            sb.Append(r.Hash);
            sb.Append(':');
            sb.Append(GetFirstGamePathOrFallback(r));
        }

        return sb.ToString();
    }

    private static Dictionary<string, FileDownloadStatus> SnapshotStatus(Dictionary<string, FileDownloadStatus>? src)
    {
        if (src == null || src.Count == 0)
            return new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);

        var dst = new Dictionary<string, FileDownloadStatus>(src.Count, StringComparer.Ordinal);

        foreach (var kv in src)
        {
            var s = kv.Value;
            if (s == null) continue;

            dst[kv.Key] = new FileDownloadStatus
            {
                DownloadStatus = s.DownloadStatus,
                TotalBytes = s.TotalBytes,
                TransferredBytes = s.TransferredBytes,
                TotalFiles = s.TotalFiles,
                TransferredFiles = s.TransferredFiles
            };
        }

        return dst;
    }

    private bool TryBeginPipeline(string dataHash)
    {
        lock (_pipelineGate)
        {
            var pipelineRunning =
                (_pairDownloadTask != null && !_pairDownloadTask.IsCompleted)
                || (_applicationTask != null && !_applicationTask.IsCompleted);

            if (pipelineRunning && string.Equals(_activePipelineHash, dataHash, StringComparison.Ordinal))
            {
                return false;
            }

            _activePipelineHash = dataHash;
            return true;
        }
    }

    private void EndPipeline(string dataHash)
    {
        lock (_pipelineGate)
        {
            if (string.Equals(_activePipelineHash, dataHash, StringComparison.Ordinal))
            {
                _activePipelineHash = null;
            }
        }
    }

    private bool IsSamePipelineStillRunning(string dataHash)
    {
        lock (_pipelineGate)
        {
            var pipelineRunning =
                (_pairDownloadTask != null && !_pairDownloadTask.IsCompleted)
                || (_applicationTask != null && !_applicationTask.IsCompleted);

            return pipelineRunning && string.Equals(_activePipelineHash, dataHash, StringComparison.Ordinal);
        }
    }

    private void CancelQueuedApplyWork()
    {
        _applyCoalesceCts?.CancelDispose();
        _applyCoalesceCts = null;
        _applyCoalesceTask = null;

        _applyQueuedBase = Guid.Empty;
        _applyQueuedData = null;
        _applyQueuedForce = false;

        _dataReceivedInDowntime = null;

        _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate();
        _applicationCancellationTokenSource = _applicationCancellationTokenSource?.CancelRecreate();

        try { _downloadManager.ClearDownload(); } catch { /* best effort */ }
        _pairDownloadTask = null;

        lock (_pipelineGate)
        {
            _activePipelineHash = null;
        }
    }

    private void EnterYieldedState(string owner)
    {
        owner ??= string.Empty;

        var alreadySameOwner =
            Pair.AutoPausedByOtherSync &&
            string.Equals(Pair.AutoPausedByOtherSyncName, owner, StringComparison.OrdinalIgnoreCase);

        var alreadyQuiesced =
            string.IsNullOrEmpty(PlayerName) &&
            _charaHandler == null &&
            !IsVisible &&
            Interlocked.CompareExchange(ref _initializeStarted, 0, 0) == 0 &&
            !_initialApplyPending;

        if (alreadySameOwner && alreadyQuiesced)
            return;

        Pair.AutoPausedByOtherSync = true;
        Pair.AutoPausedByOtherSyncName = owner;

        BroadcastLocalOtherSyncYieldState(yieldToOtherSync: true, owner: owner);

        CancelQueuedApplyWork();

        Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        Pair.SetCurrentDownloadStatus(null);

        _initialApplyPending = false;
        _redrawOnNextApplication = false;
        _addressZeroSinceTick = -1;

        _lastAttemptedDataHash = null;
        _lastAppliedTempModsFingerprint = null;
        _lastAppliedManipulationFingerprint = null;

        PlayerName = string.Empty;

        if (_charaHandler != null)
        {
            _charaHandler.Dispose();
            _charaHandler = null;
        }

        CancelPendingPenumbraCollectionTeardown();
        _ = Task.Run(() => RemovePenumbraCollectionAsync(Guid.NewGuid()));

        Interlocked.Exchange(ref _initializeStarted, 0);
        _initializeTask = null;

        if (IsVisible)
            IsVisible = false;
        else
            Mediator.Publish(new RefreshUiMessage());
    }

}