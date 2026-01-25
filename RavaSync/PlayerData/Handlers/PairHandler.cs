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
using System.Collections.Concurrent;
using System.Diagnostics;
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
    private bool _redrawOnNextApplication = false;
    private bool _hasRetriedAfterMissingDownload = false;
    private bool _hasRetriedAfterMissingAtApply = false;
    private int _manualRepairRunning = 0;
    private int? _lastAssignedObjectIndex = null;
    private DateTime _lastAssignedCollectionAssignUtc = DateTime.MinValue;
    private static readonly SemaphoreSlim GlobalApplySemaphore = new(3, 3);
    private string? _lastAttemptedDataHash;
    private string? _lastAppliedTempModsFingerprint;

    private readonly object _missingCheckGate = new();
    private string? _lastMissingCheckedHash;
    private long _lastMissingCheckedTick;
    private bool _lastMissingCheckHadMissing;
    private int _missingCheckRunning;

    private string? _lastPostApplyRepairHash;
    private long _lastPostApplyRepairTick;



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
        _penumbraCollection = _ipcManager.Penumbra.CreateTemporaryCollectionAsync(logger, Pair.UserData.UID).ConfigureAwait(false).GetAwaiter().GetResult();

        Mediator.Subscribe<FrameworkUpdateMessage>(this, (_) => FrameworkUpdate());
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, (_) =>
        {
            _downloadCancellationTokenSource?.CancelDispose();
            _charaHandler?.Invalidate();
            IsVisible = false;
        });
        Mediator.Subscribe<PenumbraInitializedMessage>(this, (_) =>
        {
            _penumbraCollection = _ipcManager.Penumbra.CreateTemporaryCollectionAsync(logger, Pair.UserData.UID).ConfigureAwait(false).GetAwaiter().GetResult();

            _lastAppliedTempModsFingerprint = null;
            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;

            if (!IsVisible && _charaHandler != null)
            {
                PlayerName = string.Empty;
                _charaHandler.Dispose();
                _charaHandler = null;
            }
        });
        Mediator.Subscribe<ClassJobChangedMessage>(this, (msg) =>
        {
            if (msg.GameObjectHandler == _charaHandler)
            {
                _redrawOnNextApplication = true;
            }
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
            var hasDiffMods = characterData.CheckUpdatedData(applicationBase, _cachedData, Logger,
                this, forceApplyCustomization, forceApplyMods: false)
                .Any(p => p.Value.Contains(PlayerChanges.ModManip) || p.Value.Contains(PlayerChanges.ModFiles));
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


        if (sameHash && !forceApplyCustomization)
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

        var charaDataToUpdate = characterData.CheckUpdatedData(applicationBase, _cachedData?.DeepClone() ?? new(), Logger, this, forceApplyCustomization, _forceApplyMods);

        if (_charaHandler != null && _forceApplyMods)
        {
            _forceApplyMods = false;
        }

        if (_redrawOnNextApplication && charaDataToUpdate.TryGetValue(ObjectKind.Player, out var player))
        {
            player.Add(PlayerChanges.ForcedRedraw);
            _redrawOnNextApplication = false;
        }

        if (charaDataToUpdate.TryGetValue(ObjectKind.Player, out var playerChanges))
        {
            _pluginWarningNotificationManager.NotifyForMissingPlugins(Pair.UserData, PlayerName!, playerChanges);
        }

        Logger.LogDebug("[BASE-{appbase}] Downloading and applying character for {name}", applicationBase, this);

        DownloadAndApplyCharacter(applicationBase, characterData.DeepClone(), charaDataToUpdate);
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
        if (_charaHandler != null)
        {
            Mediator.Publish(new PlayerUploadingMessage(_charaHandler, isUploading));
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
                _ipcManager.Penumbra.RemoveTemporaryCollectionAsync(Logger, applicationId, _penumbraCollection).GetAwaiter().GetResult();
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
                            await _ipcManager.Glamourer.ApplyAllAsync(Logger, handler, glamourerData, applicationId, token).ConfigureAwait(false);
                        }
                        break;

                    case PlayerChanges.Moodles:
                        await _ipcManager.Moodles.SetStatusAsync(handler.Address, charaData.MoodlesData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.PetNames:
                        await _ipcManager.PetNames.SetPlayerData(handler.Address, charaData.PetNamesData).ConfigureAwait(false);
                        break;

                    case PlayerChanges.ForcedRedraw:
                        if (changes.Key != ObjectKind.Player || allowPlayerRedraw)
                        {
                            needsRedraw = true;
                        }
                        else
                        {
                            Logger.LogDebug("[{applicationId}] Ignoring ForcedRedraw for Player because no Penumbra state changed this pass", applicationId);
                        }
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

        await _ipcManager.Penumbra.RedrawAsync(Logger, _charaHandler, applicationId, token).ConfigureAwait(false);
    }


    private void DownloadAndApplyCharacter(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData)
    {
        if (!updatedData.Any())
        {
            Logger.LogDebug("[BASE-{appBase}] Nothing to update for {obj}", applicationBase, this);
            return;
        }

        var updateModdedPaths = updatedData.Values.Any(v => v.Any(p => p == PlayerChanges.ModFiles));
        var updateManip = updatedData.Values.Any(v => v.Any(p => p == PlayerChanges.ModManip));


        _downloadCancellationTokenSource = _downloadCancellationTokenSource?.CancelRecreate() ?? new CancellationTokenSource();
        var downloadToken = _downloadCancellationTokenSource.Token;

        _ = DownloadAndApplyCharacterAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, downloadToken).ConfigureAwait(false);
    }

    private Task? _pairDownloadTask;

    private async Task DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData,
        bool updateModdedPaths, bool updateManip, CancellationToken downloadToken)
    {
        Dictionary<(string GamePath, string? Hash), string> moddedPaths = [];
        List<FileReplacementData> toDownloadReplacements = [];

        bool downloadedAny = false;

        if (updateModdedPaths)
        {
            int attempts = 0;
            // Initial calculation
            toDownloadReplacements = TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, downloadToken);

            if (toDownloadReplacements.Any(r =>
                r.GamePaths != null && r.GamePaths.Any(p =>
                p.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase) ||
                p.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)))
            )
            {
                _redrawOnNextApplication = true;
            }


            for (attempts = 1;attempts <= 10 && toDownloadReplacements.Count > 0 && !downloadToken.IsCancellationRequested;attempts++)
            {
                if (_pairDownloadTask != null && !_pairDownloadTask.IsCompleted)
                {
                    Logger.LogDebug("[BASE-{appBase}] Finishing prior running download task for player {name}, {kind}",
                        applicationBase, PlayerName, updatedData);
                    await _pairDownloadTask.ConfigureAwait(false);
                }

                toDownloadReplacements = toDownloadReplacements
                .GroupBy(r => r.Hash, StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .ToList();

                var missingDetails = string.Join(", ",
                    toDownloadReplacements.Select(r =>
                        $"{r.Hash}:{(r.GamePaths != null && r.GamePaths.Count() > 0 ? r.GamePaths[0] : "nogamepath")}"));

                if (attempts > 1)
                {
                    Logger.LogDebug("[BASE-{appBase}] Retry {attempt}/10 - still missing {count}: {missing}",
                    applicationBase, attempts, toDownloadReplacements.Count, missingDetails);
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
                    Logger.LogWarning(
                        "[BASE-{appBase}] Attempt {attempt}/10 queued 0 downloads, but {count} hashes still missing. Backing off then retrying.",
                        applicationBase, attempts, toDownloadReplacements.Count);

                    _downloadManager.ClearDownload();
                    _pairDownloadTask = null;

                    var delayMs = Math.Min(15000, 2000 * attempts); // 2s,4s,6s... cap 15s
                    await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);
                    if (downloadToken.IsCancellationRequested) return;

                    toDownloadReplacements =
                        TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, downloadToken);
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
                }
                catch (OperationCanceledException)
                {
                    Logger.LogTrace("[BASE-{appBase}] Download task cancelled during attempt {attempt}/10", applicationBase, attempts);
                    return;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "[BASE-{appBase}] Download task failed on attempt {attempt}/10 for {name}. Backing off then retrying.",
                        applicationBase, attempts, PlayerName);

                    _downloadManager.ClearDownload();
                    _pairDownloadTask = null;

                    var delayMs = Math.Min(15000, 2000 * attempts); 
                    await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);
                    if (downloadToken.IsCancellationRequested) return;

                    toDownloadReplacements =
                        TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, downloadToken);

                    continue;
                }

                if (downloadToken.IsCancellationRequested)
                {
                    Logger.LogTrace("[BASE-{appBase}] Detected cancellation", applicationBase);
                    return;
                }

                toDownloadReplacements =
                    TryCalculateModdedDictionary(applicationBase, charaData, out moddedPaths, downloadToken);

                if (toDownloadReplacements.Count == 0)
                    break;
            }


            //hard-stop if something is *still* missing
            if (toDownloadReplacements.Count > 0)
            {
                var missingDetails = string.Join(", ",
                    toDownloadReplacements.Select(r =>
                        $"{r.Hash}:{(r.GamePaths != null && r.GamePaths.Count() > 0 ? r.GamePaths[0] : "nogamepath")}"));

                Logger.LogWarning(
                    "[BASE-{appBase}] Aborting application for player {name}: {count} files still missing after {attempts} attempts. Missing: {missing}",
                    applicationBase,
                    PlayerName,
                    toDownloadReplacements.Count,
                    attempts,
                    missingDetails);

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

                    DownloadAndApplyCharacter(applicationBase, charaData.DeepClone(), updatedData);
                }
                
                // Do NOT apply partial moddedPaths
                return;
            }

            if (!await _playerPerformanceService.CheckBothThresholds(this, charaData).ConfigureAwait(false))
                return;
        }

        downloadToken.ThrowIfCancellationRequested();

        var appToken = _applicationCancellationTokenSource?.Token;
        var lastWaitLog = 0L;

        while ((!_applicationTask?.IsCompleted ?? false)
               && !downloadToken.IsCancellationRequested
               && (!appToken?.IsCancellationRequested ?? false))
        {
            var now = Environment.TickCount64;
            if (now - lastWaitLog >= 1000)
            {
                lastWaitLog = now;
                Logger.LogDebug("[BASE-{appBase}] Waiting for current data application (Id: {id}) for player ({handler}) to finish",
                    applicationBase, _applicationId, PlayerName);
            }

            await Task.Delay(25, downloadToken).ConfigureAwait(false);
        }


        if (downloadToken.IsCancellationRequested || (appToken?.IsCancellationRequested ?? false)) return;

        _applicationCancellationTokenSource = _applicationCancellationTokenSource.CancelRecreate() ?? new CancellationTokenSource();
        var token = _applicationCancellationTokenSource.Token;

        _applicationTask = ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, moddedPaths, downloadedAny, token);

        _ = _applicationTask.ContinueWith(t =>
        {
            try
            {
                Logger.LogWarning(t.Exception, "[BASE-{appBase}] Application task faulted for {player}", applicationBase, PlayerName);
            }
            catch
            {
            }
        }, TaskContinuationOptions.OnlyOnFaulted);

    }

    private async Task ApplyCharacterDataAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, Dictionary<(string GamePath, string? Hash), string> moddedPaths, bool downloadedAny, CancellationToken token)
    {
        var acquired = false;

        try
        {
            await GlobalApplySemaphore.WaitAsync(token).ConfigureAwait(false);
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
                await _dalamudUtil
                    .WaitWhileCharacterIsDrawing(Logger, _charaHandler!, _applicationId, 30000, token)
                    .ConfigureAwait(false);

                token.ThrowIfCancellationRequested();
                
                var penumbraStateChanged = false;
                bool needsRedraw = false;
                bool collectionReassignedThisRun = false;

                bool hasHardRedrawCriticalPenumbraChange = false;

                if (updateModdedPaths)
                {

                    var objIndex = await _dalamudUtil
                                    .RunOnFrameworkThread(() => _charaHandler!.GetGameObject()!.ObjectIndex)
                                    .ConfigureAwait(false);

                    var nowUtc = DateTime.UtcNow;

                    var firstAssign = !_lastAssignedObjectIndex.HasValue;
                    var indexChanged = _lastAssignedObjectIndex != objIndex;
                    var stale = (nowUtc - _lastAssignedCollectionAssignUtc) > TimeSpan.FromSeconds(5);

                    var needsAssign = firstAssign || indexChanged || stale;

                    if (needsAssign)
                    {
                        _lastAssignedCollectionAssignUtc = nowUtc;

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

                        collectionReassignedThisRun = firstAssign || indexChanged;
                    }


                    var cacheRoot = _fileDbManager.CacheFolder;

                    // Resolve hard-delayed hashed files to quarantine if needed
                    var resolvedModdedPaths = new Dictionary<(string GamePath, string? Hash), string>(moddedPaths.Count);

                    foreach (var kvp in moddedPaths)
                    {
                        var path = kvp.Value;

                        if (!string.IsNullOrEmpty(kvp.Key.Hash) &&
                            !string.IsNullOrEmpty(path) &&
                            ActivationPolicy.IsHardDelayed(path) &&
                            !File.Exists(path) &&
                            _downloadManager.TryResolveHardDelayedPath(kvp.Key.Hash!, path, out var resolved))
                        {
                            path = resolved;
                        }

                        resolvedModdedPaths[kvp.Key] = path;
                    }

                    // Validate required files exist at apply-time
                    var missingAtApply = resolvedModdedPaths
                        .Where(kvp =>
                        {
                            var path = kvp.Value;

                            if (string.IsNullOrEmpty(path))
                                return true;

                            if (!string.IsNullOrEmpty(kvp.Key.Hash))
                                return !File.Exists(path);

                            if (path.StartsWith(cacheRoot, StringComparison.OrdinalIgnoreCase))
                                return !File.Exists(path);

                            return false;
                        })
                        .ToList();

                    if (missingAtApply.Count > 0)
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

                            DownloadAndApplyCharacter(applicationBase, charaData.DeepClone(), updatedData);
                        }

                        // Do NOT apply a partial set in this run.
                        return;
                    }

                    var tempMods = BuildPenumbraTempMods(resolvedModdedPaths);
                    var fingerprint = ComputeTempModsFingerprint(tempMods);

                    var isFirstTempModsApply = _lastAppliedTempModsFingerprint == null;
                    var containsHardCriticalPath = resolvedModdedPaths.Keys.Any(k => IsRedrawCriticalGamePath(k.GamePath));

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
                        penumbraStateChanged = true;

                        needsRedraw |= downloadedAny;

                        long totalBytes = 0;
                        bool any = false;

                        foreach (var p in resolvedModdedPaths.Values.Distinct(StringComparer.OrdinalIgnoreCase))
                        {
                            token.ThrowIfCancellationRequested();

                            try
                            {
                                if (string.IsNullOrEmpty(p)) continue;
                                if (!File.Exists(p)) continue;

                                totalBytes += new FileInfo(p).Length;
                                any = true;
                            }
                            catch
                            {
                            }
                        }

                        LastAppliedDataBytes = any ? totalBytes : -1;

                        if (downloadedAny)
                        {
                            Logger.LogDebug("[{applicationId}] TempMods applied with hard-critical paths or first-apply; redraw will be enforced", _applicationId);
                        }
                    }
                    else
                    {
                        Logger.LogDebug("[{applicationId}] TempMods unchanged; skipping SetTemporaryModsAsync + redraw", _applicationId);
                    }
                }

                if (updateManip)
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
                    penumbraStateChanged = true;
                    needsRedraw = true;
                }

                token.ThrowIfCancellationRequested();

                var playerGlamourerUpdated =
                    updatedData.TryGetValue(ObjectKind.Player, out var playerUpdateSet)
                    && playerUpdateSet.Contains(PlayerChanges.Glamourer);

                var allowPlayerRedraw = downloadedAny || updateManip || collectionReassignedThisRun;

                foreach (var kind in updatedData)
                {
                    needsRedraw |= await ApplyCustomizationDataAsync(_applicationId, kind, charaData, allowPlayerRedraw, token).ConfigureAwait(false);
                    token.ThrowIfCancellationRequested();
                }

                if (needsRedraw && _charaHandler != null && _charaHandler.Address != nint.Zero)
                {
                    var forcedRedrawRequested =
                        updatedData.TryGetValue(ObjectKind.Player, out var playerSetForForced)
                        && playerSetForForced.Contains(PlayerChanges.ForcedRedraw);

                    var mustRedrawEvenWithGlamourer =
                    allowPlayerRedraw
                    || (forcedRedrawRequested && allowPlayerRedraw);

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
                RequestPostApplyRepair(charaData);

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
            if (acquired)
                GlobalApplySemaphore.Release();
        }
    }
    private static bool IsRedrawCriticalGamePath(string gamePath)
    {
        if (string.IsNullOrEmpty(gamePath)) return false;

        gamePath = gamePath.Trim();

        return gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase);
    }
    private bool HasAnyMissingCacheFiles(Guid applicationBase, CharacterData characterData)
    {
        try
        {

            foreach (var item in characterData.FileReplacements.SelectMany(k => k.Value.Where(v => string.IsNullOrEmpty(v.FileSwapPath))))
            {
                if (string.IsNullOrWhiteSpace(item.Hash))
                    continue;

                if (_fileDbManager.IsHashStaged(item.Hash))
                    continue;

                var fileCache = _fileDbManager.GetFileCacheByHash(item.Hash);
                if (fileCache == null)
                {
                    Logger.LogDebug(
                        "[BASE-{appBase}] Detected missing cache entry for hash {hash} during apply self-check",
                        applicationBase,
                        item.Hash);
                    return true;
                }

                // Filesystem is truth: if DB says it exists but the file isn't on disk, treat as missing.
                if (string.IsNullOrEmpty(fileCache.ResolvedFilepath)
                    || !File.Exists(fileCache.ResolvedFilepath))
                {
                    Logger.LogDebug(
                        "[BASE-{appBase}] Detected missing cache file on disk for hash {hash} (path: {path}) during apply self-check",
                        applicationBase,
                        item.Hash,
                        fileCache.ResolvedFilepath);
                    return true;
                }

                try
                {
                    try
                    {
                        var fi = new FileInfo(fileCache.ResolvedFilepath);
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
                        return true;
                    }
                }
                catch
                {
                    Logger.LogDebug(
                        "[BASE-{appBase}] Detected unreadable cache file for hash {hash} (path: {path}) during apply self-check",
                        applicationBase,
                        item.Hash,
                        fileCache.ResolvedFilepath);
                    return true;
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


    private void FrameworkUpdate()
    {
        if (string.IsNullOrEmpty(PlayerName))
        {
            var pc = _dalamudUtil.FindPlayerByNameHash(Pair.Ident);
            if (pc == default((string, nint))) return;
            Logger.LogDebug("One-Time Initializing {this}", this);
            Initialize(pc.Name);
            Logger.LogDebug("One-Time Initialized {this}", this);
            Mediator.Publish(new EventMessage(new Event(PlayerName, Pair.UserData, nameof(PairHandler), EventSeverity.Informational,
                $"Initializing User For Character {pc.Name}")));
        }

        if (_charaHandler?.Address != nint.Zero && !IsVisible)
        {
            Guid appData = Guid.NewGuid();
            IsVisible = true;

            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;
            _redrawOnNextApplication = true;

            if (_cachedData != null)
            {
                Logger.LogTrace("[BASE-{appBase}] {this} visibility changed, now: {visi}, cached data exists", appData, this, IsVisible);

                _ = _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    ApplyCharacterData(appData, _cachedData!, forceApplyCustomization: true);
                });
            }
            else
            {
                Logger.LogTrace("{this} visibility changed, now: {visi}, no cached data exists", this, IsVisible);
            }
        }
        else if (_charaHandler?.Address == nint.Zero && IsVisible)
        {
            IsVisible = false;

            _lastAssignedObjectIndex = null;
            _lastAssignedCollectionAssignUtc = DateTime.MinValue;

            _charaHandler.Invalidate();
            _downloadCancellationTokenSource?.CancelDispose();
            _downloadCancellationTokenSource = null;
            Logger.LogTrace("{this} visibility changed, now: {visi}", this, IsVisible);
        }
    }

    private void Initialize(string name)
    {
        PlayerName = name;
        _charaHandler = _gameObjectHandlerFactory.Create(ObjectKind.Player, () => _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident), isWatched: false).GetAwaiter().GetResult();

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

        //_ipcManager.Penumbra.AssignTemporaryCollectionAsync(Logger, _penumbraCollection, _charaHandler.GetGameObject()!.ObjectIndex).GetAwaiter().GetResult();
        
        var ok = _ipcManager.Penumbra.AssignTemporaryCollectionAsync(Logger, _penumbraCollection, _charaHandler.GetGameObject()!.ObjectIndex).GetAwaiter().GetResult();

        if (!ok)
            Logger.LogDebug("[Penumbra] Initial temp collection assign failed for {name}", PlayerName);

    }

    private async Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken)
    {
        nint address = _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident);
        if (address == nint.Zero) return;

        Logger.LogDebug("[{applicationId}] Reverting all Customization for {alias}/{name} {objectKind}", applicationId, Pair.UserData.AliasOrUID, name, objectKind);

        if (_customizeIds.TryGetValue(objectKind, out var customizeId))
        {
            _customizeIds.Remove(objectKind);
        }

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
            await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId).ConfigureAwait(false);
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
                await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId).ConfigureAwait(false);
                using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.MinionOrMount, () => minionOrMount, isWatched: false).ConfigureAwait(false);
                await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
            }
        }
        else if (objectKind == ObjectKind.Pet)
        {
            var pet = await _dalamudUtil.GetPetAsync(address).ConfigureAwait(false);
            if (pet != nint.Zero)
            {
                await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId).ConfigureAwait(false);
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
                await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId).ConfigureAwait(false);
                using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Pet, () => companion, isWatched: false).ConfigureAwait(false);
                await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
            }
        }
    }

    private List<FileReplacementData> TryCalculateModdedDictionary(Guid applicationBase,CharacterData charaData,out Dictionary<(string GamePath, string? Hash), string> moddedDictionary, CancellationToken token)
    {
        var st = Stopwatch.StartNew();

        var missingFiles = new ConcurrentBag<FileReplacementData>();
        var outputDict = new ConcurrentDictionary<(string GamePath, string? Hash), string>(Environment.ProcessorCount, 256);

        int migrationChanges = 0;

        object migrationLock = new();

        moddedDictionary = new Dictionary<(string GamePath, string? Hash), string>();

        try
        {
            var all = charaData.FileReplacements;
            var noSwap = new List<FileReplacementData>(256);
            var swaps = new List<FileReplacementData>(64);

            foreach (var kv in all)
            {
                foreach (var v in kv.Value)
                {
                    if (string.IsNullOrWhiteSpace(v.FileSwapPath))
                        noSwap.Add(v);
                    else
                        swaps.Add(v);
                }
            }

            Parallel.ForEach(noSwap,
                new ParallelOptions
                {
                    CancellationToken = token,
                    MaxDegreeOfParallelism = 4
                },
                item =>
                {
                    token.ThrowIfCancellationRequested();

                    var hash = item.Hash;
                    if (string.IsNullOrWhiteSpace(hash))
                        return;

                    var gamePaths = item.GamePaths;
                    if (gamePaths == null)
                        return;

                    if (_fileDbManager.IsHashStaged(hash))
                    {
                        var firstGp = gamePaths.FirstOrDefault();
                        var ext = "dat";
                        if (!string.IsNullOrWhiteSpace(firstGp))
                        {
                            var e = Path.GetExtension(firstGp);
                            if (!string.IsNullOrWhiteSpace(e) && e.Length > 1)
                                ext = e[1..];
                        }

                        var finalPath = _fileDbManager.GetCacheFilePath(hash, ext);

                        foreach (var gp in gamePaths)
                        {
                            if (string.IsNullOrWhiteSpace(gp)) continue;
                            outputDict[(gp, hash)] = finalPath;
                        }

                        return;
                    }

                    var fileCache = _fileDbManager.GetFileCacheByHash(hash);
                    if (fileCache == null)
                    {
                        missingFiles.Add(item);
                        return;
                    }

                    var resolved = fileCache.ResolvedFilepath;
                    if (string.IsNullOrWhiteSpace(resolved) || !File.Exists(resolved))
                    {
                        missingFiles.Add(item);
                        return;
                    }

                    FileInfo fi;
                    try
                    {
                        fi = new FileInfo(resolved);

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
                        var firstGp = gamePaths.FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(firstGp))
                        {
                            var e = Path.GetExtension(firstGp);
                            if (!string.IsNullOrWhiteSpace(e) && e.Length > 1)
                            {
                                var targetExt = e[1..];

                                lock (migrationLock)
                                {
                                    var cacheAgain = _fileDbManager.GetFileCacheByHash(hash);
                                    if (cacheAgain != null)
                                    {
                                        var fi2 = new FileInfo(cacheAgain.ResolvedFilepath);
                                        if (string.IsNullOrEmpty(fi2.Extension))
                                        {
                                            Interlocked.Exchange(ref migrationChanges, 1);
                                            cacheAgain = _fileDbManager.MigrateFileHashToExtension(cacheAgain, targetExt);
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
                    }

                    foreach (var gp in gamePaths)
                    {
                        if (string.IsNullOrWhiteSpace(gp)) continue;
                        outputDict[(gp, hash)] = resolved;
                    }
                });

            foreach (var item in swaps)
            {
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

                foreach (var gp in gamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gp)) continue;
                    if (string.Equals(gp, swap, StringComparison.OrdinalIgnoreCase)) continue; // swap-to-self = no-op
                    outputDict[(gp, null)] = swap;
                }
            }

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

        foreach (var kv in tempMods.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            var line = kv.Key + "\n" + kv.Value + "\n";
            var bytes = Encoding.UTF8.GetBytes(line);
            sha1.TransformBlock(bytes, 0, bytes.Length, null, 0);
        }

        sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha1.Hash!);
    }

    private Dictionary<string, string> BuildPenumbraTempMods(Dictionary<(string GamePath, string? Hash), string> moddedPaths)
    {
        var dict = new Dictionary<string, (string? Hash, string Path)>(StringComparer.Ordinal);

        foreach (var kvp in moddedPaths)
        {
            var gamePath = kvp.Key.GamePath;
            if (string.IsNullOrWhiteSpace(gamePath)) continue;

            var path = kvp.Value;
            if (string.IsNullOrWhiteSpace(path)) continue;

            var hash = kvp.Key.Hash;

            if (!dict.TryGetValue(gamePath, out var existing))
            {
                dict[gamePath] = (hash, path);
                continue;
            }

            // Decide which wins
            var existingIsSwap = string.IsNullOrEmpty(existing.Hash);
            var incomingIsSwap = string.IsNullOrEmpty(hash);

            if (existingIsSwap && !incomingIsSwap)
            {
                // hashed beats swap
                dict[gamePath] = (hash, path);
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
            if (!string.Equals(existing.Path, path, StringComparison.OrdinalIgnoreCase))
                Logger.LogDebug("TempMods conflict for {gp}: keeping {old}, ignoring {new}", gamePath, existing.Path, path);
        }

        return dict.ToDictionary(k => k.Key, v => v.Value.Path, StringComparer.Ordinal);
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
            if (!pathByHash.ContainsKey(hash))
                pathByHash[hash] = kvp.Value;
        }

        foreach (var item in missing)
        {
            if (string.IsNullOrWhiteSpace(item.Hash))
                continue;

            if (_fileDbManager.IsHashStaged(item.Hash))
                continue;

            invalidHashes.Add(item.Hash);
        }


        foreach (var (hash, path) in pathByHash)
        {
            if (string.IsNullOrWhiteSpace(hash)) continue;
            if (_fileDbManager.IsHashStaged(hash)) continue;
            
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

                //var computed = Crypto.GetFileHash(fi.FullName);
                //if (!string.Equals(computed, hash, StringComparison.OrdinalIgnoreCase))
                //{
                //    Logger.LogDebug(
                //        "[BASE-{appBase}] Manual validation: hash mismatch for {hash} at {path}, computed {computed}",
                //        applicationBase, hash, path, computed);
                //    invalidHashes.Add(hash);
                //}
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
            Logger.LogInformation(
                "[BASE-{appBase}] No missing or corrupt files detected for {pair}",
                applicationBase, Pair.UserData.AliasOrUID);

            Mediator.Publish(new EventMessage(new Event(
                PlayerName,
                Pair.UserData,
                nameof(PairHandler),
                EventSeverity.Informational,
                "RavaSync: File verification complete — no issues detected for this user.")));

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

        Mediator.Publish(new EventMessage(new Event(
            PlayerName,
            Pair.UserData,
            nameof(PairHandler),
            EventSeverity.Warning,
            $"RavaSync: Detected {invalidHashes.Count} missing/corrupt cache files; starting repair download.")));

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
        if (Interlocked.Exchange(ref _missingCheckRunning, 1) == 1)
            return;

        var hash = characterData.DataHash.Value;
        var dataCopy = characterData.DeepClone();

        _ = Task.Run(() =>
        {
            try
            {
                // Do NOT run disk IO on the framework thread.
                var missing = HasAnyMissingCacheFiles(applicationBase, dataCopy);

                lock (_missingCheckGate)
                {
                    _lastMissingCheckedHash = hash;
                    _lastMissingCheckedTick = Environment.TickCount64;
                    _lastMissingCheckHadMissing = missing;
                }

                if (missing)
                {
                    _ = _dalamudUtil.RunOnFrameworkThread(() =>
                    {
                        ApplyCharacterData(Guid.NewGuid(), dataCopy, forceApplyCustomization: true);
                    });
                }
            }
            catch
            {
            }
            finally
            {
                Interlocked.Exchange(ref _missingCheckRunning, 0);
            }
        });
    }

    private void RequestPostApplyRepair(CharacterData appliedData)
    {
        var hash = appliedData.DataHash.Value;
        var now = Environment.TickCount64;

        if (string.Equals(_lastPostApplyRepairHash, hash, StringComparison.Ordinal)
            && (now - _lastPostApplyRepairTick) < 30000)
            return;

        _lastPostApplyRepairHash = hash;
        _lastPostApplyRepairTick = now;

        if (Interlocked.CompareExchange(ref _manualRepairRunning, 1, 0) != 0)
            return;

        var applicationBase = Guid.NewGuid();
        var dataCopy = appliedData.DeepClone();

        _ = Task.Run(async () =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

            try
            {
                await Task.Delay(750, cts.Token).ConfigureAwait(false);

                await ManualVerifyAndRepairAsync(applicationBase, dataCopy, cts.Token, verifyFileHashes: false, publishEvents: false).ConfigureAwait(false);
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
                Interlocked.Exchange(ref _manualRepairRunning, 0);
            }
        });
    }


}