using Dalamud.Game.Gui.ContextMenu;
using Dalamud.Game.Text.SeStringHandling;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.User;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files.Models;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using RavaSync.PlayerData.Services;

namespace RavaSync.PlayerData.Pairs;

public class Pair
{
    private readonly PairHandlerFactory _cachedPlayerFactory;
    private readonly SemaphoreSlim _creationSemaphore = new(1);
    private readonly ILogger<Pair> _logger;
    private readonly MareMediator _mediator;
    private readonly MareConfigService _mareConfigService;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly ToyBox _toyBox;

    private CancellationTokenSource _applicationCts = new();
    private OnlineUserIdentDto? _onlineUserIdentDto = null;
    private long _lastUploadStatusTick = 0;
    private CancellationTokenSource? _uploadingClearCts;
    private CancellationTokenSource? _pendingEmptyApplyCts;
    private const int PendingEmptyFileListApplyDebounceMs = 500;
    private const int PendingUploadingEmptyFileListApplyDebounceMs = 1250;
    private string _lastAcceptedIncomingDataHash = string.Empty;
    private string _lastAcceptedIncomingPayloadFingerprint = string.Empty;


    public static Func<string, bool>? IsBlacklistedCallback { get; set; }

    public Pair(ILogger<Pair> logger, UserFullPairDto userPair, PairHandlerFactory cachedPlayerFactory,
        MareMediator mediator, MareConfigService mareConfigService, ServerConfigurationManager serverConfigurationManager)
    {
        _logger = logger;
        UserPair = userPair;
        _cachedPlayerFactory = cachedPlayerFactory;
        _mediator = mediator;
        _mareConfigService = mareConfigService;
        _serverConfigurationManager = serverConfigurationManager;

        RestoreLocalSyncPreferences();
    }

    public bool HasCachedPlayer => CachedPlayer != null && !string.IsNullOrEmpty(CachedPlayer.PlayerName) && _onlineUserIdentDto != null;
    public IndividualPairStatus IndividualPairStatus => UserPair.IndividualPairStatus;
    public bool IsDirectlyPaired => IndividualPairStatus != IndividualPairStatus.None;
    public bool IsOneSidedPair => IndividualPairStatus == IndividualPairStatus.OneSided;
    public bool IsOnline => CachedPlayer != null;

    public bool IsPaired => IndividualPairStatus == IndividualPairStatus.Bidirectional || UserPair.Groups.Any();
    public bool IsPaused =>
    UserPair.OwnPermissions.IsPaused()
    || AutoPausedByCap
    || AutoPausedByScope
    || (AutoPausedByOtherSync && !EffectiveOverrideOtherSync)
    || (IsBlacklistedCallback?.Invoke(UserData.UID) ?? false);

    public bool IsVisible => CachedPlayer?.IsVisible ?? false;
    // Whether Customize+ should be applied for this pair (default: ON)
    public bool IsCustomizePlusEnabled { get; set; } = true;
    public bool IsMetadataEnabled { get; set; } = true;

    public CharacterData? LastReceivedCharacterData { get; set; }
    public CharacterRavaSidecarUtility.SyncManifestPayload? LastReceivedSyncManifest { get; private set; }
    public string? PlayerName => CachedPlayer?.PlayerName ?? string.Empty;
    public nint PlayerCharacter => CachedPlayer?.PlayerCharacter ?? nint.Zero;
    public long LastAppliedDataBytes => CachedPlayer?.LastAppliedDataBytes ?? -1;
    public long LastAppliedDataTris { get; set; } = -1;
    public long LastAppliedApproximateVRAMBytes { get; set; } = -1;

    public void ClearDisplayedPerformanceMetrics()
    {
        LastAppliedDataTris = -1;
        LastAppliedApproximateVRAMBytes = -1;
    }
    public bool AutoPausedByCap { get; set; } = false;
    public bool AutoPausedByScope { get; set; } = false;
    public bool AutoPausedByOtherSync { get; set; } = false;
    public string AutoPausedByOtherSyncName { get; set; } = string.Empty;
    public bool OverrideOtherSync { get; set; } = false;
    public bool RemoteOtherSyncOverrideActive { get; private set; } = false;
    public bool EffectiveOverrideOtherSync => OverrideOtherSync || (RemoteOtherSyncOverrideActive && !RemoteOtherSyncYield);
    public bool RemoteOtherSyncYield { get; private set; } = false;
    public string RemoteOtherSyncOwner { get; private set; } = string.Empty;
    public string Ident => _onlineUserIdentDto?.Ident ?? string.Empty;

    public UserData UserData => UserPair.User;

    public UserFullPairDto UserPair { get; set; }
    private PairHandler? CachedPlayer { get; set; }

    public void AddContextMenu(IMenuOpenedArgs args)
    {
        if (CachedPlayer == null || (args.Target is not MenuTargetDefault target) || target.TargetObjectId != CachedPlayer.PlayerCharacterId || IsPaused) return;

        SeStringBuilder seStringBuilder = new();
        SeStringBuilder seStringBuilder2 = new();
        SeStringBuilder seStringBuilder3 = new();
        SeStringBuilder seStringBuilder4 = new();
        SeStringBuilder seStringBuilder5 = new();
        var openProfileSeString = seStringBuilder.AddText("Open Profile").Build();
        var reapplyDataSeString = seStringBuilder2.AddText("Reapply last data").Build();
        var pauseTarget = seStringBuilder5.AddText("Pause target").Build();
        var cyclePauseState = seStringBuilder3.AddText("Cycle pause state").Build();
        var changePermissions = seStringBuilder4.AddText("Change Permissions").Build();
        args.AddMenuItem(new MenuItem()
        {
            Name = openProfileSeString,
            OnClicked = (a) => _mediator.Publish(new ProfileOpenStandaloneMessage(this)),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });

        args.AddMenuItem(new MenuItem()
        {
            Name = reapplyDataSeString,
            OnClicked = (a) => ApplyLastReceivedData(forced: true),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });

        args.AddMenuItem(new MenuItem()
        {
            Name = pauseTarget,
            OnClicked = (a) => _mediator.Publish(new PauseMessage(UserData)),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });

        args.AddMenuItem(new MenuItem()
        {
            Name = changePermissions,
            OnClicked = (a) => _mediator.Publish(new OpenPermissionWindow(this)),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });

        args.AddMenuItem(new MenuItem()
        {
            Name = cyclePauseState,
            OnClicked = (a) => _mediator.Publish(new CyclePauseMessage(UserData)),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });

        SeStringBuilder seStringBuilder6 = new();
        var redrawTarget = seStringBuilder6.AddText("Redraw target").Build();
        args.AddMenuItem(new MenuItem()
        {
            Name = redrawTarget,
            OnClicked = (a) => RequestTargetRedraw(),
            UseDefaultPrefix = false,
            PrefixChar = 'R',
            PrefixColor = 708
        });
    }


    public void ApplyData(OnlineUserCharaDataDto data)
    {
        var incoming = data.CharaData;

        var incomingHash = incoming?.DataHash.Value ?? string.Empty;
        var incomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(incoming);
        if (!string.IsNullOrWhiteSpace(incomingHash)
            && string.Equals(incomingHash, _lastAcceptedIncomingDataHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, _lastAcceptedIncomingPayloadFingerprint, StringComparison.Ordinal))
        {
            _logger.LogTrace("Ignoring duplicate incoming character data {hash}/{payload} for {uid}", incomingHash, incomingPayloadFingerprint, data.User.UID);
            return;
        }

        if (incoming != null && LastReceivedCharacterData != null && IsVisible)
        {
            bool incomingHasFiles = incoming.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;
            bool previousHasFiles = LastReceivedCharacterData.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;

            // During a sender-side item swap the remote can briefly publish an empty
            // file list before the replacement file map arrives. Applying that empty
            // list makes the receiver flash vanilla, so only allow it after a short
            // confirmation window. Non-empty follow-up data cancels this immediately.
            if (!incomingHasFiles && previousHasFiles)
            {
                _pendingEmptyApplyCts?.Cancel();
                _pendingEmptyApplyCts = new CancellationTokenSource();
                var token = _pendingEmptyApplyCts.Token;
                var pending = incoming;
                var delayMs = IsUploadingRecently ? PendingUploadingEmptyFileListApplyDebounceMs : PendingEmptyFileListApplyDebounceMs;

                var immediate = incoming.DeepClone();
                immediate.FileReplacements = LastReceivedCharacterData.FileReplacements?
                    .ToDictionary(k => k.Key, v => v.Value?.ToList() ?? []) ?? [];

                var previousPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(LastReceivedCharacterData);
                var immediatePayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(immediate);
                if (!string.Equals(previousPayloadFingerprint, immediatePayloadFingerprint, StringComparison.Ordinal))
                {
                    _applicationCts = _applicationCts.CancelRecreate();

                    LastReceivedCharacterData = immediate;
                    _lastAcceptedIncomingDataHash = immediate.DataHash.Value ?? string.Empty;
                    _lastAcceptedIncomingPayloadFingerprint = immediatePayloadFingerprint;

                    _logger.LogDebug("Applying prompt non-file receiver state for {uid} while deferring empty file-list clear for {delayMs}ms", UserData.UID, delayMs);
                    ApplyLastReceivedData();
                }

                _logger.LogDebug("Deferring empty file list for {uid} for {delayMs}ms to avoid sender item-swap vanilla flicker", UserData.UID, delayMs);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(delayMs, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested) return;

                        _applicationCts = _applicationCts.CancelRecreate();

                        LastReceivedCharacterData = pending;
                        _lastAcceptedIncomingDataHash = pending?.DataHash.Value ?? string.Empty;
                        _lastAcceptedIncomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(pending);

                        if (CachedPlayer == null)
                        {
                            _logger.LogDebug("Deferred empty-list apply for {uid} but CachedPlayer does not exist, waiting", UserData.UID);
                            return;
                        }

                        _logger.LogDebug("Applying deferred empty file list for {uid} after confirmation window", UserData.UID);
                        ApplyLastReceivedData();
                    }
                    catch (OperationCanceledException)
                    {
                        // newer data arrived
                    }
                }, token);

                return;
            }
        }

        _pendingEmptyApplyCts?.Cancel();
        _pendingEmptyApplyCts = null;

        _applicationCts = _applicationCts.CancelRecreate();

        LastReceivedCharacterData = incoming;
        _lastAcceptedIncomingDataHash = incomingHash;
        _lastAcceptedIncomingPayloadFingerprint = incomingPayloadFingerprint;
        if (CachedPlayer == null)
        {
            _logger.LogDebug("Received Data for {uid} but CachedPlayer does not exist, waiting", data.User.UID);
            _ = Task.Run(async () =>
            {
                using var timeoutCts = new CancellationTokenSource();
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(120));
                var appToken = _applicationCts.Token;
                using var combined = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, appToken);
                while (CachedPlayer == null && !combined.Token.IsCancellationRequested)
                {
                    await Task.Delay(25, combined.Token).ConfigureAwait(false);
                }

                if (!combined.IsCancellationRequested)
                {
                    _logger.LogDebug("Applying delayed data for {uid}", data.User.UID);
                    ApplyLastReceivedData();
                }
            });
            return;
        }

        ApplyLastReceivedData();
    }

    public void SetLastReceivedSyncManifest(CharacterRavaSidecarUtility.SyncManifestPayload? manifest) => LastReceivedSyncManifest = manifest;

    internal bool IsDuplicateIncomingPayload(CharacterData? incoming)
    {
        var incomingHash = incoming?.DataHash.Value ?? string.Empty;
        if (string.IsNullOrWhiteSpace(incomingHash))
            return false;

        var incomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(incoming);

        return string.Equals(incomingHash, _lastAcceptedIncomingDataHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, _lastAcceptedIncomingPayloadFingerprint, StringComparison.Ordinal);
    }

    internal CharacterData? PrepareCharacterDataForLocalApply(CharacterData? data)
    {
        return RemoveNotSyncedFiles(data);
    }

    public void ApplyLastReceivedData(bool forced = false)
    {
        if (IsPaused) return;
        if (AutoPausedByOtherSync && !EffectiveOverrideOtherSync) return;
        if (CachedPlayer == null) return;
        if (LastReceivedCharacterData == null) return;

        CachedPlayer.ApplyCharacterData(Guid.NewGuid(), PrepareCharacterDataForLocalApply(LastReceivedCharacterData.DeepClone())!, forced);
    }

    public void EnterPausedVanillaState()
    {
        CachedPlayer?.EnterPausedVanillaState();
    }

    public void ReclaimFromOtherSync(bool requestApplyIfPossible = true)
    {
        AutoPausedByOtherSync = false;
        AutoPausedByOtherSyncName = string.Empty;

        CachedPlayer?.ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible: false);
    }

    public void ApplyRemoteOtherSyncOverride(bool yieldToOtherSync, string owner)
    {
        owner ??= string.Empty;

        if (RemoteOtherSyncOverrideActive
            && RemoteOtherSyncYield == yieldToOtherSync
            && string.Equals(RemoteOtherSyncOwner ?? string.Empty, owner, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        RemoteOtherSyncOverrideActive = true;
        RemoteOtherSyncYield = yieldToOtherSync;
        RemoteOtherSyncOwner = owner;

        if (!yieldToOtherSync)
        {
            AutoPausedByOtherSync = false;
            AutoPausedByOtherSyncName = string.Empty;

            ReclaimFromOtherSync(requestApplyIfPossible: true);
        }
    }

    public void ClearRemoteOtherSyncOverride()
    {
        RemoteOtherSyncOverrideActive = false;
        RemoteOtherSyncYield = false;
        RemoteOtherSyncOwner = string.Empty;
    }

    public void ExpireRemoteOtherSyncOverride(bool requestApplyIfPossible = true)
    {
        var owner = RemoteOtherSyncOwner;

        RemoteOtherSyncOverrideActive = false;
        RemoteOtherSyncYield = false;
        RemoteOtherSyncOwner = string.Empty;

        var shouldReclaim =
            AutoPausedByOtherSync
            && (!string.IsNullOrWhiteSpace(owner)
                ? string.Equals(AutoPausedByOtherSyncName, owner, StringComparison.OrdinalIgnoreCase)
                : true);

        if (!shouldReclaim)
            return;

        AutoPausedByOtherSync = false;
        AutoPausedByOtherSyncName = string.Empty;
        CachedPlayer?.ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible: false);
    }

    public void RequestManualFileRepair()
    {
        if (CachedPlayer == null) return;

        CachedPlayer.RequestManualFileRepair();
    }

    public void RequestTargetRedraw()
    {
        var address = CachedPlayer?.PlayerCharacter ?? nint.Zero;
        if (address == nint.Zero) return;
        _mediator.Publish(new PenumbraRedrawAddressMessage(address));
    }

    public void CreateCachedPlayer(OnlineUserIdentDto? dto = null)
    {
        PairHandler? oldPlayer = null;
        bool shouldCreate = false;

        try
        {
            _creationSemaphore.Wait();

            if (CachedPlayer != null) return;

            if (dto == null && _onlineUserIdentDto == null)
            {
                oldPlayer = CachedPlayer;
                CachedPlayer = null;
                return;
            }

            if (dto != null)
                _onlineUserIdentDto = dto;

            oldPlayer = CachedPlayer;
            CachedPlayer = null;
            shouldCreate = _onlineUserIdentDto != null;
        }
        finally
        {
            _creationSemaphore.Release();
        }

        // Dispose outside lock
        oldPlayer?.Dispose();

        if (!shouldCreate) return;

        var created = _cachedPlayerFactory.Create(this);

        try
        {
            _creationSemaphore.Wait();
            if (CachedPlayer == null)
                CachedPlayer = created;
            else
                created.Dispose();
        }
        finally
        {
            _creationSemaphore.Release();
        }
    }

    public string? GetNote()
    {
        return _serverConfigurationManager.GetNoteForUid(UserData.UID);
    }

    public string GetPlayerNameHash()
    {
        return CachedPlayer?.PlayerNameHash ?? string.Empty;
    }

    public bool HasAnyConnection()
    {
        return UserPair.Groups.Any() || UserPair.IndividualPairStatus != IndividualPairStatus.None;
    }

    public void MarkOffline(bool wait = true)
    {
        PairHandler? player = null;

        try
        {
            if (wait)
                _creationSemaphore.Wait();

            _applicationCts = _applicationCts.CancelRecreate();
            _pendingEmptyApplyCts?.Cancel();
            _pendingEmptyApplyCts = null;
            _uploadingClearCts?.Cancel();
            _uploadingClearCts = null;

            LastReceivedCharacterData = null;
            LastReceivedSyncManifest = null;
            _lastAcceptedIncomingDataHash = string.Empty;
            _lastAcceptedIncomingPayloadFingerprint = string.Empty;

            player = CachedPlayer;
            CachedPlayer = null;
        }
        finally
        {
            if (wait)
                _creationSemaphore.Release();
        }

        player?.Dispose();

        _onlineUserIdentDto = null;

        _uploadingClearCts?.Cancel();
        _uploadingClearCts = null;
        _isUploading = false;
        _lastUploadStatusTick = 0;
    }



    public void SetNote(string note)
    {
        _serverConfigurationManager.SetNoteForUid(UserData.UID, note);
    }

    private bool _isUploading;

    internal void SetUploadState(bool uploading)
    {
        if (!uploading)
        {
            _uploadingClearCts?.Cancel();
            _uploadingClearCts = null;
        }

        _isUploading = uploading;
        _lastUploadStatusTick = uploading ? Environment.TickCount64 : 0;
    }

    internal void SetIsUploading()
    {
        _uploadingClearCts?.Cancel();
        _uploadingClearCts = new CancellationTokenSource();

        SetUploadState(true);
        CachedPlayer?.SetUploading();

        var token = _uploadingClearCts.Token;
        var statusTick = _lastUploadStatusTick;

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(2500, token).ConfigureAwait(false);

                if (token.IsCancellationRequested)
                    return;

                if (_lastUploadStatusTick != statusTick)
                    return;

                SetUploadState(false);
                CachedPlayer?.SetUploading(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
            }
        }, token);
    }

    public bool IsUploading => IsUploadingRecently;

    public bool IsUploadingRecently
    {
        get
        {
            var lastTick = _lastUploadStatusTick;
            if (lastTick <= 0)
                return false;

            var elapsed = unchecked(Environment.TickCount64 - lastTick);
            return elapsed >= 0 && elapsed < 2500;
        }
    }

    public sealed record DownloadProgressSummary(
        bool HasAny,
        bool AnyDownloading,
        bool AnyLoading,
        long TotalBytes,
        long TransferredBytes,
        int TotalFiles,
        int TransferredFiles)
    {
        public static readonly DownloadProgressSummary None = new(false, false, false, 0, 0, 0, 0);
    }

    public enum VisibleTransferIndicator
    {
        None = 0,
        Downloading = 1,
        LoadingFiles = 2,
    }

    private volatile VisibleTransferIndicator _visibleTransferIndicator = VisibleTransferIndicator.None;
    public VisibleTransferIndicator VisibleTransferStatus => _visibleTransferIndicator;
    internal void SetVisibleTransferStatus(VisibleTransferIndicator status) => _visibleTransferIndicator = status;

    private DownloadProgressSummary _currentDownloadSummary = DownloadProgressSummary.None;
    public DownloadProgressSummary CurrentDownloadSummary => Volatile.Read(ref _currentDownloadSummary);
    internal void SetCurrentDownloadSummary(DownloadProgressSummary? summary)
    {
        Volatile.Write(ref _currentDownloadSummary, summary ?? DownloadProgressSummary.None);
    }

    private Dictionary<string, FileDownloadStatus>? _currentDownloadStatus;
    public IReadOnlyDictionary<string, FileDownloadStatus>? CurrentDownloadStatus => _currentDownloadStatus;
    internal void SetCurrentDownloadStatus(Dictionary<string, FileDownloadStatus>? status) => _currentDownloadStatus = status;

    public void SetCustomizePlusEnabled(bool enabled, bool reapply = true)
    {
        if (IsCustomizePlusEnabled == enabled)
        {
            RequestTargetRedraw();
            return;
        }
        IsCustomizePlusEnabled = enabled;
        PersistLocalSyncPreference(_mareConfigService.Current.LocalCustomizePlusDisabledUids, enabled);
        if (reapply)
            ApplyLastReceivedData(forced: true);

        RequestTargetRedraw();

    }

    public void ToggleCustomizePlusAndReapply()
    {
        SetCustomizePlusEnabled(!IsCustomizePlusEnabled);
    }

    public void SetMetadataEnabled(bool enabled, bool reapply = true)
    {
        if (IsMetadataEnabled == enabled)
            return;

        IsMetadataEnabled = enabled;
        PersistLocalSyncPreference(_mareConfigService.Current.LocalHeightMetadataDisabledUids, enabled);
        if (reapply)
        {
            CachedPlayer?.ForceManipulationReapply();
            ApplyLastReceivedData(forced: true);
        }
    }

    public void ToggleMetadataAndReapply()
    {
        SetMetadataEnabled(!IsMetadataEnabled);
    }

    private void RestoreLocalSyncPreferences()
    {
        var uid = UserData.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return;

        IsCustomizePlusEnabled = !_mareConfigService.Current.LocalCustomizePlusDisabledUids.Contains(uid);
        IsMetadataEnabled = !_mareConfigService.Current.LocalHeightMetadataDisabledUids.Contains(uid);
    }

    private void PersistLocalSyncPreference(HashSet<string> disabledUids, bool enabled)
    {
        var uid = UserData.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return;

        if (enabled)
            disabledUids.Remove(uid);
        else
            disabledUids.Add(uid);

        _mareConfigService.Save();
    }

    private CharacterData? RemoveNotSyncedFiles(CharacterData? data)
    {
        _logger.LogTrace("Removing not synced files");
        if (data == null)
        {
            _logger.LogTrace("Nothing to remove");
            return data;
        }

        bool disableIndividualAnimations = (UserPair.OtherPermissions.IsDisableAnimations() || UserPair.OwnPermissions.IsDisableAnimations());
        bool disableIndividualVFX = (UserPair.OtherPermissions.IsDisableVFX() || UserPair.OwnPermissions.IsDisableVFX());
        bool disableIndividualSounds = (UserPair.OtherPermissions.IsDisableSounds() || UserPair.OwnPermissions.IsDisableSounds());
        bool disableIndividualCustomizePlus = !IsCustomizePlusEnabled;
        bool disableIndividualMetadata = !IsMetadataEnabled;

        _logger.LogTrace("Disable: Sounds: {disableIndividualSounds}, Anims: {disableIndividualAnims}; VFX: {disableIndividualVFX}, Customize+: {disableIndividualCustomizePlus}, Metadata: {disableIndividualMetadata}",
            disableIndividualSounds, disableIndividualAnimations, disableIndividualVFX, disableIndividualCustomizePlus, disableIndividualMetadata);

        if (disableIndividualAnimations || disableIndividualSounds || disableIndividualVFX)
        {
            _logger.LogTrace("Data cleaned up: Animations disabled: {disableAnimations}, Sounds disabled: {disableSounds}, VFX disabled: {disableVFX}",
                disableIndividualAnimations, disableIndividualSounds, disableIndividualVFX);

            foreach (var objectKind in data.FileReplacements.Keys)
            {
                if (disableIndividualSounds)
                    data.FileReplacements[objectKind] = data.FileReplacements[objectKind]
                        .Where(f => !f.GamePaths.Any(p => p.EndsWith("scd", StringComparison.OrdinalIgnoreCase)))
                        .ToList();

                if (disableIndividualAnimations)
                    data.FileReplacements[objectKind] = data.FileReplacements[objectKind]
                        .Where(f => !f.GamePaths.Any(p =>
                            p.EndsWith("tmb", StringComparison.OrdinalIgnoreCase) ||
                            p.EndsWith("pap", StringComparison.OrdinalIgnoreCase)))
                        .ToList();

                if (disableIndividualVFX)
                    data.FileReplacements[objectKind] = data.FileReplacements[objectKind]
                        .Where(f => !f.GamePaths.Any(p =>
                            p.EndsWith("atex", StringComparison.OrdinalIgnoreCase) ||
                            p.EndsWith("avfx", StringComparison.OrdinalIgnoreCase)))
                        .ToList();
            }
        }

        // Handle Customize+ data separately since it's not in FileReplacements
        if (disableIndividualCustomizePlus)
        {
            data.CustomizePlusData.Clear();
        }

        if (disableIndividualMetadata)
        {
            data.ManipulationData = GetEffectiveManipulationData(data.ManipulationData);
        }

        return data;
    }

    public string GetEffectiveManipulationData(string? manipulationData)
    {
        var original = manipulationData ?? string.Empty;
        if (IsMetadataEnabled)
        {
            _logger.LogDebug("Height metadata enabled for {uid}: using original manipulation payload, length={length}", UserData.UID, original.Length);
            return original;
        }

        var beforeCount = CountManipulationEntries(original);
        var effective = StripHeightManipulationData(original);
        var afterCount = CountManipulationEntries(effective);
        var originalDecodedPreview = TryDecodeManipulationPayloadToJson(original, out var originalJson, out _) ? PreviewForLog(originalJson) : "<decode failed>";
        var filteredDecodedPreview = TryDecodeManipulationPayloadToJson(effective, out var filteredJson, out _) ? PreviewForLog(filteredJson) : "<decode failed>";

        return effective;
    }

    private static string StripHeightManipulationData(string? manipulationData)
    {
        if (string.IsNullOrWhiteSpace(manipulationData))
            return string.Empty;

        try
        {
            if (!TryDecodeManipulationPayloadToJson(manipulationData, out var decodedJson, out var encoding) || string.IsNullOrWhiteSpace(decodedJson))
                return manipulationData;

            var root = JToken.Parse(decodedJson);
            StripHeightManipulationTokens(root);

            if (!ContainsAnyManipulationEntries(root))
                return string.Empty;

            var filteredJson = root.ToString(Formatting.None);
            return EncodeManipulationPayload(filteredJson, encoding);
        }
        catch
        {
            return manipulationData;
        }
    }

    private static void StripHeightManipulationTokens(JToken token)
    {
        if (token is JObject obj)
        {
            foreach (var property in obj.Properties().ToList())
            {
                StripHeightManipulationTokens(property.Value);
            }

            return;
        }

        if (token is not JArray array)
            return;

        for (var i = array.Count - 1; i >= 0; i--)
        {
            var item = array[i];
            if (item == null)
                continue;

            if (item is JObject itemObj && IsHeightManipulationObject(itemObj))
            {
                array.RemoveAt(i);
                continue;
            }

            StripHeightManipulationTokens(item);
        }
    }

    private static bool IsHeightManipulationObject(JObject obj)
    {
        return IsRspManipulationType(obj) && HasHeightAttribute(obj);
    }

    private static int CountManipulationEntries(string? manipulationData)
    {
        if (string.IsNullOrWhiteSpace(manipulationData))
            return 0;

        try
        {
            if (!TryDecodeManipulationPayloadToJson(manipulationData, out var decodedJson, out _))
                return -1;

            if (string.IsNullOrWhiteSpace(decodedJson))
                return 0;

            var root = JToken.Parse(decodedJson);
            return CountManipulationEntries(root);
        }
        catch
        {
            return -1;
        }
    }

    private static int CountManipulationEntries(JToken? token)
    {
        if (token == null)
            return 0;

        if (token is JArray array)
            return array.Count(item => item != null);

        if (token is not JObject obj)
            return 0;

        var count = 0;
        foreach (var property in obj.Properties())
        {
            if (property.Value is JArray manipulations && property.Name.Equals("Manipulations", StringComparison.OrdinalIgnoreCase))
                count += manipulations.Count(item => item != null);
            else
                count += CountManipulationEntries(property.Value);
        }

        return count;
    }

    private static string PreviewForLog(string? value, int max = 400)
    {
        if (string.IsNullOrEmpty(value))
            return "<empty>";

        value = value.Replace("\r", " ").Replace("\n", " ");
        return value.Length <= max ? value : value[..max] + "...";
    }

    private static bool ContainsAnyManipulationEntries(JToken token)
    {
        if (token is JArray array)
            return array.Any(item => item != null);

        if (token is not JObject obj)
            return false;

        foreach (var property in obj.Properties())
        {
            if (property.Value is JArray manipulations && property.Name.Equals("Manipulations", StringComparison.OrdinalIgnoreCase) && manipulations.Any(item => item != null))
                return true;

            if (ContainsAnyManipulationEntries(property.Value))
                return true;
        }

        return false;
    }

    private enum ManipulationPayloadEncoding
    {
        PlainJson,
        Base64Utf8,
        Base64Gzip,
        PenumbraCompressed,
    }

    private static bool TryDecodeManipulationPayloadToJson(string manipulationData, out string json, out ManipulationPayloadEncoding encoding)
    {
        if (string.IsNullOrWhiteSpace(manipulationData))
        {
            json = string.Empty;
            encoding = ManipulationPayloadEncoding.PlainJson;
            return true;
        }

        var trimmed = manipulationData.TrimStart();
        if (trimmed.StartsWith("{", StringComparison.Ordinal) || trimmed.StartsWith("[", StringComparison.Ordinal))
        {
            json = manipulationData;
            encoding = ManipulationPayloadEncoding.PlainJson;
            return true;
        }

        if (TryDecodePenumbraManipulationPayloadToJson(manipulationData, out json))
        {
            encoding = ManipulationPayloadEncoding.PenumbraCompressed;
            return true;
        }

        try
        {
            var bytes = Convert.FromBase64String(manipulationData);
            if (bytes.Length >= 2 && bytes[0] == 0x1F && bytes[1] == 0x8B)
            {
                using var input = new MemoryStream(bytes);
                using var gzip = new GZipStream(input, CompressionMode.Decompress);
                using var reader = new StreamReader(gzip, new UTF8Encoding(false));
                json = reader.ReadToEnd();
                encoding = ManipulationPayloadEncoding.Base64Gzip;
                return true;
            }

            var decoded = Encoding.UTF8.GetString(bytes);
            var decodedTrimmed = decoded.TrimStart();
            if (decodedTrimmed.StartsWith("{", StringComparison.Ordinal) || decodedTrimmed.StartsWith("[", StringComparison.Ordinal))
            {
                json = decoded;
                encoding = ManipulationPayloadEncoding.Base64Utf8;
                return true;
            }
        }
        catch
        {
        }

        json = manipulationData;
        encoding = ManipulationPayloadEncoding.PlainJson;
        return false;
    }

    private static bool TryDecodePenumbraManipulationPayloadToJson(string manipulationData, out string json)
    {
        json = string.Empty;

        try
        {
            var penumbraAssembly = AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(a => string.Equals(a.GetName().Name, "Penumbra", StringComparison.Ordinal));
            var metaApiType = penumbraAssembly?.GetType("Penumbra.Api.Api.MetaApi");
            var convertManipsMethod = metaApiType?.GetMethod("ConvertManips", BindingFlags.Static | BindingFlags.NonPublic);
            if (convertManipsMethod == null)
                return false;

            object?[] args = [manipulationData, null, (byte)0];
            if (convertManipsMethod.Invoke(null, args) is not bool success || !success || args[1] == null)
                return false;

            json = JsonConvert.SerializeObject(args[1], Formatting.None);
            return !string.IsNullOrWhiteSpace(json);
        }
        catch
        {
            json = string.Empty;
            return false;
        }
    }

    private static string EncodeManipulationPayload(string manipulationJson, ManipulationPayloadEncoding encoding)
    {
        if (string.IsNullOrEmpty(manipulationJson))
            return string.Empty;

        return encoding switch
        {
            ManipulationPayloadEncoding.PenumbraCompressed => EncodeManipulationPayloadAsVersion0Base64Gzip(manipulationJson),
            ManipulationPayloadEncoding.Base64Gzip => EncodeManipulationPayloadAsGzipBase64(manipulationJson),
            ManipulationPayloadEncoding.Base64Utf8 => Convert.ToBase64String(Encoding.UTF8.GetBytes(manipulationJson)),
            _ => manipulationJson,
        };
    }

    private static string EncodeManipulationPayloadAsVersion0Base64Gzip(string manipulationJson)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            gzip.WriteByte(0);
            var bytes = Encoding.UTF8.GetBytes(manipulationJson);
            gzip.Write(bytes, 0, bytes.Length);
        }

        return Convert.ToBase64String(output.ToArray());
    }

    private static string EncodeManipulationPayloadAsGzipBase64(string manipulationJson)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        using (var writer = new StreamWriter(gzip, new UTF8Encoding(false)))
        {
            writer.Write(manipulationJson);
        }

        return Convert.ToBase64String(output.ToArray());
    }

    private static bool IsRspManipulationType(JObject obj)
    {
        return HasManipulationType(obj, "Type")
            || HasManipulationType(obj, "ManipulationType")
            || HasManipulationType(obj, "$type");
    }

    private static bool HasManipulationType(JObject obj, string key)
    {
        var typeValue = obj[key]?.Value<string>();
        if (string.IsNullOrWhiteSpace(typeValue))
            return false;

        return string.Equals(typeValue, "Rsp", StringComparison.OrdinalIgnoreCase)
            || typeValue.Contains("RspManipulation", StringComparison.OrdinalIgnoreCase)
            || typeValue.Contains("RaceSex", StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasHeightAttribute(JObject obj)
    {
        var attribute = GetAttributeValue(obj);
        return !string.IsNullOrWhiteSpace(attribute)
            && (attribute.Contains("MaxSize", StringComparison.OrdinalIgnoreCase)
                || attribute.Contains("MinSize", StringComparison.OrdinalIgnoreCase));
    }

    private static string GetAttributeValue(JObject obj)
    {
        var attribute = obj["Attribute"]?.Value<string>();
        if (!string.IsNullOrWhiteSpace(attribute))
            return attribute;

        attribute = obj["attribute"]?.Value<string>();
        if (!string.IsNullOrWhiteSpace(attribute))
            return attribute;

        if (obj["Manipulation"] is JObject manipulationObj)
        {
            var nestedAttribute = manipulationObj["Attribute"]?.Value<string>();
            if (!string.IsNullOrWhiteSpace(nestedAttribute))
                return nestedAttribute;

            nestedAttribute = manipulationObj["attribute"]?.Value<string>();
            if (!string.IsNullOrWhiteSpace(nestedAttribute))
                return nestedAttribute;
        }

        return string.Empty;
    }



}