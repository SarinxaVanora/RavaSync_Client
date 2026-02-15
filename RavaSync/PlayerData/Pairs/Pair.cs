using Dalamud.Game.Gui.ContextMenu;
using Dalamud.Game.Text.SeStringHandling;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.API.Data.Extensions;
using RavaSync.API.Dto.User;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files.Models;
using System;
using System.Linq;

namespace RavaSync.PlayerData.Pairs;

public class Pair
{
    private readonly PairHandlerFactory _cachedPlayerFactory;
    private readonly SemaphoreSlim _creationSemaphore = new(1);
    private readonly ILogger<Pair> _logger;
    private readonly MareMediator _mediator;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly ToyBox _toyBox;

    private CancellationTokenSource _applicationCts = new();
    private OnlineUserIdentDto? _onlineUserIdentDto = null;
    private long _lastUploadStatusTick = 0;
    private CancellationTokenSource? _pendingEmptyApplyCts;

    public static Func<string, bool>? IsBlacklistedCallback { get; set; }

    public Pair(ILogger<Pair> logger, UserFullPairDto userPair, PairHandlerFactory cachedPlayerFactory,
        MareMediator mediator, ServerConfigurationManager serverConfigurationManager)
    {
        _logger = logger;
        UserPair = userPair;
        _cachedPlayerFactory = cachedPlayerFactory;
        _mediator = mediator;
        _serverConfigurationManager = serverConfigurationManager;
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
    || (IsBlacklistedCallback?.Invoke(UserData.UID) ?? false);

    public bool IsVisible => CachedPlayer?.IsVisible ?? false;
    // Whether Customize+ should be applied for this pair (default: ON)
    public bool IsCustomizePlusEnabled { get; set; } = true;
    public bool IsMetadataEnabled { get; set; } = true;

    public CharacterData? LastReceivedCharacterData { get; set; }
    public string? PlayerName => CachedPlayer?.PlayerName ?? string.Empty;
    public long LastAppliedDataBytes => CachedPlayer?.LastAppliedDataBytes ?? -1;
    public long LastAppliedDataTris { get; set; } = -1;
    public long LastAppliedApproximateVRAMBytes { get; set; } = -1;
    public bool AutoPausedByCap { get; set; } = false;
    public bool AutoPausedByScope { get; set; } = false;
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
        var requestManualFileRepair = seStringBuilder5.AddText("Repair broken Sync").Build();
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
            Name = requestManualFileRepair,
            OnClicked = (a) => RequestManualFileRepair(),
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
    }


    public void ApplyData(OnlineUserCharaDataDto data)
    {
        _applicationCts = _applicationCts.CancelRecreate();
        var incoming = data.CharaData;

        if (incoming != null && LastReceivedCharacterData != null && IsUploadingRecently)
        {
            bool incomingHasFiles = incoming.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;
            bool previousHasFiles = LastReceivedCharacterData.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;

            if (!incomingHasFiles && previousHasFiles)
            {
                _pendingEmptyApplyCts?.Cancel();
                _pendingEmptyApplyCts = new CancellationTokenSource();
                var token = _pendingEmptyApplyCts.Token;
                var pending = incoming;

                _logger.LogDebug("Deferring empty file list for {uid} for 750ms while uploader is flagged uploading", UserData.UID);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(750, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested) return;

                        LastReceivedCharacterData = pending;

                        if (CachedPlayer == null)
                        {
                            _logger.LogDebug("Deferred empty-list apply for {uid} but CachedPlayer does not exist, waiting", UserData.UID);
                            return;
                        }

                        _logger.LogDebug("Applying deferred empty file list for {uid}", UserData.UID);
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


        LastReceivedCharacterData = incoming;
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
                    await Task.Delay(250, combined.Token).ConfigureAwait(false);
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

    public void ApplyLastReceivedData(bool forced = false)
    {
        if (CachedPlayer == null) return;
        if (LastReceivedCharacterData == null) return;

        CachedPlayer.ApplyCharacterData(Guid.NewGuid(), RemoveNotSyncedFiles(LastReceivedCharacterData.DeepClone())!, forced);
    }

    public void RequestManualFileRepair()
    {
        if (CachedPlayer == null) return;

        CachedPlayer.RequestManualFileRepair();
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

            LastReceivedCharacterData = null;
            player = CachedPlayer;
            CachedPlayer = null;
            _onlineUserIdentDto = null;
        }
        finally
        {
            if (wait)
                _creationSemaphore.Release();
        }

        // Dispose outside lock
        player?.Dispose();
    }


    public void SetNote(string note)
    {
        _serverConfigurationManager.SetNoteForUid(UserData.UID, note);
    }

    internal void SetIsUploading()
    {
        _lastUploadStatusTick = Environment.TickCount64;
        CachedPlayer?.SetUploading();
    }

    public bool IsUploadingRecently => (Environment.TickCount64 - _lastUploadStatusTick) < 2000;

    public enum VisibleTransferIndicator
    {
        None = 0,
        Downloading = 1,
        LoadingFiles = 2,
    }

    private volatile VisibleTransferIndicator _visibleTransferIndicator = VisibleTransferIndicator.None;
    public VisibleTransferIndicator VisibleTransferStatus => _visibleTransferIndicator;
    internal void SetVisibleTransferStatus(VisibleTransferIndicator status) => _visibleTransferIndicator = status;

    private Dictionary<string, FileDownloadStatus>? _currentDownloadStatus;
    public IReadOnlyDictionary<string, FileDownloadStatus>? CurrentDownloadStatus => _currentDownloadStatus;
    internal void SetCurrentDownloadStatus(Dictionary<string, FileDownloadStatus>? status) => _currentDownloadStatus = status;



    public void ToggleMetadataAndReapply()
    {
        IsMetadataEnabled = !IsMetadataEnabled;
        ApplyLastReceivedData(forced: true);
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
        bool disableIndividualCustomizePlus = (UserPair.OtherPermissions.IsDisableCustomizePlus() || UserPair.OwnPermissions.IsDisableCustomizePlus());

        // NEW: height metadata (Penumbra manipulations)
        bool disableIndividualMetadata = (UserPair.OtherPermissions.IsDisableMetaData() || UserPair.OwnPermissions.IsDisableMetaData());

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

        // NEW: Handle Penumbra metadata (height edits) separately as well
        if (disableIndividualMetadata)
        {
            if (data.ManipulationData != null)
                data.ManipulationData = string.Empty;
        }

        return data;
    }

}