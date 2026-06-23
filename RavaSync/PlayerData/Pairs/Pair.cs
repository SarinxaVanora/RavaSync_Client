using Dalamud.Game.Gui.ContextMenu;
using Dalamud.Game.Text.SeStringHandling;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using ApiCharacterData = RavaSync.API.Data.CharacterData;
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
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Text.Json.Nodes;
using RavaSync.PlayerData.Services;
using MenuItem = Dalamud.Game.Gui.ContextMenu.MenuItem;
using RavaSync.PlayerData.Data;

namespace RavaSync.PlayerData.Pairs;

public class Pair
{
    // RAVASYNC_VISIBILITY_DIAGNOSTICS: temporary Info-level per-pair apply queue tracing. Search '[VIS-DIAG]' to remove later.
    private const string VisibilityDiagnosticsPrefix = "[VIS-DIAG]";

    private void LogVisibilityDiagnostic(string message, params object[] args)
    {
        _logger.LogInformation(VisibilityDiagnosticsPrefix + " " + message, args);
    }

    private readonly PairHandlerFactory _cachedPlayerFactory;
    private readonly SemaphoreSlim _creationSemaphore = new(1);
    private readonly ILogger<Pair> _logger;
    private readonly MareMediator _mediator;
    private readonly MareConfigService _mareConfigService;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly ToyBox _toyBox;
    private static readonly Regex HonorificWebLinkRegex = new(@"(?ix)\b(?:https?://|www\.)\S+|\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:com|net|org|gg|io|dev|tv|uk|co|me|info|xyz|club|link|app|site)\b", RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private CancellationTokenSource _applicationCts = new();
    private OnlineUserIdentDto? _onlineUserIdentDto = null;
    private CancellationTokenSource? _pendingEmptyApplyCts;
    private CancellationTokenSource? _visibleCommitWatchCts;
    private const int PendingEmptyFileListApplyDebounceMs = 500;
    private const int PendingUploadingEmptyFileListApplyDebounceMs = 900;
    private const int ForcedApplyDuplicateWindowMs = 1500;
    private string _lastAcceptedIncomingDataHash = string.Empty;
    private string _lastAcceptedIncomingPayloadFingerprint = string.Empty;
    private string _lastForcedApplyPayloadFingerprint = string.Empty;
    private long _lastForcedApplyTick = -1;


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
    || (AutoPausedByOtherSync && !EffectiveOverrideOtherSync);

    public bool IsVisible => CachedPlayer?.IsVisible ?? false;
    // Whether Customize+ should be applied for this pair (default: ON)
    public bool IsCustomizePlusEnabled { get; set; } = true;
    public bool IsMetadataEnabled { get; set; } = true;
    public bool IsScreenShakeEnabled { get; set; } = true;
    public bool EffectiveScreenShakeEnabled => IsScreenShakeEnabled && _mareConfigService.Current.GlobalSyncScreenShake;
    public bool EffectiveHeightEditsEnabled => IsMetadataEnabled && _mareConfigService.Current.GlobalSyncHeightEdits;

    public ApiCharacterData? LastReceivedCharacterData { get; set; }
    public CharacterRavaSidecarUtility.SyncManifestPayload? LastReceivedSyncManifest { get; private set; }
    public string? PlayerName => CachedPlayer?.PlayerName ?? string.Empty;
    public string LastKnownPlayerName => CachedPlayer?.LastKnownVanillaTeardownPlayerName ?? string.Empty;
    public nint PlayerCharacter => CachedPlayer?.PlayerCharacter ?? nint.Zero;
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


    private void CancelPendingEmptyFileListApply()
    {
        try
        {
            _pendingEmptyApplyCts?.Cancel();
        }
        catch
        {
            // best effort
        }

        _pendingEmptyApplyCts = null;
    }

    public void ApplyData(OnlineUserCharaDataDto data)
    {
        var incoming = data.CharaData;

        // Treat every received user-data payload as the sound-indicator boundary. A redirected SCD
        // load marks this pair as actively making audible synced output; the next payload keeps it
        // only if another SCD was observed for this pair since the previous payload.
        ReconcileActiveSyncIndicatorsOnIncomingUserData();

        incoming = PreservePreviousPlayerPayloadForNonPlayerOnlyIncoming(incoming);

        var incomingHash = incoming?.DataHash.Value ?? string.Empty;
        LogVisibilityDiagnostic("PAIR apply-data receive uid={uid} hash={hash} visible={visible} paused={paused} cachedPlayer={cachedPlayer} lastReceived={lastHash}",
            data.User.UID,
            incomingHash,
            IsVisible,
            IsPaused,
            CachedPlayer != null,
            LastReceivedCharacterData?.DataHash.Value ?? string.Empty);
        var incomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(incoming);
        if (!string.IsNullOrWhiteSpace(incomingHash)
            && string.Equals(incomingHash, _lastAcceptedIncomingDataHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, _lastAcceptedIncomingPayloadFingerprint, StringComparison.Ordinal))
        {
            var clearedUpload = ClearRemoteUploadStatus();
            LogVisibilityDiagnostic("PAIR apply-data duplicate uid={uid} hash={hash} payload={payload} visible={visible} paused={paused} cachedPlayer={cachedPlayer} clearedUpload={clearedUpload}",
                data.User.UID, incomingHash, incomingPayloadFingerprint, IsVisible, IsPaused, CachedPlayer != null, clearedUpload);
            _logger.LogTrace("Ignoring duplicate incoming character data {hash}/{payload} for {uid}; clearedUpload={clearedUpload}", incomingHash, incomingPayloadFingerprint, data.User.UID, clearedUpload);

            // Duplicate payloads can still be useful repair/replay signals: the sender may
            // have force-pushed because a Mesh ready ACK was missed, or because visibility/pair
            // state needed a reassert without changing the payload fingerprint. The forced path
            // has its own short duplicate throttle, so let visible pairs self-heal here.
            if (IsVisible && CachedPlayer != null && LastReceivedCharacterData is ApiCharacterData && !IsPaused)
            {
                LogVisibilityDiagnostic("PAIR apply-data duplicate forcing last-received apply uid={uid} hash={hash}", data.User.UID, incomingHash);
                ApplyLastReceivedData(forced: true);
            }

            return;
        }

        var wasUploadingAtDataArrival = IsUploading;
        ClearRemoteUploadStatus();

        if (IsPaused)
        {
            LogVisibilityDiagnostic("PAIR apply-data stored uid={uid} hash={hash} reason=paused visible={visible}", data.User.UID, incomingHash, IsVisible);
            // While manually paused, cap-paused, scope-paused, or yielded to another sync,
            // receiver data should still be remembered for the eventual unpause/reclaim,
            // but no deferred empty-file-list apply must be allowed to fire later and
            // overwrite a freshly restored modded state with stale vanilla data.
            CancelPendingEmptyFileListApply();
            _applicationCts = _applicationCts.CancelRecreate();
            LastReceivedCharacterData = incoming;
            _lastAcceptedIncomingDataHash = incomingHash;
            _lastAcceptedIncomingPayloadFingerprint = incomingPayloadFingerprint;
            return;
        }

        if (incoming is ApiCharacterData incomingData && LastReceivedCharacterData is ApiCharacterData previousData && IsVisible)
        {
            bool incomingHasFiles = incomingData.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;
            bool previousHasFiles = previousData.FileReplacements?.Any(k => k.Value?.Any() ?? false) ?? false;

            // The sync manifest is the sender's authoritative file-map contract.
            // Without it, an empty FileReplacements payload is ambiguous: it can mean
            // either "the sender genuinely has no file mods" or "the file map was stripped /
            // delayed while metadata was still delivered".  Never let an ambiguous empty
            // player payload clear a previously modded visible player.
            if (!incomingHasFiles && previousHasFiles && !IncomingEmptyFileListIsAuthoritativeClear())
            {
                var preserved = incomingData.DeepClone();
                preserved.FileReplacements = previousData.FileReplacements?
                    .ToDictionary(k => k.Key, v => v.Value?.ToList() ?? []) ?? [];

                _applicationCts = _applicationCts.CancelRecreate();
                LastReceivedCharacterData = preserved;
                _lastAcceptedIncomingDataHash = preserved.DataHash.Value ?? string.Empty;
                _lastAcceptedIncomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(preserved);

                LogVisibilityDiagnostic("PAIR apply-data preserve-files uid={uid} hash={hash} reason=ambiguous-empty-file-list previousHash={previousHash}",
                    data.User.UID, incomingHash, previousData.DataHash.Value ?? string.Empty);
                _logger.LogWarning(
                    "Refusing ambiguous empty file-list clear for {uid}; preserving previous visible player files until an authoritative zero-asset manifest or non-empty manifest-backed payload arrives",
                    UserData.UID);
                ApplyLastReceivedData(forced: true);
                return;
            }

            // During a sender-side item swap the remote can briefly publish an empty
            // file list before the replacement file map arrives. Applying that empty
            // list makes the receiver flash vanilla, so only allow it after a short
            // confirmation window. Non-empty follow-up data cancels this immediately.
            if (!incomingHasFiles && previousHasFiles)
            {
                _pendingEmptyApplyCts?.Cancel();
                _pendingEmptyApplyCts = new CancellationTokenSource();
                var token = _pendingEmptyApplyCts.Token;
                var pending = incomingData;
                var delayMs = wasUploadingAtDataArrival ? PendingUploadingEmptyFileListApplyDebounceMs : PendingEmptyFileListApplyDebounceMs;

                var immediate = incomingData.DeepClone();
                immediate.FileReplacements = previousData.FileReplacements?
                    .ToDictionary(k => k.Key, v => v.Value?.ToList() ?? []) ?? [];

                var previousPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(previousData);
                var immediatePayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(immediate);
                if (!string.Equals(previousPayloadFingerprint, immediatePayloadFingerprint, StringComparison.Ordinal))
                {
                    _applicationCts = _applicationCts.CancelRecreate();

                    LastReceivedCharacterData = immediate;
                    _lastAcceptedIncomingDataHash = immediate.DataHash.Value ?? string.Empty;
                    _lastAcceptedIncomingPayloadFingerprint = immediatePayloadFingerprint;

                    LogVisibilityDiagnostic("PAIR apply-data prompt-non-file-state uid={uid} hash={hash} delayEmptyMs={delayMs}", UserData.UID, immediate.DataHash.Value ?? string.Empty, delayMs);
                    _logger.LogDebug("Applying prompt non-file receiver state for {uid} while deferring empty file-list clear for {delayMs}ms", UserData.UID, delayMs);
                    ApplyLastReceivedData();
                }

                LogVisibilityDiagnostic("PAIR apply-data defer-empty-file-list uid={uid} hash={hash} delayMs={delayMs}", UserData.UID, incomingHash, delayMs);
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

                        LogVisibilityDiagnostic("PAIR apply-data deferred-empty-now-applying uid={uid} hash={hash}", UserData.UID, pending?.DataHash.Value ?? string.Empty);
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
            LogVisibilityDiagnostic("PAIR apply-data waiting-for-cached-player uid={uid} hash={hash} visible={visible}", data.User.UID, incomingHash, IsVisible);
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
                    LogVisibilityDiagnostic("PAIR apply-data delayed-cached-player-ready uid={uid} hash={hash}", data.User.UID, LastReceivedCharacterData?.DataHash.Value ?? string.Empty);
                    _logger.LogDebug("Applying delayed data for {uid}", data.User.UID);
                    ApplyLastReceivedData();
                }
            });
            return;
        }

        LogVisibilityDiagnostic("PAIR apply-data forwarding-immediate uid={uid} hash={hash} visible={visible} forced=false", data.User.UID, incomingHash, IsVisible);
        ApplyLastReceivedData();
    }

    public void SetLastReceivedSyncManifest(CharacterRavaSidecarUtility.SyncManifestPayload? manifest) => LastReceivedSyncManifest = manifest;

    private bool IncomingEmptyFileListIsAuthoritativeClear()
    {
        // New senders always attach a manifest, including zero-file payloads.  A manifest
        // with zero assets is therefore an intentional full file clear.  A manifest with only
        // non-player assets is not allowed to clear the visible player's file bucket here; owned
        // object/minion payloads must remain additive to the player appearance.  No manifest is
        // not proof of a clear; it is an old/stripped/metadata-only payload and must not wipe a
        // visible pair's previously asserted player files.
        return CharacterRavaSidecarUtility.ManifestRepresentsAuthoritativeEmptyFileSet(LastReceivedSyncManifest);
    }

    private ApiCharacterData? PreservePreviousPlayerPayloadForNonPlayerOnlyIncoming(ApiCharacterData? incoming)
    {
        if (incoming is not ApiCharacterData incomingData)
            return incoming;

        if (LastReceivedCharacterData is not ApiCharacterData previous)
            return incoming;

        if (!HasAnyNonPlayerFilePayload(incomingData) || !HasObjectKindFilePayload(previous, ObjectKind.Player))
            return incoming;

        // Owned-object/minion payloads can still carry a small/stale Player file bucket while the
        // sender is settling a DRK pet/summon.  Metadata/plugin values may legitimately change in
        // the same payload, but the file bucket must remain additive here; otherwise a minion VFX
        // update can replace the active DRK Player temp-mod file set with a partial owned-object
        // view and knock job skills back to vanilla on the receiver.
        var incomingCarriesPlayerFiles = HasObjectKindFilePayload(incomingData, ObjectKind.Player);
        var incomingCarriesPlayerGlamourer = incomingData.GlamourerData != null
            && incomingData.GlamourerData.TryGetValue(ObjectKind.Player, out var incomingPlayerGlamourer)
            && !string.IsNullOrWhiteSpace(incomingPlayerGlamourer);
        var incomingCarriesPlayerCustomize = incomingData.CustomizePlusData != null
            && incomingData.CustomizePlusData.TryGetValue(ObjectKind.Player, out var incomingPlayerCustomize)
            && !string.IsNullOrWhiteSpace(incomingPlayerCustomize);
        var incomingCarriesExplicitPlayerAppearance = incomingCarriesPlayerFiles || incomingCarriesPlayerGlamourer || incomingCarriesPlayerCustomize;

        var clone = incomingData.DeepClone();
        clone.FileReplacements ??= [];
        clone.GlamourerData ??= [];
        clone.CustomizePlusData ??= [];

        var mergedPlayerFiles = new List<FileReplacementData>();
        if (previous.FileReplacements != null
            && previous.FileReplacements.TryGetValue(ObjectKind.Player, out var previousPlayerFiles)
            && previousPlayerFiles != null)
        {
            mergedPlayerFiles.AddRange(previousPlayerFiles.DeepClone() ?? []);
        }

        if (clone.FileReplacements.TryGetValue(ObjectKind.Player, out var incomingPlayerFiles)
            && incomingPlayerFiles != null)
        {
            mergedPlayerFiles.AddRange(incomingPlayerFiles.DeepClone() ?? []);
        }

        clone.FileReplacements[ObjectKind.Player] = mergedPlayerFiles
            .Where(item => item != null)
            .Distinct(FileReplacementDataComparer.Instance)
            .ToList();

        if (previous.GlamourerData != null
            && previous.GlamourerData.TryGetValue(ObjectKind.Player, out var previousGlamourer)
            && !clone.GlamourerData.ContainsKey(ObjectKind.Player))
        {
            clone.GlamourerData[ObjectKind.Player] = previousGlamourer;
        }

        if (previous.CustomizePlusData != null
            && previous.CustomizePlusData.TryGetValue(ObjectKind.Player, out var previousCustomize)
            && !clone.CustomizePlusData.ContainsKey(ObjectKind.Player))
        {
            clone.CustomizePlusData[ObjectKind.Player] = previousCustomize;
        }

        if (!incomingCarriesExplicitPlayerAppearance)
        {
            if (string.IsNullOrWhiteSpace(clone.ManipulationData) && !string.IsNullOrWhiteSpace(previous.ManipulationData))
                clone.ManipulationData = previous.ManipulationData;

            if (string.IsNullOrWhiteSpace(clone.HeelsData) && !string.IsNullOrWhiteSpace(previous.HeelsData))
                clone.HeelsData = previous.HeelsData;

            if (string.IsNullOrWhiteSpace(clone.HonorificData) && !string.IsNullOrWhiteSpace(previous.HonorificData))
                clone.HonorificData = previous.HonorificData;

            if (string.IsNullOrWhiteSpace(clone.MoodlesData) && !string.IsNullOrWhiteSpace(previous.MoodlesData))
                clone.MoodlesData = previous.MoodlesData;

            if (string.IsNullOrWhiteSpace(clone.PetNamesData) && !string.IsNullOrWhiteSpace(previous.PetNamesData))
                clone.PetNamesData = previous.PetNamesData;
        }

        _logger.LogTrace("Preserved/merged previous Player payload while accepting non-player incoming data for {uid}; owned-object/minion updates will not drop active player temp mods", UserData.UID);
        return clone;
    }

    private static bool HasMeaningfulPlayerAppearanceChange(ApiCharacterData incoming, ApiCharacterData previous)
    {
        return HasChangedPlayerMapValue(incoming.GlamourerData, previous.GlamourerData)
            || HasChangedPlayerMapValue(incoming.CustomizePlusData, previous.CustomizePlusData)
            || HasChangedPlayerString(incoming.ManipulationData, previous.ManipulationData)
            || HasChangedPlayerString(incoming.HeelsData, previous.HeelsData)
            || HasChangedPlayerString(incoming.HonorificData, previous.HonorificData)
            || HasChangedPlayerString(incoming.MoodlesData, previous.MoodlesData)
            || HasChangedPlayerString(incoming.PetNamesData, previous.PetNamesData);
    }

    private static bool HasChangedPlayerMapValue(Dictionary<ObjectKind, string>? incomingMap, Dictionary<ObjectKind, string>? previousMap)
    {
        if (incomingMap == null
            || !incomingMap.TryGetValue(ObjectKind.Player, out var incomingValue)
            || string.IsNullOrWhiteSpace(incomingValue))
        {
            return false;
        }

        var previousValue = string.Empty;
        previousMap?.TryGetValue(ObjectKind.Player, out previousValue);
        return !string.Equals(incomingValue, previousValue ?? string.Empty, StringComparison.Ordinal);
    }

    private static bool HasChangedPlayerString(string? incomingValue, string? previousValue)
        => !string.Equals(incomingValue ?? string.Empty, previousValue ?? string.Empty, StringComparison.Ordinal);

    private static bool HasObjectKindFilePayload(ApiCharacterData data, ObjectKind kind)
        => data.FileReplacements != null
            && data.FileReplacements.TryGetValue(kind, out var replacements)
            && replacements != null
            && replacements.Any();

    private static bool HasAnyNonPlayerFilePayload(ApiCharacterData data)
        => data.FileReplacements != null
            && data.FileReplacements.Any(kvp => kvp.Key != ObjectKind.Player && (kvp.Value?.Any() ?? false));

    internal bool IsDuplicateIncomingPayload(ApiCharacterData? incoming)
    {
        var incomingHash = incoming?.DataHash.Value ?? string.Empty;
        if (string.IsNullOrWhiteSpace(incomingHash))
            return false;

        var incomingPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(incoming);

        return string.Equals(incomingHash, _lastAcceptedIncomingDataHash, StringComparison.Ordinal)
            && string.Equals(incomingPayloadFingerprint, _lastAcceptedIncomingPayloadFingerprint, StringComparison.Ordinal);
    }

    internal ApiCharacterData? PrepareCharacterDataForLocalApply(ApiCharacterData? data)
    {
        return RemoveNotSyncedFiles(data);
    }

    public void ApplyLastReceivedData(bool forced = false)
    {
        if (IsPaused)
        {
            LogVisibilityDiagnostic("PAIR apply-last skip uid={uid} forced={forced} reason=paused", UserData.UID, forced);
            return;
        }

        if (AutoPausedByOtherSync && !EffectiveOverrideOtherSync)
        {
            LogVisibilityDiagnostic("PAIR apply-last skip uid={uid} forced={forced} reason=other-sync owner={owner}", UserData.UID, forced, RemoteOtherSyncOwner);
            return;
        }

        if (CachedPlayer == null)
        {
            LogVisibilityDiagnostic("PAIR apply-last skip uid={uid} forced={forced} reason=no-cached-player visible={visible}", UserData.UID, forced, IsVisible);
            return;
        }

        if (LastReceivedCharacterData is not ApiCharacterData sourceData)
        {
            LogVisibilityDiagnostic("PAIR apply-last skip uid={uid} forced={forced} reason=no-last-character-data visible={visible}", UserData.UID, forced, IsVisible);
            return;
        }

        var preparedDataMaybe = PrepareCharacterDataForLocalApply(sourceData.DeepClone());
        if (preparedDataMaybe is not ApiCharacterData preparedData)
        {
            LogVisibilityDiagnostic("PAIR apply-last skip uid={uid} forced={forced} reason=prepare-returned-null hash={hash}", UserData.UID, forced, sourceData.DataHash.Value ?? string.Empty);
            return;
        }

        LogVisibilityDiagnostic("PAIR apply-last submit uid={uid} hash={hash} payload={payload} forced={forced} visible={visible}",
            UserData.UID,
            preparedData.DataHash.Value ?? string.Empty,
            PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(preparedData),
            forced,
            IsVisible);
        SubmitPreparedCharacterData(preparedData, forced);
    }

    private bool SubmitPreparedCharacterData(ApiCharacterData preparedData, bool forced)
    {
        if (IsPaused)
        {
            LogVisibilityDiagnostic("PAIR submit skip uid={uid} forced={forced} reason=paused", UserData.UID, forced);
            return false;
        }

        if (AutoPausedByOtherSync && !EffectiveOverrideOtherSync)
        {
            LogVisibilityDiagnostic("PAIR submit skip uid={uid} forced={forced} reason=other-sync owner={owner}", UserData.UID, forced, RemoteOtherSyncOwner);
            return false;
        }

        if (CachedPlayer == null)
        {
            LogVisibilityDiagnostic("PAIR submit skip uid={uid} forced={forced} reason=no-cached-player", UserData.UID, forced);
            return false;
        }

        var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(preparedData);

        if (forced)
        {
            var now = Environment.TickCount64;

            if (CachedPlayer.IsPairSyncBusyForPayload(payloadFingerprint))
            {
                LogVisibilityDiagnostic("PAIR submit forced-skip uid={uid} payload={payload} reason=busy-for-payload", UserData.UID, payloadFingerprint);
                _logger.LogTrace("Skipping duplicate forced reapply for {uid}; payload {payload} is already queued/active", UserData.UID, payloadFingerprint);
                return true;
            }

            if (string.Equals(payloadFingerprint, _lastForcedApplyPayloadFingerprint, StringComparison.Ordinal)
                && unchecked(now - _lastForcedApplyTick) >= 0
                && unchecked(now - _lastForcedApplyTick) < ForcedApplyDuplicateWindowMs)
            {
                LogVisibilityDiagnostic("PAIR submit forced-skip uid={uid} payload={payload} reason=duplicate-window elapsedMs={elapsed}", UserData.UID, payloadFingerprint, unchecked(now - _lastForcedApplyTick));
                _logger.LogTrace("Skipping duplicate forced reapply for {uid}; payload {payload} was requested {elapsed}ms ago", UserData.UID, payloadFingerprint, unchecked(now - _lastForcedApplyTick));
                return true;
            }

            _lastForcedApplyPayloadFingerprint = payloadFingerprint;
            _lastForcedApplyTick = now;
        }

        var applicationId = Guid.NewGuid();
        LogVisibilityDiagnostic("PAIR submit accepted uid={uid} app={app} hash={hash} payload={payload} forced={forced}",
            UserData.UID,
            applicationId,
            preparedData.DataHash.Value ?? string.Empty,
            payloadFingerprint,
            forced);
        CachedPlayer.ApplyCharacterData(applicationId, preparedData, forced);
        return true;
    }

    internal void EnsureVisibleReceivedPayloadCommits(ApiCharacterData? expectedData, string reason)
    {
        if (expectedData == null)
            return;

        CancellationTokenSource? previous;
        var cts = new CancellationTokenSource();
        previous = Interlocked.Exchange(ref _visibleCommitWatchCts, cts);
        previous?.CancelDispose();

        _ = Task.Run(() => EnsureVisibleReceivedPayloadCommitsAsync(expectedData.DeepClone(), reason, cts.Token), cts.Token);
    }

    internal async Task<bool> WaitForVisibleReceivedPayloadCommitAsync(ApiCharacterData? expectedData, TimeSpan timeout, CancellationToken token)
    {
        if (expectedData == null)
            return false;

        using var timeoutCts = new CancellationTokenSource(timeout);
        using var combined = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, token);
        return await EnsureVisibleReceivedPayloadCommitsAsync(expectedData.DeepClone(), "mesh ready acknowledgement", combined.Token).ConfigureAwait(false);
    }

    private async Task<bool> EnsureVisibleReceivedPayloadCommitsAsync(ApiCharacterData expectedData, string reason, CancellationToken token)
    {
        var preparedDataMaybe = PrepareCharacterDataForLocalApply(expectedData.DeepClone());
        if (preparedDataMaybe is not ApiCharacterData preparedData)
        {
            LogVisibilityDiagnostic("PAIR commit-watch abort uid={uid} reason=prepare-null hash={hash} sourceReason={sourceReason}", UserData.UID, expectedData.DataHash.Value ?? string.Empty, reason);
            return false;
        }

        var expectedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(preparedData);
        LogVisibilityDiagnostic("PAIR commit-watch start uid={uid} hash={hash} payload={payload} reason={reason}", UserData.UID, preparedData.DataHash.Value ?? string.Empty, expectedPayloadFingerprint, reason);
        var firstLog = true;

        while (!token.IsCancellationRequested)
        {
            if (IsPaused || AutoPausedByOtherSync && !EffectiveOverrideOtherSync)
                return false;

            if (LastReceivedCharacterData is ApiCharacterData latest)
            {
                var latestPreparedMaybe = PrepareCharacterDataForLocalApply(latest.DeepClone());
                if (latestPreparedMaybe is ApiCharacterData latestPrepared)
                {
                    var latestFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(latestPrepared);
                    if (!string.Equals(latestFingerprint, expectedPayloadFingerprint, StringComparison.Ordinal))
                    {
                        _logger.LogTrace("Stopping visible commit watch for {uid}; newer payload superseded {payload} during {reason}", UserData.UID, expectedPayloadFingerprint, reason);
                        return false;
                    }
                }
            }

            if (CachedPlayer != null && IsVisible)
            {
                if (CachedPlayer.HasCommittedPairSyncPayload(expectedPayloadFingerprint))
                {
                    LogVisibilityDiagnostic("PAIR commit-watch committed uid={uid} payload={payload} reason={reason}", UserData.UID, expectedPayloadFingerprint, reason);
                    return true;
                }

                if (!CachedPlayer.HasPairSyncWorkPendingForPayload(expectedPayloadFingerprint))
                {
                    if (firstLog)
                    {
                        LogVisibilityDiagnostic("PAIR commit-watch resubmit uid={uid} payload={payload} reason={reason} visible={visible}", UserData.UID, expectedPayloadFingerprint, reason, IsVisible);
                        _logger.LogDebug("Visible payload {payload} for {uid} has not committed yet after {reason}; re-submitting authoritative receiver apply", expectedPayloadFingerprint, UserData.UID, reason);
                        firstLog = false;
                    }

                    SubmitPreparedCharacterData(preparedData.DeepClone(), forced: true);
                }
            }

            try
            {
                await Task.Delay(75, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var finalCommitted = CachedPlayer != null && CachedPlayer.HasCommittedPairSyncPayload(expectedPayloadFingerprint);
        LogVisibilityDiagnostic("PAIR commit-watch end uid={uid} payload={payload} reason={reason} committed={committed}", UserData.UID, expectedPayloadFingerprint, reason, finalCommitted);
        return finalCommitted;
    }

    public void GoBackToVanillaState()
    {
        CancelPendingEmptyFileListApply();
        try
        {
            _visibleCommitWatchCts?.Cancel();
        }
        catch
        {
            // best effort
        }

        CachedPlayer?.GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
    }

    public void ReclaimFromOtherSync(bool requestApplyIfPossible = true)
    {
        AutoPausedByOtherSync = false;
        AutoPausedByOtherSyncName = string.Empty;
        CancelPendingEmptyFileListApply();

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
        CancelPendingEmptyFileListApply();
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
        OnlineUserIdentDto? dtoToApplyAfterOldDispose = null;
        bool shouldCreate = false;
        bool identChanged = false;
        string oldIdent = string.Empty;
        string newIdent = string.Empty;

        try
        {
            _creationSemaphore.Wait();

            oldIdent = _onlineUserIdentDto?.Ident ?? string.Empty;
            newIdent = dto?.Ident ?? string.Empty;
            identChanged = dto != null
                && !string.IsNullOrEmpty(newIdent)
                && !string.Equals(oldIdent, newIdent, StringComparison.Ordinal);

            if (CachedPlayer != null && !identChanged)
            {
                if (dto != null)
                    _onlineUserIdentDto = dto;

                return;
            }

            if (CachedPlayer != null && identChanged)
            {
                // Keep Pair.Ident on the old ident until the old handler has disposed.
                // Its teardown/vanilla restore path may still need to validate the old
                // live actor, and swapping the ident too early can make that cleanup
                // target the newly recreated actor instead.
                oldPlayer = CachedPlayer;
                CachedPlayer = null;
                dtoToApplyAfterOldDispose = dto;
                shouldCreate = true;
            }
            else if (dto == null && _onlineUserIdentDto == null)
            {
                oldPlayer = CachedPlayer;
                CachedPlayer = null;
                return;
            }
            else
            {
                if (dto != null)
                    _onlineUserIdentDto = dto;

                shouldCreate = _onlineUserIdentDto != null;
            }
        }
        finally
        {
            _creationSemaphore.Release();
        }

        if (identChanged && oldPlayer != null)
        {
            _logger.LogDebug(
                "Online ident changed for {uid}; recreating cached pair handler ({oldIdent} -> {newIdent})",
                UserData.UID,
                string.IsNullOrEmpty(oldIdent) ? "<none>" : oldIdent,
                string.IsNullOrEmpty(newIdent) ? "<none>" : newIdent);
        }

        // Dispose outside lock, but before swapping the online ident for a recreated actor.
        if (oldPlayer != null)
        {
            try
            {
                oldPlayer.GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to force vanilla teardown while recreating cached handler for {uid}", UserData.UID);
            }

            oldPlayer.Dispose();
        }

        if (dtoToApplyAfterOldDispose != null)
        {
            var acceptedNewIdent = false;

            try
            {
                _creationSemaphore.Wait();

                if (string.Equals(_onlineUserIdentDto?.Ident ?? string.Empty, oldIdent, StringComparison.Ordinal))
                {
                    _onlineUserIdentDto = dtoToApplyAfterOldDispose;
                    acceptedNewIdent = true;
                }
            }
            finally
            {
                _creationSemaphore.Release();
            }

            if (!acceptedNewIdent)
                shouldCreate = false;
        }

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

    public void MarkOffline(bool wait = true, bool queuePerPairTeardown = true)
    {
        PairHandler? player = null;

        try
        {
            if (wait)
                _creationSemaphore.Wait();

            _applicationCts = _applicationCts.CancelRecreate();
            _pendingEmptyApplyCts?.Cancel();
            _pendingEmptyApplyCts = null;
            _visibleCommitWatchCts?.Cancel();
            _visibleCommitWatchCts = null;
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

        if (player != null)
        {
            try
            {
                if (queuePerPairTeardown)
                {
                    // Offline/disconnect can arrive before the visibility frame sees the actor vanish.
                    // Force the same hard vanilla path used by IsVisible=false so stale temp
                    // collections/customisation cannot survive a leave-room + disconnect sequence.
                    player.GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
                }
                else
                {
                    // A global slate-wipe is already queued by PairManager. Still reset the local handler
                    // state so Dispose does not run the old synchronous restore path on the hot disconnect path.
                    player.GoBackToVanillaState(revertLiveCustomizationState: false, waitForCompletion: false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to queue vanilla teardown while marking {uid} offline", UserData.UID);
            }

            player.Dispose();
        }

        _onlineUserIdentDto = null;

        SetUploadState(false);
    }



    public void SetNote(string note)
    {
        _serverConfigurationManager.SetNoteForUid(UserData.UID, note);
    }

    private bool _isUploading;

    internal void SetUploadState(bool uploading)
    {
        if (_isUploading == uploading)
            return;

        _isUploading = uploading;

        try
        {
            _mediator.Publish(new RefreshUiMessage());
        }
        catch
        {
            // best effort; CompactUI/world text will also refresh naturally next frame
        }
    }

    internal void SetIsUploading()
    {
        SetUploadState(true);
        CachedPlayer?.SetUploading();
    }

    internal bool ClearRemoteUploadStatus()
    {
        var wasUploading = _isUploading;
        SetUploadState(false);

        CachedPlayer?.SetUploading(false);
        return wasUploading;
    }

    public bool IsUploading => _isUploading;
    public bool IsUploadingRecently => _isUploading;

    private readonly object _activeSyncIndicatorLock = new();
    private bool _isPlayingSound;
    private bool _isLongLivedSoundPlaying;
    private bool _longLivedSoundObservedSinceLastUserDataPush;
    private bool _hasRemoteActiveSoundIndicatorState;

    internal readonly record struct ActiveSyncResourceIndicator(string GamePath, nint Address, bool Vfx, bool Sound);

    // VFX indicators were intentionally removed. Audio is the only live activity marker now.
    public bool IsUsingVfx => false;
    public bool IsPlayingSound => IsVisible && !IsPaused && IsSyncCategoryEffectivelyEnabled(SyncPayloadKind.Sound) && HasActiveSyncIndicator(sound: true);

    internal void MarkActiveSyncResource(bool vfx, bool sound, bool longLived, string? gamePath = null, nint address = 0)
    {
        if (!sound)
            return;

        var normalizedGamePath = NormalizeActiveSyncIndicatorPath(gamePath);
        if (string.IsNullOrWhiteSpace(normalizedGamePath) || !IsLiveSoundActiveSyncIndicatorPath(normalizedGamePath))
            return;

        var now = Environment.TickCount64;
        bool changed;
        lock (_activeSyncIndicatorLock)
        {
            if (_hasRemoteActiveSoundIndicatorState)
                return;

            // Legacy fallback for peers that do not send the active-sound sidecar yet.
            // Current peers mirror the sender's own local indicator state instead, because
            // receiver-side SCD classification cannot distinguish every one-shot from every loop.
            _longLivedSoundObservedSinceLastUserDataPush = true;
            _isLongLivedSoundPlaying = true;

            changed = RefreshPlayingSoundCompositeUnsafe(now);
        }

        if (changed)
            PublishActiveSyncIndicatorRefresh();
    }

    internal bool ReconcileActiveSyncIndicatorsOnIncomingUserData()
    {
        var now = Environment.TickCount64;
        bool changed;
        lock (_activeSyncIndicatorLock)
        {
            if (_hasRemoteActiveSoundIndicatorState)
                return false;

            _isLongLivedSoundPlaying = _longLivedSoundObservedSinceLastUserDataPush;
            _longLivedSoundObservedSinceLastUserDataPush = false;
            changed = RefreshPlayingSoundCompositeUnsafe(now);
        }

        if (changed)
            PublishActiveSyncIndicatorRefresh();

        return changed;
    }

    internal bool ApplyRemoteActiveSoundIndicator(bool isPlayingSound)
    {
        bool changed;
        lock (_activeSyncIndicatorLock)
        {
            _hasRemoteActiveSoundIndicatorState = true;
            _longLivedSoundObservedSinceLastUserDataPush = false;
            _isLongLivedSoundPlaying = isPlayingSound;
            changed = RefreshPlayingSoundCompositeUnsafe(Environment.TickCount64);
        }

        if (changed)
            PublishActiveSyncIndicatorRefresh();

        return changed;
    }

    internal bool ClearActiveSyncIndicators()
    {
        bool changed;
        lock (_activeSyncIndicatorLock)
        {
            changed = _isPlayingSound || _isLongLivedSoundPlaying || _longLivedSoundObservedSinceLastUserDataPush || _hasRemoteActiveSoundIndicatorState;
            _isPlayingSound = false;
            _isLongLivedSoundPlaying = false;
            _longLivedSoundObservedSinceLastUserDataPush = false;
            _hasRemoteActiveSoundIndicatorState = false;
        }

        if (changed)
            PublishActiveSyncIndicatorRefresh();

        return changed;
    }

    internal IReadOnlyList<ActiveSyncResourceIndicator> SnapshotActiveSyncResourceIndicators() => Array.Empty<ActiveSyncResourceIndicator>();

    internal bool ReconcileActiveSyncResourcesForAddress(nint address, IReadOnlySet<string> liveGamePaths) => false;

    internal bool ReconcileActiveSyncResourcesWithAppliedPaths(IReadOnlySet<string> appliedGamePaths) => false;

    internal bool PruneExpiredActiveSyncIndicators()
    {
        var now = Environment.TickCount64;
        bool changed;
        lock (_activeSyncIndicatorLock)
        {
            changed = RefreshPlayingSoundCompositeUnsafe(now);
        }

        if (changed)
            PublishActiveSyncIndicatorRefresh();

        return changed;
    }

    private bool HasActiveSyncIndicator(bool vfx = false, bool sound = false)
    {
        lock (_activeSyncIndicatorLock)
            return HasActiveSyncIndicatorUnsafe(vfx, sound);
    }

    private bool HasActiveSyncIndicatorUnsafe(bool vfx = false, bool sound = false)
        => sound && _isPlayingSound;

    private bool RefreshPlayingSoundCompositeUnsafe(long now)
    {
        var shouldPlay = _isLongLivedSoundPlaying;
        var changed = _isPlayingSound != shouldPlay;
        _isPlayingSound = shouldPlay;
        return changed;
    }

    private static string NormalizeActiveSyncIndicatorPath(string? path)
    {
        return (path ?? string.Empty)
            .Replace('\\', '/')
            .Trim()
            .TrimStart('/')
            .ToLowerInvariant();
    }

    private static bool IsLiveSoundActiveSyncIndicatorPath(string? path)
        => string.Equals(Path.GetExtension(NormalizeActiveSyncIndicatorPath(path)), ".scd", StringComparison.OrdinalIgnoreCase);

    private void PublishActiveSyncIndicatorRefresh()
    {
        try
        {
            _mediator.Publish(new RefreshUiMessage());
        }
        catch
        {
            // best effort; Compact UI/world text will also refresh naturally next frame
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

    public void SetScreenShakeEnabled(bool enabled, bool reapply = true)
    {
        if (IsScreenShakeEnabled == enabled)
            return;

        IsScreenShakeEnabled = enabled;
        PersistLocalSyncPreference(_mareConfigService.Current.LocalScreenShakeDisabledUids, enabled);
        if (reapply)
            ApplyLastReceivedData(forced: true);
    }

    public void ToggleScreenShakeAndReapply()
    {
        SetScreenShakeEnabled(!IsScreenShakeEnabled);
    }

    public void ForceManipulationReapply()
    {
        CachedPlayer?.ForceManipulationReapply();
    }

    private void RestoreLocalSyncPreferences()
    {
        var uid = UserData.UID;
        if (string.IsNullOrWhiteSpace(uid))
            return;

        IsCustomizePlusEnabled = !_mareConfigService.Current.LocalCustomizePlusDisabledUids.Contains(uid);
        IsMetadataEnabled = !_mareConfigService.Current.LocalHeightMetadataDisabledUids.Contains(uid);
        IsScreenShakeEnabled = !_mareConfigService.Current.LocalScreenShakeDisabledUids.Contains(uid);
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

    private ApiCharacterData? RemoveNotSyncedFiles(ApiCharacterData? data)
    {
        _logger.LogTrace("Removing not synced files");
        if (data is not ApiCharacterData dataValue)
        {
            _logger.LogTrace("Nothing to remove");
            return data;
        }

        bool disableIndividualAnimations = (UserPair.OtherPermissions.IsDisableAnimations() || UserPair.OwnPermissions.IsDisableAnimations() || !_mareConfigService.Current.GlobalSyncAnimations);
        bool disableIndividualVFX = (UserPair.OtherPermissions.IsDisableVFX() || UserPair.OwnPermissions.IsDisableVFX() || !_mareConfigService.Current.GlobalSyncVfx);
        bool disableIndividualSounds = (UserPair.OtherPermissions.IsDisableSounds() || UserPair.OwnPermissions.IsDisableSounds() || !_mareConfigService.Current.GlobalSyncSounds);
        bool disableIndividualCustomizePlus = !IsCustomizePlusEnabled || !_mareConfigService.Current.GlobalSyncCustomizePlus;
        bool disableIndividualMetadata = !EffectiveHeightEditsEnabled;
        bool disableHonorific = !_mareConfigService.Current.GlobalSyncHonorific || ShouldHideHonorificTitle(dataValue.HonorificData);
        bool disableMoodles = !_mareConfigService.Current.GlobalSyncMoodles;
        bool disablePetNames = !_mareConfigService.Current.GlobalSyncPetNames;
        bool disableScreenShake = !EffectiveScreenShakeEnabled;

        _logger.LogTrace("Disable: Sounds: {disableIndividualSounds}, Anims: {disableIndividualAnims}; VFX: {disableIndividualVFX}, Customize+: {disableIndividualCustomizePlus}, HeightEdits: {disableIndividualMetadata}, Honorific: {disableHonorific}, Moodles: {disableMoodles}, PetNames: {disablePetNames}, ScreenShake: {disableScreenShake}",
            disableIndividualSounds, disableIndividualAnimations, disableIndividualVFX, disableIndividualCustomizePlus, disableIndividualMetadata, disableHonorific, disableMoodles, disablePetNames, disableScreenShake);

        if ((disableIndividualAnimations || disableIndividualSounds || disableIndividualVFX)
            && dataValue.FileReplacements != null
            && dataValue.FileReplacements.Count > 0)
        {
            _logger.LogTrace("Data cleaned up: Animations disabled: {disableAnimations}, Sounds disabled: {disableSounds}, VFX disabled: {disableVFX}",
                disableIndividualAnimations, disableIndividualSounds, disableIndividualVFX);

            foreach (var objectKind in dataValue.FileReplacements.Keys)
            {
                if (disableIndividualSounds)
                    dataValue.FileReplacements[objectKind] = dataValue.FileReplacements[objectKind]
                        .Where(static f => !ReplacementHasAnyGamePathEnding(f, ".scd"))
                        .ToList();

                if (disableIndividualAnimations)
                    dataValue.FileReplacements[objectKind] = dataValue.FileReplacements[objectKind]
                        .Where(static f => !ReplacementHasAnyGamePathEnding(f, ".tmb", ".tmb2", ".pap"))
                        .ToList();

                if (disableIndividualVFX)
                    dataValue.FileReplacements[objectKind] = dataValue.FileReplacements[objectKind]
                        .Where(static f => !ReplacementHasAnyGamePathEnding(f, ".atex", ".avfx", ".eid", ".skp", ".shpk"))
                        .ToList();
            }
        }

        PruneEmptyFilteredFileReplacementBuckets(dataValue);

        // Handle Customize+ data separately since it's not in FileReplacements
        if (disableIndividualCustomizePlus)
        {
            dataValue.CustomizePlusData.Clear();
        }

        if (disableIndividualMetadata)
        {
            dataValue.ManipulationData = GetEffectiveManipulationData(dataValue.ManipulationData);
        }

        if (disableHonorific)
        {
            dataValue.HonorificData = string.Empty;
        }

        if (disableMoodles)
        {
            dataValue.MoodlesData = string.Empty;
        }

        if (disablePetNames)
        {
            dataValue.PetNamesData = string.Empty;
        }

        return dataValue;
    }

    private static void PruneEmptyFilteredFileReplacementBuckets(ApiCharacterData data)
    {
        if (data.FileReplacements == null || data.FileReplacements.Count == 0)
            return;

        foreach (var objectKind in data.FileReplacements.Keys.ToArray())
        {
            var replacements = data.FileReplacements[objectKind];
            if (replacements == null || replacements.Count == 0)
            {
                data.FileReplacements.Remove(objectKind);
                continue;
            }

            var cleaned = replacements
                .Where(static replacement => replacement != null && HasUsableFilteredReplacement(replacement))
                .ToList();

            if (cleaned.Count == 0)
                data.FileReplacements.Remove(objectKind);
            else if (cleaned.Count != replacements.Count)
                data.FileReplacements[objectKind] = cleaned;
        }
    }

    private static bool HasUsableFilteredReplacement(FileReplacementData replacement)
    {
        var hasGamePath = replacement.GamePaths?.Any(static path => !string.IsNullOrWhiteSpace(path)) ?? false;
        if (!hasGamePath)
            return false;

        return !string.IsNullOrWhiteSpace(replacement.Hash)
            || !string.IsNullOrWhiteSpace(replacement.FileSwapPath);
    }

    private static bool ReplacementHasAnyGamePathEnding(FileReplacementData replacement, params string[] suffixes)
    {
        if (replacement.GamePaths == null || replacement.GamePaths.Count() == 0 || suffixes.Length == 0)
            return false;

        return replacement.GamePaths.Any(path => !string.IsNullOrWhiteSpace(path)
            && suffixes.Any(suffix => path.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)));
    }


    private bool ShouldHideHonorificTitle(string? honorificData)
    {
        if (string.IsNullOrWhiteSpace(honorificData))
            return false;

        var title = TryExtractHonorificTitleText(honorificData);
        if (string.IsNullOrWhiteSpace(title))
            return false;

        if (_mareConfigService.Current.GlobalHideHonorificTitlesUsingKeywords
            && ContainsHiddenHonorificTitleKeyword(title))
            return true;

        return _mareConfigService.Current.GlobalHideHonorificWebLinks
            && HonorificWebLinkRegex.IsMatch(title);
    }

    private bool ContainsHiddenHonorificTitleKeyword(string title)
    {
        var keywords = _mareConfigService.Current.GlobalHonorificTitleHiddenKeywords;
        if (keywords == null || keywords.Count == 0)
            return false;

        foreach (var rawKeyword in keywords)
        {
            var keyword = rawKeyword?.Trim();
            if (string.IsNullOrWhiteSpace(keyword))
                continue;

            if (title.IndexOf(keyword, StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }

        return false;
    }

    private static string TryExtractHonorificTitleText(string honorificData)
    {
        try
        {
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(honorificData));
            if (string.IsNullOrWhiteSpace(decoded))
                return string.Empty;

            try
            {
                using var document = System.Text.Json.JsonDocument.Parse(decoded);
                if (document.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object
                    && document.RootElement.TryGetProperty("Title", out var titleElement)
                    && titleElement.ValueKind == System.Text.Json.JsonValueKind.String)
                    return titleElement.GetString() ?? string.Empty;
            }
            catch
            {
                // Honorific payloads should be JSON, but fall back to scanning the decoded text if that ever changes.
            }

            return decoded;
        }
        catch
        {
            return string.Empty;
        }
    }

    private enum SyncPayloadKind
    {
        Sound,
        Vfx,
    }

    private bool IsSyncCategoryEffectivelyEnabled(SyncPayloadKind kind)
    {
        return kind switch
        {
            SyncPayloadKind.Sound => !UserPair.OtherPermissions.IsDisableSounds() && !UserPair.OwnPermissions.IsDisableSounds() && _mareConfigService.Current.GlobalSyncSounds,
            SyncPayloadKind.Vfx => !UserPair.OtherPermissions.IsDisableVFX() && !UserPair.OwnPermissions.IsDisableVFX() && _mareConfigService.Current.GlobalSyncVfx,
            _ => true,
        };
    }

    private bool ContainsLastReceivedPayload(SyncPayloadKind kind)
    {
        var data = LastReceivedCharacterData;
        if (data?.FileReplacements == null || data.FileReplacements.Count == 0)
            return false;

        foreach (var list in data.FileReplacements.Values)
        {
            if (list == null)
                continue;

            foreach (var replacement in list)
            {
                if (replacement == null)
                    continue;

                if (PathMatchesSyncPayloadKind(replacement.FileSwapPath, kind))
                    return true;

                if (replacement.GamePaths == null)
                    continue;

                foreach (var gamePath in replacement.GamePaths)
                {
                    if (PathMatchesSyncPayloadKind(gamePath, kind))
                        return true;
                }
            }
        }

        return false;
    }

    private static bool PathMatchesSyncPayloadKind(string? path, SyncPayloadKind kind)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var normalized = path.Replace('\\', '/').Trim();
        var ext = Path.GetExtension(normalized).ToLowerInvariant();
        return kind switch
        {
            SyncPayloadKind.Sound => ext == ".scd",
            SyncPayloadKind.Vfx => ext is ".avfx" or ".atex" or ".shpk" or ".eid" or ".skp",
            _ => false,
        };
    }

    public string GetEffectiveManipulationData(string? manipulationData)
    {
        var original = manipulationData ?? string.Empty;
        if (EffectiveHeightEditsEnabled)
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