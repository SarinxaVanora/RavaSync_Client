using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Game.ClientState.Objects.Types;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.API.Dto;
using RavaSync.API.Dto.CharaData;
using RavaSync.API.Dto.Group;
using RavaSync.API.Dto.User;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Events;
using RavaSync.WebAPI.Files.Models;
using System.Numerics;

namespace RavaSync.Services.Mediator;

#pragma warning disable MA0048 // File name must match type name
#pragma warning disable S2094

public enum ScopeMode
{
    Everyone = 0,
    Friends = 1,
    Party = 2,
    Alliance = 3
}

public record ScopeModeChangedMessage(ScopeMode Mode) : SameThreadMessage;
public record SwitchToIntroUiMessage : MessageBase;
public record SwitchToMainUiMessage : MessageBase;
public record OpenSettingsUiMessage : MessageBase;
public record DalamudLoginMessage : MessageBase;
public record DalamudLogoutMessage : MessageBase;
public record PriorityFrameworkUpdateMessage : SameThreadMessage;
public record FrameworkUpdateMessage : SameThreadMessage;
public record ClassJobChangedMessage(GameObjectHandler GameObjectHandler) : MessageBase;
public record DelayedFrameworkUpdateMessage : SameThreadMessage;
public record ZoneSwitchStartMessage : MessageBase;
public record ZoneSwitchEndMessage : MessageBase;
public record CutsceneStartMessage : MessageBase;
public record GposeStartMessage : SameThreadMessage;
public record GposeEndMessage : MessageBase;
public record CutsceneEndMessage : MessageBase;
public record CutsceneFrameworkUpdateMessage : SameThreadMessage;
public record ConnectedMessage(ConnectionDto Connection) : MessageBase;
public record DisconnectedMessage : SameThreadMessage;
public record PenumbraModSettingChangedMessage(Guid CollectionId, string ModName, bool Inherited, string Change) : MessageBase;
public record PenumbraInitializedMessage : MessageBase;
public record PenumbraDisposedMessage : MessageBase;
public record PenumbraRedrawMessage(IntPtr Address, int ObjTblIdx, bool WasRequested) : SameThreadMessage;
public record GlamourerChangedMessage(IntPtr Address) : MessageBase;
public record HeelsOffsetMessage(string Offset) : MessageBase;
public record PenumbraResourceLoadMessage(IntPtr GameObject, string GamePath, string FilePath) : SameThreadMessage;
public record CustomizePlusMessage(nint? Address) : MessageBase;
public record HonorificMessage(string NewHonorificTitle) : MessageBase;
public record MoodlesMessage(IntPtr Address,string MoodlesData) : MessageBase;
public record PetNamesReadyMessage : MessageBase;
public record PetNamesMessage(string PetNicknamesData) : MessageBase;
public record HonorificReadyMessage : MessageBase;
public record TransientResourceChangedMessage(IntPtr Address) : MessageBase;
public record HaltScanMessage(string Source) : MessageBase;
public record ResumeScanMessage(string Source) : MessageBase;
public record NotificationMessage
    (string Title, string Message, NotificationType Type, TimeSpan? TimeShownOnScreen = null) : MessageBase;
public record CreateCacheForObjectMessage(GameObjectHandler ObjectToCreateFor, string Reason = "Unspecified") : SameThreadMessage;
public record ImmediatePlayerStatePublishMessage(GameObjectHandler ObjectToCreateFor, string Reason = "Unspecified") : SameThreadMessage;
public record ClearCacheForObjectMessage(GameObjectHandler ObjectToCreateFor) : SameThreadMessage;
public record CharacterDataCreatedMessage(CharacterData CharacterData, bool ForceOutbound = false, string Reason = "") : SameThreadMessage;
public record CharacterDataAnalyzedMessage : MessageBase;
public enum DataAnalysisOptimisationTab
{
    Textures = 0,
    Meshes = 1,
}
public record OpenDataAnalysisOptimisationTabMessage(DataAnalysisOptimisationTab Tab) : MessageBase;
public record PenumbraStartRedrawMessage(IntPtr Address) : MessageBase;
public record PenumbraEndRedrawMessage(IntPtr Address) : MessageBase;
public record HubReconnectingMessage(Exception? Exception) : SameThreadMessage;
public record HubReconnectedMessage(string? Arg) : SameThreadMessage;
public record HubClosedMessage(Exception? Exception) : SameThreadMessage;
public record DownloadReadyMessage(Guid RequestId) : MessageBase;
public record DownloadStartedMessage(GameObjectHandler DownloadId, Dictionary<string, FileDownloadStatus> DownloadStatus) : MessageBase;
public record DownloadFinishedMessage(GameObjectHandler DownloadId) : MessageBase;
public record UiToggleMessage(Type UiType) : MessageBase;
public record RestoreCompactUiStateMessage : MessageBase;
public record MainUiMinimizedMessage : MessageBase;
public record MainUiRestoredMessage : MessageBase;
public record RestoreMainUiAtPositionMessage(Vector2 Position, bool OpenToLeft = false) : MessageBase;
public record MainUiMinimizedAtPositionMessage(Vector2 Position) : MessageBase;
public record PlayerUploadingMessage(GameObjectHandler Handler, bool IsUploading) : MessageBase;
public record ClearProfileDataMessage(UserData? UserData = null) : MessageBase;
public record CyclePauseMessage(UserData UserData) : MessageBase;
public record PauseMessage(UserData UserData) : MessageBase;
public record ResumeMessage(UserData UserData) : MessageBase;
public record ResumeThresholdAutoPausedOnConnectMessage(UserData UserData) : MessageBase;
public record ProfilePopoutToggle(Pair? Pair) : MessageBase;
public record CompactUiChange(Vector2 Size, Vector2 Position) : MessageBase;
public record ProfileOpenStandaloneMessage(Pair Pair) : MessageBase;
public record RemoveWindowMessage(WindowMediatorSubscriberBase Window) : MessageBase;
public record RefreshUiMessage : MessageBase;
public record OpenBanUserPopupMessage(Pair PairToBan, GroupFullInfoDto GroupFullInfoDto) : MessageBase;
public record OpenCensusPopupMessage() : MessageBase;
public record OpenSyncshellAdminPanel(GroupFullInfoDto GroupInfo) : MessageBase;
public record OpenPermissionWindow(Pair Pair) : MessageBase;
public record DownloadLimitChangedMessage() : SameThreadMessage;
public record CensusUpdateMessage(byte Gender, byte RaceId, byte TribeId) : MessageBase;
public record TargetPairMessage(Pair Pair) : MessageBase;
public record CombatOrPerformanceStartMessage : MessageBase;
public record CombatOrPerformanceEndMessage : MessageBase;
public record EventMessage(Event Event) : MessageBase;
public record PenumbraDirectoryChangedMessage(string? ModDirectory) : MessageBase;
public record PenumbraFileCacheChangedMessage(IReadOnlyCollection<string> Paths) : MessageBase;
public record PenumbraRedrawCharacterMessage(ICharacter Character) : SameThreadMessage;
public record PenumbraRedrawAddressMessage(nint Address) : MessageBase;
public record ArmRequestedPlayerPublishAfterRedrawMessage(nint Address) : MessageBase;
public record GameObjectHandlerCreatedMessage(GameObjectHandler GameObjectHandler, bool OwnedObject) : SameThreadMessage;
public record GameObjectHandlerDestroyedMessage(GameObjectHandler GameObjectHandler, bool OwnedObject) : SameThreadMessage;
public record HaltCharaDataCreation(bool Resume = false) : SameThreadMessage;
public record GposeLobbyUserJoin(UserData UserData) : MessageBase;
public record GPoseLobbyUserLeave(UserData UserData) : MessageBase;
public record GPoseLobbyReceiveCharaData(CharaDataDownloadDto CharaDataDownloadDto) : MessageBase;
public record GPoseLobbyReceivePoseData(UserData UserData, PoseData PoseData) : MessageBase;
public record GPoseLobbyReceiveWorldData(UserData UserData, WorldData WorldData) : MessageBase;
public record OpenCharaDataHubWithFilterMessage(UserData UserData) : MessageBase;
public record PairRequestReceivedMessage(PairRequestDto Request) : MessageBase;
public record PairRequestResultMessage(PairRequestResultDto Result) : MessageBase;
public record ContextMenuPairRequestMessage(string TargetIdent, string charName) : MessageBase;
public record DirectPairRequestMessage(string TargetIdent, string TargetName) : MessageBase;
public record SyncshellGameMeshMessage(string LocalSessionId, string FromSessionId, byte[] Payload) : MessageBase;
public record RemoteMissingFileMessage(string TargetUid, string TargetIdent, string DataHash, IReadOnlyCollection<string> Hashes, string Reason) : MessageBase;
public record RemoteOtherSyncYieldMessage(string FromUid, bool YieldToOtherSync, string Owner, TimeSpan Ttl) : MessageBase;
public record LocalOtherSyncYieldStateChangedMessage(string AffectedUid, bool YieldToOtherSync, string Owner) : MessageBase;
public record PrimeTransientPathsMessage(IntPtr Address, ObjectKind Kind, IReadOnlyCollection<string> GamePaths) : SameThreadMessage;
public record RemoteOtherSyncConnectedMessage(string? Owner) : MessageBase;
public record RemoteOtherSyncDisconnectedMessage(string? Owner) : MessageBase;
public sealed record InitialFinalRedrawConsumedMessage(nint ActorAddress) : SameThreadMessage;
public record BlacklistUiMessage() : MessageBase;



#pragma warning restore S2094
#pragma warning restore MA0048 // File name must match type name