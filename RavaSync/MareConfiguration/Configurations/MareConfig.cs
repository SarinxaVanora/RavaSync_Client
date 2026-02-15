using FFXIVClientStructs.FFXIV.Common.Math;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration.Models;
using RavaSync.Themes;
using RavaSync.UI;

namespace RavaSync.MareConfiguration.Configurations;

[Serializable]
public class MareConfig : IMareConfiguration
{
    public bool AcceptedAgreement { get; set; } = false;
    public string CacheFolder { get; set; } = string.Empty;
    public bool DisableOptionalPluginWarnings { get; set; } = false;
    public bool EnableDtrEntry { get; set; } = false;
    public bool ShowUidInDtrTooltip { get; set; } = true;
    public bool PreferNoteInDtrTooltip { get; set; } = false;
    public bool UseColorsInDtr { get; set; } = true;
    public DtrEntry.Colors DtrColorsDefault { get; set; } = default;
    public DtrEntry.Colors DtrColorsNotConnected { get; set; } = new(Glow: 0x0428FFu);
    public DtrEntry.Colors DtrColorsPairsInRange { get; set; } = new(Glow: 0xFFBA47u);
    public bool EnableRightClickMenus { get; set; } = true;
    public NotificationLocation ErrorNotification { get; set; } = NotificationLocation.Both;
    public string ExportFolder { get; set; } = string.Empty;
    public bool FileScanPaused { get; set; } = false;
    public NotificationLocation InfoNotification { get; set; } = NotificationLocation.Toast;
    public bool InitialScanComplete { get; set; } = false;
    public LogLevel LogLevel { get; set; } = LogLevel.Information;
    public bool LogPerformance { get; set; } = false;
    public double MaxLocalCacheInGiB { get; set; } = 20;
    public bool OpenGposeImportOnGposeStart { get; set; } = false;
    public bool OpenPopupOnAdd { get; set; } = true;
    public int ParallelDownloads { get; set; } = 0;
    public int ParallelUploads { get; set; } = 0;
    public int DownloadSpeedLimitInBytes { get; set; } = 0;
    public DownloadSpeeds DownloadSpeedType { get; set; } = DownloadSpeeds.MBps;
    public bool PreferNotesOverNamesForVisible { get; set; } = false;
    public float ProfileDelay { get; set; } = 1.5f;
    public bool ProfilePopoutRight { get; set; } = false;
    public bool ProfilesAllowNsfw { get; set; } = false;
    public bool ProfilesShow { get; set; } = true;
    public bool ShowSyncshellUsersInVisible { get; set; } = true;
    public bool ShowCharacterNameInsteadOfNotesForVisible { get; set; } = false;
    public bool ShowOfflineUsersSeparately { get; set; } = true;
    public bool ShowSyncshellOfflineUsersSeparately { get; set; } = true;
    public bool GroupUpSyncshells { get; set; } = true;
    public bool ShowOnlineNotifications { get; set; } = false;
    public bool ShowOnlineNotificationsOnlyForIndividualPairs { get; set; } = true;
    public bool ShowOnlineNotificationsOnlyForNamedPairs { get; set; } = false;
    public bool ShowTransferBars { get; set; } = false;
    public bool ShowGlobalTransferBars { get; set; } = true;
    public bool ShowUploadProgress { get; set; } = true;
    public bool ShowTransferWindow { get; set; } = false;
    public bool ShowUploading { get; set; } = true;
    public bool ShowUploadingBigText { get; set; } = true;
    public bool ShowVisibleUsersSeparately { get; set; } = true;
    public int TimeSpanBetweenScansInSeconds { get; set; } = 30;
    public int TransferBarsHeight { get; set; } = 12;
    public bool TransferBarsShowText { get; set; } = true;
    public int TransferBarsWidth { get; set; } = 250;
    public bool UseAlternativeFileUpload { get; set; } = false;
    public bool UseCompactor { get; set; } = false;
    public bool DebugStopWhining { get; set; } = false;
    public bool AutoPopulateEmptyNotesFromCharaName { get; set; } = false;
    public int Version { get; set; } = 2;
    public NotificationLocation WarningNotification { get; set; } = NotificationLocation.Both;
    public bool UseFocusTarget { get; set; } = false;
    public bool SortPairsByVRAM { get; set; } = false;
    public string SelectedThemeId { get; set; } = ThemeManager.NoneId;
    public string FontId { get; set; } = "axis";
    public float FontSizePx { get; set; } = 16f;
    public float FontRasterizerMultiply { get; set; } = 1.0f;
    public int FontOversampleH { get; set; } = 2;
    public int FontOversampleV { get; set; } = 2;
    public bool DelayActivationEnabled { get; set; } = true;
    public bool DelayAnimationsOnly { get; set; } = true;
    public bool ApplyOnlyOnZoneChange { get; set; } = false;
    public int SafeIdleSeconds { get; set; } = 5;
    public HashSet<string> VenueAskSuppressKeys { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public int ParallelApplyDegree { get; set; } = 0;
    public bool ShowFriendshapedHeart { get; set; } = true;
    public bool EnableSendPairRequestContextMenu { get; set; } = true;
    public bool ShowMinimizedRestoreIcon { get; set; } = true;
    public bool AutoDeclineIncomingPairRequests { get; set; } = false;
    public bool SeenDiscoveryIntro { get; set; } = false;
    public bool EnableRavaDiscoveryPresence { get; set; } = false;
    public bool CacheFolderSubdirMigrationDone { get; set; } = false;

    public bool EditGlobalTransferOverlay { get; set; } = false;
    public float GlobalTransferOverlayX { get; set; } = -1f;
    public float GlobalTransferOverlayY { get; set; } = -1f;
    public float GlobalTransferOverlayScale { get; set; } = 1.0f;
    public bool GlobalTransferOverlayRowLayout { get; set; } = false;


}