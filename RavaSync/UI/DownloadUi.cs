using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Numerics;

namespace RavaSync.UI;

public class DownloadUi : WindowMediatorSubscriberBase
{
    
    
    
    private readonly MareConfigService _configService;
    private readonly ConcurrentDictionary<GameObjectHandler, DownloadAggregate> _currentDownloads = new();
    private static readonly TimeSpan DownloadUiHoldWindow = TimeSpan.FromSeconds(2);
    private readonly DalamudUtilService _dalamudUtilService;
    private readonly FileUploadManager _fileTransferManager;
    private readonly UiSharedService _uiShared;
    private readonly ConcurrentDictionary<GameObjectHandler, bool> _uploadingPlayers = new();

    private sealed class DownloadAggregate
    {
        public Dictionary<string, FileDownloadStatus> Status = new();
        public long AccTotalBytes;
        public long AccTransferredBytes;
        public int AccTotalFiles;
        public int AccTransferredFiles;

        public DateTime LastUpdateUtc;
        public bool Finished;
    }



    public DownloadUi(ILogger<DownloadUi> logger, DalamudUtilService dalamudUtilService, MareConfigService configService,
        FileUploadManager fileTransferManager, MareMediator mediator, UiSharedService uiShared, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync Downloads", performanceCollectorService)
    {
        _dalamudUtilService = dalamudUtilService;
        _configService = configService;
        _fileTransferManager = fileTransferManager;
        _uiShared = uiShared;

        SizeConstraints = new WindowSizeConstraints()
        {
            MaximumSize = new Vector2(500, 90),
            MinimumSize = new Vector2(500, 90),
        };

        Flags |= ImGuiWindowFlags.NoMove;
        Flags |= ImGuiWindowFlags.NoBackground;
        Flags |= ImGuiWindowFlags.NoInputs;
        Flags |= ImGuiWindowFlags.NoNavFocus;
        Flags |= ImGuiWindowFlags.NoResize;
        Flags |= ImGuiWindowFlags.NoScrollbar;
        Flags |= ImGuiWindowFlags.NoTitleBar;
        Flags |= ImGuiWindowFlags.NoDecoration;
        Flags |= ImGuiWindowFlags.NoFocusOnAppearing;

        DisableWindowSounds = true;

        ForceMainWindow = true;

        IsOpen = true;

        Mediator.Subscribe<DownloadStartedMessage>(this, (msg) =>
        {
            var now = DateTime.UtcNow;

            _currentDownloads.AddOrUpdate(msg.DownloadId,
                addValueFactory: _ =>
                {
                    return new DownloadAggregate
                    {
                        Status = msg.DownloadStatus,
                        LastUpdateUtc = now,
                        Finished = false
                    };
                },
                updateValueFactory: (_, existing) =>
                {
                    if (existing.Finished && (now - existing.LastUpdateUtc) <= DownloadUiHoldWindow)
                    {
                        var prevTotalBytes = existing.Status.Values.Sum(s => s.TotalBytes);
                        var prevTransferredBytes = existing.Status.Values.Sum(s => s.TransferredBytes);
                        var prevTotalFiles = existing.Status.Values.Sum(s => s.TotalFiles);
                        var prevTransferredFiles = existing.Status.Values.Sum(s => s.TransferredFiles);

                        existing.AccTotalBytes += prevTotalBytes;
                        existing.AccTransferredBytes += prevTransferredBytes;
                        existing.AccTotalFiles += prevTotalFiles;
                        existing.AccTransferredFiles += prevTransferredFiles;
                    }

                    existing.Status = msg.DownloadStatus;
                    existing.LastUpdateUtc = now;
                    existing.Finished = false;
                    return existing;
                });
        });

        Mediator.Subscribe<DownloadFinishedMessage>(this, (msg) =>
        {
            if (_currentDownloads.TryGetValue(msg.DownloadId, out var agg))
            {
                agg.Finished = true;
                agg.LastUpdateUtc = DateTime.UtcNow;
            }
        });

        Mediator.Subscribe<GposeStartMessage>(this, (_) => IsOpen = false);
        Mediator.Subscribe<GposeEndMessage>(this, (_) => IsOpen = true);
        Mediator.Subscribe<PlayerUploadingMessage>(this, (msg) =>
        {
            if (msg.IsUploading)
            {
                _uploadingPlayers[msg.Handler] = true;
            }
            else
            {
                _uploadingPlayers.TryRemove(msg.Handler, out _);
            }
        });
    }

    protected override void DrawInternal()
    {
        if (_configService.Current.ShowTransferWindow)
        {
            try
            {
                if (_fileTransferManager.CurrentUploads.Any())
                {
                    var currentUploads = _fileTransferManager.CurrentUploads.ToList();
                    var totalUploads = currentUploads.Count;

                    var doneUploads = currentUploads.Count(c => c.IsTransferred);
                    var totalUploaded = currentUploads.Sum(c => c.Transferred);
                    var totalToUpload = currentUploads.Sum(c => c.Total);

                    UiSharedService.DrawOutlinedFont($"▲", ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);
                    ImGui.SameLine();
                    var xDistance = ImGui.GetCursorPosX();
                    UiSharedService.DrawOutlinedFont($"Compressing+Uploading {doneUploads}/{totalUploads}",
                        ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);
                    ImGui.NewLine();
                    ImGui.SameLine(xDistance);
                    UiSharedService.DrawOutlinedFont(
                        $"{UiSharedService.ByteToString(totalUploaded, addSuffix: false)}/{UiSharedService.ByteToString(totalToUpload)}",
                        ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);

                    if (_currentDownloads.Any()) ImGui.Separator();
                }
            }
            catch
            {
                // ignore errors thrown from UI
            }

            try
            {
                foreach (var item in _currentDownloads.ToList())
                {
                    var statuses = item.Value.Status.Values.ToList();

                    var dlSlot = statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForSlot);
                    var dlQueue = statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForQueue);
                    var dlProg = statuses.Count(c => c.DownloadStatus == DownloadStatus.Downloading);
                    var dlDecomp = statuses.Count(c => c.DownloadStatus == DownloadStatus.Decompressing);

                    var totalFiles = item.Value.AccTotalFiles + statuses.Sum(s => s.TotalFiles);
                    var transferredFiles = item.Value.AccTransferredFiles + statuses.Sum(s => s.TransferredFiles);

                    var totalBytes = item.Value.AccTotalBytes + statuses.Sum(s => s.TotalBytes);
                    var transferredBytes = item.Value.AccTransferredBytes + statuses.Sum(s => s.TransferredBytes);

                    UiSharedService.DrawOutlinedFont($"▼", ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);
                    ImGui.SameLine();
                    var xDistance = ImGui.GetCursorPosX();
                    UiSharedService.DrawOutlinedFont($"{item.Key.Name} [W:{dlSlot}/Q:{dlQueue}/P:{dlProg}/D:{dlDecomp}]",ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);
                    ImGui.NewLine();
                    ImGui.SameLine(xDistance);
                    UiSharedService.DrawOutlinedFont($"{transferredFiles}/{totalFiles} ({UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)})",ImGuiColors.DalamudWhite, new Vector4(0, 0, 0, 255), 1);
                }
            }
            catch
            {
                // ignore errors thrown from UI
            }
        }

        if (_configService.Current.ShowTransferBars)
        {
            const int transparency = 100;
            const int dlBarBorder = 3;
            double dlProgressPercent =0.0;
            string downloadText;

            var now = DateTime.UtcNow;

            foreach (var transfer in _currentDownloads.ToList())
            {
                if (transfer.Value.Finished && (now - transfer.Value.LastUpdateUtc) > DownloadUiHoldWindow)
                {
                    _currentDownloads.TryRemove(transfer.Key, out _);
                    continue;
                }

                var screenPos = _dalamudUtilService.WorldToScreen(transfer.Key.GetGameObject());
                if (screenPos == Vector2.Zero) continue;

                var statuses = transfer.Value.Status.Values.ToList();
                if (statuses.Count == 0) continue;

                var totalBytes = transfer.Value.AccTotalBytes + statuses.Sum(s => s.TotalBytes);
                var transferredBytes = transfer.Value.AccTransferredBytes + statuses.Sum(s => s.TransferredBytes);

                var allPreparing = statuses.All(s =>
                    (s.DownloadStatus == DownloadStatus.Initializing
                     || s.DownloadStatus == DownloadStatus.WaitingForSlot
                     || s.DownloadStatus == DownloadStatus.WaitingForQueue)
                    && s.TransferredBytes == 0);

                //BAR SIZE
                var maxDlText = $"{UiSharedService.ByteToString(totalBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
                var textSize = _configService.Current.TransferBarsShowText
                    ? ImGui.CalcTextSize(maxDlText)
                    : new Vector2(10, 10);

                int dlBarHeight = _configService.Current.TransferBarsHeight > ((int)textSize.Y + 5)
                    ? _configService.Current.TransferBarsHeight
                    : (int)textSize.Y + 5;

                int dlBarWidth = _configService.Current.TransferBarsWidth > ((int)textSize.X + 10)
                    ? _configService.Current.TransferBarsWidth
                    : (int)textSize.X + 10;

                var dlBarStart = new Vector2(screenPos.X - dlBarWidth / 2f, screenPos.Y - dlBarHeight / 2f);
                var dlBarEnd = new Vector2(screenPos.X + dlBarWidth / 2f, screenPos.Y + dlBarHeight / 2f);
                var drawList = ImGui.GetBackgroundDrawList();

                // background + border
                drawList.AddRectFilled(
                    dlBarStart with { X = dlBarStart.X - dlBarBorder - 1, Y = dlBarStart.Y - dlBarBorder - 1 },
                    dlBarEnd with { X = dlBarEnd.X + dlBarBorder + 1, Y = dlBarEnd.Y + dlBarBorder + 1 },
                    UiSharedService.Color(0, 0, 0, transparency), 1);

                drawList.AddRectFilled(
                    dlBarStart with { X = dlBarStart.X - dlBarBorder, Y = dlBarStart.Y - dlBarBorder },
                    dlBarEnd with { X = dlBarEnd.X + dlBarBorder, Y = dlBarEnd.Y + dlBarBorder },
                    UiSharedService.Color(220, 220, 220, transparency), 1);

                drawList.AddRectFilled(
                    dlBarStart,
                    dlBarEnd,
                    UiSharedService.Color(0, 0, 0, transparency), 1);

                if (allPreparing)
                {
                    // Preparing state
                    dlProgressPercent = 1.0;
                    downloadText = "Preparing Files...";
                }
                else
                {
                    if (totalBytes <= 0)
                    {
                        dlProgressPercent = 0;
                    }
                    else
                    {
                        dlProgressPercent = transferredBytes / (double)totalBytes;
                        if (dlProgressPercent < 0) dlProgressPercent = 0;
                        if (dlProgressPercent > 1) dlProgressPercent = 1;
                    }

                    downloadText = $"{UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
                }

                drawList.AddRectFilled(
                    dlBarStart,
                    dlBarEnd with { X = dlBarStart.X + (float)(dlProgressPercent * dlBarWidth) },
                    UiSharedService.Color(112, 28, 180, transparency), 1);

                if (_configService.Current.TransferBarsShowText)
                {
                    var textSizeCurrent = ImGui.CalcTextSize(downloadText);

                    UiSharedService.DrawOutlinedFont(
                        drawList,
                        downloadText,
                        screenPos with
                        {
                            X = screenPos.X - textSizeCurrent.X / 2f - 1,
                            Y = screenPos.Y - textSizeCurrent.Y / 2f - 1
                        },
                        UiSharedService.Color(255, 255, 255, transparency),
                        UiSharedService.Color(0, 0, 0, transparency),
                        1);
                }
            }

            if (_configService.Current.ShowUploading)
            {
                foreach (var player in _uploadingPlayers.Select(p => p.Key).ToList())
                {
                    var screenPos = _dalamudUtilService.WorldToScreen(player.GetGameObject());
                    if (screenPos == Vector2.Zero) continue;

                    try
                    {
                        using var _ = _uiShared.UidFont.Push();
                        var uploadText = "Uploading";

                        var textSize = ImGui.CalcTextSize(uploadText);

                        var drawList = ImGui.GetBackgroundDrawList();
                        UiSharedService.DrawOutlinedFont(
                            drawList,
                            uploadText,
                            screenPos with
                            {
                                X = screenPos.X - textSize.X / 2f - 1,
                                Y = screenPos.Y - textSize.Y / 2f - 1
                            },
                            UiSharedService.Color(112, 28, 180, transparency),
                            UiSharedService.Color(0, 0, 0, transparency),
                            2);
                    }
                    catch
                    {
                        // ignore errors thrown on UI
                    }
                }
            }
        }
    }


    public override bool DrawConditions()
    {
        if (_uiShared.EditTrackerPosition) return true;
        if (!_configService.Current.ShowTransferWindow && !_configService.Current.ShowTransferBars) return false;
        if (!_currentDownloads.Any() && !_fileTransferManager.CurrentUploads.Any() && !_uploadingPlayers.Any()) return false;
        if (!IsOpen) return false;
        return true;
    }

    public override void PreDraw()
    {
        base.PreDraw();

        if (_uiShared.EditTrackerPosition)
        {
            Flags &= ~ImGuiWindowFlags.NoMove;
            Flags &= ~ImGuiWindowFlags.NoBackground;
            Flags &= ~ImGuiWindowFlags.NoInputs;
            Flags &= ~ImGuiWindowFlags.NoResize;
        }
        else
        {
            Flags |= ImGuiWindowFlags.NoMove;
            Flags |= ImGuiWindowFlags.NoBackground;
            Flags |= ImGuiWindowFlags.NoInputs;
            Flags |= ImGuiWindowFlags.NoResize;
        }

        var maxHeight = ImGui.GetTextLineHeight() * (_configService.Current.ParallelDownloads + 3);
        SizeConstraints = new()
        {
            MinimumSize = new Vector2(300, maxHeight),
            MaximumSize = new Vector2(300, maxHeight),
        };
    }
}