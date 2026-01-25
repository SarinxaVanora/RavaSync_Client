using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Textures;
using Dalamud.Interface.Textures.TextureWraps;
using Dalamud.Plugin.Services;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using System.Collections.Concurrent;
using System.IO;
using System.Numerics;
using System.Reflection;

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
    private IDalamudTextureWrap? _aetherFrame;
    private IDalamudTextureWrap? _aetherFill;
    private IDalamudTextureWrap? _aetherFillBlue;
    private readonly Dictionary<IntPtr, Vector2> _smoothedScreens = new();
    private const long MinVisibleDownloadBytes = 10L * 1024L * 1024L;

    private Vector2 _aetherFrameSize = Vector2.Zero;

    private const float FillUvX0 = 167f / 1462f;
    private const float FillUvX1 = 1299f / 1462f;
    private const float FillUvY0 = 65f / 238f;
    private const float FillUvY1 = 173f / 238f;

    private bool _globalTransfersExpanded = false;

    private sealed class DownloadAggregate
    {
        public Dictionary<string, FileDownloadStatus> Status = new();
        public long AccTotalBytes;
        public long AccTransferredBytes;
        public int AccTotalFiles;
        public int AccTransferredFiles;

        public DateTime LastUpdateUtc;
        public bool Finished;

        public float SmoothedPercent;
        public float AnimPhase;
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
        EnsureAetherTextures();
    }

    protected override void DrawInternal()
    {
        var now = DateTime.UtcNow;
        PruneExpiredDownloads(now);

        if (_configService.Current.ShowGlobalTransferBars || _configService.Current.ShowUploadProgress || _configService.Current.EditGlobalTransferOverlay)
        {
            DrawGlobalTransferOverlay(now);
        }


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
            const byte transparency = 220;

            EnsureAetherTextures();
            var haveAether = _aetherFrame != null && _aetherFill != null && _aetherFrameSize.X > 1 && _aetherFrameSize.Y > 1;

            now = DateTime.UtcNow;
            var dtPos = ImGui.GetIO().DeltaTime;
            var keepKeys = new HashSet<IntPtr>();

            foreach (var transfer in _currentDownloads.ToList())
            {
                if (transfer.Value.Finished && (now - transfer.Value.LastUpdateUtc) > DownloadUiHoldWindow)
                {
                    _currentDownloads.TryRemove(transfer.Key, out _);
                    continue;
                }

                var go = transfer.Key.GetGameObject();
                var screenPos = _dalamudUtilService.WorldToScreen(go);
                if (screenPos == Vector2.Zero) continue;

                var statuses = transfer.Value.Status.Values.ToList();
                if (statuses.Count == 0) continue;

                var totalBytes = transfer.Value.AccTotalBytes + statuses.Sum(s => s.TotalBytes);
                var transferredBytes = transfer.Value.AccTransferredBytes + statuses.Sum(s => s.TransferredBytes);

                if (totalBytes > 0 && totalBytes < MinVisibleDownloadBytes)
                    continue;

                keepKeys.Add(transfer.Key.Address);
                screenPos = SmoothAndSnapScreen(transfer.Key.Address, screenPos, dtPos);

                var allPreparing = statuses.All(s =>
                    (s.DownloadStatus == DownloadStatus.Initializing
                     || s.DownloadStatus == DownloadStatus.WaitingForSlot
                     || s.DownloadStatus == DownloadStatus.WaitingForQueue)
                    && s.TransferredBytes == 0);

                // Progress
                var sInit = statuses.Count(s => s.DownloadStatus == DownloadStatus.Initializing);
                var sSlot = statuses.Count(s => s.DownloadStatus == DownloadStatus.WaitingForSlot);
                var sQueue = statuses.Count(s => s.DownloadStatus == DownloadStatus.WaitingForQueue);
                var sProg = statuses.Count(s => s.DownloadStatus == DownloadStatus.Downloading);
                var sDecomp = statuses.Count(s => s.DownloadStatus == DownloadStatus.Decompressing);

                var isWaiting = (sSlot > 0 || sQueue > 0) && sProg == 0 && sDecomp == 0;
                var isInitializing = sInit > 0 && sProg == 0 && sDecomp == 0;
                var shouldPulse = isWaiting || isInitializing;

                double dlProgressPercent;
                string downloadText;

                if (sProg > 0)
                {
                    dlProgressPercent = totalBytes <= 0 ? 0.0 : transferredBytes / (double)totalBytes;
                    dlProgressPercent = Math.Clamp(dlProgressPercent, 0.0, 1.0);

                    downloadText = $"{UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
                }
                else if (sDecomp > 0)
                {
                    dlProgressPercent = totalBytes <= 0 ? 0.0 : transferredBytes / (double)totalBytes;
                    dlProgressPercent = Math.Clamp(dlProgressPercent, 0.0, 1.0);

                    downloadText = totalBytes > 0
                        ? $"Decompressing ({sDecomp})  {UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}"
                        : $"Decompressing ({sDecomp})";
                }
                else if (sSlot > 0 || sQueue > 0 || sInit > 0)
                {
                    dlProgressPercent = 0.0;

                    var parts = new List<string>(4);
                    if (sSlot > 0) parts.Add($"W:{sSlot}");
                    if (sQueue > 0) parts.Add($"Q:{sQueue}");
                    if (sProg > 0) parts.Add($"P:{sProg}");
                    if (sDecomp > 0) parts.Add($"D:{sDecomp}");
                    if (sInit > 0) parts.Add($"I:{sInit}");

                    var breakdown = parts.Count > 0 ? string.Join(" ", parts) : string.Empty;

                    if (sSlot > 0) downloadText = $"Waiting  [{breakdown}]";
                    else if (sQueue > 0) downloadText = $"Queued  [{breakdown}]";
                    else downloadText = $"Initializing  [{breakdown}]";
                }
                else
                {
                    dlProgressPercent = totalBytes <= 0 ? 0.0 : transferredBytes / (double)totalBytes;
                    dlProgressPercent = Math.Clamp(dlProgressPercent, 0.0, 1.0);

                    downloadText = $"{UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
                }

                var dt = ImGui.GetIO().DeltaTime;
                var target = (isWaiting || isInitializing) ? 0f : (float)dlProgressPercent;

                transfer.Value.SmoothedPercent = LerpExp(transfer.Value.SmoothedPercent, target, lambda: 12f, dt);
                transfer.Value.AnimPhase += dt;

                int dlBarWidth;
                int dlBarHeight;

                if (haveAether)
                {
                    dlBarWidth = Math.Max(_configService.Current.TransferBarsWidth, 320);
                    
                    var aspect = _aetherFrameSize.Y / _aetherFrameSize.X;
                    dlBarHeight = Math.Max(_configService.Current.TransferBarsHeight, (int)MathF.Round(dlBarWidth * aspect));

                    var minH = Math.Max(_configService.Current.TransferBarsHeight, 34);
                    if (dlBarHeight < minH) { dlBarHeight = minH; dlBarWidth = (int)MathF.Round(dlBarHeight / aspect); }

                    dlBarWidth = (int)MathF.Round(dlBarWidth * 1.10f);
                    dlBarHeight = (int)MathF.Round(dlBarHeight * 1.10f);

                }
                else
                {
                    dlBarWidth = Math.Max(_configService.Current.TransferBarsWidth, 320);
                    dlBarHeight = Math.Max(_configService.Current.TransferBarsHeight, 28);
                }

                const float barYOffsetPx = 0f;

                var barCenter = screenPos + new Vector2(0f, barYOffsetPx);

                var dlBarStart = new Vector2(barCenter.X - dlBarWidth / 2f, barCenter.Y - dlBarHeight / 2f);
                var dlBarEnd = new Vector2(barCenter.X + dlBarWidth / 2f, barCenter.Y + dlBarHeight / 2f);
                dlBarStart = new Vector2(MathF.Round(dlBarStart.X), MathF.Round(dlBarStart.Y));
                dlBarEnd = new Vector2(MathF.Round(dlBarEnd.X), MathF.Round(dlBarEnd.Y));

                var drawList = ImGui.GetBackgroundDrawList();

                DrawAetherBarComposite(drawList, dlBarStart, dlBarEnd, transfer.Value.SmoothedPercent, transparency, shouldPulse, transfer.Value.AnimPhase);

                if (_configService.Current.TransferBarsShowText)
                {
                    GetFillRect(dlBarStart, dlBarEnd, out var fillStart, out var fillEnd);
                    var mid = (fillStart + fillEnd) * 0.5f;

                    var textSizeCurrent = ImGui.CalcTextSize(downloadText);

                    UiSharedService.DrawOutlinedFont(drawList, downloadText, new Vector2(mid.X - textSizeCurrent.X / 2f, mid.Y - textSizeCurrent.Y / 2f), UiSharedService.Color(255, 255, 255, 255), UiSharedService.Color(0, 0, 0, 220), 2);
                }

            }

            if (_configService.Current.ShowUploading)
            {
                foreach (var player in _uploadingPlayers.Select(p => p.Key).ToList())
                {
                    var go = player.GetGameObject();
                    var screenPos = _dalamudUtilService.WorldToScreen(go);
                    if (screenPos == Vector2.Zero) continue;

                    keepKeys.Add(player.Address);
                    screenPos = SmoothAndSnapScreen(player.Address, screenPos, dtPos);

                    try
                    {
                        using var _ = _uiShared.UidFont.Push();
                        var uploadText = "Uploading";
                        var textSize = ImGui.CalcTextSize(uploadText);

                        var drawList = ImGui.GetBackgroundDrawList();
                        var tp = new Vector2(
                        MathF.Round(screenPos.X - textSize.X / 2f - 1f),
                        MathF.Round(screenPos.Y - textSize.Y / 2f - 1f)
                        );


                        UiSharedService.DrawOutlinedFont(
                        drawList,
                        uploadText,
                        tp,
                        UiSharedService.Color(112, 28, 180, 255),
                        UiSharedService.Color(0, 0, 0, 200),2
                        );

                    }
                    catch
                    {
                        // ignore errors thrown on UI
                    }
                }
            }
            PruneSmoothedScreens(keepKeys);
        }

    }

    private static float LerpExp(float current, float target, float lambda, float dt)
    {
        var t = 1f - MathF.Exp(-lambda * dt);
        return current + (target - current) * t;
    }

    private static void DrawFillEdgeGlow(ImDrawListPtr dl, Vector2 fillStart, Vector2 fillEnd, float fillEndX, float phase, byte alpha)
    {
        var h = fillEnd.Y - fillStart.Y;
        if (h <= 2f) return;

        var insetY = MathF.Max(2f, h * 0.12f);
        var y0 = fillStart.Y + insetY;
        var y1 = fillEnd.Y - insetY;

        var coreW = MathF.Max(2f, h * 0.10f);
        var glowW = MathF.Max(10f, h * 0.55f);

        var t = (float)ImGui.GetTime();
        var pulse = 0.72f + 0.28f * (0.5f + 0.5f * MathF.Sin(t * 3.2f + phase * 0.9f));
        var wobble = MathF.Sin(t * 1.6f + phase * 1.1f) * (h * 0.04f);

        var aCore = (byte)Math.Clamp((int)(alpha * (0.65f + 0.25f * pulse)), 0, 255);
        var aGlow = (byte)Math.Clamp((int)(alpha * (0.28f + 0.22f * pulse)), 0, 255);
        var aSoft = (byte)Math.Clamp((int)(alpha * (0.12f + 0.10f * pulse)), 0, 255);

        var x = fillEndX;
        var centerY = (y0 + y1) * 0.5f + wobble;

        dl.AddRectFilled(
            new Vector2(x - glowW, y0),
            new Vector2(x + glowW, y1),
            UiSharedService.Color(210, 185, 255, aSoft),
            h * 0.35f);

        dl.AddRectFilled(
            new Vector2(x - glowW * 0.45f, y0),
            new Vector2(x + glowW * 0.45f, y1),
            UiSharedService.Color(170, 90, 255, aGlow),
            h * 0.35f);

        dl.AddRectFilled(
            new Vector2(x - coreW, y0),
            new Vector2(x + coreW, y1),
            UiSharedService.Color(255, 255, 255, aCore),
            h * 0.35f);

        var dotR = MathF.Max(1.5f, h * 0.10f);
        dl.AddCircleFilled(new Vector2(x, centerY), dotR, UiSharedService.Color(255, 255, 255, (byte)Math.Clamp(aCore + 30, 0, 255)), 12);
    }


    private static float PingPong(float t)
    {
        t %= 2f;
        return t < 1f ? t : 2f - t;
    }

    private static float Hash01(uint x)
    {
        x ^= x >> 16;
        x *= 0x7feb352d;
        x ^= x >> 15;
        x *= 0x846ca68b;
        x ^= x >> 16;
        return (x & 0x00FFFFFF) / 16777216f;
    }

    private static void DrawEnergyVialFill(ImDrawListPtr dl, Vector2 start, Vector2 end, float progress, float phase, byte alpha)
    {
        var w = end.X - start.X;
        var h = end.Y - start.Y;
        if (w <= 1 || h <= 1) return;

        progress = Math.Clamp(progress, 0f, 1f);
        var fillEndX = start.X + w * progress;
        if (fillEndX <= start.X + 1) return;

        dl.PushClipRect(start, new Vector2(fillEndX, end.Y), true);

        var top = UiSharedService.Color(165, 90, 255, alpha);
        var bot = UiSharedService.Color(70, 10, 120, alpha);
        dl.AddRectFilledMultiColor(start, end, top, top, bot, bot);

        var bandW = MathF.Max(18f, w * 0.12f);
        var speed = 80f;
        var offset = (phase * speed) % (bandW * 2f);

        byte shimmerA = (byte)(alpha * 0.22f);
        var shimmerCol = UiSharedService.Color(255, 255, 255, shimmerA);

        for (var x = start.X - bandW * 2f; x < fillEndX + bandW; x += bandW * 2f)
        {
            var x0 = x + offset;
            var x1 = x0 + bandW;

            if (x1 <= start.X || x0 >= fillEndX) continue;

            var ySkew = MathF.Sin(phase * 1.7f + x0 * 0.01f) * (h * 0.08f);

            var p0 = new Vector2(MathF.Max(x0, start.X), start.Y + ySkew);
            var p1 = new Vector2(MathF.Min(x1, fillEndX), end.Y + ySkew);

            dl.AddRectFilled(p0, p1, shimmerCol, 1f);
        }

        uint seed = (uint)start.GetHashCode() ^ (uint)end.GetHashCode();
        int bubbleCount = (int)Math.Clamp(w / 60f, 3f, 7f);

        byte bubbleA = (byte)(alpha * 0.35f);
        var bubbleCol = UiSharedService.Color(230, 210, 255, bubbleA);

        for (int i = 0; i < bubbleCount; i++)
        {
            var r01 = Hash01(seed + (uint)(i * 1013));
            var r02 = Hash01(seed + (uint)(i * 2027));

            var bx = start.X + r01 * (fillEndX - start.X);
            var rise = (phase * (0.45f + r02 * 0.8f) + i * 0.7f) % 1f;
            var by = end.Y - rise * h;

            var rad = 1.2f + r02 * 1.8f;
            dl.AddCircleFilled(new Vector2(bx, by), rad, bubbleCol, 10);
        }

        dl.PopClipRect();

        byte surfaceA = (byte)(alpha * 0.55f);
        var surfaceCol = UiSharedService.Color(220, 200, 255, surfaceA);
        dl.AddLine(new Vector2(start.X, start.Y), new Vector2(fillEndX, start.Y), surfaceCol, 1.0f);
    }


    private void DrawAetherBarComposite(ImDrawListPtr dl, Vector2 start, Vector2 end, float progress, byte alpha, bool isPreparing, float phase)
    {
        EnsureAetherTextures();

        if (_aetherFrame == null || _aetherFill == null || _aetherFrameSize.X <= 1 || _aetherFrameSize.Y <= 1)
        {
            DrawEnergyVialFill(dl, start, end, progress, phase: 0f, alpha);
            return;
        }

        var w = end.X - start.X;
        var h = end.Y - start.Y;
        if (w <= 1 || h <= 1) return;

        progress = Math.Clamp(progress, 0f, 1f);

        GetFillRect(start, end, out var fillStart, out var fillEnd);
        if (fillEnd.X <= fillStart.X || fillEnd.Y <= fillStart.Y) return;

        var rounding = MathF.Max(2f, h * 0.18f);

        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, alpha));

        var backA = (byte)Math.Clamp((int)(alpha * 0.10f), 0, 255);
        dl.AddRectFilled(fillStart, fillEnd, UiSharedService.Color(0, 0, 0, backA), rounding);

        var effectiveProgress = isPreparing ? 1f : progress;
        var fillEndX = fillStart.X + (fillEnd.X - fillStart.X) * effectiveProgress;

        var pulseMul = isPreparing ? (0.80f + 0.20f * PingPong((float)ImGui.GetTime() * 0.85f)) : 1f;
        var fillAInt = (int)((Math.Min(255, alpha + 55)) * pulseMul);
        fillAInt = Math.Clamp(fillAInt, 0, 255);

        var fillA = (byte)fillAInt;
        var glowA = (byte)Math.Clamp((int)(fillAInt * 0.70f), 0, 255);
        var highlightA = (byte)Math.Clamp((int)(fillAInt * 0.22f), 0, 255);

        if (fillEndX > fillStart.X + 0.5f)
        {
            dl.PushClipRect(fillStart, new Vector2(fillEndX, fillEnd.Y), true);

            
            dl.AddImage(_aetherFill.Handle, start, end, Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, fillA));
            
            dl.AddImage(_aetherFill.Handle, start + new Vector2(0f, -1f), end + new Vector2(0f, -1f), Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, glowA));
            
            dl.AddRectFilledMultiColor(
                fillStart, fillEnd,
                UiSharedService.Color(255, 255, 255, highlightA),
                UiSharedService.Color(255, 255, 255, highlightA),
                UiSharedService.Color(140, 60, 230, 0),
                UiSharedService.Color(140, 60, 230, 0));

            dl.PopClipRect();

            if (!isPreparing && progress > 0f && progress < 1f)
            {
                var edgeA = (byte)Math.Min((int)alpha, 230);
                dl.AddLine(new Vector2(fillEndX, fillStart.Y + 1f), new Vector2(fillEndX, fillEnd.Y - 1f), UiSharedService.Color(255, 255, 255, edgeA), 2.0f);
                dl.AddLine(new Vector2(fillEndX - 1f, fillStart.Y + 1f), new Vector2(fillEndX - 1f, fillEnd.Y - 1f), UiSharedService.Color(235, 200, 255, (byte)Math.Clamp((int)(edgeA * 0.55f), 0, 255)), 3.0f);

                DrawFillEdgeGlow(dl, fillStart, fillEnd, fillEndX, phase, edgeA);
            }

        }

        DrawFrameBorderOnly(dl, start, end, fillStart, fillEnd, alpha);
    }
    private void DrawFrameBorderOnly(ImDrawListPtr dl, Vector2 start, Vector2 end, Vector2 holeStart, Vector2 holeEnd, byte alpha)
    {
        if (_aetherFrame == null) return;

        var col = UiSharedService.Color(255, 255, 255, alpha);

        // Top strip
        dl.PushClipRect(start, new Vector2(end.X, holeStart.Y), true);
        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, col);
        dl.PopClipRect();

        // Bottom strip
        dl.PushClipRect(new Vector2(start.X, holeEnd.Y), end, true);
        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, col);
        dl.PopClipRect();

        // Left strip
        dl.PushClipRect(new Vector2(start.X, holeStart.Y), new Vector2(holeStart.X, holeEnd.Y), true);
        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, col);
        dl.PopClipRect();

        // Right strip
        dl.PushClipRect(new Vector2(holeEnd.X, holeStart.Y), new Vector2(end.X, holeEnd.Y), true);
        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, col);
        dl.PopClipRect();
    }


    private static void GetFillRect(Vector2 start, Vector2 end, out Vector2 fillStart, out Vector2 fillEnd)
    {
        var w = end.X - start.X;
        var h = end.Y - start.Y;
        fillStart = new Vector2(start.X + w * FillUvX0, start.Y + h * FillUvY0);
        fillEnd = new Vector2(start.X + w * FillUvX1, start.Y + h * FillUvY1);
    }


    private void EnsureAetherTextures()
    {
        if (_aetherFrame != null && _aetherFill != null && _aetherFillBlue != null) return;

        try
        {
            _aetherFrame = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_frame_bar.png"));
            _aetherFill = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_fill_strip.png"));
            _aetherFillBlue = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_fill_blue.png"));

            if (_aetherFrame != null) _aetherFrameSize = new Vector2(_aetherFrame.Width, _aetherFrame.Height);
        }
        catch
        {
            _aetherFrame = null;
            _aetherFill = null;
            _aetherFillBlue = null;
            _aetherFrameSize = Vector2.Zero;
        }
    }

    private static byte[] ReadEmbedded(string resourceName)
    {
        using var s = typeof(DownloadUi).Assembly.GetManifestResourceStream(resourceName);
        if (s == null) throw new FileNotFoundException("Missing embedded resource: " + resourceName);

        using var ms = new MemoryStream();
        s.CopyTo(ms);
        return ms.ToArray();
    }


    private void PruneExpiredDownloads(DateTime nowUtc)
    {
        foreach (var kv in _currentDownloads.ToList())
        {
            if (kv.Value.Finished && (nowUtc - kv.Value.LastUpdateUtc) > DownloadUiHoldWindow)
                _currentDownloads.TryRemove(kv.Key, out _);
        }
    }

    private static bool AnyUploadsActive(FileUploadManager mgr)
        => mgr.CurrentUploads.Any();

    private (int W, int H) GetGlobalBarSize(bool haveAether)
    {
        var baseW = Math.Max(_configService.Current.TransferBarsWidth, 520);
        var baseH = Math.Max(_configService.Current.TransferBarsHeight, 40);

        var w = (int)MathF.Round(baseW * 1.15f);

        if (!haveAether || _aetherFrameSize.X <= 1 || _aetherFrameSize.Y <= 1)
        {
            var h2 = (int)MathF.Round(baseH * 1.15f);
            h2 = Math.Clamp(h2, 34, 90);
            return (w, h2);
        }

        var aspect = _aetherFrameSize.Y / _aetherFrameSize.X;
        var hCalc = (int)MathF.Round(w * aspect);

        var h = Math.Max((int)MathF.Round(baseH * 1.15f), hCalc);
        h = Math.Clamp(h, 34, 90);

        if (h != hCalc)
            w = (int)MathF.Round(h / aspect);

        return (w, h);
    }


    private void DrawUploadBarCompositeBlue(ImDrawListPtr dl, Vector2 start, Vector2 end, float progress, byte alpha, float phase)
    {
        EnsureAetherTextures();

        var fillTex = _aetherFillBlue ?? _aetherFill;

        if (_aetherFrame == null || fillTex == null || _aetherFrameSize.X <= 1 || _aetherFrameSize.Y <= 1)
        {
            DrawEnergyVialFill(dl, start, end, progress, phase, alpha);
            return;
        }

        var w = end.X - start.X;
        var h = end.Y - start.Y;
        if (w <= 1 || h <= 1) return;

        progress = Math.Clamp(progress, 0f, 1f);

        GetFillRect(start, end, out var fillStart, out var fillEnd);
        if (fillEnd.X <= fillStart.X || fillEnd.Y <= fillStart.Y) return;

        var rounding = MathF.Max(2f, h * 0.18f);

        dl.AddImage(_aetherFrame.Handle, start, end, Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, alpha));

        var backA = (byte)Math.Clamp((int)(alpha * 0.10f), 0, 255);
        dl.AddRectFilled(fillStart, fillEnd, UiSharedService.Color(0, 0, 0, backA), rounding);

        var fillEndX = fillStart.X + (fillEnd.X - fillStart.X) * progress;
        if (fillEndX <= fillStart.X + 0.5f)
        {
            DrawFrameBorderOnly(dl, start, end, fillStart, fillEnd, alpha);
            return;
        }

        dl.PushClipRect(fillStart, new Vector2(fillEndX, fillEnd.Y), true);


        var baseTint = UiSharedService.Color(170, 220, 255, alpha);
        var glowTint = UiSharedService.Color(120, 190, 255, (byte)(alpha * 0.70f));

        dl.AddImage(fillTex.Handle, start, end, Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, alpha));
        dl.AddImage(fillTex.Handle, start + new Vector2(0f, -1f), end + new Vector2(0f, -1f), Vector2.Zero, Vector2.One, UiSharedService.Color(255, 255, 255, (byte)(alpha * 0.70f)));

        var highlightA = (byte)Math.Clamp((int)(alpha * 0.18f), 0, 255);
        dl.AddRectFilledMultiColor(
            fillStart, fillEnd,
            UiSharedService.Color(255, 255, 255, highlightA),
            UiSharedService.Color(255, 255, 255, highlightA),
            UiSharedService.Color(80, 140, 220, 0),
            UiSharedService.Color(80, 140, 220, 0));

        dl.PopClipRect();

        DrawFrameBorderOnly(dl, start, end, fillStart, fillEnd, alpha);
    }


    private void DrawGlobalTransferOverlay(DateTime nowUtc)
    {
        var editing = _configService.Current.EditGlobalTransferOverlay;

        var haveDownloads = _currentDownloads.Any() || editing;
        var haveUploads = (_configService.Current.ShowUploadProgress && AnyUploadsActive(_fileTransferManager)) || editing;

        var anyVisibleDl = false;

        if (!editing)
        {
            if (!_configService.Current.ShowGlobalTransferBars && !haveUploads) return;
            if (!_configService.Current.ShowGlobalTransferBars && !haveDownloads && !haveUploads) return;
        }

        EnsureAetherTextures();
        var haveAether = _aetherFrame != null && _aetherFill != null && _aetherFrameSize.X > 1 && _aetherFrameSize.Y > 1;

        var (barWBase, barHBase) = GetGlobalBarSize(haveAether);

        var scale = Math.Clamp(_configService.Current.GlobalTransferOverlayScale, 0.70f, 1.60f);
        var barW = (int)MathF.Round(barWBase * scale);
        var barH = (int)MathF.Round(barHBase * scale);

        const byte alpha = 235;
        var gapX = 10f;
        var gapY = 8f;


        var flags = ImGuiWindowFlags.AlwaysAutoResize
            | ImGuiWindowFlags.NoFocusOnAppearing
            | ImGuiWindowFlags.NoNav;

        if (!editing)
        {
            flags |= ImGuiWindowFlags.NoDecoration;
            flags |= ImGuiWindowFlags.NoMove;
            //flags |= ImGuiWindowFlags.NoInputs;
        }

        ImGui.SetNextWindowBgAlpha(editing ? 0.35f : 0f);

        var vp = ImGui.GetMainViewport();
        var px = _configService.Current.GlobalTransferOverlayX;
        var py = _configService.Current.GlobalTransferOverlayY;

        if (float.IsNaN(px) || float.IsNaN(py))
        {
            var defaultPos = vp.WorkPos + new Vector2(vp.WorkSize.X * 0.5f, 90f);
            ImGui.SetNextWindowPos(defaultPos, ImGuiCond.FirstUseEver, new Vector2(0.5f, 0f));
        }
        else
        {
            ImGui.SetNextWindowPos(new Vector2(px, py), editing ? ImGuiCond.FirstUseEver : ImGuiCond.Always);
        }

        var windowName = editing ? "RavaSync Transfers###RavaSyncGlobalTransfers" : "###RavaSyncGlobalTransfers";
        if (!ImGui.Begin(windowName, flags))
        {
            ImGui.End();
            return;
        }

        // Persist window position when editing
        if (editing && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
        {
            var pos = ImGui.GetWindowPos();
            var changed =
                float.IsNaN(_configService.Current.GlobalTransferOverlayX) ||
                float.IsNaN(_configService.Current.GlobalTransferOverlayY) ||
                MathF.Abs(_configService.Current.GlobalTransferOverlayX - pos.X) > 0.5f ||
                MathF.Abs(_configService.Current.GlobalTransferOverlayY - pos.Y) > 0.5f;

            if (changed)
            {
                _configService.Current.GlobalTransferOverlayX = pos.X;
                _configService.Current.GlobalTransferOverlayY = pos.Y;
                _configService.Save();
            }
        }

        var dl = ImGui.GetWindowDrawList();
        var phase = (float)ImGui.GetTime();

        // ===== Editing controls =====
        if (editing)
        {
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Layout:");
            ImGui.SameLine();

            var row = _configService.Current.GlobalTransferOverlayRowLayout;

            if (ImGui.RadioButton("Stacked##globalLayout", !row))
            {
                _configService.Current.GlobalTransferOverlayRowLayout = false;
                _configService.Save();
                row = false;
            }

            ImGui.SameLine();
            if (ImGui.RadioButton("Row##globalLayout", row))
            {
                _configService.Current.GlobalTransferOverlayRowLayout = true;
                _configService.Save();
                row = true;
            }

            ImGui.SameLine(0, 14f);
            ImGui.AlignTextToFramePadding();

            ImGui.SameLine(0, 14f);
            float s = _configService.Current.GlobalTransferOverlayScale;
            ImGui.SetNextItemWidth(160f);
            if (ImGui.SliderFloat("Scale##globalTransfers", ref s, 0.70f, 1.60f, "%.2f"))
            {
                _configService.Current.GlobalTransferOverlayScale = s;
                _configService.Save();
            }

            ImGui.SameLine();
            if (ImGui.SmallButton("Reset##globalTransfers"))
            {
                _configService.Current.GlobalTransferOverlayScale = 1.0f;
                _configService.Current.GlobalTransferOverlayX = float.NaN;
                _configService.Current.GlobalTransferOverlayY = float.NaN;
                _configService.Current.GlobalTransferOverlayRowLayout = false;
                _configService.Save();
            }

            ImGui.SameLine();
            if (ImGui.SmallButton("Done editing##globalTransfers"))
            {
                _configService.Current.EditGlobalTransferOverlay = false;
                _configService.Save();
            }

            ImGui.TextDisabled("Drag this window by the title bar.");
            ImGui.Separator();
        }

        // ===== Build download aggregate =====
        long dlTotalBytes = 0;
        long dlTransferredBytes = 0;
        int dlTotalFiles = 0;
        int dlTransferredFiles = 0;

        int dlSlot = 0, dlQueue = 0, dlProg = 0, dlDecomp = 0;
        bool dlAllPreparing = true;

        if (_configService.Current.ShowGlobalTransferBars && haveDownloads)
        {
            foreach (var entry in _currentDownloads.ToList())
            {
                var statuses = entry.Value.Status.Values.ToList();

                var entryTotalBytes = entry.Value.AccTotalBytes + statuses.Sum(s => s.TotalBytes);
                if (entryTotalBytes > 0 && entryTotalBytes < MinVisibleDownloadBytes)
                    continue;

                anyVisibleDl = true;

                dlSlot += statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForSlot);
                dlQueue += statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForQueue);
                dlProg += statuses.Count(c => c.DownloadStatus == DownloadStatus.Downloading);
                dlDecomp += statuses.Count(c => c.DownloadStatus == DownloadStatus.Decompressing);

                dlTotalBytes += entryTotalBytes;
                dlTransferredBytes += entry.Value.AccTransferredBytes + statuses.Sum(s => s.TransferredBytes);

                dlTotalFiles += entry.Value.AccTotalFiles + statuses.Sum(s => s.TotalFiles);
                dlTransferredFiles += entry.Value.AccTransferredFiles + statuses.Sum(s => s.TransferredFiles);

                var thisPreparing = statuses.All(s =>
                    (s.DownloadStatus == DownloadStatus.Initializing
                     || s.DownloadStatus == DownloadStatus.WaitingForSlot
                     || s.DownloadStatus == DownloadStatus.WaitingForQueue)
                    && s.TransferredBytes == 0);

                dlAllPreparing &= thisPreparing;
            }
        }

        var dlPct = (dlTotalBytes <= 0) ? 0f : (float)(dlTransferredBytes / (double)dlTotalBytes);
        dlPct = Math.Clamp(dlPct, 0f, 1f);

        var dlText = dlAllPreparing
            ? "Preparing Files..."
            : $"{dlTransferredFiles}/{dlTotalFiles}  ({UiSharedService.ByteToString(dlTransferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(dlTotalBytes)})";

        // ===== Build upload aggregate =====
        var uploads = haveUploads ? _fileTransferManager.CurrentUploads.ToList() : null;

        var ulTotalUploads = uploads?.Count ?? 0;
        var ulDoneUploads = uploads?.Count(c => c.IsTransferred) ?? 0;
        var ulTotalUploaded = uploads?.Sum(c => c.Transferred) ?? 0;
        var ulTotalToUpload = uploads?.Sum(c => c.Total) ?? 0;

        var ulPct = (ulTotalToUpload <= 0) ? 0f : (float)(ulTotalUploaded / (double)ulTotalToUpload);
        ulPct = Math.Clamp(ulPct, 0f, 1f);

        var ulText = $"Uploading  {ulDoneUploads}/{ulTotalUploads}  ({UiSharedService.ByteToString(ulTotalUploaded, addSuffix: false)}/{UiSharedService.ByteToString(ulTotalToUpload)})";

        // ===== Draw bars (stacked vs row) =====
        void DrawDownloadBar()
        {
            var p0 = ImGui.GetCursorScreenPos();
            var p1 = p0 + new Vector2(barW, barH);

            var mp = ImGui.GetIO().MousePos;
            var hovered = mp.X >= p0.X && mp.X <= p1.X && mp.Y >= p0.Y && mp.Y <= p1.Y;

            if (hovered && ImGui.IsMouseClicked(ImGuiMouseButton.Left))
            {
                _globalTransfersExpanded = !_globalTransfersExpanded;
            }

            DrawAetherBarComposite(dl, p0, p1, dlPct, alpha, dlAllPreparing, phase);

            if (_configService.Current.TransferBarsShowText)
            {
                GetFillRect(p0, p1, out var fs, out var fe);
                var mid = (fs + fe) * 0.5f;
                var ts = ImGui.CalcTextSize(dlText);
                UiSharedService.DrawOutlinedFont(dl, dlText, new Vector2(mid.X - ts.X / 2f, mid.Y - ts.Y / 2f),
                    UiSharedService.Color(255, 255, 255, 255), UiSharedService.Color(0, 0, 0, 220), 2);
            }


            ImGui.Dummy(new Vector2(barW, barH));

            //This wasn't updating in live operation. Rather than showing it all scuffed, we just simply wont.
            //int hSlot = 0, hQueue = 0, hProg = 0, hDecomp = 0;

            //foreach (var entry in _currentDownloads.ToList())
            //{
            //    var values = entry.Value.Status?.Values;
            //    if (values == null) continue;

            //    foreach (var s in values)
            //    {
            //        if (s == null) continue;

            //        if (s.DownloadStatus == DownloadStatus.WaitingForSlot) hSlot++;
            //        else if (s.DownloadStatus == DownloadStatus.WaitingForQueue) hQueue++;
            //        else if (s.DownloadStatus == DownloadStatus.Downloading) hProg++;
            //        else if (s.DownloadStatus == DownloadStatus.Decompressing) hDecomp++;
            //    }
            //}

            //var hint = $"Slot wait: {hSlot}  •  Queued: {hQueue}  •  Downloading: {hProg}  •  Decompressing: {hDecomp}";
            //var hintSize = ImGui.CalcTextSize(hint);

            //var hintPos = new Vector2(
            //    p0.X + (barW - hintSize.X) * 0.5f,
            //    p1.Y + 6f
            //);

            //UiSharedService.DrawOutlinedFont(
            //    dl,
            //    hint,
            //    hintPos,
            //    UiSharedService.Color(255, 255, 255, 210),
            //    UiSharedService.Color(0, 0, 0, 200),2
            //);

            ImGui.Dummy(new Vector2(1, ImGui.GetTextLineHeight() + 10f));

        }

        void DrawUploadBar()
        {
            var p0 = ImGui.GetCursorScreenPos();
            var p1 = p0 + new Vector2(barW, barH);

            DrawUploadBarCompositeBlue(dl, p0, p1, ulPct, alpha, phase);

            if (_configService.Current.TransferBarsShowText)
            {
                GetFillRect(p0, p1, out var fs, out var fe);
                var mid = (fs + fe) * 0.5f;
                var ts = ImGui.CalcTextSize(ulText);
                UiSharedService.DrawOutlinedFont(dl, ulText, new Vector2(mid.X - ts.X / 2f, mid.Y - ts.Y / 2f),
                    UiSharedService.Color(255, 255, 255, 255), UiSharedService.Color(0, 0, 0, 220), 2);
            }

            ImGui.Dummy(new Vector2(barW, barH));
            ImGui.Dummy(new Vector2(1, ImGui.GetTextLineHeight() + 10f));

        }

        var showDlBar = _configService.Current.ShowGlobalTransferBars && (editing || anyVisibleDl);
        var showUlBar = haveUploads;

        if (_configService.Current.GlobalTransferOverlayRowLayout)
        {
            if (showDlBar)
            {
                ImGui.BeginGroup();
                DrawDownloadBar();
                ImGui.EndGroup();
            }

            if (showUlBar)
            {
                if (showDlBar) ImGui.SameLine(0, gapX);

                ImGui.BeginGroup();
                DrawUploadBar();
                ImGui.EndGroup();
            }

            ImGui.Dummy(new Vector2(1, gapY));
        }
        else
        {
            if (showDlBar)
            {
                DrawDownloadBar();
                ImGui.Dummy(new Vector2(1, gapY));
            }

            if (showUlBar)
            {
                DrawUploadBar();
                ImGui.Dummy(new Vector2(1, gapY));
            }
        }

        // ===== Details list =====
        if (_globalTransfersExpanded && _currentDownloads.Any())
        {
            var listW = _configService.Current.GlobalTransferOverlayRowLayout && showDlBar && showUlBar ? (barW * 2f + gapX) : barW;
            var listH = 170f * scale;

            var p = ImGui.GetCursorScreenPos();
            dl.AddRectFilled(p, p + new Vector2(listW, listH), UiSharedService.Color(0, 0, 0, 140), 10f);

            ImGui.BeginChild("##global_dl_list", new Vector2(listW, listH), false, ImGuiWindowFlags.NoScrollbar);

            foreach (var kv in _currentDownloads.ToList())
            {
                var statuses = kv.Value.Status.Values.ToList();

                var totalBytes = kv.Value.AccTotalBytes + statuses.Sum(s => s.TotalBytes);
                var transferredBytes = kv.Value.AccTransferredBytes + statuses.Sum(s => s.TransferredBytes);

                var sSlot = statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForSlot);
                var sQueue = statuses.Count(c => c.DownloadStatus == DownloadStatus.WaitingForQueue);
                var sProg = statuses.Count(c => c.DownloadStatus == DownloadStatus.Downloading);
                var sDecomp = statuses.Count(c => c.DownloadStatus == DownloadStatus.Decompressing);

                var pct = (totalBytes <= 0) ? 0f : (float)(transferredBytes / (double)totalBytes);
                pct = Math.Clamp(pct, 0f, 1f);

                ImGui.TextUnformatted($"{kv.Key.Name}  [W:{sSlot}/Q:{sQueue}/P:{sProg}/D:{sDecomp}]");
                ImGui.SameLine();
                ImGui.TextDisabled($"{UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}");

                ImGui.ProgressBar(pct, new Vector2(-1, MathF.Max(7f, 7f * scale)), string.Empty);
                ImGui.Dummy(new Vector2(1, 6));
            }

            ImGui.EndChild();
        }

        ImGui.End();
    }

    private Vector2 SmoothAndSnapScreen(IntPtr key, Vector2 target, float dt)
    {
        if (key == IntPtr.Zero) return new Vector2(MathF.Round(target.X), MathF.Round(target.Y));

        if (!_smoothedScreens.TryGetValue(key, out var cur))
            cur = target;

        if (Vector2.DistanceSquared(cur, target) > 2500f) 
            cur = target;

        const float lambda = 28f;
        var a = 1f - MathF.Exp(-lambda * dt);
        var next = cur + (target - cur) * a;

        _smoothedScreens[key] = next;

        return new Vector2(MathF.Round(next.X), MathF.Round(next.Y));
    }

    private void PruneSmoothedScreens(HashSet<IntPtr> keep)
    {
        if (_smoothedScreens.Count == 0) return;

        var remove = new List<IntPtr>();
        foreach (var k in _smoothedScreens.Keys)
            if (!keep.Contains(k))
                remove.Add(k);

        foreach (var k in remove)
            _smoothedScreens.Remove(k);
    }

    public override bool DrawConditions()
    {
        if (_uiShared.EditTrackerPosition) 
            return true;
        
        if (!_configService.Current.ShowTransferWindow && !_configService.Current.ShowTransferBars && !_configService.Current.ShowGlobalTransferBars && !_configService.Current.ShowUploadProgress && !_configService.Current.EditGlobalTransferOverlay)
            return false;

        if (!_configService.Current.EditGlobalTransferOverlay && !_currentDownloads.Any() && !_fileTransferManager.CurrentUploads.Any() && !_uploadingPlayers.Any())
            return false;

        if (!IsOpen) 
            return false;
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

        var rows = _configService.Current.ParallelDownloads <= 0 ? 8 : _configService.Current.ParallelDownloads;
        var maxHeight = ImGui.GetTextLineHeight() * (rows + 3);
        SizeConstraints = new()
        {
            MinimumSize = new Vector2(300, maxHeight),
            MaximumSize = new Vector2(300, maxHeight),
        };
    }
}