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

    private Vector2 _aetherFrameSize = Vector2.Zero;

    private const float FillUvX0 = 167f / 1462f;
    private const float FillUvX1 = 1299f / 1462f;
    private const float FillUvY0 = 65f / 238f;
    private const float FillUvY1 = 173f / 238f;

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

                // Progress
                double dlProgressPercent;
                string downloadText;

                if (allPreparing)
                {
                    dlProgressPercent = 0.0;
                    downloadText = "Preparing Files...";
                }
                else
                {
                    dlProgressPercent = totalBytes <= 0 ? 0.0 : transferredBytes / (double)totalBytes;
                    if (dlProgressPercent < 0) dlProgressPercent = 0;
                    if (dlProgressPercent > 1) dlProgressPercent = 1;

                    downloadText = $"{UiSharedService.ByteToString(transferredBytes, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
                }

                var dt = ImGui.GetIO().DeltaTime;
                var target = allPreparing ? 0f : (float)dlProgressPercent;

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

                var drawList = ImGui.GetBackgroundDrawList();

                DrawAetherBarComposite(drawList, dlBarStart, dlBarEnd, transfer.Value.SmoothedPercent, transparency, allPreparing, transfer.Value.AnimPhase);

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
                            UiSharedService.Color(112, 28, 180, 255),
                            UiSharedService.Color(0, 0, 0, 200),
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
        if (_aetherFrame != null && _aetherFill != null) return;

        try
        {
            _aetherFrame = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_frame_bar.png"));
            _aetherFill = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_fill_strip.png"));

            if (_aetherFrame != null) _aetherFrameSize = new Vector2(_aetherFrame.Width, _aetherFrame.Height);
        }
        catch
        {
            _aetherFrame = null;
            _aetherFill = null;
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

        var rows = _configService.Current.ParallelDownloads <= 0 ? 8 : _configService.Current.ParallelDownloads;
        var maxHeight = ImGui.GetTextLineHeight() * (rows + 3);
        SizeConstraints = new()
        {
            MinimumSize = new Vector2(300, maxHeight),
            MaximumSize = new Vector2(300, maxHeight),
        };
    }
}