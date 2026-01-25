using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Textures.TextureWraps;
using Microsoft.Extensions.Logging;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using System;
using System.IO;
using System.Numerics;
using RavaSync.MareConfiguration;
using Dalamud.Game.ClientState.Objects.Types;

namespace RavaSync.UI;

public sealed class TournamentHpUi : WindowMediatorSubscriberBase
{
    private readonly ToyBox _games;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly UiSharedService _uiShared;
    private readonly MareConfigService _configService;

    private IDalamudTextureWrap? _aetherFrame;
    private IDalamudTextureWrap? _aetherFill;
    private Vector2 _frameSize = Vector2.Zero;

    private readonly Dictionary<string, Vector2> _smoothedScreen = new(StringComparer.Ordinal);
    private long _lastSmoothTick;


    private const float FillUvX0 = 167f / 1462f;
    private const float FillUvX1 = 1299f / 1462f;
    private const float FillUvY0 = 65f / 238f;
    private const float FillUvY1 = 173f / 238f;

    public TournamentHpUi(ILogger<TournamentHpUi> logger,MareMediator mediator,ToyBox games, DalamudUtilService dalamudUtil, UiSharedService uiShared, MareConfigService configService,PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "Tournament HP", performanceCollectorService)
    {
        _games = games;
        _dalamudUtil = dalamudUtil;
        _uiShared = uiShared;
        _configService = configService;

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

        EnsureAetherTextures();
    }

    protected override void DrawInternal()
    {

        EnsureAetherTextures();

        var entries = _games.GetTournamentOverlayEntries();
        if (entries.Count == 0) return;

        var dl = ImGui.GetBackgroundDrawList();

        foreach (var e in entries)
        {
            var obj = _dalamudUtil.GetGameObjectBySessionId(e.SessionId);
            if (obj == null || obj.Address == IntPtr.Zero) continue;

            var screen = _dalamudUtil.WorldToScreen(obj);
            if (screen == Vector2.Zero) continue;

            screen = SmoothScreen(e.SessionId, screen);
            screen = PixelSnap(screen);

            const byte transparency = 220;

            var haveAether = _aetherFrame != null && _aetherFill != null && _frameSize.X > 1 && _frameSize.Y > 1;

            int barW;
            int barH;

            if (haveAether)
            {
                barW = Math.Max(_configService.Current.TransferBarsWidth, 320);

                var aspect = _frameSize.Y / _frameSize.X;
                barH = Math.Max(_configService.Current.TransferBarsHeight, (int)MathF.Round(barW * aspect));

                var minH = Math.Max(_configService.Current.TransferBarsHeight, 34);
                if (barH < minH)
                {
                    barH = minH;
                    barW = (int)MathF.Round(barH / aspect);
                }

                barW = (int)MathF.Round(barW * 1.10f);
                barH = (int)MathF.Round(barH * 1.10f);
            }
            else
            {
                barW = Math.Max(_configService.Current.TransferBarsWidth, 320);
                barH = Math.Max(_configService.Current.TransferBarsHeight, 28);
            }

            var yCenterOffset = -70f * (barH / 44f);

            var barCenter = screen + new Vector2(0f, yCenterOffset);
            var start = new Vector2(barCenter.X - barW / 2f, barCenter.Y - barH / 2f);
            var end = new Vector2(barCenter.X + barW / 2f, barCenter.Y + barH / 2f);
            start = PixelSnap(start);
            end = PixelSnap(end);

            var p = e.MaxHp <= 0 ? 0f : Math.Clamp(e.Hp / (float)e.MaxHp, 0f, 1f);

            if (_aetherFrame != null && _aetherFill != null)
            {
                dl.AddImage(_aetherFrame.Handle, start, end);

                GetFillRect(start, end, out var fillStart, out var fillEnd);
                var fillEndX = fillStart.X + (fillEnd.X - fillStart.X) * p;

                if (fillEndX > fillStart.X + 0.5f)
                {
                    var backA = (byte)Math.Clamp((int)(transparency * 0.10f), 0, 255);
                    dl.AddRectFilled(fillStart, fillEnd, UiSharedService.Color(0, 0, 0, backA), MathF.Max(2f, (end.Y - start.Y) * 0.18f));

                    if (fillEndX > fillStart.X + 0.5f)
                    {
                        dl.PushClipRect(fillStart, new Vector2(fillEndX, fillEnd.Y), true);

                        var tint = UiSharedService.Color(90, 255, 130, transparency);
                        var glow = UiSharedService.Color(140, 255, 190, (byte)(transparency * 0.60f));

                        dl.AddImage(_aetherFill.Handle, start, end, Vector2.Zero, Vector2.One, tint);
                        dl.AddImage(_aetherFill.Handle, start + new Vector2(0f, -1f), end + new Vector2(0f, -1f), Vector2.Zero, Vector2.One, glow);

                        dl.PopClipRect();
                    }
                }
            }
            else
            {
                dl.AddRectFilled(start, end, UiSharedService.Color(0, 0, 0, 160), 6f);
                dl.AddRect(start, end, UiSharedService.Color(255, 255, 255, 200), 6f);

                var innerStart = start + new Vector2(8f, 8f);
                var innerEnd = end - new Vector2(8f, 8f);
                var innerEndX = innerStart.X + (innerEnd.X - innerStart.X) * p;
                dl.AddRectFilled(innerStart, new Vector2(innerEndX, innerEnd.Y), UiSharedService.Color(90, 255, 130, 220), 5f);
            }

            var text = $"{e.Hp}/{e.MaxHp}";
            var ts = ImGui.CalcTextSize(text);
            var tp = new Vector2(
                start.X + (barW - ts.X) * 0.5f,
                start.Y + (barH - ts.Y) * 0.5f
            );

            UiSharedService.DrawOutlinedFont(dl, text, tp,
                UiSharedService.Color(255, 255, 255, 255), UiSharedService.Color(0, 0, 0, 255), 2);


        }
    }

    private static void GetFillRect(Vector2 start, Vector2 end, out Vector2 fillStart, out Vector2 fillEnd)
    {
        var w = end.X - start.X;
        var h = end.Y - start.Y;
        fillStart = new Vector2(start.X + w * FillUvX0, start.Y + h * FillUvY0);
        fillEnd = new Vector2(start.X + w * FillUvX1, start.Y + h * FillUvY1);
    }

    private Vector2 SmoothScreen(string sid, Vector2 target)
    {
        var now = Environment.TickCount64;

        float dt;
        if (_lastSmoothTick == 0)
            dt = 1f / 60f;
        else
            dt = Math.Clamp((now - _lastSmoothTick) / 1000f, 1f / 240f, 1f / 10f);

        _lastSmoothTick = now;

        const float speed = 24f;
        var a = 1f - MathF.Exp(-speed * dt);

        if (!_smoothedScreen.TryGetValue(sid, out var cur))
            cur = target;

        if (Vector2.DistanceSquared(cur, target) > 2500f)
            cur = target;

        var next = cur + (target - cur) * a;
        _smoothedScreen[sid] = next;
        return next;
    }

    private static Vector2 PixelSnap(Vector2 v)
        => new(MathF.Round(v.X), MathF.Round(v.Y));

    private void EnsureAetherTextures()
    {
        if (_aetherFrame != null && _aetherFill != null) return;

        try
        {
            _aetherFrame = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_frame_bar.png"));
            _aetherFill = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.aether_fill_green.png"));
            if (_aetherFrame != null) _frameSize = new Vector2(_aetherFrame.Width, _aetherFrame.Height);
        }
        catch
        {
            _aetherFrame = null;
            _aetherFill = null;
            _frameSize = Vector2.Zero;
        }
    }

    private static byte[] ReadEmbedded(string resourceName)
    {
        using var s = typeof(TournamentHpUi).Assembly.GetManifestResourceStream(resourceName);
        if (s == null) throw new FileNotFoundException("Missing embedded resource: " + resourceName);

        using var ms = new MemoryStream();
        s.CopyTo(ms);
        return ms.ToArray();
    }
}
