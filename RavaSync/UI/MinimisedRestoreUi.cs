using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Textures.TextureWraps;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using System.Numerics;

namespace RavaSync.UI;

public sealed class MinimisedRestoreUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiShared;
    private readonly MareConfigService _config;
    private bool _didDrag;

    private IDalamudTextureWrap? _iconTex;
    private Vector2? _spawnPos;


    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public MinimisedRestoreUi(ILogger<MinimisedRestoreUi> logger,
     UiSharedService uiShared,
     MareConfigService config,
     MareMediator mediator,
     PerformanceCollectorService performanceCollectorService)
     : base(logger, mediator, "###RavaSyncMinimizedRestore", performanceCollectorService)
    {
        _uiShared = uiShared;
        _config = config;

        IsOpen = _config.Current.HasValidSetup() && _config.Current.ShowMinimizedRestoreIcon;

        try
        {
            _iconTex = _uiShared.LoadImage(ReadEmbedded("RavaSync.Resources.icon.png"));
        }
        catch
        {
            _iconTex = null;
        }



        Flags |= ImGuiWindowFlags.NoDecoration
              | ImGuiWindowFlags.AlwaysAutoResize
              | ImGuiWindowFlags.NoFocusOnAppearing
              | ImGuiWindowFlags.NoNav;

        AllowPinning = false;
        AllowClickthrough = false;

        Mediator.Subscribe<MainUiMinimizedMessage>(this, (_) =>
        {
            if (_config.Current.HasValidSetup() && _config.Current.ShowMinimizedRestoreIcon)
                IsOpen = true;
        });

        Mediator.Subscribe<MainUiMinimizedAtPositionMessage>(this, (msg) =>
        {
            _spawnPos = msg.Position;
            if (_config.Current.HasValidSetup() && _config.Current.ShowMinimizedRestoreIcon)
                IsOpen = true;
        });


        Mediator.Subscribe<MainUiRestoredMessage>(this, (_) => IsOpen = false);
    }

    protected override void DrawInternal()
    {
        if (!_config.Current.HasValidSetup() || !_config.Current.ShowMinimizedRestoreIcon)
        {
            IsOpen = false;
            return;
        }

        ImGui.SetWindowFontScale(1.0f);
        ImGui.SetNextWindowBgAlpha(0.25f);

        if (_spawnPos != null)
        {
            var vp = ImGui.GetMainViewport();
            var pos = _spawnPos.Value;

            var min = vp.WorkPos;
            var max = vp.WorkPos + vp.WorkSize - new Vector2(60f, 60f);

            pos = Vector2.Clamp(pos, min, max);

            ImGui.SetWindowPos(pos, ImGuiCond.Always);
            _spawnPos = null;
        }


        using var pad = ImRaii.PushStyle(ImGuiStyleVar.WindowPadding, new Vector2(0, 0));
        using var rounding = ImRaii.PushStyle(ImGuiStyleVar.WindowRounding, 10f * ImGuiHelpers.GlobalScale);

        var size = new Vector2(44f, 44f) * ImGuiHelpers.GlobalScale;
        var start = ImGui.GetCursorScreenPos();

        ImGui.InvisibleButton("##restore", size);

        var dl = ImGui.GetWindowDrawList();
        var end = start + size;

        dl.AddRectFilled(start, end, ImGui.GetColorU32(new Vector4(0.08f, 0.02f, 0.12f, 0.85f)), 10f * ImGuiHelpers.GlobalScale);
        dl.AddRect(start, end, ImGui.GetColorU32(new Vector4(0.75f, 0.50f, 1.0f, 0.85f)), 10f * ImGuiHelpers.GlobalScale, 0, 1.5f * ImGuiHelpers.GlobalScale);

        if (_iconTex != null)
        {
            var padPx = 6f * ImGuiHelpers.GlobalScale;
            var p0 = start + new Vector2(padPx, padPx);
            var p1 = end - new Vector2(padPx, padPx);

            dl.AddImage(_iconTex.Handle, p0, p1);
        }
        else
        {
            var icon = FontAwesomeIcon.FireAlt.ToIconString();
            using (_uiShared.IconFont.Push())
            {
                var tsize = ImGui.CalcTextSize(icon);
                var tpos = start + (size - tsize) * 0.5f;
                dl.AddText(tpos, ImGui.GetColorU32(ImGuiColors.DalamudWhite), icon);
            }
        }


        if (ImGui.IsItemActivated())
            _didDrag = false;

        // IMPORTANT: give dragging a threshold, otherwise almost every click becomes a drag.
        var dragThreshold = 6.0f * ImGuiHelpers.GlobalScale;

        if (ImGui.IsItemActive() && ImGui.IsMouseDragging(ImGuiMouseButton.Left, dragThreshold))
        {
            _didDrag = true;
            var delta = ImGui.GetIO().MouseDelta;
            ImGui.SetWindowPos(ImGui.GetWindowPos() + delta, ImGuiCond.Always);
        }


        if (ImGui.IsItemDeactivated() && !_didDrag)
        {
            var pos = ImGui.GetWindowPos();
            Mediator.Publish(new RestoreMainUiAtPositionMessage(pos));

            IsOpen = false;
        }



        if (ImGui.IsItemHovered())
        {
            ImGui.BeginTooltip();
            ImGui.TextUnformatted("Open RavaSync");
            ImGui.EndTooltip();
        }
    }
    private static byte[] ReadEmbedded(string resourceName)
    {
        using var s = typeof(MinimisedRestoreUi).Assembly.GetManifestResourceStream(resourceName);
        if (s == null) throw new FileNotFoundException("Missing embedded resource: " + resourceName);

        using var ms = new MemoryStream();
        s.CopyTo(ms);
        return ms.ToArray();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _iconTex?.Dispose();
            _iconTex = null;
        }

        base.Dispose(disposing);
    }
}
