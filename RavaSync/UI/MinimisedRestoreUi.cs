using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Textures.TextureWraps;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data.Enum;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR.Utils;
using System.Numerics;

namespace RavaSync.UI;

public sealed class MinimisedRestoreUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiShared;
    private readonly MareConfigService _config;
    private readonly ApiController _apiController;
    private readonly ServerConfigurationManager _serverManager;
    private bool _didDrag;
    private bool _ignoreNextRestoredClose;

    private IDalamudTextureWrap? _iconTex;
    private Vector2? _spawnPos;


    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public MinimisedRestoreUi(ILogger<MinimisedRestoreUi> logger,
     UiSharedService uiShared,
     MareConfigService config,
     MareMediator mediator,
     PerformanceCollectorService performanceCollectorService,
     ApiController apiController,
     ServerConfigurationManager serverManager)
     : base(logger, mediator, "###RavaSyncMinimizedRestore", performanceCollectorService)
    {
        _uiShared = uiShared;
        _config = config;
        _apiController = apiController;
        _serverManager = serverManager;

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


        Mediator.Subscribe<MainUiRestoredMessage>(this, (_) =>
        {
            if (_ignoreNextRestoredClose)
            {
                _ignoreNextRestoredClose = false;
                return;
            }

            IsOpen = false;
        });
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

        ImGui.InvisibleButton("##restore", size, ImGuiButtonFlags.MouseButtonLeft | ImGuiButtonFlags.MouseButtonRight | ImGuiButtonFlags.MouseButtonMiddle);

        var dl = ImGui.GetWindowDrawList();
        var end = start + size;

        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;
        bool connectionToggleBusy = _apiController.ServerState is ServerState.Reconnecting or ServerState.Disconnecting;
        var accentColor = isConnectingOrConnected
            ? new Vector4(0.75f, 0.50f, 1.0f, 0.85f)
            : new Vector4(0.90f, 0.20f, 0.26f, 0.90f);

        dl.AddRectFilled(start, end, ImGui.GetColorU32(new Vector4(0.08f, 0.02f, 0.12f, 0.85f)), 10f * ImGuiHelpers.GlobalScale);
        dl.AddRect(start, end, ImGui.GetColorU32(accentColor), 10f * ImGuiHelpers.GlobalScale, 0, 1.5f * ImGuiHelpers.GlobalScale);

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
            if (!ImGui.IsMouseReleased(ImGuiMouseButton.Right) && !ImGui.IsMouseReleased(ImGuiMouseButton.Middle))
            {
                var pos = ImGui.GetWindowPos();
                Mediator.Publish(new RestoreMainUiAtPositionMessage(pos));
                IsOpen = false;
            }
        }

        if (!_didDrag && ImGui.IsItemHovered() && ImGui.IsMouseReleased(ImGuiMouseButton.Right))
        {
            _ignoreNextRestoredClose = true;
            Mediator.Publish(new UiToggleMessage(typeof(ToolsHubUi)));
        }

        if (!_didDrag && ImGui.IsItemHovered() && ImGui.IsMouseReleased(ImGuiMouseButton.Middle) && !connectionToggleBusy)
        {
            ToggleServerConnection();
        }

        if (ImGui.IsItemHovered())
        {
            ImGui.BeginTooltip();
            var connectHint = connectionToggleBusy
                ? _uiShared.L("UI.MinimisedRestoreUi.92C204E8", "Middle click: Connection busy")
                : isConnectingOrConnected
                    ? _uiShared.L("UI.MinimisedRestoreUi.0A1E4A91", "Middle click: Disconnect")
                    : _uiShared.L("UI.MinimisedRestoreUi.28A1F7C2", "Middle click: Connect");
            ImGui.TextUnformatted(_uiShared.L("UI.MinimisedRestoreUi.4936E024", "Left click: Open RavaSync\nRight click: Tools Hub") + "\n" + connectHint);
            ImGui.EndTooltip();
        }
    }
    private void ToggleServerConnection()
    {
        bool isConnectingOrConnected = _apiController.ServerState is ServerState.Connected or ServerState.Connecting or ServerState.Reconnecting;

        if (isConnectingOrConnected && !_serverManager.CurrentServer.FullPause)
        {
            _serverManager.CurrentServer.FullPause = true;
            _serverManager.Save();
        }
        else if (!isConnectingOrConnected && _serverManager.CurrentServer.FullPause)
        {
            _serverManager.CurrentServer.FullPause = false;
            _serverManager.Save();
        }

        _ = _apiController.CreateConnectionsAsync();
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
