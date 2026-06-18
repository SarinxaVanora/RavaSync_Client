using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.RavaCast;
using RavaSync.Services.RavaCast.Rendering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.UI;

public sealed class RavaCastUi : WindowMediatorSubscriberBase
{
    private const string DefaultScreenPlacementName = "Default";

    private readonly UiSharedService _uiShared;
    private readonly RavaCastService _ravaCast;
    private readonly RavaCastBrowserSurface _surface;
    private readonly MareConfigService _config;
    private readonly RavaCastBackendInstallerService _backendInstaller;

    private string _castName = "RavaCast";
    private string _screenName = "Everkeep Monitor";
    private string _url = string.Empty;
    private string _error = string.Empty;
    private bool _passwordProtected;
    private string _broadcastPassword = string.Empty;
    private readonly Dictionary<Guid, string> _joinPasswords = new();
    private string _firewallStatus = string.Empty;
    private readonly object _directStreamFirewallStatusLock = new();
    private bool _directStreamFirewallAllowed;
    private bool _directStreamFirewallCheckInFlight;
    private bool _directStreamFirewallStatusKnown;
    private long _lastDirectStreamFirewallCheckTick;
    private bool _webView2RuntimeInstallInFlight;
    private string _webView2RuntimeInstallStatus = string.Empty;
    private string _pageUrl = string.Empty;
    private string _pageUrlLastSynced = string.Empty;
    private Guid _pageUrlCastId;
    private bool _pageUrlDirty;
    private Vector3 _screenCentre = new(0, 0, 0);
    private float _screenWidth = 3.0f;
    private float _screenHeight = 1.70f;
    private float _screenYaw = 0.0f;
    private float _screenPitch = 0.0f;
    private string _selectedScreenPlacementName = DefaultScreenPlacementName;
    private string _saveScreenPlacementName = string.Empty;
    private bool _screenPickActive;
    private bool _screenFineTuneOpen = true;
    private bool _liveMoveScreenOpen;
    private bool _screenGizmoActive;
    private int _screenGizmoDragMode;
    private Vector2 _screenGizmoLastMouse;
    private string _screenPlacementStatus = string.Empty;
    private float _screenMoveStep = 0.10f;
    private long _lastLivePlaneUpdateTick;
    private float _volume = 0.5f;
    private RavaCastMode _mode = RavaCastMode.UrlShare;
    private RavaCastDirectStreamQuality _quality = RavaCastDirectStreamQuality.Normal720p30;
    private Vector2 _lastPreviewMove = new(-1f, -1f);
    private long _lastPreviewMoveTick;
    private bool _lastPreviewMouseInside;
    private int _lastBrowserHeldMask;
    private int _lastBrowserClickMask;
    private long _lastBrowserClickTick;
    private bool _browserPreviewFocused;
    private bool _keyboardFocusPending;
    private string _browserKeyboardCapture = string.Empty;
    private string _browserKeyboardCaptureLast = string.Empty;
    private bool _enterWasDown;
    private bool _tabWasDown;
    private bool _escapeWasDown;
    private bool _backspaceWasDown;
    private bool _deleteWasDown;
    private bool _leftWasDown;
    private bool _rightWasDown;
    private bool _upWasDown;
    private bool _downWasDown;
    private bool _homeWasDown;
    private bool _endWasDown;

    private bool _selectLobbyTab = true;
    private bool _selectCurrentTab = false;

    protected override IDisposable? BeginThemeScope() => _uiShared.BeginThemed();

    public RavaCastUi(ILogger<RavaCastUi> logger, MareMediator mediator, UiSharedService uiShared, RavaCastService ravaCast, RavaCastBrowserSurface surface,
        MareConfigService config, RavaCastBackendInstallerService backendInstaller, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync — RavaCast", performanceCollectorService)
    {
        _uiShared = uiShared;
        _ravaCast = ravaCast;
        _surface = surface;
        _config = config;
        _backendInstaller = backendInstaller;
        _volume = Math.Clamp(_config.Current.RavaCastDefaultVolume, 0.01f, 1f);
        _selectedScreenPlacementName = string.IsNullOrWhiteSpace(_config.Current.RavaCastSelectedScreenPlacementName)
            ? DefaultScreenPlacementName
            : _config.Current.RavaCastSelectedScreenPlacementName.Trim();

        RespectCloseHotkey = true;
        SizeConstraints = new()
        {
            MinimumSize = new(780 * ImGuiHelpers.GlobalScale, 520 * ImGuiHelpers.GlobalScale),
            MaximumSize = new(1250 * ImGuiHelpers.GlobalScale, 1100 * ImGuiHelpers.GlobalScale),
        };
    }

    public override void OnOpen()
    {
        base.OnOpen();
        _selectLobbyTab = true;
        ApplySelectedScreenPlacementOrDefault(showStatus: false);
    }

    public override void OnClose()
    {
        ClearBrowserPreviewFocus();
        _screenPickActive = false;
        _screenGizmoDragMode = 0;
        base.OnClose();
    }

    protected override void DrawInternal()
    {
        if (!_config.Current.SeenRavaCastIntro)
        {
            DrawIntro();
            return;
        }

        using (_uiShared.UidFont.Push())
            ImGui.TextUnformatted("RavaCast");

        var backend = _surface.BackendStatus;
        if (!backend.IsAvailable)
        {
            ImGui.SameLine();
            ImGui.TextColored(ImGuiColors.DalamudRed, $"  {FriendlyBackendHeader(backend)}");
        }

        ImGui.Separator();

        DrawBackendInstaller(backend);

        using var tabs = ImRaii.TabBar("ravacast_tabs");
        if (!tabs) return;

        using (var hostTab = ImRaii.TabItem("Host##ravacast_host"))
        {
            if (hostTab) DrawHost();
        }

        var lobbyFlags = _selectLobbyTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var lobbyTab = ImRaii.TabItem("Lobby##ravacast_lobby", lobbyFlags))
        {
            if (lobbyTab) DrawLobby();
        }

        var currentFlags = _selectCurrentTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var currentTab = ImRaii.TabItem("RavaCast##ravacast_current", currentFlags))
        {
            if (currentTab) DrawCurrent();
        }

        _selectLobbyTab = false;
        _selectCurrentTab = false;

        DrawScreenPickOverlay();
        DrawScreenGizmoOverlay();
    }

    private void DrawBackendInstaller(RavaCastBackendStatus backend)
    {
        var runtimeReady = !OperatingSystem.IsWindows() || _backendInstaller.TryGetInstalledWebView2RuntimeVersion(out var runtimeVersion);
        var missingDirectStreamFiles = _backendInstaller.MissingDirectStreamNativeFiles;
        var hasSetupIssue = !_backendInstaller.IsInstalled || !runtimeReady || missingDirectStreamFiles.Length > 0 || !string.IsNullOrWhiteSpace(_webView2RuntimeInstallStatus);
        if (!hasSetupIssue) return;

        DrawSection("Setup");

        if (!_backendInstaller.IsInstalled)
            ImGui.TextColored(ImGuiColors.DalamudRed, $"RavaCast.Renderer.exe is missing from the plugin folder: {_backendInstaller.InstallDirectory}");

        if (OperatingSystem.IsWindows() && !runtimeReady)
        {
            ImGui.TextColored(ImGuiColors.DalamudYellow, "Microsoft Edge WebView2 Evergreen Runtime is missing. URL Share and browser preview cannot render video without it.");
            using (ImRaii.Disabled(_webView2RuntimeInstallInFlight))
            {
                if (_uiShared.IconTextButton(FontAwesomeIcon.Download, _webView2RuntimeInstallInFlight ? "Installing WebView2..." : "Install WebView2 Runtime", 240 * ImGuiHelpers.GlobalScale, true))
                    StartWebView2RuntimeInstall();
            }
        }

        if (missingDirectStreamFiles.Length > 0)
        {
            ImGui.TextColored(ImGuiColors.DalamudYellow, "Direct Stream files are missing from this install: " + string.Join(", ", missingDirectStreamFiles));
            UiSharedService.TextWrapped("Reinstall or update RavaSync so the RavaCast media bridge, libdatachannel, FFmpeg, and OpenSSL runtime files are copied beside RavaSync.dll.");
        }

        if (!string.IsNullOrWhiteSpace(_webView2RuntimeInstallStatus))
            ImGui.TextColored(_webView2RuntimeInstallInFlight ? ImGuiColors.DalamudYellow : (runtimeReady ? ImGuiColors.HealerGreen : ImGuiColors.DalamudRed), _webView2RuntimeInstallStatus);

        ImGui.Separator();
    }

    private void StartWebView2RuntimeInstall()
    {
        if (_webView2RuntimeInstallInFlight) return;
        _webView2RuntimeInstallInFlight = true;
        _webView2RuntimeInstallStatus = "Installing WebView2 Evergreen Runtime...";

        _ = Task.Run(async () =>
        {
            try
            {
                var ok = await _backendInstaller.EnsureWebView2RuntimeReadyAsync(CancellationToken.None).ConfigureAwait(false);
                _webView2RuntimeInstallStatus = ok
                    ? (_backendInstaller.Detail ?? "WebView2 Evergreen Runtime is ready.")
                    : (_backendInstaller.Detail ?? "WebView2 Evergreen Runtime could not be installed.");
            }
            catch (Exception ex)
            {
                _webView2RuntimeInstallStatus = "WebView2 Evergreen Runtime install failed: " + ex.Message;
            }
            finally
            {
                _webView2RuntimeInstallInFlight = false;
            }
        });
    }

    private void DrawIntro()
    {
        _uiShared.BigText("Welcome to RavaCast", ImGuiColors.DalamudViolet);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(6);

        UiSharedService.TextWrapped("RavaCast lets nearby RavaSync users host shared screens in-world.");
        UiSharedService.TextWrapped("URL Share is the simple mode: each viewer opens the same public page on their own PC.");
        UiSharedService.TextWrapped("Direct Stream is for host-controlled watch parties: viewers receive the host's already-loaded RavaCast output, including audio, without opening or controlling the browser themselves.");
        UiSharedService.TextWrapped("RavaCast does not send, upload, or share local media files. URL Share still needs a public web URL, not file://, localhost, or private-network links. Your volume and mute controls only affect your own client.");

        ImGuiHelpers.ScaledDummy(12);
        ImGui.TextUnformatted("Initial volume");
        var introVolumePercent = Math.Clamp(_volume * 100f, 1f, 100f);
        ImGui.SetNextItemWidth(320 * ImGuiHelpers.GlobalScale);
        if (ImGui.SliderFloat("##ravacast_intro_volume", ref introVolumePercent, 1f, 100f, "%.0f%%"))
            _volume = Math.Clamp(introVolumePercent / 100f, 0.01f, 1f);

        ImGuiHelpers.ScaledDummy(12);
        DrawBackendInstaller(_surface.BackendStatus);

        if (_uiShared.IconTextButton(FontAwesomeIcon.Check, "Save and Open RavaCast", 230 * ImGuiHelpers.GlobalScale, true))
        {
            _config.Current.RavaCastDefaultVolume = _volume;
            _config.Current.SeenRavaCastIntro = true;
            _config.Save();
        }
    }

    private void DrawHost()
    {
        var current = _ravaCast.GetCurrentSession();
        var hosting = current?.IsOwner == true;

        DrawSection("Broadcast");

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Cast name");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
        ImGui.InputText("##ravacast_name", ref _castName, 64);

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("URL");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(520 * ImGuiHelpers.GlobalScale);
        ImGui.InputText("##ravacast_url", ref _url, 1024);
        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Sync, hosting ? "Change URL" : "Validate", 130 * ImGuiHelpers.GlobalScale, true))
        {
            if (hosting)
            {
                if (!_ravaCast.UpdateHostedUrl(_url, out _error)) { }
            }
            else
            {
                if (!RavaCastService.TryValidatePublicWebUrl(_url, out _, out _error)) { }
                else _error = "URL looks valid.";
            }
        }

        var requirePassword = hosting && current is not null ? current.PasswordProtected : _passwordProtected;
        if (!hosting)
        {
            ImGui.AlignTextToFramePadding();
            if (ImGui.Checkbox("Require password##ravacast_host_password_enabled", ref _passwordProtected) && !_passwordProtected)
                _broadcastPassword = string.Empty;
            UiSharedService.AttachToolTip("Optional lobby password. Viewers will need this before they can join the broadcast.");

            if (_passwordProtected)
            {
                ImGui.SameLine();
                ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
                ImGui.InputTextWithHint("##ravacast_host_password", "Password", ref _broadcastPassword, 80, ImGuiInputTextFlags.Password);
            }
        }
        else if (requirePassword)
        {
            ImGui.TextColored(ImGuiColors.DalamudYellow, "This lobby is password protected.");
        }

        if (!string.IsNullOrWhiteSpace(_error))
            ImGui.TextColored(string.Equals(_error, "URL looks valid.", StringComparison.Ordinal) ? ImGuiColors.HealerGreen : ImGuiColors.DalamudRed, _error);

        ImGuiHelpers.ScaledDummy(8);
        DrawModeControls(hosting, current);

        ImGuiHelpers.ScaledDummy(8);
        DrawScreenPlacementEditor(hosting, current, live: false);

        ImGuiHelpers.ScaledDummy(8);

        if (!hosting)
        {
            var backendReady = _surface.BackendStatus.IsAvailable;
            if (!backendReady)
                ImGui.TextColored(ImGuiColors.DalamudRed, "RavaCast.Renderer.exe is missing from the plugin folder.");

            using (ImRaii.Disabled(!backendReady || (_passwordProtected && string.IsNullOrWhiteSpace(_broadcastPassword))))
            {
                if (_uiShared.IconTextButton(FontAwesomeIcon.PlayCircle, "Start Broadcast", 200 * ImGuiHelpers.GlobalScale, true))
                {
                    var broadcastPassword = _passwordProtected ? _broadcastPassword : null;
                    if (_ravaCast.TryStartBroadcast(_castName, _url, CurrentPlane(), _mode, _quality, broadcastPassword, out _error))
                    {
                        _screenPickActive = false;
                        _screenGizmoActive = false;
                        _screenGizmoDragMode = 0;
                        _liveMoveScreenOpen = false;
                        _selectCurrentTab = true;
                        _error = string.Empty;
                    }
                }
            }
            return;
        }

        DrawSection("Owner controls");
        DrawOwnerControls(current!);
    }


    private void DrawModeControls(bool hosting, RavaCastSessionView? current)
    {
        DrawSection("Mode");
        var mode = hosting && current is not null ? current.Mode : _mode;
        var urlShare = mode == RavaCastMode.UrlShare;
        var directStream = mode == RavaCastMode.DirectStream;

        if (ImGui.RadioButton("URL Share##ravacast_mode_url", urlShare) && !urlShare)
        {
            _mode = RavaCastMode.UrlShare;
            if (hosting) _ravaCast.SetHostedMode(_mode);
        }
        ImGui.SameLine();
        if (ImGui.RadioButton("Direct Stream##ravacast_mode_direct", directStream) && !directStream)
        {
            _mode = RavaCastMode.DirectStream;
            if (hosting) _ravaCast.SetHostedMode(_mode);
        }

        if ((hosting && current?.Mode == RavaCastMode.DirectStream) || (!hosting && _mode == RavaCastMode.DirectStream))
        {
            var quality = hosting && current is not null ? current.DirectStreamQuality : _quality;
            ImGuiHelpers.ScaledDummy(4);
            ImGui.TextUnformatted("Quality");
            DrawQualityRadio("Low 720p30", RavaCastDirectStreamQuality.Low720p30, quality, hosting);
            ImGui.SameLine();
            DrawQualityRadio("Normal 720p30", RavaCastDirectStreamQuality.Normal720p30, quality, hosting);
            ImGui.SameLine();
            DrawQualityRadio("Smooth 720p60", RavaCastDirectStreamQuality.Smooth720p60, quality, hosting);
            ImGuiHelpers.ScaledDummy(4);
            DrawDirectStreamFirewallPrompt();
        }
    }

    private void DrawDirectStreamFirewallPrompt()
    {
        if (!OperatingSystem.IsWindows())
            return;

        RefreshDirectStreamFirewallStatus(force: false);

        // If the rule is already present, keep this area silent. The button is only useful
        // when Windows actually needs the Direct Stream bridge to be allowed.
        if (_directStreamFirewallAllowed || _directStreamFirewallCheckInFlight || !_directStreamFirewallStatusKnown)
            return;

        using (ImRaii.Disabled(!_backendInstaller.IsNativeBridgeInstalled))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.UserShield, "Allow Direct Stream through firewall", 300 * ImGuiHelpers.GlobalScale, true))
            {
                if (_backendInstaller.TryRequestDirectStreamFirewallPermission(out var firewallMessage))
                {
                    _firewallStatus = firewallMessage;
                    _directStreamFirewallStatusKnown = false;
                    RefreshDirectStreamFirewallStatus(force: true);
                }
                else
                {
                    _firewallStatus = firewallMessage;
                }
            }
        }

        if (!_backendInstaller.IsNativeBridgeInstalled)
            ImGui.TextColored(ImGuiColors.DalamudYellow, "Direct Stream files need to be installed first.");
        else if (!string.IsNullOrWhiteSpace(_firewallStatus))
            ImGui.TextColored(ImGuiColors.DalamudYellow, _firewallStatus);
    }

    private void RefreshDirectStreamFirewallStatus(bool force)
    {
        var now = Environment.TickCount64;
        lock (_directStreamFirewallStatusLock)
        {
            // Never run netsh/firewall probing on the ImGui/game draw thread. The old synchronous check
            // launched two netsh.exe processes every couple of seconds while the Direct Stream panel/status
            // was visible, which is exactly the kind of small periodic hitch the game should never feel.
            // Once the rules have been confirmed for this install, keep that known-good result for the
            // remainder of this plugin session. Re-checking a working firewall setup only risks hitches.
            if (_directStreamFirewallAllowed) return;
            if (_directStreamFirewallCheckInFlight) return;
            if (!force && now - _lastDirectStreamFirewallCheckTick < 30000) return;
            _lastDirectStreamFirewallCheckTick = now;
            _directStreamFirewallCheckInFlight = true;
            if (force)
                _directStreamFirewallStatusKnown = false;
        }

        _ = Task.Run(() =>
        {
            bool allowed;
            string detail;
            try
            {
                allowed = _backendInstaller.IsDirectStreamFirewallAllowed(out detail);
            }
            catch (Exception ex)
            {
                allowed = false;
                detail = "Could not query Direct Stream firewall status: " + ex.Message;
            }

            lock (_directStreamFirewallStatusLock)
            {
                _directStreamFirewallAllowed = allowed;
                _directStreamFirewallStatusKnown = true;
                _firewallStatus = allowed
                    ? string.Empty
                    : string.IsNullOrWhiteSpace(detail) ? "Direct Stream firewall status could not be confirmed." : detail;
                _directStreamFirewallCheckInFlight = false;
            }
        });
    }

    private void DrawQualityRadio(string label, RavaCastDirectStreamQuality value, RavaCastDirectStreamQuality current, bool hosting)
    {
        if (ImGui.RadioButton($"{label}##ravacast_quality_{value}", current == value) && current != value)
        {
            _quality = value;
            if (hosting) _ravaCast.SetHostedDirectStreamQuality(value);
        }
    }

    private static string FormatMode(RavaCastMode mode)
        => mode == RavaCastMode.DirectStream ? "Direct Stream" : "URL Share";

    private static string FriendlyBackendHeader(RavaCastBackendStatus backend)
    {
        if (!backend.IsAvailable) return $"RavaCast video: {backend.StatusText}";
        return $"{backend.BackendName}: {backend.StatusText}";
    }

    private static string FriendlyCurrentBackendLine(RavaCastSessionView current, RavaCastBackendStatus backend)
    {
        if (current.Mode == RavaCastMode.DirectStream && !current.IsOwner)
            return $"Video: Direct Stream - {backend.StatusText}";
        if (current.Mode == RavaCastMode.DirectStream && current.IsOwner)
            return $"Video: Host preview - {backend.StatusText}";
        return $"Video: {backend.BackendName} - {backend.StatusText}";
    }

    private static string FriendlyDirectStreamStatus(RavaCastSessionView current)
    {
        var raw = current.DirectStreamStatus ?? string.Empty;
        var detail = current.DirectStreamDetail ?? string.Empty;
        var combined = raw + " " + detail;

        if (combined.Contains("host preview has produced", StringComparison.OrdinalIgnoreCase)
            || combined.Contains("source shared D3D texture", StringComparison.OrdinalIgnoreCase)
            || combined.Contains("source shared texture", StringComparison.OrdinalIgnoreCase))
            return "Waiting for the host preview";

        if (!current.DirectStreamNativeMediaAvailable
            && (combined.Contains("media bridge files", StringComparison.OrdinalIgnoreCase)
                || combined.Contains("RavaCast.Media", StringComparison.OrdinalIgnoreCase)
                || combined.Contains("BridgeHost", StringComparison.OrdinalIgnoreCase)
                || combined.Contains("files are missing", StringComparison.OrdinalIgnoreCase)))
            return "Direct Stream files are missing from this install";

        if (raw.Contains("error", StringComparison.OrdinalIgnoreCase) || raw.Contains("failed", StringComparison.OrdinalIgnoreCase) || raw.Contains("closed", StringComparison.OrdinalIgnoreCase) || raw.Contains("exited", StringComparison.OrdinalIgnoreCase) || raw.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            return "Direct Stream needs attention";
        if (raw.Contains("remote audio playing", StringComparison.OrdinalIgnoreCase))
            return current.IsOwner ? "Streaming to viewers" : "Audio connected";
        if (raw.Contains("connected", StringComparison.OrdinalIgnoreCase) || raw.Contains("decoded texture", StringComparison.OrdinalIgnoreCase))
            return current.IsOwner ? "Streaming to viewers" : "Host video connected";
        if (raw.Contains("libdatachannel", StringComparison.OrdinalIgnoreCase) || raw.Contains("ICE", StringComparison.OrdinalIgnoreCase) || raw.Contains("SDP", StringComparison.OrdinalIgnoreCase) || raw.Contains("transport", StringComparison.OrdinalIgnoreCase) || raw.Contains("bitrate", StringComparison.OrdinalIgnoreCase))
            return current.IsOwner ? "Starting Direct Stream" : "Connecting to host video";
        if (raw.Contains("waiting for viewers", StringComparison.OrdinalIgnoreCase))
            return "Ready — waiting for viewers";
        if (raw.Contains("viewer", StringComparison.OrdinalIgnoreCase) && (raw.Contains("ready", StringComparison.OrdinalIgnoreCase) || raw.Contains("connected", StringComparison.OrdinalIgnoreCase) || raw.Contains("added", StringComparison.OrdinalIgnoreCase)))
            return current.IsOwner ? "Viewer connected" : "Connecting to host video";
        if (raw.Contains("receiver", StringComparison.OrdinalIgnoreCase) || raw.Contains("offer", StringComparison.OrdinalIgnoreCase) || raw.Contains("requested", StringComparison.OrdinalIgnoreCase) || raw.Contains("starting", StringComparison.OrdinalIgnoreCase))
            return current.IsOwner ? "Starting Direct Stream" : "Connecting to host video";
        if (string.IsNullOrWhiteSpace(raw))
            return current.IsOwner ? "Direct Stream ready" : "Waiting for host video";
        return raw.Replace("peers", "viewers", StringComparison.OrdinalIgnoreCase).Replace("peer", "viewer", StringComparison.OrdinalIgnoreCase);
    }

    private static string? FriendlyDirectStreamDetail(string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail)) return null;
        if (detail.Contains("host preview has produced", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("source shared D3D texture", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("source shared texture", StringComparison.OrdinalIgnoreCase))
            return "Wait for the preview image to appear, then start Direct Stream again.";
        if (detail.Contains("Offer/answer:", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("ICE local=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("tracks=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("received=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("decoded=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("shown=", StringComparison.OrdinalIgnoreCase))
            return detail.Replace("peers", "viewers", StringComparison.OrdinalIgnoreCase).Replace("peer", "viewer", StringComparison.OrdinalIgnoreCase);
        if (detail.Contains("libdatachannel", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("FFmpeg", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Opus", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("codecPath", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("transport=", StringComparison.OrdinalIgnoreCase))
            return null;
        if (detail.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("error", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("missing", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("cannot", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("closed", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("exited", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            return detail.Replace("peers", "viewers", StringComparison.OrdinalIgnoreCase).Replace("peer", "viewer", StringComparison.OrdinalIgnoreCase);
        return null;
    }

    private void DrawDirectStreamCurrentStatus(RavaCastSessionView current)
    {
        if (current.Mode == RavaCastMode.DirectStream)
        {
            var preset = RavaCastDirectStreamPresets.Get(current.DirectStreamQuality);
            ImGui.TextUnformatted($"Mode: Direct Stream - {preset.Label}");

            if (current.IsOwner)
                ImGui.TextUnformatted($"Viewers: {Math.Max(0, current.JoinedCount)}");

            var friendlyStatus = FriendlyDirectStreamStatus(current);
            if (friendlyStatus.Contains("attention", StringComparison.OrdinalIgnoreCase)
                || friendlyStatus.Contains("missing", StringComparison.OrdinalIgnoreCase)
                || friendlyStatus.Contains("Waiting", StringComparison.OrdinalIgnoreCase)
                || friendlyStatus.Contains("Connecting", StringComparison.OrdinalIgnoreCase))
            {
                var colour = current.DirectStreamNativeMediaAvailable ? ImGuiColors.DalamudGrey : ImGuiColors.DalamudYellow;
                ImGui.TextColored(colour, friendlyStatus);
            }

            return;
        }

        ImGui.TextUnformatted($"Mode: {FormatMode(current.Mode)}");
    }

    private void DrawLobby()
    {
        DrawSection("Active casts nearby");

        var backendReady = _surface.BackendStatus.IsAvailable;
        if (!backendReady)
            ImGui.TextColored(ImGuiColors.DalamudRed, "RavaCast.Renderer.exe is missing from the plugin folder.");

        var casts = _ravaCast.GetActiveCasts();
        if (casts.Count == 0)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No RavaCast broadcasts nearby right now.");
            return;
        }

        foreach (var cast in casts)
        {
            using (ImRaii.PushId(cast.CastId.ToString()))
            {
                ImGui.TextColored(ImGuiColors.DalamudViolet, string.IsNullOrWhiteSpace(cast.CastName) ? "RavaCast" : cast.CastName);
                ImGui.SameLine();
                ImGui.TextColored(ImGuiColors.DalamudGrey, $"by {cast.HostName}");
                ImGui.TextUnformatted($"Source: {cast.SourceDomain}");
                ImGui.TextUnformatted($"Mode: {FormatMode(cast.Mode)}");
                ImGui.TextUnformatted($"Screen: {cast.Plane.ScreenName}");
                if (cast.PasswordProtected)
                {
                    ImGui.TextColored(ImGuiColors.DalamudYellow, "Password protected");
                    var password = _joinPasswords.TryGetValue(cast.CastId, out var savedPassword) ? savedPassword : string.Empty;
                    ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
                    if (ImGui.InputTextWithHint("##ravacast_join_password", "Password", ref password, 80, ImGuiInputTextFlags.Password))
                        _joinPasswords[cast.CastId] = password;
                }

                using (ImRaii.Disabled(!backendReady || (cast.PasswordProtected && string.IsNullOrWhiteSpace(_joinPasswords.GetValueOrDefault(cast.CastId, string.Empty)))))
                {
                    var joinPassword = _joinPasswords.GetValueOrDefault(cast.CastId, string.Empty);
                    if (_uiShared.IconTextButton(FontAwesomeIcon.SignInAlt, "Join", 110 * ImGuiHelpers.GlobalScale, true))
                    {
                        _ravaCast.Join(cast.CastId, muted: false, joinPassword);
                        _selectCurrentTab = true;
                    }
                    ImGui.SameLine();
                    if (_uiShared.IconTextButton(FontAwesomeIcon.VolumeMute, "Join Muted", 150 * ImGuiHelpers.GlobalScale, true))
                    {
                        _ravaCast.Join(cast.CastId, muted: true, joinPassword);
                        _selectCurrentTab = true;
                    }
                }

                ImGui.Separator();
            }
        }
    }

    private void DrawCurrent()
    {
        var current = _ravaCast.GetCurrentSession();
        if (current is null)
        {
            ClearBrowserPreviewFocus();
            ImGui.TextColored(ImGuiColors.DalamudGrey, "You are not hosting or watching a RavaCast right now.");
            return;
        }

        DrawSection(current.IsOwner ? "Hosting" : "Watching");
        ImGui.TextUnformatted($"Cast: {current.CastName}");
        ImGui.TextUnformatted($"Source: {current.SourceDomain}");
        ImGui.TextUnformatted($"Screen: {current.Plane.ScreenName}");
        if (current.PasswordProtected)
            ImGui.TextColored(ImGuiColors.DalamudYellow, "Lobby password: enabled");
        if (current.Mode != RavaCastMode.DirectStream)
            ImGui.TextUnformatted($"Viewers: {current.JoinedCount}");
        DrawDirectStreamCurrentStatus(current);
        if (current.Mode == RavaCastMode.DirectStream)
        {
            ImGuiHelpers.ScaledDummy(4);
            DrawDirectStreamFirewallPrompt();
        }

        ImGuiHelpers.ScaledDummy(8);
        DrawSection("Audio");
        var muted = current.IsMuted;
        if (_uiShared.IconTextButton(muted ? FontAwesomeIcon.VolumeMute : FontAwesomeIcon.VolumeUp, muted ? "Unmute" : "Mute", 120 * ImGuiHelpers.GlobalScale, true))
            _ravaCast.SetLocalMuted(!muted);

        ImGui.SameLine();
        var volumePercent = Math.Clamp(current.Volume * 100f, 1f, 100f);
        ImGui.SetNextItemWidth(250 * ImGuiHelpers.GlobalScale);
        if (ImGui.SliderFloat("Volume##ravacast_current_volume", ref volumePercent, 1f, 100f, "%.0f%%"))
            _ravaCast.SetLocalVolume(volumePercent / 100f);
        if (ImGui.IsItemDeactivatedAfterEdit())
            _ravaCast.PersistLocalVolume(volumePercent / 100f);

        ImGuiHelpers.ScaledDummy(8);
        if (current.IsOwner)
        {
            DrawOwnerControls(current);
            ImGui.SameLine();
            if (_uiShared.IconTextButton(FontAwesomeIcon.ArrowsAltV, _liveMoveScreenOpen ? "Done Moving" : "Move Screen", 150 * ImGuiHelpers.GlobalScale, true))
            {
                _liveMoveScreenOpen = !_liveMoveScreenOpen;
                _screenGizmoActive = _liveMoveScreenOpen;
                _screenGizmoDragMode = 0;
                _ravaCast.SetWorldBrowserInputSuspended(_liveMoveScreenOpen);
                ClearBrowserPreviewFocus();
                if (_liveMoveScreenOpen)
                    LoadPlaneIntoEditor(current.Plane);
            }

            if (_liveMoveScreenOpen)
            {
                ImGuiHelpers.ScaledDummy(8);
                DrawScreenPlacementEditor(hosting: true, current, live: true);
            }

            ImGuiHelpers.ScaledDummy(8);
            DrawPageControls(current);
        }
        else
        {
            if (current.Mode == RavaCastMode.DirectStream)
                ClearBrowserPreviewFocus();

            if (_uiShared.IconTextButton(FontAwesomeIcon.Sync, "Resync", 120 * ImGuiHelpers.GlobalScale, true))
                _ravaCast.RequestState();
            ImGui.SameLine();
            if (_uiShared.IconTextButton(FontAwesomeIcon.SignOutAlt, "Leave Cast", 150 * ImGuiHelpers.GlobalScale, true))
            {
                ClearBrowserPreviewFocus();
                _ravaCast.Leave();
            }

            if (current.Mode != RavaCastMode.DirectStream)
            {
                ImGuiHelpers.ScaledDummy(8);
                DrawPageControls(current);
            }
        }
    }

    private void ClearBrowserPreviewFocus()
    {
        var heldMask = _lastBrowserHeldMask;
        if (_lastPreviewMouseInside || heldMask != 0)
        {
            var x = Math.Clamp(_lastPreviewMove.X < 0f ? 0.5f : _lastPreviewMove.X, 0f, 1f);
            var y = Math.Clamp(_lastPreviewMove.Y < 0f ? 0.5f : _lastPreviewMove.Y, 0f, 1f);
            _surface.SendBrowserMouse(x, y, 0, heldMask, 0, 0, 0f, 0f, leaving: true, shift: false, ctrl: false, alt: false);
        }

        if (_browserPreviewFocused)
            _surface.SendBrowserFocus(false);
        _browserPreviewFocused = false;
        _browserKeyboardCapture = string.Empty;
        _browserKeyboardCaptureLast = string.Empty;
        ResetBrowserKeyState();
    }

    private void DrawRavaCastBrowserCompatibilityControls()
    {
        if (_surface.BrowserWindowVisible)
            _surface.HideInteractiveWindow();

        var disableHardwareAcceleration = _config.Current.RavaCastDisableHardwareAcceleration;
        if (ImGui.Checkbox("Disable Hardware Acceleration##ravacast_disable_hw_accel", ref disableHardwareAcceleration))
        {
            _config.Current.RavaCastDisableHardwareAcceleration = disableHardwareAcceleration;
            _config.Save();
            ClearBrowserPreviewFocus();
            _surface.HideInteractiveWindow();
            _ravaCast.ReloadSurfaceForBrowserSettingsChange();
            _selectCurrentTab = true;
        }
        UiSharedService.AttachToolTip("Restarts the RavaCast browser with hardware acceleration disabled. Leave this ticked if video playback or capture is unstable.");
        ImGuiHelpers.ScaledDummy(2);
        ImGui.TextColored(ImGuiColors.DalamudGrey, "Leave this ticked for best media compatibility.");

        var darkMode = _config.Current.RavaCastBrowserDarkMode;
        ImGuiHelpers.ScaledDummy(4);
        ImGui.TextUnformatted("Dark Mode:");
        ImGui.SameLine();
        if (DrawOnOffSlider("##ravacast_browser_dark_mode", ref darkMode))
        {
            _config.Current.RavaCastBrowserDarkMode = darkMode;
            _config.Save();
            ClearBrowserPreviewFocus();
            _surface.HideInteractiveWindow();
            _ravaCast.ReloadSurfaceForBrowserSettingsChange();
            _selectCurrentTab = true;
        }
        UiSharedService.AttachToolTip("Starts the RavaCast browser in dark mode. Turning this off reloads the browser in normal mode.");
        ImGuiHelpers.ScaledDummy(2);
        ImGui.TextColored(ImGuiColors.DalamudGrey, "Starts dark by default. Turn it off to reload the browser in normal mode.");
    }

    private static bool DrawOnOffSlider(string id, ref bool value)
    {
        var scale = ImGuiHelpers.GlobalScale;
        var height = ImGui.GetFrameHeight();
        var width = height * 1.85f;
        var radius = height * 0.5f;
        var pos = ImGui.GetCursorScreenPos();

        ImGui.InvisibleButton(id, new Vector2(width, height), ImGuiButtonFlags.MouseButtonLeft);
        var clicked = ImGui.IsItemClicked(ImGuiMouseButton.Left);
        if (clicked)
            value = !value;

        var draw = ImGui.GetWindowDrawList();
        var hovered = ImGui.IsItemHovered();
        var bg = value
            ? ImGui.GetColorU32(hovered ? new Vector4(0.36f, 0.74f, 0.45f, 1f) : new Vector4(0.22f, 0.62f, 0.34f, 1f))
            : ImGui.GetColorU32(hovered ? new Vector4(0.42f, 0.42f, 0.46f, 1f) : new Vector4(0.28f, 0.28f, 0.32f, 1f));
        var knob = ImGui.GetColorU32(new Vector4(0.95f, 0.95f, 0.96f, 1f));
        var shadow = ImGui.GetColorU32(new Vector4(0f, 0f, 0f, 0.35f));

        draw.AddRectFilled(pos, pos + new Vector2(width, height), bg, radius);
        var knobRadius = radius - (2.5f * scale);
        var knobX = value ? pos.X + width - radius : pos.X + radius;
        var knobCentre = new Vector2(knobX, pos.Y + radius);
        draw.AddCircleFilled(knobCentre + new Vector2(0f, 1.5f * scale), knobRadius, shadow, 24);
        draw.AddCircleFilled(knobCentre, knobRadius, knob, 24);

        ImGui.SameLine();
        ImGui.TextUnformatted(value ? "On" : "Off");
        return clicked;
    }

    private void DrawPageControls(RavaCastSessionView current)
    {
        EnsurePageUrlState(current);

        DrawSection("RavaCast screen");
        DrawRavaCastBrowserCompatibilityControls();

        if (current.Mode == RavaCastMode.DirectStream && !current.IsOwner)
        {
            UiSharedService.TextWrapped("Direct Stream viewers receive the host's rendered RavaCast screen. Browser navigation stays with the host.");
        }
        else
        {
            UiSharedService.TextWrapped("Use these controls to navigate the RavaCast browser.");

            if (ImGui.Button("Back##ravacast_current_back", new Vector2(70 * ImGuiHelpers.GlobalScale, 0)))
            {
                ClearBrowserPreviewFocus();
                _ravaCast.GoBackCurrentBrowser();
            }
            ImGui.SameLine();
            if (ImGui.Button("Forward##ravacast_current_forward", new Vector2(82 * ImGuiHelpers.GlobalScale, 0)))
            {
                ClearBrowserPreviewFocus();
                _ravaCast.GoForwardCurrentBrowser();
            }
            ImGui.SameLine();
            if (ImGui.Button("Reload##ravacast_current_reload", new Vector2(78 * ImGuiHelpers.GlobalScale, 0)))
            {
                ClearBrowserPreviewFocus();
                _ravaCast.ReloadCurrentBrowser();
            }

            ImGuiHelpers.ScaledDummy(4);
            var beforeUrl = _pageUrl;
            ImGui.SetNextItemWidth(Math.Max(220f * ImGuiHelpers.GlobalScale, ImGui.GetContentRegionAvail().X - 62f * ImGuiHelpers.GlobalScale));
            var goPressed = ImGui.InputTextWithHint("##ravacast_current_url", "URL, domain, or search", ref _pageUrl, 1024, ImGuiInputTextFlags.EnterReturnsTrue);
            if (!string.Equals(beforeUrl, _pageUrl, StringComparison.Ordinal))
                _pageUrlDirty = true;
            ImGui.SameLine();
            if (ImGui.Button("Go##ravacast_current_go", new Vector2(54 * ImGuiHelpers.GlobalScale, 0)) || goPressed)
            {
                ClearBrowserPreviewFocus();
                NavigateHostedBrowserFromUrlBar();
            }

            if (!string.IsNullOrWhiteSpace(_surface.CurrentUrl))
                ImGui.TextColored(ImGuiColors.DalamudGrey, "Current browser URL: " + _surface.CurrentUrl);
        }

        if (!string.IsNullOrWhiteSpace(_error) && current.Mode != RavaCastMode.DirectStream)
            ImGui.TextColored(ImGuiColors.DalamudRed, _error);

        var backend = _surface.BackendStatus;
        var colour = backend.IsAvailable && backend.HasRenderableTexture ? ImGuiColors.HealerGreen : ImGuiColors.DalamudYellow;
        ImGui.TextColored(colour, FriendlyCurrentBackendLine(current, backend));
        if (!string.IsNullOrWhiteSpace(backend.Detail))
            UiSharedService.TextWrapped(backend.Detail);
    }

    private void NavigateHostedBrowserFromUrlBar()
    {
        var target = NormaliseBrowserNavigationText(_pageUrl);
        if (string.IsNullOrWhiteSpace(target)) return;
        _pageUrl = target;
        if (_ravaCast.NavigateCurrentBrowserFromText(target, out _error))
        {
            _pageUrlLastSynced = target;
            _pageUrlDirty = false;
            _selectCurrentTab = true;
            _error = string.Empty;
        }
    }

    private static string NormaliseBrowserNavigationText(string value)
    {
        var text = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text)) return string.Empty;
        if (Uri.TryCreate(text, UriKind.Absolute, out var absolute) && !string.IsNullOrWhiteSpace(absolute.Scheme))
            return absolute.ToString();
        if (text.Contains(' ') || !text.Contains('.'))
            return "https://www.google.com/search?q=" + Uri.EscapeDataString(text);
        return "https://" + text.TrimStart('/');
    }

    private void EnsurePageUrlState(RavaCastSessionView current)
    {
        if (_pageUrlCastId != current.CastId)
        {
            _pageUrlCastId = current.CastId;
            _pageUrl = current.Url;
            _pageUrlLastSynced = current.Url;
            _pageUrlDirty = false;
            return;
        }

        if (!_pageUrlDirty && !string.Equals(_pageUrlLastSynced, current.Url, StringComparison.Ordinal))
        {
            _pageUrl = current.Url;
            _pageUrlLastSynced = current.Url;
        }
    }

    private void DrawCenteredInteractiveBrowserPreview(RavaCastTextureFrame frame, Vector2 containerSize, bool inputEnabled)
    {
        var start = ImGui.GetCursorScreenPos();
        var imageSize = CalculateContainedPreviewSize(frame, containerSize);
        var offset = new Vector2(
            Math.Max(0f, (containerSize.X - imageSize.X) * 0.5f),
            Math.Max(0f, (containerSize.Y - imageSize.Y) * 0.5f));

        ImGui.SetCursorScreenPos(start + offset);
        DrawInteractiveBrowserPreview(frame, imageSize, inputEnabled);
        ImGui.SetCursorScreenPos(new Vector2(start.X, start.Y + containerSize.Y));
    }

    private static Vector2 CalculateContainedPreviewSize(RavaCastTextureFrame frame, Vector2 containerSize)
    {
        var frameWidth = Math.Max(1, frame.Width);
        var frameHeight = Math.Max(1, frame.Height);
        var frameAspect = frameWidth / (float)frameHeight;

        var width = Math.Max(1f, containerSize.X);
        var height = width / frameAspect;
        if (height > containerSize.Y)
        {
            height = Math.Max(1f, containerSize.Y);
            width = height * frameAspect;
        }

        return new Vector2(Math.Max(1f, width), Math.Max(1f, height));
    }

    private void DrawInteractiveBrowserPreview(RavaCastTextureFrame frame, Vector2 previewSize, bool inputEnabled)
    {
        var previewPos = ImGui.GetCursorScreenPos();
        var previewMax = previewPos + previewSize;
        ImGui.GetWindowDrawList().AddImage(frame.TextureId, previewPos, previewMax);
        ImGui.InvisibleButton("##ravacast_browser_surface", previewSize, ImGuiButtonFlags.MouseButtonLeft | ImGuiButtonFlags.MouseButtonRight | ImGuiButtonFlags.MouseButtonMiddle);
        if (!inputEnabled)
        {
            ClearBrowserPreviewFocus();
            return;
        }

        var io = ImGui.GetIO();
        var mouse = ImGui.GetMousePos();
        var mouseInPreview = mouse.X >= previewPos.X && mouse.X <= previewMax.X && mouse.Y >= previewPos.Y && mouse.Y <= previewMax.Y;
        var anyPreviewClick = mouseInPreview && (ImGui.IsMouseClicked(ImGuiMouseButton.Left) || ImGui.IsMouseClicked(ImGuiMouseButton.Right) || ImGui.IsMouseClicked(ImGuiMouseButton.Middle));

        if (anyPreviewClick)
        {
            if (!_browserPreviewFocused)
            {
                _browserKeyboardCapture = string.Empty;
                _browserKeyboardCaptureLast = string.Empty;
                _surface.SendBrowserFocus(true);
            }
            _browserPreviewFocused = true;
            _keyboardFocusPending = true;
        }
        else if (!mouseInPreview && ImGui.IsMouseClicked(ImGuiMouseButton.Left))
        {
            ClearBrowserPreviewFocus();
        }

        var normalised = new Vector2(
            Math.Clamp((mouse.X - previewPos.X) / Math.Max(1f, previewSize.X), 0f, 1f),
            Math.Clamp((mouse.Y - previewPos.Y) / Math.Max(1f, previewSize.Y), 0f, 1f));

        if (!mouseInPreview && _lastPreviewMouseInside)
            _surface.SendBrowserMouse(normalised.X, normalised.Y, 0, 0, _lastBrowserHeldMask, 0, 0f, 0f, leaving: true, shift: io.KeyShift, ctrl: io.KeyCtrl, alt: io.KeyAlt);
        _lastPreviewMouseInside = mouseInPreview;

        if (!mouseInPreview && !_browserPreviewFocused) return;

        var heldMask = BuildMouseMask(ImGui.IsMouseDown(ImGuiMouseButton.Left), ImGui.IsMouseDown(ImGuiMouseButton.Right), ImGui.IsMouseDown(ImGuiMouseButton.Middle));
        var downMask = BuildMouseMask(ImGui.IsMouseClicked(ImGuiMouseButton.Left), ImGui.IsMouseClicked(ImGuiMouseButton.Right), ImGui.IsMouseClicked(ImGuiMouseButton.Middle));
        var upMask = BuildMouseMask(ImGui.IsMouseReleased(ImGuiMouseButton.Left), ImGui.IsMouseReleased(ImGuiMouseButton.Right), ImGui.IsMouseReleased(ImGuiMouseButton.Middle));
        var wheelY = mouseInPreview ? io.MouseWheel : 0f;
        var wheelX = mouseInPreview ? io.MouseWheelH : 0f;

        var now = Environment.TickCount64;
        var doubleMask = 0;
        if (downMask != 0 && _lastBrowserClickMask == downMask && now - _lastBrowserClickTick <= 450)
            doubleMask = downMask;
        if (downMask != 0)
        {
            _lastBrowserClickMask = downMask;
            _lastBrowserClickTick = now;
        }

        var moved = mouseInPreview && Vector2.DistanceSquared(normalised, _lastPreviewMove) > 0.0000005f && now - _lastPreviewMoveTick >= 4;
        var buttonChanged = downMask != 0 || upMask != 0 || heldMask != _lastBrowserHeldMask;
        var wheeled = Math.Abs(wheelY) > 0.01f || Math.Abs(wheelX) > 0.01f;

        // The preview must behave like a real browser surface: hover state, video controls, scrubbers,
        // tooltips and cursor-sensitive page UI all need mouseMoved packets while the pointer is inside
        // the preview.  The system cursor flicker is handled in the renderer capture path instead;
        // starving WebView2 of move events makes the preview feel broken.
        if (mouseInPreview && (moved || buttonChanged || wheeled))
        {
            _lastPreviewMoveTick = now;
            _lastPreviewMove = normalised;
            _lastBrowserHeldMask = heldMask;
            _surface.SendBrowserMouse(normalised.X, normalised.Y, downMask, upMask, heldMask, doubleMask, wheelX, wheelY, leaving: false, shift: io.KeyShift, ctrl: io.KeyCtrl, alt: io.KeyAlt);
        }
    }

    private void DrawBrowserKeyboardCapture()
    {
        if (!_browserPreviewFocused)
        {
            _browserKeyboardCapture = string.Empty;
            _browserKeyboardCaptureLast = string.Empty;
            ResetBrowserKeyState();
            return;
        }

        var io = ImGui.GetIO();

        // Keep an active, invisible ImGui item so Dalamud captures keyboard/text input for the
        // RavaCast window. Do not mark this ReadOnly: that prevents some backends from entering
        // text-input mode, making typing/paste appear focused while no characters are produced.
        //
        // Important: do not forward io.InputQueueCharacters directly here. In Dalamud/ImGui that
        // queue can remain visible for more than one draw tick while this hidden input is active,
        // which turns a single typed character into an endless browser insert loop. Instead, treat
        // the hidden input buffer as an append-only capture buffer and forward only the newly added
        // suffix once. Backspace/delete/navigation are sent through SendBrowserKeyOnChange below.
        var hiddenInputCursor = ImGui.GetCursorPos();
        ImGui.PushStyleVar(ImGuiStyleVar.Alpha, 0f);
        ImGui.SetCursorPos(new Vector2(-10000f, -10000f));
        ImGui.SetNextItemWidth(1f);
        if (_keyboardFocusPending)
        {
            ImGui.SetKeyboardFocusHere();
            _keyboardFocusPending = false;
        }
        var captureChanged = ImGui.InputText("##ravacast_browser_keyboard_capture", ref _browserKeyboardCapture, 2048, ImGuiInputTextFlags.Password | ImGuiInputTextFlags.NoUndoRedo);
        var capturedText = _browserKeyboardCapture;
        ImGui.PopStyleVar();
        ImGui.SetCursorPos(hiddenInputCursor);

        // The hidden input exists only to collect keyboard text for the browser preview.
        // Do not force ImGui's mouse cursor here: while FFXIV owns the foreground cursor,
        // overriding it every frame can make the real system pointer blink/flicker.

        if (captureChanged)
        {
            var textToSend = GetNewBrowserCaptureText(_browserKeyboardCaptureLast, capturedText);
            _browserKeyboardCaptureLast = capturedText;
            if (!string.IsNullOrEmpty(textToSend))
                _surface.SendTextInput(textToSend);

            if (_browserKeyboardCapture.Length > 1536)
            {
                _browserKeyboardCapture = string.Empty;
                _browserKeyboardCaptureLast = string.Empty;
            }
        }

        var shift = io.KeyShift;
        var ctrl = io.KeyCtrl;
        var alt = io.KeyAlt;
        SendBrowserKeyOnChange(ImGuiKey.Enter, ref _enterWasDown, 13, "\r", shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Tab, ref _tabWasDown, 9, "\t", shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Escape, ref _escapeWasDown, 27, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Backspace, ref _backspaceWasDown, 8, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Delete, ref _deleteWasDown, 46, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.LeftArrow, ref _leftWasDown, 37, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.RightArrow, ref _rightWasDown, 39, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.UpArrow, ref _upWasDown, 38, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.DownArrow, ref _downWasDown, 40, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Home, ref _homeWasDown, 36, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.End, ref _endWasDown, 35, null, shift, ctrl, alt);
    }

    private static string GetNewBrowserCaptureText(string previous, string current)
    {
        if (string.IsNullOrEmpty(current)) return string.Empty;
        if (string.IsNullOrEmpty(previous)) return current;

        // Normal typing and paste append to the hidden capture buffer. Only forward that new
        // suffix once. If the buffer changed in any other way, it was an edit/navigation action
        // inside the hidden input, not printable browser text to inject.
        return current.StartsWith(previous, StringComparison.Ordinal)
            ? current[previous.Length..]
            : string.Empty;
    }

    private void SendBrowserKeyOnChange(ImGuiKey key, ref bool wasDown, int virtualKey, string? text, bool shift, bool ctrl, bool alt)
    {
        var down = ImGui.IsKeyDown(key);
        if (down != wasDown)
            _surface.SendBrowserKey(virtualKey, down, down ? text : null, shift, ctrl, alt);
        wasDown = down;
    }

    private static int BuildMouseMask(bool left, bool right, bool middle)
    {
        var mask = 0;
        if (left) mask |= 1;
        if (right) mask |= 2;
        if (middle) mask |= 4;
        return mask;
    }

    private void ResetBrowserKeyState()
    {
        _keyboardFocusPending = false;
        _lastBrowserHeldMask = 0;
        _lastPreviewMouseInside = false;
        _enterWasDown = false;
        _tabWasDown = false;
        _escapeWasDown = false;
        _backspaceWasDown = false;
        _deleteWasDown = false;
        _leftWasDown = false;
        _rightWasDown = false;
        _upWasDown = false;
        _downWasDown = false;
        _homeWasDown = false;
        _endWasDown = false;
    }


    private void DrawSavedScreenPlacementControls(bool hosting, bool live)
    {
        var savedPlacements = GetSavedScreenPlacements();
        var selectedPlacement = FindSavedScreenPlacement(_selectedScreenPlacementName);
        var selectedLabel = selectedPlacement is null ? DefaultScreenPlacementName : selectedPlacement.Name;

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Saved Screens");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
        if (ImGui.BeginCombo($"##ravacast_screen_preset_{(live ? "live" : "setup")}", selectedLabel))
        {
            var defaultSelected = selectedPlacement is null;
            if (ImGui.Selectable($"{DefaultScreenPlacementName}##ravacast_screen_default_{(live ? "live" : "setup")}", defaultSelected))
            {
                _selectedScreenPlacementName = DefaultScreenPlacementName;
                PersistSelectedScreenPlacementName();
                ApplyDefaultScreenPlacement(showStatus: true);
                RequestHostedPlaneUpdate(hosting, force: live);
            }
            if (defaultSelected) ImGui.SetItemDefaultFocus();

            ImGui.Separator();
            if (savedPlacements.Count == 0)
            {
                ImGui.TextColored(ImGuiColors.DalamudGrey, "No saved screens yet");
            }
            else
            {
                ImGui.TextColored(ImGuiColors.DalamudGrey, "Saved Screens");
                foreach (var placement in savedPlacements.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(placement.Name)) continue;
                    var isSelected = string.Equals(placement.Name, selectedLabel, StringComparison.OrdinalIgnoreCase);
                    if (ImGui.Selectable($"{placement.Name}##ravacast_saved_screen_{placement.Name}", isSelected))
                    {
                        LoadSavedScreenPlacement(placement, showStatus: true);
                        RequestHostedPlaneUpdate(hosting, force: live);
                    }
                    if (isSelected) ImGui.SetItemDefaultFocus();
                }
            }

            ImGui.EndCombo();
        }
        UiSharedService.AttachToolTip("Choose Default to place the screen in front of you, or choose a saved screen to reuse its position, size, rotation, and tilt.");

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Save as");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
        ImGui.InputTextWithHint($"##ravacast_save_screen_name_{(live ? "live" : "setup")}", "Saved screen name", ref _saveScreenPlacementName, 64);
        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Save, "Save Screen", 140 * ImGuiHelpers.GlobalScale, true))
            SaveCurrentScreenPlacement();

        ImGui.SameLine();
        using (ImRaii.Disabled(selectedPlacement is null))
        {
            if (_uiShared.IconTextButton(FontAwesomeIcon.Trash, "Delete", 105 * ImGuiHelpers.GlobalScale, true) && selectedPlacement is not null)
                DeleteSavedScreenPlacement(selectedPlacement.Name);
        }
    }

    private List<RavaCastSavedScreenPlacement> GetSavedScreenPlacements()
    {
        _config.Current.RavaCastSavedScreenPlacements ??= [];
        _config.Current.RavaCastSavedScreenPlacements.RemoveAll(p => p is null || string.IsNullOrWhiteSpace(p.Name));
        return _config.Current.RavaCastSavedScreenPlacements;
    }

    private RavaCastSavedScreenPlacement? FindSavedScreenPlacement(string? name)
    {
        if (string.IsNullOrWhiteSpace(name) || string.Equals(name, DefaultScreenPlacementName, StringComparison.OrdinalIgnoreCase))
            return null;

        foreach (var placement in GetSavedScreenPlacements())
        {
            if (string.Equals(placement.Name, name.Trim(), StringComparison.OrdinalIgnoreCase))
                return placement;
        }

        return null;
    }

    private void ApplySelectedScreenPlacementOrDefault(bool showStatus)
    {
        var selected = FindSavedScreenPlacement(_selectedScreenPlacementName);
        if (selected is not null)
        {
            LoadSavedScreenPlacement(selected, showStatus);
            return;
        }

        _selectedScreenPlacementName = DefaultScreenPlacementName;
        PersistSelectedScreenPlacementName();
        ApplyDefaultScreenPlacement(showStatus);
    }

    private void ApplyDefaultScreenPlacement(bool showStatus)
    {
        if (_ravaCast.TryGetPlayerSuggestedPlacement(out var centre, out var yaw))
        {
            _screenCentre = centre;
            _screenYaw = yaw;
            _screenPitch = 0f;
            if (showStatus) _screenPlacementStatus = "Default screen selected. It will appear in front of you when you start the broadcast.";
        }
        else if (showStatus)
        {
            _screenPlacementStatus = "Default screen selected, but RavaCast could not find your current position yet.";
        }
    }

    private void LoadSavedScreenPlacement(RavaCastSavedScreenPlacement placement, bool showStatus)
    {
        _selectedScreenPlacementName = placement.Name.Trim();
        PersistSelectedScreenPlacementName();
        _screenName = string.IsNullOrWhiteSpace(placement.ScreenName) ? _screenName : placement.ScreenName.Trim();
        _screenCentre = new Vector3(placement.CentreX, placement.CentreY, placement.CentreZ);
        _screenWidth = Math.Clamp(float.IsFinite(placement.Width) ? placement.Width : 3.0f, 0.25f, 20f);
        _screenHeight = Math.Clamp(float.IsFinite(placement.Height) ? placement.Height : 1.70f, 0.15f, 12f);
        _screenYaw = WrapRadians(float.IsFinite(placement.YawRadians) ? placement.YawRadians : 0f);
        _screenPitch = Math.Clamp(float.IsFinite(placement.PitchRadians) ? placement.PitchRadians : 0f, DegreesToRadians(-80f), DegreesToRadians(80f));
        _screenFineTuneOpen = true;
        _saveScreenPlacementName = placement.Name.Trim();
        if (showStatus) _screenPlacementStatus = $"Loaded saved screen '{placement.Name}'. It will be used when you start the broadcast.";
    }

    private void SaveCurrentScreenPlacement()
    {
        var name = SanitiseSavedScreenName(string.IsNullOrWhiteSpace(_saveScreenPlacementName) ? _screenName : _saveScreenPlacementName);
        if (string.IsNullOrWhiteSpace(name) || string.Equals(name, DefaultScreenPlacementName, StringComparison.OrdinalIgnoreCase))
        {
            _screenPlacementStatus = "Give the saved screen a unique name first.";
            return;
        }

        var saved = GetSavedScreenPlacements();
        var existing = saved.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
        if (existing is null)
        {
            existing = new RavaCastSavedScreenPlacement { Name = name };
            saved.Add(existing);
        }

        var territoryRaw = CurrentPlane().TerritoryId;
        existing.Name = name;
        existing.ScreenName = string.IsNullOrWhiteSpace(_screenName) ? name : _screenName.Trim();
        existing.TerritoryId = territoryRaw;
        existing.CentreX = _screenCentre.X;
        existing.CentreY = _screenCentre.Y;
        existing.CentreZ = _screenCentre.Z;
        existing.Width = Math.Clamp(_screenWidth, 0.25f, 20f);
        existing.Height = Math.Clamp(_screenHeight, 0.15f, 12f);
        existing.YawRadians = WrapRadians(_screenYaw);
        existing.PitchRadians = Math.Clamp(_screenPitch, DegreesToRadians(-80f), DegreesToRadians(80f));
        existing.UpdatedUtc = DateTime.UtcNow;

        _selectedScreenPlacementName = name;
        _saveScreenPlacementName = name;
        PersistSelectedScreenPlacementName();
        _config.Save();
        _screenPlacementStatus = $"Saved screen '{name}'.";
    }

    private void DeleteSavedScreenPlacement(string name)
    {
        var saved = GetSavedScreenPlacements();
        var removed = saved.RemoveAll(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
        if (removed <= 0) return;

        _selectedScreenPlacementName = DefaultScreenPlacementName;
        _saveScreenPlacementName = string.Empty;
        PersistSelectedScreenPlacementName();
        ApplyDefaultScreenPlacement(showStatus: false);
        _config.Save();
        _screenPlacementStatus = $"Deleted saved screen '{name}'.";
    }

    private void PersistSelectedScreenPlacementName()
    {
        var savedName = string.Equals(_selectedScreenPlacementName, DefaultScreenPlacementName, StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : _selectedScreenPlacementName.Trim();
        if (!string.Equals(_config.Current.RavaCastSelectedScreenPlacementName, savedName, StringComparison.Ordinal))
        {
            _config.Current.RavaCastSelectedScreenPlacementName = savedName;
            _config.Save();
        }
    }

    private static string SanitiseSavedScreenName(string name)
        => (name ?? string.Empty).Trim();

    private void DrawScreenPlacementEditor(bool hosting, RavaCastSessionView? current, bool live)
    {
        DrawSection(live ? "Screen position" : "Screen placement");

        DrawSavedScreenPlacementControls(hosting, live);

        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Screen name");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
        if (ImGui.InputText($"##ravacast_screen_name_{(live ? "live" : "setup")}", ref _screenName, 64) && live)
            RequestHostedPlaneUpdate(hosting, force: true);

        if (_uiShared.IconTextButton(FontAwesomeIcon.MapPin, _screenGizmoActive ? "Place Again" : "Place Screen", 180 * ImGuiHelpers.GlobalScale, true))
        {
            _screenPickActive = false;
            if (_ravaCast.TryGetPlayerSuggestedPlacement(out var centre, out var yaw))
            {
                _screenCentre = centre;
                _screenYaw = yaw;
                _screenPitch = 0f;
                _selectedScreenPlacementName = DefaultScreenPlacementName;
                PersistSelectedScreenPlacementName();
                _screenFineTuneOpen = true;
                _screenGizmoActive = true;
                _screenGizmoDragMode = 0;
                _screenPlacementStatus = "Placed in front of you. Drag the handles to move, resize, rotate, tilt, or change distance. Press Escape to hide them.";
                RequestHostedPlaneUpdate(hosting, force: live);
            }
            else
            {
                _screenPlacementStatus = "Could not place the screen because RavaCast could not find your current position.";
            }
        }
        ImGui.SameLine();
        if (_uiShared.IconTextButton(FontAwesomeIcon.Save, live ? "Apply Now" : "Apply screen", 150 * ImGuiHelpers.GlobalScale, true))
        {
            RequestHostedPlaneUpdate(hosting, force: true);
            _screenPickActive = false;
            _screenGizmoActive = false;
            _screenGizmoDragMode = 0;
            _liveMoveScreenOpen = false;
            _ravaCast.SetWorldBrowserInputSuspended(false);
            _screenPlacementStatus = live ? "Screen position applied." : "Screen placement applied.";
        }

        ImGui.SameLine();
        if (ImGui.SmallButton((_screenFineTuneOpen ? "Hide controls" : "Fine tune") + $"##ravacast_finetune_{(live ? "live" : "setup")}"))
            _screenFineTuneOpen = !_screenFineTuneOpen;

        if (!string.IsNullOrWhiteSpace(_screenPlacementStatus))
            ImGui.TextColored(ImGuiColors.DalamudGrey, _screenPlacementStatus);

        if (!_screenFineTuneOpen)
            return;

        var changed = DrawScreenPlacementSliders();

        _screenWidth = Math.Clamp(_screenWidth, 0.25f, 20f);
        _screenHeight = Math.Clamp(_screenHeight, 0.15f, 12f);
        _screenYaw = WrapRadians(_screenYaw);
        _screenPitch = Math.Clamp(_screenPitch, DegreesToRadians(-80f), DegreesToRadians(80f));

        if (changed && live)
            RequestHostedPlaneUpdate(hosting);
    }

    private void DrawScreenPickOverlay()
    {
        if (!_screenPickActive) return;

        var io = ImGui.GetIO();
        var displaySize = io.DisplaySize;
        if (displaySize.X <= 1f || displaySize.Y <= 1f)
        {
            _screenPickActive = false;
            return;
        }

        var draw = ImGui.GetForegroundDrawList();
        draw.AddRectFilled(Vector2.Zero, displaySize, 0x66000000);

        var prompt = "Click where you want the screen centre. Press Escape to cancel.";
        var promptSize = ImGui.CalcTextSize(prompt);
        var promptPos = new Vector2((displaySize.X - promptSize.X) / 2f, 64f * ImGuiHelpers.GlobalScale);
        draw.AddRectFilled(promptPos - new Vector2(12f, 8f), promptPos + promptSize + new Vector2(12f, 8f), 0xDD16061F, 8f);
        draw.AddText(promptPos, 0xFFFFFFFF, prompt);

        var mouse = ImGui.GetMousePos();
        draw.AddCircle(mouse, 10f * ImGuiHelpers.GlobalScale, 0xFFB86BFF, 32, 2f * ImGuiHelpers.GlobalScale);
        draw.AddLine(mouse - new Vector2(18f, 0f), mouse + new Vector2(18f, 0f), 0xFFB86BFF, 1.5f * ImGuiHelpers.GlobalScale);
        draw.AddLine(mouse - new Vector2(0f, 18f), mouse + new Vector2(0f, 18f), 0xFFB86BFF, 1.5f * ImGuiHelpers.GlobalScale);

        ImGui.SetNextWindowPos(Vector2.Zero);
        ImGui.SetNextWindowSize(displaySize);
        var flags = ImGuiWindowFlags.NoTitleBar | ImGuiWindowFlags.NoResize | ImGuiWindowFlags.NoMove | ImGuiWindowFlags.NoScrollbar | ImGuiWindowFlags.NoSavedSettings | ImGuiWindowFlags.NoBackground | ImGuiWindowFlags.NoBringToFrontOnFocus;
        if (ImGui.Begin("##ravacast_screen_pick_overlay", flags))
        {
            if (ImGui.IsKeyPressed(ImGuiKey.Escape))
            {
                _screenPickActive = false;
                _screenPlacementStatus = "Screen placement cancelled.";
                ImGui.End();
                return;
            }

            if (ImGui.IsMouseClicked(ImGuiMouseButton.Left))
            {
                if (_ravaCast.TryPickScreenPlacementFromCursor(mouse, displaySize, _screenCentre.Y, _screenHeight, out var centre, out var yaw, out var error))
                {
                    _screenCentre = centre;
                    _screenYaw = yaw;
                    _screenPitch = 0f;
                    _selectedScreenPlacementName = DefaultScreenPlacementName;
                    PersistSelectedScreenPlacementName();
                    _screenFineTuneOpen = true;
                    _screenPickActive = false;
                    _screenGizmoActive = true;
                    _screenGizmoDragMode = 0;
                    _screenPlacementStatus = "Screen placed. Drag the handles to move, resize, rotate, tilt, or change distance.";
                    var current = _ravaCast.GetCurrentSession();
                    if (current?.IsOwner == true && _liveMoveScreenOpen)
                        RequestHostedPlaneUpdate(hosting: true, force: true);
                }
                else
                {
                    _screenPlacementStatus = string.IsNullOrWhiteSpace(error) ? "Could not place the screen from that click." : error;
                }
            }
        }
        ImGui.End();
    }


    private void DrawScreenGizmoOverlay()
    {
        if (!_screenGizmoActive) return;

        if (ImGui.IsKeyPressed(ImGuiKey.Escape))
        {
            _screenGizmoActive = false;
            _screenGizmoDragMode = 0;
            _liveMoveScreenOpen = false;
            _ravaCast.SetWorldBrowserInputSuspended(false);
            _screenPlacementStatus = "Screen handles hidden.";
            return;
        }

        var io = ImGui.GetIO();
        var displaySize = io.DisplaySize;
        if (displaySize.X <= 1f || displaySize.Y <= 1f) return;

        if (!_ravaCast.TryProjectScreenPlaneToViewport(CurrentPlane(), displaySize, out var tl, out var tr, out var br, out var bl, out var projectError))
        {
            var drawError = ImGui.GetForegroundDrawList();
            var message = string.IsNullOrWhiteSpace(projectError) ? "Screen handles are off-screen." : projectError;
            var pos = new Vector2(24f * ImGuiHelpers.GlobalScale, 96f * ImGuiHelpers.GlobalScale);
            var size = ImGui.CalcTextSize(message);
            drawError.AddRectFilled(pos - new Vector2(10f, 7f), pos + size + new Vector2(10f, 7f), 0xDD16061F, 8f);
            drawError.AddText(pos, 0xFFFFFFFF, message);
            return;
        }

        var draw = ImGui.GetForegroundDrawList();
        const uint edge = 0xFFB86BFF;
        const uint fill = 0x22B86BFF;
        const uint handle = 0xFFE9D5FF;
        const uint depthHandleColour = 0xFFFFD67B;
        const uint tiltHandleColour = 0xFF6BC4FF;
        draw.AddQuadFilled(tl, tr, br, bl, fill);
        draw.AddQuad(tl, tr, br, bl, edge, 2.5f * ImGuiHelpers.GlobalScale);

        var centre = (tl + tr + br + bl) / 4f;
        var topMid = (tl + tr) / 2f;
        var bottomMid = (bl + br) / 2f;
        var leftMid = (tl + bl) / 2f;
        var topVector = topMid - centre;
        if (topVector.LengthSquared() < 1f) topVector = new Vector2(0f, -1f);
        topVector = Vector2.Normalize(topVector);
        var rotateHandle = topMid + topVector * (44f * ImGuiHelpers.GlobalScale);
        var depthHandle = bottomMid - topVector * (44f * ImGuiHelpers.GlobalScale);
        var sideVector = leftMid - centre;
        if (sideVector.LengthSquared() < 1f) sideVector = new Vector2(-1f, 0f);
        sideVector = Vector2.Normalize(sideVector);
        var tiltHandle = leftMid + sideVector * (44f * ImGuiHelpers.GlobalScale);
        var scaleHandle = br;
        var moveHandle = centre;
        var handleRadius = 10f * ImGuiHelpers.GlobalScale;

        draw.AddLine(topMid, rotateHandle, edge, 1.5f * ImGuiHelpers.GlobalScale);
        draw.AddLine(bottomMid, depthHandle, depthHandleColour, 1.5f * ImGuiHelpers.GlobalScale);
        draw.AddLine(leftMid, tiltHandle, tiltHandleColour, 1.5f * ImGuiHelpers.GlobalScale);
        draw.AddCircleFilled(moveHandle, handleRadius, handle);
        draw.AddCircle(moveHandle, handleRadius + 2f, edge, 24, 2f * ImGuiHelpers.GlobalScale);
        draw.AddCircleFilled(scaleHandle, handleRadius, handle);
        draw.AddCircle(scaleHandle, handleRadius + 2f, edge, 24, 2f * ImGuiHelpers.GlobalScale);
        draw.AddCircleFilled(rotateHandle, handleRadius, handle);
        draw.AddCircle(rotateHandle, handleRadius + 2f, edge, 24, 2f * ImGuiHelpers.GlobalScale);
        draw.AddCircleFilled(depthHandle, handleRadius, depthHandleColour);
        draw.AddCircle(depthHandle, handleRadius + 2f, 0xFFFFFFFF, 24, 2f * ImGuiHelpers.GlobalScale);
        draw.AddCircleFilled(tiltHandle, handleRadius, tiltHandleColour);
        draw.AddCircle(tiltHandle, handleRadius + 2f, 0xFFFFFFFF, 24, 2f * ImGuiHelpers.GlobalScale);

        var help = "Drag screen to move · corner to resize · top to rotate · side to tilt · blue to change distance · Esc to hide";
        var helpSize = ImGui.CalcTextSize(help);
        var helpPos = new Vector2((displaySize.X - helpSize.X) / 2f, 64f * ImGuiHelpers.GlobalScale);
        draw.AddRectFilled(helpPos - new Vector2(12f, 8f), helpPos + helpSize + new Vector2(12f, 8f), 0xDD16061F, 8f);
        draw.AddText(helpPos, 0xFFFFFFFF, help);

        // Use separate tiny transparent hit windows around the actual handles/body. This was
        // the version that behaved reliably in-game: the handles are real ImGui items, but the
        // rest of the screen remains free for normal camera movement.
        var mouse = ImGui.GetMousePos();
        var changed = false;
        var projectedWidth = Math.Max(16f, Vector2.Distance(tl, tr));
        var pixelToWorld = Math.Clamp(_screenWidth / projectedWidth, 0.0025f, 0.10f);
        var right = ScreenRightFromYaw(_screenYaw);
        var normal = ScreenNormalFromYaw(_screenYaw);

        if (_screenGizmoDragMode == 0)
        {
            var hitRadius = handleRadius * 2.35f;
            TryStartGizmoCircleHit("rotate", rotateHandle, hitRadius, 3);
            TryStartGizmoCircleHit("depth", depthHandle, hitRadius, 4);
            TryStartGizmoCircleHit("tilt", tiltHandle, hitRadius, 5);
            TryStartGizmoCircleHit("scale", scaleHandle, hitRadius, 2);
            TryStartGizmoCircleHit("move", moveHandle, hitRadius, 1);
            TryStartGizmoQuadHit("body", tl, tr, br, bl, 1);
        }

        if (_screenGizmoDragMode != 0 && ImGui.IsMouseDown(ImGuiMouseButton.Left))
        {
            var delta = mouse - _screenGizmoLastMouse;
            if (delta.LengthSquared() > 0.01f)
            {
                switch (_screenGizmoDragMode)
                {
                    case 1:
                        // Screen yaw/right is defined from the viewer side, but the projected
                        // widget drag delta is screen-space. Invert horizontal translation so
                        // dragging left moves the world screen left from the user's view.
                        _screenCentre -= right * (delta.X * pixelToWorld);
                        _screenCentre -= Vector3.UnitY * (delta.Y * pixelToWorld);
                        changed = true;
                        break;
                    case 2:
                        _screenWidth -= delta.X * pixelToWorld;
                        _screenHeight -= delta.Y * pixelToWorld;
                        changed = true;
                        break;
                    case 3:
                        _screenYaw += delta.X * 0.010f;
                        changed = true;
                        break;
                    case 4:
                        // Match the on-screen blue depth handle: if the handle now feels inverted,
                        // flip the sign against the screen-to-viewer normal rather than changing yaw.
                        _screenCentre += normal * (delta.Y * pixelToWorld * 1.75f);
                        changed = true;
                        break;
                    case 5:
                        _screenPitch += delta.Y * 0.010f;
                        changed = true;
                        break;
                }

                _screenGizmoLastMouse = mouse;
            }
        }

        if (!ImGui.IsMouseDown(ImGuiMouseButton.Left)) _screenGizmoDragMode = 0;

        if (changed)
        {
            _screenWidth = Math.Clamp(_screenWidth, 0.25f, 20f);
            _screenHeight = Math.Clamp(_screenHeight, 0.15f, 12f);
            _screenYaw = WrapRadians(_screenYaw);
            _screenPitch = Math.Clamp(_screenPitch, DegreesToRadians(-80f), DegreesToRadians(80f));
            var current = _ravaCast.GetCurrentSession();
            if (current?.IsOwner == true && _liveMoveScreenOpen)
                RequestHostedPlaneUpdate(hosting: true);
        }
    }

    private bool TryStartGizmoCircleHit(string id, Vector2 centre, float radius, int mode)
        => TryStartGizmoHitBox($"##ravacast_gizmo_{id}", centre - new Vector2(radius), centre + new Vector2(radius), p => Vector2.Distance(p, centre) <= radius, mode);

    private bool TryStartGizmoQuadHit(string id, Vector2 a, Vector2 b, Vector2 c, Vector2 d, int mode)
    {
        var min = new Vector2(MathF.Min(MathF.Min(a.X, b.X), MathF.Min(c.X, d.X)), MathF.Min(MathF.Min(a.Y, b.Y), MathF.Min(c.Y, d.Y)));
        var max = new Vector2(MathF.Max(MathF.Max(a.X, b.X), MathF.Max(c.X, d.X)), MathF.Max(MathF.Max(a.Y, b.Y), MathF.Max(c.Y, d.Y)));
        var pad = 8f * ImGuiHelpers.GlobalScale;
        return TryStartGizmoHitBox($"##ravacast_gizmo_{id}", min - new Vector2(pad), max + new Vector2(pad), p => PointInQuad(p, a, b, c, d), mode);
    }

    private bool TryStartGizmoHitBox(string id, Vector2 min, Vector2 max, Func<Vector2, bool> preciseHitTest, int mode)
    {
        if (_screenGizmoDragMode != 0) return false;

        var size = max - min;
        if (size.X < 4f || size.Y < 4f) return false;

        var clicked = false;
        var flags = ImGuiWindowFlags.NoTitleBar
            | ImGuiWindowFlags.NoResize
            | ImGuiWindowFlags.NoMove
            | ImGuiWindowFlags.NoScrollbar
            | ImGuiWindowFlags.NoSavedSettings
            | ImGuiWindowFlags.NoBackground
            | ImGuiWindowFlags.NoFocusOnAppearing
            | ImGuiWindowFlags.NoBringToFrontOnFocus;

        ImGui.SetNextWindowPos(min);
        ImGui.SetNextWindowSize(size);
        if (ImGui.Begin(id, flags))
        {
            ImGui.SetCursorScreenPos(min);
            ImGui.InvisibleButton($"{id}_hit", size, ImGuiButtonFlags.MouseButtonLeft);
            var mouse = ImGui.GetMousePos();
            if (ImGui.IsItemHovered() && ImGui.IsMouseClicked(ImGuiMouseButton.Left) && preciseHitTest(mouse))
                clicked = true;
        }
        ImGui.End();

        if (!clicked) return false;
        _screenGizmoDragMode = mode;
        _screenGizmoLastMouse = ImGui.GetMousePos();
        return true;
    }

    private static bool PointInQuad(Vector2 p, Vector2 a, Vector2 b, Vector2 c, Vector2 d)
        => PointInTriangle(p, a, b, c) || PointInTriangle(p, a, c, d);

    private static bool PointInTriangle(Vector2 p, Vector2 a, Vector2 b, Vector2 c)
    {
        static float Sign(Vector2 p1, Vector2 p2, Vector2 p3)
            => (p1.X - p3.X) * (p2.Y - p3.Y) - (p2.X - p3.X) * (p1.Y - p3.Y);

        var d1 = Sign(p, a, b);
        var d2 = Sign(p, b, c);
        var d3 = Sign(p, c, a);
        var hasNeg = d1 < 0f || d2 < 0f || d3 < 0f;
        var hasPos = d1 > 0f || d2 > 0f || d3 > 0f;
        return !(hasNeg && hasPos);
    }

    private void DrawGizmoHandle(string id, Vector2 centre, float size, int mode, ref bool changed, float pixelToWorld)
    {
        var mouse = ImGui.GetMousePos();
        ImGui.SetCursorScreenPos(centre - new Vector2(size / 2f));
        ImGui.InvisibleButton(id, new Vector2(size, size), ImGuiButtonFlags.MouseButtonLeft);
        if (ImGui.IsItemHovered())
            ImGui.GetForegroundDrawList().AddCircle(centre, size * 0.55f, 0xFFFFFFFF, 24, 3f * ImGuiHelpers.GlobalScale);
        if (ImGui.IsItemActivated()) { _screenGizmoDragMode = mode; _screenGizmoLastMouse = mouse; }
        if (!ImGui.IsItemActive() || _screenGizmoDragMode != mode) return;

        var delta = mouse - _screenGizmoLastMouse;
        if (delta.LengthSquared() <= 0.01f) return;
        if (mode == 2)
        {
            _screenWidth -= delta.X * pixelToWorld;
            _screenHeight -= delta.Y * pixelToWorld;
        }
        else if (mode == 3)
        {
            _screenYaw += delta.X * 0.010f;
        }
        else if (mode == 5)
        {
            _screenPitch += delta.Y * 0.010f;
        }

        _screenGizmoLastMouse = mouse;
        changed = true;
    }

    private void RequestHostedPlaneUpdate(bool hosting, bool force = false)
    {
        if (!hosting) return;

        var plane = CurrentPlane();

        // Keep the local in-world screen perfectly responsive while dragging/sliding, but do not
        // blast every tiny placement delta over mesh/Direct Stream. A quiet update makes the host
        // see the screen move immediately; periodic/final broadcasts keep viewers in sync without
        // collapsing stream frame rate during live placement.
        if (!force)
            _ravaCast.UpdateHostedPlane(plane, broadcast: false);

        if (!force && !_ravaCast.ShouldBroadcastHostedPlaneNow(force: false))
            return;

        _lastLivePlaneUpdateTick = Environment.TickCount64;
        _ravaCast.UpdateHostedPlane(plane, broadcast: true);
    }

    private void LoadPlaneIntoEditor(RavaCastPlane plane)
    {
        if (plane.TerritoryId == 0) return;
        var centre = (plane.TopLeft + plane.TopRight + plane.BottomRight + plane.BottomLeft) / 4f;
        var right = plane.TopRight - plane.TopLeft;
        var down = plane.BottomLeft - plane.TopLeft;
        _screenName = string.IsNullOrWhiteSpace(plane.ScreenName) ? _screenName : plane.ScreenName;
        _screenCentre = centre;
        _screenWidth = Math.Clamp(right.Length(), 0.25f, 20f);
        _screenHeight = Math.Clamp(down.Length(), 0.15f, 12f);
        if (right.LengthSquared() > 0.0001f)
        {
            _screenYaw = WrapRadians(MathF.Atan2(right.Z, -right.X));
            var up = -down;
            if (up.LengthSquared() > 0.0001f)
            {
                up = Vector3.Normalize(up);
                var normal = ScreenNormalFromYaw(_screenYaw);
                _screenPitch = Math.Clamp(MathF.Asin(Math.Clamp(Vector3.Dot(up, normal), -1f, 1f)), DegreesToRadians(-80f), DegreesToRadians(80f));
            }
            else
            {
                _screenPitch = 0f;
            }
        }
    }

    // Yaw is the screen-to-viewer normal.  The right vector is therefore the viewer's right,
    // not the plane's old back-side right; keeping these aligned stops the browser image and
    // left/right drag controls from being mirrored.
    private static Vector3 ScreenRightFromYaw(float yaw) => new(-MathF.Cos(yaw), 0f, MathF.Sin(yaw));
    private static Vector3 ScreenNormalFromYaw(float yaw) => new(MathF.Sin(yaw), 0f, MathF.Cos(yaw));
    private static float DegreesToRadians(float degrees) => degrees * (MathF.PI / 180f);

    private static float WrapRadians(float radians)
    {
        while (radians > MathF.PI) radians -= MathF.Tau;
        while (radians < -MathF.PI) radians += MathF.Tau;
        return radians;
    }

    private void DrawOwnerControls(RavaCastSessionView current)
    {
        if (_uiShared.IconTextButton(FontAwesomeIcon.Stop, "End Cast", 130 * ImGuiHelpers.GlobalScale, true))
        {
            ClearBrowserPreviewFocus();
            _ravaCast.EndBroadcast();
            _selectLobbyTab = true;
        }
    }

    private RavaCastPlane CurrentPlane()
        => _ravaCast.BuildPlane(_screenName, _screenCentre, _screenWidth, _screenHeight, _screenYaw, _screenPitch);

    private static void DrawSection(string label)
    {
        ImGuiHelpers.ScaledDummy(4);
        ImGui.TextColored(ImGuiColors.DalamudViolet, label);
        ImGui.Separator();
    }

    private bool DrawScreenPlacementSliders()
    {
        var changed = false;

        ImGui.TextColored(ImGuiColors.DalamudGrey, "Position");
        changed |= DrawSliderRow("X", ref _screenCentre.X, -20f, 20f);
        changed |= DrawSliderRow("Y", ref _screenCentre.Y, -10f, 10f);
        changed |= DrawSliderRow("Z", ref _screenCentre.Z, -20f, 20f);

        ImGuiHelpers.ScaledDummy(4);
        ImGui.TextColored(ImGuiColors.DalamudGrey, "Size and Rotation");
        changed |= DrawSliderRow("Width", ref _screenWidth, 0.25f, 20f);
        changed |= DrawSliderRow("Height", ref _screenHeight, 0.15f, 12f);
        changed |= DrawSliderRow("Yaw", ref _screenYaw, -MathF.PI, MathF.PI);
        changed |= DrawSliderRow("Tilt", ref _screenPitch, DegreesToRadians(-80f), DegreesToRadians(80f));

        return changed;
    }

    private static bool DrawSliderRow(string label, ref float value, float min, float max)
    {
        var labelWidth = 62f * ImGuiHelpers.GlobalScale;
        var sliderWidth = Math.Max(180f * ImGuiHelpers.GlobalScale, ImGui.GetContentRegionAvail().X - labelWidth - (8f * ImGuiHelpers.GlobalScale));

        ImGui.PushID($"ravacast_screen_{label}");
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(label);
        ImGui.SameLine(labelWidth);
        ImGui.SetNextItemWidth(sliderWidth);

        ImGui.PushStyleColor(ImGuiCol.SliderGrab, new Vector4(0.10f, 0.52f, 1.00f, 1.00f));
        ImGui.PushStyleColor(ImGuiCol.SliderGrabActive, new Vector4(0.35f, 0.72f, 1.00f, 1.00f));
        var changed = ImGui.SliderFloat("##value", ref value, min, max, "%.3f");
        ImGui.PopStyleColor(2);
        ImGui.PopID();

        return changed;
    }
}
