using Dalamud.Bindings.ImGui;
using Dalamud.Plugin;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.Services.RavaCast;
using SharpDX.Direct3D11;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Pipes;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using GameDevice = FFXIVClientStructs.FFXIV.Client.Graphics.Kernel.Device;
using D3DDevice = SharpDX.Direct3D11.Device;
using DxgiDevice = SharpDX.DXGI.Device;

namespace RavaSync.Services.RavaCast.Rendering;

/// <summary>
/// WebView2/WGC D3D backend for RavaCast.
///
/// The companion renderer process owns a real Edge WebView2 host window kept behind the game, captures it through
/// Windows Graphics Capture, and publishes the resolved browser output as a D3D11 shared texture. The plugin opens
/// that shared texture and draws the exact same output in the preview and the in-world RavaCast screen.
/// </summary>
public sealed class RavaCastWebView2D3DTextureBackend : IRavaCastTextureBackend
{
    private const int SurfaceWidth = 1280;
    private const int SurfaceHeight = 720;
    private const string SharedTextureSourceWebView = "webview";
    private const string SharedTextureSourceDirectStreamReceiver = "directStreamReceiver";

    private readonly ILogger<RavaCastWebView2D3DTextureBackend> _logger;
    private readonly IDalamudPluginInterface _pluginInterface;
    private readonly RavaCastBackendInstallerService _installer;
    private readonly MareConfigService _config;
    private readonly object _stateLock = new();
    private const string DirectStreamV2TransportDetail = "Direct Stream v2 uses libdatachannel with FFmpeg H.264 live video and Opus audio-over-datachannel.";
    private readonly object _frameLock = new();
    private readonly ConcurrentQueue<string> _pendingCommands = new();
    private readonly SemaphoreSlim _commandSignal = new(0);
    private readonly List<(ShaderResourceView? Srv, Texture2D? Texture, long DisposeAfterTick)> _deferredTextureDisposals = new();

    private Process? _process;
    private NamedPipeServerStream? _pipe;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private CancellationTokenSource? _processCts;
    private Task? _commandWriterTask;
    private D3DDevice? _gameDevice;
    private Texture2D? _sharedTexture;
    private ShaderResourceView? _sharedSrv;
    private IntPtr _openedSharedHandle;
    private int _sharedWidth;
    private int _sharedHeight;
    private IntPtr _lastWebViewSharedHandle;
    private int _lastWebViewSharedWidth;
    private int _lastWebViewSharedHeight;
    private IntPtr _lastDirectStreamSharedHandle;
    private int _lastDirectStreamSharedWidth;
    private int _lastDirectStreamSharedHeight;
    private IntPtr _lastFailedSharedHandle;
    private int _lastFailedSharedWidth;
    private int _lastFailedSharedHeight;
    private string _lastFailedSharedSource = string.Empty;
    private long _nextFailedSharedRetryTick;
    private int _failedSharedOpenCount;
    private string? _pipeName;
    private string _statusText = "Ready";
    private string? _detail;
    private bool _disposed;
    private bool _isStarting;
    private bool _connected;
    private bool _browserWindowVisible;
    private long _lastPublishedFrame;
    private long _frameIndex;
    private DateTime _lastFrameUtc;
    private string _lastCursor = "Default";
    private RavaCastCookiePayload[] _latestConsentCookies = [];
    private RavaCastMediaSnapshot _currentMedia = RavaCastMediaSnapshot.Empty();
    private long _lastMediaSyncCommandTick;
    private RavaCastCookiePayload[] _pendingSharedConsentCookies = [];
    private string _lastImportedConsentSignature = string.Empty;
    private readonly ConcurrentDictionary<string, bool> _directStreamPeers = new(StringComparer.Ordinal);
    private bool _directStreamPublisherActive;
    private bool _directStreamReceiverActive;
    private bool _rendererReceiverOnlyMode;
    private bool _usingDirectStreamReceiverTexture;
    private string _activeVisualSource = SharedTextureSourceWebView;
    private bool _nativeMediaAvailable;
    private string _directStreamStatusText = "Direct Stream v2 media layer missing";
    private string? _directStreamDetail = "RavaCast.Media.Native.dll / RavaCast.Media.BridgeHost.exe are not bundled yet; Direct Stream commands are wired, but media will not start until both files are supplied.";
    public event EventHandler<RavaCastDirectStreamSignalProducedEventArgs>? DirectStreamSignalProduced;

    public RavaCastWebView2D3DTextureBackend(ILogger<RavaCastWebView2D3DTextureBackend> logger, IDalamudPluginInterface pluginInterface, RavaCastBackendInstallerService installer, MareConfigService config)
    {
        _logger = logger;
        _pluginInterface = pluginInterface;
        _installer = installer;
        _config = config;
        _nativeMediaAvailable = NativeMediaBoundaryAvailable;
        if (_nativeMediaAvailable)
        {
            _directStreamStatusText = "Direct Stream v2 bridge ready";
            _directStreamDetail = DirectStreamV2TransportDetail;
        }
        else
        {
            _directStreamStatusText = "Direct Stream files missing";
            _directStreamDetail = "Missing Direct Stream files: " + string.Join(", ", _installer.MissingDirectStreamNativeFiles);
        }
    }

    public string RendererPath => _installer.RendererPath;
    public bool CanStart => OperatingSystem.IsWindows() && _installer.IsInstalled && File.Exists(RendererPath);
    public bool HasRendererConnection => _connected;
    public string MissingReason => OperatingSystem.IsWindows()
        ? BuildMissingReason()
        : "RavaCast WebView2 renderer is Windows-first for this build.";

    public RavaCastBackendStatus Status
    {
        get
        {
            lock (_stateLock)
            {
                bool hasFrame;
                bool usingDirectStreamReceiverTexture;
                lock (_frameLock)
                {
                    hasFrame = _sharedSrv is not null;
                    usingDirectStreamReceiverTexture = _usingDirectStreamReceiverTexture;
                }

                if (!OperatingSystem.IsWindows())
                    return RavaCastBackendStatus.Unavailable("RavaCast video output is Windows-first for this build.");
                if (!CanStart)
                    return new RavaCastBackendStatus(false, false, false, "RavaCast video", "Renderer missing", MissingReason);

                if (_directStreamReceiverActive)
                {
                    var receiverStatus = usingDirectStreamReceiverTexture && hasFrame
                        ? "Showing host video"
                        : "Waiting for host video";
                    return new RavaCastBackendStatus(true, true, hasFrame, "Direct Stream receiver", receiverStatus, FriendlyDirectStreamBackendDetail(_directStreamDetail));
                }

                var backendName = _directStreamPublisherActive ? "Host preview" : "Browser output";
                return new RavaCastBackendStatus(true, IsOpen, hasFrame, backendName, FriendlyBrowserBackendStatus(_statusText), FriendlyBrowserBackendDetail(_detail));
            }
        }
    }

    public RavaCastDirectStreamBackendStatus DirectStreamStatus
    {
        get
        {
            lock (_stateLock)
            {
                _nativeMediaAvailable = NativeMediaBoundaryAvailable;
                if (!_nativeMediaAvailable)
                {
                    _directStreamStatusText = "Direct Stream files missing";
                    _directStreamDetail = "Missing Direct Stream files: " + string.Join(", ", _installer.MissingDirectStreamNativeFiles);
                }
                return new RavaCastDirectStreamBackendStatus(_directStreamPublisherActive, _directStreamReceiverActive, _nativeMediaAvailable, _directStreamStatusText, _directStreamDetail, _directStreamPeers.Count);
            }
        }
    }

    public RavaCastMediaSnapshot CurrentMedia
    {
        get { lock (_stateLock) return _currentMedia; }
    }

    public bool IsOpen { get; private set; }
    public string CurrentUrl { get; private set; } = string.Empty;
    private string _lastNavigationRequestUrl = string.Empty;
    public bool Muted { get; private set; }
    public float Volume { get; private set; } = 0.5f;
    public bool BrowserWindowVisible => _browserWindowVisible;

    public void Open(string url, bool muted, float volume)
    {
        if (!CanStart)
        {
            SetStatus("Renderer missing", MissingReason);
            return;
        }

        if (_rendererReceiverOnlyMode)
            StopRendererProcess();

        CurrentUrl = url ?? string.Empty;
        _lastNavigationRequestUrl = CurrentUrl;
        lock (_stateLock)
            _currentMedia = RavaCastMediaSnapshot.Empty(CurrentUrl);
        Muted = muted;
        Volume = NormaliseVolume(volume);
        ActivateBrowserVisualSource(stopReceiver: true, clearDirectStreamFrame: true);
        IsOpen = !string.IsNullOrWhiteSpace(CurrentUrl);
        if (!IsOpen) return;

        EnsureRendererStarted();
        Send(new { op = "navigate", url = CurrentUrl });
        SendAudio();
        SetStatus("Starting browser output", "Waiting for the first browser frame.");
    }

    public void ApplyState(string url)
    {
        if (!CanStart) return;

        // URL Share must own the visible browser texture. If a previous Direct Stream receiver left
        // its visual source active, switch back to the WebView source instead of ignoring browser frames.
        // Direct Stream publishers use the browser as the capture source, but once opened,
        // the browser owns its own live navigation state. Re-applying the advertised URL
        // from framework/state ticks can force modern sites into a reload loop.
        var requestedUrl = url ?? string.Empty;
        var urlChanged = !string.Equals(_lastNavigationRequestUrl, requestedUrl, StringComparison.OrdinalIgnoreCase);
        if (urlChanged)
        {
            lock (_stateLock)
                _currentMedia = RavaCastMediaSnapshot.Empty(requestedUrl);
        }
        if (_directStreamReceiverActive)
            ActivateBrowserVisualSource(stopReceiver: true, clearDirectStreamFrame: true);
        if (_directStreamPublisherActive && IsOpen)
            return;

        if (!IsOpen && !string.IsNullOrWhiteSpace(requestedUrl))
        {
            Open(requestedUrl, Muted, Volume);
            return;
        }

        // CurrentUrl is updated from the real browser once a site redirects or mutates history. Do not use it
        // as the navigation equality check, or viewers get forced back to the original host URL over and over.
        // Track the last URL we explicitly asked the renderer to navigate to instead.
        if (!string.IsNullOrWhiteSpace(requestedUrl) && urlChanged)
        {
            _lastNavigationRequestUrl = requestedUrl;
            CurrentUrl = requestedUrl;
            Send(new { op = "navigate", url = requestedUrl });
        }
    }

    public void ApplyMediaState(double positionSeconds, bool isPlaying, bool force)
    {
        if (!IsOpen || _directStreamReceiverActive) return;

        // URL Share only uses host media time as the initial join/new-URL position.
        // Do not periodically chase/drift-correct viewers after playback begins.
        if (!force) return;

        positionSeconds = Math.Max(0, positionSeconds);
        Send(new { op = "syncMedia", positionSeconds, isPlaying, force });
    }

    public void SetMuted(bool muted)
    {
        if (Muted == muted) return;
        Muted = muted;
        SendAudio();
    }

    public void SetVolume(float volume)
    {
        volume = NormaliseVolume(volume);
        if (Math.Abs(Volume - volume) < 0.001f) return;
        Volume = volume;
        SendAudio();
    }

    public void ShowInteractiveWindow()
    {
        _browserWindowVisible = true;
        Send(new { op = "showWindow" });
        SetStatus(_statusText, "RavaCast browser window shown.");
    }

    public void HideInteractiveWindow()
    {
        _browserWindowVisible = false;
        Send(new { op = "hideWindow" });
        SetStatus(_statusText, "RavaCast browser window hidden behind the game.");
    }

    public void SendPointerMove(float normalisedX, float normalisedY)
    {
        // Passive hover packets can leave Chromium/WebView2 believing its off-screen HWND owns the
        // system cursor. Only action packets are sent to the renderer; clicks/wheel/drag still work.
    }

    public void SendPointerClick(float normalisedX, float normalisedY)
    {
        if (!IsOpen) return;
        Send(new { op = "mouse", x = ClampNormalised(normalisedX), y = ClampNormalised(normalisedY), down = 1, up = 1, @double = 0, wheelX = 0, wheelY = 0, leaving = false });
    }

    public void SendPointerWheel(float normalisedX, float normalisedY, float wheelDelta)
    {
        if (!IsOpen || Math.Abs(wheelDelta) < 0.01f) return;
        Send(new { op = "mouse", x = ClampNormalised(normalisedX), y = ClampNormalised(normalisedY), down = 0, up = 0, @double = 0, wheelX = 0, wheelY = -(int)Math.Round(Math.Clamp(wheelDelta, -8f, 8f) * 100f), leaving = false });
    }

    public void SendBrowserMouse(float normalisedX, float normalisedY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt)
    {
        if (!IsOpen) return;
        var roundedWheelX = (int)Math.Round(Math.Clamp(wheelX, -8f, 8f) * 100f);
        var roundedWheelY = -(int)Math.Round(Math.Clamp(wheelY, -8f, 8f) * 100f);
        Send(new
        {
            op = "mouse",
            x = ClampNormalised(normalisedX),
            y = ClampNormalised(normalisedY),
            down = downMask,
            up = upMask,
            held = heldMask,
            @double = doubleMask,
            wheelX = roundedWheelX,
            wheelY = roundedWheelY,
            leaving,
            shift,
            ctrl,
            alt
        });
    }

    public void SendBrowserMousePixels(int pixelX, int pixelY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt)
    {
        if (!IsOpen) return;
        var roundedWheelX = (int)Math.Round(Math.Clamp(wheelX, -8f, 8f) * 100f);
        var roundedWheelY = -(int)Math.Round(Math.Clamp(wheelY, -8f, 8f) * 100f);
        Send(new
        {
            op = "mouse",
            px = Math.Clamp(pixelX, 0, SurfaceWidth - 1),
            py = Math.Clamp(pixelY, 0, SurfaceHeight - 1),
            down = downMask,
            up = upMask,
            held = heldMask,
            @double = doubleMask,
            wheelX = roundedWheelX,
            wheelY = roundedWheelY,
            leaving,
            shift,
            ctrl,
            alt
        });
    }

    // Passive mouse-move packets are meaningful for the browser preview: many pages only reveal
    // controls, hover state, scrubbers, tooltips, and drag affordances after receiving mouseMoved.
    // Filtering those packets made the preview clickable but not properly track the pointer.

    public void SendBrowserFocus(bool focused)
    {
        if (!IsOpen) return;
        Send(new { op = "focus", focused });
    }

    public void SendTextInput(string text)
    {
        if (!IsOpen || string.IsNullOrEmpty(text)) return;
        if (text.Length > 2048)
            text = text[..2048];
        Send(new { op = "textInput", text });
    }

    public void SendSpecialKey(string key)
    {
        if (!IsOpen || string.IsNullOrWhiteSpace(key)) return;
        Send(new { op = "specialKey", key });
    }

    public void SendBrowserKey(int virtualKey, bool down, string? text, bool shift, bool ctrl, bool alt)
    {
        if (!IsOpen || virtualKey <= 0) return;
        Send(new { op = "key", vk = virtualKey, down, text = text ?? string.Empty, shift, ctrl, alt });
    }

    public void TryDismissConsentPrompt(bool preferReject)
    {
        SetStatus(_statusText, "Use the preview to handle consent prompts.");
    }

    public void ReloadPage()
    {
        if (!IsOpen) return;
        Send(new { op = "reload" });
    }

    public void GoBack()
    {
        if (!IsOpen) return;
        Send(new { op = "back" });
    }

    public RavaCastCookiePayload[] GetShareableConsentCookies()
    {
        lock (_stateLock)
            return _latestConsentCookies.ToArray();
    }

    public void ApplySharedConsentCookies(string url, IReadOnlyList<RavaCastCookiePayload> cookies)
    {
        if (cookies is null || cookies.Count == 0) return;
        var safe = cookies
            .Where(IsShareableConsentCookie)
            .Take(64)
            .ToArray();
        if (safe.Length == 0) return;

        var signature = BuildConsentSignature(safe);
        if (string.Equals(signature, _lastImportedConsentSignature, StringComparison.Ordinal)) return;
        _lastImportedConsentSignature = signature;
        _pendingSharedConsentCookies = safe;
        // Import consent cookies silently. Reloading a joined viewer's URL Share browser here can combine with
        // normal site redirects/history updates and look like a forced reload loop.
        SendSharedConsentCookies(url, safe, reloadCurrentPage: false);
    }

    public void Close()
    {
        IsOpen = false;
        _browserWindowVisible = false;
        CurrentUrl = string.Empty;
        _lastNavigationRequestUrl = string.Empty;
        _latestConsentCookies = [];
        _pendingSharedConsentCookies = [];
        _lastImportedConsentSignature = string.Empty;
        lock (_stateLock)
            _currentMedia = RavaCastMediaSnapshot.Empty();
        StopDirectStreamPublisher();
        StopDirectStreamReceiver();
        _directStreamPeers.Clear();
        _usingDirectStreamReceiverTexture = false;
        StopRendererProcess();
        ClearCurrentFrame();
        SetStatus("Ready", null);
    }

    private void StopRendererProcess()
    {
        var process = _process;

        try
        {
            if (_connected && _writer is not null)
            {
                _writer.WriteLine(JsonSerializer.Serialize(new { op = "quit" }));
                _writer.Flush();
            }
        }
        catch { }

        try { _processCts?.Cancel(); } catch { }
        try { _commandSignal.Release(); } catch { }
        try { _writer?.Dispose(); } catch { }
        try { _reader?.Dispose(); } catch { }
        try { _pipe?.Dispose(); } catch { }
        try { _processCts?.Dispose(); } catch { }

        while (_pendingCommands.TryDequeue(out _)) { }
        _writer = null;
        _reader = null;
        _pipe = null;
        _pipeName = null;
        _processCts = null;
        _commandWriterTask = null;
        _connected = false;
        _isStarting = false;
        _rendererReceiverOnlyMode = false;
        _process = null;
        ResetCachedSharedTextureHandles();

        if (process is null) return;
        _ = Task.Run(async () =>
        {
            try
            {
                if (!process.HasExited)
                {
                    // RavaCast's renderer owns a real WebView2/Chromium window. If that process is
                    // the thing negotiating the system cursor, letting it linger for 1.5s after Stop
                    // leaves the cursor blinking for a visible beat. Give graceful quit one short
                    // frame-window, then tear the renderer tree down.
                    await Task.Delay(250).ConfigureAwait(false);
                    if (!process.HasExited)
                        process.Kill(entireProcessTree: true);
                }
            }
            catch { }
            finally
            {
                try { process.Dispose(); } catch { }
            }
        });
    }

    public RavaCastTextureFrame? TryGetCurrentFrame()
    {
        DisposeDeferredTextures(force: false);

        lock (_frameLock)
        {
            if (_sharedSrv is null || _sharedWidth <= 0 || _sharedHeight <= 0) return null;
            return new RavaCastTextureFrame(new ImTextureID(_sharedSrv.NativePointer), _sharedWidth, _sharedHeight, _frameIndex, _lastFrameUtc);
        }
    }



    public bool StartDirectStreamPublisher(Guid castId, RavaCastDirectStreamQuality quality, out string error)
    {
        error = string.Empty;
        if (!CanStart)
        {
            error = MissingReason;
            SetDirectStreamStatus(false, _directStreamReceiverActive, "Direct Stream renderer unavailable", error);
            return false;
        }

        if (!NativeMediaBoundaryAvailable)
        {
            error = string.Join(", ", _installer.MissingDirectStreamNativeFiles);
            if (string.IsNullOrWhiteSpace(error))
                error = "Direct Stream runtime files are not packaged beside the renderer.";
            _nativeMediaAvailable = false;
            SetDirectStreamStatus(false, _directStreamReceiverActive, "Direct Stream files missing", "Missing Direct Stream files: " + error);
            return false;
        }
        _nativeMediaAvailable = true;

        var preset = RavaCastDirectStreamPresets.Get(quality);
        EnsureRendererStarted();
        SetDirectStreamStatus(true, _directStreamReceiverActive, "Starting host video", $"Preparing Direct Stream for viewers at {preset.Label}. {DirectStreamV2TransportDetail}");
        Send(new
        {
            op = "directStreamStartPublisher",
            castId = castId.ToString("D"),
            width = preset.Width,
            height = preset.Height,
            fps = preset.Fps,
            videoBitrateKbps = preset.VideoBitrateKbps,
            audioBitrateKbps = preset.AudioBitrateKbps
        });
        return true;
    }



    public void StopDirectStreamPublisher()
    {
        if (_directStreamPublisherActive)
            Send(new { op = "directStreamStopPublisher" });
        _directStreamPublisherActive = false;
        _directStreamPeers.Clear();
        SetDirectStreamStatus(false, _directStreamReceiverActive, _directStreamReceiverActive ? "Direct Stream receiver active" : "Direct Stream stopped", null);
    }

    public bool StartDirectStreamReceiver(Guid castId, string hostSessionId, string viewerSessionId, RavaCastDirectStreamQuality quality, out string error)
    {
        error = string.Empty;
        if (!CanStart)
        {
            error = MissingReason;
            SetDirectStreamStatus(_directStreamPublisherActive, false, "Direct Stream renderer unavailable", error);
            return false;
        }

        if (!NativeMediaBoundaryAvailable)
        {
            error = string.Join(", ", _installer.MissingDirectStreamNativeFiles);
            if (string.IsNullOrWhiteSpace(error))
                error = "Direct Stream runtime files are not packaged beside the renderer.";
            _nativeMediaAvailable = false;
            SetDirectStreamStatus(_directStreamPublisherActive, false, "Direct Stream files missing", "Missing Direct Stream files: " + error);
            return false;
        }
        _nativeMediaAvailable = true;

        var preset = RavaCastDirectStreamPresets.Get(quality);
        EnsureRendererStarted(directStreamReceiverOnly: !IsOpen && !_directStreamPublisherActive);
        ActivateDirectStreamReceiverVisualSource();
        SetDirectStreamStatus(_directStreamPublisherActive, true, "Connecting to host video", $"Waiting for the host stream at {preset.Label}. {DirectStreamV2TransportDetail}");
        Send(new
        {
            op = "directStreamStartReceiver",
            castId = castId.ToString("D"),
            hostSessionId,
            viewerSessionId,
            width = preset.Width,
            height = preset.Height,
            fps = preset.Fps,
            videoBitrateKbps = preset.VideoBitrateKbps,
            audioBitrateKbps = preset.AudioBitrateKbps
        });
        SendAudio();
        return true;
    }

    public void StopDirectStreamReceiver()
    {
        if (_directStreamReceiverActive)
            Send(new { op = "directStreamStopReceiver" });
        _directStreamReceiverActive = false;
        ActivateBrowserVisualSource(stopReceiver: false, clearDirectStreamFrame: true);
        SetDirectStreamStatus(_directStreamPublisherActive, false, _directStreamPublisherActive ? "Direct Stream publisher active" : "Direct Stream stopped", null);
    }

    public void AddDirectStreamPeer(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return;
        _directStreamPeers[peerId] = true;
        Send(new { op = "directStreamAddPeer", peerId });
        SetDirectStreamStatus(_directStreamPublisherActive, _directStreamReceiverActive, "Viewer connected", $"Viewers: {_directStreamPeers.Count}");
    }

    public void RemoveDirectStreamPeer(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return;
        _directStreamPeers.TryRemove(peerId, out _);
        Send(new { op = "directStreamRemovePeer", peerId });
        SetDirectStreamStatus(_directStreamPublisherActive, _directStreamReceiverActive, _directStreamPeers.Count == 0 ? "Waiting for viewers" : "Viewer left", $"Viewers: {_directStreamPeers.Count}");
    }

    public void HandleDirectStreamSignal(string peerId, string signalType, string payloadJson)
    {
        if (string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType)) return;
        Send(new { op = "directStreamSignal", peerId, signalType, payloadJson = payloadJson ?? string.Empty });
    }

    private void EnsureRendererStarted(bool directStreamReceiverOnly = false)
    {
        if (_disposed) return;

        if (!directStreamReceiverOnly && _rendererReceiverOnlyMode && (_connected || _isStarting))
            StopRendererProcess();

        if (_connected || _isStarting) return;
        _rendererReceiverOnlyMode = directStreamReceiverOnly;
        _isStarting = true;
        _processCts = new CancellationTokenSource();
        _ = Task.Run(() => StartRendererAsync(_processCts.Token, directStreamReceiverOnly));
    }

    private void ApplyBrowserAccelerationEnvironment(ProcessStartInfo psi)
    {
        var disable = _config.Current.RavaCastDisableHardwareAcceleration;
        var darkMode = _config.Current.RavaCastBrowserDarkMode;

        // These are consumed by RavaCast.Renderer when it builds the WebView2 browser process flags.
        // Ticked/default: keep the existing compatibility path (GPU + accelerated video decode disabled).
        // Unticked: explicitly opt back into hardware acceleration and accelerated video decode for A/B testing.
        psi.Environment["RAVACAST_WEBVIEW2_DISABLE_GPU"] = disable ? "1" : "0";
        psi.Environment["RAVACAST_WEBVIEW2_DISABLE_HARDWARE_ACCELERATION"] = disable ? "1" : "0";
        psi.Environment["RAVACAST_WEBVIEW2_DISABLE_ACCELERATED_VIDEO_DECODE"] = disable ? "1" : "0";
        psi.Environment["RAVACAST_WEBVIEW2_HARDWARE_ACCELERATION"] = disable ? "0" : "1";
        psi.Environment["RAVACAST_WEBVIEW2_ACCELERATED_VIDEO_DECODE"] = disable ? "0" : "1";
        psi.Environment["RAVACAST_WEBVIEW2_DARK_MODE"] = darkMode ? "1" : "0";
    }

    private async Task StartRendererAsync(CancellationToken token, bool directStreamReceiverOnly)
    {
        try
        {
            if (!directStreamReceiverOnly)
            {
                if (!_installer.IsWebView2RuntimeInstalled)
                    SetStatus("Installing WebView2 runtime", "RavaCast needs Microsoft Edge WebView2 Evergreen Runtime. Installing it silently before the browser starts.");

                if (!await _installer.EnsureWebView2RuntimeReadyAsync(token).ConfigureAwait(false))
                {
                    SetStatus("WebView2 runtime unavailable", _installer.Detail ?? "Microsoft Edge WebView2 Evergreen Runtime could not be detected or installed.");
                    return;
                }
            }

            var identity = $"{Environment.ProcessId}_{Guid.NewGuid():N}";
            _pipeName = "RavaCastWebView2_" + identity;
            _pipe = new NamedPipeServerStream(_pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

            var ravaCastDir = Path.Combine(_pluginInterface.ConfigDirectory.FullName, "RavaCast");
            var cacheDir = Path.Combine(ravaCastDir, "WebView2Profile");
            var rendererLogPath = Path.Combine(ravaCastDir, "RavaCast.Renderer.log");
            Directory.CreateDirectory(cacheDir);

            // RavaCast renderer file logging is intentionally error-only. Do not append
            // a launch banner or healthy-path diagnostics for every cast/session.

            var psi = new ProcessStartInfo
            {
                FileName = RendererPath,
                UseShellExecute = false,
                RedirectStandardInput = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = Path.GetDirectoryName(RendererPath) ?? _pluginInterface.ConfigDirectory.FullName
            };
            ApplyBrowserAccelerationEnvironment(psi);
            var gameAdapterLuid = TryGetGameAdapterLuid();
            if (gameAdapterLuid.HasValue)
            {
                psi.ArgumentList.Add("--adapter-luid");
                psi.ArgumentList.Add(gameAdapterLuid.Value.ToString(CultureInfo.InvariantCulture));
                psi.Environment["RAVACAST_D3D_ADAPTER_LUID"] = gameAdapterLuid.Value.ToString(CultureInfo.InvariantCulture);
            }

            psi.ArgumentList.Add("--pipe");
            psi.ArgumentList.Add(_pipeName);
            psi.ArgumentList.Add("--width");
            psi.ArgumentList.Add(SurfaceWidth.ToString(CultureInfo.InvariantCulture));
            psi.ArgumentList.Add("--height");
            psi.ArgumentList.Add(SurfaceHeight.ToString(CultureInfo.InvariantCulture));
            psi.ArgumentList.Add("--profile");
            psi.ArgumentList.Add(cacheDir);
            psi.ArgumentList.Add("--log");
            psi.ArgumentList.Add(rendererLogPath);
            psi.ArgumentList.Add("--parent");
            psi.ArgumentList.Add(Environment.ProcessId.ToString(CultureInfo.InvariantCulture));
            if (directStreamReceiverOnly)
                psi.ArgumentList.Add("--direct-stream-receiver-only");

            _process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start RavaCast.Renderer.exe");
            _process.OutputDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data) && IsRavaCastErrorText(e.Data)) _logger.LogWarning("RavaCast renderer: {line}", e.Data); };
            _process.ErrorDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data) && IsRavaCastErrorText(e.Data)) _logger.LogWarning("RavaCast renderer: {line}", e.Data); };
            try { _process.BeginOutputReadLine(); } catch { }
            try { _process.BeginErrorReadLine(); } catch { }
            SetStatus(directStreamReceiverOnly ? "Starting Direct Stream receiver" : "Starting video output", null);
            await _pipe.WaitForConnectionAsync(token).ConfigureAwait(false);
            _reader = new StreamReader(_pipe);
            _writer = new StreamWriter(_pipe) { AutoFlush = true };
            _connected = true;
            _isStarting = false;
            _commandWriterTask = Task.Run(() => WriteLoopAsync(token), CancellationToken.None);
            SetStatus(directStreamReceiverOnly ? "Direct Stream receiver connected" : "Video output connected", null);

            if (!directStreamReceiverOnly && !string.IsNullOrWhiteSpace(CurrentUrl))
            {
                SendPendingSharedConsentCookies(reloadCurrentPage: false);
                Send(new { op = "navigate", url = CurrentUrl });
                SendAudio();
                Send(new { op = _browserWindowVisible ? "showWindow" : "hideWindow" });
            }

            await ReadLoopAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RavaCast WebView2 renderer failed");
            SetStatus("Video output failed", ex.Message);
        }
        finally
        {
            _isStarting = false;
            _connected = false;
            if (!_disposed && IsOpen)
                SetStatus("Video output stopped", _process is { HasExited: true } ? $"Renderer exited with code {_process.ExitCode}." : null);
            else if (!_disposed)
                SetStatus("Ready", null);
        }
    }

    private async Task ReadLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested && _reader is not null)
        {
            var line = await _reader.ReadLineAsync(token).ConfigureAwait(false);
            if (line is null) break;
            HandleRendererMessage(line);
        }
    }

    private void HandleRendererMessage(string line)
    {
        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            var op = root.TryGetProperty("op", out var opProp) ? opProp.GetString() : string.Empty;
            if (string.Equals(op, "error", StringComparison.OrdinalIgnoreCase) && root.TryGetProperty("message", out var errorTextProp))
                _logger.LogWarning("RavaCast renderer error: {error}", errorTextProp.GetString());
            switch (op)
            {
                case "sharedTexture":
                    var handle = root.TryGetProperty("handle", out var handleProp) ? new IntPtr(handleProp.GetInt64()) : IntPtr.Zero;
                    var width = root.TryGetProperty("width", out var sw) ? sw.GetInt32() : SurfaceWidth;
                    var height = root.TryGetProperty("height", out var sh) ? sh.GetInt32() : SurfaceHeight;
                    var source = root.TryGetProperty("source", out var sourceProp) ? sourceProp.GetString() ?? SharedTextureSourceWebView : SharedTextureSourceWebView;
                    CacheSharedTextureHandle(source, handle, width, height);
                    if (!ShouldAcceptVisualSource(source))
                    {
                        SetStatusIfChanged(DescribeIgnoredVisualSource(source), null);
                        break;
                    }
                    if (ShouldThrottleSharedTextureOpen(handle, width, height, source))
                        break;
                    _ = Task.Run(() => OpenSharedTexture(handle, width, height, source));
                    break;
                case "ready":
                    SetStatus("WebView2 renderer ready", null);
                    if (!string.IsNullOrWhiteSpace(CurrentUrl))
                    {
                        SendPendingSharedConsentCookies(reloadCurrentPage: false);
                        Send(new { op = "navigate", url = CurrentUrl });
                        SendAudio();
                        Send(new { op = _browserWindowVisible ? "showWindow" : "hideWindow" });
                    }
                    break;
                case "frame":
                    var frameSource = root.TryGetProperty("source", out var frameSourceProp) ? frameSourceProp.GetString() ?? SharedTextureSourceWebView : SharedTextureSourceWebView;
                    if (!ShouldAcceptVisualSource(frameSource))
                        break;
                    EnsureActiveVisualSourceBound(frameSource);
                    var frame = root.TryGetProperty("frame", out var f) ? f.GetInt64() : 0;
                    if (frame > _lastPublishedFrame)
                    {
                        _lastPublishedFrame = frame;
                        lock (_frameLock)
                        {
                            _frameIndex = frame;
                            _lastFrameUtc = DateTime.UtcNow;
                        }
                        if (string.Equals(frameSource, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase))
                        {
                            _usingDirectStreamReceiverTexture = true;
                            SetStatusIfChanged("Showing host video", null);
                            SetDirectStreamStatus(_directStreamPublisherActive, true, "Host video connected", null);
                        }
                        else
                        {
                            SetStatusIfChanged("Browser output ready", null);
                        }
                    }
                    break;
                case "urlChanged":
                    if (root.TryGetProperty("url", out var urlProp))
                    {
                        var nextUrl = urlProp.GetString();
                        if (!string.IsNullOrWhiteSpace(nextUrl))
                        {
                            CurrentUrl = nextUrl;
                            lock (_stateLock)
                            {
                                if (!string.Equals(_currentMedia.Url, nextUrl, StringComparison.OrdinalIgnoreCase))
                                    _currentMedia = _currentMedia with { Url = nextUrl, PositionSeconds = 0, StateUtc = DateTime.UtcNow };
                            }
                        }
                    }
                    break;
                case "mediaState":
                    UpdateMediaState(root);
                    break;
                case "status":
                    SetStatus(root.TryGetProperty("text", out var text) ? text.GetString() ?? "Video output status" : "Video output status", null);
                    break;
                case "error":
                    SetStatus("Video output error", root.TryGetProperty("message", out var msg) ? msg.GetString() : null);
                    break;
                case "cursor":
                    _lastCursor = root.TryGetProperty("cursor", out var cursor) ? cursor.GetString() ?? "Default" : "Default";
                    break;
                case "consentCookies":
                    UpdateLatestConsentCookies(root);
                    break;
                case "directStreamSignal":
                    HandleDirectStreamSignalFromRenderer(root);
                    break;
                case "directStreamStatus":
                    UpdateDirectStreamStatus(root);
                    break;
                case "directStreamError":
                    UpdateDirectStreamError(root);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to process RavaCast renderer message: {line}", line);
        }
    }

    private bool ShouldThrottleSharedTextureOpen(IntPtr handle, int width, int height, string source)
    {
        if (handle == IntPtr.Zero) return false;
        var now = Environment.TickCount64;
        lock (_frameLock)
        {
            if (_lastFailedSharedHandle != handle
                || _lastFailedSharedWidth != width
                || _lastFailedSharedHeight != height
                || !string.Equals(_lastFailedSharedSource, source ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                return false;
            return now < _nextFailedSharedRetryTick;
        }
    }

    private void RecordFailedSharedTextureOpen(IntPtr handle, int width, int height, string source)
    {
        if (handle == IntPtr.Zero) return;
        lock (_frameLock)
        {
            if (_lastFailedSharedHandle == handle
                && _lastFailedSharedWidth == width
                && _lastFailedSharedHeight == height
                && string.Equals(_lastFailedSharedSource, source ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            {
                _failedSharedOpenCount = Math.Min(_failedSharedOpenCount + 1, 6);
            }
            else
            {
                _lastFailedSharedHandle = handle;
                _lastFailedSharedWidth = width;
                _lastFailedSharedHeight = height;
                _lastFailedSharedSource = source ?? string.Empty;
                _failedSharedOpenCount = 1;
            }

            var delayMs = Math.Min(5000, 250 * (1 << Math.Max(0, _failedSharedOpenCount - 1)));
            _nextFailedSharedRetryTick = Environment.TickCount64 + delayMs;
        }
    }

    private void ClearFailedSharedTextureOpen(IntPtr handle, int width, int height, string source)
    {
        lock (_frameLock)
        {
            if (_lastFailedSharedHandle != handle
                || _lastFailedSharedWidth != width
                || _lastFailedSharedHeight != height
                || !string.Equals(_lastFailedSharedSource, source ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                return;

            _lastFailedSharedHandle = IntPtr.Zero;
            _lastFailedSharedWidth = 0;
            _lastFailedSharedHeight = 0;
            _lastFailedSharedSource = string.Empty;
            _nextFailedSharedRetryTick = 0;
            _failedSharedOpenCount = 0;
        }
    }

    private unsafe void OpenSharedTexture(IntPtr handle, int width, int height, string source)
    {
        var directStreamReceiverTexture = string.Equals(source, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase);
        if (handle == IntPtr.Zero)
        {
            SetStatus(directStreamReceiverTexture ? "Host video missing" : "Browser output missing", "The renderer sent an empty video texture handle.");
            return;
        }

        if (!ShouldAcceptVisualSource(source))
            return;

        var nextWidth = Math.Max(1, width);
        var nextHeight = Math.Max(1, height);

        lock (_frameLock)
        {
            if (_openedSharedHandle == handle && _sharedSrv is not null)
                return;
        }

        Texture2D? openedTexture = null;
        ShaderResourceView? openedSrv = null;

        try
        {
            var gameDevice = GetOrCreateGameDevice();

            // OpenSharedResource can occasionally stall while the producer process/GPU is handing off a new
            // shared texture, especially when the receiver video path is starting. Never hold _frameLock while
            // doing this. The UI/world draw thread takes _frameLock every frame to fetch the current SRV; holding
            // it around a potentially blocking driver call can freeze the whole game even though the bridge lives
            // out-of-process.
            openedTexture = gameDevice.OpenSharedResource<Texture2D>(handle);
            openedSrv = new ShaderResourceView(gameDevice, openedTexture);

            lock (_frameLock)
            {
                if (_disposed)
                {
                    try { openedSrv.Dispose(); } catch { }
                    try { openedTexture.Dispose(); } catch { }
                    return;
                }

                if (_openedSharedHandle == handle && _sharedSrv is not null)
                {
                    try { openedSrv.Dispose(); } catch { }
                    try { openedTexture.Dispose(); } catch { }
                    return;
                }

                QueueSharedTextureForDeferredDisposeLocked();
                _sharedTexture = openedTexture;
                _sharedSrv = openedSrv;
                openedTexture = null;
                openedSrv = null;
                _openedSharedHandle = handle;
                _sharedWidth = nextWidth;
                _sharedHeight = nextHeight;
                _usingDirectStreamReceiverTexture = directStreamReceiverTexture;
                _frameIndex = Math.Max(1, _frameIndex);
                _lastFrameUtc = DateTime.UtcNow;
            }

            ClearFailedSharedTextureOpen(handle, nextWidth, nextHeight, source);

            if (directStreamReceiverTexture)
            {
                SetStatus("Showing host video", null);
                SetDirectStreamStatus(_directStreamPublisherActive, true, "Host video connected", null);
            }
            else
            {
                SetStatus("Browser output ready", null);
            }
        }
        catch (Exception ex)
        {
            try { openedSrv?.Dispose(); } catch { }
            try { openedTexture?.Dispose(); } catch { }
            RecordFailedSharedTextureOpen(handle, nextWidth, nextHeight, source);
            _logger.LogWarning(ex, "Failed to open RavaCast shared D3D texture (source={Source}, handle=0x{Handle:X}, size={Width}x{Height})", source, handle.ToInt64(), nextWidth, nextHeight);
            SetStatus(directStreamReceiverTexture ? "Host video failed" : "Browser output failed", ex.Message);
        }
    }

    private D3DDevice GetOrCreateGameDevice()
    {
        lock (_frameLock)
        {
            if (_gameDevice is not null)
                return _gameDevice;
        }

        D3DDevice created;
        unsafe
        {
            var gameDevice = GameDevice.Instance();
            if (gameDevice is null || gameDevice->D3D11Forwarder == null)
                throw new InvalidOperationException("Game D3D11 device is unavailable.");

            var ptr = (IntPtr)gameDevice->D3D11Forwarder;
            Marshal.AddRef(ptr);
            created = new D3DDevice(ptr);
        }

        lock (_frameLock)
        {
            if (_gameDevice is null)
            {
                _gameDevice = created;
                return _gameDevice;
            }
        }

        try { created.Dispose(); } catch { }
        lock (_frameLock)
            return _gameDevice ?? throw new ObjectDisposedException(nameof(RavaCastWebView2D3DTextureBackend));
    }

    private long? TryGetGameAdapterLuid()
    {
        try
        {
            var gameDevice = GetOrCreateGameDevice();
            using var dxgiDevice = gameDevice.QueryInterface<DxgiDevice>();
            using var adapter = dxgiDevice.Adapter;
            var luid = adapter.Description.Luid;
            if (luid == 0) return null;
            return luid;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not resolve the game D3D adapter LUID for RavaCast renderer launch; renderer will use its default adapter.");
            return null;
        }
    }


    private void UpdateMediaState(JsonElement root)
    {
        try
        {
            var current = CurrentMedia;
            var url = root.TryGetProperty("url", out var urlProp) && urlProp.ValueKind == JsonValueKind.String ? urlProp.GetString() ?? CurrentUrl : CurrentUrl;
            var title = root.TryGetProperty("title", out var titleProp) && titleProp.ValueKind == JsonValueKind.String ? titleProp.GetString() ?? string.Empty : string.Empty;
            var isPlaying = root.TryGetProperty("isPlaying", out var playingProp) && playingProp.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? playingProp.GetBoolean()
                : current.IsPlaying;
            var position = TryReadFiniteDouble(root, "positionSeconds", out var pos) ? Math.Max(0, pos) : current.PositionSeconds;
            double? duration = null;
            if (TryReadFiniteDouble(root, "durationSeconds", out var dur) && dur > 0)
                duration = dur;

            lock (_stateLock)
                _currentMedia = new RavaCastMediaSnapshot(url, title, isPlaying, position, duration, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read RavaCast media state from renderer");
        }
    }

    private static bool TryReadFiniteDouble(JsonElement root, string propertyName, out double value)
    {
        value = 0;
        if (!root.TryGetProperty(propertyName, out var prop) || prop.ValueKind != JsonValueKind.Number)
            return false;
        return prop.TryGetDouble(out value) && double.IsFinite(value);
    }

    private void HandleDirectStreamSignalFromRenderer(JsonElement root)
    {
        var peerId = root.TryGetProperty("peerId", out var peer) ? peer.GetString() ?? string.Empty : string.Empty;
        var signalType = root.TryGetProperty("signalType", out var type) ? type.GetString() ?? string.Empty : string.Empty;
        var payloadJson = root.TryGetProperty("payloadJson", out var payload) ? payload.GetString() ?? string.Empty : string.Empty;
        if (string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType)) return;
        DirectStreamSignalProduced?.Invoke(this, new RavaCastDirectStreamSignalProducedEventArgs(peerId, signalType, payloadJson));
    }

    private void UpdateDirectStreamStatus(JsonElement root)
    {
        lock (_stateLock)
        {
            _directStreamStatusText = root.TryGetProperty("text", out var text) ? text.GetString() ?? "Direct Stream status" : "Direct Stream status";
            _directStreamDetail = root.TryGetProperty("detail", out var detail) ? detail.GetString() : null;
            if (root.TryGetProperty("publisherActive", out var publisher)) _directStreamPublisherActive = publisher.GetBoolean();
            if (root.TryGetProperty("receiverActive", out var receiver)) _directStreamReceiverActive = receiver.GetBoolean();
            if (root.TryGetProperty("nativeMediaAvailable", out var nativeMedia)) _nativeMediaAvailable = nativeMedia.GetBoolean();
            // Native connectedPeers is the transport/media-ready count, not the lobby viewer count.
            // Do not shrink _directStreamPeers from it: viewers have already joined at the RavaCast layer,
            // and showing zero here makes the host think nobody joined while the media transport is still negotiating.
        }

        if (_directStreamReceiverActive && IsDirectStreamReceiverVisualSourceActive)
        {
            bool hasReceiverFrame;
            lock (_frameLock)
                hasReceiverFrame = _usingDirectStreamReceiverTexture && _sharedSrv is not null;
            SetStatusIfChanged(hasReceiverFrame ? "Showing host video" : "Waiting for host video", null);
        }
    }

    private void UpdateDirectStreamError(JsonElement root)
    {
        lock (_stateLock)
        {
            _directStreamStatusText = "Direct Stream error";
            _directStreamDetail = root.TryGetProperty("message", out var msg) ? msg.GetString() : null;
            if (root.TryGetProperty("publisherActive", out var publisher)) _directStreamPublisherActive = publisher.GetBoolean();
            if (root.TryGetProperty("receiverActive", out var receiver)) _directStreamReceiverActive = receiver.GetBoolean();
            if (root.TryGetProperty("nativeMediaAvailable", out var nativeMedia)) _nativeMediaAvailable = nativeMedia.GetBoolean();
        }
    }

    private void SendPendingSharedConsentCookies(bool reloadCurrentPage)
    {
        var cookies = _pendingSharedConsentCookies;
        if (cookies.Length == 0) return;
        SendSharedConsentCookies(CurrentUrl, cookies, reloadCurrentPage);
    }

    private void SendSharedConsentCookies(string url, RavaCastCookiePayload[] cookies, bool reloadCurrentPage)
    {
        if (cookies.Length == 0) return;
        Send(new
        {
            op = "importConsentCookies",
            url = url ?? string.Empty,
            reload = reloadCurrentPage,
            cookies = cookies.Select(c => new
            {
                c.Name,
                c.Value,
                c.Domain,
                c.Path,
                c.ExpiresUnixMs,
                c.Secure,
                c.SameSite
            }).ToArray()
        });
    }

    private void UpdateLatestConsentCookies(JsonElement root)
    {
        try
        {
            if (!root.TryGetProperty("cookies", out var cookiesElement)) return;
            var cookies = JsonSerializer.Deserialize<RavaCastCookiePayload[]>(cookiesElement.GetRawText()) ?? [];
            lock (_stateLock)
                _latestConsentCookies = cookies.Where(IsShareableConsentCookie).Take(64).ToArray();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read shareable RavaCast consent cookies from renderer");
        }
    }

    private static bool IsShareableConsentCookie(RavaCastCookiePayload cookie)
    {
        if (string.IsNullOrWhiteSpace(cookie.Name) || string.IsNullOrWhiteSpace(cookie.Domain)) return false;
        if (string.IsNullOrWhiteSpace(cookie.Value) || cookie.Value.Length > 4096) return false;
        var name = cookie.Name.ToLowerInvariant();
        if (name.Contains("session", StringComparison.Ordinal)
            || name.Contains("auth", StringComparison.Ordinal)
            || name.Contains("token", StringComparison.Ordinal)
            || name.Contains("login", StringComparison.Ordinal)
            || name.Contains("account", StringComparison.Ordinal)
            || name.Contains("passwd", StringComparison.Ordinal)
            || name.Contains("password", StringComparison.Ordinal)
            || name.Contains("csrf", StringComparison.Ordinal)
            || name.Contains("xsrf", StringComparison.Ordinal)
            || name is "sid" or "ssid" or "uid" or "userid" or "user_id")
            return false;

        return name.Contains("consent", StringComparison.Ordinal)
            || name.Contains("cookie", StringComparison.Ordinal)
            || name.Contains("privacy", StringComparison.Ordinal)
            || name.Contains("gdpr", StringComparison.Ordinal)
            || name.Contains("notice", StringComparison.Ordinal)
            || name.Contains("optanon", StringComparison.Ordinal)
            || name.Contains("onetrust", StringComparison.Ordinal)
            || name.Contains("didomi", StringComparison.Ordinal)
            || name.Contains("euconsent", StringComparison.Ordinal)
            || name.Contains("tcf", StringComparison.Ordinal)
            || name.Equals("socs", StringComparison.Ordinal)
            || name.Equals("consent", StringComparison.Ordinal);
    }

    private static string BuildConsentSignature(IReadOnlyList<RavaCastCookiePayload> cookies)
        => string.Join("\n", cookies
            .OrderBy(c => c.Domain, StringComparer.OrdinalIgnoreCase)
            .ThenBy(c => c.Path, StringComparer.Ordinal)
            .ThenBy(c => c.Name, StringComparer.Ordinal)
            .Select(c => $"{c.Domain}|{c.Path}|{c.Name}|{c.Value}|{c.ExpiresUnixMs}|{c.Secure}|{c.SameSite}"));


    private void ResetCachedSharedTextureHandles()
    {
        _lastWebViewSharedHandle = IntPtr.Zero;
        _lastWebViewSharedWidth = 0;
        _lastWebViewSharedHeight = 0;
        _lastDirectStreamSharedHandle = IntPtr.Zero;
        _lastDirectStreamSharedWidth = 0;
        _lastDirectStreamSharedHeight = 0;
    }

    private void CacheSharedTextureHandle(string? source, IntPtr handle, int width, int height)
    {
        if (handle == IntPtr.Zero) return;
        var normalised = string.IsNullOrWhiteSpace(source) ? SharedTextureSourceWebView : source;
        if (string.Equals(normalised, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase))
        {
            _lastDirectStreamSharedHandle = handle;
            _lastDirectStreamSharedWidth = Math.Max(1, width);
            _lastDirectStreamSharedHeight = Math.Max(1, height);
            return;
        }

        _lastWebViewSharedHandle = handle;
        _lastWebViewSharedWidth = Math.Max(1, width);
        _lastWebViewSharedHeight = Math.Max(1, height);
    }

    private void EnsureActiveVisualSourceBound(string? source)
    {
        lock (_frameLock)
        {
            if (_sharedSrv is not null)
                return;
        }

        TryRebindCachedVisualSourceTexture(source);
    }

    private void TryRebindCachedVisualSourceTexture(string? source = null)
    {
        var activeSource = string.IsNullOrWhiteSpace(source) ? _activeVisualSource : source;
        IntPtr handle;
        int width;
        int height;

        if (string.Equals(activeSource, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase))
        {
            handle = _lastDirectStreamSharedHandle;
            width = _lastDirectStreamSharedWidth;
            height = _lastDirectStreamSharedHeight;
        }
        else
        {
            handle = _lastWebViewSharedHandle;
            width = _lastWebViewSharedWidth;
            height = _lastWebViewSharedHeight;
            activeSource = SharedTextureSourceWebView;
        }

        if (handle == IntPtr.Zero || width <= 0 || height <= 0) return;
        if (ShouldThrottleSharedTextureOpen(handle, width, height, activeSource)) return;

        lock (_frameLock)
        {
            if (_openedSharedHandle == handle && _sharedSrv is not null)
                return;
        }

        _ = Task.Run(() => OpenSharedTexture(handle, width, height, activeSource));
    }

    private bool IsDirectStreamReceiverVisualSourceActive => string.Equals(_activeVisualSource, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase);

    private void ActivateBrowserVisualSource(bool stopReceiver, bool clearDirectStreamFrame)
    {
        if (stopReceiver && _directStreamReceiverActive)
        {
            try { Send(new { op = "directStreamStopReceiver" }); } catch { }
            _directStreamReceiverActive = false;
        }

        _activeVisualSource = SharedTextureSourceWebView;
        if (clearDirectStreamFrame && _usingDirectStreamReceiverTexture)
            ClearCurrentFrame();
        _usingDirectStreamReceiverTexture = false;
        TryRebindCachedVisualSourceTexture();
    }

    private void ActivateDirectStreamReceiverVisualSource()
    {
        _activeVisualSource = SharedTextureSourceDirectStreamReceiver;
        _usingDirectStreamReceiverTexture = false;
        ClearCurrentFrame();
    }

    private bool ShouldAcceptVisualSource(string? source)
    {
        var normalised = string.IsNullOrWhiteSpace(source) ? SharedTextureSourceWebView : source;
        return string.Equals(normalised, _activeVisualSource, StringComparison.OrdinalIgnoreCase);
    }

    private string DescribeIgnoredVisualSource(string? source)
    {
        var normalised = string.IsNullOrWhiteSpace(source) ? SharedTextureSourceWebView : source;
        return IsDirectStreamReceiverVisualSourceActive
            ? "Waiting for host video"
            : string.Equals(normalised, SharedTextureSourceDirectStreamReceiver, StringComparison.OrdinalIgnoreCase)
                ? "Browser output ready"
                : "Browser output waiting";
    }

    private void SendAudio()
    {
        Send(new { op = "audio", muted = Muted, volume = NormaliseVolume(Volume) });
    }

    private void Send(object command)
    {
        try
        {
            if (_processCts?.IsCancellationRequested == true) return;
            _pendingCommands.Enqueue(JsonSerializer.Serialize(command));
            _commandSignal.Release();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to queue RavaCast WebView2 command");
        }
    }

    private async Task WriteLoopAsync(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested && _writer is not null)
            {
                await _commandSignal.WaitAsync(token).ConfigureAwait(false);
                var wrote = false;
                while (_pendingCommands.TryDequeue(out var json))
                {
                    if (token.IsCancellationRequested || _writer is null) return;
                    await _writer.WriteLineAsync(json).ConfigureAwait(false);
                    wrote = true;
                }
                if (wrote && _writer is not null)
                    await _writer.FlushAsync().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (IOException)
        {
            // Normal renderer shutdown closes the pipe; no log needed.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RavaCast WebView2 command writer failed");
            SetStatus("WebView2 command writer failed", ex.Message);
        }
    }

    private void HideCurrentFrame()
    {
        lock (_frameLock)
        {
            _frameIndex = 0;
            _lastFrameUtc = default;
        }
    }

    private void DisposeSharedTextureLocked()
    {
        try { _sharedSrv?.Dispose(); } catch { }
        try { _sharedTexture?.Dispose(); } catch { }
        _sharedSrv = null;
        _sharedTexture = null;
        _openedSharedHandle = IntPtr.Zero;
        _sharedWidth = 0;
        _sharedHeight = 0;
    }

    private void QueueSharedTextureForDeferredDisposeLocked()
    {
        if (_sharedSrv is null && _sharedTexture is null)
        {
            _openedSharedHandle = IntPtr.Zero;
            _sharedWidth = 0;
            _sharedHeight = 0;
            return;
        }

        // Do not release the SRV immediately from an End Cast button click. The current ImGui/world
        // render pass may already have queued this texture ID for drawing, and releasing it mid-frame
        // can crash the game/device. Hide it immediately, then release it after the render pass has
        // had time to drain.
        _deferredTextureDisposals.Add((_sharedSrv, _sharedTexture, Environment.TickCount64 + 2000));
        _sharedSrv = null;
        _sharedTexture = null;
        _openedSharedHandle = IntPtr.Zero;
        _sharedWidth = 0;
        _sharedHeight = 0;
        ScheduleDeferredTextureDrain();
    }

    private void DisposeDeferredTextures(bool force)
    {
        var due = new List<(ShaderResourceView? Srv, Texture2D? Texture)>();
        lock (_frameLock)
        {
            var now = Environment.TickCount64;
            for (var i = _deferredTextureDisposals.Count - 1; i >= 0; i--)
            {
                var item = _deferredTextureDisposals[i];
                if (!force && item.DisposeAfterTick > now) continue;
                due.Add((item.Srv, item.Texture));
                _deferredTextureDisposals.RemoveAt(i);
            }
        }

        foreach (var item in due)
        {
            try { item.Srv?.Dispose(); } catch { }
            try { item.Texture?.Dispose(); } catch { }
        }
    }

    private void ScheduleDeferredTextureDrain()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(2500).ConfigureAwait(false);
                DisposeDeferredTextures(force: false);
            }
            catch { }
        });
    }

    private void ClearCurrentFrame()
    {
        lock (_frameLock)
        {
            QueueSharedTextureForDeferredDisposeLocked();
            _lastPublishedFrame = 0;
            _frameIndex = 0;
            _lastFrameUtc = default;
        }
    }

    private void SetStatus(string status, string? detail)
    {
        lock (_stateLock)
        {
            _statusText = status;
            _detail = detail;
        }
    }

    private void SetStatusIfChanged(string status, string? detail)
    {
        lock (_stateLock)
        {
            if (string.Equals(_statusText, status, StringComparison.Ordinal) && string.Equals(_detail, detail, StringComparison.Ordinal))
                return;
            _statusText = status;
            _detail = detail;
        }
    }


    private void SetDirectStreamStatus(bool publisherActive, bool receiverActive, string status, string? detail)
    {
        lock (_stateLock)
        {
            _directStreamPublisherActive = publisherActive;
            _directStreamReceiverActive = receiverActive;
            _directStreamStatusText = status;
            _directStreamDetail = detail;
        }
    }

    private static bool IsRavaCastErrorText(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return false;
        return text.Contains("\"op\":\"error\"", StringComparison.OrdinalIgnoreCase)
            || text.Contains("error", StringComparison.OrdinalIgnoreCase)
            || text.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || text.Contains("failure", StringComparison.OrdinalIgnoreCase)
            || text.Contains("exception", StringComparison.OrdinalIgnoreCase)
            || text.Contains("fatal", StringComparison.OrdinalIgnoreCase)
            || text.Contains("crash", StringComparison.OrdinalIgnoreCase)
            || text.Contains("unhandled", StringComparison.OrdinalIgnoreCase)
            || text.Contains("could not", StringComparison.OrdinalIgnoreCase)
            || text.Contains("cannot", StringComparison.OrdinalIgnoreCase)
            || text.Contains("unavailable", StringComparison.OrdinalIgnoreCase)
            || text.Contains("missing", StringComparison.OrdinalIgnoreCase)
            || text.Contains("rejected", StringComparison.OrdinalIgnoreCase)
            || text.Contains("unexpected", StringComparison.OrdinalIgnoreCase)
            || text.Contains("broken", StringComparison.OrdinalIgnoreCase);
    }

    private bool NativeMediaBoundaryAvailable => _installer.IsNativeBridgeInstalled;

    private string BuildMissingReason()
    {
        if (!File.Exists(RendererPath))
            return $"RavaCast WebView2 renderer executable was not found in the plugin folder: {RendererPath}";
        if (!_installer.IsWebView2RuntimeInstalled)
            return "Microsoft Edge WebView2 Evergreen Runtime is missing. RavaCast will try to install it silently when video starts.";
        return "RavaCast video output is not ready yet.";
    }

    private static string FriendlyBrowserBackendStatus(string status)
    {
        if (string.IsNullOrWhiteSpace(status)) return "Ready";
        if (status.Contains("first WGC frame", StringComparison.OrdinalIgnoreCase) || status.Contains("shared texture ready", StringComparison.OrdinalIgnoreCase) || status.Contains("Browser output ready", StringComparison.OrdinalIgnoreCase) || status.Contains("Rendering", StringComparison.OrdinalIgnoreCase))
            return "Browser output ready";
        if (status.Contains("starting", StringComparison.OrdinalIgnoreCase) || status.Contains("launched", StringComparison.OrdinalIgnoreCase) || status.Contains("connected", StringComparison.OrdinalIgnoreCase) || status.Contains("capture active", StringComparison.OrdinalIgnoreCase))
            return "Starting browser output";
        if (status.Contains("failed", StringComparison.OrdinalIgnoreCase) || status.Contains("error", StringComparison.OrdinalIgnoreCase))
            return "Video output needs attention";
        if (status.Contains("stopped", StringComparison.OrdinalIgnoreCase))
            return "Video output stopped";
        return status.Replace("WebView2", "browser", StringComparison.OrdinalIgnoreCase)
            .Replace("WGC", "", StringComparison.OrdinalIgnoreCase)
            .Replace("D3D", "", StringComparison.OrdinalIgnoreCase)
            .Trim();
    }

    private static string? FriendlyBrowserBackendDetail(string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail)) return null;
        if (detail.Contains("error", StringComparison.OrdinalIgnoreCase) || detail.Contains("failed", StringComparison.OrdinalIgnoreCase) || detail.Contains("missing", StringComparison.OrdinalIgnoreCase) || detail.Contains("not found", StringComparison.OrdinalIgnoreCase))
            return detail;
        return null;
    }

    private static string? FriendlyDirectStreamBackendDetail(string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail)) return null;
        if (detail.Contains("Offer/answer:", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("ICE local=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("tracks=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("received=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("decoded=", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("shown=", StringComparison.OrdinalIgnoreCase))
            return detail;
        if (detail.Contains("libdatachannel", StringComparison.OrdinalIgnoreCase) || detail.Contains("FFmpeg", StringComparison.OrdinalIgnoreCase) || detail.Contains("Opus", StringComparison.OrdinalIgnoreCase))
            return detail;
        if (detail.Contains("RavaCast.Media", StringComparison.OrdinalIgnoreCase) || detail.Contains("BridgeHost", StringComparison.OrdinalIgnoreCase))
            return "Direct Stream files are missing or the video bridge failed to start.";
        if (detail.Contains("failed", StringComparison.OrdinalIgnoreCase) || detail.Contains("error", StringComparison.OrdinalIgnoreCase) || detail.Contains("missing", StringComparison.OrdinalIgnoreCase) || detail.Contains("not found", StringComparison.OrdinalIgnoreCase) || detail.Contains("cannot", StringComparison.OrdinalIgnoreCase) || detail.Contains("closed", StringComparison.OrdinalIgnoreCase) || detail.Contains("exited", StringComparison.OrdinalIgnoreCase) || detail.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            return detail;
        return null;
    }

    private static float NormaliseVolume(float volume)
    {
        if (float.IsNaN(volume) || float.IsInfinity(volume)) return 0.50f;
        return Math.Clamp(volume, 0.01f, 1f);
    }

    private static float ClampNormalised(float value)
    {
        if (float.IsNaN(value) || float.IsInfinity(value)) return 0f;
        return Math.Clamp(value, 0f, 1f);
    }


    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        StopRendererProcess();
        ClearCurrentFrame();
        DisposeDeferredTextures(force: true);
        try { _gameDevice?.Dispose(); } catch { }
        _gameDevice = null;
        try { _commandSignal.Dispose(); } catch { }
    }
}
