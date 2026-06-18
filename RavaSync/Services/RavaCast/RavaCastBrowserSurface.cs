using Microsoft.Extensions.Logging;
using RavaSync.Services.RavaCast.Rendering;
using System;
using System.Collections.Generic;

namespace RavaSync.Services.RavaCast;

/// <summary>
/// Local playback/render surface facade for RavaCast. Lobby/mesh/UI code talks to this facade only;
/// the actual browser/media implementation is hidden behind IRavaCastTextureBackend.
/// </summary>
public sealed class RavaCastBrowserSurface : IDisposable
{
    private readonly ILogger<RavaCastBrowserSurface> _logger;
    private readonly IRavaCastTextureBackend _backend;

    public RavaCastBrowserSurface(ILogger<RavaCastBrowserSurface> logger, IRavaCastTextureBackend backend)
    {
        _logger = logger;
        _backend = backend;
    }

    public bool IsOpen => _backend.IsOpen;
    public string CurrentUrl => _backend.CurrentUrl;
    public bool Muted => _backend.Muted;
    public float Volume => _backend.Volume;
    public bool BrowserWindowVisible => _backend.BrowserWindowVisible;
    public RavaCastBackendStatus BackendStatus => _backend.Status;
    public RavaCastDirectStreamBackendStatus DirectStreamStatus => _backend.DirectStreamStatus;
    public RavaCastMediaSnapshot CurrentMedia => _backend.CurrentMedia;

    public event EventHandler<RavaCastDirectStreamSignalProducedEventArgs>? DirectStreamSignalProduced
    {
        add => _backend.DirectStreamSignalProduced += value;
        remove => _backend.DirectStreamSignalProduced -= value;
    }

    public void Open(string url, bool muted, float volume)
    {
        _backend.Open(url, muted, volume);
        // Healthy RavaCast opens are intentionally not logged; keep RavaCast logs error-only.
    }

    public void ApplyState(string url)
        => _backend.ApplyState(url);

    public void ApplyMediaState(double positionSeconds, bool isPlaying, bool force = false)
        => _backend.ApplyMediaState(positionSeconds, isPlaying, force);

    public void SetMuted(bool muted) => _backend.SetMuted(muted);

    public void SetVolume(float volume) => _backend.SetVolume(volume);

    public void ShowInteractiveWindow() => _backend.ShowInteractiveWindow();

    public void HideInteractiveWindow() => _backend.HideInteractiveWindow();

    public void SendPointerMove(float normalisedX, float normalisedY) => _backend.SendPointerMove(normalisedX, normalisedY);

    public void SendPointerClick(float normalisedX, float normalisedY) => _backend.SendPointerClick(normalisedX, normalisedY);

    public void SendPointerWheel(float normalisedX, float normalisedY, float wheelDelta) => _backend.SendPointerWheel(normalisedX, normalisedY, wheelDelta);

    public void SendBrowserMouse(float normalisedX, float normalisedY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt)
        => _backend.SendBrowserMouse(normalisedX, normalisedY, downMask, upMask, heldMask, doubleMask, wheelX, wheelY, leaving, shift, ctrl, alt);

    public void SendBrowserMousePixels(int pixelX, int pixelY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt)
        => _backend.SendBrowserMousePixels(pixelX, pixelY, downMask, upMask, heldMask, doubleMask, wheelX, wheelY, leaving, shift, ctrl, alt);

    public void SendBrowserFocus(bool focused) => _backend.SendBrowserFocus(focused);

    public void SendTextInput(string text) => _backend.SendTextInput(text);

    public void SendSpecialKey(string key) => _backend.SendSpecialKey(key);

    public void SendBrowserKey(int virtualKey, bool down, string? text, bool shift, bool ctrl, bool alt)
        => _backend.SendBrowserKey(virtualKey, down, text, shift, ctrl, alt);

    public void TryDismissConsentPrompt(bool preferReject) => _backend.TryDismissConsentPrompt(preferReject);

    public void ReloadPage() => _backend.ReloadPage();

    public void GoBack() => _backend.GoBack();

    public RavaCastCookiePayload[] GetShareableConsentCookies() => _backend.GetShareableConsentCookies();

    public void ApplySharedConsentCookies(string url, IReadOnlyList<RavaCastCookiePayload> cookies) => _backend.ApplySharedConsentCookies(url, cookies);

    public void Close() => _backend.Close();

    public RavaCastTextureFrame? TryGetCurrentFrame() => _backend.TryGetCurrentFrame();

    public bool StartDirectStreamPublisher(Guid castId, RavaCastDirectStreamQuality quality, out string error) => _backend.StartDirectStreamPublisher(castId, quality, out error);

    public void StopDirectStreamPublisher() => _backend.StopDirectStreamPublisher();

    public bool StartDirectStreamReceiver(Guid castId, string hostSessionId, string viewerSessionId, RavaCastDirectStreamQuality quality, out string error) => _backend.StartDirectStreamReceiver(castId, hostSessionId, viewerSessionId, quality, out error);

    public void StopDirectStreamReceiver() => _backend.StopDirectStreamReceiver();

    public void AddDirectStreamPeer(string peerId) => _backend.AddDirectStreamPeer(peerId);

    public void RemoveDirectStreamPeer(string peerId) => _backend.RemoveDirectStreamPeer(peerId);

    public void HandleDirectStreamSignal(string peerId, string signalType, string payloadJson) => _backend.HandleDirectStreamSignal(peerId, signalType, payloadJson);

    public void Dispose() => _backend.Dispose();

    private static string SafeDomain(string url)
        => Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri.Host : "unknown";
}
