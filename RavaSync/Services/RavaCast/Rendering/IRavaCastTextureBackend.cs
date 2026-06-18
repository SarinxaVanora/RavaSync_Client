using System;

using RavaSync.Services.RavaCast;

namespace RavaSync.Services.RavaCast.Rendering;

public interface IRavaCastTextureBackend : IDisposable
{
    RavaCastBackendStatus Status { get; }
    bool IsOpen { get; }
    string CurrentUrl { get; }
    bool Muted { get; }
    float Volume { get; }
    bool BrowserWindowVisible { get; }
    RavaCastDirectStreamBackendStatus DirectStreamStatus { get; }
    RavaCastMediaSnapshot CurrentMedia { get; }

    event EventHandler<RavaCastDirectStreamSignalProducedEventArgs>? DirectStreamSignalProduced;

    void Open(string url, bool muted, float volume);
    void ApplyState(string url);
    void ApplyMediaState(double positionSeconds, bool isPlaying, bool force);
    void SetMuted(bool muted);
    void SetVolume(float volume);
    void ShowInteractiveWindow();
    void HideInteractiveWindow();
    void SendPointerMove(float normalisedX, float normalisedY);
    void SendPointerClick(float normalisedX, float normalisedY);
    void SendPointerWheel(float normalisedX, float normalisedY, float wheelDelta);
    void SendBrowserMouse(float normalisedX, float normalisedY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt);
    void SendBrowserMousePixels(int pixelX, int pixelY, int downMask, int upMask, int heldMask, int doubleMask, float wheelX, float wheelY, bool leaving, bool shift, bool ctrl, bool alt);
    void SendBrowserFocus(bool focused);
    void SendTextInput(string text);
    void SendSpecialKey(string key);
    void SendBrowserKey(int virtualKey, bool down, string? text, bool shift, bool ctrl, bool alt);
    void TryDismissConsentPrompt(bool preferReject);
    void ReloadPage();
    void GoBack();
    RavaCastCookiePayload[] GetShareableConsentCookies();
    void ApplySharedConsentCookies(string url, IReadOnlyList<RavaCastCookiePayload> cookies);
    void Close();
    RavaCastTextureFrame? TryGetCurrentFrame();

    bool StartDirectStreamPublisher(Guid castId, RavaCastDirectStreamQuality quality, out string error);
    void StopDirectStreamPublisher();
    bool StartDirectStreamReceiver(Guid castId, string hostSessionId, string viewerSessionId, RavaCastDirectStreamQuality quality, out string error);
    void StopDirectStreamReceiver();
    void AddDirectStreamPeer(string peerId);
    void RemoveDirectStreamPeer(string peerId);
    void HandleDirectStreamSignal(string peerId, string signalType, string payloadJson);
}
