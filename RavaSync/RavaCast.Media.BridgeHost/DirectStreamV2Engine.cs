using System.Collections.Concurrent;
using System.Text.Json;

namespace RavaCast.Media.BridgeHost;

internal sealed class DirectStreamV2Engine : IDisposable
{
    private readonly BridgeTransport _transport;
    private readonly ConcurrentDictionary<string, RtcPeerSession> _sessions = new(StringComparer.Ordinal);
    private readonly object _stateGate = new();
    private readonly HashSet<string> _pendingPublisherPeers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentQueue<PendingSignal>> _pendingSignals = new(StringComparer.Ordinal);
    private const int PendingSignalTtlMs = 15_000;
    private const int MaxPendingSignalsPerPeer = 64;
    private string _castId = string.Empty;
    private bool _publisherActive;
    private bool _receiverActive;
    private bool _libDataChannelReady;
    private PublisherConfig? _publisherConfig;
    private ReceiverConfig? _receiverConfig;
    private readonly DirectStreamPublisherMediaHub _publisherMediaHub;

    public DirectStreamV2Engine(BridgeTransport transport)
    {
        _transport = transport;
        _publisherMediaHub = new DirectStreamPublisherMediaHub(transport, () => _publisherActive, () => _receiverActive, () => _sessions.Count);
    }

    public async Task HandleCommandAsync(JsonElement root)
    {
        var op = ReadString(root, "op");
        try
        {
            switch (op)
            {
                case "startPublisher":
                    await StartPublisherAsync(root).ConfigureAwait(false);
                    break;
                case "stopPublisher":
                    await StopPublisherAsync(ReadString(root, "reason", "Publisher stopped")).ConfigureAwait(false);
                    break;
                case "startReceiver":
                    await StartReceiverAsync(root).ConfigureAwait(false);
                    break;
                case "stopReceiver":
                    await StopReceiverAsync(ReadString(root, "reason", "Receiver stopped")).ConfigureAwait(false);
                    break;
                case "addPeer":
                    await AddPeerAsync(ReadString(root, "peerId")).ConfigureAwait(false);
                    break;
                case "removePeer":
                    await RemovePeerAsync(ReadString(root, "peerId")).ConfigureAwait(false);
                    break;
                case "signal":
                    await HandleSignalAsync(ReadString(root, "peerId"), ReadString(root, "signalType"), ReadString(root, "payloadJson")).ConfigureAwait(false);
                    break;
                case "setAudio":
                    await SetAudioAsync(ReadBool(root, "muted", false), ReadFloat(root, "volume", 1f)).ConfigureAwait(false);
                    break;
                case "shutdown":
                    await ShutdownAsync(ReadString(root, "reason", "BridgeHost shutdown requested")).ConfigureAwait(false);
                    break;
            }
        }
        catch (Exception ex)
        {
            Program.Log("BridgeHost command '" + op + "' failed: " + Program.Flatten(ex));
            await _transport.SendErrorAsync($"BridgeHost command '{op}' failed: {ex.Message}", _publisherActive, _receiverActive, true, _sessions.Count).ConfigureAwait(false);
        }
    }

    private async Task StartPublisherAsync(JsonElement root)
    {
        _castId = ReadString(root, "castId");
        _publisherConfig = new PublisherConfig(
            _castId,
            (nint)ReadLong(root, "sharedTextureHandle", 0),
            ReadInt(root, "sourceWidth", 1280),
            ReadInt(root, "sourceHeight", 720),
            ReadInt(root, "targetWidth", ReadInt(root, "sourceWidth", 1280)),
            ReadInt(root, "targetHeight", ReadInt(root, "sourceHeight", 720)),
            Math.Clamp(ReadInt(root, "fps", 30), 15, 120),
            Math.Max(300, ReadInt(root, "videoBitrateKbps", 3500)),
            Math.Max(0, ReadInt(root, "audioBitrateKbps", 128)),
            Math.Max(0, ReadInt(root, "audioSourceProcessId", 0)),
            ReadString(root, "stunServersJson"));
        _publisherMediaHub.UpdateConfig(_publisherConfig);

        if (!await EnsureLibDataChannelAsync().ConfigureAwait(false))
            return;

        string[] pendingPeers;
        lock (_stateGate)
        {
            _publisherActive = true;
            pendingPeers = _pendingPublisherPeers.ToArray();
            _pendingPublisherPeers.Clear();
        }

        await _transport.SendStatusAsync(
            "Ready for viewers",
            $"Direct Stream v2 transport is live for cast {_castId}. Source={_publisherConfig.SourceWidth}x{_publisherConfig.SourceHeight}, target={_publisherConfig.TargetWidth}x{_publisherConfig.TargetHeight}@{_publisherConfig.Fps}. Viewers will receive the resolved RavaCast output through FFmpeg H.264 over libdatachannel, with browser-only WebView2 audio pid={_publisherConfig.AudioSourceProcessId} sent as Opus RTP over the Direct Stream audio media track.",
            true,
            _receiverActive,
            true,
            _sessions.Count).ConfigureAwait(false);

        foreach (var pendingPeer in pendingPeers)
        {
            if (_sessions.ContainsKey(pendingPeer))
                continue;
            await AddPeerAsync(pendingPeer).ConfigureAwait(false);
        }
    }

    private async Task StopPublisherAsync(string reason)
    {
        lock (_stateGate)
        {
            _publisherActive = false;
            _pendingPublisherPeers.Clear();
        }

        _publisherMediaHub.Stop(reason);

        foreach (var session in _sessions.Values.Where(s => s.Role == RtcPeerRole.Publisher).ToArray())
            _pendingSignals.TryRemove(session.PeerId, out _);

        foreach (var session in _sessions.Values.Where(s => s.Role == RtcPeerRole.Publisher).ToArray())
            RemoveSession(session.PeerId);
        await _transport.SendStatusAsync("Host stream stopped", reason, false, _receiverActive, true, _sessions.Count).ConfigureAwait(false);
    }

    private async Task StartReceiverAsync(JsonElement root)
    {
        _castId = ReadString(root, "castId");
        _receiverConfig = new ReceiverConfig(
            _castId,
            ReadString(root, "hostSessionId"),
            ReadString(root, "viewerSessionId"),
            ReadInt(root, "targetWidth", 1280),
            ReadInt(root, "targetHeight", 720),
            Math.Clamp(ReadInt(root, "fps", 30), 15, 120),
            Math.Max(300, ReadInt(root, "videoBitrateKbps", 3500)),
            Math.Max(0, ReadInt(root, "audioBitrateKbps", 128)),
            ReadString(root, "stunServersJson"));

        if (!await EnsureLibDataChannelAsync().ConfigureAwait(false))
            return;

        var session = CreateOrReplaceSession(_receiverConfig.HostSessionId, RtcPeerRole.Receiver, _receiverConfig.StunServersJson);
        session.StartReceiver(_receiverConfig);
        lock (_stateGate) _receiverActive = true;
        DrainPendingSignals(session);

        await _transport.SendStatusAsync(
            "Connecting to host video",
            $"Direct Stream v2 receiver is waiting for the host offer. Target={_receiverConfig.TargetWidth}x{_receiverConfig.TargetHeight}@{_receiverConfig.Fps}.",
            _publisherActive,
            true,
            true,
            _sessions.Count).ConfigureAwait(false);
    }

    private async Task StopReceiverAsync(string reason)
    {
        lock (_stateGate) _receiverActive = false;
        foreach (var session in _sessions.Values.Where(s => s.Role == RtcPeerRole.Receiver).ToArray())
        {
            _pendingSignals.TryRemove(session.PeerId, out _);
            RemoveSession(session.PeerId);
        }
        await _transport.SendStatusAsync("Host video stopped", reason, _publisherActive, false, true, _sessions.Count).ConfigureAwait(false);
    }

    private Task SetAudioAsync(bool muted, float volume)
    {
        var safeVolume = Math.Clamp(volume, 0f, 1f);
        foreach (var session in _sessions.Values.ToArray())
            session.SetPlaybackAudioState(muted, safeVolume);
        return Task.CompletedTask;
    }

    private async Task AddPeerAsync(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId))
            return;

        PublisherConfig? config;
        lock (_stateGate)
        {
            config = _publisherConfig;
            if (!_publisherActive || config is null)
            {
                _pendingPublisherPeers.Add(peerId);
                config = null;
            }
        }

        if (config is null)
        {
            await _transport.SendStatusAsync(
                "Viewer queued",
                $"Viewer {peerId} is waiting for the Direct Stream publisher to finish starting.",
                _publisherActive,
                _receiverActive,
                true,
                _sessions.Count).ConfigureAwait(false);
            return;
        }

        if (!await EnsureLibDataChannelAsync().ConfigureAwait(false))
            return;

        if (_sessions.TryGetValue(peerId, out var existing) && existing.Role == RtcPeerRole.Publisher)
        {
            existing.ResendLocalNegotiation("viewer ready retry");
            await _transport.SendStatusAsync(
                existing.HasRemoteDescription || existing.IsConnected ? "Viewer already connecting" : "Resent viewer offer",
                $"Direct Stream v2 transport is already negotiating with viewer {peerId}; reused the existing offer/ICE instead of creating an ICE restart.",
                true,
                _receiverActive,
                true,
                _sessions.Count).ConfigureAwait(false);
            return;
        }

        var session = CreateOrReplaceSession(peerId, RtcPeerRole.Publisher, config.StunServersJson);
        _publisherMediaHub.RegisterSession(session);
        session.StartPublisher(config);
        DrainPendingSignals(session);

        await _transport.SendStatusAsync(
            "Viewer connecting",
            $"Negotiating Direct Stream v2 transport with viewer {peerId}. Live RavaCast video frames will start once the viewer connection opens.",
            true,
            _receiverActive,
            true,
            _sessions.Count).ConfigureAwait(false);
    }

    private async Task RemovePeerAsync(string peerId)
    {
        if (!string.IsNullOrWhiteSpace(peerId))
        {
            lock (_stateGate) _pendingPublisherPeers.Remove(peerId);
            _pendingSignals.TryRemove(peerId, out _);
            RemoveSession(peerId);
        }

        await _transport.SendStatusAsync(_sessions.Count == 0 ? "Waiting for viewers" : "Viewer left", $"Viewers: {_sessions.Count}", _publisherActive, _receiverActive, true, _sessions.Count).ConfigureAwait(false);
    }

    private async Task HandleSignalAsync(string peerId, string signalType, string payloadJson)
    {
        if (string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType))
            return;

        if (!await EnsureLibDataChannelAsync().ConfigureAwait(false))
            return;

        if (!_sessions.TryGetValue(peerId, out var session))
        {
            if (_receiverActive)
            {
                var config = _receiverConfig;
                session = CreateOrReplaceSession(peerId, RtcPeerRole.Receiver, config?.StunServersJson ?? string.Empty);
                if (config is not null) session.StartReceiver(config);
                DrainPendingSignals(session);
            }
            else if (_publisherActive)
            {
                var config = _publisherConfig;
                if (config is null)
                {
                    await QueuePendingSignalAsync(peerId, signalType, payloadJson, "publisher config is not ready yet").ConfigureAwait(false);
                    return;
                }

                session = CreateOrReplaceSession(peerId, RtcPeerRole.Publisher, config.StunServersJson);
                _publisherMediaHub.RegisterSession(session);
                session.StartPublisher(config);
                DrainPendingSignals(session);
            }
            else
            {
                await QueuePendingSignalAsync(peerId, signalType, payloadJson, "receiver/publisher session is still starting").ConfigureAwait(false);
                return;
            }
        }

        session.HandleSignal(signalType, payloadJson);
    }

    private async Task QueuePendingSignalAsync(string peerId, string signalType, string payloadJson, string reason)
    {
        PrunePendingSignals();
        var queue = _pendingSignals.GetOrAdd(peerId, _ => new ConcurrentQueue<PendingSignal>());
        while (queue.Count >= MaxPendingSignalsPerPeer && queue.TryDequeue(out _)) { }
        queue.Enqueue(new PendingSignal(signalType, payloadJson ?? string.Empty, Environment.TickCount64));

        await _transport.SendStatusAsync(
            "Direct Stream signal queued",
            $"Queued {signalType} from {peerId}: {reason}. This prevents early offers/ICE from being dropped during join startup.",
            _publisherActive,
            _receiverActive,
            true,
            _sessions.Count).ConfigureAwait(false);
    }

    private void DrainPendingSignals(RtcPeerSession session)
    {
        if (!_pendingSignals.TryRemove(session.PeerId, out var queue))
            return;

        var now = Environment.TickCount64;
        while (queue.TryDequeue(out var pending))
        {
            if (now - pending.CreatedTick > PendingSignalTtlMs)
                continue;

            try
            {
                session.HandleSignal(pending.Type, pending.PayloadJson);
            }
            catch (Exception ex)
            {
                Program.Log($"Direct Stream queued signal failed for {session.PeerId}/{pending.Type}: {Program.Flatten(ex)}");
                _ = _transport.SendErrorAsync($"Direct Stream queued signal failed for {session.PeerId}/{pending.Type}: {ex.Message}", _publisherActive, _receiverActive, true, _sessions.Count);
            }
        }
    }

    private void PrunePendingSignals()
    {
        var now = Environment.TickCount64;
        foreach (var pair in _pendingSignals.ToArray())
        {
            var queue = pair.Value;
            while (queue.TryPeek(out var pending) && now - pending.CreatedTick > PendingSignalTtlMs)
                queue.TryDequeue(out _);

            if (queue.IsEmpty)
                _pendingSignals.TryRemove(pair.Key, out _);
        }
    }

    private async Task ShutdownAsync(string reason)
    {
        lock (_stateGate)
        {
            _publisherActive = false;
            _receiverActive = false;
            _pendingPublisherPeers.Clear();
            _pendingSignals.Clear();
        }
        _publisherMediaHub.Stop(reason);
        foreach (var session in _sessions.Values.ToArray())
            session.Dispose();
        _sessions.Clear();
        await _transport.SendStatusAsync("Direct Stream bridge stopped", reason, false, false, true, 0).ConfigureAwait(false);
    }

    private async Task<bool> EnsureLibDataChannelAsync()
    {
        if (_libDataChannelReady)
            return true;

        if (LibDataChannelNative.TryPreload(out var detail))
        {
            _libDataChannelReady = true;
            await _transport.SendStatusAsync("Direct Stream transport ready", detail + " " + FfmpegRuntimeProbe.Describe(), _publisherActive, _receiverActive, true, _sessions.Count).ConfigureAwait(false);
            return true;
        }

        await _transport.SendErrorAsync("Direct Stream v2 cannot start because libdatachannel was not found or could not initialise. " + detail, _publisherActive, _receiverActive, true, _sessions.Count).ConfigureAwait(false);
        return false;
    }

    private RtcPeerSession CreateOrReplaceSession(string peerId, RtcPeerRole role, string stunServersJson)
    {
        RemoveSession(peerId);
        var session = new RtcPeerSession(peerId, role, stunServersJson, _transport, () => _publisherActive, () => _receiverActive, () => _sessions.Count, role == RtcPeerRole.Publisher ? _publisherMediaHub : null);
        _sessions[peerId] = session;
        if (role == RtcPeerRole.Publisher)
            _publisherMediaHub.RegisterSession(session);
        return session;
    }

    private void RemoveSession(string peerId)
    {
        if (_sessions.TryRemove(peerId, out var old))
        {
            if (old.Role == RtcPeerRole.Publisher)
                _publisherMediaHub.UnregisterSession(old.PeerId);
            old.Dispose();
        }
    }

    public void Dispose()
    {
        lock (_stateGate)
        {
            _publisherActive = false;
            _receiverActive = false;
            _pendingPublisherPeers.Clear();
            _pendingSignals.Clear();
        }

        _publisherMediaHub.Stop("engine disposed");
        foreach (var session in _sessions.Values.ToArray())
            session.Dispose();
        _sessions.Clear();
        _publisherMediaHub.Dispose();
    }

    internal static string ReadString(JsonElement root, string name, string fallback = "") => root.TryGetProperty(name, out var value) ? value.GetString() ?? fallback : fallback;
    internal static bool ReadBool(JsonElement root, string name, bool fallback = false) => root.TryGetProperty(name, out var value) && (value.ValueKind == JsonValueKind.True || value.ValueKind == JsonValueKind.False) ? value.GetBoolean() : fallback;
    internal static int ReadInt(JsonElement root, string name, int fallback) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : fallback;
    internal static long ReadLong(JsonElement root, string name, long fallback) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetInt64() : fallback;
    internal static float ReadFloat(JsonElement root, string name, float fallback = 0f) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? (float)value.GetDouble() : fallback;
}

internal sealed record PendingSignal(string Type, string PayloadJson, long CreatedTick);

internal sealed record PublisherConfig(string CastId, nint SharedTextureHandle, int SourceWidth, int SourceHeight, int TargetWidth, int TargetHeight, int Fps, int VideoBitrateKbps, int AudioBitrateKbps, int AudioSourceProcessId, string StunServersJson);
internal sealed record ReceiverConfig(string CastId, string HostSessionId, string ViewerSessionId, int TargetWidth, int TargetHeight, int Fps, int VideoBitrateKbps, int AudioBitrateKbps, string StunServersJson);
