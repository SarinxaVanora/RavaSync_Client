using System.Collections.Concurrent;

namespace RavaCast.Media.BridgeHost;

internal sealed class DirectStreamPublisherMediaHub : IDisposable
{
    private readonly BridgeTransport _transport;
    private readonly Func<bool> _publisherActive;
    private readonly Func<bool> _receiverActive;
    private readonly Func<int> _viewerCount;
    private readonly ConcurrentDictionary<string, RtcPeerSession> _sessions = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private readonly object _audioOggGate = new();
    private readonly object _videoPrimeGate = new();
    private readonly HashSet<string> _videoPrimedPeers = new(StringComparer.Ordinal);
    private OggOpusPacketReader _audioOggPacketReader = new();
    private byte[]? _lastDecoderRefreshAccessUnit;
    private PublisherConfig? _config;
    private FfmpegH264LiveTextureSender? _videoPump;
    private FfmpegOpusAudioSender? _audioPump;
    private bool _disposed;
    private bool _videoStartInProgress;
    private bool _audioStartInProgress;
    private DateTime _lastVideoStartAttemptUtc = DateTime.MinValue;
    private DateTime _lastAudioStartAttemptUtc = DateTime.MinValue;
    private long _broadcastVideoFrames;
    private long _broadcastVideoTargets;
    private long _broadcastAudioChunks;
    private long _broadcastAudioTargets;

    public DirectStreamPublisherMediaHub(BridgeTransport transport, Func<bool> publisherActive, Func<bool> receiverActive, Func<int> viewerCount)
    {
        _transport = transport;
        _publisherActive = publisherActive;
        _receiverActive = receiverActive;
        _viewerCount = viewerCount;
    }

    public void UpdateConfig(PublisherConfig config)
    {
        FfmpegH264LiveTextureSender? oldVideo = null;
        FfmpegOpusAudioSender? oldAudio = null;
        lock (_gate)
        {
            if (_disposed) return;
            var changed = _config is not null && !PublisherConfigMatches(_config, config);
            _config = config;
            if (changed)
            {
                oldVideo = _videoPump;
                oldAudio = _audioPump;
                _videoPump = null;
                _audioPump = null;
                _videoStartInProgress = false;
                _audioStartInProgress = false;
                lock (_audioOggGate) _audioOggPacketReader = new OggOpusPacketReader();
                lock (_videoPrimeGate)
                {
                    _lastDecoderRefreshAccessUnit = null;
                    _videoPrimedPeers.Clear();
                }
                Interlocked.Exchange(ref _broadcastVideoFrames, 0);
                Interlocked.Exchange(ref _broadcastVideoTargets, 0);
                Interlocked.Exchange(ref _broadcastAudioChunks, 0);
                Interlocked.Exchange(ref _broadcastAudioTargets, 0);
            }
        }

        try { oldVideo?.Dispose(); } catch { }
        try { oldAudio?.Dispose(); } catch { }
    }

    public void RegisterSession(RtcPeerSession session)
    {
        if (session.Role != RtcPeerRole.Publisher) return;
        _sessions[session.PeerId] = session;
    }

    public void UnregisterSession(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId)) return;
        _sessions.TryRemove(peerId, out _);
        lock (_videoPrimeGate) _videoPrimedPeers.Remove(peerId);
        if (_sessions.IsEmpty)
            StopPumps("last viewer left");
    }

    public void NotifySessionMediaReady(RtcPeerSession session, string reason)
    {
        if (session.Role != RtcPeerRole.Publisher) return;
        RegisterSession(session);
        TryStartVideo(reason);
        TryStartAudio(reason);
        TryPrimeVideoSession(session, reason);
    }

    public void Stop(string reason)
    {
        _sessions.Clear();
        StopPumps(reason);
    }

    private void TryStartVideo(string reason)
    {
        PublisherConfig? config;
        var now = DateTime.UtcNow;
        lock (_gate)
        {
            if (_disposed || _videoPump is not null || _videoStartInProgress) return;
            if (_config is not { } currentConfig) return;
            if (!_sessions.Values.Any(s => s.CanSendSharedVideo)) return;
            if ((now - _lastVideoStartAttemptUtc).TotalMilliseconds < 750) return;
            _videoStartInProgress = true;
            _lastVideoStartAttemptUtc = now;
            config = currentConfig;
        }

        _ = Task.Run(() => StartVideoWorker(config, reason));
    }

    private void StartVideoWorker(PublisherConfig config, string reason)
    {
        FfmpegH264LiveTextureSender? pump = null;
        var success = false;
        var detail = string.Empty;
        try
        {
            Program.Log($"Direct Stream starting one shared host video capture/encoder after {reason}; viewers={_sessions.Count}.");
            success = FfmpegH264LiveTextureSender.TryStart(config, BroadcastVideo, ReportVideoStatus, out pump, out detail);
        }
        catch (Exception ex)
        {
            detail = Program.Flatten(ex);
            Program.Log("Direct Stream shared host video encoder start threw: " + detail);
        }

        var accepted = false;
        lock (_gate)
        {
            _videoStartInProgress = false;
            if (!_disposed && success && pump is not null && _config is not null && PublisherConfigMatches(_config, config))
            {
                _videoPump = pump;
                accepted = true;
            }
        }

        if (!accepted)
        {
            try { pump?.Dispose(); } catch { }
        }

        if (accepted)
        {
            _ = _transport.SendStatusAsync("Shared live video ready", detail + $" One FFmpeg video encoder is now feeding {_sessions.Count} viewer peer(s). Audio will start after the first encoded video frame so it cannot race ahead of the captured browser picture.", true, _receiverActive(), true, _viewerCount());
        }
        else if (!success)
        {
            _ = _transport.SendErrorAsync("Direct Stream shared live video failed", detail + " Direct Stream v2 needs FFmpeg and the host shared D3D texture adapter to encode live RavaCast output.", true, _receiverActive(), true, _viewerCount());
        }
    }

    private void TryStartAudio(string reason)
    {
        PublisherConfig? config;
        var now = DateTime.UtcNow;
        lock (_gate)
        {
            if (_disposed || _audioPump is not null || _audioStartInProgress) return;
            if (_videoPump is null) return;
            if (_config is not { } currentConfig) return;
            if (currentConfig.AudioBitrateKbps <= 0) return;
            if (!_sessions.Values.Any(s => s.CanSendSharedAudio)) return;
            if ((now - _lastAudioStartAttemptUtc).TotalSeconds < 2) return;
            _audioStartInProgress = true;
            _lastAudioStartAttemptUtc = now;
            config = currentConfig;
        }

        _ = Task.Run(() => StartAudioWorker(config, reason));
    }

    private void StartAudioWorker(PublisherConfig config, string reason)
    {
        FfmpegOpusAudioSender? pump = null;
        var success = false;
        var detail = string.Empty;
        try
        {
            Program.Log($"Direct Stream starting one shared browser-only audio capture/encoder after {reason}; viewers={_sessions.Count}.");
            success = FfmpegOpusAudioSender.TryStart(config, BroadcastAudio, ReportAudioStatus, out pump, out detail);
        }
        catch (Exception ex)
        {
            detail = Program.Flatten(ex);
            Program.Log("Direct Stream shared browser-only audio encoder start threw: " + detail);
        }

        var accepted = false;
        lock (_gate)
        {
            _audioStartInProgress = false;
            if (!_disposed && success && pump is not null && _config is not null && PublisherConfigMatches(_config, config))
            {
                _audioPump = pump;
                accepted = true;
            }
        }

        if (!accepted)
        {
            try { pump?.Dispose(); } catch { }
        }

        if (accepted)
            _ = _transport.SendStatusAsync("Shared host audio ready", detail + $" One Opus encoder is now feeding {_sessions.Count} viewer peer(s).", true, _receiverActive(), true, _viewerCount());
        else if (!success)
            _ = _transport.SendStatusAsync("Host audio unavailable", detail + " Video will continue without Direct Stream audio.", true, _receiverActive(), true, _viewerCount());
    }

    private void BroadcastVideo(byte[] accessUnit)
    {
        if (accessUnit.Length == 0) return;
        var isDecoderRefresh = IsH264DecoderRefreshAccessUnit(accessUnit);
        if (isDecoderRefresh)
        {
            lock (_videoPrimeGate)
                _lastDecoderRefreshAccessUnit = accessUnit.ToArray();
        }

        var sessions = _sessions.Values.ToArray();
        var targets = 0;
        foreach (var session in sessions)
        {
            if (!session.CanSendSharedVideo) continue;

            var prime = TakeVideoPrimeForSession(session.PeerId, isDecoderRefresh);
            if (prime is not null)
                session.QueueSharedEncodedVideoSample(prime);

            if (session.QueueSharedEncodedVideoSample(accessUnit))
                targets++;
        }

        if (targets > 0)
        {
            var frames = Interlocked.Increment(ref _broadcastVideoFrames);
            Interlocked.Add(ref _broadcastVideoTargets, targets);
            if (frames == 1)
                TryStartAudio("first shared video frame broadcast");
        }
    }

    private void BroadcastAudio(byte[] chunk)
    {
        if (chunk.Length == 0) return;

        List<byte[]> opusPackets;
        lock (_audioOggGate)
            opusPackets = _audioOggPacketReader.Append(chunk);

        var sessions = _sessions.Values.ToArray();
        var targets = 0;
        foreach (var session in sessions)
        {
            if (!session.CanSendSharedAudio) continue;

            foreach (var packet in opusPackets)
                session.SendSharedOpusAudioPacket(packet);
            targets++;
        }

        if (targets > 0)
        {
            Interlocked.Increment(ref _broadcastAudioChunks);
            Interlocked.Add(ref _broadcastAudioTargets, targets);
        }
    }

    private void ReportVideoStatus(string detail)
    {
        var ready = _sessions.Values.Count(s => s.CanSendSharedVideo);
        var frames = Interlocked.Read(ref _broadcastVideoFrames);
        var targets = Interlocked.Read(ref _broadcastVideoTargets);
        var queued = _sessions.Values.Sum(s => s.SharedVideoQueuedFrames);
        var dropped = _sessions.Values.Sum(s => s.SharedVideoDroppedFrames);
        var dropDetail = dropped > 0 ? $"; stale queued frames dropped={dropped:n0}" : string.Empty;
        _ = _transport.SendStatusAsync("Sending shared live video", detail + $" Shared encoder fan-out: ready viewers={ready}/{_sessions.Count}; encoded frames broadcast={frames:n0}; viewer frame queues={targets:n0}; queued peer frames={queued:n0}{dropDetail}.", true, _receiverActive(), true, _viewerCount());
    }

    private void ReportAudioStatus(string detail)
    {
        var ready = _sessions.Values.Count(s => s.CanSendSharedAudio);
        var chunks = Interlocked.Read(ref _broadcastAudioChunks);
        var targets = Interlocked.Read(ref _broadcastAudioTargets);
        _ = _transport.SendStatusAsync("Sending shared host audio", detail + $" Shared audio fan-out: ready viewers={ready}/{_sessions.Count}; encoded chunks broadcast={chunks:n0}; viewer chunk sends={targets:n0}.", true, _receiverActive(), true, _viewerCount());
    }

    private void StopPumps(string reason)
    {
        FfmpegH264LiveTextureSender? oldVideo;
        FfmpegOpusAudioSender? oldAudio;
        lock (_gate)
        {
            oldVideo = _videoPump;
            oldAudio = _audioPump;
            _videoPump = null;
            _audioPump = null;
            _videoStartInProgress = false;
            _audioStartInProgress = false;
            lock (_audioOggGate) _audioOggPacketReader = new OggOpusPacketReader();
            lock (_videoPrimeGate)
            {
                _lastDecoderRefreshAccessUnit = null;
                _videoPrimedPeers.Clear();
            }
        }

        try { oldVideo?.Dispose(); } catch { }
        try { oldAudio?.Dispose(); } catch { }
        if (oldVideo is not null || oldAudio is not null)
            Program.Log($"Direct Stream stopped shared host media pumps: {reason}.");
    }


    private void TryPrimeVideoSession(RtcPeerSession session, string reason)
    {
        if (!session.CanSendSharedVideo) return;
        var prime = TakeVideoPrimeForSession(session.PeerId, isCurrentAccessUnitDecoderRefresh: false);
        if (prime is null) return;

        if (session.QueueSharedEncodedVideoSample(prime))
            Program.Log($"Direct Stream primed viewer {session.PeerId} with the latest cached H.264 decoder refresh after {reason}.");
    }

    private byte[]? TakeVideoPrimeForSession(string peerId, bool isCurrentAccessUnitDecoderRefresh)
    {
        lock (_videoPrimeGate)
        {
            if (_videoPrimedPeers.Contains(peerId)) return null;
            _videoPrimedPeers.Add(peerId);

            if (isCurrentAccessUnitDecoderRefresh) return null;
            return _lastDecoderRefreshAccessUnit?.ToArray();
        }
    }

    private static bool IsH264DecoderRefreshAccessUnit(byte[] accessUnit)
    {
        if (accessUnit.Length < 5) return false;
        for (var i = 0; i < accessUnit.Length - 4; i++)
        {
            var startCodeLength = H264StartCodeLengthAt(accessUnit, i);
            if (startCodeLength == 0) continue;
            var nalIndex = i + startCodeLength;
            if (nalIndex >= accessUnit.Length) continue;
            var nalType = accessUnit[nalIndex] & 0x1F;
            if (nalType == 5) return true;
        }

        return false;
    }

    private static int H264StartCodeLengthAt(byte[] data, int index)
    {
        if (index + 3 < data.Length && data[index] == 0 && data[index + 1] == 0 && data[index + 2] == 0 && data[index + 3] == 1) return 4;
        if (index + 2 < data.Length && data[index] == 0 && data[index + 1] == 0 && data[index + 2] == 1) return 3;
        return 0;
    }

    private static bool PublisherConfigMatches(PublisherConfig left, PublisherConfig right)
    {
        return left.CastId == right.CastId
               && left.SharedTextureHandle == right.SharedTextureHandle
               && left.SourceWidth == right.SourceWidth
               && left.SourceHeight == right.SourceHeight
               && left.TargetWidth == right.TargetWidth
               && left.TargetHeight == right.TargetHeight
               && left.Fps == right.Fps
               && left.VideoBitrateKbps == right.VideoBitrateKbps
               && left.AudioBitrateKbps == right.AudioBitrateKbps
               && left.AudioSourceProcessId == right.AudioSourceProcessId
               && left.StunServersJson == right.StunServersJson;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
        }
        _sessions.Clear();
        lock (_videoPrimeGate)
        {
            _lastDecoderRefreshAccessUnit = null;
            _videoPrimedPeers.Clear();
        }
        StopPumps("bridge disposed");
    }
}
