using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace RavaCast.Media.BridgeHost;

internal enum RtcPeerRole
{
    Publisher,
    Receiver
}

internal sealed class RtcPeerSession : IDisposable
{
    private sealed record QueuedVideoFrame(byte[] Frame, DateTime EnqueuedUtc);

    private static readonly ConcurrentDictionary<int, RtcPeerSession> SessionsByPc = new();
    private static readonly ConcurrentDictionary<int, RtcPeerSession> SessionsByTrack = new();
    private static readonly LibDataChannelNative.DescriptionCallback DescriptionCallback = OnLocalDescription;
    private static readonly LibDataChannelNative.CandidateCallback CandidateCallback = OnLocalCandidate;
    private static readonly LibDataChannelNative.StateChangeCallback StateCallback = OnStateChanged;
    private static readonly LibDataChannelNative.IceStateChangeCallback IceStateCallback = OnIceStateChanged;
    private static readonly LibDataChannelNative.TrackCallback TrackCallback = OnTrack;
    private static readonly LibDataChannelNative.DataChannelCallback DataChannelCallback = OnDataChannel;
    private static readonly LibDataChannelNative.FrameCallback FrameCallback = OnFrame;
    private static readonly LibDataChannelNative.MessageCallback MessageCallback = OnMessage;
    private static readonly LibDataChannelNative.OpenCallback OpenCallback = OnChannelOpen;
    private static readonly LibDataChannelNative.ClosedCallback ClosedCallback = OnChannelClosed;
    private static readonly LibDataChannelNative.ErrorCallback ErrorCallback = OnChannelError;

    private readonly BridgeTransport _transport;
    private readonly Func<bool> _publisherActive;
    private readonly Func<bool> _receiverActive;
    private readonly Func<int> _viewerCount;
    private readonly DirectStreamPublisherMediaHub? _publisherMediaHub;
    private readonly object _gate = new();
    private int _pc = -1;
    private int _videoTrack = -1;
    private int _audioTrack = -1;
    private int _audioChannel = -1;
    private int _videoChannel = -1;
    private bool _disposed;
    private volatile bool _connected;
    private volatile bool _iceTransportReady;
    private volatile bool _videoTrackOpen;
    private volatile bool _audioTrackOpen;
    private volatile bool _audioChannelOpen;
    private volatile bool _videoChannelOpen;
    private volatile bool _remoteDescriptionSeen;
    private volatile bool _localDescriptionSent;
    private readonly List<(string Candidate, string Mid)> _pendingRemoteIceCandidates = [];
    private readonly List<(int Track, bool IsAudio)> _pendingReceiverTracks = [];
    private readonly HashSet<int> _configuredReceiverTracks = [];
    private readonly HashSet<string> _seenRemoteIceCandidateKeys = [];
    private readonly List<(string Candidate, string Mid, string PayloadJson)> _sentLocalIceCandidates = [];
    private readonly object _rtpGate = new();
    private readonly object _audioOggGate = new();
    private readonly OggOpusPacketReader _audioOggPacketReader = new();
    private readonly OggOpusPageWriter _audioFallbackPageWriter = new();
    private byte[]? _h264FragmentBuffer;
    private uint _h264FragmentTimestamp;
    private ushort _h264LastSequence;
    private bool _h264FragmentActive;
    private MemoryStream? _h264AccessUnitBuffer;
    private uint _h264AccessUnitTimestamp;
    private ushort _h264AccessUnitLastSequence;
    private bool _h264AccessUnitActive;
    private string _lastLocalDescriptionType = string.Empty;
    private string _lastLocalDescriptionPayloadJson = string.Empty;
    private DateTime _lastNegotiationResendUtc = DateTime.MinValue;
    private long _localIceCandidatesSent;
    private long _remoteIceCandidatesSeen;
    private long _mediaTracksSeen;
    private PublisherConfig? _publisherConfig;
    private ReceiverConfig? _receiverConfig;
    private FfmpegH264LiveTextureSender? _videoPump;
    private FfmpegOpusAudioSender? _audioPump;
    private FfmpegOpusAudioPlayer? _audioPlayer;
    private FfmpegH264FrameDecoder? _decoder;
    private D3D11SharedTextureFrameSink? _receiverTextureSink;
    private bool _decoderStartAttempted;
    private long _receiverTextureWriteFailures;
    private long _receiverTextureAdvertiseCount;
    private DateTime _lastReceiverTextureFailureUtc = DateTime.MinValue;
    private DateTime _lastReceiverTextureAdvertiseUtc = DateTime.MinValue;
    private const int SharedVideoSendQueueCapacity = 4;
    private BlockingCollection<byte[]>? _sharedVideoQueue;
    private CancellationTokenSource? _sharedVideoQueueCts;
    private Task? _sharedVideoQueueTask;
    private long _sharedVideoQueuedFrames;
    private long _sharedVideoDroppedFrames;
    private long _sentFrames;
    private long _sendFailures;
    private long _receivedFrames;
    private long _receivedBytes;
    private long _sentAudioBytes;
    private long _sendAudioFailures;
    private long _receivedAudioBytes;
    private long _videoTrackPacketsSeen;
    private long _videoTrackRtpPayloadsSeen;
    private long _videoTrackRawAccessUnitsSeen;
    private long _videoTrackDroppedPacketsSeen;
    private long _videoChannelAccessUnitsSeen;
    private long _videoChannelSendFailures;
    private bool _firstVideoTrackPacketLogged;
    private bool _firstVideoRtpPayloadLogged;
    private bool _firstVideoRawAccessUnitLogged;
    private bool _firstVideoChannelAccessUnitLogged;
    private DateTime _lastVideoRtpGapLogUtc = DateTime.MinValue;
    private long _audioTrackPacketsSeen;
    private long _audioTrackRtpPayloadsSeen;
    private long _audioTrackRawPayloadsSeen;
    private long _audioTrackDroppedPacketsSeen;
    private bool _firstAudioTrackPacketLogged;
    private bool _firstAudioRtpPayloadLogged;
    private bool _firstAudioPlayerWriteLogged;
    private long _audioPlayerWriteFailures;
    private DateTime _lastSendFailureLogUtc = DateTime.MinValue;
    private DateTime _lastAudioFailureLogUtc = DateTime.MinValue;
    private DateTime _lastAudioStartAttemptUtc = DateTime.MinValue;
    private volatile bool _audioPumpStartInProgress;
    private DateTime _lastReceiveStatusUtc = DateTime.MinValue;
    private DateTime _lastAudioStatusUtc = DateTime.MinValue;
    private bool _playbackMuted;
    private float _playbackVolume = 1f;
    private readonly object _videoPresentGate = new();
    private readonly Queue<QueuedVideoFrame> _pendingVideoPresentFrames = new();
    private bool _videoPresentWorkerRunning;
    private DateTime _firstDecodedVideoFrameUtc = DateTime.MinValue;
    private DateTime _lastVideoSyncWaitLogUtc = DateTime.MinValue;
    private bool _videoSyncInitialHoldLogged;
    private long _videoSyncPresentedFrames;
    private long _videoSyncDroppedFrames;
    private static readonly int ReceiverVideoSyncDelayOverrideMs = ResolveReceiverVideoSyncDelayOverrideMs();
    private static readonly int ReceiverAudioStartWaitMs = ResolveReceiverAudioStartWaitMs();
    private static readonly int ReceiverVideoSyncQueueLimit = ResolveReceiverVideoSyncQueueLimit();

    public string PeerId { get; }
    public RtcPeerRole Role { get; }
    public string StunServersJson { get; }
    public bool IsConnected => _connected;
    public bool HasRemoteDescription => _remoteDescriptionSeen;
    private bool CanSendMedia => _connected || _iceTransportReady || _videoTrackOpen || CanSendAudio;
    private bool CanSendVideo => _videoChannelOpen || _videoTrackOpen || _videoTrack >= 0 && (_connected || _iceTransportReady || IsNativeTrackOpen(_videoTrack));
    private bool CanSendAudio => _audioTrackOpen || _audioTrack >= 0 && (_connected || _iceTransportReady || IsNativeTrackOpen(_audioTrack));

    private static bool IsNativeTrackOpen(int track)
    {
        if (track < 0) return false;
        try { return LibDataChannelNative.rtcIsOpen(track); }
        catch { return false; }
    }

    public RtcPeerSession(string peerId, RtcPeerRole role, string stunServersJson, BridgeTransport transport, Func<bool> publisherActive, Func<bool> receiverActive, Func<int> viewerCount, DirectStreamPublisherMediaHub? publisherMediaHub = null)
    {
        PeerId = peerId;
        Role = role;
        StunServersJson = stunServersJson ?? string.Empty;
        _transport = transport;
        _publisherActive = publisherActive;
        _receiverActive = receiverActive;
        _viewerCount = viewerCount;
        _publisherMediaHub = publisherMediaHub;
    }

    internal bool CanSendSharedVideo => Role == RtcPeerRole.Publisher && CanSendVideo;
    internal bool CanSendSharedAudio => Role == RtcPeerRole.Publisher && (CanSendAudio || _audioChannelOpen);
    internal long SharedVideoQueuedFrames => Math.Max(0, Interlocked.Read(ref _sharedVideoQueuedFrames));
    internal long SharedVideoDroppedFrames => Interlocked.Read(ref _sharedVideoDroppedFrames);

    internal void SendSharedEncodedVideoSample(byte[] accessUnit) => SendEncodedVideoSample(accessUnit);

    internal bool QueueSharedEncodedVideoSample(byte[] accessUnit)
    {
        if (accessUnit.Length == 0 || Role != RtcPeerRole.Publisher) return false;
        var queue = EnsureSharedVideoQueue();
        if (queue is null) return false;

        try
        {
            if (queue.TryAdd(accessUnit))
            {
                Interlocked.Increment(ref _sharedVideoQueuedFrames);
                return true;
            }

            if (queue.TryTake(out _))
            {
                Interlocked.Decrement(ref _sharedVideoQueuedFrames);
                Interlocked.Increment(ref _sharedVideoDroppedFrames);
            }

            if (queue.TryAdd(accessUnit))
            {
                Interlocked.Increment(ref _sharedVideoQueuedFrames);
                return true;
            }
        }
        catch (InvalidOperationException)
        {
        }

        Interlocked.Increment(ref _sharedVideoDroppedFrames);
        return false;
    }

    internal void SendSharedOpusAudioPacket(byte[] packet)
    {
        SendOpusAudioPacket(packet);
        if (!_audioChannelOpen) return;
        foreach (var page in _audioFallbackPageWriter.WriteRawOpusPacket(packet))
            SendOggAudioChunkToDataChannel(page);
    }

    public void SetPlaybackAudioState(bool muted, float volume)
    {
        var safeVolume = Math.Clamp(volume, 0f, 1f);
        _playbackMuted = muted;
        _playbackVolume = safeVolume;
        _audioPlayer?.SetPlaybackAudioState(muted, safeVolume);
    }

    public void StartPublisher(PublisherConfig config)
    {
        _publisherConfig = config;
        _publisherMediaHub?.RegisterSession(this);
        EnsurePeerConnection();
        AddSendTracks(config);
        AddAudioDataChannel();
        AddVideoDataChannel();
        // Do not start FFmpeg/D3D readback until libdatachannel actually opens the viewer media path.
        // Starting the live texture pump during handshake hammered the shared texture even when no media
        // could leave the host yet, causing visible game hitches/stutters while the viewer was still stuck
        // on "Waiting for host video".
        var rc = LibDataChannelNative.rtcSetLocalDescription(_pc, "offer");
        if (rc != 0)
            throw new InvalidOperationException($"libdatachannel rtcSetLocalDescription(offer) failed with {rc}.");
        _localDescriptionSent = true;
        Program.Log($"Direct Stream publisher created local offer for viewer {PeerId}. {BuildHandshakeDetail()}");
        ArmHandshakeWatchdog("host offer sent");
    }

    public void StartReceiver(ReceiverConfig config)
    {
        _receiverConfig = config;
        EnsurePeerConnection();
        ArmHandshakeWatchdog("viewer waiting for host offer");
    }

    public bool ResendLocalNegotiation(string reason)
    {
        if (string.IsNullOrWhiteSpace(_lastLocalDescriptionPayloadJson) || string.IsNullOrWhiteSpace(_lastLocalDescriptionType))
            return false;

        var now = DateTime.UtcNow;
        if ((now - _lastNegotiationResendUtc).TotalMilliseconds < 750)
            return true;

        List<(string Candidate, string Mid, string PayloadJson)> candidates;
        lock (_gate)
        {
            candidates = [.. _sentLocalIceCandidates];
            _lastNegotiationResendUtc = now;
        }

        Program.Log($"Direct Stream {Role} resending existing local {_lastLocalDescriptionType} to {PeerId} after {reason}; candidates={candidates.Count}. No new SDP/ICE restart is created.");
        _ = _transport.SendSignalAsync(PeerId, _lastLocalDescriptionType, _lastLocalDescriptionPayloadJson);
        foreach (var (_, _, candidatePayloadJson) in candidates)
            _ = _transport.SendSignalAsync(PeerId, "ice", candidatePayloadJson);
        return true;
    }

    public void HandleSignal(string signalType, string payloadJson)
    {
        EnsurePeerConnection();
        var type = signalType.Trim().ToLowerInvariant();
        if (type is "offer" or "answer")
        {
            var (sdp, descriptionType) = ParseDescriptionSignal(payloadJson, type);
            Program.Log($"Direct Stream {Role} received remote {descriptionType} from {PeerId}. SDP length={sdp.Length}.");

            if (Role == RtcPeerRole.Receiver && type == "offer" && _remoteDescriptionSeen)
            {
                Program.Log($"Direct Stream receiver ignored duplicate/restarted offer from {PeerId}; current session already has a remote offer and local answer. Restarting ICE in-place is not supported by this BridgeHost session.");
                ResendLocalNegotiation("duplicate host offer ignored");
                return;
            }

            if (Role == RtcPeerRole.Publisher && type == "answer" && _remoteDescriptionSeen)
            {
                Program.Log($"Direct Stream publisher ignored duplicate remote answer from {PeerId}; the answer is already applied and the signalling state is stable.");
                return;
            }

            var rc = LibDataChannelNative.rtcSetRemoteDescription(_pc, sdp, descriptionType);
            if (rc != 0)
                throw new InvalidOperationException($"libdatachannel rtcSetRemoteDescription({descriptionType}) failed with {rc}.");

            _remoteDescriptionSeen = true;
            DrainPendingRemoteIceCandidates();

            if (type == "offer")
            {
                rc = LibDataChannelNative.rtcSetLocalDescription(_pc, "answer");
                if (rc != 0)
                    throw new InvalidOperationException($"libdatachannel rtcSetLocalDescription(answer) failed with {rc}.");
                _localDescriptionSent = true;
                Program.Log($"Direct Stream receiver sent answer to host {PeerId}. {BuildHandshakeDetail()}");
                QueueConfigurePendingReceiverTracks("viewer answer sent");
                ArmHandshakeWatchdog("viewer answer sent");
            }
            else if (Role == RtcPeerRole.Publisher)
            {
                MaybeStartPublisherMedia("viewer answer accepted");
            }
            return;
        }

        if (type is "ice" or "candidate" or "icecandidate")
        {
            var (candidate, mid) = ParseCandidateSignal(payloadJson);
            if (string.IsNullOrWhiteSpace(candidate)) return;

            if (!_remoteDescriptionSeen)
            {
                lock (_gate)
                {
                    if (!_remoteDescriptionSeen)
                    {
                        if (_pendingRemoteIceCandidates.Count < 128)
                            _pendingRemoteIceCandidates.Add((candidate, mid));
                        Program.Log($"Direct Stream {Role} queued remote ICE from {PeerId} until the remote description is set. mid={mid}; candidateLength={candidate.Length}.");
                        return;
                    }
                }
            }

            AddRemoteIceCandidate(candidate, mid);
        }
    }

    private void AddRemoteIceCandidate(string candidate, string mid)
    {
        var key = (mid ?? string.Empty) + "" + candidate;
        lock (_gate)
        {
            if (!_seenRemoteIceCandidateKeys.Add(key))
            {
                Program.Log($"Direct Stream {Role} ignored duplicate remote ICE from {PeerId}. mid={mid}; candidateLength={candidate.Length}.");
                return;
            }
        }

        Interlocked.Increment(ref _remoteIceCandidatesSeen);
        Program.Log($"Direct Stream {Role} received remote ICE from {PeerId}. mid={mid}; candidateLength={candidate.Length}.");
        var pc = _pc;
        if (_disposed || pc < 0)
        {
            Program.Log($"Direct Stream {Role} ignored remote ICE from {PeerId} because the peer connection is already closed. mid={mid}; candidateLength={candidate.Length}.");
            return;
        }

        var rc = LibDataChannelNative.rtcAddRemoteCandidate(pc, candidate, string.IsNullOrWhiteSpace(mid) ? null : mid);
        if (rc != 0)
        {
            // Late/duplicate/stale trickle ICE can arrive after a peer has restarted, left, or had its transport
            // replaced. libdatachannel returns a negative code for those candidates. They are not worth killing
            // the whole BridgeHost over; the next valid candidate or offer/answer retry can still connect.
            Program.Log($"Direct Stream {Role} ignored remote ICE candidate from {PeerId}: rtcAddRemoteCandidate returned {rc}. mid={mid}; candidateLength={candidate.Length}; remoteDescriptionSeen={_remoteDescriptionSeen}; connected={_connected}.");
            return;
        }
    }

    private void DrainPendingRemoteIceCandidates()
    {
        List<(string Candidate, string Mid)> pending;
        lock (_gate)
        {
            if (_pendingRemoteIceCandidates.Count == 0)
                return;

            pending = [.. _pendingRemoteIceCandidates];
            _pendingRemoteIceCandidates.Clear();
        }

        foreach (var (candidate, mid) in pending)
            AddRemoteIceCandidate(candidate, mid);
    }

    private void EnsurePeerConnection()
    {
        lock (_gate)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(RtcPeerSession));
            if (_pc >= 0) return;

            using var ice = RtcStringArray.FromJson(StunServersJson);
            var config = new LibDataChannelNative.RtcConfiguration
            {
                iceServers = ice.Pointer,
                iceServersCount = ice.Count,
                certificateType = 0,
                iceTransportPolicy = 0,
                // Keep UDP as the fast path, but allow libdatachannel to gather TCP candidates too.
                // This gives Direct Stream a better chance on Windows/Wine machines where UDP inbound
                // is blocked until the firewall rule has been accepted.
                enableIceTcp = true,
                enableIceUdpMux = false,
                disableAutoNegotiation = true,
                forceMediaTransport = false,
                mtu = 0,
                maxMessageSize = 0
            };

            var pc = LibDataChannelNative.rtcCreatePeerConnection(ref config);
            if (pc < 0)
                throw new InvalidOperationException($"libdatachannel rtcCreatePeerConnection failed with {pc}.");

            _pc = pc;
            SessionsByPc[pc] = this;
            LibDataChannelNative.rtcSetLocalDescriptionCallback(pc, DescriptionCallback);
            LibDataChannelNative.rtcSetLocalCandidateCallback(pc, CandidateCallback);
            LibDataChannelNative.rtcSetStateChangeCallback(pc, StateCallback);
            LibDataChannelNative.rtcSetIceStateChangeCallback(pc, IceStateCallback);
            LibDataChannelNative.rtcSetTrackCallback(pc, TrackCallback);
            LibDataChannelNative.rtcSetDataChannelCallback(pc, DataChannelCallback);
        }
    }

    private void AddSendTracks(PublisherConfig config)
    {
        lock (_gate)
        {
            if (_videoTrack < 0)
            {
                var videoSsrc = (uint)Random.Shared.Next(1, int.MaxValue);
                var mediaSdp = string.Join("\r\n", new[]
                {
                    "m=video 9 UDP/TLS/RTP/SAVPF 102",
                    "c=IN IP4 0.0.0.0",
                    "a=mid:video",
                    "a=sendonly",
                    "a=rtpmap:102 H264/90000",
                    "a=fmtp:102 packetization-mode=1;profile-level-id=42e01f;level-asymmetry-allowed=1",
                    "a=msid:ravacast_stream ravacast_video",
                    $"a=ssrc:{videoSsrc} cname:ravacast"
                });

                _videoTrack = LibDataChannelNative.rtcAddTrack(_pc, mediaSdp);
                if (_videoTrack < 0)
                    throw new InvalidOperationException($"libdatachannel rtcAddTrack(video) failed with {_videoTrack}.");

                SessionsByTrack[_videoTrack] = this;
                LibDataChannelNative.rtcSetOpenCallback(_videoTrack, OpenCallback);
                LibDataChannelNative.rtcSetClosedCallback(_videoTrack, ClosedCallback);
                LibDataChannelNative.rtcSetErrorCallback(_videoTrack, ErrorCallback);

                var packetizer = LibDataChannelNative.RtcPacketizerInit.ForH264(videoSsrc, 102);
                try
                {
                    var rc = LibDataChannelNative.rtcSetH264Packetizer(_videoTrack, ref packetizer);
                    if (rc != 0)
                        Program.Log($"rtcSetH264Packetizer returned {rc}; H.264 frame send will not be active for this viewer until packetizer setup succeeds.");
                }
                finally
                {
                    Free(packetizer.cname);
                }

                _ = LibDataChannelNative.rtcChainRtcpSrReporter(_videoTrack);
                _ = LibDataChannelNative.rtcChainRtcpNackResponder(_videoTrack, 512);
            }

            if (config.AudioBitrateKbps > 0 && _audioTrack < 0)
            {
                var audioSsrc = (uint)Random.Shared.Next(1, int.MaxValue);
                var audioSdp = string.Join("\r\n", new[]
                {
                    "m=audio 9 UDP/TLS/RTP/SAVPF 111",
                    "c=IN IP4 0.0.0.0",
                    "a=mid:audio",
                    "a=sendonly",
                    "a=rtpmap:111 opus/48000/2",
                    "a=fmtp:111 minptime=10;useinbandfec=1",
                    "a=msid:ravacast_stream ravacast_audio",
                    $"a=ssrc:{audioSsrc} cname:ravacast"
                });

                _audioTrack = LibDataChannelNative.rtcAddTrack(_pc, audioSdp);
                if (_audioTrack < 0)
                {
                    Program.Log($"libdatachannel rtcAddTrack(audio) returned {_audioTrack}; continuing without Direct Stream audio for this viewer.");
                    _audioTrack = -1;
                    return;
                }

                SessionsByTrack[_audioTrack] = this;
                LibDataChannelNative.rtcSetOpenCallback(_audioTrack, OpenCallback);
                LibDataChannelNative.rtcSetClosedCallback(_audioTrack, ClosedCallback);
                LibDataChannelNative.rtcSetErrorCallback(_audioTrack, ErrorCallback);

                var audioPacketizer = LibDataChannelNative.RtcPacketizerInit.ForOpus(audioSsrc, 111);
                try
                {
                    var rc = LibDataChannelNative.rtcSetOpusPacketizer(_audioTrack, ref audioPacketizer);
                    if (rc != 0)
                        Program.Log($"rtcSetOpusPacketizer returned {rc}; Opus audio send will not be active for this viewer until packetizer setup succeeds.");
                }
                catch (EntryPointNotFoundException ex)
                {
                    Program.Log($"rtcSetOpusPacketizer is missing from datachannel.dll ({ex.Message}); Direct Stream audio cannot be sent on this libdatachannel build.");
                }
                finally
                {
                    Free(audioPacketizer.cname);
                }

                _ = LibDataChannelNative.rtcChainRtcpSrReporter(_audioTrack);
            }
        }
    }

    private void AddAudioDataChannel()
    {
        lock (_gate)
        {
            if (_audioChannel >= 0) return;
            _audioChannel = LibDataChannelNative.rtcCreateDataChannel(_pc, "ravacast-audio-opus");
            if (_audioChannel < 0)
            {
                Program.Log($"rtcCreateDataChannel(audio) returned {_audioChannel}; continuing without Direct Stream audio for this viewer.");
                _audioChannel = -1;
                return;
            }

            SessionsByTrack[_audioChannel] = this;
            LibDataChannelNative.rtcSetOpenCallback(_audioChannel, OpenCallback);
            LibDataChannelNative.rtcSetClosedCallback(_audioChannel, ClosedCallback);
            LibDataChannelNative.rtcSetErrorCallback(_audioChannel, ErrorCallback);
            LibDataChannelNative.rtcSetMessageCallback(_audioChannel, MessageCallback);
        }
    }

    private void AddVideoDataChannel()
    {
        lock (_gate)
        {
            if (_videoChannel >= 0) return;
            _videoChannel = LibDataChannelNative.rtcCreateDataChannel(_pc, "ravacast-video-h264");
            if (_videoChannel < 0)
            {
                Program.Log($"rtcCreateDataChannel(video) returned {_videoChannel}; continuing with RTP-only Direct Stream video for this viewer.");
                _videoChannel = -1;
                return;
            }

            SessionsByTrack[_videoChannel] = this;
            LibDataChannelNative.rtcSetOpenCallback(_videoChannel, OpenCallback);
            LibDataChannelNative.rtcSetClosedCallback(_videoChannel, ClosedCallback);
            LibDataChannelNative.rtcSetErrorCallback(_videoChannel, ErrorCallback);
            LibDataChannelNative.rtcSetMessageCallback(_videoChannel, MessageCallback);
        }
    }

    private void MaybeStartPublisherMedia(string reason)
    {
        if (Role != RtcPeerRole.Publisher) return;
        var config = _publisherConfig;
        if (config is null) return;

        var videoReady = CanSendSharedVideo;
        var audioReady = CanSendSharedAudio;
        if (!videoReady && !audioReady && !_connected)
        {
            Program.Log($"Direct Stream shared media hub not starting for viewer {PeerId} yet: {reason}; waiting for track open/ICE. {BuildHandshakeDetail()}");
            return;
        }

        if (_publisherMediaHub is null)
        {
            Program.Log($"Direct Stream shared media hub is missing for publisher viewer {PeerId}; cannot start host capture/encoder. {BuildHandshakeDetail()}");
            return;
        }

        Program.Log($"Direct Stream shared media hub checking for viewer {PeerId}: {reason}; videoReady={videoReady}; audioReady={audioReady}. {BuildHandshakeDetail()}");
        _publisherMediaHub.NotifySessionMediaReady(this, reason);
    }

    private void StartLiveTextureVideo(PublisherConfig config)
    {
        lock (_gate)
        {
            if (_videoPump is not null) return;
            if (FfmpegH264LiveTextureSender.TryStart(config, SendEncodedVideoSample, ReportSenderStatus, out var pump, out var detail))
            {
                _videoPump = pump;
                _ = _transport.SendStatusAsync("Live video ready", detail + " Viewers will receive the resolved RavaCast output, not a URL or local browser.", true, _receiverActive(), true, _viewerCount());
            }
            else
            {
                _ = _transport.SendErrorAsync("Direct Stream live video failed", detail + " Direct Stream v2 needs FFmpeg and the host shared D3D texture adapter to encode live RavaCast output.", true, _receiverActive(), true, _viewerCount());
            }
        }
    }

    private BlockingCollection<byte[]>? EnsureSharedVideoQueue()
    {
        lock (_gate)
        {
            if (_disposed) return null;
            if (_sharedVideoQueue is not null && !_sharedVideoQueue.IsAddingCompleted)
                return _sharedVideoQueue;

            var queue = new BlockingCollection<byte[]>(SharedVideoSendQueueCapacity);
            var cts = new CancellationTokenSource();
            _sharedVideoQueue = queue;
            _sharedVideoQueueCts = cts;
            _sharedVideoQueueTask = Task.Run(() => SharedVideoSendLoop(queue, cts.Token), cts.Token);
            return queue;
        }
    }

    private void SharedVideoSendLoop(BlockingCollection<byte[]> queue, CancellationToken token)
    {
        try
        {
            foreach (var accessUnit in queue.GetConsumingEnumerable(token))
            {
                Interlocked.Decrement(ref _sharedVideoQueuedFrames);
                if (token.IsCancellationRequested) break;
                SendEncodedVideoSample(accessUnit);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            Program.Log($"Direct Stream shared video queue failed for viewer {PeerId}: {Program.Flatten(ex)}");
        }
    }

    private void StopSharedVideoQueue()
    {
        BlockingCollection<byte[]>? queue;
        CancellationTokenSource? cts;
        lock (_gate)
        {
            queue = _sharedVideoQueue;
            cts = _sharedVideoQueueCts;
            _sharedVideoQueue = null;
            _sharedVideoQueueCts = null;
            _sharedVideoQueueTask = null;
        }

        try { cts?.Cancel(); } catch { }
        try { queue?.CompleteAdding(); } catch { }
        try { queue?.Dispose(); } catch { }
        try { cts?.Dispose(); } catch { }
        Interlocked.Exchange(ref _sharedVideoQueuedFrames, 0);
    }

    private void SendEncodedVideoSample(byte[] accessUnit)
    {
        if (accessUnit.Length == 0) return;

        // Primary path: send the encoded H.264 access unit over the negotiated video media track.
        // The raw video data channel is only a fallback. A previous version preferred the data
        // channel whenever the local side reported it open; on some libdatachannel builds the
        // publisher-side data channel can open while the receiver never raises OnDataChannel, so
        // frames were counted as sent locally but vanished before the receiver could decode them.
        int track;
        lock (_gate)
        {
            if (_disposed || _videoTrack < 0)
            {
                if (SendVideoAccessUnitToDataChannel(accessUnit))
                    Interlocked.Increment(ref _sentFrames);
                return;
            }
            track = _videoTrack;
        }

        if (_videoTrackOpen || _connected || _iceTransportReady || IsNativeTrackOpen(track))
        {
            int rc;
            try { rc = LibDataChannelNative.rtcSendMessage(track, accessUnit, accessUnit.Length); }
            catch (Exception ex)
            {
                Program.Log("rtcSendMessage(video) threw: " + Program.Flatten(ex));
                if (SendVideoAccessUnitToDataChannel(accessUnit))
                    Interlocked.Increment(ref _sentFrames);
                return;
            }

            if (rc == 0)
            {
                _videoTrackOpen = true;
                Interlocked.Increment(ref _sentFrames);
                return;
            }

            if (SendVideoAccessUnitToDataChannel(accessUnit))
            {
                Interlocked.Increment(ref _sentFrames);
                return;
            }

            Interlocked.Increment(ref _sendFailures);
            var now = DateTime.UtcNow;
            if ((now - _lastSendFailureLogUtc).TotalSeconds >= 2)
            {
                _lastSendFailureLogUtc = now;
                Program.Log($"rtcSendMessage(video) returned {rc} for viewer {PeerId}; sent={Interlocked.Read(ref _sentFrames)}, failures={Interlocked.Read(ref _sendFailures)}.");
            }
            return;
        }

        if (SendVideoAccessUnitToDataChannel(accessUnit))
            Interlocked.Increment(ref _sentFrames);
    }

    private bool SendVideoAccessUnitToDataChannel(byte[] accessUnit)
    {
        int channel;
        lock (_gate)
        {
            if (_disposed || _videoChannel < 0 || !_videoChannelOpen) return false;
            channel = _videoChannel;
        }

        try
        {
            if (!LibDataChannelNative.rtcIsOpen(channel)) return false;
        }
        catch
        {
            return false;
        }

        int rc;
        try { rc = LibDataChannelNative.rtcSendMessage(channel, accessUnit, accessUnit.Length); }
        catch (Exception ex)
        {
            Program.Log("rtcSendMessage(video data channel) threw: " + Program.Flatten(ex));
            return false;
        }

        if (rc == 0) return true;

        var failures = Interlocked.Increment(ref _videoChannelSendFailures);
        var now = DateTime.UtcNow;
        if ((now - _lastSendFailureLogUtc).TotalSeconds >= 2)
        {
            _lastSendFailureLogUtc = now;
            Program.Log($"rtcSendMessage(video data channel) returned {rc} for viewer {PeerId}; failures={failures:n0}; size={accessUnit.Length:n0}.");
        }
        return false;
    }

    private void ReportSenderStatus(string detail)
    {
        var sent = Interlocked.Read(ref _sentFrames);
        var failures = Interlocked.Read(ref _sendFailures);
        var sending = CanSendVideo;
        var extra = sending ? $" Sent to viewer: {sent:n0} frames" + (failures > 0 ? $", send retries/failures: {failures:n0}." : ".") : $" Waiting for viewer connection before sending frames. {BuildHandshakeDetail()}";
        _ = _transport.SendStatusAsync("Sending live video", detail + extra, true, _receiverActive(), true, _viewerCount());
    }

    private void QueueConfigurePendingReceiverTracks(string reason)
    {
        if (Role != RtcPeerRole.Receiver) return;
        (int Track, bool IsAudio)[] tracks;
        lock (_gate)
        {
            if (_pendingReceiverTracks.Count == 0) return;
            tracks = _pendingReceiverTracks.ToArray();
            _pendingReceiverTracks.Clear();
        }

        foreach (var pending in tracks)
        {
            if (pending.IsAudio) QueueConfigureReceiverAudioTrack(pending.Track, reason);
            else QueueConfigureReceiverTrack(pending.Track, reason);
        }
    }

    private void QueueConfigureReceiverTrack(int tr, string reason)
    {
        if (Role != RtcPeerRole.Receiver || tr < 0) return;
        _ = Task.Run(() => ConfigureReceiverTrack(tr, reason));
    }

    private void ConfigureReceiverTrack(int tr, string reason)
    {
        lock (_gate)
        {
            if (_disposed || _configuredReceiverTracks.Contains(tr)) return;
            _configuredReceiverTracks.Add(tr);
        }

        try
        {
            LibDataChannelNative.rtcSetOpenCallback(tr, OpenCallback);
            LibDataChannelNative.rtcSetClosedCallback(tr, ClosedCallback);
            LibDataChannelNative.rtcSetErrorCallback(tr, ErrorCallback);

            // Register the message callback even when native depacketization is available. Some
            // libdatachannel builds deliver media through rtcSetMessageCallback, while others deliver
            // depacketized frames through rtcSetFrameCallback. Audio already handled both paths; video
            // did not, which could leave Direct Stream with audio but a black receiver texture.
            LibDataChannelNative.rtcSetMessageCallback(tr, MessageCallback);

            var nativeDepacketizer = TryConfigureNativeH264Depacketizer(tr, out var depacketizerDetail);
            _ = LibDataChannelNative.rtcChainRtcpReceivingSession(tr);
            var frameCallbackDetail = TrySetOptionalFrameCallback(tr);

            if (nativeDepacketizer)
            {
                Program.Log($"Direct Stream receiver configured native H.264 media track {tr} from {PeerId} after {reason}. {depacketizerDetail} {frameCallbackDetail} Message callback also registered for RTP/raw fallback. {BuildHandshakeDetail()}");
                _ = _transport.SendStatusAsync("Receiving host media", $"Direct Stream received media track {tr}; native H.264 frames and message fallback are both armed. {BuildHandshakeDetail()}", _publisherActive(), true, true, _viewerCount());
            }
            else
            {
                Program.Log($"Direct Stream receiver configured RTP/raw fallback H.264 media track {tr} from {PeerId} after {reason}. {depacketizerDetail} {frameCallbackDetail} {BuildHandshakeDetail()}");
                _ = _transport.SendStatusAsync("Receiving host media", $"Direct Stream received media track {tr}; using C# RTP/raw H.264 fallback because native depacketization is unavailable. {BuildHandshakeDetail()}", _publisherActive(), true, true, _viewerCount());
            }
        }
        catch (Exception ex)
        {
            Program.Log($"Direct Stream receiver failed to configure media track {tr} from {PeerId}: {Program.Flatten(ex)}");
            _ = _transport.SendErrorAsync($"Direct Stream receiver failed to configure host media track: {ex.Message}", _publisherActive(), true, true, _viewerCount());
        }
    }

    private static bool TryConfigureNativeH264Depacketizer(int tr, out string detail)
    {
        try
        {
            var rc = LibDataChannelNative.rtcSetH264Depacketizer(tr, 3); // RTC_NAL_SEPARATOR_START_SEQUENCE
            if (rc == 0)
            {
                detail = "Native rtcSetH264Depacketizer configured.";
                return true;
            }

            detail = $"rtcSetH264Depacketizer returned {rc}; using RTP fallback.";
            return false;
        }
        catch (EntryPointNotFoundException ex)
        {
            detail = $"rtcSetH264Depacketizer is missing from datachannel.dll ({ex.Message}); using RTP fallback.";
            return false;
        }
        catch (DllNotFoundException ex)
        {
            detail = $"datachannel.dll could not be loaded for rtcSetH264Depacketizer ({ex.Message}); using RTP fallback.";
            return false;
        }
        catch (Exception ex)
        {
            detail = $"rtcSetH264Depacketizer failed ({ex.Message}); using RTP fallback.";
            return false;
        }
    }

    private void QueueConfigureReceiverAudioTrack(int tr, string reason)
    {
        if (Role != RtcPeerRole.Receiver || tr < 0) return;
        _ = Task.Run(() => ConfigureReceiverAudioTrack(tr, reason));
    }

    private void ConfigureReceiverAudioTrack(int tr, string reason)
    {
        lock (_gate)
        {
            if (_disposed || _configuredReceiverTracks.Contains(tr)) return;
            _configuredReceiverTracks.Add(tr);
        }

        try
        {
            LibDataChannelNative.rtcSetOpenCallback(tr, OpenCallback);
            LibDataChannelNative.rtcSetClosedCallback(tr, ClosedCallback);
            LibDataChannelNative.rtcSetErrorCallback(tr, ErrorCallback);

            // Always register the message callback first. This datachannel.dll build does not export
            // rtcSetFrameCallback, but it still delivers media RTP packets through rtcSetMessageCallback.
            // If we let the missing frame callback throw the whole setup path away, the UI reports an
            // audio-track failure even though RTP packets can still arrive.
            LibDataChannelNative.rtcSetMessageCallback(tr, MessageCallback);

            var nativeDepacketizer = TryConfigureNativeOpusDepacketizer(tr, out var depacketizerDetail);
            _ = LibDataChannelNative.rtcChainRtcpReceivingSession(tr);

            var frameCallbackDetail = TrySetOptionalFrameCallback(tr);
            if (nativeDepacketizer)
            {
                Program.Log($"Direct Stream receiver configured native Opus audio track {tr} from {PeerId} after {reason}. {depacketizerDetail} {frameCallbackDetail} {BuildHandshakeDetail()}");
                _ = _transport.SendStatusAsync("Receiving host audio", $"Direct Stream received Opus audio track {tr}; browser-only host audio will play when RTP frames arrive. {BuildHandshakeDetail()}", _publisherActive(), true, true, _viewerCount());
            }
            else
            {
                Program.Log($"Direct Stream receiver configured RTP/raw fallback Opus audio track {tr} from {PeerId} after {reason}. {depacketizerDetail} {frameCallbackDetail} {BuildHandshakeDetail()}");
                _ = _transport.SendStatusAsync("Receiving host audio", $"Direct Stream received Opus audio track {tr}; using C# RTP/raw Opus fallback because this libdatachannel build has no native Opus depacketizer. {BuildHandshakeDetail()}", _publisherActive(), true, true, _viewerCount());
            }
        }
        catch (Exception ex)
        {
            Program.Log($"Direct Stream receiver failed to configure Opus audio track {tr} from {PeerId}: {Program.Flatten(ex)}");
            _ = _transport.SendErrorAsync($"Direct Stream receiver failed to configure host audio track: {ex.Message}", _publisherActive(), true, true, _viewerCount());
        }
    }

    private static string TrySetOptionalFrameCallback(int tr)
    {
        try
        {
            LibDataChannelNative.rtcSetFrameCallback(tr, FrameCallback);
            return "Frame callback registered.";
        }
        catch (EntryPointNotFoundException ex)
        {
            return $"rtcSetFrameCallback is missing from datachannel.dll ({ex.Message}); message callback only.";
        }
        catch (DllNotFoundException ex)
        {
            return $"datachannel.dll could not be loaded for rtcSetFrameCallback ({ex.Message}); message callback only.";
        }
        catch (Exception ex)
        {
            return $"rtcSetFrameCallback could not be registered ({ex.Message}); message callback only.";
        }
    }

    private static bool TryConfigureNativeOpusDepacketizer(int tr, out string detail)
    {
        try
        {
            var rc = LibDataChannelNative.rtcSetOpusDepacketizer(tr);
            if (rc == 0)
            {
                detail = "Native rtcSetOpusDepacketizer configured.";
                return true;
            }

            detail = $"rtcSetOpusDepacketizer returned {rc}; using RTP fallback.";
            return false;
        }
        catch (EntryPointNotFoundException ex)
        {
            detail = $"rtcSetOpusDepacketizer is missing from datachannel.dll ({ex.Message}); using RTP fallback.";
            return false;
        }
        catch (DllNotFoundException ex)
        {
            detail = $"datachannel.dll could not be loaded for rtcSetOpusDepacketizer ({ex.Message}); using RTP fallback.";
            return false;
        }
    }

    private void ReceiveVideoTransportMessage(IntPtr data, int size)
    {
        if (Role != RtcPeerRole.Receiver || data == IntPtr.Zero || size <= 0) return;
        var packet = new byte[size];
        Marshal.Copy(data, packet, 0, size);
        ReceiveVideoTrackBytes(packet, "message");
    }


    private void ReceiveVideoDataChannelMessage(IntPtr data, int size)
    {
        if (Role != RtcPeerRole.Receiver || data == IntPtr.Zero || size <= 0) return;
        var packet = new byte[size];
        Marshal.Copy(data, packet, 0, size);
        ReceiveVideoDataChannelBytes(packet);
    }

    private void ReceiveVideoDataChannelBytes(byte[] accessUnit)
    {
        if (accessUnit.Length == 0) return;
        var seen = Interlocked.Increment(ref _videoChannelAccessUnitsSeen);
        if (!_firstVideoChannelAccessUnitLogged)
        {
            _firstVideoChannelAccessUnitLogged = true;
            Program.Log($"Direct Stream receiver saw first raw H.264 video data-channel access unit from {PeerId}: size={accessUnit.Length}; annexB={LooksLikeAnnexBH264(accessUnit)}; avcc={LooksLikeLengthPrefixedH264(accessUnit)}; singleNal={LooksLikeSingleH264Nal(accessUnit)}; firstBytes={FormatFirstBytes(accessUnit)}. {BuildHandshakeDetail()}");
        }

        if (LooksLikeAnnexBH264(accessUnit))
        {
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            ReceiveEncodedVideoFrameBytes(accessUnit);
            return;
        }

        if (TryConvertLengthPrefixedH264ToAnnexB(accessUnit, out var converted))
        {
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            ReceiveEncodedVideoFrameBytes(converted);
            return;
        }

        if (LooksLikeSingleH264Nal(accessUnit))
        {
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            ReceiveEncodedVideoFrameBytes(WrapH264NalAsAnnexB(accessUnit));
            return;
        }

        var dropped = Interlocked.Increment(ref _videoTrackDroppedPacketsSeen);
        if (dropped == 1 || dropped % 128 == 0)
            Program.Log($"Direct Stream receiver could not classify H.264 video data-channel packet from {PeerId}: size={accessUnit.Length}; videoDc={seen:n0}; dropped={dropped:n0}; firstBytes={FormatFirstBytes(accessUnit)}. {BuildHandshakeDetail()}");
    }

    private void ReceiveVideoTrackBytes(byte[] packet, string source)
    {
        if (packet.Length == 0) return;
        if (_videoChannelOpen && Interlocked.Read(ref _videoChannelAccessUnitsSeen) > 0) return;

        var seen = Interlocked.Increment(ref _videoTrackPacketsSeen);
        var looksRtp = LooksLikeRtpPacket(packet);
        var payloadType = looksRtp ? packet[1] & 0x7F : -1;

        if (!_firstVideoTrackPacketLogged)
        {
            _firstVideoTrackPacketLogged = true;
            Program.Log($"Direct Stream receiver saw first H.264 video track {source} packet from {PeerId}: size={packet.Length}; looksRtp={looksRtp}; payloadType={payloadType}; annexB={LooksLikeAnnexBH264(packet)}; avcc={LooksLikeLengthPrefixedH264(packet)}; firstBytes={FormatFirstBytes(packet)}. {BuildHandshakeDetail()}");
        }

        if (looksRtp)
        {
            ReceiveVideoRtpPacketBytes(packet, payloadType);
            return;
        }

        if (LooksLikeAnnexBH264(packet))
        {
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            LogFirstVideoRawAccessUnit(source, packet, "Annex-B");
            ReceiveEncodedVideoFrameBytes(packet);
            return;
        }

        if (TryConvertLengthPrefixedH264ToAnnexB(packet, out var lengthPrefixedAccessUnit))
        {
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            LogFirstVideoRawAccessUnit(source, lengthPrefixedAccessUnit, "length-prefixed/AVCC");
            ReceiveEncodedVideoFrameBytes(lengthPrefixedAccessUnit);
            return;
        }

        if (LooksLikeSingleH264Nal(packet))
        {
            var accessUnit = WrapH264NalAsAnnexB(packet);
            Interlocked.Increment(ref _videoTrackRawAccessUnitsSeen);
            LogFirstVideoRawAccessUnit(source, accessUnit, "single NAL");
            ReceiveEncodedVideoFrameBytes(accessUnit);
            return;
        }

        var dropped = Interlocked.Increment(ref _videoTrackDroppedPacketsSeen);
        if (dropped == 1 || dropped % 128 == 0)
            Program.Log($"Direct Stream receiver could not classify H.264 video track {source} packet from {PeerId}: size={packet.Length}; seen={seen:n0}; dropped={dropped:n0}; firstBytes={FormatFirstBytes(packet)}. {BuildHandshakeDetail()}");
    }

    private void ReceiveVideoRtpPacketBytes(byte[] packet, int payloadType)
    {
        if (packet.Length == 0) return;
        if (payloadType != 102)
        {
            var dropped = Interlocked.Increment(ref _videoTrackDroppedPacketsSeen);
            if (dropped == 1 || dropped % 128 == 0)
                Program.Log($"Direct Stream receiver dropped H.264 RTP packet from {PeerId}: unexpected payloadType={payloadType}; size={packet.Length}; dropped={dropped:n0}; firstBytes={FormatFirstBytes(packet)}.");
            return;
        }

        var rtpPackets = Interlocked.Increment(ref _videoTrackRtpPayloadsSeen);
        if (!_firstVideoRtpPayloadLogged)
        {
            _firstVideoRtpPayloadLogged = true;
            Program.Log($"Direct Stream receiver assembling first H.264 RTP packet in-process from {PeerId}: packet={packet.Length} bytes; rtpPackets={rtpPackets:n0}; firstBytes={FormatFirstBytes(packet)}.");
        }

        var bytes = Interlocked.Add(ref _receivedBytes, packet.Length);

        // Do the RTP-to-Annex-B assembly in-process, then feed the existing raw-H.264 FFmpeg
        // decoder path. The previous UDP/SDP loopback decoder could happily receive RTP packets
        // while producing zero decoded frames, which left the viewer with only the initially-created
        // grey receiver texture. Audio still worked because its RTP fallback is handled directly.
        var completedAccessUnits = 0;
        foreach (var accessUnit in DepacketizeH264Rtp(packet))
        {
            completedAccessUnits++;
            ReceiveEncodedVideoFrameBytes(accessUnit);
        }

        var now = DateTime.UtcNow;
        if ((now - _lastReceiveStatusUtc).TotalSeconds < 2 && rtpPackets % 180 != 0) return;
        _lastReceiveStatusUtc = now;
        var decoder = _decoder;
        var decoded = decoder?.DecodedFrames ?? 0;
        var written = _receiverTextureSink?.WrittenFrames ?? 0;
        _ = _transport.SendStatusAsync("Host video RTP arriving", $"Received {rtpPackets:n0} H.264 RTP packets ({bytes / 1024:n0} KB). Completed AUs={completedAccessUnits:n0}; decoded={decoded:n0}; shown={written:n0}.", _publisherActive(), true, true, _viewerCount());
    }

    private void LogFirstVideoRawAccessUnit(string source, byte[] accessUnit, string format)
    {
        if (_firstVideoRawAccessUnitLogged) return;
        _firstVideoRawAccessUnitLogged = true;
        Program.Log($"Direct Stream receiver accepted first raw H.264 {format} {source} access unit from {PeerId}: size={accessUnit.Length}; firstBytes={FormatFirstBytes(accessUnit)}. {BuildHandshakeDetail()}");
    }

    private void ReceiveAudioTransportMessage(IntPtr data, int size)
    {
        if (Role != RtcPeerRole.Receiver || data == IntPtr.Zero || size <= 0) return;
        var packet = new byte[size];
        Marshal.Copy(data, packet, 0, size);
        ReceiveAudioTrackBytes(packet, "message");
    }

    private void ReceiveAudioTrackBytes(byte[] packet, string source)
    {
        if (packet.Length == 0) return;
        var seen = Interlocked.Increment(ref _audioTrackPacketsSeen);
        var looksRtp = LooksLikeRtpPacket(packet);
        var payloadType = looksRtp ? packet[1] & 0x7F : -1;

        if (!_firstAudioTrackPacketLogged)
        {
            _firstAudioTrackPacketLogged = true;
            Program.Log($"Direct Stream receiver saw first Opus audio track {source} packet from {PeerId}: size={packet.Length}; looksRtp={looksRtp}; payloadType={payloadType}; firstBytes={FormatFirstBytes(packet)}. {BuildHandshakeDetail()}");
        }

        if (TryExtractRtpPayload(packet, 111, out var payload))
        {
            var rtpPayloads = Interlocked.Increment(ref _audioTrackRtpPayloadsSeen);
            if (!_firstAudioRtpPayloadLogged)
            {
                _firstAudioRtpPayloadLogged = true;
                Program.Log($"Direct Stream receiver extracted first Opus RTP payload from {PeerId}: payload={payload.Length} bytes; rtpPayloads={rtpPayloads:n0}; firstBytes={FormatFirstBytes(payload)}.");
            }

            ReceiveOpusAudioFrameBytes(payload);
            return;
        }

        if (looksRtp)
        {
            var dropped = Interlocked.Increment(ref _audioTrackDroppedPacketsSeen);
            if (dropped == 1 || dropped % 128 == 0)
                Program.Log($"Direct Stream receiver dropped Opus audio RTP packet from {PeerId}: unexpected payloadType={payloadType}; size={packet.Length}; dropped={dropped:n0}.");
            return;
        }

        // Some libdatachannel builds give the application the raw Opus payload rather than the RTP
        // packet when no native Opus depacketizer is available. Treat non-RTP track bytes as raw Opus.
        Interlocked.Increment(ref _audioTrackRawPayloadsSeen);
        if (LooksLikeOggPage(packet))
            ReceiveAudioBytes(packet);
        else
            ReceiveOpusAudioFrameBytes(packet);
    }

    private static bool TryExtractRtpPayload(byte[] packet, byte expectedPayloadType, out byte[] payload)
    {
        payload = [];
        if (packet.Length < 12) return false;
        if ((packet[0] >> 6) != 2) return false;

        var csrcCount = packet[0] & 0x0F;
        var extension = (packet[0] & 0x10) != 0;
        var payloadType = packet[1] & 0x7F;
        var offset = 12 + (csrcCount * 4);
        if (offset > packet.Length) return false;

        if (extension)
        {
            if (offset + 4 > packet.Length) return false;
            var extensionLengthWords = (packet[offset + 2] << 8) | packet[offset + 3];
            offset += 4 + (extensionLengthWords * 4);
            if (offset > packet.Length) return false;
        }

        if (offset >= packet.Length || payloadType != expectedPayloadType) return false;
        payload = new byte[packet.Length - offset];
        Buffer.BlockCopy(packet, offset, payload, 0, payload.Length);
        return payload.Length > 0;
    }

    private static bool LooksLikeRtpPacket(byte[] packet) => packet.Length >= 12 && (packet[0] >> 6) == 2;

    private static bool LooksLikeAnnexBH264(byte[] packet) => H264StartCodeLengthAt(packet, 0) > 0;

    private static bool LooksLikeLengthPrefixedH264(byte[] packet)
    {
        if (packet.Length < 5) return false;
        var nalLength = ReadBigEndianInt32(packet, 0);
        if (nalLength <= 0 || nalLength > packet.Length - 4) return false;
        return LooksLikeH264NalHeader(packet[4]);
    }

    private static bool LooksLikeSingleH264Nal(byte[] packet)
    {
        if (packet.Length == 0 || packet.Length > 2 * 1024 * 1024) return false;
        return LooksLikeH264NalHeader(packet[0]);
    }

    private static bool LooksLikeH264NalHeader(byte nalHeader)
    {
        if ((nalHeader & 0x80) != 0) return false;
        var nalType = nalHeader & 0x1F;
        return nalType is >= 1 and <= 31;
    }

    private static byte[] WrapH264NalAsAnnexB(byte[] nal)
    {
        var accessUnit = new byte[nal.Length + 4];
        WriteAnnexBStartCode(accessUnit, 0);
        Buffer.BlockCopy(nal, 0, accessUnit, 4, nal.Length);
        return accessUnit;
    }

    private static bool TryConvertLengthPrefixedH264ToAnnexB(byte[] packet, out byte[] accessUnit)
    {
        accessUnit = [];
        if (packet.Length < 5 || LooksLikeRtpPacket(packet) || LooksLikeAnnexBH264(packet)) return false;

        var pos = 0;
        using var combined = new MemoryStream(packet.Length + 32);
        while (pos + 4 <= packet.Length)
        {
            var nalLength = ReadBigEndianInt32(packet, pos);
            pos += 4;
            if (nalLength <= 0 || nalLength > packet.Length - pos) return false;
            if (!LooksLikeH264NalHeader(packet[pos])) return false;

            combined.Write(new byte[] { 0, 0, 0, 1 });
            combined.Write(packet, pos, nalLength);
            pos += nalLength;
        }

        if (pos != packet.Length || combined.Length <= 4) return false;
        accessUnit = combined.ToArray();
        return true;
    }

    private static int ReadBigEndianInt32(byte[] packet, int offset)
        => packet.Length >= offset + 4
            ? (packet[offset] << 24) | (packet[offset + 1] << 16) | (packet[offset + 2] << 8) | packet[offset + 3]
            : -1;

    private static int H264StartCodeLengthAt(byte[] data, int index)
    {
        if (index < 0 || index + 3 > data.Length) return 0;
        if (index + 4 <= data.Length && data[index] == 0 && data[index + 1] == 0 && data[index + 2] == 0 && data[index + 3] == 1) return 4;
        if (index + 3 <= data.Length && data[index] == 0 && data[index + 1] == 0 && data[index + 2] == 1) return 3;
        return 0;
    }

    private static bool LooksLikeOggPage(byte[] packet) => packet.Length >= 4 && packet[0] == (byte)'O' && packet[1] == (byte)'g' && packet[2] == (byte)'g' && packet[3] == (byte)'S';

    private static string FormatFirstBytes(byte[] packet)
    {
        var length = Math.Min(packet.Length, 12);
        var builder = new StringBuilder(length * 3);
        for (var i = 0; i < length; i++)
        {
            if (i > 0) builder.Append(' ');
            builder.Append(packet[i].ToString("X2"));
        }
        return builder.ToString();
    }

    private IEnumerable<byte[]> DepacketizeH264Rtp(byte[] packet)
    {
        if (!TryParseRtpPacket(packet, out var marker, out var payloadType, out var sequence, out var timestamp, out var offset, out var payloadLength))
            yield break;

        if (payloadType != 102)
            Program.Log($"Direct Stream receiver saw unexpected RTP payload type {payloadType}; expected H.264 payload type 102.");

        byte[]? completedPreviousTimestamp = null;
        byte[]? completedCurrentTimestamp = null;
        lock (_rtpGate)
        {
            // Some libdatachannel builds deliver media RTP through rtcSetMessageCallback but do not
            // reliably preserve/mark the final RTP packet of a frame for C# consumers. If we only
            // flush on the RTP marker bit, the receiver can collect packets forever and decoded=0.
            // A timestamp change is also a valid frame boundary for this fallback path, so flush the
            // previous timestamp before starting the new one.
            if (_h264AccessUnitActive && _h264AccessUnitTimestamp != timestamp)
            {
                if (_h264AccessUnitBuffer is { Length: > 4 })
                    completedPreviousTimestamp = _h264AccessUnitBuffer.ToArray();
                ResetH264RtpAssembly(timestamp);
            }
            else if (!_h264AccessUnitActive)
            {
                ResetH264RtpAssembly(timestamp);
            }

            if (_h264AccessUnitActive && _h264AccessUnitBuffer is { Length: > 0 } && sequence != (ushort)(_h264AccessUnitLastSequence + 1))
            {
                var now = DateTime.UtcNow;
                if ((now - _lastVideoRtpGapLogUtc).TotalSeconds >= 2)
                {
                    _lastVideoRtpGapLogUtc = now;
                    Program.Log($"Direct Stream receiver detected H.264 RTP sequence gap from {PeerId}; expected {(ushort)(_h264AccessUnitLastSequence + 1)}, got {sequence}. Dropping partial access unit for timestamp {_h264AccessUnitTimestamp}.");
                }
                ResetH264RtpAssembly(timestamp);
            }

            if (AppendH264RtpPayloadToCurrentAccessUnit(packet, offset, payloadLength, sequence, timestamp))
            {
                _h264AccessUnitLastSequence = sequence;

                if (marker && _h264AccessUnitBuffer is { Length: > 0 })
                {
                    completedCurrentTimestamp = _h264AccessUnitBuffer.ToArray();
                    ResetH264RtpAssembly();
                }
            }
            else if (marker)
            {
                ResetH264RtpAssembly();
            }
        }

        if (completedPreviousTimestamp is { Length: > 4 })
            yield return completedPreviousTimestamp;
        if (completedCurrentTimestamp is { Length: > 4 })
            yield return completedCurrentTimestamp;
    }

    private static bool TryParseRtpPacket(byte[] packet, out bool marker, out int payloadType, out ushort sequence, out uint timestamp, out int payloadOffset, out int payloadLength)
    {
        marker = false;
        payloadType = -1;
        sequence = 0;
        timestamp = 0;
        payloadOffset = 0;
        payloadLength = 0;

        if (packet.Length < 12) return false;
        var version = packet[0] >> 6;
        if (version != 2) return false;

        var csrcCount = packet[0] & 0x0F;
        var extension = (packet[0] & 0x10) != 0;
        var padding = (packet[0] & 0x20) != 0;
        marker = (packet[1] & 0x80) != 0;
        payloadType = packet[1] & 0x7F;
        sequence = (ushort)((packet[2] << 8) | packet[3]);
        timestamp = ((uint)packet[4] << 24) | ((uint)packet[5] << 16) | ((uint)packet[6] << 8) | packet[7];

        var offset = 12 + (csrcCount * 4);
        if (offset > packet.Length) return false;

        if (extension)
        {
            if (offset + 4 > packet.Length) return false;
            var extensionLengthWords = (packet[offset + 2] << 8) | packet[offset + 3];
            offset += 4 + (extensionLengthWords * 4);
            if (offset > packet.Length) return false;
        }

        var length = packet.Length - offset;
        if (padding)
        {
            var paddingLength = packet[^1];
            if (paddingLength <= 0 || paddingLength > length) return false;
            length -= paddingLength;
        }

        if (length <= 0) return false;
        payloadOffset = offset;
        payloadLength = length;
        return true;
    }

    private bool AppendH264RtpPayloadToCurrentAccessUnit(byte[] packet, int offset, int payloadLength, ushort sequence, uint timestamp)
    {
        if (payloadLength <= 0 || offset < 0 || offset + payloadLength > packet.Length)
            return false;

        _h264AccessUnitBuffer ??= new MemoryStream(Math.Max(4096, payloadLength + 64));
        _h264AccessUnitTimestamp = timestamp;
        _h264AccessUnitActive = true;

        var nalHeader = packet[offset];
        var nalType = nalHeader & 0x1F;

        if (nalType is >= 1 and <= 23)
        {
            WriteAnnexBStartCode(_h264AccessUnitBuffer);
            _h264AccessUnitBuffer.Write(packet, offset, payloadLength);
            return true;
        }

        if (nalType == 24) // STAP-A
        {
            var pos = offset + 1;
            var end = offset + payloadLength;
            var wroteAny = false;
            while (pos + 2 <= end)
            {
                var nalLength = (packet[pos] << 8) | packet[pos + 1];
                pos += 2;
                if (nalLength <= 0 || pos + nalLength > end) break;
                WriteAnnexBStartCode(_h264AccessUnitBuffer);
                _h264AccessUnitBuffer.Write(packet, pos, nalLength);
                pos += nalLength;
                wroteAny = true;
            }

            return wroteAny;
        }

        if (nalType == 28) // FU-A
        {
            if (payloadLength < 2) return false;

            var fuIndicator = packet[offset];
            var fuHeader = packet[offset + 1];
            var start = (fuHeader & 0x80) != 0;
            var end = (fuHeader & 0x40) != 0;
            var reconstructedNalHeader = (byte)((fuIndicator & 0xE0) | (fuHeader & 0x1F));
            var fragmentOffset = offset + 2;
            var fragmentLength = payloadLength - 2;
            if (fragmentLength <= 0) return false;

            if (start)
            {
                _h264FragmentBuffer = new byte[1 + fragmentLength];
                _h264FragmentBuffer[0] = reconstructedNalHeader;
                Buffer.BlockCopy(packet, fragmentOffset, _h264FragmentBuffer, 1, fragmentLength);
                _h264FragmentTimestamp = timestamp;
                _h264LastSequence = sequence;
                _h264FragmentActive = true;
            }
            else if (_h264FragmentActive && _h264FragmentBuffer is not null && _h264FragmentTimestamp == timestamp)
            {
                if (sequence != (ushort)(_h264LastSequence + 1))
                {
                    _h264FragmentBuffer = null;
                    _h264FragmentActive = false;
                    return false;
                }

                var oldLength = _h264FragmentBuffer.Length;
                Array.Resize(ref _h264FragmentBuffer, oldLength + fragmentLength);
                Buffer.BlockCopy(packet, fragmentOffset, _h264FragmentBuffer, oldLength, fragmentLength);
                _h264LastSequence = sequence;
            }
            else
            {
                return false;
            }

            if (end && _h264FragmentActive && _h264FragmentBuffer is not null && _h264FragmentTimestamp == timestamp)
            {
                WriteAnnexBStartCode(_h264AccessUnitBuffer);
                _h264AccessUnitBuffer.Write(_h264FragmentBuffer, 0, _h264FragmentBuffer.Length);
                _h264FragmentBuffer = null;
                _h264FragmentActive = false;
            }

            return true;
        }

        return false;
    }

    private void ResetH264RtpAssembly(uint? newTimestamp = null)
    {
        _h264AccessUnitBuffer?.Dispose();
        _h264AccessUnitBuffer = newTimestamp.HasValue ? new MemoryStream(256 * 1024) : null;
        _h264AccessUnitTimestamp = newTimestamp ?? 0;
        _h264AccessUnitLastSequence = 0;
        _h264AccessUnitActive = newTimestamp.HasValue;
        _h264FragmentBuffer = null;
        _h264FragmentTimestamp = 0;
        _h264LastSequence = 0;
        _h264FragmentActive = false;
    }

    private static void WriteAnnexBStartCode(Stream stream)
    {
        stream.WriteByte(0);
        stream.WriteByte(0);
        stream.WriteByte(0);
        stream.WriteByte(1);
    }

    private static void WriteAnnexBStartCode(byte[] buffer, int offset)
    {
        buffer[offset] = 0;
        buffer[offset + 1] = 0;
        buffer[offset + 2] = 0;
        buffer[offset + 3] = 1;
    }

    private void StartAudioPumpIfNeeded()
    {
        if (Role != RtcPeerRole.Publisher) return;

        PublisherConfig config;
        int audioTrack;
        var now = DateTime.UtcNow;
        lock (_gate)
        {
            if (_disposed) return;
            if (_audioPump is not null) return;
            if (_videoPump is null) return;
            if (_audioPumpStartInProgress) return;
            if (_audioTrack < 0 && _audioChannel < 0) return;
            if (!CanSendAudio && !_audioChannelOpen) return;
            if ((now - _lastAudioStartAttemptUtc).TotalSeconds < 2) return;
            if (_publisherConfig is not { } currentConfig) return;

            config = currentConfig;
            audioTrack = _audioTrack;
            _audioPumpStartInProgress = true;
            _lastAudioStartAttemptUtc = now;
        }

        _ = Task.Run(() => StartAudioPumpWorker(config, audioTrack));
    }

    private void StartAudioPumpWorker(PublisherConfig config, int audioTrack)
    {
        FfmpegOpusAudioSender? pump = null;
        var success = false;
        var detail = string.Empty;

        try
        {
            Program.Log($"Direct Stream starting browser-only audio capture for viewer {PeerId}; audioTrack={audioTrack}; {BuildHandshakeDetail()}");
            success = FfmpegOpusAudioSender.TryStart(config, SendEncodedAudioBytes, ReportAudioSenderStatus, out pump, out detail);
        }
        catch (Exception ex)
        {
            detail = Program.Flatten(ex);
            Program.Log("Direct Stream browser-only audio capture start threw: " + detail);
        }

        lock (_gate)
        {
            _audioPumpStartInProgress = false;
            if (_disposed || _audioTrack != audioTrack || _audioPump is not null)
            {
                try { pump?.Dispose(); } catch { }
                return;
            }

            if (success && pump is not null)
                _audioPump = pump;
        }

        if (success && pump is not null)
        {
            Program.Log($"Direct Stream browser-only audio capture ready for viewer {PeerId}: {detail}");
            _ = _transport.SendStatusAsync("Host audio ready", detail + " Audio is sent as Opus RTP over the Direct Stream audio media track.", true, _receiverActive(), true, _viewerCount());
        }
        else
        {
            Program.Log($"Direct Stream browser-only audio capture unavailable for viewer {PeerId}: {detail}");
            _ = _transport.SendStatusAsync("Host audio unavailable", detail + " Video will continue without Direct Stream audio.", true, _receiverActive(), true, _viewerCount());
        }
    }

    private void SendEncodedAudioBytes(byte[] chunk)
    {
        if (chunk.Length == 0) return;

        // Primary path: raw Opus packets over the negotiated Opus RTP media track.
        // Fallback path: the original Ogg/Opus stream over a WebRTC data channel.
        // The fallback is still peer-to-peer WebRTC, not SignalR, and it keeps browser-only audio scoped
        // to the RavaCast renderer process tree.
        SendOggAudioChunkToDataChannel(chunk);

        List<byte[]> packets;
        lock (_audioOggGate)
            packets = _audioOggPacketReader.Append(chunk);

        foreach (var packet in packets)
            SendOpusAudioPacket(packet);
    }

    private void SendOggAudioChunkToDataChannel(byte[] chunk)
    {
        if (chunk.Length == 0) return;
        int channel;
        lock (_gate)
        {
            if (_disposed || _audioChannel < 0 || !_audioChannelOpen) return;
            channel = _audioChannel;
        }

        try
        {
            if (!LibDataChannelNative.rtcIsOpen(channel)) return;
        }
        catch
        {
            return;
        }

        int rc;
        try { rc = LibDataChannelNative.rtcSendMessage(channel, chunk, chunk.Length); }
        catch (Exception ex)
        {
            Program.Log("rtcSendMessage(audio data channel fallback) threw: " + Program.Flatten(ex));
            return;
        }

        if (rc == 0)
        {
            var total = Interlocked.Add(ref _sentAudioBytes, chunk.Length);
            if (total == chunk.Length)
                Program.Log($"Direct Stream first Ogg/Opus audio chunk sent over data channel fallback to viewer {PeerId}; chunk={chunk.Length} bytes. {BuildHandshakeDetail()}");
            return;
        }

        Interlocked.Increment(ref _sendAudioFailures);
        var now = DateTime.UtcNow;
        if ((now - _lastAudioFailureLogUtc).TotalSeconds >= 2)
        {
            _lastAudioFailureLogUtc = now;
            Program.Log($"rtcSendMessage(audio data channel fallback) returned {rc} for viewer {PeerId}; sentAudio={Interlocked.Read(ref _sentAudioBytes) / 1024:n0} KB, failures={Interlocked.Read(ref _sendAudioFailures)}.");
        }
    }

    private void SendOpusAudioPacket(byte[] packet)
    {
        if (packet.Length == 0) return;
        int track;
        lock (_gate)
        {
            if (_disposed || _audioTrack < 0) return;
            track = _audioTrack;
        }

        if (!_audioTrackOpen && !_connected && !_iceTransportReady) return;

        int rc;
        try { rc = LibDataChannelNative.rtcSendMessage(track, packet, packet.Length); }
        catch (Exception ex)
        {
            Program.Log("rtcSendMessage(audio RTP) threw: " + Program.Flatten(ex));
            return;
        }

        if (rc == 0)
        {
            _audioTrackOpen = true;
            var total = Interlocked.Add(ref _sentAudioBytes, packet.Length);
            if (total == packet.Length)
                Program.Log($"Direct Stream first Opus RTP audio packet sent to viewer {PeerId}; packet={packet.Length} bytes. {BuildHandshakeDetail()}");
            return;
        }

        Interlocked.Increment(ref _sendAudioFailures);
        var now = DateTime.UtcNow;
        if ((now - _lastAudioFailureLogUtc).TotalSeconds >= 2)
        {
            _lastAudioFailureLogUtc = now;
            Program.Log($"rtcSendMessage(audio RTP) returned {rc} for viewer {PeerId}; sentAudio={Interlocked.Read(ref _sentAudioBytes) / 1024:n0} KB, failures={Interlocked.Read(ref _sendAudioFailures)}.");
        }
    }

    private void ReportAudioSenderStatus(string detail)
    {
        var sentKb = Interlocked.Read(ref _sentAudioBytes) / 1024;
        var failures = Interlocked.Read(ref _sendAudioFailures);
        var extra = CanSendAudio ? $" Sent to viewer: {sentKb:n0} KB" + (failures > 0 ? $", send failures: {failures:n0}." : ".") : " Waiting for viewer audio track before sending audio.";
        _ = _transport.SendStatusAsync("Sending host audio", detail + extra, true, _receiverActive(), true, _viewerCount());
    }

    private void ReceiveOpusAudioFrame(IntPtr data, int size)
    {
        if (Role != RtcPeerRole.Receiver || data == IntPtr.Zero || size <= 0) return;
        var packet = new byte[size];
        Marshal.Copy(data, packet, 0, size);
        ReceiveAudioTrackBytes(packet, "frame");
    }

    private void ReceiveOpusAudioFrameBytes(byte[] packet)
    {
        if (packet.Length == 0) return;

        EnsureAudioPlayer();
        var player = _audioPlayer;
        if (player is null || player.IsBroken)
        {
            var failures = Interlocked.Increment(ref _audioPlayerWriteFailures);
            if (failures == 1 || failures % 128 == 0)
                Program.Log($"Direct Stream receiver could not play Opus packet from {PeerId}: audio player missing/broken; packet={packet.Length} bytes; failures={failures:n0}.");
            return;
        }

        if (!player.PushRawOpusPacket(packet))
        {
            var failures = Interlocked.Increment(ref _audioPlayerWriteFailures);
            if (failures == 1 || failures % 128 == 0)
                Program.Log($"Direct Stream receiver rejected Opus packet from {PeerId}: packet={packet.Length} bytes; playerBroken={player.IsBroken}; failures={failures:n0}.");
            return;
        }

        var received = Interlocked.Add(ref _receivedAudioBytes, packet.Length);
        if (!_firstAudioPlayerWriteLogged)
        {
            _firstAudioPlayerWriteLogged = true;
            Program.Log($"Direct Stream receiver accepted first Opus audio packet for playback from {PeerId}: packet={packet.Length} bytes; audioReceived={received} bytes.");
        }

        var now = DateTime.UtcNow;
        if ((now - _lastAudioStatusUtc).TotalSeconds < 3) return;
        _lastAudioStatusUtc = now;
        _ = _transport.SendStatusAsync("Host audio connected", $"Received {received / 1024:n0} KB of Opus RTP audio from the host.", _publisherActive(), true, true, _viewerCount());
    }

    private void ReceiveAudioBytes(IntPtr data, int size)
    {
        if (Role != RtcPeerRole.Receiver || data == IntPtr.Zero || size <= 0) return;
        var chunk = new byte[size];
        Marshal.Copy(data, chunk, 0, size);
        ReceiveAudioBytes(chunk);
    }

    private void ReceiveAudioBytes(byte[] chunk)
    {
        if (Role != RtcPeerRole.Receiver || chunk.Length == 0) return;

        EnsureAudioPlayer();
        var player = _audioPlayer;
        if (player is null || player.IsBroken) return;
        if (!player.PushOggOpusBytes(chunk)) return;

        var received = Interlocked.Add(ref _receivedAudioBytes, chunk.Length);
        var now = DateTime.UtcNow;
        if ((now - _lastAudioStatusUtc).TotalSeconds < 3) return;
        _lastAudioStatusUtc = now;
        _ = _transport.SendStatusAsync("Host audio connected", $"Received {received / 1024:n0} KB of Opus audio from the host.", _publisherActive(), true, true, _viewerCount());
    }

    private void EnsureAudioPlayer()
    {
        if (_audioPlayer is not null) return;
        lock (_gate)
        {
            if (_audioPlayer is not null) return;
            if (FfmpegOpusAudioPlayer.TryStart(out var player, out var detail))
            {
                _audioPlayer = player;
                player.SetPlaybackAudioState(_playbackMuted, _playbackVolume);
                _ = _transport.SendStatusAsync("Preparing host audio", detail, _publisherActive(), true, true, _viewerCount());
            }
            else
            {
                _ = _transport.SendStatusAsync("Host audio unavailable", detail + " Video will continue without Direct Stream audio playback.", _publisherActive(), true, true, _viewerCount());
            }
        }
    }

    private void ReceiveEncodedVideoFrame(IntPtr data, int size)
    {
        if (data == IntPtr.Zero || size <= 0) return;
        var frame = new byte[size];
        Marshal.Copy(data, frame, 0, size);
        ReceiveVideoTrackBytes(frame, "frame");
    }

    private void ReceiveEncodedVideoFrameBytes(byte[] frame)
    {
        if (frame.Length == 0) return;
        var received = Interlocked.Increment(ref _receivedFrames);
        var bytes = Interlocked.Add(ref _receivedBytes, frame.Length);

        EnsureDecoderAndTextureSink(useRtpInput: false);
        var decoder = _decoder;
        if (decoder is not null && !decoder.IsBroken)
            decoder.PushEncodedAccessUnit(frame);

        var now = DateTime.UtcNow;
        if ((now - _lastReceiveStatusUtc).TotalSeconds < 2 && received % 90 != 0) return;
        _lastReceiveStatusUtc = now;
        var decoded = decoder?.DecodedFrames ?? 0;
        var written = _receiverTextureSink?.WrittenFrames ?? 0;
        _ = _transport.SendStatusAsync("Host video frames arriving", $"Received {received:n0} H.264 frames ({bytes / 1024:n0} KB). Decoded={decoded:n0}; shown={written:n0}.", _publisherActive(), true, true, _viewerCount());
    }

    private void EnsureDecoderAndTextureSink(bool useRtpInput = false, int payloadType = 102)
    {
        if (_decoder is not null)
        {
            if (!_decoder.IsBroken) return;

            try { _decoder.Dispose(); } catch { }
            _decoder = null;
            _decoderStartAttempted = false;
        }

        if (_decoderStartAttempted) return;
        _decoderStartAttempted = true;
        var config = _receiverConfig;
        var width = config?.TargetWidth ?? 1280;
        var height = config?.TargetHeight ?? 720;

        if (_receiverTextureSink is null)
        {
            if (D3D11SharedTextureFrameSink.TryCreate(width, height, out var sink, out var sinkDetail))
            {
                _receiverTextureSink = sink;
                // Do not publish the newly-created texture before a decoded frame has been written.
                // Publishing the empty texture is what made the viewer switch to a live-looking but
                // grey/blank Direct Stream surface when video decode had not caught up yet.
                _ = _transport.SendStatusAsync("Preparing host video", sinkDetail + " Waiting for the first decoded frame before publishing the receiver texture.", _publisherActive(), true, true, _viewerCount());
            }
            else
            {
                _ = _transport.SendErrorAsync("Direct Stream output unavailable", sinkDetail, _publisherActive(), true, true, _viewerCount());
            }
        }

        FfmpegH264FrameDecoder? decoder;
        string detail;
        var started = useRtpInput
            ? FfmpegH264FrameDecoder.TryStartRtp(width, height, payloadType, OnDecodedVideoFrame, out decoder, out detail)
            : FfmpegH264FrameDecoder.TryStart(width, height, OnDecodedVideoFrame, out decoder, out detail);

        if (started)
        {
            _decoder = decoder;
            _ = _transport.SendStatusAsync("Decoding host video", detail + (useRtpInput ? " RTP packets are being handled by FFmpeg and decoded frames are being written into the receiver texture." : " Decoded frames are being written into the receiver texture."), _publisherActive(), true, true, _viewerCount());
        }
        else
        {
            // Allow a later packet/reconnect to retry. FFmpeg RTP startup can fail if a transient
            // UDP port pair is busy; keeping the old one-shot latch here leaves Direct Stream video
            // permanently black until the whole BridgeHost is restarted.
            _decoderStartAttempted = false;
            _ = _transport.SendErrorAsync("Direct Stream needs FFmpeg", detail + " Add ffmpeg beside BridgeHost or to PATH to decode the H.264 host stream.", _publisherActive(), true, true, _viewerCount());
        }
    }

    private void OnDecodedVideoFrame(byte[] bgraFrame)
    {
        if (bgraFrame.Length == 0) return;
        QueueDecodedVideoFrameForSyncedPresent(bgraFrame);
    }

    private void QueueDecodedVideoFrameForSyncedPresent(byte[] bgraFrame)
    {
        lock (_videoPresentGate)
        {
            if (_disposed) return;
            if (_firstDecodedVideoFrameUtc == DateTime.MinValue)
                _firstDecodedVideoFrameUtc = DateTime.UtcNow;

            _pendingVideoPresentFrames.Enqueue(new QueuedVideoFrame(bgraFrame, DateTime.UtcNow));
            while (_pendingVideoPresentFrames.Count > ReceiverVideoSyncQueueLimit)
            {
                _pendingVideoPresentFrames.Dequeue();
                Interlocked.Increment(ref _videoSyncDroppedFrames);
            }

            if (_videoPresentWorkerRunning) return;
            _videoPresentWorkerRunning = true;
        }

        _ = Task.Run(VideoPresentLoopAsync);
    }

    private async Task VideoPresentLoopAsync()
    {
        while (true)
        {
            QueuedVideoFrame? frameToPresent = null;
            var delayMs = 0;

            lock (_videoPresentGate)
            {
                if (_disposed)
                {
                    _pendingVideoPresentFrames.Clear();
                    _videoPresentWorkerRunning = false;
                    return;
                }

                if (_pendingVideoPresentFrames.Count == 0)
                {
                    _videoPresentWorkerRunning = false;
                    return;
                }

                var candidate = _pendingVideoPresentFrames.Peek();
                delayMs = GetVideoSyncPresentDelayMs(candidate.EnqueuedUtc, DateTime.UtcNow);
                if (delayMs <= 0)
                    frameToPresent = _pendingVideoPresentFrames.Dequeue();
            }

            if (frameToPresent is not null)
            {
                PresentDecodedVideoFrame(frameToPresent.Frame);
                continue;
            }

            try { await Task.Delay(Math.Clamp(delayMs, 1, 15)).ConfigureAwait(false); }
            catch { return; }
        }
    }

    private int GetVideoSyncPresentDelayMs(DateTime frameQueuedUtc, DateTime nowUtc)
    {
        if (ShouldWaitForReceiverAudioClock(nowUtc))
            return 15;

        var targetDelayMs = GetReceiverVideoSyncDelayMs();
        if (targetDelayMs <= 0) return 0;

        return (int)Math.Ceiling((frameQueuedUtc.AddMilliseconds(targetDelayMs) - nowUtc).TotalMilliseconds);
    }

    private bool ShouldWaitForReceiverAudioClock(DateTime nowUtc)
    {
        if (!ReceiverAudioSyncExpected()) return false;
        if (_playbackMuted || _playbackVolume <= 0.001f) return false;

        var player = _audioPlayer;
        if (player?.PlaybackStarted == true) return false;

        var firstVideoUtc = _firstDecodedVideoFrameUtc;
        if (firstVideoUtc == DateTime.MinValue) return false;

        var waitedMs = (nowUtc - firstVideoUtc).TotalMilliseconds;
        if (waitedMs <= ReceiverAudioStartWaitMs)
        {
            if (!_videoSyncInitialHoldLogged)
            {
                _videoSyncInitialHoldLogged = true;
                Program.Log($"Direct Stream receiver is holding decoded video briefly for audio sync: audioExpected={ReceiverAudioSyncExpected()}; playerStarted={player?.PlaybackStarted}; waitLimit={ReceiverAudioStartWaitMs}ms; videoDelay={GetReceiverVideoSyncDelayMs()}ms.");
            }
            return true;
        }

        if ((nowUtc - _lastVideoSyncWaitLogUtc).TotalSeconds >= 3)
        {
            _lastVideoSyncWaitLogUtc = nowUtc;
            Program.Log($"Direct Stream receiver audio sync wait expired after {waitedMs:0}ms; presenting video without waiting for receiver audio start. audioPlayer={(player is null ? "missing" : player.IsBroken ? "broken" : "not-started")}; audioReceived={Interlocked.Read(ref _receivedAudioBytes) / 1024:n0} KB.");
        }

        return false;
    }

    private bool ReceiverAudioSyncExpected()
    {
        var config = _receiverConfig;
        if (config is not null && config.AudioBitrateKbps <= 0) return false;
        return _audioTrack >= 0 || _audioChannel >= 0 || config?.AudioBitrateKbps > 0;
    }

    private int GetReceiverVideoSyncDelayMs()
    {
        if (ReceiverVideoSyncDelayOverrideMs >= 0) return ReceiverVideoSyncDelayOverrideMs;
        var player = _audioPlayer;
        if (player is not null) return Math.Clamp(player.PlaybackPrebufferMs, 0, 600);
        return 140;
    }

    private void PresentDecodedVideoFrame(byte[] bgraFrame)
    {
        var sink = _receiverTextureSink;
        if (sink is null) return;

        if (!sink.WriteFrame(bgraFrame, out var error))
        {
            Interlocked.Increment(ref _receiverTextureWriteFailures);
            var now = DateTime.UtcNow;
            if ((now - _lastReceiverTextureFailureUtc).TotalSeconds >= 2)
            {
                _lastReceiverTextureFailureUtc = now;
                Program.Log("Decoded Direct Stream frame could not be written to the receiver texture: " + (error ?? "unknown error"));
            }
            return;
        }

        // The renderer copies the BridgeHost shared texture only when this callback fires, so
        // notify it for every presented frame. Status/logging inside AdvertiseReceiverTexture remains
        // throttled; the per-frame callback is the actual video-present signal.
        var presented = Interlocked.Increment(ref _videoSyncPresentedFrames);
        if (presented == 1)
            Program.Log($"Direct Stream receiver presented first synced video frame; videoDelay={GetReceiverVideoSyncDelayMs()}ms; droppedForSync={Interlocked.Read(ref _videoSyncDroppedFrames):n0}; audioStarted={_audioPlayer?.PlaybackStarted}.");
        AdvertiseReceiverTexture(sink, "synced decoded frame", force: true);
    }

    private void AdvertiseReceiverTexture(D3D11SharedTextureFrameSink sink, string reason, bool force)
    {
        var now = DateTime.UtcNow;
        if (!force && (now - _lastReceiverTextureAdvertiseUtc).TotalSeconds < 2)
            return;

        if (!force)
            _lastReceiverTextureAdvertiseUtc = now;

        var count = Interlocked.Increment(ref _receiverTextureAdvertiseCount);
        _ = _transport.SendTextureAsync(sink.SharedHandle, sink.Width, sink.Height);

        if (count == 1 || count % 120 == 0 || !force && count % 30 == 0)
            Program.Log($"Direct Stream receiver advertised shared texture to plugin after {reason}: handle=0x{sink.SharedHandle.ToInt64():X}; size={sink.Width}x{sink.Height}; count={count:n0}.");
    }

    private static void OnLocalDescription(int pc, IntPtr sdpPtr, IntPtr typePtr, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        var sdp = Marshal.PtrToStringAnsi(sdpPtr) ?? string.Empty;
        var type = Marshal.PtrToStringAnsi(typePtr) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sdp) || string.IsNullOrWhiteSpace(type)) return;
        session._localDescriptionSent = true;
        Program.Log($"Direct Stream {session.Role} sending local {type} to {session.PeerId}. SDP length={sdp.Length}.");
        var payload = JsonSerializer.Serialize(new { sdp, type });
        session._lastLocalDescriptionType = type;
        session._lastLocalDescriptionPayloadJson = payload;
        _ = session._transport.SendSignalAsync(session.PeerId, type, payload);
    }

    private static void OnLocalCandidate(int pc, IntPtr candPtr, IntPtr midPtr, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        var candidate = Marshal.PtrToStringAnsi(candPtr) ?? string.Empty;
        var mid = Marshal.PtrToStringAnsi(midPtr) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(candidate)) return;
        Interlocked.Increment(ref session._localIceCandidatesSent);
        Program.Log($"Direct Stream {session.Role} sending local ICE to {session.PeerId}. mid={mid}; candidateLength={candidate.Length}.");
        var payload = JsonSerializer.Serialize(new { candidate, mid });
        lock (session._gate)
        {
            if (session._sentLocalIceCandidates.Count < 64)
                session._sentLocalIceCandidates.Add((candidate, mid, payload));
        }
        _ = session._transport.SendSignalAsync(session.PeerId, "ice", payload);
    }

    private static void OnStateChanged(int pc, int state, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        session._connected = state == 2;
        if (state == 2) session._iceTransportReady = true;
        var friendly = state switch
        {
            1 => "Connecting",
            2 => session.Role == RtcPeerRole.Publisher ? "Viewer connected" : "Host connected",
            3 => "Connection interrupted",
            4 => "Connection failed",
            5 => "Connection closed",
            _ => "Preparing connection"
        };
        var detail = session.Role == RtcPeerRole.Publisher && state == 2
            ? $"Viewer {session.PeerId} is connected. FFmpeg H.264 live frames are now being sent over libdatachannel."
            : session.Role == RtcPeerRole.Receiver && state == 2
                ? $"Connected to host. Waiting for H.264 frames. {session.BuildHandshakeDetail()}"
                : $"Direct Stream transport state for viewer {session.PeerId}: {friendly}. {session.BuildHandshakeDetail()}";
        _ = session._transport.SendStatusAsync(friendly, detail, session._publisherActive(), session._receiverActive(), true, session._viewerCount());
        if (state == 2)
            session.MaybeStartPublisherMedia("peer connection connected");
    }

    private static void OnIceStateChanged(int pc, int state, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        var ready = state is 2 or 3; // libdatachannel RTC_ICE_CONNECTED / RTC_ICE_COMPLETED
        session._iceTransportReady = ready || (session._iceTransportReady && state is not 4 and not 5 and not 6);
        Program.Log($"Viewer {session.PeerId} ICE state {state}. {session.BuildHandshakeDetail()}");
        if (ready)
            session.MaybeStartPublisherMedia($"ICE state {state}");
    }

    private static void OnTrack(int pc, int tr, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        SessionsByTrack[tr] = session;
        var mid = TryGetTrackMid(tr);
        var isAudio = session.Role == RtcPeerRole.Receiver && (mid.Equals("audio", StringComparison.OrdinalIgnoreCase) || (session._videoTrack >= 0 && session._audioTrack < 0 && !mid.Equals("video", StringComparison.OrdinalIgnoreCase)));

        if (session.Role == RtcPeerRole.Receiver)
        {
            if (isAudio) session._audioTrack = tr;
            else if (session._videoTrack < 0) session._videoTrack = tr;
        }

        Interlocked.Increment(ref session._mediaTracksSeen);

        lock (session._gate)
        {
            var alreadyPending = false;
            foreach (var pending in session._pendingReceiverTracks)
            {
                if (pending.Track == tr)
                {
                    alreadyPending = true;
                    break;
                }
            }

            if (!alreadyPending && !session._configuredReceiverTracks.Contains(tr))
                session._pendingReceiverTracks.Add((tr, isAudio));
        }

        var kind = isAudio ? "audio" : "video";
        Program.Log($"Direct Stream receiver saw {kind} media track {tr} mid='{mid}' from {session.PeerId}; deferring track setup until after the answer is created. {session.BuildHandshakeDetail()}");

        // libdatachannel can raise the track callback while rtcSetRemoteDescription is still unwinding.
        // Running depacketizer/RTCP setup synchronously inside that callback can prevent the receiver from
        // returning to HandleSignal, so the local answer never gets created. Configure only after the
        // remote offer has been accepted and our answer has been queued.
        if (session._remoteDescriptionSeen && session._localDescriptionSent)
        {
            if (isAudio) session.QueueConfigureReceiverAudioTrack(tr, "track callback after answer");
            else session.QueueConfigureReceiverTrack(tr, "track callback after answer");
        }
    }

    private static void OnFrame(int tr, IntPtr data, int size, IntPtr info, IntPtr userPtr)
    {
        if (!SessionsByTrack.TryGetValue(tr, out var session) || size <= 0) return;
        if (session.Role == RtcPeerRole.Receiver && tr == session._audioTrack)
        {
            session.ReceiveOpusAudioFrame(data, size);
            return;
        }

        session.ReceiveEncodedVideoFrame(data, size);
    }

    private static void OnDataChannel(int pc, int dc, IntPtr userPtr)
    {
        if (!SessionsByPc.TryGetValue(pc, out var session)) return;
        SessionsByTrack[dc] = session;
        var label = TryGetDataChannelLabel(dc);
        if (session.Role == RtcPeerRole.Receiver)
        {
            if (label.Equals("ravacast-video-h264", StringComparison.OrdinalIgnoreCase))
                session._videoChannel = dc;
            else if (label.Equals("ravacast-audio-opus", StringComparison.OrdinalIgnoreCase))
                session._audioChannel = dc;
            else if (session._audioChannel < 0)
                session._audioChannel = dc;
            else if (session._videoChannel < 0)
                session._videoChannel = dc;
        }
        LibDataChannelNative.rtcSetOpenCallback(dc, OpenCallback);
        LibDataChannelNative.rtcSetClosedCallback(dc, ClosedCallback);
        LibDataChannelNative.rtcSetErrorCallback(dc, ErrorCallback);
        LibDataChannelNative.rtcSetMessageCallback(dc, MessageCallback);
        var channelName = label.Equals("ravacast-video-h264", StringComparison.OrdinalIgnoreCase) ? "video" : "audio";
        _ = session._transport.SendStatusAsync($"Receiving host {channelName}", $"Direct Stream {channelName} data channel created for host {session.PeerId}. label='{label}'.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
    }

    private static void OnMessage(int id, IntPtr data, int size, IntPtr userPtr)
    {
        if (!SessionsByTrack.TryGetValue(id, out var session) || size <= 0) return;
        if (session.Role == RtcPeerRole.Receiver && id == session._videoTrack)
        {
            session.ReceiveVideoTransportMessage(data, size);
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._audioTrack)
        {
            session.ReceiveAudioTransportMessage(data, size);
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._videoChannel)
        {
            session.ReceiveVideoDataChannelMessage(data, size);
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._audioChannel)
        {
            session.ReceiveAudioBytes(data, size);
            return;
        }

        session.ReceiveAudioBytes(data, size);
    }

    private static void OnChannelOpen(int id, IntPtr userPtr)
    {
        if (!SessionsByTrack.TryGetValue(id, out var session)) return;
        if (session.Role == RtcPeerRole.Publisher && id == session._videoTrack)
        {
            session._videoTrackOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Viewer video connected", $"Video track opened for viewer {session.PeerId}. Live frames can now be sent.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            session.MaybeStartPublisherMedia("video track opened");
            return;
        }

        if (session.Role == RtcPeerRole.Publisher && id == session._audioTrack)
        {
            session._audioTrackOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Viewer audio connected", $"Opus audio RTP track opened for viewer {session.PeerId}. Shared browser-only host audio can now fan out to this peer.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            session.MaybeStartPublisherMedia("audio track opened");
            return;
        }

        if (session.Role == RtcPeerRole.Publisher && id == session._audioChannel)
        {
            session._audioChannelOpen = true;
            _ = session._transport.SendStatusAsync("Viewer audio data channel connected", $"Audio data channel fallback opened for viewer {session.PeerId}; Opus RTP remains the primary path, but shared Ogg/Opus chunks can now fan out over peer-to-peer WebRTC if RTP audio is not delivered by this libdatachannel build.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            session.MaybeStartPublisherMedia("audio data channel opened");
            return;
        }

        if (session.Role == RtcPeerRole.Publisher && id == session._videoChannel)
        {
            session._videoChannelOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Viewer video data channel connected", $"Raw H.264 video data channel fallback opened for viewer {session.PeerId}; this bypasses libdatachannel media-track depacketizer/frame-callback gaps.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            session.MaybeStartPublisherMedia("video data channel opened");
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._audioTrack)
        {
            session._audioTrackOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Receiving host audio", $"Host Opus audio RTP track opened. {session.BuildHandshakeDetail()}", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._audioChannel)
        {
            session._audioChannelOpen = true;
            _ = session._transport.SendStatusAsync("Receiving host audio", $"Host audio data channel fallback opened. {session.BuildHandshakeDetail()}", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._videoChannel)
        {
            session._videoChannelOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Receiving host video", $"Host raw H.264 video data channel fallback opened. Waiting for decoded video frames. {session.BuildHandshakeDetail()}", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            return;
        }

        if (session.Role == RtcPeerRole.Receiver && id == session._videoTrack)
        {
            session._videoTrackOpen = true;
            session._iceTransportReady = true;
            _ = session._transport.SendStatusAsync("Receiving host video", $"Host video media track opened. Waiting for decoded video frames. {session.BuildHandshakeDetail()}", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
            return;
        }

        _ = session._transport.SendStatusAsync("Direct Stream channel connected", $"Channel opened for {session.PeerId}.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
    }

    private static void OnChannelClosed(int id, IntPtr userPtr)
    {
        if (!SessionsByTrack.TryGetValue(id, out var session)) return;
        if (id == session._videoTrack) session._videoTrackOpen = false;
        if (id == session._audioTrack) session._audioTrackOpen = false;
        if (id == session._audioChannel) session._audioChannelOpen = false;
        if (id == session._videoChannel) session._videoChannelOpen = false;
        _ = session._transport.SendStatusAsync("Direct Stream channel closed", $"Media/data channel closed for {session.PeerId}.", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
    }

    private static void OnChannelError(int id, IntPtr errorPtr, IntPtr userPtr)
    {
        if (!SessionsByTrack.TryGetValue(id, out var session)) return;
        var error = Marshal.PtrToStringAnsi(errorPtr) ?? "Unknown data channel error";
        _ = session._transport.SendErrorAsync($"Direct Stream data channel error for {session.PeerId}: {error}", session._publisherActive(), session._receiverActive(), true, session._viewerCount());
    }

    private void ArmHandshakeWatchdog(string phase)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(8)).ConfigureAwait(false);
                if (_disposed) return;
                if (Role == RtcPeerRole.Receiver && Interlocked.Read(ref _receivedFrames) > 0) return;
                if (Role == RtcPeerRole.Publisher && Interlocked.Read(ref _sentFrames) > 0) return;
                var hint = Role == RtcPeerRole.Receiver
                    ? " If offer/answer and ICE are present but received stays at zero, the host has not sent video RTP yet or the receiver media handler is not seeing it. Check the host detail for sent=0 versus sendFailures."
                    : " If the viewer has answered but sent stays at zero, the host capture/encoder is not producing frames yet; if sendFailures rises, the media track is not open.";
                await _transport.SendStatusAsync(
                    Role == RtcPeerRole.Receiver ? "Still waiting for host video" : "Still connecting viewer video",
                    $"{phase}. {BuildHandshakeDetail()}{hint}",
                    _publisherActive(),
                    _receiverActive(),
                    true,
                    _viewerCount()).ConfigureAwait(false);
            }
            catch
            {
            }
        });
    }

    private string BuildHandshakeDetail()
    {
        var localIce = Interlocked.Read(ref _localIceCandidatesSent);
        var remoteIce = Interlocked.Read(ref _remoteIceCandidatesSeen);
        var tracks = Interlocked.Read(ref _mediaTracksSeen);
        var received = Interlocked.Read(ref _receivedFrames);
        var decoded = _decoder?.DecodedFrames ?? 0;
        var shown = _receiverTextureSink?.WrittenFrames ?? 0;
        var sent = Interlocked.Read(ref _sentFrames);
        var sendFailures = Interlocked.Read(ref _sendFailures);
        var sharedQueued = Interlocked.Read(ref _sharedVideoQueuedFrames);
        var sharedDropped = Interlocked.Read(ref _sharedVideoDroppedFrames);
        var audioSentKb = Interlocked.Read(ref _sentAudioBytes) / 1024;
        var audioReceivedKb = Interlocked.Read(ref _receivedAudioBytes) / 1024;
        var audioFailures = Interlocked.Read(ref _sendAudioFailures);
        var videoTrackPackets = Interlocked.Read(ref _videoTrackPacketsSeen);
        var videoTrackRtp = Interlocked.Read(ref _videoTrackRtpPayloadsSeen);
        var videoTrackRaw = Interlocked.Read(ref _videoTrackRawAccessUnitsSeen);
        var videoTrackDropped = Interlocked.Read(ref _videoTrackDroppedPacketsSeen);
        var videoChannelRaw = Interlocked.Read(ref _videoChannelAccessUnitsSeen);
        var videoChannelFailures = Interlocked.Read(ref _videoChannelSendFailures);
        var audioTrackPackets = Interlocked.Read(ref _audioTrackPacketsSeen);
        var audioTrackRtp = Interlocked.Read(ref _audioTrackRtpPayloadsSeen);
        var audioTrackRaw = Interlocked.Read(ref _audioTrackRawPayloadsSeen);
        var audioTrackDropped = Interlocked.Read(ref _audioTrackDroppedPacketsSeen);
        var videoTrackState = _videoTrackOpen ? "open" : _videoTrack >= 0 && (_connected || _iceTransportReady) ? "probing" : _videoTrack >= 0 ? "created" : "missing";
        var audioTrackState = _audioTrackOpen ? "open" : _audioTrack >= 0 && (_connected || _iceTransportReady) ? "probing" : _audioTrack >= 0 ? "created" : "missing";
        var trackDetail = Role == RtcPeerRole.Publisher
            ? $"outboundTrack={(_videoTrack >= 0 ? "ready" : "missing")}; videoOpen={_videoTrackOpen}; sendReady={CanSendVideo}; sent={sent:n0}; sendFailures={sendFailures:n0}; videoChannel={(_videoChannelOpen ? "open" : _videoChannel >= 0 ? "created" : "missing")}; videoDcFailures={videoChannelFailures:n0}; sharedQueued={sharedQueued:n0}; sharedDropped={sharedDropped:n0}; audioTrack={audioTrackState}; audioChannel={(_audioChannelOpen ? "open" : _audioChannel >= 0 ? "created" : "missing")}; audioReady={CanSendAudio}; audioSent={audioSentKb:n0} KB; audioFailures={audioFailures:n0}"
            : $"tracks={tracks}; videoTrack={videoTrackState}; videoPackets={videoTrackPackets:n0}; videoRtp={videoTrackRtp:n0}; videoRaw={videoTrackRaw:n0}; videoDropped={videoTrackDropped:n0}; videoChannel={(_videoChannelOpen ? "open" : _videoChannel >= 0 ? "created" : "missing")}; videoDc={videoChannelRaw:n0}; audioTrack={audioTrackState}; audioPackets={audioTrackPackets:n0}; audioRtp={audioTrackRtp:n0}; audioRaw={audioTrackRaw:n0}; audioDropped={audioTrackDropped:n0}; audioChannel={(_audioChannelOpen ? "open" : _audioChannel >= 0 ? "created" : "missing")}; audioReceived={audioReceivedKb:n0} KB";
        return $"Offer/answer: local={_localDescriptionSent}, remote={_remoteDescriptionSeen}; ICE local={localIce}, remote={remoteIce}; pcConnected={_connected}; iceReady={_iceTransportReady}; {trackDetail}; received={received:n0}, decoded={decoded:n0}, shown={shown:n0}.";
    }

    private static int ResolveReceiverVideoSyncDelayOverrideMs()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_VIDEO_SYNC_DELAY_MS");
        if (string.IsNullOrWhiteSpace(raw)) return -1;
        return int.TryParse(raw, out var parsed) ? Math.Clamp(parsed, 0, 1000) : -1;
    }

    private static int ResolveReceiverAudioStartWaitMs()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_START_WAIT_MS");
        if (int.TryParse(raw, out var parsed)) return Math.Clamp(parsed, 0, 3000);
        return 1500;
    }

    private static int ResolveReceiverVideoSyncQueueLimit()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_VIDEO_SYNC_QUEUE_FRAMES");
        if (int.TryParse(raw, out var parsed)) return Math.Clamp(parsed, 2, 60);
        return 12;
    }

    private static string TryGetDataChannelLabel(int dc)
    {
        try
        {
            var buffer = new byte[128];
            var rc = LibDataChannelNative.rtcGetDataChannelLabel(dc, buffer, buffer.Length);
            if (rc < 0) return string.Empty;
            var length = Array.IndexOf(buffer, (byte)0);
            if (length < 0) length = buffer.Length;
            return length <= 0 ? string.Empty : Encoding.UTF8.GetString(buffer, 0, length);
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string TryGetTrackMid(int tr)
    {
        try
        {
            var buffer = new byte[128];
            var rc = LibDataChannelNative.rtcGetTrackMid(tr, buffer, buffer.Length);
            if (rc < 0) return string.Empty;
            var length = Array.IndexOf(buffer, (byte)0);
            if (length < 0) length = buffer.Length;
            return length <= 0 ? string.Empty : Encoding.UTF8.GetString(buffer, 0, length);
        }
        catch
        {
            return string.Empty;
        }
    }

    private static (string Sdp, string Type) ParseDescriptionSignal(string payloadJson, string fallbackType)
    {
        if (string.IsNullOrWhiteSpace(payloadJson)) return (string.Empty, fallbackType);
        using var doc = JsonDocument.Parse(payloadJson);
        var root = doc.RootElement;
        return (ReadString(root, "sdp"), ReadString(root, "type", fallbackType));
    }

    private static (string Candidate, string Mid) ParseCandidateSignal(string payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson)) return (string.Empty, string.Empty);
        using var doc = JsonDocument.Parse(payloadJson);
        var root = doc.RootElement;
        return (ReadString(root, "candidate"), ReadString(root, "mid"));
    }

    private static string ReadString(JsonElement root, string name, string fallback = "") => root.TryGetProperty(name, out var value) ? value.GetString() ?? fallback : fallback;
    private static void Free(IntPtr ptr) { if (ptr != IntPtr.Zero) Marshal.FreeHGlobal(ptr); }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _pendingRemoteIceCandidates.Clear();
            _pendingReceiverTracks.Clear();
            _configuredReceiverTracks.Clear();
            _seenRemoteIceCandidateKeys.Clear();
            _sentLocalIceCandidates.Clear();
            _audioTrackOpen = false;
            _audioChannelOpen = false;
            _videoChannelOpen = false;
            _h264FragmentBuffer = null;
            _h264FragmentActive = false;
            _h264AccessUnitBuffer?.Dispose();
            _h264AccessUnitBuffer = null;
            _h264AccessUnitActive = false;
            lock (_videoPresentGate)
            {
                _pendingVideoPresentFrames.Clear();
                _videoPresentWorkerRunning = false;
            }
        }

        StopSharedVideoQueue();
        _publisherMediaHub?.UnregisterSession(PeerId);

        try { _videoPump?.Dispose(); } catch { }
        _videoPump = null;
        try { _audioPump?.Dispose(); } catch { }
        _audioPump = null;
        try { _audioPlayer?.Dispose(); } catch { }
        _audioPlayer = null;
        try { _decoder?.Dispose(); } catch { }
        _decoder = null;
        try { _receiverTextureSink?.Dispose(); } catch { }
        _receiverTextureSink = null;

        if (_videoChannel >= 0)
        {
            try { LibDataChannelNative.rtcDeleteDataChannel(_videoChannel); } catch { }
            SessionsByTrack.TryRemove(_videoChannel, out _);
            _videoChannel = -1;
        }
        if (_audioChannel >= 0)
        {
            try { LibDataChannelNative.rtcDeleteDataChannel(_audioChannel); } catch { }
            SessionsByTrack.TryRemove(_audioChannel, out _);
            _audioChannel = -1;
        }
        if (_audioTrack >= 0)
        {
            try { LibDataChannelNative.rtcDeleteTrack(_audioTrack); } catch { }
            SessionsByTrack.TryRemove(_audioTrack, out _);
            _audioTrack = -1;
        }
        if (_videoTrack >= 0)
        {
            try { LibDataChannelNative.rtcDeleteTrack(_videoTrack); } catch { }
            SessionsByTrack.TryRemove(_videoTrack, out _);
            _videoTrack = -1;
        }
        if (_pc >= 0)
        {
            SessionsByPc.TryRemove(_pc, out _);
            try { LibDataChannelNative.rtcClosePeerConnection(_pc); } catch { }
            try { LibDataChannelNative.rtcDeletePeerConnection(_pc); } catch { }
            _pc = -1;
        }
    }
}
