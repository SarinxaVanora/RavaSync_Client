using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace RavaCast.Media.BridgeHost;

internal sealed class FfmpegH264FrameDecoder : IDisposable
{
    private enum DecoderInputMode
    {
        RawH264,
        RtpH264
    }

    private readonly Process _process;
    private readonly Action<byte[]> _onDecodedFrame;
    private readonly object _stdinGate = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _stdoutTask;
    private readonly Task _stderrTask;
    private readonly int _rawFrameBytes;
    private readonly byte[] _frameBuffer;
    private readonly DecoderInputMode _inputMode;
    private readonly UdpClient? _rtpSender;
    private readonly IPEndPoint? _rtpEndPoint;
    private readonly string? _sdpPath;
    private int _frameBufferOffset;
    private long _decodedFrames;
    private volatile bool _broken;

    private FfmpegH264FrameDecoder(Process process, int rawFrameBytes, Action<byte[]> onDecodedFrame, DecoderInputMode inputMode, UdpClient? rtpSender = null, IPEndPoint? rtpEndPoint = null, string? sdpPath = null)
    {
        _process = process;
        _rawFrameBytes = Math.Max(1, rawFrameBytes);
        _frameBuffer = new byte[_rawFrameBytes];
        _onDecodedFrame = onDecodedFrame;
        _inputMode = inputMode;
        _rtpSender = rtpSender;
        _rtpEndPoint = rtpEndPoint;
        _sdpPath = sdpPath;
        _stdoutTask = Task.Run(ReadStdoutAsync);
        _stderrTask = Task.Run(ReadStderrAsync);
    }

    public long DecodedFrames => Interlocked.Read(ref _decodedFrames);
    public bool IsBroken => _broken;
    public bool AcceptsRtpPackets => _inputMode == DecoderInputMode.RtpH264;

    public static bool TryStart(int width, int height, Action<byte[]> onDecodedFrame, out FfmpegH264FrameDecoder? decoder, out string detail)
    {
        decoder = null;
        if (!FfmpegRuntimeProbe.TryFindFfmpeg(out var ffmpeg, out detail))
            return false;

        PrepareSize(ref width, ref height, out var rawFrameBytes);
        var args = "-hide_banner -loglevel error -fflags nobuffer -flags low_delay -f h264 -i pipe:0 -an -vf " + Quote($"scale={width}:{height}:flags=fast_bilinear,format=bgra") + " -f rawvideo -pix_fmt bgra pipe:1";

        if (!TryStartProcess(ffmpeg, args, redirectInput: true, out var process, out detail))
            return false;

        decoder = new FfmpegH264FrameDecoder(process, rawFrameBytes, onDecodedFrame, DecoderInputMode.RawH264);
        detail = $"FFmpeg H.264 live decoder started ({width}x{height} BGRA output).";
        return true;
    }

    public static bool TryStartRtp(int width, int height, int payloadType, Action<byte[]> onDecodedFrame, out FfmpegH264FrameDecoder? decoder, out string detail)
    {
        decoder = null;
        if (!FfmpegRuntimeProbe.TryFindFfmpeg(out var ffmpeg, out detail))
            return false;

        PrepareSize(ref width, ref height, out var rawFrameBytes);
        payloadType = payloadType is >= 96 and <= 127 ? payloadType : 102;
        if (!TryGetAvailableUdpPortPair(out var rtpPort, out var rtcpPort))
        {
            detail = "Could not find a free UDP RTP/RTCP port pair for FFmpeg H.264 RTP decode.";
            return false;
        }

        var sdpPath = Path.Combine(Path.GetTempPath(), $"ravacast_h264_{Environment.ProcessId}_{Guid.NewGuid():N}.sdp");
        var sdp = BuildH264RtpSdp(rtpPort, rtcpPort, payloadType);

        try
        {
            File.WriteAllText(sdpPath, sdp);
        }
        catch (Exception ex)
        {
            detail = "Failed to create temporary FFmpeg RTP SDP file: " + ex.Message;
            return false;
        }

        UdpClient? sender = null;
        try
        {
            sender = new UdpClient(AddressFamily.InterNetwork);
            sender.Connect(IPAddress.Loopback, rtpPort);
            var args = "-hide_banner -loglevel error -protocol_whitelist file,udp,rtp -fflags nobuffer -flags low_delay -analyzeduration 1000000 -probesize 65536 -reorder_queue_size 0 -i " + Quote(sdpPath) + " -an -vf " + Quote($"scale={width}:{height}:flags=fast_bilinear,format=bgra") + " -f rawvideo -pix_fmt bgra pipe:1";
            if (!TryStartProcess(ffmpeg, args, redirectInput: false, out var process, out detail))
            {
                try { File.Delete(sdpPath); } catch { }
                sender.Dispose();
                return false;
            }

            decoder = new FfmpegH264FrameDecoder(process, rawFrameBytes, onDecodedFrame, DecoderInputMode.RtpH264, sender, new IPEndPoint(IPAddress.Loopback, rtpPort), sdpPath);
            detail = $"FFmpeg H.264 RTP decoder started on 127.0.0.1:{rtpPort} (RTCP {rtcpPort}) payload type {payloadType} ({width}x{height} BGRA output).";
            return true;
        }
        catch (Exception ex)
        {
            try { sender?.Dispose(); } catch { }
            try { File.Delete(sdpPath); } catch { }
            detail = "FFmpeg H.264 RTP decoder failed to start: " + ex.Message;
            return false;
        }
    }

    public bool PushEncodedAccessUnit(byte[] data)
    {
        if (_broken || data.Length == 0) return false;
        if (_inputMode != DecoderInputMode.RawH264)
            return false;

        try
        {
            lock (_stdinGate)
            {
                if (_process.HasExited) { _broken = true; return false; }
                _process.StandardInput.BaseStream.Write(data, 0, data.Length);
                _process.StandardInput.BaseStream.Flush();
            }
            return true;
        }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg live decoder stdin failed: " + Program.Flatten(ex));
            return false;
        }
    }

    public bool PushRtpPacket(byte[] packet)
    {
        if (_broken || packet.Length == 0) return false;
        if (_inputMode != DecoderInputMode.RtpH264 || _rtpSender is null)
            return false;

        try
        {
            if (_process.HasExited) { _broken = true; return false; }
            _rtpSender.Send(packet, packet.Length);
            return true;
        }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg live RTP decoder UDP send failed: " + Program.Flatten(ex));
            return false;
        }
    }

    private async Task ReadStdoutAsync()
    {
        var buffer = new byte[64 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var read = await _process.StandardOutput.BaseStream.ReadAsync(buffer.AsMemory(0, buffer.Length), _cts.Token).ConfigureAwait(false);
                if (read <= 0) break;

                var srcOffset = 0;
                while (srcOffset < read)
                {
                    var needed = _rawFrameBytes - _frameBufferOffset;
                    var toCopy = Math.Min(needed, read - srcOffset);
                    Buffer.BlockCopy(buffer, srcOffset, _frameBuffer, _frameBufferOffset, toCopy);
                    _frameBufferOffset += toCopy;
                    srcOffset += toCopy;

                    if (_frameBufferOffset == _rawFrameBytes)
                    {
                        var frame = new byte[_rawFrameBytes];
                        Buffer.BlockCopy(_frameBuffer, 0, frame, 0, _rawFrameBytes);
                        _frameBufferOffset = 0;
                        Interlocked.Increment(ref _decodedFrames);
                        _onDecodedFrame(frame);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg live decoder stdout failed: " + Program.Flatten(ex));
        }
    }

    private async Task ReadStderrAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var line = await _process.StandardError.ReadLineAsync(_cts.Token).ConfigureAwait(false);
                if (line is null) break;
                if (!string.IsNullOrWhiteSpace(line)) Program.Log("ffmpeg-live-recv: " + line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg live decoder stderr failed: " + Program.Flatten(ex));
        }
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        try { _rtpSender?.Dispose(); } catch { }
        try { if (_process.StartInfo.RedirectStandardInput) _process.StandardInput.Close(); } catch { }
        try { if (!_process.HasExited) _process.Kill(entireProcessTree: true); } catch { }
        try { _process.Dispose(); } catch { }
        try { if (!string.IsNullOrWhiteSpace(_sdpPath)) File.Delete(_sdpPath); } catch { }
        try { _cts.Dispose(); } catch { }
    }

    private static bool TryStartProcess(string ffmpeg, string args, bool redirectInput, out Process process, out string detail)
    {
        process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = ffmpeg,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardInput = redirectInput,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            },
            EnableRaisingEvents = true
        };

        try
        {
            if (!process.Start())
            {
                detail = "FFmpeg live decoder failed to start.";
                return false;
            }

            detail = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            detail = "FFmpeg H.264 live decoder failed to start: " + ex.Message;
            try { process.Dispose(); } catch { }
            return false;
        }
    }

    private static void PrepareSize(ref int width, ref int height, out int rawFrameBytes)
    {
        width = Math.Clamp(width <= 0 ? 1280 : width, 16, 7680);
        height = Math.Clamp(height <= 0 ? 720 : height, 16, 4320);
        if ((width & 1) == 1) width--;
        if ((height & 1) == 1) height--;
        rawFrameBytes = checked(width * height * 4);
    }

    private static bool TryGetAvailableUdpPortPair(out int rtpPort, out int rtcpPort)
    {
        // FFmpeg's RTP demuxer opens both the RTP port from the SDP and a paired RTCP port.
        // The previous code only checked the RTP port, which can still fail with WSAEADDRINUSE
        // (-10048) when the implicit RTCP port is already taken. Pick and probe both together.
        rtpPort = 0;
        rtcpPort = 0;

        for (var attempt = 0; attempt < 96; attempt++)
        {
            var candidate = Random.Shared.Next(20000, 61000);
            if ((candidate & 1) == 1) candidate--;
            if (candidate < 1024 || candidate > 65533) continue;

            Socket? rtpProbe = null;
            Socket? rtcpProbe = null;
            try
            {
                rtpProbe = CreateExclusiveUdpProbe(candidate);
                rtcpProbe = CreateExclusiveUdpProbe(candidate + 1);
                rtpPort = candidate;
                rtcpPort = candidate + 1;
                return true;
            }
            catch
            {
                // Try another pair.
            }
            finally
            {
                try { rtpProbe?.Dispose(); } catch { }
                try { rtcpProbe?.Dispose(); } catch { }
            }
        }

        return false;
    }

    private static Socket CreateExclusiveUdpProbe(int port)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)
        {
            ExclusiveAddressUse = true
        };
        socket.Bind(new IPEndPoint(IPAddress.Loopback, port));
        return socket;
    }

    private static string BuildH264RtpSdp(int rtpPort, int rtcpPort, int payloadType)
    {
        return string.Join("\r\n", [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=RavaCast Direct Stream H264",
            "c=IN IP4 127.0.0.1",
            "t=0 0",
            $"m=video {rtpPort} RTP/AVP {payloadType}",
            $"a=rtcp:{rtcpPort} IN IP4 127.0.0.1",
            $"a=rtpmap:{payloadType} H264/90000",
            $"a=fmtp:{payloadType} packetization-mode=1",
            "a=recvonly",
            string.Empty
        ]);
    }

    private static string Quote(string value) => '"' + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + '"';
}
