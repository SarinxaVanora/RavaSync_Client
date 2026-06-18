using System.Diagnostics;

namespace RavaCast.Media.BridgeHost;

internal sealed class FfmpegH264LiveTextureSender : IDisposable
{
    private readonly Process _process;
    private readonly D3D11SharedTextureFrameSource _source;
    private readonly Action<byte[]> _onAccessUnit;
    private readonly Action<string> _onStatus;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _stdoutTask;
    private readonly Task _stderrTask;
    private readonly Task _captureTask;
    private readonly int _fps;
    private long _capturedFrames;
    private long _encodedFrames;
    private long _encodedBytes;
    private long _droppedFrames;
    private DateTime _lastStatusUtc = DateTime.MinValue;
    private DateTime _lastCaptureErrorUtc = DateTime.MinValue;

    private FfmpegH264LiveTextureSender(Process process, D3D11SharedTextureFrameSource source, int fps, Action<byte[]> onAccessUnit, Action<string> onStatus)
    {
        _process = process;
        _source = source;
        _fps = fps;
        _onAccessUnit = onAccessUnit;
        _onStatus = onStatus;
        _stdoutTask = Task.Run(ReadStdoutAsync);
        _stderrTask = Task.Run(ReadStderrAsync);
        _captureTask = Task.Run(CaptureLoopAsync);
    }

    public static bool TryStart(PublisherConfig config, Action<byte[]> onAccessUnit, Action<string> onStatus, out FfmpegH264LiveTextureSender? sender, out string detail)
    {
        sender = null;
        if (!FfmpegRuntimeProbe.TryFindFfmpeg(out var ffmpeg, out detail))
            return false;

        if (!D3D11SharedTextureFrameSource.TryOpen(config, out var source, out var sourceDetail) || source is null)
        {
            detail = sourceDetail;
            return false;
        }

        var targetWidth = Math.Clamp(config.TargetWidth <= 0 ? source.Width : config.TargetWidth, 16, 7680);
        var targetHeight = Math.Clamp(config.TargetHeight <= 0 ? source.Height : config.TargetHeight, 16, 4320);
        if ((targetWidth & 1) == 1) targetWidth--;
        if ((targetHeight & 1) == 1) targetHeight--;
        var fps = Math.Clamp(config.Fps <= 0 ? 30 : config.Fps, 1, 120);
        var bitrate = Math.Max(300, config.VideoBitrateKbps);
        var keyint = Math.Clamp(fps, 15, 120);
        var needsScale = source.Width != targetWidth || source.Height != targetHeight;
        var vf = needsScale ? $"scale={targetWidth}:{targetHeight}:flags=fast_bilinear,format=yuv420p" : "format=yuv420p";

        var args = string.Join(' ', new[]
        {
            "-hide_banner",
            "-loglevel", "error",
            "-fflags", "nobuffer",
            "-flags", "low_delay",
            "-f", "rawvideo",
            "-pix_fmt", "bgra",
            "-s", source.Width + "x" + source.Height,
            "-r", fps.ToString(),
            "-i", "pipe:0",
            "-an",
            "-vf", Quote(vf),
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-profile:v", "baseline",
            "-g", keyint.ToString(),
            "-keyint_min", keyint.ToString(),
            "-bf", "0",
            "-b:v", bitrate + "k",
            "-maxrate", bitrate + "k",
            "-bufsize", Math.Max(bitrate, bitrate * 2) + "k",
            "-x264-params", Quote("repeat-headers=1:aud=1:scenecut=0"),
            "-f", "h264",
            "pipe:1"
        });

        try
        {
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = ffmpeg,
                    Arguments = args,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                },
                EnableRaisingEvents = true
            };
            if (!process.Start())
            {
                source.Dispose();
                detail = "FFmpeg live-texture encoder failed to start.";
                return false;
            }

            try { process.PriorityClass = ProcessPriorityClass.BelowNormal; } catch { }

            sender = new FfmpegH264LiveTextureSender(process, source, fps, onAccessUnit, onStatus);
            detail = $"{sourceDetail} FFmpeg live encoder started ({source.Width}x{source.Height} -> {targetWidth}x{targetHeight}@{fps}, {bitrate} kbps).";
            return true;
        }
        catch (Exception ex)
        {
            source.Dispose();
            detail = "FFmpeg live-texture encoder failed to start: " + ex.Message;
            return false;
        }
    }

    private async Task CaptureLoopAsync()
    {
        var frameDelay = TimeSpan.FromMilliseconds(1000.0 / Math.Max(1, _fps));
        var next = DateTime.UtcNow;

        while (!_cts.IsCancellationRequested && !_process.HasExited)
        {
            try
            {
                var now = DateTime.UtcNow;
                if (now < next)
                    await Task.Delay(next - now, _cts.Token).ConfigureAwait(false);
                next = DateTime.UtcNow + frameDelay;

                if (!_source.TryReadFrame(out var frame, out var error))
                {
                    Interlocked.Increment(ref _droppedFrames);
                    MaybeLogCaptureError(error);
                    continue;
                }

                await _process.StandardInput.BaseStream.WriteAsync(frame.AsMemory(0, frame.Length), _cts.Token).ConfigureAwait(false);
                Interlocked.Increment(ref _capturedFrames);
                MaybeReportStatus();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _droppedFrames);
                MaybeLogCaptureError(ex.Message);
                try { await Task.Delay(100, _cts.Token).ConfigureAwait(false); } catch { break; }
            }
        }

        try { _process.StandardInput.Close(); } catch { }
    }

    private async Task ReadStdoutAsync()
    {
        var reader = new H264AnnexBAccessUnitReader();
        var buffer = new byte[64 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var read = await _process.StandardOutput.BaseStream.ReadAsync(buffer.AsMemory(0, buffer.Length), _cts.Token).ConfigureAwait(false);
                if (read <= 0) break;
                reader.Append(buffer, read);
                while (reader.TryTakeNext(out var frame))
                {
                    Interlocked.Increment(ref _encodedFrames);
                    Interlocked.Add(ref _encodedBytes, frame.Length);
                    _onAccessUnit(frame);
                    MaybeReportStatus();
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg live-texture stdout failed: " + Program.Flatten(ex));
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
                if (!string.IsNullOrWhiteSpace(line)) Program.Log("ffmpeg-live-send: " + line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg live-texture stderr failed: " + Program.Flatten(ex));
        }
    }

    private void MaybeReportStatus()
    {
        var now = DateTime.UtcNow;
        if ((now - _lastStatusUtc).TotalSeconds < 2) return;
        _lastStatusUtc = now;
        var captured = Interlocked.Read(ref _capturedFrames);
        var encoded = Interlocked.Read(ref _encodedFrames);
        var bytes = Interlocked.Read(ref _encodedBytes);
        var dropped = Interlocked.Read(ref _droppedFrames);
        _onStatus($"Live RavaCast video sending: captured {captured:n0}, encoded {encoded:n0}, {bytes / 1024:n0} KB" + (dropped > 0 ? $", dropped {dropped:n0}." : "."));
    }

    private void MaybeLogCaptureError(string? error)
    {
        var now = DateTime.UtcNow;
        if ((now - _lastCaptureErrorUtc).TotalSeconds < 2) return;
        _lastCaptureErrorUtc = now;
        Program.Log("Live RavaCast texture capture dropped a frame: " + (error ?? "unknown error"));
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        try { _process.StandardInput.Close(); } catch { }
        try { if (!_process.HasExited) _process.Kill(entireProcessTree: true); } catch { }
        try { _process.Dispose(); } catch { }
        try { _source.Dispose(); } catch { }
        try { _cts.Dispose(); } catch { }
    }

    private static string Quote(string value) => '"' + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + '"';
}
