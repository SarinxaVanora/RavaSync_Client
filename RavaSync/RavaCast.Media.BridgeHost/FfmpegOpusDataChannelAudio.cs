using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using NAudio.CoreAudioApi;
using NAudio.Wave;

namespace RavaCast.Media.BridgeHost;

internal sealed class FfmpegOpusAudioSender : IDisposable
{
    private readonly Process _process;
    private readonly Action<byte[]> _onBytes;
    private readonly Action<string> _onStatus;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _stdoutTask;
    private readonly Task _stderrTask;
    private readonly object _stdinGate = new();
    private readonly IWaveIn? _browserAudioCapture;
    private long _sentBytes;
    private long _capturedPcmBytes;
    private double _capturedPcmPeak;
    private long _chunks;
    private DateTime _lastStatusUtc = DateTime.MinValue;
    private DateTime _lastCaptureStatusUtc = DateTime.MinValue;
    private volatile bool _inputBroken;

    private FfmpegOpusAudioSender(Process process, Action<byte[]> onBytes, Action<string> onStatus, IWaveIn? browserAudioCapture = null)
    {
        _process = process;
        _onBytes = onBytes;
        _onStatus = onStatus;
        _browserAudioCapture = browserAudioCapture;
        _stdoutTask = Task.Run(ReadStdoutAsync);
        _stderrTask = Task.Run(ReadStderrAsync);
    }

    public static bool TryStart(PublisherConfig config, Action<byte[]> onBytes, Action<string> onStatus, out FfmpegOpusAudioSender? sender, out string detail)
    {
        sender = null;
        if (config.AudioBitrateKbps <= 0)
        {
            detail = "Direct Stream audio is disabled by this quality preset.";
            return false;
        }

        if (!FfmpegRuntimeProbe.TryFindFfmpeg(out var ffmpeg, out detail))
            return false;

        var overrideArgs = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_CAPTURE_ARGS");
        if (!string.IsNullOrWhiteSpace(overrideArgs))
            return TryStartFfmpegDeviceCapture(ffmpeg, overrideArgs, "Using RAVACAST_AUDIO_CAPTURE_ARGS.", config, onBytes, onStatus, out sender, out detail);

        if (OperatingSystem.IsWindows())
            return TryStartWindowsBrowserLoopbackCapture(ffmpeg, config, onBytes, onStatus, out sender, out detail);

        detail = "Direct Stream refuses to capture whole-system audio by default. Set RAVACAST_AUDIO_CAPTURE_ARGS only if you have routed the RavaCast browser into an isolated source/sink yourself.";
        return false;
    }

    private static bool TryStartFfmpegDeviceCapture(string ffmpeg, string captureArgs, string sourceDetail, PublisherConfig config, Action<byte[]> onBytes, Action<string> onStatus, out FfmpegOpusAudioSender? sender, out string detail)
    {
        sender = null;
        var bitrate = Math.Clamp(config.AudioBitrateKbps, 32, 512);
        var args = "-hide_banner -loglevel warning " + captureArgs + " -vn -ac 2 -ar 48000 -c:a libopus -application audio -frame_duration 20 -b:a " + bitrate + "k -page_duration 20000 -f ogg -flush_packets 1 pipe:1";

        try
        {
            var process = CreateFfmpegProcess(ffmpeg, args, redirectStandardInput: false, redirectStandardOutput: true);
            if (!process.Start())
            {
                detail = "FFmpeg Opus audio capture failed to start.";
                return false;
            }

            sender = new FfmpegOpusAudioSender(process, onBytes, onStatus);
            detail = $"FFmpeg Opus audio capture started ({bitrate} kbps). {sourceDetail}";
            return true;
        }
        catch (Exception ex)
        {
            detail = "FFmpeg Opus audio capture failed to start: " + ex.Message;
            return false;
        }
    }

    private static bool TryStartWindowsBrowserLoopbackCapture(string ffmpeg, PublisherConfig config, Action<byte[]> onBytes, Action<string> onStatus, out FfmpegOpusAudioSender? sender, out string detail)
    {
        sender = null;
        IWaveIn? capture = null;
        Process? process = null;

        if (config.AudioSourceProcessId <= 0)
        {
            detail = "Browser-only Direct Stream audio needs the WebView2 browser process id. Refusing to fall back to whole-system audio capture.";
            return false;
        }

        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 20348))
        {
            detail = "Browser-only Direct Stream audio needs Windows process-loopback capture support. Refusing to fall back to whole-system audio capture.";
            return false;
        }

        try
        {
            capture = WindowsBrowserProcessLoopbackCapture.Create((uint)config.AudioSourceProcessId);
            var format = capture.WaveFormat;
            var sampleFormat = ResolveRawSampleFormat(format, out var sampleDetail);
            if (string.IsNullOrWhiteSpace(sampleFormat))
            {
                detail = sampleDetail;
                capture.Dispose();
                return false;
            }

            var channels = Math.Max(1, format.Channels);
            var sampleRate = Math.Max(8000, format.SampleRate);
            var bitrate = Math.Clamp(config.AudioBitrateKbps, 32, 512);
            var args = $"-hide_banner -loglevel warning -fflags nobuffer -flags low_delay -f {sampleFormat} -ar {sampleRate} -ac {channels} -i pipe:0 -vn -ac 2 -ar 48000 -c:a libopus -application audio -frame_duration 20 -b:a {bitrate}k -page_duration 20000 -f ogg -flush_packets 1 pipe:1";

            process = CreateFfmpegProcess(ffmpeg, args, redirectStandardInput: true, redirectStandardOutput: true);
            if (!process.Start())
            {
                detail = "FFmpeg Opus audio encoder failed to start for browser-only audio capture.";
                capture.Dispose();
                process.Dispose();
                return false;
            }

            var created = new FfmpegOpusAudioSender(process, onBytes, onStatus, capture);
            capture.DataAvailable += created.OnLoopbackDataAvailable;
            capture.RecordingStopped += created.OnLoopbackRecordingStopped;
            Program.Log($"Windows browser-only audio capture starting for the RavaCast renderer/WebView2 process tree pid={config.AudioSourceProcessId}. {sampleDetail}");
            capture.StartRecording();

            sender = created;
            detail = $"Windows browser-only audio capture started for the RavaCast renderer/WebView2 process tree pid={config.AudioSourceProcessId} through Windows process loopback + FFmpeg Opus ({bitrate} kbps). {sampleDetail} This does not capture the host's whole default audio output.";
            Program.Log(detail);
            return true;
        }
        catch (Exception ex)
        {
            try { capture?.Dispose(); } catch { }
            try { if (process is not null && !process.HasExited) process.Kill(entireProcessTree: true); } catch { }
            try { process?.Dispose(); } catch { }
            detail = "Windows browser-only audio capture failed to start; whole-system audio capture was not used: " + ex.Message;
            return false;
        }
    }

    private void OnLoopbackDataAvailable(object? sender, WaveInEventArgs e)
    {
        if (_inputBroken || _cts.IsCancellationRequested || e.BytesRecorded <= 0) return;

        try
        {
            lock (_stdinGate)
            {
                if (_process.HasExited)
                {
                    _inputBroken = true;
                    return;
                }

                _process.StandardInput.BaseStream.Write(e.Buffer, 0, e.BytesRecorded);
            }

            var captured = Interlocked.Add(ref _capturedPcmBytes, e.BytesRecorded);
            var peak = CalculatePcmPeak(e.Buffer, e.BytesRecorded, _browserAudioCapture?.WaveFormat);
            if (peak > _capturedPcmPeak) _capturedPcmPeak = peak;
            var now = DateTime.UtcNow;
            if (captured == e.BytesRecorded || (now - _lastCaptureStatusUtc).TotalSeconds >= 3)
            {
                _lastCaptureStatusUtc = now;
                Program.Log($"Windows browser-only audio capture delivered {captured / 1024:n0} KB PCM to FFmpeg; pcmPeak={_capturedPcmPeak:0.000}.");
            }
        }
        catch (Exception ex)
        {
            _inputBroken = true;
            Program.Log("Windows loopback audio capture could not feed FFmpeg: " + Program.Flatten(ex));
        }
    }

    private void OnLoopbackRecordingStopped(object? sender, StoppedEventArgs e)
    {
        if (e.Exception is not null)
            Program.Log("Windows loopback audio capture stopped with an error: " + Program.Flatten(e.Exception));
        else
            Program.Log("Windows loopback audio capture stopped.");
    }

    private async Task ReadStdoutAsync()
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var read = await _process.StandardOutput.BaseStream.ReadAsync(buffer.AsMemory(0, buffer.Length), _cts.Token).ConfigureAwait(false);
                if (read <= 0) break;
                var chunk = new byte[read];
                Buffer.BlockCopy(buffer, 0, chunk, 0, read);
                var encoded = Interlocked.Add(ref _sentBytes, read);
                var chunks = Interlocked.Increment(ref _chunks);
                if (chunks == 1)
                    Program.Log($"FFmpeg Opus encoder produced first output chunk: {read:n0} bytes.");
                _onBytes(chunk);
                MaybeReportStatus();
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg Opus audio stdout failed: " + Program.Flatten(ex));
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
                if (!string.IsNullOrWhiteSpace(line)) Program.Log("ffmpeg-audio-send: " + line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg Opus audio stderr failed: " + Program.Flatten(ex));
        }
    }

    private void MaybeReportStatus()
    {
        var now = DateTime.UtcNow;
        if ((now - _lastStatusUtc).TotalSeconds < 3) return;
        _lastStatusUtc = now;
        var detail = $"Audio encoding: {Interlocked.Read(ref _sentBytes) / 1024:n0} KB in {Interlocked.Read(ref _chunks):n0} chunks; captured PCM={Interlocked.Read(ref _capturedPcmBytes) / 1024:n0} KB; capturePeak={_capturedPcmPeak:0.000}.";
        Program.Log(detail);
        _onStatus(detail);
    }


    internal static double CalculatePcmPeak(byte[] buffer, int bytesRecorded, WaveFormat? format)
    {
        if (buffer.Length == 0 || bytesRecorded <= 0 || format is null) return 0;
        var bytes = Math.Min(buffer.Length, bytesRecorded);
        var encoding = format.Encoding;
        var bits = format.BitsPerSample;

        if ((encoding == WaveFormatEncoding.IeeeFloat || encoding == WaveFormatEncoding.Extensible) && bits == 32)
        {
            var peak = 0.0;
            for (var i = 0; i + 3 < bytes; i += 4)
            {
                var sample = BitConverter.ToSingle(buffer, i);
                if (!float.IsNaN(sample) && !float.IsInfinity(sample))
                    peak = Math.Max(peak, Math.Abs(sample));
            }
            return Math.Min(1.0, peak);
        }

        if ((encoding == WaveFormatEncoding.Pcm || encoding == WaveFormatEncoding.Extensible) && bits == 16)
        {
            var peak = 0.0;
            for (var i = 0; i + 1 < bytes; i += 2)
                peak = Math.Max(peak, Math.Abs(BitConverter.ToInt16(buffer, i)) / 32768.0);
            return Math.Min(1.0, peak);
        }

        if ((encoding == WaveFormatEncoding.Pcm || encoding == WaveFormatEncoding.Extensible) && bits == 24)
        {
            var peak = 0.0;
            for (var i = 0; i + 2 < bytes; i += 3)
            {
                var sample = buffer[i] | (buffer[i + 1] << 8) | (buffer[i + 2] << 16);
                if ((sample & 0x800000) != 0) sample |= unchecked((int)0xFF000000);
                peak = Math.Max(peak, Math.Abs(sample / 8388608.0));
            }
            return Math.Min(1.0, peak);
        }

        if ((encoding == WaveFormatEncoding.Pcm || encoding == WaveFormatEncoding.Extensible) && bits == 32)
        {
            var peak = 0.0;
            for (var i = 0; i + 3 < bytes; i += 4)
                peak = Math.Max(peak, Math.Abs(BitConverter.ToInt32(buffer, i) / 2147483648.0));
            return Math.Min(1.0, peak);
        }

        return 0;
    }

    private static Process CreateFfmpegProcess(string ffmpeg, string args, bool redirectStandardInput, bool redirectStandardOutput)
    {
        return new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = ffmpeg,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardInput = redirectStandardInput,
                RedirectStandardOutput = redirectStandardOutput,
                RedirectStandardError = true
            },
            EnableRaisingEvents = true
        };
    }

    private static string ResolveRawSampleFormat(WaveFormat format, out string detail)
    {
        var encoding = format.Encoding;
        var bits = format.BitsPerSample;
        if (encoding == WaveFormatEncoding.IeeeFloat && bits == 32)
        {
            detail = $"Default render endpoint format is {format.SampleRate} Hz, {format.Channels} channel(s), 32-bit float.";
            return "f32le";
        }

        if (encoding == WaveFormatEncoding.Pcm || encoding == WaveFormatEncoding.Extensible)
        {
            var sampleFormat = bits switch
            {
                16 => "s16le",
                24 => "s24le",
                32 when encoding == WaveFormatEncoding.Extensible => "f32le",
                32 => "s32le",
                _ => string.Empty
            };

            if (!string.IsNullOrWhiteSpace(sampleFormat))
            {
                detail = $"Default render endpoint format is {format.SampleRate} Hz, {format.Channels} channel(s), {bits}-bit {encoding}.";
                return sampleFormat;
            }
        }

        detail = $"Unsupported Windows loopback sample format: {encoding}, {bits} bits. Set RAVACAST_AUDIO_CAPTURE_ARGS to a working FFmpeg capture command.";
        return string.Empty;
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        try
        {
            if (_browserAudioCapture is not null)
            {
                _browserAudioCapture.DataAvailable -= OnLoopbackDataAvailable;
                _browserAudioCapture.RecordingStopped -= OnLoopbackRecordingStopped;
                _browserAudioCapture.StopRecording();
                _browserAudioCapture.Dispose();
            }
        }
        catch { }
        try { if (_process.StartInfo.RedirectStandardInput) _process.StandardInput.Close(); } catch { }
        try { if (!_process.HasExited) _process.Kill(entireProcessTree: true); } catch { }
        try { _process.Dispose(); } catch { }
        try { _cts.Dispose(); } catch { }
    }
}



internal sealed class WindowsBrowserProcessLoopbackCapture : IWaveIn
{
    private const double ActivePcmThreshold = 0.0005;
    private readonly uint _rootProcessId;
    private readonly List<WindowsProcessLoopbackCapture> _captures = [];
    private readonly Dictionary<WindowsProcessLoopbackCapture, string> _labels = [];
    private readonly Dictionary<uint, WindowsProcessLoopbackCapture> _capturesByPid = [];
    private readonly object _gate = new();
    private WindowsProcessLoopbackCapture? _selectedCapture;
    private string _selectedLabel = string.Empty;
    private double _selectedPeak;
    private DateTime _selectedSilentSinceUtc = DateTime.MinValue;
    private DateTime _lastSilentStatusUtc = DateTime.MinValue;
    private DateTime _lastSwitchStatusUtc = DateTime.MinValue;
    private DateTime _lastCandidateRefreshUtc = DateTime.MinValue;
    private volatile bool _recording;

    private WindowsBrowserProcessLoopbackCapture(uint rootProcessId, IReadOnlyList<(uint Pid, string Label)> candidates)
    {
        _rootProcessId = rootProcessId;
        if (candidates.Count == 0) throw new InvalidOperationException("No RavaCast/WebView2 process-loopback candidates were found.");

        foreach (var (pid, label) in candidates)
            TryAddCandidate(pid, label, startImmediately: false);

        if (_captures.Count == 0) throw new InvalidOperationException("No RavaCast/WebView2 process-loopback candidates could be activated.");
        WaveFormat ??= WaveFormat.CreateIeeeFloatWaveFormat(48000, 2);
        Program.Log("Windows browser-only audio process candidates: " + string.Join(", ", _labels.Values));
    }

    public event EventHandler<WaveInEventArgs>? DataAvailable;
    public event EventHandler<StoppedEventArgs>? RecordingStopped;
    public WaveFormat WaveFormat { get; set; } = WaveFormat.CreateIeeeFloatWaveFormat(48000, 2);

    public static WindowsBrowserProcessLoopbackCapture Create(uint rendererProcessId)
    {
        var forced = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_SOURCE_PID");
        if (uint.TryParse(forced, out var forcedPid) && forcedPid > 0)
        {
            Program.Log($"Windows browser-only audio capture is forced to pid={forcedPid} by RAVACAST_AUDIO_SOURCE_PID.");
            return new WindowsBrowserProcessLoopbackCapture(rendererProcessId, [(forcedPid, $"forced pid={forcedPid}")]);
        }

        var candidates = FindCandidateProcesses(rendererProcessId);
        return new WindowsBrowserProcessLoopbackCapture(rendererProcessId, candidates);
    }

    public void StartRecording()
    {
        if (_recording) return;
        _recording = true;
        WindowsProcessLoopbackCapture[] captures;
        lock (_gate)
            captures = _captures.ToArray();
        foreach (var capture in captures)
        {
            try { capture.StartRecording(); }
            catch (Exception ex) { Program.Log($"Windows browser-only audio candidate {_labels.GetValueOrDefault(capture, "unknown")} failed to start: {Program.Flatten(ex)}"); }
        }
    }

    public void StopRecording()
    {
        _recording = false;
        WindowsProcessLoopbackCapture[] captures;
        lock (_gate)
            captures = _captures.ToArray();
        foreach (var capture in captures)
        {
            try { capture.StopRecording(); } catch { }
        }
    }

    public void Dispose()
    {
        StopRecording();
        WindowsProcessLoopbackCapture[] captures;
        lock (_gate)
            captures = _captures.ToArray();
        foreach (var capture in captures)
        {
            try { capture.DataAvailable -= OnCandidateDataAvailable; } catch { }
            try { capture.RecordingStopped -= OnCandidateRecordingStopped; } catch { }
            try { capture.Dispose(); } catch { }
        }
        lock (_gate)
        {
            _captures.Clear();
            _labels.Clear();
            _capturesByPid.Clear();
            _selectedCapture = null;
        }
    }

    private bool TryAddCandidate(uint pid, string label, bool startImmediately)
    {
        WindowsProcessLoopbackCapture? capture = null;
        try
        {
            lock (_gate)
            {
                if (_capturesByPid.ContainsKey(pid)) return false;
            }

            capture = new WindowsProcessLoopbackCapture(pid, includeTargetProcessTree: true);
            capture.DataAvailable += OnCandidateDataAvailable;
            capture.RecordingStopped += OnCandidateRecordingStopped;

            lock (_gate)
            {
                if (_capturesByPid.ContainsKey(pid))
                {
                    capture.DataAvailable -= OnCandidateDataAvailable;
                    capture.RecordingStopped -= OnCandidateRecordingStopped;
                    capture.Dispose();
                    return false;
                }

                _captures.Add(capture);
                _labels[capture] = label;
                _capturesByPid[pid] = capture;
                WaveFormat ??= capture.WaveFormat;
            }

            if (startImmediately && _recording)
                capture.StartRecording();

            Program.Log($"Windows browser-only audio added process-loopback candidate {label}.");
            return true;
        }
        catch (Exception ex)
        {
            try
            {
                if (capture is not null)
                {
                    capture.DataAvailable -= OnCandidateDataAvailable;
                    capture.RecordingStopped -= OnCandidateRecordingStopped;
                    capture.Dispose();
                }
            }
            catch { }
            Program.Log($"Windows browser-only audio candidate {label} could not be activated: {Program.Flatten(ex)}");
            return false;
        }
    }

    private void RefreshCandidateProcesses(string reason)
    {
        var now = DateTime.UtcNow;
        if ((now - _lastCandidateRefreshUtc).TotalSeconds < 2) return;
        _lastCandidateRefreshUtc = now;

        var added = 0;
        foreach (var (pid, label) in FindCandidateProcesses(_rootProcessId))
        {
            if (TryAddCandidate(pid, label, startImmediately: true))
                added++;
        }

        if (added > 0)
            Program.Log($"Windows browser-only audio refreshed process-loopback candidates after {reason}; added={added:n0}.");
    }

    private void OnCandidateDataAvailable(object? sender, WaveInEventArgs e)
    {
        if (!_recording || sender is not WindowsProcessLoopbackCapture capture || e.BytesRecorded <= 0) return;
        var label = _labels.GetValueOrDefault(capture, "unknown");
        var peak = FfmpegOpusAudioSender.CalculatePcmPeak(e.Buffer, e.BytesRecorded, capture.WaveFormat);
        var shouldForward = false;
        var shouldRefresh = false;

        lock (_gate)
        {
            var now = DateTime.UtcNow;
            if (_selectedCapture == capture)
            {
                shouldForward = true;
                _selectedPeak = peak;
                if (peak > ActivePcmThreshold)
                    _selectedSilentSinceUtc = DateTime.MinValue;
                else if (_selectedSilentSinceUtc == DateTime.MinValue)
                    _selectedSilentSinceUtc = now;
            }
            else if (peak > ActivePcmThreshold)
            {
                var selectedSilentMs = _selectedSilentSinceUtc == DateTime.MinValue ? 0 : (now - _selectedSilentSinceUtc).TotalMilliseconds;
                var selectedMissingOrSilent = _selectedCapture is null || selectedSilentMs >= 250 || _selectedPeak <= ActivePcmThreshold;
                var louderThanSelected = peak > Math.Max(ActivePcmThreshold, _selectedPeak * 2.0) && (now - _lastSwitchStatusUtc).TotalMilliseconds >= 500;
                if (selectedMissingOrSilent || louderThanSelected)
                {
                    _selectedCapture = capture;
                    _selectedLabel = label;
                    _selectedPeak = peak;
                    _selectedSilentSinceUtc = DateTime.MinValue;
                    shouldForward = true;
                    if ((now - _lastSwitchStatusUtc).TotalMilliseconds >= 500)
                    {
                        _lastSwitchStatusUtc = now;
                        Program.Log($"Windows browser-only audio selected active source {_selectedLabel}; pcmPeak={peak:0.000}. Other candidates stay armed for WebView2 process changes.");
                    }
                }
            }
            else if (_selectedCapture is null)
            {
                if ((now - _lastSilentStatusUtc).TotalSeconds >= 3)
                {
                    _lastSilentStatusUtc = now;
                    shouldRefresh = true;
                    Program.Log($"Windows browser-only audio is waiting for non-silent PCM from RavaCast/WebView2 candidates; latest silent candidate={label}; pcmPeak={peak:0.000}.");
                }
            }
        }

        if (shouldRefresh)
            RefreshCandidateProcesses("silent candidates");

        if (shouldForward)
            DataAvailable?.Invoke(this, e);
    }

    private void OnCandidateRecordingStopped(object? sender, StoppedEventArgs e)
    {
        if (e.Exception is null) return;
        var label = sender is WindowsProcessLoopbackCapture capture ? _labels.GetValueOrDefault(capture, "unknown") : "unknown";
        Program.Log($"Windows browser-only audio candidate {label} stopped with an error: {Program.Flatten(e.Exception)}");
        RecordingStopped?.Invoke(this, e);
    }

    private static IReadOnlyList<(uint Pid, string Label)> FindCandidateProcesses(uint webView2BrowserProcessId)
    {
        // Keep Direct Stream audio scoped to RavaCast's own renderer/WebView2 process family, but do not
        // rely on one root pid being the active Windows audio-session owner. Some WebView2 startup/site
        // paths put audio on a child msedgewebview2 process, and process-tree capture can be silent on a
        // subset of Windows/driver combinations even while a direct child process capture works.
        var processes = SafeGetProcesses();
        var parentByPid = GetParentProcessMap();
        var nameByPid = new Dictionary<uint, string>();
        foreach (var process in processes)
        {
            try { nameByPid[(uint)process.Id] = SafeProcessName(process); }
            catch { }
        }

        var candidates = new List<(uint Pid, string Label)>();
        var seen = new HashSet<uint>();
        void Add(uint pid, string label)
        {
            if (pid == 0 || !seen.Add(pid)) return;
            candidates.Add((pid, label));
        }

        Add(webView2BrowserProcessId, $"RavaCast WebView2/browser audio root pid={webView2BrowserProcessId} tree");

        var ancestor = webView2BrowserProcessId;
        var ancestorDepth = 0;
        while (parentByPid.TryGetValue(ancestor, out var parentPid) && parentPid != 0 && ancestorDepth++ < 8)
        {
            ancestor = parentPid;
            var ancestorName = nameByPid.TryGetValue(ancestor, out var parentName) ? parentName : string.Empty;
            if (ancestorName.Contains("RavaCast", StringComparison.OrdinalIgnoreCase))
                Add(ancestor, $"RavaCast parent {ancestorName} pid={ancestor} tree");
        }

        foreach (var process in processes)
        {
            uint pid;
            try { pid = (uint)process.Id; }
            catch { continue; }

            if (pid == webView2BrowserProcessId || !IsDescendantOf(pid, webView2BrowserProcessId, parentByPid))
                continue;

            var name = nameByPid.TryGetValue(pid, out var processName) ? processName : SafeProcessName(process);
            if (IsBrowserAudioProcessName(name) || name.Contains("RavaCast", StringComparison.OrdinalIgnoreCase))
                Add(pid, $"RavaCast/WebView2 child {name} pid={pid} tree");
        }

        foreach (var session in FindBrowserAudioSessionCandidates(processes, nameByPid, parentByPid, webView2BrowserProcessId))
            Add(session.Pid, $"RavaCast/WebView active audio session {session.Name} pid={session.Pid} state={session.State} peak={session.Peak:0.000}");

        Program.Log("Windows browser-only audio using RavaCast/WebView2 process-loopback candidates: " + string.Join(", ", candidates.Select(c => c.Label)) + ". Default-device whole-system capture is not used.");
        return candidates;
    }

    private static List<(uint Pid, string Name, float Peak, string State)> FindBrowserAudioSessionCandidates(Process[] processes, Dictionary<uint, string> nameByPid, Dictionary<uint, uint> parentByPid, uint rendererProcessId)
    {
        var sessions = new List<(uint Pid, string Name, float Peak, string State)>();
        if (!OperatingSystem.IsWindows()) return sessions;

        try
        {
            using var enumerator = new MMDeviceEnumerator();
            using var device = enumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Multimedia);
            var sessionCollection = device.AudioSessionManager.Sessions;
            for (var i = 0; i < sessionCollection.Count; i++)
            {
                var session = sessionCollection[i];
                uint pid;
                try { pid = session.GetProcessID > 0 ? (uint)session.GetProcessID : 0; }
                catch { pid = 0; }
                if (pid == 0) continue;

                var name = nameByPid.TryGetValue(pid, out var processName) ? processName : SafeProcessNameByPid(processes, pid);
                var isRendererFamily = pid == rendererProcessId || IsDescendantOf(pid, rendererProcessId, parentByPid);

                float peak;
                try { peak = session.AudioMeterInformation.MasterPeakValue; }
                catch { peak = 0; }

                var state = "unknown";
                try { state = session.State.ToString(); } catch { }

                // WebView2 can own the live audio session from an msedgewebview2.exe process that is
                // not reported as a normal child of the top-level app/window process. OBS documents the
                // same class of issue for WebView2-backed apps: application capture may need to target
                // the WebView2 subprocess itself, not the visible owner window. Keep the rescue path
                // application-scoped by adding active browser-like sessions as individual process-loopback
                // candidates, rather than falling back to default-device/whole-desktop capture.
                var isActiveBrowserAudio = IsBrowserAudioProcessName(name) && (state.Equals("Active", StringComparison.OrdinalIgnoreCase) || peak > ActivePcmThreshold);
                if (!isRendererFamily && !isActiveBrowserAudio) continue;

                sessions.Add((pid, name, peak, isRendererFamily ? state : state + "/external-webview"));
            }
        }
        catch (Exception ex)
        {
            Program.Log("Windows browser-only audio could not enumerate render audio sessions: " + Program.Flatten(ex));
        }

        if (sessions.Count > 0)
        {
            var preview = string.Join(", ", sessions
                .OrderByDescending(s => s.Peak)
                .ThenBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
                .ThenBy(s => s.Pid)
                .Take(16)
                .Select(s => $"{s.Name} pid={s.Pid} state={s.State} peak={s.Peak:0.000}"));
            Program.Log("Windows browser-only audio render session candidates: " + preview);
        }
        else
        {
            Program.Log("Windows browser-only audio found no renderer/WebView render sessions on the default output device.");
        }

        return sessions
            .OrderByDescending(s => s.Peak)
            .ThenBy(s => s.State == "Active" ? 0 : 1)
            .ThenBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Pid)
            .ToList();
    }

    private static bool IsBrowserAudioProcessName(string name) =>
        name.Contains("webview", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("msedge", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("edge", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("chrome", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("cef", StringComparison.OrdinalIgnoreCase);

    private static string SafeProcessNameByPid(Process[] processes, uint pid)
    {
        foreach (var process in processes)
        {
            try
            {
                if (process.Id == pid) return SafeProcessName(process);
            }
            catch { }
        }

        return "unknown";
    }

    private static Process[] SafeGetProcesses()
    {
        try { return Process.GetProcesses(); }
        catch { return []; }
    }

    private static string SafeProcessName(Process process)
    {
        try { return process.ProcessName; }
        catch { return "unknown"; }
    }

    private static bool IsDescendantOf(uint pid, uint rootPid, Dictionary<uint, uint> parentByPid)
    {
        var seen = new HashSet<uint>();
        var current = pid;
        while (parentByPid.TryGetValue(current, out var parent) && parent != 0 && seen.Add(current))
        {
            if (parent == rootPid) return true;
            current = parent;
        }
        return false;
    }

    private static Dictionary<uint, uint> GetParentProcessMap()
    {
        var map = new Dictionary<uint, uint>();
        if (!OperatingSystem.IsWindows()) return map;

        var snapshot = CreateToolhelp32Snapshot(0x00000002, 0);
        if (snapshot == IntPtr.Zero || snapshot == new IntPtr(-1)) return map;

        try
        {
            var entry = new ProcessEntry32 { dwSize = (uint)Marshal.SizeOf<ProcessEntry32>() };
            if (!Process32First(snapshot, ref entry)) return map;
            do
            {
                map[entry.th32ProcessID] = entry.th32ParentProcessID;
            } while (Process32Next(snapshot, ref entry));
        }
        finally
        {
            CloseHandle(snapshot);
        }

        return map;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr CreateToolhelp32Snapshot(uint dwFlags, uint th32ProcessID);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool Process32First(IntPtr hSnapshot, ref ProcessEntry32 lppe);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool Process32Next(IntPtr hSnapshot, ref ProcessEntry32 lppe);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr hObject);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    private struct ProcessEntry32
    {
        public uint dwSize;
        public uint cntUsage;
        public uint th32ProcessID;
        public IntPtr th32DefaultHeapID;
        public uint th32ModuleID;
        public uint cntThreads;
        public uint th32ParentProcessID;
        public int pcPriClassBase;
        public uint dwFlags;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
        public string szExeFile;
    }
}


internal sealed class WindowsProcessLoopbackCapture : IWaveIn
{
    private const string VirtualAudioDeviceProcessLoopback = "VAD\\Process_Loopback";
    private const int AudioClientActivationTypeProcessLoopback = 1;
    private const int ProcessLoopbackModeIncludeTargetProcessTree = 0;
    private const int ClsCtxAll = 23;
    private const ushort VtBlob = 65;
    private const int AudclntSBufferEmpty = unchecked((int)0x88900001);
    private static readonly Guid IAudioClientGuid = new("1CB9AD4C-DBFA-4c32-B178-C2F568A703B2");
    private static readonly Guid IAudioCaptureClientGuid = new("C8ADBD64-E71E-48a0-A4DE-185C395CD317");
    private static readonly Guid PcmSubFormatGuid = new("00000001-0000-0010-8000-00aa00389b71");
    private static readonly Guid IeeeFloatSubFormatGuid = new("00000003-0000-0010-8000-00aa00389b71");

    private readonly uint _targetProcessId;
    private readonly bool _includeTargetProcessTree;
    private readonly CancellationTokenSource _cts = new();
    private Thread? _captureThread;
    private volatile bool _recording;

    public WindowsProcessLoopbackCapture(uint targetProcessId, bool includeTargetProcessTree)
    {
        if (targetProcessId == 0) throw new ArgumentOutOfRangeException(nameof(targetProcessId));
        _targetProcessId = targetProcessId;
        _includeTargetProcessTree = includeTargetProcessTree;
        WaveFormat = GetProcessLoopbackMixFormat(targetProcessId, includeTargetProcessTree);
    }

    public event EventHandler<WaveInEventArgs>? DataAvailable;
    public event EventHandler<StoppedEventArgs>? RecordingStopped;
    public WaveFormat WaveFormat { get; set; }

    public void StartRecording()
    {
        if (_recording) return;
        _recording = true;
        _captureThread = new Thread(CaptureThreadMain)
        {
            IsBackground = true,
            Name = "RavaCast browser-only audio capture"
        };
        try { _captureThread.SetApartmentState(ApartmentState.MTA); } catch { }
        _captureThread.Start();
    }

    public void StopRecording()
    {
        _recording = false;
        try { _cts.Cancel(); } catch { }
        try
        {
            if (_captureThread is not null && _captureThread.IsAlive && Thread.CurrentThread != _captureThread)
                _captureThread.Join(TimeSpan.FromSeconds(2));
        }
        catch { }
    }

    private void CaptureThreadMain()
    {
        Exception? stoppedException = null;
        IAudioClient? audioClient = null;
        IAudioCaptureClient? captureClient = null;
        IntPtr mixFormat = IntPtr.Zero;

        try
        {
            audioClient = ActivateProcessLoopbackAudioClient(_targetProcessId, _includeTargetProcessTree);

            // The virtual process-loopback IAudioClient can activate successfully but still return
            // E_NOTIMPL from GetMixFormat on some Windows builds. Use the default render endpoint
            // mix format for shared-mode initialisation instead; the capture remains scoped to the
            // target RavaCast.Renderer process tree, not the whole desktop output.
            mixFormat = AllocWaveFormatPointer(WaveFormat);

            using var receiveEvent = new EventWaitHandle(false, EventResetMode.AutoReset);
            var session = Guid.Empty;
            var hr = audioClient.Initialize(AudioClientShareMode.Shared, AudioClientStreamFlags.Loopback | AudioClientStreamFlags.EventCallback, 10_000_000, 0, mixFormat, ref session);
            ThrowForHResult(hr, "IAudioClient.Initialize failed for process loopback capture");
            hr = audioClient.SetEventHandle(receiveEvent.SafeWaitHandle.DangerousGetHandle());
            ThrowForHResult(hr, "IAudioClient.SetEventHandle failed for process loopback capture");

            var audioCaptureClientGuid = IAudioCaptureClientGuid;
            hr = audioClient.GetService(ref audioCaptureClientGuid, out var service);
            ThrowForHResult(hr, "IAudioClient.GetService(IAudioCaptureClient) failed");
            captureClient = (IAudioCaptureClient)service;

            hr = audioClient.Start();
            ThrowForHResult(hr, "IAudioClient.Start failed for process loopback capture");

            var blockAlign = Math.Max(1, WaveFormat.BlockAlign);
            var zeroBuffer = Array.Empty<byte>();

            while (!_cts.IsCancellationRequested && _recording)
            {
                receiveEvent.WaitOne(100);
                hr = captureClient.GetNextPacketSize(out var packetFrames);
                if (hr == AudclntSBufferEmpty || packetFrames == 0)
                    continue;

                ThrowForHResult(hr, "IAudioCaptureClient.GetNextPacketSize failed");

                while (packetFrames > 0 && !_cts.IsCancellationRequested && _recording)
                {
                    hr = captureClient.GetBuffer(out var data, out var frames, out var flags, out _, out _);
                    ThrowForHResult(hr, "IAudioCaptureClient.GetBuffer failed");

                    var byteCount = checked((int)frames * blockAlign);
                    if (byteCount > 0)
                    {
                        var buffer = new byte[byteCount];
                        if ((flags & AudioClientBufferFlags.Silent) != 0 || data == IntPtr.Zero)
                        {
                            if (zeroBuffer.Length < byteCount) zeroBuffer = new byte[byteCount];
                            Buffer.BlockCopy(zeroBuffer, 0, buffer, 0, byteCount);
                        }
                        else
                        {
                            Marshal.Copy(data, buffer, 0, byteCount);
                        }

                        DataAvailable?.Invoke(this, new WaveInEventArgs(buffer, byteCount));
                    }

                    hr = captureClient.ReleaseBuffer(frames);
                    ThrowForHResult(hr, "IAudioCaptureClient.ReleaseBuffer failed");

                    hr = captureClient.GetNextPacketSize(out packetFrames);
                    if (hr == AudclntSBufferEmpty) break;
                    ThrowForHResult(hr, "IAudioCaptureClient.GetNextPacketSize failed");
                }
            }
        }
        catch (Exception ex)
        {
            stoppedException = ex;
        }
        finally
        {
            try { audioClient?.Stop(); } catch { }
            if (mixFormat != IntPtr.Zero) Marshal.FreeCoTaskMem(mixFormat);
            try { if (captureClient is not null) Marshal.ReleaseComObject(captureClient); } catch { }
            try { if (audioClient is not null) Marshal.ReleaseComObject(audioClient); } catch { }
            _recording = false;
            RecordingStopped?.Invoke(this, new StoppedEventArgs(stoppedException));
        }
    }

    private static WaveFormat GetProcessLoopbackMixFormat(uint targetProcessId, bool includeTargetProcessTree)
    {
        // OBS initialises Application Audio Capture with the program output format rather than trusting
        // the current playback device mix format. Keep RavaCast equally predictable: the process-loopback
        // client always delivers 48 kHz stereo 32-bit float PCM into FFmpeg, and FFmpeg then encodes Opus.
        var format = WaveFormat.CreateIeeeFloatWaveFormat(48000, 2);
        Program.Log($"Browser-only process loopback will initialise with OBS-style fixed capture format: {format.SampleRate} Hz, {format.Channels} channel(s), {format.BitsPerSample}-bit {format.Encoding}; targetPid={targetProcessId}; includeTree={includeTargetProcessTree}.");
        return format;
    }

    private static IntPtr AllocWaveFormatPointer(WaveFormat format)
    {
        var channels = (ushort)Math.Clamp(format.Channels > 0 ? format.Channels : 2, 1, ushort.MaxValue);
        var sampleRate = Math.Max(8000, format.SampleRate > 0 ? format.SampleRate : 48000);
        var bits = (ushort)Math.Clamp(format.BitsPerSample > 0 ? format.BitsPerSample : 32, 8, ushort.MaxValue);
        var bytesPerSample = Math.Max(1, bits / 8);
        var blockAlign = (ushort)Math.Clamp(channels * bytesPerSample, 1, ushort.MaxValue);
        var averageBytesPerSecond = checked(sampleRate * blockAlign);

        // WAVEFORMATEXTENSIBLE, matching the shape OBS uses for process-loopback capture. This avoids a
        // class of silent captures caused by asking the virtual Process_Loopback device for the physical
        // endpoint's mixed format or by giving it a minimal WAVEFORMATEX with no channel mask/subformat.
        var ptr = Marshal.AllocCoTaskMem(40);
        try
        {
            Marshal.WriteInt16(ptr, 0, unchecked((short)0xFFFE)); // WAVE_FORMAT_EXTENSIBLE
            Marshal.WriteInt16(ptr, 2, unchecked((short)channels));
            Marshal.WriteInt32(ptr, 4, sampleRate);
            Marshal.WriteInt32(ptr, 8, averageBytesPerSecond);
            Marshal.WriteInt16(ptr, 12, unchecked((short)blockAlign));
            Marshal.WriteInt16(ptr, 14, unchecked((short)bits));
            Marshal.WriteInt16(ptr, 16, 22); // cbSize
            Marshal.WriteInt16(ptr, 18, unchecked((short)bits)); // wValidBitsPerSample
            Marshal.WriteInt32(ptr, 20, channels switch
            {
                1 => 0x4, // SPEAKER_FRONT_CENTER
                2 => 0x3, // SPEAKER_FRONT_LEFT | SPEAKER_FRONT_RIGHT
                _ => 0
            });
            Marshal.Copy(IeeeFloatSubFormatGuid.ToByteArray(), 0, IntPtr.Add(ptr, 24), 16);
            return ptr;
        }
        catch
        {
            Marshal.FreeCoTaskMem(ptr);
            throw;
        }
    }

    private static IAudioClient ActivateProcessLoopbackAudioClient(uint targetProcessId, bool includeTargetProcessTree)
    {
        var activationParams = new AudioClientActivationParams
        {
            ActivationType = AudioClientActivationTypeProcessLoopback,
            ProcessLoopbackParams = new ProcessLoopbackParams
            {
                TargetProcessId = targetProcessId,
                ProcessLoopbackMode = includeTargetProcessTree ? ProcessLoopbackModeIncludeTargetProcessTree : 1
            }
        };

        var activationParamsPtr = IntPtr.Zero;
        var propVariantPtr = IntPtr.Zero;

        try
        {
            activationParamsPtr = Marshal.AllocHGlobal(Marshal.SizeOf<AudioClientActivationParams>());
            Marshal.StructureToPtr(activationParams, activationParamsPtr, false);

            var propVariant = new PropVariant
            {
                vt = VtBlob,
                blob = new PropVariantBlob
                {
                    cbSize = Marshal.SizeOf<AudioClientActivationParams>(),
                    pBlobData = activationParamsPtr
                }
            };
            propVariantPtr = Marshal.AllocHGlobal(Marshal.SizeOf<PropVariant>());
            Marshal.StructureToPtr(propVariant, propVariantPtr, false);

            var completion = new ActivateCompletionHandler();
            var audioClientGuid = IAudioClientGuid;
            ThrowForHResult(ActivateAudioInterfaceAsync(VirtualAudioDeviceProcessLoopback, ref audioClientGuid, propVariantPtr, completion, out _), "ActivateAudioInterfaceAsync failed for browser-only process loopback");
            return completion.WaitForAudioClient(TimeSpan.FromSeconds(5));
        }
        finally
        {
            if (propVariantPtr != IntPtr.Zero) Marshal.FreeHGlobal(propVariantPtr);
            if (activationParamsPtr != IntPtr.Zero) Marshal.FreeHGlobal(activationParamsPtr);
        }
    }

    private static WaveFormat WaveFormatFromPointer(IntPtr waveFormatPtr)
    {
        var tag = (WaveFormatEncoding)Marshal.ReadInt16(waveFormatPtr, 0);
        var channels = Marshal.ReadInt16(waveFormatPtr, 2);
        var sampleRate = Marshal.ReadInt32(waveFormatPtr, 4);
        var blockAlign = Marshal.ReadInt16(waveFormatPtr, 12);
        var bits = Marshal.ReadInt16(waveFormatPtr, 14);

        if (tag == WaveFormatEncoding.IeeeFloat && bits == 32)
            return WaveFormat.CreateIeeeFloatWaveFormat(sampleRate, channels);

        if (tag == WaveFormatEncoding.Pcm)
            return new WaveFormat(sampleRate, bits, channels);

        if (tag == WaveFormatEncoding.Extensible)
        {
            var subFormatBytes = new byte[16];
            Marshal.Copy(waveFormatPtr + 24, subFormatBytes, 0, 16);
            var subFormat = new Guid(subFormatBytes);
            if (subFormat == IeeeFloatSubFormatGuid || bits == 32 && blockAlign == channels * 4)
                return WaveFormat.CreateIeeeFloatWaveFormat(sampleRate, channels);
            if (subFormat == PcmSubFormatGuid)
                return new WaveFormat(sampleRate, bits, channels);
        }

        throw new NotSupportedException($"Unsupported browser-only process loopback mix format: tag={tag}, sampleRate={sampleRate}, channels={channels}, bits={bits}.");
    }

    private static void ThrowForHResult(int hr, string message)
    {
        if (hr >= 0) return;
        throw new InvalidOperationException(message + $" HRESULT=0x{hr:X8}", Marshal.GetExceptionForHR(hr));
    }

    public void Dispose()
    {
        StopRecording();
        try { _cts.Dispose(); } catch { }
    }

    [DllImport("Mmdevapi.dll", ExactSpelling = true, CharSet = CharSet.Unicode)]
    private static extern int ActivateAudioInterfaceAsync([MarshalAs(UnmanagedType.LPWStr)] string deviceInterfacePath, ref Guid riid, IntPtr activationParams, IActivateAudioInterfaceCompletionHandler completionHandler, out IActivateAudioInterfaceAsyncOperation activationOperation);

    [StructLayout(LayoutKind.Sequential)]
    private struct PropVariant
    {
        public ushort vt;
        public ushort wReserved1;
        public ushort wReserved2;
        public ushort wReserved3;
        public PropVariantBlob blob;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct PropVariantBlob
    {
        public int cbSize;
        public IntPtr pBlobData;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct AudioClientActivationParams
    {
        public int ActivationType;
        public ProcessLoopbackParams ProcessLoopbackParams;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ProcessLoopbackParams
    {
        public uint TargetProcessId;
        public int ProcessLoopbackMode;
    }

    [ComImport]
    [Guid("41D949AB-9862-444A-80F6-C261334DA5EB")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IActivateAudioInterfaceCompletionHandler
    {
        void ActivateCompleted(IActivateAudioInterfaceAsyncOperation activateOperation);
    }

    [ComImport]
    [Guid("72A22D78-CDE4-431D-B8CC-843A71199B6D")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IActivateAudioInterfaceAsyncOperation
    {
        [PreserveSig]
        int GetActivateResult(out int activateResult, [MarshalAs(UnmanagedType.IUnknown)] out object activatedInterface);
    }

    private sealed class ActivateCompletionHandler : IActivateAudioInterfaceCompletionHandler
    {
        private readonly ManualResetEventSlim _completed = new(false);
        private Exception? _exception;
        private IAudioClient? _audioClient;

        public void ActivateCompleted(IActivateAudioInterfaceAsyncOperation activateOperation)
        {
            try
            {
                ThrowForHResult(activateOperation.GetActivateResult(out var activateResult, out var activatedInterface), "IActivateAudioInterfaceAsyncOperation.GetActivateResult failed");
                ThrowForHResult(activateResult, "Browser-only process loopback activation failed");
                _audioClient = (IAudioClient)activatedInterface;
            }
            catch (Exception ex)
            {
                _exception = ex;
            }
            finally
            {
                _completed.Set();
            }
        }

        public IAudioClient WaitForAudioClient(TimeSpan timeout)
        {
            if (!_completed.Wait(timeout))
                throw new TimeoutException("Timed out activating browser-only process loopback capture.");
            if (_exception is not null)
                throw _exception;
            return _audioClient ?? throw new InvalidOperationException("Browser-only process loopback activation returned no audio client.");
        }
    }

    [ComImport]
    [Guid("1CB9AD4C-DBFA-4C32-B178-C2F568A703B2")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IAudioClient
    {
        [PreserveSig]
        int Initialize(AudioClientShareMode shareMode, AudioClientStreamFlags streamFlags, long hnsBufferDuration, long hnsPeriodicity, IntPtr pFormat, ref Guid audioSessionGuid);
        [PreserveSig]
        int GetBufferSize(out uint bufferSize);
        [PreserveSig]
        int GetStreamLatency(out long latency);
        [PreserveSig]
        int GetCurrentPadding(out uint currentPadding);
        [PreserveSig]
        int IsFormatSupported(AudioClientShareMode shareMode, AudioClientStreamFlags streamFlags, IntPtr pFormat, out IntPtr closestMatchFormat);
        [PreserveSig]
        int GetMixFormat(out IntPtr deviceFormat);
        [PreserveSig]
        int GetDevicePeriod(out long defaultDevicePeriod, out long minimumDevicePeriod);
        [PreserveSig]
        int Start();
        [PreserveSig]
        int Stop();
        [PreserveSig]
        int Reset();
        [PreserveSig]
        int SetEventHandle(IntPtr eventHandle);
        [PreserveSig]
        int GetService(ref Guid riid, [MarshalAs(UnmanagedType.IUnknown)] out object service);
    }

    [ComImport]
    [Guid("C8ADBD64-E71E-48A0-A4DE-185C395CD317")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IAudioCaptureClient
    {
        [PreserveSig]
        int GetBuffer(out IntPtr data, out uint numFramesToRead, out AudioClientBufferFlags bufferFlags, out ulong devicePosition, out ulong qpcPosition);
        [PreserveSig]
        int ReleaseBuffer(uint numFramesRead);
        [PreserveSig]
        int GetNextPacketSize(out uint numFramesInNextPacket);
    }

    private enum AudioClientShareMode
    {
        Shared = 0,
        Exclusive = 1
    }

    [Flags]
    private enum AudioClientStreamFlags
    {
        None = 0,
        CrossProcess = 0x00010000,
        Loopback = 0x00020000,
        EventCallback = 0x00040000,
        NoPersist = 0x00080000
    }

    [Flags]
    private enum AudioClientBufferFlags
    {
        None = 0,
        DataDiscontinuity = 0x1,
        Silent = 0x2,
        TimestampError = 0x4
    }
}


internal sealed class OggOpusPacketReader
{
    private readonly List<byte> _buffer = [];
    private readonly MemoryStream _packet = new();
    private int _headersSkipped;

    public List<byte[]> Append(byte[] data)
    {
        var packets = new List<byte[]>();
        if (data.Length == 0) return packets;
        _buffer.AddRange(data);

        while (TryReadPage(out var payload, out var segments))
        {
            var payloadOffset = 0;
            foreach (var segmentLength in segments)
            {
                if (segmentLength > 0)
                    _packet.Write(payload, payloadOffset, segmentLength);
                payloadOffset += segmentLength;

                if (segmentLength < 255)
                {
                    var packet = _packet.ToArray();
                    _packet.SetLength(0);
                    if (packet.Length == 0) continue;

                    if (_headersSkipped < 2 && (StartsWith(packet, "OpusHead") || StartsWith(packet, "OpusTags")))
                    {
                        _headersSkipped++;
                        continue;
                    }

                    if (_headersSkipped < 2)
                    {
                        _headersSkipped++;
                        continue;
                    }

                    packets.Add(packet);
                }
            }
        }

        return packets;
    }

    private bool TryReadPage(out byte[] payload, out byte[] segments)
    {
        payload = [];
        segments = [];

        while (_buffer.Count >= 4 && !(_buffer[0] == (byte)'O' && _buffer[1] == (byte)'g' && _buffer[2] == (byte)'g' && _buffer[3] == (byte)'S'))
            _buffer.RemoveAt(0);

        if (_buffer.Count < 27) return false;
        var segmentCount = _buffer[26];
        var headerLength = 27 + segmentCount;
        if (_buffer.Count < headerLength) return false;

        segments = _buffer.GetRange(27, segmentCount).ToArray();
        var payloadLength = 0;
        foreach (var segment in segments) payloadLength += segment;
        var pageLength = headerLength + payloadLength;
        if (_buffer.Count < pageLength) return false;

        payload = _buffer.GetRange(headerLength, payloadLength).ToArray();
        _buffer.RemoveRange(0, pageLength);
        return true;
    }

    private static bool StartsWith(byte[] packet, string text)
    {
        var bytes = Encoding.ASCII.GetBytes(text);
        if (packet.Length < bytes.Length) return false;
        for (var i = 0; i < bytes.Length; i++)
            if (packet[i] != bytes[i]) return false;
        return true;
    }
}

internal sealed class OggOpusPageWriter
{
    private readonly int _serial = Random.Shared.Next(1, int.MaxValue);
    private int _sequence;
    private long _granule;
    private bool _headersWritten;

    public IEnumerable<byte[]> WriteRawOpusPacket(byte[] opusPacket)
    {
        if (!_headersWritten)
        {
            _headersWritten = true;
            yield return BuildPage(BuildOpusHead(), 0x02, 0);
            yield return BuildPage(BuildOpusTags(), 0x00, 0);
        }

        _granule += 960; // 20 ms Opus frames at 48 kHz, matching FFmpeg -frame_duration 20.
        yield return BuildPage(opusPacket, 0x00, _granule);
    }

    private byte[] BuildPage(byte[] packet, byte headerType, long granulePosition)
    {
        var laces = new List<byte>();
        var remaining = packet.Length;
        while (remaining >= 255)
        {
            laces.Add(255);
            remaining -= 255;
        }
        laces.Add((byte)remaining);

        var page = new byte[27 + laces.Count + packet.Length];
        page[0] = (byte)'O'; page[1] = (byte)'g'; page[2] = (byte)'g'; page[3] = (byte)'S';
        page[4] = 0;
        page[5] = headerType;
        WriteInt64Le(page, 6, granulePosition);
        WriteInt32Le(page, 14, _serial);
        WriteInt32Le(page, 18, _sequence++);
        page[26] = (byte)laces.Count;
        for (var i = 0; i < laces.Count; i++) page[27 + i] = laces[i];
        Buffer.BlockCopy(packet, 0, page, 27 + laces.Count, packet.Length);

        var crc = OggCrc(page);
        WriteUInt32Le(page, 22, crc);
        return page;
    }

    private static byte[] BuildOpusHead()
    {
        var packet = new byte[19];
        Buffer.BlockCopy(Encoding.ASCII.GetBytes("OpusHead"), 0, packet, 0, 8);
        packet[8] = 1;      // version
        packet[9] = 2;      // channels
        packet[10] = 0x38;  // pre-skip 312, little-endian
        packet[11] = 0x01;
        WriteInt32Le(packet, 12, 48000);
        packet[16] = 0; packet[17] = 0; // output gain
        packet[18] = 0;                 // channel mapping family
        return packet;
    }

    private static byte[] BuildOpusTags()
    {
        var vendor = Encoding.UTF8.GetBytes("RavaCast");
        var packet = new byte[8 + 4 + vendor.Length + 4];
        Buffer.BlockCopy(Encoding.ASCII.GetBytes("OpusTags"), 0, packet, 0, 8);
        WriteInt32Le(packet, 8, vendor.Length);
        Buffer.BlockCopy(vendor, 0, packet, 12, vendor.Length);
        WriteInt32Le(packet, 12 + vendor.Length, 0);
        return packet;
    }

    private static void WriteInt32Le(byte[] buffer, int offset, int value)
    {
        unchecked
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }
    }

    private static void WriteUInt32Le(byte[] buffer, int offset, uint value)
    {
        unchecked
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }
    }

    private static void WriteInt64Le(byte[] buffer, int offset, long value)
    {
        unchecked
        {
            for (var i = 0; i < 8; i++)
                buffer[offset + i] = (byte)(value >> (8 * i));
        }
    }

    private static uint OggCrc(byte[] data)
    {
        uint crc = 0;
        foreach (var b in data)
            crc = (crc << 8) ^ CrcTable[(int)(((crc >> 24) & 0xFF) ^ b)];
        return crc;
    }

    private static readonly uint[] CrcTable = BuildCrcTable();

    private static uint[] BuildCrcTable()
    {
        var table = new uint[256];
        for (var i = 0; i < table.Length; i++)
        {
            var r = (uint)i << 24;
            for (var j = 0; j < 8; j++)
                r = (r & 0x80000000) != 0 ? (r << 1) ^ 0x04C11DB7u : r << 1;
            table[i] = r;
        }
        return table;
    }
}


internal sealed class FfmpegOpusAudioPlayer : IDisposable
{
    private readonly Process _process;
    private readonly object _stdinGate = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _stderrTask;
    private readonly Task? _pcmStdoutTask;
    private readonly BufferedWaveProvider? _waveBuffer;
    private readonly IWavePlayer? _waveOut;
    private readonly OggOpusPageWriter _rawOpusPageWriter = new();
    private readonly List<byte> _pendingOggInput = [];
    private readonly object _playbackStateGate = new();
    private bool _playbackMuted;
    private float _playbackVolume = 1f;
    private bool _rawOpusInputStarted;
    private bool _oggOpusInputStarted;
    private long _receivedBytes;
    private long _chunks;
    private long _decodedPcmBytes;
    private DateTime _lastPlaybackStatusUtc = DateTime.MinValue;
    private bool _firstDecodedPcmLogged;
    private bool _firstPcmQueuedLogged;
    private volatile bool _broken;
    private readonly int _playbackPrebufferMs = ResolvePlaybackPrebufferMs();
    private readonly int _playbackMaxBufferedMs = ResolvePlaybackMaxBufferedMs();
    private volatile bool _playbackStarted;

    private FfmpegOpusAudioPlayer(Process process, BufferedWaveProvider? waveBuffer = null, IWavePlayer? waveOut = null)
    {
        _process = process;
        _waveBuffer = waveBuffer;
        _waveOut = waveOut;
        _playbackStarted = waveBuffer is null || waveOut is null;
        _stderrTask = Task.Run(ReadStderrAsync);
        if (_waveBuffer is not null)
            _pcmStdoutTask = Task.Run(ReadDecodedPcmStdoutAsync);
    }

    public long ReceivedBytes => Interlocked.Read(ref _receivedBytes);
    public long Chunks => Interlocked.Read(ref _chunks);
    public bool IsBroken => _broken;
    public bool PlaybackStarted => _playbackStarted;
    public int PlaybackPrebufferMs => _playbackPrebufferMs;

    public void SetPlaybackAudioState(bool muted, float volume)
    {
        lock (_playbackStateGate)
        {
            _playbackMuted = muted;
            _playbackVolume = Math.Clamp(volume, 0f, 1f);
        }
    }

    private static int ResolvePlaybackPrebufferMs()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_SYNC_DELAY_MS");
        if (int.TryParse(raw, out var parsed)) return Math.Clamp(parsed, 0, 1000);
        return 140;
    }

    private static int ResolvePlaybackMaxBufferedMs()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_MAX_BUFFER_MS");
        if (int.TryParse(raw, out var parsed)) return Math.Clamp(parsed, 120, 2000);
        return 360;
    }

    public static bool TryStart(out FfmpegOpusAudioPlayer? player, out string detail)
    {
        player = null;
        if (!FfmpegRuntimeProbe.TryFindFfmpeg(out var ffmpeg, out detail))
            return false;

        var overrideArgs = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_PLAYBACK_ARGS");
        if (!string.IsNullOrWhiteSpace(overrideArgs))
            return TryStartFfmpegDevicePlayback(ffmpeg, overrideArgs, "Using RAVACAST_AUDIO_PLAYBACK_ARGS.", out player, out detail);

        if (OperatingSystem.IsWindows())
            return TryStartWindowsWaveOutPlayback(ffmpeg, out player, out detail);

        if (OperatingSystem.IsLinux())
            return TryStartFfmpegDevicePlayback(ffmpeg, "-f pulse default", "Using FFmpeg PulseAudio default audio playback. Override with RAVACAST_AUDIO_PLAYBACK_ARGS if needed.", out player, out detail);

        detail = "No default Direct Stream audio playback path is known for this platform. Set RAVACAST_AUDIO_PLAYBACK_ARGS.";
        return false;
    }

    private static bool TryStartFfmpegDevicePlayback(string ffmpeg, string playbackArgs, string playbackDetail, out FfmpegOpusAudioPlayer? player, out string detail)
    {
        player = null;
        var args = "-hide_banner -loglevel warning -fflags nobuffer -flags low_delay -f ogg -i pipe:0 -vn -ac 2 -ar 48000 " + playbackArgs;

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
                    RedirectStandardError = true
                },
                EnableRaisingEvents = true
            };

            if (!process.Start())
            {
                detail = "FFmpeg Opus audio playback failed to start.";
                return false;
            }

            player = new FfmpegOpusAudioPlayer(process);
            detail = "FFmpeg Opus audio playback started. " + playbackDetail;
            return true;
        }
        catch (Exception ex)
        {
            detail = "FFmpeg Opus audio playback failed to start: " + ex.Message;
            return false;
        }
    }

    private static bool TryStartWindowsWaveOutPlayback(string ffmpeg, out FfmpegOpusAudioPlayer? player, out string detail)
    {
        player = null;
        Process? process = null;
        IWavePlayer? waveOut = null;

        try
        {
            var waveFormat = new WaveFormat(48000, 16, 2);
            var waveBuffer = new BufferedWaveProvider(waveFormat)
            {
                BufferDuration = TimeSpan.FromSeconds(2),
                DiscardOnBufferOverflow = true
            };

            var playbackDetail = CreateWindowsAudioOutput(waveBuffer, out waveOut);

            var args = "-hide_banner -loglevel warning -fflags nobuffer -flags low_delay -f ogg -i pipe:0 -vn -ac 2 -ar 48000 -f s16le pipe:1";
            process = new Process
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
                detail = "FFmpeg Opus decoder failed to start for Windows audio playback.";
                waveOut.Dispose();
                process.Dispose();
                return false;
            }

            player = new FfmpegOpusAudioPlayer(process, waveBuffer, waveOut);
            detail = "Windows audio playback prepared through FFmpeg Opus decode + " + playbackDetail + ". Audio starts after a small receiver prebuffer so it lines up better with decoded video. This avoids relying on FFmpeg's optional WASAPI output device.";
            Program.Log("Direct Stream Windows audio output ready: " + playbackDetail + ".");
            return true;
        }
        catch (Exception ex)
        {
            try { waveOut?.Dispose(); } catch { }
            try { if (process is not null && !process.HasExited) process.Kill(entireProcessTree: true); } catch { }
            try { process?.Dispose(); } catch { }
            detail = "Windows audio playback failed to start: " + ex.Message;
            return false;
        }
    }

    private static string CreateWindowsAudioOutput(BufferedWaveProvider waveBuffer, out IWavePlayer waveOut)
    {
        var overrideDriver = Environment.GetEnvironmentVariable("RAVACAST_AUDIO_PLAYBACK_DRIVER")?.Trim().ToLowerInvariant();

        if (overrideDriver is "waveout")
        {
            waveOut = CreateWaveOut(waveBuffer);
            return "NAudio WaveOutEvent default output (forced by RAVACAST_AUDIO_PLAYBACK_DRIVER=waveout)";
        }

        if (overrideDriver is "directsound")
        {
            waveOut = CreateDirectSoundOut(waveBuffer);
            return "NAudio DirectSound default output (forced by RAVACAST_AUDIO_PLAYBACK_DRIVER=directsound)";
        }

        if (overrideDriver is "wasapi" or null or "")
        {
            try
            {
                var enumerator = new MMDeviceEnumerator();
                var device = enumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Multimedia);
                var wasapiOut = new WasapiOut(device, NAudio.CoreAudioApi.AudioClientShareMode.Shared, true, 80);
                wasapiOut.Init(waveBuffer);
                waveOut = wasapiOut;
                return $"NAudio WASAPI shared render device '{device.FriendlyName}'";
            }
            catch (Exception ex) when (overrideDriver is null or "")
            {
                Program.Log("Direct Stream WASAPI receiver playback could not start; falling back to DirectSound. " + Program.Flatten(ex));
            }
        }

        if (overrideDriver is "wasapi")
            throw new InvalidOperationException("WASAPI receiver playback was forced but could not start. Try RAVACAST_AUDIO_PLAYBACK_DRIVER=directsound or waveout.");

        try
        {
            waveOut = CreateDirectSoundOut(waveBuffer);
            return "NAudio DirectSound default output";
        }
        catch (Exception ex)
        {
            Program.Log("Direct Stream DirectSound receiver playback could not start; falling back to WaveOutEvent. " + Program.Flatten(ex));
            waveOut = CreateWaveOut(waveBuffer);
            return "NAudio WaveOutEvent default output";
        }
    }

    private static IWavePlayer CreateDirectSoundOut(BufferedWaveProvider waveBuffer)
    {
        var output = new DirectSoundOut(DirectSoundOut.DSDEVID_DefaultPlayback, 80);
        output.Init(waveBuffer);
        return output;
    }

    private static IWavePlayer CreateWaveOut(BufferedWaveProvider waveBuffer)
    {
        var output = new WaveOutEvent
        {
            DesiredLatency = 80,
            NumberOfBuffers = 3
        };
        output.Init(waveBuffer);
        return output;
    }

    public bool PushOggOpusBytes(byte[] data)
    {
        if (_broken || data.Length == 0) return false;
        try
        {
            byte[] bytesToWrite;
            lock (_stdinGate)
            {
                if (_process.HasExited) { _broken = true; return false; }
                if (_rawOpusInputStarted) return false;

                if (!_oggOpusInputStarted)
                {
                    _pendingOggInput.AddRange(data);
                    var oggStart = IndexOfOggPage(_pendingOggInput);
                    if (oggStart < 0)
                    {
                        TrimPendingOggInput();
                        return false;
                    }

                    if (oggStart > 0)
                        _pendingOggInput.RemoveRange(0, oggStart);

                    // Do not poison FFmpeg with a mid-stream Ogg fragment. It must see the initial
                    // OggS page/OpusHead first. Once we have that, normal streaming chunks can follow.
                    if (_pendingOggInput.Count < 27)
                        return false;

                    bytesToWrite = _pendingOggInput.ToArray();
                    _pendingOggInput.Clear();
                    _oggOpusInputStarted = true;
                }
                else
                {
                    bytesToWrite = data;
                }

                _process.StandardInput.BaseStream.Write(bytesToWrite, 0, bytesToWrite.Length);
                _process.StandardInput.BaseStream.Flush();
            }
            Interlocked.Add(ref _receivedBytes, data.Length);
            Interlocked.Increment(ref _chunks);
            return true;
        }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg Opus audio playback stdin failed: " + Program.Flatten(ex));
            return false;
        }
    }

    public bool PushRawOpusPacket(byte[] packet)
    {
        if (_broken || packet.Length == 0) return false;
        try
        {
            lock (_stdinGate)
            {
                if (_process.HasExited) { _broken = true; return false; }
                if (_oggOpusInputStarted) return false;
                _rawOpusInputStarted = true;
                _pendingOggInput.Clear();
                var writtenPages = 0;
                var writtenBytes = 0;
                foreach (var page in _rawOpusPageWriter.WriteRawOpusPacket(packet))
                {
                    _process.StandardInput.BaseStream.Write(page, 0, page.Length);
                    writtenPages++;
                    writtenBytes += page.Length;
                }
                _process.StandardInput.BaseStream.Flush();
                if (Interlocked.Read(ref _chunks) == 0)
                    Program.Log($"FFmpeg Opus audio playback accepted first raw Opus packet: packet={packet.Length} bytes; oggPages={writtenPages}; oggBytes={writtenBytes}.");
            }
            Interlocked.Add(ref _receivedBytes, packet.Length);
            Interlocked.Increment(ref _chunks);
            return true;
        }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg Opus audio playback raw RTP stdin failed: " + Program.Flatten(ex));
            return false;
        }
    }

    private static int IndexOfOggPage(List<byte> data)
    {
        for (var i = 0; i <= data.Count - 4; i++)
            if (data[i] == (byte)'O' && data[i + 1] == (byte)'g' && data[i + 2] == (byte)'g' && data[i + 3] == (byte)'S')
                return i;
        return -1;
    }

    private void TrimPendingOggInput()
    {
        const int maxBuffered = 256 * 1024;
        if (_pendingOggInput.Count <= maxBuffered) return;
        _pendingOggInput.RemoveRange(0, _pendingOggInput.Count - maxBuffered);
    }

    private async Task ReadDecodedPcmStdoutAsync()
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var read = await _process.StandardOutput.BaseStream.ReadAsync(buffer.AsMemory(0, buffer.Length), _cts.Token).ConfigureAwait(false);
                if (read <= 0) break;
                var decoded = Interlocked.Add(ref _decodedPcmBytes, read);
                var peak = CalculatePcm16Peak(buffer, read);
                if (!_firstDecodedPcmLogged)
                {
                    _firstDecodedPcmLogged = true;
                    Program.Log($"FFmpeg Opus audio playback decoded first PCM chunk: {read:n0} bytes; pcmPeak={peak:0.000}.");
                }
                else if (decoded % (256 * 1024) < read)
                {
                    Program.Log($"FFmpeg Opus audio playback decoded {decoded / 1024:n0} KB PCM total; pcmPeak={peak:0.000}.");
                }

                var waveBuffer = _waveBuffer;
                if (waveBuffer is null) continue;

                var muted = false;
                var volume = 1f;
                lock (_playbackStateGate)
                {
                    muted = _playbackMuted;
                    volume = _playbackVolume;
                }

                if (muted || volume <= 0.001f)
                    continue;

                if (volume < 0.999f)
                    ApplyPcm16VolumeInPlace(buffer, read, volume);

                TrimPlaybackDriftIfNeeded(waveBuffer);
                waveBuffer.AddSamples(buffer, 0, read);
                EnsureWaveOutStarted(waveBuffer);

                var now = DateTime.UtcNow;
                if (!_firstPcmQueuedLogged)
                {
                    _firstPcmQueuedLogged = true;
                    Program.Log($"Direct Stream receiver queued first PCM audio to output: chunk={read:n0} bytes; buffered={waveBuffer.BufferedBytes:n0} bytes; playbackState={_waveOut?.PlaybackState}.");
                }
                else if ((now - _lastPlaybackStatusUtc).TotalSeconds >= 3)
                {
                    _lastPlaybackStatusUtc = now;
                    Program.Log($"Direct Stream receiver audio output status: decoded={decoded / 1024:n0} KB; buffered={waveBuffer.BufferedBytes:n0} bytes; playbackState={_waveOut?.PlaybackState}; pcmPeak={peak:0.000}.");
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("FFmpeg Opus audio decoded PCM stdout failed: " + Program.Flatten(ex));
        }
    }


    private void TrimPlaybackDriftIfNeeded(BufferedWaveProvider waveBuffer)
    {
        var bytesPerSecond = Math.Max(1, waveBuffer.WaveFormat.AverageBytesPerSecond);
        var maxBytes = Math.Max(1, bytesPerSecond * _playbackMaxBufferedMs / 1000);
        var bufferedBytes = waveBuffer.BufferedBytes;
        if (bufferedBytes <= maxBytes) return;

        waveBuffer.ClearBuffer();
    }

    private void EnsureWaveOutStarted(BufferedWaveProvider waveBuffer)
    {
        if (_playbackStarted || _waveOut is null) return;
        var bytesPerSecond = Math.Max(1, waveBuffer.WaveFormat.AverageBytesPerSecond);
        var targetBytes = Math.Max(1, bytesPerSecond * _playbackPrebufferMs / 1000);
        if (waveBuffer.BufferedBytes < targetBytes) return;

        try
        {
            _waveOut.Play();
            _playbackStarted = true;
            Program.Log($"Direct Stream receiver audio output started after {_playbackPrebufferMs}ms sync prebuffer; buffered={waveBuffer.BufferedBytes:n0} bytes.");
        }
        catch (Exception ex)
        {
            _broken = true;
            Program.Log("Direct Stream receiver audio output could not start after sync prebuffer: " + Program.Flatten(ex));
        }
    }

    private static void ApplyPcm16VolumeInPlace(byte[] buffer, int length, float volume)
    {
        var gain = Math.Clamp(volume, 0f, 1f);
        for (var i = 0; i + 1 < length; i += 2)
        {
            var sample = (short)(buffer[i] | (buffer[i + 1] << 8));
            var scaled = (int)MathF.Round(sample * gain);
            scaled = Math.Clamp(scaled, short.MinValue, short.MaxValue);
            unchecked
            {
                buffer[i] = (byte)scaled;
                buffer[i + 1] = (byte)(scaled >> 8);
            }
        }
    }

    private static double CalculatePcm16Peak(byte[] buffer, int length)
    {
        var samples = length / 2;
        if (samples <= 0) return 0;

        var max = 0;
        for (var i = 0; i + 1 < length; i += 2)
        {
            var sample = (short)(buffer[i] | (buffer[i + 1] << 8));
            var abs = sample == short.MinValue ? 32768 : Math.Abs(sample);
            if (abs > max) max = abs;
        }

        return max / 32768.0;
    }

    private async Task ReadStderrAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested && !_process.HasExited)
            {
                var line = await _process.StandardError.ReadLineAsync(_cts.Token).ConfigureAwait(false);
                if (line is null) break;
                if (!string.IsNullOrWhiteSpace(line)) Program.Log("ffmpeg-audio-play: " + line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Program.Log("FFmpeg Opus audio playback stderr failed: " + Program.Flatten(ex));
        }
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        try { _process.StandardInput.Close(); } catch { }
        try { if (!_process.HasExited) _process.Kill(entireProcessTree: true); } catch { }
        try { _process.Dispose(); } catch { }
        try { _waveOut?.Stop(); } catch { }
        try { _waveOut?.Dispose(); } catch { }
        try { _cts.Dispose(); } catch { }
    }
}
