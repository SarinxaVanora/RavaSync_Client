using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace RavaCast.Media.Native;

/// <summary>
/// Thin NativeAOT ABI shim loaded by RavaCast.Renderer.exe.
///
/// The actual Direct Stream media work lives in RavaCast.Media.BridgeHost.exe so the renderer can P/Invoke a tiny,
/// stable C ABI while transport/codec work runs out-of-process.
///
/// Direct Stream v2 keeps this NativeAOT shim tiny. The actual media engine lives in BridgeHost and uses
/// libdatachannel for WebRTC transport with FFmpeg/libav for H.264 video plus Opus audio over an RTP media track as the codec path.
/// </summary>
public static unsafe class NativeExports
{
    private const int Ok = 0;
    private const int NotInitialised = -1;
    private const int BridgeHostMissing = -20;
    private const int BridgeHostUnavailable = -21;
    private const int InvalidArgument = -22;

    private const string HostExeName = "RavaCast.Media.BridgeHost.exe";
    private static readonly object Gate = new();
    private static readonly HashSet<string> Peers = new(StringComparer.Ordinal);
    private static delegate* unmanaged[Stdcall]<char*, char*, int, int, int, void> _statusCallback;
    private static delegate* unmanaged[Stdcall]<char*, char*, char*, void> _signalCallback;
    private static delegate* unmanaged[Stdcall]<nint, int, int, void> _textureCallback;
    private static bool _initialised;
    private static bool _publisherActive;
    private static bool _receiverActive;
    private static string _lastError = string.Empty;
    private static BridgeHostClient? _host;

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_Initialise", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int Initialise(delegate* unmanaged[Stdcall]<char*, char*, int, int, int, void> statusCallback, delegate* unmanaged[Stdcall]<char*, char*, char*, void> signalCallback, delegate* unmanaged[Stdcall]<nint, int, int, void> textureCallback)
    {
        lock (Gate)
        {
            _statusCallback = statusCallback;
            _signalCallback = signalCallback;
            _textureCallback = textureCallback;
            _publisherActive = false;
            _receiverActive = false;
            Peers.Clear();
            _initialised = true;
        }

        try
        {
            EnsureHost();
            SendStatus("RavaCast Direct Stream bridge ready", "Native shim and BridgeHost are connected. Direct Stream v2 uses libdatachannel transport with FFmpeg H.264 live video and Opus audio over an RTP media track.");
            return Ok;
        }
        catch (FileNotFoundException ex)
        {
            _lastError = ex.Message;
            SendStatus("RavaCast Direct Stream bridge host missing", _lastError);
            return BridgeHostMissing;
        }
        catch (Exception ex)
        {
            _lastError = ex.ToString();
            SendStatus("RavaCast Direct Stream bridge failed to initialise", _lastError);
            return BridgeHostUnavailable;
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_StartPublisher", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int StartPublisher(nint sharedTextureHandle, int sourceWidth, int sourceHeight, int targetWidth, int targetHeight, int fps, int videoBitrateKbps, int audioBitrateKbps, int audioSourceProcessId, char* castId, char* stunServersJson)
    {
        if (!_initialised) return NotInitialised;
        if (sharedTextureHandle == 0 || sourceWidth <= 0 || sourceHeight <= 0 || targetWidth <= 0 || targetHeight <= 0) return InvalidArgument;

        var rc = SendCommandNoThrow(new Dictionary<string, object?>
        {
            ["op"] = "startPublisher",
            ["castId"] = PtrToString(castId),
            ["sharedTextureHandle"] = (long)sharedTextureHandle,
            ["sourceWidth"] = sourceWidth,
            ["sourceHeight"] = sourceHeight,
            ["targetWidth"] = targetWidth,
            ["targetHeight"] = targetHeight,
            ["fps"] = fps,
            ["videoBitrateKbps"] = videoBitrateKbps,
            ["audioBitrateKbps"] = audioBitrateKbps,
            ["audioSourceProcessId"] = Math.Max(0, audioSourceProcessId),
            ["stunServersJson"] = PtrToString(stunServersJson)
        });

        if (rc == Ok) SendStatus("Starting host video", "Direct Stream v2 host request sent to BridgeHost.");
        return rc;
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_StopPublisher", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int StopPublisher(char* reason)
    {
        lock (Gate)
        {
            _publisherActive = false;
            Peers.Clear();
        }
        return SendCommand(new Dictionary<string, object?> { ["op"] = "stopPublisher", ["reason"] = PtrToString(reason) }, "Direct Stream publisher stopped");
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_StartReceiver", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int StartReceiver(char* castId, char* hostSessionId, char* viewerSessionId, int targetWidth, int targetHeight, int fps, int videoBitrateKbps, int audioBitrateKbps, char* stunServersJson)
    {
        if (!_initialised) return NotInitialised;

        var rc = SendCommandNoThrow(new Dictionary<string, object?>
        {
            ["op"] = "startReceiver",
            ["castId"] = PtrToString(castId),
            ["hostSessionId"] = PtrToString(hostSessionId),
            ["viewerSessionId"] = PtrToString(viewerSessionId),
            ["targetWidth"] = targetWidth,
            ["targetHeight"] = targetHeight,
            ["fps"] = fps,
            ["videoBitrateKbps"] = videoBitrateKbps,
            ["audioBitrateKbps"] = audioBitrateKbps,
            ["stunServersJson"] = PtrToString(stunServersJson)
        });

        if (rc == Ok)
        {
            lock (Gate) _receiverActive = true;
            SendStatus("Connecting to host video", "Direct Stream v2 viewer request sent to BridgeHost.");
        }
        return rc;
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_StopReceiver", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int StopReceiver(char* reason)
    {
        lock (Gate) _receiverActive = false;
        return SendCommand(new Dictionary<string, object?> { ["op"] = "stopReceiver", ["reason"] = PtrToString(reason) }, "Direct Stream receiver stopped");
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_AddPeer", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int AddPeer(char* peerId)
    {
        if (!_initialised) return NotInitialised;
        var peer = PtrToString(peerId);
        if (string.IsNullOrWhiteSpace(peer)) return InvalidArgument;
        lock (Gate) Peers.Add(peer);
        return SendCommand(new Dictionary<string, object?> { ["op"] = "addPeer", ["peerId"] = peer }, "Direct Stream peer add queued");
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_RemovePeer", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int RemovePeer(char* peerId)
    {
        var peer = PtrToString(peerId);
        lock (Gate) if (!string.IsNullOrWhiteSpace(peer)) Peers.Remove(peer);
        return SendCommand(new Dictionary<string, object?> { ["op"] = "removePeer", ["peerId"] = peer }, "Direct Stream peer remove queued");
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_HandleSignal", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int HandleSignal(char* peerId, char* signalType, char* payloadJson)
    {
        if (!_initialised) return NotInitialised;
        var peer = PtrToString(peerId);
        var type = PtrToString(signalType);
        if (string.IsNullOrWhiteSpace(peer) || string.IsNullOrWhiteSpace(type)) return InvalidArgument;
        return SendCommand(new Dictionary<string, object?> { ["op"] = "signal", ["peerId"] = peer, ["signalType"] = type, ["payloadJson"] = PtrToString(payloadJson) }, "Direct Stream signal queued");
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_SetAudio", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int SetAudio(int muted, float volume)
    {
        if (!_initialised) return NotInitialised;
        return SendCommandNoThrow(new Dictionary<string, object?>
        {
            ["op"] = "setAudio",
            ["muted"] = muted != 0,
            ["volume"] = Math.Clamp(volume, 0f, 1f)
        });
    }

    [UnmanagedCallersOnly(EntryPoint = "RavaCastMedia_Shutdown", CallConvs = new[] { typeof(CallConvStdcall) })]
    public static int Shutdown(char* reason)
    {
        lock (Gate)
        {
            _publisherActive = false;
            _receiverActive = false;
            Peers.Clear();
        }

        try { _host?.Send(new Dictionary<string, object?> { ["op"] = "shutdown", ["reason"] = PtrToString(reason) }); } catch { }
        try { _host?.Dispose(); } catch { }
        _host = null;
        SendStatus("Direct Stream bridge shut down", PtrToString(reason));
        return Ok;
    }

    private static int SendCommandNoThrow(Dictionary<string, object?> command)
    {
        try
        {
            EnsureHost();
            _host!.Send(command);
            return Ok;
        }
        catch (FileNotFoundException ex)
        {
            _lastError = ex.Message;
            SendStatus("RavaCast Direct Stream bridge host missing", _lastError);
            return BridgeHostMissing;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            try
            {
                if (_host is not null)
                    _lastError = _host.BuildFailureDetail(_lastError);
            }
            catch { }
            try { _host?.Dispose(); } catch { }
            _host = null;
            SendStatus("RavaCast Direct Stream bridge host unavailable", _lastError);
            return BridgeHostUnavailable;
        }
    }

    private static int SendCommand(Dictionary<string, object?> command, string successText)
    {
        try
        {
            EnsureHost();
            _host!.Send(command);
            SendStatus(successText, null);
            return Ok;
        }
        catch (FileNotFoundException ex)
        {
            _lastError = ex.Message;
            SendStatus("RavaCast Direct Stream bridge host missing", _lastError);
            return BridgeHostMissing;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            try
            {
                if (_host is not null)
                    _lastError = _host.BuildFailureDetail(_lastError);
            }
            catch { }
            try { _host?.Dispose(); } catch { }
            _host = null;
            SendStatus("RavaCast Direct Stream bridge host unavailable", _lastError);
            return BridgeHostUnavailable;
        }
    }

    private static void EnsureHost()
    {
        if (_host?.IsConnected == true) return;
        try { _host?.Dispose(); } catch { }
        _host = BridgeHostClient.Start(HostExePath, OnHostEvent);
    }

    private static string HostExePath => Path.Combine(AppContext.BaseDirectory, HostExeName);

    private static void OnHostEvent(JsonElement root)
    {
        var op = root.TryGetProperty("op", out var opProp) ? opProp.GetString() ?? string.Empty : string.Empty;
        switch (op)
        {
            case "status":
                lock (Gate)
                {
                    if (root.TryGetProperty("publisherActive", out var pub)) _publisherActive = pub.GetBoolean();
                    if (root.TryGetProperty("receiverActive", out var recv)) _receiverActive = recv.GetBoolean();
                }
                SendStatus(ReadString(root, "text", "Direct Stream bridge status"), ReadString(root, "detail", string.Empty), ReadInt(root, "connectedPeers", -1));
                break;
            case "error":
                _lastError = ReadString(root, "message", string.Empty);
                SendStatus("Direct Stream bridge error", _lastError, ReadInt(root, "connectedPeers", -1));
                break;
            case "signal":
                SendSignal(ReadString(root, "peerId", string.Empty), ReadString(root, "signalType", string.Empty), ReadString(root, "payloadJson", string.Empty));
                break;
            case "texture":
                SendTexture((nint)ReadLong(root, "handle", 0), ReadInt(root, "width", 0), ReadInt(root, "height", 0));
                break;
        }
    }

    private static void SendStatus(string text, string? detail, int connectedPeersOverride = -1)
    {
        delegate* unmanaged[Stdcall]<char*, char*, int, int, int, void> callback;
        bool publisher;
        bool receiver;
        int peers;
        lock (Gate)
        {
            callback = _statusCallback;
            publisher = _publisherActive;
            receiver = _receiverActive;
            peers = connectedPeersOverride >= 0 ? connectedPeersOverride : Peers.Count;
        }

        if (callback == null) return;
        var textPtr = StringToHGlobalUni(text);
        var detailPtr = StringToHGlobalUni(detail ?? string.Empty);
        try { callback((char*)textPtr, (char*)detailPtr, publisher ? 1 : 0, receiver ? 1 : 0, peers); }
        finally
        {
            Marshal.FreeHGlobal(textPtr);
            Marshal.FreeHGlobal(detailPtr);
        }
    }

    private static void SendSignal(string peerId, string signalType, string payloadJson)
    {
        var callback = _signalCallback;
        if (callback == null || string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType)) return;
        var peerPtr = StringToHGlobalUni(peerId);
        var typePtr = StringToHGlobalUni(signalType);
        var payloadPtr = StringToHGlobalUni(payloadJson ?? string.Empty);
        try { callback((char*)peerPtr, (char*)typePtr, (char*)payloadPtr); }
        finally
        {
            Marshal.FreeHGlobal(peerPtr);
            Marshal.FreeHGlobal(typePtr);
            Marshal.FreeHGlobal(payloadPtr);
        }
    }

    private static void SendTexture(nint handle, int width, int height)
    {
        var callback = _textureCallback;
        if (callback == null || handle == 0 || width <= 0 || height <= 0) return;
        callback(handle, width, height);
    }

    private static string PtrToString(char* value) => value == null ? string.Empty : Marshal.PtrToStringUni((nint)value) ?? string.Empty;
    private static nint StringToHGlobalUni(string value) => Marshal.StringToHGlobalUni(value ?? string.Empty);
    private static string ReadString(JsonElement root, string name, string fallback) => root.TryGetProperty(name, out var value) ? value.GetString() ?? fallback : fallback;
    private static int ReadInt(JsonElement root, string name, int fallback) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : fallback;
    private static long ReadLong(JsonElement root, string name, long fallback) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetInt64() : fallback;

    private sealed class BridgeHostClient : IDisposable
    {
        private readonly NamedPipeServerStream _pipe;
        private StreamReader? _reader;
        private StreamWriter? _writer;
        private readonly Process _process;
        private Thread? _readerThread;
        private readonly Action<JsonElement> _eventHandler;
        private readonly object _outputGate = new();
        private readonly object _writerGate = new();
        private string _lastStdOut = string.Empty;
        private string _lastStdErr = string.Empty;
        private volatile bool _disposed;

        private BridgeHostClient(NamedPipeServerStream pipe, Process process, Action<JsonElement> eventHandler)
        {
            _pipe = pipe;
            _process = process;
            _eventHandler = eventHandler;
        }

        private void CompleteConnection()
        {
            if (!_pipe.IsConnected)
                throw new InvalidOperationException("RavaCast.Media.BridgeHost.exe pipe connection completed but the server pipe is not connected.");

            // Do not create StreamWriter/StreamReader while the NamedPipeServerStream is still unconnected.
            // On some machines, setting StreamWriter.AutoFlush over an unconnected named pipe flushes immediately
            // and throws "Pipe hasn't been connected yet", which collapsed into bridge return code -21.
            _reader = new StreamReader(_pipe);
            _writer = new StreamWriter(_pipe) { AutoFlush = true };
            _readerThread = new Thread(ReadLoop) { IsBackground = true, Name = "RavaCast Direct Stream bridge reader" };
        }

        public bool IsConnected
        {
            get
            {
                try { return !_disposed && _pipe.IsConnected && !_process.HasExited; }
                catch { return false; }
            }
        }

        public static BridgeHostClient Start(string exePath, Action<JsonElement> eventHandler)
        {
            if (!File.Exists(exePath))
                throw new FileNotFoundException($"{HostExeName} was not found beside RavaCast.Renderer.exe: {exePath}");

            var pipeName = "RavaCastMediaBridge_" + Environment.ProcessId + "_" + Guid.NewGuid().ToString("N");
            var pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            var psi = new ProcessStartInfo
            {
                FileName = exePath,
                Arguments = "--pipe " + pipeName,
                UseShellExecute = false,
                CreateNoWindow = true,
                WorkingDirectory = AppContext.BaseDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            var process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start RavaCast.Media.BridgeHost.exe");
            try { process.PriorityClass = ProcessPriorityClass.BelowNormal; } catch { }
            var client = new BridgeHostClient(pipe, process, eventHandler);
            process.OutputDataReceived += (_, e) => client.RememberStdOut(e.Data);
            process.ErrorDataReceived += (_, e) => client.RememberStdErr(e.Data);
            try
            {
                process.EnableRaisingEvents = true;
                process.Exited += (_, _) => client.ReportBridgeHostClosed("RavaCast.Media.BridgeHost.exe exited unexpectedly.");
            }
            catch { }
            try { process.BeginOutputReadLine(); } catch { }
            try { process.BeginErrorReadLine(); } catch { }
            var connected = pipe.WaitForConnectionAsync();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);
            while (!connected.IsCompleted && DateTime.UtcNow < deadline)
            {
                if (process.HasExited)
                {
                    var detail = client.BuildFailureDetail("RavaCast.Media.BridgeHost.exe exited before connecting its IPC pipe.");
                    client.Dispose();
                    throw new InvalidOperationException(detail);
                }
                Thread.Sleep(50);
            }
            if (!connected.IsCompleted)
            {
                var detail = client.BuildFailureDetail("Timed out waiting for RavaCast.Media.BridgeHost.exe to connect its IPC pipe.");
                client.Dispose();
                throw new TimeoutException(detail);
            }
            try { connected.GetAwaiter().GetResult(); }
            catch (Exception ex)
            {
                var detail = client.BuildFailureDetail("RavaCast.Media.BridgeHost.exe IPC connection failed: " + ex.Message);
                client.Dispose();
                throw new InvalidOperationException(detail, ex);
            }
            client.CompleteConnection();
            client.StartReader();
            return client;
        }


        private void StartReader()
        {
            if (_readerThread is null) throw new InvalidOperationException("BridgeHost IPC reader was not initialised.");
            _readerThread.Start();
        }

        private void RememberStdOut(string? line)
        {
            if (string.IsNullOrWhiteSpace(line)) return;
            lock (_outputGate) _lastStdOut = line.Length > 1000 ? line[^1000..] : line;
        }

        private void RememberStdErr(string? line)
        {
            if (string.IsNullOrWhiteSpace(line)) return;
            lock (_outputGate) _lastStdErr = line.Length > 1000 ? line[^1000..] : line;
        }

        public string BuildFailureDetail(string baseMessage)
        {
            try
            {
                var exit = _process.HasExited ? $" BridgeHost exit code={_process.ExitCode}." : " BridgeHost process is still running.";
                string stdout;
                string stderr;
                lock (_outputGate)
                {
                    stdout = _lastStdOut;
                    stderr = _lastStdErr;
                }
                var output = string.Empty;
                if (!string.IsNullOrWhiteSpace(stderr)) output += " stderr=" + stderr;
                if (!string.IsNullOrWhiteSpace(stdout)) output += " stdout=" + stdout;
                return baseMessage + exit + output + " Log=" + Path.Combine(AppContext.BaseDirectory, "RavaCast.Media.BridgeHost.log");
            }
            catch
            {
                return baseMessage + " Log=" + Path.Combine(AppContext.BaseDirectory, "RavaCast.Media.BridgeHost.log");
            }
        }

        public void Send(Dictionary<string, object?> command)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(BridgeHostClient));
            if (_writer is null || !_pipe.IsConnected)
                throw new InvalidOperationException("RavaCast.Media.BridgeHost.exe IPC pipe is not connected yet.");

            // RavaCast.Media.Native is published with NativeAOT. Reflection-based System.Text.Json serialization
            // is disabled there, so do not call JsonSerializer.Serialize(Dictionary<string, object?>).
            // The bridge command contract is intentionally simple, so write it explicitly and keep the native shim
            // trim/AOT-safe. This fixes the viewer-side "reflection-based serialization has been disabled" -21 path.
            var line = SerializeCommand(command);

            lock (_writerGate)
            {
                _writer.WriteLine(line);
            }
        }

        private static string SerializeCommand(Dictionary<string, object?> command)
        {
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();

                foreach (var pair in command)
                {
                    writer.WritePropertyName(pair.Key);

                    switch (pair.Value)
                    {
                        case null:
                            writer.WriteNullValue();
                            break;
                        case string text:
                            writer.WriteStringValue(text);
                            break;
                        case bool flag:
                            writer.WriteBooleanValue(flag);
                            break;
                        case int number:
                            writer.WriteNumberValue(number);
                            break;
                        case long number:
                            writer.WriteNumberValue(number);
                            break;
                        case uint number:
                            writer.WriteNumberValue(number);
                            break;
                        case ulong number when number <= long.MaxValue:
                            writer.WriteNumberValue((long)number);
                            break;
                        case float number:
                            writer.WriteNumberValue(number);
                            break;
                        case double number:
                            writer.WriteNumberValue(number);
                            break;
                        case decimal number:
                            writer.WriteNumberValue(number);
                            break;
                        default:
                            writer.WriteStringValue(pair.Value.ToString() ?? string.Empty);
                            break;
                    }
                }

                writer.WriteEndObject();
            }

            return Encoding.UTF8.GetString(stream.ToArray());
        }

        private void ReadLoop()
        {
            while (!_disposed)
            {
                string? line;
                if (_reader is null) break;
                try { line = _reader.ReadLine(); }
                catch
                {
                    ReportBridgeHostClosed("RavaCast.Media.BridgeHost.exe IPC pipe failed.");
                    break;
                }

                if (line is null)
                {
                    ReportBridgeHostClosed("RavaCast.Media.BridgeHost.exe closed its IPC pipe.");
                    break;
                }

                if (string.IsNullOrWhiteSpace(line)) continue;
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    _eventHandler(doc.RootElement.Clone());
                }
                catch { }
            }
        }

        public void ReportBridgeHostClosed(string reason)
        {
            if (_disposed) return;
            _disposed = true;
            var detail = BuildFailureDetail(reason);
            try
            {
                using var doc = JsonDocument.Parse("{\"op\":\"status\",\"text\":\"Direct Stream v2 bridge closed\",\"detail\":\"" + EscapeJson(detail) + "\",\"publisherActive\":false,\"receiverActive\":false,\"nativeMediaAvailable\":true,\"connectedPeers\":0}");
                _eventHandler(doc.RootElement.Clone());
            }
            catch { }
        }

        private static string EscapeJson(string value)
            => (value ?? string.Empty).Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r").Replace("\n", "\\n");

        public void Dispose()
        {
            _disposed = true;
            try { _writer?.Dispose(); } catch { }
            try { _reader?.Dispose(); } catch { }
            try { _pipe.Dispose(); } catch { }
            try
            {
                if (!_process.HasExited) _process.Kill(entireProcessTree: true);
            }
            catch { }
            try { _process.Dispose(); } catch { }
        }
    }
}
