using System.IO.Pipes;
using System.Text.Json;

namespace RavaCast.Media.BridgeHost;

internal static class Program
{
    private static readonly object LogGate = new();
    public static string LogPath => Path.Combine(AppContext.BaseDirectory, "RavaCast.Media.BridgeHost.log");

    public static async Task<int> Main(string[] args)
    {
        InstallCrashLogging();

        try
        {
            var pipeName = ReadArg(args, "pipe");
            if (string.IsNullOrWhiteSpace(pipeName))
            {
                Log("BridgeHost started without --pipe.");
                return 2;
            }

            using var pipe = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
            await pipe.ConnectAsync(8000).ConfigureAwait(false);
            using var transport = new BridgeTransport(pipe);
            using var engine = new DirectStreamV2Engine(transport);

            await transport.SendStatusAsync(
                "Direct Stream bridge ready",
                "Direct Stream v2 is ready: libdatachannel transport plus FFmpeg H.264 live video and Opus audio over an RTP media track are wired.",
                false,
                false,
                true,
                0).ConfigureAwait(false);

            await foreach (var root in transport.ReadCommandsAsync())
                await engine.HandleCommandAsync(root).ConfigureAwait(false);

            return 0;
        }
        catch (Exception ex)
        {
            Log("BridgeHost fatal startup failure: " + Flatten(ex));
            return 1;
        }
    }

    private static void InstallCrashLogging()
    {
        AppDomain.CurrentDomain.UnhandledException += (_, e) => Log("Unhandled exception: " + Flatten(e.ExceptionObject as Exception));
        TaskScheduler.UnobservedTaskException += (_, e) =>
        {
            Log("Unobserved task exception: " + Flatten(e.Exception));
            e.SetObserved();
        };
    }

    internal static void Log(string message)
    {
        try
        {
            if (!IsErrorLogMessage(message)) return;
            lock (LogGate)
                File.AppendAllText(LogPath, DateTimeOffset.Now.ToString("O") + " | " + message + Environment.NewLine);
        }
        catch { }
    }

    private static bool IsErrorLogMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message)) return false;
        return message.Contains("error", StringComparison.OrdinalIgnoreCase)
            || message.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || message.Contains("failure", StringComparison.OrdinalIgnoreCase)
            || message.Contains("exception", StringComparison.OrdinalIgnoreCase)
            || message.Contains("fatal", StringComparison.OrdinalIgnoreCase)
            || message.Contains("crash", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unhandled", StringComparison.OrdinalIgnoreCase)
            || message.Contains("could not", StringComparison.OrdinalIgnoreCase)
            || message.Contains("cannot", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("missing", StringComparison.OrdinalIgnoreCase)
            || message.Contains("rejected", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unexpected", StringComparison.OrdinalIgnoreCase)
            || message.Contains("broken", StringComparison.OrdinalIgnoreCase);
    }

    internal static string Flatten(Exception? ex)
    {
        if (ex is null) return "Unknown error";
        return ex.GetType().Name + ": " + ex.Message + (ex.InnerException is null ? string.Empty : " -> " + Flatten(ex.InnerException));
    }

    private static string ReadArg(string[] args, string name)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (!string.Equals(args[i], "--" + name, StringComparison.OrdinalIgnoreCase)) continue;
            return i + 1 < args.Length ? args[i + 1] : string.Empty;
        }

        return string.Empty;
    }
}

internal sealed class BridgeTransport : IDisposable
{
    private readonly NamedPipeClientStream _pipe;
    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    public BridgeTransport(NamedPipeClientStream pipe)
    {
        _pipe = pipe;
        _reader = new StreamReader(pipe);
        _writer = new StreamWriter(pipe) { AutoFlush = true };
    }

    public async IAsyncEnumerable<JsonElement> ReadCommandsAsync()
    {
        while (_pipe.IsConnected)
        {
            string? line;
            try { line = await _reader.ReadLineAsync().ConfigureAwait(false); }
            catch { yield break; }

            if (string.IsNullOrWhiteSpace(line)) continue;

            JsonDocument? doc = null;
            try { doc = JsonDocument.Parse(line); }
            catch { continue; }

            using (doc)
                yield return doc.RootElement.Clone();
        }
    }

    public Task SendStatusAsync(string text, string? detail, bool publisherActive, bool receiverActive, bool nativeMediaAvailable, int connectedViewers)
        => SendAsync(new { op = "status", text, detail = detail ?? string.Empty, publisherActive, receiverActive, nativeMediaAvailable, connectedPeers = connectedViewers });

    public Task SendErrorAsync(string message, bool publisherActive, bool receiverActive, bool nativeMediaAvailable, int connectedViewers)
        => SendAsync(new { op = "error", message, detail = string.Empty, publisherActive, receiverActive, nativeMediaAvailable, connectedPeers = connectedViewers });

    public Task SendErrorAsync(string message, string? detail, bool publisherActive, bool receiverActive, bool nativeMediaAvailable, int connectedViewers)
        => SendAsync(new { op = "error", message, detail = detail ?? string.Empty, publisherActive, receiverActive, nativeMediaAvailable, connectedPeers = connectedViewers });

    public Task SendSignalAsync(string peerId, string signalType, string payloadJson)
        => SendAsync(new { op = "signal", peerId, signalType, payloadJson = payloadJson ?? string.Empty });

    public Task SendTextureAsync(nint sharedHandle, int width, int height)
        => SendAsync(new { op = "texture", handle = sharedHandle.ToInt64(), width, height });

    private async Task SendAsync(object payload)
    {
        var json = JsonSerializer.Serialize(payload);
        await _writeGate.WaitAsync().ConfigureAwait(false);
        try { await _writer.WriteLineAsync(json).ConfigureAwait(false); }
        finally { _writeGate.Release(); }
    }

    public void Dispose()
    {
        _writeGate.Dispose();
        try { _writer.Dispose(); } catch { }
        try { _reader.Dispose(); } catch { }
        try { _pipe.Dispose(); } catch { }
    }
}
