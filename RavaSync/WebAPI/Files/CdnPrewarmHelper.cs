using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using RavaSync.API.Routes;

namespace RavaSync.WebAPI.Files;

internal static class CdnPrewarmHelper
{
    private static readonly HttpClient HttpClient = new();
    private static readonly ConcurrentDictionary<string, byte> Warmed = new(StringComparer.OrdinalIgnoreCase);
    private static readonly SemaphoreSlim SaveGate = new(1, 1);

    private static string? _stateFilePath;
    private static bool _initialized;

    private static long _lastSaveTicks;
    private const int SaveMinIntervalSeconds = 30;

    public static void Initialize(string configRootPath)
    {
        if (_initialized) return;
        _initialized = true;

        _stateFilePath = Path.Combine(configRootPath, "cdn_warmed.txt");

        HttpClient.DefaultRequestHeaders.UserAgent.Clear();
        HttpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("RavaSyncClient", "1.0"));
        HttpClient.Timeout = TimeSpan.FromSeconds(10);

        if (!File.Exists(_stateFilePath))
            return;

        foreach (var line in File.ReadLines(_stateFilePath))
        {
            var hash = line.Trim();
            if (!string.IsNullOrEmpty(hash))
                Warmed.TryAdd(hash.ToUpperInvariant(), 0);
        }
    }

    /// <summary>
    /// Prewarm the given hashes on the CDN by issuing tiny range GETs (bytes=0-0)
    /// to /cdn/{hash}. Hashes already warmed are skipped.
    /// Safe to call often; work is de-duped and failures ignored.
    /// </summary>
    public static async Task PrewarmAsync(Uri? filesCdnUri, IEnumerable<string> hashes, CancellationToken ct)
    {
        if (!_initialized) return;
        if (filesCdnUri is null) return;
        if (_stateFilePath is null) return;

        var pending = hashes
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Select(h => h.ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(h => Warmed.TryAdd(h, 0))
            .ToArray();

        if (pending.Length == 0) return;

        const int maxConcurrency = 8;
        using var throttler = new SemaphoreSlim(maxConcurrency, maxConcurrency);

        var tasks = new Task[pending.Length];
        for (int i = 0; i < pending.Length; i++)
        {
            var hash = pending[i];
            tasks[i] = PrewarmOneAsync(filesCdnUri, hash, throttler, ct);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        // Avoid hammering disk if this is called frequently.
        await SaveStateMaybeAsync(ct).ConfigureAwait(false);
    }

    private static async Task PrewarmOneAsync(Uri filesCdnUri, string hash, SemaphoreSlim throttler, CancellationToken ct)
    {
        await throttler.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var url = MareFiles.CdnGetFullPath(filesCdnUri, hash);

            using var req = new HttpRequestMessage(HttpMethod.Get, url);
            req.Headers.Range = new RangeHeaderValue(0, 0);

            using var resp = await HttpClient.SendAsync(
                req,
                HttpCompletionOption.ResponseHeadersRead,
                ct).ConfigureAwait(false);

            if (resp.IsSuccessStatusCode || resp.StatusCode == HttpStatusCode.PartialContent)
                return;

            Warmed.TryRemove(hash, out _);
        }
        catch
        {
            // Allow retry later.
            Warmed.TryRemove(hash, out _);
        }
        finally
        {
            throttler.Release();
        }
    }

    private static async Task SaveStateMaybeAsync(CancellationToken ct)
    {
        var nowTicks = DateTime.UtcNow.Ticks;
        var lastTicks = Interlocked.Read(ref _lastSaveTicks);

        if (lastTicks != 0)
        {
            var elapsed = new TimeSpan(nowTicks - lastTicks);
            if (elapsed.TotalSeconds < SaveMinIntervalSeconds)
                return;
        }
        if (Interlocked.CompareExchange(ref _lastSaveTicks, nowTicks, lastTicks) != lastTicks)
            return;

        await SaveStateAsync(ct).ConfigureAwait(false);
    }

    private static async Task SaveStateAsync(CancellationToken ct)
    {
        if (_stateFilePath is null) return;

        await SaveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var dir = Path.GetDirectoryName(_stateFilePath);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);

            var all = Warmed.Keys
                .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            await File.WriteAllLinesAsync(_stateFilePath, all, ct).ConfigureAwait(false);
        }
        finally
        {
            SaveGate.Release();
        }
    }
}
