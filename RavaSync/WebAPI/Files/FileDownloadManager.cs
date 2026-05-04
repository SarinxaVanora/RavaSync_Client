using Dalamud.Utility;
using K4os.Compression.LZ4.Legacy;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Dto.Files;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.Interop.GameModel;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace RavaSync.WebAPI.Files;

public partial class FileDownloadManager : DisposableMediatorSubscriberBase
{
    private Dictionary<string, FileDownloadStatus> _downloadStatus;
    private readonly FileCompactor _fileCompactor;
    private readonly FileCacheManager _fileDbManager;
    private readonly FileTransferOrchestrator _orchestrator;
    private readonly HttpClient _cdnFastClient;
    private readonly List<ThrottledStream> _activeDownloadStreams;
    private static readonly ArrayPool<byte> _downloadBufferPool = ArrayPool<byte>.Shared;
    private const int DownloadBufferSize = 1024 * 1024;
    const int Lz4BlockSize = 4 * 1024 * 1024;
    private const int CdnBufferSize = 4 * 1024 * 1024;
    private readonly MareConfigService _mareConfigService;
    private const int MaxCdnAttemptsPerFile = 3;
    private static readonly TimeSpan CdnAttemptTimeout = TimeSpan.FromSeconds(60);
    private const long TinyFilePayloadBytes = 2L * 1024 * 1024;
    private const int TinyFileParallelFloor = 16;
    private const int TinyFileParallelMax = 48;
    private const long CdnInMemorySpoolMaxPayloadBytes = 4L * 1024 * 1024;
    private const long CdnInMemorySpoolMaxGroupBytes = 1024L * 1024 * 1024;


    private const int AutoCdnMinParallel = 4;
    private const int AutoCdnMaxParallel = 32;

    private const long AutoCdnSlowBpsThreshold = 768 * 1024;

    private int _autoCdnParallel = 12;

    private static readonly SemaphoreSlim CdnDecodeConcurrencySemaphore = new(Math.Max(2, Math.Min(GetCdnDecodeParallelism(), 8)));
    private static readonly SemaphoreSlim CdnValidationConcurrencySemaphore = new(Math.Max(2, Math.Min(GetCdnValidationParallelism(), 12)));

    private readonly ConcurrentDictionary<string, byte> _createdDirs = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, string> _pendingCacheEntryByPath = new(StringComparer.OrdinalIgnoreCase);
    private readonly CancellationTokenSource _cachePersistCts = new();
    private Task _cachePersistLoop = Task.CompletedTask;
    private int _cachePersistLoopStarted;
    private int _cachePersistFlushRunning;
    private const int CachePersistBatchSize = 64;
    private static readonly TimeSpan CachePersistFlushInterval = TimeSpan.FromMilliseconds(250);


    private sealed class CdnInMemorySpoolBudget
    {
        private long _usedBytes;

        public bool TryReserve(long bytes)
        {
            if (bytes <= 0) return true;
            if (bytes > CdnInMemorySpoolMaxGroupBytes) return false;

            while (true)
            {
                var used = Volatile.Read(ref _usedBytes);
                var next = used + bytes;
                if (next > CdnInMemorySpoolMaxGroupBytes)
                    return false;

                if (Interlocked.CompareExchange(ref _usedBytes, next, used) == used)
                    return true;
            }
        }

        public void Release(long bytes)
        {
            if (bytes <= 0) return;
            var after = Interlocked.Add(ref _usedBytes, -bytes);
            if (after < 0)
                Interlocked.Exchange(ref _usedBytes, 0);
        }
    }

    private sealed class GroupProgressState
    {
        public long PendingBytes;
        public long PendingFiles;
        public long LastFlushTicks;
        public long LastPublishTicks;
    }

    private readonly ConcurrentDictionary<string, GroupProgressState> _groupProgress
        = new(StringComparer.Ordinal);

    private void AddGroupProgress(GameObjectHandler downloadId, string groupKey, long bytes, long files)
    {
        var state = _groupProgress.GetOrAdd(groupKey, _ => new GroupProgressState());

        Interlocked.Add(ref state.PendingBytes, bytes);
        Interlocked.Add(ref state.PendingFiles, files);

        TryFlushGroupProgress(groupKey, state, force: false);
        var now = Stopwatch.GetTimestamp();
        var last = Volatile.Read(ref state.LastPublishTicks);

        long intervalTicks = (long)(Stopwatch.Frequency * 0.15);

        if ((now - last) >= intervalTicks &&
            Interlocked.CompareExchange(ref state.LastPublishTicks, now, last) == last)
        {
            TryFlushGroupProgress(groupKey, state, force: true);
            Mediator.Publish(new DownloadStartedMessage(downloadId, _downloadStatus));
        }
    }

    private void TryFlushGroupProgress(string groupKey, GroupProgressState state, bool force)
    {
        // Flush at most ~20Hz unless forced.
        var now = Stopwatch.GetTimestamp();
        var last = Volatile.Read(ref state.LastFlushTicks);

        // 50ms in Stopwatch ticks
        long flushIntervalTicks = (long)(Stopwatch.Frequency * 0.05);

        if (!force && (now - last) < flushIntervalTicks)
            return;

        // If another thread just flushed, bail.
        if (!force && Interlocked.CompareExchange(ref state.LastFlushTicks, now, last) != last)
            return;

        var bytes = Interlocked.Exchange(ref state.PendingBytes, 0);
        var files = Interlocked.Exchange(ref state.PendingFiles, 0);

        if (bytes == 0 && files == 0 && !force)
            return;

        if (_downloadStatus.TryGetValue(groupKey, out var st))
        {
            lock (st)
            {
                if (files != 0)
                {
                    var nextFiles = Math.Max(0, st.TransferredFiles + (int)files);

                    if (st.TotalFiles > 0)
                        nextFiles = Math.Min(st.TotalFiles, nextFiles);

                    st.TransferredFiles = nextFiles;
                }

                if (bytes != 0)
                {
                    var nextBytes = Math.Max(0, st.TransferredBytes + bytes);

                    if (st.TotalBytes > 0)
                        nextBytes = Math.Min(st.TotalBytes, nextBytes);

                    st.TransferredBytes = nextBytes;
                }
            }
        }
    }

    private void ForceFlushGroupProgress(string groupKey)
    {
        if (_groupProgress.TryGetValue(groupKey, out var state))
            TryFlushGroupProgress(groupKey, state, force: true);
    }


    private void EnsureDirectoryForFile(string filePath)
    {
        var dir = Path.GetDirectoryName(filePath);
        if (string.IsNullOrWhiteSpace(dir)) return;

        if (_createdDirs.TryAdd(dir, 0))
        {
            Directory.CreateDirectory(dir);
        }
    }


    private static int GetCdnDecodeParallelism()
    {
        var cpu = Environment.ProcessorCount;
        if (cpu <= 8) return 3;
        if (cpu <= 16) return 4;
        if (cpu <= 24) return 6;
        return 8;
    }

    private static int GetCdnValidationParallelism()
    {
        var cpu = Environment.ProcessorCount;
        if (cpu <= 8) return 4;
        if (cpu <= 16) return 6;
        if (cpu <= 24) return 8;
        return 12;
    }

    private int GetCdnParallelTarget(int configuredParallel, int filesInGroup)
    {
        var maxParallel = AutoCdnMaxParallel;

        if (configuredParallel > 0)
            return Math.Clamp(configuredParallel, 1, Math.Min(maxParallel, Math.Max(1, filesInGroup)));

        var p = Volatile.Read(ref _autoCdnParallel);
        p = Math.Clamp(p, AutoCdnMinParallel, maxParallel);
        return Math.Clamp(p, 1, Math.Max(1, filesInGroup));
    }

    private static bool IsTinyFileHeavy(IReadOnlyList<DownloadFileTransfer> transfers)
    {
        if (transfers.Count < 16) return false;

        var tinyCount = 0;
        for (var i = 0; i < transfers.Count; i++)
        {
            var total = transfers[i].Total;
            if (total > 0 && total <= TinyFilePayloadBytes)
                tinyCount++;
        }

        return tinyCount * 4 >= transfers.Count * 3;
    }

    private void UpdateAutoCdnParallel(int usedParallel, bool success, bool hadTimeoutOrBackoff, bool hadSlow)
    {
        // Only adjust when config is Auto (0)
        if (_mareConfigService.Current.ParallelDownloads > 0) return;

        var cur = Volatile.Read(ref _autoCdnParallel);
        var next = cur;

        if (!success)
            next = Math.Max(AutoCdnMinParallel, cur - 2);
        else if (hadTimeoutOrBackoff)
            next = Math.Max(AutoCdnMinParallel, cur - 1);
        else if (hadSlow)
            next = Math.Max(AutoCdnMinParallel, cur - 1);
        else
        {
            if (usedParallel >= cur)
            {
                var step = cur < 16 ? 4 : 2;
                next = Math.Min(AutoCdnMaxParallel, cur + step);
            }

        }

        if (next != cur)
        {
            Volatile.Write(ref _autoCdnParallel, next);
            Logger.LogDebug("CDN auto parallel: {cur} => {next} (success={success}, timeout/backoff={timeout}, slow={slow}, used={used})",
                cur, next, success, hadTimeoutOrBackoff, hadSlow, usedParallel);
        }
    }

    private readonly ConcurrentDictionary<string, DateTime> _lastCentralFileRepairRequestByKey = new(StringComparer.OrdinalIgnoreCase);

    // Shared across all per-pair download managers. Room/reconnect bursts often need the
    // same hashes for several visible users; only one pair should actually fetch a hash
    // while the rest wait for the cache to be populated.
    private static readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> GlobalInflightDownloadsByHash = new(StringComparer.OrdinalIgnoreCase);

    private static bool TryClaimGlobalInflightDownload(string hash, out Task<bool>? existingDownload)
    {
        existingDownload = null;

        if (string.IsNullOrWhiteSpace(hash))
            return false;

        var claim = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (GlobalInflightDownloadsByHash.TryAdd(hash, claim))
            return true;

        if (GlobalInflightDownloadsByHash.TryGetValue(hash, out var existing))
            existingDownload = existing.Task;

        return false;
    }

    private static void CompleteGlobalInflightDownload(string hash, bool success)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return;

        if (GlobalInflightDownloadsByHash.TryRemove(hash, out var completion))
            completion.TrySetResult(success);
    }

    private static async Task WaitForGlobalInflightDownloadsAsync(List<Task<bool>> waits, CancellationToken ct)
    {
        if (waits.Count == 0)
            return;

        try
        {
            await Task.WhenAll(waits.Select(t => t.WaitAsync(ct))).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Failed owners will clear their global claims and the caller's normal
            // recalculate/retry path will decide what remains missing.
        }
    }

    private string _centralFileRepairTargetUid = string.Empty;
    private string _centralFileRepairTargetIdent = string.Empty;
    private string _centralFileRepairDataHash = string.Empty;

    private static readonly TimeSpan CentralFileRepairRequestCooldown = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan CentralFileRepairGraceDelay = TimeSpan.FromSeconds(8);
    private const int MaxCentralFileRepairAttemptsPerFile = 2;

    private readonly ConcurrentDictionary<string, int> _centralFileRepairAttemptsByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Cdn404State> _cdn404State = new(StringComparer.OrdinalIgnoreCase);

    private sealed class Cdn404State
    {
        public DateTime FirstSeenUtc;
        public DateTime LastSeenUtc;
        public int Count;
        public DateTime LastSelfHealUtc;
    }

    public FileDownloadManager(ILogger<FileDownloadManager> logger, MareMediator mediator,
        FileTransferOrchestrator orchestrator,
        FileCacheManager fileCacheManager, FileCompactor fileCompactor, MareConfigService mareConfigService) : base(logger, mediator)
    {
        _downloadStatus = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);
        _orchestrator = orchestrator;
        _fileDbManager = fileCacheManager;
        _fileCompactor = fileCompactor;
        _activeDownloadStreams = [];
        _mareConfigService = mareConfigService;
        _cdnFastClient = CreateCdnFastClient();

        Mediator.Subscribe<DownloadLimitChangedMessage>(this, (msg) =>
        {
            if (_activeDownloadStreams.Count == 0) return;

            var limit = _orchestrator.DownloadLimitPerSlot();
            var newLimit = limit <= 0 ? ThrottledStream.Infinite : limit;
            Logger.LogTrace("Setting new Download Speed Limit to {newLimit}", newLimit);

            foreach (var stream in _activeDownloadStreams.ToArray())
            {
                stream.BandwidthLimit = newLimit;
            }
        });


    }

    private static HttpClient CreateCdnFastClient()
    {
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            ConnectTimeout = TimeSpan.FromSeconds(60),
            MaxConnectionsPerServer = 512,
            EnableMultipleHttp2Connections = true,
        };

        var client = new HttpClient(handler, disposeHandler: true)
        {
            Timeout = Timeout.InfiniteTimeSpan,
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        };

        return client;
    }

    private static string? TryGetHeaderValue(HttpResponseMessage response, string headerName)
    {
        if (response.Headers.TryGetValues(headerName, out var values))
        {
            foreach (var v in values)
            {
                if (!string.IsNullOrEmpty(v)) return v;
            }
        }

        if (response.Content?.Headers != null && response.Content.Headers.TryGetValues(headerName, out var values2))
        {
            foreach (var v in values2)
            {
                if (!string.IsNullOrEmpty(v)) return v;
            }
        }

        return null;
    }

    private static string AppendCacheBustQuery(string url)
    {
        var sep = url.IndexOf('?', StringComparison.Ordinal) >= 0 ? "&" : "?";
        return url + sep + "rava_repair=" + Guid.NewGuid().ToString("N");
    }

    private bool TryPlanCdn404SelfHeal(string hash, string baseUrl, HttpResponseMessage response,out string cacheBustUrl, out string? cfCacheStatus, out string? age, out string? cfRay, out string reason)
    {
        cfCacheStatus = TryGetHeaderValue(response, "CF-Cache-Status");
        age = TryGetHeaderValue(response, "Age");
        cfRay = TryGetHeaderValue(response, "CF-RAY");

        var cachey = false;
        if (!string.IsNullOrEmpty(cfCacheStatus))
        {
            cachey =
                !cfCacheStatus.Equals("MISS", StringComparison.OrdinalIgnoreCase) &&
                !cfCacheStatus.Equals("BYPASS", StringComparison.OrdinalIgnoreCase) &&
                !cfCacheStatus.Equals("DYNAMIC", StringComparison.OrdinalIgnoreCase);
        }

        if (!cachey && !string.IsNullOrEmpty(age))
        {
            cachey = true;
        }

        var now = DateTime.UtcNow;
        var state = _cdn404State.GetOrAdd(hash, _ => new Cdn404State
        {
            FirstSeenUtc = now,
            LastSeenUtc = now,
            Count = 0,
            LastSelfHealUtc = DateTime.MinValue
        });

        lock (state)
        {
            if ((now - state.LastSeenUtc) > TimeSpan.FromMinutes(10))
            {
                state.FirstSeenUtc = now;
                state.Count = 0;
                state.LastSelfHealUtc = DateTime.MinValue;
            }

            state.LastSeenUtc = now;
            state.Count++;

            if ((now - state.LastSelfHealUtc) < TimeSpan.FromMinutes(2))
            {
                cacheBustUrl = string.Empty;
                reason = "self-heal recently attempted";
                return false;
            }

            if (cachey || (state.Count >= 2 && (now - state.FirstSeenUtc) < TimeSpan.FromMinutes(5)))
            {
                state.LastSelfHealUtc = now;
                cacheBustUrl = AppendCacheBustQuery(baseUrl);
                reason = cachey ? "cached 404 suspected" : "repeat 404";
                return true;
            }
        }

        cacheBustUrl = string.Empty;
        reason = "not eligible for self-heal";
        return false;
    }

    public void ConfigureFileRepairContext(string targetUid, string targetIdent, string dataHash)
    {
        _centralFileRepairTargetUid = targetUid ?? string.Empty;
        _centralFileRepairTargetIdent = targetIdent ?? string.Empty;
        _centralFileRepairDataHash = dataHash ?? string.Empty;
    }

    private void PublishCentralFileRepairRequest(string hash, string reason)
    {
        if (string.IsNullOrWhiteSpace(hash)
            || string.IsNullOrWhiteSpace(_centralFileRepairTargetUid)
            || string.IsNullOrWhiteSpace(_centralFileRepairTargetIdent))
        {
            return;
        }

        var dataHash = _centralFileRepairDataHash ?? string.Empty;
        var key = string.Join('|', _centralFileRepairTargetUid, dataHash, hash);
        var now = DateTime.UtcNow;

        if (_lastCentralFileRepairRequestByKey.TryGetValue(key, out var last) && now - last < CentralFileRepairRequestCooldown)
            return;

        _lastCentralFileRepairRequestByKey[key] = now;

        Logger.LogWarning(
            "CDN/B2 object {hash} was still missing after CDN retries; requesting source {uid} to force-upload it to central B2 ({reason})",
            hash,
            _centralFileRepairTargetUid,
            reason);

        Mediator.Publish(new RemoteMissingFileMessage(
            _centralFileRepairTargetUid,
            _centralFileRepairTargetIdent,
            dataHash,
            [hash],
            reason));
    }

    public List<DownloadFileTransfer> CurrentDownloads { get; private set; } = [];

    public List<FileTransfer> ForbiddenTransfers => _orchestrator.ForbiddenTransfers;

    public bool IsDownloading => !CurrentDownloads.Any();

    public static void MungeBuffer(Span<byte> buffer)
    {
        for (int i = 0; i < buffer.Length; ++i)
        {
            buffer[i] ^= 42;
        }
    }

    public void ClearDownload()
    {
        ReleaseInflight(CurrentDownloads);
        CurrentDownloads.Clear();

        _downloadStatus = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);

        _createdDirs.Clear();
        _groupProgress.Clear();
        _centralFileRepairAttemptsByHash.Clear();
    }

    public async Task DownloadFiles(GameObjectHandler gameObject, List<FileReplacementData> fileReplacementDto, CancellationToken ct)
    {
        Mediator.Publish(new HaltScanMessage(nameof(DownloadFiles)));
        try
        {
            await DownloadFilesInternal(gameObject, fileReplacementDto, ct).ConfigureAwait(false);
        }
        catch
        {
            ClearDownload();
        }
        finally
        {
            Mediator.Publish(new DownloadFinishedMessage(gameObject));
            Mediator.Publish(new ResumeScanMessage(nameof(DownloadFiles)));
        }
    }


    private static async Task CopyExactlyWithProgressAsync(Stream src, Stream dst, long bytes, Action<int> onRead, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        try
        {
            long remaining = bytes;
            while (remaining > 0)
            {
                int want = (int)Math.Min(buffer.Length, remaining);
                int read = await src.ReadAsync(buffer.AsMemory(0, want), ct).ConfigureAwait(false);
                if (read <= 0) throw new EndOfStreamException($"Expected {bytes} bytes, got {bytes - remaining}");

                await dst.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                remaining -= read;

                onRead(read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task CopyToWithProgressAsync(Stream src, Stream dst, Action<int> onRead, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        try
        {
            while (true)
            {
                int read = await src.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0) break;

                await dst.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                onRead(read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task CopyExactlyAsync(Stream src, Stream dst, long bytes, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        try
        {
            long remaining = bytes;
            while (remaining > 0)
            {
                int want = (int)Math.Min(buffer.Length, remaining);
                int read = await src.ReadAsync(buffer.AsMemory(0, want), ct).ConfigureAwait(false);
                if (read <= 0) throw new EndOfStreamException($"Expected {bytes} bytes, got {bytes - remaining}");
                await dst.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                remaining -= read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    protected override void Dispose(bool disposing)
    {
        ClearDownload();
        foreach (var stream in _activeDownloadStreams.ToArray())
        {
            try
            {
                stream.Dispose();
            }
            catch
            {
                // do nothing
                //
            }
        }
        if (disposing)
        {
            try { FlushPendingCacheEntries(allowDuringShutdown: true).GetAwaiter().GetResult(); } catch { /* ignore */ }
            try { _cachePersistCts.Cancel(); } catch { /* ignore */ }
            try { _cachePersistLoop.GetAwaiter().GetResult(); } catch { /* ignore */ }
            try { FlushPendingCacheEntries(allowDuringShutdown: true).GetAwaiter().GetResult(); } catch { /* ignore */ }
            try { _cachePersistCts.Dispose(); } catch { /* ignore */ }
            try { _cdnFastClient.Dispose(); } catch { /* ignore */ }
        }

        base.Dispose(disposing);
    }



    private sealed class ProgressReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly Action<int> _onRead;

        public ProgressReadStream(Stream inner, Action<int> onRead)
        {
            _inner = inner;
            _onRead = onRead;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var r = _inner.Read(buffer, offset, count);
            if (r > 0) _onRead(r);
            return r;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var r = await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (r > 0) _onRead(r);
            return r;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class PrefixedReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly byte[] _prefix;
        private int _offset;
        private readonly int _count;

        public PrefixedReadStream(Stream inner, byte[] prefix, int count)
        {
            _inner = inner;
            _prefix = prefix;
            _count = count;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_offset < _count)
            {
                int n = Math.Min(count, _count - _offset);
                Buffer.BlockCopy(_prefix, _offset, buffer, offset, n);
                _offset += n;
                return n;
            }

            return _inner.Read(buffer, offset, count);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_offset < _count)
            {
                int n = Math.Min(buffer.Length, _count - _offset);
                _prefix.AsMemory(_offset, n).CopyTo(buffer);
                _offset += n;
                return n;
            }

            return await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_offset < _count)
            {
                int n = Math.Min(count, _count - _offset);
                Buffer.BlockCopy(_prefix, _offset, buffer, offset, n);
                _offset += n;
                return Task.FromResult(n);
            }

            return _inner.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    public async Task<List<DownloadFileTransfer>> InitiateDownloadList(GameObjectHandler gameObjectHandler, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
        Logger.LogDebug("Download start: {id}", gameObjectHandler.Name);

        List<DownloadFileDto> downloadFileInfoFromService =
        [
            .. await FilesGetSizes(fileReplacement.Select(f => f.Hash).Distinct(StringComparer.Ordinal).ToList(), ct).ConfigureAwait(false),
        ];

        Logger.LogDebug("Files with size 0 or less: {files}",
            string.Join(", ", downloadFileInfoFromService.Where(f => f.Size <= 0).Select(f => f.Hash)));

        foreach (var dto in downloadFileInfoFromService.Where(c => c.IsForbidden))
        {
            if (!_orchestrator.ForbiddenTransfers.Exists(f => string.Equals(f.Hash, dto.Hash, StringComparison.Ordinal)))
            {
                _orchestrator.ForbiddenTransfers.Add(new DownloadFileTransfer(dto));
            }
        }

        var expectedPathByHash = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fr in fileReplacement)
        {
            if (string.IsNullOrEmpty(fr.Hash) || fr.GamePaths == null || fr.GamePaths.Count() == 0)
                continue;

            var gamePath = fr.GamePaths[0];
            var ext = "dat";

            if (!string.IsNullOrEmpty(gamePath))
            {
                var dot = gamePath.LastIndexOf('.');
                if (dot >= 0 && dot < gamePath.Length - 1)
                    ext = gamePath[(dot + 1)..];
            }

            var localPath = _fileDbManager.GetCacheFilePath(fr.Hash, ext);
            expectedPathByHash[fr.Hash] = localPath;
        }

        // de-dupe strictly by hash, then:
        // - use filesystem as truth for presence + validate cache entries against expected sizes
        // - DO NOT mark hashes inflight unless they are actually transferable (dto.FileExists=true)
        // - self-heal stray on-disk files without DB entries when possible
        var candidates = downloadFileInfoFromService
            .GroupBy(d => d.Hash, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .Where(dto =>
            {
                // Prefer RawSize if provided, otherwise fall back to Size for basic length checks.
                var expected = dto.RawSize > 0 ? dto.RawSize : 0;

                var cacheEntry = _fileDbManager.GetFileCacheByHash(dto.Hash);
                if (cacheEntry != null)
                {
                    var resolved = cacheEntry.ResolvedFilepath;

                    if (!string.IsNullOrWhiteSpace(resolved) && File.Exists(resolved))
                    {
                        // Fast cache path: this is already a registered cache entry. During
                        // reconnect/room-entry bursts, hashing every existing multi-GB file per
                        // pair makes cached rooms feel like fresh downloads. If the server gives
                        // us a raw size, use that. If it does not, trust the registered non-empty
                        // cache entry and skip the expensive SHA pass.
                        try
                        {
                            var len = new FileInfo(resolved).Length;
                            if (len > 0 && (expected <= 0 || len == expected))
                            {
                                Logger.LogTrace("Skip download for {Hash}: registered cache entry present at {Path} (len={Length}, expected={Expected})", dto.Hash, resolved, len, expected);
                                return false;
                            }
                        }
                        catch
                        {
                            // fall through to validation below
                        }

                        // Only do the expensive full hash validation when we have useful size
                        // information and the quick path did not prove the file good.
                        if (expected > 0 && ValidateDownloadedFileLowPri(resolved, dto.Hash, expectedRawSize: expected, ct))
                        {
                            Logger.LogTrace("Skip download for {Hash}: present and validated at {Path}", dto.Hash, resolved);
                            return false;
                        }

                        Logger.LogWarning("Cache file for {Hash} exists but failed validation/size at {Path}; forcing re-download", dto.Hash, resolved);

                        try { File.Delete(resolved); } catch { /* ignore */ }
                        try { _fileDbManager.RemoveHashedFile(cacheEntry.Hash, cacheEntry.PrefixedFilePath); } catch { /* ignore */ }
                    }
                    else
                    {
                        Logger.LogWarning("Cache entry for {Hash} exists but file is missing at {Path}; forcing re-download", dto.Hash, resolved);
                        try { _fileDbManager.RemoveHashedFile(cacheEntry.Hash, cacheEntry.PrefixedFilePath); } catch { /* ignore */ }
                    }
                }

                // Stray file at expected cache path? Validate and register.
                if (expectedPathByHash.TryGetValue(dto.Hash, out var path) && File.Exists(path))
                {
                    if (ValidateDownloadedFileLowPri(path, dto.Hash, expectedRawSize: expected, ct))
                    {
                        PersistFileToStorageLowPri(dto.Hash, path, ct);
                        Logger.LogTrace("Skip download for {Hash}: existing file at {Path} validated and registered in cache", dto.Hash, path);
                        return false;
                    }

                    try
                    {
                        Logger.LogWarning("Existing file for {Hash} at {Path} failed validation, deleting to force clean re-download", dto.Hash, path);
                        File.Delete(path);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "Failed to delete invalid existing file for {Hash} at {Path}", dto.Hash, path);
                    }
                }

                return true;
            })
            .ToList();

        // Only keep transfers that the server says actually exist and can be downloaded right now.
        var candidateTransfers = candidates.Select(d => new DownloadFileTransfer(d)).ToList();

        var untransferable = candidateTransfers.Where(t => !t.CanBeTransferred).ToList();
        if (untransferable.Count > 0)
        {
            Logger.LogDebug("Skipping {count} hashes because they are not transferable yet (FileExists=false or forbidden): {hashes}",
                untransferable.Count,
                string.Join(", ", untransferable.Select(t => t.Hash)));
        }

        var transferable = candidateTransfers.Where(t => t.CanBeTransferred).ToList();

        var sharedWaits = new List<Task<bool>>();
        var claimedTransfers = new List<DownloadFileTransfer>(transferable.Count);

        foreach (var transfer in transferable)
        {
            if (TryClaimGlobalInflightDownload(transfer.Hash, out var existingDownload))
            {
                claimedTransfers.Add(transfer);
                continue;
            }

            if (existingDownload != null)
            {
                sharedWaits.Add(existingDownload);
                Logger.LogTrace("Hash {hash} is already downloading in another pair; waiting for shared cache result instead of duplicating transfer", transfer.Hash);
            }
        }

        CurrentDownloads = claimedTransfers;

        // If every missing hash is already being downloaded by another pair, wait here so
        // the caller can immediately recalculate and apply from cache rather than starting
        // a duplicate CDN storm or burning retry attempts.
        if (CurrentDownloads.Count == 0 && sharedWaits.Count > 0)
            await WaitForGlobalInflightDownloadsAsync(sharedWaits, ct).ConfigureAwait(false);

        return CurrentDownloads;

    }

    private void MarkAllGroupsComplete()
    {
        foreach (var status in _downloadStatus.Values)
        {
            if (status == null)
                continue;

            lock (status)
            {
                status.DownloadStatus = DownloadStatus.Decompressing;
                if (status.TotalBytes > 0)
                    status.TransferredBytes = status.TotalBytes;
                if (status.TotalFiles > 0)
                    status.TransferredFiles = status.TotalFiles;
            }
        }
    }

    private void FlushAndPublishFinalDownloadStatus(GameObjectHandler downloadId)
    {
        foreach (var key in _groupProgress.Keys)
        {
            ForceFlushGroupProgress(key);
        }

        MarkAllGroupsComplete();
        Mediator.Publish(new DownloadStartedMessage(downloadId, _downloadStatus));
    }

    private async Task DownloadFilesInternal(GameObjectHandler gameObjectHandler, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
       var downloadGroups = CurrentDownloads
            .GroupBy(f => f.DownloadUri.Host + ":" + f.DownloadUri.Port, StringComparer.Ordinal)
            .ToList();

        foreach (var downloadGroup in downloadGroups)
        {
            _downloadStatus[downloadGroup.Key] = new FileDownloadStatus()
            {
                DownloadStatus = DownloadStatus.WaitingForQueue,
                TotalBytes = downloadGroup.Sum(c => c.Total),
                TotalFiles = downloadGroup.Count(),
                TransferredBytes = 0,
                TransferredFiles = 0
            };
        }

        Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, _downloadStatus));


        var anyGroupFailure = 0;
        var failedGroups = new ConcurrentBag<string>();

        await Parallel.ForEachAsync(downloadGroups, new ParallelOptions()
        {
            MaxDegreeOfParallelism = GetDownloadGroupParallelism(downloadGroups.Count),
            CancellationToken = ct,
        },
        async (fileGroup, token) =>
        {
            try
            {
                var cdnOk = await TryDownloadFilesFromCdnAsync(gameObjectHandler, fileGroup, fileReplacement, token).ConfigureAwait(false);
                if (cdnOk)
                {
                    Logger.LogDebug("CDN fast-path: group {group} fully satisfied via CDN, skipping blk pipeline", fileGroup.Key);
                    return;
                }

                Interlocked.Exchange(ref anyGroupFailure, 1);
                failedGroups.Add(fileGroup.Key);

                Logger.LogWarning("CDN fast-path: group {group} could not be fully satisfied via CDN; will fail overall after other groups finish",
                    fileGroup.Key);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Interlocked.Exchange(ref anyGroupFailure, 1);
                failedGroups.Add(fileGroup.Key);

                Logger.LogWarning(ex, "CDN fast-path: error for group {group}; will fail overall after other groups finish",
                    fileGroup.Key);
            }
        }).ConfigureAwait(false);

        if (Volatile.Read(ref anyGroupFailure) != 0)
        {
            Logger.LogWarning("CDN fast-path: one or more groups failed. Aborting download. Failed groups: {groups}",
                string.Join(", ", failedGroups.Distinct(StringComparer.Ordinal)));

            ClearDownload();
            return;
        }

        FlushAndPublishFinalDownloadStatus(gameObjectHandler);

        Logger.LogDebug("Download end: {id}", gameObjectHandler);

        ClearDownload();
    }


    private async Task<List<DownloadFileDto>> FilesGetSizes(List<string> hashes, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        var response = await _orchestrator.SendRequestAsync(HttpMethod.Get, MareFiles.ServerFilesGetSizesFullPath(_orchestrator.UploadCdnUri!), hashes, ct).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<List<DownloadFileDto>>(cancellationToken: ct).ConfigureAwait(false) ?? [];
    }


    private Task PersistFileToStorageLowPri(string fileHash, string filePath, CancellationToken ct)
    {
        try
        {
            QueueCachePersistence(fileHash, filePath);
        }
        catch
        {
            // ignore and let later validation/register paths recover naturally
        }

        return Task.CompletedTask;
    }

    private void QueueCachePersistence(string fileHash, string filePath)
    {
        _pendingCacheEntryByPath[filePath] = fileHash;
        EnsureCachePersistLoopStarted();

        if (_pendingCacheEntryByPath.Count >= CachePersistBatchSize)
        {
            _ = Task.Run(() => FlushPendingCacheEntries());
        }
    }

    private void EnsureCachePersistLoopStarted()
    {
        if (Volatile.Read(ref _cachePersistLoopStarted) != 0)
            return;

        if (Interlocked.CompareExchange(ref _cachePersistLoopStarted, 1, 0) != 0)
            return;

        _cachePersistLoop = Task.Run(() => CachePersistLoopAsync(_cachePersistCts.Token));
    }

    private async Task CachePersistLoopAsync(CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(CachePersistFlushInterval);
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                await FlushPendingCacheEntries().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Cache persist loop terminated unexpectedly");
        }
    }

    private async Task FlushPendingCacheEntries(bool allowDuringShutdown = false)
    {
        if (Interlocked.Exchange(ref _cachePersistFlushRunning, 1) != 0)
            return;

        try
        {
            while (allowDuringShutdown || !_cachePersistCts.IsCancellationRequested)
            {
                var pending = _pendingCacheEntryByPath.ToArray();
                if (pending.Length == 0)
                    break;

                var processed = 0;
                foreach (var kvp in pending)
                {
                    if (processed >= CachePersistBatchSize)
                        break;

                    if (!_pendingCacheEntryByPath.TryGetValue(kvp.Key, out var currentHash) || !string.Equals(currentHash, kvp.Value, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (!_pendingCacheEntryByPath.TryRemove(kvp.Key, out var fileHash))
                        continue;

                    PersistFileToStorage(fileHash, kvp.Key);
                    processed++;
                }

                if (processed == 0)
                    break;

                if (!allowDuringShutdown && !_pendingCacheEntryByPath.IsEmpty)
                {
                    try
                    {
                        await Task.Delay(8, _cachePersistCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
                else
                {
                    await Task.Yield();
                }
            }
        }
        finally
        {
            Volatile.Write(ref _cachePersistFlushRunning, 0);

            if (!_cachePersistCts.IsCancellationRequested && !_pendingCacheEntryByPath.IsEmpty)
            {
                _ = Task.Run(() => FlushPendingCacheEntries());
            }
        }
    }

    private void PersistFileToStorage(string fileHash, string filePath)
    {
        var fi = new FileInfo(filePath);
        if (!fi.Exists) return;

        try
        {
            var nowUtc = DateTime.UtcNow;
            if ((nowUtc - fi.LastAccessTimeUtc) >= TimeSpan.FromMinutes(30))
            {
                fi.LastAccessTimeUtc = nowUtc;
            }
        }
        catch
        {
            // ignore metadata update failures; cache accounting still happens below
        }

        try
        {
            var entry = _fileDbManager.CreateCacheEntry(filePath);
            if (entry != null && !string.Equals(entry.Hash, fileHash, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogError("Hash mismatch after extracting, got {hash}, expected {expectedHash}, deleting file", entry.Hash, fileHash);
                File.Delete(filePath);
                _fileDbManager.RemoveHashedFile(entry.Hash, entry.PrefixedFilePath);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error creating cache entry");
        }
    }

    private async Task WaitForDownloadReady(List<DownloadFileTransfer> downloadFileTransfer, Guid requestId, CancellationToken downloadCt)
    {
        bool alreadyCancelled = false;
        CancellationTokenSource? localTimeoutCts = null;
        CancellationTokenSource? composite = null;

        try
        {
            localTimeoutCts = new();
            localTimeoutCts.CancelAfter(TimeSpan.FromSeconds(5));
            composite = CancellationTokenSource.CreateLinkedTokenSource(downloadCt, localTimeoutCts.Token);

            while (!_orchestrator.IsDownloadReady(requestId))
            {
                try
                {
                    await Task.Delay(250, composite.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                    if (downloadCt.IsCancellationRequested) throw;

                    // PATCH: only poke the server if orchestrator is still alive
                    if (_orchestrator.IsInitialized)
                    {
                        var req = await _orchestrator.SendRequestAsync(
                            HttpMethod.Get,
                            MareFiles.RequestCheckQueueFullPath(_orchestrator.FilesCdnUri!, requestId),
                            downloadFileTransfer.Select(c => c.Hash).ToList(),
                            downloadCt
                        ).ConfigureAwait(false);
                        req.EnsureSuccessStatusCode();
                    }

                    localTimeoutCts.Dispose();
                    composite.Dispose();
                    localTimeoutCts = new();
                    localTimeoutCts.CancelAfter(TimeSpan.FromSeconds(5));
                    composite = CancellationTokenSource.CreateLinkedTokenSource(downloadCt, localTimeoutCts.Token);
                }
            }

            Logger.LogDebug("Download {requestId} ready", requestId);
        }
        catch (TaskCanceledException)
        {
            try
            {
                if (_orchestrator.IsInitialized)
                {
                    await _orchestrator.SendRequestAsync(
                        HttpMethod.Get,
                        MareFiles.RequestCancelFullPath(_orchestrator.FilesCdnUri!, requestId)
                    ).ConfigureAwait(false);
                    alreadyCancelled = true;
                }
            }
            catch (ObjectDisposedException)
            {
                // Orchestrator/HttpClient was torn downâ€”ignore.
            }

            throw;
        }
        finally
        {
            if (downloadCt.IsCancellationRequested && !alreadyCancelled)
            {
                try
                {
                    if (_orchestrator.IsInitialized)
                    {
                        await _orchestrator.SendRequestAsync(
                            HttpMethod.Get,
                            MareFiles.RequestCancelFullPath(_orchestrator.FilesCdnUri!, requestId)
                        ).ConfigureAwait(false);
                    }
                }
                catch (ObjectDisposedException)
                {
                    // Orchestrator/HttpClient was torn downâ€”ignore.
                }
            }

            // clean up CTS objects reliably
            try { localTimeoutCts?.Dispose(); } catch { }
            try { composite?.Dispose(); } catch { }

            _orchestrator.ClearDownloadRequest(requestId);
        }
    }

    private static bool DetectHeaderIsMunged(FileStream fs)
    {
        long pos = fs.Position;
        int b = fs.ReadByte();
        if (b == -1) throw new EndOfStreamException();
        fs.Position = pos;

        // If XOR(42) yields '#', it's munged; if it's already '#', it's plain.
        if ((char)(b ^ 42) == '#') return true;
        if ((char)b == '#') return false;

        // Neither looks like a header start
        throw new InvalidDataException("Data is invalid, first char is neither '#' nor munged '#'");
    }

    private int GetParallelApplyDegree(int filesInBlock)
    {
        int logical = Environment.ProcessorCount;

        // Cap by CPU, but also by how many files we actually have
        int maxByCpu =
            logical <= 4 ? 2 :
            logical <= 8 ? 3 :
            logical <= 16 ? 4 :
                            6;

        return Math.Clamp(filesInBlock, 1, maxByCpu);
    }


    private static bool TryParseHeader(ReadOnlySpan<byte> headerBytes, bool munged, out string fileHash, out long fileLengthBytes)
    {
        fileHash = string.Empty;
        fileLengthBytes = 0;

        if (headerBytes.Length < 3) return false;

        // Decode bytes to chars depending on munged flag
        Span<char> chars = stackalloc char[headerBytes.Length];
        for (int i = 0; i < headerBytes.Length; i++)
        {
            chars[i] = munged ? (char)(headerBytes[i] ^ 42) : (char)headerBytes[i];
        }

        if (chars[0] != '#') return false;

        // Expected format: #<hash>:<length>#
        var body = chars[1..];

        var colonIndex = body.IndexOf(':');
        if (colonIndex <= 0) return false;

        var endIndex = body[(colonIndex + 1)..].IndexOf('#');
        if (endIndex < 0) return false;

        var hashSpan = body[..colonIndex];
        var lenSpan = body.Slice(colonIndex + 1, endIndex);

        if (hashSpan.Length == 0 || lenSpan.Length == 0) return false;

        if (!long.TryParse(lenSpan, out fileLengthBytes)) return false;

        fileHash = new string(hashSpan);
        return true;
    }

    private static async Task<(string fileHash, long fileLengthBytes, bool munged)> ReadCdnHeaderAsync(Stream stream, CancellationToken ct)
    {
        var headerBytes = new List<byte>(128);
        var one = new byte[1];

        // Read byte-by-byte (header is tiny, max 256) but in an async + cancellable way.
        while (headerBytes.Count < 256)
        {
            var n = await stream.ReadAsync(one.AsMemory(0, 1), ct).ConfigureAwait(false);
            if (n <= 0) throw new EndOfStreamException("Unexpected end of stream while reading CDN header");
            headerBytes.Add(one[0]);

            // Only try parse when last interpreted char could be '#'
            byte last = headerBytes[^1];
            bool couldBeTerminator = (char)last == '#' || (char)(last ^ 42) == '#';
            if (!couldBeTerminator) continue;

            var span = CollectionsMarshal.AsSpan(headerBytes);

            if (TryParseHeader(span, munged: false, out var hash, out var len))
                return (hash, len, false);

            if (TryParseHeader(span, munged: true, out hash, out len))
                return (hash, len, true);
        }

        throw new InvalidDataException("CDN header too large or invalid");
    }

    private void CleanupFailedCdnDownload(string pathToDelete)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(pathToDelete) && File.Exists(pathToDelete))
                File.Delete(pathToDelete);
        }
        catch
        {
            // ignore
        }
    }

    private static string CreateTempDownloadPath(string finalPath)
    {
        var dir = Path.GetDirectoryName(finalPath);
        if (string.IsNullOrEmpty(dir))
            throw new InvalidOperationException($"Could not determine directory for {finalPath}");

        var name = Path.GetFileName(finalPath);

        // Keep temp in the same directory so the move is atomic on Windows.
        var tmp = name + ".tmp." + Environment.ProcessId + "." + Guid.NewGuid().ToString("N");
        return Path.Combine(dir, tmp);
    }
    private static bool IsFileInUse(Exception ex)
    {
        if (ex is UnauthorizedAccessException) return true;

        if (ex is IOException ioEx)
        {
            if (OperatingSystem.IsWindows())
            {
                const int SharingViolation = unchecked((int)0x80070020);
                const int LockViolation = unchecked((int)0x80070021);
                return ioEx.HResult == SharingViolation || ioEx.HResult == LockViolation;
            }
            return true;
        }
        return false;
    }


    private static async Task<(string HashUpper, long BytesWritten)> CopyRawToFileAndHashAsync(Stream input,string destPath,long expectedRawSize,CancellationToken ct)
    {
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);

        var outOpts = new FileStreamOptions
        {
            Access = FileAccess.Write,
            Mode = FileMode.Create,
            Share = FileShare.None,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            BufferSize = DownloadBufferSize
        };

        long total = 0;
        var buffer = _downloadBufferPool.Rent(256 * 1024);

        try
        {
            await using var outFs = new FileStream(destPath, outOpts);

            while (true)
            {
                var read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0)
                    break;

                sha1.AppendData(buffer, 0, read);
                await outFs.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                total += read;

                if (expectedRawSize > 0 && total > expectedRawSize)
                    throw new InvalidDataException($"Raw bytes exceeded expected size (expected {expectedRawSize}, got {total})");
            }

            if (expectedRawSize > 0 && total != expectedRawSize)
                throw new InvalidDataException($"Raw bytes did not match expected size (expected {expectedRawSize}, got {total})");

            return (Convert.ToHexString(sha1.GetHashAndReset()), total);
        }
        finally
        {
            _downloadBufferPool.Return(buffer);
        }
    }

    private static async Task<(string HashUpper, long BytesWritten)> DecompressLz4ToFileAndHashAsync(Stream input, string destPath, long expectedRawSize, CancellationToken ct)
    {
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);

        var outOpts = new FileStreamOptions
        {
            Access = FileAccess.Write,
            Mode = FileMode.Create,
            Share = FileShare.None,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            BufferSize = DownloadBufferSize
        };

        long total = 0;
        var minDecodeBuffer = Math.Max(Lz4BlockSize, 256 * 1024);
        var buffer = _downloadBufferPool.Rent(minDecodeBuffer);
        try
        {
            await using var outFs = new FileStream(destPath, outOpts);
            using var fullReadInput = new FullReadStream(input);
            await using var lz4 = new LZ4Stream(fullReadInput, LZ4StreamMode.Decompress, LZ4StreamFlags.None, Lz4BlockSize);

            const long YieldEveryBytes = 2L * 1024 * 1024; // 2MB time-slice
            long sinceYield = 0;

            while (true)
            {
                var read = await lz4.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0) break;

                sha1.AppendData(buffer, 0, read);
                await outFs.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                total += read;
                sinceYield += read;

                // Safety: if server told us the raw size, never allow overflow expansions.
                if (expectedRawSize > 0 && total > expectedRawSize)
                    throw new InvalidDataException($"Decompressed bytes exceeded expected size (expected {expectedRawSize}, got {total})");

                // Cooperative scheduling: avoid long uninterrupted CPU bursts inside the game process.
                if (sinceYield >= YieldEveryBytes)
                {
                    sinceYield = 0;
                    await Task.Yield();
                    ct.ThrowIfCancellationRequested();
                }
            }

            //await outFs.FlushAsync(ct).ConfigureAwait(false);

            // Safety: if we know the exact size, enforce it.
            if (expectedRawSize > 0 && total != expectedRawSize)
                throw new InvalidDataException($"Decompressed bytes did not match expected size (expected {expectedRawSize}, got {total})");

            return (Convert.ToHexString(sha1.GetHashAndReset()), total);
        }
        finally
        {
            _downloadBufferPool.Return(buffer);
        }
    }


    private async Task<(string ComputedHash, long BytesWritten, bool UsedXorCompat)> DecodeHeaderlessSpoolWithCompatAsync(string spoolPath,string tempPath,long expectedRawSize,string expectedHash,CancellationToken ct)
    {
        await CdnDecodeConcurrencySemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            bool TryMatches((string HashUpper, long BytesWritten) res)
                => res.BytesWritten > 0 && string.Equals(res.HashUpper, expectedHash, StringComparison.OrdinalIgnoreCase);

            try
            {
                await using var src1 = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 256 * 1024, FileOptions.SequentialScan | FileOptions.Asynchronous);
                var res1 = await DecompressLz4ToFileAndHashAsync(src1, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (TryMatches(res1))
                    return (res1.HashUpper, res1.BytesWritten, false);
            }
            catch
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
            }

            try
            {
                await using var src2 = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 256 * 1024, FileOptions.SequentialScan | FileOptions.Asynchronous);
                using var xor2 = new LimitedXorStream(src2, src2.Length, 42);
                var res2 = await DecompressLz4ToFileAndHashAsync(xor2, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (TryMatches(res2))
                    return (res2.HashUpper, res2.BytesWritten, true);
            }
            catch
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
            }

            try
            {
                await using var src3 = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 256 * 1024, FileOptions.SequentialScan | FileOptions.Asynchronous);
                var res3 = await CopyRawToFileAndHashAsync(src3, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (TryMatches(res3))
                    return (res3.HashUpper, res3.BytesWritten, false);
            }
            catch
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
            }

            await using var src4 = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 256 * 1024, FileOptions.SequentialScan | FileOptions.Asynchronous);
            using var xor4 = new LimitedXorStream(src4, src4.Length, 42);
            var res4 = await CopyRawToFileAndHashAsync(xor4, tempPath, expectedRawSize, ct).ConfigureAwait(false);
            return (res4.HashUpper, res4.BytesWritten, true);
        }
        finally
        {
            try { if (File.Exists(spoolPath)) File.Delete(spoolPath); } catch { }
            CdnDecodeConcurrencySemaphore.Release();
        }
    }

    private async Task<(string ComputedHash, long BytesWritten, bool UsedXorCompat)> DecodeHeaderlessBytesWithCompatAsync(byte[] payloadBuffer, int payloadLength, string tempPath,long expectedRawSize, string expectedHash, CancellationToken ct)
    {
        try
        {
            using (var src = new MemoryStream(payloadBuffer, 0, payloadLength, writable: false, publiclyVisible: true))
            {
                var (hash, bytes) = await DecompressLz4ToFileAndHashAsync(src, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (bytes > 0 && string.Equals(hash, expectedHash, StringComparison.OrdinalIgnoreCase))
                    return (hash, bytes, false);
            }
        }
        catch
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
        }

        try
        {
            using (var src = new MemoryStream(payloadBuffer, 0, payloadLength, writable: false, publiclyVisible: true))
            using (var xor = new LimitedXorStream(src, payloadLength, 42))
            {
                var (hash, bytes) = await DecompressLz4ToFileAndHashAsync(xor, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (bytes > 0 && string.Equals(hash, expectedHash, StringComparison.OrdinalIgnoreCase))
                    return (hash, bytes, true);
            }
        }
        catch
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
        }

        try
        {
            using (var src = new MemoryStream(payloadBuffer, 0, payloadLength, writable: false, publiclyVisible: true))
            {
                var (hash, bytes) = await CopyRawToFileAndHashAsync(src, tempPath, expectedRawSize, ct).ConfigureAwait(false);
                if (bytes > 0 && string.Equals(hash, expectedHash, StringComparison.OrdinalIgnoreCase))
                    return (hash, bytes, false);
            }
        }
        catch
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
        }

        using (var src = new MemoryStream(payloadBuffer, 0, payloadLength, writable: false, publiclyVisible: true))
        using (var xor = new LimitedXorStream(src, payloadLength, 42))
        {
            var (hash, bytes) = await CopyRawToFileAndHashAsync(xor, tempPath, expectedRawSize, ct).ConfigureAwait(false);
            return (hash, bytes, true);
        }
    }

    private async Task<(string ComputedHash, long BytesWritten)> DecodeHeaderedBytesAsync(byte[] payloadBuffer, int payloadLength, bool munged, string tempPath, long expectedRawSize, CancellationToken ct)
    {
        using var src = new MemoryStream(payloadBuffer, 0, payloadLength, writable: false, publiclyVisible: true);
        Stream payloadStream = munged ? new LimitedXorStream(src, payloadLength, 42) : src;
        using (payloadStream)
        {
            return await DecompressLz4ToFileAndHashAsync(payloadStream, tempPath, expectedRawSize, ct).ConfigureAwait(false);
        }
    }

    private async Task<(byte[] Buffer, int Length)> ReadSmallPayloadToPooledBufferAsync(Stream input, long maxBytes, Action<long> progress, CancellationToken ct)
    {
        if (maxBytes <= 0 || maxBytes > int.MaxValue)
            throw new InvalidDataException($"Invalid small payload size {maxBytes}");

        var length = (int)maxBytes;
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(length);
        var offset = 0;

        try
        {
            while (offset < length)
            {
                var read = await input.ReadAsync(payloadBuffer.AsMemory(offset, length - offset), ct).ConfigureAwait(false);
                if (read <= 0)
                    throw new EndOfStreamException($"CDN payload ended early, expected {length} bytes but only received {offset}");

                offset += read;
                progress(read);
            }

            return (payloadBuffer, length);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
            throw;
        }
    }
    private async Task<bool> TryDownloadFilesFromCdnAsync(GameObjectHandler gameObjectHandler, IGrouping<string, DownloadFileTransfer> fileGroup, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {

        var success = false;
        List<DownloadFileTransfer>? transfers = null;

        try
        {
            if (_orchestrator.FilesCdnUri == null) return false;

            transfers = fileGroup.ToList();
            if (transfers.Count == 0) return true;

            var fileExtByHash = fileReplacement
                .Where(f => !string.IsNullOrEmpty(f.Hash) && f.GamePaths != null && f.GamePaths.Count() > 0)
                .GroupBy(f => f.Hash, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g =>
                {
                    var gp = g.First().GamePaths[0];
                    if (string.IsNullOrEmpty(gp)) return "dat";

                    var dot = gp.LastIndexOf('.');
                    return (dot >= 0 && dot < gp.Length - 1) ? gp[(dot + 1)..] : "dat";
                }, StringComparer.OrdinalIgnoreCase);

            var configured = _mareConfigService.Current.ParallelDownloads;
            var tinyFileHeavy = IsTinyFileHeavy(transfers);

            // 0 => Auto (adaptive)
            var cdnParallel = GetCdnParallelTarget(configured, transfers.Count);
            if (configured <= 0 && tinyFileHeavy)
                cdnParallel = Math.Clamp(Math.Max(cdnParallel, Math.Min(TinyFileParallelMax, transfers.Count)), 1, Math.Min(TinyFileParallelMax, transfers.Count));
            else
                cdnParallel = Math.Min(cdnParallel, transfers.Count);

            var anyFailure = new int[1];
            var anyTimeoutOrBackoff = new int[1];
            var anySlow = new int[1];
            var downloadedStageCount = new int[1];

            var spooledFiles = new ConcurrentBag<CdnSpooledFile>();
            var results = new ConcurrentBag<CdnDownloadedFile>();
            var inMemorySpoolBudget = new CdnInMemorySpoolBudget();
            using var decodeQueue = new BlockingCollection<CdnSpooledFile>();
            var groupKey = fileGroup.Key;

            if (_downloadStatus.TryGetValue(groupKey, out var statusStart))
            {
                lock (statusStart)
                {
                    statusStart.DownloadStatus = DownloadStatus.Downloading;
                }

                Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, _downloadStatus));
            }

            var decodeParallel = Math.Clamp(GetCdnDecodeParallelism(), 1, Math.Max(1, Math.Min(transfers.Count, GetCdnDecodeParallelism())));
            var decodeWorkers = new List<Task>(decodeParallel);

            for (var i = 0; i < decodeParallel; i++)
            {
                decodeWorkers.Add(Task.Run(async () =>
                {
                    try
                    {
                        foreach (var spooled in decodeQueue.GetConsumingEnumerable(ct))
                        {
                            if (Volatile.Read(ref anyFailure[0]) != 0)
                            {
                                try { if (File.Exists(spooled.TempPath)) File.Delete(spooled.TempPath); } catch { }
                                CleanupSpooledPayload(spooled);
                                continue;
                            }

                            await DecodeOneCdnSpoolAsync(
                                spooled,
                                results,
                                anyFailure,
                                anySlow,
                                ct).ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                    }
                }, ct));
            }

            var index = -1;
            var workers = new List<Task>(cdnParallel);


            for (int i = 0; i < cdnParallel; i++)
            {
                workers.Add(Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested && Volatile.Read(ref anyFailure[0]) == 0)
                    {
                        var j = Interlocked.Increment(ref index);
                        if (j >= transfers.Count) break;

                        await DownloadOneFromCdnToSpoolAsync(
                            gameObjectHandler,
                            groupKey,
                            fileExtByHash,
                            spooledFiles,
                            decodeQueue,
                            downloadedStageCount,
                            results,
                            inMemorySpoolBudget,
                            transfers[j],
                            anyFailure,
                            anyTimeoutOrBackoff,
                            anySlow,
                            ct).ConfigureAwait(false);
                    }
                }, ct));
            }

            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
                ForceFlushGroupProgress(groupKey);
            }
            catch (OperationCanceledException)
            {
                Interlocked.Exchange(ref anyFailure[0], 1);

                try { ForceFlushGroupProgress(groupKey); } catch { /* ignore */ }

                if (ct.IsCancellationRequested)
                {
                    decodeQueue.CompleteAdding();
                    try { await Task.WhenAll(decodeWorkers).ConfigureAwait(false); } catch { /* ignore */ }
                    return false;
                }
            }
            finally
            {
                decodeQueue.CompleteAdding();
            }

            var hadTimeout = Volatile.Read(ref anyTimeoutOrBackoff[0]) != 0;
            var hadSlowLink = Volatile.Read(ref anySlow[0]) != 0;

            if (Volatile.Read(ref anyFailure[0]) != 0 || Volatile.Read(ref downloadedStageCount[0]) != transfers.Count)
            {
                Logger.LogDebug("CDN staged-path: at least one file failed during download/spool, falling back to blk pipeline for group {group}", groupKey);
                try { await Task.WhenAll(decodeWorkers).ConfigureAwait(false); } catch { /* ignore */ }
                CleanupSpooledFiles(spooledFiles);
                UpdateAutoCdnParallel(cdnParallel, success: false, hadTimeoutOrBackoff: hadTimeout, hadSlow: hadSlowLink);
                return false;
            }

            if (_downloadStatus.TryGetValue(groupKey, out var statusDecode))
            {
                lock (statusDecode)
                {
                    statusDecode.DownloadStatus = DownloadStatus.Decompressing;
                }

                Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, _downloadStatus));
            }

            try
            {
                await Task.WhenAll(decodeWorkers).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Interlocked.Exchange(ref anyFailure[0], 1);
                if (ct.IsCancellationRequested)
                {
                    CleanupSpooledFiles(spooledFiles);
                    return false;
                }
            }

            if (Volatile.Read(ref anyFailure[0]) != 0 || results.Count != transfers.Count)
            {
                Logger.LogDebug("CDN staged-path: at least one file failed during decode/finalize, falling back to blk pipeline for group {group}", groupKey);
                CleanupSpooledFiles(spooledFiles);
                UpdateAutoCdnParallel(cdnParallel, success: false, hadTimeoutOrBackoff: hadTimeout, hadSlow: true);
                return false;
            }

            _fileDbManager.RegisterDownloadedCacheFiles(results.Select(r => (r.Hash, r.FinalPath)));

            foreach (var result in results)
                CompleteGlobalInflightDownload(result.Hash, success: true);

            if (_downloadStatus.TryGetValue(groupKey, out var status))
            {
                lock (status)
                {
                    status.DownloadStatus = DownloadStatus.Downloading;
                }
            }
            Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, _downloadStatus));

            Logger.LogDebug("CDN staged-path: completed all files for group {group} via CDN", groupKey);
            success = true;

            UpdateAutoCdnParallel(cdnParallel, success: true, hadTimeoutOrBackoff: hadTimeout, hadSlow: hadSlowLink);
            return true;
        }
        finally
        {
            if (!success && transfers != null)
                ReleaseInflight(transfers);
        }

    }




    private async Task DownloadOneFromCdnToSpoolAsync(GameObjectHandler gameObjectHandler, string groupKey, Dictionary<string, string> fileExtByHash, ConcurrentBag<CdnSpooledFile> spooledFiles, BlockingCollection<CdnSpooledFile> decodeQueue, int[] downloadedStageCount, ConcurrentBag<CdnDownloadedFile> results, CdnInMemorySpoolBudget inMemorySpoolBudget, DownloadFileTransfer transfer, int[] anyFailure, int[] anyTimeoutOrBackoff, int[] anySlow, CancellationToken ct)
    {
        try
        {
            var attempts = 0;
            string? plannedCdnUrl = null;

            while (attempts++ < MaxCdnAttemptsPerFile && !ct.IsCancellationRequested)
            {
                var baseCdnUrl = MareFiles
                    .CdnGetFullPath(_orchestrator.FilesCdnUri!, transfer.Hash.ToUpperInvariant())
                    .ToString();

                var isCacheBusted = plannedCdnUrl != null;
                var cdnUrl = plannedCdnUrl ?? baseCdnUrl;
                plannedCdnUrl = null;

                long attemptProgressBytes = 0;
                long attemptProgressFiles = 0;
                string? spoolPath = null;
                byte[]? inMemoryPayload = null;
                int inMemoryPayloadLength = 0;
                long inMemoryBudgetReservation = 0;

                void ReleaseInMemoryPayload()
                {
                    if (inMemoryPayload != null)
                    {
                        try { ArrayPool<byte>.Shared.Return(inMemoryPayload); } catch { }
                        inMemoryPayload = null;
                    }

                    if (inMemoryBudgetReservation > 0)
                    {
                        inMemorySpoolBudget.Release(inMemoryBudgetReservation);
                        inMemoryBudgetReservation = 0;
                    }
                }

                void CleanupAttemptSpool()
                {
                    if (!string.IsNullOrWhiteSpace(spoolPath))
                        CleanupFailedCdnDownload(spoolPath);

                    ReleaseInMemoryPayload();
                }

                void AddAttemptProgress(long read)
                {
                    if (read <= 0)
                        return;

                    Interlocked.Add(ref attemptProgressBytes, read);
                    AddGroupProgress(gameObjectHandler, groupKey, read, 0);
                }

                void CommitAttemptFile()
                {
                    Interlocked.Exchange(ref attemptProgressFiles, 1);
                    AddGroupProgress(gameObjectHandler, groupKey, 0, 1);
                }

                void RollbackAttemptProgress()
                {
                    var bytes = Interlocked.Exchange(ref attemptProgressBytes, 0);
                    if (bytes > 0)
                        AddGroupProgress(gameObjectHandler, groupKey, -bytes, 0);

                    var files = Interlocked.Exchange(ref attemptProgressFiles, 0);
                    if (files > 0)
                        AddGroupProgress(gameObjectHandler, groupKey, 0, -files);
                }

                Logger.LogDebug("CDN staged-path: downloading {hash} from {url} (attempt {attempt}/{max})",
                    transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                try
                {
                    using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    attemptCts.CancelAfter(CdnAttemptTimeout);
                    var attemptToken = attemptCts.Token;

                    using var response = await _cdnFastClient
                        .GetAsync(cdnUrl, HttpCompletionOption.ResponseHeadersRead, attemptToken)
                        .ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        if (response.StatusCode == HttpStatusCode.NotFound && attempts >= MaxCdnAttemptsPerFile)
                        {
                            var repairAttempts = _centralFileRepairAttemptsByHash.AddOrUpdate(
                                transfer.Hash,
                                1,
                                (_, current) => current + 1);

                            if (repairAttempts <= MaxCentralFileRepairAttemptsPerFile)
                            {
                                PublishCentralFileRepairRequest(
                                    transfer.Hash,
                                    isCacheBusted ? "final cache-busted CDN 404" : "final CDN 404");

                                Logger.LogWarning(
                                    "Waiting {delay}s for central B2 repair upload of {hash}, then retrying CDN download. Repair attempt {attempt}/{max}",
                                    CentralFileRepairGraceDelay.TotalSeconds,
                                    transfer.Hash,
                                    repairAttempts,
                                    MaxCentralFileRepairAttemptsPerFile);

                                await Task.Delay(CentralFileRepairGraceDelay, ct).ConfigureAwait(false);

                                plannedCdnUrl = AppendCacheBustQuery(baseCdnUrl);
                                attempts = 0;
                                continue;
                            }

                            Logger.LogWarning(
                                "Central B2 repair retry limit reached for {hash}; continuing normal CDN failure handling",
                                transfer.Hash);
                        }
                        else if (response.StatusCode == HttpStatusCode.NotFound && !isCacheBusted &&
                                                                            TryPlanCdn404SelfHeal(transfer.Hash, baseCdnUrl, response,
                                out var cacheBustUrl, out var cfCacheStatus, out var age, out var cfRay, out var reason))
                        {
                            plannedCdnUrl = cacheBustUrl;

                            Logger.LogDebug(
                                "CDN staged-path: 404 for {hash} ({cfCacheStatus}, age {age}); self-heal retry via cache-bust: {url} ({reason})",
                                transfer.Hash,
                                cfCacheStatus ?? "n/a",
                                age ?? "n/a",
                                cacheBustUrl,
                                reason);

                            continue;
                        }

                        if ((int)response.StatusCode == 429)
                        {
                            Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);

                            var delayMs = Math.Min(2000, 250 * attempts);
                            await Task.Delay(delayMs, attemptToken).ConfigureAwait(false);
                            continue;
                        }

                        if (response.StatusCode == HttpStatusCode.RequestTimeout ||
                            response.StatusCode == HttpStatusCode.BadGateway ||
                            response.StatusCode == HttpStatusCode.ServiceUnavailable ||
                            response.StatusCode == HttpStatusCode.GatewayTimeout)
                        {
                            Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);
                            continue;
                        }

                        response.EnsureSuccessStatusCode();
                    }

                    await using var rawStream = await response.Content.ReadAsStreamAsync(attemptToken).ConfigureAwait(false);

                    var firstByteBuf = new byte[1];
                    var read0 = await rawStream.ReadAsync(firstByteBuf.AsMemory(0, 1), attemptToken).ConfigureAwait(false);
                    if (read0 <= 0) throw new InvalidDataException("CDN response stream was empty");

                    using var respStream = new PrefixedReadStream(rawStream, firstByteBuf, read0);
                    var b0 = firstByteBuf[0];
                    var looksHeadered = b0 == (byte)'#' || (byte)(b0 ^ 42) == (byte)'#';

                    string headerHash = transfer.Hash;
                    long payloadLen = -1;
                    bool munged = false;

                    if (looksHeadered)
                    {
                        (headerHash, payloadLen, munged) = await ReadCdnHeaderAsync(respStream, attemptToken).ConfigureAwait(false);

                        if (!string.Equals(headerHash, transfer.Hash, StringComparison.OrdinalIgnoreCase))
                            throw new InvalidDataException($"CDN staged-path: header hash mismatch for {transfer.Hash}, got {headerHash}");

                        if (payloadLen <= 0)
                            throw new InvalidDataException($"CDN staged-path: invalid payload length {payloadLen} for {transfer.Hash}");
                    }

                    var ext = fileExtByHash.TryGetValue(headerHash, out var e) ? e : "dat";
                    var finalPath = _fileDbManager.GetCacheFilePath(headerHash, ext);
                    EnsureDirectoryForFile(finalPath);

                    var tempPath = CreateTempDownloadPath(finalPath);
                    var compressedBytes = 0L;

                    var canKeepPayloadInMemory = looksHeadered
                        && payloadLen > 0
                        && payloadLen <= CdnInMemorySpoolMaxPayloadBytes
                        && inMemorySpoolBudget.TryReserve(payloadLen);

                    if (canKeepPayloadInMemory)
                    {
                        inMemoryBudgetReservation = payloadLen;
                        await using var payloadStream = new LimitedXorStream(respStream, payloadLen, munged ? (byte)42 : (byte)0);
                        (inMemoryPayload, inMemoryPayloadLength) = await ReadSmallPayloadToPooledBufferAsync(payloadStream, payloadLen, read =>
                        {
                            AddAttemptProgress(read);
                        }, attemptToken).ConfigureAwait(false);

                        compressedBytes = inMemoryPayloadLength;
                    }
                    else
                    {
                        spoolPath = tempPath + ".spool." + Guid.NewGuid().ToString("N");

                        await using (var spoolOut = new FileStream(
                            spoolPath,
                            new FileStreamOptions
                            {
                                Access = FileAccess.Write,
                                Mode = FileMode.Create,
                                Share = FileShare.None,
                                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
                                BufferSize = 256 * 1024
                            }))
                        {
                            if (looksHeadered)
                            {
                                await using var payloadStream = new LimitedXorStream(respStream, payloadLen, munged ? (byte)42 : (byte)0);
                                await CopyExactlyWithProgressAsync(payloadStream, spoolOut, payloadLen, read =>
                                {
                                    AddAttemptProgress(read);
                                }, attemptToken).ConfigureAwait(false);
                            }
                            else
                            {
                                await CopyToWithProgressAsync(respStream, spoolOut, read =>
                                {
                                    AddAttemptProgress(read);
                                }, attemptToken).ConfigureAwait(false);
                            }
                        }

                        var fi = new FileInfo(spoolPath);
                        if (!fi.Exists || fi.Length <= 0)
                            throw new InvalidDataException($"CDN staged-path: spooled payload was empty for {transfer.Hash}");

                        if (looksHeadered && payloadLen > 0 && fi.Length != payloadLen)
                            throw new InvalidDataException($"CDN staged-path: spooled payload size mismatch for {transfer.Hash} (expected {payloadLen}, got {fi.Length})");

                        compressedBytes = fi.Length;
                    }

                    if (compressedBytes <= 0)
                        throw new InvalidDataException($"CDN staged-path: spooled payload was empty for {transfer.Hash}");

                    var spooled = new CdnSpooledFile(
                        transfer,
                        headerHash,
                        spoolPath,
                        inMemoryPayload,
                        inMemoryPayloadLength,
                        inMemoryBudgetReservation,
                        inMemorySpoolBudget,
                        tempPath,
                        finalPath,
                        looksHeadered,
                        transfer.TotalRaw,
                        compressedBytes);

                    spooledFiles.Add(spooled);
                    decodeQueue.Add(spooled, ct);
                    Interlocked.Increment(ref downloadedStageCount[0]);

                    inMemoryPayload = null;
                    inMemoryPayloadLength = 0;
                    inMemoryBudgetReservation = 0;

                    _cdn404State.TryRemove(headerHash, out _);
                    _cdn404State.TryRemove(transfer.Hash, out _);

                    CommitAttemptFile();
                    return;
                }
                catch (OperationCanceledException oce) when (!ct.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);
                    CleanupAttemptSpool();
                    RollbackAttemptProgress();

                    Logger.LogWarning(oce,
                        "CDN staged-path: timeout while downloading {hash} from {url}, attempt {attempt}/{max}",
                        transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }

                    continue;
                }
                catch (OperationCanceledException)
                {
                    CleanupAttemptSpool();
                    RollbackAttemptProgress();
                    Interlocked.Exchange(ref anyFailure[0], 1);
                    throw;
                }
                catch (IOException ioEx)
                {
                    Logger.LogDebug(ioEx,
                        "CDN staged-path: IO error for {hash} (attempt {attempt}/{max})",
                        transfer.Hash, attempts, MaxCdnAttemptsPerFile);

                    CleanupAttemptSpool();
                    RollbackAttemptProgress();

                    try
                    {
                        var ext2 = fileExtByHash.TryGetValue(transfer.Hash, out var e2) ? e2 : "dat";
                        var finalPath2 = _fileDbManager.GetCacheFilePath(transfer.Hash, ext2);

                        if (File.Exists(finalPath2) && ValidateDownloadedFileLowPri(finalPath2, transfer.Hash, expectedRawSize: 0, ct))
                        {
                            PersistFileToStorageLowPri(transfer.Hash, finalPath2, ct);
                            results.Add(new CdnDownloadedFile(transfer.Hash, finalPath2));
                            Interlocked.Increment(ref downloadedStageCount[0]);

                            _cdn404State.TryRemove(transfer.Hash, out _);
                            AddGroupProgress(gameObjectHandler, groupKey, transfer.Total, 1);
                            return;
                        }
                    }
                    catch { /* ignore */ }

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }

                    continue;
                }
                catch (Exception ex)
                {
                    CleanupAttemptSpool();
                    RollbackAttemptProgress();

                    if (ex is OperationCanceledException || ex.GetBaseException() is OperationCanceledException)
                    {
                        if (ct.IsCancellationRequested)
                            throw;

                        Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);

                        Logger.LogDebug(
                            "CDN staged-path: cancelled while downloading {hash} from {url}, attempt {attempt}/{max}",
                            transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                        if (attempts >= MaxCdnAttemptsPerFile)
                        {
                            Interlocked.Exchange(ref anyFailure[0], 1);
                            return;
                        }

                        continue;
                    }

                    Logger.LogWarning(ex,
                        "CDN staged-path: error while downloading {hash} from {url}, attempt {attempt}/{max}",
                        transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }
                }
            }

            Interlocked.Exchange(ref anyFailure[0], 1);
            Logger.LogWarning(
                "CDN staged-path: all {max} attempts failed for {hash}, treating as failed for group {group}",
                MaxCdnAttemptsPerFile,
                transfer.Hash,
                groupKey);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Exchange(ref anyFailure[0], 1);
            throw;
        }
    }

    private async Task DecodeOneCdnSpoolAsync(CdnSpooledFile spooled, ConcurrentBag<CdnDownloadedFile> results, int[] anyFailure, int[] anySlow, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            string computed;
            long bytesWritten;
            bool usedHeaderlessXorCompat = false;

            if (spooled.IsHeadered)
            {
                await CdnDecodeConcurrencySemaphore.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    if (spooled.InMemoryPayload != null)
                    {
                        using var decodeSrc = new MemoryStream(spooled.InMemoryPayload, 0, spooled.InMemoryPayloadLength, writable: false, publiclyVisible: true);

                        (computed, bytesWritten) = await DecompressLz4ToFileAndHashAsync(
                            decodeSrc,
                            spooled.TempPath,
                            spooled.ExpectedRawSize,
                            ct).ConfigureAwait(false);
                    }
                    else
                    {
                        await using var decodeSrc = new FileStream(spooled.SpoolPath!, FileMode.Open, FileAccess.Read, FileShare.Read,
                            256 * 1024, FileOptions.SequentialScan | FileOptions.Asynchronous);

                        (computed, bytesWritten) = await DecompressLz4ToFileAndHashAsync(
                            decodeSrc,
                            spooled.TempPath,
                            spooled.ExpectedRawSize,
                            ct).ConfigureAwait(false);
                    }
                }
                finally
                {
                    CdnDecodeConcurrencySemaphore.Release();
                    CleanupSpooledPayload(spooled);
                }
            }
            else
            {
                (computed, bytesWritten, usedHeaderlessXorCompat) = await DecodeHeaderlessSpoolWithCompatAsync(
                    spooled.SpoolPath!,
                    spooled.TempPath,
                    spooled.ExpectedRawSize,
                    spooled.Hash,
                    ct).ConfigureAwait(false);
            }

            sw.Stop();

            if (usedHeaderlessXorCompat)
                Logger.LogDebug("CDN staged-path: headerless XOR compat path used for {hash}", spooled.Hash);

            if (bytesWritten >= (2 * 1024 * 1024) && sw.Elapsed.TotalSeconds >= 1.0)
            {
                var bps = (long)(bytesWritten / sw.Elapsed.TotalSeconds);
                if (bps < AutoCdnSlowBpsThreshold)
                    Interlocked.Exchange(ref anySlow[0], 1);
            }

            if (bytesWritten <= 0 || !string.Equals(computed, spooled.Hash, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogWarning(
                    "CDN staged-path: bad decoded content for {hash} (computed={computed}, bytes={bytes}); cleaning temp",
                    spooled.Hash, computed, bytesWritten);

                CleanupFailedCdnDownload(spooled.TempPath);
                Interlocked.Exchange(ref anyFailure[0], 1);
                return;
            }

            var requiresExtraValidation = string.Equals(Path.GetExtension(spooled.TempPath), ".mdl", StringComparison.OrdinalIgnoreCase);

            bool extraOk;
            try
            {
                if (!requiresExtraValidation)
                {
                    extraOk = true;
                }
                else
                {
                    await CdnValidationConcurrencySemaphore.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        extraOk = ExtraContentValidation(spooled.TempPath, spooled.Hash);
                    }
                    finally
                    {
                        CdnValidationConcurrencySemaphore.Release();
                    }
                }
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                extraOk = false;
            }

            if (!extraOk)
            {
                Logger.LogWarning("CDN staged-path: extra validation failed for {hash}, cleaning temp", spooled.Hash);
                CleanupFailedCdnDownload(spooled.TempPath);
                Interlocked.Exchange(ref anyFailure[0], 1);
                return;
            }

            try
            {
                File.Move(spooled.TempPath, spooled.FinalPath, overwrite: true);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "CDN staged-path: failed to finalize {hash} to {path}", spooled.Hash, spooled.FinalPath);
                CleanupFailedCdnDownload(spooled.TempPath);
                Interlocked.Exchange(ref anyFailure[0], 1);
                return;
            }

            results.Add(new CdnDownloadedFile(spooled.Hash, spooled.FinalPath));
        }
        catch (OperationCanceledException)
        {
            CleanupFailedCdnDownload(spooled.TempPath);
            Interlocked.Exchange(ref anyFailure[0], 1);
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "CDN staged-path: unexpected decode/finalize error for {hash}", spooled.Hash);
            CleanupFailedCdnDownload(spooled.TempPath);
            Interlocked.Exchange(ref anyFailure[0], 1);
        }
    }

    private void CleanupSpooledPayload(CdnSpooledFile spooled)
    {
        if (!string.IsNullOrWhiteSpace(spooled.SpoolPath))
        {
            try { if (File.Exists(spooled.SpoolPath)) File.Delete(spooled.SpoolPath); } catch { }
        }

        if (spooled.InMemoryPayload != null)
        {
            try { ArrayPool<byte>.Shared.Return(spooled.InMemoryPayload); } catch { }

            if (spooled.InMemoryBudgetReservation > 0)
                spooled.InMemoryBudget?.Release(spooled.InMemoryBudgetReservation);
        }
    }

    private void CleanupSpooledFiles(IEnumerable<CdnSpooledFile> spooledFiles)
    {
        foreach (var spooled in spooledFiles)
        {
            try { if (File.Exists(spooled.TempPath)) File.Delete(spooled.TempPath); } catch { }
            CleanupSpooledPayload(spooled);
        }
    }




    private bool ExtraContentValidation(string filePath, string hash)
    {
        try
        {
            var fi = new FileInfo(filePath);
            if (!fi.Exists)
            {
                Logger.LogWarning("ExtraValidation: {file} for {hash} does not exist after download", filePath, hash);
                return false;
            }

            try
            {
                _ = new MdlFile(filePath);
                return true;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex,
                    "ExtraValidation: MDL header parse failed for {file} ({hash}), treating as invalid",
                    filePath,
                    hash);
                return false;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "ExtraValidation: unexpected error while validating {file} ({hash})",
                filePath,
                hash);
            return false;
        }
    }

    private bool ValidateDownloadedFileLowPri(string fullPath, string fileHash, long expectedRawSize, CancellationToken ct)
    {
        try
        {
            const long InlineTinyValidationBytes = 256 * 1024;

            long sizeHint = expectedRawSize;
            if (sizeHint <= 0)
            {
                try
                {
                    var fi = new FileInfo(fullPath);
                    if (fi.Exists)
                        sizeHint = fi.Length;
                }
                catch
                {
                    // ignore and fall through to worker path
                }
            }

            if (sizeHint > 0 && sizeHint <= InlineTinyValidationBytes)
            {
                return ValidateDownloadedFile(fullPath, fileHash, expectedRawSize);
            }

            CdnValidationConcurrencySemaphore.Wait(ct);
            try
            {
                return ValidateDownloadedFile(fullPath, fileHash, expectedRawSize);
            }
            finally
            {
                CdnValidationConcurrencySemaphore.Release();
            }
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch
        {
            // Conservative: treat failures as invalid so we force a clean redownload.
            return false;
        }
    }
    private bool ValidateDownloadedFile(string fullPath, string fileHash, long expectedRawSize)
    {
        // Conservative: IO errors should not silently "pass" validation, because that allows partial/corrupt files
        // to persist and potentially crash-loop on apply.
        try
        {
            var fi = new FileInfo(fullPath);

            if (!fi.Exists)
            {
                Logger.LogWarning(
                    "Validation failed for {hash}: file does not exist at {path}",
                    fileHash, fullPath);
                return false;
            }

            if (fi.Length == 0)
            {
                Logger.LogWarning(
                    "Validation failed for {hash}: zero-length file at {path}",
                    fileHash, fullPath);
                return false;
            }

            if (expectedRawSize > 0 && fi.Length != expectedRawSize)
            {
                Logger.LogWarning(
                    "Validation failed for {hash}: size mismatch, expected {expected} but got {actual} at {path}",
                    fileHash, expectedRawSize, fi.Length, fullPath);
                return false;
            }

            string computed;
            const int MaxIoRetries = 2;
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    computed = Crypto.GetFileHash(fi.FullName);
                    break;
                }
                catch (IOException ioEx) when (attempt < MaxIoRetries)
                {
                    Logger.LogDebug(ioEx, "IO error while hashing {path} (attempt {attempt}/{max})", fullPath, attempt + 1, MaxIoRetries + 1);
                    Thread.Sleep(50 * (attempt + 1));
                    continue;
                }
            }

            if (!string.Equals(computed, fileHash, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogWarning(
                    "Validation failed for {hash}: content hash mismatch, got {actual} for {path}",
                    fileHash, computed, fullPath);
                return false;
            }

            return true;
        }
        catch (IOException ioEx)
        {
            Logger.LogWarning(
                ioEx,
                "IO error during validation of {hash} at {path}; treating as invalid to force clean re-download",
                fileHash, fullPath);
            return false;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Validation threw for {hash} at {path}", fileHash, fullPath);
            return false;
        }
    }


    private int GetDownloadGroupParallelism(int groupCount)
    {
        var max = Math.Min(Environment.ProcessorCount, 6);
        var configured = _mareConfigService.Current.ParallelDownloads;

        if (configured > 0)
            return Math.Clamp(Math.Min(configured, groupCount), 2, max);

        var cpu = Environment.ProcessorCount;
        var auto =
            cpu <= 4 ? 2 :
            cpu <= 8 ? 3 :
            cpu <= 16 ? 4 :
                        6;

        return Math.Clamp(Math.Min(auto, groupCount), 2, max);
    }



    private void ReleaseInflight(IEnumerable<DownloadFileTransfer> transfers)
    {
        foreach (var t in transfers)
        {
            try { CompleteGlobalInflightDownload(t.Hash, success: false); }
            catch { /* ignore */ }
        }
    }



    private sealed record CdnDownloadedFile(string Hash, string FinalPath);
    private sealed record CdnSpooledFile(
        DownloadFileTransfer Transfer,
        string Hash,
        string? SpoolPath,
        byte[]? InMemoryPayload,
        int InMemoryPayloadLength,
        long InMemoryBudgetReservation,
        CdnInMemorySpoolBudget? InMemoryBudget,
        string TempPath,
        string FinalPath,
        bool IsHeadered,
        long ExpectedRawSize,
        long CompressedBytes);

    sealed class LimitedXorStream : Stream
    {
        private readonly Stream _src;
        private long _remaining;
        private readonly byte _xorKey;

        public LimitedXorStream(Stream src, long length, byte xorKey = 42)
        {
            _src = src;
            _remaining = length;
            _xorKey = xorKey;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _remaining;
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_remaining <= 0) return 0;
            var toRead = (int)Math.Min(count, _remaining);
            var read = _src.Read(buffer, offset, toRead);
            if (read > 0)
            {
                if (_xorKey != 0)
                    for (int i = 0; i < read; i++) buffer[offset + i] ^= _xorKey;
                _remaining -= read;
            }
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_remaining <= 0) return 0;
            var toRead = (int)Math.Min(buffer.Length, _remaining);
            var dst = buffer.Slice(0, toRead);
            var read = await _src.ReadAsync(dst, ct).ConfigureAwait(false);
            if (read > 0)
            {
                if (_xorKey != 0)
                {
                    var span = dst.Span.Slice(0, read);
                    for (int i = 0; i < span.Length; i++) span[i] ^= _xorKey;
                }
                _remaining -= read;
            }
            return read;
        }

        public override long Seek(long o, SeekOrigin so) => throw new NotSupportedException();
        public override void SetLength(long v) => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => throw new NotSupportedException();
    }

    private sealed class FullReadStream : Stream
    {
        private readonly Stream _inner;

        public FullReadStream(Stream inner)
        {
            _inner = inner;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var total = 0;

            while (total < count)
            {
                var read = _inner.Read(buffer, offset + total, count - total);
                if (read <= 0)
                    break;

                total += read;
            }

            return total;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var total = 0;

            while (total < buffer.Length)
            {
                var read = await _inner.ReadAsync(buffer.Slice(total), cancellationToken).ConfigureAwait(false);
                if (read <= 0)
                    break;

                total += read;
            }

            return total;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

}
