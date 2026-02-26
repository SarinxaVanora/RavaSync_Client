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
using System.Net.Mail;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using System.Collections.Concurrent;

namespace RavaSync.WebAPI.Files;

public partial class FileDownloadManager : DisposableMediatorSubscriberBase
{
    private readonly Dictionary<string, FileDownloadStatus> _downloadStatus;
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
    private readonly DelayedActivatorService _delayedActivator;
    private const int MaxCdnAttemptsPerFile = 3;
    private static readonly TimeSpan CdnAttemptTimeout = TimeSpan.FromSeconds(60);

    private readonly CdnDecodeWorker _cdnDecodeWorker;

    private readonly SemaphoreSlim _cdnDecodeGate = new(GetCdnDecodeParallelism(), GetCdnDecodeParallelism());

    private const int AutoCdnMinParallel = 2;
    private const int AutoCdnMaxParallel = 12;

    private const long AutoCdnSlowBpsThreshold = 768 * 1024;

    private int _autoCdnParallel = 4;


    private static int GetCdnDecodeParallelism()
    {
        // We run inside the game process, so don't use all cores.
        // This caps only the expensive local work (decompress/hash/write), not HTTP fetching.
        var cpu = Environment.ProcessorCount;

        // Conservative defaults for frame pacing:
        // - 4–8 logical cores: 1 decode worker
        // - 10–16 logical cores: 2 decode workers
        // - 18+ logical cores: 3 decode workers
        //
        // Cap at 3 on purpose; beyond this, frame-time regressions usually outweigh gains.
        if (cpu <= 8) return 1;
        if (cpu <= 16) return 2;
        return 3;
    }

    private int GetCdnParallelTarget(int configuredParallel, int filesInGroup)
    {
        var maxParallel = AutoCdnMaxParallel;

        // During room-entry storms, clamp parallelism to reduce IO/CPU spikes.
        if (SyncStorm.IsActive)
            maxParallel = Math.Min(maxParallel, 4);

        if (configuredParallel > 0)
            return Math.Clamp(configuredParallel, 1, Math.Min(maxParallel, Math.Max(1, filesInGroup)));

        var p = Volatile.Read(ref _autoCdnParallel);
        p = Math.Clamp(p, AutoCdnMinParallel, maxParallel);
        return Math.Clamp(p, 1, Math.Max(1, filesInGroup));

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
                var step = cur < 6 ? 2 : 1;
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

    // per-hash inflight dedupe (across pairs) so the same hash doesn't spawn duplicated bars
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _inflightHashes
        = new(StringComparer.OrdinalIgnoreCase);
    
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Cdn404State> _cdn404State
        = new(StringComparer.OrdinalIgnoreCase);

    private sealed class Cdn404State
    {
        public DateTime FirstSeenUtc;
        public DateTime LastSeenUtc;
        public int Count;
        public DateTime LastSelfHealUtc;
    }

    public FileDownloadManager(ILogger<FileDownloadManager> logger, MareMediator mediator,
        FileTransferOrchestrator orchestrator,
        FileCacheManager fileCacheManager, FileCompactor fileCompactor, MareConfigService mareConfigService, DelayedActivatorService delayedActivator) : base(logger, mediator)
    {
        _downloadStatus = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);
        _orchestrator = orchestrator;
        _fileDbManager = fileCacheManager;
        _fileCompactor = fileCompactor;
        _activeDownloadStreams = [];
        _mareConfigService = mareConfigService;
        _delayedActivator = delayedActivator;
        _cdnFastClient = CreateCdnFastClient();
        _cdnDecodeWorker = new CdnDecodeWorker(
            Logger,
            GetCdnDecodeParallelism(),
            (stream, destPath, expectedRaw, ct) =>
            {

                return DecompressLz4ToFileAndHashAsync(stream, destPath, expectedRaw, ct).GetAwaiter().GetResult();
            });

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
            ConnectTimeout = TimeSpan.FromSeconds(10),
            MaxConnectionsPerServer = 256,
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
        _downloadStatus.Clear();
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
            try { _cdnDecodeWorker.Dispose(); } catch { /* ignore */ }
            try { _cdnFastClient.Dispose(); } catch { /* ignore */ }
        }

        base.Dispose(disposing);
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
        // - skip staged hashes (already queued/applied)
        // - skip hashes already pending in delayed activation (queued but not applied yet)
        // - use filesystem as truth for presence + validate cache entries against expected sizes
        // - DO NOT mark hashes inflight unless they are actually transferable (dto.FileExists=true)
        // - self-heal stray on-disk files without DB entries when possible
        var candidates = downloadFileInfoFromService
            .GroupBy(d => d.Hash, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .Where(dto => !_fileDbManager.IsHashStaged(dto.Hash))
            .Where(dto => !_delayedActivator.IsHashPending(dto.Hash))
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
                        // Quick length gate if we know the expected size.
                        if (expected > 0)
                        {
                            try
                            {
                                var len = new FileInfo(resolved).Length;
                                if (len == expected)
                                {
                                    Logger.LogTrace("Skip download for {Hash}: present on disk with matching length at {Path}", dto.Hash, resolved);
                                    return false;
                                }
                            }
                            catch
                            {
                                // fall through to full validation
                            }
                        }

                        // Full validation: if it validates, we can skip; otherwise force redownload.
                        if (ValidateDownloadedFileLowPri(resolved, dto.Hash, expectedRawSize: expected, ct))
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

        // mark inflight to prevent duplicate progress bars ONLY for transferable items
        CurrentDownloads = transferable
            .Where(t => _inflightHashes.TryAdd(t.Hash, (byte)0))
            .ToList();

        return CurrentDownloads;

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
            Logger.LogError("CDN fast-path: one or more groups failed. Aborting download. Failed groups: {groups}",
                string.Join(", ", failedGroups.Distinct(StringComparer.Ordinal)));

            throw new HttpRequestException("CDN fast-path failed for groups: " + string.Join(", ", failedGroups.Distinct(StringComparer.Ordinal)));
        }

        Logger.LogDebug("Download end: {id}", gameObjectHandler);

        ClearDownload();
    }


    private async Task<List<DownloadFileDto>> FilesGetSizes(List<string> hashes, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        var response = await _orchestrator.SendRequestAsync(HttpMethod.Get, MareFiles.ServerFilesGetSizesFullPath(_orchestrator.UploadCdnUri!), hashes, ct).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<List<DownloadFileDto>>(cancellationToken: ct).ConfigureAwait(false) ?? [];
    }


    private void PersistFileToStorageLowPri(string fileHash, string filePath, CancellationToken ct)
    {
        try
        {
            _ = _cdnDecodeWorker.EnqueueWorkAsync(_ =>
            {
                PersistFileToStorage(fileHash, filePath);
                return true;
            }, ct);
        }
        catch
        {
            // best effort
        }
    }

    private void PersistFileToStorage(string fileHash, string filePath)
    {
        var fi = new FileInfo(filePath);

        DateTime start = new(1995, 1, 1, 1, 1, 1, DateTimeKind.Local);
        int range = Math.Max(1, (DateTime.Today - start).Days);

        DateTime RandomDayInThePast()
        {
            return start.AddDays(Random.Shared.Next(range));
        }

        fi.CreationTime = RandomDayInThePast();
        fi.LastAccessTime = DateTime.Today;
        fi.LastWriteTime = RandomDayInThePast();

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
                // Orchestrator/HttpClient was torn down—ignore.
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
                    // Orchestrator/HttpClient was torn down—ignore.
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

    private void CleanupFailedCdnDownload(string pathToDelete, string? stagedHash = null)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(pathToDelete) && File.Exists(pathToDelete))
                File.Delete(pathToDelete);
        }
        catch { /* ignore */ }

        if (!string.IsNullOrWhiteSpace(stagedHash))
        {
            try { _fileDbManager.UnstageFile(stagedHash); } catch { /* ignore */ }
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
    public bool TryResolveHardDelayedPath(string hash, string finalCachePath, out string resolvedPath)
    {
        resolvedPath = finalCachePath;

        if (string.IsNullOrWhiteSpace(hash)) return false;
        if (string.IsNullOrWhiteSpace(finalCachePath)) return false;

        if (!ActivationPolicy.IsHardDelayed(finalCachePath))
            return false;

        if (File.Exists(finalCachePath))
            return true;

        var ext = Path.GetExtension(finalCachePath);
        var quarantinePath = Path.Combine(_delayedActivator.QuarantineRoot, hash.ToUpperInvariant() + ext);

        if (File.Exists(quarantinePath))
        {
            try
            {
                if (new FileInfo(quarantinePath).Length < 16)
                {
                    Logger.LogWarning("Quarantine file is zero-byte, deleting and forcing redownload: {file}", quarantinePath);
                    try { File.Delete(quarantinePath); } catch { }
                    return false;
                }
            }
            catch
            {
                return false;
            }

            resolvedPath = quarantinePath;
            return true;
        }

        return false;
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
        var buffer = _downloadBufferPool.Rent(256 * 1024);
        try
        {
            await using var outFs = new FileStream(destPath, outOpts);
            await using var lz4 = new LZ4Stream(input, LZ4StreamMode.Decompress, LZ4StreamFlags.None, Lz4BlockSize);

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


    private async Task<(string ComputedHash, long BytesWritten, bool UsedXorCompat)> DecodeHeaderlessSpoolWithCompatAsync(
        string spoolPath,
        string tempPath,
        long expectedRawSize,
        string expectedHash,
        CancellationToken ct)
    {
        var (hash, bytes, usedXor) = await _cdnDecodeWorker.EnqueueDecodeWithCompatAsync(
            spoolPath,
            tempPath,
            expectedRawSize,
            expectedHashUpper: expectedHash,
            ct).ConfigureAwait(false);

        return (hash, bytes, usedXor);
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

            // 0 => Auto (adaptive)
            var cdnParallel = GetCdnParallelTarget(configured, transfers.Count);
            cdnParallel = Math.Min(cdnParallel, transfers.Count);

            var anyFailure = new int[1];
            var anyTimeoutOrBackoff = new int[1];
            var anySlow = new int[1];

            var results = new ConcurrentBag<CdnDownloadedFile>();
            var groupKey = fileGroup.Key;

            if (_downloadStatus.TryGetValue(groupKey, out var statusStart))
            {
                lock (statusStart)
                {
                    statusStart.DownloadStatus = DownloadStatus.Downloading;
                }
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

                        await DownloadOneFromCdnAsync(
                            gameObjectHandler,
                            groupKey,
                            fileExtByHash,
                            results,
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
            }
            catch (OperationCanceledException)
            {
                Interlocked.Exchange(ref anyFailure[0], 1);
                throw;
            }

            var hadTimeout = Volatile.Read(ref anyTimeoutOrBackoff[0]) != 0;
            var hadSlowLink = Volatile.Read(ref anySlow[0]) != 0;

            if (Volatile.Read(ref anyFailure[0]) != 0)
            {
                Logger.LogDebug("CDN fast-path: at least one file failed, falling back to blk pipeline for group {group}", groupKey);
                UpdateAutoCdnParallel(cdnParallel, success: false, hadTimeoutOrBackoff: hadTimeout, hadSlow: hadSlowLink);
                return false;
            }

            foreach (var r in results)
            {
                if (r.DestPath == r.FinalPath)
                {
                    PersistFileToStorageLowPri(r.Hash, r.FinalPath, ct);
                }
                else
                {
                    _delayedActivator.Enqueue(new PendingFile(
                        r.DestPath,
                        r.FinalPath,
                        r.Hash,
                        r.Hard,
                        r.Soft,
                        gameObjectHandler?.Address
                    ));
                }
            }

            if (_downloadStatus.TryGetValue(groupKey, out var status))
            {
                lock (status)
                {
                    status.DownloadStatus = DownloadStatus.Decompressing;
                }
            }

            Logger.LogDebug("CDN fast-path: completed all files for group {group} via CDN", groupKey);
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




    private async Task DownloadOneFromCdnAsync(GameObjectHandler gameObjectHandler,string groupKey,Dictionary<string, string> fileExtByHash,ConcurrentBag<CdnDownloadedFile> results,DownloadFileTransfer transfer,int[] anyFailure,int[] anyTimeoutOrBackoff,int[] anySlow,CancellationToken ct)
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

                Logger.LogDebug("CDN fast-path: downloading {hash} from {url} (attempt {attempt}/{max})",
                    transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                try
                {
                    using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    attemptCts.CancelAfter(CdnAttemptTimeout);
                    var attemptToken = attemptCts.Token;

                    var sw = Stopwatch.StartNew();

                    using var response = await _cdnFastClient
                        .GetAsync(cdnUrl, HttpCompletionOption.ResponseHeadersRead, attemptToken)
                        .ConfigureAwait(false);
                    if (!response.IsSuccessStatusCode)
                    {
                        if (response.StatusCode == HttpStatusCode.NotFound && !isCacheBusted &&
                            TryPlanCdn404SelfHeal(transfer.Hash, baseCdnUrl, response,
                                out var cacheBustUrl, out var cfCacheStatus, out var age, out var cfRay, out var reason))
                        {
                            plannedCdnUrl = cacheBustUrl;

                            Logger.LogDebug(
                                "CDN fast-path: 404 for {hash} ({cfCacheStatus}, age {age}); self-heal retry via cache-bust: {url} ({reason})",
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

                    Stream? input = null;
                    if (looksHeadered)
                    {
                        (headerHash, payloadLen, munged) = await ReadCdnHeaderAsync(respStream, attemptToken).ConfigureAwait(false);

                        if (!string.Equals(headerHash, transfer.Hash, StringComparison.OrdinalIgnoreCase))
                            throw new InvalidDataException($"CDN fast-path: header hash mismatch for {transfer.Hash}, got {headerHash}");

                        if (payloadLen <= 0)
                            throw new InvalidDataException($"CDN fast-path: invalid payload length {payloadLen} for {transfer.Hash}");

                        input = new LimitedXorStream(respStream, payloadLen, munged ? (byte)42 : (byte)0);
                    }

                    var ext = fileExtByHash.TryGetValue(headerHash, out var e) ? e : "dat";
                    var filePath = _fileDbManager.GetCacheFilePath(headerHash, ext);

                    var cfg = _mareConfigService.Current;
                    var hard = ActivationPolicy.IsHardDelayed(filePath);
                    var soft = !hard && ActivationPolicy.IsSoftDelayed(filePath);

                    Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);

                    // CRITICAL: write to temp first so we never truncate/zero the live cache file.
                    var tempPath = CreateTempDownloadPath(filePath);
                    var destPath = filePath;

                    string? stagedHash = null;

                    try
                    {
                        string computed;
                        long bytesWritten;
                        bool usedHeaderlessXorCompat = false;

                        var spoolPath = tempPath + ".spool." + Guid.NewGuid().ToString("N");

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
                                await CopyExactlyAsync(respStream, spoolOut, payloadLen, attemptToken).ConfigureAwait(false);
                            }
                            else
                            {
                                await respStream.CopyToAsync(spoolOut, 256 * 1024, attemptToken).ConfigureAwait(false);
                            }

                            await spoolOut.FlushAsync(attemptToken).ConfigureAwait(false);
                        }

                        sw.Stop();
                        sw = Stopwatch.StartNew();

                        if (looksHeadered)
                        {
                            (computed, bytesWritten) = await _cdnDecodeWorker.EnqueueAsync(
                                spoolPath,
                                tempPath,
                                transfer.TotalRaw,
                                expectedHashUpper: headerHash,
                                xorMunged: munged,
                                attemptToken).ConfigureAwait(false);
                        }
                        else
                        {
                            (computed, bytesWritten, usedHeaderlessXorCompat) =
                                await DecodeHeaderlessSpoolWithCompatAsync(
                                    spoolPath,
                                    tempPath,
                                    transfer.TotalRaw,
                                    transfer.Hash,
                                    attemptToken).ConfigureAwait(false);
                        }

                        if (usedHeaderlessXorCompat)
                        {
                            Logger.LogDebug("CDN fast-path: headerless XOR compat path used for {hash}", transfer.Hash);
                        }

                        sw.Stop();

                        if (bytesWritten >= (2 * 1024 * 1024) && sw.Elapsed.TotalSeconds >= 1.0)
                        {
                            var bps = (long)(bytesWritten / sw.Elapsed.TotalSeconds);
                            if (bps < AutoCdnSlowBpsThreshold)
                                Interlocked.Exchange(ref anySlow[0], 1);
                        }

                        if (bytesWritten <= 0 || !string.Equals(computed, headerHash, StringComparison.OrdinalIgnoreCase))
                        {
                            Logger.LogWarning(
                                "CDN fast-path: bad content for {hash} (computed={computed}, bytes={bytes}); cleaning temp and retrying",
                                headerHash, computed, bytesWritten);

                            CleanupFailedCdnDownload(tempPath, stagedHash);

                            if (attempts >= MaxCdnAttemptsPerFile)
                            {
                                Interlocked.Exchange(ref anyFailure[0], 1);
                                return;
                            }

                            continue;
                        }
                    }
                    catch (OperationCanceledException oce) when (!ct.IsCancellationRequested)
                    {
                        // Per-attempt timeout: never leave partial files behind.
                        Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);
                        CleanupFailedCdnDownload(tempPath, stagedHash);

                        Logger.LogWarning(oce,
                            "CDN fast-path: timeout while downloading {hash} from {url}, attempt {attempt}/{max}",
                            transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                        if (attempts >= MaxCdnAttemptsPerFile)
                        {
                            Interlocked.Exchange(ref anyFailure[0], 1);
                            return;
                        }

                        continue;
                    }
                    catch (Exception ex)
                    {
                        CleanupFailedCdnDownload(tempPath, stagedHash);

                        Logger.LogWarning(ex,
                            "CDN fast-path: error while downloading {hash} from {url}, attempt {attempt}/{max}",
                            transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                        if (attempts >= MaxCdnAttemptsPerFile)
                        {
                            Interlocked.Exchange(ref anyFailure[0], 1);
                            return;
                        }

                        continue;
                    }

                    bool extraOk;
                    try
                    {
                        extraOk = await _cdnDecodeWorker.EnqueueWorkAsync(
                            _ => ExtraContentValidation(tempPath, headerHash),
                            attemptToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        extraOk = false;
                    }

                    if (!extraOk)
                    {
                        Logger.LogWarning("CDN fast-path: extra validation failed for {hash}, cleaning temp and retrying", headerHash);

                        CleanupFailedCdnDownload(tempPath, stagedHash);

                        if (attempts >= MaxCdnAttemptsPerFile)
                        {
                            Interlocked.Exchange(ref anyFailure[0], 1);
                            return;
                        }

                        continue;
                    }

                    if (cfg.DelayActivationEnabled && hard)
                    {
                        destPath = Path.Combine(_delayedActivator.QuarantineRoot, headerHash + Path.GetExtension(filePath));
                        Directory.CreateDirectory(Path.GetDirectoryName(destPath)!);

                        stagedHash = headerHash;
                        _fileDbManager.StageFile(headerHash, filePath);

                        try
                        {
                            File.Move(tempPath, destPath, overwrite: true);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "CDN fast-path: failed to move {hash} into quarantine {path}", headerHash, destPath);

                            CleanupFailedCdnDownload(tempPath, stagedHash);
                            CleanupFailedCdnDownload(destPath, stagedHash);

                            if (attempts >= MaxCdnAttemptsPerFile)
                            {
                                Interlocked.Exchange(ref anyFailure[0], 1);
                                return;
                            }

                            continue;
                        }
                    }
                    else
                    {
                        try
                        {
                            File.Move(tempPath, filePath, overwrite: true);
                        }
                        catch (Exception ex) when (hard && IsFileInUse(ex))
                        {
                            var quarantinePath = Path.Combine(_delayedActivator.QuarantineRoot, headerHash + Path.GetExtension(filePath));
                            Directory.CreateDirectory(Path.GetDirectoryName(quarantinePath)!);

                            stagedHash = headerHash;
                            _fileDbManager.StageFile(headerHash, filePath);

                            try
                            {
                                File.Move(tempPath, quarantinePath, overwrite: true);
                                destPath = quarantinePath;
                            }
                            catch (Exception ex2)
                            {
                                Logger.LogWarning(ex2,
                                    "CDN fast-path: failed to move {hash} into quarantine fallback {path}",
                                    headerHash, quarantinePath);

                                CleanupFailedCdnDownload(tempPath, stagedHash);
                                CleanupFailedCdnDownload(quarantinePath, stagedHash);

                                if (attempts >= MaxCdnAttemptsPerFile)
                                {
                                    Interlocked.Exchange(ref anyFailure[0], 1);
                                    return;
                                }

                                continue;
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "CDN fast-path: failed to finalize {hash} to {path}", headerHash, filePath);

                            CleanupFailedCdnDownload(tempPath, stagedHash);

                            if (attempts >= MaxCdnAttemptsPerFile)
                            {
                                Interlocked.Exchange(ref anyFailure[0], 1);
                                return;
                            }

                            continue;
                        }
                    }

                    _cdn404State.TryRemove(headerHash, out _);
                    _cdn404State.TryRemove(transfer.Hash, out _);

                    results.Add(new CdnDownloadedFile(headerHash, filePath, destPath, hard, soft));

                    if (_downloadStatus.TryGetValue(groupKey, out var st))
                    {
                        lock (st)
                        {
                            st.TransferredFiles++;
                            st.TransferredBytes += transfer.Total;
                        }
                    }

                    return;
                }
                catch (OperationCanceledException oce) when (!ct.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref anyTimeoutOrBackoff[0], 1);

                    Logger.LogWarning(oce,
                        "CDN fast-path: timed out for {hash} (attempt {attempt}/{max})",
                        transfer.Hash, attempts, MaxCdnAttemptsPerFile);

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }

                    continue;
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Exchange(ref anyFailure[0], 1);
                    throw;
                }
                catch (IOException ioEx)
                {
                    Logger.LogDebug(ioEx,
                        "CDN fast-path: IO error for {hash} (attempt {attempt}/{max})",
                        transfer.Hash, attempts, MaxCdnAttemptsPerFile);

                    try
                    {
                        var ext2 = fileExtByHash.TryGetValue(transfer.Hash, out var e2) ? e2 : "dat";
                        var finalPath2 = _fileDbManager.GetCacheFilePath(transfer.Hash, ext2);

                        if (File.Exists(finalPath2) && ValidateDownloadedFileLowPri(finalPath2, transfer.Hash, expectedRawSize: 0, ct))
                        {
                            PersistFileToStorageLowPri(transfer.Hash, finalPath2, ct);

                            _cdn404State.TryRemove(transfer.Hash, out _);

                            if (_downloadStatus.TryGetValue(groupKey, out var st2))
                            {
                                lock (st2)
                                {
                                    st2.TransferredFiles++;
                                    st2.TransferredBytes += transfer.Total;
                                }
                            }

                            try
                            {
                                if (_fileDbManager.IsHashStaged(transfer.Hash) && !_delayedActivator.IsHashPending(transfer.Hash))
                                    _fileDbManager.UnstageFile(transfer.Hash);
                            }
                            catch { /* ignore */ }

                            return;
                        }
                    }
                    catch { /* ignore */ }

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);

                        try
                        {
                            if (_fileDbManager.IsHashStaged(transfer.Hash) && !_delayedActivator.IsHashPending(transfer.Hash))
                                _fileDbManager.UnstageFile(transfer.Hash);
                        }
                        catch { /* ignore */ }

                        return;
                    }

                    continue;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "CDN fast-path: unexpected error for {hash} (attempt {attempt}/{max})",
                        transfer.Hash, attempts, MaxCdnAttemptsPerFile);

                    if (attempts >= MaxCdnAttemptsPerFile)
                    {
                        Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }
                }
            }

            Interlocked.Exchange(ref anyFailure[0], 1);
            Logger.LogWarning(
                "CDN fast-path: all {max} attempts failed for {hash}, treating as failed for group {group}",
                MaxCdnAttemptsPerFile,
                transfer.Hash,
                groupKey);
        }
        finally
        {
            _inflightHashes.TryRemove(transfer.Hash, out _);
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

            if (fi.Length < 16)
            {
                Logger.LogWarning("ExtraValidation: {file} for {hash} is too small ({len} bytes)", filePath, hash, fi.Length);
                return false;
            }

            var ext = fi.Extension;
            if (ext.Equals(".mdl", StringComparison.OrdinalIgnoreCase))
            {

                try
                {
                    _ = new MdlFile(filePath);
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

            return true;
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
            // Run validation + hashing on the low-priority decode worker threads.
            // We block the caller, but the heavy work is off-thread and less likely to steal frame time.
            return _cdnDecodeWorker.EnqueueWorkAsync(
                _ => ValidateDownloadedFile(fullPath, fileHash, expectedRawSize),
                ct).GetAwaiter().GetResult();
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
            try { _inflightHashes.TryRemove(t.Hash, out _); }
            catch { /* ignore */ }
        }
    }



    private sealed class CdnDecodeWorker : IDisposable
    {
        private readonly ILogger _logger;
        private readonly ConcurrentQueue<DecodeJob> _q = new();
        private readonly ConcurrentQueue<DecodeCompatJob> _compatQ = new();
        private readonly ConcurrentQueue<WorkJob> _workQ = new();
        private readonly SemaphoreSlim _signal = new(0, int.MaxValue);
        private readonly CancellationTokenSource _cts = new();
        private readonly List<Thread> _threads = new();

        private sealed record DecodeJob(
            string SpoolPath,
            string TempOutPath,
            long ExpectedRawSize,
            string ExpectedHashUpper,
            bool XorMunged,
            TaskCompletionSource<(string HashUpper, long BytesWritten)> Tcs);

        private sealed record DecodeCompatJob(
            string SpoolPath,
            string TempOutPath,
            long ExpectedRawSize,
            string ExpectedHashUpper,
            TaskCompletionSource<(string HashUpper, long BytesWritten, bool UsedXorCompat)> Tcs);

        private sealed record WorkJob(
            Func<CancellationToken, bool> Work,
            TaskCompletionSource<bool> Tcs);

        public CdnDecodeWorker(
            ILogger logger,
            int workers,
            Func<Stream, string, long, CancellationToken, (string HashUpper, long BytesWritten)> decompressFromWorker)
        {
            _logger = logger;
            _decompressFromWorker = decompressFromWorker ?? throw new ArgumentNullException(nameof(decompressFromWorker));

            workers = Math.Clamp(workers, 1, 3);

            for (int i = 0; i < workers; i++)
            {
                var t = new Thread(() => WorkerLoop(_cts.Token))
                {
                    IsBackground = true,
                    Name = $"RavaSync.CdnDecodeWorker.{i}",
                    Priority = ThreadPriority.BelowNormal
                };
                _threads.Add(t);
                t.Start();
            }
        }

        public Task<bool> EnqueueWorkAsync(Func<CancellationToken, bool> work, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (ct.CanBeCanceled)
            {
                ct.Register(() => tcs.TrySetCanceled(ct));
            }

            _workQ.Enqueue(new WorkJob(work, tcs));
            _signal.Release();
            return tcs.Task;
        }

        public Task<(string HashUpper, long BytesWritten)> EnqueueAsync(string spoolPath,string tempOutPath,long expectedRawSize,string expectedHashUpper,bool xorMunged,CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<(string HashUpper, long BytesWritten)>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            if (ct.CanBeCanceled)
            {
                ct.Register(() => tcs.TrySetCanceled(ct));
            }

            _q.Enqueue(new DecodeJob(spoolPath, tempOutPath, expectedRawSize, expectedHashUpper, xorMunged, tcs));
            _signal.Release();
            return tcs.Task;
        }

        public Task<(string HashUpper, long BytesWritten, bool UsedXorCompat)> EnqueueDecodeWithCompatAsync(string spoolPath,string tempOutPath,long expectedRawSize,string expectedHashUpper,CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<(string HashUpper, long BytesWritten, bool UsedXorCompat)>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            if (ct.CanBeCanceled)
                ct.Register(() => tcs.TrySetCanceled(ct));

            _compatQ.Enqueue(new DecodeCompatJob(spoolPath, tempOutPath, expectedRawSize, expectedHashUpper, tcs));
            _signal.Release();
            return tcs.Task;
        }

        private void WorkerLoop(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    _signal.Wait(ct);

                    while (!ct.IsCancellationRequested)
                    {
                        if (_q.TryDequeue(out var job))
                        {
                            if (job.Tcs.Task.IsCanceled) continue;

                            try
                            {
                                using var src = new FileStream(job.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                    256 * 1024, FileOptions.SequentialScan);

                                Stream input = src;
                                if (job.XorMunged)
                                    input = new LimitedXorStream(src, src.Length, 42);

                                var res = _decompressFromWorker(input, job.TempOutPath, job.ExpectedRawSize, ct);
                                job.Tcs.TrySetResult(res);
                            }
                            catch (OperationCanceledException oce)
                            {
                                job.Tcs.TrySetCanceled(oce.CancellationToken);
                            }
                            catch (Exception ex)
                            {
                                job.Tcs.TrySetException(ex);
                            }
                            finally
                            {
                                try { if (File.Exists(job.SpoolPath)) File.Delete(job.SpoolPath); } catch { /* ignore */ }
                            }

                            continue;
                        }
                        if (_compatQ.TryDequeue(out var cj))
                        {
                            if (cj.Tcs.Task.IsCanceled) continue;

                            try
                            {
                                // Attempt 1: plain headerless
                                (string HashUpper, long BytesWritten) res1;
                                using (var src1 = new FileStream(
                                    cj.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                    256 * 1024, FileOptions.SequentialScan))
                                {
                                    res1 = _decompressFromWorker(src1, cj.TempOutPath, cj.ExpectedRawSize, ct);
                                }

                                if (res1.BytesWritten > 0 &&
                                    string.Equals(res1.HashUpper, cj.ExpectedHashUpper, StringComparison.OrdinalIgnoreCase))
                                {
                                    cj.Tcs.TrySetResult((res1.HashUpper, res1.BytesWritten, false));
                                    continue;
                                }

                                // Attempt 2: XOR(42) fallback
                                try { if (File.Exists(cj.TempOutPath)) File.Delete(cj.TempOutPath); } catch { /* ignore */ }

                                (string HashUpper, long BytesWritten) res2;
                                using (var src2 = new FileStream(
                                    cj.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                    256 * 1024, FileOptions.SequentialScan))
                                using (var xor = new LimitedXorStream(src2, src2.Length, 42))
                                {
                                    res2 = _decompressFromWorker(xor, cj.TempOutPath, cj.ExpectedRawSize, ct);
                                }

                                cj.Tcs.TrySetResult((res2.HashUpper, res2.BytesWritten, true));
                            }
                            catch (OperationCanceledException oce)
                            {
                                cj.Tcs.TrySetCanceled(oce.CancellationToken);
                            }
                            catch (Exception ex)
                            {
                                cj.Tcs.TrySetException(ex);
                            }
                            finally
                            {
                                try { if (File.Exists(cj.SpoolPath)) File.Delete(cj.SpoolPath); } catch { /* ignore */ }
                            }

                            continue;
                        }

                        if (_workQ.TryDequeue(out var w))
                        {
                            if (w.Tcs.Task.IsCanceled) continue;

                            try
                            {
                                var ok = w.Work(ct);
                                w.Tcs.TrySetResult(ok);
                            }
                            catch (OperationCanceledException oce)
                            {
                                w.Tcs.TrySetCanceled(oce.CancellationToken);
                            }
                            catch (Exception ex)
                            {
                                w.Tcs.TrySetException(ex);
                            }

                            continue;
                        }

                        break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "CdnDecodeWorker crashed");
            }
        }

        private readonly Func<Stream, string, long, CancellationToken, (string HashUpper, long BytesWritten)> _decompressFromWorker;

        public void Dispose()
        {
            try
            {
                _cts.Cancel();
                _signal.Release(_threads.Count);
            }
            catch { }

            foreach (var t in _threads)
            {
                try { if (!t.Join(250)) t.Interrupt(); } catch { }
            }

            _cts.Dispose();
            _signal.Dispose();
        }
    }

    private sealed record CdnDownloadedFile(string Hash, string FinalPath, string DestPath, bool Hard, bool Soft);

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

}
