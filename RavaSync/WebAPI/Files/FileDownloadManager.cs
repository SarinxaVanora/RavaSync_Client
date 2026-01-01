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

        Mediator.Subscribe<DownloadLimitChangedMessage>(this, (msg) =>
        {
            if (!_activeDownloadStreams.Any()) return;
            var limit = _orchestrator.DownloadLimitPerSlot();
            var newLimit = limit <= 0 ? ThrottledStream.Infinite : limit;
            Logger.LogTrace("Setting new Download Speed Limit to {newLimit}", newLimit);
            foreach (var stream in _activeDownloadStreams)
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
            MaxConnectionsPerServer = 64,
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

    protected override void Dispose(bool disposing)
    {
        ClearDownload();
        foreach (var stream in _activeDownloadStreams.ToList())
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
            if (string.IsNullOrEmpty(fr.Hash) || fr.GamePaths == null || !fr.GamePaths.Any())
                continue;

            // use the first path's extension to construct the cache path
            var ext = fr.GamePaths[0].Split(".")[^1];
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
                var expected = dto.RawSize > 0 ? dto.RawSize : dto.Size;

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
                        if (ValidateDownloadedFile(resolved, dto.Hash, expectedRawSize: expected))
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
                    if (ValidateDownloadedFile(path, dto.Hash, expectedRawSize: expected))
                    {
                        PersistFileToStorage(dto.Hash, path);
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
        var downloadGroups = CurrentDownloads.GroupBy(f => f.DownloadUri.Host + ":" + f.DownloadUri.Port, StringComparer.Ordinal);

        foreach (var downloadGroup in downloadGroups)
        {
            _downloadStatus[downloadGroup.Key] = new FileDownloadStatus()
            {
                DownloadStatus = DownloadStatus.Initializing,
                TotalBytes = downloadGroup.Sum(c => c.Total),
                TotalFiles = 1,
                TransferredBytes = 0,
                TransferredFiles = 0
            };
        }

        Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, _downloadStatus));

        await Parallel.ForEachAsync(downloadGroups, new ParallelOptions()
        {

            MaxDegreeOfParallelism = Math.Clamp(_mareConfigService.Current.ParallelDownloads, 2, Math.Min(Environment.ProcessorCount, 6)),
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
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "CDN fast-path: error, falling back to blk pipeline for group {group}", fileGroup.Key);
            }


            Logger.LogError("CDN fast-path: group {group} could not be fully satisfied. CDN-only mode is enabled; aborting download group.", fileGroup.Key);
            throw new HttpRequestException("CDN fast-path failed for group " + fileGroup.Key);
        }).ConfigureAwait(false);

        Logger.LogDebug("Download end: {id}", gameObjectHandler);

        ClearDownload();
    }


    private async Task<List<DownloadFileDto>> FilesGetSizes(List<string> hashes, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        var response = await _orchestrator.SendRequestAsync(HttpMethod.Get, MareFiles.ServerFilesGetSizesFullPath(_orchestrator.UploadCdnUri!), hashes, ct).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<List<DownloadFileDto>>(cancellationToken: ct).ConfigureAwait(false) ?? [];
    }

    private void PersistFileToStorage(string fileHash, string filePath)
    {
        var fi = new FileInfo(filePath);
        Func<DateTime> RandomDayInThePast()
        {
            DateTime start = new(1995, 1, 1, 1, 1, 1, DateTimeKind.Local);
            Random gen = new();
            int range = (DateTime.Today - start).Days;
            return () => start.AddDays(gen.Next(range));
        }

        fi.CreationTime = RandomDayInThePast().Invoke();
        fi.LastAccessTime = DateTime.Today;
        fi.LastWriteTime = RandomDayInThePast().Invoke();
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

        var hashBuilder = new List<char>();
        var lenBuilder = new List<char>();
        bool readingHash = true;

        for (int i = 1; i < chars.Length; i++)
        {
            var c = chars[i];

            if (readingHash)
            {
                if (c == ':')
                {
                    readingHash = false;
                    continue;
                }

                if (c == '#')
                {
                    // premature end, we never saw ':'
                    return false;
                }

                hashBuilder.Add(c);
            }
            else
            {
                if (c == '#')
                {
                    if (hashBuilder.Count == 0 || lenBuilder.Count == 0) return false;
                    if (!long.TryParse(new string(lenBuilder.ToArray()), out fileLengthBytes)) return false;

                    fileHash = new string(hashBuilder.ToArray());
                    return true;
                }

                lenBuilder.Add(c);
            }
        }

        return false;
    }

    private static (string fileHash, long fileLengthBytes, bool munged) ReadCdnHeader(Stream stream)
    {
        var headerBytes = new List<byte>(128);

        // We read byte-by-byte until we have enough to successfully parse "#hash:length#",
        // trying both unmunged and munged interpretations.
        while (headerBytes.Count < 256)
        {
            int b = stream.ReadByte();
            if (b == -1) throw new EndOfStreamException("Unexpected end of stream while reading CDN header");
            headerBytes.Add((byte)b);

            // Optimization: only try parse when last interpreted char could be '#'
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

    private void CleanupFailedCdnDownload(string destPath, string hash, MareConfig cfg, bool hard)
    {
        try
        {
            if (File.Exists(destPath)) File.Delete(destPath);
        }
        catch { /* ignore */ }

        if (cfg.DelayActivationEnabled && hard)
        {
            try
            {
                _fileDbManager.UnstageFile(hash);
            }
            catch { /* ignore */ }
        }
    }


    private static async Task<(string HashUpper, long BytesWritten)> DecompressLz4ToFileAndHashAsync(Stream input, string destPath, CancellationToken ct)
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

            while (true)
            {
                var read = await lz4.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0) break;

                sha1.AppendData(buffer, 0, read);
                await outFs.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                total += read;
            }

            await outFs.FlushAsync(ct).ConfigureAwait(false);

            return (Convert.ToHexString(sha1.GetHashAndReset()), total);
        }
        finally
        {
            _downloadBufferPool.Return(buffer);
        }
    }

    private async Task<bool> TryDownloadFilesFromCdnAsync(GameObjectHandler gameObjectHandler, IGrouping<string, DownloadFileTransfer> fileGroup, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
        if (_orchestrator.FilesCdnUri == null) return false;

        // Hash -> extension map as in blk path
        var fileExtByHash = fileReplacement
            .Where(f => !string.IsNullOrEmpty(f.Hash) && f.GamePaths != null && f.GamePaths.Any())
            .GroupBy(f => f.Hash, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g =>
            {
                var gp = g.First().GamePaths[0];
                return gp.Contains('.') ? gp.Split(".")[^1] : "dat";
            }, StringComparer.OrdinalIgnoreCase);

        // CDN pipeline is IO-bound, not CPU-bound — allow aggressive concurrency
        var cdnParallel = Math.Clamp(_mareConfigService.Current.ParallelDownloads, 4, 12);
        var concurrency = new SemaphoreSlim(cdnParallel, cdnParallel);

        var tasks = new List<Task>(fileGroup.Count());
        var anyFailure = new int[1];

        var results = new System.Collections.Concurrent.ConcurrentBag<CdnDownloadedFile>();

        foreach (var transfer in fileGroup)
        {
            tasks.Add(DownloadOneFromCdnAsync(gameObjectHandler, fileGroup, fileExtByHash, results, concurrency, transfer, anyFailure, ct));
        }

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
            throw;
        }

        if (System.Threading.Volatile.Read(ref anyFailure[0]) != 0)
        {
            Logger.LogDebug("CDN fast-path: at least one file failed, falling back to blk pipeline for group {group}", fileGroup.Key);
            return false;
        }

        foreach (var r in results)
        {
            if (r.DestPath == r.FinalPath)
            {
                PersistFileToStorage(r.Hash, r.FinalPath);
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

        if (_downloadStatus.TryGetValue(fileGroup.Key, out var status))
        {
            status.DownloadStatus = DownloadStatus.Decompressing;
        }

        Logger.LogDebug("CDN fast-path: completed all files for group {group} via CDN", fileGroup.Key);
        return true;
    }

    private async Task DownloadOneFromCdnAsync(GameObjectHandler gameObjectHandler, IGrouping<string, DownloadFileTransfer> fileGroup, Dictionary<string, string> fileExtByHash, System.Collections.Concurrent.ConcurrentBag<CdnDownloadedFile> results, SemaphoreSlim concurrency, DownloadFileTransfer transfer, int[] anyFailure, CancellationToken ct)
    {
        await concurrency.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var attempts = 0;
            string? plannedCdnUrl = null;

            while (attempts++ < MaxCdnAttemptsPerFile && !ct.IsCancellationRequested)
            {
                var baseCdnUrl = MareFiles.CdnGetFullPath(_orchestrator.FilesCdnUri!, transfer.Hash.ToUpperInvariant()).ToString();
                var isCacheBusted = plannedCdnUrl != null;
                var cdnUrl = plannedCdnUrl ?? baseCdnUrl;
                plannedCdnUrl = null;

                Logger.LogDebug("CDN fast-path: downloading {hash} from {url} (attempt {attempt}/{max})",
                    transfer.Hash, cdnUrl, attempts, MaxCdnAttemptsPerFile);

                try
                {
                    using var response = await _cdnFastClient.GetAsync(cdnUrl, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        if (response.StatusCode == System.Net.HttpStatusCode.NotFound && !isCacheBusted
                            && TryPlanCdn404SelfHeal(transfer.Hash, baseCdnUrl, response, out var cacheBustUrl, out var cfCacheStatus, out var age, out var cfRay, out var reason))
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

                        response.EnsureSuccessStatusCode();
                    }

                    await using var rawStream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

                    var firstByteBuf = new byte[1];
                    var read0 = await rawStream.ReadAsync(firstByteBuf.AsMemory(0, 1), ct).ConfigureAwait(false);
                    if (read0 <= 0) throw new InvalidDataException("CDN response stream was empty");

                    using var respStream = new PrefixedReadStream(rawStream, firstByteBuf, read0);
                    var b0 = firstByteBuf[0];
                    var looksHeadered = b0 == (byte)'#' || (byte)(b0 ^ 42) == (byte)'#';

                    string headerHash = transfer.Hash;
                    long payloadLen = -1;
                    bool munged = false;

                    Stream input;
                    if (looksHeadered)
                    {
                        (headerHash, payloadLen, munged) = ReadCdnHeader(respStream);

                        if (!string.Equals(headerHash, transfer.Hash, StringComparison.OrdinalIgnoreCase))
                            throw new InvalidDataException($"CDN fast-path: header hash mismatch for {transfer.Hash}, got {headerHash}");

                        if (payloadLen <= 0)
                            throw new InvalidDataException($"CDN fast-path: invalid payload length {payloadLen} for {transfer.Hash}");

                        input = new LimitedXorStream(respStream, payloadLen, munged ? (byte)42 : (byte)0);
                    }
                    else
                    {
                        input = respStream;
                    }

                    var ext = fileExtByHash.TryGetValue(headerHash, out var e) ? e : "dat";
                    var filePath = _fileDbManager.GetCacheFilePath(headerHash, ext);

                    var cfg = _mareConfigService.Current;
                    var hard = ActivationPolicy.IsHardDelayed(filePath);
                    var soft = !hard && ActivationPolicy.IsSoftDelayed(filePath);

                    string destPath;
                    if (cfg.DelayActivationEnabled && hard)
                    {
                        destPath = Path.Combine(_delayedActivator.QuarantineRoot, headerHash + Path.GetExtension(filePath));
                        Directory.CreateDirectory(Path.GetDirectoryName(destPath)!);
                        _fileDbManager.StageFile(headerHash, filePath);
                    }
                    else
                    {
                        destPath = filePath;
                        Directory.CreateDirectory(Path.GetDirectoryName(destPath)!);
                    }

                    try
                    {
                        var (computed, bytesWritten) = await DecompressLz4ToFileAndHashAsync(input, destPath, ct).ConfigureAwait(false);

                        if (bytesWritten <= 0 || !string.Equals(computed, headerHash, StringComparison.OrdinalIgnoreCase))
                        {
                            Logger.LogDebug(
                                "CDN fast-path: downloaded file for {hash} failed validation (attempt {attempt}/{max})",
                                headerHash, attempts, MaxCdnAttemptsPerFile);

                            CleanupFailedCdnDownload(destPath, headerHash, cfg, hard);

                            if (attempts >= MaxCdnAttemptsPerFile)
                            {
                                System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
                                return;
                            }

                            continue;
                        }
                    }
                    finally
                    {
                        if (!ReferenceEquals(input, respStream))
                        {
                            try { input.Dispose(); } catch { /* ignore */ }
                        }
                    }

                    if (!ExtraContentValidation(destPath, headerHash))
                    {
                        Logger.LogDebug(
                            "CDN fast-path: extra validation failed for {hash} (attempt {attempt}/{max})",
                            headerHash, attempts, MaxCdnAttemptsPerFile);

                        CleanupFailedCdnDownload(destPath, headerHash, cfg, hard);

                        if (attempts >= MaxCdnAttemptsPerFile)
                        {
                            System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
                            return;
                        }

                        continue;
                    }

                    _cdn404State.TryRemove(headerHash, out _);
                    _cdn404State.TryRemove(transfer.Hash, out _);

                    results.Add(new CdnDownloadedFile(headerHash, filePath, destPath, hard, soft));

                    if (_downloadStatus.TryGetValue(fileGroup.Key, out var st))
                    {
                        lock (st)
                        {
                            st.TransferredFiles++;
                            st.TransferredBytes += transfer.Total;
                        }
                    }

                    return;
                }
                catch (OperationCanceledException)
                {
                    System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
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

                        if (File.Exists(finalPath2) && ValidateDownloadedFile(finalPath2, transfer.Hash, expectedRawSize: 0))
                        {
                            PersistFileToStorage(transfer.Hash, finalPath2);

                            _cdn404State.TryRemove(transfer.Hash, out _);

                            if (_downloadStatus.TryGetValue(fileGroup.Key, out var st2))
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
                        System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);

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
                        System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
                        return;
                    }
                }
            }

            System.Threading.Interlocked.Exchange(ref anyFailure[0], 1);
            Logger.LogWarning(
                "CDN fast-path: all {max} attempts failed for {hash}, treating as failed for group {group}",
                MaxCdnAttemptsPerFile,
                transfer.Hash,
                fileGroup.Key);
        }
        finally
        {
            _inflightHashes.TryRemove(transfer.Hash, out _);
            concurrency.Release();
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


    private bool ValidateDownloadedFile(string fullPath, string fileHash, long expectedRawSize)
    {
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

            // Strong check: recompute hash and compare to server hash
            var computed = Crypto.GetFileHash(fi.FullName);
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
            Logger.LogDebug(
                ioEx,
                "IO error during validation of {hash} at {path}; treating as valid and keeping file",
                fileHash, fullPath);

            return true;
        }
        catch (Exception ex)
        {
            // Anything else is genuinely suspicious – allow the caller to delete + retry.
            Logger.LogWarning(ex, "Validation threw for {hash} at {path}", fileHash, fullPath);
            return false;
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
