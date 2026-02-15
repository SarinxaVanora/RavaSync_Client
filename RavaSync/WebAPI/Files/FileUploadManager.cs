using FFXIVClientStructs.FFXIV.Component.Text;
using RavaSync.API.Data;
using RavaSync.API.Dto.Files;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI;
using RavaSync.WebAPI.Files.Models;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using static Dalamud.Interface.Utility.Raii.ImRaii;

namespace RavaSync.WebAPI.Files;

public sealed class FileUploadManager : DisposableMediatorSubscriberBase
{
    private readonly FileCacheManager _fileDbManager;
    private readonly HttpClient _directUploadClient;
    private readonly MareConfigService _mareConfigService;
    private readonly FileTransferOrchestrator _orchestrator;
    private readonly ServerConfigurationManager _serverManager;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTime> _verifiedUploadedHashes = new(StringComparer.Ordinal);
    private CancellationTokenSource? _uploadCancellationTokenSource = new();
    private static readonly TimeSpan VerifiedHashTtl = TimeSpan.FromHours(12);
    private const int MaxCdnExistenceChecks = 8;


    public FileUploadManager(ILogger<FileUploadManager> logger, MareMediator mediator,
        MareConfigService mareConfigService,
        FileTransferOrchestrator orchestrator,
        FileCacheManager fileDbManager,
        ServerConfigurationManager serverManager) : base(logger, mediator)
    {
        _mareConfigService = mareConfigService;
        _orchestrator = orchestrator;
        _fileDbManager = fileDbManager;
        _serverManager = serverManager;
        _directUploadClient = CreateDirectUploadClient();

        Mediator.Subscribe<DisconnectedMessage>(this, (msg) =>
        {
            Reset();
        });
    }

    public List<FileTransfer> CurrentUploads { get; } = [];
    private enum AuthMode { NeedAuthorization, PreAuthorized }

    public bool IsUploading => CurrentUploads.Count > 0;

    public bool CancelUpload()
    {
        if (CurrentUploads.Any())
        {
            Logger.LogDebug("Cancelling current upload");
            _uploadCancellationTokenSource?.Cancel();
            _uploadCancellationTokenSource?.Dispose();
            _uploadCancellationTokenSource = null;
            CurrentUploads.Clear();
            return true;
        }

        return false;
    }

    public async Task DeleteAllFiles()
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");

        await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesDeleteAllFullPath(_orchestrator.UploadCdnUri!)).ConfigureAwait(false);
    }

    public Task<List<string>> UploadFiles(List<string> hashesToUpload, IProgress<string> progress, CancellationToken? ct = null)
    {
        return UploadFilesCore(hashesToUpload, progress, ct ?? CancellationToken.None,
                        AuthMode.NeedAuthorization, Array.Empty<string>());
    }

    private async Task UploadFileStreamedFromDisk(string fileHash, string filePath, bool munged, IProgress<UploadProgress>? progress, CancellationToken uploadToken, int streamBufferSize)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        uploadToken.ThrowIfCancellationRequested();

        Logger.LogDebug("[{hash}] Preparing file for upload", fileHash);

        var fileSize = new FileInfo(filePath).Length;

        var ticketUri = MareFiles.ServerFilesUploadTicketFullPath(_orchestrator.UploadCdnUri!, fileHash);
        var ticketReq = new UploadTicketRequestDto(fileSize, munged, 0);

        // Acquire ticket (must be DirectB2)
        var ticketResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, ticketUri, ticketReq, uploadToken).ConfigureAwait(false);
        if (ticketResp.StatusCode == HttpStatusCode.NotFound)
            throw new InvalidOperationException($"[{fileHash}] Upload ticket endpoint not found; refusing legacy upload.");

        ticketResp.EnsureSuccessStatusCode();

        var ticketJson = await ticketResp.Content.ReadAsStringAsync(uploadToken).ConfigureAwait(false);
        var ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(ticketJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (ticket == null)
            throw new InvalidOperationException($"[{fileHash}] Upload ticket response could not be parsed.");

        if (!string.Equals(ticket.Mode, "DirectB2", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"[{fileHash}] Server did not provide DirectB2 ticket (mode={ticket.Mode}); refusing legacy upload.");

        if (!ticket.UploadRequired)
            return;

        Logger.LogDebug("[{hash}] Direct upload ticket acquired, compressing to temp (LZ4)", fileHash);

        var rawProgress = progress == null ? null : new Progress<long>(b => progress.Report(new UploadProgress(b, fileSize)));
        await using var temp = await TempLz4UploadFile.CreateAsync(filePath, uploadToken, rawProgress).ConfigureAwait(false);

        // compression done (raw -> temp)
        progress?.Report(new UploadProgress(fileSize, fileSize));

        const int maxAttempts = 4;
        Exception? last = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            uploadToken.ThrowIfCancellationRequested();

            try
            {
                var fs0 = new FileStream(temp.TempPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                    bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

                // report *network bytes sent* (based on bytes read by HttpClient from the stream)
                var netProgress = progress == null
                    ? null
                    : new Progress<long>(sent => progress.Report(new UploadProgress(sent, temp.CompressedSize)));

                await using Stream fs = netProgress == null ? fs0 : new ProgressReadStream(fs0, netProgress);

                using var put = new HttpRequestMessage(HttpMethod.Put, ticket.UploadUrl);

                var content = new StreamContent(fs, streamBufferSize);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                content.Headers.ContentLength = temp.CompressedSize;
                try { content.Headers.ContentMD5 = Convert.FromBase64String(temp.Md5Base64); } catch { }

                put.Content = content;
                put.Headers.ExpectContinue = false;

                var putResp = await _directUploadClient.SendAsync(put, HttpCompletionOption.ResponseHeadersRead, uploadToken).ConfigureAwait(false);
                Logger.LogDebug("[{hash}] Direct upload PUT attempt {attempt}/{max} Status: {status}", fileHash, attempt, maxAttempts, putResp.StatusCode);

                putResp.EnsureSuccessStatusCode();

                var etag = putResp.Headers.ETag?.Tag;

                var completeUri = MareFiles.ServerFilesUploadCompleteFullPath(_orchestrator.UploadCdnUri!, fileHash);
                var complete = new UploadCompleteDto(temp.RawSize, temp.CompressedSize, temp.Md5Base64, etag, true);

                var completeResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, completeUri, complete, uploadToken).ConfigureAwait(false);
                Logger.LogDebug("[{hash}] UploadComplete Status: {status}", fileHash, completeResp.StatusCode);
                completeResp.EnsureSuccessStatusCode();

                if (_orchestrator.FilesCdnUri != null)
                {
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await CdnPrewarmHelper.PrewarmAsync(
                                _orchestrator.FilesCdnUri,
                                new[] { fileHash },
                                CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogTrace(ex, "[{hash}] CDN prewarm failed (ignored)", fileHash);
                        }
                    });
                }

                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                last = ex;
                Logger.LogWarning(ex, "[{hash}] Direct upload PUT failed (attempt {attempt}/{max}); retrying", fileHash, attempt, maxAttempts);
                await Task.Delay(250 * attempt, uploadToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                last = ex;
                break;
            }
        }

        throw new InvalidOperationException($"[{fileHash}] DirectB2 upload failed after {maxAttempts} attempts.", last);
    }

    public async Task<CharacterData> UploadFiles(CharacterData data, List<UserData> visiblePlayers)
    {
        CancelUpload();

        _uploadCancellationTokenSource = new CancellationTokenSource();
        var uploadToken = _uploadCancellationTokenSource.Token;

        try
        {
            Logger.LogDebug("Sending Character data {hash} to service {url}",
                data.DataHash.Value, _serverManager.CurrentApiUrl);

            foreach (var kvp in data.FileReplacements)
            {
                data.FileReplacements[kvp.Key].RemoveAll(i =>
                    _orchestrator.ForbiddenTransfers.Exists(f =>
                        string.Equals(f.Hash, i.Hash, StringComparison.OrdinalIgnoreCase)));
            }

            var unverifiedUploads = GetUnverifiedFiles(data);
            if (unverifiedUploads.Any())
            {
                await UploadUnverifiedFiles(unverifiedUploads, visiblePlayers, uploadToken).ConfigureAwait(false);
                Logger.LogDebug("Verification complete for {hash}", data.DataHash.Value);
            }

            return data;
        }
        finally
        {
            try { _uploadCancellationTokenSource?.Dispose(); } catch { /* ignore */ }
            _uploadCancellationTokenSource = null;

            CurrentUploads.Clear();
        }
    }



    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            try { _directUploadClient.Dispose(); } catch { /* ignore */ }
        }
        Reset();
    }

    private async Task<List<UploadFileDto>> FilesSend(List<string> hashes, List<string> uids, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        FilesSendDto filesSendDto = new()
        {
            FileHashes = hashes,
            UIDs = uids
        };
        var response = await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesFilesSendFullPath(_orchestrator.UploadCdnUri!), filesSendDto, ct).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<List<UploadFileDto>>(cancellationToken: ct).ConfigureAwait(false) ?? [];
    }

    private HashSet<string> GetUnverifiedFiles(CharacterData data)
    {
        var now = DateTime.UtcNow;
        var cutoff = now.Subtract(VerifiedHashTtl);

        HashSet<string> unverified = new(StringComparer.Ordinal);

        foreach (var kvp in data.FileReplacements)
        {
            var list = kvp.Value;
            for (int i = 0; i < list.Count; i++)
            {
                var fr = list[i];

                if (!string.IsNullOrEmpty(fr.FileSwapPath))
                    continue;

                var hash = fr.Hash;
                if (string.IsNullOrEmpty(hash))
                    continue;

                if (_verifiedUploadedHashes.TryGetValue(hash, out var verifiedTime) && verifiedTime >= cutoff)
                    continue;

                unverified.Add(hash);
            }
        }

        if (unverified.Count > 0)
            Logger.LogDebug("Need verification for {count} hashes (ttl {ttl})", unverified.Count, VerifiedHashTtl);

        return unverified;
    }

    private void Reset()
    {
        _uploadCancellationTokenSource?.Cancel();
        _uploadCancellationTokenSource?.Dispose();
        _uploadCancellationTokenSource = null;
        CurrentUploads.Clear();
        _verifiedUploadedHashes.Clear();
    }

    private async Task UploadUnverifiedFiles(HashSet<string> unverifiedUploadHashes, List<UserData> visiblePlayers, CancellationToken uploadToken)
    {
        unverifiedUploadHashes = unverifiedUploadHashes
            .Where(h => _fileDbManager.GetFileCacheByHash(h) != null)
            .ToHashSet(StringComparer.Ordinal);

        Logger.LogDebug("Verifying {count} files", unverifiedUploadHashes.Count);

        var filesToUpload = await FilesSend(
            [.. unverifiedUploadHashes],
            visiblePlayers.Select(p => p.UID).ToList(),
            uploadToken
        ).ConfigureAwait(false);

        foreach (var f in filesToUpload.Where(f => f.IsForbidden))
        {
            if (_orchestrator.ForbiddenTransfers.TrueForAll(x => !string.Equals(x.Hash, f.Hash, StringComparison.Ordinal)))
            {
                _orchestrator.ForbiddenTransfers.Add(new UploadFileTransfer(f)
                {
                    LocalFile = _fileDbManager.GetFileCacheByHash(f.Hash)?.ResolvedFilepath ?? string.Empty,
                });
            }
            _verifiedUploadedHashes[f.Hash] = DateTime.UtcNow;
        }

        // Allowed hashes to push
        var toPush = filesToUpload
            .Where(f => !f.IsForbidden)
            .Select(f => f.Hash)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (toPush.Count == 0) return;

        var prog = new Progress<string>(msg => Logger.LogInformation(msg));
        var failed = await UploadFilesCore(toPush, prog, uploadToken, AuthMode.PreAuthorized).ConfigureAwait(false);

        if (failed.Any())
            Logger.LogInformation("Some uploads did not complete ({count}): {list}", failed.Count, string.Join(", ", failed));

    }


    private async Task<List<string>> UploadFilesCore(IEnumerable<string> inputHashes, IProgress<string>? progress, CancellationToken ct, AuthMode authMode, IReadOnlyList<string>? uidsForFilesSend = null)
    {
        var token = ct;

        // ---- Local presence check ----
        var present = inputHashes
            .Where(h => _fileDbManager.GetFileCacheByHash(h) != null)
            .ToHashSet(StringComparer.Ordinal);

        var missingLocal = inputHashes.Except(present, StringComparer.Ordinal).ToList();
        if (missingLocal.Any())
            return missingLocal;

        // ---- Authorization ----
        List<string> allowedHashes;
        var forbidden = new List<string>();

        if (authMode == AuthMode.NeedAuthorization)
        {
            var uids = (uidsForFilesSend ?? Array.Empty<string>()).ToList();
            var auth = await FilesSend([.. present], uids, token).ConfigureAwait(false);

            foreach (var f in auth.Where(f => f.IsForbidden))
            {
                forbidden.Add(f.Hash);
                if (_orchestrator.ForbiddenTransfers.TrueForAll(
                        x => !string.Equals(x.Hash, f.Hash, StringComparison.Ordinal)))
                {
                    _orchestrator.ForbiddenTransfers.Add(new UploadFileTransfer(f)
                    {
                        LocalFile = _fileDbManager.GetFileCacheByHash(f.Hash)?.ResolvedFilepath ?? string.Empty,
                    });
                }

                _verifiedUploadedHashes[f.Hash] = DateTime.UtcNow;
            }

            allowedHashes = auth
                .Where(f => !f.IsForbidden)
                .Select(f => f.Hash)
                .Distinct(StringComparer.Ordinal)
                .ToList();
        }
        else
        {
            allowedHashes = present.ToList();
        }

        if (allowedHashes.Count == 0)
            return forbidden;

        // ---- UI bookkeeping ----
        var sizeByHash = new Dictionary<string, long>(StringComparer.Ordinal);

        foreach (var h in allowedHashes)
        {
            try
            {
                if (!CurrentUploads.Any(u => string.Equals(u.Hash, h, StringComparison.Ordinal)))
                {
                    var cacheEntry = _fileDbManager.GetFileCacheByHash(h);
                    if (cacheEntry == null) continue;

                    var size = new FileInfo(cacheEntry.ResolvedFilepath).Length;
                    sizeByHash[h] = size;

                    CurrentUploads.Add(new UploadFileTransfer(new UploadFileDto { Hash = h })
                    {
                        Total = size,
                    });
                }
            }
            catch
            {
                // best effort
            }
        }

        const int MaxUploadAttempts = 3;

        // fixed if user explicitly configured, adaptive only for Auto (0)
        var configured = _mareConfigService.Current.ParallelUploads;
        var cpuAuto = GetCpuAutoParallelUploads();
        var isAuto = configured <= 0;

        var desiredParallel = isAuto ? 1 : Math.Clamp(configured, 1, 10);
        var maxParallel = isAuto ? Math.Clamp(cpuAuto, 1, 10) : desiredParallel;

        if (isAuto && allowedHashes.Count >= 4 && sizeByHash.Count > 0)
        {
            var small = 0;
            foreach (var s in sizeByHash.Values)
                if (s <= 256 * 1024) small++;

            if (small >= (sizeByHash.Count * 3 / 4))
                desiredParallel = Math.Min(maxParallel, 4);
        }

        var bytesSinceSample = 0L;
        var lastUploadedByHash = new System.Collections.Concurrent.ConcurrentDictionary<string, long>(StringComparer.Ordinal);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var lastSampleAt = TimeSpan.Zero;
        double ewmaBps = 0;

        // hysteresis so we don’t flap
        int pendingDesired = desiredParallel;
        int pendingHits = 0;

        bool fastStartUsed = false;

        double SampleBpsIfReady()
        {
            var now = sw.Elapsed;
            if (now - lastSampleAt < TimeSpan.FromSeconds(2))
                return -1;

            var dt = (now - lastSampleAt).TotalSeconds;
            lastSampleAt = now;

            var bytes = Interlocked.Exchange(ref bytesSinceSample, 0);
            var instant = dt > 0 ? bytes / dt : 0;

            // EWMA smoothing
            ewmaBps = ewmaBps <= 0 ? instant : (ewmaBps * 0.70) + (instant * 0.30);
            return ewmaBps;
        }

        int ComputeDesiredFromBps(double bps)
        {
            // One “slot” per ~128 KiB/s of sustained throughput, rounded, clamped.
            const double slot = 128 * 1024;

            if (bps <= 0) return 1;

            var bySpeed = (int)Math.Round(bps / slot, MidpointRounding.AwayFromZero);
            bySpeed = Math.Clamp(bySpeed, 1, maxParallel);

            return bySpeed;
        }

        var queue = new System.Collections.Concurrent.ConcurrentQueue<string>(allowedHashes);
        var running = new List<Task>(Math.Min(allowedHashes.Count, maxParallel));
        var succeeded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        int idx = 0;

        while (!queue.IsEmpty || running.Count > 0)
        {
            // launch up to desiredParallel
            while (running.Count < desiredParallel && queue.TryDequeue(out var hash))
            {
                var localIdx = Interlocked.Increment(ref idx);
                running.Add(UploadOneCoreAsync(hash, localIdx, token));
            }

            // Adaptive step every ~2s (Auto only) — only when we have active uploads
            if (isAuto && running.Count > 0)
            {
                var bps = SampleBpsIfReady();
                if (bps >= 0)
                {
                    var next = ComputeDesiredFromBps(bps);

                    // ---- Fast-start boost (one-time jump) ----
                    const double fastStartBps = 2 * 1024 * 1024; // 2 MiB/s
                    if (!fastStartUsed && desiredParallel == 1 && bps >= fastStartBps)
                    {
                        desiredParallel = Math.Min(maxParallel, 4);
                        pendingHits = 0;
                        pendingDesired = desiredParallel;
                        fastStartUsed = true;
                    }
                    else
                    {
                        // hysteresis: require 2 consecutive samples before changing,
                        // and only step by 1 each time.
                        if (next != desiredParallel)
                        {
                            if (pendingDesired != next)
                            {
                                pendingDesired = next;
                                pendingHits = 1;
                            }
                            else if (++pendingHits >= 2)
                            {
                                desiredParallel += Math.Sign(pendingDesired - desiredParallel);
                                desiredParallel = Math.Clamp(desiredParallel, 1, maxParallel);
                                pendingHits = 0;
                            }
                        }
                        else
                        {
                            pendingHits = 0;
                            pendingDesired = desiredParallel;
                        }
                    }
                }
            }

            if (running.Count == 0)
            {
                // nothing running yet; loop will start more
                continue;
            }

            var finished = await Task.WhenAny(running).ConfigureAwait(false);
            running.Remove(finished);
        }

        async Task UploadOneCoreAsync(string hash, int localIdx, CancellationToken tokenInner)
        {
            for (int attempt = 1; attempt <= MaxUploadAttempts; attempt++)
            {
                try
                {
                    var cache = _fileDbManager.GetFileCacheByHash(hash);
                    if (cache is null)
                    {
                        Logger.LogWarning("[{hash}] Upload skipped: cache entry disappeared", hash);
                        return;
                    }

                    var filePath = cache.ResolvedFilepath;
                    var origSize = sizeByHash.TryGetValue(hash, out var s) ? s : new FileInfo(filePath).Length;

                    var prog = new Progress<UploadProgress>(up =>
                    {
                        try
                        {
                            // Update UI entry
                            var entry = CurrentUploads.FirstOrDefault(u => string.Equals(u.Hash, hash, StringComparison.Ordinal));
                            if (entry != null)
                            {
                                entry.Total = up.Size;
                                entry.Transferred = up.Uploaded;
                            }

                            if (up.Size != origSize)
                            {
                                long delta = 0;
                                lastUploadedByHash.AddOrUpdate(hash,
                                    up.Uploaded,
                                    (_, old) =>
                                    {
                                        delta = up.Uploaded - old;
                                        return up.Uploaded;
                                    });

                                if (delta > 0)
                                    Interlocked.Add(ref bytesSinceSample, delta);
                            }

                            // light trace
                            var pct = up.Size > 0 ? (int)(100L * up.Uploaded / up.Size) : 0;
                            if (pct % 25 == 0)
                                Logger.LogTrace("[{idx}] {file}: {pct}% (attempt {attempt}/{max})",
                                    localIdx, Path.GetFileName(filePath), pct, attempt, MaxUploadAttempts);
                        }
                        catch
                        {
                            // ignore
                        }
                    });

                    IProgress<UploadProgress> throttledProg = new ThrottledProgress<UploadProgress>(prog, TimeSpan.FromMilliseconds(100));

                    var buf = ChooseUploadStreamBufferSize(ewmaBps);

                    await UploadFileStreamedFromDisk(
                        fileHash: hash,
                        filePath: filePath,
                        munged: _mareConfigService.Current.UseAlternativeFileUpload,
                        progress: throttledProg,
                        uploadToken: tokenInner,
                        streamBufferSize: buf
                    ).ConfigureAwait(false);

                    _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                    succeeded[hash] = 1;

                    var finalEntry = CurrentUploads.FirstOrDefault(u => string.Equals(u.Hash, hash, StringComparison.Ordinal));
                    if (finalEntry != null) finalEntry.Transferred = finalEntry.Total;

                    return; // success
                }
                catch (OperationCanceledException)
                {
                    Logger.LogDebug("[{hash}] Upload cancelled on attempt {attempt}/{max}", hash, attempt, MaxUploadAttempts);
                    throw;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "[{hash}] Upload failed on attempt {attempt}/{max}",
                        hash, attempt, MaxUploadAttempts);

                    if (attempt == MaxUploadAttempts)
                    {
                        Logger.LogWarning("[{hash}] All upload attempts failed, marking as failed for this run", hash);
                    }
                    else
                    {
                        await Task.Delay(200, tokenInner).ConfigureAwait(false);
                    }
                }
                finally
                {
                    lastUploadedByHash.TryRemove(hash, out _);
                }
            }
        }

        // ---- UI: never leave "Uploading..." stuck once this batch is done ----
        CurrentUploads.Clear();
        progress?.Report("No uploads in progress");

        // ---- Result classification ----
        var failed = allowedHashes
            .Where(h => !succeeded.ContainsKey(h))
            .Concat(forbidden)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        return failed;
    }

    private sealed class ThrottledProgress<T> : IProgress<T>
    {
        private readonly IProgress<T> _inner;
        private readonly TimeSpan _minInterval;
        private readonly object _gate = new();

        private long _lastTicks;
        private bool _hasPending;
        private T _pending;

        public ThrottledProgress(IProgress<T> inner, TimeSpan minInterval)
        {
            _inner = inner;
            _minInterval = minInterval;
            _lastTicks = 0;
            _hasPending = false;
            _pending = default!;
        }

        public void Report(T value)
        {
            var now = DateTime.UtcNow.Ticks;

            lock (_gate)
            {
                _pending = value;

                var elapsed = now - _lastTicks;
                if (_lastTicks != 0 && elapsed < _minInterval.Ticks)
                {
                    if (_hasPending) return;

                    _hasPending = true;
                    var due = _minInterval - TimeSpan.FromTicks(elapsed);
                    _ = FlushLater(due);
                    return;
                }

                _lastTicks = now;
            }

            _inner.Report(value);
        }

        private async Task FlushLater(TimeSpan delay)
        {
            try
            {
                await Task.Delay(delay).ConfigureAwait(false);

                T value;
                lock (_gate)
                {
                    _hasPending = false;
                    value = _pending;
                    _pending = default!;
                    _lastTicks = DateTime.UtcNow.Ticks;
                }

                _inner.Report(value);
            }
            catch
            {
                // ignore
            }
        }
    }

    private int GetCpuAutoParallelUploads()
    {
        var cpu = Environment.ProcessorCount;

        var auto =
            cpu <= 4 ? 2 :
            cpu <= 8 ? 3 :
            cpu <= 16 ? 4 :
                        6;

        return Math.Clamp(auto, 1, 10);
    }

    private static int ChooseUploadStreamBufferSize(double ewmaBps)
    {
        if (ewmaBps > 0 && ewmaBps < 128 * 1024) return 64 * 1024;     // <128 KiB/s
        if (ewmaBps > 0 && ewmaBps < 1024 * 1024) return 256 * 1024;  // <1 MiB/s
        return 1024 * 1024;                                           // default
    }
    private sealed class ProgressReadStream : Stream
    {
        private const int ReportEveryBytes = 64 * 1024;

        private readonly Stream _inner;
        private readonly IProgress<long> _progress;
        private long _totalRead;
        private long _sinceReport;

        public ProgressReadStream(Stream inner, IProgress<long> progress)
        {
            _inner = inner;
            _progress = progress;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        private void OnRead(int read)
        {
            if (read <= 0)
            {
                if (_sinceReport > 0)
                {
                    _sinceReport = 0;
                    _progress.Report(_totalRead);
                }
                return;
            }

            _totalRead += read;
            _sinceReport += read;

            if (_sinceReport >= ReportEveryBytes)
            {
                _sinceReport = 0;
                _progress.Report(_totalRead);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = _inner.Read(buffer, offset, count);
            OnRead(read);
            return read;
        }

        public override int Read(Span<byte> buffer)
        {
            var read = _inner.Read(buffer);
            OnRead(read);
            return read;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var read = await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
            OnRead(read);
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            OnRead(read);
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }

    private static HttpClient CreateDirectUploadClient()
    {
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = 64,
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
            AutomaticDecompression = DecompressionMethods.None,

            AllowAutoRedirect = false,
        };

        var client = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromMinutes(120),

            DefaultRequestVersion = HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact
        };

        return client;
    }
}
