using FFXIVClientStructs.FFXIV.Component.Text;
using K4os.Compression.LZ4.Legacy;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Dto.Files;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI;
using RavaSync.Utils;
using RavaSync.WebAPI.Files.Models;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Globalization;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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
    private readonly SemaphoreSlim _centralRepairUploadSemaphore = new(1, 1);
    private CancellationTokenSource? _uploadCancellationTokenSource = new();
    private static readonly TimeSpan VerifiedHashTtl = TimeSpan.FromHours(12);
    private const int MaxCdnExistenceChecks = 8;
    private const int HealthyCdnProbeAttempts = 3;
    private const int MaxConfiguredParallelUploads = 12;
    private const long TinyUploadThresholdBytes = 64L * 1024;
    private const long SmallUploadThresholdBytes = 256L * 1024;
    private const long SmallUploadInMemoryThresholdBytes = 1024L * 1024;
    private const long TinyUploadProgressThresholdBytes = 128L * 1024;
    private static readonly int UploadPayloadPreparationParallelism = GetUploadPayloadPreparationParallelism();
    private static readonly SemaphoreSlim UploadPayloadPreparationSemaphore = new(UploadPayloadPreparationParallelism, UploadPayloadPreparationParallelism);

    private static int GetUploadPayloadPreparationParallelism()
    {
        var cpu = Environment.ProcessorCount;
        if (cpu <= 8) return 1;
        if (cpu <= 16) return 2;
        if (cpu <= 24) return 3;
        return 4;
    }


    public FileUploadManager(ILogger<FileUploadManager> logger, MareMediator mediator,MareConfigService mareConfigService,FileTransferOrchestrator orchestrator,FileCacheManager fileDbManager,ServerConfigurationManager serverManager) : base(logger, mediator)
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
    private readonly object _currentUploadsLock = new();
    private enum AuthMode { NeedAuthorization, PreAuthorized }

    public bool IsUploading
    {
        get
        {
            lock (_currentUploadsLock)
            {
                return CurrentUploads.Count > 0;
            }
        }
    }

    public FileTransfer[] GetCurrentUploadsSnapshot()
    {
        lock (_currentUploadsLock)
        {
            return CurrentUploads.ToArray();
        }
    }

    private void CompleteAndClearCurrentUploads(bool publishRefresh = true)
    {
        lock (_currentUploadsLock)
        {
            foreach (var upload in CurrentUploads.OfType<UploadFileTransfer>())
            {
                try
                {
                    if (upload.Total > 0)
                        upload.Transferred = upload.Total;
                }
                catch
                {
                    // best effort
                }
            }

            CurrentUploads.Clear();
        }

        if (publishRefresh)
        {
            try
            {
                Mediator.Publish(new RefreshUiMessage());
            }
            catch
            {
                // best effort
            }
        }
    }

    private void ClearCurrentUploads()
    {
        lock (_currentUploadsLock)
        {
            CurrentUploads.Clear();
        }
    }

    public bool CancelUpload()
    {
        lock (_currentUploadsLock)
        {
            if (CurrentUploads.Count <= 0)
                return false;
        }

        Logger.LogDebug("Cancelling current upload");
        _uploadCancellationTokenSource?.Cancel();
        _uploadCancellationTokenSource?.Dispose();
        _uploadCancellationTokenSource = null;
        CompleteAndClearCurrentUploads();
        return true;
    }

    public async Task DeleteAllFiles()
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");

        await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesDeleteAllFullPath(_orchestrator.UploadCdnUri!)).ConfigureAwait(false);
    }

    public Task<List<string>> UploadFiles(List<string> hashesToUpload, IProgress<string> progress, CancellationToken? ct = null)
    {
        return UploadFilesCore(hashesToUpload, progress, ct ?? CancellationToken.None, AuthMode.NeedAuthorization, Array.Empty<string>());
    }

    public async Task<List<string>> ForceUploadMissingHashesAsync(IReadOnlyCollection<string> hashes, string reason, CancellationToken ct = default)
    {
        var toUpload = hashes?
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Distinct(StringComparer.Ordinal)
            .ToList() ?? [];

        if (toUpload.Count == 0)
            return [];

        await _centralRepairUploadSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            Logger.LogWarning("Central B2 force-upload requested for {count} missing hash(es). Reason={reason}. Hashes={hashes}",
                toUpload.Count,
                reason,
                string.Join(", ", toUpload.Take(20)));

            var prog = new Progress<string>(msg => Logger.LogInformation(msg));
            var failed = await UploadFilesCore(toUpload, prog, ct, AuthMode.PreAuthorized, Array.Empty<string>(), forceUploadTickets: true).ConfigureAwait(false);

            if (failed.Count > 0)
            {
                Logger.LogWarning("Central B2 force-upload incomplete; {count} hash(es) failed: {hashes}",
                    failed.Count,
                    string.Join(", ", failed.Take(20)));
            }
            else
            {
                Logger.LogInformation("Central B2 force-upload completed for {count} missing hash(es)", toUpload.Count);
            }

            return failed;
        }
        finally
        {
            _centralRepairUploadSemaphore.Release();
        }
    }


    private bool TryValidateUploadSource(string fileHash, FileCacheEntity? cacheEntry)
    {
        try
        {
            var filePath = cacheEntry?.ResolvedFilepath;
            if (string.IsNullOrWhiteSpace(filePath))
            {
                Logger.LogWarning("[{hash}] Upload source path is empty", fileHash);
                return false;
            }

            var fi = new FileInfo(filePath);
            if (!fi.Exists)
            {
                Logger.LogWarning("[{hash}] Upload source file does not exist: {path}", fileHash, filePath);
                return false;
            }

            if (fi.Length <= 0)
            {
                Logger.LogWarning("[{hash}] Upload source file is zero-length: {path}", fileHash, filePath);
                return false;
            }

            var sizeMatches = !cacheEntry?.Size.HasValue ?? true;
            if (cacheEntry?.Size.HasValue == true)
            {
                sizeMatches = cacheEntry.Size.Value == fi.Length;
            }

            var ticksMatches = true;
            if (!string.IsNullOrWhiteSpace(cacheEntry?.LastModifiedDateTicks))
            {
                ticksMatches = string.Equals(
                    fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture),
                    cacheEntry!.LastModifiedDateTicks,
                    StringComparison.Ordinal);
            }

            if (sizeMatches && ticksMatches)
                return true;

            Logger.LogDebug(
                "[{hash}] Upload source metadata changed; falling back to hash validation. CachedSize={cachedSize}, CurrentSize={currentSize}, CachedTicks={cachedTicks}, CurrentTicks={currentTicks}, Path={path}",
                fileHash,
                cacheEntry?.Size,
                fi.Length,
                cacheEntry?.LastModifiedDateTicks,
                fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture),
                filePath);

            var computedHash = Crypto.GetFileHash(fi.FullName);
            if (!string.Equals(computedHash, fileHash, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogWarning(
                    "[{hash}] Upload source hash mismatch after metadata change. Computed={computed}, Path={path}",
                    fileHash, computedHash, filePath);
                return false;
            }

            cacheEntry!.Size = fi.Length;
            cacheEntry.CompressedSize = null;
            cacheEntry.LastModifiedDateTicks = fi.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture);
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "[{hash}] Failed validating upload source", fileHash);
            return false;
        }
    }

    private Uri GetUploadTicketUri(string fileHash, bool forceUploadTicket)
    {
        var uri = MareFiles.ServerFilesUploadTicketFullPath(_orchestrator.UploadCdnUri!, fileHash);

        if (!forceUploadTicket)
            return uri;

        var builder = new UriBuilder(uri);
        var query = builder.Query;

        if (string.IsNullOrWhiteSpace(query))
        {
            builder.Query = "force=true";
        }
        else
        {
            builder.Query = query.TrimStart('?') + "&force=true";
        }

        return builder.Uri;
    }

    private async Task UploadFileStreamedFromDisk(string fileHash, string filePath, bool munged, IProgress<UploadProgress>? progress, CancellationToken uploadToken, int streamBufferSize, bool forceUploadTicket = false)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        uploadToken.ThrowIfCancellationRequested();

        Logger.LogDebug("[{hash}] Preparing file for upload", fileHash);

        var fileSize = new FileInfo(filePath).Length;

        var ticketUri = GetUploadTicketUri(fileHash, forceUploadTicket);
        var ticketReq = new UploadTicketRequestDto(fileSize, munged, 0);

        using var ticketResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, ticketUri, ticketReq, uploadToken).ConfigureAwait(false);
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
        {
            if (await VerifyCdnObjectHealthyAsync(fileHash, uploadToken).ConfigureAwait(false))
                return;

            if (forceUploadTicket)
                throw new InvalidOperationException($"[{fileHash}] Forced upload ticket still reported UploadRequired=false while CDN/B2 object is missing.");

            Logger.LogWarning("[{hash}] Server reported upload not required, but CDN object was missing/empty; retrying with forced upload ticket", fileHash);

            ticketUri = GetUploadTicketUri(fileHash, forceUploadTicket: true);
            using var forcedTicketResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, ticketUri, ticketReq, uploadToken).ConfigureAwait(false);
            forcedTicketResp.EnsureSuccessStatusCode();

            var forcedTicketJson = await forcedTicketResp.Content.ReadAsStringAsync(uploadToken).ConfigureAwait(false);
            ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(forcedTicketJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (ticket == null || !string.Equals(ticket.Mode, "DirectB2", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"[{fileHash}] Forced upload ticket failed; cannot continue.");

            if (!ticket.UploadRequired)
                throw new InvalidOperationException($"[{fileHash}] Forced upload ticket did not require upload while CDN/B2 object is missing.");
        }

        Logger.LogDebug("[{hash}] Direct upload ticket acquired, preparing payload (LZ4)", fileHash);

        TempLz4UploadFile? temp = null;
        byte[]? inMemoryPayload = null;
        long rawSize;
        long compressedSize;
        string md5Base64;

        try
        {
            var rawProgress = progress == null ? null : new Progress<long>(b => progress.Report(new UploadProgress(b, fileSize)));

            if (fileSize <= SmallUploadInMemoryThresholdBytes)
            {
                // Small files stay ungated so upload storms still feel snappy.
                var prepared = await PrepareSmallUploadInMemoryAsync(filePath, uploadToken, rawProgress).ConfigureAwait(false);
                inMemoryPayload = prepared.PayloadBytes;
                rawSize = prepared.RawSize;
                compressedSize = prepared.CompressedSize;
                md5Base64 = prepared.Md5Base64;
            }
            else
            {
                // Large LZ4 preparation is CPU/disk-heavy. Pace prep separately from network upload
                // so uploads remain fast without several compressors hammering the game process at once.
                await UploadPayloadPreparationSemaphore.WaitAsync(uploadToken).ConfigureAwait(false);
                try
                {
                    temp = await TempLz4UploadFile.CreateAsync(filePath, uploadToken, rawProgress).ConfigureAwait(false);
                }
                finally
                {
                    UploadPayloadPreparationSemaphore.Release();
                }

                if (temp == null)
                    throw new InvalidOperationException($"[{fileHash}] Failed to prepare temporary upload payload.");

                rawSize = temp.RawSize;
                compressedSize = temp.CompressedSize;
                md5Base64 = temp.Md5Base64;
            }

            if (rawSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length raw payload.");

            if (compressedSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length compressed payload.");

            if (inMemoryPayload != null && inMemoryPayload.LongLength != compressedSize)
                throw new InvalidDataException($"[{fileHash}] In-memory payload size mismatch (expected {compressedSize}, got {inMemoryPayload.LongLength}).");

            if (temp != null)
            {
                var fi = new FileInfo(temp.TempPath);
                if (!fi.Exists || fi.Length != compressedSize)
                    throw new InvalidDataException($"[{fileHash}] Temporary upload payload size mismatch (expected {compressedSize}, got {fi.Length}).");
            }

            progress?.Report(new UploadProgress(rawSize, rawSize));

            const int maxAttempts = 4;
            Exception? last = null;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                uploadToken.ThrowIfCancellationRequested();

                try
                {
                    await using var fs0 = OpenPreparedUploadPayloadStream(inMemoryPayload, temp, streamBufferSize);

                    var netProgress = progress == null
                        ? null
                        : new Progress<long>(sent =>
                        {
                            var rawTotal = rawSize;
                            var netTotal = compressedSize;

                            long mapped = sent;
                            if (netTotal > 0 && rawTotal > 0)
                                mapped = (long)((double)sent * rawTotal / netTotal);

                            if (mapped > rawTotal) mapped = rawTotal;

                            progress.Report(new UploadProgress(mapped, rawTotal));
                        });

                    await using Stream fs = netProgress == null ? fs0 : new ProgressReadStream(fs0, netProgress);

                    using var put = new HttpRequestMessage(HttpMethod.Put, ticket.UploadUrl);

                    var content = new StreamContent(fs, streamBufferSize);
                    content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                    content.Headers.ContentLength = compressedSize;
                    try { content.Headers.ContentMD5 = Convert.FromBase64String(md5Base64); } catch { }

                    put.Content = content;
                    put.Headers.ExpectContinue = false;

                    using var putResp = await _directUploadClient.SendAsync(put, HttpCompletionOption.ResponseHeadersRead, uploadToken).ConfigureAwait(false);

                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[{hash}] Direct upload PUT attempt {attempt}/{max} Status: {status}",
                            fileHash, attempt, maxAttempts, putResp.StatusCode);
                    }

                    putResp.EnsureSuccessStatusCode();
                    progress?.Report(new UploadProgress(rawSize, rawSize));

                    var etag = putResp.Headers.ETag?.Tag;

                    var completeUri = MareFiles.ServerFilesUploadCompleteFullPath(_orchestrator.UploadCdnUri!, fileHash);
                    var complete = new UploadCompleteDto(rawSize, compressedSize, md5Base64, etag, true);

                    using var completeResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, completeUri, complete, uploadToken).ConfigureAwait(false);
                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[{hash}] UploadComplete Status: {status}", fileHash, completeResp.StatusCode);
                    }
                    completeResp.EnsureSuccessStatusCode();

                    return;
                }
                catch (Exception ex) when (attempt < maxAttempts)
                {
                    last = ex;

                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug(ex, "[{hash}] Direct upload PUT failed (attempt {attempt}/{max}); retrying",
                            fileHash, attempt, maxAttempts);

                    if (IsLikelyTransientTlsReset(ex))
                    {
                        Logger.LogWarning("[{hash}] Upload failed due to transient TLS/reset; reacquiring DirectB2 ticket before retry", fileHash);

                        using var ticketResp2 = await _orchestrator
                            .SendRequestAsync(HttpMethod.Post, GetUploadTicketUri(fileHash, forceUploadTicket), ticketReq, uploadToken)
                            .ConfigureAwait(false);

                        ticketResp2.EnsureSuccessStatusCode();

                        var ticketJson2 = await ticketResp2.Content.ReadAsStringAsync(uploadToken).ConfigureAwait(false);
                        ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(
                            ticketJson2, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                        if (ticket == null || !string.Equals(ticket.Mode, "DirectB2", StringComparison.OrdinalIgnoreCase))
                            throw new InvalidOperationException($"[{fileHash}] Ticket refresh failed; cannot continue.", ex);

                        if (!ticket.UploadRequired)
                        {
                            if (await VerifyCdnObjectHealthyAsync(fileHash, uploadToken).ConfigureAwait(false))
                            {
                                Logger.LogDebug("[{hash}] Ticket refresh reports upload not required and CDN object is healthy; treating as success", fileHash);
                                return;
                            }

                            if (forceUploadTicket)
                                throw new InvalidOperationException($"[{fileHash}] Forced refreshed upload ticket still reported UploadRequired=false while CDN/B2 object is missing.");

                            Logger.LogWarning("[{hash}] Ticket refresh reported upload not required, but CDN object was missing/empty; retrying upload", fileHash);
                        }
                    }

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
        finally
        {
            if (temp != null)
                await temp.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async Task<(CharacterData Data, bool Success, string? Error)> UploadFiles(CharacterData data, List<UserData> visiblePlayers)
    {
        CancelUpload();

        _uploadCancellationTokenSource = new CancellationTokenSource();
        var uploadToken = _uploadCancellationTokenSource.Token;

        try
        {
            Logger.LogDebug("Preparing Character data {hash} for upload/share against service {url}",
                data.DataHash.Value, _serverManager.CurrentApiUrl);

            foreach (var kvp in data.FileReplacements)
            {
                data.FileReplacements[kvp.Key].RemoveAll(i =>
                    _orchestrator.ForbiddenTransfers.Exists(f =>
                        string.Equals(f.Hash, i.Hash, StringComparison.OrdinalIgnoreCase)));
            }

            var allCandidateHashes = GetAllCandidateHashes(data);
            if (allCandidateHashes.Count == 0)
                return (data, true, null);

            var presentLocal = allCandidateHashes
                .Where(h => _fileDbManager.GetFileCacheByHash(h) != null)
                .ToHashSet(StringComparer.Ordinal);

            var missingLocal = allCandidateHashes
                .Except(presentLocal, StringComparer.Ordinal)
                .ToList();

            if (missingLocal.Count > 0)
            {
                Logger.LogWarning(
                    "Upload aborted: {count} hashes are referenced by character data but missing locally (first 20): {list}",
                    missingLocal.Count, string.Join(", ", missingLocal.Take(20)));

                return (data, false, $"{missingLocal.Count} file(s) missing locally");
            }

            var uids = (visiblePlayers ?? new List<UserData>())
                .Select(p => p.UID)
                .Where(u => !string.IsNullOrWhiteSpace(u))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            Logger.LogDebug("Authorizing/share-check for {count} hashes to {uids} recipients", presentLocal.Count, uids.Count);

            var auth = await FilesSend([.. presentLocal], uids, uploadToken).ConfigureAwait(false);

            var now = DateTime.UtcNow;
            foreach (var h in presentLocal)
                _verifiedUploadedHashes[h] = now;

            foreach (var f in auth.Where(f => f.IsForbidden))
            {
                if (_orchestrator.ForbiddenTransfers.TrueForAll(x => !string.Equals(x.Hash, f.Hash, StringComparison.Ordinal)))
                {
                    _orchestrator.ForbiddenTransfers.Add(new UploadFileTransfer(f)
                    {
                        LocalFile = _fileDbManager.GetFileCacheByHash(f.Hash)?.ResolvedFilepath ?? string.Empty,
                    });
                }

                _verifiedUploadedHashes[f.Hash] = now;
                RemoveHashFromCharacterData(data, f.Hash);
            }

            var toPush = auth.Where(f => !f.IsForbidden).Select(f => f.Hash).Distinct(StringComparer.Ordinal).ToList();
            var failed = new List<string>();

            if (toPush.Count > 0)
            {
                Logger.LogDebug("Need to upload {count} hashes for this payload", toPush.Count);

                var prog = new Progress<string>(msg => Logger.LogInformation(msg));
                failed = await UploadFilesCore(toPush, prog, uploadToken, AuthMode.PreAuthorized).ConfigureAwait(false);

                if (failed.Any())
                {
                    var recovered = await RecheckFailedUploadsAsync(failed, uploadToken).ConfigureAwait(false);

                    if (recovered.Count > 0)
                    {
                        failed = failed.Except(recovered, StringComparer.Ordinal).ToList();

                        Logger.LogWarning("Recovered {count} upload(s) after failure via server recheck: {list}", recovered.Count, string.Join(", ", recovered.Take(20)));
                    }
                }

                if (failed.Any())
                {
                    Logger.LogWarning("Upload batch incomplete ({count} failed): {list}", failed.Count, string.Join(", ", failed.Take(20)));
                    return (data, false, $"{failed.Count} upload(s) failed");
                }
            }

            var prewarmHashes = toPush.Except(failed, StringComparer.Ordinal).Distinct(StringComparer.Ordinal).ToArray();
            QueueCdnPrewarm(prewarmHashes);

            Logger.LogDebug("Upload/share barrier complete for {hash}", data.DataHash.Value);
            return (data, true, null);
        }
        catch (OperationCanceledException)
        {
            return (data, false, "Upload cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error during character payload upload/share");
            return (data, false, ex.Message);
        }
        finally
        {
            try { _uploadCancellationTokenSource?.Dispose(); } catch { }
            _uploadCancellationTokenSource = null;

            CompleteAndClearCurrentUploads();
        }
    }

    private static HashSet<string> GetAllCandidateHashes(CharacterData data)
    {
        HashSet<string> hashes = new(StringComparer.Ordinal);

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

                hashes.Add(hash);
            }
        }

        return hashes;
    }

    private static void RemoveHashFromCharacterData(CharacterData data, string hash)
    {
        foreach (var kvp in data.FileReplacements)
        {
            kvp.Value.RemoveAll(fr => string.Equals(fr.Hash, hash, StringComparison.OrdinalIgnoreCase));
        }
    }

    private void QueueCdnPrewarm(IEnumerable<string> hashes)
    {
        if (_orchestrator.FilesCdnUri == null)
            return;

        var hashesToPrewarm = hashes.Where(h => !string.IsNullOrWhiteSpace(h)).Distinct(StringComparer.Ordinal).ToArray();

        if (hashesToPrewarm.Length == 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await CdnPrewarmHelper.PrewarmAsync(_orchestrator.FilesCdnUri, hashesToPrewarm, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "CDN prewarm failed (ignored)");
            }
        });
    }

    private static async Task<(byte[] PayloadBytes, long RawSize, long CompressedSize, string Md5Base64)> PrepareSmallUploadInMemoryAsync(string filePath, CancellationToken ct, IProgress<long>? rawReadProgress)
    {
        const int BlockSize = 256 * 1024;

        await using var input = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        await using var output = new MemoryStream(capacity: (int)Math.Min(input.Length + (64 * 1024), SmallUploadInMemoryThresholdBytes * 2));
        await using var lz4 = new LZ4Stream(output, LZ4StreamMode.Compress, LZ4StreamFlags.None, BlockSize);

        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(BlockSize);
        long raw = 0;
        const long YieldEveryBytes = 2L * 1024 * 1024;
        long sinceYield = 0;

        try
        {
            int read;
            while ((read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false)) > 0)
            {
                await lz4.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                raw += read;
                sinceYield += read;
                rawReadProgress?.Report(raw);

                if (sinceYield >= YieldEveryBytes)
                {
                    sinceYield = 0;
                    await Task.Yield();
                    ct.ThrowIfCancellationRequested();
                }
            }

            await lz4.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }

        if (raw <= 0)
            throw new InvalidDataException($"Refusing to prepare zero-length upload payload from {filePath}");

        var payloadBytes = output.ToArray();
        if (payloadBytes.LongLength <= 0)
            throw new InvalidDataException($"Compressed in-memory payload was zero-length for {filePath}");

        using var md5 = MD5.Create();
        var md5Base64 = Convert.ToBase64String(md5.ComputeHash(payloadBytes));

        return (payloadBytes, raw, payloadBytes.LongLength, md5Base64);
    }

    private static Stream OpenPreparedUploadPayloadStream(byte[]? inMemoryPayload, TempLz4UploadFile? temp, int streamBufferSize)
    {
        if (inMemoryPayload != null)
            return new MemoryStream(inMemoryPayload, writable: false);

        if (temp == null)
            throw new InvalidOperationException("No prepared upload payload is available.");

        return new FileStream(temp.TempPath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: Math.Max(streamBufferSize, 64 * 1024), options: FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    private async Task<bool> VerifyCdnObjectHealthyAsync(string fileHash, CancellationToken ct)
    {
        if (_orchestrator.FilesCdnUri == null || string.IsNullOrWhiteSpace(fileHash))
            return false;

        var cdnUrl = MareFiles.CdnGetFullPath(_orchestrator.FilesCdnUri, fileHash.ToUpperInvariant());

        for (int attempt = 1; attempt <= HealthyCdnProbeAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, cdnUrl);
                req.Headers.Range = new RangeHeaderValue(0, 0);

                using var resp = await _directUploadClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                if (!resp.IsSuccessStatusCode && resp.StatusCode != HttpStatusCode.PartialContent)
                {
                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug("[{hash}] CDN health probe attempt {attempt}/{max} returned {status}", fileHash, attempt, HealthyCdnProbeAttempts, resp.StatusCode);
                }
                else
                {
                    var totalLength = resp.Content.Headers.ContentRange?.Length ?? resp.Content.Headers.ContentLength ?? 0;
                    if (totalLength > 0)
                        return true;

                    Logger.LogWarning("[{hash}] CDN health probe returned success but zero-length content", fileHash);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[{hash}] CDN health probe failed on attempt {attempt}/{max}", fileHash, attempt, HealthyCdnProbeAttempts);
            }

            if (attempt < HealthyCdnProbeAttempts)
                await Task.Delay(150 * attempt, ct).ConfigureAwait(false);
        }

        return false;
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
        CompleteAndClearCurrentUploads();
        _verifiedUploadedHashes.Clear();
    }


    private async Task<List<string>> RecheckFailedUploadsAsync(IEnumerable<string> failedHashes, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized) return [];

        var recovered = new List<string>();
        var now = DateTime.UtcNow;

        int checkedCount = 0;

        foreach (var hash in failedHashes.Distinct(StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();

            if (checkedCount >= MaxCdnExistenceChecks)
                break;

            checkedCount++;

            try
            {
                var cache = _fileDbManager.GetFileCacheByHash(hash);
                if (cache == null || string.IsNullOrWhiteSpace(cache.ResolvedFilepath) || !File.Exists(cache.ResolvedFilepath))
                    continue;

                var fileSize = new FileInfo(cache.ResolvedFilepath).Length;

                var ticketUri = MareFiles.ServerFilesUploadTicketFullPath(_orchestrator.UploadCdnUri!, hash);
                var ticketReq = new UploadTicketRequestDto(fileSize, _mareConfigService.Current.UseAlternativeFileUpload, 0);

                using var ticketResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, ticketUri, ticketReq, ct).ConfigureAwait(false);

                if (!ticketResp.IsSuccessStatusCode)
                    continue;

                var ticketJson = await ticketResp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                var ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(
                    ticketJson,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (ticket == null)
                    continue;
                if (!ticket.UploadRequired)
                {
                    if (await VerifyCdnObjectHealthyAsync(hash, ct).ConfigureAwait(false))
                    {
                        _verifiedUploadedHashes[hash] = now;
                        recovered.Add(hash);

                        Logger.LogDebug("[{hash}] Recheck recovered upload (server reports UploadRequired=false and CDN object is healthy)", hash);
                    }
                    else
                    {
                        Logger.LogWarning("[{hash}] Recheck found UploadRequired=false but CDN object was missing/empty; will not treat as recovered", hash);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "[{hash}] Recheck failed (ignored)", hash);
            }
        }

        return recovered;
    }
    private async Task UploadUnverifiedFiles(HashSet<string> unverifiedUploadHashes, List<UserData> visiblePlayers, CancellationToken uploadToken)
    {
        var localPathByHash = unverifiedUploadHashes
            .Select(h => new { Hash = h, Cache = _fileDbManager.GetFileCacheByHash(h) })
            .Where(x => x.Cache != null)
            .ToDictionary(x => x.Hash, x => x.Cache!.ResolvedFilepath, StringComparer.Ordinal);

        unverifiedUploadHashes = localPathByHash.Keys.ToHashSet(StringComparer.Ordinal);

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
                    LocalFile = localPathByHash.TryGetValue(f.Hash, out var localPath) ? localPath : string.Empty,
                });
            }
            _verifiedUploadedHashes[f.Hash] = DateTime.UtcNow;
        }

        // Allowed hashes to push
        var toPush = filesToUpload.Where(f => !f.IsForbidden).Select(f => f.Hash).Distinct(StringComparer.Ordinal).ToList();

        if (toPush.Count == 0) return;

        var prog = new Progress<string>(msg => Logger.LogInformation(msg));
        var failed = await UploadFilesCore(toPush, prog, uploadToken, AuthMode.PreAuthorized).ConfigureAwait(false);

        if (failed.Any())
            Logger.LogInformation("Some uploads did not complete ({count}): {list}", failed.Count, string.Join(", ", failed));

    }

    private async Task<List<string>> UploadFilesCore(IEnumerable<string> inputHashes, IProgress<string>? progress, CancellationToken ct, AuthMode authMode, IReadOnlyList<string>? uidsForFilesSend = null, bool forceUploadTickets = false)
    {
        var token = ct;

        try
        {

            // Cache local paths once for this batch (reduces repeated cache/db lookups)
            var cacheEntryByHash = inputHashes
            .Distinct(StringComparer.Ordinal)
            .Select(h => new { Hash = h, Cache = _fileDbManager.GetFileCacheByHash(h) })
            .Where(x => x.Cache != null)
            .ToDictionary(x => x.Hash, x => x.Cache!, StringComparer.Ordinal);

            var localPathByHash = cacheEntryByHash
                .ToDictionary(x => x.Key, x => x.Value.ResolvedFilepath, StringComparer.Ordinal);

            // ---- Local presence check ----
            var present = inputHashes.Where(h => localPathByHash.ContainsKey(h)).ToHashSet(StringComparer.Ordinal);

            var missingLocal = inputHashes.Except(present, StringComparer.Ordinal).ToList();
            if (missingLocal.Any())
                return missingLocal;

            var invalidLocal = new List<string>();

            foreach (var hash in present)
            {
                if (!cacheEntryByHash.TryGetValue(hash, out var cacheEntry) || !TryValidateUploadSource(hash, cacheEntry))
                {
                    invalidLocal.Add(hash);
                }
            }

            if (invalidLocal.Any())
                return missingLocal.Concat(invalidLocal).Distinct(StringComparer.Ordinal).ToList();

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
                        LocalFile = localPathByHash.TryGetValue(f.Hash, out var localPath) ? localPath : string.Empty,
                    });
                }

                _verifiedUploadedHashes[f.Hash] = DateTime.UtcNow;
            }

            allowedHashes = auth.Where(f => !f.IsForbidden).Select(f => f.Hash).Distinct(StringComparer.Ordinal).ToList();
        }
        else
        {
            allowedHashes = present.ToList();
        }

        if (allowedHashes.Count == 0)
            return forbidden;

        // ---- UI bookkeeping ----
        var sizeByHash = new Dictionary<string, long>(StringComparer.Ordinal);

        var uploadEntryByHash = new System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer>(StringComparer.Ordinal);

            lock (_currentUploadsLock)
            {
                foreach (var existing in CurrentUploads)
                {
                    if (existing is UploadFileTransfer upload && !string.IsNullOrEmpty(upload.Hash))
                        uploadEntryByHash[upload.Hash] = upload;
                }

                foreach (var h in allowedHashes)
                {
                    try
                    {
                        if (!uploadEntryByHash.TryGetValue(h, out var entry))
                        {
                            if (!localPathByHash.TryGetValue(h, out var localPath)) continue;

                            var size = new FileInfo(localPath).Length;
                            sizeByHash[h] = size;

                            entry = new UploadFileTransfer(new UploadFileDto { Hash = h })
                            {
                                Total = size,
                            };

                            CurrentUploads.Add(entry);
                            uploadEntryByHash[h] = entry;
                        }
                        else if (!sizeByHash.ContainsKey(h) && localPathByHash.TryGetValue(h, out var localPath))
                        {
                            sizeByHash[h] = new FileInfo(localPath).Length;
                        }
                    }
                    catch
                    {
                        // best effort
                    }
                }
            }

            const int MaxUploadAttempts = 3;

            // fixed if user explicitly configured, adaptive only for Auto (0)
            var configured = _mareConfigService.Current.ParallelUploads;
            var cpuAuto = GetCpuAutoParallelUploads();
            var isAuto = configured <= 0;

            var maxParallel = isAuto ? Math.Clamp(cpuAuto, 1, MaxConfiguredParallelUploads) : Math.Clamp(configured, 1, MaxConfiguredParallelUploads);

            var mostlySmallFiles = false;
            var tinyHeavyFiles = false;
            if (allowedHashes.Count >= 4 && sizeByHash.Count > 0)
            {
                var small = 0;
                var tiny = 0;
                foreach (var s in sizeByHash.Values)
                {
                    if (s <= SmallUploadThresholdBytes) small++;
                    if (s <= TinyUploadThresholdBytes) tiny++;
                }

                mostlySmallFiles = small >= (sizeByHash.Count * 3 / 4);
                tinyHeavyFiles = allowedHashes.Count >= 8 && tiny >= (sizeByHash.Count * 3 / 4);
            }

            var desiredParallel = isAuto
                ? (mostlySmallFiles ? Math.Min(maxParallel, maxParallel <= 3 ? maxParallel : 4) : 1)
                : maxParallel;
            var orderedHashes = allowedHashes
                .OrderByDescending(h => sizeByHash.TryGetValue(h, out var size) ? size : 0)
                .ThenBy(h => h, StringComparer.Ordinal)
                .ToList();

            var bytesSinceSample = 0L;
            var lastUploadedByHash = new System.Collections.Concurrent.ConcurrentDictionary<string, long>(StringComparer.Ordinal);

            var lastTraceBucketByHash = new System.Collections.Concurrent.ConcurrentDictionary<string, int>(StringComparer.Ordinal);

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

                var minSampleInterval = tinyHeavyFiles
                    ? (now < TimeSpan.FromSeconds(4) ? TimeSpan.FromMilliseconds(400) : TimeSpan.FromSeconds(1))
                    : (now < TimeSpan.FromSeconds(8) ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(2));

                if (now - lastSampleAt < minSampleInterval)
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
                var slot = tinyHeavyFiles
                    ? 128d * 1024d
                    : (mostlySmallFiles ? 192d * 1024d : 256d * 1024d);

                if (bps <= 0) return 1;

                var bySpeed = tinyHeavyFiles
                    ? (int)Math.Ceiling(bps / slot)
                    : (int)Math.Round(bps / slot, MidpointRounding.AwayFromZero);

                bySpeed = Math.Clamp(bySpeed, 1, maxParallel);

                return bySpeed;
            }

            var queue = new System.Collections.Concurrent.ConcurrentQueue<string>(orderedHashes);
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

                        if (tinyHeavyFiles && bps > 0)
                        {
                            if (bps < 192 * 1024)
                            {
                                desiredParallel = 1;
                                pendingDesired = 1;
                                pendingHits = 0;
                            }
                            else if (bps < 512 * 1024 && desiredParallel > 2)
                            {
                                desiredParallel = 2;
                                pendingDesired = 2;
                                pendingHits = 0;
                            }
                        }

                        // ---- Fast-start boost (one-time jump) ----
                        if (!fastStartUsed && desiredParallel <= (tinyHeavyFiles ? 4 : 2))
                        {
                            if (tinyHeavyFiles)
                            {
                                if (bps >= 12 * 1024 * 1024) // 12 MiB/s+
                                {
                                    desiredParallel = Math.Min(maxParallel, 10);
                                    pendingHits = 0;
                                    pendingDesired = desiredParallel;
                                    fastStartUsed = true;
                                    Interlocked.Exchange(ref bytesSinceSample, 0);
                                    lastSampleAt = sw.Elapsed;
                                }
                                else if (bps >= 6 * 1024 * 1024) // 6 MiB/s+
                                {
                                    desiredParallel = Math.Min(maxParallel, 8);
                                    pendingHits = 0;
                                    pendingDesired = desiredParallel;
                                    fastStartUsed = true;
                                    Interlocked.Exchange(ref bytesSinceSample, 0);
                                    lastSampleAt = sw.Elapsed;
                                }
                                else if (bps >= 2 * 1024 * 1024) // 2 MiB/s+
                                {
                                    desiredParallel = Math.Min(maxParallel, 6);
                                    pendingHits = 0;
                                    pendingDesired = desiredParallel;
                                    fastStartUsed = true;
                                    Interlocked.Exchange(ref bytesSinceSample, 0);
                                    lastSampleAt = sw.Elapsed;
                                }
                            }
                            else if (bps >= 8 * 1024 * 1024) // 8 MiB/s+
                            {
                                desiredParallel = Math.Min(maxParallel, 6);
                                pendingHits = 0;
                                pendingDesired = desiredParallel;
                                fastStartUsed = true;
                                Interlocked.Exchange(ref bytesSinceSample, 0);
                                lastSampleAt = sw.Elapsed;

                            }
                            else if (bps >= 2 * 1024 * 1024) // 2 MiB/s+
                            {
                                desiredParallel = Math.Min(maxParallel, 4);
                                pendingHits = 0;
                                pendingDesired = desiredParallel;
                                fastStartUsed = true;
                                Interlocked.Exchange(ref bytesSinceSample, 0);
                                lastSampleAt = sw.Elapsed;
                            }
                        }

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
                    if (!localPathByHash.TryGetValue(hash, out var filePath))
                    {
                        Logger.LogWarning("[{hash}] Upload skipped: cache entry disappeared", hash);
                        return;
                    }

                    var origSize = sizeByHash.TryGetValue(hash, out var s) ? s : new FileInfo(filePath).Length;
                    var useLiveProgress = origSize > TinyUploadProgressThresholdBytes;

                    IProgress<UploadProgress>? throttledProg = null;
                    if (useLiveProgress)
                    {
                        var prog = new Progress<UploadProgress>(up =>
                        {
                            try
                            {
                                if (uploadEntryByHash.TryGetValue(hash, out var entry))
                                {
                                    entry.Total = up.Size;
                                    entry.Transferred = up.Uploaded;
                                }

                                long delta = 0;
                                lastUploadedByHash.AddOrUpdate(hash, up.Uploaded, (_, old) =>
                                {
                                    delta = up.Uploaded - old;
                                    return up.Uploaded;
                                });

                                if (delta > 0)
                                    Interlocked.Add(ref bytesSinceSample, delta);

                                var pct = up.Size > 0 ? (int)(100L * up.Uploaded / up.Size) : 0;
                                var bucket = (pct / 25) * 25;

                                if (bucket is not (25 or 50 or 75 or 100))
                                    return;

                                var traceKey = hash + "|" + attempt.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                if (lastTraceBucketByHash.TryGetValue(traceKey, out var lastBucket) && lastBucket == bucket)
                                    return;

                                lastTraceBucketByHash[traceKey] = bucket;
                                Logger.LogTrace("[{idx}] {file}: {pct}% (attempt {attempt}/{max})", localIdx, Path.GetFileName(filePath), bucket, attempt, MaxUploadAttempts);
                            }
                            catch
                            {
                                // ignore
                            }
                        });

                        throttledProg = new ThrottledProgress<UploadProgress>(prog, TimeSpan.FromMilliseconds(150));
                    }

                    var buf = ChooseUploadStreamBufferSize(ewmaBps);

                    await UploadFileStreamedFromDisk(hash, filePath, _mareConfigService.Current.UseAlternativeFileUpload, throttledProg, tokenInner, buf, forceUploadTickets).ConfigureAwait(false);

                    if (!useLiveProgress)
                        Interlocked.Add(ref bytesSinceSample, origSize);

                    _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                    succeeded[hash] = 1;

                    if (uploadEntryByHash.TryGetValue(hash, out var finalEntry))
                        finalEntry.Transferred = finalEntry.Total;

                    return;
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

                        var traceKey = hash + "|" + attempt.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        lastTraceBucketByHash.TryRemove(traceKey, out _);
                    }
                }
        }

            // ---- Result classification ----
            var failed = allowedHashes
                .Where(h => !succeeded.ContainsKey(h))
                .Concat(forbidden)
                .Distinct(StringComparer.Ordinal)
                .ToList();

            return failed;
        }
        finally
        {
            // ---- UI: never leave "Uploading..." stuck once this batch is done/cancelled/faulted ----
            CompleteAndClearCurrentUploads();
            progress?.Report("No uploads in progress");
        }
    }

    private static bool IsLikelyTransientTlsReset(Exception ex)
    {
        if (ex is HttpRequestException hre)
        {
            var inner = hre.InnerException;
            while (inner != null)
            {
                if (inner is System.Net.Sockets.SocketException se && se.SocketErrorCode == System.Net.Sockets.SocketError.ConnectionReset)
                    return true;

                if (inner is IOException io && io.Message.Contains("forcibly closed", StringComparison.OrdinalIgnoreCase))
                    return true;

                inner = inner.InnerException;
            }
        }

        return false;
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
            cpu <= 24 ? 6 :
                        8;

        return Math.Clamp(auto, 1, MaxConfiguredParallelUploads);
    }

    private static int ChooseUploadStreamBufferSize(double ewmaBps)
    {
        if (ewmaBps > 0 && ewmaBps < 128 * 1024) return 64 * 1024;         // <128 KiB/s
        if (ewmaBps > 0 && ewmaBps < 1024 * 1024) return 256 * 1024;       // <1 MiB/s
        if (ewmaBps > 0 && ewmaBps < 8 * 1024 * 1024) return 1024 * 1024;  // <8 MiB/s
        return 2 * 1024 * 1024;                                             // fast links
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
