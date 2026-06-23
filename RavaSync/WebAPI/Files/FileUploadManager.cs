using FFXIVClientStructs.FFXIV.Component.Text;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Dto.Files;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using CharacterDataPushSanitizer = RavaSync.PlayerData.Data.CharacterDataPushSanitizer;
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
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _recentOutboundRecipientUids = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _locallyAbandonedUploadReservationHashes = new(StringComparer.Ordinal);
    private readonly object _outboundRecipientUidsLock = new();
    private HashSet<string> _outboundRecipientUids = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _centralRepairUploadSemaphore = new(1, 1);
    private readonly object _uploadCancellationLock = new();
    private CancellationTokenSource? _uploadCancellationTokenSource;
    private static readonly TimeSpan VerifiedHashTtl = TimeSpan.FromHours(12);
    private const int MaxCdnExistenceChecks = 8;
    private const int HealthyCdnProbeAttempts = 3;
    private const int MaxConfiguredParallelUploads = 32;
    private const int MaxSmallFileAutoParallelUploads = 64;
    private const int MaxTinyFileAutoParallelUploads = 96;
    private const long TinyUploadThresholdBytes = 256L * 1024;
    private const long SmallUploadThresholdBytes = 1024L * 1024;
    private const long RequestBoundUploadThresholdBytes = 1024L * 1024;
    private const long SmallUploadInMemoryThresholdBytes = 1024L * 1024;
    private const long TinyUploadProgressThresholdBytes = 512L * 1024;
    private const int LocalOutboundUploadStatusEchoSuppressMs = 2500;
    private const int LocalAbandonedUploadReservationRetryMs = 10 * 60 * 1000;
    private const int ReservedUploadWaitPollMs = 750;
    private const int ReservedUploadWaitTimeoutMs = 120_000;
    private const string WaitingForSharedFilesStatus = "Waiting for shared files to arrive";
    private const string FinalisingUploadsStatus = "Finalising uploads";
    private static readonly int UploadPayloadPreparationParallelism = GetUploadPayloadPreparationParallelism();
    private static readonly SemaphoreSlim UploadPayloadPreparationSemaphore = new(UploadPayloadPreparationParallelism, UploadPayloadPreparationParallelism);

    private static int GetUploadPayloadPreparationParallelism()
    {
        var cpu = Environment.ProcessorCount;
        if (cpu <= 4) return 1;
        if (cpu <= 8) return 2;
        if (cpu <= 16) return 3;
        if (cpu <= 24) return 4;
        return 5;
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

    public bool IsLocalOutboundUploadRecipient(string uid)
    {
        if (string.IsNullOrWhiteSpace(uid))
            return false;

        lock (_outboundRecipientUidsLock)
        {
            if (_outboundRecipientUids.Contains(uid))
                return true;
        }

        if (_recentOutboundRecipientUids.TryGetValue(uid, out var suppressUntilTick))
        {
            var remaining = unchecked(suppressUntilTick - Environment.TickCount64);
            if (remaining > 0)
                return true;

            _recentOutboundRecipientUids.TryRemove(uid, out _);
        }

        return false;
    }

    private void SetLocalOutboundUploadRecipients(IEnumerable<string> uids)
    {
        lock (_outboundRecipientUidsLock)
        {
            _outboundRecipientUids = uids
                .Where(u => !string.IsNullOrWhiteSpace(u))
                .Distinct(StringComparer.Ordinal)
                .ToHashSet(StringComparer.Ordinal);
        }
    }

    private void ClearLocalOutboundUploadRecipients()
    {
        lock (_outboundRecipientUidsLock)
        {
            if (_outboundRecipientUids.Count > 0)
            {
                var suppressUntilTick = unchecked(Environment.TickCount64 + LocalOutboundUploadStatusEchoSuppressMs);
                foreach (var uid in _outboundRecipientUids)
                    _recentOutboundRecipientUids[uid] = suppressUntilTick;
            }

            _outboundRecipientUids.Clear();
        }
    }

    private void MarkCurrentUploadsAsLocallyAbandonedReservations(string reason)
    {
        List<string> hashes;
        lock (_currentUploadsLock)
        {
            hashes = CurrentUploads
                .OfType<UploadFileTransfer>()
                .Select(u => u.Hash)
                .Where(h => !string.IsNullOrWhiteSpace(h))
                .Select(h => h.Trim().ToUpperInvariant())
                .Distinct(StringComparer.Ordinal)
                .ToList();
        }

        if (hashes.Count == 0)
            return;

        var expiresTick = unchecked(Environment.TickCount64 + LocalAbandonedUploadReservationRetryMs);
        foreach (var hash in hashes)
            _locallyAbandonedUploadReservationHashes[hash] = expiresTick;

        Logger.LogWarning(
            "Marked {count} upload hash(es) as locally abandoned after {reason}; the next upload may force-refresh their server reservations instead of waiting forever",
            hashes.Count,
            reason);
    }

    private bool IsRecentlyAbandonedLocalUploadReservation(string? hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return false;

        var key = hash.Trim().ToUpperInvariant();
        if (!_locallyAbandonedUploadReservationHashes.TryGetValue(key, out var expiresTick))
            return false;

        if (unchecked(expiresTick - Environment.TickCount64) > 0)
            return true;

        _locallyAbandonedUploadReservationHashes.TryRemove(key, out _);
        return false;
    }

    private void ClearLocallyAbandonedUploadReservation(string? hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return;

        _locallyAbandonedUploadReservationHashes.TryRemove(hash.Trim().ToUpperInvariant(), out _);
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

            // The global upload overlay is frame-driven, but a second refresh after the
            // last throttled progress callback has had time to flush prevents the UI
            // lingering at 100% after the actual upload list has been cleared.
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(250).ConfigureAwait(false);
                    if (!IsUploading)
                        Mediator.Publish(new RefreshUiMessage());
                }
                catch
                {
                    // best effort
                }
            });
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
        var hadVisibleUploads = false;
        lock (_currentUploadsLock)
        {
            hadVisibleUploads = CurrentUploads.Count > 0;
        }

        CancellationTokenSource? ctsToCancel;
        lock (_uploadCancellationLock)
        {
            ctsToCancel = _uploadCancellationTokenSource;
            _uploadCancellationTokenSource = null;
        }

        if (!hadVisibleUploads && ctsToCancel == null)
            return false;

        Logger.LogDebug("Cancelling current upload");
        MarkCurrentUploadsAsLocallyAbandonedReservations("local upload cancellation");
        try { ctsToCancel?.Cancel(); } catch { /* best effort */ }

        CompleteAndClearCurrentUploads();
        ClearLocalOutboundUploadRecipients();
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

    public Task<List<string>> UploadFiles(List<string> hashesToUpload, IReadOnlyList<string> recipientUids, IProgress<string> progress, CancellationToken? ct = null, bool forceUploadTickets = false)
    {
        return UploadFilesCore(hashesToUpload, progress, ct ?? CancellationToken.None, AuthMode.NeedAuthorization, recipientUids ?? Array.Empty<string>(), forceUploadTickets);
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
            Logger.LogDebug("Legacy missing-hash upload request for {count} hash(es). Reason={reason}. Hashes={hashes}",
                toUpload.Count,
                reason,
                string.Join(", ", toUpload.Take(20)));

            var prog = new Progress<string>(msg => Logger.LogInformation(msg));
            var failed = await UploadFilesCore(toUpload, prog, ct, AuthMode.PreAuthorized, Array.Empty<string>(), forceUploadTickets: false).ConfigureAwait(false);

            if (failed.Count > 0)
            {
                Logger.LogWarning("Legacy missing-hash upload flow incomplete; {count} hash(es) failed: {hashes}",
                    failed.Count,
                    string.Join(", ", failed.Take(20)));
            }
            else
            {
                Logger.LogDebug("Legacy missing-hash upload flow completed for {count} hash(es)", toUpload.Count);
            }

            return failed;
        }
        finally
        {
            _centralRepairUploadSemaphore.Release();
        }
    }


    private bool TryValidateUploadSource(string fileHash, FileCacheEntity? cacheEntry, bool requireHashValidation = false)
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

            if (!requireHashValidation && sizeMatches && ticksMatches)
                return true;

            Logger.LogDebug(
                requireHashValidation
                    ? "[{hash}] Forced repair upload source validation requested; verifying hash before upload. CachedSize={cachedSize}, CurrentSize={currentSize}, CachedTicks={cachedTicks}, CurrentTicks={currentTicks}, Path={path}"
                    : "[{hash}] Upload source metadata changed; falling back to hash validation. CachedSize={cachedSize}, CurrentSize={currentSize}, CachedTicks={cachedTicks}, CurrentTicks={currentTicks}, Path={path}",
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

    private async Task UploadFileStreamedFromDisk(string fileHash, string filePath, bool munged, IProgress<UploadProgress>? progress, CancellationToken uploadToken, int streamBufferSize, bool forceUploadTicket = false, Action? onBytesUploaded = null)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        uploadToken.ThrowIfCancellationRequested();

        Logger.LogDebug("[{hash}] Preparing file for upload", fileHash);

        var fileSize = new FileInfo(filePath).Length;

        var ticketUri = GetUploadTicketUri(fileHash, forceUploadTicket);
        // RawV2 uploads must be the exact bytes that produced the SHA1 hash.
        // The old alternative/munged upload path only applied to legacy LZ4 transport.
        var ticketReq = new UploadTicketRequestDto(fileSize, clientWouldMunge: false, clientParallelUploads: 0, payloadEncoding: FilePayloadEncoding.RawV2);

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
            Logger.LogDebug("[{hash}] Upload ticket reports upload not required; trusting server-side object state and skipping local CDN verification", fileHash);
            _verifiedUploadedHashes[fileHash] = DateTime.UtcNow;
            progress?.Report(new UploadProgress(fileSize, fileSize));
            return;
        }

        Logger.LogDebug("[{hash}] Direct upload ticket acquired, preparing raw payload", fileHash);

        byte[]? inMemoryPayload = null;
        long rawSize;
        long payloadSize;
        string md5Base64;

        try
        {
            if (fileSize <= SmallUploadInMemoryThresholdBytes)
            {
                // Small files stay in-memory, but the payload is now raw bytes, not LZ4.
                var prepared = await PrepareSmallRawUploadInMemoryAsync(filePath, uploadToken).ConfigureAwait(false);
                inMemoryPayload = prepared.PayloadBytes;
                rawSize = prepared.RawSize;
                payloadSize = prepared.PayloadSize;
                md5Base64 = prepared.Md5Base64;
            }
            else
            {
                // Large files are streamed straight from the final cache file.
                // We only precompute Content-MD5 for B2 integrity; no temp LZ4 payload is created.
                rawSize = fileSize;
                payloadSize = fileSize;
                md5Base64 = await ComputeFileMd5Base64Async(filePath, uploadToken).ConfigureAwait(false);
            }

            if (rawSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length raw payload.");

            if (payloadSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length payload.");

            if (rawSize != payloadSize)
                throw new InvalidDataException($"[{fileHash}] RawV2 payload size mismatch (raw={rawSize}, payload={payloadSize}).");

            if (inMemoryPayload != null && inMemoryPayload.LongLength != payloadSize)
                throw new InvalidDataException($"[{fileHash}] In-memory payload size mismatch (expected {payloadSize}, got {inMemoryPayload.LongLength}).");

            if (inMemoryPayload == null)
            {
                var fi = new FileInfo(filePath);
                if (!fi.Exists || fi.Length != payloadSize)
                    throw new InvalidDataException($"[{fileHash}] Source upload payload size mismatch (expected {payloadSize}, got {fi.Length}).");
            }

            progress?.Report(new UploadProgress(0, rawSize));

            const int maxAttempts = 4;
            Exception? last = null;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                uploadToken.ThrowIfCancellationRequested();

                try
                {
                    await using var fs0 = OpenRawUploadPayloadStream(inMemoryPayload, filePath, streamBufferSize);

                    var netProgress = progress == null
                        ? null
                        : new Progress<long>(sent =>
                        {
                            var rawTotal = rawSize;
                            var netTotal = payloadSize;

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
                    content.Headers.ContentLength = payloadSize;
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

                    // The B2 PUT is complete here, but the server-side UploadComplete
                    // metadata call is still part of the upload barrier. Surface that as
                    // finalisation instead of making the UI look frozen at 100%.
                    onBytesUploaded?.Invoke();
                    progress?.Report(new UploadProgress(rawSize, rawSize));

                    var etag = putResp.Headers.ETag?.Tag;

                    var completeUri = MareFiles.ServerFilesUploadCompleteFullPath(_orchestrator.UploadCdnUri!, fileHash);
                    var complete = new UploadCompleteDto(rawSize, payloadSize, md5Base64, etag, true, FilePayloadEncoding.RawV2);

                    using var completeResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, completeUri, complete, uploadToken).ConfigureAwait(false);
                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[{hash}] UploadComplete Status: {status}", fileHash, completeResp.StatusCode);
                    }
                    completeResp.EnsureSuccessStatusCode();
                    progress?.Report(new UploadProgress(rawSize, rawSize));

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
                            Logger.LogDebug("[{hash}] Ticket refresh reports upload not required; treating server-side object state as authoritative", fileHash);
                            _verifiedUploadedHashes[fileHash] = DateTime.UtcNow;
                            progress?.Report(new UploadProgress(fileSize, fileSize));
                            return;
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
            // RawV2 uses either an in-memory byte array or the original cache file; no temp payload cleanup required.
        }
    }



    private async Task<UploadCompleteBatchFileDto> UploadFileStreamedFromDiskWithTicket(string fileHash, string filePath, UploadTicketBatchFileResponseDto ticket, IProgress<UploadProgress>? progress, CancellationToken uploadToken, int streamBufferSize)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        uploadToken.ThrowIfCancellationRequested();

        if (!ticket.Success)
            throw new InvalidOperationException($"[{fileHash}] Batch upload ticket failed: {ticket.Error}");

        if (!string.Equals(ticket.Mode, "DirectB2", StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(ticket.UploadUrl))
            throw new InvalidOperationException($"[{fileHash}] Server did not provide a usable DirectB2 ticket (mode={ticket.Mode}).");

        var fileSize = new FileInfo(filePath).Length;
        Logger.LogDebug("[{hash}] Batch DirectB2 ticket acquired, preparing raw payload", fileHash);

        byte[]? inMemoryPayload = null;
        long rawSize;
        long payloadSize;
        string md5Base64;

        try
        {
            if (fileSize <= SmallUploadInMemoryThresholdBytes)
            {
                var prepared = await PrepareSmallRawUploadInMemoryAsync(filePath, uploadToken).ConfigureAwait(false);
                inMemoryPayload = prepared.PayloadBytes;
                rawSize = prepared.RawSize;
                payloadSize = prepared.PayloadSize;
                md5Base64 = prepared.Md5Base64;
            }
            else
            {
                rawSize = fileSize;
                payloadSize = fileSize;
                md5Base64 = await ComputeFileMd5Base64Async(filePath, uploadToken).ConfigureAwait(false);
            }

            if (rawSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length raw payload.");

            if (payloadSize <= 0)
                throw new InvalidDataException($"[{fileHash}] Refusing to upload zero-length payload.");

            if (rawSize != payloadSize)
                throw new InvalidDataException($"[{fileHash}] RawV2 payload size mismatch (raw={rawSize}, payload={payloadSize}).");

            if (inMemoryPayload != null && inMemoryPayload.LongLength != payloadSize)
                throw new InvalidDataException($"[{fileHash}] In-memory payload size mismatch (expected {payloadSize}, got {inMemoryPayload.LongLength}).");

            if (inMemoryPayload == null)
            {
                var fi = new FileInfo(filePath);
                if (!fi.Exists || fi.Length != payloadSize)
                    throw new InvalidDataException($"[{fileHash}] Source upload payload size mismatch (expected {payloadSize}, got {fi.Length}).");
            }

            progress?.Report(new UploadProgress(0, rawSize));

            const int maxAttempts = 4;
            Exception? last = null;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                uploadToken.ThrowIfCancellationRequested();

                try
                {
                    await using var fs0 = OpenRawUploadPayloadStream(inMemoryPayload, filePath, streamBufferSize);

                    var netProgress = progress == null
                        ? null
                        : new Progress<long>(sent =>
                        {
                            var mapped = sent;
                            if (mapped > rawSize) mapped = rawSize;
                            progress.Report(new UploadProgress(mapped, rawSize));
                        });

                    await using Stream fs = netProgress == null ? fs0 : new ProgressReadStream(fs0, netProgress);

                    using var put = new HttpRequestMessage(HttpMethod.Put, ticket.UploadUrl);

                    var content = new StreamContent(fs, streamBufferSize);
                    content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                    content.Headers.ContentLength = payloadSize;
                    try { content.Headers.ContentMD5 = Convert.FromBase64String(md5Base64); } catch { }

                    put.Content = content;
                    put.Headers.ExpectContinue = false;

                    using var putResp = await _directUploadClient.SendAsync(put, HttpCompletionOption.ResponseHeadersRead, uploadToken).ConfigureAwait(false);

                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[{hash}] Batch DirectB2 PUT attempt {attempt}/{max} Status: {status}",
                            fileHash, attempt, maxAttempts, putResp.StatusCode);
                    }

                    putResp.EnsureSuccessStatusCode();

                    // The B2 PUT is complete here. Batch metadata finalisation may still be in
                    // flight, but the upload bytes are done, so show the sender a true 100%
                    // instead of sitting at 99% during UploadCompletes.
                    progress?.Report(new UploadProgress(rawSize, rawSize));

                    return new UploadCompleteBatchFileDto
                    {
                        Hash = fileHash,
                        RawSize = rawSize,
                        CompressedSize = payloadSize,
                        ContentMd5Base64 = md5Base64,
                        ETag = putResp.Headers.ETag?.Tag,
                        WasDirect = true,
                        PayloadEncoding = FilePayloadEncoding.RawV2
                    };
                }
                catch (Exception ex) when (attempt < maxAttempts)
                {
                    last = ex;

                    if (Logger.IsEnabled(LogLevel.Debug))
                        Logger.LogDebug(ex, "[{hash}] Batch DirectB2 PUT failed (attempt {attempt}/{max}); retrying", fileHash, attempt, maxAttempts);

                    await Task.Delay(250 * attempt, uploadToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    last = ex;
                    break;
                }
            }

            throw new InvalidOperationException($"[{fileHash}] Batch DirectB2 upload failed after {maxAttempts} attempts.", last);
        }
        finally
        {
            // RawV2 uses either an in-memory byte array or the original cache file; no temp payload cleanup required.
        }
    }

    private async Task UploadCompleteSingleAsync(UploadCompleteBatchFileDto completeFile, CancellationToken uploadToken)
    {
        var completeUri = MareFiles.ServerFilesUploadCompleteFullPath(_orchestrator.UploadCdnUri!, completeFile.Hash);
        var complete = new UploadCompleteDto(completeFile.RawSize, completeFile.CompressedSize, completeFile.ContentMd5Base64, completeFile.ETag, completeFile.WasDirect, completeFile.PayloadEncoding);

        using var completeResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, completeUri, complete, uploadToken).ConfigureAwait(false);
        if (Logger.IsEnabled(LogLevel.Debug))
            Logger.LogDebug("[{hash}] UploadComplete Status: {status}", completeFile.Hash, completeResp.StatusCode);

        completeResp.EnsureSuccessStatusCode();
    }

    private async Task<Dictionary<string, UploadTicketBatchFileResponseDto>> GetUploadTicketsBatchAsync(List<string> hashes, IReadOnlyDictionary<string, long> sizeByHash, bool forceUploadTicket, CancellationToken ct)
    {
        var result = new Dictionary<string, UploadTicketBatchFileResponseDto>(StringComparer.Ordinal);

        if (!_orchestrator.IsInitialized || _orchestrator.UploadCdnUri == null || hashes.Count == 0)
            return result;

        const int batchSize = 256;
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        foreach (var chunk in hashes.Distinct(StringComparer.Ordinal).Chunk(batchSize))
        {
            ct.ThrowIfCancellationRequested();

            var req = new UploadTicketBatchRequestDto
            {
                Force = forceUploadTicket,
                Files = chunk
                    .Where(h => sizeByHash.TryGetValue(h, out var size) && size > 0)
                    .Select(h => new UploadTicketBatchFileRequestDto
                    {
                        Hash = h,
                        RawSize = sizeByHash[h],
                        ClientWouldMunge = false,
                        ClientParallelUploads = 0,
                        PayloadEncoding = FilePayloadEncoding.RawV2
                    })
                    .ToList()
            };

            if (req.Files.Count == 0)
                continue;

            try
            {
                using var resp = await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesUploadTicketsFullPath(_orchestrator.UploadCdnUri!), req, ct).ConfigureAwait(false);
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    Logger.LogDebug("Batch upload tickets endpoint not available; falling back to per-file tickets.");
                    return result;
                }

                if (!resp.IsSuccessStatusCode)
                {
                    Logger.LogWarning("Batch upload tickets failed with HTTP {status}; falling back to per-file tickets for this batch.", (int)resp.StatusCode);
                    return result;
                }

                var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                var dto = JsonSerializer.Deserialize<UploadTicketBatchResponseDto>(body, options);
                if (dto?.Files == null)
                    continue;

                foreach (var ticket in dto.Files)
                {
                    if (!string.IsNullOrWhiteSpace(ticket.Hash))
                        result[ticket.Hash.ToUpperInvariant()] = ticket;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Batch upload tickets failed; falling back to per-file tickets for this batch.");
                return result;
            }
        }

        return result;
    }

    private async Task<HashSet<string>> UploadCompletesBatchAsync(IReadOnlyCollection<UploadCompleteBatchFileDto> completeFiles, CancellationToken ct)
    {
        var completed = new HashSet<string>(StringComparer.Ordinal);

        if (!_orchestrator.IsInitialized || _orchestrator.UploadCdnUri == null || completeFiles.Count == 0)
            return completed;

        const int batchSize = 256;
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        foreach (var chunk in completeFiles.Chunk(batchSize))
        {
            ct.ThrowIfCancellationRequested();

            var request = new UploadCompleteBatchRequestDto { Files = chunk.ToList() };

            try
            {
                using var resp = await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesUploadCompletesFullPath(_orchestrator.UploadCdnUri!), request, ct).ConfigureAwait(false);
                if (resp.StatusCode == HttpStatusCode.NotFound || resp.StatusCode == HttpStatusCode.Conflict)
                {
                    Logger.LogDebug("Batch upload complete endpoint unavailable/disabled ({status}); falling back to per-file complete.", resp.StatusCode);
                    return completed;
                }

                if (!resp.IsSuccessStatusCode)
                {
                    Logger.LogWarning("Batch upload complete failed with HTTP {status}; falling back to per-file complete for remaining files.", (int)resp.StatusCode);
                    return completed;
                }

                var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                var dto = JsonSerializer.Deserialize<UploadCompleteBatchResponseDto>(body, options);
                if (dto?.Files == null)
                    continue;

                foreach (var file in dto.Files)
                {
                    if (file.Success && !string.IsNullOrWhiteSpace(file.Hash))
                        completed.Add(file.Hash.ToUpperInvariant());
                    else if (!string.IsNullOrWhiteSpace(file.Hash))
                        Logger.LogWarning("[{hash}] Batch upload complete failed: {status} {error}", file.Hash, file.StatusCode, file.Error);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Batch upload complete failed; falling back to per-file complete for remaining files.");
                return completed;
            }
        }

        return completed;
    }

    public async Task<(CharacterData Data, bool Success, string? Error)> UploadFiles(CharacterData data, List<UserData> visiblePlayers, IReadOnlyCollection<string>? preflightMissingHashes = null)
    {
        CancelUpload();

        var uploadCts = new CancellationTokenSource();
        lock (_uploadCancellationLock)
        {
            _uploadCancellationTokenSource = uploadCts;
        }

        var uploadToken = uploadCts.Token;

        try
        {
            var sanitizerResult = CharacterDataPushSanitizer.SanitizeForPush(data);
            if (sanitizerResult.Changed)
            {
                Logger.LogDebug("Removed server-invalid outbound data before upload/share: {replacements} replacement(s), {gamePaths} game path(s), {buckets} object bucket(s), {honorific} honorific payload(s)",
                    sanitizerResult.RemovedReplacements,
                    sanitizerResult.RemovedGamePaths,
                    sanitizerResult.RemovedBuckets,
                    sanitizerResult.RemovedHonorificData);
            }

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
                    "Pruning {count} hash(es) referenced by character data but missing locally before upload/share (first 20): {list}",
                    missingLocal.Count, string.Join(", ", missingLocal.Take(20)));

                RemoveHashesFromCharacterData(data, missingLocal);

                allCandidateHashes = GetAllCandidateHashes(data);
                if (allCandidateHashes.Count == 0)
                    return (data, true, null);

                presentLocal = allCandidateHashes
                    .Where(h => _fileDbManager.GetFileCacheByHash(h) != null)
                    .ToHashSet(StringComparer.Ordinal);
            }

            var uids = (visiblePlayers ?? new List<UserData>())
                .Select(p => p.UID)
                .Where(u => !string.IsNullOrWhiteSpace(u))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            SetLocalOutboundUploadRecipients(uids);

            HashSet<string> hashesForServerCheck;
            if (preflightMissingHashes != null)
            {
                var meshMissing = preflightMissingHashes
                    .Where(h => !string.IsNullOrWhiteSpace(h))
                    .Select(h => h.Trim().ToUpperInvariant())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                hashesForServerCheck = presentLocal
                    .Where(h => meshMissing.Contains(h))
                    .ToHashSet(StringComparer.Ordinal);
            }
            else
            {
                hashesForServerCheck = presentLocal;
            }

            if (hashesForServerCheck.Count == 0)
                return (data, true, null);

            Logger.LogDebug("Authorizing/share-check for {count} hashes to {uids} recipients", hashesForServerCheck.Count, uids.Count);

            var auth = await FilesSend([.. hashesForServerCheck], uids, uploadToken).ConfigureAwait(false);

            var now = DateTime.UtcNow;

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

            var verifiedNow = DateTime.UtcNow;
            foreach (var hash in hashesForServerCheck)
                _verifiedUploadedHashes[hash] = verifiedNow;

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
            lock (_uploadCancellationLock)
            {
                if (ReferenceEquals(_uploadCancellationTokenSource, uploadCts))
                    _uploadCancellationTokenSource = null;
            }

            try { uploadCts.Dispose(); } catch { }

            CompleteAndClearCurrentUploads();
            ClearLocalOutboundUploadRecipients();
        }
    }

    private static HashSet<string> GetAllCandidateHashes(CharacterData data)
    {
        HashSet<string> hashes = new(StringComparer.OrdinalIgnoreCase);

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

                hashes.Add(hash.Trim().ToUpperInvariant());
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

        foreach (var objectKind in data.FileReplacements.Keys.ToArray())
        {
            if (data.FileReplacements.TryGetValue(objectKind, out var replacements) && replacements.Count == 0)
                data.FileReplacements.Remove(objectKind);
        }
    }

    private static void RemoveHashesFromCharacterData(CharacterData data, IEnumerable<string> hashes)
    {
        var removeSet = hashes?
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Select(h => h.Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];

        if (removeSet.Count == 0)
            return;

        foreach (var kvp in data.FileReplacements)
            kvp.Value.RemoveAll(fr => !string.IsNullOrWhiteSpace(fr.Hash) && removeSet.Contains(fr.Hash));

        foreach (var objectKind in data.FileReplacements.Keys.ToArray())
        {
            if (data.FileReplacements.TryGetValue(objectKind, out var replacements) && replacements.Count == 0)
                data.FileReplacements.Remove(objectKind);
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

    private static async Task<(byte[] PayloadBytes, long RawSize, long PayloadSize, string Md5Base64)> PrepareSmallRawUploadInMemoryAsync(string filePath, CancellationToken ct)
    {
        await using var input = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        if (input.Length <= 0)
            throw new InvalidDataException($"Refusing to prepare zero-length upload payload from {filePath}");

        if (input.Length > int.MaxValue)
            throw new InvalidDataException($"In-memory raw upload payload is too large for {filePath}");

        var payloadBytes = new byte[(int)input.Length];
        var offset = 0;

        while (offset < payloadBytes.Length)
        {
            var read = await input.ReadAsync(payloadBytes.AsMemory(offset, payloadBytes.Length - offset), ct).ConfigureAwait(false);
            if (read <= 0)
                throw new EndOfStreamException($"Unexpected end of file while preparing raw upload payload from {filePath}");

            offset += read;
        }

        using var md5 = MD5.Create();
        var md5Base64 = Convert.ToBase64String(md5.ComputeHash(payloadBytes));

        return (payloadBytes, payloadBytes.LongLength, payloadBytes.LongLength, md5Base64);
    }

    private static async Task<string> ComputeFileMd5Base64Async(string filePath, CancellationToken ct)
    {
        await using var input = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(1024 * 1024);
        try
        {
            int read;
            while ((read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false)) > 0)
            {
                md5.AppendData(buffer, 0, read);
            }

            return Convert.ToBase64String(md5.GetHashAndReset());
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static Stream OpenRawUploadPayloadStream(byte[]? inMemoryPayload, string filePath, int streamBufferSize)
    {
        if (inMemoryPayload != null)
            return new MemoryStream(inMemoryPayload, writable: false);

        return new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
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
            UIDs = uids,
            DesiredPayloadEncoding = FilePayloadEncoding.RawV2,
            ForcePayloadEncoding = true
        };

        using var response = await _orchestrator.SendRequestAsync(HttpMethod.Post, MareFiles.ServerFilesFilesSendFullPath(_orchestrator.UploadCdnUri!), filesSendDto, ct).ConfigureAwait(false);
        var body = response.Content == null
            ? string.Empty
            : await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            var trimmedError = TrimHttpBodyForLog(body);
            throw new HttpRequestException(
                $"FilesSend failed with HTTP {(int)response.StatusCode} {response.ReasonPhrase}. Response: {trimmedError}",
                null,
                response.StatusCode);
        }

        if (string.IsNullOrWhiteSpace(body))
        {
            Logger.LogDebug("FilesSend returned HTTP {status} with an empty body; treating as no uploads required", (int)response.StatusCode);
            return [];
        }

        try
        {
            return JsonSerializer.Deserialize<List<UploadFileDto>>(body) ?? [];
        }
        catch (JsonException ex)
        {
            var trimmedBody = TrimHttpBodyForLog(body);
            throw new InvalidOperationException($"FilesSend returned invalid JSON from HTTP {(int)response.StatusCode}. Response: {trimmedBody}", ex);
        }
    }

    private static string TrimHttpBodyForLog(string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return "<empty>";

        var normalized = body.Replace("\r", " ").Replace("\n", " ").Trim();
        return normalized.Length <= 500 ? normalized : normalized[..500] + "...";
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
        CancellationTokenSource? ctsToCancel;
        lock (_uploadCancellationLock)
        {
            ctsToCancel = _uploadCancellationTokenSource;
            _uploadCancellationTokenSource = null;
        }

        MarkCurrentUploadsAsLocallyAbandonedReservations("connection reset while uploading");
        try { ctsToCancel?.Cancel(); } catch { /* best effort */ }

        CompleteAndClearCurrentUploads();
        ClearLocalOutboundUploadRecipients();
        _recentOutboundRecipientUids.Clear();
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
                var ticketReq = new UploadTicketRequestDto(fileSize, clientWouldMunge: false, clientParallelUploads: 0, payloadEncoding: FilePayloadEncoding.RawV2);

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
                    _verifiedUploadedHashes[hash] = now;
                    recovered.Add(hash);

                    Logger.LogDebug("[{hash}] Recheck recovered upload because the server reports UploadRequired=false", hash);
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
                var requireHashValidation = forceUploadTickets || IsRecentlyAbandonedLocalUploadReservation(hash);
                if (!cacheEntryByHash.TryGetValue(hash, out var cacheEntry) || !TryValidateUploadSource(hash, cacheEntry, requireHashValidation: requireHashValidation))
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

            var succeeded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            var reservedUploadHashes = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            var forceUploadTicketHashes = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            var pendingBatchCompletes = new System.Collections.Concurrent.ConcurrentBag<UploadCompleteBatchFileDto>();

            var batchTicketsByHash = await GetUploadTicketsBatchAsync(allowedHashes, sizeByHash, forceUploadTickets, token).ConfigureAwait(false);
            if (batchTicketsByHash.Count > 0)
            {
                var alreadyCovered = new List<string>();
                var alreadyReserved = new List<string>();

                foreach (var hash in allowedHashes)
                {
                    if (!batchTicketsByHash.TryGetValue(hash, out var ticket))
                        continue;

                    if (!ticket.Success)
                    {
                        if (IsUploadAlreadyReserved(ticket.Error))
                        {
                            if (IsRecentlyAbandonedLocalUploadReservation(hash))
                            {
                                // The last owner of this reservation was our own cancelled/disconnected upload.
                                // Waiting for "shared files" here deadlocks: nobody else is going to complete it.
                                // Leave the hash in allowedHashes and force-refresh its ticket on the actual upload attempt.
                                forceUploadTicketHashes[hash] = 1;
                                ClearUploadTransferWaitingStatus(hash, uploadEntryByHash);
                                Logger.LogWarning(
                                    "[{hash}] Batch upload ticket is still reserved after a locally abandoned upload; forcing a fresh upload ticket instead of waiting for ourselves",
                                    hash);
                                continue;
                            }

                            // The server already has an active reservation for this hash.
                            // Do not fall back to the per-file ticket endpoint, because that
                            // only creates a 409 retry storm against the same reservation.
                            // The owner of the reservation is allowed to finish naturally.
                            alreadyReserved.Add(hash);
                            reservedUploadHashes[hash] = 1;
                            MarkUploadTransferWaitingForSharedFiles(hash, uploadEntryByHash);

                            Logger.LogDebug("[{hash}] Batch upload ticket says this hash is already reserved; waiting for the shared upload to become available before releasing the barrier", hash);
                            continue;
                        }

                        Logger.LogWarning("[{hash}] Batch upload ticket error, falling back to per-file ticket: {error}", hash, ticket.Error);
                        continue;
                    }

                    if (!ticket.UploadRequired)
                    {
                        alreadyCovered.Add(hash);
                        succeeded[hash] = 1;
                        _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                        ClearLocallyAbandonedUploadReservation(hash);

                        if (uploadEntryByHash.TryGetValue(hash, out var entry))
                            entry.Transferred = entry.Total;
                    }
                }

                if (alreadyCovered.Count > 0 || alreadyReserved.Count > 0)
                {
                    var skipUpload = alreadyCovered
                        .Concat(alreadyReserved)
                        .Distinct(StringComparer.Ordinal)
                        .ToList();

                    allowedHashes = allowedHashes.Except(skipUpload, StringComparer.Ordinal).ToList();

                    if (alreadyCovered.Count > 0)
                        Logger.LogDebug("Batch upload tickets skipped {count} hash(es) already present on B2", alreadyCovered.Count);

                    if (alreadyReserved.Count > 0)
                        Logger.LogDebug("Batch upload tickets skipped {count} hash(es) already reserved by another active upload", alreadyReserved.Count);
                }
            }

            if (allowedHashes.Count == 0)
            {
                var arrivedReservedOnly = await WaitForReservedUploadHashesToArriveAsync(reservedUploadHashes.Keys.ToList(), sizeByHash, uploadEntryByHash, progress, token).ConfigureAwait(false);
                foreach (var hash in arrivedReservedOnly)
                {
                    succeeded[hash] = 1;
                    reservedUploadHashes.TryRemove(hash, out _);
                    ClearLocallyAbandonedUploadReservation(hash);
                }

                return forbidden
                    .Concat(reservedUploadHashes.Keys)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
            }

            const int MaxUploadAttempts = 3;

            // fixed if user explicitly configured, adaptive only for Auto (0)
            var configured = _mareConfigService.Current.ParallelUploads;
            var cpuAuto = GetCpuAutoParallelUploads();
            var isAuto = configured <= 0;

            var maxParallel = isAuto ? Math.Clamp(cpuAuto, 1, MaxConfiguredParallelUploads) : Math.Clamp(configured, 1, MaxConfiguredParallelUploads);

            var mostlySmallFiles = false;
            var tinyHeavyFiles = false;
            var requestBoundFiles = false;
            if (allowedHashes.Count >= 4 && sizeByHash.Count > 0)
            {
                var small = 0;
                var tiny = 0;
                var requestBound = 0;
                foreach (var s in sizeByHash.Values)
                {
                    if (s <= SmallUploadThresholdBytes) small++;
                    if (s <= TinyUploadThresholdBytes) tiny++;
                    if (s <= RequestBoundUploadThresholdBytes) requestBound++;
                }

                mostlySmallFiles = small >= (sizeByHash.Count * 3 / 4);
                tinyHeavyFiles = allowedHashes.Count >= 8 && tiny >= (sizeByHash.Count * 3 / 4);
                requestBoundFiles = allowedHashes.Count >= 8 && requestBound >= (sizeByHash.Count * 3 / 4);
            }

            if (isAuto && tinyHeavyFiles)
            {
                // Tiny-file storms are RTT/request-overhead bound, not bandwidth bound.
                // They need enough in-flight ticket/PUT/complete work to hide latency.
                maxParallel = Math.Clamp(Math.Max(cpuAuto * 8, 48), 24, MaxTinyFileAutoParallelUploads);
            }
            else if (isAuto && (mostlySmallFiles || requestBoundFiles))
            {
                // MTRL/ATEX/PAP support packs often contain hundreds of sub-512 KiB files.
                // Per-file request overhead dominates, so start wider instead of slowly
                // learning that the connection can handle more lanes.
                maxParallel = Math.Clamp(Math.Max(cpuAuto * 5, 32), 16, MaxSmallFileAutoParallelUploads);
            }

            var desiredParallel = isAuto
                ? (tinyHeavyFiles || requestBoundFiles
                    ? maxParallel
                    : (mostlySmallFiles ? Math.Min(maxParallel, Math.Max(24, cpuAuto * 3)) : Math.Min(maxParallel, Math.Max(3, cpuAuto))))
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

            bool fastStartUsed = tinyHeavyFiles || requestBoundFiles;

            double SampleBpsIfReady()
            {
                var now = sw.Elapsed;

                var minSampleInterval = (tinyHeavyFiles || requestBoundFiles)
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
                if (isAuto && (tinyHeavyFiles || requestBoundFiles))
                    return maxParallel;

                var slot = mostlySmallFiles
                    ? 192d * 1024d
                    : 256d * 1024d;

                if (bps <= 0) return 1;

                var bySpeed = (int)Math.Round(bps / slot, MidpointRounding.AwayFromZero);

                bySpeed = Math.Clamp(bySpeed, 1, maxParallel);

                if (isAuto && mostlySmallFiles)
                    return Math.Clamp(Math.Max(bySpeed, Math.Min(maxParallel, 4)), 1, maxParallel);

                return bySpeed;
            }

            var queue = new System.Collections.Concurrent.ConcurrentQueue<string>(orderedHashes);
        var running = new List<Task>(Math.Min(allowedHashes.Count, maxParallel));
        int idx = 0;
        var rollingUploadCompletesDisabled = false;

        async Task FlushPendingUploadCompletesAsync(bool final)
        {
            if (!final && rollingUploadCompletesDisabled)
                return;

            var threshold = final ? 1 : (tinyHeavyFiles || requestBoundFiles ? 64 : 256);
            if (pendingBatchCompletes.Count < threshold)
                return;

            var drained = new List<UploadCompleteBatchFileDto>();
            while (pendingBatchCompletes.TryTake(out var complete))
                drained.Add(complete);

            var pendingCompletes = drained
                .GroupBy(c => c.Hash, StringComparer.Ordinal)
                .Select(g => g.First())
                .ToList();

            if (pendingCompletes.Count == 0)
                return;

            Logger.LogDebug("Completing {count} direct B2 uploads via {mode} batch metadata call", pendingCompletes.Count, final ? "final" : "rolling");

            if (final)
            {
                foreach (var complete in pendingCompletes)
                    MarkUploadTransferFinalising(complete.Hash, uploadEntryByHash);

                progress?.Report(FinalisingUploadsStatus);
                try { Mediator.Publish(new RefreshUiMessage()); } catch { }
            }

            var batchCompleted = await UploadCompletesBatchAsync(pendingCompletes, token).ConfigureAwait(false);
            if (!final && batchCompleted.Count == 0)
                rollingUploadCompletesDisabled = true;

            foreach (var complete in pendingCompletes)
            {
                if (!batchCompleted.Contains(complete.Hash))
                {
                    if (!final)
                    {
                        pendingBatchCompletes.Add(complete);
                        continue;
                    }

                    try
                    {
                        await UploadCompleteSingleAsync(complete, token).ConfigureAwait(false);
                        batchCompleted.Add(complete.Hash);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(ex, "[{hash}] Per-file upload complete fallback failed", complete.Hash);
                    }
                }

                if (batchCompleted.Contains(complete.Hash))
                {
                    _verifiedUploadedHashes[complete.Hash] = DateTime.UtcNow;
                    succeeded[complete.Hash] = 1;
                    ClearLocallyAbandonedUploadReservation(complete.Hash);
                    ClearUploadTransferFinalisingStatus(complete.Hash, uploadEntryByHash);
                    if (uploadEntryByHash.TryGetValue(complete.Hash, out var finalEntry))
                        finalEntry.Transferred = finalEntry.Total;
                }
            }
        }

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

                        // Tiny-file storms report low early EWMA because each file only contributes bytes
                        // after ticket + PUT + complete round-trips finish. Do not downshift them based
                        // purely on the first few low bandwidth samples, or auto mode collapses back into
                        // serialized request overhead.

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

            if (tinyHeavyFiles || requestBoundFiles)
                await FlushPendingUploadCompletesAsync(final: false).ConfigureAwait(false);
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
                    // Hundreds of sub-128 KiB files do not benefit from per-read progress callbacks.
                    // The callback/timer/log churn can cost more than the upload itself, so tiny files
                    // complete as whole-file units while larger files still get live progress.
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

                    var buf = origSize <= SmallUploadThresholdBytes
                        ? 64 * 1024
                        : ChooseUploadStreamBufferSize(ewmaBps);

                    UploadCompleteBatchFileDto? pendingComplete = null;

                    if (batchTicketsByHash.TryGetValue(hash, out var batchTicket)
                        && batchTicket.Success
                        && batchTicket.UploadRequired
                        && string.Equals(batchTicket.Mode, "DirectB2", StringComparison.OrdinalIgnoreCase)
                        && !string.IsNullOrWhiteSpace(batchTicket.UploadUrl))
                    {
                        pendingComplete = await UploadFileStreamedFromDiskWithTicket(hash, filePath, batchTicket, throttledProg, tokenInner, buf).ConfigureAwait(false);
                        pendingBatchCompletes.Add(pendingComplete);
                    }
                    else
                    {
                        var useForceUploadTicket = forceUploadTickets || forceUploadTicketHashes.ContainsKey(hash);
                        await UploadFileStreamedFromDisk(hash, filePath, _mareConfigService.Current.UseAlternativeFileUpload, throttledProg, tokenInner, buf, useForceUploadTicket,
                            onBytesUploaded: () =>
                            {
                                MarkUploadTransferFinalising(hash, uploadEntryByHash);
                                try { Mediator.Publish(new RefreshUiMessage()); } catch { }
                            }).ConfigureAwait(false);
                    }

                    if (!useLiveProgress)
                        Interlocked.Add(ref bytesSinceSample, origSize);

                    if (pendingComplete == null)
                    {
                        _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                        succeeded[hash] = 1;
                        ClearLocallyAbandonedUploadReservation(hash);
                        forceUploadTicketHashes.TryRemove(hash, out _);
                        ClearUploadTransferFinalisingStatus(hash, uploadEntryByHash);

                        if (uploadEntryByHash.TryGetValue(hash, out var finalEntry))
                            finalEntry.Transferred = finalEntry.Total;
                    }
                    else if (uploadEntryByHash.TryGetValue(hash, out var pendingEntry))
                    {
                        // The file bytes have reached B2; only metadata finalisation remains.
                        // Keep the UI honest by showing completed bytes rather than a fake
                        // 99% stall while the batch UploadCompletes call is finishing.
                        pendingEntry.Transferred = pendingEntry.Total;
                    }

                    return;
                }
                catch (OperationCanceledException)
                {
                    Logger.LogDebug("[{hash}] Upload cancelled on attempt {attempt}/{max}", hash, attempt, MaxUploadAttempts);
                    throw;
                }
                catch (Exception ex) when (IsUploadReservationConflict(ex))
                {
                    if (!forceUploadTickets && !forceUploadTicketHashes.ContainsKey(hash) && IsRecentlyAbandonedLocalUploadReservation(hash))
                    {
                        forceUploadTicketHashes[hash] = 1;
                        ClearUploadTransferWaitingStatus(hash, uploadEntryByHash);
                        Logger.LogWarning(
                            "[{hash}] Upload ticket conflicted with a locally abandoned reservation; retrying with a forced fresh ticket",
                            hash);
                        continue;
                    }

                    // Another active upload already owns this hash reservation. Re-requesting
                    // the ticket just hammers the server with 409s, but a reservation is not a
                    // completed upload. Keep this hash failed for the barrier unless the later
                    // server recheck proves it became present.
                    Logger.LogDebug("[{hash}] Upload skipped because the server already has an active reservation for this hash; waiting for the shared upload to become available before releasing the barrier", hash);
                    reservedUploadHashes[hash] = 1;
                    MarkUploadTransferWaitingForSharedFiles(hash, uploadEntryByHash);

                    return;
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

            await FlushPendingUploadCompletesAsync(final: true).ConfigureAwait(false);

            var arrivedReserved = await WaitForReservedUploadHashesToArriveAsync(reservedUploadHashes.Keys.ToList(), sizeByHash, uploadEntryByHash, progress, token).ConfigureAwait(false);
            foreach (var hash in arrivedReserved)
            {
                succeeded[hash] = 1;
                reservedUploadHashes.TryRemove(hash, out _);
                ClearLocallyAbandonedUploadReservation(hash);
            }

            // ---- Result classification ----
            var failed = allowedHashes
                .Where(h => !succeeded.ContainsKey(h))
                .Concat(reservedUploadHashes.Keys)
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

    private void MarkUploadTransferFinalising(string hash, System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer> uploadEntryByHash)
    {
        if (uploadEntryByHash.TryGetValue(hash, out var entry) && !string.Equals(entry.StatusText, WaitingForSharedFilesStatus, StringComparison.Ordinal))
            entry.StatusText = FinalisingUploadsStatus;
    }

    private void ClearUploadTransferFinalisingStatus(string hash, System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer> uploadEntryByHash)
    {
        if (uploadEntryByHash.TryGetValue(hash, out var entry) && string.Equals(entry.StatusText, FinalisingUploadsStatus, StringComparison.Ordinal))
            entry.StatusText = null;
    }

    private void MarkUploadTransferWaitingForSharedFiles(string hash, System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer> uploadEntryByHash)
    {
        if (uploadEntryByHash.TryGetValue(hash, out var entry))
            entry.StatusText = WaitingForSharedFilesStatus;
    }

    private void ClearUploadTransferWaitingStatus(string hash, System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer> uploadEntryByHash)
    {
        if (uploadEntryByHash.TryGetValue(hash, out var entry) && string.Equals(entry.StatusText, WaitingForSharedFilesStatus, StringComparison.Ordinal))
            entry.StatusText = null;
    }

    private async Task<HashSet<string>> WaitForReservedUploadHashesToArriveAsync(IReadOnlyCollection<string> reservedHashes, IReadOnlyDictionary<string, long> sizeByHash, System.Collections.Concurrent.ConcurrentDictionary<string, UploadFileTransfer> uploadEntryByHash, IProgress<string>? progress, CancellationToken token)
    {
        var pending = reservedHashes?
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Distinct(StringComparer.Ordinal)
            .ToHashSet(StringComparer.Ordinal) ?? [];

        var arrived = new HashSet<string>(StringComparer.Ordinal);
        if (pending.Count == 0)
            return arrived;

        foreach (var hash in pending)
            MarkUploadTransferWaitingForSharedFiles(hash, uploadEntryByHash);

        progress?.Report(WaitingForSharedFilesStatus);
        Logger.LogInformation("Waiting for {count} shared/reserved upload hash(es) to arrive before releasing upload barrier", pending.Count);

        try
        {
            Mediator.Publish(new RefreshUiMessage());
        }
        catch
        {
            // best effort
        }

        var deadlineTick = unchecked(Environment.TickCount64 + ReservedUploadWaitTimeoutMs);
        var lastProgressTick = 0L;

        while (pending.Count > 0)
        {
            token.ThrowIfCancellationRequested();

            var ticketHashes = pending
                .Where(h => sizeByHash.TryGetValue(h, out var size) && size > 0)
                .ToList();

            if (ticketHashes.Count == 0)
                break;

            var tickets = await GetUploadTicketsBatchAsync(ticketHashes, sizeByHash, forceUploadTicket: false, token).ConfigureAwait(false);
            var becameAvailable = new List<string>();
            var noLongerReservedButMissing = new List<string>();

            foreach (var hash in ticketHashes)
            {
                if (!tickets.TryGetValue(hash, out var ticket))
                    continue;

                if (ticket.Success && !ticket.UploadRequired)
                {
                    becameAvailable.Add(hash);
                    continue;
                }

                if (ticket.Success && ticket.UploadRequired && !string.IsNullOrWhiteSpace(ticket.UploadUrl))
                {
                    noLongerReservedButMissing.Add(hash);
                    continue;
                }

                if (!ticket.Success && !IsUploadAlreadyReserved(ticket.Error))
                    Logger.LogDebug("[{hash}] Reserved upload wait still cannot verify file presence; ticket error={error}", hash, ticket.Error);
            }

            foreach (var hash in becameAvailable)
            {
                pending.Remove(hash);
                arrived.Add(hash);
                _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                ClearLocallyAbandonedUploadReservation(hash);
                ClearUploadTransferWaitingStatus(hash, uploadEntryByHash);
                if (uploadEntryByHash.TryGetValue(hash, out var entry))
                    entry.Transferred = entry.Total;
            }

            if (becameAvailable.Count > 0)
            {
                Logger.LogInformation("Shared upload wait recovered {count} hash(es); {remaining} still waiting", becameAvailable.Count, pending.Count);
                try { Mediator.Publish(new RefreshUiMessage()); } catch { }
            }

            foreach (var hash in noLongerReservedButMissing)
            {
                Logger.LogWarning("[{hash}] Shared upload reservation cleared but server still reports the object missing; leaving it failed for this barrier", hash);
                pending.Remove(hash);
                ClearUploadTransferWaitingStatus(hash, uploadEntryByHash);
            }

            if (pending.Count == 0)
                break;

            var nowTick = Environment.TickCount64;
            if (unchecked(deadlineTick - nowTick) <= 0)
            {
                Logger.LogWarning("Timed out waiting for {count} shared/reserved upload hash(es) to arrive", pending.Count);
                break;
            }

            if (lastProgressTick == 0 || unchecked(nowTick - lastProgressTick) >= 2500)
            {
                lastProgressTick = nowTick;
                progress?.Report(WaitingForSharedFilesStatus);
                try { Mediator.Publish(new RefreshUiMessage()); } catch { }
            }

            await Task.Delay(ReservedUploadWaitPollMs, token).ConfigureAwait(false);
        }

        return arrived;
    }

    private static bool IsUploadAlreadyReserved(string? message)
    {
        return !string.IsNullOrWhiteSpace(message)
               && message.Contains("Upload already reserved", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsUploadReservationConflict(Exception ex)
    {
        if (ex is HttpRequestException { StatusCode: HttpStatusCode.Conflict })
            return true;

        var current = ex;
        while (current != null)
        {
            if (IsUploadAlreadyReserved(current.Message))
                return true;

            current = current.InnerException;
        }

        return false;
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
            cpu <= 4 ? 3 :
            cpu <= 8 ? 4 :
            cpu <= 16 ? 6 :
            cpu <= 24 ? 8 :
                        12;

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
            MaxConnectionsPerServer = 512,
            EnableMultipleHttp2Connections = true,
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

            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        };

        return client;
    }
}
