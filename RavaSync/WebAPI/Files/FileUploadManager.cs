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

    private async Task UploadFileStreamedFromDisk(string fileHash, string filePath, bool munged, IProgress<UploadProgress>? progress, CancellationToken uploadToken)
    {
        if (!_orchestrator.IsInitialized) throw new InvalidOperationException("FileTransferManager is not initialized");
        uploadToken.ThrowIfCancellationRequested();

        Logger.LogDebug("[{hash}] Preparing file for upload", fileHash);

        var fileSize = new FileInfo(filePath).Length;

        try
        {
            var ticketUri = MareFiles.ServerFilesUploadTicketFullPath(_orchestrator.UploadCdnUri!, fileHash);
            var ticketReq = new UploadTicketRequestDto(fileSize, munged, 0);

            var ticketResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, ticketUri, ticketReq, uploadToken).ConfigureAwait(false);
            if (ticketResp.StatusCode != HttpStatusCode.NotFound && ticketResp.IsSuccessStatusCode)
            {
                var ticketJson = await ticketResp.Content.ReadAsStringAsync(uploadToken).ConfigureAwait(false);
                var ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(ticketJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (ticket != null && ticket.Mode == "DirectB2")
                {
                    if (!ticket.UploadRequired)
                    {
                        return;
                        //REMOVED DUE TO Added time to hit uploading. Server checks B2 instead now. D'oh
                        //if (await CdnHasFileAsync(fileHash, uploadToken).ConfigureAwait(false))
                        //    return;

                        //Logger.LogWarning("[{hash}] Ticket says upload not required but CDN is missing it; forcing DirectB2 re-upload.", fileHash);

                        //var forcedTicketUri = new UriBuilder(ticketUri) { Query = "force=1" }.Uri;
                        //var forcedResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, forcedTicketUri, ticketReq, uploadToken).ConfigureAwait(false);
                        //forcedResp.EnsureSuccessStatusCode();

                        //var forcedJson = await forcedResp.Content.ReadAsStringAsync(uploadToken).ConfigureAwait(false);
                        //ticket = JsonSerializer.Deserialize<UploadTicketResponseDto>(forcedJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                        //if (ticket == null || ticket.Mode != "DirectB2" || !ticket.UploadRequired)
                        //    throw new InvalidOperationException($"[{fileHash}] Forced ticket refused but CDN is missing the file.");
                    }


                    Logger.LogDebug("[{hash}] Direct upload ticket acquired, compressing to temp (LZ4)", fileHash);

                    var rawProgress = progress == null ? null : new Progress<long>(b => progress.Report(new UploadProgress(fileSize, b)));
                    await using var temp = await TempLz4UploadFile.CreateAsync(filePath, uploadToken, rawProgress).ConfigureAwait(false);

                    progress?.Report(new UploadProgress(fileSize, fileSize));

                    await using var fs = new FileStream(temp.TempPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                        bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

                    using var put = new HttpRequestMessage(HttpMethod.Put, ticket.UploadUrl);
                    var content = new StreamContent(fs, 1024 * 1024);
                    content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                    content.Headers.ContentLength = temp.CompressedSize;
                    try { content.Headers.ContentMD5 = Convert.FromBase64String(temp.Md5Base64); } catch { }
                    put.Content = content;
                    put.Headers.ExpectContinue = false;

                    var putResp = await _directUploadClient.SendAsync(put, HttpCompletionOption.ResponseHeadersRead, uploadToken).ConfigureAwait(false);
                    Logger.LogDebug("[{hash}] Direct upload PUT Status: {status}", fileHash, putResp.StatusCode);
                    putResp.EnsureSuccessStatusCode();

                    var etag = putResp.Headers.ETag?.Tag;

                    var completeUri = MareFiles.ServerFilesUploadCompleteFullPath(_orchestrator.UploadCdnUri!, fileHash);
                    var complete = new UploadCompleteDto(temp.RawSize, temp.CompressedSize, temp.Md5Base64, etag, true);

                    var completeResp = await _orchestrator.SendRequestAsync(HttpMethod.Post, completeUri, complete, uploadToken).ConfigureAwait(false);
                    Logger.LogDebug("[{hash}] UploadComplete Status: {status}", fileHash, completeResp.StatusCode);
                    completeResp.EnsureSuccessStatusCode();

                    if (_orchestrator.FilesCdnUri != null)
                    {
                        await CdnPrewarmHelper.PrewarmAsync(
                            _orchestrator.FilesCdnUri,
                            new[] { fileHash },
                            uploadToken).ConfigureAwait(false);
                    }

                    return;
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "[{hash}] Direct upload path failed/unsupported, falling back to legacy upload", fileHash);
        }

        Logger.LogDebug("[{hash}] Legacy upload (streaming via file server)", fileHash);

        using var legacyContent = new Lz4FileContent(
            path: filePath,
            fileSize: fileSize,
            progress: progress,
            xorKey: munged ? (byte)0x2A : null
        );

        HttpResponseMessage response;
        if (!munged)
        {
            response = await _orchestrator.SendRequestAsync(
                HttpMethod.Post,
                MareFiles.ServerFilesUploadFullPath(_orchestrator.UploadCdnUri!, fileHash),
                legacyContent,
                uploadToken).ConfigureAwait(false);
        }
        else
        {
            response = await _orchestrator.SendRequestAsync(
                HttpMethod.Post,
                MareFiles.ServerFilesUploadMunged(_orchestrator.UploadCdnUri!, fileHash),
                legacyContent,
                uploadToken).ConfigureAwait(false);
        }

        Logger.LogDebug("[{hash}] Upload Status: {status}", fileHash, response.StatusCode);
        response.EnsureSuccessStatusCode();

        progress?.Report(new UploadProgress(fileSize, fileSize));

        if (_orchestrator.FilesCdnUri != null)
        {
            await CdnPrewarmHelper.PrewarmAsync(
                _orchestrator.FilesCdnUri,
                new[] { fileHash },
                uploadToken).ConfigureAwait(false);
        }
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
        foreach (var h in allowedHashes)
        {
            try
            {
                if (!CurrentUploads.Any(u => string.Equals(u.Hash, h, StringComparison.Ordinal)))
                {
                    var cacheEntry = _fileDbManager.GetFileCacheByHash(h);
                    if (cacheEntry == null) continue;

                    CurrentUploads.Add(new UploadFileTransfer(new UploadFileDto { Hash = h })
                    {
                        Total = new FileInfo(cacheEntry.ResolvedFilepath).Length,
                    });
                }
            }
            catch
            {
                // best effort
            }
        }

        const int MaxUploadAttempts = 3;

        var maxParallelUploads = Math.Clamp(_mareConfigService.Current.ParallelUploads, 1, 10);
        using var sem = new System.Threading.SemaphoreSlim(maxParallelUploads, maxParallelUploads);

        var tasks = new List<Task>(allowedHashes.Count);
        var succeeded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        int idx = 0;

        foreach (var hash in allowedHashes)
        {
            var localIdx = Interlocked.Increment(ref idx);
            tasks.Add(UploadOneAsync(hash, localIdx, token));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        async Task UploadOneAsync(string hash, int localIdx, CancellationToken token)
        {
            await sem.WaitAsync(token).ConfigureAwait(false);
            try
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

                        var prog = new Progress<UploadProgress>(up =>
                        {
                            try
                            {
                                var pct = up.Size > 0 ? (int)(100L * up.Uploaded / up.Size) : 0;
                                if (pct % 10 == 0)
                                    Logger.LogTrace("[{idx}] {file}: {pct}% (attempt {attempt}/{max})",
                                        localIdx, Path.GetFileName(filePath), pct, attempt, MaxUploadAttempts);
                            }
                            catch
                            {
                                // ignore
                            }
                        });

                        await UploadFileStreamedFromDisk(
                            fileHash: hash,
                            filePath: filePath,
                            munged: _mareConfigService.Current.UseAlternativeFileUpload,
                            progress: prog,
                            uploadToken: token
                        ).ConfigureAwait(false);

                        _verifiedUploadedHashes[hash] = DateTime.UtcNow;
                        succeeded[hash] = 1;

                        var entry = CurrentUploads.FirstOrDefault(
                            u => string.Equals(u.Hash, hash, StringComparison.Ordinal));
                        if (entry != null) entry.Transferred = entry.Total;

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
                            await Task.Delay(200, token).ConfigureAwait(false);
                        }
                    }
                }
            }
            finally
            {
                sem.Release();
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
    //private async Task<bool> CdnHasFileAsync(string hash, CancellationToken ct)
    //{
    //    if (_orchestrator.FilesCdnUri == null) return false;

    //    var uri = new Uri(_orchestrator.FilesCdnUri, $"cdn/{hash}");

    //    using var req = new HttpRequestMessage(HttpMethod.Head, uri);
    //    req.Headers.CacheControl = new CacheControlHeaderValue { NoCache = true };
    //    req.Headers.Pragma.ParseAdd("no-cache");

    //    using var resp = await _directUploadClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
    //    return resp.IsSuccessStatusCode;
    //}

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
            AutomaticDecompression = DecompressionMethods.None
        };

        var client = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromMinutes(30)
        };

        return client;
    }

}
