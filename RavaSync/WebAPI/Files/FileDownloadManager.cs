using K4os.Compression.LZ4.Legacy;
using Dalamud.Utility;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Dto.Files;
using RavaSync.API.Routes;
using RavaSync.FileCache;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using RavaSync.WebAPI.Files.Models;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.WebAPI.Files;

public partial class FileDownloadManager : DisposableMediatorSubscriberBase
{
    private static readonly bool IsWineRuntime = SafeIsWine();
    private static readonly int BufferSize = IsWineRuntime ? 512 * 1024 : 1024 * 1024;
    private const int Lz4BlockSize = 256 * 1024;
    // Global CDN file slots across all active pair downloads. This is async request/file concurrency, not dedicated threads.
    // Network work remains broad on Wine/Linux; expensive legacy decode and cache finalisation are
    // pressure-gated separately so smoothness does not come from starving the connection.
    private static readonly int MaxCdnParallelism = IsWineRuntime ? 128 : 256;
    private static readonly int DownloadStatusPublishIntervalMs = IsWineRuntime ? 300 : 150;
    private static readonly SemaphoreSlim GlobalCdnFileSemaphore = new(MaxCdnParallelism, MaxCdnParallelism);
    private static readonly ConcurrentDictionary<string, Lazy<Task<bool>>> GlobalInFlightDownloadsByHash = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan SmallFirstResponseTimeout = TimeSpan.FromSeconds(6);
    private static readonly TimeSpan NormalFirstResponseTimeout = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan LargeFirstResponseTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SmallReadProgressTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan NormalReadProgressTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan LargeReadProgressTimeout = TimeSpan.FromSeconds(25);
    private const long NormalReadProgressTimeoutThreshold = 2L * 1024 * 1024;
    private const long LargeReadProgressTimeoutThreshold = 32L * 1024 * 1024;
    private static readonly TimeSpan DirectCdnSizeHintTtl = TimeSpan.FromMinutes(10);

    private readonly FileCompactor _fileCompactor;
    private readonly FileCacheManager _fileDbManager;
    private readonly FileTransferOrchestrator _orchestrator;
    private readonly MareConfigService _mareConfigService;
    private readonly List<ThrottledStream> _activeDownloadStreams = [];
    private readonly object _activeStreamsLock = new();
    private Dictionary<string, FileDownloadStatus> _downloadStatus = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _fileProgressBytes = new(StringComparer.OrdinalIgnoreCase);
    private long _lastStatusPublishMs;

    private string _centralFileRepairTargetUid = string.Empty;
    private string _centralFileRepairTargetIdent = string.Empty;
    private string _centralFileRepairDataHash = string.Empty;
    private readonly ConcurrentDictionary<string, int> _centralFileRepairAttemptsByHash = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _centralFileRepairFailedHashes = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, (long Size, long ExpiresUtcTicks)> DirectCdnSizeHintsByHash = new(StringComparer.OrdinalIgnoreCase);
    private static int _cdnClientCursor;
    private static readonly HttpClient[] SharedCdnClients = CreateCdnClients();
    private static readonly int LinuxLegacyDecodeConcurrency = Math.Clamp(Environment.ProcessorCount / 8, 1, 2);
    private static readonly SemaphoreSlim LinuxLegacyDecodeSemaphore = new(LinuxLegacyDecodeConcurrency, LinuxLegacyDecodeConcurrency);
    private static readonly ConcurrentQueue<PendingCacheRegistration> LinuxPendingCacheRegistrations = new();
    private static int _linuxCacheRegistrationPumpRunning;

    private sealed class PendingCacheRegistration
    {
        public PendingCacheRegistration(FileCacheManager manager, string hash, string path)
        {
            Manager = manager;
            Hash = hash;
            Path = path;
        }

        public FileCacheManager Manager { get; }
        public string Hash { get; }
        public string Path { get; }
        public TaskCompletionSource<bool> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static bool SafeIsWine()
    {
        try { return Util.IsWine(); }
        catch { return false; }
    }

    public FileDownloadManager(ILogger<FileDownloadManager> logger, MareMediator mediator, FileTransferOrchestrator orchestrator, FileCacheManager fileCacheManager, FileCompactor fileCompactor, MareConfigService mareConfigService) : base(logger, mediator)
    {
        _orchestrator = orchestrator;
        _fileDbManager = fileCacheManager;
        _fileCompactor = fileCompactor;
        _mareConfigService = mareConfigService;

        Mediator.Subscribe<DownloadLimitChangedMessage>(this, _ =>
        {
            var limit = _orchestrator.DownloadLimitPerSlot();
            var newLimit = limit <= 0 ? ThrottledStream.Infinite : limit;

            lock (_activeStreamsLock)
            {
                foreach (var stream in _activeDownloadStreams.ToArray())
                    stream.BandwidthLimit = newLimit;
            }
        });
    }

    public List<DownloadFileTransfer> CurrentDownloads { get; private set; } = [];
    public List<FileTransfer> ForbiddenTransfers => _orchestrator.ForbiddenTransfers;
    public bool IsDownloading => CurrentDownloads.Any();

    public static void RegisterDirectCdnSizeHints(IReadOnlyDictionary<string, long>? sizeHints)
    {
        if (sizeHints == null || sizeHints.Count == 0)
            return;

        var expires = DateTime.UtcNow.Add(DirectCdnSizeHintTtl).Ticks;
        foreach (var item in sizeHints)
        {
            if (string.IsNullOrWhiteSpace(item.Key) || item.Value <= 0)
                continue;

            DirectCdnSizeHintsByHash[item.Key.Trim().ToUpperInvariant()] = (item.Value, expires);
        }
    }

    public static bool HasSessionDownloadFailedHash(string? hash) => false;
    public static int MarkSessionDownloadFailedHashes(IEnumerable<string?> hashes, string reason) => 0;

    public static void MungeBuffer(Span<byte> buffer)
    {
        for (var i = 0; i < buffer.Length; i++)
            buffer[i] ^= 42;
    }

    public void ConfigureFileRepairContext(string targetUid, string targetIdent, string dataHash)
    {
        _centralFileRepairTargetUid = targetUid ?? string.Empty;
        _centralFileRepairTargetIdent = targetIdent ?? string.Empty;
        _centralFileRepairDataHash = dataHash ?? string.Empty;
        _centralFileRepairFailedHashes.Clear();
    }

    public bool HasCentralFileRepairFailedHash(string? hash)
        => !string.IsNullOrWhiteSpace(hash) && _centralFileRepairFailedHashes.ContainsKey(hash);

    public void ClearDownload()
    {
        CurrentDownloads.Clear();
        _downloadStatus = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);
        _fileProgressBytes.Clear();
        _centralFileRepairAttemptsByHash.Clear();
        lock (_activeStreamsLock)
            _activeDownloadStreams.Clear();
    }

    public async Task<List<DownloadFileTransfer>> InitiateDownloadList(GameObjectHandler gameObjectHandler, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
        Logger.LogDebug("Download start: {id}", gameObjectHandler.Name);

        var requestedHashes = fileReplacement
            .Select(static f => f.Hash)
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (requestedHashes.Count == 0)
        {
            CurrentDownloads = [];
            return CurrentDownloads;
        }

        var dtos = await FilesGetSizes(requestedHashes, ct).ConfigureAwait(false);
        var returnedHashes = dtos
            .Select(static d => d.Hash)
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h.Trim().ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var extensionByHash = BuildExtensionByHash(fileReplacement);
        var transfers = new List<DownloadFileTransfer>();
        var missingOrForbidden = requestedHashes
            .Where(h => !returnedHashes.Contains(h))
            .ToList();

        foreach (var dto in dtos.GroupBy(static d => d.Hash, StringComparer.OrdinalIgnoreCase).Select(static g => g.First()))
        {
            ct.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(dto.Hash))
                continue;


            if (dto.IsForbidden)
            {
                if (!_orchestrator.ForbiddenTransfers.Exists(f => string.Equals(f.Hash, dto.Hash, StringComparison.OrdinalIgnoreCase)))
                    _orchestrator.ForbiddenTransfers.Add(new DownloadFileTransfer(dto));

                missingOrForbidden.Add(dto.Hash);
                continue;
            }

            if (IsHashAlreadyCached(dto.Hash, extensionByHash))
                continue;

            var transfer = new DownloadFileTransfer(dto);
            if (!transfer.CanBeTransferred)
            {
                missingOrForbidden.Add(dto.Hash);
                continue;
            }

            transfers.Add(transfer);
        }

        if (missingOrForbidden.Count > 0)
            TryPublishCentralFileRepairRequestBatch(missingOrForbidden, "file service reported missing/forbidden/untransferable file(s)");

        CurrentDownloads = transfers;
        return CurrentDownloads;
    }

    public async Task DownloadFiles(GameObjectHandler gameObject, List<FileReplacementData> fileReplacementDto, CancellationToken ct)
    {
        Mediator.Publish(new HaltScanMessage(nameof(DownloadFiles)));
        try
        {
            await DownloadFilesInternal(gameObject, fileReplacementDto, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            ClearDownload();
            throw;
        }
        catch
        {
            ClearDownload();
            throw;
        }
        finally
        {
            Mediator.Publish(new DownloadFinishedMessage(gameObject));
            Mediator.Publish(new ResumeScanMessage(nameof(DownloadFiles)));
        }
    }

    private async Task DownloadFilesInternal(GameObjectHandler gameObjectHandler, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
        if (CurrentDownloads.Count == 0)
            return;

        var extensionByHash = BuildExtensionByHash(fileReplacement);
        var mainSet = CurrentDownloads.ToList();

        var failed = await DownloadBatchAsync(gameObjectHandler, mainSet, extensionByHash, cacheBust: false, ct).ConfigureAwait(false);

        var unresolved = failed.Where(t => !IsHashAlreadyCached(t.Hash, extensionByHash)).DistinctBy(t => t.Hash, StringComparer.OrdinalIgnoreCase).ToList();
        if (unresolved.Count > 0)
        {
            var unresolvedBytes = unresolved.Sum(static t => Math.Max(1, t.Total));
            var unresolvedDetails = DescribeTransfersForLog(unresolved, extensionByHash);

            TryPublishCentralFileRepairRequestBatch(unresolved.Select(t => t.Hash), "download main pass left unresolved file(s)");
            Logger.LogWarning("Download main pass finished with {count} unresolved file(s), approx {bytes} bytes ({mib:0.00} MiB). Completing the visible bar and failing this pass so the normal outer retry only requests the missing file(s). Details: {details}", unresolved.Count, unresolvedBytes, unresolvedBytes / 1024d / 1024d, unresolvedDetails);

            CompleteAllDownloadStatus(gameObjectHandler);
            ClearDownload();
            throw new InvalidOperationException($"Download pass finished with {unresolved.Count} unresolved file(s): {unresolvedDetails}");
        }

        CompleteAllDownloadStatus(gameObjectHandler);
        Logger.LogDebug("Download end: {id}", gameObjectHandler.Name);
        ClearDownload();
    }

    private async Task<bool> TryDownloadFilesFromCdnAsync(GameObjectHandler gameObjectHandler, IGrouping<string, DownloadFileTransfer> fileGroup, List<FileReplacementData> fileReplacement, CancellationToken ct)
    {
        var transfers = fileGroup.ToList();
        if (transfers.Count == 0)
            return true;

        var extensionByHash = BuildExtensionByHash(fileReplacement);
        var failed = await DownloadBatchAsync(gameObjectHandler, transfers, extensionByHash, cacheBust: false, ct).ConfigureAwait(false);
        return failed.Count == 0;
    }

    private async Task<List<DownloadFileTransfer>> DownloadBatchAsync(GameObjectHandler gameObjectHandler, IReadOnlyCollection<DownloadFileTransfer> transfers, IReadOnlyDictionary<string, string> extensionByHash, bool cacheBust, CancellationToken ct)
    {
        var failed = new ConcurrentBag<DownloadFileTransfer>();
        var externalWaits = new List<(DownloadFileTransfer Transfer, Lazy<Task<bool>> SharedDownload)>();
        var actualNetworkTransfers = new List<DownloadFileTransfer>();

        foreach (var transfer in transfers
            .Where(static t => !string.IsNullOrWhiteSpace(t.Hash))
            .GroupBy(static t => t.Hash.Trim().ToUpperInvariant(), StringComparer.OrdinalIgnoreCase)
            .Select(static g => g.First()))
        {
            ct.ThrowIfCancellationRequested();

            var hash = transfer.Hash.Trim().ToUpperInvariant();
            if (IsHashAlreadyCached(hash, extensionByHash))
                continue;

            if (GlobalInFlightDownloadsByHash.TryGetValue(hash, out var sharedDownload))
            {
                // This pair still needs the file, but this client is not actually grabbing another
                // copy. Keep it out of this pair's visible payload so duplicate pair/retry bars do
                // not show the original full room size while they are only waiting for global de-dupe.
                externalWaits.Add((transfer, sharedDownload));
                continue;
            }

            actualNetworkTransfers.Add(transfer);
        }

        ResetDownloadStatus(gameObjectHandler, actualNetworkTransfers, DownloadStatus.Downloading);

        var options = new ParallelOptions { MaxDegreeOfParallelism = MaxCdnParallelism, CancellationToken = ct };

        await Parallel.ForEachAsync(actualNetworkTransfers, options, async (transfer, token) =>
        {
            var hash = transfer.Hash?.Trim().ToUpperInvariant();
            if (string.IsNullOrWhiteSpace(hash))
                return;

            if (IsHashAlreadyCached(hash, extensionByHash))
            {
                AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: false);
                return;
            }

            Lazy<Task<bool>>? newDownload = null;
            newDownload = new Lazy<Task<bool>>(
                () => DownloadOneFileThroughGlobalDedupeSlotAsync(gameObjectHandler, transfer, extensionByHash, cacheBust, token, hash, newDownload!),
                LazyThreadSafetyMode.ExecutionAndPublication);

            var activeDownload = GlobalInFlightDownloadsByHash.GetOrAdd(hash, newDownload);
            var ownsDownload = ReferenceEquals(activeDownload, newDownload);

            if (!ownsDownload)
            {
                // Race-safe path: another pair/batch started this hash after our preflight but before
                // our GetOrAdd. Remove it from this visible payload instead of pretending this pair is
                // downloading those bytes too.
                RemovePendingTransferFromDownloadStatus(gameObjectHandler, transfer, forcePublish: true);
            }

            var ok = false;
            try
            {
                ok = await activeDownload.Value.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                ok = false;
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Shared CDN download failed for {hash}", hash);
                ok = false;
            }

            if (ok || IsHashAlreadyCached(hash, extensionByHash))
                return;

            if (ownsDownload)
                AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: true);

            failed.Add(transfer);
        }).ConfigureAwait(false);

        if (externalWaits.Count > 0)
        {
            var waitOptions = new ParallelOptions { MaxDegreeOfParallelism = Math.Min(MaxCdnParallelism, Math.Max(1, externalWaits.Count)), CancellationToken = ct };
            await Parallel.ForEachAsync(externalWaits, waitOptions, async (wait, token) =>
            {
                var hash = wait.Transfer.Hash?.Trim().ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(hash))
                    return;

                var ok = false;
                try
                {
                    ok = await wait.SharedDownload.Value.WaitAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    ok = false;
                }
                catch (Exception ex)
                {
                    Logger.LogDebug(ex, "Global de-dupe wait failed for {hash}", hash);
                    ok = false;
                }

                if (ok || IsHashAlreadyCached(hash, extensionByHash))
                    return;

                failed.Add(wait.Transfer);
            }).ConfigureAwait(false);
        }

        PublishDownloadStatus(gameObjectHandler, force: true);
        return failed.ToList();
    }

    private async Task<bool> DownloadOneFileThroughGlobalDedupeSlotAsync(GameObjectHandler gameObjectHandler, DownloadFileTransfer transfer, IReadOnlyDictionary<string, string> extensionByHash, bool cacheBust, CancellationToken ct, string hash, Lazy<Task<bool>> owner)
    {
        try
        {
            if (IsHashAlreadyCached(hash, extensionByHash))
            {
                AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: false);
                return true;
            }

            await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
            await GlobalCdnFileSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Another active pair may have completed the same hash while this file was waiting for
                // the shared CDN slot. Re-check here so shared slots are spent on real network work only.
                if (IsHashAlreadyCached(hash, extensionByHash))
                {
                    AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: false);
                    return true;
                }

                return await DownloadOneFileAsync(gameObjectHandler, transfer, extensionByHash, cacheBust, ct).ConfigureAwait(false);
            }
            finally
            {
                GlobalCdnFileSemaphore.Release();
            }
        }
        finally
        {
            if (GlobalInFlightDownloadsByHash.TryGetValue(hash, out var currentOwner) && ReferenceEquals(currentOwner, owner))
                GlobalInFlightDownloadsByHash.TryRemove(hash, out _);
        }
    }

    private async Task<bool> DownloadOneFileAsync(GameObjectHandler gameObjectHandler, DownloadFileTransfer transfer, IReadOnlyDictionary<string, string> extensionByHash, bool cacheBust, CancellationToken ct)
    {
        var hash = transfer.Hash.Trim().ToUpperInvariant();
        var ext = extensionByHash.TryGetValue(hash, out var mappedExt) ? mappedExt : "dat";
        var finalPath = _fileDbManager.GetCacheFilePath(hash, ext);
        var tempPath = CreateTempPath(finalPath);
        var url = BuildDownloadUrl(transfer, cacheBust);

        try
        {
            EnsureDirectory(finalPath);

            using var response = await GetCdnResponseAsync(url, transfer, ct).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                TryPublishCentralFileRepairRequest(hash, cacheBust ? "cache-busted CDN 404" : "CDN 404");
                return false;
            }

            if (!response.IsSuccessStatusCode)
            {
                Logger.LogDebug("CDN download failed for {hash}: HTTP {status} from {url}", hash, (int)response.StatusCode, url);
                return false;
            }

            await using var responseStream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            var readProgressTimeout = GetReadProgressTimeout(transfer);
            using var guarded = new StalledReadGuardStream(responseStream, readProgressTimeout, ct);
            using var throttled = RegisterActiveStream(guarded);

            try
            {
                var firstByte = new byte[1];
                var firstRead = await throttled.ReadAsync(firstByte.AsMemory(0, 1), ct).ConfigureAwait(false);
                if (firstRead <= 0)
                    throw new InvalidDataException($"CDN response stream was empty for {hash}");

                if (!transfer.IsRawPayload)
                {
                    var spoolPath = CreateSpoolPath(hash);
                    try
                    {
                        await using var fullResponse = new PrefixedReadStream(throttled, firstByte, firstRead);
                        await CopyStreamToFileAsync(fullResponse, spoolPath, read => AddProgress(gameObjectHandler, transfer, read, fileComplete: false, forcePublish: false), ct).ConfigureAwait(false);
                        SetGroupPhase(gameObjectHandler, GetGroupKey(transfer), DownloadStatus.Decompressing);

                        const long legacyExpectedRawSize = 0;
                        var decodedLegacy = await DecodeLegacyPayloadWithPressureGateAsync(spoolPath, tempPath, legacyExpectedRawSize, hash, ct).ConfigureAwait(false);
                        if (!string.Equals(decodedLegacy.ComputedHash, hash, StringComparison.OrdinalIgnoreCase))
                        {
                            Logger.LogWarning("Downloaded legacy hash mismatch for {hash}; computed {computed}, bytes={bytes}, expectedBytes={expectedBytes}, ext={ext}, mode={mode}", hash, decodedLegacy.ComputedHash, decodedLegacy.BytesWritten, Math.Max(1, transfer.Total), ext, GetPayloadMode(transfer));
                            TryDelete(tempPath);
                            return false;
                        }

                        await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
                        File.Move(tempPath, finalPath, overwrite: true);
                        await RegisterDownloadedCacheFileAsync(_fileDbManager, hash, finalPath).ConfigureAwait(false);
                        AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: false);
                        return true;
                    }
                    finally
                    {
                        TryDelete(spoolPath);
                    }
                }

                var detected = await DetectCdnHeaderAsync(throttled, firstByte, firstRead, ct).ConfigureAwait(false);
                await using var payloadBase = detected.PayloadStream;

                if (detected.HasHeader)
                {
                    if (!string.Equals(detected.HeaderHash, hash, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidDataException($"CDN header hash mismatch for {hash}, got {detected.HeaderHash}");

                    if (detected.PayloadLength <= 0)
                        throw new InvalidDataException($"CDN header had invalid payload length {detected.PayloadLength} for {hash}");
                }

                var expectedRawSize = detected.PayloadLength > 0 ? detected.PayloadLength : transfer.TotalRaw > 0 ? transfer.TotalRaw : 0;
                await using var payloadStream = detected.HasHeader
                    ? new LimitedXorStream(payloadBase, detected.PayloadLength, detected.Munged ? (byte)42 : (byte)0)
                    : payloadBase;

                (string ComputedHash, long BytesWritten) decoded;

                decoded = await CopyRawToFileAndHashAsync(payloadStream, tempPath, expectedRawSize, read => AddProgress(gameObjectHandler, transfer, read, fileComplete: false, forcePublish: false), ct).ConfigureAwait(false);

                if (!string.Equals(decoded.ComputedHash, hash, StringComparison.OrdinalIgnoreCase))
                {
                    // Direct CDN plans can occasionally point at a legacy/headered LZ4 object while the client only has
                    // a hash-based RawV2 plan. Treat the raw bytes we just wrote as a legacy spool before failing.
                    var fallbackSpoolPath = CreateSpoolPath(hash);
                    try
                    {
                        File.Move(tempPath, fallbackSpoolPath, overwrite: true);
                        SetGroupPhase(gameObjectHandler, GetGroupKey(transfer), DownloadStatus.Decompressing);
                        const long legacyExpectedRawSize = 0;
                        decoded = await DecodeLegacyPayloadWithPressureGateAsync(fallbackSpoolPath, tempPath, legacyExpectedRawSize, hash, ct).ConfigureAwait(false);
                    }
                    catch
                    {
                        Logger.LogWarning("Downloaded hash mismatch for {hash}; computed {computed}, bytes={bytes}, expectedBytes={expectedBytes}, ext={ext}, mode={mode}", hash, decoded.ComputedHash, decoded.BytesWritten, Math.Max(1, transfer.Total), ext, GetPayloadMode(transfer));
                        TryDelete(tempPath);
                        return false;
                    }
                    finally
                    {
                        TryDelete(fallbackSpoolPath);
                    }
                }

                if (!string.Equals(decoded.ComputedHash, hash, StringComparison.OrdinalIgnoreCase))
                {
                    Logger.LogWarning("Downloaded hash mismatch for {hash}; computed {computed}, bytes={bytes}, expectedBytes={expectedBytes}, ext={ext}, mode={mode}", hash, decoded.ComputedHash, decoded.BytesWritten, Math.Max(1, transfer.Total), ext, GetPayloadMode(transfer));
                    TryDelete(tempPath);
                    return false;
                }

                await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
                File.Move(tempPath, finalPath, overwrite: true);
                await RegisterDownloadedCacheFileAsync(_fileDbManager, hash, finalPath).ConfigureAwait(false);
                AddProgress(gameObjectHandler, transfer, 0, fileComplete: true, forcePublish: false);
                return true;
            }
            finally
            {
                UnregisterActiveStream(throttled);
            }

        }
        catch (OperationCanceledException ex) when (!ct.IsCancellationRequested)
        {
            Logger.LogDebug(ex, "Download timed out or stalled for {hash}; expectedBytes={expectedBytes}, ext={ext}, mode={mode}. Leaving it unresolved for the normal outer missing-file retry. Url={url}", hash, Math.Max(1, transfer.Total), ext, GetPayloadMode(transfer), url);
            TryDelete(tempPath);
            return false;
        }
        catch (OperationCanceledException)
        {
            TryDelete(tempPath);
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Download failed for {hash} from {url}", hash, url);
            TryDelete(tempPath);
            return false;
        }
    }

    private async Task<HttpResponseMessage> GetCdnResponseAsync(string url, DownloadFileTransfer transfer, CancellationToken ct)
    {
        var client = GetNextCdnClient();
        using var firstResponseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        firstResponseCts.CancelAfter(GetFirstResponseTimeout(transfer));
        return await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, firstResponseCts.Token).ConfigureAwait(false);
    }

    private ThrottledStream RegisterActiveStream(Stream source)
    {
        var limit = _orchestrator.DownloadLimitPerSlot();
        var stream = new ThrottledStream(source, limit <= 0 ? ThrottledStream.Infinite : limit);

        lock (_activeStreamsLock)
            _activeDownloadStreams.Add(stream);

        return stream;
    }

    private void UnregisterActiveStream(ThrottledStream stream)
    {
        lock (_activeStreamsLock)
            _activeDownloadStreams.Remove(stream);
    }

    private static Task RegisterDownloadedCacheFileAsync(FileCacheManager manager, string hash, string path)
    {
        if (!IsWineRuntime)
        {
            manager.RegisterDownloadedCacheFiles([(hash, path)]);
            return Task.CompletedTask;
        }

        var pending = new PendingCacheRegistration(manager, hash, path);
        LinuxPendingCacheRegistrations.Enqueue(pending);
        EnsureLinuxCacheRegistrationPump();
        return pending.Completion.Task;
    }

    private static void EnsureLinuxCacheRegistrationPump()
    {
        if (Interlocked.CompareExchange(ref _linuxCacheRegistrationPumpRunning, 1, 0) == 0)
            _ = Task.Run(ProcessLinuxCacheRegistrationQueueAsync);
    }

    private static async Task ProcessLinuxCacheRegistrationQueueAsync()
    {
        while (true)
        {
            await Task.Delay(10).ConfigureAwait(false);

            var batch = new List<PendingCacheRegistration>(256);
            while (batch.Count < 1024 && LinuxPendingCacheRegistrations.TryDequeue(out var pending))
                batch.Add(pending);

            foreach (var group in batch.GroupBy(static item => item.Manager))
            {
                try
                {
                    await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(CancellationToken.None).ConfigureAwait(false);
                    group.Key.RegisterDownloadedCacheFiles(group.Select(static item => (item.Hash, item.Path)));
                    foreach (var item in group)
                        item.Completion.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    foreach (var item in group)
                        item.Completion.TrySetException(ex);
                }
            }

            if (!LinuxPendingCacheRegistrations.IsEmpty)
                continue;

            Interlocked.Exchange(ref _linuxCacheRegistrationPumpRunning, 0);
            if (LinuxPendingCacheRegistrations.IsEmpty || Interlocked.CompareExchange(ref _linuxCacheRegistrationPumpRunning, 1, 0) != 0)
                return;
        }
    }

    private static Task<(string ComputedHash, long BytesWritten)> DecodeLegacyPayloadWithPressureGateAsync(string spoolPath, string tempPath, long expectedRawSize, string expectedHash, CancellationToken ct)
    {
        if (!IsWineRuntime)
            return DecodeLegacyPayloadWithCompatAsync(spoolPath, tempPath, expectedRawSize, expectedHash, ct);

        return DecodeLegacyPayloadWithPressureGateCoreAsync(spoolPath, tempPath, expectedRawSize, expectedHash, ct);
    }

    private static async Task<(string ComputedHash, long BytesWritten)> DecodeLegacyPayloadWithPressureGateCoreAsync(string spoolPath, string tempPath, long expectedRawSize, string expectedHash, CancellationToken ct)
    {
        await LinuxLegacyDecodeSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await DecodeLegacyPayloadWithCompatAsync(spoolPath, tempPath, expectedRawSize, expectedHash, ct).ConfigureAwait(false);
        }
        finally
        {
            LinuxLegacyDecodeSemaphore.Release();
        }
    }

    private static HttpClient[] CreateCdnClients()
    {
        var clients = new HttpClient[IsWineRuntime ? 4 : 8];
        for (var i = 0; i < clients.Length; i++)
        {
            var handler = new SocketsHttpHandler
            {
                AutomaticDecompression = DecompressionMethods.None,
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromSeconds(IsWineRuntime ? 15 : 10),
                MaxConnectionsPerServer = IsWineRuntime ? 512 : 2048,
                EnableMultipleHttp2Connections = true,
            };

            clients[i] = new HttpClient(handler, disposeHandler: true)
            {
                Timeout = Timeout.InfiniteTimeSpan,
                DefaultRequestVersion = HttpVersion.Version20,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
            };
        }

        return clients;
    }

    private static HttpClient GetNextCdnClient()
    {
        var index = (int)((uint)Interlocked.Increment(ref _cdnClientCursor) % (uint)SharedCdnClients.Length);
        return SharedCdnClients[index];
    }

    private async Task<List<DownloadFileDto>> FilesGetSizes(List<string> hashes, CancellationToken ct)
    {
        if (!_orchestrator.IsInitialized)
            throw new InvalidOperationException("FileTransferManager is not initialized");

        if (_orchestrator.FilesCdnUri != null)
            return await BuildDirectCdnDownloadPlanAsync(hashes, ct).ConfigureAwait(false);

        var response = await _orchestrator.SendRequestAsync(HttpMethod.Get, MareFiles.ServerFilesGetSizesFullPath(_orchestrator.UploadCdnUri!), hashes, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<DownloadFileDto>>(cancellationToken: ct).ConfigureAwait(false) ?? [];
    }

    private Task<List<DownloadFileDto>> BuildDirectCdnDownloadPlanAsync(List<string> hashes, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        if (_orchestrator.FilesCdnUri == null)
            return Task.FromResult(new List<DownloadFileDto>());

        var result = hashes
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(hash => new DownloadFileDto
            {
                FileExists = true,
                ForbiddenBy = string.Empty,
                IsForbidden = false,
                Hash = hash,
                Size = GetDirectCdnSizeHint(hash),
                Url = MareFiles.CdnGetFullPath(_orchestrator.FilesCdnUri!, hash).ToString(),
                RawSize = 0,
                PayloadEncoding = FilePayloadEncoding.RawV2
            })
            .ToList();

        return Task.FromResult(result);
    }

    private static long GetDirectCdnSizeHint(string hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return 1;

        var normalized = hash.Trim().ToUpperInvariant();
        if (!DirectCdnSizeHintsByHash.TryGetValue(normalized, out var hint))
            return 1;

        if (hint.ExpiresUtcTicks <= DateTime.UtcNow.Ticks)
        {
            DirectCdnSizeHintsByHash.TryRemove(normalized, out _);
            return 1;
        }

        return hint.Size > 0 ? hint.Size : 1;
    }

    private bool IsHashAlreadyCached(string hash, IReadOnlyDictionary<string, string> extensionByHash)
    {
        if (string.IsNullOrWhiteSpace(hash))
            return true;

        var normalized = hash.Trim().ToUpperInvariant();
        var entry = _fileDbManager.GetFileCacheByHash(normalized);
        if (entry != null && !string.IsNullOrWhiteSpace(entry.ResolvedFilepath) && File.Exists(entry.ResolvedFilepath))
        {
            try
            {
                if (new FileInfo(entry.ResolvedFilepath).Length > 0)
                    return true;
            }
            catch
            {
                return true;
            }
        }

        var ext = extensionByHash.TryGetValue(normalized, out var mappedExt) ? mappedExt : "dat";
        var expectedPath = _fileDbManager.GetCacheFilePath(normalized, ext);
        if (!File.Exists(expectedPath))
            return false;

        try
        {
            if (new FileInfo(expectedPath).Length <= 0)
                return false;

            var computed = ComputeSha1(expectedPath, CancellationToken.None);
            if (!string.Equals(computed, normalized, StringComparison.OrdinalIgnoreCase))
                return false;

            _fileDbManager.RegisterDownloadedCacheFiles([(normalized, expectedPath)]);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private Dictionary<string, string> BuildExtensionByHash(IEnumerable<FileReplacementData> replacements)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var replacement in replacements ?? [])
        {
            if (string.IsNullOrWhiteSpace(replacement.Hash))
                continue;

            var ext = "dat";
            var gamePath = replacement.GamePaths?.FirstOrDefault(static p => !string.IsNullOrWhiteSpace(p));
            if (!string.IsNullOrWhiteSpace(gamePath))
            {
                var candidate = Path.GetExtension(gamePath).TrimStart('.');
                if (!string.IsNullOrWhiteSpace(candidate))
                    ext = candidate;
            }

            result[replacement.Hash.Trim().ToUpperInvariant()] = ext;
        }

        return result;
    }

    private static TimeSpan GetFirstResponseTimeout(DownloadFileTransfer transfer)
    {
        var expectedBytes = Math.Max(1, transfer.Total);
        if (IsWineRuntime)
        {
            if (expectedBytes >= LargeReadProgressTimeoutThreshold)
                return TimeSpan.FromSeconds(18);

            if (expectedBytes >= NormalReadProgressTimeoutThreshold)
                return TimeSpan.FromSeconds(12);

            return TimeSpan.FromSeconds(10);
        }

        if (expectedBytes >= LargeReadProgressTimeoutThreshold)
            return LargeFirstResponseTimeout;

        if (expectedBytes >= NormalReadProgressTimeoutThreshold)
            return NormalFirstResponseTimeout;

        return SmallFirstResponseTimeout;
    }

    private static TimeSpan GetReadProgressTimeout(DownloadFileTransfer transfer)
    {
        var expectedBytes = Math.Max(1, transfer.Total);
        if (IsWineRuntime)
        {
            if (expectedBytes >= LargeReadProgressTimeoutThreshold)
                return TimeSpan.FromSeconds(35);

            if (expectedBytes >= NormalReadProgressTimeoutThreshold)
                return TimeSpan.FromSeconds(18);

            return TimeSpan.FromSeconds(8);
        }

        if (expectedBytes >= LargeReadProgressTimeoutThreshold)
            return LargeReadProgressTimeout;

        if (expectedBytes >= NormalReadProgressTimeoutThreshold)
            return NormalReadProgressTimeout;

        return SmallReadProgressTimeout;
    }

    private static string GetPayloadMode(DownloadFileTransfer transfer)
        => transfer.IsRawPayload ? "RawV2" : "LegacyLZ4";

    private static string DescribeTransfersForLog(IEnumerable<DownloadFileTransfer> transfers, IReadOnlyDictionary<string, string> extensionByHash)
    {
        return string.Join("; ", transfers
            .OrderByDescending(static t => Math.Max(1, t.Total))
            .Take(30)
            .Select(t => DescribeTransferForLog(t, extensionByHash)));
    }

    private static string DescribeTransferForLog(DownloadFileTransfer transfer, IReadOnlyDictionary<string, string> extensionByHash)
    {
        var hash = transfer.Hash?.Trim().ToUpperInvariant() ?? string.Empty;
        var ext = extensionByHash.TryGetValue(hash, out var mappedExt) ? mappedExt : "dat";
        var bytes = Math.Max(1, transfer.Total);
        return $"{hash}.{ext} {bytes} bytes ({bytes / 1024d / 1024d:0.00} MiB) {GetPayloadMode(transfer)}";
    }

    private string BuildDownloadUrl(DownloadFileTransfer transfer, bool cacheBust)
    {
        string url;
        try
        {
            url = transfer.DownloadUri.ToString();
        }
        catch
        {
            url = MareFiles.CdnGetFullPath(_orchestrator.FilesCdnUri!, transfer.Hash.ToUpperInvariant()).ToString();
        }

        if (!cacheBust)
            return url;

        var sep = url.Contains('?', StringComparison.Ordinal) ? "&" : "?";
        return url + sep + "rava_retry=" + Guid.NewGuid().ToString("N");
    }

    private void ResetDownloadStatus(GameObjectHandler gameObjectHandler, IReadOnlyCollection<DownloadFileTransfer> transfers, DownloadStatus phase)
    {
        var activeTransfers = transfers.ToList();
        CurrentDownloads = activeTransfers;
        _fileProgressBytes.Clear();

        var grouped = activeTransfers.GroupBy(GetGroupKey, StringComparer.Ordinal).ToList();
        var next = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);

        foreach (var group in grouped)
        {
            next[group.Key] = new FileDownloadStatus
            {
                DownloadStatus = phase,
                TotalBytes = group.Sum(static t => Math.Max(1, t.Total)),
                TotalFiles = group.Count(),
                TransferredBytes = 0,
                TransferredFiles = 0,
            };
        }

        _downloadStatus = next;
        PublishDownloadStatus(gameObjectHandler, force: true);
    }

    private void SetGroupPhase(GameObjectHandler gameObjectHandler, string groupKey, DownloadStatus phase)
    {
        if (_downloadStatus.TryGetValue(groupKey, out var status))
        {
            lock (status)
                status.DownloadStatus = phase;
        }

        PublishDownloadStatus(gameObjectHandler, force: false);
    }

    private void AddProgress(GameObjectHandler gameObjectHandler, DownloadFileTransfer transfer, long bytes, bool fileComplete, bool forcePublish)
    {
        var groupKey = GetGroupKey(transfer);
        if (!_downloadStatus.TryGetValue(groupKey, out var status))
            return;

        var hash = transfer.Hash?.Trim().ToUpperInvariant() ?? string.Empty;
        var expectedBytes = Math.Max(1, transfer.Total);

        lock (status)
        {
            if (bytes > 0)
            {
                _fileProgressBytes.AddOrUpdate(hash, bytes, (_, existing) => existing + bytes);
                status.TransferredBytes += bytes;
                if (status.TransferredBytes > status.TotalBytes)
                    status.TotalBytes = status.TransferredBytes;
            }

            if (fileComplete)
            {
                var alreadyCounted = _fileProgressBytes.TryGetValue(hash, out var counted) ? counted : 0;
                var remainingVisualBytes = Math.Max(0, expectedBytes - alreadyCounted);
                if (remainingVisualBytes > 0)
                {
                    _fileProgressBytes[hash] = expectedBytes;
                    status.TransferredBytes += remainingVisualBytes;
                    if (status.TransferredBytes > status.TotalBytes)
                        status.TotalBytes = status.TransferredBytes;
                }

                status.TransferredFiles = Math.Min(status.TotalFiles, status.TransferredFiles + 1);
                if (status.TransferredFiles >= status.TotalFiles && status.TotalBytes > 0)
                    status.TransferredBytes = status.TotalBytes;
            }
        }

        PublishDownloadStatus(gameObjectHandler, forcePublish);
    }


    private void RemovePendingTransferFromDownloadStatus(GameObjectHandler gameObjectHandler, DownloadFileTransfer transfer, bool forcePublish)
    {
        var groupKey = GetGroupKey(transfer);
        if (!_downloadStatus.TryGetValue(groupKey, out var status))
            return;

        var hash = transfer.Hash?.Trim().ToUpperInvariant() ?? string.Empty;
        var expectedBytes = Math.Max(1, transfer.Total);

        lock (status)
        {
            var alreadyCounted = _fileProgressBytes.TryGetValue(hash, out var counted) ? counted : 0;
            var remainingVisualBytes = Math.Max(0, expectedBytes - alreadyCounted);

            status.TotalBytes = Math.Max(status.TransferredBytes, status.TotalBytes - remainingVisualBytes);
            status.TotalFiles = Math.Max(status.TransferredFiles, status.TotalFiles - 1);

            if (status.TotalFiles == 0)
                status.TransferredBytes = status.TotalBytes = 0;
        }

        PublishDownloadStatus(gameObjectHandler, forcePublish);
    }

    private void CompleteAllDownloadStatus(GameObjectHandler gameObjectHandler)
    {
        foreach (var status in _downloadStatus.Values)
        {
            lock (status)
            {
                status.DownloadStatus = DownloadStatus.Decompressing;
                status.TransferredBytes = Math.Max(status.TransferredBytes, status.TotalBytes);
                status.TransferredFiles = Math.Max(status.TransferredFiles, status.TotalFiles);
            }
        }

        PublishDownloadStatus(gameObjectHandler, force: true);
    }

    private void PublishDownloadStatus(GameObjectHandler gameObjectHandler, bool force = false)
    {
        var now = Environment.TickCount64;
        if (!force)
        {
            var last = Interlocked.Read(ref _lastStatusPublishMs);
            if (now - last < DownloadStatusPublishIntervalMs)
                return;

            if (Interlocked.CompareExchange(ref _lastStatusPublishMs, now, last) != last)
                return;
        }
        else
        {
            Interlocked.Exchange(ref _lastStatusPublishMs, now);
        }

        var snapshot = new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);
        foreach (var kv in _downloadStatus)
        {
            var current = kv.Value;
            lock (current)
            {
                snapshot[kv.Key] = new FileDownloadStatus
                {
                    DownloadStatus = current.DownloadStatus,
                    TotalBytes = current.TotalBytes,
                    TotalFiles = current.TotalFiles,
                    TransferredBytes = current.TransferredBytes,
                    TransferredFiles = current.TransferredFiles,
                };
            }
        }

        Mediator.Publish(new DownloadStartedMessage(gameObjectHandler, snapshot));
    }

    private static string GetGroupKey(DownloadFileTransfer transfer)
    {
        try
        {
            var uri = transfer.DownloadUri;
            return uri.Host + ":" + uri.Port;
        }
        catch
        {
            return "cdn";
        }
    }

    private static async Task CopyStreamToFileAsync(Stream input, string path, Action<long> progress, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            await using var output = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            while (true)
            {
                await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
                var read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0)
                    break;

                await output.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                progress(read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task<(string ComputedHash, long BytesWritten)> CopyRawToFileAndHashAsync(Stream input, string path, long expectedRawSize, Action<long> progress, CancellationToken ct)
    {
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        long total = 0;

        try
        {
            await using var output = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            while (true)
            {
                await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
                var read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0)
                    break;

                sha1.AppendData(buffer, 0, read);
                await output.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                total += read;
                progress(read);

                if (expectedRawSize > 0 && total > expectedRawSize)
                    throw new InvalidDataException($"Raw payload exceeded expected size for download; expected {expectedRawSize}, got {total}");
            }

            if (expectedRawSize > 0 && total != expectedRawSize)
                throw new InvalidDataException($"Raw payload did not match expected size for download; expected {expectedRawSize}, got {total}");

            return (Convert.ToHexString(sha1.GetHashAndReset()), total);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task<(string ComputedHash, long BytesWritten)> DecodeLegacyPayloadWithCompatAsync(string spoolPath, string tempPath, long expectedRawSize, string expectedHash, CancellationToken ct)
    {
        var attempts = new List<Func<Task<(string ComputedHash, long BytesWritten)>>>
        {
            () => TryDecodeHeaderedAsync(spoolPath, tempPath, expectedRawSize, useRaw: false, ct),
            () => TryDecodeHeaderedAsync(spoolPath, tempPath, expectedRawSize, useRaw: true, ct),
            () => DecodeHeaderlessAsync(spoolPath, tempPath, expectedRawSize, xor: false, useRaw: false, ct),
            () => DecodeHeaderlessAsync(spoolPath, tempPath, expectedRawSize, xor: true, useRaw: false, ct),
            () => DecodeHeaderlessAsync(spoolPath, tempPath, expectedRawSize, xor: false, useRaw: true, ct),
            () => DecodeHeaderlessAsync(spoolPath, tempPath, expectedRawSize, xor: true, useRaw: true, ct),
        };

        Exception? firstError = null;
        foreach (var attempt in attempts)
        {
            ct.ThrowIfCancellationRequested();
            TryDelete(tempPath);

            try
            {
                var decoded = await attempt().ConfigureAwait(false);
                if (decoded.BytesWritten > 0 && string.Equals(decoded.ComputedHash, expectedHash, StringComparison.OrdinalIgnoreCase))
                    return decoded;

                firstError ??= new InvalidDataException($"Legacy decode produced hash {decoded.ComputedHash}, expected {expectedHash}");
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                firstError ??= ex;
            }
        }

        throw firstError ?? new InvalidDataException($"Could not decode legacy payload for {expectedHash}");
    }

    private static async Task<(string ComputedHash, long BytesWritten)> TryDecodeHeaderedAsync(string spoolPath, string tempPath, long expectedRawSize, bool useRaw, CancellationToken ct)
    {
        await using var input = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
        var header = await ReadCdnHeaderAsync(input, ct).ConfigureAwait(false);
        Stream payload = header.Munged ? new XorReadStream(input, 42) : input;
        try
        {
            var decodeExpectedSize = useRaw
                ? header.Length > 0 ? header.Length : expectedRawSize
                : expectedRawSize;

            return useRaw
                ? await CopyRawToFileAndHashAsync(payload, tempPath, decodeExpectedSize, _ => { }, ct).ConfigureAwait(false)
                : await DecompressLz4ToFileAndHashAsync(payload, tempPath, decodeExpectedSize, ct).ConfigureAwait(false);
        }
        finally
        {
            if (!ReferenceEquals(payload, input))
                payload.Dispose();
        }
    }

    private static async Task<(string ComputedHash, long BytesWritten)> DecodeHeaderlessAsync(string spoolPath, string tempPath, long expectedRawSize, bool xor, bool useRaw, CancellationToken ct)
    {
        await using var input = new FileStream(spoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
        Stream payload = xor ? new XorReadStream(input, 42) : input;
        try
        {
            return useRaw
                ? await CopyRawToFileAndHashAsync(payload, tempPath, expectedRawSize, _ => { }, ct).ConfigureAwait(false)
                : await DecompressLz4ToFileAndHashAsync(payload, tempPath, expectedRawSize, ct).ConfigureAwait(false);
        }
        finally
        {
            if (!ReferenceEquals(payload, input))
                payload.Dispose();
        }
    }

    private static async Task<(string ComputedHash, long BytesWritten)> DecompressLz4ToFileAndHashAsync(Stream input, string path, long expectedRawSize, CancellationToken ct)
    {
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        long total = 0;

        try
        {
            await using var output = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var fullRead = new FullReadStream(input);
            await using var lz4 = new LZ4Stream(fullRead, LZ4StreamMode.Decompress, LZ4StreamFlags.None, Lz4BlockSize);

            while (true)
            {
                await LinuxSmoothMode.YieldBackgroundWorkIfNeededAsync(ct).ConfigureAwait(false);
                var read = await lz4.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read <= 0)
                    break;

                sha1.AppendData(buffer, 0, read);
                await output.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                total += read;

                if (expectedRawSize > 0 && total > expectedRawSize)
                    throw new InvalidDataException($"LZ4 payload exceeded expected size; expected {expectedRawSize}, got {total}");
            }

            if (expectedRawSize > 0 && total != expectedRawSize)
                throw new InvalidDataException($"LZ4 payload did not match expected size; expected {expectedRawSize}, got {total}");

            return (Convert.ToHexString(sha1.GetHashAndReset()), total);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task<(bool HasHeader, string HeaderHash, long PayloadLength, bool Munged, Stream PayloadStream)> DetectCdnHeaderAsync(Stream stream, byte[] firstBytes, int firstByteCount, CancellationToken ct)
    {
        if (firstByteCount <= 0)
            return (false, string.Empty, 0, false, new PrefixedReadStream(stream, Array.Empty<byte>(), 0));

        var bytes = new List<byte>(256);
        for (var i = 0; i < firstByteCount; i++)
            bytes.Add(firstBytes[i]);

        static bool CouldBeHeaderStart(byte b) => b == (byte)'#' || (b ^ 42) == (byte)'#';

        if (!CouldBeHeaderStart(bytes[0]))
            return (false, string.Empty, 0, false, new PrefixedReadStream(stream, bytes.ToArray(), bytes.Count));

        var one = new byte[1];
        while (bytes.Count < 256)
        {
            var arr = bytes.ToArray();
            if (TryParseHeader(arr, munged: false, out var hash, out var length))
                return (true, hash, length, false, new PrefixedReadStream(stream, Array.Empty<byte>(), 0));

            if (TryParseHeader(arr, munged: true, out hash, out length))
                return (true, hash, length, true, new PrefixedReadStream(stream, Array.Empty<byte>(), 0));

            var read = await stream.ReadAsync(one.AsMemory(0, 1), ct).ConfigureAwait(false);
            if (read <= 0)
                break;

            bytes.Add(one[0]);
        }

        return (false, string.Empty, 0, false, new PrefixedReadStream(stream, bytes.ToArray(), bytes.Count));
    }

    private static async Task<(string Hash, long Length, bool Munged)> ReadCdnHeaderAsync(Stream stream, CancellationToken ct)
    {
        var bytes = new List<byte>(128);
        var one = new byte[1];

        while (bytes.Count < 256)
        {
            var read = await stream.ReadAsync(one.AsMemory(0, 1), ct).ConfigureAwait(false);
            if (read <= 0)
                throw new EndOfStreamException("Unexpected end of CDN header");

            bytes.Add(one[0]);
            if (one[0] != (byte)'#' && (one[0] ^ 42) != (byte)'#')
                continue;

            var arr = bytes.ToArray();
            if (TryParseHeader(arr, munged: false, out var hash, out var length))
                return (hash, length, false);

            if (TryParseHeader(arr, munged: true, out hash, out length))
                return (hash, length, true);
        }

        throw new InvalidDataException("CDN header was missing or too large");
    }

    private static bool TryParseHeader(ReadOnlySpan<byte> headerBytes, bool munged, out string fileHash, out long fileLengthBytes)
    {
        fileHash = string.Empty;
        fileLengthBytes = 0;

        if (headerBytes.Length < 4)
            return false;

        Span<char> chars = stackalloc char[headerBytes.Length];
        for (var i = 0; i < headerBytes.Length; i++)
            chars[i] = munged ? (char)(headerBytes[i] ^ 42) : (char)headerBytes[i];

        if (chars[0] != '#')
            return false;

        var colonIndex = chars.IndexOf(':');
        if (colonIndex <= 1)
            return false;

        var endIndex = chars[(colonIndex + 1)..].IndexOf('#');
        if (endIndex < 0)
            return false;

        endIndex += colonIndex + 1;
        var hashSpan = chars[1..colonIndex];
        var lenSpan = chars[(colonIndex + 1)..endIndex];

        if (!long.TryParse(lenSpan, out fileLengthBytes))
            return false;

        fileHash = new string(hashSpan).Trim().ToUpperInvariant();
        return fileHash.Length > 0;
    }

    private int TryPublishCentralFileRepairRequestBatch(IEnumerable<string?> hashes, string reason)
    {
        var filtered = hashes
            .Where(static h => !string.IsNullOrWhiteSpace(h))
            .Select(static h => h!.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (filtered.Count == 0 || string.IsNullOrWhiteSpace(_centralFileRepairTargetUid) || string.IsNullOrWhiteSpace(_centralFileRepairTargetIdent))
            return 0;

        var allowed = new List<string>(filtered.Count);
        foreach (var hash in filtered)
        {
            var attempts = _centralFileRepairAttemptsByHash.AddOrUpdate(hash, 1, (_, current) => current + 1);
            if (attempts <= 2)
            {
                allowed.Add(hash);
                continue;
            }

            _centralFileRepairFailedHashes[hash] = reason;
        }

        if (allowed.Count == 0)
            return 0;

        Logger.LogDebug("Requesting central B2 repair for {count} hash(es). Reason={reason}. Hashes={hashes}", allowed.Count, reason, string.Join(", ", allowed.Take(20)));
        Mediator.Publish(new RemoteMissingFileMessage(_centralFileRepairTargetUid, _centralFileRepairTargetIdent, _centralFileRepairDataHash, allowed, reason));
        return allowed.Count;
    }

    private bool TryPublishCentralFileRepairRequest(string hash, string reason)
        => TryPublishCentralFileRepairRequestBatch([hash], reason) > 0;

    private static string ComputeSha1(string path, CancellationToken ct)
    {
        using var sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        try
        {
            using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var read = input.Read(buffer, 0, buffer.Length);
                if (read <= 0)
                    break;

                sha1.AppendData(buffer, 0, read);
            }

            return Convert.ToHexString(sha1.GetHashAndReset());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void EnsureDirectory(string path)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);
    }

    private static string CreateTempPath(string finalPath)
    {
        var dir = Path.GetDirectoryName(finalPath);
        if (string.IsNullOrWhiteSpace(dir))
            dir = Path.GetTempPath();

        Directory.CreateDirectory(dir);
        return Path.Combine(dir, Path.GetFileName(finalPath) + "." + Guid.NewGuid().ToString("N") + ".tmp");
    }

    private static string CreateSpoolPath(string hash)
        => Path.Combine(Path.GetTempPath(), "ravasync_dl_" + hash + "_" + Guid.NewGuid().ToString("N") + ".payload");

    private static void TryDelete(string? path)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // ignored
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_activeStreamsLock)
                _activeDownloadStreams.Clear();
        }

        base.Dispose(disposing);
    }

    private sealed class StalledReadGuardStream : Stream
    {
        private readonly Stream _inner;
        private readonly TimeSpan _idleTimeout;
        private readonly CancellationToken _outerToken;

        public StalledReadGuardStream(Stream inner, TimeSpan idleTimeout, CancellationToken outerToken)
        {
            _inner = inner;
            _idleTimeout = idleTimeout;
            _outerToken = outerToken;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_outerToken, cancellationToken);
            linked.CancelAfter(_idleTimeout);

            try
            {
                return await _inner.ReadAsync(buffer, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!_outerToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException($"CDN stream made no progress for {_idleTimeout.TotalSeconds:0}s", null, CancellationToken.None);
            }
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
        private readonly int _count;
        private int _offset;

        public PrefixedReadStream(Stream inner, byte[] prefix, int count)
        {
            _inner = inner;
            _prefix = prefix;
            _count = Math.Clamp(count, 0, prefix.Length);
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
                var n = Math.Min(count, _count - _offset);
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
                var n = Math.Min(buffer.Length, _count - _offset);
                _prefix.AsMemory(_offset, n).CopyTo(buffer);
                _offset += n;
                return n;
            }

            return await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class LimitedXorStream : Stream
    {
        private readonly Stream _inner;
        private long _remaining;
        private readonly byte _xorKey;

        public LimitedXorStream(Stream inner, long length, byte xorKey)
        {
            _inner = inner;
            _remaining = Math.Max(0, length);
            _xorKey = xorKey;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _remaining;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_remaining <= 0)
                return 0;

            var toRead = (int)Math.Min(count, _remaining);
            var read = _inner.Read(buffer, offset, toRead);
            if (read > 0)
            {
                if (_xorKey != 0)
                {
                    for (var i = 0; i < read; i++)
                        buffer[offset + i] ^= _xorKey;
                }

                _remaining -= read;
            }

            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_remaining <= 0)
                return 0;

            var toRead = (int)Math.Min(buffer.Length, _remaining);
            var slice = buffer[..toRead];
            var read = await _inner.ReadAsync(slice, cancellationToken).ConfigureAwait(false);
            if (read > 0)
            {
                if (_xorKey != 0)
                {
                    var span = slice.Span[..read];
                    for (var i = 0; i < span.Length; i++)
                        span[i] ^= _xorKey;
                }

                _remaining -= read;
            }

            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class XorReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly byte _key;

        public XorReadStream(Stream inner, byte key)
        {
            _inner = inner;
            _key = key;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = _inner.Read(buffer, offset, count);
            for (var i = 0; i < read; i++)
                buffer[offset + i] ^= _key;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            var span = buffer.Span[..read];
            for (var i = 0; i < span.Length; i++)
                span[i] ^= _key;
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class FullReadStream : Stream
    {
        private readonly Stream _inner;
        public FullReadStream(Stream inner) => _inner = inner;
        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
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
                var read = await _inner.ReadAsync(buffer[total..], cancellationToken).ConfigureAwait(false);
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
