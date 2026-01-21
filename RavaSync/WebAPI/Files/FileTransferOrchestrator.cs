using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI.Files.Models;
using RavaSync.WebAPI.SignalR;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Headers;

using System.Reflection;
using System.Text;
using System.Text.Json;

namespace RavaSync.WebAPI.Files;

public class FileTransferOrchestrator : DisposableMediatorSubscriberBase
{
    private readonly ConcurrentDictionary<Guid, bool> _downloadReady = new();
    private readonly HttpClient _httpClient;
    private readonly MareConfigService _mareConfig;
    private readonly object _semaphoreModificationLock = new();
    private readonly TokenProvider _tokenProvider;
    private int _availableDownloadSlots;
    private SemaphoreSlim _downloadSemaphore;
    private int CurrentlyUsedDownloadSlots => _availableDownloadSlots - _downloadSemaphore.CurrentCount;

    public Uri? FilesCdnUri { private set; get; }
    public Uri? UploadCdnUri { private set; get; }
    public List<FileTransfer> ForbiddenTransfers { get; } = [];
    public bool IsInitialized => FilesCdnUri != null;

    public FileTransferOrchestrator(ILogger<FileTransferOrchestrator> logger, MareConfigService mareConfig,
        MareMediator mediator, TokenProvider tokenProvider, HttpClient httpClient) : base(logger, mediator)
    {
        _mareConfig = mareConfig;
        _tokenProvider = tokenProvider;
        _httpClient = httpClient;
        var ver = Assembly.GetExecutingAssembly().GetName().Version;
        _httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("RavaSync", ver!.Major + "." + ver!.Minor + "." + ver!.Build));

        _availableDownloadSlots = GetEffectiveParallelDownloads();
        _downloadSemaphore = new(_availableDownloadSlots, _availableDownloadSlots);

        Mediator.Subscribe<ConnectedMessage>(this, (msg) =>
        {
            FilesCdnUri = msg.Connection.ServerInfo.FileServerAddress;
            UploadCdnUri = msg.Connection.ServerInfo.UploadFileServerAddress;
        });

        Mediator.Subscribe<DisconnectedMessage>(this, (msg) =>
        {
            FilesCdnUri = null;
            UploadCdnUri = null;
        });
        Mediator.Subscribe<DownloadReadyMessage>(this, (msg) =>
        {
            _downloadReady[msg.RequestId] = true;
        });
    }

    public void ClearDownloadRequest(Guid guid)
    {
        _downloadReady.Remove(guid, out _);
    }

    public bool IsDownloadReady(Guid guid)
    {
        if (_downloadReady.TryGetValue(guid, out bool isReady) && isReady)
        {
            return true;
        }

        return false;
    }

    public void ReleaseDownloadSlot()
    {
        try
        {
            _downloadSemaphore.Release();
            Mediator.Publish(new DownloadLimitChangedMessage());
        }
        catch (SemaphoreFullException)
        {
            // ignore
        }
    }

    public async Task<HttpResponseMessage> SendRequestAsync(
        HttpMethod method, Uri uri,
        CancellationToken? ct = null,
        HttpCompletionOption httpCompletionOption = HttpCompletionOption.ResponseHeadersRead)
    {
        var requestMessage = new HttpRequestMessage(method, uri);
        var resp = await SendRequestInternalAsync(requestMessage, ct, httpCompletionOption).ConfigureAwait(false);
        requestMessage.Dispose();
        return resp;
    }


    public async Task<HttpResponseMessage> SendRequestAsync<T>(HttpMethod method, Uri uri, T content, CancellationToken ct) where T : class
    {
        // DO NOT dispose the request prematurely; HttpClient owns the content stream lifecycle.
        var requestMessage = new HttpRequestMessage(method, uri);

        if (content is HttpContent httpContent)
        {
            requestMessage.Content = httpContent;

            if (httpContent is StreamContent || httpContent is Lz4FileContent)
                requestMessage.Headers.ExpectContinue = false;
        }
        else if (content is not null)
        {
            // Avoid JsonContent.Create; stick to plain StringContent
            var json = JsonSerializer.Serialize(content);
            requestMessage.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }
        var resp = await SendRequestInternalAsync(requestMessage, ct).ConfigureAwait(false);

        requestMessage.Dispose();
        return resp;
    }

    public async Task WaitForDownloadSlotAsync(CancellationToken token)
    {
        lock (_semaphoreModificationLock)
        {
            var desired = GetEffectiveParallelDownloads();

            if (_availableDownloadSlots != desired && _downloadSemaphore.CurrentCount == _availableDownloadSlots)
            {
                _availableDownloadSlots = desired;
                _downloadSemaphore = new SemaphoreSlim(_availableDownloadSlots, _availableDownloadSlots);
            }
        }

        await _downloadSemaphore.WaitAsync(token).ConfigureAwait(false);
        Mediator.Publish(new DownloadLimitChangedMessage());
    }

    private int GetEffectiveParallelDownloads()
    {
        var configured = _mareConfig.Current.ParallelDownloads;

        if (configured > 0)
            return Math.Clamp(configured, 1, 12);

        var cpu = Environment.ProcessorCount;

        var auto =
            cpu <= 4 ? 2 :
            cpu <= 8 ? 3 :
            cpu <= 16 ? 4 :
                        6;

        return Math.Clamp(auto, 1, 12);
    }



    public long DownloadLimitPerSlot()
    {
        var limit = _mareConfig.Current.DownloadSpeedLimitInBytes;
        if (limit <= 0) return 0;
        limit = _mareConfig.Current.DownloadSpeedType switch
        {
            MareConfiguration.Models.DownloadSpeeds.Bps => limit,
            MareConfiguration.Models.DownloadSpeeds.KBps => limit * 1024,
            MareConfiguration.Models.DownloadSpeeds.MBps => limit * 1024 * 1024,
            _ => limit,
        };
        var currentUsedDlSlots = CurrentlyUsedDownloadSlots;
        var avaialble = _availableDownloadSlots;
        var currentCount = _downloadSemaphore.CurrentCount;
        var dividedLimit = limit / (currentUsedDlSlots == 0 ? 1 : currentUsedDlSlots);
        if (dividedLimit < 0)
        {
            Logger.LogWarning("Calculated Bandwidth Limit is negative, returning Infinity: {value}, CurrentlyUsedDownloadSlots is {currentSlots}, " +
                "DownloadSpeedLimit is {limit}, available slots: {avail}, current count: {count}", dividedLimit, currentUsedDlSlots, limit, avaialble, currentCount);
            return long.MaxValue;
        }
        return Math.Clamp(dividedLimit, 1, long.MaxValue);
    }

    private async Task<HttpResponseMessage> SendRequestInternalAsync(
      HttpRequestMessage originalRequestMessage,
      CancellationToken? ct = null,
      HttpCompletionOption httpCompletionOption = HttpCompletionOption.ResponseHeadersRead)
    {
        async Task<HttpRequestMessage> BuildRequestAsync()
        {
            var clone = new HttpRequestMessage(originalRequestMessage.Method, originalRequestMessage.RequestUri);

            foreach (var h in originalRequestMessage.Headers)
                clone.Headers.TryAddWithoutValidation(h.Key, h.Value);

            if (originalRequestMessage.Content is HttpContent content)
            {
                if (content is StreamContent || content is Lz4FileContent)
                {
                    clone.Content = content;

                    if (clone.Method == HttpMethod.Post || clone.Method == HttpMethod.Put)
                        clone.Headers.ExpectContinue = false;
                }
                else
                {
                    var bytes = await content.ReadAsByteArrayAsync().ConfigureAwait(false);
                    var bac = new ByteArrayContent(bytes);
                    foreach (var h in content.Headers)
                        bac.Headers.TryAddWithoutValidation(h.Key, h.Value);
                    clone.Content = bac;
                }
            }

            var token = await _tokenProvider.GetOrUpdateToken(ct ?? CancellationToken.None).ConfigureAwait(false);
            clone.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            return clone;
        }


        bool nonReplayable = originalRequestMessage.Content is StreamContent
                             || originalRequestMessage.Content is Lz4FileContent;

        if (originalRequestMessage.Content is null)
        {
            Logger.LogDebug("HTTP {method} {uri}", originalRequestMessage.Method, originalRequestMessage.RequestUri);
        }
        else if (originalRequestMessage.Content is StringContent || originalRequestMessage.Content is ByteArrayContent)
        {
            var len = originalRequestMessage.Content.Headers.ContentLength;
            Logger.LogDebug("HTTP {method} {uri} (buffered body length: {len})",
                originalRequestMessage.Method, originalRequestMessage.RequestUri, len ?? -1);
        }
        else
        {
            Logger.LogDebug("HTTP {method} {uri} (streaming body)",
                originalRequestMessage.Method, originalRequestMessage.RequestUri);
        }

        // One-shot path for streaming/non-replayable bodies
        if (nonReplayable)
        {
            using var oneShot = await BuildRequestAsync().ConfigureAwait(false);
            if (ct is not null)
                return await _httpClient.SendAsync(oneShot, httpCompletionOption, ct.Value).ConfigureAwait(false);
            return await _httpClient.SendAsync(oneShot, httpCompletionOption).ConfigureAwait(false);
        }

        // Replayable: transient retry with jitter
        var delays = new[] { 0.5, 1.0, 2.0, 4.0 }; // seconds
        var rng = new Random();

        for (int attempt = 0; attempt <= delays.Length; attempt++)
        {
            using var req = await BuildRequestAsync().ConfigureAwait(false);

            try
            {
                if (ct is not null)
                    return await _httpClient.SendAsync(req, httpCompletionOption, ct.Value).ConfigureAwait(false);
                return await _httpClient.SendAsync(req, httpCompletionOption).ConfigureAwait(false);
            }
            catch (TaskCanceledException) when (ct is null || !ct.Value.IsCancellationRequested)
            {
                // treat as transient timeout
            }
            catch (HttpRequestException hre) when ((int?)(hre.StatusCode ?? 0) is 408 or 429
                                                   || (int?)(hre.StatusCode ?? 0) is >= 500 and <= 599)
            {
                Logger.LogDebug(hre, "Transient HTTP error on {uri}", req.RequestUri);
            }
            catch (Exception ex) when (attempt < delays.Length)
            {
                Logger.LogDebug(ex, "Transient error on {uri}", req.RequestUri);
            }

            if (attempt == delays.Length) break;
            var delay = TimeSpan.FromSeconds(delays[attempt] * (1.0 + rng.NextDouble() * 0.25));
            await Task.Delay(delay, ct ?? CancellationToken.None).ConfigureAwait(false);
        }

        // Final send (let it throw)
        using (var finalReq = await BuildRequestAsync().ConfigureAwait(false))
        {
            if (ct is not null)
                return await _httpClient.SendAsync(finalReq, httpCompletionOption, ct.Value).ConfigureAwait(false);
            return await _httpClient.SendAsync(finalReq, httpCompletionOption).ConfigureAwait(false);
        }
    }

}