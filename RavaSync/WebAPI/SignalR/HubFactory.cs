using RavaSync.API.SignalR;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.WebAPI.SignalR.Utils;
using MessagePack;
using MessagePack.Resolvers;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Net.WebSockets;

namespace RavaSync.WebAPI.SignalR;

public class HubFactory : MediatorSubscriberBase
{
    private readonly ILoggerProvider _loggingProvider;
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly TokenProvider _tokenProvider;
    private HubConnection? _instance;
    private bool _isDisposed = false;
    private readonly bool _isWine = false;

    public HubFactory(ILogger<HubFactory> logger, MareMediator mediator,
        ServerConfigurationManager serverConfigurationManager,
        TokenProvider tokenProvider, ILoggerProvider pluginLog,
        DalamudUtilService dalamudUtilService) : base(logger, mediator)
    {
        _serverConfigurationManager = serverConfigurationManager;
        _tokenProvider = tokenProvider;
        _loggingProvider = pluginLog;
        _isWine = dalamudUtilService.IsWine;
    }

    public async Task DisposeHubAsync()
    {
        if (_instance == null || _isDisposed) return;

        Logger.LogDebug("Disposing current HubConnection");

        _isDisposed = true;

        _instance.Closed -= HubOnClosed;
        _instance.Reconnecting -= HubOnReconnecting;
        _instance.Reconnected -= HubOnReconnected;

        await _instance.StopAsync().ConfigureAwait(false);
        await _instance.DisposeAsync().ConfigureAwait(false);

        _instance = null;

        Logger.LogDebug("Current HubConnection disposed");
    }

    public HubConnection GetOrCreate(CancellationToken ct)
    {
        if (!_isDisposed && _instance != null) return _instance;

        return BuildHubConnection(ct);
    }

    private HubConnection BuildHubConnection(CancellationToken ct)
    {
        var transportType = _serverConfigurationManager.GetTransport() switch
        {
            HttpTransportType.None => HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling,
            HttpTransportType.WebSockets => HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling,
            HttpTransportType.ServerSentEvents => HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling,
            HttpTransportType.LongPolling => HttpTransportType.LongPolling,
            _ => HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling
        };

        if (_isWine && !_serverConfigurationManager.CurrentServer.ForceWebSockets
            && transportType.HasFlag(HttpTransportType.WebSockets))
        {
            Logger.LogDebug("Wine detected, falling back to ServerSentEvents / LongPolling");
            transportType = HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling;
        }

        Logger.LogDebug("Building new HubConnection using transport {transport}", transportType);

        _instance = new HubConnectionBuilder()
            //.WithUrl(_serverConfigurationManager.CurrentApiUrl + IMareHub.Path, options =>
            //{
            //    options.AccessTokenProvider = () => _tokenProvider.GetOrUpdateToken(CancellationToken.None);
            //    options.Transports = HttpTransportType.WebSockets; //transportType; Changed to allow less "wobble" over Proxy, which was causing timeouts and miss fires on users in France at the time. 20/12/2025
            //    options.SkipNegotiation = true;
            //})
            .WithUrl(_serverConfigurationManager.CurrentApiUrl + IMareHub.Path, options =>
            {
                options.AccessTokenProvider = () => _tokenProvider.GetOrUpdateToken(CancellationToken.None);
                options.Transports = HttpTransportType.WebSockets; //transportType; Changed to allow less "wobble" over Proxy, which was causing timeouts and miss fires on users in France at the time. 20/12/2025
                options.SkipNegotiation = true;
                options.WebSocketConfiguration = (ClientWebSocketOptions ws) =>
                {
                    ws.KeepAliveInterval = TimeSpan.FromSeconds(20);
                };
            })

            .AddMessagePackProtocol(opt =>
            {
                var resolver = CompositeResolver.Create(StandardResolverAllowPrivate.Instance,
                    BuiltinResolver.Instance,
                    AttributeFormatterResolver.Instance,
                    DynamicEnumAsStringResolver.Instance,
                    DynamicGenericResolver.Instance,
                    DynamicUnionResolver.Instance,
                    DynamicObjectResolver.Instance,
                    PrimitiveObjectResolver.Instance,
                    StandardResolver.Instance);

                opt.SerializerOptions =
                    MessagePackSerializerOptions.Standard
                        .WithCompression(MessagePackCompression.Lz4Block)
                        .WithResolver(resolver);
            })
            .WithAutomaticReconnect(new ForeverRetryPolicy(Mediator))
            .ConfigureLogging(a =>
            {
                a.ClearProviders().AddProvider(_loggingProvider);
                a.SetMinimumLevel(LogLevel.Information);
            })
            .Build();
        
        _instance.ServerTimeout = TimeSpan.FromMinutes(10);
        _instance.KeepAliveInterval = TimeSpan.FromSeconds(15);
        _instance.HandshakeTimeout = TimeSpan.FromSeconds(90);

        _instance.Closed += HubOnClosed;
        _instance.Reconnecting += HubOnReconnecting;
        _instance.Reconnected += HubOnReconnected;

        _isDisposed = false;

        return _instance;
    }

    private Task HubOnClosed(Exception? arg)
    {
        Mediator.Publish(new HubClosedMessage(arg));
        return Task.CompletedTask;
    }

    private Task HubOnReconnected(string? arg)
    {
        Mediator.Publish(new HubReconnectedMessage(arg));
        return Task.CompletedTask;
    }

    private Task HubOnReconnecting(Exception? arg)
    {
        Mediator.Publish(new HubReconnectingMessage(arg));
        return Task.CompletedTask;
    }
}