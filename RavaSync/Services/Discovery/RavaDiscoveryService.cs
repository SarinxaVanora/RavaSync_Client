using Dalamud.Game.ClientState;
using Dalamud.Game.ClientState.Objects;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using RavaSync.API.Data;
using RavaSync.Interop;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Services.Discovery;

public sealed class RavaDiscoveryService
    : DisposableMediatorSubscriberBase, IHostedService
{
    private readonly ILogger<RavaDiscoveryService> _logger;
    private readonly IObjectTable _objects;
    private readonly IClientState _clientState;
    private readonly IRavaMesh _mesh;
    private readonly MareMediator _mediator;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly ApiController _api;
    private readonly MareConfigService _configService;

    private readonly ConcurrentDictionary<string, DateTime> _lastHelloSentUtc = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, RavaPeerInfo> _knownPeers = new(StringComparer.Ordinal);

    private string? _mySessionId;
    private byte[] _myPeerKey = Array.Empty<byte>();
    private UserData? _ownUser;
    private bool _lastDiscoveryPresence = false;

    private static readonly TimeSpan HelloInterval = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PeerTtl = TimeSpan.FromSeconds(9);
    private static readonly TimeSpan PruneInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan HelloScanInterval = TimeSpan.FromMilliseconds(250);
    private DateTime _nextHelloScanUtc = DateTime.MinValue;

    private const int MaxHellosPerTick = 6;

    private uint _lastTerritoryId = 0;
    private DateTime _lastPruneUtc = DateTime.MinValue;
    private int _roundRobinStartIndex = 0;
    private bool _isMeshListening = false;


    public sealed record RavaPeerInfo(
        string SessionId,
        byte[] PeerKey,
        DateTime FirstSeenUtc,
        DateTime LastSeenUtc
    );

    public RavaDiscoveryService(ILogger<RavaDiscoveryService> logger,IObjectTable objectTable,IClientState clientState,IRavaMesh mesh,MareMediator mediator,DalamudUtilService dalamudUtil, ApiController api, MareConfigService configService) : base(logger, mediator)
    {
        _logger = logger;
        _objects = objectTable;
        _clientState = clientState;
        _mesh = mesh;
        _mediator = mediator;
        _dalamudUtil = dalamudUtil;
        _api = api;
        _configService = configService;

        _mediator.Subscribe<FrameworkUpdateMessage>(this, _ => OnFrameworkUpdate());

        _mediator.Subscribe<ConnectedMessage>(this, msg =>
        {
            _ownUser = msg.Connection.User;
        });
        _mediator.Subscribe<DisconnectedMessage>(this, _ =>
        {
            ResetMeshListener();
        });

        _mediator.Subscribe<HubReconnectingMessage>(this, _ =>
        {
            ResetMeshListener();
        });

        _mediator.Subscribe<HubClosedMessage>(this, _ =>
        {
            ResetMeshListener();
        });
    }

    private void ResetMeshListener()
    {
        try
        {
            if (!string.IsNullOrEmpty(_mySessionId))
            {
                _mesh.Unlisten(_mySessionId);
            }
        }
        catch
        {
            // ignore
        }

        _isMeshListening = false;
        _knownPeers.Clear();
        _lastHelloSentUtc.Clear();
    }


    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RavaDiscoveryService starting");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RavaDiscoveryService stopping");
        _mediator.UnsubscribeAll(this);
        _knownPeers.Clear();
        _lastHelloSentUtc.Clear();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Public API: given an ident, do we know this ident belongs to a RavaSync user?
    /// </summary>
    public bool IsIdentKnownAsRavaUser(string ident)
    {
        var sessionId = RavaSessionId.FromIdent(ident);
        return _knownPeers.ContainsKey(sessionId);
    }

    private void OnFrameworkUpdate()
    {
        if (!_clientState.IsLoggedIn || _clientState.LocalPlayer is null) return;

        if (_mySessionId == null)
        {
            try
            {
                var me = _clientState.LocalPlayer;
                var myIdent = _dalamudUtil.GetIdentFromGameObject(me);
                if (string.IsNullOrEmpty(myIdent)) return;

                _mySessionId = RavaSessionId.FromIdent(myIdent);

                if (_myPeerKey.Length == 0)
                    _myPeerKey = Guid.NewGuid().ToByteArray();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize RavaDiscoveryService session");
                return;
            }
        }

        if (_mySessionId == null) return;

        if (_api.IsConnected && !_isMeshListening)
        {
            try
            {
                _mesh.Listen(_mySessionId, OnMeshMessageAsync);
                _isMeshListening = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start mesh listener");
                return;
            }
        }

        if (!_api.IsConnected) return;

        var discoveryEnabled = _configService.Current.EnableRavaDiscoveryPresence;

        if (!discoveryEnabled && _lastDiscoveryPresence)
        {
            if (!_knownPeers.IsEmpty)
            {
                foreach (var peer in _knownPeers.Values)
                {
                    var goodbye = new RavaGoodbye(_mySessionId, _myPeerKey);
                    _ = _mesh.SendAsync(peer.SessionId, goodbye);
                }
            }

            _knownPeers.Clear();
            _lastHelloSentUtc.Clear();

            _logger.LogDebug("RavaDiscovery: discovery disabled, sent goodbyes to all known peers");
        }

        _lastDiscoveryPresence = discoveryEnabled;

        HandleTerritoryChange();
        if (!discoveryEnabled) return;

        var now = DateTime.UtcNow;
        PrunePeers(now);

        if (now < _nextHelloScanUtc) return;
        _nextHelloScanUtc = now + HelloScanInterval;

        int sent = 0;

        var len = _objects.Length;
        if (len == 0) return;

        if (_roundRobinStartIndex >= len)
            _roundRobinStartIndex = 0;

        for (int offset = 0; offset < len && sent < MaxHellosPerTick; offset++)
        {
            var idx = (_roundRobinStartIndex + offset) % len;

            if (_objects[idx] is not IPlayerCharacter pc) continue;
            if (pc.Address == IntPtr.Zero) continue;
            if (_clientState.LocalPlayer!.Address == pc.Address) continue;

            string ident;
            try
            {
                ident = _dalamudUtil.GetIdentFromGameObject(pc) ?? string.Empty;
            }
            catch
            {
                continue;
            }

            if (string.IsNullOrEmpty(ident)) continue;

            var sessionId = RavaSessionId.FromIdent(ident);

            if (_lastHelloSentUtc.TryGetValue(sessionId, out var last) &&
                (now - last) < HelloInterval)
            {
                continue;
            }

            _lastHelloSentUtc[sessionId] = now;

            var hello = new RavaHello(_mySessionId, _myPeerKey);
            _ = _mesh.SendAsync(sessionId, hello);

            sent++;
        }

        _roundRobinStartIndex = (_roundRobinStartIndex + MaxHellosPerTick) % len;
    }

    private Task OnMeshMessageAsync(string sessionId, IRavaMeshMessage message)
    {
        var discoveryEnabled = _configService.Current.EnableRavaDiscoveryPresence;

        try
        {
            switch (message)
            {
                case RavaGame game:
                    _mediator.Publish(new SyncshellGameMeshMessage(sessionId, game.FromSessionId, game.Payload));
                    break;

                case RavaHello hello:
                    if (!discoveryEnabled) break;
                    HandleHello(hello);
                    break;

                case RavaHelloAck ack:
                    if (!discoveryEnabled) break;
                    HandleHelloAck(ack);
                    break;

                case RavaPairRequest pr:
                    if (!discoveryEnabled) break;
                    HandlePairRequest(sessionId, pr);
                    break;

                case RavaGoodbye bye:
                    if (!discoveryEnabled) break;
                    HandleGoodbye(bye);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling mesh message");
        }

        return Task.CompletedTask;
    }



    private void HandleHello(RavaHello hello)
    {
        if (_mySessionId == null) return;

        var info = new RavaPeerInfo(
            SessionId: hello.FromSessionId,
            PeerKey: hello.FromPeerKey,
            FirstSeenUtc: DateTime.UtcNow,
            LastSeenUtc: DateTime.UtcNow);

        _knownPeers.AddOrUpdate(
            hello.FromSessionId,
            info,
            (_, existing) => existing with { LastSeenUtc = DateTime.UtcNow });

        // reply back so the other side also knows we're here
        var ack = new RavaHelloAck(_mySessionId, _myPeerKey);
        _ = _mesh.SendAsync(hello.FromSessionId, ack);
    }

    private void HandleHelloAck(RavaHelloAck ack)
    {
        var info = new RavaPeerInfo(
            SessionId: ack.FromSessionId,
            PeerKey: ack.FromPeerKey,
            FirstSeenUtc: DateTime.UtcNow,
            LastSeenUtc: DateTime.UtcNow);

        _knownPeers.AddOrUpdate(
            ack.FromSessionId,
            info,
            (_, existing) => existing with { LastSeenUtc = DateTime.UtcNow });
    }


    private void HandlePairRequest(string localSessionId, RavaPairRequest pr)
    {

        Mediator.Publish(new PairRequestReceivedMessage(pr.Request));
    }

    private void HandleGoodbye(RavaGoodbye bye)
    {
        if (_knownPeers.TryRemove(bye.FromSessionId, out _))
        {
            _logger.LogDebug("RavaDiscovery: received goodbye");
        }
    }

   private void HandleTerritoryChange()
    {
        var territory = _dalamudUtil.CurrentTerritoryId;

        if (_lastTerritoryId == 0)
        {
            _lastTerritoryId = territory;
            return;
        }

        if (territory == _lastTerritoryId)
            return;

        var oldSession = _mySessionId;

        SendGoodbyeToAll();

        _knownPeers.Clear();
        _lastHelloSentUtc.Clear();

        _mySessionId = null;

        if (!string.IsNullOrEmpty(oldSession))
        {
            _mesh.Unlisten(oldSession);
            _isMeshListening = false;
        }



        _lastTerritoryId = territory;
    }


    private void PrunePeers(DateTime nowUtc)
    {
        if ((nowUtc - _lastPruneUtc) < PruneInterval)
            return;

        _lastPruneUtc = nowUtc;

        foreach (var kvp in _knownPeers)
        {
            var info = kvp.Value;
            if ((nowUtc - info.LastSeenUtc) > PeerTtl)
            {
                _knownPeers.TryRemove(kvp.Key, out _);
                _lastHelloSentUtc.TryRemove(kvp.Key, out _);
            }
        }
    }

    private void SendGoodbyeToAll()
    {
        if (_mySessionId == null)
            return;

        var peers = _knownPeers.Keys.ToArray();

        foreach (var peerSessionId in peers)
        {
            _ = _mesh.SendAsync(peerSessionId, new RavaGoodbye(_mySessionId, _myPeerKey));
        }
    }


}
