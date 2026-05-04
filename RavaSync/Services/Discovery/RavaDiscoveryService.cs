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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Services.Discovery;

public sealed class RavaDiscoveryService : DisposableMediatorSubscriberBase, IHostedService
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
    private static readonly TimeSpan HelloScanInterval = TimeSpan.FromMilliseconds(750);
    private DateTime _nextHelloScanUtc = DateTime.MinValue;

    private const int MaxHellosPerTick = 6;

    private uint _lastTerritoryId = 0;
    private DateTime _lastPruneUtc = DateTime.MinValue;
    private int _roundRobinStartIndex = 0;
    private bool _isMeshListening = false;

    private readonly object _yieldBroadcastGate = new();
    private sealed record LocalYieldStateEntry(bool Yield, string Owner);
    private sealed record YieldBroadcastStateEntry(bool Yield, string Owner);
    private readonly ConcurrentDictionary<string, LocalYieldStateEntry> _localYieldStatesByUid = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, YieldBroadcastStateEntry> _lastYieldBroadcastByUid = new(StringComparer.Ordinal);

    private sealed record RemoteYieldCacheEntry(bool Yield, string Owner, DateTime LastSeenUtc);
    private readonly ConcurrentDictionary<string, RemoteYieldCacheEntry> _lastRemoteYieldByUid = new(StringComparer.Ordinal);
    private static readonly TimeSpan RemoteYieldDedupWindow = TimeSpan.FromSeconds(6);


    private sealed record ObjectSessionCacheEntry(string SessionId,DateTime LastSeenUtc);

    private readonly ConcurrentDictionary<nint, ObjectSessionCacheEntry> _objectSessionCache = new();
    private static readonly TimeSpan ObjectSessionCacheTtl = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ObjectSessionCachePruneInterval = TimeSpan.FromSeconds(5);
    private DateTime _lastObjectSessionPruneUtc = DateTime.MinValue;

    private readonly ConcurrentQueue<SyncshellGameMeshMessage> _pendingGameMeshMessages = new();
    private const int MaxGameMeshMessagesPerTick = 8;

    public sealed record RavaPeerInfo(string SessionId,byte[] PeerKey,DateTime FirstSeenUtc,DateTime LastSeenUtc);
    public sealed record RavaYieldByIdentReceivedMessage(string FromIdent, bool YieldToOtherSync, string Owner, TimeSpan Ttl);

    public RavaDiscoveryService(ILogger<RavaDiscoveryService> logger,IObjectTable objectTable,
        IClientState clientState,IRavaMesh mesh,MareMediator mediator,DalamudUtilService dalamudUtil,ApiController api,MareConfigService configService) : base(logger, mediator)
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

        _mediator.Subscribe<LocalOtherSyncYieldStateChangedMessage>(this, msg =>
        {
            var affectedUid = msg.AffectedUid ?? string.Empty;
            if (string.IsNullOrWhiteSpace(affectedUid))
                return;

            var yield = msg.YieldToOtherSync;
            var owner = yield ? (msg.Owner ?? string.Empty) : string.Empty;

            _localYieldStatesByUid[affectedUid] = new LocalYieldStateEntry(yield, owner);

            BroadcastLocalOtherSyncState(affectedUid, yield, owner);
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
        _objectSessionCache.Clear();

        _lastYieldBroadcastByUid.Clear();
        _lastRemoteYieldByUid.Clear();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RavaDiscoveryService starting");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RavaDiscoveryService stopping");

        ResetMeshListener();
        _mediator.UnsubscribeAll(this);

        return Task.CompletedTask;
    }

    public bool IsIdentKnownAsRavaUser(string ident)
    {
        var sessionId = RavaSessionId.FromIdent(ident);
        return _knownPeers.ContainsKey(sessionId);
    }

    public string[] GetKnownPeerSessionIds()
    {
        if (_knownPeers.IsEmpty)
            return Array.Empty<string>();

        return _knownPeers.Keys.ToArray();
    }


    private void OnFrameworkUpdate()
    {
        if (!_clientState.IsLoggedIn || _objects.LocalPlayer is null) return;

        if (_mySessionId == null)
        {
            try
            {
                var me = _objects.LocalPlayer;
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

                // Re-broadcast current local yield states on mesh start (reconnect case)
                BroadcastAllLocalOtherSyncStates();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start mesh listener");
                return;
            }
        }

        if (!_api.IsConnected) return;

        var discoveryEnabled = _configService.Current.EnableRavaDiscoveryPresence;

        if (!discoveryEnabled && _pendingGameMeshMessages.IsEmpty && !_lastDiscoveryPresence)
            return;

        if (!discoveryEnabled && _lastDiscoveryPresence)
        {
            SendGoodbyeToAll();
            _knownPeers.Clear();
            _lastHelloSentUtc.Clear();
            _objectSessionCache.Clear();

            _logger.LogDebug("RavaDiscovery: discovery disabled, sent goodbyes to all known peers");
        }

        _lastDiscoveryPresence = discoveryEnabled;

        HandleTerritoryChange();
        FlushPendingGameMeshMessages();
        if (!discoveryEnabled) return;

        var now = DateTime.UtcNow;
        if (_objects.Length == 0)
        {
            if (!_knownPeers.IsEmpty)
                PrunePeers(now);
            if (!_objectSessionCache.IsEmpty)
                PruneObjectSessionCache(now);
            return;
        }

        if (!_knownPeers.IsEmpty)
            PrunePeers(now);
        if (!_objectSessionCache.IsEmpty)
            PruneObjectSessionCache(now);

        if (now < _nextHelloScanUtc) return;
        _nextHelloScanUtc = now + HelloScanInterval;

        int sent = 0;

        var len = _objects.Length;

        if (_roundRobinStartIndex >= len)
            _roundRobinStartIndex = 0;

        for (int offset = 0; offset < len && sent < MaxHellosPerTick; offset++)
        {
            var idx = (_roundRobinStartIndex + offset) % len;

            if (_objects[idx] is not IPlayerCharacter pc) continue;
            if (pc.Address == IntPtr.Zero) continue;
            if (_objects.LocalPlayer!.Address == pc.Address) continue;

            if (!TryGetSessionIdForPlayer(pc, now, out var sessionId))
                continue;

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
                case RavaHello h:
                    TouchPeer(h.FromSessionId, h.FromPeerKey);
                    break;
                case RavaHelloAck a:
                    TouchPeer(a.FromSessionId, a.FromPeerKey);
                    break;
                case RavaYield y:
                    TouchPeer(y.FromSessionId, peerKey: null);
                    break;
                case RavaGame g:
                    TouchPeer(g.FromSessionId, peerKey: null);
                    break;
                case RavaGoodbye b:
                    TouchPeer(b.FromSessionId, peerKey: null);
                    break;
            }

            switch (message)
            {
                case RavaYield y:
                    {
                        var now = DateTime.UtcNow;

                        var targetUid = y.AffectedUid ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(targetUid))
                            break;

                        var ownUid = _ownUser?.UID ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(ownUid) || !string.Equals(targetUid, ownUid, StringComparison.Ordinal))
                            break;

                        var fromUid = y.FromUid ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(fromUid))
                            break;

                        var owner = y.Owner ?? string.Empty;
                        if (!y.YieldToOtherSync)
                            owner = string.Empty;

                        var cacheKey = fromUid;
                        var incoming = new RemoteYieldCacheEntry(y.YieldToOtherSync, owner, now);

                        if (_lastRemoteYieldByUid.TryGetValue(cacheKey, out var last))
                        {
                            var same =
                                last.Yield == incoming.Yield
                                && string.Equals(last.Owner, incoming.Owner, StringComparison.OrdinalIgnoreCase);

                            if (same && (now - last.LastSeenUtc) < RemoteYieldDedupWindow)
                            {
                                _lastRemoteYieldByUid[cacheKey] = last with { LastSeenUtc = now };
                                break;
                            }
                        }

                        _lastRemoteYieldByUid[cacheKey] = incoming;

                        _mediator.Publish(new RemoteOtherSyncYieldMessage( FromUid: fromUid,YieldToOtherSync: incoming.Yield,Owner: incoming.Owner,Ttl: TimeSpan.FromSeconds(12)));

                        break;
                    }

                case RavaGame game:
                    _pendingGameMeshMessages.Enqueue(new SyncshellGameMeshMessage(sessionId, game.FromSessionId, game.Payload));
                    break;

                case RavaHello hello:
                    if (!discoveryEnabled) break;
                    HandleHello(sessionId, hello);
                    break;

                case RavaHelloAck ack:
                    if (!discoveryEnabled) break;
                    HandleHelloAck(sessionId, ack);
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

    private void FlushPendingGameMeshMessages()
    {
        if (_pendingGameMeshMessages.IsEmpty)
            return;

        for (var i = 0; i < MaxGameMeshMessagesPerTick; i++)
        {
            if (!_pendingGameMeshMessages.TryDequeue(out var msg))
                break;

            _mediator.Publish(msg);
        }
    }

    private void TouchPeer(string sessionId, byte[]? peerKey)
    {
        if (string.IsNullOrWhiteSpace(sessionId))
            return;

        var now = DateTime.UtcNow;

        _knownPeers.AddOrUpdate(sessionId,
            _ => new RavaPeerInfo(
                SessionId: sessionId,
                PeerKey: peerKey ?? Array.Empty<byte>(),
                FirstSeenUtc: now,
                LastSeenUtc: now),
            (_, existing) => existing with
            {
                LastSeenUtc = now,
                PeerKey = (peerKey != null && peerKey.Length > 0) ? peerKey : existing.PeerKey
            });
    }

    private void HandleHello(string transportSessionId, RavaHello hello)
    {
        if (_mySessionId == null) return;

        var from = !string.IsNullOrWhiteSpace(hello.FromSessionId) ? hello.FromSessionId : transportSessionId;
        if (string.IsNullOrWhiteSpace(from))
            return;

        var now = DateTime.UtcNow;

        var info = new RavaPeerInfo(
            SessionId: from,
            PeerKey: hello.FromPeerKey ?? Array.Empty<byte>(),
            FirstSeenUtc: now,
            LastSeenUtc: now);

        _knownPeers.AddOrUpdate(from, info, (_, existing) => existing with { LastSeenUtc = now });

        BroadcastLocalOtherSyncStatesToPeer(from);

        var ack = new RavaHelloAck(_mySessionId, _myPeerKey);
        _ = _mesh.SendAsync(from, ack);
    }

    private void HandleHelloAck(string transportSessionId, RavaHelloAck ack)
    {
        var from = !string.IsNullOrWhiteSpace(ack.FromSessionId) ? ack.FromSessionId : transportSessionId;
        if (string.IsNullOrWhiteSpace(from))
            return;

        var now = DateTime.UtcNow;

        var info = new RavaPeerInfo(
            SessionId: from,
            PeerKey: ack.FromPeerKey ?? Array.Empty<byte>(),
            FirstSeenUtc: now,
            LastSeenUtc: now);

        _knownPeers.AddOrUpdate(from, info, (_, existing) => existing with { LastSeenUtc = now });

        BroadcastLocalOtherSyncStatesToPeer(from);
    }

    private void HandlePairRequest(string localSessionId, RavaPairRequest pr)
    {
        Mediator.Publish(new PairRequestReceivedMessage(pr.Request));
    }

    private void HandleGoodbye(RavaGoodbye bye)
    {
        if (_knownPeers.TryRemove(bye.FromSessionId, out _))
        {
            _lastHelloSentUtc.TryRemove(bye.FromSessionId, out _);
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
        _objectSessionCache.Clear();

        _mySessionId = null;

        _lastYieldBroadcastByUid.Clear();

        if (!string.IsNullOrEmpty(oldSession))
        {
            try
            {
                _mesh.Unlisten(oldSession);
            }
            catch
            {
                // ignore
            }

            _isMeshListening = false;
        }

        _lastTerritoryId = territory;

    }

    private void PruneObjectSessionCache(DateTime nowUtc)
    {
        if (_objectSessionCache.IsEmpty)
            return;

        if ((nowUtc - _lastObjectSessionPruneUtc) < ObjectSessionCachePruneInterval)
            return;

        _lastObjectSessionPruneUtc = nowUtc;

        foreach (var kvp in _objectSessionCache)
        {
            if ((nowUtc - kvp.Value.LastSeenUtc) > ObjectSessionCacheTtl)
                _objectSessionCache.TryRemove(kvp.Key, out _);
        }
    }

    private bool TryGetSessionIdForPlayer(IPlayerCharacter pc, DateTime nowUtc, out string sessionId)
    {
        sessionId = string.Empty;

        var addr = pc.Address;
        if (addr == IntPtr.Zero)
            return false;

        if (_objectSessionCache.TryGetValue(addr, out var cached))
        {
            if ((nowUtc - cached.LastSeenUtc) <= ObjectSessionCacheTtl)
            {
                _objectSessionCache[addr] = cached with { LastSeenUtc = nowUtc };
                sessionId = cached.SessionId;
                return !string.IsNullOrEmpty(sessionId);
            }
        }

        string ident;
        try
        {
            ident = _dalamudUtil.GetIdentFromGameObject(pc) ?? string.Empty;
        }
        catch
        {
            return false;
        }

        if (string.IsNullOrEmpty(ident))
            return false;

        sessionId = RavaSessionId.FromIdent(ident);
        if (string.IsNullOrEmpty(sessionId))
            return false;

        _objectSessionCache[addr] = new ObjectSessionCacheEntry(sessionId, nowUtc);
        return true;
    }

    private void PrunePeers(DateTime nowUtc)
    {
        if (_knownPeers.IsEmpty)
            return;

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

    private void BroadcastLocalOtherSyncStatesToPeer(string sessionId)
    {
        if (!_api.IsConnected) return;
        if (!_isMeshListening) return;
        if (_mySessionId == null) return;
        if (string.IsNullOrWhiteSpace(sessionId)) return;

        foreach (var kvp in _localYieldStatesByUid)
        {
            var msg = new RavaYield(
                FromSessionId: _mySessionId,
                FromUid: _ownUser?.UID ?? string.Empty,
                AffectedUid: kvp.Key,
                YieldToOtherSync: kvp.Value.Yield,
                Owner: kvp.Value.Yield ? (kvp.Value.Owner ?? string.Empty) : string.Empty);

            _ = _mesh.SendAsync(sessionId, msg);
        }
    }

    private void BroadcastAllLocalOtherSyncStates()
    {
        if (!_api.IsConnected) return;
        if (!_isMeshListening) return;
        if (_mySessionId == null) return;

        foreach (var kvp in _localYieldStatesByUid)
        {
            BroadcastLocalOtherSyncState(kvp.Key,kvp.Value.Yield,kvp.Value.Yield ? kvp.Value.Owner : string.Empty);
        }
    }

    public void BroadcastLocalOtherSyncState(string affectedUid, bool yieldToOtherSync, string owner)
    {
        if (!_api.IsConnected) return;
        if (!_isMeshListening) return;
        if (_mySessionId == null) return;
        if (string.IsNullOrWhiteSpace(affectedUid)) return;

        owner ??= string.Empty;

        if (!yieldToOtherSync)
            owner = string.Empty;

        var outgoing = new YieldBroadcastStateEntry(yieldToOtherSync, owner);
        if (_lastYieldBroadcastByUid.TryGetValue(affectedUid, out var last)
            && last.Yield == outgoing.Yield
            && string.Equals(last.Owner, outgoing.Owner, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _lastYieldBroadcastByUid[affectedUid] = outgoing;

        var peers = _knownPeers.Keys.ToArray();
        if (peers.Length == 0) return;

        var msg = new RavaYield(
            FromSessionId: _mySessionId,
            FromUid: _ownUser?.UID ?? string.Empty,
            AffectedUid: affectedUid,
            YieldToOtherSync: yieldToOtherSync,
            Owner: owner);

        for (int i = 0; i < peers.Length; i++)
        {
            _ = _mesh.SendAsync(peers[i], msg);
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