using Dalamud.Game.ClientState;
using Dalamud.Game.ClientState.Objects;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto.Group;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mediator;
using System;
using System.Collections.Concurrent;
using System.Linq;

namespace RavaSync.Services;

public enum SyncshellGameKind
{
    Blackjack = 0,
    Poker = 1,
    Bingo = 2
}

public enum SyncshellGameOp
{
    Invite = 0,
    Join = 1,
    Leave = 2,

    BjPlaceBet = 10,
    BjAction = 11,

    BjStatePublic = 20,
    BjStatePrivate = 21,

    PokerAction = 30,
    PokerReveal = 31,

    PokerStatePublic = 40,
    PokerStatePrivate = 41
}

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellGameEnvelope(Guid GameId, SyncshellGameKind Kind, SyncshellGameOp Op, byte[]? Payload = null);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellInvitePayload(string HostSessionId, string HostName, SyncshellGameKind Kind, int TableBuyIn = 0);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellJoinPayload(string PlayerSessionId, string PlayerName, int BuyIn = 0);

public record SyncshellGameInvite(Guid GameId, string HostSessionId, string HostName, SyncshellGameKind Kind, int TableBuyIn = 0);

public sealed partial class SyncshellGameService : DisposableMediatorSubscriberBase
{
    private readonly ILogger<SyncshellGameService> _logger;
    private readonly IRavaMesh _mesh;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IObjectTable _objects;
    private readonly IClientState _clientState;

    public ConcurrentDictionary<Guid, SyncshellGameInvite> Invites { get; } = new();

    private string _cachedMySessionId = string.Empty;
    private string _cachedMyName = "Player";

    public SyncshellGameService(ILogger<SyncshellGameService> logger, MareMediator mediator,
        IRavaMesh mesh, DalamudUtilService dalamudUtil, IObjectTable objects, IClientState clientState)
        : base(logger, mediator)
    {
        _logger = logger;
        _mesh = mesh;
        _dalamudUtil = dalamudUtil;
        _objects = objects;
        _clientState = clientState;

        Mediator.Subscribe<SyncshellGameMeshMessage>(this, OnGameMesh);
    }

    public void Join(Guid gameId, int pokerBuyIn = 0)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        var myName = _clientState.LocalPlayer?.Name.TextValue ?? "Player";
        _cachedMySessionId = mySessionId;
        _cachedMyName = myName;

        if (_hostedBlackjack.TryGetValue(gameId, out var hostBj) &&
            string.Equals(hostBj.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientBlackjack[gameId] = BlackjackClientView.FromHost(hostBj);
            PushBlackjackState(hostBj);
            return;
        }

        if (_hostedPoker.TryGetValue(gameId, out var hostPoker) &&
            string.Equals(hostPoker.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientPoker[gameId] = PokerClientView.FromHost(hostPoker);
            PushPokerState(hostPoker);
            return;
        }

        if (!Invites.TryGetValue(gameId, out var inv))
            return;

        var buyIn = (inv.Kind == SyncshellGameKind.Poker)
            ? Math.Max(Math.Max(0, pokerBuyIn), Math.Max(0, inv.TableBuyIn))
            : 0;

        var join = new SyncshellJoinPayload(mySessionId, myName, buyIn);

        Send(mySessionId, inv.HostSessionId, new SyncshellGameEnvelope(
            gameId, inv.Kind, SyncshellGameOp.Join,
            MessagePackSerializer.Serialize(join)));

        if (inv.Kind == SyncshellGameKind.Blackjack)
        {
            _clientBlackjack[gameId] = new BlackjackClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null);
        }
        else if (inv.Kind == SyncshellGameKind.Poker)
        {
            _clientPoker[gameId] = new PokerClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null);
        }

        Invites.TryRemove(gameId, out _);
    }

    private void OnGameMesh(SyncshellGameMeshMessage msg)
    {
        SyncshellGameEnvelope env;
        try
        {
            env = MessagePackSerializer.Deserialize<SyncshellGameEnvelope>(msg.Payload);
        }
        catch
        {
            return;
        }

        if (!string.IsNullOrEmpty(msg.LocalSessionId))
            _cachedMySessionId = msg.LocalSessionId;

        switch (env.Op)
        {
            case SyncshellGameOp.Invite:
                HandleInvite(env);
                break;

            case SyncshellGameOp.Join:
                if (env.Kind == SyncshellGameKind.Blackjack) HandleJoin(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Poker) HandlePokerJoin(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.Leave:
                if (env.Kind == SyncshellGameKind.Blackjack) HandleLeave(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Poker) HandlePokerLeave(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BjPlaceBet:
                HandleBjBet(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BjAction:
                HandleBjAction(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BjStatePublic:
                HandleBjPublic(msg.LocalSessionId, env);
                break;

            case SyncshellGameOp.BjStatePrivate:
                HandleBjPrivate(msg.LocalSessionId, env);
                break;

            case SyncshellGameOp.PokerAction:
                HandlePokerAction(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.PokerStatePublic:
                HandlePokerPublic(msg.LocalSessionId, env);
                break;

            case SyncshellGameOp.PokerStatePrivate:
                HandlePokerPrivate(msg.LocalSessionId, env);
                break;

            case SyncshellGameOp.PokerReveal:
                HandlePokerReveal(msg.FromSessionId, env);
                break;
        }
    }

    private void HandleInvite(SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        var payload = MessagePackSerializer.Deserialize<SyncshellInvitePayload>(env.Payload);
        Invites[env.GameId] = new SyncshellGameInvite(env.GameId, payload.HostSessionId, payload.HostName, payload.Kind, payload.TableBuyIn);
        _logger.LogInformation("SyncshellGames: received invite {gameId} kind {kind} from {hostName} ({hostSessionId})",
            env.GameId, payload.Kind, payload.HostName, payload.HostSessionId);
    }

    private void BroadcastInviteNearby(Guid gameId, string hostSessionId, string hostName, SyncshellGameKind kind, int tableBuyIn = 0)
    {
        var payload = new SyncshellInvitePayload(hostSessionId, hostName, kind, Math.Max(0, tableBuyIn));
        var env = new SyncshellGameEnvelope(gameId, kind, SyncshellGameOp.Invite, MessagePackSerializer.Serialize(payload));
        var bytes = MessagePackSerializer.Serialize(env);

        var players = _objects.OfType<IPlayerCharacter>()
            .Where(p => p.Address != IntPtr.Zero && _clientState.LocalPlayer is not null && p.Address != _clientState.LocalPlayer.Address)
            .ToArray();

        _logger.LogInformation("SyncshellGames: broadcasting {kind} invite {gameId} from {host} to {count} nearby players",
            kind, gameId, hostSessionId, players.Length);

        foreach (var pc in players)
        {
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
            _ = _mesh.SendAsync(sessionId, new Services.Discovery.RavaGame(hostSessionId, bytes));
        }
    }

    private void Send(string fromSessionId, string targetSessionId, SyncshellGameEnvelope env)
    {
        var bytes = MessagePackSerializer.Serialize(env);
        _ = _mesh.SendAsync(targetSessionId, new Services.Discovery.RavaGame(fromSessionId, bytes));
    }

    private string? GetMySessionId()
    {
        if (!string.IsNullOrEmpty(_cachedMySessionId))
            return _cachedMySessionId;

        if (!_clientState.IsLoggedIn || _clientState.LocalPlayer is null) return null;

        try
        {
            var ident = _dalamudUtil.GetIdentFromGameObject(_clientState.LocalPlayer);
            if (string.IsNullOrEmpty(ident)) return null;

            var sid = RavaSessionId.FromIdent(ident);
            _cachedMySessionId = sid;

            var name = _clientState.LocalPlayer?.Name.TextValue;
            if (!string.IsNullOrEmpty(name))
                _cachedMyName = name;

            return sid;
        }
        catch
        {
            return null;
        }
    }

}
