using Dalamud.Game.ClientState;
using Dalamud.Game.ClientState.Objects;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin.Services;
using MessagePack;
using Microsoft.Extensions.Hosting;
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
    // NOTE: invite 0 = lobby
    Invite = 0,
    Join = 1,
    Leave = 2,

    JoinDenied = 3,

    DirectInvite = 4,
    LobbyClosed = 5,

    BjPlaceBet = 10,
    BjAction = 11,

    BjStatePublic = 20,
    BjStatePrivate = 21,

    PokerAction = 30,
    PokerReveal = 31,

    PokerStatePublic = 40,
    PokerStatePrivate = 41,

    BingoCallBingo = 50,
    BingoClaimResult = 51,

    BingoStatePublic = 60,
    BingoStatePrivate = 61
}


[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellGameEnvelope(Guid GameId, SyncshellGameKind Kind, SyncshellGameOp Op, byte[]? Payload = null);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellInvitePayload(
    string HostSessionId,
    string HostName,
    SyncshellGameKind Kind,
    int TableBuyIn = 0,
    string LobbyName = "",
    int CurrentPlayers = 1,
    int MaxPlayers = 0,
    bool PasswordProtected = false,
    string PasswordSalt = ""
);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellDirectInvitePayload(
    string HostSessionId,
    string HostName,
    SyncshellGameKind Kind,
    Guid GameId,
    int TableBuyIn = 0,
    string LobbyName = "",
    string InviteToken = ""
);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellJoinPayload(
    string PlayerSessionId,
    string PlayerName,
    int BuyIn = 0,
    string? PasswordHash = null,
    string? InviteToken = null
);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellJoinDeniedPayload(string Reason);

public record SyncshellGameInvite(
    Guid GameId,
    string HostSessionId,
    string HostName,
    SyncshellGameKind Kind,
    int TableBuyIn = 0,
    string LobbyName = "",
    int CurrentPlayers = 1,
    int MaxPlayers = 0,
    bool PasswordProtected = false,
    string PasswordSalt = "",
    long LastSeenTicks = 0
);

public record SyncshellDirectInvite(
    Guid GameId,
    string HostSessionId,
    string HostName,
    SyncshellGameKind Kind,
    int TableBuyIn,
    string LobbyName,
    string InviteToken,
    long ReceivedTicks
);

public record ToyBoxHostedLobbySummary(
    Guid GameId,
    SyncshellGameKind Kind,
    string LobbyName,
    int TableBuyIn,
    int CurrentPlayers,
    int MaxPlayers,
    bool PasswordProtected
);


public sealed partial class ToyBox : DisposableMediatorSubscriberBase, IHostedService
{
    private readonly ILogger<ToyBox> _logger;
    private readonly IRavaMesh _mesh;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IObjectTable _objects;
    private readonly IClientState _clientState;
    private readonly IFramework _framework;

    public ConcurrentDictionary<Guid, SyncshellGameInvite> Invites { get; } = new(); // (UI calls these "Lobbies")
    public ConcurrentDictionary<Guid, SyncshellDirectInvite> DirectInvites { get; } = new();

    private const int LobbyBroadcastIntervalMs = 2500;
    private const int LobbyTtlMs = 9000;
    private const int DirectInviteTtlMs = 120000;

    private long _lastLobbyBroadcastTick = 0;
    private long _lastTick = 0;

    private string _cachedMySessionId = string.Empty;
    private string _cachedMyName = "Player";

    public ToyBox(ILogger<ToyBox> logger, MareMediator mediator,
        IRavaMesh mesh, DalamudUtilService dalamudUtil, IObjectTable objects, IClientState clientState,
        IFramework framework)
        : base(logger, mediator)
    {
        _logger = logger;
        _mesh = mesh;
        _dalamudUtil = dalamudUtil;
        _objects = objects;
        _clientState = clientState;
        _framework = framework;

        Mediator.Subscribe<SyncshellGameMeshMessage>(this, OnGameMesh);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _framework.Update += FrameworkOnUpdate;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _framework.Update -= FrameworkOnUpdate;
        return Task.CompletedTask;
    }

    private void FrameworkOnUpdate(IFramework _)
    {
        var now = Environment.TickCount64;
        if (now - _lastTick < 1000) return;
        _lastTick = now;

        try
        {
            TickLobbiesInternal();
        }
        catch
        {
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }


    public void Join(Guid gameId, int pokerBuyIn = 0, string? passwordPlaintext = null, bool fromDirectInvite = false)
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

        // Direct invite path
        if (fromDirectInvite && DirectInvites.TryGetValue(gameId, out var direct))
        {
            var buyIn = (direct.Kind == SyncshellGameKind.Poker)
                ? Math.Max(Math.Max(0, pokerBuyIn), Math.Max(0, direct.TableBuyIn))
                : 0;

            var join = new SyncshellJoinPayload(mySessionId, myName, buyIn, null, direct.InviteToken);

            Send(mySessionId, direct.HostSessionId, new SyncshellGameEnvelope(
                gameId, direct.Kind, SyncshellGameOp.Join,
                MessagePackSerializer.Serialize(join)));

            if (direct.Kind == SyncshellGameKind.Blackjack)
                _clientBlackjack[gameId] = new BlackjackClientView(gameId, direct.HostSessionId, direct.HostName, mySessionId, myName, null, null);
            else if (direct.Kind == SyncshellGameKind.Poker)
                _clientPoker[gameId] = new PokerClientView(gameId, direct.HostSessionId, direct.HostName, mySessionId, myName, null, null);

            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        // Lobby join path
        if (!Invites.TryGetValue(gameId, out var inv))
            return;

        var buyIn2 = (inv.Kind == SyncshellGameKind.Poker)
            ? Math.Max(Math.Max(0, pokerBuyIn), Math.Max(0, inv.TableBuyIn))
            : 0;

        string? passwordHash = null;
        if (inv.PasswordProtected)
            passwordHash = HashPassword(passwordPlaintext ?? string.Empty, inv.PasswordSalt);

        var join2 = new SyncshellJoinPayload(mySessionId, myName, buyIn2, passwordHash, null);

        Send(mySessionId, inv.HostSessionId, new SyncshellGameEnvelope(
            gameId, inv.Kind, SyncshellGameOp.Join,
            MessagePackSerializer.Serialize(join2)));

        if (inv.Kind == SyncshellGameKind.Blackjack)
            _clientBlackjack[gameId] = new BlackjackClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null);
        else if (inv.Kind == SyncshellGameKind.Poker)
            _clientPoker[gameId] = new PokerClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null);
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

            case SyncshellGameOp.DirectInvite:
                HandleDirectInvite(env);
                break;

            case SyncshellGameOp.JoinDenied:
                HandleJoinDenied(env);
                break;

            case SyncshellGameOp.Join:
                if (env.Kind == SyncshellGameKind.Blackjack) HandleJoin(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Poker) HandlePokerJoin(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Bingo) HandleBingoJoin(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.Leave:
                if (env.Kind == SyncshellGameKind.Blackjack) HandleLeave(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Poker) HandlePokerLeave(msg.FromSessionId, env);
                else if (env.Kind == SyncshellGameKind.Bingo) HandleBingoLeave(msg.FromSessionId, env);
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

            case SyncshellGameOp.BingoCallBingo:
                HandleBingoCallBingo(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BingoClaimResult:
                HandleBingoClaimResult(msg.LocalSessionId, msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BingoStatePublic:
                HandleBingoPublic(msg.LocalSessionId, msg.FromSessionId, env);
                break;

            case SyncshellGameOp.BingoStatePrivate:
                HandleBingoPrivate(msg.LocalSessionId, msg.FromSessionId, env);
                break;
            case SyncshellGameOp.LobbyClosed:
                HandleLobbyClosed(env);
                break;

        }
    }

    private void HandleInvite(SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        SyncshellInvitePayload payload;
        try { payload = MessagePackSerializer.Deserialize<SyncshellInvitePayload>(env.Payload); }
        catch { return; }

        Invites[env.GameId] = new SyncshellGameInvite(
            env.GameId,
            payload.HostSessionId,
            payload.HostName,
            payload.Kind,
            payload.TableBuyIn,
            payload.LobbyName,
            payload.CurrentPlayers,
            payload.MaxPlayers,
            payload.PasswordProtected,
            payload.PasswordSalt,
            Environment.TickCount64);
    }


    private void BroadcastInviteNearby(Guid gameId, string hostSessionId, string hostName, SyncshellGameKind kind,
     int tableBuyIn = 0, string lobbyName = "", int currentPlayers = 1, int maxPlayers = 0,
     bool passwordProtected = false, string passwordSalt = "")
    {
        var payload = new SyncshellInvitePayload(
            hostSessionId,
            hostName,
            kind,
            Math.Max(0, tableBuyIn),
            lobbyName ?? string.Empty,
            Math.Max(1, currentPlayers),
            Math.Max(0, maxPlayers),
            passwordProtected,
            passwordSalt ?? string.Empty);

        Invites[gameId] = new SyncshellGameInvite(
            gameId,
            payload.HostSessionId,
            payload.HostName,
            payload.Kind,
            payload.TableBuyIn,
            payload.LobbyName,
            payload.CurrentPlayers,
            payload.MaxPlayers,
            payload.PasswordProtected,
            payload.PasswordSalt,
            Environment.TickCount64);

        var env = new SyncshellGameEnvelope(gameId, kind, SyncshellGameOp.Invite, MessagePackSerializer.Serialize(payload));
        var bytes = MessagePackSerializer.Serialize(env);

        var local = _clientState.LocalPlayer;
        if (local is null) return;

        var players = _objects.OfType<IPlayerCharacter>()
            .Where(p => p.Address != IntPtr.Zero && p.Address != local.Address)
            .ToArray();

        foreach (var pc in players)
        {
            string ident;
            try
            {
                ident = _dalamudUtil.GetIdentFromGameObject(pc);
            }
            catch
            {
                continue;
            }

            if (string.IsNullOrEmpty(ident)) continue;

            var sessionId = RavaSessionId.FromIdent(ident);
            if (string.IsNullOrEmpty(sessionId)) continue;

            _ = _mesh.SendAsync(sessionId, new RavaGame(hostSessionId, bytes));
        }
    }




    private void Send(string fromSessionId, string targetSessionId, SyncshellGameEnvelope env)
    {
        var bytes = MessagePackSerializer.Serialize(env);
        _ = _mesh.SendAsync(targetSessionId, new RavaGame(fromSessionId, bytes));
    }

    private void SendJoinDenied(string hostSessionId, string targetSessionId, Guid gameId, SyncshellGameKind kind, string reason)
    {
        var payload = new SyncshellJoinDeniedPayload(reason);
        var env = new SyncshellGameEnvelope(gameId, kind, SyncshellGameOp.JoinDenied, MessagePackSerializer.Serialize(payload));
        Send(hostSessionId, targetSessionId, env);
    }


    public IReadOnlyList<ToyBoxHostedLobbySummary> GetHostedLobbies()
    {
        var list = new System.Collections.Generic.List<ToyBoxHostedLobbySummary>(4);

        foreach (var kv in _hostedBlackjack)
        {
            var g = kv.Value;
            list.Add(new ToyBoxHostedLobbySummary(
                g.GameId,
                SyncshellGameKind.Blackjack,
                g.LobbyName,
                0,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected));
        }

        foreach (var kv in _hostedPoker)
        {
            var g = kv.Value;
            list.Add(new ToyBoxHostedLobbySummary(
                g.GameId,
                SyncshellGameKind.Poker,
                g.LobbyName,
                g.BuyIn,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected));
        }
        foreach (var kv in _hostedBingo)
        {
            var g = kv.Value;
            list.Add(new ToyBoxHostedLobbySummary(
                g.GameId,
                SyncshellGameKind.Bingo,
                g.LobbyName,
                0,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected));
        }

        return list;
    }

    public void SendDirectInviteToIdent(Guid gameId, string targetIdent)
    {
        if (string.IsNullOrEmpty(targetIdent)) return;

        // Resolve target session id
        var targetSessionId = RavaSessionId.FromIdent(targetIdent);
        if (string.IsNullOrEmpty(targetSessionId)) return;

        // Find hosted game + issue token
        if (_hostedBlackjack.TryGetValue(gameId, out var bj))
        {
            var token = bj.IssueInviteToken(targetSessionId);

            var payload = new SyncshellDirectInvitePayload(
                bj.HostSessionId, bj.HostName, SyncshellGameKind.Blackjack, bj.GameId,
                0, bj.LobbyName, token);

            var env = new SyncshellGameEnvelope(bj.GameId, SyncshellGameKind.Blackjack, SyncshellGameOp.DirectInvite,
                MessagePackSerializer.Serialize(payload));

            Send(bj.HostSessionId, targetSessionId, env);
            return;
        }

        if (_hostedPoker.TryGetValue(gameId, out var pk))
        {
            var token = pk.IssueInviteToken(targetSessionId);

            var payload = new SyncshellDirectInvitePayload(
                pk.HostSessionId, pk.HostName, SyncshellGameKind.Poker, pk.GameId,
                pk.BuyIn, pk.LobbyName, token);

            var env = new SyncshellGameEnvelope(pk.GameId, SyncshellGameKind.Poker, SyncshellGameOp.DirectInvite,
                MessagePackSerializer.Serialize(payload));

            Send(pk.HostSessionId, targetSessionId, env);
            return;
        }


        if (_hostedBingo.TryGetValue(gameId, out var bg))
        {
            var token = bg.IssueInviteToken(targetSessionId);

            var payload = new SyncshellDirectInvitePayload(
                bg.HostSessionId, bg.HostName, SyncshellGameKind.Bingo, bg.GameId,
                0, bg.LobbyName, token);

            var env = new SyncshellGameEnvelope(bg.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.DirectInvite,
                MessagePackSerializer.Serialize(payload));

            Send(bg.HostSessionId, targetSessionId, env);
        }
    }

    public void AcceptDirectInvite(Guid gameId)
    {
        if (!DirectInvites.TryGetValue(gameId, out var inv))
            return;

        if (inv.Kind == SyncshellGameKind.Bingo)
        {
            JoinBingo(gameId, cardCount: 0, markerColorArgb: 0, passwordPlaintext: null, fromDirectInvite: true);
            return;
        }

        Join(gameId, inv.Kind == SyncshellGameKind.Poker ? inv.TableBuyIn : 0, null, fromDirectInvite: true);
    }


    private void HandleDirectInvite(SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        SyncshellDirectInvitePayload payload;
        try { payload = MessagePackSerializer.Deserialize<SyncshellDirectInvitePayload>(env.Payload); }
        catch { return; }

        DirectInvites[env.GameId] = new SyncshellDirectInvite(
            payload.GameId,
            payload.HostSessionId,
            payload.HostName,
            payload.Kind,
            payload.TableBuyIn,
            payload.LobbyName,
            payload.InviteToken,
            Environment.TickCount64);

        // friendly toast
        Mediator.Publish(new NotificationMessage(
            "Toy Box",
            $"Invite received from {payload.HostName} ({payload.Kind}). Open Toy Box to accept.",
            MareConfiguration.Models.NotificationType.Info,
            TimeSpan.FromSeconds(6)));
    }

    private void HandleJoinDenied(SyncshellGameEnvelope env)
    {
        string reason = "Join denied.";
        if (env.Payload != null)
        {
            try { reason = MessagePackSerializer.Deserialize<SyncshellJoinDeniedPayload>(env.Payload).Reason; }
            catch { /* ignore */ }
        }

        if (env.Kind == SyncshellGameKind.Blackjack)
            _clientBlackjack.TryRemove(env.GameId, out _);
        else if (env.Kind == SyncshellGameKind.Poker)
            _clientPoker.TryRemove(env.GameId, out _);
        else if (env.Kind == SyncshellGameKind.Bingo)
            _clientBingo.TryRemove(env.GameId, out _);

        Mediator.Publish(new NotificationMessage(
            "Toy Box",
            reason,
            MareConfiguration.Models.NotificationType.Warning,
            TimeSpan.FromSeconds(6)));
    }

    public static string HashPassword(string password, string salt)
    {
        if (string.IsNullOrEmpty(password)) return string.Empty;
        if (salt == null) salt = string.Empty;

        var data = System.Text.Encoding.UTF8.GetBytes($"{salt}:{password}");
        var hash = System.Security.Cryptography.SHA256.HashData(data);

        return string.Concat(hash.Select(b => b.ToString("x2")));
    }
    private void HandleLobbyClosed(SyncshellGameEnvelope env)
    {
        Invites.TryRemove(env.GameId, out _);
        DirectInvites.TryRemove(env.GameId, out _);

        if (env.Kind == SyncshellGameKind.Blackjack)
            _clientBlackjack.TryRemove(env.GameId, out _);
        else if (env.Kind == SyncshellGameKind.Poker)
            _clientPoker.TryRemove(env.GameId, out _);
        else if (env.Kind == SyncshellGameKind.Bingo)
            _clientBingo.TryRemove(env.GameId, out _);
    }
    private void BroadcastLobbyClosedNearby(Guid gameId, string hostSessionId, SyncshellGameKind kind)
    {
        Invites.TryRemove(gameId, out _);

        var env = new SyncshellGameEnvelope(gameId, kind, SyncshellGameOp.LobbyClosed);
        var bytes = MessagePackSerializer.Serialize(env);

        var local = _clientState.LocalPlayer;
        if (local is null) return;

        var players = _objects.OfType<IPlayerCharacter>()
            .Where(p => p.Address != IntPtr.Zero && p.Address != local.Address)
            .ToArray();

        foreach (var pc in players)
        {
            string ident;
            try { ident = _dalamudUtil.GetIdentFromGameObject(pc); }
            catch { continue; }

            if (string.IsNullOrEmpty(ident)) continue;

            var sessionId = RavaSessionId.FromIdent(ident);
            if (string.IsNullOrEmpty(sessionId)) continue;

            _ = _mesh.SendAsync(sessionId, new RavaGame(hostSessionId, bytes));
        }
    }
    private void TickLobbiesInternal()
    {
        var now = Environment.TickCount64;

        foreach (var kv in Invites)
        {
            if (now - kv.Value.LastSeenTicks > LobbyTtlMs)
                Invites.TryRemove(kv.Key, out _);
        }

        foreach (var kv in DirectInvites)
        {
            if (now - kv.Value.ReceivedTicks > DirectInviteTtlMs)
                DirectInvites.TryRemove(kv.Key, out _);
        }

        // Broadcast hosted lobbies
        if (now - _lastLobbyBroadcastTick < LobbyBroadcastIntervalMs)
            return;

        _lastLobbyBroadcastTick = now;

        foreach (var kv in _hostedBlackjack)
        {
            var g = kv.Value;

            BroadcastInviteNearby(
                g.GameId,
                g.HostSessionId,
                g.HostName,
                SyncshellGameKind.Blackjack,
                0,
                g.LobbyName,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected,
                g.PasswordSalt);
        }

        foreach (var kv in _hostedPoker)
        {
            var g = kv.Value;

            BroadcastInviteNearby(
                g.GameId,
                g.HostSessionId,
                g.HostName,
                SyncshellGameKind.Poker,
                g.BuyIn,
                g.LobbyName,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected,
                g.PasswordSalt);
        }

        foreach (var kv in _hostedBingo)
        {
            var g = kv.Value;

            BroadcastInviteNearby(
                g.GameId,
                g.HostSessionId,
                g.HostName,
                SyncshellGameKind.Bingo,
                0,
                g.LobbyName,
                g.CurrentPlayers,
                g.MaxPlayers,
                g.PasswordProtected,
                g.PasswordSalt);
        }
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
