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
using System.Collections.Generic;
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
    BjStatePrivate = 21
}

public enum BjActionKind
{
    Hit = 0,
    Stick = 1
}

public enum BjStage
{
    Lobby = 0,
    Betting = 1,
    Playing = 2,
    Results = 3
}

public enum BjOutcome
{
    Win = 0,
    Lose = 1,
    Push = 2,
    Bust = 3
}

[MessagePackObject(keyAsPropertyName: true)]
public record BjResultPublic(string SessionId, string Name, int Bet, BjOutcome Outcome, int Payout);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellGameEnvelope(Guid GameId, SyncshellGameKind Kind, SyncshellGameOp Op, byte[]? Payload = null);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellInvitePayload(string HostSessionId, string HostName, SyncshellGameKind Kind);

[MessagePackObject(keyAsPropertyName: true)]
public record SyncshellJoinPayload(string PlayerSessionId, string PlayerName);

[MessagePackObject(keyAsPropertyName: true)]
public record BjBetPayload(string PlayerSessionId, int Bet);

[MessagePackObject(keyAsPropertyName: true)]
public record BjActionPayload(string PlayerSessionId, BjActionKind Action);

[MessagePackObject(keyAsPropertyName: true)]
public record BjPlayerPublic(string SessionId, string Name, int Bet, bool Done, bool Bust, bool BetConfirmed);

[MessagePackObject(keyAsPropertyName: true)]
public record BjStatePublic(Guid GameId, BjStage Stage, string HostSessionId, string CurrentTurnSessionId,
    byte DealerUpCard, List<BjPlayerPublic> Players, List<byte>? DealerCardsReveal = null, int DealerTotalReveal = 0, List<BjResultPublic>? Results = null);

[MessagePackObject(keyAsPropertyName: true)]
public record BjStatePrivate(Guid GameId, BjStage Stage, bool IsYourTurn,
    byte DealerUpCard, byte? DealerHoleCardIfHost, List<byte> YourCards, int YourTotal, bool YourBust);

public record SyncshellGameInvite(Guid GameId, string HostSessionId, string HostName, SyncshellGameKind Kind);

public sealed class SyncshellGameService : DisposableMediatorSubscriberBase
{
    private readonly ILogger<SyncshellGameService> _logger;
    private readonly IRavaMesh _mesh;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IObjectTable _objects;
    private readonly IClientState _clientState;

    public ConcurrentDictionary<Guid, SyncshellGameInvite> Invites { get; } = new();

    private readonly ConcurrentDictionary<Guid, BlackjackHostGame> _hostedBlackjack = new();
    private readonly ConcurrentDictionary<Guid, BlackjackClientView> _clientBlackjack = new();

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

    public bool TryGetClientBlackjack(Guid gameId, out BlackjackClientView view) => _clientBlackjack.TryGetValue(gameId, out view!);
    public bool TryGetHostedBlackjack(Guid gameId, out BlackjackHostGame game) => _hostedBlackjack.TryGetValue(gameId, out game!);

    public Guid HostBlackjack(GroupFullInfoDto group)
    {
        var hostSessionId = GetMySessionId();
        if (string.IsNullOrEmpty(hostSessionId)) return Guid.Empty;

        var hostName = _clientState.LocalPlayer?.Name.TextValue ?? "Host";
        _cachedMySessionId = hostSessionId;
        _cachedMyName = hostName;

        var gameId = Guid.NewGuid();

        var game = new BlackjackHostGame(gameId, hostSessionId, hostName);
        _hostedBlackjack[gameId] = game;

        _clientBlackjack[gameId] = BlackjackClientView.FromHost(game);

        BroadcastInviteNearby(gameId, hostSessionId, hostName, SyncshellGameKind.Blackjack);

        _logger.LogInformation("Hosted Blackjack {gameId}", gameId);
        return gameId;
    }

    public void Join(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        var myName = _clientState.LocalPlayer?.Name.TextValue ?? "Player";
        _cachedMySessionId = mySessionId;
        _cachedMyName = myName;

        if (_hostedBlackjack.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientBlackjack[gameId] = BlackjackClientView.FromHost(hostGame);
            PushBlackjackState(hostGame);
            return;
        }

        if (!Invites.TryGetValue(gameId, out var inv))
            return;

        var join = new SyncshellJoinPayload(mySessionId, myName);

        Send(mySessionId, inv.HostSessionId, new SyncshellGameEnvelope(
            gameId, inv.Kind, SyncshellGameOp.Join,
            MessagePackSerializer.Serialize(join)));

        _clientBlackjack[gameId] = new BlackjackClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null);

        Invites.TryRemove(gameId, out _);
    }

    public void LeaveBlackjack(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (_hostedBlackjack.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientBlackjack.TryRemove(gameId, out _);
            return;
        }

        if (_clientBlackjack.TryGetValue(gameId, out var view))
        {
            Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Blackjack, SyncshellGameOp.Leave,
                MessagePackSerializer.Serialize(new SyncshellJoinPayload(mySessionId, view.MyName))));
        }

        _clientBlackjack.TryRemove(gameId, out _);
    }

    public void BlackjackConfirmBet(Guid gameId, int bet)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_clientBlackjack.TryGetValue(gameId, out var view)) return;

        Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Blackjack, SyncshellGameOp.BjPlaceBet,
            MessagePackSerializer.Serialize(new BjBetPayload(mySessionId, bet))));
    }

    public void BlackjackAction(Guid gameId, BjActionKind action)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_clientBlackjack.TryGetValue(gameId, out var view)) return;

        Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Blackjack, SyncshellGameOp.BjAction,
            MessagePackSerializer.Serialize(new BjActionPayload(mySessionId, action))));
    }

    public void HostBlackjackStartBetting(Guid gameId)
    {
        if (_hostedBlackjack.TryGetValue(gameId, out var game))
        {
            game.Stage = BjStage.Betting;
            game.ClearRound();
            PushBlackjackState(game);
        }
    }

    public void HostBlackjackDealAndPlay(Guid gameId)
    {
        if (_hostedBlackjack.TryGetValue(gameId, out var game))
        {
            game.DealInitial();
            PushBlackjackState(game);
        }
    }

    public void HostBlackjackNextRound(Guid gameId)
    {
        if (_hostedBlackjack.TryGetValue(gameId, out var game))
        {
            game.Stage = BjStage.Betting;
            game.ClearRound();
            PushBlackjackState(game);
        }
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
                HandleJoin(msg.FromSessionId, env);
                break;

            case SyncshellGameOp.Leave:
                HandleLeave(msg.FromSessionId, env);
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
        }
    }

    private void HandleInvite(SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        var payload = MessagePackSerializer.Deserialize<SyncshellInvitePayload>(env.Payload);
        Invites[env.GameId] = new SyncshellGameInvite(env.GameId, payload.HostSessionId, payload.HostName, payload.Kind);
        _logger.LogInformation("SyncshellGames: received invite {gameId} kind {kind} from {hostName} ({hostSessionId})",
            env.GameId, payload.Kind, payload.HostName, payload.HostSessionId);
    }

    private void HandleJoin(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (!_hostedBlackjack.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var join = MessagePackSerializer.Deserialize<SyncshellJoinPayload>(env.Payload);

        if (string.Equals(join.PlayerSessionId, game.HostSessionId, StringComparison.Ordinal))
            return;

        game.AddPlayer(join.PlayerSessionId, join.PlayerName);

        PushBlackjackState(game);
    }

    private void HandleLeave(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;

        if (_hostedBlackjack.TryGetValue(env.GameId, out var game))
        {
            game.RemovePlayer(fromSessionId);
            PushBlackjackState(game);
        }
        else
        {
            _clientBlackjack.TryRemove(env.GameId, out _);
        }
    }

    private void HandleBjBet(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (!_hostedBlackjack.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var bet = MessagePackSerializer.Deserialize<BjBetPayload>(env.Payload);
        game.SetBet(bet.PlayerSessionId, Math.Max(0, bet.Bet));

        PushBlackjackState(game);
    }

    private void HandleBjAction(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (!_hostedBlackjack.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var act = MessagePackSerializer.Deserialize<BjActionPayload>(env.Payload);
        game.ApplyAction(act.PlayerSessionId, act.Action);

        PushBlackjackState(game);
    }

    private void HandleBjPublic(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var pub = MessagePackSerializer.Deserialize<BjStatePublic>(env.Payload);

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientBlackjack.AddOrUpdate(env.GameId,
            _ => BlackjackClientView.FromPublic(pub, mySessionId, myName),
            (_, existing) => existing.WithPublic(pub));
    }

    private void HandleBjPrivate(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var priv = MessagePackSerializer.Deserialize<BjStatePrivate>(env.Payload);

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientBlackjack.AddOrUpdate(env.GameId,
            _ => BlackjackClientView.FromPrivate(priv, mySessionId, myName),
            (_, existing) => existing.WithPrivate(priv));
    }

    private void PushBlackjackState(BlackjackHostGame game)
    {
        game.AutoAdvanceIfPossible();

        var pub = game.BuildPublicState();
        var pubBytes = MessagePackSerializer.Serialize(pub);
        var pubEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Blackjack, SyncshellGameOp.BjStatePublic, pubBytes);

        foreach (var p in game.Players.Values)
        {
            Send(game.HostSessionId, p.SessionId, pubEnv);

            var priv = game.BuildPrivateState(p.SessionId);
            var privBytes = MessagePackSerializer.Serialize(priv);
            var privEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Blackjack, SyncshellGameOp.BjStatePrivate, privBytes);
            Send(game.HostSessionId, p.SessionId, privEnv);
        }

        // Host-only view updated locally
        _clientBlackjack[game.GameId] = BlackjackClientView.FromHost(game);
    }

    private void BroadcastInviteNearby(Guid gameId, string hostSessionId, string hostName, SyncshellGameKind kind)
    {
        var payload = new SyncshellInvitePayload(hostSessionId, hostName, kind);
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

    public sealed record BlackjackPlayer(string SessionId, string Name)
    {
        public int Bet { get; set; }
        public bool BetConfirmed { get; set; }
        public List<byte> Cards { get; } = [];
        public bool Done { get; set; }
        public bool Bust { get; set; }
    }

    public sealed class BlackjackHostGame
    {
        private readonly Random _rng = new();

        public Guid GameId { get; }
        public string HostSessionId { get; }
        public string HostName { get; }

        public BjStage Stage { get; set; } = BjStage.Lobby;

        public ConcurrentDictionary<string, BlackjackPlayer> Players { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, BjResultPublic> Results { get; } = new(StringComparer.Ordinal);

        public string CurrentTurnSessionId { get; private set; } = string.Empty;

        public List<byte> DealerCards { get; } = [];
        private readonly List<byte> _deck = [];

        public BlackjackHostGame(Guid gameId, string hostSessionId, string hostName)
        {
            GameId = gameId;
            HostSessionId = hostSessionId;
            HostName = hostName;
        }

        public void AddPlayer(string sessionId, string name)
        {
            Players.TryAdd(sessionId, new BlackjackPlayer(sessionId, name));

            if (Stage == BjStage.Lobby)
                Stage = BjStage.Betting;
        }

        public void RemovePlayer(string sessionId)
        {
            Players.TryRemove(sessionId, out _);

            if (CurrentTurnSessionId == sessionId)
                CurrentTurnSessionId = string.Empty;
        }

        public void SetBet(string sessionId, int bet)
        {
            if (Stage != BjStage.Betting) return;

            if (Players.TryGetValue(sessionId, out var p))
            {
                p.Bet = bet;
                p.BetConfirmed = true;
            }
        }

        public void ClearRound()
        {
            DealerCards.Clear();
            CurrentTurnSessionId = string.Empty;

            foreach (var p in Players.Values)
            {
                p.Cards.Clear();
                p.Done = false;
                p.Bust = false;
                p.BetConfirmed = false;
            }

            Results.Clear();
        }

        public void DealInitial()
        {
            Stage = BjStage.Playing;

            BuildDeck();
            ShuffleDeck();

            DealerCards.Clear();
            DealerCards.Add(Draw());
            DealerCards.Add(Draw());

            foreach (var p in Players.Values)
            {
                if (!p.BetConfirmed)
                {
                    p.Done = true;
                    p.Bust = false;
                    p.Cards.Clear();
                }
            }

            foreach (var p in Players.Values.Where(p => p.BetConfirmed).OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase))
            {
                p.Cards.Clear();
                p.Cards.Add(Draw());
                p.Cards.Add(Draw());
                p.Done = false;
                p.Bust = false;

                if (HandValue(p.Cards).Total == 21)
                    p.Done = true;
            }

            CurrentTurnSessionId = Players.Values
                .Where(p => p.BetConfirmed)
                .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault(p => !p.Done)?.SessionId ?? string.Empty;

            if (string.IsNullOrEmpty(CurrentTurnSessionId))
                ResolveDealerAndResults();
        }

        public void ApplyAction(string sessionId, BjActionKind action)
        {
            if (Stage != BjStage.Playing) return;
            if (!string.Equals(CurrentTurnSessionId, sessionId, StringComparison.Ordinal)) return;
            if (!Players.TryGetValue(sessionId, out var p)) return;
            if (p.Done) return;

            if (action == BjActionKind.Hit)
            {
                p.Cards.Add(Draw());

                var v = HandValue(p.Cards);
                if (v.Total > 21)
                {
                    p.Bust = true;
                    p.Done = true;
                }
                else if (v.Total == 21)
                {
                    p.Done = true;
                }
            }
            else if (action == BjActionKind.Stick)
            {
                p.Done = true;
            }
        }

        public void AutoAdvanceIfPossible()
        {
            if (Stage != BjStage.Playing) return;

            var ordered = Players.Values
                .Where(p => p.BetConfirmed)
                .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (ordered.Count == 0)
            {
                ResolveDealerAndResults();
                return;
            }

            if (string.IsNullOrEmpty(CurrentTurnSessionId) || !Players.TryGetValue(CurrentTurnSessionId, out var cur) || cur.Done)
            {
                var next = ordered.FirstOrDefault(p => !p.Done);
                if (next != null)
                {
                    CurrentTurnSessionId = next.SessionId;
                    return;
                }

                ResolveDealerAndResults();
            }
        }

        private void ResolveDealerAndResults()
        {
            while (HandValue(DealerCards).Total < 17)
                DealerCards.Add(Draw());

            var dealerValue = HandValue(DealerCards).Total;
            bool dealerBust = dealerValue > 21;

            Results.Clear();

            foreach (var p in Players.Values.Where(p => p.BetConfirmed))
            {
                var pv = HandValue(p.Cards).Total;

                BjOutcome outcome;
                int payout;

                if (pv > 21 || p.Bust)
                {
                    outcome = BjOutcome.Bust;
                    payout = 0;
                }
                else if (dealerBust)
                {
                    outcome = BjOutcome.Win;
                    payout = p.Bet * 2;
                }
                else if (pv > dealerValue)
                {
                    outcome = BjOutcome.Win;
                    payout = p.Bet * 2;
                }
                else if (pv == dealerValue)
                {
                    outcome = BjOutcome.Push;
                    payout = p.Bet;
                }
                else
                {
                    outcome = BjOutcome.Lose;
                    payout = 0;
                }

                Results[p.SessionId] = new BjResultPublic(p.SessionId, p.Name, p.Bet, outcome, payout);
            }

            Stage = BjStage.Results;
            CurrentTurnSessionId = string.Empty;
        }

        public BjStatePublic BuildPublicState()
        {
            var dealerUp = DealerCards.Count > 0 ? DealerCards[0] : (byte)0;

            var players = Players.Values
                .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .Select(p => new BjPlayerPublic(p.SessionId, p.Name, p.Bet, p.Done, p.Bust, p.BetConfirmed))
                .ToList();

            if (Stage == BjStage.Results)
            {
                var dealerTotal = HandValue(DealerCards).Total;

                var results = Results.Values
                    .OrderBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                return new BjStatePublic(GameId, Stage, HostSessionId, CurrentTurnSessionId, dealerUp, players,
                    DealerCardsReveal: DealerCards.ToList(), DealerTotalReveal: dealerTotal, Results: results);
            }

            return new BjStatePublic(GameId, Stage, HostSessionId, CurrentTurnSessionId, dealerUp, players);
        }

        public BjStatePrivate BuildPrivateState(string sessionId)
        {
            var dealerUp = DealerCards.Count > 0 ? DealerCards[0] : (byte)0;

            bool isHost = string.Equals(sessionId, HostSessionId, StringComparison.Ordinal);
            byte? hole = isHost && DealerCards.Count > 1 ? DealerCards[1] : null;

            if (!Players.TryGetValue(sessionId, out var p))
                return new BjStatePrivate(GameId, Stage, false, dealerUp, hole, [], 0, false);

            var hv = HandValue(p.Cards);
            return new BjStatePrivate(GameId, Stage,
                IsYourTurn: Stage == BjStage.Playing && string.Equals(CurrentTurnSessionId, sessionId, StringComparison.Ordinal),
                DealerUpCard: dealerUp,
                DealerHoleCardIfHost: hole,
                YourCards: p.Cards.ToList(),
                YourTotal: hv.Total,
                YourBust: hv.Total > 21);
        }

        private void BuildDeck()
        {
            _deck.Clear();
            for (byte i = 0; i < 52; i++)
                _deck.Add(i);
        }

        private void ShuffleDeck()
        {
            for (int i = _deck.Count - 1; i > 0; i--)
            {
                int j = _rng.Next(i + 1);
                (_deck[i], _deck[j]) = (_deck[j], _deck[i]);
            }
        }

        private byte Draw()
        {
            if (_deck.Count == 0)
            {
                BuildDeck();
                ShuffleDeck();
            }

            var c = _deck[^1];
            _deck.RemoveAt(_deck.Count - 1);
            return c;
        }

        public static (int Total, bool Soft) HandValue(List<byte> cards)
        {
            int total = 0;
            int aces = 0;

            foreach (var c in cards)
            {
                int rank = c % 13;
                int v;

                if (rank == 0) { v = 11; aces++; }
                else if (rank >= 10) v = 10;
                else v = rank + 1;

                total += v;
            }

            while (total > 21 && aces > 0)
            {
                total -= 10;
                aces--;
            }

            bool soft = cards.Any(c => (c % 13) == 0) && total <= 21;
            return (total, soft);
        }
    }

    public sealed record BlackjackClientView(Guid GameId, string HostSessionId, string HostName, string MySessionId, string MyName,
        BjStatePublic? Public, BjStatePrivate? Private)
    {
        public static BlackjackClientView FromHost(BlackjackHostGame host)
        {
            return new BlackjackClientView(host.GameId, host.HostSessionId, host.HostName, host.HostSessionId, host.HostName,
                host.BuildPublicState(), host.BuildPrivateState(host.HostSessionId));
        }

        public static BlackjackClientView FromPublic(BjStatePublic pub, string mySessionId, string myName)
        {
            return new BlackjackClientView(pub.GameId, pub.HostSessionId, "Host", mySessionId, myName, pub, null);
        }

        public static BlackjackClientView FromPrivate(BjStatePrivate priv, string mySessionId, string myName)
        {
            return new BlackjackClientView(priv.GameId, "", "Host", mySessionId, myName, null, priv);
        }

        public BlackjackClientView WithPublic(BjStatePublic pub) => this with { Public = pub, HostSessionId = pub.HostSessionId };
        public BlackjackClientView WithPrivate(BjStatePrivate priv) => this with { Private = priv };
    }
}
