using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto.Group;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RavaSync.Services;

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

public sealed partial class ToyBox
{
    private readonly ConcurrentDictionary<Guid, BlackjackHostGame> _hostedBlackjack = new();
    private readonly ConcurrentDictionary<Guid, BlackjackClientView> _clientBlackjack = new();

    public bool TryGetClientBlackjack(Guid gameId, out BlackjackClientView view) => _clientBlackjack.TryGetValue(gameId, out view!);
    public bool TryGetHostedBlackjack(Guid gameId, out BlackjackHostGame game) => _hostedBlackjack.TryGetValue(gameId, out game!);

    public Guid HostBlackjack(string lobbyName = "", int maxPlayers = 0, string? password = null)
    {
        var hostSessionId = GetMySessionId();
        if (string.IsNullOrEmpty(hostSessionId)) return Guid.Empty;

        var hostName = _clientState.LocalPlayer?.Name.TextValue ?? "Host";
        _cachedMySessionId = hostSessionId;
        _cachedMyName = hostName;

        var gameId = Guid.NewGuid();

        string salt = string.Empty;
        string hash = string.Empty;
        if (!string.IsNullOrWhiteSpace(password))
        {
            salt = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(8));
            hash = ToyBox.HashPassword(password, salt);
        }

        var game = new BlackjackHostGame(gameId, hostSessionId, hostName, lobbyName ?? string.Empty, Math.Max(0, maxPlayers), salt, hash);
        _hostedBlackjack[gameId] = game;

        _clientBlackjack[gameId] = BlackjackClientView.FromHost(game);

        BroadcastInviteNearby(gameId, hostSessionId, hostName, SyncshellGameKind.Blackjack,
            0, game.LobbyName, game.CurrentPlayers, game.MaxPlayers, game.PasswordProtected, game.PasswordSalt);

        _logger.LogInformation("Hosted Blackjack {gameId}", gameId);
        return gameId;
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
            if (game.Stage != BjStage.Betting)
                return;

            if (game.Players.Values.Count == 0)
                return;

            if (game.Players.Values.Any(p => !p.BetConfirmed))
                return;

            game.DealInitial();
            PushBlackjackState(game);
        }
    }

    public void HostBlackjackDealerDraw(Guid gameId)
    {
        if (_hostedBlackjack.TryGetValue(gameId, out var game))
        {
            game.DealerDrawOne();
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

    private void HandleJoin(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Blackjack) return;
        if (!_hostedBlackjack.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var join = MessagePackSerializer.Deserialize<SyncshellJoinPayload>(env.Payload);

        if (game.MaxPlayers > 0 && game.CurrentPlayers >= game.MaxPlayers)
        {
            SendJoinDenied(game.HostSessionId, join.PlayerSessionId, env.GameId, env.Kind, "Lobby is full.");
            return;
        }

        if (game.PasswordProtected)
        {
            var invitedOk = !string.IsNullOrEmpty(join.InviteToken) && game.ConsumeInviteToken(join.PlayerSessionId, join.InviteToken);
            var passwordOk = string.Equals(join.PasswordHash ?? string.Empty, game.PasswordHash, StringComparison.Ordinal);

            if (!invitedOk && !passwordOk)
            {
                SendJoinDenied(game.HostSessionId, join.PlayerSessionId, env.GameId, env.Kind, "Wrong password.");
                return;
            }
        }

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

        public bool DealerTurnInProgress { get; private set; }
        public Guid GameId { get; }
        public string HostSessionId { get; }
        public string HostName { get; }

        public string LobbyName { get; }
        public int MaxPlayers { get; }
        public string PasswordSalt { get; }
        public string PasswordHash { get; }
        public bool PasswordProtected => !string.IsNullOrEmpty(PasswordHash);
        public int CurrentPlayers => 1 + Players.Count;

        private readonly ConcurrentDictionary<string, string> _inviteTokens = new(StringComparer.Ordinal);

        public BjStage Stage { get; set; } = BjStage.Lobby;

        public ConcurrentDictionary<string, BlackjackPlayer> Players { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, BjResultPublic> Results { get; } = new(StringComparer.Ordinal);

        public string CurrentTurnSessionId { get; private set; } = string.Empty;

        public List<byte> DealerCards { get; } = [];
        private readonly List<byte> _deck = [];

        public BlackjackHostGame(Guid gameId, string hostSessionId, string hostName, string lobbyName, int maxPlayers, string passwordSalt, string passwordHash)
        {
            GameId = gameId;
            HostSessionId = hostSessionId;
            HostName = hostName;

            LobbyName = lobbyName ?? string.Empty;
            MaxPlayers = Math.Max(0, maxPlayers);
            PasswordSalt = passwordSalt ?? string.Empty;
            PasswordHash = passwordHash ?? string.Empty;
        }

        public string IssueInviteToken(string targetSessionId)
        {
            var token = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(10));
            _inviteTokens[targetSessionId] = token;
            return token;
        }

        public bool ConsumeInviteToken(string targetSessionId, string token)
        {
            if (!_inviteTokens.TryGetValue(targetSessionId, out var stored)) return false;
            if (!string.Equals(stored, token, StringComparison.Ordinal)) return false;

            _inviteTokens.TryRemove(targetSessionId, out _);
            return true;
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
            DealerTurnInProgress = false;
        }

        public void DealInitial()
        {
            Stage = BjStage.Playing;

            BuildDeck();
            ShuffleDeck();

            DealerCards.Clear();
            DealerCards.Add(Draw());
            DealerTurnInProgress = false;

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
            {
                CurrentTurnSessionId = string.Empty;
                DealerTurnInProgress = true;
            }

        }

        public void DealerDrawOne()
        {
            if (Stage != BjStage.Playing) return;
            if (!DealerTurnInProgress) return;

            if (DealerCards.Count == 1)
            {
                DealerCards.Add(Draw());
                return;
            }

            if (HandValue(DealerCards).Total < 17)
            {
                DealerCards.Add(Draw());
                return;
            }

            ResolveDealerAndResults();
            DealerTurnInProgress = false;
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
                Stage = BjStage.Results;
                CurrentTurnSessionId = string.Empty;
                Results.Clear();
                DealerTurnInProgress = false;
                return;
            }

            if (DealerTurnInProgress)
                return; 

            if (string.IsNullOrEmpty(CurrentTurnSessionId) || !Players.TryGetValue(CurrentTurnSessionId, out var cur) || cur.Done)
            {
                var next = ordered.FirstOrDefault(p => !p.Done);
                if (next != null)
                {
                    CurrentTurnSessionId = next.SessionId;
                    return;
                }

                CurrentTurnSessionId = string.Empty;
                DealerTurnInProgress = true;
            }
        }


        private void ResolveDealerAndResults()
        {
            var dealerValue = HandValue(DealerCards).Total;
            bool dealerBust = dealerValue > 21;

            Results.Clear();

            foreach (var p in Players.Values.Where(p => p.BetConfirmed))
            {
                var pv = HandValue(p.Cards).Total;

                BjOutcome outcum;
                int payout;

                if (pv > 21 || p.Bust)
                {
                    outcum = BjOutcome.Bust;
                    payout = 0;
                }
                else if (dealerBust)
                {
                    outcum = BjOutcome.Win;
                    payout = p.Bet * 2;
                }
                else if (pv > dealerValue)
                {
                    outcum = BjOutcome.Win;
                    payout = p.Bet * 2;
                }
                else if (pv == dealerValue)
                {
                    outcum = BjOutcome.Push;
                    payout = p.Bet;
                }
                else
                {
                    outcum = BjOutcome.Lose;
                    payout = 0;
                }

                Results[p.SessionId] = new BjResultPublic(p.SessionId, p.Name, p.Bet, outcum, payout);
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

            if (DealerTurnInProgress)
            {
                return new BjStatePublic(GameId, Stage, HostSessionId, CurrentTurnSessionId, dealerUp, players,
                    DealerCardsReveal: DealerCards.ToList());
            }

            return new BjStatePublic(GameId, Stage, HostSessionId, CurrentTurnSessionId, dealerUp, players);
        }


        public BjStatePrivate BuildPrivateState(string sessionId)
        {
            var dealerUp = DealerCards.Count > 0 ? DealerCards[0] : (byte)0;

            bool isHost = string.Equals(sessionId, HostSessionId, StringComparison.Ordinal);
            byte? hole = null;

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

    public void CloseBJLobby(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_hostedBingo.TryRemove(gameId, out var game) ||
            !string.Equals(game.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            LeaveBlackjack(gameId);
            Invites.TryRemove(gameId, out _);
            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        _clientBingo.TryRemove(gameId, out _);
        Invites.TryRemove(gameId, out _);
        DirectInvites.TryRemove(gameId, out _);

        BroadcastLobbyClosedNearby(gameId, mySessionId, SyncshellGameKind.Blackjack);

        var env = new SyncshellGameEnvelope(gameId, SyncshellGameKind.Blackjack, SyncshellGameOp.LobbyClosed);
        foreach (var sid in game.Players.Keys)
        {
            if (string.Equals(sid, mySessionId, StringComparison.Ordinal)) continue;
            Send(mySessionId, sid, env);
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
