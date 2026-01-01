using Dalamud.Game.ClientState;
using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto.Group;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RavaSync.Services;

public enum PokerStage
{
    Lobby = 0,
    Preflop = 1,
    Flop = 2,
    Turn = 3,
    River = 4,
    Showdown = 5,
    RoundOver = 6
}

public enum PokerActionKind
{
    Fold = 0,
    Check = 1,
    Call = 2,
    Raise = 3,
    AllIn = 4
}

[MessagePackObject(keyAsPropertyName: true)]
public record PokerActionPayload(string PlayerSessionId, PokerActionKind Action, int Amount = 0);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerRevealPayload(string PlayerSessionId, List<byte> HoleCards);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerPlayerPublic(string SessionId, string Name, int Stack, int CommittedThisStreet, int CommittedThisHand, bool Folded, bool AllIn);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerPotPublic(int Amount, List<string> EligibleSessionIds);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerStatePublic(Guid GameId, PokerStage Stage, string HostSessionId, string CurrentTurnSessionId, int DealerButtonIndex, int SmallBlind, int BigBlind, int CurrentBet, int MinRaise, int PotTotal, List<PokerPotPublic> Pots, List<byte> BoardCards, 
    List<PokerPlayerPublic> Players, bool HandInProgress, int TableBuyIn, List<PokerRevealPublic>? Reveals = null, List<PokerHandResultPublic>? LastHandResults = null);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerStatePrivate(Guid GameId, PokerStage Stage, bool IsYourTurn, List<byte> YourHoleCards, List<PokerActionKind> AllowedActions, int CallAmount, int MinRaiseTo, int MaxRaiseTo);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerRevealPublic(string SessionId, List<byte> HoleCards);

[MessagePackObject(keyAsPropertyName: true)]
public record PokerHandResultPublic(string SessionId, string Name, int WonAmount, string HandLabel);


public sealed partial class SyncshellGameService
{
    private readonly ConcurrentDictionary<Guid, PokerHostGame> _hostedPoker = new();
    private readonly ConcurrentDictionary<Guid, PokerClientView> _clientPoker = new();

    public bool TryGetClientPoker(Guid gameId, out PokerClientView view) => _clientPoker.TryGetValue(gameId, out view!);
    public bool TryGetHostedPoker(Guid gameId, out PokerHostGame game) => _hostedPoker.TryGetValue(gameId, out game!);

    public Guid HostPoker(GroupFullInfoDto group, int tableBuyIn, int smallBlind, int bigBlind)
    {
        var hostSessionId = GetMySessionId();
        if (string.IsNullOrEmpty(hostSessionId)) return Guid.Empty;

        var hostName = _clientState.LocalPlayer?.Name.TextValue ?? "Host";
        _cachedMySessionId = hostSessionId;
        _cachedMyName = hostName;

        var gameId = Guid.NewGuid();

        var game = new PokerHostGame(gameId, hostSessionId, hostName);
        game.SetBuyIn(Math.Max(1, tableBuyIn));
        game.SmallBlind = Math.Max(1, smallBlind);
        game.BigBlind = Math.Max(game.SmallBlind, bigBlind);

        game.AddPlayer(hostSessionId, hostName);
        _hostedPoker[gameId] = game;

        _clientPoker[gameId] = PokerClientView.FromHost(game);

        BroadcastInviteNearby(gameId, hostSessionId, hostName, SyncshellGameKind.Poker, game.BuyIn);

        _logger.LogInformation("Hosted Poker {gameId}", gameId);
        return gameId;
    }

    private void HandlePokerReveal(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (!_hostedPoker.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var rev = MessagePackSerializer.Deserialize<PokerRevealPayload>(env.Payload);
        game.ApplyReveal(rev.PlayerSessionId, rev.HoleCards);

        PushPokerState(game);
    }

    public void PokerReveal(Guid gameId, List<byte> holeCards)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_clientPoker.TryGetValue(gameId, out var view)) return;

        var payload = new PokerRevealPayload(mySessionId, holeCards ?? []);
        Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Poker, SyncshellGameOp.PokerReveal,
            MessagePackSerializer.Serialize(payload)));
    }

    public void LeavePoker(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (_hostedPoker.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientPoker.TryRemove(gameId, out _);
            return;
        }

        if (_clientPoker.TryGetValue(gameId, out var view))
        {
            Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Poker, SyncshellGameOp.Leave,
                MessagePackSerializer.Serialize(new SyncshellJoinPayload(mySessionId, view.MyName))));
        }

        _clientPoker.TryRemove(gameId, out _);
    }

    public void PokerAction(Guid gameId, PokerActionKind action, int amount = 0)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_clientPoker.TryGetValue(gameId, out var view)) return;

        var payload = new PokerActionPayload(mySessionId, action, amount);
        Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Poker, SyncshellGameOp.PokerAction,
            MessagePackSerializer.Serialize(payload)));
    }

    private void HandlePokerJoin(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (!_hostedPoker.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var join = MessagePackSerializer.Deserialize<SyncshellJoinPayload>(env.Payload);

        if (string.Equals(join.PlayerSessionId, game.HostSessionId, StringComparison.Ordinal))
            return;

        game.AddPlayer(join.PlayerSessionId, join.PlayerName, join.BuyIn);
        PushPokerState(game);
    }

    private void HandlePokerLeave(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (_hostedPoker.TryGetValue(env.GameId, out var game))
        {
            game.RemovePlayer(fromSessionId);
            PushPokerState(game);
        }
        else
        {
            _clientPoker.TryRemove(env.GameId, out _);
        }
    }

    private void HandlePokerAction(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (!_hostedPoker.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var act = MessagePackSerializer.Deserialize<PokerActionPayload>(env.Payload);
        game.ApplyAction(act.PlayerSessionId, act.Action, act.Amount);

        PushPokerState(game);
    }

    private void HandlePokerPublic(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var pub = MessagePackSerializer.Deserialize<PokerStatePublic>(env.Payload);

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientPoker.AddOrUpdate(env.GameId,
            _ => PokerClientView.FromPublic(pub, mySessionId, myName),
            (_, existing) => existing.WithPublic(pub));
    }

    private void HandlePokerPrivate(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var priv = MessagePackSerializer.Deserialize<PokerStatePrivate>(env.Payload);

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientPoker.AddOrUpdate(env.GameId,
            _ => PokerClientView.FromPrivate(priv, mySessionId, myName),
            (_, existing) =>
            {
                var merged = priv;

                if ((merged.YourHoleCards == null || merged.YourHoleCards.Count == 0) &&
                    existing.Private?.YourHoleCards?.Count > 0 &&
                    merged.Stage != PokerStage.Lobby)
                {
                    merged = merged with { YourHoleCards = existing.Private.YourHoleCards };
                }

                return existing.WithPrivate(merged);
            });
    }

    private void PushPokerState(PokerHostGame game)
    {
        game.AutoAdvanceIfPossible();

        var pub = game.BuildPublicState();
        var pubBytes = MessagePackSerializer.Serialize(pub);
        var pubEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Poker, SyncshellGameOp.PokerStatePublic, pubBytes);

        PokerStatePrivate? hostPriv = null;

        foreach (var p in game.Players.Values)
        {
            Send(game.HostSessionId, p.SessionId, pubEnv);

            var priv = game.BuildPrivateState(p.SessionId);
            var privBytes = MessagePackSerializer.Serialize(priv);
            var privEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Poker, SyncshellGameOp.PokerStatePrivate, privBytes);
            Send(game.HostSessionId, p.SessionId, privEnv);

            if (string.Equals(p.SessionId, game.HostSessionId, StringComparison.Ordinal))
                hostPriv = priv;
        }

        if (_clientPoker.TryGetValue(game.GameId, out var existingHostView) && hostPriv != null)
        {
            if ((hostPriv.YourHoleCards == null || hostPriv.YourHoleCards.Count == 0)
                && existingHostView.Private?.YourHoleCards?.Count > 0
                && hostPriv.Stage != PokerStage.Lobby)
            {
                hostPriv = hostPriv with { YourHoleCards = existingHostView.Private!.YourHoleCards };
            }
        }

        _clientPoker[game.GameId] = new PokerClientView(
            game.GameId,
            game.HostSessionId,
            game.HostName,
            game.HostSessionId,
            game.HostName,
            pub,
            hostPriv);
    }

    public void HostPokerStartHand(Guid gameId, int buyIn)
    {
        if (_hostedPoker.TryGetValue(gameId, out var game))
        {
            game.SetBuyIn(Math.Max(1, buyIn));
            game.StartHand();
            PushPokerState(game);
        }
    }

    public void HostPokerConfigure(Guid gameId, int tableBuyIn, int smallBlind, int bigBlind)
    {
        if (_hostedPoker.TryGetValue(gameId, out var game))
        {
            game.SetBuyIn(Math.Max(1, tableBuyIn));
            game.SmallBlind = Math.Max(1, smallBlind);
            game.BigBlind = Math.Max(game.SmallBlind, bigBlind);

            BroadcastInviteNearby(game.GameId, game.HostSessionId, game.HostName, SyncshellGameKind.Poker, game.BuyIn);

            if (game.Stage == PokerStage.Lobby)
                PushPokerState(game);
        }
    }


    public void HostPokerAdvance(Guid gameId)
    {
        if (_hostedPoker.TryGetValue(gameId, out var game))
        {
            game.AdvanceStreetUnsafe();
            PushPokerState(game);
        }
    }


    public sealed class PokerHostGame
    {
        private readonly Random _rng = new();
        private readonly ConcurrentDictionary<string, List<byte>> _pendingDeal = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, List<byte>> _revealed = new(StringComparer.Ordinal);
        private readonly List<PokerHandResultPublic> _lastHandResults = new();


        public Guid GameId { get; }
        public string HostSessionId { get; }
        public string HostName { get; }

        public PokerStage Stage { get; private set; } = PokerStage.Lobby;

        public int SmallBlind { get; set; } = 100;
        public int BigBlind { get; set; } = 200;

        public ConcurrentDictionary<string, PokerPlayer> Players { get; } = new(StringComparer.Ordinal);

        private readonly List<byte> _deck = [];
        public List<byte> Board { get; } = [];

        public bool HandInProgress { get; private set; } = false;

        public int DealerButtonIndex { get; private set; } = 0;

        public string CurrentTurnSessionId { get; private set; } = string.Empty;

        public int CurrentBet { get; private set; } = 0;
        public int MinRaise { get; private set; } = 0;
        public int BuyIn { get; private set; } = 10000;

        private string _lastAggressor = string.Empty;
        private string _firstToActThisStreet = string.Empty;

        private readonly List<PokerPot> _pots = [];

        public PokerHostGame(Guid gameId, string hostSessionId, string hostName)
        {
            GameId = gameId;
            HostSessionId = hostSessionId;
            HostName = hostName;
        }

        public void AddPlayer(string sessionId, string name)
        {
            Players.TryAdd(sessionId, new PokerPlayer(sessionId, name));

            if (Stage == PokerStage.Lobby)
                Stage = PokerStage.Lobby;
        }

        public void AddPlayer(string sessionId, string name, int buyIn)
        {
            var p = new PokerPlayer(sessionId, name);

            int actualBuyIn = buyIn <= 0 ? BuyIn : buyIn;
            if (actualBuyIn < BuyIn) actualBuyIn = BuyIn;

            p.Stack = actualBuyIn;
            p.HasBoughtIn = true;

            Players.TryAdd(sessionId, p);
        }

        public void RemovePlayer(string sessionId)
        {
            Players.TryRemove(sessionId, out _);
            if (CurrentTurnSessionId == sessionId)
                CurrentTurnSessionId = string.Empty;
        }
        public void SetBuyIn(int buyIn)
        {
            BuyIn = Math.Max(1, buyIn);
        }
        public void StartHand()
        {
            int eligible = Players.Values.Count(p => !p.HasBoughtIn || p.Stack > 0);
            if (eligible < 2) return;

            foreach (var p in Players.Values)
            {
                if (!p.HasBoughtIn && !HandInProgress)
                {
                    p.Stack = BuyIn;
                    p.HasBoughtIn = true;
                }
            }

            HandInProgress = true;

            Board.Clear();
            _pots.Clear();

            BuildDeck();
            ShuffleDeck();

            foreach (var p in Players.Values)
                p.ResetForNewHand();

            // Move dealer button
            DealerButtonIndex = NormalizeIndex(DealerButtonIndex, OrderedPlayers().Count);
            DealerButtonIndex = (DealerButtonIndex + 1) % OrderedPlayers().Count;

            _pendingDeal.Clear();
            _lastHandResults.Clear();
            _revealed.Clear();

            foreach (var p in OrderedPlayers())
            {
                _pendingDeal[p.SessionId] = new List<byte>(2) { Draw(), Draw() };
            }


            // Post blinds
            CurrentBet = 0;
            MinRaise = BigBlind;
            _lastAggressor = string.Empty;

            PostBlinds();

            Stage = PokerStage.Preflop;
            CurrentTurnSessionId = NextToActPreflop();
            _firstToActThisStreet = CurrentTurnSessionId;
        }

        private void PostBlinds()
        {
            var ordered = OrderedPlayers();
            if (ordered.Count < 2) return;

            int sbIndex;
            int bbIndex;

            if (ordered.Count == 2)
            {
                sbIndex = DealerButtonIndex;
                bbIndex = (DealerButtonIndex + 1) % ordered.Count;
            }
            else
            {
                sbIndex = (DealerButtonIndex + 1) % ordered.Count;
                bbIndex = (DealerButtonIndex + 2) % ordered.Count;
            }

            var sb = ordered[sbIndex];
            var bb = ordered[bbIndex];

            ForceCommit(sb, SmallBlind);
            ForceCommit(bb, BigBlind);

            CurrentBet = BigBlind;
            MinRaise = BigBlind;
            _lastAggressor = bb.SessionId;
        }

        private void ForceCommit(PokerPlayer p, int amount)
        {
            if (p.Folded) return;
            int commit = Math.Max(0, Math.Min(amount, p.Stack));
            p.Stack -= commit;
            p.CommittedThisStreet += commit;
            p.CommittedThisHand += commit;
            if (p.Stack == 0) p.AllIn = true;
        }

        public void ApplyAction(string sessionId, PokerActionKind action, int amount)
        {
            if (!HandInProgress) return;
            if (!string.Equals(CurrentTurnSessionId, sessionId, StringComparison.Ordinal)) return;
            if (!Players.TryGetValue(sessionId, out var p)) return;
            if (p.Folded || p.AllIn) return;

            var allowed = BuildAllowedActions(sessionId, out var callAmount, out var minRaiseTo, out var maxRaiseTo);
            if (!allowed.Contains(action))
                return;

            p.ActedThisStreet = true;

            switch (action)
            {
                case PokerActionKind.Fold:
                    p.Folded = true;
                    break;

                case PokerActionKind.Check:
                    break;

                case PokerActionKind.Call:
                    ForceCommit(p, callAmount);
                    break;

                case PokerActionKind.AllIn:
                    ForceCommit(p, p.Stack);
                    if (p.CommittedThisStreet > CurrentBet)
                    {
                        int raiseBy = p.CommittedThisStreet - CurrentBet;
                        CurrentBet = p.CommittedThisStreet;
                        MinRaise = Math.Max(MinRaise, raiseBy);
                        _lastAggressor = p.SessionId;
                    }
                    break;

                case PokerActionKind.Raise:
                    int target = Math.Clamp(amount, minRaiseTo, maxRaiseTo);
                    int need = target - p.CommittedThisStreet;
                    ForceCommit(p, need);

                    int raise = target - CurrentBet;
                    CurrentBet = target;
                    MinRaise = Math.Max(MinRaise, raise);
                    _lastAggressor = p.SessionId;
                    break;
            }

            AdvanceTurn();
        }

        public void AutoAdvanceIfPossible()
        {
            if (!HandInProgress) return;

            var active = OrderedPlayers().Where(x => !x.Folded).ToList();
            if (active.Count == 1)
            {
                AwardToSingle(active[0]);
                return;
            }

            if (string.IsNullOrEmpty(CurrentTurnSessionId))
                CurrentTurnSessionId = FindFirstToActThisStreet();

            if (IsBettingRoundComplete())
            {
                CollectStreetIntoPots();

                if (Stage == PokerStage.Preflop)
                {
                    Burn();
                    Board.Add(Draw());
                    Board.Add(Draw());
                    Board.Add(Draw());
                    Stage = PokerStage.Flop;
                    ResetStreetForNextRound();
                }
                else if (Stage == PokerStage.Flop)
                {
                    Burn();
                    Board.Add(Draw());
                    Stage = PokerStage.Turn;
                    ResetStreetForNextRound();
                }
                else if (Stage == PokerStage.Turn)
                {
                    Burn();
                    Board.Add(Draw());
                    Stage = PokerStage.River;
                    ResetStreetForNextRound();
                }
                else if (Stage == PokerStage.River)
                {
                    Stage = PokerStage.Showdown;

                    CurrentTurnSessionId = string.Empty;
                }

            }
        }

        public void AdvanceStreetUnsafe()
        {
            if (!HandInProgress) return;

            CollectStreetIntoPots();

            if (Stage == PokerStage.Preflop)
            {
                Burn();
                Board.Add(Draw());
                Board.Add(Draw());
                Board.Add(Draw());
                Stage = PokerStage.Flop;
                ResetStreetForNextRound();
                return;
            }

            if (Stage == PokerStage.Flop)
            {
                Burn();
                Board.Add(Draw());
                Stage = PokerStage.Turn;
                ResetStreetForNextRound();
                return;
            }

            if (Stage == PokerStage.Turn)
            {
                Burn();
                Board.Add(Draw());
                Stage = PokerStage.River;
                ResetStreetForNextRound();
                return;
            }

            else if (Stage == PokerStage.River)
            {
                Stage = PokerStage.Showdown;
                CurrentTurnSessionId = string.Empty;
            }

        }

        public void ApplyReveal(string sessionId, List<byte> holeCards)
        {
            if (!HandInProgress) return;
            if (Stage != PokerStage.Showdown) return;

            if (!Players.TryGetValue(sessionId, out var p)) return;
            if (p.Folded) return;

            if (holeCards == null || holeCards.Count != 2) return;

            // store reveal
            _revealed[sessionId] = holeCards;

            // resolve when everyone still in has revealed
            var alive = Players.Values.Where(x => !x.Folded).Select(x => x.SessionId).ToList();
            if (alive.Count > 0 && alive.All(sid => _revealed.ContainsKey(sid)))
            {
                ResolveShowdownUsingReveals();

                Stage = PokerStage.RoundOver;
                HandInProgress = false;
                CurrentTurnSessionId = string.Empty;
            }
        }

        private static string HandLabel(HandRank rank)
        {
            return rank.Category switch
            {
                HandCategory.StraightFlush => "Straight Flush",
                HandCategory.FourKind => "Four of a Kind",
                HandCategory.FullHouse => "Full House",
                HandCategory.Flush => "Flush",
                HandCategory.Straight => "Straight",
                HandCategory.ThreeKind => "Three of a Kind",
                HandCategory.TwoPair => "Two Pair",
                HandCategory.OnePair => "One Pair",
                _ => "High Card"
            };
        }

        private void ResolveShowdownUsingReveals()
        {
            CollectStreetIntoPots();

            var best = new Dictionary<string, HandRank>(StringComparer.Ordinal);

            foreach (var p in Players.Values.Where(p => !p.Folded))
            {
                if (!_revealed.TryGetValue(p.SessionId, out var hole) || hole.Count != 2)
                    continue;

                var seven = new List<byte>(Board.Count + 2);
                seven.AddRange(Board);
                seven.AddRange(hole);

                best[p.SessionId] = PokerHandEvaluator.Evaluate7(seven);
            }

            var won = new Dictionary<string, int>(StringComparer.Ordinal);

            foreach (var pot in _pots)
            {
                if (pot.Eligible.Count == 0) continue;

                var ranked = pot.Eligible
                    .Where(sid => best.ContainsKey(sid))
                    .Select(sid => (sid, rank: best[sid]))
                    .OrderByDescending(x => x.rank)
                    .ToList();

                if (ranked.Count == 0) continue;

                var top = ranked[0].rank;
                var tied = ranked.Where(x => x.rank.Equals(top)).Select(x => x.sid).ToList();

                int share = pot.Amount / tied.Count;
                int remainder = pot.Amount % tied.Count;

                foreach (var sid in tied)
                {
                    if (Players.TryGetValue(sid, out var pl))
                    {
                        pl.Stack += share;
                        won[sid] = won.TryGetValue(sid, out var cur) ? cur + share : share;
                    }
                }

                if (remainder > 0)
                {
                    foreach (var sid in OrderedPlayers().Select(p => p.SessionId))
                    {
                        if (tied.Contains(sid))
                        {
                            Players[sid].Stack += remainder;
                            won[sid] = won.TryGetValue(sid, out var cur) ? cur + remainder : remainder;
                            break;
                        }
                    }
                }
            }

            _lastHandResults.Clear();
            foreach (var kv in won.OrderByDescending(k => k.Value))
            {
                if (Players.TryGetValue(kv.Key, out var pl))
                {
                    var label = best.TryGetValue(kv.Key, out var r) ? HandLabel(r) : "Winner";
                    _lastHandResults.Add(new PokerHandResultPublic(pl.SessionId, pl.Name, kv.Value, label));
                }
            }
        }


        private void AdvanceTurn()
        {
            if (!HandInProgress) return;

            var ordered = OrderedPlayers();
            if (ordered.Count == 0) return;

            int start = ordered.FindIndex(x => x.SessionId == CurrentTurnSessionId);
            for (int i = 1; i <= ordered.Count; i++)
            {
                int idx = (start + i) % ordered.Count;
                var nxt = ordered[idx];
                if (nxt.Folded || nxt.AllIn) continue;
                CurrentTurnSessionId = nxt.SessionId;
                return;
            }

            CurrentTurnSessionId = string.Empty;
        }

        private bool IsBettingRoundComplete()
        {
            var ordered = OrderedPlayers().Where(p => !p.Folded).ToList();
            if (ordered.Count <= 1) return true;

            // Everyone all-in: nothing left to do, just run it out.
            if (ordered.All(p => p.AllIn)) return true;

            if (CurrentBet <= 0)
            {
                foreach (var p in ordered)
                {
                    if (p.AllIn) continue;
                    if (!p.ActedThisStreet) return false;
                }

                return true;
            }

            foreach (var p in ordered)
            {
                if (p.AllIn) continue;
                if (p.CommittedThisStreet != CurrentBet) return false;
            }

            if (string.IsNullOrEmpty(_lastAggressor))
            {
                foreach (var p in ordered)
                {
                    if (p.AllIn) continue;
                    if (!p.ActedThisStreet) return false;
                }

                return true;
            }

            if (Players.TryGetValue(_lastAggressor, out var aggressor) && !aggressor.Folded && !aggressor.AllIn)
            {
                if (!aggressor.ActedThisStreet) return false;
            }

            return string.Equals(CurrentTurnSessionId, _lastAggressor, StringComparison.Ordinal)
                || string.Equals(CurrentTurnSessionId, _firstToActThisStreet, StringComparison.Ordinal);
        }

        private void ResetStreetForNextRound()
        {
            foreach (var p in Players.Values)
            {
                p.CommittedThisStreet = 0;
                p.ActedThisStreet = false;
            }

            CurrentBet = 0;
            MinRaise = BigBlind;
            _lastAggressor = string.Empty;

            CurrentTurnSessionId = FindFirstToActThisStreet();
            _firstToActThisStreet = CurrentTurnSessionId;
        }

        private void CollectStreetIntoPots()
        {
            _pots.Clear();

            var contrib = Players.Values
                .Where(p => p.CommittedThisHand > 0)
                .Select(p => new { p.SessionId, p.CommittedThisHand, Eligible = !p.Folded })
                .OrderBy(x => x.CommittedThisHand)
                .ToList();

            if (contrib.Count == 0) return;

            int prev = 0;
            var remainingEligible = new HashSet<string>(contrib.Where(x => x.Eligible).Select(x => x.SessionId), StringComparer.Ordinal);

            foreach (var c in contrib)
            {
                int level = c.CommittedThisHand;
                int slice = level - prev;
                if (slice <= 0) continue;

                int participants = contrib.Count(x => x.CommittedThisHand >= level);
                int potAmount = slice * participants;

                var eligible = contrib
                    .Where(x => x.Eligible && x.CommittedThisHand >= level)
                    .Select(x => x.SessionId)
                    .ToList();

                _pots.Add(new PokerPot(potAmount, eligible));
                prev = level;
            }
        }

        private void AwardToSingle(PokerPlayer winner)
        {
            CollectStreetIntoPots();
            int total = _pots.Sum(p => p.Amount);
            winner.Stack += total;

            _lastHandResults.Clear();
            _lastHandResults.Add(new PokerHandResultPublic(
                winner.SessionId,
                winner.Name,
                total,
                "Won (everyone folded)"
            ));

            Stage = PokerStage.RoundOver;
            HandInProgress = false;
            CurrentTurnSessionId = string.Empty;
        }



        public PokerStatePublic BuildPublicState()
        {
            var players = OrderedPlayers()
                .Select(p => new PokerPlayerPublic(p.SessionId, p.Name, p.Stack, p.CommittedThisStreet, p.CommittedThisHand, p.Folded, p.AllIn))
                .ToList();

            var pots = _pots.Select(p => new PokerPotPublic(p.Amount, p.Eligible.ToList())).ToList();

            var reveals = (Stage == PokerStage.Showdown || Stage == PokerStage.RoundOver)
                ? _revealed.Select(kvp => new PokerRevealPublic(kvp.Key, kvp.Value.ToList())).ToList()
                : new List<PokerRevealPublic>();

            var results = (Stage == PokerStage.RoundOver)
                ? _lastHandResults.ToList()
                : new List<PokerHandResultPublic>();

            return new PokerStatePublic(
                GameId, Stage, HostSessionId,
                CurrentTurnSessionId,
                DealerButtonIndex,
                SmallBlind, BigBlind,
                CurrentBet, MinRaise,
                PotTotal: pots.Sum(p => p.Amount),
                Pots: pots,
                BoardCards: Board.ToList(),
                Players: players,
                HandInProgress: HandInProgress,
                TableBuyIn: BuyIn,
                Reveals: reveals,
                LastHandResults: results
            );
        }

        public PokerStatePrivate BuildPrivateState(string sessionId)
        {
            bool isYourTurn = HandInProgress && string.Equals(CurrentTurnSessionId, sessionId, StringComparison.Ordinal);

            if (!Players.TryGetValue(sessionId, out var p))
                return new PokerStatePrivate(GameId, Stage, false, [], [], 0, 0, 0);

            // Only include hole cards once
            List<byte> hole = [];
            if (Stage != PokerStage.Lobby && _pendingDeal.TryRemove(sessionId, out var dealt))
                hole = dealt;

            // During showdown, no betting actions
            if (Stage == PokerStage.Showdown || Stage == PokerStage.RoundOver)
                return new PokerStatePrivate(GameId, Stage, false, hole, [], 0, 0, 0);

            var allowed = BuildAllowedActions(sessionId, out var callAmount, out var minRaiseTo, out var maxRaiseTo);

            return new PokerStatePrivate(GameId, Stage, isYourTurn,
                YourHoleCards: hole,
                AllowedActions: allowed,
                CallAmount: callAmount,
                MinRaiseTo: minRaiseTo,
                MaxRaiseTo: maxRaiseTo);
        }


        private List<PokerActionKind> BuildAllowedActions(string sessionId, out int callAmount, out int minRaiseTo, out int maxRaiseTo)
        {
            callAmount = 0;
            minRaiseTo = 0;
            maxRaiseTo = 0;

            if (!HandInProgress) return [];

            if (!Players.TryGetValue(sessionId, out var p)) return [];
            if (p.Folded || p.AllIn) return [];

            callAmount = Math.Max(0, CurrentBet - p.CommittedThisStreet);
            maxRaiseTo = p.CommittedThisStreet + p.Stack;

            var allowed = new List<PokerActionKind>();

            allowed.Add(PokerActionKind.Fold);

            if (callAmount == 0)
                allowed.Add(PokerActionKind.Check);
            else
                allowed.Add(PokerActionKind.Call);

            minRaiseTo = CurrentBet == 0 ? BigBlind : (CurrentBet + MinRaise);

            if (maxRaiseTo > CurrentBet)
            {
                allowed.Add(PokerActionKind.Raise);
                allowed.Add(PokerActionKind.AllIn);
            }
            else if (p.Stack > 0)
            {
                allowed.Add(PokerActionKind.AllIn);
            }

            return allowed;
        }

        private string FindFirstToActThisStreet()
        {
            var ordered = OrderedPlayers();
            if (ordered.Count == 0) return string.Empty;

            int start = (DealerButtonIndex + 1) % ordered.Count;
            for (int i = 0; i < ordered.Count; i++)
            {
                int idx = (start + i) % ordered.Count;
                var p = ordered[idx];
                if (p.Folded || p.AllIn) continue;
                return p.SessionId;
            }

            return string.Empty;
        }

        private string NextToActPreflop()
        {
            var ordered = OrderedPlayers();
            if (ordered.Count < 2) return string.Empty;

            // Heads-up: SB (dealer) acts first preflop.
            if (ordered.Count == 2)
            {
                int sbIndex = DealerButtonIndex;
                var sb = ordered[sbIndex];
                if (!sb.Folded && !sb.AllIn) return sb.SessionId;
            }

            int bbIndex = (DealerButtonIndex + 2) % ordered.Count;
            int start = (bbIndex + 1) % ordered.Count;

            for (int i = 0; i < ordered.Count; i++)
            {
                int idx = (start + i) % ordered.Count;
                var p = ordered[idx];
                if (p.Folded || p.AllIn) continue;
                return p.SessionId;
            }

            return string.Empty;
        }

        private static int NormalizeIndex(int idx, int count)
        {
            if (count <= 0) return 0;
            if (idx < 0) return 0;
            if (idx >= count) return 0;
            return idx;
        }

        private List<PokerPlayer> OrderedPlayers()
        {
            return Players.Values
                .Where(p => p.Stack > 0 || p.CommittedThisHand > 0)
                .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }


        private void Burn()
        {
            _ = Draw();
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
    }

    public sealed record PokerPlayer(string SessionId, string Name)
    {
        public int Stack { get; set; } = 0;
        public bool HasBoughtIn { get; set; } = false;

        public int CommittedThisStreet { get; set; }
        public int CommittedThisHand { get; set; }

        public bool Folded { get; set; }
        public bool AllIn { get; set; }
        public bool ActedThisStreet { get; set; }

        public void ResetForNewHand()
        {
            CommittedThisStreet = 0;
            CommittedThisHand = 0;
            Folded = false;
            AllIn = false;
            ActedThisStreet = false;
        }
    }



    private sealed record PokerPot(int Amount, List<string> Eligible);

    public sealed record PokerClientView(Guid GameId, string HostSessionId, string HostName, string MySessionId, string MyName,
        PokerStatePublic? Public, PokerStatePrivate? Private)
    {
        public static PokerClientView FromHost(PokerHostGame host)
        {
            return new PokerClientView(host.GameId, host.HostSessionId, host.HostName, host.HostSessionId, host.HostName,
                host.BuildPublicState(), host.BuildPrivateState(host.HostSessionId));
        }

        public static PokerClientView FromPublic(PokerStatePublic pub, string mySessionId, string myName)
        {
            return new PokerClientView(pub.GameId, pub.HostSessionId, "Host", mySessionId, myName, pub, null);
        }

        public static PokerClientView FromPrivate(PokerStatePrivate priv, string mySessionId, string myName)
        {
            return new PokerClientView(priv.GameId, "", "Host", mySessionId, myName, null, priv);
        }

        public PokerClientView WithPublic(PokerStatePublic pub) => this with { Public = pub, HostSessionId = pub.HostSessionId };
        public PokerClientView WithPrivate(PokerStatePrivate priv) => this with { Private = priv };
    }

    // ---- Hand evaluation (full 7-card hold'em) ----
    // This evaluator produces a comparable HandRank (category + kickers).
    private static class PokerHandEvaluator
    {
        public static HandRank Evaluate7(List<byte> seven)
        {
            // brute-force best 5-of-7: 21 combos
            HandRank best = default;
            bool hasBest = false;

            for (int a = 0; a < 7; a++)
                for (int b = a + 1; b < 7; b++)
                    for (int c = b + 1; c < 7; c++)
                        for (int d = c + 1; d < 7; d++)
                            for (int e = d + 1; e < 7; e++)
                            {
                                var hand = new byte[] { seven[a], seven[b], seven[c], seven[d], seven[e] };
                                var r = Evaluate5(hand);

                                if (!hasBest || r.CompareTo(best) > 0)
                                {
                                    best = r;
                                    hasBest = true;
                                }
                            }

            return best;
        }

        private static HandRank Evaluate5(byte[] cards)
        {
            int[] r = new int[5];
            int[] s = new int[5];

            for (int i = 0; i < 5; i++)
            {
                int rank = cards[i] % 13;
                r[i] = rank == 0 ? 14 : (rank + 1);
                s[i] = cards[i] / 13;
            }

            Array.Sort(r);
            Array.Reverse(r);

            bool flush = s.All(x => x == s[0]);

            // count ranks
            var groups = r.GroupBy(x => x)
                .Select(g => (Rank: g.Key, Count: g.Count()))
                .OrderByDescending(x => x.Count)
                .ThenByDescending(x => x.Rank)
                .ToList();

            bool straight = IsStraight(r, out int straightHigh);

            if (straight && flush)
                return new HandRank(HandCategory.StraightFlush, [straightHigh]);

            if (groups[0].Count == 4)
            {
                int quad = groups[0].Rank;
                int kicker = groups[1].Rank;
                return new HandRank(HandCategory.FourKind, [quad, kicker]);
            }

            if (groups[0].Count == 3 && groups[1].Count == 2)
                return new HandRank(HandCategory.FullHouse, [groups[0].Rank, groups[1].Rank]);

            if (flush)
                return new HandRank(HandCategory.Flush, r);

            if (straight)
                return new HandRank(HandCategory.Straight, [straightHigh]);

            if (groups[0].Count == 3)
            {
                int trips = groups[0].Rank;
                var kickers = groups.Skip(1).Select(x => x.Rank).OrderByDescending(x => x).ToArray();
                return new HandRank(HandCategory.ThreeKind, [trips, kickers[0], kickers[1]]);
            }

            if (groups[0].Count == 2 && groups[1].Count == 2)
            {
                int highPair = Math.Max(groups[0].Rank, groups[1].Rank);
                int lowPair = Math.Min(groups[0].Rank, groups[1].Rank);
                int kicker = groups.Skip(2).First().Rank;
                return new HandRank(HandCategory.TwoPair, [highPair, lowPair, kicker]);
            }

            if (groups[0].Count == 2)
            {
                int pair = groups[0].Rank;
                var kickers = groups.Skip(1).Select(x => x.Rank).OrderByDescending(x => x).ToArray();
                return new HandRank(HandCategory.OnePair, [pair, kickers[0], kickers[1], kickers[2]]);
            }

            return new HandRank(HandCategory.HighCard, r);
        }

        private static bool IsStraight(int[] ranksDesc, out int high)
        {
            var distinct = ranksDesc.Distinct().ToList();
            if (distinct.Count != 5)
            {
                high = 0;
                return false;
            }

            if (distinct[0] - distinct[4] == 4)
            {
                high = distinct[0];
                return true;
            }
            if (distinct[0] == 14 && distinct[1] == 5 && distinct[2] == 4 && distinct[3] == 3 && distinct[4] == 2)
            {
                high = 5;
                return true;
            }

            high = 0;
            return false;
        }
    }

    private enum HandCategory
    {
        HighCard = 0,
        OnePair = 1,
        TwoPair = 2,
        ThreeKind = 3,
        Straight = 4,
        Flush = 5,
        FullHouse = 6,
        FourKind = 7,
        StraightFlush = 8
    }

    private readonly record struct HandRank(HandCategory Category, int[] Kickers) : IComparable<HandRank>
    {
        public int CompareTo(HandRank other)
        {
            int c = Category.CompareTo(other.Category);
            if (c != 0) return c;

            int n = Math.Min(Kickers.Length, other.Kickers.Length);
            for (int i = 0; i < n; i++)
            {
                int k = Kickers[i].CompareTo(other.Kickers[i]);
                if (k != 0) return k;
            }

            return Kickers.Length.CompareTo(other.Kickers.Length);
        }
    }
}
