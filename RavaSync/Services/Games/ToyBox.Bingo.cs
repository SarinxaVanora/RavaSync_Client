using Dalamud.Game.ClientState;
using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.Services.Mediator;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RavaSync.Services;

public enum BingoPhase
{
    OneLine = 0,
    TwoLines = 1,
    FullHouse = 2
}

[MessagePackObject(keyAsPropertyName: true)]
public record BingoJoinPayload(
    string PlayerSessionId,
    string PlayerName,
    int CardCount,
    int MarkerColorArgb,
    string? PasswordHash = null,
    string? InviteToken = null);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoCallBingoPayload(string PlayerSessionId, BingoPhase Phase);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoClaimResultPayload(
    string PlayerSessionId,
    BingoPhase Phase,
    bool Approved,
    string Message,
    int CardIndex = -1,
    List<int>? MatchedIndices = null);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoCard(List<int> Numbers);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoPlayerPublic(string SessionId, string Name, int CardCount, bool HasPendingClaim);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoClaimPublic(string SessionId, string Name, BingoPhase Phase, long ClaimedTicks);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoWinnerPublic(string SessionId, string Name, BingoPhase Phase, int CardIndex, int WonAmount);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoClaimPreview(int CardIndex, BingoCard Card, int MarkerColorArgb, List<int> MatchedIndices);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoStatePublic(
    Guid GameId,
    string HostSessionId,
    string HostName,
    string LobbyName,
    BingoPhase Phase,
    int PotOneLine,
    int PotTwoLines,
    int PotFullHouse,
    int CurrentNumber,
    List<int> CalledNumbers,
    List<BingoPlayerPublic> Players,
    List<BingoClaimPublic> PendingClaims,
    List<BingoWinnerPublic> Winners);

[MessagePackObject(keyAsPropertyName: true)]
public record BingoStatePrivate(
    Guid GameId,
    BingoPhase Phase,
    int MarkerColorArgb,
    List<BingoCard> YourCards);

public sealed partial class ToyBox
{
    private readonly ConcurrentDictionary<Guid, BingoHostGame> _hostedBingo = new();
    private readonly ConcurrentDictionary<Guid, BingoClientView> _clientBingo = new();

    public bool TryGetClientBingo(Guid gameId, out BingoClientView view) => _clientBingo.TryGetValue(gameId, out view!);
    public bool TryGetHostedBingo(Guid gameId, out BingoHostGame game) => _hostedBingo.TryGetValue(gameId, out game!);

    public void SetLocalBingoMarker(Guid gameId, int markerColorArgb)
    {
        if (markerColorArgb == 0)
            markerColorArgb = unchecked((int)0xFFAA55FF);

        _clientBingo.AddOrUpdate(gameId,
            _ => new BingoClientView(
                gameId,
                "",
                "Host",
                _cachedMySessionId ?? "",
                _cachedMyName ?? "Player",
                null,
                new BingoStatePrivate(gameId, BingoPhase.OneLine, markerColorArgb, []),
                null),
            (_, existing) =>
            {
                var priv = existing.Private;
                if (priv == null)
                    return existing with { Private = new BingoStatePrivate(gameId, BingoPhase.OneLine, markerColorArgb, []) };

                if (priv.MarkerColorArgb == markerColorArgb) return existing;
                return existing with { Private = priv with { MarkerColorArgb = markerColorArgb } };
            });
    }

    public Guid HostBingo(string lobbyName = "", int maxPlayers = 0, string? password = null)
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

        var game = new BingoHostGame(gameId, hostSessionId, hostName, lobbyName ?? string.Empty, Math.Max(0, maxPlayers), salt, hash);

        _hostedBingo[gameId] = game;
        _clientBingo[gameId] = BingoClientView.FromHost(game);

        BroadcastInviteNearby(gameId, hostSessionId, hostName, SyncshellGameKind.Bingo,
            0, game.LobbyName, game.CurrentPlayers, game.MaxPlayers, game.PasswordProtected, game.PasswordSalt);

        _logger.LogInformation("Hosted Bingo {gameId}", gameId);
        return gameId;
    }

    public void JoinBingo(Guid gameId, int cardCount, int markerColorArgb, string? passwordPlaintext = null, bool fromDirectInvite = false)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        var myName = _clientState.LocalPlayer?.Name.TextValue ?? "Player";
        _cachedMySessionId = mySessionId;
        _cachedMyName = myName;

        cardCount = Math.Clamp(cardCount, 1, 10);
        if (markerColorArgb == 0) markerColorArgb = unchecked((int)0xFFAA55FF);

        if (_hostedBingo.TryGetValue(gameId, out var hostBingo) &&
            string.Equals(hostBingo.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientBingo[gameId] = BingoClientView.FromHost(hostBingo);
            PushBingoState(hostBingo);
            return;
        }

        // Direct invite path
        if (fromDirectInvite && DirectInvites.TryGetValue(gameId, out var direct))
        {
            var join = new BingoJoinPayload(mySessionId, myName, cardCount, markerColorArgb, null, direct.InviteToken);

            Send(mySessionId, direct.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Bingo, SyncshellGameOp.Join,
                MessagePackSerializer.Serialize(join)));

            _clientBingo[gameId] = new BingoClientView(gameId, direct.HostSessionId, direct.HostName, mySessionId, myName, null, null, null);
            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        // Lobby join path
        if (!Invites.TryGetValue(gameId, out var inv))
            return;

        string? passwordHash = null;
        if (inv.PasswordProtected)
            passwordHash = HashPassword(passwordPlaintext ?? string.Empty, inv.PasswordSalt);

        var join2 = new BingoJoinPayload(mySessionId, myName, cardCount, markerColorArgb, passwordHash, null);

        Send(mySessionId, inv.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Bingo, SyncshellGameOp.Join,
            MessagePackSerializer.Serialize(join2)));

        _clientBingo[gameId] = new BingoClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, null, null, null);
    }

    public void LeaveBingo(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (_hostedBingo.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientBingo.TryRemove(gameId, out _);
            return;
        }

        if (_clientBingo.TryGetValue(gameId, out var view))
        {
            Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Bingo, SyncshellGameOp.Leave,
                MessagePackSerializer.Serialize(new SyncshellJoinPayload(mySessionId, view.MyName))));
        }

        _clientBingo.TryRemove(gameId, out _);
    }

    public void HostBingoSetPhase(Guid gameId, BingoPhase phase)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            game.SetPhase(phase);
            PushBingoState(game);
        }
    }

    public void HostBingoCallNumber(Guid gameId, int number)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            if (game.CallNumber(number))
                PushBingoState(game);
        }
    }

    public void HostBingoRollRandom(Guid gameId)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            if (game.RollRandomNumber())
                PushBingoState(game);
        }
    }

    public void HostBingoSetPots(Guid gameId, int potOneLine, int potTwoLines, int potFullHouse)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            game.SetPots(potOneLine, potTwoLines, potFullHouse);
            PushBingoState(game);
        }
    }

    public void HostBingoSetPlayerCards(Guid gameId, string playerSessionId, int cardCount)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            if (game.SetPlayerCardCount(playerSessionId, cardCount))
                PushBingoState(game);
        }
    }

    public void HostBingoAdvancePhase(Guid gameId)
    {
        if (_hostedBingo.TryGetValue(gameId, out var game))
        {
            if (game.AdvancePhase())
                PushBingoState(game);
        }
    }

    public void BingoCallBingo(Guid gameId, BingoPhase phase)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_clientBingo.TryGetValue(gameId, out var view)) return;

        var payload = new BingoCallBingoPayload(mySessionId, phase);
        Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoCallBingo,
            MessagePackSerializer.Serialize(payload)));
    }

    public void HostBingoApproveClaim(Guid gameId, string playerSessionId, BingoPhase phase)
    {
        if (!_hostedBingo.TryGetValue(gameId, out var game)) return;

        if (!game.PendingClaims.TryGetValue(playerSessionId, out var claim)) return;
        if (claim.Phase != phase) phase = claim.Phase;

        if (game.TryGetEligibleCard(playerSessionId, phase, out var cardIndex, out var matched))
        {
            game.PendingClaims.TryRemove(playerSessionId, out _);

            int won = game.GetPotForPhase(phase);
            game.Winners.Add(new BingoWinnerPublic(playerSessionId, claim.Name, phase, cardIndex, won));

            var msg = won > 0 ? $"Bingo approved! You won {won}." : "Bingo approved!";
            Send(game.HostSessionId, playerSessionId, new SyncshellGameEnvelope(
                game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoClaimResult,
                MessagePackSerializer.Serialize(new BingoClaimResultPayload(playerSessionId, phase, true, msg, cardIndex, matched))));

            PushBingoState(game);
            return;
        }

        game.PendingClaims.TryRemove(playerSessionId, out _);

        Send(game.HostSessionId, playerSessionId, new SyncshellGameEnvelope(
            game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoClaimResult,
            MessagePackSerializer.Serialize(new BingoClaimResultPayload(playerSessionId, phase, false, "Claim not valid (no eligible card)."))));

        PushBingoState(game);
    }

    public void HostBingoDenyClaim(Guid gameId, string playerSessionId, string reason = "Claim denied.")
    {
        if (!_hostedBingo.TryGetValue(gameId, out var game)) return;

        if (!game.PendingClaims.TryRemove(playerSessionId, out var claim)) return;

        Send(game.HostSessionId, playerSessionId, new SyncshellGameEnvelope(
            game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoClaimResult,
            MessagePackSerializer.Serialize(new BingoClaimResultPayload(playerSessionId, claim.Phase, false, reason))));

        PushBingoState(game);
    }

    private void HandleBingoJoin(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (!_hostedBingo.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var join = MessagePackSerializer.Deserialize<BingoJoinPayload>(env.Payload);

        if (game.MaxPlayers > 0 && game.CurrentPlayers >= game.MaxPlayers)
        {
            SendJoinDenied(game.HostSessionId, join.PlayerSessionId, env.GameId, env.Kind, "Lobby is full.");
            return;
        }

        // Password check
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

        game.AddPlayer(join.PlayerSessionId, join.PlayerName, join.CardCount, join.MarkerColorArgb);
        PushBingoState(game);
    }

    private void HandleBingoLeave(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (_hostedBingo.TryGetValue(env.GameId, out var game))
        {
            game.RemovePlayer(fromSessionId);
            PushBingoState(game);
        }
        else
        {
            _clientBingo.TryRemove(env.GameId, out _);
        }
    }

    private void HandleBingoCallBingo(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (!_hostedBingo.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        var payload = MessagePackSerializer.Deserialize<BingoCallBingoPayload>(env.Payload);

        if (!string.Equals(payload.PlayerSessionId, fromSessionId, StringComparison.Ordinal))
            return;

        if (!game.Players.TryGetValue(payload.PlayerSessionId, out var p))
            return;

        // Enforce sequential phases: players can only claim the current phase.
        if (payload.Phase != game.Phase)
        {
            Send(game.HostSessionId, payload.PlayerSessionId, new SyncshellGameEnvelope(
                game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoClaimResult,
                MessagePackSerializer.Serialize(new BingoClaimResultPayload(payload.PlayerSessionId, payload.Phase, false,
                    $"This game is currently on {payload.Phase}. Claim that phase first."))));
            return;
        }

        game.PendingClaims[payload.PlayerSessionId] = new BingoClaimPublic(payload.PlayerSessionId, p.Name, payload.Phase, Environment.TickCount64);
        PushBingoState(game);
    }

    private void HandleBingoPublic(string localSessionId, string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var pub = MessagePackSerializer.Deserialize<BingoStatePublic>(env.Payload);
        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientBingo.AddOrUpdate(env.GameId,
            _ => BingoClientView.FromPublic(pub, mySessionId, myName),
            (_, existing) => existing.WithPublic(pub));
    }

    private void HandleBingoPrivate(string localSessionId, string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var privIncoming = MessagePackSerializer.Deserialize<BingoStatePrivate>(env.Payload);

        _clientBingo.AddOrUpdate(env.GameId,
            _ => new BingoClientView(env.GameId, fromSessionId ?? string.Empty, "Host", _cachedMySessionId, _cachedMyName, null, privIncoming, null),
            (_, existing) =>
            {
                var existingPriv = existing.Private;
                var merged = privIncoming;
                if (existingPriv != null &&
                    existingPriv.MarkerColorArgb != 0 &&
                    existingPriv.MarkerColorArgb != privIncoming.MarkerColorArgb)
                {
                    merged = privIncoming with { MarkerColorArgb = existingPriv.MarkerColorArgb };
                }

                return existing.WithPrivate(merged).WithHostIfMissing(fromSessionId);
            });
    }

    private void HandleBingoClaimResult(string localSessionId, string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Payload is null) return;

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var res = MessagePackSerializer.Deserialize<BingoClaimResultPayload>(env.Payload);
        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        _clientBingo.AddOrUpdate(env.GameId,
            _ => new BingoClientView(env.GameId, fromSessionId ?? string.Empty, "Host", mySessionId, myName, null, null, res),
            (_, existing) => existing.WithClaimResult(res).WithHostIfMissing(fromSessionId));

        var title = res.Approved ? "Bingo!" : "Bingo (denied)";
        Mediator.Publish(new NotificationMessage(
            "Toy Box",
            res.Message ?? title,
            res.Approved ? MareConfiguration.Models.NotificationType.Info : MareConfiguration.Models.NotificationType.Warning,
            TimeSpan.FromSeconds(6)));
    }

    private void PushBingoState(BingoHostGame game)
    {
        var pub = game.BuildPublicState();
        var pubBytes = MessagePackSerializer.Serialize(pub);
        var pubEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoStatePublic, pubBytes);

        foreach (var p in game.Players.Values)
        {
            Send(game.HostSessionId, p.SessionId, pubEnv);

            var priv = game.BuildPrivateState(p.SessionId);
            var privBytes = MessagePackSerializer.Serialize(priv);
            var privEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Bingo, SyncshellGameOp.BingoStatePrivate, privBytes);
            Send(game.HostSessionId, p.SessionId, privEnv);
        }

        // Keep host view in sync without treating the host as a player.
        _clientBingo[game.GameId] = BingoClientView.FromHost(game);
    }

    public sealed record BingoClientView(
            Guid GameId,
            string HostSessionId,
            string HostName,
            string MySessionId,
            string MyName,
            BingoStatePublic? Public,
            BingoStatePrivate? Private,
            BingoClaimResultPayload? LastClaimResult)
    {
        public static BingoClientView FromHost(BingoHostGame host)
        {
            return new BingoClientView(host.GameId, host.HostSessionId, host.HostName, host.HostSessionId, host.HostName,
                host.BuildPublicState(), host.BuildPrivateState(host.HostSessionId), null);
        }

        public static BingoClientView FromPublic(BingoStatePublic pub, string mySessionId, string myName)
        {
            return new BingoClientView(pub.GameId, pub.HostSessionId, pub.HostName, mySessionId, myName, pub, null, null);
        }

        public static BingoClientView FromPrivate(BingoStatePrivate priv, string mySessionId, string myName)
        {
            return new BingoClientView(priv.GameId, "", "Host", mySessionId, myName, null, priv, null);
        }

        public BingoClientView WithHostIfMissing(string fromSessionId)
        {
            if (string.IsNullOrEmpty(fromSessionId)) return this;
            if (!string.IsNullOrEmpty(HostSessionId)) return this;
            return this with { HostSessionId = fromSessionId };
        }

        public BingoClientView WithPublic(BingoStatePublic pub) => this with { Public = pub, HostSessionId = pub.HostSessionId, HostName = pub.HostName };
        public BingoClientView WithPrivate(BingoStatePrivate priv) => this with { Private = priv };
        public BingoClientView WithClaimResult(BingoClaimResultPayload res) => this with { LastClaimResult = res };
    }

    public sealed class BingoHostGame
    {
        private readonly Random _rng = new();
        private readonly ConcurrentDictionary<string, string> _inviteTokens = new(StringComparer.Ordinal);
        private readonly HashSet<int> _called = new();

        public Guid GameId { get; }
        public string HostSessionId { get; }
        public string HostName { get; }
        public string LobbyName { get; }
        public int MaxPlayers { get; }
        public string PasswordSalt { get; }
        public string PasswordHash { get; }
        public bool PasswordProtected => !string.IsNullOrEmpty(PasswordHash);
        public int CurrentPlayers => 1 + Players.Count;

        public BingoPhase Phase { get; private set; } = BingoPhase.OneLine;
        public int CurrentNumber { get; private set; } = 0;

        public int PotOneLine { get; private set; } = 0;
        public int PotTwoLines { get; private set; } = 0;
        public int PotFullHouse { get; private set; } = 0;

        public List<int> CalledNumbers { get; } = [];

        public ConcurrentDictionary<string, BingoHostPlayer> Players { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, BingoClaimPublic> PendingClaims { get; } = new(StringComparer.Ordinal);
        public List<BingoWinnerPublic> Winners { get; } = [];

        public BingoHostGame(Guid gameId, string hostSessionId, string hostName, string lobbyName, int maxPlayers, string passwordSalt, string passwordHash)
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

        public void SetPhase(BingoPhase phase)
        {
            if (Phase != phase)
            {
                Phase = phase;
                PendingClaims.Clear();
            }
        }

        public bool AdvancePhase()
        {
            if (Phase == BingoPhase.FullHouse) return false;
            SetPhase((BingoPhase)((int)Phase + 1));
            return true;
        }

        public void SetPots(int potOneLine, int potTwoLines, int potFullHouse)
        {
            PotOneLine = Math.Max(0, potOneLine);
            PotTwoLines = Math.Max(0, potTwoLines);
            PotFullHouse = Math.Max(0, potFullHouse);
        }

        public int GetPotForPhase(BingoPhase phase) => phase switch
        {
            BingoPhase.OneLine => PotOneLine,
            BingoPhase.TwoLines => PotTwoLines,
            _ => PotFullHouse
        };

        public bool SetPlayerCardCount(string sessionId, int cardCount)
        {
            if (!Players.TryGetValue(sessionId, out var p)) return false;

            cardCount = Math.Clamp(cardCount, 0, 10);
            int current = p.Cards.Count;
            if (current == cardCount) return false;

            if (cardCount < current)
            {
                p.Cards.RemoveRange(cardCount, current - cardCount);
            }
            else
            {
                for (int i = current; i < cardCount; i++)
                    p.Cards.Add(GenerateCard(_rng));
            }

            PendingClaims.TryRemove(sessionId, out _);
            return true;
        }

        public void AddPlayer(string sessionId, string name, int cardCount, int markerColorArgb)
        {
            cardCount = Math.Clamp(cardCount, 0, 10);

            var cards = new List<BingoCard>(cardCount);
            for (int i = 0; i < cardCount; i++)
                cards.Add(GenerateCard(_rng));

            Players[sessionId] = new BingoHostPlayer(sessionId, name ?? "Player", markerColorArgb, cards);
        }

        public void RemovePlayer(string sessionId)
        {
            Players.TryRemove(sessionId, out _);
            PendingClaims.TryRemove(sessionId, out _);
        }

        public bool CallNumber(int number)
        {
            if (number < 1 || number > 49) return false;
            if (_called.Contains(number))
            {
                CurrentNumber = number;
                return false;
            }

            _called.Add(number);
            CalledNumbers.Add(number);
            CurrentNumber = number;
            return true;
        }

        public bool RollRandomNumber()
        {
            if (_called.Count >= 49) return false;

            // try a few randoms first
            for (int i = 0; i < 25; i++)
            {
                var n = _rng.Next(1, 50);
                if (!_called.Contains(n))
                    return CallNumber(n);
            }

            for (int n = 1; n <= 49; n++)
            {
                if (!_called.Contains(n))
                    return CallNumber(n);
            }

            return false;
        }

        public BingoStatePublic BuildPublicState()
        {
            var players = Players.Values
                .OrderBy(p => p.Name)
                .Select(p => new BingoPlayerPublic(p.SessionId, p.Name, p.Cards.Count, PendingClaims.ContainsKey(p.SessionId)))
                .ToList();

            var claims = PendingClaims.Values
                .OrderByDescending(c => c.ClaimedTicks)
                .ToList();

            return new BingoStatePublic(
                GameId,
                HostSessionId,
                HostName,
                LobbyName,
                Phase,
                PotOneLine,
                PotTwoLines,
                PotFullHouse,
                CurrentNumber,
                CalledNumbers.ToList(),
                players,
                claims,
                Winners.ToList());
        }

        public BingoStatePrivate BuildPrivateState(string sessionId)
        {
            if (!Players.TryGetValue(sessionId, out var p))
                return new BingoStatePrivate(GameId, Phase, unchecked((int)0xFFAA55FF), []);

            return new BingoStatePrivate(GameId, Phase, p.MarkerColorArgb, p.Cards.ToList());
        }

        public bool TryGetEligibleCard(string sessionId, BingoPhase phase, out int cardIndex, out List<int> matchedIndices)
        {
            cardIndex = -1;
            matchedIndices = [];

            if (!Players.TryGetValue(sessionId, out var p))
                return false;

            for (int i = 0; i < p.Cards.Count; i++)
            {
                if (TryGetEligibleLines(p.Cards[i], phase, out var matched))
                {
                    cardIndex = i;
                    matchedIndices = matched;
                    return true;
                }
            }

            return false;
        }

        private bool TryGetEligibleLines(BingoCard card, BingoPhase phase, out List<int> matchedIndices)
        {
            matchedIndices = [];
            if (card?.Numbers == null || card.Numbers.Count < 25)
                return false;

            // Precompute called hits
            bool[] hit = new bool[25];
            for (int i = 0; i < 25; i++)
                hit[i] = _called.Contains(card.Numbers[i]);

            static bool IsLineComplete(int[] line, bool[] hit)
            {
                for (int i = 0; i < line.Length; i++)
                {
                    if (!hit[line[i]])
                        return false;
                }
                return true;
            }

            var lines = BingoLines;
            var completed = new List<int[]>(4);

            foreach (var line in lines)
            {
                if (IsLineComplete(line, hit))
                    completed.Add(line);
            }

            if (phase == BingoPhase.FullHouse)
            {
                for (int i = 0; i < 25; i++)
                {
                    if (!hit[i])
                        return false;
                }

                matchedIndices = Enumerable.Range(0, 25).ToList();
                return true;
            }

            if (phase == BingoPhase.OneLine)
            {
                if (completed.Count >= 1)
                {
                    matchedIndices = completed[0].ToList();
                    return true;
                }

                return false;
            }

            // Two lines
            if (completed.Count >= 2)
            {
                var set = new HashSet<int>();
                foreach (var idx in completed[0]) set.Add(idx);
                foreach (var idx in completed[1]) set.Add(idx);
                matchedIndices = set.OrderBy(i => i).ToList();
                return true;
            }

            return false;
        }

        private static readonly int[][] BingoLines = BuildBingoLines();

        private static int[][] BuildBingoLines()
        {
            var list = new List<int[]>(12);

            for (int r = 0; r < 5; r++)
            {
                int[] line = new int[5];
                for (int c = 0; c < 5; c++)
                    line[c] = r * 5 + c;
                list.Add(line);
            }

            for (int c = 0; c < 5; c++)
            {
                int[] line = new int[5];
                for (int r = 0; r < 5; r++)
                    line[r] = r * 5 + c;
                list.Add(line);
            }

            list.Add([0, 6, 12, 18, 24]);
            list.Add([4, 8, 12, 16, 20]);

            return list.ToArray();
        }

        private static BingoCard GenerateCard(Random rng)
        {
            // Organised rows:
            // Row 1:  1-9  (9 numbers)
            // Row 2: 10-19
            // Row 3: 20-29
            // Row 4: 30-39
            // Row 5: 40-49
            // Each row picks 5 unique numbers from its range.
            static List<int> Pick(Random rng, int start, int endInclusive, int count)
            {
                var pool = Enumerable.Range(start, endInclusive - start + 1).ToList();
                // Fisher–Yates partial shuffle
                for (int i = pool.Count - 1; i > 0; i--)
                {
                    int j = rng.Next(i + 1);
                    (pool[i], pool[j]) = (pool[j], pool[i]);
                }
                return pool.Take(count).ToList();
            }

            var nums = new List<int>(25);

            var r1 = Pick(rng, 1, 9, 5);
            var r2 = Pick(rng, 10, 19, 5);
            var r3 = Pick(rng, 20, 29, 5);
            var r4 = Pick(rng, 30, 39, 5);
            var r5 = Pick(rng, 40, 49, 5);

            void ShuffleRow(List<int> row)
            {
                for (int i = row.Count - 1; i > 0; i--)
                {
                    int j = rng.Next(i + 1);
                    (row[i], row[j]) = (row[j], row[i]);
                }
            }

            ShuffleRow(r1); ShuffleRow(r2); ShuffleRow(r3); ShuffleRow(r4); ShuffleRow(r5);

            nums.AddRange(r1);
            nums.AddRange(r2);
            nums.AddRange(r3);
            nums.AddRange(r4);
            nums.AddRange(r5);

            return new BingoCard(nums);
        }
    }

    public sealed class BingoHostPlayer
    {
        public string SessionId { get; }
        public string Name { get; set; }
        public int MarkerColorArgb { get; set; }
        public List<BingoCard> Cards { get; }

        public BingoHostPlayer(string sessionId, string name, int markerColorArgb, List<BingoCard> cards)
        {
            SessionId = sessionId;
            Name = name;
            MarkerColorArgb = markerColorArgb;
            Cards = cards ?? [];
        }
    }

    public bool TryGetBingoClaimPreview(Guid gameId, string playerSessionId, BingoPhase phase, out BingoClaimPreview preview)
    {
        preview = default!;
        if (!_hostedBingo.TryGetValue(gameId, out var game)) return false;
        if (!game.PendingClaims.TryGetValue(playerSessionId, out var claim)) return false;

        if (claim.Phase != phase) phase = claim.Phase;

        if (!game.Players.TryGetValue(playerSessionId, out var p)) return false;
        if (!game.TryGetEligibleCard(playerSessionId, phase, out var cardIndex, out var matched)) return false;
        if (cardIndex < 0 || cardIndex >= p.Cards.Count) return false;

        preview = new BingoClaimPreview(cardIndex, p.Cards[cardIndex], p.MarkerColorArgb, matched);
        return true;
    }

    public void CloseBingoLobby(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_hostedBingo.TryRemove(gameId, out var game) ||
            !string.Equals(game.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            LeaveBingo(gameId);
            Invites.TryRemove(gameId, out _);
            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        _clientBingo.TryRemove(gameId, out _);
        Invites.TryRemove(gameId, out _);
        DirectInvites.TryRemove(gameId, out _);

        BroadcastLobbyClosedNearby(gameId, mySessionId, SyncshellGameKind.Bingo);

        var env = new SyncshellGameEnvelope(gameId, SyncshellGameKind.Bingo, SyncshellGameOp.LobbyClosed);
        foreach (var sid in game.Players.Keys)
        {
            if (string.Equals(sid, mySessionId, StringComparison.Ordinal)) continue;
            Send(mySessionId, sid, env);
        }
    }

}
