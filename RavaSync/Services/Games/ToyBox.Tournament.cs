using Dalamud.Game.Text;
using Dalamud.Game.Text.SeStringHandling;
using FFXIVClientStructs.FFXIV.Common.Math;
using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.Services.Discovery;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace RavaSync.Services;

public enum TournamentRole
{
    Fighter = 0,
    Spectator = 1
}

public enum TournamentStage
{
    Lobby = 0,
    InProgress = 1,
    Finished = 2
}

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentJoinPayload(
    string PlayerSessionId,
    string PlayerName,
    TournamentRole Role,
    string? PasswordHash = null,
    string? InviteToken = null
);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentBetPayload(
    string PlayerSessionId,
    string PickSessionId,
    int Amount
);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentFighterPublic(string SessionId, string Name);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentMatchPublic(
    int RoundIndex,
    int MatchIndex,
    string FighterASessionId,
    string FighterAName,
    string FighterBSessionId,
    string FighterBName,
    string WinnerSessionId,
    string WinnerName
);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentRoundPublic(int RoundIndex, List<TournamentMatchPublic> Matches);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentStatePublic(
    Guid GameId,
    TournamentStage Stage,
    string HostSessionId,
    string HostName,
    string LobbyName,
    int MaxHp,
    int CurrentRoundIndex,
    int CurrentMatchIndex,
    bool MatchInProgress,
    string ActiveFighterASessionId,
    string ActiveFighterAName,
    int ActiveFighterAHp,
    string ActiveFighterBSessionId,
    string ActiveFighterBName,
    int ActiveFighterBHp,
    List<TournamentFighterPublic> Fighters,
    List<string> Spectators,
    List<TournamentRoundPublic> Rounds
);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentBetPublic(string PickSessionId, int Amount);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentStatePrivate(
    Guid GameId,
    TournamentRole Role,
    int Bank,
    TournamentBetPublic? CurrentBet
);

[MessagePackObject(keyAsPropertyName: true)]
public record TournamentRollPayload(
    string PlayerSessionId,
    int Roll
);


public sealed partial class ToyBox
{
    private readonly ConcurrentDictionary<Guid, TournamentHostGame> _hostedTournament = new();
    private readonly ConcurrentDictionary<Guid, TournamentClientView> _clientTournament = new();

    public bool TryGetClientTournament(Guid gameId, out TournamentClientView view) => _clientTournament.TryGetValue(gameId, out view!);
    public bool TryGetHostedTournament(Guid gameId, out TournamentHostGame game) => _hostedTournament.TryGetValue(gameId, out game!);

    public IEnumerable<Guid> GetJoinedTournamentIds() => _clientTournament.Keys;

    public Guid HostTournament(string lobbyName = "", int maxHp = 100, int maxPlayers = 0, string? password = null)
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

        var game = new TournamentHostGame(gameId, hostSessionId, hostName, lobbyName ?? string.Empty, Math.Max(1, maxHp), Math.Max(0, maxPlayers), salt, hash);
        _hostedTournament[gameId] = game;

        _clientTournament[gameId] = TournamentClientView.FromHost(game, hostSessionId, hostName);

        BroadcastInviteNearby(gameId, hostSessionId, hostName, SyncshellGameKind.Tournament,
            0, game.LobbyName, game.CurrentPlayers, game.MaxPlayers, game.PasswordProtected, game.PasswordSalt);

        return gameId;
    }

    public void CloseTournamentLobby(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (!_hostedTournament.TryRemove(gameId, out var game) ||
            !string.Equals(game.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            LeaveTournament(gameId);
            Invites.TryRemove(gameId, out _);
            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        _clientTournament.TryRemove(gameId, out _);
        Invites.TryRemove(gameId, out _);
        DirectInvites.TryRemove(gameId, out _);

        BroadcastLobbyClosedNearby(gameId, mySessionId, SyncshellGameKind.Tournament);

        var env = new SyncshellGameEnvelope(gameId, SyncshellGameKind.Tournament, SyncshellGameOp.LobbyClosed);
        foreach (var sid in game.AllParticipantSessionIds())
        {
            if (string.Equals(sid, mySessionId, StringComparison.Ordinal)) continue;
            Send(mySessionId, sid, env);
        }
    }

    public void JoinTournament(Guid gameId, TournamentRole role, string? passwordPlaintext = null, bool fromDirectInvite = false)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        var myName = _clientState.LocalPlayer?.Name.TextValue ?? "Player";
        _cachedMySessionId = mySessionId;
        _cachedMyName = myName;

        if (_hostedTournament.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            hostGame.AddOrUpdateParticipant(mySessionId, myName, role);
            PushTournamentState(hostGame);
            return;
        }

        if (fromDirectInvite && DirectInvites.TryGetValue(gameId, out var direct))
        {
            var join = new TournamentJoinPayload(mySessionId, myName, role, null, direct.InviteToken);

            Send(mySessionId, direct.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Tournament, SyncshellGameOp.Join,
                MessagePackSerializer.Serialize(join)));

            _clientTournament[gameId] = new TournamentClientView(gameId, direct.HostSessionId, direct.HostName, mySessionId, myName, role, null, null);

            DirectInvites.TryRemove(gameId, out _);
            return;
        }

        if (!Invites.TryGetValue(gameId, out var inv))
            return;

        string? passwordHash = null;
        if (inv.PasswordProtected)
            passwordHash = HashPassword(passwordPlaintext ?? string.Empty, inv.PasswordSalt);

        var join2 = new TournamentJoinPayload(mySessionId, myName, role, passwordHash, null);

        Send(mySessionId, inv.HostSessionId, new SyncshellGameEnvelope(
            gameId, SyncshellGameKind.Tournament, SyncshellGameOp.Join,
            MessagePackSerializer.Serialize(join2)));

        _clientTournament[gameId] = new TournamentClientView(gameId, inv.HostSessionId, inv.HostName, mySessionId, myName, role, null, null);
    }

    public void LeaveTournament(Guid gameId)
    {
        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        if (_hostedTournament.TryGetValue(gameId, out var hostGame) &&
            string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
        {
            _clientTournament.TryRemove(gameId, out _);
            return;
        }

        if (_clientTournament.TryGetValue(gameId, out var view))
        {
            Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
                gameId, SyncshellGameKind.Tournament, SyncshellGameOp.Leave,
                MessagePackSerializer.Serialize(new SyncshellJoinPayload(mySessionId, view.MyName))));
        }

        _clientTournament.TryRemove(gameId, out _);
    }

    public void HostTournamentRandomizeFighters(Guid gameId)
    {
        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            game.RandomizeFighterOrder();
            PushTournamentState(game);
        }
    }

    public void HostTournamentMoveFighter(Guid gameId, int index, int delta)
    {
        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            game.MoveFighter(index, delta);
            PushTournamentState(game);
        }
    }

    public void HostTournamentMoveFighterToSpectator(Guid gameId, string fighterSessionId, string fighterName)
    {
        if (string.IsNullOrEmpty(fighterSessionId)) return;

        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            if (game.Stage != TournamentStage.Lobby)
                return;

            game.AddOrUpdateParticipant(fighterSessionId, fighterName ?? "Player", TournamentRole.Spectator);
            PushTournamentState(game);
        }
    }

    public void HostTournamentBuildBracket(Guid gameId)
    {
        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            game.BuildBracket();
            PushTournamentState(game);
        }
    }

    public void HostTournamentStartNextMatch(Guid gameId)
    {
        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            game.StartNextMatch();
            PushTournamentState(game);
        }
    }

    public void HostTournamentForceWin(Guid gameId, string winnerSessionId)
    {
        if (_hostedTournament.TryGetValue(gameId, out var game))
        {
            game.ForceWin(winnerSessionId);
            PushTournamentState(game);
        }
    }

    public IReadOnlyList<TournamentOverlayEntry> GetTournamentOverlayEntries()
    {
        var list = new List<TournamentOverlayEntry>(8);

        foreach (var v in _clientTournament.Values)
        {
            var pub = v.Public;
            if (pub == null) continue;
            if (pub.Stage != TournamentStage.InProgress) continue;
            if (!pub.MatchInProgress) continue;
            if (string.IsNullOrEmpty(pub.ActiveFighterASessionId) || string.IsNullOrEmpty(pub.ActiveFighterBSessionId)) continue;

            list.Add(new TournamentOverlayEntry(pub.ActiveFighterASessionId, pub.ActiveFighterAName, pub.ActiveFighterAHp, pub.MaxHp));
            list.Add(new TournamentOverlayEntry(pub.ActiveFighterBSessionId, pub.ActiveFighterBName, pub.ActiveFighterBHp, pub.MaxHp));
        }

        return list;
    }

    public sealed record TournamentOverlayEntry(string SessionId, string Name, int Hp, int MaxHp);

    private void HandleTournamentJoin(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Tournament) return;
        if (!_hostedTournament.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        TournamentJoinPayload join;
        try { join = MessagePackSerializer.Deserialize<TournamentJoinPayload>(env.Payload); }
        catch { return; }

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

        game.AddOrUpdateParticipant(join.PlayerSessionId, join.PlayerName, join.Role);
        PushTournamentState(game);
    }

    private void HandleTournamentLeave(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Tournament) return;
        if (!_hostedTournament.TryGetValue(env.GameId, out var game)) return;
        if (env.Payload is null) return;

        SyncshellJoinPayload leave;
        try { leave = MessagePackSerializer.Deserialize<SyncshellJoinPayload>(env.Payload); }
        catch { return; }

        game.RemoveParticipant(leave.PlayerSessionId);
        PushTournamentState(game);
    }


    private void HandleTournamentPublic(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Tournament) return;
        if (env.Payload is null) return;

        TournamentStatePublic pub;
        try { pub = MessagePackSerializer.Deserialize<TournamentStatePublic>(env.Payload); }
        catch { return; }

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        if (_clientTournament.TryGetValue(env.GameId, out var view))
            _clientTournament[env.GameId] = view.WithPublic(pub);
        else
            _clientTournament[env.GameId] = TournamentClientView.FromPublic(pub, mySessionId, myName);
    }

    private void HandleTournamentPrivate(string localSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Tournament) return;
        if (env.Payload is null) return;

        TournamentStatePrivate priv;
        try { priv = MessagePackSerializer.Deserialize<TournamentStatePrivate>(env.Payload); }
        catch { return; }

        if (!string.IsNullOrEmpty(localSessionId))
            _cachedMySessionId = localSessionId;

        var mySessionId = _cachedMySessionId;
        var myName = _cachedMyName;

        if (_clientTournament.TryGetValue(env.GameId, out var view))
            _clientTournament[env.GameId] = view.WithPrivate(priv);
        else
            _clientTournament[env.GameId] = TournamentClientView.FromPrivate(priv, mySessionId, myName);
    }

    private void HandleTournamentRoll(string fromSessionId, SyncshellGameEnvelope env)
    {
        if (env.Kind != SyncshellGameKind.Tournament) return;
        if (env.Payload is null) return;
        if (!_hostedTournament.TryGetValue(env.GameId, out var game)) return;

        TournamentRollPayload payload;
        try { payload = MessagePackSerializer.Deserialize<TournamentRollPayload>(env.Payload); }
        catch { return; }

        if (!game.MatchInProgress) return;
        if (payload.Roll <= 0) return;

        var rollerSessionId = fromSessionId;

        if (!string.Equals(payload.PlayerSessionId, fromSessionId, StringComparison.Ordinal))
        {
            Logger.LogWarning("TournamentRoll session mismatch: payload={payloadSid} from={fromSid} game={gameId}. Using fromSid.",
                payload.PlayerSessionId, fromSessionId, env.GameId);
        }

        var a = game.ActiveFighterASessionId;
        var b = game.ActiveFighterBSessionId;

        if (string.IsNullOrEmpty(a) || string.IsNullOrEmpty(b))
            return;

        if (!string.Equals(rollerSessionId, a, StringComparison.Ordinal)
            && !string.Equals(rollerSessionId, b, StringComparison.Ordinal))
        {
            Logger.LogDebug("TournamentRoll ignored: roller is not an active fighter. roller={roller} A={a} B={b} game={gameId}",
                rollerSessionId, a, b, env.GameId);
            return;
        }

        if (!game.ApplyRollDamage(rollerSessionId, payload.Roll))
        {
            Logger.LogDebug("TournamentRoll ignored: ApplyRollDamage returned false. roller={roller} roll={roll} game={gameId} matchInProgress={inProgress}",
                rollerSessionId, payload.Roll, env.GameId, game.MatchInProgress);
            return;
        }

        PushTournamentState(game);
    }

    private void PushTournamentState(TournamentHostGame game)
    {
        var pub = game.BuildPublicState();
        var pubBytes = MessagePackSerializer.Serialize(pub);
        var pubEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Tournament, SyncshellGameOp.TournamentStatePublic, pubBytes);

        foreach (var sid in game.AllParticipantSessionIds())
        {
            if (string.Equals(sid, game.HostSessionId, StringComparison.Ordinal))
                continue;

            Send(game.HostSessionId, sid, pubEnv);

            var priv = game.BuildPrivateState(sid);
            var privBytes = MessagePackSerializer.Serialize(priv);
            var privEnv = new SyncshellGameEnvelope(game.GameId, SyncshellGameKind.Tournament, SyncshellGameOp.TournamentStatePrivate, privBytes);
            Send(game.HostSessionId, sid, privEnv);
        }

        _clientTournament[game.GameId] = TournamentClientView.FromHost(game, game.HostSessionId, game.HostName);
    }

    private static readonly Regex _rollKeywordRegex =
        new(@"\brolls?\b|\bdice\b|\brandom\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex _rollValueRegex =
        new(@"\brolls?\s+(\d{1,6})\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex _obtainValueRegex =
        new(@"\bobtain(?:s)?\s+(\d{1,6})\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex _resultValueRegex =
        new(@"\bresult[: ]\s*(\d{1,6})\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex _anyIntRegex =
        new(@"\b\d{1,6}\b", RegexOptions.Compiled);



    private static int IndexOfName(string msg, string name)
    {
        if (string.IsNullOrEmpty(msg) || string.IsNullOrEmpty(name)) return -1;
        return msg.IndexOf(name, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryExtractRollValue(string msg, out int roll)
    {
        roll = 0;
        if (string.IsNullOrWhiteSpace(msg)) return false;

        var m = _rollValueRegex.Match(msg);
        if (m.Success && int.TryParse(m.Groups[1].Value, out roll) && roll > 0)
            return true;

        m = _obtainValueRegex.Match(msg);
        if (m.Success && int.TryParse(m.Groups[1].Value, out roll) && roll > 0)
            return true;

        m = _resultValueRegex.Match(msg);
        if (m.Success && int.TryParse(m.Groups[1].Value, out roll) && roll > 0)
            return true;

        if (msg.IndexOf("out of", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            var first = _anyIntRegex.Match(msg);
            if (first.Success && int.TryParse(first.Value, out roll) && roll > 0)
                return true;
        }

        var matches = _anyIntRegex.Matches(msg);
        if (matches.Count == 0) return false;

        return int.TryParse(matches[matches.Count - 1].Value, out roll) && roll > 0;
    }

    private static bool IsMyRollMessage(string msg)
    {
        if (string.IsNullOrWhiteSpace(msg)) return false;

        return msg.StartsWith("You ", StringComparison.OrdinalIgnoreCase) ||
               msg.IndexOf("You roll", StringComparison.OrdinalIgnoreCase) >= 0;
    }


    private void TryHandleTournamentRoll(XivChatType type, SeString sender, SeString message)
    {
        var msg = message.TextValue ?? string.Empty;
        if (msg.Length == 0) return;

        if (!_rollKeywordRegex.IsMatch(msg))
            return;

        if (!TryExtractRollValue(msg, out var roll))
            return;

        var mySessionId = GetMySessionId();
        if (string.IsNullOrEmpty(mySessionId)) return;

        var isMyRoll = IsMyRollMessage(msg);

        if (isMyRoll)
        {
            RelayMyTournamentRoll(mySessionId, roll);
            return;
        }

        if (_hostedTournament.IsEmpty) return;

        var senderName = sender.TextValue ?? string.Empty;

        var senderIsUseless =
            string.IsNullOrWhiteSpace(senderName) ||
            senderName.StartsWith("Random", StringComparison.OrdinalIgnoreCase);

        if (senderIsUseless)
        {
            var nm = Regex.Match(msg, @"^(?:Random!\s*)?(.+?)\s+roll", RegexOptions.IgnoreCase);
            if (nm.Success)
                senderName = nm.Groups[1].Value.Trim();
        }

        if (string.IsNullOrWhiteSpace(senderName))
            return;

        foreach (var kv in _hostedTournament)
        {
            var game = kv.Value;
            if (!game.MatchInProgress) continue;

            var rollerSid = game.ResolveSessionIdFromChatName(senderName);
            if (string.IsNullOrEmpty(rollerSid))
            {
                rollerSid = game.ResolveRollerFromRollMessage(msg, _cachedMyName);
                if (string.IsNullOrEmpty(rollerSid)) continue;
            }

            if (!game.ApplyRollDamage(rollerSid, roll))
                continue;

            PushTournamentState(game);
        }
    }

    private void RelayMyTournamentRoll(string mySessionId, int roll)
    {
        foreach (var kv in _clientTournament)
        {
            var view = kv.Value;
            var pub = view.Public;
            if (pub == null) continue;
            if (pub.Stage != TournamentStage.InProgress) continue;
            if (!pub.MatchInProgress) continue;

            var imActive =
                string.Equals(pub.ActiveFighterASessionId, mySessionId, StringComparison.Ordinal) ||
                string.Equals(pub.ActiveFighterBSessionId, mySessionId, StringComparison.Ordinal);

            if (!imActive) continue;

            if (_hostedTournament.TryGetValue(pub.GameId, out var hostGame) &&
                string.Equals(hostGame.HostSessionId, mySessionId, StringComparison.Ordinal))
            {
                if (hostGame.ApplyRollDamage(mySessionId, roll))
                    PushTournamentState(hostGame);

                continue;
            }

            var payload = new TournamentRollPayload(mySessionId, roll);

            Send(mySessionId, view.HostSessionId, new SyncshellGameEnvelope(
                pub.GameId, SyncshellGameKind.Tournament, SyncshellGameOp.TournamentRoll,
                MessagePackSerializer.Serialize(payload)));
        }
    }


    public sealed class TournamentHostGame
    {
        private readonly Random _rng = new();

        public Guid GameId { get; }
        public string HostSessionId { get; }
        public string HostName { get; }

        public string LobbyName { get; }
        public int MaxPlayers { get; }
        public string PasswordSalt { get; }
        public string PasswordHash { get; }
        public bool PasswordProtected => !string.IsNullOrEmpty(PasswordHash);

        public int MaxHp { get; }
        public int StartingBank { get; }

        public TournamentStage Stage { get; private set; } = TournamentStage.Lobby;

        private readonly ConcurrentDictionary<string, string> _inviteTokens = new(StringComparer.Ordinal);

        private readonly List<string> _fighterOrder = new();
        private readonly ConcurrentDictionary<string, string> _names = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, SpectatorInfo> _spectators = new(StringComparer.Ordinal);

        private readonly List<List<TournamentMatch>> _rounds = new();

        public int CurrentRoundIndex { get; private set; } = 0;
        public int CurrentMatchIndex { get; private set; } = 0;

        public bool MatchInProgress { get; private set; } = false;
        private int _activeRound = -1;
        private int _activeMatch = -1;

        private readonly ConcurrentDictionary<string, MatchBet> _bets = new(StringComparer.Ordinal);

        private sealed class SpectatorInfo
        {
            public string Name = "Spectator";
            public int Bank;
            public TournamentBetPublic? CurrentBet;
        }

        private sealed class TournamentMatch
        {
            public string A = string.Empty;
            public string B = string.Empty;
            public string Winner = string.Empty;

            public int HpA;
            public int HpB;
        }

        private sealed class MatchBet
        {
            public bool Locked;
            public readonly ConcurrentDictionary<string, TournamentBetPublic> Bets = new(StringComparer.Ordinal);
        }

        public TournamentHostGame(Guid gameId, string hostSessionId, string hostName, string lobbyName, int maxHp,int maxPlayers, string passwordSalt, string passwordHash)
        {
            GameId = gameId;
            HostSessionId = hostSessionId;
            HostName = hostName;

            LobbyName = lobbyName ?? string.Empty;
            MaxHp = Math.Max(1, maxHp);
            MaxPlayers = Math.Max(0, maxPlayers);

            PasswordSalt = passwordSalt ?? string.Empty;
            PasswordHash = passwordHash ?? string.Empty;

            _names[HostSessionId] = HostName;
        }

        public int CurrentPlayers => 1 + _fighterOrder.Count + _spectators.Count;

        public IEnumerable<string> AllParticipantSessionIds()
        {
            foreach (var f in _fighterOrder) yield return f;
            foreach (var s in _spectators.Keys) yield return s;
        }

        public void AddOrUpdateParticipant(string sessionId, string name, TournamentRole role)
        {
            if (string.IsNullOrEmpty(sessionId)) return;
            if (string.IsNullOrWhiteSpace(name)) name = "Player";

            _names[sessionId] = name;

            if (role == TournamentRole.Fighter)
            {
                _spectators.TryRemove(sessionId, out _);
                if (!_fighterOrder.Contains(sessionId, StringComparer.Ordinal))
                    _fighterOrder.Add(sessionId);
            }
            else
            {
                _fighterOrder.RemoveAll(x => string.Equals(x, sessionId, StringComparison.Ordinal));
                _spectators.AddOrUpdate(sessionId,
                    _ => new SpectatorInfo { Name = name, Bank = StartingBank },
                    (_, existing) => { existing.Name = name; return existing; });
            }

            if (Stage == TournamentStage.Lobby)
                return;

            if (Stage == TournamentStage.InProgress && !_fighterOrder.Contains(sessionId, StringComparer.Ordinal))
                return;
        }

        public void RemoveParticipant(string sessionId)
        {
            if (string.IsNullOrEmpty(sessionId)) return;
            _fighterOrder.RemoveAll(x => string.Equals(x, sessionId, StringComparison.Ordinal));
            _spectators.TryRemove(sessionId, out _);
        }

        public void RandomizeFighterOrder()
        {
            for (int i = _fighterOrder.Count - 1; i > 0; i--)
            {
                int j = _rng.Next(i + 1);
                (_fighterOrder[i], _fighterOrder[j]) = (_fighterOrder[j], _fighterOrder[i]);
            }
        }

        public void MoveFighter(int index, int delta)
        {
            if (index < 0 || index >= _fighterOrder.Count) return;
            var newIndex = index + delta;
            if (newIndex < 0 || newIndex >= _fighterOrder.Count) return;

            var item = _fighterOrder[index];
            _fighterOrder.RemoveAt(index);
            _fighterOrder.Insert(newIndex, item);
        }

        public string ActiveFighterASessionId
        {
            get
            {
                if (!MatchInProgress) return string.Empty;
                if (_activeRound < 0 || _activeMatch < 0) return string.Empty;
                if (_activeRound >= _rounds.Count) return string.Empty;
                if (_activeMatch >= _rounds[_activeRound].Count) return string.Empty;
                return _rounds[_activeRound][_activeMatch].A ?? string.Empty;
            }
        }

        public string ActiveFighterBSessionId
        {
            get
            {
                if (!MatchInProgress) return string.Empty;
                if (_activeRound < 0 || _activeMatch < 0) return string.Empty;
                if (_activeRound >= _rounds.Count) return string.Empty;
                if (_activeMatch >= _rounds[_activeRound].Count) return string.Empty;
                return _rounds[_activeRound][_activeMatch].B ?? string.Empty;
            }
        }

        public string IssueInviteToken(string targetSessionId)
        {
            if (string.IsNullOrEmpty(targetSessionId)) return string.Empty;
            var token = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(10));
            _inviteTokens[targetSessionId] = token;
            return token;
        }

        public bool ConsumeInviteToken(string targetSessionId, string token)
        {
            if (string.IsNullOrEmpty(targetSessionId) || string.IsNullOrEmpty(token)) return false;
            return _inviteTokens.TryRemove(targetSessionId, out var have) && string.Equals(have, token, StringComparison.Ordinal);
        }

        public string ResolveRollerFromRollMessage(string msg, string cachedMyName)
        {
            if (!MatchInProgress) return string.Empty;
            if (_activeRound < 0 || _activeMatch < 0) return string.Empty;
            if (_activeRound >= _rounds.Count) return string.Empty;

            var m = _rounds[_activeRound][_activeMatch];
            if (string.IsNullOrEmpty(m.A) || string.IsNullOrEmpty(m.B)) return string.Empty;

            var nameA = GetName(m.A);
            var nameB = GetName(m.B);

            if (msg.IndexOf("You roll", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                if (!string.IsNullOrEmpty(cachedMyName))
                {
                    if (string.Equals(cachedMyName, nameA, StringComparison.OrdinalIgnoreCase)) return m.A;
                    if (string.Equals(cachedMyName, nameB, StringComparison.OrdinalIgnoreCase)) return m.B;
                }
                return string.Empty;
            }

            var ia = IndexOfName(msg, nameA);
            var ib = IndexOfName(msg, nameB);

            if (ia >= 0 && (ib < 0 || ia < ib)) return m.A;
            if (ib >= 0) return m.B;

            if (!string.IsNullOrEmpty(cachedMyName) && IndexOfName(msg, cachedMyName) >= 0)
            {
                if (string.Equals(cachedMyName, nameA, StringComparison.OrdinalIgnoreCase)) return m.A;
                if (string.Equals(cachedMyName, nameB, StringComparison.OrdinalIgnoreCase)) return m.B;
            }

            return string.Empty;
        }


        public void BuildBracket()
        {
            _rounds.Clear();
            _bets.Clear();

            Stage = TournamentStage.InProgress;
            MatchInProgress = false;
            _activeRound = -1;
            _activeMatch = -1;

            CurrentRoundIndex = 0;
            CurrentMatchIndex = 0;

            var round0 = new List<TournamentMatch>();
            for (int i = 0; i < _fighterOrder.Count; i += 2)
            {
                var a = _fighterOrder[i];
                var b = (i + 1 < _fighterOrder.Count) ? _fighterOrder[i + 1] : string.Empty;

                var m = new TournamentMatch
                {
                    A = a,
                    B = b,
                    Winner = string.Empty,
                    HpA = 0,
                    HpB = 0
                };

                if (string.IsNullOrEmpty(b))
                    m.Winner = a;

                round0.Add(m);
            }

            _rounds.Add(round0);

            TryAdvanceRoundsIfComplete();
        }

        public void StartNextMatch()
        {
            if (Stage != TournamentStage.InProgress) return;

            if (_rounds.Count == 0)
                BuildBracket();

            TryAdvanceRoundsIfComplete();

            if (Stage != TournamentStage.InProgress) return;

            if (CurrentRoundIndex < 0 || CurrentRoundIndex >= _rounds.Count) return;

            var round = _rounds[CurrentRoundIndex];

            for (int i = 0; i < round.Count; i++)
            {
                if (!string.IsNullOrEmpty(round[i].Winner)) continue;
                if (string.IsNullOrEmpty(round[i].A) || string.IsNullOrEmpty(round[i].B))
                {
                    if (!string.IsNullOrEmpty(round[i].A)) round[i].Winner = round[i].A;
                    continue;
                }

                CurrentMatchIndex = i;
                _activeRound = CurrentRoundIndex;
                _activeMatch = i;

                round[i].HpA = MaxHp;
                round[i].HpB = MaxHp;

                MatchInProgress = true;

                GetBetBucket(CurrentRoundIndex, i).Locked = true;
                return;
            }

            TryAdvanceRoundsIfComplete();
        }

        public void ForceWin(string winnerSessionId)
        {
            if (!MatchInProgress) return;
            if (_activeRound < 0 || _activeMatch < 0) return;
            if (_activeRound >= _rounds.Count) return;

            var m = _rounds[_activeRound][_activeMatch];
            if (!string.Equals(winnerSessionId, m.A, StringComparison.Ordinal) &&
                !string.Equals(winnerSessionId, m.B, StringComparison.Ordinal))
                return;

            m.Winner = winnerSessionId;
            MatchInProgress = false;

            SettleBets(_activeRound, _activeMatch, winnerSessionId);

            TryAdvanceRoundsIfComplete();
        }

        public string ResolveSessionIdFromChatName(string chatName)
        {
            if (string.IsNullOrWhiteSpace(chatName))
                return string.Empty;

            chatName = chatName.Trim();

            foreach (var kv in _names)
            {
                var sid = kv.Key;
                var name = kv.Value;

                if (string.IsNullOrEmpty(name)) continue;

                if (string.Equals(name, chatName, StringComparison.OrdinalIgnoreCase))
                    return sid;

                if (chatName.StartsWith(name, StringComparison.OrdinalIgnoreCase))
                    return sid;
            }

            foreach (var kv in _spectators)
            {
                var sid = kv.Key;
                var name = kv.Value.Name;

                if (string.IsNullOrEmpty(name)) continue;

                if (string.Equals(name, chatName, StringComparison.OrdinalIgnoreCase))
                    return sid;

                if (chatName.StartsWith(name, StringComparison.OrdinalIgnoreCase))
                    return sid;
            }

            return string.Empty;
        }


        public bool ApplyRollDamage(string rollerSessionId, int roll)
        {
            if (!MatchInProgress) return false;
            if (_activeRound < 0 || _activeMatch < 0) return false;
            if (_activeRound >= _rounds.Count) return false;

            var m = _rounds[_activeRound][_activeMatch];
            if (string.IsNullOrEmpty(m.A) || string.IsNullOrEmpty(m.B)) return false;
            if (!string.IsNullOrEmpty(m.Winner)) return false;

            var changed = false;

            if (string.Equals(rollerSessionId, m.A, StringComparison.Ordinal))
            {
                m.HpB -= roll;
                if (m.HpB < 0) m.HpB = 0;
                changed = true;

                if (m.HpB <= 0)
                {
                    m.HpB = 0;
                    m.Winner = m.A;
                    MatchInProgress = false;
                    SettleBets(_activeRound, _activeMatch, m.A);
                    TryAdvanceRoundsIfComplete();
                }
            }
            else if (string.Equals(rollerSessionId, m.B, StringComparison.Ordinal))
            {
                m.HpA -= roll;
                if (m.HpA < 0) m.HpA = 0;
                changed = true;

                if (m.HpA <= 0)
                {
                    m.HpA = 0;
                    m.Winner = m.B;
                    MatchInProgress = false;
                    SettleBets(_activeRound, _activeMatch, m.B);
                    TryAdvanceRoundsIfComplete();
                }
            }

            if (!changed) return false;

            _rounds[_activeRound][_activeMatch] = m;
            return true;
        }


        private void TryAdvanceRoundsIfComplete()
        {
            while (Stage == TournamentStage.InProgress)
            {
                if (CurrentRoundIndex < 0 || CurrentRoundIndex >= _rounds.Count) break;

                var round = _rounds[CurrentRoundIndex];

                foreach (var mm in round)
                {
                    if (string.IsNullOrEmpty(mm.B) && string.IsNullOrEmpty(mm.Winner) && !string.IsNullOrEmpty(mm.A))
                        mm.Winner = mm.A;
                }

                if (round.Any(m => string.IsNullOrEmpty(m.Winner)))
                    return;

                var winners = round.Select(m => m.Winner).Where(x => !string.IsNullOrEmpty(x)).ToList();
                if (winners.Count == 1)
                {
                    Stage = TournamentStage.Finished;
                    MatchInProgress = false;
                    return;
                }

                var next = new List<TournamentMatch>();
                for (int i = 0; i < winners.Count; i += 2)
                {
                    var a = winners[i];
                    var b = (i + 1 < winners.Count) ? winners[i + 1] : string.Empty;

                    var nm = new TournamentMatch { A = a, B = b };
                    if (string.IsNullOrEmpty(b))
                        nm.Winner = a;

                    next.Add(nm);
                }

                _rounds.Add(next);
                CurrentRoundIndex++;
                CurrentMatchIndex = 0;

                if (CurrentRoundIndex >= _rounds.Count)
                    return;
            }
        }

        private MatchBet GetBetBucket(int roundIndex, int matchIndex)
        {
            var key = $"{roundIndex}:{matchIndex}";
            return _bets.GetOrAdd(key, _ => new MatchBet());
        }

        private void SettleBets(int roundIndex, int matchIndex, string winnerSessionId)
        {
            var bucket = GetBetBucket(roundIndex, matchIndex);
            var all = bucket.Bets.ToArray();
            if (all.Length == 0) return;

            var total = all.Sum(kv => kv.Value.Amount);
            var winnersTotal = all.Where(kv => string.Equals(kv.Value.PickSessionId, winnerSessionId, StringComparison.Ordinal)).Sum(kv => kv.Value.Amount);

            foreach (var kv in all)
            {
                if (!_spectators.TryGetValue(kv.Key, out var spec)) continue;

                if (spec.CurrentBet != null)
                    spec.CurrentBet = null;

                if (!string.Equals(kv.Value.PickSessionId, winnerSessionId, StringComparison.Ordinal))
                    continue;

                if (winnersTotal <= 0)
                    continue;

                var share = (total * (kv.Value.Amount / (double)winnersTotal));
                var payout = (int)Math.Floor(share);
                if (payout < kv.Value.Amount) payout = kv.Value.Amount;

                spec.Bank += payout;
            }

            bucket.Bets.Clear();
        }

        public string ResolveSessionIdFromName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return string.Empty;

            foreach (var kv in _names)
            {
                if (string.Equals(kv.Value, name, StringComparison.OrdinalIgnoreCase))
                    return kv.Key;
            }

            foreach (var kv in _spectators)
            {
                if (string.Equals(kv.Value.Name, name, StringComparison.OrdinalIgnoreCase))
                    return kv.Key;
            }

            return string.Empty;
        }

        public TournamentStatePublic BuildPublicState()
        {
            var fighters = _fighterOrder.Select(sid => new TournamentFighterPublic(sid, GetName(sid))).ToList();
            var spectators = _spectators.Values.Select(s => s.Name).OrderBy(x => x).ToList();

            string aSid = string.Empty, bSid = string.Empty;
            string aName = string.Empty, bName = string.Empty;
            int hpA = 0, hpB = 0;

            if (MatchInProgress && _activeRound >= 0 && _activeMatch >= 0 && _activeRound < _rounds.Count)
            {
                var m = _rounds[_activeRound][_activeMatch];
                aSid = m.A;
                bSid = m.B;
                aName = GetName(aSid);
                bName = GetName(bSid);
                hpA = m.HpA;
                hpB = m.HpB;
            }

            var rounds = new List<TournamentRoundPublic>(_rounds.Count);
            for (int r = 0; r < _rounds.Count; r++)
            {
                var ms = new List<TournamentMatchPublic>(_rounds[r].Count);
                for (int i = 0; i < _rounds[r].Count; i++)
                {
                    var m = _rounds[r][i];
                    var w = m.Winner ?? string.Empty;
                    ms.Add(new TournamentMatchPublic(
                        r, i,
                        m.A, GetName(m.A),
                        m.B, GetName(m.B),
                        w, GetName(w)
                    ));
                }
                rounds.Add(new TournamentRoundPublic(r, ms));
            }

            return new TournamentStatePublic(
                GameId,
                Stage,
                HostSessionId,
                HostName,
                LobbyName,
                MaxHp,
                CurrentRoundIndex,
                CurrentMatchIndex,
                MatchInProgress,
                aSid, aName, hpA,
                bSid, bName, hpB,
                fighters,
                spectators,
                rounds
            );
        }

        public TournamentStatePrivate BuildPrivateState(string sessionId)
        {
            if (_spectators.TryGetValue(sessionId, out var spec))
            {
                return new TournamentStatePrivate(GameId, TournamentRole.Spectator, spec.Bank, spec.CurrentBet);
            }

            if (_fighterOrder.Contains(sessionId, StringComparer.Ordinal))
            {
                return new TournamentStatePrivate(GameId, TournamentRole.Fighter, 0, null);
            }

            return new TournamentStatePrivate(GameId, TournamentRole.Spectator, 0, null);
        }

        private string GetName(string sessionId)
        {
            if (string.IsNullOrEmpty(sessionId)) return string.Empty;
            if (_names.TryGetValue(sessionId, out var n)) return n;
            return "Player";
        }
    }

    public sealed record TournamentClientView(
        Guid GameId,
        string HostSessionId,
        string HostName,
        string MySessionId,
        string MyName,
        TournamentRole MyRole,
        TournamentStatePublic? Public,
        TournamentStatePrivate? Private)
    {
        public static TournamentClientView FromHost(TournamentHostGame host, string mySessionId, string myName)
        {
            var priv = host.BuildPrivateState(mySessionId);
            return new TournamentClientView(host.GameId, host.HostSessionId, host.HostName, mySessionId, myName,
                priv.Role, host.BuildPublicState(), priv);
        }

        public static TournamentClientView FromPublic(TournamentStatePublic pub, string mySessionId, string myName)
        {
            return new TournamentClientView(pub.GameId, pub.HostSessionId, pub.HostName, mySessionId, myName, TournamentRole.Spectator, pub, null);
        }

        public static TournamentClientView FromPrivate(TournamentStatePrivate priv, string mySessionId, string myName)
        {
            return new TournamentClientView(priv.GameId, "", "Host", mySessionId, myName, priv.Role, null, priv);
        }

        public TournamentClientView WithPublic(TournamentStatePublic pub) => this with { Public = pub, HostSessionId = pub.HostSessionId, HostName = pub.HostName };
        public TournamentClientView WithPrivate(TournamentStatePrivate priv) => this with { Private = priv, MyRole = priv.Role };
    }
}
