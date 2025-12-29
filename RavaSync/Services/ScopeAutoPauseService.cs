using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Configurations;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace RavaSync.Services;

public class ScopeAutoPauseService : MediatorSubscriberBase
{
    private readonly ILogger<ScopeAutoPauseService> _logger;
    private readonly MareMediator _mediator;
    private readonly PairManager _pairManager;
    private readonly DalamudUtilService _xiv;
    private readonly TransientConfigService _transient;
    private readonly IFriendResolver _friends;

    // who we paused via scope (so we can resume correctly)
    private readonly ConcurrentDictionary<string, bool> _autoScopePausedUids = new(StringComparer.Ordinal);

    // small anti-thrash so we don’t double-act on the same uid within a short window
    private readonly ConcurrentDictionary<string, DateTime> _lastActionAtUtc = new(StringComparer.Ordinal);
    private const int ActionDebounceMs = 1000;

    // last-known names per UID (raw for FriendResolver, normalized for Party/Alliance matching)
    private readonly ConcurrentDictionary<string, string> _lastKnownNameRaw = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _lastKnownNameNorm = new(StringComparer.Ordinal);

    private readonly string _statePath =
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "RavaSync", "autoScopeState.json");

    private volatile ScopeMode _mode;

    public ScopeAutoPauseService(
        ILogger<ScopeAutoPauseService> logger,
        MareMediator mediator,
        PairManager pairManager,
        DalamudUtilService xiv,
        TransientConfigService transientConfigService,
        IFriendResolver friendResolver
    ) : base(logger, mediator)
    {
        _logger = logger;
        _mediator = mediator;
        _pairManager = pairManager;
        _xiv = xiv;
        _transient = transientConfigService;
        _friends = friendResolver;

        _mode = (ScopeMode)(_transient.Current.SelectedScopeMode);

        LoadState();

        _mediator.Subscribe<FrameworkUpdateMessage>(this, _ => Tick());
        _mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => Tick());
        _mediator.Subscribe<ScopeModeChangedMessage>(this, OnScopeChanged);
    }

    // ---------- Normalization helpers ----------
    private static string RemoveDiacritics(string s)
    {
        var formD = s.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(formD.Length);
        foreach (var ch in formD)
        {
            var uc = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (uc != UnicodeCategory.NonSpacingMark) sb.Append(ch);
        }
        return sb.ToString().Normalize(NormalizationForm.FormC);
    }

    private static string NormalizeName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return string.Empty;
        var n = name.Trim();

        // Drop world suffix patterns like "First Last@World" or "First Last (World)"
        int at = n.IndexOf('@');
        if (at >= 0) n = n[..at].Trim();

        int paren = n.IndexOf('(');
        if (paren > 0) n = n[..paren].Trim();

        // Collapse internal whitespace
        n = string.Join(' ', n.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries));

        // Case/diacritics insensitive baseline
        n = RemoveDiacritics(n).ToLowerInvariant();
        return n;
    }

    private static HashSet<string>? NormalizeSet(HashSet<string>? raw)
        => raw is null ? null : new HashSet<string>(raw.Select(NormalizeName), StringComparer.Ordinal);

    private static bool CooldownPassed(ConcurrentDictionary<string, DateTime> map, string uid, int ms)
        => !map.TryGetValue(uid, out var when) || (DateTime.UtcNow - when).TotalMilliseconds >= ms;

    private void Touch(string uid) => _lastActionAtUtc[uid] = DateTime.UtcNow;

    private (string raw, string norm) GetBestNamePair(string uid, string? liveName)
    {
        // Prefer live, otherwise cached; always provide both raw + normalized
        string raw = !string.IsNullOrEmpty(liveName)
            ? liveName!
            : (_lastKnownNameRaw.TryGetValue(uid, out var cachedRaw) ? cachedRaw : string.Empty);

        string norm = !string.IsNullOrEmpty(liveName)
            ? NormalizeName(liveName)
            : (_lastKnownNameNorm.TryGetValue(uid, out var cachedNorm) ? cachedNorm : string.Empty);

        return (raw, norm);
    }

    // ---------- Scope toggle handling ----------
    private void OnScopeChanged(ScopeModeChangedMessage msg)
    {
        _mode = msg.Mode;
        _transient.Current.SelectedScopeMode = (int)_mode;
        _transient.Save();

        // Build fresh membership sets for the new mode (+ normalized copies)
        HashSet<string>? party = null, alliance = null;
        if (_mode == ScopeMode.Party) party = _xiv.GetPartyNames();
        else if (_mode == ScopeMode.Alliance) alliance = _xiv.GetAllianceNames();

        var partyNorm = NormalizeSet(party);
        var allianceNorm = NormalizeSet(alliance);

        // If widening to Everyone -> resume everything we scope-paused
        if (_mode == ScopeMode.Everyone)
        {
            if (!_autoScopePausedUids.IsEmpty)
            {
                foreach (var uid in _autoScopePausedUids.Keys.ToArray())
                {
                    var pair = _pairManager.GetPairByUID(uid);
                    if (pair != null)
                    {
                        pair.AutoPausedByScope = false;
                        _mediator.Publish(new ResumeMessage(pair.UserData));
                    }
                    _autoScopePausedUids.TryRemove(uid, out _);
                    Touch(uid);
                }
                SaveState();
            }
            return;
        }

        // For Friends/Party/Alliance: immediately resume any pairs that are NOW in-scope,
        // regardless of current visibility (explicit user action should take effect right away).
        bool changed = false;
        foreach (var uid in _autoScopePausedUids.Keys.ToArray())
        {
            var pair = _pairManager.GetPairByUID(uid);
            if (pair == null)
            {
                _autoScopePausedUids.TryRemove(uid, out _);
                Touch(uid);
                changed = true;
                continue;
            }

            var (raw, norm) = GetBestNamePair(uid, pair.PlayerName);

            bool nowInScope = _mode switch
            {
                ScopeMode.Friends => !string.IsNullOrEmpty(raw) && _friends.IsFriend(raw),
                ScopeMode.Party => partyNorm is not null && !string.IsNullOrEmpty(norm) && partyNorm.Contains(norm),
                ScopeMode.Alliance => allianceNorm is not null && !string.IsNullOrEmpty(norm) && allianceNorm.Contains(norm),
                _ => true
            };

            if (nowInScope)
            {
                _autoScopePausedUids.TryRemove(uid, out _);
                pair.AutoPausedByScope = false;
                _mediator.Publish(new ResumeMessage(pair.UserData));
                Touch(uid);   // bypass debounce on scope change
                changed = true;
            }
        }

        if (changed) SaveState();
        // Normal enforcement continues on next Tick()
    }

    // ---------- Per-frame enforcement ----------
    private void Tick()
    {
        // Update last-known names for any visible actors with a live name
        foreach (var kv in _pairManager.PairsWithGroups.Keys)
        {
            if (!kv.IsVisible) continue;
            if (string.IsNullOrEmpty(kv.PlayerName)) continue;

            var uid = kv.UserData.UID;
            _lastKnownNameRaw[uid] = kv.PlayerName!;
            _lastKnownNameNorm[uid] = NormalizeName(kv.PlayerName!);
        }

        // Fast path: Everyone => nothing to enforce, already handled in OnScopeChanged
        if (_mode == ScopeMode.Everyone) return;

        // Build membership sets once (only if needed) + normalized versions for matching
        HashSet<string>? party = null, alliance = null;
        if (_mode == ScopeMode.Party) party = _xiv.GetPartyNames();
        else if (_mode == ScopeMode.Alliance) alliance = _xiv.GetAllianceNames();

        var partyNorm = NormalizeSet(party);
        var allianceNorm = NormalizeSet(alliance);

        // --- PASS 1: Pause visible + OUT OF SCOPE (one action per tick) ---
        foreach (var pair in _pairManager.PairsWithGroups.Keys)
        {
            if (!pair.IsVisible) continue; // enforce scope only for visible people

            var (raw, norm) = GetBestNamePair(pair.UserData.UID, pair.PlayerName);

            bool inScope = _mode switch
            {
                ScopeMode.Friends => !string.IsNullOrEmpty(raw) && _friends.IsFriend(raw),
                ScopeMode.Party => partyNorm is not null && !string.IsNullOrEmpty(norm) && partyNorm.Contains(norm),
                ScopeMode.Alliance => allianceNorm is not null && !string.IsNullOrEmpty(norm) && allianceNorm.Contains(norm),
                _ => true
            };

            if (!inScope && !_autoScopePausedUids.ContainsKey(pair.UserData.UID) && CooldownPassed(_lastActionAtUtc, pair.UserData.UID, ActionDebounceMs))
            {
                _autoScopePausedUids[pair.UserData.UID] = true;
                pair.AutoPausedByScope = true;
                _mediator.Publish(new PauseMessage(pair.UserData));
                Touch(pair.UserData.UID);
                SaveState();
                return; // one action per tick
            }
        }

        // --- PASS 2: Resume NOW IN SCOPE (one action per tick) ---
        // NOTE: we DO NOT require visibility here. This fixes the “join party doesn’t unpause” case.
        foreach (var uid in _autoScopePausedUids.Keys.ToArray())
        {
            var pair = _pairManager.GetPairByUID(uid);

            // Pair gone from manager -> forget our marker
            if (pair == null)
            {
                _autoScopePausedUids.TryRemove(uid, out _);
                Touch(uid);
                SaveState();
                return;
            }

            var (raw, norm) = GetBestNamePair(uid, pair.PlayerName);

            bool nowInScope = _mode switch
            {
                ScopeMode.Friends => !string.IsNullOrEmpty(raw) && _friends.IsFriend(raw),
                ScopeMode.Party => partyNorm is not null && !string.IsNullOrEmpty(norm) && partyNorm.Contains(norm),
                ScopeMode.Alliance => allianceNorm is not null && !string.IsNullOrEmpty(norm) && allianceNorm.Contains(norm),
                _ => true
            };

            if (nowInScope && CooldownPassed(_lastActionAtUtc, uid, ActionDebounceMs))
            {
                _autoScopePausedUids.TryRemove(uid, out _);
                pair.AutoPausedByScope = false;
                _mediator.Publish(new ResumeMessage(pair.UserData));
                Touch(uid);
                SaveState();
                return;
            }
        }
    }

    // ---------- Persistence ----------
    private record PersistedState(List<string> Uids);

    private void LoadState()
    {
        try
        {
            if (!File.Exists(_statePath)) return;
            var json = File.ReadAllText(_statePath);
            var state = JsonSerializer.Deserialize<PersistedState>(json);
            _autoScopePausedUids.Clear();
            foreach (var uid in state?.Uids ?? Enumerable.Empty<string>())
                _autoScopePausedUids[uid] = true;
            _logger.LogInformation("Loaded scope auto-pause state: {count} uids", _autoScopePausedUids.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed loading autoScopeState");
        }
    }

    private void SaveState()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_statePath)!);
            var json = JsonSerializer.Serialize(new PersistedState(_autoScopePausedUids.Keys.ToList()));
            File.WriteAllText(_statePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed saving autoScopeState");
        }
    }
}
