using Dalamud.Plugin.Services;
using RavaSync.API.Data.Extensions;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.UI;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;

namespace RavaSync.Services;

/// <summary>
/// When the player stands in a registerable location that is not linked to a venue,
/// and the user can manage at least one Syncshell (owner or moderator),
/// opens VenueRegistrationUi (unless suppressed).
/// </summary>
public sealed class VenueRegistrationService : IDisposable
{
    private readonly ILogger<VenueRegistrationService> _log;
    private readonly DalamudUtilService _util;
    private readonly IFramework _framework;
    private readonly IGameGui _gameGui;
    private readonly MareConfigService _cfg;
    private readonly ApiController _api;
    private readonly MareMediator _mediator;
    private readonly PairManager _pairManager;

    private DateTime _nextPollUtc = DateTime.MinValue;
    private string _lastKey = string.Empty;
    private DateTime _lastUiOpenUtc = DateTime.MinValue;

    public VenueRegistrationService(
        ILogger<VenueRegistrationService> logger,
        DalamudUtilService util,
        IFramework framework,
        IGameGui gameGui,
        MareConfigService cfg,
        ApiController api,
        MareMediator mediator,
        PairManager pairManager)
    {
        _log = logger;
        _util = util;
        _framework = framework;
        _gameGui = gameGui;
        _cfg = cfg;
        _api = api;
        _mediator = mediator;
        _pairManager = pairManager;

        _framework.Update += OnUpdate;
    }

    public void Dispose() => _framework.Update -= OnUpdate;

    private async void OnUpdate(IFramework fw)
    {
        var now = DateTime.UtcNow;
        if (now < _nextPollUtc) return;
        _nextPollUtc = now.AddSeconds(1);

        if (!_util.TryGetRegisterableVenue(_gameGui, out var addr, out _))
        {
            _lastKey = string.Empty;
            return;
        }

        if (addr.CanonicalKey == _lastKey) return;
        _lastKey = addr.CanonicalKey;

        if (_cfg.Current.VenueAskSuppressKeys.Contains(addr.CanonicalKey)) return;

        try
        {
            // If linked, invite service handles join UI
            var existing = await _api.VenueLookup(addr.CanonicalKey).ConfigureAwait(false);
            if (existing != null) return;

            var myUid = _api.UID;

            // User can open registration UI if they own OR moderate at least one Syncshell.
            // GroupPairs.Keys are GroupFullInfoDto, which carry GroupUserInfo.
            bool canManageAnyShell = _pairManager.GroupPairs.Keys.Any(g =>
                string.Equals(g.OwnerUID, myUid, StringComparison.OrdinalIgnoreCase) ||
                g.GroupUserInfo.IsModerator());

            if (!canManageAnyShell) return;

            // Debounce registration toggle
            if ((now - _lastUiOpenUtc).TotalMilliseconds < 750) return;
            _lastUiOpenUtc = now;

            // Open registration UI on Framework thread
            _util.RunOnFrameworkThread(() =>
                _mediator.Publish(new UiToggleMessage(typeof(VenueRegistrationUi))));
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "VenueRegistrationService polling failed");
        }
    }
}
