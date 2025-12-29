using Dalamud.Plugin.Services;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.UI;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using System;
using System.Globalization;
using System.Linq;

namespace RavaSync.Services;

internal static class VenueKeyUtils
{
    // Single logical key type for whole housing plot
    public const string PlotAreaKind = "EstatePlot";

    /// <summary>
    /// Normalises a canonical housing key so that all locations belonging to the same plot
    /// (interior, rooms, garden) collapse to the same "plot key".
    /// </summary>
    public static string ToPlotKey(string canonicalKey)
    {
        if (string.IsNullOrEmpty(canonicalKey)) return canonicalKey;

        var parts = canonicalKey.Split(':');
        if (parts.Length != 6)
            return canonicalKey; // defensive, don't blow up on weird data

        // [0] ServerId
        // [1] TerritoryId
        // [2] AreaKind (EstateInterior / EstateRoom / whatever)
        // [3] WardId
        // [4] HouseId
        // [5] RoomId

        parts[2] = PlotAreaKind;
        parts[5] = "0"; // room-less, "whole plot" view

        return string.Join(":", parts);
    }
}

public sealed class VenueInviteService : IDisposable
{
    private readonly ILogger<VenueInviteService> _log;
    private readonly DalamudUtilService _util;
    private readonly IFramework _framework;
    private readonly IGameGui _gameGui;
    private readonly MareConfigService _cfg;
    private readonly ApiController _api;
    private readonly MareMediator _mediator;
    private readonly PairManager _pairManager;

    private string _lastPlotKey = string.Empty;
    private DateTime _nextPollUtc = DateTime.MinValue;

    public VenueInviteService(
        ILogger<VenueInviteService> logger,
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
        try
        {
            var now = DateTime.UtcNow;
            if (now < _nextPollUtc) return;
            _nextPollUtc = now.AddSeconds(1);

            // Optional guard: don't spam lookups if we're not connected
            if (!_api.IsConnected)
            {
                _lastPlotKey = string.Empty;
                return;
            }

            var map = _util.GetMapData(); // Framework thread

            // Any housing plot (interior, room, or garden)
            bool onHousingPlot = map.HouseId != 0;
            if (!onHousingPlot)
            {
                _lastPlotKey = string.Empty;
                return;
            }

            // This is the legacy/server key. Do not change this format.
            string areaKind = map.RoomId != 0 ? "EstateRoom" : "EstateInterior";
            string locationKey = string.Format(
                CultureInfo.InvariantCulture,
                "{0}:{1}:{2}:{3}:{4}:{5}",
                map.ServerId, map.TerritoryId, areaKind, map.WardId, map.HouseId, map.RoomId);

            // Plot key is local-only grouping.
            string plotKey = VenueKeyUtils.ToPlotKey(locationKey);

            // Only react when we move to a new plot (garden + interior share one plot key).
            if (plotKey == _lastPlotKey) return;
            _lastPlotKey = plotKey;

            // Respect suppression for both forms
            if (_cfg.Current.VenueAskSuppressKeys.Contains(plotKey)
                || _cfg.Current.VenueAskSuppressKeys.Contains(locationKey))
            {
                return;
            }

            var info = await _api.VenueLookup(locationKey).ConfigureAwait(false);
            if (info == null) return;

            if (!info.AutoInviteEnabled) return;

            bool alreadyInShell = _pairManager.GroupPairs.Keys
                .Any(g => string.Equals(g.GID, info.ShellGid, StringComparison.OrdinalIgnoreCase));
            if (alreadyInShell) return;

            // For UI/suppression we pass the plot key so it "covers" interior + garden.
            var uiKey = plotKey;
            _mediator.Publish(new OpenVenueJoinUiMessage(uiKey, info.DisplayName, info.ShellGid));
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "[RavaSync] VenueInviteService polling failed");
        }
    }
}
