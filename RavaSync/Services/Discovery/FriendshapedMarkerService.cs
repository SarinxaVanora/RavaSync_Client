using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Game.Gui.NamePlate;
using Dalamud.Game.Text.SeStringHandling;
using Dalamud.Plugin.Services;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RavaSync.Services;

public class FriendshapedMarkerService : DisposableMediatorSubscriberBase
{
    private readonly INamePlateGui _namePlateGui;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareConfigService _config;
    private readonly ApiController _api;
    private readonly PairManager _pairManager;
    private readonly RavaDiscoveryService _discovery;

    private readonly ConcurrentDictionary<nint, MarkerCacheEntry> _markerCache = new();

    private static readonly TimeSpan MarkerCacheTtl = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MarkerCacheCleanupInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MarkerCacheStaleTtl = TimeSpan.FromSeconds(10);
    private DateTime _lastMarkerCacheCleanupUtc = DateTime.MinValue;

    private sealed class MarkerCacheEntry
    {
        public bool ShouldMark { get; init; }
        public DateTime LastCheckedUtc { get; init; }
    }

    public FriendshapedMarkerService(
        ILogger<FriendshapedMarkerService> logger,
        MareMediator mediator,
        INamePlateGui namePlateGui,
        DalamudUtilService dalamudUtil,
        MareConfigService config,
        ApiController api,
        PairManager pairManager,
        RavaDiscoveryService discovery) : base(logger, mediator)
    {
        _namePlateGui = namePlateGui;
        _dalamudUtil = dalamudUtil;
        _config = config;
        _api = api;
        _pairManager = pairManager;
        _discovery = discovery;

        _namePlateGui.OnNamePlateUpdate += OnNamePlateUpdate;
        _namePlateGui.RequestRedraw();
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _namePlateGui.OnNamePlateUpdate -= OnNamePlateUpdate;
        _markerCache.Clear();
    }

    private void OnNamePlateUpdate(INamePlateUpdateContext context, IReadOnlyList<INamePlateUpdateHandler> handlers)
    {
        try
        {
            // Cheap bails first
            var cfg = _config.Current;
            if (!cfg.EnableRavaDiscoveryPresence) return;
            if (!cfg.EnableRightClickMenus) return;
            if (!cfg.ShowFriendshapedHeart) return;
            if (_api.ServerState != ServerState.Connected) return;

            var nowUtc = DateTime.UtcNow;
            var selfPtr = _dalamudUtil.GetPlayerPtr();

            foreach (var handler in handlers)
            {
                if (handler.PlayerCharacter is not IPlayerCharacter player)
                    continue;

                var addr = player.Address;
                if (addr == nint.Zero)
                    continue;

                // Don’t tag ourselves
                if (selfPtr != IntPtr.Zero && selfPtr == addr)
                    continue;

                // Cache marker eligibility for a short TTL
                if (!_markerCache.TryGetValue(addr, out var cached) || (nowUtc - cached.LastCheckedUtc) > MarkerCacheTtl)
                {
                    string? ident = null;
                    try
                    {
                        ident = _dalamudUtil.GetIdentFromGameObject(player);
                    }
                    catch
                    {
                        // Ignore per-player failures in the hot path
                    }

                    bool shouldMark = false;
                    if (!string.IsNullOrEmpty(ident))
                    {
                        // Don’t mark directly paired users
                        if (!_pairManager.IsIdentDirectlyPaired(ident))
                        {
                            // In-memory discovery check only
                            shouldMark = _discovery.IsIdentKnownAsRavaUser(ident);
                        }
                    }

                    cached = new MarkerCacheEntry
                    {
                        ShouldMark = shouldMark,
                        LastCheckedUtc = nowUtc
                    };

                    _markerCache[addr] = cached;
                }

                if (!cached.ShouldMark)
                    continue;

                // Avoid adding duplicate hearts
                var currentName = handler.Name;
                var textValue = currentName.TextValue;
                if (!string.IsNullOrEmpty(textValue) && textValue.Contains("♥", StringComparison.Ordinal))
                    continue;

                var builder = new SeStringBuilder();
                builder.Append(currentName);
                builder.AddText(" ♥");
                handler.Name = builder.Build();
            }

            // Very light timed cleanup to stop cache growing forever
            if ((nowUtc - _lastMarkerCacheCleanupUtc) > MarkerCacheCleanupInterval && _markerCache.Count > 256)
            {
                _lastMarkerCacheCleanupUtc = nowUtc;

                foreach (var kvp in _markerCache)
                {
                    if ((nowUtc - kvp.Value.LastCheckedUtc) > MarkerCacheStaleTtl)
                        _markerCache.TryRemove(kvp.Key, out _);
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error in FriendshapedMarkerService.OnNamePlateUpdate");
        }
    }
}