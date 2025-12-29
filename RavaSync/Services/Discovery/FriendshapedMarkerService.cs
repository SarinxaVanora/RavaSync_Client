using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Game.Gui.NamePlate;
using Dalamud.Game.Text.SeStringHandling;
using Dalamud.Plugin.Services;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using RavaSync.Services.Discovery;


namespace RavaSync.Services;

public class FriendshapedMarkerService : DisposableMediatorSubscriberBase
{
    private readonly INamePlateGui _namePlateGui;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareConfigService _config;
    private readonly ApiController _api;
    private readonly PairManager _pairManager;
    private readonly RavaDiscoveryService _discovery;

    private sealed class OnlineCacheEntry
    {
        public bool IsOnline { get; init; }
        public DateTime LastCheckedUtc { get; init; }
    }

    public FriendshapedMarkerService(ILogger<FriendshapedMarkerService> logger,MareMediator mediator,INamePlateGui namePlateGui,DalamudUtilService dalamudUtil,
        MareConfigService config,ApiController api,PairManager pairManager,RavaDiscoveryService discovery) : base(logger, mediator)
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
    }

    private void OnNamePlateUpdate(INamePlateUpdateContext context,IReadOnlyList<INamePlateUpdateHandler> handlers)
    {
        try
        {
            // cheap bails up front
            if (!_config.Current.EnableRavaDiscoveryPresence) return;
            if (!_config.Current.EnableRightClickMenus) return;
            if (!_config.Current.ShowFriendshapedHeart) return;
            if (_api.ServerState != ServerState.Connected) return;

            var selfPtr = _dalamudUtil.GetPlayerPtr();

            foreach (var handler in handlers)
            {
                var player = handler.PlayerCharacter as IPlayerCharacter;
                if (player == null)
                    continue;

                // don't tag ourselves
                if (selfPtr != IntPtr.Zero && selfPtr == player.Address)
                    continue;

                string? ident;
                try
                {
                    ident = _dalamudUtil.GetIdentFromGameObject(player);
                }
                catch (Exception ex)
                {
                    continue;
                }

                if (string.IsNullOrEmpty(ident))
                    continue;

                // already directly paired? no friendshaped marker
                if (_pairManager.IsIdentDirectlyPaired(ident))
                    continue;

                // purely in-memory check – no network here
                if (!_discovery.IsIdentKnownAsRavaUser(ident))
                    continue;

                // already has a heart? don't touch it
                var currentName = handler.Name;
                if (currentName.TextValue.Contains("♥"))
                    continue;

                var builder = new SeStringBuilder();
                builder.Append(currentName);
                builder.AddText(" ♥");
                handler.Name = builder.Build();
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error in FriendshapedMarkerService.OnNamePlateUpdate");
        }
    }
}
