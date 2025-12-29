using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Game.Gui.ContextMenu;
using Dalamud.Game.Text.SeStringHandling;
using Dalamud.Plugin.Services;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR.Utils;
using Microsoft.Extensions.Logging;
using System;
using RavaSync.Services.Discovery;


namespace RavaSync.Services;

public class PairRequestContextMenuService : DisposableMediatorSubscriberBase
{
    private readonly IContextMenu _contextMenu;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareConfigService _config;
    private readonly ApiController _api;
    private readonly PairManager _pairManager;
    private readonly RavaDiscoveryService _discovery;

    public PairRequestContextMenuService(
        ILogger<PairRequestContextMenuService> logger,
        MareMediator mediator,
        IContextMenu contextMenu,
        DalamudUtilService dalamudUtil,
        MareConfigService config,
        ApiController api,
        PairManager pairManager,
        RavaDiscoveryService discovery)
        : base(logger, mediator)
    {
        _contextMenu = contextMenu;
        _dalamudUtil = dalamudUtil;
        _config = config;
        _api = api;
        _pairManager = pairManager;

        _contextMenu.OnMenuOpened += OnMenuOpened;
        _discovery = discovery;
    }


    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _contextMenu.OnMenuOpened -= OnMenuOpened;
    }

    private void OnMenuOpened(IMenuOpenedArgs args)
    {
        try
        {
            if (!_config.Current.EnableRavaDiscoveryPresence) return;
            if (args.Target is not MenuTargetDefault target) return;
            if (target.TargetObject is not IPlayerCharacter player) return;

            // Don’t show on ourselves
            var selfPtr = _dalamudUtil.GetPlayerPtr();
            if (selfPtr != IntPtr.Zero && selfPtr == player.Address)
                return;

            var entityId = (uint)target.TargetObjectId;

            if (!ShouldOfferPairRequestMenu(entityId, out var ident))
                return;

            var label = new SeStringBuilder()
                .AddText("Send pair request")
                .Build();

            args.AddMenuItem(new MenuItem
            {
                Name = label,
                OnClicked = _ =>
                {
                    // Pass the ident we already resolved and gated on
                    Mediator.Publish(new ContextMenuPairRequestMessage(ident, target.TargetName));
                },
                UseDefaultPrefix = false,
                PrefixChar = 'R',
                PrefixColor = 708
            });
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error in pair request context menu");
        }
    }


    public bool ShouldOfferPairRequestMenu(uint entityId, out string ident)
    {
        ident = string.Empty;

        // Global checks
        if (!_config.Current.EnableRavaDiscoveryPresence) return false;
        if (!_config.Current.EnableRightClickMenus) return false;
        if (!_config.Current.EnableSendPairRequestContextMenu) return false;
        if (_api.ServerState != ServerState.Connected) return false;

        // Resolve ident from entity id on framework thread
        string id;
        try
        {
            id = _dalamudUtil.GetIdentFromEntityId(entityId);
        }
        catch
        {
            return false;
        }

        if (string.IsNullOrEmpty(id)) return false;

        // Not if we’re already directly paired
        if (_pairManager.IsIdentDirectlyPaired(id)) return false;

        if (!_discovery.IsIdentKnownAsRavaUser(id)) return false;

        ident = id;
        return true;
    }

}
