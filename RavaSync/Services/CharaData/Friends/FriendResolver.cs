using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Dalamud.Game.ClientState.Objects.Enums;
using Dalamud.Plugin.Services;
using FFXIVClientStructs.FFXIV.Client.Game.Character;
using Microsoft.Extensions.Logging;

namespace RavaSync.Services;

public sealed class FriendResolver : IFriendResolver, IDisposable
{
    private readonly ILogger<FriendResolver> _logger;
    private readonly IFramework _framework;
    private readonly IObjectTable _objects;

    // live set of visible-friend names
    private readonly ConcurrentDictionary<string, int> _friendNames = new(StringComparer.Ordinal);

    private int _friendScanId = 0;
    private long _nextScanAtTick = 0;
    private const int FriendScanIntervalMs = 250;


    public FriendResolver(
        ILogger<FriendResolver> logger,
        IFramework framework,
        IObjectTable objectTable)
    {
        _logger = logger;
        _framework = framework;
        _objects = objectTable;

        _framework.Update += OnFrameworkUpdate;
    }

    public bool IsFriend(string playerName)
        => !string.IsNullOrEmpty(playerName) && _friendNames.ContainsKey(playerName);

    private void OnFrameworkUpdate(IFramework _frm)
    {
        try
        {
            var nowTick = Environment.TickCount64;
            if (nowTick < _nextScanAtTick)
                return;

            _nextScanAtTick = nowTick + FriendScanIntervalMs;
            var scanId = unchecked(++_friendScanId);

            for (int i = 0; i < _objects.Length; i++)
            {
                var obj = _objects[i];
                if (obj is null || obj.ObjectKind != ObjectKind.Player)
                    continue;

                unsafe
                {
                    var ch = (Character*)obj.Address;
                    if (ch == null || !ch->IsFriend)
                        continue;
                }

                var name = obj.Name.ToString();
                if (string.IsNullOrEmpty(name))
                    continue;

                _friendNames[name] = scanId;
            }


            foreach (var kv in _friendNames)
            {
                if (kv.Value != scanId)
                    _friendNames.TryRemove(kv.Key, out _);
            }

        }
        catch (Exception ex)
        {
            // keep it quiet; we’ll try again next frame
            _logger.LogDebug(ex, "FriendResolver update failed (continuing).");
        }
    }

    public void Dispose()
    {
        _framework.Update -= OnFrameworkUpdate;
    }
}
