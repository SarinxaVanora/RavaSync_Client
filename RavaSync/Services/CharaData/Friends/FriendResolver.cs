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

    // live set of visible-friend names (case-sensitive like MiniMappingway)
    private readonly ConcurrentDictionary<string, byte> _friendNames = new(StringComparer.Ordinal);

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

    private void OnFrameworkUpdate(IFramework _)
    {
        try
        {
            // Rebuild the set from what's currently visible
            var seen = new HashSet<string>(StringComparer.Ordinal);

            for (int i = 0; i < _objects.Length; i++)
            {
                var obj = _objects[i];
                if (obj is null || obj.ObjectKind != ObjectKind.Player)
                    continue;

                var name = obj.Name.ToString();
                if (string.IsNullOrEmpty(name))
                    continue;

                unsafe
                {
                    // MiniMappingway casts to Character* and reads IsFriend
                    var ch = (Character*)obj.Address;
                    if (ch != null && ch->IsFriend)
                        seen.Add(name);
                }
            }

            // minimal churn: remove those no longer friends this frame
            foreach (var existing in _friendNames.Keys)
            {
                if (!seen.Contains(existing))
                    _friendNames.TryRemove(existing, out byte _);
            }

            // add new ones
            foreach (var n in seen)
                _friendNames[n] = 1;
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
