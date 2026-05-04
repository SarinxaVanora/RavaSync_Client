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

    private readonly ConcurrentDictionary<string, byte> _friendNames = new(StringComparer.Ordinal);
    private readonly HashSet<string> _scanNames = new(StringComparer.Ordinal);

    private int _scanCursor = -1;
    private long _nextScanAtTick;
    private const int FriendScanIntervalMs = 500;
    private const int FriendScanSliceSize = 24;


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
            if (_scanCursor < 0)
            {
                if (nowTick < _nextScanAtTick)
                    return;

                _scanCursor = 0;
                _scanNames.Clear();
            }

            var objectCount = _objects.Length;
            if (objectCount <= 0)
            {
                if (!_friendNames.IsEmpty)
                    _friendNames.Clear();

                _scanCursor = -1;
                _nextScanAtTick = nowTick + FriendScanIntervalMs;
                return;
            }

            var end = Math.Min(objectCount, _scanCursor + FriendScanSliceSize);
            for (int i = _scanCursor; i < end; i++)
            {
                var obj = _objects[i];
                if (obj is null || obj.ObjectKind != ObjectKind.Pc)
                    continue;

                unsafe
                {
                    var ch = (Character*)obj.Address;
                    if (ch == null || !ch->IsFriend)
                        continue;
                }

                var name = obj.Name.ToString();
                if (!string.IsNullOrEmpty(name))
                    _scanNames.Add(name);
            }

            _scanCursor = end;
            if (_scanCursor < objectCount)
                return;

            foreach (var name in _scanNames)
                _friendNames[name] = 0;

            foreach (var kv in _friendNames)
            {
                if (!_scanNames.Contains(kv.Key))
                    _friendNames.TryRemove(kv.Key, out _);
            }

            _scanCursor = -1;
            _nextScanAtTick = nowTick + FriendScanIntervalMs;
        }
        catch (Exception ex)
        {
            _scanCursor = -1;
            _nextScanAtTick = Environment.TickCount64 + FriendScanIntervalMs;
            _logger.LogDebug(ex, "FriendResolver update failed (continuing).");
        }
    }

    public void Dispose()
    {
        _framework.Update -= OnFrameworkUpdate;
    }
}
