using System;
using System.Collections.Generic;

namespace RavaSync.Utils;

internal static class SyncStorm
{
    private static readonly object _gate = new();
    private static readonly Queue<long> _recentVisibleTicks = new();

    // Storm stays active for this many ms after we detect it
    private const int StormHoldMs = 2500;

    // Count how many "became visible" events in this window
    private const int WindowMs = 1000;

    // Threshold to consider it a storm
    private const int Threshold = 8;

    private static long _stormUntilTick;

    public static bool IsActive => Environment.TickCount64 < Volatile.Read(ref _stormUntilTick);

    public static void RegisterVisibleNow()
    {
        var now = Environment.TickCount64;

        lock (_gate)
        {
            _recentVisibleTicks.Enqueue(now);

            // drop old
            while (_recentVisibleTicks.Count > 0 && (now - _recentVisibleTicks.Peek()) > WindowMs)
                _recentVisibleTicks.Dequeue();

            if (_recentVisibleTicks.Count >= Threshold)
            {
                var until = now + StormHoldMs;
                var cur = _stormUntilTick;
                if (until > cur)
                    _stormUntilTick = until;
            }
        }
    }
}
