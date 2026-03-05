internal static class SyncStorm
{
    private static readonly object _gate = new();
    private static readonly Queue<long> _recentVisibleTicks = new();

    private const int StormHoldMs = 2200;

    private const int WindowMs = 1100;

    private const int TriggerThreshold = 12;

    private const int ExtendThreshold = 16;

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

            var isActive = now < _stormUntilTick;
            var threshold = isActive ? ExtendThreshold : TriggerThreshold;

            if (_recentVisibleTicks.Count >= threshold)
            {
                var until = now + StormHoldMs;
                if (until > _stormUntilTick)
                    _stormUntilTick = until;
            }
        }
    }
}
