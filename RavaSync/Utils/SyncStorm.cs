using System;
using System.Threading;

internal static class SyncStorm
{
    private static readonly object Gate = new();
    private static long _windowStartTick;
    private static int _visibleCountInWindow;
    private static long _activeUntilTick;

    private static int _queuedDownloads;
    private static int _activeDownloads;
    private static int _queuedApplies;
    private static int _activeApplies;

    private const int WindowMs = 2500;
    private const int ActivationVisibleCount = 5;
    private const int ActiveDurationMs = 22000;
    private const int BacklogActivationCount = 4;
    private const int DownloadPressureCount = 2;

    public static int QueuedDownloads => Math.Max(0, Volatile.Read(ref _queuedDownloads));
    public static int ActiveDownloads => Math.Max(0, Volatile.Read(ref _activeDownloads));
    public static int QueuedApplies => Math.Max(0, Volatile.Read(ref _queuedApplies));
    public static int ActiveApplies => Math.Max(0, Volatile.Read(ref _activeApplies));
    public static int TotalBacklog => QueuedDownloads + ActiveDownloads + QueuedApplies + ActiveApplies;

    public static bool IsActive
    {
        get
        {
            var timerActive = unchecked(Environment.TickCount64 - Volatile.Read(ref _activeUntilTick)) < 0;
            if (timerActive) return true;

            // Do not relax merely because the original timer expired. A room-entry burst can
            // legitimately still have queued downloads/applies after 22s, especially on a slow
            // disk or when several pairs share large assets. Keep storm pacing while there is
            // real backlog, then return to normal speed as soon as pressure drains.
            return TotalBacklog >= BacklogActivationCount;
        }
    }

    public static bool IsDownloadPressureHigh => IsActive || (QueuedDownloads + ActiveDownloads) >= DownloadPressureCount;
    public static bool IsApplyPressureHigh => IsActive || (QueuedApplies + ActiveApplies) >= BacklogActivationCount;

    public static void RegisterVisibleNow()
    {
        var now = Environment.TickCount64;

        lock (Gate)
        {
            if (_windowStartTick == 0 || unchecked(now - _windowStartTick) > WindowMs)
            {
                _windowStartTick = now;
                _visibleCountInWindow = 0;
            }

            _visibleCountInWindow++;

            if (_visibleCountInWindow >= ActivationVisibleCount)
                ExtendActiveUntil(now + ActiveDurationMs);
        }
    }

    public static void RegisterDownloadQueued()
    {
        Interlocked.Increment(ref _queuedDownloads);
        ExtendActiveForBacklog();
    }

    public static void RegisterDownloadQueueCancelled()
    {
        ClampAfterDecrement(ref _queuedDownloads);
    }

    public static void RegisterDownloadStarted()
    {
        ClampAfterDecrement(ref _queuedDownloads);
        Interlocked.Increment(ref _activeDownloads);
        ExtendActiveForBacklog();
    }

    public static void RegisterDownloadFinished()
    {
        ClampAfterDecrement(ref _activeDownloads);
    }

    public static void RegisterApplyQueued()
    {
        Interlocked.Increment(ref _queuedApplies);
        ExtendActiveForBacklog();
    }

    public static void RegisterApplyQueueCancelled()
    {
        ClampAfterDecrement(ref _queuedApplies);
    }

    public static void RegisterApplyStarted()
    {
        ClampAfterDecrement(ref _queuedApplies);
        Interlocked.Increment(ref _activeApplies);
        ExtendActiveForBacklog();
    }

    public static void RegisterApplyFinished()
    {
        ClampAfterDecrement(ref _activeApplies);
    }

    private static void ExtendActiveForBacklog()
    {
        if (TotalBacklog < BacklogActivationCount)
            return;

        ExtendActiveUntil(Environment.TickCount64 + ActiveDurationMs);
    }

    private static void ExtendActiveUntil(long activeUntil)
    {
        while (true)
        {
            var current = Volatile.Read(ref _activeUntilTick);
            if (unchecked(activeUntil - current) <= 0)
                return;

            if (Interlocked.CompareExchange(ref _activeUntilTick, activeUntil, current) == current)
                return;
        }
    }

    private static void ClampAfterDecrement(ref int value)
    {
        var after = Interlocked.Decrement(ref value);
        if (after < 0)
            Interlocked.Exchange(ref value, 0);
    }
}
