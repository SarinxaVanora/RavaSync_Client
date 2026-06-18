internal static class SyncStorm
{
    // SyncStorm is intentionally disabled.
    // Keep this shim so existing call-sites compile, but never switch RavaSync into
    // storm pacing, reduced download lanes, pressure spool limits, or delayed apply handoffs.
    public static int QueuedDownloads => 0;
    public static int ActiveDownloads => 0;
    public static int QueuedApplies => 0;
    public static int ActiveApplies => 0;
    public static int TotalBacklog => 0;

    public static bool IsActive => false;
    public static bool IsDownloadPressureHigh => false;
    public static bool IsApplyPressureHigh => false;

    public static void RegisterVisibleNow()
    {
    }

    public static void RegisterDownloadQueued()
    {
    }

    public static void RegisterDownloadQueueCancelled()
    {
    }

    public static void RegisterDownloadStarted()
    {
    }

    public static void RegisterDownloadFinished()
    {
    }

    public static void RegisterApplyQueued()
    {
    }

    public static void RegisterApplyQueueCancelled()
    {
    }

    public static void RegisterApplyStarted()
    {
    }

    public static void RegisterApplyFinished()
    {
    }
}
