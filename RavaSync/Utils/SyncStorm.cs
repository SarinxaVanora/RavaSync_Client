internal static class SyncStorm
{
    // RavaSync used to enter a storm mode when many pairs became visible at once.
    // That protected frame pacing, but it also serialized applies/download smoothing right
    // when first-visible state needed to land quickly. Keep the facade so call sites stay
    // simple, but make storm mode inert.
    public static bool IsActive => false;

    public static void RegisterVisibleNow()
    {
        // Intentionally no-op.
    }
}
