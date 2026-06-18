using Dalamud.Utility;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Utils;

/// <summary>
/// Automatic Wine/Linux pressure control. Windows callers receive their original timings unchanged.
/// The pressure score rises only when framework IPC becomes expensive and decays while the game is calm,
/// allowing Linux to run at normal speed instead of paying permanent worst-case delays.
/// </summary>
public static class LinuxSmoothMode
{
    private const int MaxPressure = 100;
    private const int DecayIntervalMs = 125;
    private static int _pressure;
    private static long _lastDecayTick = Environment.TickCount64;
    private static long _lastFrameworkTick;

    public static bool IsActive { get; } = SafeIsWine();

    public static int Pressure => IsActive ? ReadDecayedPressure() : 0;


    public static void RecordFrameworkTick()
    {
        if (!IsActive)
            return;

        var now = Environment.TickCount64;
        while (true)
        {
            var last = Interlocked.Read(ref _lastFrameworkTick);
            if (last <= 0)
            {
                if (Interlocked.CompareExchange(ref _lastFrameworkTick, now, last) == last)
                    return;

                continue;
            }

            var elapsed = unchecked(now - last);
            if (elapsed < 8)
                return;

            if (Interlocked.CompareExchange(ref _lastFrameworkTick, now, last) != last)
                continue;

            if (elapsed >= 250)
                AdjustPressure(50);
            else if (elapsed >= 160)
                AdjustPressure(34);
            else if (elapsed >= 100)
                AdjustPressure(22);
            else if (elapsed >= 65)
                AdjustPressure(12);
            else if (elapsed <= 22)
                AdjustPressure(-2);

            return;
        }
    }

    public static int ComputeFrameworkIpcSpacing(int windowsSpacingMs, long elapsedMs, int maximumWineSpacingMs)
    {
        if (!IsActive)
            return windowsSpacingMs;

        RecordFrameworkWork(elapsedMs);
        var pressure = ReadDecayedPressure();
        var callPenalty = elapsedMs switch
        {
            <= 12 => 0,
            <= 30 => 8,
            <= 60 => 20,
            <= 120 => 40,
            <= 250 => 75,
            _ => 120,
        };

        return Math.Clamp(windowsSpacingMs + callPenalty + pressure, windowsSpacingMs, maximumWineSpacingMs);
    }

    public static int ComputeFrameworkYield(int windowsDelayMs)
    {
        if (!IsActive)
            return windowsDelayMs;

        var pressure = ReadDecayedPressure();
        return Math.Clamp(windowsDelayMs + pressure * 2, windowsDelayMs, Math.Max(windowsDelayMs, 260));
    }

    public static int ComputeDispatchSpacing(int windowsSpacingMs, int maximumWineSpacingMs)
    {
        if (!IsActive)
            return windowsSpacingMs;

        var pressure = ReadDecayedPressure();
        var extra = (maximumWineSpacingMs - windowsSpacingMs) * pressure / MaxPressure;
        return windowsSpacingMs + extra;
    }

    public static int ComputeIncomingCoalesceDelay()
    {
        if (!IsActive)
            return 0;

        return 60 + ReadDecayedPressure() * 2;
    }

    public static int ComputeReceiverApplyDispatchSpacing(bool lifecycleApply)
    {
        if (!IsActive)
            return lifecycleApply ? 125 : 35;

        var pressure = ReadDecayedPressure();
        var baseMs = lifecycleApply ? 125 : 35;
        var maxMs = lifecycleApply ? 900 : 450;
        var extra = (maxMs - baseMs) * pressure / MaxPressure;
        return baseMs + extra;
    }

    public static int ComputeCacheReadyRetryDelay(int windowsDelayMs)
    {
        if (!IsActive)
            return windowsDelayMs;

        var pressure = ReadDecayedPressure();
        return Math.Clamp(windowsDelayMs + pressure / 2, windowsDelayMs, 100);
    }

    public static int ComputeMaintenancePollDelay(int windowsDelayMs, int maximumWineDelayMs)
    {
        if (!IsActive)
            return windowsDelayMs;

        var pressure = ReadDecayedPressure();
        var extra = (maximumWineDelayMs - windowsDelayMs) * pressure / MaxPressure;
        return Math.Clamp(windowsDelayMs + extra, windowsDelayMs, Math.Max(windowsDelayMs, maximumWineDelayMs));
    }

    public static int ComputeActiveIndicatorValidationInterval(int windowsIntervalMs)
    {
        if (!IsActive)
            return windowsIntervalMs;

        var pressure = ReadDecayedPressure();
        return Math.Clamp(windowsIntervalMs + pressure * 15, windowsIntervalMs, 2500);
    }

    public static void RecordReceiverApplyPhase(string? phase, long elapsedMs)
    {
        if (!IsActive || elapsedMs <= 0)
            return;

        if (phase == null)
            return;

        // Cache/file readiness waits are mostly IO latency and should not make the framework
        // throttle harder. The expensive phases below are the ones that run Penumbra,
        // Glamourer, redraw, or framework target validation and can show up as Wine stutter.
        if (phase.Contains("Penumbra", StringComparison.OrdinalIgnoreCase)
            || phase.Contains("Glamourer", StringComparison.OrdinalIgnoreCase)
            || phase.Contains("Customization", StringComparison.OrdinalIgnoreCase)
            || phase.Contains("redraw", StringComparison.OrdinalIgnoreCase)
            || phase.Contains("reapply", StringComparison.OrdinalIgnoreCase))
        {
            RecordFrameworkWork(elapsedMs);

            if (elapsedMs >= 1500)
                AdjustPressure(25);
            else if (elapsedMs >= 750)
                AdjustPressure(14);
            else if (elapsedMs >= 350)
                AdjustPressure(7);
        }
    }

    public static ValueTask YieldBackgroundWorkIfNeededAsync(CancellationToken token)
    {
        if (!IsActive)
            return ValueTask.CompletedTask;

        var pressure = ReadDecayedPressure();
        if (pressure < 30)
            return ValueTask.CompletedTask;

        var delayMs = Math.Clamp((pressure - 20) / 2, 5, 40);
        return new ValueTask(Task.Delay(delayMs, token));
    }

    public static int ComputeBackgroundRepairDelay(int windowsDelayMs)
    {
        if (!IsActive)
            return windowsDelayMs;

        var pressure = ReadDecayedPressure();
        return Math.Clamp(windowsDelayMs + pressure * 20, windowsDelayMs, Math.Max(windowsDelayMs, 3500));
    }

    public static void RecordFrameworkWork(long elapsedMs)
    {
        if (!IsActive)
            return;

        ApplyDecay();
        var delta = elapsedMs switch
        {
            >= 250 => 45,
            >= 120 => 30,
            >= 70 => 20,
            >= 40 => 12,
            >= 25 => 6,
            <= 8 => -5,
            <= 15 => -2,
            _ => 0,
        };

        if (delta != 0)
            AdjustPressure(delta);
    }

    private static int ReadDecayedPressure()
    {
        ApplyDecay();
        return Math.Clamp(Volatile.Read(ref _pressure), 0, MaxPressure);
    }

    private static void ApplyDecay()
    {
        var now = Environment.TickCount64;

        while (true)
        {
            var last = Interlocked.Read(ref _lastDecayTick);
            var elapsed = now - last;
            if (elapsed < DecayIntervalMs)
                return;

            var steps = (int)Math.Min(MaxPressure, elapsed / DecayIntervalMs);
            var advanced = last + steps * DecayIntervalMs;
            if (Interlocked.CompareExchange(ref _lastDecayTick, advanced, last) != last)
                continue;

            AdjustPressure(-steps);
            return;
        }
    }

    private static void AdjustPressure(int delta)
    {
        while (true)
        {
            var current = Volatile.Read(ref _pressure);
            var next = Math.Clamp(current + delta, 0, MaxPressure);
            if (next == current || Interlocked.CompareExchange(ref _pressure, next, current) == current)
                return;
        }
    }

    private static bool SafeIsWine()
    {
        try { return Util.IsWine(); }
        catch { return false; }
    }
}
