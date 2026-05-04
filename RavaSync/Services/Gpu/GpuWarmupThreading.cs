using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Services.Gpu;

internal static class GpuWarmupThreading
{
    private const int WindowsThreadModeBackgroundBegin = 0x00010000;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetThreadPriority(IntPtr hThread, int nPriority);


    public static Task RunBackgroundWarmupAsync(Action action, CancellationToken token)
        => Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundWarmupPriority();
            token.ThrowIfCancellationRequested();
            action();
        }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    public static Task<T> RunBackgroundWarmupAsync<T>(Func<T> action, CancellationToken token)
        => Task.Factory.StartNew(() =>
        {
            TrySetCurrentThreadToBackgroundWarmupPriority();
            token.ThrowIfCancellationRequested();
            return action();
        }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    public static void TrySetCurrentThreadToBackgroundWarmupPriority()
    {
        try
        {
            Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
        }
        catch
        {
        }

        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                _ = SetThreadPriority(GetCurrentThread(), WindowsThreadModeBackgroundBegin);
        }
        catch
        {
        }
    }
}
