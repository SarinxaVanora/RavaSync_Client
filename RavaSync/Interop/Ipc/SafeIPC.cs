using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RavaSync.Interop.Ipc;

public class SafeIpc
{
    public static async Task<bool> TryRun(
        ILogger logger, string opName, TimeSpan timeout, Func<CancellationToken, Task> work,
        Action<Exception>? onError = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await work(cts.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException oce)
        {
            logger.LogWarning("IPC '{op}' timed out after {ms}ms: {msg}", opName, timeout.TotalMilliseconds, oce.Message);
            onError?.Invoke(oce);
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "IPC '{op}' threw: {msg}", opName, ex.Message);
            onError?.Invoke(ex);
            return false;
        }
    }

    public static async Task<(bool ok, T? value)> TryCall<T>(
        ILogger logger, string opName, TimeSpan timeout, Func<CancellationToken, Task<T>> work,
        Action<Exception>? onError = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var v = await work(cts.Token).ConfigureAwait(false);
            return (true, v);
        }
        catch (OperationCanceledException oce)
        {
            logger.LogWarning("IPC '{op}' timed out after {ms}ms: {msg}", opName, timeout.TotalMilliseconds, oce.Message);
            onError?.Invoke(oce);
            return (false, default);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "IPC '{op}' threw: {msg}", opName, ex.Message);
            onError?.Invoke(ex);
            return (false, default);
        }
    }
}
