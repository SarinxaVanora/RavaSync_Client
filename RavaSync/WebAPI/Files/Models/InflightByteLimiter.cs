public sealed class InflightByteLimiter
{
    private readonly SemaphoreSlim _sem = new(1, 1);
    private long _available;
    private long _total;

    public InflightByteLimiter(long initialBudgetBytes)
    {
        _total = initialBudgetBytes;
        _available = initialBudgetBytes;
    }

    public async Task<long> AcquireAsync(long requested, CancellationToken ct)
    {
        if (requested <= 0) return 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            await _sem.WaitAsync(ct).ConfigureAwait(false);
            long grant = 0;
            if (_available > 0)
            {
                grant = Math.Min(requested, _available);
                _available -= grant;
            }
            _sem.Release();
            if (grant > 0) return grant;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
    }

    // NEW: non-blocking acquire — returns 0 immediately if nothing available
    public async Task<long> TryAcquireAsync(long requested, CancellationToken ct)
    {
        if (requested <= 0) return 0;
        await _sem.WaitAsync(ct).ConfigureAwait(false);
        long grant = 0;
        if (_available > 0)
        {
            grant = Math.Min(requested, _available);
            _available -= grant;
        }
        _sem.Release();
        return grant;
    }

    public async Task ReleaseAsync(long released)
    {
        if (released <= 0) return;
        await _sem.WaitAsync().ConfigureAwait(false);
        _available += released;
        if (_available > _total) _available = _total;
        _sem.Release();
    }

    public async Task SetBudgetAsync(long newBudget)
    {
        if (newBudget < 0) newBudget = 0;
        await _sem.WaitAsync().ConfigureAwait(false);
        var inUse = _total - _available;
        _total = newBudget;
        _available = Math.Max(0, _total - inUse);
        _sem.Release();
    }
}
