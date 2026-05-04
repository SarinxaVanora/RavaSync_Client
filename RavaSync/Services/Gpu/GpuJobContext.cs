using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace RavaSync.Services.Gpu;

public sealed class GpuJobContext : IDisposable
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private readonly Action<GpuJobResult> _onDispose;
    private bool _disposed;

    public GpuJobContext(string operationName, CancellationToken cancellationToken, Action<GpuJobResult> onDispose)
    {
        OperationName = operationName;
        CancellationToken = cancellationToken;
        _onDispose = onDispose;
        StartedAtUtc = DateTimeOffset.UtcNow;
    }

    public string OperationName { get; }
    public CancellationToken CancellationToken { get; }
    public DateTimeOffset StartedAtUtc { get; }
    public bool WasSuccessful { get; private set; }
    public string? Detail { get; private set; }

    public void CompleteSuccess(string? detail = null)
    {
        WasSuccessful = true;
        Detail = detail;
    }

    public void CompleteFailure(string? detail = null)
    {
        WasSuccessful = false;
        Detail = detail;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stopwatch.Stop();
        _onDispose(new GpuJobResult(OperationName, WasSuccessful, StartedAtUtc, _stopwatch.Elapsed, Detail));
    }
}

public sealed record GpuJobResult(string OperationName, bool WasSuccessful, DateTimeOffset StartedAtUtc, TimeSpan Duration, string? Detail);
internal sealed class GpuResourcePool
{
    public Rental RentBytes(int minimumLength) => new(ArrayPool<byte>.Shared.Rent(Math.Max(1, minimumLength)));

    internal readonly struct Rental : IDisposable
    {
        private readonly byte[]? _buffer;

        public Rental(byte[] buffer)
        {
            _buffer = buffer;
        }

        public byte[] Buffer => _buffer ?? Array.Empty<byte>();

        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer, clearArray: false);
            }
        }
    }
}

public sealed class GpuTelemetry
{
    private readonly ConcurrentQueue<GpuJobResult> _recentResults = new();
    private const int MaxResults = 32;

    public void Record(GpuJobResult result)
    {
        _recentResults.Enqueue(result);
        while (_recentResults.Count > MaxResults && _recentResults.TryDequeue(out _))
        {
        }
    }

    public IReadOnlyList<GpuJobResult> GetRecentResults()
        => _recentResults.ToArray().OrderByDescending(r => r.StartedAtUtc).ToList();
}