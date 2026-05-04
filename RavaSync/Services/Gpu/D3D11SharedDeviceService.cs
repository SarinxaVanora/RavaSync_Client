using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Vortice.Direct3D;
using Vortice.Direct3D11;
using static Vortice.Direct3D11.D3D11;

namespace RavaSync.Services.Gpu;

public sealed class D3D11SharedDeviceService : IDisposable
{
    private readonly ILogger<D3D11SharedDeviceService> _logger;
    private readonly GpuCapabilityService _capabilityService;
    private readonly Lazy<State?> _state;
    private bool _disposed;

    public D3D11SharedDeviceService(ILogger<D3D11SharedDeviceService> logger, GpuCapabilityService capabilityService)
    {
        _logger = logger;
        _capabilityService = capabilityService;
        _state = new Lazy<State?>(CreateState, isThreadSafe: true);
    }

    public bool IsAvailable => _state.Value != null;

    public State? TryGetState() => _state.Value;

    public Task<bool> WarmupAsync(CancellationToken token)
        => Task.Factory.StartNew(() =>
        {
            GpuWarmupThreading.TrySetCurrentThreadToBackgroundWarmupPriority();
            token.ThrowIfCancellationRequested();
            return _state.Value != null;
        }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private State? CreateState()
    {
        try
        {
            var snapshot = _capabilityService.Current;
            if (!snapshot.IsSupported)
                return null;

            var featureLevels = new[] { FeatureLevel.Level_11_1, FeatureLevel.Level_11_0 };
            var result = D3D11CreateDevice(
                null,
                DriverType.Hardware,
                DeviceCreationFlags.None,
                featureLevels,
                out ID3D11Device device,
                out FeatureLevel featureLevel,
                out ID3D11DeviceContext context);

            if (result.Failure)
            {
                _logger.LogWarning("Failed to create shared D3D11 device. HRESULT={hresult}", result.Code);
                return null;
            }

            _logger.LogInformation("Shared D3D11 device initialised at feature level {featureLevel}", featureLevel);
            return new State(device, context, featureLevel);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to initialise shared D3D11 device.");
            return null;
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        if (_state.IsValueCreated)
            _state.Value?.Dispose();
    }

    public sealed class State : IDisposable
    {
        public State(ID3D11Device device, ID3D11DeviceContext context, FeatureLevel featureLevel)
        {
            Device = device;
            Context = context;
            FeatureLevel = featureLevel;
        }

        public ID3D11Device Device { get; }
        public ID3D11DeviceContext Context { get; }
        public FeatureLevel FeatureLevel { get; }
        public SemaphoreSlim ContextLock { get; } = new(1, 1);

        public void Dispose()
        {
            ContextLock.Dispose();
            Context.Dispose();
            Device.Dispose();
        }
    }
}
