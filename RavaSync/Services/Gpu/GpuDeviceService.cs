using Microsoft.Extensions.Logging;
using System.Threading;

namespace RavaSync.Services.Gpu;

public sealed class GpuDeviceService
{
    private readonly ILogger<GpuDeviceService> _logger;
    private readonly GpuCapabilityService _capabilityService;
    private readonly GpuTelemetry _telemetry;

    public GpuDeviceService(ILogger<GpuDeviceService> logger, GpuCapabilityService capabilityService, GpuTelemetry telemetry)
    {
        _logger = logger;
        _capabilityService = capabilityService;
        _telemetry = telemetry;
    }

    public bool IsAvailable => _capabilityService.Current.IsSupported;

    public GpuJobContext? TryBeginJob(string operationName, CancellationToken cancellationToken, out string reason)
    {
        var snapshot = _capabilityService.Current;
        if (!snapshot.IsSupported)
        {
            reason = snapshot.SupportSummary;
            return null;
        }

        reason = string.Empty;
        _logger.LogTrace("Beginning GPU job scope for {operation}", operationName);
        return new GpuJobContext(operationName, cancellationToken, result =>
        {
            _telemetry.Record(result);
            _logger.LogTrace("GPU job scope completed for {operation}. Success={success}. Duration={duration}ms. Detail={detail}",
                result.OperationName, result.WasSuccessful, result.Duration.TotalMilliseconds, result.Detail ?? string.Empty);
        });
    }
}
