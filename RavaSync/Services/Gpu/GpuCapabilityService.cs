using Microsoft.Extensions.Logging;
using System;
using System.Runtime.InteropServices;

namespace RavaSync.Services.Gpu;

public sealed record GpuCapabilitySnapshot(bool IsWindows, bool Is64BitProcess, bool HasD3D11, bool HasDxgi, bool HasD3DCompiler, bool IsSupported, string SupportSummary, string RuntimeSummary, DateTimeOffset CheckedAtUtc);
public sealed class GpuCapabilityService
{
    private readonly ILogger<GpuCapabilityService> _logger;
    private readonly Lazy<GpuCapabilitySnapshot> _snapshot;

    public GpuCapabilityService(ILogger<GpuCapabilityService> logger)
    {
        _logger = logger;
        _snapshot = new Lazy<GpuCapabilitySnapshot>(Probe, isThreadSafe: true);
    }

    public GpuCapabilitySnapshot Current => _snapshot.Value;

    private GpuCapabilitySnapshot Probe()
    {
        bool isWindows = OperatingSystem.IsWindows();
        bool is64Bit = Environment.Is64BitProcess;
        bool hasD3d11 = isWindows && CanLoad("d3d11.dll");
        bool hasDxgi = isWindows && CanLoad("dxgi.dll");
        bool hasCompiler = isWindows && (CanLoad("d3dcompiler_47.dll") || CanLoad("d3dcompiler_43.dll"));

        bool supported = isWindows && is64Bit && hasD3d11 && hasDxgi && hasCompiler;

        string summary = supported
            ? "GPU foundation can be enabled for a local D3D11-backed compute path with shader compilation support."
            : BuildUnsupportedSummary(isWindows, is64Bit, hasD3d11, hasDxgi, hasCompiler);

        string runtime = $"OS={(isWindows ? "Windows" : "Non-Windows")}, Process={(is64Bit ? "x64" : "x86")}, D3D11={(hasD3d11 ? "yes" : "no")}, DXGI={(hasDxgi ? "yes" : "no")}, Compiler={(hasCompiler ? "yes" : "no")}";

        var snapshot = new GpuCapabilitySnapshot(isWindows,is64Bit,hasD3d11,hasDxgi,hasCompiler,supported,summary,runtime,DateTimeOffset.UtcNow);

        _logger.LogInformation("GPU capability probe complete. Supported={supported}. {runtime}. {summary}",
            snapshot.IsSupported, snapshot.RuntimeSummary, snapshot.SupportSummary);

        return snapshot;
    }

    private static bool CanLoad(string library)
    {
        IntPtr handle = IntPtr.Zero;
        try
        {
            return NativeLibrary.TryLoad(library, out handle);
        }
        catch
        {
            return false;
        }
        finally
        {
            if (handle != IntPtr.Zero)
            {
                try { NativeLibrary.Free(handle); } catch { }
            }
        }
    }

    private static string BuildUnsupportedSummary(bool isWindows, bool is64Bit, bool hasD3d11, bool hasDxgi, bool hasCompiler)
    {
        if (!isWindows) return "GPU compute path is disabled because this runtime is not Windows.";
        if (!is64Bit) return "GPU compute path is disabled because the process is not 64-bit.";
        if (!hasD3d11) return "GPU compute path is disabled because d3d11.dll was not available.";
        if (!hasDxgi) return "GPU compute path is disabled because dxgi.dll was not available.";
        if (!hasCompiler) return "GPU compute path can be analysed, but shader compilation helpers were not detected yet.";
        return "GPU compute path is disabled.";
    }
}
