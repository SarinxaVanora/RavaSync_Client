using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Vortice.Direct3D11;
using Vortice.Direct3D;
using Vortice.DXGI;

namespace RavaSync.Services.Gpu;

public sealed partial class D3D11MeshAnalysisService : IDisposable
{
    private const string TriangleScanShaderSource = """
cbuffer TriangleScanConstants : register(b0)
{
    uint TriangleCount;
    uint PositionCount;
    uint HasPositions;
    uint Padding;
};

StructuredBuffer<uint> Indices : register(t0);
StructuredBuffer<float3> Positions : register(t1);

struct TriangleMetric
{
    uint Flags;
    uint KeyLow;
    uint KeyHigh;
    uint Reserved;
    float4 NormalAndArea;
};

RWStructuredBuffer<TriangleMetric> Output : register(u0);

static const uint FlagDegenerate = 1u;
static const uint FlagPositionOutOfRange = 2u;
static const uint FlagAreaValid = 4u;

uint2 PackRotation(uint a, uint b, uint c)
{
    uint hi = a & 0xFFFFu;
    uint lo = ((b & 0xFFFFu) << 16) | (c & 0xFFFFu);
    return uint2(lo, hi);
}

bool LessThanKey(uint2 left, uint2 right)
{
    return left.y < right.y || (left.y == right.y && left.x < right.x);
}

uint2 OrientedKey(uint a, uint b, uint c)
{
    uint2 k0 = PackRotation(a, b, c);
    uint2 k1 = PackRotation(b, c, a);
    uint2 k2 = PackRotation(c, a, b);

    uint2 best = LessThanKey(k1, k0) ? k1 : k0;
    return LessThanKey(k2, best) ? k2 : best;
}

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint triIndex = threadId.x;
    if (triIndex >= TriangleCount)
        return;

    uint baseIndex = triIndex * 3u;
    uint a = Indices[baseIndex + 0u];
    uint b = Indices[baseIndex + 1u];
    uint c = Indices[baseIndex + 2u];

    uint flags = 0u;
    if (a == b || b == c || a == c)
        flags |= FlagDegenerate;

    uint2 key = OrientedKey(a, b, c);

    float4 normalAndArea = float4(0.0f, 0.0f, 0.0f, 0.0f);
    if (HasPositions != 0u)
    {
        if (a < PositionCount && b < PositionCount && c < PositionCount)
        {
            float3 pa = Positions[a];
            float3 pb = Positions[b];
            float3 pc = Positions[c];
            float3 crossValue = cross(pb - pa, pc - pa);
            float area2 = length(crossValue);
            if (area2 > 1.0e-12f)
            {
                normalAndArea = float4(normalize(crossValue), area2);
                flags |= FlagAreaValid;
            }
        }
        else
        {
            flags |= FlagPositionOutOfRange;
        }
    }

    TriangleMetric metric;
    metric.Flags = flags;
    metric.KeyLow = key.x;
    metric.KeyHigh = key.y;
    metric.Reserved = 0u;
    metric.NormalAndArea = normalAndArea;
    Output[triIndex] = metric;
}
""";

    public const uint TriangleFlagDegenerate = 1u;
    public const uint TriangleFlagPositionOutOfRange = 2u;
    public const uint TriangleFlagAreaValid = 4u;

    private readonly ILogger<D3D11MeshAnalysisService> _logger;
    private readonly D3D11SharedDeviceService _sharedDeviceService;
    private readonly D3D11ShaderBytecodeCache _shaderBytecodeDiskCache;
    private readonly SemaphoreSlim _queue = new(1, 1);
    private readonly ConcurrentDictionary<string, byte[]> _shaderCache = new(StringComparer.Ordinal);
    private bool _disposed;

    public D3D11MeshAnalysisService(ILogger<D3D11MeshAnalysisService> logger, D3D11SharedDeviceService sharedDeviceService, D3D11ShaderBytecodeCache shaderBytecodeDiskCache)
    {
        _logger = logger;
        _sharedDeviceService = sharedDeviceService;
        _shaderBytecodeDiskCache = shaderBytecodeDiskCache;
    }

    public bool IsAvailable => _sharedDeviceService.IsAvailable;


    public async Task<bool> WarmupAsync(CancellationToken token)
    {
        if (!await _sharedDeviceService.WarmupAsync(token).ConfigureAwait(false))
            return false;

        await GpuWarmupThreading.RunBackgroundWarmupAsync(() =>
        {
            var shaderKeys = new[]
            {
                "mesh-triangle-scan-v1",
                "mesh-body-proximity-v1",
                "mesh-attribute-protection-v1",
                "mesh-protected-expand-v1",
                "mesh-average-edge-length-v1",
                "mesh-collapse-candidate-score-v2",
            };

            foreach (var shaderKey in shaderKeys)
            {
                token.ThrowIfCancellationRequested();
                _ = GetMeshShaderBytecode(shaderKey);
                Thread.Yield();
            }
        }, token).ConfigureAwait(false);

        return true;
    }

    public bool TryScanTriangles(ReadOnlySpan<uint> indices, ReadOnlySpan<Vector3> positions, out TriangleScanMetric[] metrics)
    {
        metrics = [];

        if (indices.Length == 0 || indices.Length % 3 != 0)
            return false;

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return false;

        _queue.Wait();
        state.ContextLock.Wait();
        try
        {
            return ExecuteTriangleScan(state, indices, positions, out metrics);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "GPU mesh triangle scan failed; falling back to CPU analysis.");
            metrics = [];
            return false;
        }
        finally
        {
            state.ContextLock.Release();
            _queue.Release();
        }
    }

    private bool ExecuteTriangleScan(D3D11SharedDeviceService.State state, ReadOnlySpan<uint> indices, ReadOnlySpan<Vector3> positions, out TriangleScanMetric[] metrics)
    {
        metrics = [];

        int triangleCount = indices.Length / 3;
        if (triangleCount <= 0)
            return false;

        var shaderBytecode = GetMeshShaderBytecode("mesh-triangle-scan-v1");
        using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

        using var indexBuffer = state.Device.CreateBuffer(indices,
            BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            structureByteStride: sizeof(uint));
        using var indexSrv = CreateStructuredBufferSrv(state.Device, indexBuffer, indices.Length);

        ID3D11Buffer? positionBuffer = null;
        ID3D11ShaderResourceView? positionSrv = null;
        try
        {
            if (!positions.IsEmpty)
            {
                positionBuffer = state.Device.CreateBuffer(positions,
                    BindFlags.ShaderResource,
                    ResourceUsage.Default,
                    CpuAccessFlags.None,
                    ResourceOptionFlags.BufferStructured,
                    structureByteStride: (uint)Marshal.SizeOf<Vector3>());
                positionSrv = CreateStructuredBufferSrv(state.Device, positionBuffer, positions.Length);
            }

            using var constants = CreateConstantBuffer(state.Device, new TriangleScanConstants(
                (uint)triangleCount,
                (uint)positions.Length,
                positions.IsEmpty ? 0u : 1u,
                0u));

            uint outputStride = (uint)Marshal.SizeOf<TriangleMetricRaw>();
            uint outputByteWidth = checked((uint)(triangleCount * (int)outputStride));
            using var outputBuffer = state.Device.CreateBuffer(
                outputByteWidth,
                BindFlags.UnorderedAccess | BindFlags.ShaderResource,
                ResourceUsage.Default,
                CpuAccessFlags.None,
                ResourceOptionFlags.BufferStructured,
                outputStride);
            using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, triangleCount);
            using var stagingBuffer = state.Device.CreateBuffer(
                outputByteWidth,
                BindFlags.None,
                ResourceUsage.Staging,
                CpuAccessFlags.Read,
                ResourceOptionFlags.None,
                0);

            state.Context.CSSetShader(computeShader);
            state.Context.CSSetConstantBuffer(0, constants);
            state.Context.CSSetShaderResource(0, indexSrv);
            state.Context.CSSetShaderResource(1, positionSrv);
            state.Context.CSSetUnorderedAccessView(0, outputUav);
            state.Context.Dispatch((uint)((triangleCount + 63) / 64), 1, 1);
            state.Context.Flush();
            state.Context.CopyResource(stagingBuffer, outputBuffer);

            var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
            try
            {
                var result = new TriangleScanMetric[triangleCount];
                unsafe
                {
                    var rawSpan = new ReadOnlySpan<TriangleMetricRaw>((void*)mapped.DataPointer, triangleCount);
                    for (int i = 0; i < rawSpan.Length; i++)
                    {
                        var raw = rawSpan[i];
                        ulong key = ((ulong)raw.KeyHigh << 32) | raw.KeyLow;
                        result[i] = new TriangleScanMetric(raw.Flags, key, raw.NormalAndArea);
                    }
                }

                metrics = result;
                return true;
            }
            finally
            {
                state.Context.Unmap(stagingBuffer, 0);
                state.Context.CSSetShader(null);
                state.Context.CSSetConstantBuffer(0, null);
                state.Context.CSSetShaderResource(0, null);
                state.Context.CSSetShaderResource(1, null);
                state.Context.CSSetUnorderedAccessView(0, null);
            }
        }
        finally
        {
            positionSrv?.Dispose();
            positionBuffer?.Dispose();
        }
    }


    private static ID3D11ShaderResourceView CreateStructuredBufferSrv(ID3D11Device device, ID3D11Buffer buffer, int count)
    {
        var desc = new ShaderResourceViewDescription
        {
            Format = Format.Unknown,
            ViewDimension = ShaderResourceViewDimension.Buffer,
            Buffer = new BufferShaderResourceView
            {
                FirstElement = 0,
                NumElements = (uint)count,
            },
        };

        return device.CreateShaderResourceView(buffer, desc);
    }

    private static ID3D11UnorderedAccessView CreateStructuredBufferUav(ID3D11Device device, ID3D11Buffer buffer, int count)
    {
        var desc = new UnorderedAccessViewDescription
        {
            Format = Format.Unknown,
            ViewDimension = UnorderedAccessViewDimension.Buffer,
            Buffer = new BufferUnorderedAccessView
            {
                FirstElement = 0,
                NumElements = (uint)count,
                Flags = BufferUnorderedAccessViewFlags.None,
            },
        };

        return device.CreateUnorderedAccessView(buffer, desc);
    }

    private static ID3D11Buffer CreateConstantBuffer(ID3D11Device device, TriangleScanConstants constants)
    {
        var description = new BufferDescription(
            (uint)Marshal.SizeOf<TriangleScanConstants>(),
            BindFlags.ConstantBuffer,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.None,
            0);

        unsafe
        {
            var initialData = new SubresourceData((IntPtr)(&constants), 0, 0);
            return device.CreateBuffer(description, initialData);
        }
    }

    private byte[] GetMeshShaderBytecode(string shaderKey)
        => _shaderCache.GetOrAdd(shaderKey, key =>
        {
            var source = key switch
            {
                "mesh-triangle-scan-v1" => TriangleScanShaderSource,
                "mesh-body-proximity-v1" => BodyProximityShaderSource,
                "mesh-attribute-protection-v1" => AttributeProtectionShaderSource,
                "mesh-protected-expand-v1" => ProtectedExpandShaderSource,
                "mesh-average-edge-length-v1" => AverageEdgeLengthShaderSource,
                "mesh-collapse-candidate-score-v2" => CollapseCandidateScoreShaderSource,
                _ => throw new InvalidOperationException($"Unexpected mesh shader key: {key}"),
            };

            return _shaderBytecodeDiskCache.GetOrCompile($"mesh-analysis-{key}", source, () => CompileMeshShaderSource(key, source));
        });

    private static byte[] CompileMeshShaderSource(string shaderKey, string source)
    {
        var hresult = TryCompile(source, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 mesh analysis shader compilation failed for {shaderKey} with HRESULT 0x{hresult:X8}."
                : errors);

        return bytecode;
    }

    private static int TryCompile(string shaderSource, out byte[]? bytecode, out string? errors)
    {
        bytecode = null;
        errors = null;

        IntPtr shaderBlob = IntPtr.Zero;
        IntPtr errorBlob = IntPtr.Zero;
        try
        {
            int hr;
            try
            {
                hr = D3DCompile47(shaderSource, (nuint)shaderSource.Length, null, IntPtr.Zero, IntPtr.Zero, "main", "cs_5_0", 0u, 0u, out shaderBlob, out errorBlob);
            }
            catch (DllNotFoundException)
            {
                hr = unchecked((int)0x8007007E);
            }

            if (hr < 0)
            {
                if (errorBlob != IntPtr.Zero)
                    errors = ReadBlob(errorBlob);

                if (hr == unchecked((int)0x8007007E))
                {
                    ReleaseBlob(errorBlob);
                    errorBlob = IntPtr.Zero;
                    hr = D3DCompile43(shaderSource, (nuint)shaderSource.Length, null, IntPtr.Zero, IntPtr.Zero, "main", "cs_5_0", 0u, 0u, out shaderBlob, out errorBlob);
                }
            }

            if (hr >= 0 && shaderBlob != IntPtr.Zero)
            {
                bytecode = ReadBlobBytes(shaderBlob);
                if (bytecode.Length > 0)
                    return hr;

                bytecode = null;
                errors ??= "Shader compilation returned an empty blob.";
            }

            if (errorBlob != IntPtr.Zero && string.IsNullOrWhiteSpace(errors))
                errors = ReadBlob(errorBlob);

            return hr;
        }
        finally
        {
            ReleaseBlob(shaderBlob);
            ReleaseBlob(errorBlob);
        }
    }

    private static string ReadBlob(IntPtr blobPtr)
    {
        var bytes = ReadBlobBytes(blobPtr);
        return bytes.Length == 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(bytes);
    }

    private static unsafe byte[] ReadBlobBytes(IntPtr blobPtr)
    {
        if (blobPtr == IntPtr.Zero)
            return [];

        void** vtable = *(void***)blobPtr;
        if (vtable == null)
            return [];

        var getBufferPointer = (delegate* unmanaged[Stdcall]<IntPtr, IntPtr>)vtable[3];
        var getBufferSize = (delegate* unmanaged[Stdcall]<IntPtr, nuint>)vtable[4];
        if (getBufferPointer == null || getBufferSize == null)
            return [];

        nuint sizeNative = getBufferSize(blobPtr);
        if (sizeNative == 0)
            return [];

        int size = checked((int)sizeNative);
        IntPtr bufferPtr = getBufferPointer(blobPtr);
        if (bufferPtr == IntPtr.Zero)
            return [];

        var bytes = new byte[size];
        Marshal.Copy(bufferPtr, bytes, 0, size);
        return bytes;
    }

    private static void ReleaseBlob(IntPtr blobPtr)
    {
        if (blobPtr != IntPtr.Zero)
            Marshal.Release(blobPtr);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _queue.Dispose();
    }


    [StructLayout(LayoutKind.Sequential)]
    private readonly struct TriangleMetricRaw
    {
        public readonly uint Flags;
        public readonly uint KeyLow;
        public readonly uint KeyHigh;
        public readonly uint Reserved;
        public readonly Vector4 NormalAndArea;
    }

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct TriangleScanConstants(uint TriangleCount, uint PositionCount, uint HasPositions, uint Padding);

    public readonly record struct TriangleScanMetric(uint Flags, ulong OrientedKey, Vector4 NormalAndArea)
    {
        public bool IsDegenerate => (Flags & TriangleFlagDegenerate) != 0;
        public bool HasArea => (Flags & TriangleFlagAreaValid) != 0;
    }

    [DllImport("d3dcompiler_47.dll", EntryPoint = "D3DCompile", ExactSpelling = true, CharSet = CharSet.Ansi)]
    private static extern int D3DCompile47(
        string srcData,
        nuint srcDataSize,
        string? sourceName,
        IntPtr defines,
        IntPtr include,
        string entryPoint,
        string target,
        uint flags1,
        uint flags2,
        out IntPtr code,
        out IntPtr errorMsgs);

    [DllImport("d3dcompiler_43.dll", EntryPoint = "D3DCompile", ExactSpelling = true, CharSet = CharSet.Ansi)]
    private static extern int D3DCompile43(
        string srcData,
        nuint srcDataSize,
        string? sourceName,
        IntPtr defines,
        IntPtr include,
        string entryPoint,
        string target,
        uint flags1,
        uint flags2,
        out IntPtr code,
        out IntPtr errorMsgs);
}
