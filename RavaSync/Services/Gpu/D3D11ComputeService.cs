using Microsoft.Extensions.Logging;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Vortice.Direct3D11;
using Vortice.DXGI;

namespace RavaSync.Services.Gpu;

public sealed class D3D11ComputeService : IDisposable
{
    private const string ResizeShaderSourceTemplate = """
Texture2D<float4> SourceTexture : register(t0);
RWTexture2D<float4> OutputTexture : register(u0);

static const uint SourceWidth = __SOURCE_WIDTH__;
static const uint SourceHeight = __SOURCE_HEIGHT__;
static const uint OutputWidth = __OUTPUT_WIDTH__;
static const uint OutputHeight = __OUTPUT_HEIGHT__;

float4 ReadSource(uint x, uint y)
{
    x = min(x, SourceWidth - 1);
    y = min(y, SourceHeight - 1);
    return SourceTexture.Load(int3(x, y, 0));
}

[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= OutputWidth || threadId.y >= OutputHeight)
        return;

    float2 scale = float2((float)SourceWidth / (float)OutputWidth, (float)SourceHeight / (float)OutputHeight);
    float2 src = (float2(threadId.x, threadId.y) + 0.5) * scale - 0.5;

    int2 baseCoord = int2(floor(src));
    float2 fracCoord = frac(src);

    uint x0 = (uint)max(baseCoord.x, 0);
    uint y0 = (uint)max(baseCoord.y, 0);
    uint x1 = min(x0 + 1, SourceWidth - 1);
    uint y1 = min(y0 + 1, SourceHeight - 1);

    float4 c00 = ReadSource(x0, y0);
    float4 c10 = ReadSource(x1, y0);
    float4 c01 = ReadSource(x0, y1);
    float4 c11 = ReadSource(x1, y1);

    float4 top = lerp(c00, c10, fracCoord.x);
    float4 bottom = lerp(c01, c11, fracCoord.x);
    OutputTexture[threadId.xy] = saturate(lerp(top, bottom, fracCoord.y));
}
""";

    private const string NormalResizeShaderSourceTemplate = """
Texture2D<float4> SourceTexture : register(t0);
RWTexture2D<float4> OutputTexture : register(u0);

static const uint SourceWidth = __SOURCE_WIDTH__;
static const uint SourceHeight = __SOURCE_HEIGHT__;
static const uint OutputWidth = __OUTPUT_WIDTH__;
static const uint OutputHeight = __OUTPUT_HEIGHT__;

float4 ReadSource(uint x, uint y)
{
    x = min(x, SourceWidth - 1);
    y = min(y, SourceHeight - 1);
    return SourceTexture.Load(int3(x, y, 0));
}

float3 DecodeNormal(float4 c)
{
    float2 xy = c.rg * 2.0f - 1.0f;
    float z2 = saturate(1.0f - dot(xy, xy));
    float z = sqrt(z2);
    return normalize(float3(xy.x, xy.y, z));
}

float4 EncodeNormal(float3 n, float blue, float alpha)
{
    n = normalize(n);
    float2 rg = n.xy * 0.5f + 0.5f;
    return saturate(float4(rg.x, rg.y, blue, alpha));
}

[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= OutputWidth || threadId.y >= OutputHeight)
        return;

    float2 scale = float2((float)SourceWidth / (float)OutputWidth, (float)SourceHeight / (float)OutputHeight);
    float2 src = (float2(threadId.x, threadId.y) + 0.5) * scale - 0.5;

    int2 baseCoord = int2(floor(src));
    float2 fracCoord = frac(src);

    uint x0 = (uint)max(baseCoord.x, 0);
    uint y0 = (uint)max(baseCoord.y, 0);
    uint x1 = min(x0 + 1, SourceWidth - 1);
    uint y1 = min(y0 + 1, SourceHeight - 1);

    float4 s00 = ReadSource(x0, y0);
    float4 s10 = ReadSource(x1, y0);
    float4 s01 = ReadSource(x0, y1);
    float4 s11 = ReadSource(x1, y1);

    float3 n00 = DecodeNormal(s00);
    float3 n10 = DecodeNormal(s10);
    float3 n01 = DecodeNormal(s01);
    float3 n11 = DecodeNormal(s11);

    float3 top = lerp(n00, n10, fracCoord.x);
    float3 bottom = lerp(n01, n11, fracCoord.x);
    float3 blended = normalize(lerp(top, bottom, fracCoord.y));

    float blueTop = lerp(s00.b, s10.b, fracCoord.x);
    float blueBottom = lerp(s01.b, s11.b, fracCoord.x);
    float blue = lerp(blueTop, blueBottom, fracCoord.y);

    float alphaTop = lerp(s00.a, s10.a, fracCoord.x);
    float alphaBottom = lerp(s01.a, s11.a, fracCoord.x);
    float alpha = lerp(alphaTop, alphaBottom, fracCoord.y);

    OutputTexture[threadId.xy] = EncodeNormal(blended, blue, alpha);
}
""";

    private readonly ILogger<D3D11ComputeService> _logger;
    private readonly D3D11SharedDeviceService _sharedDeviceService;
    private readonly D3D11ShaderBytecodeCache _shaderBytecodeDiskCache;
    private readonly SemaphoreSlim _queue = new(1, 1);
    private readonly ConcurrentDictionary<string, byte[]> _shaderCache = new(StringComparer.Ordinal);
    private bool _disposed;

    public D3D11ComputeService(ILogger<D3D11ComputeService> logger, D3D11SharedDeviceService sharedDeviceService, D3D11ShaderBytecodeCache shaderBytecodeDiskCache)
    {
        _logger = logger;
        _sharedDeviceService = sharedDeviceService;
        _shaderBytecodeDiskCache = shaderBytecodeDiskCache;
    }

    public bool IsAvailable => _sharedDeviceService.IsAvailable;

    public Task<bool> WarmupAsync(CancellationToken token)
        => _sharedDeviceService.WarmupAsync(token);

    public async Task<Image<Rgba32>?> TryResizeAsync(Image<Rgba32> source, int targetWidth, int targetHeight, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (targetWidth <= 0 || targetHeight <= 0)
            throw new ArgumentOutOfRangeException(nameof(targetWidth));

        if (source.Width == targetWidth && source.Height == targetHeight)
            return source.Clone();

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return null;

        var shaderKey = $"resize:{source.Width}x{source.Height}->{targetWidth}x{targetHeight}";
        await _queue.WaitAsync(token).ConfigureAwait(false);
        try
        {
            await state.ContextLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                token.ThrowIfCancellationRequested();
                return await Task.Run(() => ExecuteResize(state, shaderKey, source, targetWidth, targetHeight, normalMap: false, token: token), token).ConfigureAwait(false);
            }
            finally
            {
                state.ContextLock.Release();
            }
        }
        finally
        {
            _queue.Release();
        }
    }


    public async Task<Image<Rgba32>?> TryResizeNormalMapAsync(Image<Rgba32> source, int targetWidth, int targetHeight, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (targetWidth <= 0 || targetHeight <= 0)
            throw new ArgumentOutOfRangeException(nameof(targetWidth));

        if (source.Width == targetWidth && source.Height == targetHeight)
            return source.Clone();

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return null;

        var shaderKey = $"normal-resize:{source.Width}x{source.Height}->{targetWidth}x{targetHeight}";
        await _queue.WaitAsync(token).ConfigureAwait(false);
        try
        {
            await state.ContextLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                token.ThrowIfCancellationRequested();
                return await Task.Run(() => ExecuteResize(state, shaderKey, source, targetWidth, targetHeight, normalMap: true, token: token), token).ConfigureAwait(false);
            }
            finally
            {
                state.ContextLock.Release();
            }
        }
        finally
        {
            _queue.Release();
        }
    }

    private Image<Rgba32>? ExecuteResize(D3D11SharedDeviceService.State state, string shaderKey, Image<Rgba32> source, int targetWidth, int targetHeight, bool normalMap, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        var shaderBytecode = _shaderCache.GetOrAdd(shaderKey, _ => CompileResizeShader(source.Width, source.Height, targetWidth, targetHeight, normalMap));
        using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

        var srcBytes = new byte[source.Width * source.Height * 4];
        source.CopyPixelDataTo(srcBytes);

        using var sourceTexture = state.Device.CreateTexture2D<byte>(
            srcBytes,
            Format.R8G8B8A8_UNorm,
            (uint)source.Width,
            (uint)source.Height,
            bindFlags: BindFlags.ShaderResource);

        using var outputTexture = state.Device.CreateTexture2D(
            Format.R32G32B32A32_Float,
            (uint)targetWidth,
            (uint)targetHeight,
            bindFlags: BindFlags.UnorderedAccess | BindFlags.ShaderResource);

        using var stagingTexture = state.Device.CreateTexture2D(
            Format.R32G32B32A32_Float,
            (uint)targetWidth,
            (uint)targetHeight,
            bindFlags: BindFlags.None,
            usage: ResourceUsage.Staging,
            cpuAccessFlags: CpuAccessFlags.Read);

        using var sourceView = state.Device.CreateShaderResourceView(sourceTexture);
        using var outputView = state.Device.CreateUnorderedAccessView(outputTexture);

        state.Context.CSSetShader(computeShader);
        state.Context.CSSetShaderResource(0, sourceView);
        state.Context.CSSetUnorderedAccessView(0, outputView);
        state.Context.Dispatch((uint)((targetWidth + 7) / 8), (uint)((targetHeight + 7) / 8), 1);
        state.Context.Flush();
        state.Context.CopyResource(stagingTexture, outputTexture);

        var mapped = state.Context.Map(stagingTexture, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
        try
        {
            var resultBytes = new byte[targetWidth * targetHeight * 4];
            unsafe
            {
                byte* rowBase = (byte*)mapped.DataPointer;
                for (int y = 0; y < targetHeight; y++)
                {
                    float* row = (float*)(rowBase + (y * mapped.RowPitch));
                    int dest = y * targetWidth * 4;
                    for (int x = 0; x < targetWidth; x++)
                    {
                        float r = row[(x * 4) + 0];
                        float g = row[(x * 4) + 1];
                        float b = row[(x * 4) + 2];
                        float a = row[(x * 4) + 3];
                        resultBytes[dest++] = ToByte(r);
                        resultBytes[dest++] = ToByte(g);
                        resultBytes[dest++] = ToByte(b);
                        resultBytes[dest++] = ToByte(a);
                    }
                }
            }

            return Image.LoadPixelData<Rgba32>(resultBytes, targetWidth, targetHeight);
        }
        finally
        {
            state.Context.Unmap(stagingTexture, 0);
            state.Context.CSSetShader(null);
            state.Context.CSSetShaderResource(0, null);
            state.Context.CSSetUnorderedAccessView(0, null);
        }
    }

    private static byte ToByte(float value)
    {
        var clamped = Math.Clamp(value, 0f, 1f);
        return (byte)Math.Clamp((int)Math.Round(clamped * 255f), 0, 255);
    }


    private byte[] CompileResizeShader(int sourceWidth, int sourceHeight, int targetWidth, int targetHeight, bool normalMap)
    {
        var source = (normalMap ? NormalResizeShaderSourceTemplate : ResizeShaderSourceTemplate)
            .Replace("__SOURCE_WIDTH__", sourceWidth.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("__SOURCE_HEIGHT__", sourceHeight.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("__OUTPUT_WIDTH__", targetWidth.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("__OUTPUT_HEIGHT__", targetHeight.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal);

        var cacheKey = normalMap
            ? $"compute-resize-normal-{sourceWidth}x{sourceHeight}-to-{targetWidth}x{targetHeight}"
            : $"compute-resize-{sourceWidth}x{sourceHeight}-to-{targetWidth}x{targetHeight}";

        return _shaderBytecodeDiskCache.GetOrCompile(cacheKey, source, () => CompileResizeShaderSource(source));
    }

    private static byte[] CompileResizeShaderSource(string source)
    {
        var hresult = TryCompile(source, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 compute shader compilation failed with HRESULT 0x{hresult:X8}."
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
