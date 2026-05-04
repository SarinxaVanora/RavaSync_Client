using Lumina.Data.Files;
using Microsoft.Extensions.Logging;
using RavaSync.Services.Optimisation;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Vortice.Direct3D11;
using Vortice.DXGI;

namespace RavaSync.Services.Gpu;

public sealed class D3D11TextureCompressionService : IDisposable
{
    private const string BcCommonShader = """
Texture2D<float4> SourceTexture : register(t0);
RWTexture2D<uint4> OutputBlocks : register(u0);

cbuffer EncodeConstants : register(b0)
{
    uint SourceWidth;
    uint SourceHeight;
    uint BlockCountX;
    uint BlockCountY;
};

static const uint BC7Weights4[16] = { 0, 4, 9, 13, 17, 21, 26, 30, 34, 38, 43, 47, 51, 55, 60, 64 };

uint ClampX(uint x) { return min(x, SourceWidth - 1); }
uint ClampY(uint y) { return min(y, SourceHeight - 1); }

uint4 LoadPixelU8(uint2 blockOrigin, uint index)
{
    uint2 coord = blockOrigin + uint2(index & 3, index >> 2);
    float4 c = saturate(SourceTexture.Load(int3(ClampX(coord.x), ClampY(coord.y), 0)));
    return uint4(c * 255.0f + 0.5f);
}

uint Expand5(uint v) { return (v << 3) | (v >> 2); }
uint Expand6(uint v) { return (v << 2) | (v >> 4); }
uint Pack565(uint3 rgb)
{
    uint r = (rgb.x * 31u + 127u) / 255u;
    uint g = (rgb.y * 63u + 127u) / 255u;
    uint b = (rgb.z * 31u + 127u) / 255u;
    return (r << 11) | (g << 5) | b;
}

uint3 Unpack565(uint p)
{
    return uint3(Expand5((p >> 11) & 31u), Expand6((p >> 5) & 63u), Expand5(p & 31u));
}

uint ComputeBc1Indices(uint4 px[16], uint3 c0, uint3 c1)
{
    uint3 p0 = c0;
    uint3 p1 = c1;
    uint3 p2 = (2u * c0 + c1 + 1u) / 3u;
    uint3 p3 = (c0 + 2u * c1 + 1u) / 3u;
    uint indices = 0u;
    for (uint i = 0u; i < 16u; ++i)
    {
        uint best = 0u;
        uint bestErr = 0xFFFFFFFFu;
        uint3 rgb = px[i].rgb;
        uint3 palette[4] = { p0, p1, p2, p3 };
        [unroll]
        for (uint j = 0u; j < 4u; ++j)
        {
            int3 diff = int3(rgb) - int3(palette[j]);
            uint err = uint(diff.x * diff.x + diff.y * diff.y + diff.z * diff.z);
            if (err < bestErr)
            {
                bestErr = err;
                best = j;
            }
        }

        indices |= (best & 3u) << (i * 2u);
    }

    return indices;
}

uint4 EncodeBc1Block(uint2 blockId)
{
    uint2 origin = blockId * 4u;
    uint4 px[16];
    uint3 minRgb = uint3(255u, 255u, 255u);
    uint3 maxRgb = uint3(0u, 0u, 0u);
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
    {
        px[i] = LoadPixelU8(origin, i);
        minRgb = min(minRgb, px[i].rgb);
        maxRgb = max(maxRgb, px[i].rgb);
    }

    uint c0 = Pack565(maxRgb);
    uint c1 = Pack565(minRgb);
    if (c0 == c1)
    {
        if (c1 > 0u) c1 -= 1u; else c0 += 1u;
    }
    if (c0 < c1)
    {
        uint t = c0; c0 = c1; c1 = t;
    }

    uint indices = ComputeBc1Indices(px, Unpack565(c0), Unpack565(c1));
    return uint4((c1 << 16) | c0, indices, 0u, 0u);
}

uint SelectBc4Index(uint value, uint e0, uint e1)
{
    uint palette[8];
    palette[0] = e0;
    palette[1] = e1;
    if (e0 > e1)
    {
        palette[2] = (6u * e0 + 1u * e1 + 3u) / 7u;
        palette[3] = (5u * e0 + 2u * e1 + 3u) / 7u;
        palette[4] = (4u * e0 + 3u * e1 + 3u) / 7u;
        palette[5] = (3u * e0 + 4u * e1 + 3u) / 7u;
        palette[6] = (2u * e0 + 5u * e1 + 3u) / 7u;
        palette[7] = (1u * e0 + 6u * e1 + 3u) / 7u;
    }
    else
    {
        palette[2] = (4u * e0 + 1u * e1 + 2u) / 5u;
        palette[3] = (3u * e0 + 2u * e1 + 2u) / 5u;
        palette[4] = (2u * e0 + 3u * e1 + 2u) / 5u;
        palette[5] = (1u * e0 + 4u * e1 + 2u) / 5u;
        palette[6] = 0u;
        palette[7] = 255u;
    }

    uint best = 0u;
    uint bestErr = 0xFFFFFFFFu;
    [unroll]
    for (uint i = 0u; i < 8u; ++i)
    {
        int diff = int(value) - int(palette[i]);
        uint err = uint(diff * diff);
        if (err < bestErr)
        {
            bestErr = err;
            best = i;
        }
    }

    return best;
}

uint2 EncodeBc4FromChannel(uint4 px[16], uint channel)
{
    uint minV = 255u;
    uint maxV = 0u;
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
    {
        uint v = px[i][channel];
        minV = min(minV, v);
        maxV = max(maxV, v);
    }

    uint e0 = maxV;
    uint e1 = minV;
    uint lo = e0 | (e1 << 8);
    uint hi = 0u;
    uint bitPos = 16u;
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
    {
        uint idx = SelectBc4Index(px[i][channel], e0, e1);
        if (bitPos < 32u)
        {
            lo |= idx << bitPos;
            if (bitPos > 29u)
                hi |= idx >> (32u - bitPos);
        }
        else
        {
            hi |= idx << (bitPos - 32u);
        }

        bitPos += 3u;
    }

    return uint2(lo, hi);
}

void WriteBc1(uint2 blockId)
{
    OutputBlocks[blockId] = EncodeBc1Block(blockId);
}

void WriteBc3(uint2 blockId)
{
    uint2 origin = blockId * 4u;
    uint4 px[16];
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
        px[i] = LoadPixelU8(origin, i);

    uint2 alpha = EncodeBc4FromChannel(px, 3u);
    uint4 color = EncodeBc1Block(blockId);
    OutputBlocks[blockId] = uint4(alpha.x, alpha.y, color.x, color.y);
}

void WriteBc5(uint2 blockId)
{
    uint2 origin = blockId * 4u;
    uint4 px[16];
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
        px[i] = LoadPixelU8(origin, i);

    uint2 red = EncodeBc4FromChannel(px, 0u);
    uint2 green = EncodeBc4FromChannel(px, 1u);
    OutputBlocks[blockId] = uint4(red.x, red.y, green.x, green.y);
}

struct BitWriter
{
    uint4 words;
    uint bitPos;
};

void WriteBits(inout BitWriter bw, uint value, uint bitCount)
{
    uint remaining = bitCount;
    uint shift = 0u;
    while (remaining > 0u)
    {
        uint wordIndex = bw.bitPos >> 5u;
        uint bitIndex = bw.bitPos & 31u;
        uint writable = min(remaining, 32u - bitIndex);
        uint mask = writable == 32u ? 0xFFFFFFFFu : ((1u << writable) - 1u);
        bw.words[wordIndex] |= ((value >> shift) & mask) << bitIndex;
        bw.bitPos += writable;
        shift += writable;
        remaining -= writable;
    }
}


uint4 SnapBc7Endpoint(uint4 endpoint, uint pbit)
{
    uint4 recon = endpoint;
    [unroll]
    for (uint c = 0u; c < 4u; ++c)
    {
        uint v = endpoint[c];
        if ((v & 1u) != pbit)
        {
            if (v == 0u) v = 1u;
            else if (v == 255u) v = 254u;
            else if (pbit == 1u) v += 1u; else v -= 1u;
        }
        recon[c] = v;
    }
    return recon;
}

uint ComputeBc7EndpointError(uint4 a, uint4 b)
{
    int4 d = int4(a) - int4(b);
    return uint(d.x * d.x + d.y * d.y + d.z * d.z + d.w * d.w);
}

uint4 InterpolateBc7(uint4 e0, uint4 e1, uint weight)
{
    return (e0 * (64u - weight) + e1 * weight + 32u) >> 6u;
}

uint PickBc7Index(uint4 px, uint4 e0, uint4 e1)
{
    uint best = 0u;
    uint bestErr = 0xFFFFFFFFu;
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
    {
        uint4 c = InterpolateBc7(e0, e1, BC7Weights4[i]);
        int4 d = int4(px) - int4(c);
        uint err = uint(d.x * d.x + d.y * d.y + d.z * d.z + d.w * d.w);
        if (err < bestErr)
        {
            bestErr = err;
            best = i;
        }
    }
    return best;
}

uint ComputeLuma(uint4 rgba)
{
    return rgba.r * 54u + rgba.g * 183u + rgba.b * 19u + rgba.a * 12u;
}

void FindBc7Endpoints(uint4 px[16], out uint4 endpoint0, out uint4 endpoint1)
{
    uint minLuma = 0xFFFFFFFFu;
    uint maxLuma = 0u;
    uint minIndex = 0u;
    uint maxIndex = 0u;
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
    {
        uint l = ComputeLuma(px[i]);
        if (l < minLuma)
        {
            minLuma = l;
            minIndex = i;
        }
        if (l > maxLuma)
        {
            maxLuma = l;
            maxIndex = i;
        }
    }

    endpoint0 = px[minIndex];
    endpoint1 = px[maxIndex];

    if (all(endpoint0 == endpoint1))
    {
        uint4 minRgba = uint4(255u, 255u, 255u, 255u);
        uint4 maxRgba = uint4(0u, 0u, 0u, 0u);
        [unroll]
        for (uint i = 0u; i < 16u; ++i)
        {
            minRgba = min(minRgba, px[i]);
            maxRgba = max(maxRgba, px[i]);
        }
        endpoint0 = minRgba;
        endpoint1 = maxRgba;
    }
}

void WriteBc7(uint2 blockId)
{
    uint2 origin = blockId * 4u;
    uint4 px[16];
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
        px[i] = LoadPixelU8(origin, i);

    uint4 baseEndpoint0;
    uint4 baseEndpoint1;
    FindBc7Endpoints(px, baseEndpoint0, baseEndpoint1);

    uint p0 = ComputeBc7EndpointError(SnapBc7Endpoint(baseEndpoint0, 0u), baseEndpoint0) <= ComputeBc7EndpointError(SnapBc7Endpoint(baseEndpoint0, 1u), baseEndpoint0) ? 0u : 1u;
    uint p1 = ComputeBc7EndpointError(SnapBc7Endpoint(baseEndpoint1, 0u), baseEndpoint1) <= ComputeBc7EndpointError(SnapBc7Endpoint(baseEndpoint1, 1u), baseEndpoint1) ? 0u : 1u;
    uint4 endpoint0 = SnapBc7Endpoint(baseEndpoint0, p0);
    uint4 endpoint1 = SnapBc7Endpoint(baseEndpoint1, p1);
    uint idx[16];
    [unroll]
    for (uint i = 0u; i < 16u; ++i)
        idx[i] = PickBc7Index(px[i], endpoint0, endpoint1);

    if (idx[0] >= 8u)
    {
        uint4 t = endpoint0; endpoint0 = endpoint1; endpoint1 = t;
        uint tp = p0; p0 = p1; p1 = tp;
        [unroll]
        for (uint i = 0u; i < 16u; ++i)
            idx[i] = 15u - idx[i];
    }

    uint4 q0 = endpoint0 >> 1u;
    uint4 q1 = endpoint1 >> 1u;

    BitWriter bw;
    bw.words = uint4(0u, 0u, 0u, 0u);
    bw.bitPos = 0u;
    WriteBits(bw, 0x40u, 7u);
    WriteBits(bw, q0.r, 7u);
    WriteBits(bw, q1.r, 7u);
    WriteBits(bw, q0.g, 7u);
    WriteBits(bw, q1.g, 7u);
    WriteBits(bw, q0.b, 7u);
    WriteBits(bw, q1.b, 7u);
    WriteBits(bw, q0.a, 7u);
    WriteBits(bw, q1.a, 7u);
    WriteBits(bw, p0, 1u);
    WriteBits(bw, p1, 1u);
    WriteBits(bw, idx[0], 3u);
    [unroll]
    for (uint i = 1u; i < 16u; ++i)
        WriteBits(bw, idx[i], 4u);

    OutputBlocks[blockId] = bw.words;
}
""";

    private const string Bc1EntryShader = BcCommonShader + """
[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= BlockCountX || threadId.y >= BlockCountY)
        return;

    WriteBc1(threadId.xy);
}
""";

    private const string Bc3EntryShader = BcCommonShader + """
[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= BlockCountX || threadId.y >= BlockCountY)
        return;

    WriteBc3(threadId.xy);
}
""";

    private const string Bc5EntryShader = BcCommonShader + """
[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= BlockCountX || threadId.y >= BlockCountY)
        return;

    WriteBc5(threadId.xy);
}
""";

    private const string Bc7EntryShader = BcCommonShader + """
[numthreads(8, 8, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    if (threadId.x >= BlockCountX || threadId.y >= BlockCountY)
        return;

    WriteBc7(threadId.xy);
}
""";

    private readonly ILogger<D3D11TextureCompressionService> _logger;
    private readonly D3D11SharedDeviceService _sharedDeviceService;
    private readonly D3D11ComputeService _d3d11ComputeService;
    private readonly D3D11ShaderBytecodeCache _shaderBytecodeDiskCache;
    private readonly SemaphoreSlim _queue = new(1, 1);
    private readonly ConcurrentDictionary<string, byte[]> _shaderCache = new(StringComparer.Ordinal);
    private bool _disposed;

    public D3D11TextureCompressionService(ILogger<D3D11TextureCompressionService> logger, D3D11SharedDeviceService sharedDeviceService, D3D11ComputeService d3d11ComputeService, D3D11ShaderBytecodeCache shaderBytecodeDiskCache)
    {
        _logger = logger;
        _sharedDeviceService = sharedDeviceService;
        _d3d11ComputeService = d3d11ComputeService;
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
                nameof(TexFile.TextureFormat.BC1),
                nameof(TexFile.TextureFormat.BC3),
                nameof(TexFile.TextureFormat.BC5),
                nameof(TexFile.TextureFormat.BC7),
            };

            foreach (var shaderKey in shaderKeys)
            {
                token.ThrowIfCancellationRequested();
                _ = GetEncodeShaderBytecode(shaderKey);
                Thread.Yield();
            }
        }, token).ConfigureAwait(false);

        return true;
    }

    public async Task<bool> TryCompressTextureAsync(string sourcePath, string outputPath, TexFile.TextureFormat targetFormat, bool treatAsNormalMap, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(sourcePath)) throw new ArgumentNullException(nameof(sourcePath));
        if (string.IsNullOrWhiteSpace(outputPath)) throw new ArgumentNullException(nameof(outputPath));

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return false;

        if (!NativeTexCodec.TryLoadRgba32(sourcePath, out var sourceImage, out _, out var loadReason) || sourceImage == null)
        {
            _logger.LogDebug("Skipping local D3D texture compression for {path}: {reason}", sourcePath, loadReason);
            return false;
        }

        try
        {
            using var imageScope = sourceImage;
            return await TryCompressTextureCoreAsync(state, imageScope, outputPath, targetFormat, treatAsNormalMap, token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Local D3D texture compression failed for {path} -> {targetFormat}", sourcePath, targetFormat);
            return false;
        }
    }

    private async Task<bool> TryCompressTextureCoreAsync(D3D11SharedDeviceService.State state, Image<Rgba32> sourceImage, string outputPath, TexFile.TextureFormat targetFormat, bool treatAsNormalMap, CancellationToken token)
    {
        var mipChain = await BuildMipChainAsync(sourceImage, targetFormat, treatAsNormalMap, token).ConfigureAwait(false);
        try
        {
            var encodedLevels = new (int Width, int Height, byte[] RawData)[mipChain.Count];
            for (int i = 0; i < mipChain.Count; i++)
            {
                token.ThrowIfCancellationRequested();
                var mip = mipChain[i];
                var raw = await CompressMipAsync(state, mip, targetFormat, token).ConfigureAwait(false);
                if (raw == null || raw.Length == 0)
                    return false;

                encodedLevels[i] = (mip.Width, mip.Height, raw);
            }

            return NativeTexCodec.TrySaveRawCompressedMipChain(outputPath, encodedLevels, targetFormat, out var saveReason)
                ? true
                : throw new InvalidOperationException(saveReason);
        }
        finally
        {
            for (int i = 0; i < mipChain.Count; i++)
                mipChain[i]?.Dispose();
        }
    }

    private async Task<byte[]?> CompressMipAsync(D3D11SharedDeviceService.State state, Image<Rgba32> mipImage, TexFile.TextureFormat targetFormat, CancellationToken token)
    {
        await _queue.WaitAsync(token).ConfigureAwait(false);
        try
        {
            await state.ContextLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                token.ThrowIfCancellationRequested();
                return await Task.Run(() => ExecuteCompression(state, mipImage, targetFormat, token), token).ConfigureAwait(false);
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

    private byte[] ExecuteCompression(D3D11SharedDeviceService.State state, Image<Rgba32> source, TexFile.TextureFormat targetFormat, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        string shaderKey;
        int bytesPerBlock;
        switch (targetFormat)
        {
            case TexFile.TextureFormat.BC1:
                shaderKey = nameof(TexFile.TextureFormat.BC1);
                bytesPerBlock = 8;
                break;
            case TexFile.TextureFormat.BC3:
                shaderKey = nameof(TexFile.TextureFormat.BC3);
                bytesPerBlock = 16;
                break;
            case TexFile.TextureFormat.BC5:
                shaderKey = nameof(TexFile.TextureFormat.BC5);
                bytesPerBlock = 16;
                break;
            case TexFile.TextureFormat.BC7:
                shaderKey = nameof(TexFile.TextureFormat.BC7);
                bytesPerBlock = 16;
                break;
            default:
                throw new NotSupportedException($"D3D11 texture compression does not support {targetFormat}.");
        }

        var shaderBytecode = GetEncodeShaderBytecode(shaderKey);
        using var shader = state.Device.CreateComputeShader(shaderBytecode);

        var srcBytes = new byte[source.Width * source.Height * 4];
        source.CopyPixelDataTo(srcBytes);

        using var sourceTexture = state.Device.CreateTexture2D<byte>(srcBytes,Format.R8G8B8A8_UNorm,(uint)source.Width,(uint)source.Height,bindFlags: BindFlags.ShaderResource);
        using var sourceView = state.Device.CreateShaderResourceView(sourceTexture);

        int blockCountX = (source.Width + 3) / 4;
        int blockCountY = (source.Height + 3) / 4;
        int blockCount = checked(blockCountX * blockCountY);
        int outputTextureBytes = checked(blockCount * 16);

        using var outputTexture = state.Device.CreateTexture2D(Format.R32G32B32A32_UInt,(uint)blockCountX,(uint)blockCountY,bindFlags: BindFlags.UnorderedAccess | BindFlags.ShaderResource);
        using var outputUav = state.Device.CreateUnorderedAccessView(outputTexture);
        using var stagingTexture = state.Device.CreateTexture2D(Format.R32G32B32A32_UInt,(uint)blockCountX,(uint)blockCountY,bindFlags: BindFlags.None,usage: ResourceUsage.Staging,cpuAccessFlags: CpuAccessFlags.Read);
        using var constants = CreateConstantBuffer(state.Device, new EncodeConstants((uint)source.Width, (uint)source.Height, (uint)blockCountX, (uint)blockCountY));

        state.Context.CSSetShader(shader);
        state.Context.CSSetShaderResource(0, sourceView);
        state.Context.CSSetUnorderedAccessView(0, outputUav);
        state.Context.CSSetConstantBuffer(0, constants);
        state.Context.Dispatch((uint)((blockCountX + 7) / 8), (uint)((blockCountY + 7) / 8), 1);
        state.Context.Flush();
        state.Context.CopyResource(stagingTexture, outputTexture);

        var mapped = state.Context.Map(stagingTexture, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
        try
        {
            var rawBlocks = new byte[outputTextureBytes];
            unsafe
            {
                byte* rowBase = (byte*)mapped.DataPointer;
                for (int y = 0; y < blockCountY; y++)
                {
                    Marshal.Copy((IntPtr)(rowBase + (y * mapped.RowPitch)), rawBlocks, y * blockCountX * 16, blockCountX * 16);
                }
            }

            var trimmed = new byte[checked(blockCount * bytesPerBlock)];
            if (bytesPerBlock == 16)
            {
                Buffer.BlockCopy(rawBlocks, 0, trimmed, 0, trimmed.Length);
            }
            else
            {
                for (int i = 0, src = 0, dst = 0; i < blockCount; i++, src += 16, dst += 8)
                    Buffer.BlockCopy(rawBlocks, src, trimmed, dst, 8);
            }

            return trimmed;
        }
        finally
        {
            state.Context.Unmap(stagingTexture, 0);
            state.Context.CSSetShader(null);
            state.Context.CSSetShaderResource(0, null);
            state.Context.CSSetUnorderedAccessView(0, null);
            state.Context.CSSetConstantBuffer(0, null);
        }
    }

    private async Task<List<Image<Rgba32>>> BuildMipChainAsync(Image<Rgba32> sourceImage, TexFile.TextureFormat targetFormat, bool treatAsNormalMap, CancellationToken token)
    {
        var result = new List<Image<Rgba32>>();
        result.Add(sourceImage.Clone());

        int width = sourceImage.Width;
        int height = sourceImage.Height;
        bool normalMap = treatAsNormalMap;
        while (width > 1 || height > 1)
        {
            token.ThrowIfCancellationRequested();
            int nextWidth = Math.Max(1, width / 2);
            int nextHeight = Math.Max(1, height / 2);
            var previous = result[result.Count - 1];
            var next = normalMap
                ? await _d3d11ComputeService.TryResizeNormalMapAsync(previous, nextWidth, nextHeight, token).ConfigureAwait(false)
                : await _d3d11ComputeService.TryResizeAsync(previous, nextWidth, nextHeight, token).ConfigureAwait(false);
            if (next == null)
                break;

            result.Add(next);
            width = nextWidth;
            height = nextHeight;
        }

        return result;
    }

    private static ID3D11Buffer CreateConstantBuffer(ID3D11Device device, EncodeConstants constants)
    {
        var description = new BufferDescription(
            (uint)Marshal.SizeOf<EncodeConstants>(),
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

    private byte[] GetEncodeShaderBytecode(string shaderKey)
        => _shaderCache.GetOrAdd(shaderKey, key =>
        {
            string source = key switch
            {
                nameof(TexFile.TextureFormat.BC1) => Bc1EntryShader,
                nameof(TexFile.TextureFormat.BC3) => Bc3EntryShader,
                nameof(TexFile.TextureFormat.BC5) => Bc5EntryShader,
                nameof(TexFile.TextureFormat.BC7) => Bc7EntryShader,
                _ => throw new NotSupportedException(key),
            };

            return _shaderBytecodeDiskCache.GetOrCompile($"texture-compress-{key}", source, () => CompileEncodeShaderSource(key, source));
        });

    private static byte[] CompileEncodeShaderSource(string shaderKey, string source)
    {
        var hresult = TryCompile(source, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 texture compression shader compilation failed for {shaderKey} with HRESULT 0x{hresult:X8}."
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

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _queue.Dispose();
    }

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct EncodeConstants(uint SourceWidth, uint SourceHeight, uint BlockCountX, uint BlockCountY);

}
