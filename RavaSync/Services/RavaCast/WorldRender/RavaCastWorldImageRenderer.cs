using Dalamud.Bindings.ImGui;
using GameControl = FFXIVClientStructs.FFXIV.Client.Game.Control.Control;
using FFXIVClientStructs.FFXIV.Client.Graphics.Scene;
using FFXIVClientStructs.FFXIV.Client.Graphics.Render;
using Microsoft.Extensions.Logging;
using RavaSync.Services.RavaCast;
using RavaSync.Services.RavaCast.Rendering;
using SharpDX.Direct3D;
using SharpDX.Direct3D11;
using SharpDX.DXGI;
using SharpDX.Mathematics.Interop;
using System;
using System.Runtime.InteropServices;
using Vector2 = System.Numerics.Vector2;
using Vector3 = System.Numerics.Vector3;
using Device = FFXIVClientStructs.FFXIV.Client.Graphics.Kernel.Device;
using D3DDevice = SharpDX.Direct3D11.Device;
using D3DDeviceContext = SharpDX.Direct3D11.DeviceContext;
using D3DBuffer = SharpDX.Direct3D11.Buffer;

namespace RavaSync.Services.RavaCast.WorldRender;

public sealed class RavaCastWorldImageRenderer : IDisposable
{
    private const float DepthBias = 0.00035f;
    private const float BorderWidthUv = 0.0055f;
    private readonly ILogger<RavaCastWorldImageRenderer> _logger;
    private bool _disposed;
    private bool _initialiseFailed;
    private string? _lastError;

    private D3DDevice? _device;
    private D3DDeviceContext? _context;
    private VertexShader? _vertexShader;
    private PixelShader? _pixelShader;
    private SamplerState? _sampler;
    private BlendState? _opaqueBlendState;
    private DepthStencilState? _disabledDepthState;
    private RasterizerState? _quadRasterizerState;
    private D3DBuffer? _constantBuffer;
    private Texture2D? _targetTexture;
    private RenderTargetView? _targetRtv;
    private ShaderResourceView? _targetSrv;
    private Texture2D? _depthTextureView;
    private ShaderResourceView? _depthSrv;
    private IntPtr _cachedDepthPtr;
    private int _targetWidth;
    private int _targetHeight;

    public RavaCastWorldImageRenderer(ILogger<RavaCastWorldImageRenderer> logger)
    {
        _logger = logger;
    }

    public string? LastError => _lastError;

    public bool TryRender(RavaCastPlane plane, RavaCastTextureFrame frame, out ImTextureID textureId)
        => TryRenderCore(plane, frame, out textureId);

    public bool TryRenderPlaceholder(RavaCastPlane plane, out ImTextureID textureId)
        => TryRenderCore(plane, null, out textureId);

    private bool TryRenderCore(RavaCastPlane plane, RavaCastTextureFrame? frame, out ImTextureID textureId)
    {
        textureId = default;
        var hasMedia = frame is not null && frame.IsValid && frame.TextureId.Handle != 0;
        if (_disposed || (frame is not null && !hasMedia))
            return false;

        try
        {
            if (!EnsureInitialised())
                return false;

            if (!TryGetViewportSize(out var width, out var height) || width <= 32 || height <= 32)
                return false;

            EnsureTarget(width, height);
            if (!UpdateSceneDepth())
                return false;

            var constants = BuildConstants(plane, frame, width, height, hasMedia);
            _context!.UpdateSubresource(ref constants, _constantBuffer);

            ShaderResourceView? mediaSrv = null;
            try
            {
                if (hasMedia && frame is not null)
                {
                    var mediaPtr = new IntPtr(unchecked((long)frame.TextureId.Handle));
                    if (mediaPtr == IntPtr.Zero)
                        return false;

                    Marshal.AddRef(mediaPtr);
                    mediaSrv = new ShaderResourceView(mediaPtr);
                }

                _context.ClearRenderTargetView(_targetRtv, new RawColor4(0f, 0f, 0f, 0f));
                _context.Rasterizer.SetViewport(0, 0, width, height);
                _context.Rasterizer.State = _quadRasterizerState;
                _context.OutputMerger.SetBlendState(_opaqueBlendState);
                _context.OutputMerger.SetDepthStencilState(_disabledDepthState, 0);
                _context.OutputMerger.SetTargets(_targetRtv);
                _context.InputAssembler.InputLayout = null;
                _context.InputAssembler.PrimitiveTopology = PrimitiveTopology.TriangleStrip;
                _context.VertexShader.Set(_vertexShader);
                _context.VertexShader.SetConstantBuffer(0, _constantBuffer);
                _context.PixelShader.Set(_pixelShader);
                _context.PixelShader.SetConstantBuffer(0, _constantBuffer);
                _context.PixelShader.SetSampler(0, _sampler);
                _context.PixelShader.SetShaderResource(0, mediaSrv);
                _context.PixelShader.SetShaderResource(1, _depthSrv);
                _context.Draw(4, 0);
                _context.PixelShader.SetShaderResource(0, null);
                _context.PixelShader.SetShaderResource(1, null);
                _context.PixelShader.SetSampler(0, null);

                _context.Flush();
            }
            finally
            {
                try { mediaSrv?.Dispose(); } catch { }
            }

            textureId = new ImTextureID(_targetSrv!.NativePointer);
            _lastError = null;
            return textureId.Handle != 0;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return false;
        }
    }

    private unsafe bool EnsureInitialised()
    {
        if (_device is not null)
            return true;
        if (_initialiseFailed)
            return false;

        try
        {
            var device = Device.Instance();
            if (device is null || device->D3D11Forwarder == null)
            {
                _lastError = "Game D3D11 device unavailable";
                _initialiseFailed = true;
                return false;
            }

            var ptr = (IntPtr)device->D3D11Forwarder;
            Marshal.AddRef(ptr);
            _device = new D3DDevice(ptr);
            _context = _device.ImmediateContext;

            var vsBytes = CompileShader(VertexShaderSource, "vs", "vs_5_0");
            var psBytes = CompileShader(PixelShaderSource, "ps", "ps_5_0");
            _vertexShader = new VertexShader(_device, vsBytes);
            _pixelShader = new PixelShader(_device, psBytes);
            _constantBuffer = new D3DBuffer(_device, Marshal.SizeOf<Constants>(), ResourceUsage.Default, BindFlags.ConstantBuffer, CpuAccessFlags.None, ResourceOptionFlags.None, 0);

            var samplerDesc = SamplerStateDescription.Default();
            samplerDesc.Filter = Filter.MinMagMipLinear;
            samplerDesc.AddressU = TextureAddressMode.Clamp;
            samplerDesc.AddressV = TextureAddressMode.Clamp;
            samplerDesc.AddressW = TextureAddressMode.Clamp;
            _sampler = new SamplerState(_device, samplerDesc);

            var blendDesc = new BlendStateDescription();
            blendDesc.RenderTarget[0].IsBlendEnabled = false;
            blendDesc.RenderTarget[0].RenderTargetWriteMask = ColorWriteMaskFlags.All;
            _opaqueBlendState = new BlendState(_device, blendDesc);

            var depthDesc = new DepthStencilStateDescription
            {
                IsDepthEnabled = false,
                DepthWriteMask = DepthWriteMask.Zero,
                DepthComparison = Comparison.Always,
                IsStencilEnabled = false
            };
            _disabledDepthState = new DepthStencilState(_device, depthDesc);

            var rasterDesc = RasterizerStateDescription.Default();
            rasterDesc.CullMode = CullMode.None;
            rasterDesc.FillMode = FillMode.Solid;
            _quadRasterizerState = new RasterizerState(_device, rasterDesc);
            return true;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            _initialiseFailed = true;
            _logger.LogWarning(ex, "Failed to initialise RavaCast world-image renderer");
            DisposeDxObjects();
            return false;
        }
    }

    private unsafe bool TryGetViewportSize(out int width, out int height)
    {
        width = 0;
        height = 0;
        var device = Device.Instance();
        if (device is null) return false;
        width = Math.Max(1, (int)device->Width);
        height = Math.Max(1, (int)device->Height);
        return true;
    }

    private void EnsureTarget(int width, int height)
    {
        if (_targetTexture is not null && _targetWidth == width && _targetHeight == height)
            return;

        _targetSrv?.Dispose();
        _targetRtv?.Dispose();
        _targetTexture?.Dispose();

        _targetWidth = width;
        _targetHeight = height;

        var desc = new Texture2DDescription
        {
            Width = width,
            Height = height,
            MipLevels = 1,
            ArraySize = 1,
            Format = Format.B8G8R8A8_UNorm,
            SampleDescription = new SampleDescription(1, 0),
            Usage = ResourceUsage.Default,
            BindFlags = BindFlags.RenderTarget | BindFlags.ShaderResource,
            CpuAccessFlags = CpuAccessFlags.None,
            OptionFlags = ResourceOptionFlags.None
        };

        _targetTexture = new Texture2D(_device, desc);
        _targetRtv = new RenderTargetView(_device, _targetTexture);
        _targetSrv = new ShaderResourceView(_device, _targetTexture);
    }

    private unsafe bool UpdateSceneDepth()
    {
        var rtm = RenderTargetManager.Instance();
        var depthStencil = rtm != null ? rtm->DepthStencil : null;
        if (depthStencil is null || depthStencil->D3D11Texture2D == null)
        {
            _lastError = "Scene depth unavailable";
            return false;
        }

        var depthPtr = (IntPtr)depthStencil->D3D11Texture2D;
        if (depthPtr == IntPtr.Zero)
            return false;

        if (_depthSrv is not null && depthPtr == _cachedDepthPtr)
            return true;

        _depthSrv?.Dispose();
        _depthTextureView?.Dispose();
        _depthSrv = null;
        _depthTextureView = null;
        _cachedDepthPtr = depthPtr;

        _depthTextureView = new Texture2D(depthPtr);
        try
        {
            var textureDesc = _depthTextureView.Description;
            if (textureDesc.SampleDescription.Count > 1)
            {
                _lastError = $"Scene depth is multisampled ({textureDesc.SampleDescription.Count}x); RavaCast depth SRV path only supports single-sample depth for now.";
                return false;
            }

            var srvFormat = GetDepthShaderResourceFormat(textureDesc.Format);
            if (srvFormat == Format.Unknown)
            {
                _lastError = $"Unsupported scene depth format: {textureDesc.Format}";
                return false;
            }

            var srvDesc = new ShaderResourceViewDescription
            {
                Format = srvFormat,
                Dimension = ShaderResourceViewDimension.Texture2D
            };
            srvDesc.Texture2D.MostDetailedMip = 0;
            srvDesc.Texture2D.MipLevels = 1;
            _depthSrv = new ShaderResourceView(_device, _depthTextureView, srvDesc);
            return true;
        }
        finally
        {
            // This wrapper points at a game-owned texture.  Keep the SRV, but never release the texture itself.
            if (_depthTextureView is not null)
                _depthTextureView.NativePointer = IntPtr.Zero;
        }
    }

    private static Format GetDepthShaderResourceFormat(Format format) => format switch
    {
        Format.R24G8_Typeless => Format.R24_UNorm_X8_Typeless,
        Format.D24_UNorm_S8_UInt => Format.R24_UNorm_X8_Typeless,
        Format.R32_Typeless => Format.R32_Float,
        Format.D32_Float => Format.R32_Float,
        Format.R32G8X24_Typeless => Format.R32_Float_X8X24_Typeless,
        Format.D32_Float_S8X24_UInt => Format.R32_Float_X8X24_Typeless,
        Format.R16_Typeless => Format.R16_UNorm,
        Format.D16_UNorm => Format.R16_UNorm,
        _ => Format.Unknown
    };

    private unsafe Constants BuildConstants(RavaCastPlane plane, RavaCastTextureFrame? frame, int width, int height, bool hasMedia)
    {
        var control = GameControl.Instance();
        var viewProj = System.Numerics.Matrix4x4.Identity;
        if (control is not null)
        {
            var src = (float*)&control->ViewProjectionMatrix;
            var dst = (float*)&viewProj;
            for (var i = 0; i < 16; i++)
                dst[i] = src[i];
        }

        var centre = (plane.TopLeft + plane.TopRight + plane.BottomRight + plane.BottomLeft) / 4f;
        var right = plane.TopRight - plane.TopLeft;
        var down = plane.BottomLeft - plane.TopLeft;

        var planeWidth = MathF.Max(0.001f, right.Length());
        var planeHeight = MathF.Max(0.001f, down.Length());
        var planeAspect = Math.Clamp(planeWidth / planeHeight, 0.05f, 20.0f);
        var mediaAspect = frame is not null && frame.IsValid ? Math.Clamp((float)frame.Width / Math.Max(1, frame.Height), 0.05f, 20.0f) : 16f / 9f;

        // Keep the owner's mapped plane at full size.  The source media is fitted inside that
        // physical plane in UV space, producing stable letterbox/pillarbox bars instead of
        // stretching the video or resizing the in-world screen.
        var contentScale = Vector2.One;
        var contentOffset = Vector2.Zero;
        if (planeAspect > mediaAspect)
        {
            contentScale.X = mediaAspect / planeAspect;
            contentOffset.X = (1f - contentScale.X) / 2f;
        }
        else
        {
            contentScale.Y = planeAspect / mediaAspect;
            contentOffset.Y = (1f - contentScale.Y) / 2f;
        }

        return new Constants
        {
            ViewProj = viewProj,
            Centre = centre,
            Right = right,
            Down = down,
            ViewportSize = new Vector2(width, height),
            DepthUvScale = Vector2.One,
            ContentScale = contentScale,
            ContentOffset = contentOffset,
            OcclusionBias = DepthBias,
            Opacity = 1f,
            BorderWidth = BorderWidthUv,
            HasMedia = hasMedia ? 1f : 0f,
            DepthReversed = EstimateDepthReversed(viewProj, centre),
            Pad4 = Vector2.Zero,
            Pad5 = 0f,
            BorderColour = new System.Numerics.Vector4(0.66f, 0.18f, 1.00f, 0.95f),
            FillColour = new System.Numerics.Vector4(0.005f, 0.002f, 0.010f, 1.0f)
        };
    }

    private static unsafe float EstimateDepthReversed(System.Numerics.Matrix4x4 viewProj, Vector3 planeCentre)
    {
        try
        {
            var camera = CameraManager.Instance()->CurrentCamera;
            if (camera is null) return 1f;

            var cameraPos = new Vector3(camera->Position.X, camera->Position.Y, camera->Position.Z);
            var toCamera = cameraPos - planeCentre;
            if (toCamera.LengthSquared() <= 0.0001f) return 1f;

            var closer = planeCentre + Vector3.Normalize(toCamera) * 0.10f;
            var planeDepth = ProjectDepth(planeCentre, viewProj);
            var closerDepth = ProjectDepth(closer, viewProj);
            if (!IsFinite(planeDepth) || !IsFinite(closerDepth)) return 1f;

            return closerDepth > planeDepth ? 1f : 0f;
        }
        catch
        {
            // FFXIV has historically used reversed-Z on the path this was built against. Keep that
            // as the safe fallback if camera/depth probing fails.
            return 1f;
        }
    }

    private static float ProjectDepth(Vector3 world, System.Numerics.Matrix4x4 viewProj)
    {
        var clip = System.Numerics.Vector4.Transform(new System.Numerics.Vector4(world, 1f), viewProj);
        if (MathF.Abs(clip.W) <= 0.000001f) return float.NaN;
        return clip.Z / clip.W;
    }

    private static bool IsFinite(float value) => !float.IsNaN(value) && !float.IsInfinity(value);


    private static byte[] CompileShader(string source, string entryPoint, string target)
    {
        IntPtr shaderBlob = IntPtr.Zero;
        IntPtr errorBlob = IntPtr.Zero;
        try
        {
            int hr;
            try
            {
                hr = D3DCompile47(source, (nuint)source.Length, null, IntPtr.Zero, IntPtr.Zero, entryPoint, target, 0u, 0u, out shaderBlob, out errorBlob);
            }
            catch (DllNotFoundException)
            {
                hr = unchecked((int)0x8007007E);
            }

            if (hr < 0 && hr == unchecked((int)0x8007007E))
            {
                ReleaseBlob(errorBlob);
                errorBlob = IntPtr.Zero;
                hr = D3DCompile43(source, (nuint)source.Length, null, IntPtr.Zero, IntPtr.Zero, entryPoint, target, 0u, 0u, out shaderBlob, out errorBlob);
            }

            if (hr < 0)
            {
                var errors = errorBlob == IntPtr.Zero ? string.Empty : System.Text.Encoding.UTF8.GetString(ReadBlobBytes(errorBlob));
                Marshal.ThrowExceptionForHR(hr);
                throw new InvalidOperationException(errors);
            }

            var bytes = ReadBlobBytes(shaderBlob);
            if (bytes.Length == 0)
                throw new InvalidOperationException($"Shader {entryPoint}/{target} compiled to an empty blob.");
            return bytes;
        }
        finally
        {
            ReleaseBlob(shaderBlob);
            ReleaseBlob(errorBlob);
        }
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

        var sizeNative = getBufferSize(blobPtr);
        if (sizeNative == 0)
            return [];

        var size = checked((int)sizeNative);
        var bufferPtr = getBufferPointer(blobPtr);
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
        if (_disposed) return;
        _disposed = true;
        DisposeDxObjects();
    }

    private void DisposeDxObjects()
    {
        try { _targetSrv?.Dispose(); } catch { }
        try { _targetRtv?.Dispose(); } catch { }
        try { _targetTexture?.Dispose(); } catch { }
        try { _depthSrv?.Dispose(); } catch { }
        try { _depthTextureView?.Dispose(); } catch { }
        try { _constantBuffer?.Dispose(); } catch { }
        try { _quadRasterizerState?.Dispose(); } catch { }
        try { _disabledDepthState?.Dispose(); } catch { }
        try { _opaqueBlendState?.Dispose(); } catch { }
        try { _sampler?.Dispose(); } catch { }
        try { _pixelShader?.Dispose(); } catch { }
        try { _vertexShader?.Dispose(); } catch { }
        try { if (_context is not null) { _context.NativePointer = IntPtr.Zero; _context.Dispose(); } } catch { }
        try
        {
            if (_device is not null)
            {
                _device.NativePointer = IntPtr.Zero;
                _device.Dispose();
            }
        }
        catch { }

        _targetSrv = null;
        _targetRtv = null;
        _targetTexture = null;
        _depthSrv = null;
        _depthTextureView = null;
        _constantBuffer = null;
        _quadRasterizerState = null;
        _disabledDepthState = null;
        _opaqueBlendState = null;
        _sampler = null;
        _pixelShader = null;
        _vertexShader = null;
        _context = null;
        _device = null;
        _cachedDepthPtr = IntPtr.Zero;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Constants
    {
        public System.Numerics.Matrix4x4 ViewProj;
        public Vector3 Centre;
        public float Pad0;
        public Vector3 Right;
        public float Pad1;
        public Vector3 Down;
        public float Pad2;
        public Vector2 ViewportSize;
        public Vector2 DepthUvScale;
        public Vector2 ContentScale;
        public Vector2 ContentOffset;
        public float OcclusionBias;
        public float Opacity;
        public float BorderWidth;
        public float HasMedia;
        public float DepthReversed;
        public Vector2 Pad4;
        public float Pad5;
        public System.Numerics.Vector4 BorderColour;
        public System.Numerics.Vector4 FillColour;
    }

    private const string VertexShaderSource = """
cbuffer RavaCastConstants : register(b0)
{
    row_major float4x4 ViewProj;
    float3 Centre;
    float Pad0;
    float3 Right;
    float Pad1;
    float3 Down;
    float Pad2;
    float2 ViewportSize;
    float2 DepthUvScale;
    float2 ContentScale;
    float2 ContentOffset;
    float OcclusionBias;
    float Opacity;
    float BorderWidth;
    float HasMedia;
    float DepthReversed;
    float2 Pad4;
    float Pad5;
    float4 BorderColour;
    float4 FillColour;
};

struct VsOut
{
    float4 Position : SV_POSITION;
    float2 Uv : TEXCOORD0;
};

VsOut vs(uint vertexId : SV_VertexID)
{
    float2 corner = float2(vertexId & 1u, (vertexId >> 1u) & 1u);
    float2 offset = corner - 0.5f;
    float3 world = Centre + offset.x * Right + offset.y * Down;

    VsOut output;
    output.Position = mul(float4(world, 1.0f), ViewProj);
    output.Uv = corner;
    return output;
}
""";

    private const string PixelShaderSource = """
cbuffer RavaCastConstants : register(b0)
{
    row_major float4x4 ViewProj;
    float3 Centre;
    float Pad0;
    float3 Right;
    float Pad1;
    float3 Down;
    float Pad2;
    float2 ViewportSize;
    float2 DepthUvScale;
    float2 ContentScale;
    float2 ContentOffset;
    float OcclusionBias;
    float Opacity;
    float BorderWidth;
    float HasMedia;
    float DepthReversed;
    float2 Pad4;
    float Pad5;
    float4 BorderColour;
    float4 FillColour;
};

Texture2D RavaCastMedia : register(t0);
Texture2D SceneDepth : register(t1);
SamplerState LinearClamp : register(s0);

struct PsIn
{
    float4 Position : SV_POSITION;
    float2 Uv : TEXCOORD0;
};

float4 ps(PsIn input) : SV_TARGET
{
    float2 depthUv = (input.Position.xy / max(ViewportSize, float2(1.0f, 1.0f))) * DepthUvScale;
    float sceneDepth = SceneDepth.SampleLevel(LinearClamp, depthUv, 0).r;
    float surfaceDepth = input.Position.z;

    // If already-rendered scene geometry is closer than the RavaCast plane, hide this pixel.
    // Depth direction is probed on the CPU each frame because this path has differed between clients/settings.
    bool sceneCloser = DepthReversed > 0.5f
        ? sceneDepth > surfaceDepth + OcclusionBias
        : sceneDepth + OcclusionBias < surfaceDepth;
    if (sceneDepth > 0.000001f && sceneDepth < 0.999999f && sceneCloser)
        discard;

    float edge = min(min(input.Uv.x, 1.0f - input.Uv.x), min(input.Uv.y, 1.0f - input.Uv.y));
    if (edge <= BorderWidth)
    {
        float4 border = BorderColour;
        border.a *= Opacity;
        return border;
    }

    // The captured browser texture arrives horizontally mirrored once projected onto the
    // in-world two-sided plane. Flip media sampling here so text is readable from the intended
    // viewer/player side without changing the physical plane placement math.
    float2 readableUv = float2(1.0f - input.Uv.x, input.Uv.y);
    float2 mediaUv = (readableUv - ContentOffset) / max(ContentScale, float2(0.0001f, 0.0001f));
    if (any(mediaUv < 0.0f) || any(mediaUv > 1.0f))
    {
        float4 fill = FillColour;
        fill.a *= Opacity;
        return fill;
    }

    if (HasMedia < 0.5f)
    {
        float4 fill = FillColour;
        fill.a *= Opacity;
        return fill;
    }

    float4 colour = RavaCastMedia.Sample(LinearClamp, saturate(mediaUv));
    if (colour.a <= 0.001f)
        colour = FillColour;

    colour.a *= Opacity;
    return colour;
}
""";

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
