using System.Runtime.InteropServices;
using SharpDX;
using SharpDX.DXGI;
using SharpDX.Direct3D;
using SharpDX.Direct3D11;
using D3DDevice = SharpDX.Direct3D11.Device;
using D3DTexture2D = SharpDX.Direct3D11.Texture2D;
using DxgiResource = SharpDX.DXGI.Resource;

namespace RavaCast.Media.BridgeHost;

/// <summary>
/// Windows/Wine adapter that exposes decoded BGRA frames as a shared D3D11 texture for the existing RavaCast renderer path.
/// </summary>
internal sealed class D3D11SharedTextureFrameSink : IDisposable
{
    private readonly object _gate = new();
    private readonly D3DDevice _device;
    private readonly DeviceContext _context;
    private readonly D3DTexture2D _texture;
    private bool _disposed;
    private long _writtenFrames;

    private D3D11SharedTextureFrameSink(D3DDevice device, D3DTexture2D texture, nint sharedHandle, int width, int height)
    {
        _device = device;
        _context = device.ImmediateContext;
        _texture = texture;
        SharedHandle = sharedHandle;
        Width = width;
        Height = height;
    }

    public nint SharedHandle { get; }
    public int Width { get; }
    public int Height { get; }
    public long WrittenFrames => Interlocked.Read(ref _writtenFrames);

    public static bool TryCreate(int width, int height, out D3D11SharedTextureFrameSink? sink, out string detail)
    {
        sink = null;
        if (!OperatingSystem.IsWindows())
        {
            detail = "Decoded shared-texture output is only available in the Windows/Wine D3D adapter. The portable decode path is still active.";
            return false;
        }

        width = Math.Clamp(width <= 0 ? 1280 : width, 16, 7680);
        height = Math.Clamp(height <= 0 ? 720 : height, 16, 4320);
        if ((width & 1) == 1) width--;
        if ((height & 1) == 1) height--;

        try
        {
            var device = new D3DDevice(DriverType.Hardware, DeviceCreationFlags.BgraSupport, FeatureLevel.Level_11_1, FeatureLevel.Level_11_0);
            var texture = new D3DTexture2D(device, new Texture2DDescription
            {
                Width = width,
                Height = height,
                MipLevels = 1,
                ArraySize = 1,
                Format = Format.B8G8R8A8_UNorm,
                SampleDescription = new SampleDescription(1, 0),
                Usage = ResourceUsage.Default,
                BindFlags = BindFlags.ShaderResource | BindFlags.RenderTarget,
                CpuAccessFlags = CpuAccessFlags.None,
                OptionFlags = ResourceOptionFlags.Shared
            });

            using var resource = texture.QueryInterface<DxgiResource>();
            var handle = resource.SharedHandle;
            if (handle == IntPtr.Zero)
            {
                texture.Dispose();
                device.Dispose();
                detail = "Decoded shared-texture output created a D3D texture but the shared handle was empty.";
                return false;
            }

            sink = new D3D11SharedTextureFrameSink(device, texture, handle, width, height);
            detail = $"Decoded shared-texture output created ({width}x{height}).";
            return true;
        }
        catch (Exception ex)
        {
            detail = "Decoded shared-texture output failed to initialise: " + ex.Message;
            return false;
        }
    }

    public bool WriteFrame(byte[] bgraFrame, out string? error)
    {
        error = null;
        if (bgraFrame.Length < Width * Height * 4)
        {
            error = $"Decoded frame was too small ({bgraFrame.Length} bytes for {Width}x{Height} BGRA).";
            return false;
        }

        lock (_gate)
        {
            if (_disposed)
            {
                error = "Decoded shared-texture output has already been disposed.";
                return false;
            }

            GCHandle pin = default;
            try
            {
                pin = GCHandle.Alloc(bgraFrame, GCHandleType.Pinned);
                var box = new DataBox(pin.AddrOfPinnedObject(), Width * 4, 0);
                _context.UpdateSubresource(box, _texture, 0);
                _context.Flush();
                Interlocked.Increment(ref _writtenFrames);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
            finally
            {
                if (pin.IsAllocated) pin.Free();
            }
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
        }

        try { _texture.Dispose(); } catch { }
        try { _context.Dispose(); } catch { }
        try { _device.Dispose(); } catch { }
    }
}
