using System.Runtime.InteropServices;
using SharpDX;
using SharpDX.DXGI;
using SharpDX.Direct3D;
using SharpDX.Direct3D11;
using D3DDevice = SharpDX.Direct3D11.Device;
using D3DTexture2D = SharpDX.Direct3D11.Texture2D;
using D3D11MapFlags = SharpDX.Direct3D11.MapFlags;
namespace RavaCast.Media.BridgeHost;

/// <summary>
/// Windows/Wine adapter for reading the host RavaCast output texture.
/// The media core stays libdatachannel + FFmpeg; this class only bridges the current D3D shared texture into BGRA frames.
/// </summary>
internal sealed class D3D11SharedTextureFrameSource : IDisposable
{
    private readonly object _gate = new();
    private readonly D3DDevice _device;
    private readonly DeviceContext _context;
    private readonly D3DTexture2D _source;
    private const int ReadbackLatencyFrames = 2;

    private readonly D3DTexture2D[] _stagingRing;
    private readonly Queue<int> _pendingCopies = new();
    private readonly byte[] _scratch;
    private int _nextCopyIndex;
    private bool _disposed;

    private D3D11SharedTextureFrameSource(D3DDevice device, D3DTexture2D source, D3DTexture2D[] stagingRing, int width, int height)
    {
        _device = device;
        _context = device.ImmediateContext;
        _source = source;
        _stagingRing = stagingRing;
        Width = width;
        Height = height;
        _scratch = new byte[checked(width * height * 4)];
    }

    public int Width { get; }
    public int Height { get; }
    public int FrameBytes => _scratch.Length;

    public static bool TryOpen(PublisherConfig config, out D3D11SharedTextureFrameSource? source, out string detail)
    {
        source = null;
        if (!OperatingSystem.IsWindows())
        {
            detail = "Live shared-texture capture is only available in the Windows/Wine D3D adapter. The portable media core remains libdatachannel + FFmpeg.";
            return false;
        }

        if (config.SharedTextureHandle == 0)
        {
            detail = "The host shared texture handle was empty.";
            return false;
        }

        try
        {
            var device = new D3DDevice(DriverType.Hardware, DeviceCreationFlags.BgraSupport, FeatureLevel.Level_11_1, FeatureLevel.Level_11_0);
            var opened = device.OpenSharedResource<D3DTexture2D>(config.SharedTextureHandle);
            var desc = opened.Description;
            var width = Math.Max(1, desc.Width);
            var height = Math.Max(1, desc.Height);
            var stagingRing = new D3DTexture2D[4];
            try
            {
                for (var i = 0; i < stagingRing.Length; i++)
                {
                    stagingRing[i] = new D3DTexture2D(device, new Texture2DDescription
                    {
                        Width = width,
                        Height = height,
                        MipLevels = 1,
                        ArraySize = 1,
                        Format = Format.B8G8R8A8_UNorm,
                        SampleDescription = new SampleDescription(1, 0),
                        Usage = ResourceUsage.Staging,
                        BindFlags = BindFlags.None,
                        CpuAccessFlags = CpuAccessFlags.Read,
                        OptionFlags = ResourceOptionFlags.None
                    });
                }
            }
            catch
            {
                foreach (var staging in stagingRing)
                {
                    try { staging?.Dispose(); } catch { }
                }
                throw;
            }

            source = new D3D11SharedTextureFrameSource(device, opened, stagingRing, width, height);
            detail = $"Live RavaCast texture capture opened ({width}x{height}) with non-blocking delayed readback.";
            return true;
        }
        catch (Exception ex)
        {
            detail = "Live RavaCast texture capture failed to open: " + ex.Message;
            return false;
        }
    }

    public bool TryReadFrame(out byte[] frame, out string? error)
    {
        frame = [];
        error = null;

        lock (_gate)
        {
            if (_disposed)
            {
                error = "Shared texture frame source has already been disposed.";
                return false;
            }

            // Always queue the next GPU copy first, then read an older staging texture.
            // The previous version dequeued the only pending copy on the next capture tick, so the
            // supposedly triple-buffered path was often trying to map a GPU copy that was still in flight.
            // On some drivers that returned an empty DataPointer, which then surfaced as
            // "Value cannot be null. (Parameter 'source')" from Marshal.Copy and meant FFmpeg received 0 frames.
            var copyError = QueueNextGpuCopy();
            if (_pendingCopies.Count <= ReadbackLatencyFrames)
            {
                error = copyError ?? $"Priming non-blocking Direct Stream readback ({_pendingCopies.Count}/{ReadbackLatencyFrames + 1}).";
                return false;
            }

            var readIndex = _pendingCopies.Dequeue();
            var readOk = false;
            string? readError = null;
            DataBox map = default;

            try
            {
                // Read a staging texture copied several capture ticks earlier, never the texture we just
                // copied into. DoNotWait keeps this off the game/render critical path; if the GPU is still
                // busy we simply skip this tick rather than causing a visible hitch.
                map = _context.MapSubresource(_stagingRing[readIndex], 0, MapMode.Read, D3D11MapFlags.DoNotWait);
                if (map.DataPointer == IntPtr.Zero)
                {
                    readError = "Direct Stream readback map returned no CPU pointer yet.";
                }
                else
                {
                    var rowBytes = Width * 4;
                    if (map.RowPitch < rowBytes)
                    {
                        readError = $"Direct Stream readback row pitch was too small ({map.RowPitch} < {rowBytes}).";
                    }
                    else
                    {
                        for (var y = 0; y < Height; y++)
                        {
                            var src = map.DataPointer + y * map.RowPitch;
                            Marshal.Copy(src, _scratch, y * rowBytes, rowBytes);
                        }

                        // The capture loop writes this buffer to FFmpeg before requesting the next frame,
                        // so we can reuse the scratch buffer instead of allocating several MB every frame.
                        frame = _scratch;
                        readOk = true;
                    }
                }
            }
            catch (Exception ex)
            {
                readError = ex.Message;
            }
            finally
            {
                if (map.DataPointer != IntPtr.Zero)
                {
                    try { _context.UnmapSubresource(_stagingRing[readIndex], 0); } catch { }
                }
            }

            if (!readOk)
            {
                error = copyError is null ? readError : readError + " | copy: " + copyError;
                return false;
            }

            return true;
        }
    }

    private string? QueueNextGpuCopy()
    {
        if (_stagingRing.Length == 0) return "No staging textures were created for Direct Stream readback.";

        var copyIndex = TakeFreeCopyIndex();
        if (copyIndex < 0)
        {
            if (_pendingCopies.Count == 0) return "No free Direct Stream staging texture was available.";
            copyIndex = _pendingCopies.Dequeue();
        }

        try
        {
            _context.CopyResource(_source, _stagingRing[copyIndex]);
            // Submit the copy to the BridgeHost D3D queue without flushing the renderer/game process.
            // This keeps capture non-blocking while making the delayed staging readback become visible
            // reliably enough for FFmpeg to receive frames.
            _context.Flush();
            _pendingCopies.Enqueue(copyIndex);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private int TakeFreeCopyIndex()
    {
        for (var attempt = 0; attempt < _stagingRing.Length; attempt++)
        {
            var index = _nextCopyIndex;
            _nextCopyIndex = (_nextCopyIndex + 1) % _stagingRing.Length;
            if (!PendingCopyContains(index))
                return index;
        }

        return -1;
    }

    private bool PendingCopyContains(int index)
    {
        foreach (var pending in _pendingCopies)
            if (pending == index) return true;
        return false;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
        }

        foreach (var staging in _stagingRing)
            try { staging.Dispose(); } catch { }
        try { _source.Dispose(); } catch { }
        try { _context.Dispose(); } catch { }
        try { _device.Dispose(); } catch { }
    }
}
