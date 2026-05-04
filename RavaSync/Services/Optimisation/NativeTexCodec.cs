using Lumina.Data.Files;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace RavaSync.Services.Optimisation;

internal static class NativeTexCodec
{

    public static bool TryLoadRgba32(string path, out Image<Rgba32>? image, out TexFile.TextureFormat sourceFormat, out string reason)
    {
        image = null;
        sourceFormat = default;
        reason = string.Empty;

        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream);

            var header = ReadHeader(reader);
            sourceFormat = header.Format;
            if (header.Width == 0 || header.Height == 0)
            {
                reason = "Texture header contained zero width or height.";
                return false;
            }

            if (header.Format != TexFile.TextureFormat.A8R8G8B8 && header.Format != TexFile.TextureFormat.B8G8R8A8)
            {
                reason = $"Only uncompressed RGBA TEX inputs are currently supported by the native path. Source format was {header.Format}.";
                return false;
            }

            var headerSize = Marshal.SizeOf<TexFile.TexHeader>();
            var offset = GetSurfaceOffset(in header, 0);
            if (offset == 0)
                offset = (uint)headerSize;

            stream.Position = offset;
            int width = header.Width;
            int height = header.Height;
            int byteCount = checked(width * height * 4);
            var data = reader.ReadBytes(byteCount);
            if (data.Length != byteCount)
            {
                reason = $"Texture payload was truncated. Expected {byteCount} bytes for the first mip, read {data.Length}.";
                return false;
            }

            image = new Image<Rgba32>(width, height);
            int idx = 0;
            for (int y = 0; y < height; y++)
            {
                for (int x = 0; x < width; x++)
                {
                    byte b = data[idx++];
                    byte g = data[idx++];
                    byte r = data[idx++];
                    byte a = data[idx++];
                    image[x, y] = new Rgba32(r, g, b, a);
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
            image?.Dispose();
            image = null;
            return false;
        }
    }

    public static bool TrySaveRawCompressed(string path, int width, int height, TexFile.TextureFormat targetFormat, byte[] rawData, out string reason)
    {
        return TrySaveRawCompressedMipChain(path, [(width, height, rawData)], targetFormat, out reason);
    }

    public static bool TrySaveRawCompressedMipChain(string path, IReadOnlyList<(int Width, int Height, byte[] RawData)> mipLevels, TexFile.TextureFormat targetFormat, out string reason)
    {
        reason = string.Empty;

        if (targetFormat != TexFile.TextureFormat.BC1 && targetFormat != TexFile.TextureFormat.BC3 && targetFormat != TexFile.TextureFormat.BC7)
        {
            reason = $"Raw compressed TEX output only supports BC1/BC3/BC7. Requested {targetFormat}.";
            return false;
        }

        if (mipLevels == null || mipLevels.Count == 0)
        {
            reason = "Compressed TEX output requires at least one mip level.";
            return false;
        }

        try
        {
            SaveTexMipChain(path, targetFormat, mipLevels);
            return true;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
            return false;
        }
    }

    public static bool TrySaveUncompressed(string path, Image<Rgba32> image, TexFile.TextureFormat targetFormat, out string reason)
    {
        reason = string.Empty;

        if (targetFormat != TexFile.TextureFormat.A8R8G8B8 && targetFormat != TexFile.TextureFormat.B8G8R8A8)
        {
            reason = $"Uncompressed TEX output only supports A8R8G8B8/B8G8R8A8. Requested {targetFormat}.";
            return false;
        }

        try
        {
            var rawData = new byte[image.Width * image.Height * 4];
            int idx = 0;
            for (int y = 0; y < image.Height; y++)
            {
                for (int x = 0; x < image.Width; x++)
                {
                    var px = image[x, y];
                    rawData[idx++] = px.B;
                    rawData[idx++] = px.G;
                    rawData[idx++] = px.R;
                    rawData[idx++] = px.A;
                }
            }

            SaveTex(path, image.Width, image.Height, targetFormat, rawData, mipCount: 1);
            return true;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
            return false;
        }
    }


    private static unsafe uint GetSurfaceOffset(in TexFile.TexHeader header, int index)
    {
        if ((uint)index >= 13u)
            return 0;

        return header.OffsetToSurface[index];
    }

    private static unsafe TexFile.TexHeader ReadHeader(BinaryReader reader)
    {
        int size = Marshal.SizeOf<TexFile.TexHeader>();
        Span<byte> buffer = stackalloc byte[size];
        int read = reader.Read(buffer);
        if (read != size)
            throw new EndOfStreamException($"Incomplete TEX header. Expected {size} bytes, got {read}.");

        return MemoryMarshal.Read<TexFile.TexHeader>(buffer);
    }

    private static unsafe void SaveTex(string path, int width, int height, TexFile.TextureFormat format, byte[] pixels, int mipCount)
    {
        SaveTexMipChain(path, format, [(width, height, pixels)]);
    }

    private static unsafe void SaveTexMipChain(string path, TexFile.TextureFormat format, IReadOnlyList<(int Width, int Height, byte[] RawData)> mipLevels)
    {
        var top = mipLevels[0];
        var mipCount = Math.Clamp(mipLevels.Count, 1, 13);

        var header = new TexFile.TexHeader
        {
            Type = TexFile.Attribute.TextureType2D,
            Format = format,
            Width = (ushort)Math.Clamp(top.Width, 1, ushort.MaxValue),
            Height = (ushort)Math.Clamp(top.Height, 1, ushort.MaxValue),
            Depth = 1,
            MipCount = (byte)mipCount,
            ArraySize = 1,
        };

        int headerSize = Marshal.SizeOf<TexFile.TexHeader>();
        uint runningOffset = (uint)headerSize;
        for (int i = 0; i < 13; i++)
        {
            if (i < mipCount)
            {
                header.OffsetToSurface[i] = runningOffset;
                runningOffset += (uint)mipLevels[i].RawData.Length;
            }
            else
            {
                header.OffsetToSurface[i] = 0;
            }
        }

        header.LodOffset[0] = 0;
        header.LodOffset[1] = 0;
        header.LodOffset[2] = 0;

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        using var writer = new BinaryWriter(stream);

        Span<byte> headerBytes = stackalloc byte[headerSize];
        var localHeader = header;
        MemoryMarshal.Write(headerBytes, in localHeader);
        writer.Write(headerBytes);
        for (int i = 0; i < mipCount; i++)
            writer.Write(mipLevels[i].RawData);
    }

}