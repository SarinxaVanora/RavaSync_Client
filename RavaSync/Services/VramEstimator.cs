using Lumina.Data.Files;
using RavaSync.Interop.GameModel;
using System;
using System.IO;
using MdlFile = RavaSync.Interop.GameModel.MdlFile;

namespace RavaSync.Services;

internal static class VramEstimator
{
    public static bool TryEstimateTexVramBytes(string texPath, out long vramBytes)
    {
        vramBytes = 0;

        try
        {
            using var fs = new FileStream(texPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var br = new BinaryReader(fs);

            if (fs.Length < 0x40) return false;

            // Same offset as CharacterAnalyzer.Format
            fs.Position = 0x04;
            var fmt = (TexFile.TextureFormat)br.ReadInt32();

            // Common FFXIV TEX header layout:
            // 0x08: width (u16)
            // 0x0A: height (u16)
            // 0x0C: depth (u16)   (often 1; can be >1 or used for cube faces depending on asset)
            // 0x0E: mipCount (u16)
            // 0x10: arraySize (u16) (often 1; can be >1 for arrays / cube maps)
            fs.Position = 0x08;
            ushort w = br.ReadUInt16();
            ushort h = br.ReadUInt16();
            ushort depth = br.ReadUInt16();
            ushort mipCount = br.ReadUInt16();
            ushort arraySize = br.ReadUInt16();

            if (w == 0 || h == 0) return false;

            int slices = Math.Max(1, (int)depth) * Math.Max(1, (int)arraySize);

            // Clamp mipCount to what the dimensions can actually support.
            int maxMips = 1;
            int maxDim = Math.Max((int)w, (int)h);
            while (maxDim > 1)
            {
                maxDim >>= 1;
                maxMips++;
            }

            int mips = mipCount <= 0 ? 1 : Math.Min(mipCount, (ushort)maxMips);

            // format sizing
            int blockBytes = 0;
            int blockW = 1, blockH = 1;
            int bytesPerPixel = 0;

            switch (fmt)
            {
                // 4bpp (8 bytes per 4x4 block)
                case TexFile.TextureFormat.BC1:
                case TexFile.TextureFormat.BC4:
                    blockBytes = 8; blockW = 4; blockH = 4;
                    break;

                // 8bpp (16 bytes per 4x4 block)
                case TexFile.TextureFormat.BC2:
                case TexFile.TextureFormat.BC3:
                case TexFile.TextureFormat.BC5:
                case TexFile.TextureFormat.BC7:
                    blockBytes = 16; blockW = 4; blockH = 4;
                    break;

                // Common uncompressed (32bpp)
                case TexFile.TextureFormat.A8R8G8B8:
                    bytesPerPixel = 4;
                    break;

                default:
                    // Fallback: unknown format -> estimate from file payload size.
                    // Use a conservative header skip; TEX headers are typically small,
                    // but we keep it stable and non-zero.
                    vramBytes = Math.Max(0, (fs.Length - 0x80)) * slices;
                    return vramBytes > 0;
            }

            long total = 0;
            int mw = w;
            int mh = h;

            for (int m = 0; m < mips; m++)
            {
                long levelBytes;

                if (bytesPerPixel > 0)
                {
                    levelBytes = (long)mw * mh * bytesPerPixel;
                }
                else
                {
                    int bw = (mw + (blockW - 1)) / blockW;
                    int bh = (mh + (blockH - 1)) / blockH;
                    levelBytes = (long)bw * bh * blockBytes;
                }

                total += levelBytes;

                mw = Math.Max(1, mw / 2);
                mh = Math.Max(1, mh / 2);
            }

            total *= slices;

            vramBytes = Math.Max(0, total);
            return vramBytes > 0;
        }
        catch
        {
            return false;
        }
    }

    public static bool TryEstimateMdlVramBytes(string mdlPath, out long vramBytes)
    {
        vramBytes = 0;

        try
        {
            var mdl = new MdlFile(mdlPath);

            // Sum all available LODs to keep this stable + deterministic.
            var lods = Math.Clamp(mdl.LodCount, (byte)0, (byte)3);

            long sum = 0;
            for (int i = 0; i < lods; i++)
            {
                sum += mdl.VertexBufferSize[i];
                sum += mdl.IndexBufferSize[i];
            }

            vramBytes = Math.Max(0, sum);
            return vramBytes > 0;
        }
        catch
        {
            return false;
        }
    }
}