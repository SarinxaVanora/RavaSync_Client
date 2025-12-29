using System.IO;
using System.Text;

namespace RavaSync.Fonts;

internal readonly struct TrueTypeMetrics
{
    public readonly int UnitsPerEm;
    public readonly short Ascent;
    public readonly short CapHeight;
    public readonly bool HasCapHeight;

    public TrueTypeMetrics(int upm, short asc, short cap, bool hasCap)
    { UnitsPerEm = upm; Ascent = asc; CapHeight = cap; HasCapHeight = hasCap; }

    public static TrueTypeMetrics TryRead(string path)
    {
        try
        {
            using var fs = File.OpenRead(path);
            using var br = new BinaryReader(fs, Encoding.ASCII, leaveOpen: true);

            // sfnt header
            br.ReadUInt32();
            ushort numTables = Swap(br.ReadUInt16());
            br.ReadUInt16(); br.ReadUInt16(); br.ReadUInt16();

            uint headOff = 0, os2Off = 0, hheaOff = 0;
            for (int i = 0; i < numTables; i++)
            {
                var tag = br.ReadUInt32(); br.ReadUInt32();
                var off = br.ReadUInt32(); br.ReadUInt32();
                var s = TagToString(tag);
                if (s == "head") headOff = off;
                if (s == "OS/2") os2Off = off;
                if (s == "hhea") hheaOff = off;
            }

            int upm = 0; short asc = 0; short cap = 0; bool hasCap = false;

            if (headOff != 0) { fs.Position = headOff + 18; upm = Swap(br.ReadUInt16()); }
            if (hheaOff != 0) { fs.Position = hheaOff + 4; asc = (short)Swap(br.ReadUInt16()); }
            if (os2Off != 0)
            {
                fs.Position = os2Off + 2; var ver = Swap(br.ReadUInt16());
                if (ver >= 2) { fs.Position = os2Off + 88; cap = (short)Swap(br.ReadUInt16()); hasCap = cap != 0; }
            }

            return new TrueTypeMetrics(upm, asc, cap, hasCap);
        }
        catch { return new TrueTypeMetrics(0, 0, 0, false); }
    }

    private static ushort Swap(ushort v) => (ushort)((v >> 8) | (v << 8));
    private static string TagToString(uint t)
        => Encoding.ASCII.GetString(new[] { (byte)(t >> 24), (byte)(t >> 16), (byte)(t >> 8), (byte)t });
}
