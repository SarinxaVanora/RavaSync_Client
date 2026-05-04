using Lumina.Data;
using Lumina.Extensions;
using System.Text;
using static Lumina.Data.Parsing.MdlStructs;

namespace RavaSync.Interop.GameModel;

#pragma warning disable S1104 // Fields should not have public accessibility

// This code is completely and shamelessly borrowed from Penumbra to load V5 and V6 model files.
// Original Source: https://github.com/Ottermandias/Penumbra.GameData/blob/main/Files/MdlFile.cs
public class MdlFile
{
    public const int V5 = 0x01000005;
    public const int V6 = 0x01000006;
    public const uint NumVertices = 17;
    public const uint FileHeaderSize = 0x44;

    // Raw data to write back.
    public uint Version = 0x01000005;
    public float Radius;
    public float ModelClipOutDistance;
    public float ShadowClipOutDistance;
    public byte BgChangeMaterialIndex;
    public byte BgCrestChangeMaterialIndex;
    public ushort CullingGridCount;
    public byte Flags3;
    public byte Unknown6;
    public ushort Unknown8;
    public ushort Unknown9;

    // Offsets are stored relative to RuntimeSize instead of file start.
    public uint[] VertexOffset = [0, 0, 0];
    public uint[] IndexOffset = [0, 0, 0];

    public uint[] VertexBufferSize = [0, 0, 0];
    public uint[] IndexBufferSize = [0, 0, 0];
    public byte LodCount;
    public bool EnableIndexBufferStreaming;
    public bool EnableEdgeGeometry;

    public ModelFlags1 Flags1;
    public ModelFlags2 Flags2;

    public VertexDeclarationStruct[] VertexDeclarations = [];
    public ElementIdStruct[] ElementIds = [];
    public MeshStruct[] Meshes = [];
    public BoundingBoxStruct[] BoneBoundingBoxes = [];
    public LodStruct[] Lods = [];
    public ExtraLodStruct[] ExtraLods = [];
    public long DataSectionOffset;
    public long LodTableOffset;
    public long[] LodOffsets = [];
    public int LodStructSize;
    public long MeshTableOffset;
    public long[] MeshOffsets = [];
    public int MeshStructSize;
    public ushort BoneCount;
    public ushort MaterialCount;
    public ushort AttributeCount;
    public ushort ShapeCount;
    public ushort ShapeMeshCount;
    public ushort ShapeValueCount;
    public ushort TotalSubmeshCount;
    public SubmeshStruct[] SubMeshes = [];
    public long SubMeshTableOffset;
    public long[] SubMeshOffsets = [];
    public int SubMeshStructSize;
    public string[] Strings = [];
    public string[] Attributes = [];
    public string[] MaterialStrings = [];
    public string SourcePath = string.Empty;

    public MdlFile(string filePath)
    {
        SourcePath = filePath ?? string.Empty;
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var r = new LuminaBinaryReader(stream);

        var header = LoadModelFileHeader(r);
        LodCount = header.LodCount;
        VertexBufferSize = header.VertexBufferSize;
        IndexBufferSize = header.IndexBufferSize;
        VertexOffset = header.VertexOffset;
        IndexOffset = header.IndexOffset;

        var dataOffset = FileHeaderSize + header.RuntimeSize + header.StackSize;
        DataSectionOffset = dataOffset;
        for (var i = 0; i < LodCount; ++i)
        {
            VertexOffset[i] -= dataOffset;
            IndexOffset[i] -= dataOffset;
        }

        VertexDeclarations = new VertexDeclarationStruct[header.VertexDeclarationCount];
        for (var i = 0; i < header.VertexDeclarationCount; ++i)
            VertexDeclarations[i] = VertexDeclarationStruct.Read(r);

        var strings = LoadStrings(r);
        var stringOffsets = strings.Item1;
        Strings = strings.Item2;

        var modelHeader = LoadModelHeader(r);
        ElementIds = new ElementIdStruct[modelHeader.ElementIdCount];
        for (var i = 0; i < modelHeader.ElementIdCount; i++)
            ElementIds[i] = ElementIdStruct.Read(r);

        Lods = new LodStruct[3];
        LodOffsets = new long[3];
        LodTableOffset = r.BaseStream.Position;
        long lodStructSize = 0;
        for (var i = 0; i < 3; i++)
        {
            var before = r.BaseStream.Position;
            LodOffsets[i] = before;
            var lod = r.ReadStructure<LodStruct>();
            if (lodStructSize == 0)
                lodStructSize = r.BaseStream.Position - before;
            if (i < LodCount)
            {
                lod.VertexDataOffset -= dataOffset;
                lod.IndexDataOffset -= dataOffset;
            }

            Lods[i] = lod;
        }

        LodStructSize = (int)lodStructSize;

        ExtraLods = (modelHeader.Flags2 & ModelFlags2.ExtraLodEnabled) != 0
            ? r.ReadStructuresAsArray<ExtraLodStruct>(3)
            : [];

        Meshes = new MeshStruct[modelHeader.MeshCount];
        MeshOffsets = new long[modelHeader.MeshCount];
        MeshTableOffset = r.BaseStream.Position;
        long meshStructSize = 0;
        for (var i = 0; i < modelHeader.MeshCount; i++)
        {
            var before = r.BaseStream.Position;
            MeshOffsets[i] = before;
            Meshes[i] = MeshStruct.Read(r);
            if (meshStructSize == 0)
                meshStructSize = r.BaseStream.Position - before;
        }

        MeshStructSize = (int)meshStructSize;

        Attributes = new string[modelHeader.AttributeCount];
        for (var i = 0; i < modelHeader.AttributeCount; ++i)
        {
            var offset = r.ReadUInt32();
            var stringIndex = stringOffsets.AsSpan().IndexOf(offset);
            Attributes[i] = stringIndex >= 0 ? Strings[stringIndex] : string.Empty;
        }

        _ = r.ReadStructuresAsArray<TerrainShadowMeshStruct>(modelHeader.TerrainShadowMeshCount);

        SubMeshes = new SubmeshStruct[modelHeader.SubmeshCount];
        SubMeshOffsets = new long[modelHeader.SubmeshCount];
        SubMeshTableOffset = r.BaseStream.Position;
        long subMeshStructSize = 0;
        for (var i = 0; i < modelHeader.SubmeshCount; i++)
        {
            var before = r.BaseStream.Position;
            SubMeshOffsets[i] = before;
            SubMeshes[i] = r.ReadStructure<SubmeshStruct>();
            if (subMeshStructSize == 0)
                subMeshStructSize = r.BaseStream.Position - before;
        }

        SubMeshStructSize = (int)subMeshStructSize;
    }

    private ModelFileHeader LoadModelFileHeader(LuminaBinaryReader r)
    {
        var header = ModelFileHeader.Read(r);
        Version = header.Version;
        EnableIndexBufferStreaming = header.EnableIndexBufferStreaming;
        EnableEdgeGeometry = header.EnableEdgeGeometry;
        return header;
    }

    private ModelHeader LoadModelHeader(BinaryReader r)
    {
        var modelHeader = r.ReadStructure<ModelHeader>();
        Radius = modelHeader.Radius;
        BoneCount = modelHeader.BoneCount;
        MaterialCount = modelHeader.MaterialCount;
        AttributeCount = modelHeader.AttributeCount;
        ShapeCount = modelHeader.ShapeCount;
        ShapeMeshCount = modelHeader.ShapeMeshCount;
        ShapeValueCount = modelHeader.ShapeValueCount;
        TotalSubmeshCount = modelHeader.SubmeshCount;
        Flags1 = modelHeader.Flags1;
        Flags2 = modelHeader.Flags2;
        ModelClipOutDistance = modelHeader.ModelClipOutDistance;
        ShadowClipOutDistance = modelHeader.ShadowClipOutDistance;
        CullingGridCount = modelHeader.CullingGridCount;
        Flags3 = modelHeader.Flags3;
        Unknown6 = modelHeader.Unknown6;
        Unknown8 = modelHeader.Unknown8;
        Unknown9 = modelHeader.Unknown9;
        BgChangeMaterialIndex = modelHeader.BGChangeMaterialIndex;
        BgCrestChangeMaterialIndex = modelHeader.BGCrestChangeMaterialIndex;

        MaterialStrings = ExtractMaterialStrings(Strings, BoneCount, MaterialCount);
        return modelHeader;
    }

    private static (uint[], string[]) LoadStrings(BinaryReader r)
    {
        var stringCount = r.ReadUInt16();
        r.ReadUInt16();
        var stringSize = (int)r.ReadUInt32();
        var stringData = r.ReadBytes(stringSize);
        var start = 0;
        var strings = new string[stringCount];
        var offsets = new uint[stringCount];
        for (var i = 0; i < stringCount; ++i)
        {
            var span = stringData.AsSpan(start);
            var idx = span.IndexOf((byte)'\0');
            strings[i] = Encoding.UTF8.GetString(span[..idx]);
            offsets[i] = (uint)start;
            start = start + idx + 1;
        }

        return (offsets, strings);
    }

    public unsafe struct ModelHeader
    {
        // MeshHeader
        public float Radius;
        public ushort MeshCount;
        public ushort AttributeCount;
        public ushort SubmeshCount;
        public ushort MaterialCount;
        public ushort BoneCount;
        public ushort BoneTableCount;
        public ushort ShapeCount;
        public ushort ShapeMeshCount;
        public ushort ShapeValueCount;
        public byte LodCount;
        public ModelFlags1 Flags1;
        public ushort ElementIdCount;
        public byte TerrainShadowMeshCount;
        public ModelFlags2 Flags2;
        public float ModelClipOutDistance;
        public float ShadowClipOutDistance;
        public ushort CullingGridCount;
        public ushort TerrainShadowSubmeshCount;
        public byte Flags3;
        public byte BGChangeMaterialIndex;
        public byte BGCrestChangeMaterialIndex;
        public byte Unknown6;
        public ushort BoneTableArrayCountTotal;
        public ushort Unknown8;
        public ushort Unknown9;
        private fixed byte _padding[6];
    }

    private static string[] ExtractMaterialStrings(string[] strings, ushort boneCount, ushort materialCount)
    {
        if (strings.Length == 0 || materialCount == 0)
            return [];

        int start = Math.Min(strings.Length, boneCount);
        if (start + materialCount <= strings.Length)
        {
            var directSlice = strings.Skip(start).Take(materialCount).ToArray();
            if (directSlice.Length == materialCount && directSlice.All(static s => s.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)))
                return directSlice;
        }

        return strings.Where(static s => s.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(materialCount)
            .ToArray();
    }

    public struct ShapeStruct
    {
        public uint StringOffset;
        public ushort[] ShapeMeshStartIndex;
        public ushort[] ShapeMeshCount;

        public static ShapeStruct Read(LuminaBinaryReader br)
        {
            ShapeStruct ret = new ShapeStruct();
            ret.StringOffset = br.ReadUInt32();
            ret.ShapeMeshStartIndex = br.ReadUInt16Array(3);
            ret.ShapeMeshCount = br.ReadUInt16Array(3);
            return ret;
        }
    }

    [Flags]
    public enum ModelFlags1 : byte
    {
        DustOcclusionEnabled = 0x80,
        SnowOcclusionEnabled = 0x40,
        RainOcclusionEnabled = 0x20,
        Unknown1 = 0x10,
        LightingReflectionEnabled = 0x08,
        WavingAnimationDisabled = 0x04,
        LightShadowDisabled = 0x02,
        ShadowDisabled = 0x01,
    }

    [Flags]
    public enum ModelFlags2 : byte
    {
        Unknown2 = 0x80,
        BgUvScrollEnabled = 0x40,
        EnableForceNonResident = 0x20,
        ExtraLodEnabled = 0x10,
        ShadowMaskEnabled = 0x08,
        ForceLodRangeEnabled = 0x04,
        EdgeGeometryEnabled = 0x02,
        Unknown3 = 0x01
    }

    public struct VertexDeclarationStruct
    {
        // There are always 17, but stop when stream = -1
        public VertexElement[] VertexElements;

        public static VertexDeclarationStruct Read(LuminaBinaryReader br)
        {
            VertexDeclarationStruct ret = new VertexDeclarationStruct();

            var elems = new List<VertexElement>();

            // Read the vertex elements that we need
            var thisElem = br.ReadStructure<VertexElement>();
            do
            {
                elems.Add(thisElem);
                thisElem = br.ReadStructure<VertexElement>();
            } while (thisElem.Stream != 255);

            // Skip the number of bytes that we don't need to read
            // We skip elems.Count * 9 because we had to read the invalid element
            int toSeek = 17 * 8 - (elems.Count + 1) * 8;
            br.Seek(br.BaseStream.Position + toSeek);

            ret.VertexElements = elems.ToArray();

            return ret;
        }
    }
}
#pragma warning restore S1104 // Fields should not have public accessibility