using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using RavaSync.Interop.GameModel;
using RavaSync.Services.Gpu;
using static Lumina.Data.Parsing.MdlStructs;

namespace RavaSync.Services.Optimisation;

public sealed partial class MeshOptimisationService
{
    private const int MinGpuAttributePairCount = 64;

    private void TryAugmentProtectedVerticesWithGpu(
        HashSet<ushort> protectedVertices,
        Vector3[] positions,
        IReadOnlyList<Vector3>? authoritativeNormals,
        IReadOnlyList<Vector2>? primaryUvs,
        IReadOnlyList<MeshSkinningVertex>? skinning)
    {
        if (positions.Length == 0)
            return;

        bool hasAuthoritativeNormals = authoritativeNormals is { Count: > 0 };
        bool hasPrimaryUvs = primaryUvs is { Count: > 0 };
        bool hasSkinning = skinning is { Count: > 0 };
        if (!hasAuthoritativeNormals && !hasPrimaryUvs && !hasSkinning)
            return;

        var pairs = BuildGpuAttributePairs(positions);
        if (pairs.Count < MinGpuAttributePairCount)
            return;

        ReadOnlySpan<Vector3> normals = authoritativeNormals is { Count: > 0 } ? authoritativeNormals.ToArray() : Array.Empty<Vector3>();
        ReadOnlySpan<Vector2> uvs = primaryUvs is { Count: > 0 } ? primaryUvs.ToArray() : Array.Empty<Vector2>();
        var gpuSkinning = skinning is { Count: > 0 } ? ConvertToGpuSkinning(skinning) : Array.Empty<D3D11MeshAnalysisService.GpuSkinningVertex>();

        if (!_d3d11MeshAnalysisService.TryBuildAttributeProtectionMask(pairs.ToArray(), positions.Length, normals, uvs, gpuSkinning, out var mask))
            return;

        for (int i = 0; i < mask.Length && i <= ushort.MaxValue; i++)
        {
            if (mask[i])
                protectedVertices.Add((ushort)i);
        }
    }

    private static List<D3D11MeshAnalysisService.AttributePair> BuildGpuAttributePairs(Vector3[] positions)
    {
        var groups = new Dictionary<QuantizedPositionKey, List<ushort>>();
        int limit = Math.Min(positions.Length, ushort.MaxValue + 1);
        for (int i = 0; i < limit; i++)
        {
            var key = QuantizedPositionKey.From(positions[i], PositionTwinQuantizationStep);
            if (!groups.TryGetValue(key, out var vertices))
            {
                vertices = [];
                groups[key] = vertices;
            }

            vertices.Add((ushort)i);
        }

        var pairs = new List<D3D11MeshAnalysisService.AttributePair>();
        foreach (var group in groups.Values)
        {
            if (group.Count < 2)
                continue;

            for (int i = 0; i < group.Count; i++)
            {
                ushort left = group[i];
                for (int j = i + 1; j < group.Count; j++)
                {
                    ushort right = group[j];
                    pairs.Add(new D3D11MeshAnalysisService.AttributePair(left, right));
                }
            }
        }

        return pairs;
    }

    private void TryAugmentBodyProtectedVerticesWithGpu(HashSet<ushort> protectedVertices, IReadOnlyList<Vector3> positions, BodySurfaceGuide bodySurface, float protectionDistance)
    {
        if (positions.Count == 0 || bodySurface.Triangles.Length == 0)
            return;

        var queryPositions = positions as Vector3[] ?? positions.ToArray();
        if (queryPositions.Length == 0)
            return;

        var bodyTriangles = ConvertToGpuBodyTriangles(bodySurface);
        if (bodyTriangles.Length == 0)
            return;

        if (!_d3d11MeshAnalysisService.TryBuildBodyProximityMask(queryPositions, bodyTriangles, protectionDistance, out var mask))
            return;

        for (int i = 0; i < mask.Length && i <= ushort.MaxValue; i++)
        {
            if (mask[i])
                protectedVertices.Add((ushort)i);
        }
    }

    private static D3D11MeshAnalysisService.GpuBodyTriangle[] ConvertToGpuBodyTriangles(BodySurfaceGuide bodySurface)
    {
        if (bodySurface.Triangles.Length == 0 || bodySurface.Positions.Length == 0)
            return Array.Empty<D3D11MeshAnalysisService.GpuBodyTriangle>();

        var result = new D3D11MeshAnalysisService.GpuBodyTriangle[bodySurface.Triangles.Length];
        int count = 0;
        for (int i = 0; i < bodySurface.Triangles.Length; i++)
        {
            var triangle = bodySurface.Triangles[i];
            if ((uint)triangle.A >= (uint)bodySurface.Positions.Length
                || (uint)triangle.B >= (uint)bodySurface.Positions.Length
                || (uint)triangle.C >= (uint)bodySurface.Positions.Length)
            {
                continue;
            }

            var a = bodySurface.Positions[triangle.A];
            var b = bodySurface.Positions[triangle.B];
            var c = bodySurface.Positions[triangle.C];
            var minValue = Vector3.Min(a, Vector3.Min(b, c));
            var maxValue = Vector3.Max(a, Vector3.Max(b, c));
            result[count++] = new D3D11MeshAnalysisService.GpuBodyTriangle(a, b, c, minValue, maxValue);
        }

        if (count == result.Length)
            return result;

        Array.Resize(ref result, count);
        return result;
    }

    private static D3D11MeshAnalysisService.GpuSkinningVertex[] ConvertToGpuSkinning(IReadOnlyList<MeshSkinningVertex> skinning)
    {
        var result = new D3D11MeshAnalysisService.GpuSkinningVertex[skinning.Count];
        for (int i = 0; i < skinning.Count; i++)
        {
            var vertex = skinning[i];
            result[i] = new D3D11MeshAnalysisService.GpuSkinningVertex(
                vertex.Bone0,
                vertex.Bone1,
                vertex.Bone2,
                vertex.Bone3,
                vertex.Weights);
        }

        return result;
    }


private bool TryExpandProtectedVerticesWithGpu(HashSet<ushort> protectedVertices, ReadOnlySpan<byte> bytes, long absoluteStart, uint indexCount, int vertexCount)
{
    if (protectedVertices.Count == 0 || indexCount < 3 || vertexCount <= 0)
        return false;

    var indices = ReadIndexBufferAsUInt32(bytes, absoluteStart, indexCount);
    if (indices.Length == 0)
        return false;

    var seedMask = new bool[vertexCount];
    foreach (ushort vertex in protectedVertices)
    {
        if ((uint)vertex < (uint)vertexCount)
            seedMask[vertex] = true;
    }

    if (!_d3d11MeshAnalysisService.TryExpandProtectedVertexMask(indices, seedMask, vertexCount, out var expandedMask))
        return false;

    for (int i = 0; i < expandedMask.Length && i <= ushort.MaxValue; i++)
    {
        if (expandedMask[i])
            protectedVertices.Add((ushort)i);
    }

    return true;
}

private bool TryComputeAverageEdgeLengthWithGpu(ReadOnlySpan<byte> bytes, long absoluteStart, uint indexCount, IReadOnlyList<Vector3> positions, out float averageEdgeLength)
{
    averageEdgeLength = 0f;
    if (positions.Count == 0 || indexCount < 3)
        return false;

    var indices = ReadIndexBufferAsUInt32(bytes, absoluteStart, indexCount);
    if (indices.Length == 0)
        return false;

    var positionArray = positions as Vector3[] ?? positions.ToArray();
    return _d3d11MeshAnalysisService.TryComputeAverageEdgeLength(indices, positionArray, out averageEdgeLength);
}


private bool TryBuildBodyProximityVertexMaskWithGpu(IReadOnlyList<Vector3> positions, BodyCollisionGuard bodyCollisionGuard, out bool[] mask)
{
    mask = [];
    if (positions.Count == 0 || bodyCollisionGuard.Surface.Triangles.Length == 0)
        return false;

    float maxDistance = MathF.Sqrt(bodyCollisionGuard.DistanceSq);
    if (!(maxDistance > 0f))
        return false;

    var queryPositions = positions as Vector3[] ?? positions.ToArray();
    if (queryPositions.Length == 0)
        return false;

    var bodyTriangles = ConvertToGpuBodyTriangles(bodyCollisionGuard.Surface);
    if (bodyTriangles.Length == 0)
        return false;

    return _d3d11MeshAnalysisService.TryBuildBodyProximityMask(queryPositions, bodyTriangles, maxDistance, out mask);
}

private bool TryComputeNearBodyTriangleRatioWithGpu(IReadOnlyList<TriangleIndices> triangles, IReadOnlyList<Vector3> positions, BodyCollisionGuard bodyCollisionGuard, out float nearBodyRatio)
{
    nearBodyRatio = 0f;
    if (triangles.Count == 0 || positions.Count == 0 || bodyCollisionGuard.Surface.Triangles.Length == 0)
        return false;

    float maxDistance = MathF.Sqrt(bodyCollisionGuard.DistanceSq);
    if (!(maxDistance > 0f))
        return false;

    if (!TryBuildBodyProximityVertexMaskWithGpu(positions, bodyCollisionGuard, out var mask))
        return false;

    int nearBodyTriangles = 0;
    foreach (var triangle in triangles)
    {
        if (((uint)triangle.A < (uint)mask.Length && mask[triangle.A])
            || ((uint)triangle.B < (uint)mask.Length && mask[triangle.B])
            || ((uint)triangle.C < (uint)mask.Length && mask[triangle.C]))
        {
            nearBodyTriangles++;
        }
    }

    nearBodyRatio = (float)nearBodyTriangles / triangles.Count;
    return true;
}

private static uint[] ReadIndexBufferAsUInt32(ReadOnlySpan<byte> bytes, long absoluteStart, uint indexCount)
{
    if (indexCount == 0)
        return Array.Empty<uint>();

    long absoluteEnd = absoluteStart + ((long)indexCount * sizeof(ushort));
    if (absoluteStart < 0 || absoluteEnd > bytes.Length)
        return Array.Empty<uint>();

    var result = new uint[indexCount];
    int readOffset = checked((int)absoluteStart);
    for (uint i = 0; i < indexCount; i++)
    {
        result[i] = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(readOffset + checked((int)(i * sizeof(ushort))), sizeof(ushort)));
    }

    return result;
}

}
