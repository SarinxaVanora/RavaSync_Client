using System.Buffers.Binary;
using static Lumina.Data.Parsing.MdlStructs;
using System.Numerics;
using CoreBoneWeight = RavaSync.Services.Optimisation.Reduction.BoneWeight;
using CoreVec2 = RavaSync.Services.Optimisation.Reduction.Vector2F;
using CoreVec3 = RavaSync.Services.Optimisation.Reduction.Vector3F;
using CoreVec3d = RavaSync.Services.Optimisation.Reduction.Vector3;
using CoreVec4 = RavaSync.Services.Optimisation.Reduction.Vector4F;

namespace RavaSync.Services.Optimisation;

public sealed partial class MeshOptimisationService
{
    private bool[]? BuildPredictableAttributeProtectedVertexMask(PredictableDecodedMeshData decoded, PredictableVertexFormat format)
    {
        var positions = decoded.Positions;
        if (positions.Length == 0)
            return null;

        var uvChannels = decoded.UvChannels;
        bool hasUv = format.UvChannelCount > 0 && uvChannels != null && uvChannels.Length > 0;
        var normals = decoded.Normals;
        bool hasNormals = format.HasNormals && normals != null && normals.Length == positions.Length;
        var boneWeights = decoded.BoneWeights;
        bool hasBones = format.HasSkinning && boneWeights != null && boneWeights.Length == positions.Length;
        if (!hasUv && !hasBones && !hasNormals)
            return null;

        const double positionEpsilon = 1e-6;
        double inv = 1d / positionEpsilon;
        var buckets = new Dictionary<PredictablePositionKey, List<int>>(positions.Length);
        for (int i = 0; i < positions.Length; i++)
        {
            var position = positions[i];
            var key = new PredictablePositionKey(
                (long)Math.Round(position.x * inv),
                (long)Math.Round(position.y * inv),
                (long)Math.Round(position.z * inv));

            if (!buckets.TryGetValue(key, out var list))
            {
                list = [];
                buckets[key] = list;
            }

            list.Add(i);
        }

        var pairs = new List<(int First, int Current)>();
        foreach (var bucket in buckets.Values)
        {
            if (bucket.Count <= 1)
                continue;

            for (int i = 0; i < bucket.Count; i++)
            {
                int first = bucket[i];
                for (int j = i + 1; j < bucket.Count; j++)
                    pairs.Add((first, bucket[j]));
            }
        }

        if (pairs.Count == 0)
            return null;

        bool[]? protectedVertices = null;
        float uvThresholdSq = PredictableUvSimilarityThreshold * PredictableUvSimilarityThreshold;
        float normalThresholdCos = MathF.Cos(MathF.Max(0f, PredictableNormalSimilarityThresholdDegrees) * (MathF.PI / 180f));
        foreach (var pair in pairs)
        {
            if (hasUv && !ArePredictableUvsSimilar(uvChannels!, format.UvChannelDimensions, format.UvChannelCount, pair.First, pair.Current, uvThresholdSq))
            {
                protectedVertices ??= new bool[positions.Length];
                protectedVertices[pair.First] = true;
                protectedVertices[pair.Current] = true;
                continue;
            }

            if (hasNormals && !ArePredictableNormalsSimilar(normals!, pair.First, pair.Current, normalThresholdCos))
            {
                protectedVertices ??= new bool[positions.Length];
                protectedVertices[pair.First] = true;
                protectedVertices[pair.Current] = true;
                continue;
            }

            if (hasBones && !ArePredictableBoneWeightsSimilar(boneWeights!, pair.First, pair.Current, PredictableBoneWeightSimilarityThreshold))
            {
                protectedVertices ??= new bool[positions.Length];
                protectedVertices[pair.First] = true;
                protectedVertices[pair.Current] = true;
            }
        }

        return protectedVertices;
    }

    private static bool[]? MergePredictableProtectedVertexMasks(bool[]? left, bool[]? right, int vertexCount)
    {
        if (vertexCount <= 0)
            return null;

        if (left == null || left.Length != vertexCount)
            left = null;
        if (right == null || right.Length != vertexCount)
            right = null;

        if (left == null)
            return right;
        if (right == null)
            return left;

        var merged = new bool[vertexCount];
        bool any = false;
        for (int i = 0; i < vertexCount; i++)
        {
            bool value = left[i] || right[i];
            merged[i] = value;
            any |= value;
        }

        return any ? merged : null;
    }


    private bool TryValidatePredictableReplacementPayloadRoundTrip(
        PredictableVertexFormat format,
        MeshStruct originalMesh,
        MeshStruct updatedMesh,
        SubmeshStruct[] updatedSubMeshes,
        byte[][] streamBuffers,
        ushort[] encodedIndices,
        PredictableDecodedMeshData expectedDecoded,
        int[][] expectedSubMeshIndices,
        out string? reason)
    {
        reason = null;

        if (streamBuffers.Length < 3)
        {
            reason = "Missing vertex streams for round-trip validation.";
            return false;
        }

        var syntheticMesh = updatedMesh;
        syntheticMesh.StartIndex = 0;
        syntheticMesh.IndexCount = checked((uint)encodedIndices.Length);
        syntheticMesh.VertexCount = updatedMesh.VertexCount;
        if (syntheticMesh.VertexBufferOffset.Length > 0)
            syntheticMesh.VertexBufferOffset[0] = 0;
        if (syntheticMesh.VertexBufferOffset.Length > 1)
            syntheticMesh.VertexBufferOffset[1] = checked((uint)streamBuffers[0].Length);
        if (syntheticMesh.VertexBufferOffset.Length > 2)
            syntheticMesh.VertexBufferOffset[2] = checked((uint)(streamBuffers[0].Length + streamBuffers[1].Length));

        int vertexBytesLength = streamBuffers[0].Length + streamBuffers[1].Length + streamBuffers[2].Length;
        var syntheticBytes = new byte[vertexBytesLength + (encodedIndices.Length * sizeof(ushort))];
        int writeOffset = 0;
        Array.Copy(streamBuffers[0], 0, syntheticBytes, writeOffset, streamBuffers[0].Length);
        writeOffset += streamBuffers[0].Length;
        Array.Copy(streamBuffers[1], 0, syntheticBytes, writeOffset, streamBuffers[1].Length);
        writeOffset += streamBuffers[1].Length;
        Array.Copy(streamBuffers[2], 0, syntheticBytes, writeOffset, streamBuffers[2].Length);
        writeOffset += streamBuffers[2].Length;

        for (int i = 0; i < encodedIndices.Length; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(syntheticBytes.AsSpan(writeOffset + (i * sizeof(ushort)), sizeof(ushort)), encodedIndices[i]);
        }

        if (!TryDecodePredictableMeshDataCore(
                syntheticBytes,
                syntheticMesh,
                0,
                vertexBytesLength,
                format,
                updatedSubMeshes,
                out var roundTripDecoded,
                out var roundTripSubMeshIndices,
                out reason))
        {
            return false;
        }

        return TryValidatePredictableRoundTripMatch(expectedDecoded, expectedSubMeshIndices, roundTripDecoded, roundTripSubMeshIndices, format, out reason);
    }

    private static bool TryValidatePredictableRoundTripMatch(
        PredictableDecodedMeshData expectedDecoded,
        int[][] expectedSubMeshIndices,
        PredictableDecodedMeshData roundTripDecoded,
        int[][] roundTripSubMeshIndices,
        PredictableVertexFormat format,
        out string? reason)
    {
        reason = null;

        if (expectedDecoded.Positions.Length != roundTripDecoded.Positions.Length)
        {
            reason = "Vertex count changed after round-trip.";
            return false;
        }

        if (expectedSubMeshIndices.Length != roundTripSubMeshIndices.Length)
        {
            reason = "Submesh count changed after round-trip.";
            return false;
        }

        for (int subMeshIndex = 0; subMeshIndex < expectedSubMeshIndices.Length; subMeshIndex++)
        {
            var expectedIndices = expectedSubMeshIndices[subMeshIndex];
            var actualIndices = roundTripSubMeshIndices[subMeshIndex];
            if (expectedIndices.Length != actualIndices.Length)
            {
                reason = $"Submesh {subMeshIndex} index count changed after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedIndices.Length; i++)
            {
                if (expectedIndices[i] != actualIndices[i])
                {
                    reason = $"Submesh {subMeshIndex} index buffer changed after round-trip.";
                    return false;
                }
            }
        }

        for (int i = 0; i < expectedDecoded.Positions.Length; i++)
        {
            if (!ArePredictablePositionsSimilar(expectedDecoded.Positions[i], roundTripDecoded.Positions[i], PredictableRoundTripPositionTolerance))
            {
                reason = $"Vertex {i}: position changed after round-trip.";
                return false;
            }
        }

        if (format.HasNormals)
        {
            if (expectedDecoded.Normals == null || roundTripDecoded.Normals == null)
            {
                reason = "Normals missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.Normals.Length; i++)
            {
                if (!ArePredictableVec3Similar(expectedDecoded.Normals[i], roundTripDecoded.Normals[i], PredictableRoundTripPositionTolerance))
                {
                    reason = $"Vertex {i}: normal changed after round-trip.";
                    return false;
                }
            }
        }

        if (format.HasTangent1)
        {
            if (expectedDecoded.Tangents == null || roundTripDecoded.Tangents == null)
            {
                reason = "Tangents missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.Tangents.Length; i++)
            {
                if (!ArePredictableVec4Similar(expectedDecoded.Tangents[i], roundTripDecoded.Tangents[i], PredictableRoundTripPositionTolerance))
                {
                    reason = $"Vertex {i}: tangent changed after round-trip.";
                    return false;
                }
            }
        }

        if (format.HasTangent2)
        {
            if (expectedDecoded.Tangents2 == null || roundTripDecoded.Tangents2 == null)
            {
                reason = "Stored bitangents missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.Tangents2.Length; i++)
            {
                if (!ArePredictableVec4Similar(expectedDecoded.Tangents2[i], roundTripDecoded.Tangents2[i], PredictableRoundTripPositionTolerance))
                {
                    reason = $"Vertex {i}: stored bitangent changed after round-trip.";
                    return false;
                }
            }
        }

        if (format.ColorChannelCount > 0)
        {
            if (expectedDecoded.ColorChannels == null || roundTripDecoded.ColorChannels == null)
            {
                reason = "Color channels missing after round-trip.";
                return false;
            }

            for (int channel = 0; channel < format.ColorChannelCount; channel++)
            {
                var expectedColors = expectedDecoded.ColorChannels[channel];
                var actualColors = roundTripDecoded.ColorChannels[channel];
                if (expectedColors.Length != actualColors.Length)
                {
                    reason = $"Color channel {channel} changed vertex count after round-trip.";
                    return false;
                }

                for (int i = 0; i < expectedColors.Length; i++)
                {
                    if (!ArePredictableVec4Similar(expectedColors[i], actualColors[i], PredictableRoundTripPositionTolerance))
                    {
                        reason = $"Vertex {i}: color channel {channel} changed after round-trip.";
                        return false;
                    }
                }
            }
        }

        if (format.UvChannelCount > 0)
        {
            if (expectedDecoded.UvChannels == null || roundTripDecoded.UvChannels == null)
            {
                reason = "UV channels missing after round-trip.";
                return false;
            }

            for (int channel = 0; channel < format.UvChannelCount; channel++)
            {
                var expectedUvs = expectedDecoded.UvChannels[channel];
                var actualUvs = roundTripDecoded.UvChannels[channel];
                if (expectedUvs.Length != actualUvs.Length)
                {
                    reason = $"UV channel {channel} changed vertex count after round-trip.";
                    return false;
                }

                for (int i = 0; i < expectedUvs.Length; i++)
                {
                    if (!ArePredictableVec2Similar(expectedUvs[i], actualUvs[i], PredictableRoundTripPositionTolerance))
                    {
                        reason = $"Vertex {i}: UV channel {channel} changed after round-trip.";
                        return false;
                    }
                }
            }
        }

        if (format.HasPositionW)
        {
            if (expectedDecoded.PositionWs == null || roundTripDecoded.PositionWs == null)
            {
                reason = "Position W missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.PositionWs.Length; i++)
            {
                if (MathF.Abs(expectedDecoded.PositionWs[i] - roundTripDecoded.PositionWs[i]) > PredictableRoundTripPositionTolerance)
                {
                    reason = $"Vertex {i}: position W changed after round-trip.";
                    return false;
                }
            }
        }

        if (format.HasNormalW)
        {
            if (expectedDecoded.NormalWs == null || roundTripDecoded.NormalWs == null)
            {
                reason = "Normal W missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.NormalWs.Length; i++)
            {
                if (MathF.Abs(expectedDecoded.NormalWs[i] - roundTripDecoded.NormalWs[i]) > PredictableRoundTripPositionTolerance)
                {
                    reason = $"Vertex {i}: normal W changed after round-trip.";
                    return false;
                }
            }
        }

        if (format.HasSkinning)
        {
            if (expectedDecoded.BoneWeights == null || roundTripDecoded.BoneWeights == null)
            {
                reason = "Bone weights missing after round-trip.";
                return false;
            }

            for (int i = 0; i < expectedDecoded.BoneWeights.Length; i++)
            {
                if (!ArePredictableBoneWeightsEquivalent(expectedDecoded.BoneWeights[i], roundTripDecoded.BoneWeights[i], PredictableRoundTripWeightTolerance))
                {
                    reason = $"Vertex {i}: bone weights changed after round-trip.";
                    return false;
                }
            }
        }

        return true;
    }

    private static bool TryValidatePredictableShapePreservation(
        PredictableDecodedMeshData originalDecoded,
        int[][] originalSubMeshIndices,
        PredictableDecodedMeshData decimatedDecoded,
        int[][] decimatedSubMeshIndices,
        out string? reason)
    {
        reason = null;

        if (originalSubMeshIndices.Length != decimatedSubMeshIndices.Length)
        {
            reason = "Submesh count changed during shape validation.";
            return false;
        }

        if (originalDecoded.Positions.Length == 0 || decimatedDecoded.Positions.Length == 0)
        {
            reason = "Missing positions for shape validation.";
            return false;
        }

        for (int subMeshIndex = 0; subMeshIndex < originalSubMeshIndices.Length; subMeshIndex++)
        {
            var originalIndices = originalSubMeshIndices[subMeshIndex];
            var decimatedIndices = decimatedSubMeshIndices[subMeshIndex];
            int originalTriangles = originalIndices.Length / 3;
            int decimatedTriangles = decimatedIndices.Length / 3;

            if (originalTriangles <= 0)
                continue;

            if (decimatedTriangles <= 0)
            {
                reason = $"Submesh {subMeshIndex} lost all triangles during decimation.";
                return false;
            }

            if (!TryComputePredictableIndexedBounds(originalDecoded.Positions, originalIndices, out var originalMin, out var originalMax, out var originalCenter, out var originalDiagonal)
                || !TryComputePredictableIndexedBounds(decimatedDecoded.Positions, decimatedIndices, out var decimatedMin, out var decimatedMax, out var decimatedCenter, out var decimatedDiagonal))
            {
                reason = $"Submesh {subMeshIndex} bounds could not be computed.";
                return false;
            }

            double expansionTolerance = Math.Max(0.001d, originalDiagonal * 0.05d);
            if (!IsWithinExpandedBounds(decimatedMin, decimatedMax, originalMin, originalMax, expansionTolerance))
            {
                reason = $"Submesh {subMeshIndex} expanded outside original bounds.";
                return false;
            }

            double maxCenterShift = Math.Max(0.001d, originalDiagonal * 0.20d);
            if (DistanceSquared(originalCenter, decimatedCenter) > (maxCenterShift * maxCenterShift))
            {
                reason = $"Submesh {subMeshIndex} moved too far from its original center.";
                return false;
            }

            double maxDiagonal = Math.Max(originalDiagonal * 1.25d, originalDiagonal + expansionTolerance);
            if (decimatedDiagonal > maxDiagonal)
            {
                reason = $"Submesh {subMeshIndex} diagonal grew too large after decimation.";
                return false;
            }
        }

        return true;
    }

    private static bool TryComputePredictableIndexedBounds(
        CoreVec3d[] positions,
        int[] indices,
        out CoreVec3d min,
        out CoreVec3d max,
        out CoreVec3d center,
        out double diagonal)
    {
        min = default;
        max = default;
        center = default;
        diagonal = 0d;

        if (positions.Length == 0 || indices.Length == 0)
            return false;

        bool any = false;
        double minX = 0d, minY = 0d, minZ = 0d;
        double maxX = 0d, maxY = 0d, maxZ = 0d;
        for (int i = 0; i < indices.Length; i++)
        {
            int index = indices[i];
            if ((uint)index >= (uint)positions.Length)
                return false;

            var position = positions[index];
            if (!any)
            {
                minX = maxX = position.x;
                minY = maxY = position.y;
                minZ = maxZ = position.z;
                any = true;
                continue;
            }

            minX = Math.Min(minX, position.x);
            minY = Math.Min(minY, position.y);
            minZ = Math.Min(minZ, position.z);
            maxX = Math.Max(maxX, position.x);
            maxY = Math.Max(maxY, position.y);
            maxZ = Math.Max(maxZ, position.z);
        }

        if (!any)
            return false;

        min = new CoreVec3d(minX, minY, minZ);
        max = new CoreVec3d(maxX, maxY, maxZ);
        center = new CoreVec3d((minX + maxX) * 0.5d, (minY + maxY) * 0.5d, (minZ + maxZ) * 0.5d);
        diagonal = Math.Sqrt(((maxX - minX) * (maxX - minX)) + ((maxY - minY) * (maxY - minY)) + ((maxZ - minZ) * (maxZ - minZ)));
        return double.IsFinite(diagonal);
    }

    private static bool IsWithinExpandedBounds(CoreVec3d testMin, CoreVec3d testMax, CoreVec3d baseMin, CoreVec3d baseMax, double tolerance)
        => testMin.x >= baseMin.x - tolerance
            && testMin.y >= baseMin.y - tolerance
            && testMin.z >= baseMin.z - tolerance
            && testMax.x <= baseMax.x + tolerance
            && testMax.y <= baseMax.y + tolerance
            && testMax.z <= baseMax.z + tolerance;

    private static double DistanceSquared(CoreVec3d left, CoreVec3d right)
    {
        double dx = left.x - right.x;
        double dy = left.y - right.y;
        double dz = left.z - right.z;
        return (dx * dx) + (dy * dy) + (dz * dz);
    }

    private static bool ArePredictablePositionsSimilar(CoreVec3d left, CoreVec3d right, float tolerance)
        => Math.Abs(left.x - right.x) <= tolerance
            && Math.Abs(left.y - right.y) <= tolerance
            && Math.Abs(left.z - right.z) <= tolerance;

    private static bool ArePredictableVec2Similar(CoreVec2 left, CoreVec2 right, float tolerance)
        => MathF.Abs(left.x - right.x) <= tolerance
            && MathF.Abs(left.y - right.y) <= tolerance;

    private static bool ArePredictableVec3Similar(CoreVec3 left, CoreVec3 right, float tolerance)
        => MathF.Abs(left.x - right.x) <= tolerance
            && MathF.Abs(left.y - right.y) <= tolerance
            && MathF.Abs(left.z - right.z) <= tolerance;

    private static bool ArePredictableVec4Similar(CoreVec4 left, CoreVec4 right, float tolerance)
        => MathF.Abs(left.x - right.x) <= tolerance
            && MathF.Abs(left.y - right.y) <= tolerance
            && MathF.Abs(left.z - right.z) <= tolerance
            && MathF.Abs(left.w - right.w) <= tolerance;

    private static bool ArePredictableBoneWeightsEquivalent(CoreBoneWeight left, CoreBoneWeight right, float tolerance)
        => left.index0 == right.index0
            && left.index1 == right.index1
            && left.index2 == right.index2
            && left.index3 == right.index3
            && MathF.Abs(left.weight0 - right.weight0) <= tolerance
            && MathF.Abs(left.weight1 - right.weight1) <= tolerance
            && MathF.Abs(left.weight2 - right.weight2) <= tolerance
            && MathF.Abs(left.weight3 - right.weight3) <= tolerance;


    private static bool TryValidatePredictableDecodedMeshData(PredictableDecodedMeshData decoded, int[][] subMeshIndices, PredictableVertexFormat format, out string? reason)
    {
        reason = null;
        int vertexCount = decoded.Positions.Length;
        if (vertexCount <= 0)
        {
            reason = "No vertices after decimation.";
            return false;
        }

        for (int i = 0; i < vertexCount; i++)
        {
            var position = decoded.Positions[i];
            if (!double.IsFinite(position.x) || !double.IsFinite(position.y) || !double.IsFinite(position.z))
            {
                reason = $"Vertex {i}: invalid position.";
                return false;
            }
        }

        if (format.HasNormals)
        {
            if (decoded.Normals == null || decoded.Normals.Length != vertexCount)
            {
                reason = "Normals do not match vertex count.";
                return false;
            }

            for (int i = 0; i < decoded.Normals.Length; i++)
            {
                if (!IsFinite(decoded.Normals[i]))
                {
                    reason = $"Vertex {i}: invalid normal.";
                    return false;
                }
            }
        }

        if (format.HasTangent1)
        {
            if (decoded.Tangents == null || decoded.Tangents.Length != vertexCount)
            {
                reason = "Tangents do not match vertex count.";
                return false;
            }

            for (int i = 0; i < decoded.Tangents.Length; i++)
            {
                if (!IsFinite(decoded.Tangents[i]))
                {
                    reason = $"Vertex {i}: invalid tangent.";
                    return false;
                }
            }
        }

        if (format.HasTangent2)
        {
            if (decoded.Tangents2 == null || decoded.Tangents2.Length != vertexCount)
            {
                reason = "Stored bitangents do not match vertex count.";
                return false;
            }

            for (int i = 0; i < decoded.Tangents2.Length; i++)
            {
                if (!IsFinite(decoded.Tangents2[i]))
                {
                    reason = $"Vertex {i}: invalid stored bitangent.";
                    return false;
                }
            }
        }

        if (format.ColorChannelCount > 0)
        {
            if (decoded.ColorChannels == null || decoded.ColorChannels.Length < format.ColorChannelCount)
            {
                reason = "Missing color channels after decimation.";
                return false;
            }

            for (int channel = 0; channel < format.ColorChannelCount; channel++)
            {
                var colors = decoded.ColorChannels[channel];
                if (colors == null || colors.Length != vertexCount)
                {
                    reason = $"Color channel {channel} does not match vertex count.";
                    return false;
                }

                for (int i = 0; i < colors.Length; i++)
                {
                    if (!IsFinite(colors[i]))
                    {
                        reason = $"Vertex {i}: invalid color payload.";
                        return false;
                    }
                }
            }
        }

        if (format.HasSkinning)
        {
            if (decoded.BoneWeights == null || decoded.BoneWeights.Length != vertexCount)
            {
                reason = "Bone weights do not match vertex count.";
                return false;
            }

            for (int i = 0; i < decoded.BoneWeights.Length; i++)
            {
                var weight = decoded.BoneWeights[i];
                float sum = weight.weight0 + weight.weight1 + weight.weight2 + weight.weight3;
                if (!float.IsFinite(sum))
                {
                    reason = $"Vertex {i}: invalid bone weights.";
                    return false;
                }
            }
        }

        if (format.UvChannelCount > 0)
        {
            if (decoded.UvChannels == null || decoded.UvChannels.Length < format.UvChannelCount)
            {
                reason = "Missing UV channels after decimation.";
                return false;
            }

            for (int channel = 0; channel < format.UvChannelCount; channel++)
            {
                var uvs = decoded.UvChannels[channel];
                if (uvs == null || uvs.Length != vertexCount)
                {
                    reason = $"UV channel {channel} does not match vertex count.";
                    return false;
                }

                for (int i = 0; i < uvs.Length; i++)
                {
                    if (!IsFinite(uvs[i]))
                    {
                        reason = $"Vertex {i}: invalid UV payload.";
                        return false;
                    }
                }
            }
        }

        if (format.HasPositionW && (decoded.PositionWs == null || decoded.PositionWs.Length != vertexCount))
        {
            reason = "Missing position W after decimation.";
            return false;
        }

        if (format.HasNormalW && (decoded.NormalWs == null || decoded.NormalWs.Length != vertexCount))
        {
            reason = "Missing normal W after decimation.";
            return false;
        }

        for (int subMeshIndex = 0; subMeshIndex < subMeshIndices.Length; subMeshIndex++)
        {
            var indices = subMeshIndices[subMeshIndex];
            if (indices.Length % 3 != 0)
            {
                reason = $"Submesh {subMeshIndex}: index count is not a multiple of three.";
                return false;
            }

            for (int i = 0; i < indices.Length; i++)
            {
                if ((uint)indices[i] >= (uint)vertexCount)
                {
                    reason = $"Submesh {subMeshIndex}: index {indices[i]} out of range.";
                    return false;
                }
            }
        }

        return true;
    }

    private static bool ArePredictableUvsSimilar(CoreVec2[][] uvChannels, int[] uvChannelDimensions, int channelCount, int indexA, int indexB, float thresholdSq)
    {
        int channels = Math.Min(channelCount, Math.Min(uvChannels.Length, uvChannelDimensions.Length));
        for (int channel = 0; channel < channels; channel++)
        {
            var uvs = uvChannels[channel];
            if (uvs == null || (uint)indexA >= (uint)uvs.Length || (uint)indexB >= (uint)uvs.Length)
                continue;

            float dx = uvs[indexA].x - uvs[indexB].x;
            if (uvChannelDimensions[channel] <= 1)
            {
                if (dx * dx > thresholdSq)
                    return false;

                continue;
            }

            float dy = uvs[indexA].y - uvs[indexB].y;
            if ((dx * dx) + (dy * dy) > thresholdSq)
                return false;
        }

        return true;
    }

    private static bool ArePredictableNormalsSimilar(CoreVec3[] normals, int indexA, int indexB, float thresholdCos)
    {
        if ((uint)indexA >= (uint)normals.Length || (uint)indexB >= (uint)normals.Length)
            return true;

        if (!TryNormalizePredictableNormal(normals[indexA], out var normalA)
            || !TryNormalizePredictableNormal(normals[indexB], out var normalB))
        {
            return true;
        }

        return Vector3.Dot(normalA, normalB) >= thresholdCos;
    }

    private static bool TryNormalizePredictableNormal(CoreVec3 value, out Vector3 normalized)
    {
        normalized = new Vector3(value.x, value.y, value.z);
        float lengthSq = normalized.LengthSquared();
        if (lengthSq <= 1e-8f || !float.IsFinite(lengthSq))
            return false;

        normalized /= MathF.Sqrt(lengthSq);
        return float.IsFinite(normalized.X) && float.IsFinite(normalized.Y) && float.IsFinite(normalized.Z);
    }

    private static bool ArePredictableBoneWeightsSimilar(CoreBoneWeight[] boneWeights, int indexA, int indexB, float threshold)
    {
        if ((uint)indexA >= (uint)boneWeights.Length || (uint)indexB >= (uint)boneWeights.Length)
            return true;

        if (threshold <= 0f)
            return true;

        var a = boneWeights[indexA];
        var b = boneWeights[indexB];
        float overlap = GetPredictableBoneWeightOverlap(a, b);
        float denom = MathF.Max(GetPredictableBoneWeightSum(a), GetPredictableBoneWeightSum(b));
        if (denom <= 1e-6f)
            return true;

        return (overlap / denom) >= threshold;
    }

    private static float GetPredictableBoneWeightSum(in CoreBoneWeight weight)
        => weight.weight0 + weight.weight1 + weight.weight2 + weight.weight3;

    private static float GetPredictableBoneWeightOverlap(in CoreBoneWeight a, in CoreBoneWeight b)
    {
        float overlap = 0f;
        for (int i = 0; i < 4; i++)
        {
            int index = a.GetIndex(i);
            if (index < 0)
                continue;

            float weightA = a.GetWeight(i);
            if (weightA <= 0f)
                continue;

            for (int j = 0; j < 4; j++)
            {
                if (b.GetIndex(j) != index)
                    continue;

                float weightB = b.GetWeight(j);
                if (weightB > 0f)
                    overlap += MathF.Min(weightA, weightB);
                break;
            }
        }

        return overlap;
    }

    private static bool IsFinite(CoreVec2 value)
        => float.IsFinite(value.x) && float.IsFinite(value.y);

    private static bool IsFinite(CoreVec3 value)
        => float.IsFinite(value.x) && float.IsFinite(value.y) && float.IsFinite(value.z);

    private static bool IsFinite(CoreVec4 value)
        => float.IsFinite(value.x) && float.IsFinite(value.y) && float.IsFinite(value.z) && float.IsFinite(value.w);

    private readonly record struct PredictablePositionKey(long X, long Y, long Z);
}
