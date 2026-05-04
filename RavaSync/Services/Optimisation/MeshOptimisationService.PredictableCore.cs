using System.Buffers.Binary;
using System.IO;
using System.Linq;
using ReductionCore = RavaSync.Services.Optimisation.Reduction;
using NumVec2 = System.Numerics.Vector2;
using NumVec3 = System.Numerics.Vector3;
using NumVec4 = System.Numerics.Vector4;
using CoreVec2 = RavaSync.Services.Optimisation.Reduction.Vector2F;
using CoreVec3 = RavaSync.Services.Optimisation.Reduction.Vector3F;
using CoreVec3d = RavaSync.Services.Optimisation.Reduction.Vector3;
using CoreVec4 = RavaSync.Services.Optimisation.Reduction.Vector4F;
using CoreBoneWeight = RavaSync.Services.Optimisation.Reduction.BoneWeight;
using static Lumina.Data.Parsing.MdlStructs;
using Microsoft.Extensions.Logging;

namespace RavaSync.Services.Optimisation;

public sealed partial class MeshOptimisationService
{
    private const float PredictableTargetRatio = 0.8f;
    private const int PredictableTriangleThreshold = 0;
    private const bool PredictableAvoidBodyIntersection = true;
    private const int PredictableMinComponentTriangles = 6;
    private const int PredictableMaxComponentsPerSubMesh = 100;
    private const float PredictableMaxCollapseEdgeLengthFactor = 1.25f;
    private const float PredictableNormalSimilarityThresholdDegrees = 60f;
    private const float PredictableBoneWeightSimilarityThreshold = 0.85f;
    private const float PredictableUvSimilarityThreshold = 0.02f;
    private const float PredictableUvSeamAngleCos = 0.99f;
    private const float PredictableBodyCollisionDistanceFactor = 0.75f;
    private const float PredictableBodyCollisionNoOpDistanceFactor = 0.25f;
    private const float PredictableBodyCollisionAdaptiveRelaxFactor = 1.0f;
    private const float PredictableBodyCollisionAdaptiveNearRatio = 0.4f;
    private const float PredictableBodyCollisionAdaptiveUvThreshold = 0.08f;
    private const float PredictableBodyCollisionNoOpUvSeamAngleCos = 0.98f;
    private const float PredictableBodyCollisionProtectionFactor = 1.5f;
    private const float PredictableBodyCollisionProxyInflate = 0.0005f;
    private const float PredictableBodyCollisionPenetrationFactor = 0.75f;
    private const float PredictableBodyProxyTargetRatioMin = 0.85f;
    private const int PredictableBodyGuideProxyTriangleThreshold = 8_000;
    private const float PredictableMinBodyCollisionDistance = 0.0001f;
    private const float PredictableRoundTripPositionTolerance = 0.01f;
    private const float PredictableRoundTripWeightTolerance = 0.0065f;
    private const byte Single1Type = 0;
    private const byte Single2Type = 1;
    private const byte Single3Type = 2;
    private const byte Single4Type = 3;
    private const byte UByte4Type = 5;
    private const byte Short2Type = 6;
    private const byte Short4Type = 7;
    private const byte NByte4Type = 8;
    private const byte NShort2Type = 9;
    private const byte NShort4Type = 10;
    private const byte Half2Type = 13;
    private const byte Half4Type = 14;
    private const byte UShort2Type = 16;
    private const byte UShort4Type = 17;

    private bool TryPlanMeshRewriteWithPredictableCore(ReadOnlySpan<byte> bytes, RavaSync.Interop.GameModel.MdlFile mdl, int meshIndex, long absoluteStart, long absoluteVertexStart, ModelGuardContext guardContext, out MeshRewritePlan plan)
    {
        plan = default;

        if (mdl.LodCount != 1)
            return false;

        // Shape data is a hard stop here. We do not touch it unless we can round-trip it cleanly.
        if (mdl.ShapeCount > 0 || mdl.ShapeMeshCount > 0 || mdl.ShapeValueCount > 0)
            return false;

        if ((uint)meshIndex >= (uint)mdl.Meshes.Length)
            return false;

        var mesh = mdl.Meshes[meshIndex];
        if (ResolveMeshCleanupMode(mdl, meshIndex) == MeshCleanupMode.Disabled)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: cleanup mode disabled.", meshIndex);
            return false;
        }

        if (mesh.VertexCount <= 0 || mesh.IndexCount < 6 || mesh.SubMeshCount <= 0)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: invalid mesh counts (vertices={VertexCount}, indices={IndexCount}, submeshes={SubMeshCount}).", meshIndex, mesh.VertexCount, mesh.IndexCount, mesh.SubMeshCount);
            return false;
        }

        if (!TryGetVertexDeclarationForMesh(mdl, mesh, meshIndex, out var declaration))
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: vertex declaration unavailable.", meshIndex);
            return false;
        }

        if (!TryBuildPredictableVertexFormat(declaration, out var format, out var formatReason))
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: unsupported vertex format. Reason={Reason}", meshIndex, string.IsNullOrWhiteSpace(formatReason) ? "Unknown format failure" : formatReason);
            return false;
        }

        int submeshStart = mesh.SubMeshIndex;
        int submeshEnd = submeshStart + mesh.SubMeshCount;
        if (submeshStart < 0 || submeshEnd > mdl.SubMeshes.Length)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: submesh range invalid.", meshIndex);
            return false;
        }

        var meshSubMeshes = mdl.SubMeshes.Skip(submeshStart).Take(mesh.SubMeshCount).ToArray();
        if (!TryDecodePredictableMeshData(bytes, mdl, mesh, meshIndex, absoluteVertexStart, absoluteStart, format, meshSubMeshes, out var decoded, out var subMeshIndices, out var decodeReason))
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: decode failed. Reason={Reason}", meshIndex, string.IsNullOrWhiteSpace(decodeReason) ? "Unknown decode failure" : decodeReason);
            return false;
        }

        int sourceTriangles = subMeshIndices.Sum(static sm => sm.Length / 3);
        if (sourceTriangles <= 0)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: no triangles after decode.", meshIndex);
            return false;
        }

        if (sourceTriangles < PredictableTriangleThreshold)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: below triangle threshold ({Triangles} < {Threshold}).", meshIndex, sourceTriangles, PredictableTriangleThreshold);
            return false;
        }

        // Gear can pass through, but body-backed meshes stay off limits.
        bool isBodyMaterialMesh = IsProtectedBodyMesh(mdl, mesh);
        if (isBodyMaterialMesh)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: protected body mesh. MaterialIndex={MaterialIndex}, ModelPath={ModelPath}", meshIndex, mesh.MaterialIndex, mdl.SourcePath);
            return false;
        }

        int targetTriangles = Math.Max(1, (int)MathF.Floor(sourceTriangles * PredictableTargetRatio));
        if (targetTriangles >= sourceTriangles)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: invalid target triangle count ({Target} >= {Source}).", meshIndex, targetTriangles, sourceTriangles);
            return false;
        }

        BodyCollisionGuard? bodyCollisionGuard = null;
        if (PredictableAvoidBodyIntersection && !isBodyMaterialMesh && guardContext.BodySurface is { } bodySurface)
            bodyCollisionGuard = new BodyCollisionGuard(bodySurface, 0f);

        if (!TryDecimateWithPredictableCore(decoded, subMeshIndices, format, targetTriangles, bodyCollisionGuard, out var decimated, out var decimatedSubMeshIndices, out var removedTriangles))
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: decimation failed or produced no usable result.", meshIndex);
            return false;
        }

        if (removedTriangles <= 0)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: decimation removed no triangles.", meshIndex);
            return false;
        }

        int decimatedTriangles = decimatedSubMeshIndices.Sum(static sm => sm.Length / 3);
        if (decimatedTriangles <= 0 || decimatedTriangles >= sourceTriangles)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: decimated triangle count invalid ({Decimated} vs {Source}).", meshIndex, decimatedTriangles, sourceTriangles);
            return false;
        }

        if (!TryValidatePredictableShapePreservation(decoded, subMeshIndices, decimated, decimatedSubMeshIndices, out var shapeReason))
        {
            _logger.LogDebug("Predictable rewrite rejected for mesh {MeshIndex}: shape preservation validation failed. Reason={Reason}", meshIndex, string.IsNullOrWhiteSpace(shapeReason) ? "Unknown shape validation failure" : shapeReason);
            return false;
        }

        if (!TryEncodePredictableMeshData(decimated, decimatedSubMeshIndices, format, mesh, meshSubMeshes, out var updatedMesh, out var updatedSubMeshes, out var streamBuffers, out var indices, out var encodeReason))
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: encode failed. Reason={Reason}", meshIndex, string.IsNullOrWhiteSpace(encodeReason) ? "Unknown encode failure" : encodeReason);
            return false;
        }

        if (indices.Length < 3 || indices.Length >= mesh.IndexCount)
        {
            _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: encoded index count invalid ({IndexCount} vs original {OriginalIndexCount}).", meshIndex, indices.Length, mesh.IndexCount);
            return false;
        }

        var encodedIndices = new ushort[indices.Length];
        for (int i = 0; i < indices.Length; i++)
        {
            if ((uint)indices[i] > ushort.MaxValue)
            {
                _logger.LogDebug("Predictable rewrite skipped for mesh {MeshIndex}: encoded index {Index} exceeded UInt16 range.", meshIndex, i);
                return false;
            }

            encodedIndices[i] = (ushort)indices[i];
        }

        // If the payload cannot survive a full decode/encode round-trip, we bin it.
        if (!TryValidatePredictableReplacementPayloadRoundTrip(format, mesh, updatedMesh, updatedSubMeshes, streamBuffers, encodedIndices, decimated, decimatedSubMeshIndices, out var roundTripReason))
        {
            _logger.LogDebug("Predictable rewrite rejected for mesh {MeshIndex}: round-trip validation failed. Reason={Reason}", meshIndex, string.IsNullOrWhiteSpace(roundTripReason) ? "Unknown round-trip validation failure" : roundTripReason);
            return false;
        }

        var updates = new List<SubmeshMetadataUpdate>(updatedSubMeshes.Length);
        for (int i = 0; i < updatedSubMeshes.Length; i++)
        {
            updates.Add(new SubmeshMetadataUpdate(submeshStart + i, mesh.StartIndex + updatedSubMeshes[i].IndexOffset, updatedSubMeshes[i].IndexCount));
        }

        plan = new MeshRewritePlan(encodedIndices, removedTriangles, updates, streamBuffers, updatedMesh.VertexCount);
        return true;
    }

    private bool TryDecimateWithPredictableCore(
        PredictableDecodedMeshData decoded,
        int[][] subMeshIndices,
        PredictableVertexFormat format,
        int targetTriangles,
        BodyCollisionGuard? bodyCollisionGuard,
        out PredictableDecodedMeshData decimated,
        out int[][] decimatedSubMeshIndices,
        out long removedTriangles)
    {
        decimated = default!;
        decimatedSubMeshIndices = [];
        removedTriangles = 0;

        int totalTriangles = subMeshIndices.Sum(static x => x.Length / 3);
        if (totalTriangles <= 0)
            return false;

        float targetRatio = Math.Clamp(targetTriangles / (float)totalTriangles, 0f, 1f);
        var outputSubMeshes = new List<int>[subMeshIndices.Length];
        for (int i = 0; i < outputSubMeshes.Length; i++)
            outputSubMeshes[i] = [];

        var positions = new List<CoreVec3d>();
        List<CoreVec3>? normals = format.HasNormals ? [] : null;
        List<CoreVec4>? tangents = format.HasTangent1 ? [] : null;
        List<CoreVec4>? tangents2 = format.HasTangent2 ? [] : null;
        List<CoreVec4>[]? colorChannels = null;
        if (format.ColorChannelCount > 0)
        {
            colorChannels = new List<CoreVec4>[format.ColorChannelCount];
            for (int i = 0; i < format.ColorChannelCount; i++)
                colorChannels[i] = [];
        }

        List<CoreBoneWeight>? boneWeights = format.HasSkinning ? [] : null;
        List<CoreVec2>[]? uvChannels = null;
        if (format.UvChannelCount > 0)
        {
            uvChannels = new List<CoreVec2>[format.UvChannelCount];
            for (int i = 0; i < format.UvChannelCount; i++)
                uvChannels[i] = [];
        }

        List<float>? positionWs = format.HasPositionW ? [] : null;
        List<float>? normalWs = format.HasNormalW ? [] : null;

        for (int subMeshIndex = 0; subMeshIndex < subMeshIndices.Length; subMeshIndex++)
        {
            var indices = subMeshIndices[subMeshIndex];
            if (indices.Length == 0)
                continue;

            var components = BuildPredictableComponentsForSubMesh(indices);
            bool useWholeSubMeshDecimation = components.Count > PredictableMaxComponentsPerSubMesh;
            if (useWholeSubMeshDecimation)
                components = [indices];

            foreach (var componentIndices in components)
            {
                int componentTriangles = componentIndices.Length / 3;
                if (componentTriangles <= 0)
                    continue;

                if (!TryBuildPredictableComponentDecoded(decoded, format, componentIndices, out var componentDecoded, out var componentLocalIndices, out _))
                    return false;

                var resultDecoded = componentDecoded;
                var resultIndices = componentLocalIndices;
                int componentTarget = ComputePredictableComponentTarget(componentTriangles, targetRatio, PredictableMinComponentTriangles);
                if (componentTarget < componentTriangles)
                {
                    bool[]? attributeProtectedVertices = useWholeSubMeshDecimation
                        ? BuildPredictableAttributeProtectedVertexMask(componentDecoded, format)
                        : null;
                    if (TryDecimatePredictableComponent(componentDecoded, format, componentLocalIndices, componentTarget, bodyCollisionGuard, attributeProtectedVertices, out var componentDecimated, out var componentDecimatedIndices))
                    {
                        int decimatedComponentTriangles = componentDecimatedIndices.Length / 3;
                        if (decimatedComponentTriangles > 0
                            && decimatedComponentTriangles < componentTriangles
                            && componentDecimatedIndices.Length >= 3)
                        {
                            resultDecoded = componentDecimated;
                            resultIndices = componentDecimatedIndices;
                            removedTriangles += componentTriangles - decimatedComponentTriangles;
                        }
                    }
                }

                if (!AppendPredictableComponentData(resultDecoded, resultIndices, format, positions, normals, tangents, tangents2, colorChannels, boneWeights, uvChannels, positionWs, normalWs, outputSubMeshes[subMeshIndex], out _))
                    return false;
            }
        }

        if (removedTriangles <= 0)
            return false;

        int finalTriangles = outputSubMeshes.Sum(static x => x.Count / 3);
        if (finalTriangles <= 0 || finalTriangles >= totalTriangles)
            return false;

        decimated = new PredictableDecodedMeshData(
            positions.ToArray(),
            normals?.ToArray(),
            tangents?.ToArray(),
            tangents2?.ToArray(),
            colorChannels?.Select(static x => x.ToArray()).ToArray(),
            boneWeights?.ToArray(),
            uvChannels?.Select(static x => x.ToArray()).ToArray(),
            positionWs?.ToArray(),
            normalWs?.ToArray(),
            decoded.BlendWeightEncoding);
        decimatedSubMeshIndices = outputSubMeshes.Select(static x => x.ToArray()).ToArray();
        return true;
    }

    private bool TryDecimatePredictableComponent(
        PredictableDecodedMeshData componentDecoded,
        PredictableVertexFormat format,
        int[] componentIndices,
        int targetTriangles,
        BodyCollisionGuard? bodyCollisionGuard,
        bool[]? attributeProtectedVertices,
        out PredictableDecodedMeshData decimated,
        out int[] decimatedIndices)
    {
        decimated = default!;
        decimatedIndices = [];

        int componentTriangles = componentIndices.Length / 3;
        if (componentTriangles <= 0)
            return false;

        float averageEdgeLength = ComputePredictableAverageEdgeLength(componentDecoded.Positions, componentIndices);

        bool RunDecimation(
            float bodyCollisionDistanceFactor,
            bool applyBodyProtection,
            bool expandBodyProtection,
            bool allowBodyProtectionWhenRelaxed,
            bool forceRelaxTopology,
            bool blockUvSeamVertices,
            float? uvSeamAngleCosOverride,
            out PredictableDecodedMeshData runDecimated,
            out int[] runDecimatedIndices,
            out ReductionCore.DecimationStats runStats)
        {
            runDecimated = default!;
            runDecimatedIndices = [];
            runStats = default;

            if (!TryBuildPredictableOptimisedMeshBuffer(componentDecoded, [componentIndices], format, out var sharedMesh, out _))
                return false;

            bool relaxTopology = forceRelaxTopology;
            var decimator = new ReductionCore.DecimateModifier();
            bool[]? mergedProtectedVertices = attributeProtectedVertices;

            if (bodyCollisionGuard is { } guard)
            {
                float threshold = MathF.Max((averageEdgeLength * bodyCollisionDistanceFactor) + PredictableBodyCollisionProxyInflate, PredictableMinBodyCollisionDistance);
                float thresholdSq = threshold * threshold;
                float protectionThreshold = MathF.Max(threshold * PredictableBodyCollisionProtectionFactor, threshold);
                float protectionThresholdSq = protectionThreshold * protectionThreshold;

                float[] bodyDistanceSq = new float[componentDecoded.Positions.Length];
                bool[]? bodyProtectedVertices = applyBodyProtection ? new bool[componentDecoded.Positions.Length] : null;
                for (int i = 0; i < componentDecoded.Positions.Length; i++)
                {
                    var point = ToNumerics(componentDecoded.Positions[i]);
                    bodyDistanceSq[i] = ComputeBodySurfaceDistanceSq(guard.Surface, point, thresholdSq);
                    if (bodyProtectedVertices != null)
                        bodyProtectedVertices[i] = ComputeBodySurfaceDistanceSq(guard.Surface, point, protectionThresholdSq) <= protectionThresholdSq;
                }

                if (!forceRelaxTopology
                    && IsPredictableNearBodyDominant(bodyDistanceSq, thresholdSq, componentDecoded.Positions.Length, PredictableBodyCollisionAdaptiveNearRatio))
                {
                    threshold = MathF.Max(threshold * PredictableBodyCollisionAdaptiveRelaxFactor, PredictableMinBodyCollisionDistance);
                    thresholdSq = threshold * threshold;
                    relaxTopology = true;
                    protectionThreshold = MathF.Max(threshold * PredictableBodyCollisionProtectionFactor, threshold);
                    protectionThresholdSq = protectionThreshold * protectionThreshold;

                    for (int i = 0; i < componentDecoded.Positions.Length; i++)
                    {
                        var point = ToNumerics(componentDecoded.Positions[i]);
                        bodyDistanceSq[i] = ComputeBodySurfaceDistanceSq(guard.Surface, point, thresholdSq);
                        if (bodyProtectedVertices != null)
                            bodyProtectedVertices[i] = ComputeBodySurfaceDistanceSq(guard.Surface, point, protectionThresholdSq) <= protectionThresholdSq;
                    }
                }

                if (bodyProtectedVertices != null && expandBodyProtection && componentIndices.Length >= 3)
                {
                    var expanded = (bool[])bodyProtectedVertices.Clone();
                    for (int i = 0; i + 2 < componentIndices.Length; i += 3)
                    {
                        int a = componentIndices[i];
                        int b = componentIndices[i + 1];
                        int c = componentIndices[i + 2];
                        if ((uint)a >= bodyProtectedVertices.Length || (uint)b >= bodyProtectedVertices.Length || (uint)c >= bodyProtectedVertices.Length)
                            continue;

                        if (bodyProtectedVertices[a] || bodyProtectedVertices[b] || bodyProtectedVertices[c])
                        {
                            expanded[a] = true;
                            expanded[b] = true;
                            expanded[c] = true;
                        }
                    }

                    bodyProtectedVertices = expanded;
                }

                decimator.SetBodyCollision(bodyDistanceSq, thresholdSq, point => ComputeBodySurfaceDistanceSq(guard.Surface, ToNumerics(point), thresholdSq));

                if (!relaxTopology || allowBodyProtectionWhenRelaxed)
                    mergedProtectedVertices = MergePredictableProtectedVertexMasks(bodyProtectedVertices, attributeProtectedVertices, componentDecoded.Positions.Length);
            }

            if (relaxTopology)
                sharedMesh.attributeDefinitions = [new ReductionCore.AttributeDefinition(ReductionCore.AttributeType.Normals, 0d, 0)];

            var settings = ReductionCore.DecimateModifier.CreateDefaultSettings() with
            {
                LimitCollapseEdgeLength = averageEdgeLength > 0f,
                MaxCollapseEdgeLength = averageEdgeLength > 0f ? MathF.Max(averageEdgeLength * PredictableMaxCollapseEdgeLengthFactor, 0f) : float.PositiveInfinity,
                CollapseToEndpointsOnly = true,
                NormalSimilarityThresholdDegrees = PredictableNormalSimilarityThresholdDegrees,
                BoneWeightSimilarityThreshold = PredictableBoneWeightSimilarityThreshold,
                UvSimilarityThreshold = relaxTopology ? PredictableBodyCollisionAdaptiveUvThreshold : PredictableUvSimilarityThreshold,
                AllowBoundaryCollapses = false,
                BlockUvSeamVertices = blockUvSeamVertices,
                UvSeamAngleCos = uvSeamAngleCosOverride ?? PredictableUvSeamAngleCos,
                BodyCollisionPenetrationFactor = PredictableBodyCollisionPenetrationFactor,
            };
            decimator.SetSettings(settings);

            if (mergedProtectedVertices != null)
                decimator.SetProtectedVertices(mergedProtectedVertices);

            var connectedMesh = sharedMesh.ToConnectedMesh();
            decimator.Initialize(connectedMesh);
            decimator.DecimateToPolycount(targetTriangles);
            runStats = decimator.GetStats();

            var decimatedShared = connectedMesh.ToOptimisedMeshBuffer();
            if (!TryConvertPredictableOptimisedMeshBuffer(decimatedShared, format, 1, componentDecoded.BlendWeightEncoding, out runDecimated, out var subMeshes, out _))
                return false;

            if (subMeshes.Length == 0)
                return false;

            if (!TryValidatePredictableDecodedMeshData(runDecimated, subMeshes, format, out _))
                return false;

            int runTriangleCount = subMeshes[0].Length / 3;
            if (runTriangleCount <= 0 || runTriangleCount >= componentTriangles)
                return false;

            runDecimatedIndices = subMeshes[0];
            return true;
        }

        if (!RunDecimation(
                PredictableBodyCollisionDistanceFactor,
                applyBodyProtection: true,
                expandBodyProtection: true,
                allowBodyProtectionWhenRelaxed: true,
                forceRelaxTopology: false,
                blockUvSeamVertices: true,
                uvSeamAngleCosOverride: null,
                out decimated,
                out decimatedIndices,
                out var stats))
        {
            return false;
        }

        if (stats.CollapsedEdges == 0 && targetTriangles < componentTriangles && bodyCollisionGuard != null)
        {
            if (RunDecimation(
                    PredictableBodyCollisionNoOpDistanceFactor,
                    applyBodyProtection: true,
                    expandBodyProtection: false,
                    allowBodyProtectionWhenRelaxed: true,
                    forceRelaxTopology: true,
                    blockUvSeamVertices: false,
                    uvSeamAngleCosOverride: PredictableBodyCollisionNoOpUvSeamAngleCos,
                    out var fallbackDecimated,
                    out var fallbackIndices,
                    out var fallbackStats))
            {
                int fallbackTriangles = fallbackIndices.Length / 3;
                if (fallbackStats.CollapsedEdges > 0 && fallbackTriangles > 0 && fallbackTriangles < componentTriangles)
                {
                    decimated = fallbackDecimated;
                    decimatedIndices = fallbackIndices;
                    stats = fallbackStats;
                }
            }
        }

        return true;
    }

    private static int ComputePredictableComponentTarget(int componentTriangles, float targetRatio, int minComponentTriangles)
    {
        if (componentTriangles <= 0)
            return 0;

        int minimumTriangles = Math.Max(1, minComponentTriangles);
        if (componentTriangles <= minimumTriangles)
            return componentTriangles;

        int target = (int)MathF.Round(componentTriangles * targetRatio);
        target = Math.Max(1, target);
        return Math.Min(componentTriangles, Math.Max(minimumTriangles, target));
    }

    private static bool IsPredictableNearBodyDominant(float[] distanceSq, float thresholdSq, int vertexCount, float adaptiveNearRatio)
    {
        if (vertexCount <= 0 || distanceSq.Length == 0 || thresholdSq <= 0f)
            return false;

        int limit = Math.Min(vertexCount, distanceSq.Length);
        int nearCount = 0;
        for (int i = 0; i < limit; i++)
        {
            if (distanceSq[i] <= thresholdSq)
                nearCount++;
        }

        return nearCount >= limit * adaptiveNearRatio;
    }

    private static List<int[]> BuildPredictableComponentsForSubMesh(int[] indices)
    {
        if (indices.Length == 0)
            return [];

        int triangleCount = indices.Length / 3;
        if (triangleCount <= 1)
            return [indices];

        int[] parent = new int[triangleCount];
        byte[] rank = new byte[triangleCount];
        for (int i = 0; i < triangleCount; i++)
            parent[i] = i;

        var vertexToTriangles = new Dictionary<int, List<int>>();
        for (int tri = 0; tri < triangleCount; tri++)
        {
            int baseIndex = tri * 3;
            for (int corner = 0; corner < 3; corner++)
            {
                int vertex = indices[baseIndex + corner];
                if (!vertexToTriangles.TryGetValue(vertex, out var list))
                {
                    list = [];
                    vertexToTriangles.Add(vertex, list);
                }
                list.Add(tri);
            }
        }

        foreach (var pair in vertexToTriangles)
        {
            var list = pair.Value;
            for (int i = 1; i < list.Count; i++)
                UnionPredictable(parent, rank, list[0], list[i]);
        }

        var grouped = new Dictionary<int, List<int>>();
        for (int tri = 0; tri < triangleCount; tri++)
        {
            int root = FindPredictable(parent, tri);
            if (!grouped.TryGetValue(root, out var list))
            {
                list = [];
                grouped.Add(root, list);
            }
            list.Add(tri);
        }

        var components = new List<int[]>(grouped.Count);
        foreach (var group in grouped.Values)
        {
            int[] slice = new int[group.Count * 3];
            for (int i = 0; i < group.Count; i++)
            {
                int tri = group[i] * 3;
                slice[(i * 3) + 0] = indices[tri + 0];
                slice[(i * 3) + 1] = indices[tri + 1];
                slice[(i * 3) + 2] = indices[tri + 2];
            }
            components.Add(slice);
        }

        return components;
    }

    private static int FindPredictable(int[] parent, int value)
    {
        int root = value;
        while (parent[root] != root)
            root = parent[root];

        while (parent[value] != value)
        {
            int next = parent[value];
            parent[value] = root;
            value = next;
        }

        return root;
    }

    private static void UnionPredictable(int[] parent, byte[] rank, int a, int b)
    {
        int rootA = FindPredictable(parent, a);
        int rootB = FindPredictable(parent, b);
        if (rootA == rootB)
            return;

        if (rank[rootA] < rank[rootB])
        {
            parent[rootA] = rootB;
            return;
        }

        parent[rootB] = rootA;
        if (rank[rootA] == rank[rootB])
            rank[rootA]++;
    }

    private static bool TryBuildPredictableComponentDecoded(PredictableDecodedMeshData decoded, PredictableVertexFormat format, int[] componentIndices, out PredictableDecodedMeshData componentDecoded, out int[] componentLocalIndices, out string? reason)
    {
        reason = null;
        componentDecoded = default!;
        componentLocalIndices = [];
        if (componentIndices.Length == 0)
        {
            reason = "Component has no indices.";
            return false;
        }

        var vertexMap = new Dictionary<int, int>();
        var positions = new List<CoreVec3d>();
        List<CoreVec3>? normals = format.HasNormals ? [] : null;
        List<CoreVec4>? tangents = format.HasTangent1 ? [] : null;
        List<CoreVec4>? tangents2 = format.HasTangent2 ? [] : null;
        List<CoreVec4>[]? colorChannels = null;
        if (format.ColorChannelCount > 0)
        {
            colorChannels = new List<CoreVec4>[format.ColorChannelCount];
            for (int i = 0; i < format.ColorChannelCount; i++)
                colorChannels[i] = [];
        }
        List<CoreBoneWeight>? boneWeights = format.HasSkinning ? [] : null;
        List<float>? positionWs = format.HasPositionW ? [] : null;
        List<float>? normalWs = format.HasNormalW ? [] : null;
        List<CoreVec2>[]? uvChannels = null;
        if (format.UvChannelCount > 0)
        {
            uvChannels = new List<CoreVec2>[format.UvChannelCount];
            for (int i = 0; i < format.UvChannelCount; i++)
                uvChannels[i] = [];
        }

        componentLocalIndices = new int[componentIndices.Length];
        for (int i = 0; i < componentIndices.Length; i++)
        {
            int globalIndex = componentIndices[i];
            if (globalIndex < 0 || globalIndex >= decoded.Positions.Length)
            {
                reason = "Component vertex index out of bounds.";
                return false;
            }

            if (!vertexMap.TryGetValue(globalIndex, out int localIndex))
            {
                localIndex = positions.Count;
                vertexMap.Add(globalIndex, localIndex);
                positions.Add(decoded.Positions[globalIndex]);
                if (normals != null) normals.Add(decoded.Normals?[globalIndex] ?? default);
                if (tangents != null) tangents.Add(decoded.Tangents?[globalIndex] ?? default);
                if (tangents2 != null) tangents2.Add(decoded.Tangents2?[globalIndex] ?? default);
                if (colorChannels != null)
                {
                    for (int c = 0; c < colorChannels.Length; c++)
                        colorChannels[c].Add(decoded.ColorChannels != null && c < decoded.ColorChannels.Length ? decoded.ColorChannels[c][globalIndex] : default);
                }
                if (boneWeights != null) boneWeights.Add(decoded.BoneWeights?[globalIndex] ?? default);
                if (positionWs != null) positionWs.Add(decoded.PositionWs?[globalIndex] ?? 0f);
                if (normalWs != null) normalWs.Add(decoded.NormalWs?[globalIndex] ?? 0f);
                if (uvChannels != null)
                {
                    for (int c = 0; c < uvChannels.Length; c++)
                        uvChannels[c].Add(decoded.UvChannels != null && c < decoded.UvChannels.Length ? decoded.UvChannels[c][globalIndex] : default);
                }
            }

            componentLocalIndices[i] = localIndex;
        }

        componentDecoded = new PredictableDecodedMeshData(
            positions.ToArray(),
            normals?.ToArray(),
            tangents?.ToArray(),
            tangents2?.ToArray(),
            colorChannels?.Select(static x => x.ToArray()).ToArray(),
            boneWeights?.ToArray(),
            uvChannels?.Select(static x => x.ToArray()).ToArray(),
            positionWs?.ToArray(),
            normalWs?.ToArray(),
            decoded.BlendWeightEncoding);
        return true;
    }

    private static bool AppendPredictableComponentData(
        PredictableDecodedMeshData component,
        int[] componentIndices,
        PredictableVertexFormat format,
        List<CoreVec3d> positions,
        List<CoreVec3>? normals,
        List<CoreVec4>? tangents,
        List<CoreVec4>? tangents2,
        List<CoreVec4>[]? colorChannels,
        List<CoreBoneWeight>? boneWeights,
        List<CoreVec2>[]? uvChannels,
        List<float>? positionWs,
        List<float>? normalWs,
        List<int> outputIndices,
        out string? reason)
    {
        reason = null;
        if (component.Positions.Length == 0 || componentIndices.Length == 0)
            return true;

        int baseIndex = positions.Count;
        positions.AddRange(component.Positions);
        if (normals != null && component.Normals != null) normals.AddRange(component.Normals);
        if (tangents != null && component.Tangents != null) tangents.AddRange(component.Tangents);
        if (tangents2 != null && component.Tangents2 != null) tangents2.AddRange(component.Tangents2);
        if (colorChannels != null && component.ColorChannels != null)
        {
            if (colorChannels.Length != component.ColorChannels.Length)
            {
                reason = "Color channel mismatch while merging components.";
                return false;
            }
            for (int i = 0; i < colorChannels.Length; i++)
                colorChannels[i].AddRange(component.ColorChannels[i]);
        }
        if (boneWeights != null && component.BoneWeights != null) boneWeights.AddRange(component.BoneWeights);
        if (positionWs != null && component.PositionWs != null) positionWs.AddRange(component.PositionWs);
        if (normalWs != null && component.NormalWs != null) normalWs.AddRange(component.NormalWs);
        if (uvChannels != null && component.UvChannels != null)
        {
            if (uvChannels.Length != component.UvChannels.Length)
            {
                reason = "UV channel mismatch while merging components.";
                return false;
            }
            for (int i = 0; i < uvChannels.Length; i++)
                uvChannels[i].AddRange(component.UvChannels[i]);
        }
        for (int i = 0; i < componentIndices.Length; i++)
            outputIndices.Add(componentIndices[i] + baseIndex);
        return true;
    }

    private static float ComputePredictableAverageEdgeLength(CoreVec3d[] positions, int[] indices)
    {
        if (positions.Length == 0 || indices.Length < 3)
            return 0f;

        double sum = 0d;
        int count = 0;
        for (int i = 0; i + 2 < indices.Length; i += 3)
        {
            int i0 = indices[i];
            int i1 = indices[i + 1];
            int i2 = indices[i + 2];
            if ((uint)i0 >= positions.Length || (uint)i1 >= positions.Length || (uint)i2 >= positions.Length)
                continue;

            sum += CoreVec3d.Distance(positions[i0], positions[i1]);
            sum += CoreVec3d.Distance(positions[i1], positions[i2]);
            sum += CoreVec3d.Distance(positions[i2], positions[i0]);
            count += 3;
        }

        return count > 0 ? (float)(sum / count) : 0f;
    }

    private static bool TryBuildPredictableOptimisedMeshBuffer(PredictableDecodedMeshData decoded, int[][] subMeshIndices, PredictableVertexFormat format, out ReductionCore.OptimisedMeshBuffer sharedMesh, out string? reason)
    {
        sharedMesh = default!;
        reason = null;
        int vertexCount = decoded.Positions.Length;
        if (vertexCount == 0)
        {
            reason = "No vertices to decimate.";
            return false;
        }

        int totalIndexCount = subMeshIndices.Sum(static x => x.Length);
        int[] triangles = new int[totalIndexCount];
        ReductionCore.MeshGroup[] groups = new ReductionCore.MeshGroup[subMeshIndices.Length];
        int cursor = 0;
        for (int i = 0; i < subMeshIndices.Length; i++)
        {
            var subMesh = subMeshIndices[i];
            if (subMesh.Length > 0)
                Array.Copy(subMesh, 0, triangles, cursor, subMesh.Length);
            groups[i] = new ReductionCore.MeshGroup { firstIndex = cursor, indexCount = subMesh.Length };
            cursor += subMesh.Length;
        }

        var flags = BuildPredictableFfxivAttributeFlags(format);
        var attributes = new ReductionCore.MetaAttributeList<ReductionCore.FfxivVertexAttribute>(vertexCount);
        for (int i = 0; i < vertexCount; i++)
        {
            var attr = new ReductionCore.FfxivVertexAttribute(
                flags,
                format.HasNormals && decoded.Normals != null ? decoded.Normals[i] : default,
                format.HasTangent1 && decoded.Tangents != null ? decoded.Tangents[i] : default,
                format.HasTangent2 && decoded.Tangents2 != null ? decoded.Tangents2[i] : default,
                format.UvChannelCount > 0 && decoded.UvChannels != null ? decoded.UvChannels[0][i] : default,
                format.UvChannelCount > 1 && decoded.UvChannels != null ? decoded.UvChannels[1][i] : default,
                format.UvChannelCount > 2 && decoded.UvChannels != null ? decoded.UvChannels[2][i] : default,
                format.UvChannelCount > 3 && decoded.UvChannels != null ? decoded.UvChannels[3][i] : default,
                format.ColorChannelCount > 0 && decoded.ColorChannels != null ? decoded.ColorChannels[0][i] : default,
                format.ColorChannelCount > 1 && decoded.ColorChannels != null ? decoded.ColorChannels[1][i] : default,
                format.HasSkinning && decoded.BoneWeights != null ? decoded.BoneWeights[i] : default,
                format.HasPositionW && decoded.PositionWs != null ? decoded.PositionWs[i] : 0f,
                format.HasNormalW && decoded.NormalWs != null ? decoded.NormalWs[i] : 0f);
            attributes[i] = new ReductionCore.MetaAttribute<ReductionCore.FfxivVertexAttribute>(attr);
        }

        sharedMesh = new ReductionCore.OptimisedMeshBuffer
        {
            positions = decoded.Positions,
            triangles = triangles,
            groups = groups,
            attributes = attributes,
            attributeDefinitions = [new ReductionCore.AttributeDefinition(ReductionCore.AttributeType.Normals, ReductionCore.ConnectedMesh.EdgeBorderPenalty, 0)],
        };
        return true;
    }

    private static bool TryConvertPredictableOptimisedMeshBuffer(ReductionCore.OptimisedMeshBuffer decimatedShared, PredictableVertexFormat format, int expectedSubMeshCount, PredictableBlendWeightEncoding blendWeightEncoding, out PredictableDecodedMeshData decimated, out int[][] decimatedSubMeshIndices, out string? reason)
    {
        decimated = default!;
        decimatedSubMeshIndices = [];
        reason = null;
        if (decimatedShared.triangles == null || decimatedShared.triangles.Length == 0)
        {
            reason = "No triangles after decimation.";
            return false;
        }

        int[][] subMeshIndices;
        if (decimatedShared.groups != null && decimatedShared.groups.Length == expectedSubMeshCount)
        {
            subMeshIndices = new int[decimatedShared.groups.Length][];
            for (int i = 0; i < decimatedShared.groups.Length; i++)
            {
                var group = decimatedShared.groups[i];
                if (group.firstIndex < 0 || group.indexCount < 0 || group.firstIndex + group.indexCount > decimatedShared.triangles.Length)
                {
                    reason = "Invalid submesh group range after decimation.";
                    return false;
                }
                var slice = new int[group.indexCount];
                if (group.indexCount > 0)
                    Array.Copy(decimatedShared.triangles, group.firstIndex, slice, 0, group.indexCount);
                subMeshIndices[i] = slice;
            }
        }
        else if (expectedSubMeshCount == 1)
        {
            subMeshIndices = [decimatedShared.triangles];
        }
        else
        {
            reason = "Submesh group count mismatch after decimation.";
            return false;
        }

        var attrList = decimatedShared.attributes as ReductionCore.MetaAttributeList<ReductionCore.FfxivVertexAttribute>;
        if (attrList == null)
        {
            reason = "Missing vertex attributes after decimation.";
            return false;
        }

        int vertexCount = decimatedShared.positions.Length;
        CoreVec3[]? normals = format.HasNormals ? new CoreVec3[vertexCount] : null;
        CoreVec4[]? tangents = format.HasTangent1 ? new CoreVec4[vertexCount] : null;
        CoreVec4[]? tangents2 = format.HasTangent2 ? new CoreVec4[vertexCount] : null;
        CoreVec4[][]? colorChannels = null;
        if (format.ColorChannelCount > 0)
        {
            colorChannels = new CoreVec4[format.ColorChannelCount][];
            for (int i = 0; i < format.ColorChannelCount; i++)
                colorChannels[i] = new CoreVec4[vertexCount];
        }
        CoreBoneWeight[]? boneWeights = format.HasSkinning ? new CoreBoneWeight[vertexCount] : null;
        float[]? positionWs = format.HasPositionW ? new float[vertexCount] : null;
        float[]? normalWs = format.HasNormalW ? new float[vertexCount] : null;
        CoreVec2[][]? uvChannels = null;
        if (format.UvChannelCount > 0)
        {
            uvChannels = new CoreVec2[format.UvChannelCount][];
            for (int i = 0; i < format.UvChannelCount; i++)
                uvChannels[i] = new CoreVec2[vertexCount];
        }

        for (int i = 0; i < vertexCount; i++)
        {
            var attr = (ReductionCore.MetaAttribute<ReductionCore.FfxivVertexAttribute>)attrList[i];
            var data = attr.attr0;
            if (normals != null) normals[i] = data.normal;
            if (tangents != null) tangents[i] = data.tangent1;
            if (tangents2 != null) tangents2[i] = data.tangent2;
            if (colorChannels != null)
            {
                if (colorChannels.Length > 0) colorChannels[0][i] = data.color;
                if (colorChannels.Length > 1) colorChannels[1][i] = data.color1;
            }
            if (boneWeights != null) boneWeights[i] = data.boneWeight;
            if (positionWs != null) positionWs[i] = data.positionW;
            if (normalWs != null) normalWs[i] = data.normalW;
            if (uvChannels != null)
            {
                if (uvChannels.Length > 0) uvChannels[0][i] = data.uv0;
                if (uvChannels.Length > 1) uvChannels[1][i] = data.uv1;
                if (uvChannels.Length > 2) uvChannels[2][i] = data.uv2;
                if (uvChannels.Length > 3) uvChannels[3][i] = data.uv3;
            }
        }

        decimated = new PredictableDecodedMeshData(decimatedShared.positions, normals, tangents, tangents2, colorChannels, boneWeights, uvChannels, positionWs, normalWs, blendWeightEncoding);
        decimatedSubMeshIndices = subMeshIndices;
        return true;
    }

    private static ReductionCore.FfxivAttributeFlags BuildPredictableFfxivAttributeFlags(PredictableVertexFormat format)
    {
        var flags = ReductionCore.FfxivAttributeFlags.None;
        if (format.HasNormals) flags |= ReductionCore.FfxivAttributeFlags.Normal;
        if (format.HasTangent1) flags |= ReductionCore.FfxivAttributeFlags.Tangent1;
        if (format.HasTangent2) flags |= ReductionCore.FfxivAttributeFlags.Tangent2;
        if (format.ColorChannelCount > 0) flags |= ReductionCore.FfxivAttributeFlags.Color;
        if (format.ColorChannelCount > 1) flags |= ReductionCore.FfxivAttributeFlags.Color1;
        if (format.HasSkinning) flags |= ReductionCore.FfxivAttributeFlags.BoneWeights;
        if (format.HasPositionW) flags |= ReductionCore.FfxivAttributeFlags.PositionW;
        if (format.HasNormalW) flags |= ReductionCore.FfxivAttributeFlags.NormalW;
        if (format.UvChannelCount > 0) flags |= ReductionCore.FfxivAttributeFlags.Uv0;
        if (format.UvChannelCount > 1) flags |= ReductionCore.FfxivAttributeFlags.Uv1;
        if (format.UvChannelCount > 2) flags |= ReductionCore.FfxivAttributeFlags.Uv2;
        if (format.UvChannelCount > 3) flags |= ReductionCore.FfxivAttributeFlags.Uv3;
        return flags;
    }

    private bool TryDecodePredictableMeshData(ReadOnlySpan<byte> bytes, RavaSync.Interop.GameModel.MdlFile mdl, MeshStruct mesh, int meshIndex, long absoluteVertexStart, long absoluteIndexStart, PredictableVertexFormat format, SubmeshStruct[] meshSubMeshes, out PredictableDecodedMeshData decoded, out int[][] subMeshIndices, out string? reason)
    {
        _ = mdl;
        _ = meshIndex;
        return TryDecodePredictableMeshDataCore(bytes, mesh, absoluteVertexStart, absoluteIndexStart, format, meshSubMeshes, out decoded, out subMeshIndices, out reason);
    }

    private bool TryDecodePredictableMeshDataCore(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, long absoluteIndexStart, PredictableVertexFormat format, SubmeshStruct[] meshSubMeshes, out PredictableDecodedMeshData decoded, out int[][] subMeshIndices, out string? reason)
    {
        decoded = default!;
        subMeshIndices = [];
        reason = null;

        if (!TryBuildPredictableSubMeshIndices(bytes, mesh, absoluteIndexStart, meshSubMeshes, out subMeshIndices, out reason))
            return false;

        int vertexCount = mesh.VertexCount;
        var positions = new CoreVec3d[vertexCount];
        CoreVec3[]? normals = format.HasNormals ? new CoreVec3[vertexCount] : null;
        CoreVec4[]? tangents = format.HasTangent1 ? new CoreVec4[vertexCount] : null;
        CoreVec4[]? tangents2 = format.HasTangent2 ? new CoreVec4[vertexCount] : null;
        CoreVec4[][]? colorChannels = null;
        if (format.ColorChannelCount > 0)
        {
            colorChannels = new CoreVec4[format.ColorChannelCount][];
            for (int i = 0; i < format.ColorChannelCount; i++) colorChannels[i] = new CoreVec4[vertexCount];
        }
        CoreBoneWeight[]? boneWeights = format.HasSkinning ? new CoreBoneWeight[vertexCount] : null;
        float[]? positionWs = format.HasPositionW ? new float[vertexCount] : null;
        float[]? normalWs = format.HasNormalW ? new float[vertexCount] : null;
        CoreVec2[][]? uvChannels = null;
        if (format.UvChannelCount > 0)
        {
            uvChannels = new CoreVec2[format.UvChannelCount][];
            for (int i = 0; i < format.UvChannelCount; i++) uvChannels[i] = new CoreVec2[vertexCount];
        }

        var blendWeightEncoding = DetectPredictableBlendWeightEncoding(bytes, mesh, absoluteVertexStart, format);

        using var stream0 = new BinaryReader(new MemoryStream(bytes.ToArray(), false));
        using var stream1 = new BinaryReader(new MemoryStream(bytes.ToArray(), false));
        using var stream2 = new BinaryReader(new MemoryStream(bytes.ToArray(), false));
        var streams = new[] { stream0, stream1, stream2 };
        bool[] usedStreams = new bool[3];
        foreach (var element in format.SortedElements)
            usedStreams[element.Stream] = true;

        for (int streamIndex = 0; streamIndex < 3; streamIndex++)
        {
            if (!usedStreams[streamIndex])
                continue;

            int stride = mesh.VertexBufferStride[streamIndex];
            if (stride <= 0)
            {
                reason = $"Vertex stream {streamIndex} has no stride.";
                return false;
            }

            long streamStart = absoluteVertexStart + mesh.VertexBufferOffset[streamIndex];
            long streamLength = (long)vertexCount * stride;
            long streamEnd = streamStart + streamLength;
            if (streamStart < 0 || streamEnd > bytes.Length)
            {
                reason = $"Vertex stream {streamIndex} range is invalid.";
                return false;
            }

            streams[streamIndex].BaseStream.Position = streamStart;
        }

        var colorLookup = format.ColorElements.ToDictionary(static e => PredictableElementKey.From(e.Element), static e => e.Channel);
        var uvLookup = format.UvElements.ToDictionary(static e => PredictableElementKey.From(e.Element), static e => e);

        for (int vertexIndex = 0; vertexIndex < vertexCount; vertexIndex++)
        {
            int[]? indices = null;
            float[]? weights = null;
            foreach (var element in format.SortedElements)
            {
                var stream = streams[element.Stream];
                switch (element.Usage)
                {
                    case PositionUsage:
                        if (element.Type == Single4Type && positionWs != null)
                            positions[vertexIndex] = ReadPredictablePositionWithW(stream, out positionWs[vertexIndex]);
                        else
                            positions[vertexIndex] = ReadPredictablePosition(element.Type, stream);
                        break;
                    case NormalUsage when normals != null:
                        if (element.Type == Single4Type && normalWs != null)
                            normals[vertexIndex] = ReadPredictableNormalWithW(stream, out normalWs[vertexIndex]);
                        else
                            normals[vertexIndex] = ReadPredictableNormal(element.Type, stream);
                        break;
                    case 5 when tangents != null:
                        tangents[vertexIndex] = ReadPredictableTangent(element.Type, stream);
                        break;
                    case 6 when tangents2 != null:
                        tangents2[vertexIndex] = ReadPredictableTangent(element.Type, stream);
                        break;
                    case 7 when colorChannels != null:
                        if (!colorLookup.TryGetValue(PredictableElementKey.From(element), out var colorChannel))
                        {
                            reason = "Color mapping missing.";
                            return false;
                        }
                        colorChannels[colorChannel][vertexIndex] = ReadPredictableColor(element.Type, stream);
                        break;
                    case BlendIndicesUsage:
                        indices = ReadPredictableIndices(element.Type, stream);
                        break;
                    case BlendWeightUsage:
                        weights = ReadPredictableWeights(element.Type, stream, blendWeightEncoding);
                        break;
                    case TexCoordUsage when uvChannels != null:
                        if (!uvLookup.TryGetValue(PredictableElementKey.From(element), out var uvElement))
                        {
                            reason = "UV mapping missing.";
                            return false;
                        }
                        ReadPredictableUv(element.Type, stream, uvElement, uvChannels, vertexIndex);
                        break;
                    default:
                        _ = ReadPredictableDiscard(element.Type, stream);
                        break;
                }
            }

            if (boneWeights != null)
            {
                if (indices == null || weights == null || indices.Length != 4 || weights.Length != 4)
                {
                    reason = "Missing or invalid skinning data.";
                    return false;
                }
                boneWeights[vertexIndex] = new CoreBoneWeight(indices[0], indices[1], indices[2], indices[3], weights[0], weights[1], weights[2], weights[3]);
            }
        }

        decoded = new PredictableDecodedMeshData(positions, normals, tangents, tangents2, colorChannels, boneWeights, uvChannels, positionWs, normalWs, blendWeightEncoding);
        return true;
    }

    private static bool TryBuildPredictableSubMeshIndices(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteIndexStart, SubmeshStruct[] meshSubMeshes, out int[][] subMeshIndices, out string? reason)
    {
        reason = null;
        subMeshIndices = new int[meshSubMeshes.Length][];
        if (!TryReadMeshIndices(bytes, absoluteIndexStart, mesh.IndexCount, out var sourceIndices))
        {
            reason = "Failed to read mesh indices.";
            return false;
        }

        for (int i = 0; i < meshSubMeshes.Length; i++)
        {
            if (!TryGetSubmeshRangeWithinMesh(mesh.StartIndex, mesh.IndexCount, meshSubMeshes[i], out var submeshOffset, out var submeshIndexCount))
            {
                reason = "Invalid submesh range.";
                return false;
            }
            int[] indices = new int[submeshIndexCount];
            for (int j = 0; j < submeshIndexCount; j++)
                indices[j] = sourceIndices[submeshOffset + j];
            subMeshIndices[i] = indices;
        }
        return true;
    }

    private static bool TryEncodePredictableMeshData(PredictableDecodedMeshData decimated, int[][] decimatedSubMeshIndices, PredictableVertexFormat format, MeshStruct originalMesh, SubmeshStruct[] originalSubMeshes, out MeshStruct updatedMesh, out SubmeshStruct[] updatedSubMeshes, out byte[][] vertexStreams, out int[] indices, out string? reason)
    {
        updatedMesh = originalMesh;
        updatedSubMeshes = [];
        vertexStreams = [[], [], []];
        indices = [];
        reason = null;

        if (decimatedSubMeshIndices.Length != originalSubMeshes.Length)
        {
            reason = "Decimated submesh count mismatch.";
            return false;
        }

        int vertexCount = decimated.Positions.Length;
        if (vertexCount <= 0 || vertexCount > ushort.MaxValue)
        {
            reason = "Vertex count out of range after decimation.";
            return false;
        }

        if (format.HasTangent1
            && TryRebuildPredictableStoredBitangents(decimated, decimatedSubMeshIndices, format, out var rebuiltTangents))
        {
            decimated = new PredictableDecodedMeshData(
                decimated.Positions,
                decimated.Normals,
                rebuiltTangents,
                decimated.Tangents2,
                decimated.ColorChannels,
                decimated.BoneWeights,
                decimated.UvChannels,
                decimated.PositionWs,
                decimated.NormalWs,
                decimated.BlendWeightEncoding);
        }

        if (!TryValidatePredictableDecodedMeshData(decimated, decimatedSubMeshIndices, format, out reason))
            return false;

        if (!TryBuildPredictableDeclaredVertexElements(format, originalMesh, out var declaredElements, out reason))
            return false;

        var streamBuffers = new byte[3][];
        for (int streamIndex = 0; streamIndex < 3; streamIndex++)
        {
            int stride = originalMesh.VertexBufferStride[streamIndex];
            streamBuffers[streamIndex] = stride > 0 ? new byte[stride * vertexCount] : [];
        }

        for (int vertexIndex = 0; vertexIndex < vertexCount; vertexIndex++)
        {
            foreach (var declaredElement in declaredElements)
            {
                int stride = originalMesh.VertexBufferStride[declaredElement.Stream];
                if (stride <= 0)
                    continue;
                var target = streamBuffers[declaredElement.Stream].AsSpan((vertexIndex * stride) + declaredElement.Offset, declaredElement.Size);
                switch (declaredElement.Usage)
                {
                    case PositionUsage:
                        WritePredictablePosition(declaredElement.Type, decimated.Positions[vertexIndex], target, format.HasPositionW ? decimated.PositionWs?[vertexIndex] : null);
                        break;
                    case NormalUsage:
                        WritePredictableNormal(declaredElement.Type, decimated.Normals?[vertexIndex] ?? default, target, format.HasNormalW ? decimated.NormalWs?[vertexIndex] : null);
                        break;
                    case 5:
                        WritePredictableTangent(declaredElement.Type, decimated.Tangents?[vertexIndex] ?? default, target);
                        break;
                    case 6:
                        WritePredictableTangent(declaredElement.Type, decimated.Tangents2?[vertexIndex] ?? default, target);
                        break;
                    case 7:
                        var colorValue = decimated.ColorChannels != null && declaredElement.ColorChannel is int colorChannel && colorChannel < decimated.ColorChannels.Length
                            ? decimated.ColorChannels[colorChannel][vertexIndex]
                            : default;
                        WritePredictableColor(declaredElement.Type, colorValue, target);
                        break;
                    case BlendIndicesUsage:
                        WritePredictableBlendIndices(declaredElement.Type, decimated.BoneWeights?[vertexIndex] ?? default, target);
                        break;
                    case BlendWeightUsage:
                        WritePredictableBlendWeights(declaredElement.Type, decimated.BoneWeights?[vertexIndex] ?? default, decimated.BlendWeightEncoding, target);
                        break;
                    case TexCoordUsage:
                        if (declaredElement.UvElement is not PredictableUvElementPacking uvPacking || decimated.UvChannels == null)
                        {
                            reason = "UV packing missing while encoding.";
                            return false;
                        }
                        WritePredictableUv(declaredElement.Type, uvPacking, decimated.UvChannels, vertexIndex, target);
                        break;
                    default:
                        target.Clear();
                        break;
                }
            }
        }

        updatedMesh.VertexCount = (ushort)vertexCount;
        var newSubMeshes = new List<SubmeshStruct>(originalSubMeshes.Length);
        var indexList = new List<int>();
        for (int subMeshIndex = 0; subMeshIndex < originalSubMeshes.Length; subMeshIndex++)
        {
            var subMesh = decimatedSubMeshIndices[subMeshIndex];
            if (subMesh.Any(index => index < 0 || index >= vertexCount))
            {
                reason = "Decimated indices out of range.";
                return false;
            }
            int offset = indexList.Count;
            indexList.AddRange(subMesh);
            var updatedSubMesh = originalSubMeshes[subMeshIndex];
            updatedSubMesh.IndexOffset = (uint)offset;
            updatedSubMesh.IndexCount = (uint)subMesh.Length;
            newSubMeshes.Add(updatedSubMesh);
        }

        updatedSubMeshes = newSubMeshes.ToArray();
        indices = indexList.ToArray();
        vertexStreams = streamBuffers;
        return true;
    }

    private static bool TryBuildPredictableDeclaredVertexElements(PredictableVertexFormat format, MeshStruct originalMesh, out PredictableDeclaredVertexElement[] declaredElements, out string? reason)
    {
        declaredElements = [];
        reason = null;
        var colorLookup = format.ColorElements.ToDictionary(static e => PredictableElementKey.From(e.Element), static e => e.Channel);
        var uvLookup = format.UvElements.ToDictionary(static e => PredictableElementKey.From(e.Element), static e => e);
        var elements = new List<PredictableDeclaredVertexElement>(format.SortedElements.Count);
        foreach (var element in format.SortedElements)
        {
            int stride = originalMesh.VertexBufferStride[element.Stream];
            if (stride == 0)
                continue;

            int size = GetPredictableElementSize(element.Type);
            if (element.Offset + size > stride)
            {
                reason = "Vertex element stride overflow.";
                return false;
            }
            int? colorChannel = null;
            PredictableUvElementPacking? uvElement = null;
            if (element.Usage == 7)
            {
                if (!colorLookup.TryGetValue(PredictableElementKey.From(element), out var resolvedColorChannel))
                {
                    reason = "Color mapping missing.";
                    return false;
                }
                colorChannel = resolvedColorChannel;
            }
            else if (element.Usage == TexCoordUsage)
            {
                if (!uvLookup.TryGetValue(PredictableElementKey.From(element), out var resolvedUv))
                {
                    reason = "UV mapping missing.";
                    return false;
                }
                uvElement = resolvedUv;
            }
            elements.Add(new PredictableDeclaredVertexElement(element.Stream, element.Offset, element.Type, element.Usage, colorChannel, uvElement, size));
        }
        declaredElements = elements.ToArray();
        return true;
    }

    private static bool TryBuildPredictableVertexFormat(RavaSync.Interop.GameModel.MdlFile.VertexDeclarationStruct declaration, out PredictableVertexFormat format, out string? reason)
    {
        reason = null;
        format = default!;
        var elements = declaration.VertexElements;
        foreach (var element in elements)
        {
            if (element.Stream >= 3)
            {
                reason = "Vertex stream index out of range.";
                return false;
            }

            if (!IsPredictableSupportedUsage(element.Usage))
            {
                reason = $"Unsupported usage {element.Usage}.";
                return false;
            }

            if (!IsPredictableSupportedType(element.Type))
            {
                reason = $"Unsupported vertex type {element.Type}.";
                return false;
            }
        }

        var positionElements = elements.Where(static e => e.Usage == PositionUsage).ToArray();
        if (positionElements.Length != 1)
        {
            reason = "Expected single position element.";
            return false;
        }

        if (positionElements[0].Type != Single3Type && positionElements[0].Type != Single4Type)
        {
            reason = "Unsupported position element type.";
            return false;
        }

        var normalElements = elements.Where(static e => e.Usage == NormalUsage).ToArray();
        if (normalElements.Length > 1)
        {
            reason = "Multiple normal elements unsupported.";
            return false;
        }

        if (normalElements.Length == 1)
        {
            var normalType = normalElements[0].Type;
            if (normalType != Single3Type
                && normalType != Single4Type
                && normalType != NByte4Type
                && normalType != NShort4Type)
            {
                reason = "Unsupported normal element type.";
                return false;
            }
        }

        var tangent1Elements = elements.Where(static e => e.Usage == 5).ToArray();
        if (tangent1Elements.Length > 1)
        {
            reason = "Multiple tangent1 elements unsupported.";
            return false;
        }

        if (tangent1Elements.Length == 1)
        {
            var tangentType = tangent1Elements[0].Type;
            if (tangentType != Single4Type
                && tangentType != NByte4Type
                && tangentType != NShort4Type)
            {
                reason = "Unsupported tangent1 element type.";
                return false;
            }
        }

        var tangent2Elements = elements.Where(static e => e.Usage == 6).ToArray();
        if (tangent2Elements.Length > 1)
        {
            reason = "Multiple tangent2 elements unsupported.";
            return false;
        }

        if (tangent2Elements.Length == 1)
        {
            var tangentType = tangent2Elements[0].Type;
            if (tangentType != Single4Type
                && tangentType != NByte4Type
                && tangentType != NShort4Type)
            {
                reason = "Unsupported tangent2 element type.";
                return false;
            }
        }

        var colorElements = elements.Where(static e => e.Usage == 7).OrderBy(static e => e.UsageIndex).ToArray();
        if (colorElements.Length > 2)
        {
            reason = "More than two color elements unsupported.";
            return false;
        }

        var colorMappings = new List<PredictableColorElementPacking>(colorElements.Length);
        for (int i = 0; i < colorElements.Length; i++)
        {
            var colorType = colorElements[i].Type;
            if (colorType != UByte4Type
                && colorType != NByte4Type
                && colorType != Single4Type
                && colorType != Short4Type
                && colorType != NShort4Type
                && colorType != UShort4Type)
            {
                reason = "Unsupported color element type.";
                return false;
            }

            colorMappings.Add(new PredictableColorElementPacking(colorElements[i], i));
        }

        var blendIndicesElements = elements.Where(static e => e.Usage == BlendIndicesUsage).ToArray();
        var blendWeightsElements = elements.Where(static e => e.Usage == BlendWeightUsage).ToArray();
        if (blendIndicesElements.Length != blendWeightsElements.Length)
        {
            reason = "Blend indices/weights mismatch.";
            return false;
        }

        if (blendIndicesElements.Length > 1 || blendWeightsElements.Length > 1)
        {
            reason = "Multiple blend elements unsupported.";
            return false;
        }

        if (blendIndicesElements.Length == 1)
        {
            var indexType = blendIndicesElements[0].Type;
            if (indexType != UByte4Type && indexType != UShort4Type)
            {
                reason = "Unsupported blend index type.";
                return false;
            }

            var weightType = blendWeightsElements[0].Type;
            if (weightType != UByte4Type
                && weightType != NByte4Type
                && weightType != Single4Type
                && weightType != UShort4Type
                && weightType != NShort4Type)
            {
                reason = "Unsupported blend weight type.";
                return false;
            }
        }

        if (!TryBuildPredictableUvElements(elements, out var uvElements, out var uvChannelCount, out var uvChannelDimensions, out reason))
            return false;

        format = new PredictableVertexFormat(
            elements.OrderBy(static e => e.Offset).ToList(),
            positionElements[0],
            normalElements.Length == 1 ? normalElements[0] : (VertexElement?)null,
            tangent1Elements.Length == 1 ? tangent1Elements[0] : (VertexElement?)null,
            tangent2Elements.Length == 1 ? tangent2Elements[0] : (VertexElement?)null,
            colorMappings,
            blendIndicesElements.Length == 1 ? blendIndicesElements[0] : (VertexElement?)null,
            blendWeightsElements.Length == 1 ? blendWeightsElements[0] : (VertexElement?)null,
            uvElements,
            uvChannelCount,
            uvChannelDimensions);
        return true;
    }

    private static bool TryBuildPredictableUvElements(IReadOnlyList<VertexElement> elements, out List<PredictableUvElementPacking> uvElements, out int uvChannelCount, out int[] uvChannelDimensions, out string? reason)
    {
        uvElements = [];
        uvChannelCount = 0;
        uvChannelDimensions = [];
        reason = null;
        var channelDimensions = new List<int>();
        var uvList = elements.Where(static e => e.Usage == TexCoordUsage).OrderBy(static e => e.UsageIndex).ToList();
        foreach (var element in uvList)
        {
            var type = element.Type;
            if (type == Single1Type)
            {
                if (uvChannelCount + 1 > 4)
                {
                    reason = "Too many UV channels.";
                    return false;
                }

                uvElements.Add(new PredictableUvElementPacking(element, uvChannelCount, null));
                channelDimensions.Add(1);
                uvChannelCount += 1;
            }
            else if (type is Half2Type or Single2Type or Short2Type or NShort2Type or UShort2Type)
            {
                if (uvChannelCount + 1 > 4)
                {
                    reason = "Too many UV channels.";
                    return false;
                }

                uvElements.Add(new PredictableUvElementPacking(element, uvChannelCount, null));
                channelDimensions.Add(2);
                uvChannelCount += 1;
            }
            else if (type is Half4Type or Single4Type or Short4Type or NShort4Type or UShort4Type)
            {
                if (uvChannelCount + 2 > 4)
                {
                    reason = "Too many UV channels.";
                    return false;
                }

                uvElements.Add(new PredictableUvElementPacking(element, uvChannelCount, uvChannelCount + 1));
                channelDimensions.Add(2);
                channelDimensions.Add(2);
                uvChannelCount += 2;
            }
            else
            {
                reason = "Unsupported UV type.";
                return false;
            }
        }
        uvChannelDimensions = channelDimensions.ToArray();
        return true;
    }

    private static bool IsPredictableSupportedUsage(byte usage)
        => usage is PositionUsage or NormalUsage or 5 or 6 or 7 or BlendIndicesUsage or BlendWeightUsage or TexCoordUsage;

    private static bool IsPredictableSupportedType(byte type)
        => type is Single1Type or Single2Type or Single3Type or Single4Type
            or UByte4Type or Short2Type or Short4Type or NByte4Type or NShort2Type or NShort4Type or UShort2Type or UShort4Type
            or Half2Type or Half4Type;

    private static int GetPredictableElementSize(byte type)
        => type switch
        {
            Single1Type => 4,
            Single2Type => 8,
            Single3Type => 12,
            Single4Type => 16,
            UByte4Type => 4,
            NByte4Type => 4,
            Short2Type => 4,
            Short4Type => 8,
            NShort2Type => 4,
            NShort4Type => 8,
            UShort2Type => 4,
            UShort4Type => 8,
            Half2Type => 4,
            Half4Type => 8,
            _ => 0,
        };

    private static PredictableBlendWeightEncoding DetectPredictableBlendWeightEncoding(ReadOnlySpan<byte> bytes, MeshStruct mesh, long absoluteVertexStart, PredictableVertexFormat format)
    {
        if (!format.BlendWeightsElement.HasValue)
            return PredictableBlendWeightEncoding.Default;

        var element = format.BlendWeightsElement.Value;
        if (element.Type != UShort4Type)
            return PredictableBlendWeightEncoding.Default;

        int stride = mesh.VertexBufferStride[element.Stream];
        if (stride <= 0 || mesh.VertexCount <= 0)
            return PredictableBlendWeightEncoding.Default;

        int elementSize = GetPredictableElementSize(UShort4Type);
        long streamStart = absoluteVertexStart + mesh.VertexBufferOffset[element.Stream];
        for (int vertexIndex = 0; vertexIndex < mesh.VertexCount; vertexIndex++)
        {
            long offset = streamStart + (vertexIndex * stride) + element.Offset;
            if (offset < 0 || offset + elementSize > bytes.Length)
                return PredictableBlendWeightEncoding.Default;

            int baseOffset = checked((int)offset);
            ushort w0 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(baseOffset + 0, 2));
            ushort w1 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(baseOffset + 2, 2));
            ushort w2 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(baseOffset + 4, 2));
            ushort w3 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(baseOffset + 6, 2));
            if (w0 > byte.MaxValue || w1 > byte.MaxValue || w2 > byte.MaxValue || w3 > byte.MaxValue)
                return PredictableBlendWeightEncoding.Default;

            int sum = w0 + w1 + w2 + w3;
            if (sum != 0 && sum != byte.MaxValue)
                return PredictableBlendWeightEncoding.Default;
        }

        return PredictableBlendWeightEncoding.UShortAsByte;
    }

    private static CoreVec3d ReadPredictablePosition(byte type, BinaryReader reader)
        => type switch
        {
            Single3Type => new CoreVec3d(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            Single4Type => new CoreVec3d(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            _ => default,
        };

    private static CoreVec3d ReadPredictablePositionWithW(BinaryReader reader, out float w)
    {
        float x = reader.ReadSingle();
        float y = reader.ReadSingle();
        float z = reader.ReadSingle();
        w = reader.ReadSingle();
        return new CoreVec3d(x, y, z);
    }

    private static CoreVec3 ReadPredictableNormal(byte type, BinaryReader reader)
        => type switch
        {
            Single3Type => new CoreVec3(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            Single4Type => new CoreVec3(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            NByte4Type => ToCoreVec3(ReadPredictableNByte4(reader)),
            NShort4Type => ToCoreVec3(ReadPredictableNShort4(reader)),
            _ => default,
        };

    private static CoreVec3 ReadPredictableNormalWithW(BinaryReader reader, out float w)
    {
        float x = reader.ReadSingle();
        float y = reader.ReadSingle();
        float z = reader.ReadSingle();
        w = reader.ReadSingle();
        return new CoreVec3(x, y, z);
    }

    private static CoreVec4 ReadPredictableTangent(byte type, BinaryReader reader)
        => type switch
        {
            Single4Type => new CoreVec4(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            NByte4Type => ToCoreVec4(ReadPredictableNByte4(reader)),
            NShort4Type => ToCoreVec4(ReadPredictableNShort4(reader)),
            _ => default,
        };

    private static CoreVec4 ReadPredictableColor(byte type, BinaryReader reader)
        => type switch
        {
            UByte4Type => ToCoreVec4(ReadPredictableUByte4(reader), normalize: true),
            NByte4Type => ToCoreVec4(ReadPredictableNByte4(reader)),
            Single4Type => new CoreVec4(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            Short4Type => ToCoreVec4(ReadPredictableShort4(reader)),
            NShort4Type => ToCoreVec4(ReadPredictableNShort4(reader)),
            UShort4Type => ToCoreVec4(ReadPredictableUShort4(reader)),
            _ => default,
        };

    private static void ReadPredictableUv(byte type, BinaryReader reader, PredictableUvElementPacking mapping, CoreVec2[][] uvChannels, int vertexIndex)
    {
        if (type == Single1Type)
        {
            uvChannels[mapping.FirstChannel][vertexIndex] = new CoreVec2(reader.ReadSingle(), 0f);
            return;
        }

        if (type is Half2Type or Single2Type or Short2Type or NShort2Type or UShort2Type)
        {
            CoreVec2 value = type switch
            {
                Half2Type => ReadPredictableHalf2(reader),
                Single2Type => new CoreVec2(reader.ReadSingle(), reader.ReadSingle()),
                Short2Type => ToCoreVec2(ReadPredictableShort2(reader)),
                NShort2Type => ToCoreVec2(ReadPredictableNShort2(reader)),
                UShort2Type => ToCoreVec2(ReadPredictableUShort2(reader)),
                _ => default,
            };
            uvChannels[mapping.FirstChannel][vertexIndex] = value;
            return;
        }

        CoreVec4 value4 = type switch
        {
            Half4Type => ReadPredictableHalf4(reader),
            Single4Type => new CoreVec4(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()),
            Short4Type => ToCoreVec4(ReadPredictableShort4(reader)),
            NShort4Type => ToCoreVec4(ReadPredictableNShort4(reader)),
            UShort4Type => ToCoreVec4(ReadPredictableUShort4(reader)),
            _ => default,
        };
        uvChannels[mapping.FirstChannel][vertexIndex] = new CoreVec2(value4.x, value4.y);
        if (mapping.SecondChannel is int second)
            uvChannels[second][vertexIndex] = new CoreVec2(value4.z, value4.w);
    }

    private static int[] ReadPredictableIndices(byte type, BinaryReader reader)
        => type switch
        {
            UByte4Type => [reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte()],
            UShort4Type => [reader.ReadUInt16(), reader.ReadUInt16(), reader.ReadUInt16(), reader.ReadUInt16()],
            _ => [],
        };

    private static float[] ReadPredictableWeights(byte type, BinaryReader reader, PredictableBlendWeightEncoding encoding)
        => type switch
        {
            UByte4Type => [reader.ReadByte() / 255f, reader.ReadByte() / 255f, reader.ReadByte() / 255f, reader.ReadByte() / 255f],
            NByte4Type => [reader.ReadByte() / 255f, reader.ReadByte() / 255f, reader.ReadByte() / 255f, reader.ReadByte() / 255f],
            Single4Type => [reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle()],
            NShort4Type => [
                reader.ReadUInt16() / 65535f,
                reader.ReadUInt16() / 65535f,
                reader.ReadUInt16() / 65535f,
                reader.ReadUInt16() / 65535f],
            UShort4Type when encoding == PredictableBlendWeightEncoding.UShortAsByte => [reader.ReadUInt16() / 255f, reader.ReadUInt16() / 255f, reader.ReadUInt16() / 255f, reader.ReadUInt16() / 255f],
            UShort4Type => [reader.ReadUInt16() / 65535f, reader.ReadUInt16() / 65535f, reader.ReadUInt16() / 65535f, reader.ReadUInt16() / 65535f],
            Half4Type => [HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16())],
            _ => [],
        };

    private static CoreVec4 ReadPredictableDiscard(byte type, BinaryReader reader)
    {
        int size = GetPredictableElementSize(type);
        if (size <= 0)
            return default;
        reader.BaseStream.Seek(size, SeekOrigin.Current);
        return default;
    }

    private static void WritePredictablePosition(byte type, CoreVec3d value, Span<byte> target, float? wOverride = null)
    {
        BinaryPrimitives.WriteSingleLittleEndian(target[0..4], (float)value.x);
        BinaryPrimitives.WriteSingleLittleEndian(target[4..8], (float)value.y);
        BinaryPrimitives.WriteSingleLittleEndian(target[8..12], (float)value.z);
        if (type == Single4Type)
            BinaryPrimitives.WriteSingleLittleEndian(target[12..16], wOverride ?? 1f);
    }

    private static void WritePredictableNormal(byte type, CoreVec3 value, Span<byte> target, float? wOverride = null)
    {
        if (type == Single3Type || type == Single4Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], value.x);
            BinaryPrimitives.WriteSingleLittleEndian(target[4..8], value.y);
            BinaryPrimitives.WriteSingleLittleEndian(target[8..12], value.z);
            if (type == Single4Type)
                BinaryPrimitives.WriteSingleLittleEndian(target[12..16], wOverride ?? 1f);
            return;
        }
        if (type == NByte4Type)
        {
            target[0] = EncodeSignedNormalizedByte(value.x);
            target[1] = EncodeSignedNormalizedByte(value.y);
            target[2] = EncodeSignedNormalizedByte(value.z);
            target[3] = EncodeSignedNormalizedByte(wOverride ?? 1f);
            return;
        }
        if (type == NShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortSnorm(value.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortSnorm(value.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShortSnorm(value.z));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShortSnorm(wOverride ?? 1f));
        }
    }

    private static void WritePredictableTangent(byte type, CoreVec4 value, Span<byte> target)
    {
        if (type == Single4Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], value.x);
            BinaryPrimitives.WriteSingleLittleEndian(target[4..8], value.y);
            BinaryPrimitives.WriteSingleLittleEndian(target[8..12], value.z);
            BinaryPrimitives.WriteSingleLittleEndian(target[12..16], value.w);
            return;
        }
        if (type == NByte4Type)
        {
            target[0] = EncodeSignedNormalizedByte(value.x);
            target[1] = EncodeSignedNormalizedByte(value.y);
            target[2] = EncodeSignedNormalizedByte(value.z);
            target[3] = EncodeSignedNormalizedByte(value.w);
            return;
        }
        if (type == NShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortSnorm(value.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortSnorm(value.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShortSnorm(value.z));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShortSnorm(value.w));
        }
    }

    private static void WritePredictableColor(byte type, CoreVec4 value, Span<byte> target)
    {
        if (type == UByte4Type)
        {
            target[0] = ToByte(value.x);
            target[1] = ToByte(value.y);
            target[2] = ToByte(value.z);
            target[3] = ToByte(value.w);
            return;
        }
        if (type == NByte4Type)
        {
            target[0] = EncodeSignedNormalizedByte(value.x);
            target[1] = EncodeSignedNormalizedByte(value.y);
            target[2] = EncodeSignedNormalizedByte(value.z);
            target[3] = EncodeSignedNormalizedByte(value.w);
            return;
        }
        if (type == Single4Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], value.x);
            BinaryPrimitives.WriteSingleLittleEndian(target[4..8], value.y);
            BinaryPrimitives.WriteSingleLittleEndian(target[8..12], value.z);
            BinaryPrimitives.WriteSingleLittleEndian(target[12..16], value.w);
            return;
        }
        if (type == Short4Type)
        {
            BinaryPrimitives.WriteInt16LittleEndian(target[0..2], ToShort(value.x));
            BinaryPrimitives.WriteInt16LittleEndian(target[2..4], ToShort(value.y));
            BinaryPrimitives.WriteInt16LittleEndian(target[4..6], ToShort(value.z));
            BinaryPrimitives.WriteInt16LittleEndian(target[6..8], ToShort(value.w));
            return;
        }
        if (type == NShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortSnorm(value.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortSnorm(value.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShortSnorm(value.z));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShortSnorm(value.w));
            return;
        }
        if (type == UShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShort(value.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShort(value.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShort(value.z));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShort(value.w));
        }
    }

    private static void WritePredictableBlendIndices(byte type, CoreBoneWeight weights, Span<byte> target)
    {
        if (type == UShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], (ushort)Math.Clamp(weights.index0, 0, ushort.MaxValue));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], (ushort)Math.Clamp(weights.index1, 0, ushort.MaxValue));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], (ushort)Math.Clamp(weights.index2, 0, ushort.MaxValue));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], (ushort)Math.Clamp(weights.index3, 0, ushort.MaxValue));
            return;
        }
        target[0] = (byte)Math.Clamp(weights.index0, 0, byte.MaxValue);
        target[1] = (byte)Math.Clamp(weights.index1, 0, byte.MaxValue);
        target[2] = (byte)Math.Clamp(weights.index2, 0, byte.MaxValue);
        target[3] = (byte)Math.Clamp(weights.index3, 0, byte.MaxValue);
    }

    private static void WritePredictableBlendWeights(byte type, CoreBoneWeight weights, PredictableBlendWeightEncoding encoding, Span<byte> target)
    {
        if (type == Single4Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], weights.weight0);
            BinaryPrimitives.WriteSingleLittleEndian(target[4..8], weights.weight1);
            BinaryPrimitives.WriteSingleLittleEndian(target[8..12], weights.weight2);
            BinaryPrimitives.WriteSingleLittleEndian(target[12..16], weights.weight3);
            return;
        }
        if (type == Half4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], SingleToHalf(weights.weight0));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], SingleToHalf(weights.weight1));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], SingleToHalf(weights.weight2));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], SingleToHalf(weights.weight3));
            return;
        }

        float w0 = Clamp01(weights.weight0);
        float w1 = Clamp01(weights.weight1);
        float w2 = Clamp01(weights.weight2);
        float w3 = Clamp01(weights.weight3);

        if (type != UShort4Type || encoding == PredictableBlendWeightEncoding.UShortAsByte)
            NormalizeWeights(ref w0, ref w1, ref w2, ref w3);

        if (type == UShort4Type && encoding == PredictableBlendWeightEncoding.UShortAsByte)
        {
            QuantizeByteWeights(w0, w1, w2, w3, out int b0, out int b1, out int b2, out int b3);
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], (ushort)b0);
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], (ushort)b1);
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], (ushort)b2);
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], (ushort)b3);
            return;
        }

        if (type == NShort4Type || type == UShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortNormalized(w0));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortNormalized(w1));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShortNormalized(w2));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShortNormalized(w3));
            return;
        }

        QuantizeByteWeights(w0, w1, w2, w3, out int q0, out int q1, out int q2, out int q3);
        target[0] = (byte)q0;
        target[1] = (byte)q1;
        target[2] = (byte)q2;
        target[3] = (byte)q3;
    }

    private static void WritePredictableUv(byte type, PredictableUvElementPacking mapping, CoreVec2[][] uvChannels, int vertexIndex, Span<byte> target)
    {
        var first = uvChannels[mapping.FirstChannel][vertexIndex];
        if (type == Single1Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], first.x);
            return;
        }

        if (type is Half2Type or Single2Type or Short2Type or NShort2Type or UShort2Type)
        {
            if (type == Single2Type)
            {
                BinaryPrimitives.WriteSingleLittleEndian(target[0..4], first.x);
                BinaryPrimitives.WriteSingleLittleEndian(target[4..8], first.y);
                return;
            }
            if (type == Half2Type)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], SingleToHalf(first.x));
                BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], SingleToHalf(first.y));
                return;
            }
            if (type == Short2Type)
            {
                BinaryPrimitives.WriteInt16LittleEndian(target[0..2], ToShort(first.x));
                BinaryPrimitives.WriteInt16LittleEndian(target[2..4], ToShort(first.y));
                return;
            }
            if (type == NShort2Type)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortSnorm(first.x));
                BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortSnorm(first.y));
                return;
            }

            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShort(first.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShort(first.y));
            return;
        }

        var second = mapping.SecondChannel is int secondChannel ? uvChannels[secondChannel][vertexIndex] : default;
        if (type == Single4Type)
        {
            BinaryPrimitives.WriteSingleLittleEndian(target[0..4], first.x);
            BinaryPrimitives.WriteSingleLittleEndian(target[4..8], first.y);
            BinaryPrimitives.WriteSingleLittleEndian(target[8..12], second.x);
            BinaryPrimitives.WriteSingleLittleEndian(target[12..16], second.y);
            return;
        }
        if (type == Half4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], SingleToHalf(first.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], SingleToHalf(first.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], SingleToHalf(second.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], SingleToHalf(second.y));
            return;
        }
        if (type == Short4Type)
        {
            BinaryPrimitives.WriteInt16LittleEndian(target[0..2], ToShort(first.x));
            BinaryPrimitives.WriteInt16LittleEndian(target[2..4], ToShort(first.y));
            BinaryPrimitives.WriteInt16LittleEndian(target[4..6], ToShort(second.x));
            BinaryPrimitives.WriteInt16LittleEndian(target[6..8], ToShort(second.y));
            return;
        }
        if (type == NShort4Type)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShortSnorm(first.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShortSnorm(first.y));
            BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShortSnorm(second.x));
            BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShortSnorm(second.y));
            return;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(target[0..2], ToUShort(first.x));
        BinaryPrimitives.WriteUInt16LittleEndian(target[2..4], ToUShort(first.y));
        BinaryPrimitives.WriteUInt16LittleEndian(target[4..6], ToUShort(second.x));
        BinaryPrimitives.WriteUInt16LittleEndian(target[6..8], ToUShort(second.y));
    }

    private static NumVec4 ReadPredictableUByte4(BinaryReader reader) => new(reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte());
    private static NumVec4 ReadPredictableNByte4(BinaryReader reader)
    {
        var v = ReadPredictableUByte4(reader);
        return new NumVec4(DecodeSignedNormalizedByte((byte)v.X), DecodeSignedNormalizedByte((byte)v.Y), DecodeSignedNormalizedByte((byte)v.Z), DecodeSignedNormalizedByte((byte)v.W));
    }
    private static NumVec2 ReadPredictableShort2(BinaryReader reader) => new(reader.ReadInt16(), reader.ReadInt16());
    private static NumVec4 ReadPredictableShort4(BinaryReader reader) => new(reader.ReadInt16(), reader.ReadInt16(), reader.ReadInt16(), reader.ReadInt16());
    private static NumVec2 ReadPredictableNShort2(BinaryReader reader)
        => new((reader.ReadUInt16() / 65535f * 2f) - 1f, (reader.ReadUInt16() / 65535f * 2f) - 1f);
    private static NumVec4 ReadPredictableNShort4(BinaryReader reader)
        => new((reader.ReadUInt16() / 65535f * 2f) - 1f, (reader.ReadUInt16() / 65535f * 2f) - 1f, (reader.ReadUInt16() / 65535f * 2f) - 1f, (reader.ReadUInt16() / 65535f * 2f) - 1f);
    private static NumVec2 ReadPredictableUShort2(BinaryReader reader) => new(reader.ReadUInt16(), reader.ReadUInt16());
    private static NumVec4 ReadPredictableUShort4(BinaryReader reader) => new(reader.ReadUInt16(), reader.ReadUInt16(), reader.ReadUInt16(), reader.ReadUInt16());
    private static CoreVec2 ReadPredictableHalf2(BinaryReader reader) => new(HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()));
    private static CoreVec4 ReadPredictableHalf4(BinaryReader reader) => new(HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()), HalfToSingle(reader.ReadUInt16()));

    private static CoreVec2 ToCoreVec2(NumVec2 value, bool normalize = false)
        => normalize ? new CoreVec2(value.X / 65535f, value.Y / 65535f) : new CoreVec2(value.X, value.Y);
    private static CoreVec3 ToCoreVec3(NumVec4 value)
        => new(value.X, value.Y, value.Z);
    private static CoreVec4 ToCoreVec4(NumVec4 value, bool normalize = false)
        => normalize ? new CoreVec4(value.X / 255f, value.Y / 255f, value.Z / 255f, value.W / 255f) : new CoreVec4(value.X, value.Y, value.Z, value.W);
    private static NumVec3 ToNumerics(ReductionCore.Vector3 value) => new((float)value.x, (float)value.y, (float)value.z);


    private static ushort SingleToHalf(float value)
        => BitConverter.HalfToUInt16Bits((Half)value);

    private static byte EncodeSignedNormalizedByte(float value)
    {
        float clamped = Math.Clamp(value, -1f, 1f);
        int signed = (int)MathF.Round(((clamped + 1f) * 0.5f) * 255f);
        return (byte)Math.Clamp(signed, 0, 255);
    }

    private static short EncodeSignedNormalizedShort(float value)
        => (short)Math.Clamp((int)MathF.Round(Math.Clamp(value, -1f, 1f) * 32767f), short.MinValue, short.MaxValue);


    private static float Clamp01(float value)
        => Math.Clamp(value, 0f, 1f);

    private static byte ToByte(float value)
        => (byte)Math.Clamp((int)Math.Round(Clamp01(value) * 255f), 0, 255);

    private static short ToShort(float value)
        => (short)Math.Clamp((int)Math.Round(value), short.MinValue, short.MaxValue);

    private static ushort ToUShort(float value)
        => (ushort)Math.Clamp((int)Math.Round(value), ushort.MinValue, ushort.MaxValue);

    private static ushort ToUShortNormalized(float value)
        => (ushort)Math.Clamp((int)Math.Round(Clamp01(value) * ushort.MaxValue), ushort.MinValue, ushort.MaxValue);

    private static ushort ToUShortSnorm(float value)
    {
        float normalized = (Math.Clamp(value, -1f, 1f) * 0.5f) + 0.5f;
        return ToUShortNormalized(normalized);
    }

    private static void NormalizeWeights(ref float w0, ref float w1, ref float w2, ref float w3)
    {
        float sum = w0 + w1 + w2 + w3;
        if (sum <= float.Epsilon)
            return;

        w0 /= sum;
        w1 /= sum;
        w2 /= sum;
        w3 /= sum;
    }

    private static void QuantizeByteWeights(float w0, float w1, float w2, float w3, out int b0, out int b1, out int b2, out int b3)
    {
        float sum = w0 + w1 + w2 + w3;
        if (sum <= 1e-6f)
        {
            w0 = 1f;
            w1 = 0f;
            w2 = 0f;
            w3 = 0f;
            sum = 1f;
        }

        int targetSum = (int)MathF.Round(sum * 255f);
        if (sum > 0f && targetSum == 0)
            targetSum = 1;

        targetSum = Math.Clamp(targetSum, 0, 255);
        if (targetSum == 0)
        {
            b0 = b1 = b2 = b3 = 0;
            return;
        }

        float scale = targetSum / sum;
        float scaled0 = w0 * scale;
        float scaled1 = w1 * scale;
        float scaled2 = w2 * scale;
        float scaled3 = w3 * scale;

        b0 = (int)MathF.Floor(scaled0);
        b1 = (int)MathF.Floor(scaled1);
        b2 = (int)MathF.Floor(scaled2);
        b3 = (int)MathF.Floor(scaled3);

        int remainder = targetSum - (b0 + b1 + b2 + b3);
        if (remainder > 0)
        {
            Span<float> fractions = stackalloc float[4];
            fractions[0] = scaled0 - b0;
            fractions[1] = scaled1 - b1;
            fractions[2] = scaled2 - b2;
            fractions[3] = scaled3 - b3;

            Span<int> order = stackalloc int[4] { 0, 1, 2, 3 };
            for (int i = 0; i < order.Length - 1; i++)
            {
                for (int j = i + 1; j < order.Length; j++)
                {
                    if (fractions[order[j]] > fractions[order[i]])
                        (order[i], order[j]) = (order[j], order[i]);
                }
            }

            for (int i = 0; i < remainder && i < order.Length; i++)
            {
                switch (order[i])
                {
                    case 0: b0++; break;
                    case 1: b1++; break;
                    case 2: b2++; break;
                    case 3: b3++; break;
                }
            }
        }

        b0 = Math.Clamp(b0, 0, 255);
        b1 = Math.Clamp(b1, 0, 255);
        b2 = Math.Clamp(b2, 0, 255);
        b3 = Math.Clamp(b3, 0, 255);
    }

    private static bool TryRebuildPredictableStoredBitangents(
        PredictableDecodedMeshData decimated,
        int[][] decimatedSubMeshIndices,
        PredictableVertexFormat format,
        out CoreVec4[] rebuiltTangents)
    {
        rebuiltTangents = [];

        if (!format.HasTangent1
            || !format.HasNormals
            || decimated.Normals == null
            || decimated.Tangents == null
            || decimated.UvChannels == null
            || decimated.UvChannels.Length == 0)
        {
            return false;
        }

        var positions = decimated.Positions;
        var normals = decimated.Normals;
        var sourceTangents = decimated.Tangents;
        int vertexCount = positions.Length;
        if (vertexCount == 0
            || normals.Length != vertexCount
            || sourceTangents.Length != vertexCount)
        {
            return false;
        }

        if (!TryGetPredictableTangentUvChannel(format, decimated.UvChannels, vertexCount, out var tangentUvs))
            return false;

        var tangentSums = new NumVec3[vertexCount];
        var bitangentSums = new NumVec3[vertexCount];
        var hasContribution = new bool[vertexCount];
        bool hasAnyContribution = false;

        for (int subMeshIndex = 0; subMeshIndex < decimatedSubMeshIndices.Length; subMeshIndex++)
        {
            var indices = decimatedSubMeshIndices[subMeshIndex];
            for (int i = 0; i + 2 < indices.Length; i += 3)
            {
                int index0 = indices[i];
                int index1 = indices[i + 1];
                int index2 = indices[i + 2];
                if ((uint)index0 >= (uint)vertexCount
                    || (uint)index1 >= (uint)vertexCount
                    || (uint)index2 >= (uint)vertexCount)
                {
                    continue;
                }

                var p0 = ToNumerics(positions[index0]);
                var p1 = ToNumerics(positions[index1]);
                var p2 = ToNumerics(positions[index2]);
                var uvA = new NumVec2(tangentUvs[index0].x, tangentUvs[index0].y);
                var uvB = new NumVec2(tangentUvs[index1].x, tangentUvs[index1].y);
                var uvC = new NumVec2(tangentUvs[index2].x, tangentUvs[index2].y);

                var edge1 = p1 - p0;
                var edge2 = p2 - p0;
                var uvEdge1 = uvB - uvA;
                var uvEdge2 = uvC - uvA;
                float determinant = (uvEdge1.X * uvEdge2.Y) - (uvEdge2.X * uvEdge1.Y);
                if (MathF.Abs(determinant) <= 1e-6f)
                    continue;

                float invDeterminant = 1f / determinant;
                var triangleTangent = (((uvEdge2.Y * edge1) - (uvEdge1.Y * edge2)) * invDeterminant);
                var triangleBitangent = (((uvEdge1.X * edge2) - (uvEdge2.X * edge1)) * invDeterminant);
                if (!IsFinite(triangleTangent) || !IsFinite(triangleBitangent))
                    continue;

                tangentSums[index0] += triangleTangent;
                tangentSums[index1] += triangleTangent;
                tangentSums[index2] += triangleTangent;
                bitangentSums[index0] += triangleBitangent;
                bitangentSums[index1] += triangleBitangent;
                bitangentSums[index2] += triangleBitangent;
                hasContribution[index0] = true;
                hasContribution[index1] = true;
                hasContribution[index2] = true;
                hasAnyContribution = true;
            }
        }

        if (!hasAnyContribution)
            return false;

        rebuiltTangents = new CoreVec4[vertexCount];
        for (int i = 0; i < vertexCount; i++)
        {
            var original = sourceTangents[i];
            if (!hasContribution[i])
            {
                rebuiltTangents[i] = SanitisePredictableStoredBitangent(original, normals[i]);
                continue;
            }

            var normal = new NumVec3(normals[i].x, normals[i].y, normals[i].z);
            if (!TryNormalizePredictableVector(normal, out normal))
            {
                rebuiltTangents[i] = SanitisePredictableStoredBitangent(original, normals[i]);
                continue;
            }

            var tangent = tangentSums[i];
            tangent -= normal * NumVec3.Dot(normal, tangent);
            if (!TryNormalizePredictableVector(tangent, out tangent))
            {
                rebuiltTangents[i] = SanitisePredictableStoredBitangent(original, normals[i]);
                continue;
            }

            var bitangent = bitangentSums[i];
            bitangent -= normal * NumVec3.Dot(normal, bitangent);
            if (!TryNormalizePredictableVector(bitangent, out bitangent))
            {
                bitangent = NumVec3.Cross(normal, tangent);
            }

            float handedness = NumVec3.Dot(NumVec3.Cross(normal, tangent), bitangent) < 0f ? -1f : 1f;
            rebuiltTangents[i] = SanitisePredictableStoredBitangent(new CoreVec4(tangent.X, tangent.Y, tangent.Z, handedness), normals[i]);
        }

        return true;
    }

    private static bool TryGetPredictableTangentUvChannel(PredictableVertexFormat format, CoreVec2[][] uvChannels, int vertexCount, out CoreVec2[] tangentUvs)
    {
        tangentUvs = [];
        if (uvChannels.Length == 0 || format.UvChannelCount <= 0)
            return false;

        int channels = Math.Min(Math.Min(format.UvChannelCount, format.UvChannelDimensions.Length), uvChannels.Length);
        for (int channel = 0; channel < channels; channel++)
        {
            if (format.UvChannelDimensions[channel] < 2)
                continue;

            var candidate = uvChannels[channel];
            if (candidate != null && candidate.Length == vertexCount)
            {
                tangentUvs = candidate;
                return true;
            }
        }

        return false;
    }

    private static CoreVec4 SanitisePredictableStoredBitangent(CoreVec4 tangent, CoreVec3 normal)
    {
        var tangent3 = new NumVec3(tangent.x, tangent.y, tangent.z);
        var normal3 = new NumVec3(normal.x, normal.y, normal.z);

        if (!TryNormalizePredictableVector(normal3, out normal3))
            return new CoreVec4(1f, 0f, 0f, tangent.w >= 0f ? 1f : -1f);

        tangent3 -= normal3 * NumVec3.Dot(normal3, tangent3);
        if (!TryNormalizePredictableVector(tangent3, out tangent3))
        {
            tangent3 = MathF.Abs(normal3.X) < 0.9f
                ? NumVec3.Normalize(NumVec3.Cross(normal3, NumVec3.UnitX))
                : NumVec3.Normalize(NumVec3.Cross(normal3, NumVec3.UnitY));
        }

        float handedness = tangent.w >= 0f ? 1f : -1f;
        return new CoreVec4(tangent3.X, tangent3.Y, tangent3.Z, handedness);
    }

    private static bool TryNormalizePredictableVector(NumVec3 value, out NumVec3 normalized)
    {
        normalized = value;
        float lengthSq = value.LengthSquared();
        if (lengthSq <= 1e-8f || !float.IsFinite(lengthSq))
            return false;

        normalized = value / MathF.Sqrt(lengthSq);
        return IsFinite(normalized);
    }

    private static bool IsFinite(NumVec3 value)
        => float.IsFinite(value.X) && float.IsFinite(value.Y) && float.IsFinite(value.Z);

    private readonly record struct PredictableColorElementPacking(VertexElement Element, int Channel);
    private readonly record struct PredictableUvElementPacking(VertexElement Element, int FirstChannel, int? SecondChannel);
    private readonly record struct PredictableDeclaredVertexElement(byte Stream, byte Offset, byte Type, byte Usage, int? ColorChannel, PredictableUvElementPacking? UvElement, int Size);
    private readonly record struct PredictableElementKey(byte Stream, byte Offset, byte Type, byte Usage, byte UsageIndex)
    {
        public static PredictableElementKey From(VertexElement element) => new(element.Stream, element.Offset, element.Type, element.Usage, element.UsageIndex);
    }
    private enum PredictableBlendWeightEncoding
    {
        Default,
        UShortAsByte,
    }

    private sealed class PredictableVertexFormat(
        List<VertexElement> sortedElements,
        VertexElement positionElement,
        VertexElement? normalElement,
        VertexElement? tangent1Element,
        VertexElement? tangent2Element,
        List<PredictableColorElementPacking> colorElements,
        VertexElement? blendIndicesElement,
        VertexElement? blendWeightsElement,
        List<PredictableUvElementPacking> uvElements,
        int uvChannelCount,
        int[] uvChannelDimensions)
    {
        public List<VertexElement> SortedElements { get; } = sortedElements;
        public VertexElement PositionElement { get; } = positionElement;
        public VertexElement? NormalElement { get; } = normalElement;
        public VertexElement? Tangent1Element { get; } = tangent1Element;
        public VertexElement? Tangent2Element { get; } = tangent2Element;
        public List<PredictableColorElementPacking> ColorElements { get; } = colorElements;
        public VertexElement? BlendIndicesElement { get; } = blendIndicesElement;
        public VertexElement? BlendWeightsElement { get; } = blendWeightsElement;
        public List<PredictableUvElementPacking> UvElements { get; } = uvElements;
        public int UvChannelCount { get; } = uvChannelCount;
        public int[] UvChannelDimensions { get; } = uvChannelDimensions;
        public bool HasNormals => NormalElement.HasValue;
        public bool HasTangent1 => Tangent1Element.HasValue;
        public bool HasTangent2 => Tangent2Element.HasValue;
        public bool HasColors => ColorElements.Count > 0;
        public int ColorChannelCount => ColorElements.Count;
        public bool HasSkinning => BlendIndicesElement.HasValue && BlendWeightsElement.HasValue;
        public bool HasPositionW => PositionElement.Type == Single4Type;
        public bool HasNormalW => NormalElement.HasValue && NormalElement.Value.Type == Single4Type;
    }

    private sealed class PredictableDecodedMeshData(
        CoreVec3d[] positions,
        CoreVec3[]? normals,
        CoreVec4[]? tangents,
        CoreVec4[]? tangents2,
        CoreVec4[][]? colorChannels,
        CoreBoneWeight[]? boneWeights,
        CoreVec2[][]? uvChannels,
        float[]? positionWs,
        float[]? normalWs,
        PredictableBlendWeightEncoding blendWeightEncoding)
    {
        public CoreVec3d[] Positions { get; } = positions;
        public CoreVec3[]? Normals { get; } = normals;
        public CoreVec4[]? Tangents { get; } = tangents;
        public CoreVec4[]? Tangents2 { get; } = tangents2;
        public CoreVec4[][]? ColorChannels { get; } = colorChannels;
        public CoreBoneWeight[]? BoneWeights { get; } = boneWeights;
        public CoreVec2[][]? UvChannels { get; } = uvChannels;
        public float[]? PositionWs { get; } = positionWs;
        public float[]? NormalWs { get; } = normalWs;
        public PredictableBlendWeightEncoding BlendWeightEncoding { get; } = blendWeightEncoding;
    }
}
