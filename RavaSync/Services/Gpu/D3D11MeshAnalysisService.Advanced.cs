using Microsoft.Extensions.Logging;
using System;
using Vortice.Direct3D11;
using System.Numerics;
using System.Runtime.InteropServices;
using static Vortice.Direct3D11.D3D11;

namespace RavaSync.Services.Gpu;

public sealed partial class D3D11MeshAnalysisService
{
    private const string BodyProximityShaderSource = """
cbuffer BodyProximityConstants : register(b0)
{
    uint QueryCount;
    uint TriangleCount;
    float ThresholdSq;
    uint Padding;
};

struct BodyTriangle
{
    float3 A;
    float3 B;
    float3 C;
    float3 Min;
    float3 Max;
};

StructuredBuffer<float3> QueryPositions : register(t0);
StructuredBuffer<BodyTriangle> BodyTriangles : register(t1);
RWStructuredBuffer<uint> OutputMask : register(u0);

float PointTriangleDistanceSq(float3 p, float3 a, float3 b, float3 c)
{
    float3 ab = b - a;
    float3 ac = c - a;
    float3 ap = p - a;
    float d1 = dot(ab, ap);
    float d2 = dot(ac, ap);
    if (d1 <= 0.0f && d2 <= 0.0f)
    {
        float3 diff = p - a;
        return dot(diff, diff);
    }

    float3 bp = p - b;
    float d3 = dot(ab, bp);
    float d4 = dot(ac, bp);
    if (d3 >= 0.0f && d4 <= d3)
    {
        float3 diff = p - b;
        return dot(diff, diff);
    }

    float vc = d1 * d4 - d3 * d2;
    if (vc <= 0.0f && d1 >= 0.0f && d3 <= 0.0f)
    {
        float v = d1 / (d1 - d3);
        float3 proj = a + v * ab;
        float3 diff = p - proj;
        return dot(diff, diff);
    }

    float3 cp = p - c;
    float d5 = dot(ab, cp);
    float d6 = dot(ac, cp);
    if (d6 >= 0.0f && d5 <= d6)
    {
        float3 diff = p - c;
        return dot(diff, diff);
    }

    float vb = d5 * d2 - d1 * d6;
    if (vb <= 0.0f && d2 >= 0.0f && d6 <= 0.0f)
    {
        float w = d2 / (d2 - d6);
        float3 proj = a + w * ac;
        float3 diff = p - proj;
        return dot(diff, diff);
    }

    float va = d3 * d6 - d5 * d4;
    if (va <= 0.0f && (d4 - d3) >= 0.0f && (d5 - d6) >= 0.0f)
    {
        float w = (d4 - d3) / ((d4 - d3) + (d5 - d6));
        float3 proj = b + w * (c - b);
        float3 diff = p - proj;
        return dot(diff, diff);
    }

    float denom = 1.0f / (va + vb + vc);
    float v2 = vb * denom;
    float w2 = vc * denom;
    float3 projPoint = a + ab * v2 + ac * w2;
    float3 delta = p - projPoint;
    return dot(delta, delta);
}

float PointAabbDistanceSq(float3 p, float3 minValue, float3 maxValue)
{
    float3 zero = float3(0.0f, 0.0f, 0.0f);
    float3 below = max(minValue - p, zero);
    float3 above = max(p - maxValue, zero);
    float3 delta = below + above;
    return dot(delta, delta);
}

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint queryIndex = threadId.x;
    if (queryIndex >= QueryCount)
        return;

    float3 query = QueryPositions[queryIndex];
    [loop]
    for (uint triangleIndex = 0u; triangleIndex < TriangleCount; triangleIndex++)
    {
        BodyTriangle tri = BodyTriangles[triangleIndex];
        if (PointAabbDistanceSq(query, tri.Min, tri.Max) > ThresholdSq)
            continue;

        if (PointTriangleDistanceSq(query, tri.A, tri.B, tri.C) <= ThresholdSq)
        {
            OutputMask[queryIndex] = 1u;
            return;
        }
    }

    OutputMask[queryIndex] = 0u;
}
""";

    private const string AttributeProtectionShaderSource = """
cbuffer AttributeProtectionConstants : register(b0)
{
    uint PairCount;
    uint VertexCount;
    uint HasUvs;
    uint HasNormals;
    float UvThresholdSq;
    float BoneOverlapThreshold;
    float NormalThresholdCos;
    uint HasSkinning;
};

struct BoneWeightData
{
    uint4 Indices;
    float4 Weights;
};

StructuredBuffer<uint2> AttributePairs : register(t0);
StructuredBuffer<float2> Uvs : register(t1);
StructuredBuffer<float4> Normals : register(t2);
StructuredBuffer<BoneWeightData> BoneWeights : register(t3);
RWStructuredBuffer<uint> OutputMask : register(u0);

bool AreUvsSimilar(uint left, uint right)
{
    float2 uvA = Uvs[left];
    float2 uvB = Uvs[right];
    float2 delta = uvA - uvB;
    return dot(delta, delta) <= UvThresholdSq;
}

bool AreNormalsSimilar(uint left, uint right)
{
    float3 normalA = Normals[left].xyz;
    float3 normalB = Normals[right].xyz;
    float lengthSqA = dot(normalA, normalA);
    float lengthSqB = dot(normalB, normalB);
    if (lengthSqA <= 1.0e-10f || lengthSqB <= 1.0e-10f)
        return true;

    normalA *= rsqrt(lengthSqA);
    normalB *= rsqrt(lengthSqB);
    return dot(normalA, normalB) >= NormalThresholdCos;
}

uint GetBoneIndex(BoneWeightData bone, uint slot)
{
    if (slot == 0u) return bone.Indices.x;
    if (slot == 1u) return bone.Indices.y;
    if (slot == 2u) return bone.Indices.z;
    return bone.Indices.w;
}

float GetBoneWeight(BoneWeightData bone, uint slot)
{
    if (slot == 0u) return bone.Weights.x;
    if (slot == 1u) return bone.Weights.y;
    if (slot == 2u) return bone.Weights.z;
    return bone.Weights.w;
}

bool AreBoneWeightsSimilar(uint left, uint right)
{
    if (BoneOverlapThreshold <= 0.0f)
        return true;

    BoneWeightData leftBone = BoneWeights[left];
    BoneWeightData rightBone = BoneWeights[right];

    float sumLeft = leftBone.Weights.x + leftBone.Weights.y + leftBone.Weights.z + leftBone.Weights.w;
    float sumRight = rightBone.Weights.x + rightBone.Weights.y + rightBone.Weights.z + rightBone.Weights.w;
    float denom = max(sumLeft, sumRight);
    if (denom <= 1.0e-6f)
        return true;

    float overlap = 0.0f;
    [loop]
    for (uint i = 0u; i < 4u; i++)
    {
        uint index = GetBoneIndex(leftBone, i);
        float weightA = GetBoneWeight(leftBone, i);
        if (weightA <= 0.0f)
            continue;

        [loop]
        for (uint j = 0u; j < 4u; j++)
        {
            if (GetBoneIndex(rightBone, j) == index)
            {
                float weightB = GetBoneWeight(rightBone, j);
                if (weightB > 0.0f)
                    overlap += min(weightA, weightB);
                break;
            }
        }
    }

    return (overlap / denom) >= BoneOverlapThreshold;
}

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint pairIndex = threadId.x;
    if (pairIndex >= PairCount)
        return;

    uint2 pair = AttributePairs[pairIndex];
    if (pair.x >= VertexCount || pair.y >= VertexCount)
        return;

    bool protect = false;
    if (HasUvs != 0u)
        protect = !AreUvsSimilar(pair.x, pair.y);
    if (!protect && HasNormals != 0u)
        protect = !AreNormalsSimilar(pair.x, pair.y);
    if (!protect && HasSkinning != 0u)
        protect = !AreBoneWeightsSimilar(pair.x, pair.y);

    if (protect)
    {
        OutputMask[pair.x] = 1u;
        OutputMask[pair.y] = 1u;
    }
}
""";


    private const string ProtectedExpandShaderSource = """
cbuffer ProtectedExpandConstants : register(b0)
{
    uint VertexCount;
    uint TriangleCount;
    uint Padding0;
    uint Padding1;
};

StructuredBuffer<uint> SeedMask : register(t0);
StructuredBuffer<uint> Indices : register(t1);
RWStructuredBuffer<uint> OutputMask : register(u0);

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint triangleIndex = threadId.x;
    if (triangleIndex >= TriangleCount)
        return;

    uint baseIndex = triangleIndex * 3u;
    uint a = Indices[baseIndex + 0u];
    uint b = Indices[baseIndex + 1u];
    uint c = Indices[baseIndex + 2u];
    if (a >= VertexCount || b >= VertexCount || c >= VertexCount)
        return;

    uint seed = SeedMask[a] | SeedMask[b] | SeedMask[c];
    if (seed != 0u)
    {
        OutputMask[a] = 1u;
        OutputMask[b] = 1u;
        OutputMask[c] = 1u;
    }
}
""";

    private const string AverageEdgeLengthShaderSource = """
cbuffer AverageEdgeLengthConstants : register(b0)
{
    uint TriangleCount;
    uint PositionCount;
    uint Padding0;
    uint Padding1;
};

StructuredBuffer<uint> Indices : register(t0);
StructuredBuffer<float3> Positions : register(t1);
RWStructuredBuffer<float2> OutputMetrics : register(u0);

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint triangleIndex = threadId.x;
    if (triangleIndex >= TriangleCount)
        return;

    uint baseIndex = triangleIndex * 3u;
    uint a = Indices[baseIndex + 0u];
    uint b = Indices[baseIndex + 1u];
    uint c = Indices[baseIndex + 2u];
    if (a >= PositionCount || b >= PositionCount || c >= PositionCount)
    {
        OutputMetrics[triangleIndex] = float2(0.0f, 0.0f);
        return;
    }

    float3 pa = Positions[a];
    float3 pb = Positions[b];
    float3 pc = Positions[c];
    float edgeSum = length(pb - pa) + length(pc - pb) + length(pa - pc);
    OutputMetrics[triangleIndex] = float2(edgeSum, 3.0f);
}
""";

    private const string CollapseCandidateScoreShaderSource = """
cbuffer CollapseCandidateConstants : register(b0)
{
    uint CandidateCount;
    uint PositionCount;
    uint HasNormals;
    uint Padding0;
};

struct CollapseCandidate
{
    uint KeepVertex;
    uint DropVertex;
    float ReferenceEdgeLength;
    float LocalMedianEdgeLength;
    uint BoundaryPressure;
    uint Flags;
    float BoundaryPenaltyScale;
    float NearBodyPenalty;
};

StructuredBuffer<CollapseCandidate> Candidates : register(t0);
StructuredBuffer<float3> Positions : register(t1);
StructuredBuffer<float4> Normals : register(t2);
RWStructuredBuffer<float2> OutputMetrics : register(u0);

float SafeLengthSq(float3 value)
{
    return dot(value, value);
}

[numthreads(64, 1, 1)]
void main(uint3 threadId : SV_DispatchThreadID)
{
    uint candidateIndex = threadId.x;
    if (candidateIndex >= CandidateCount)
        return;

    CollapseCandidate candidate = Candidates[candidateIndex];
    if (candidate.KeepVertex >= PositionCount || candidate.DropVertex >= PositionCount || candidate.KeepVertex == candidate.DropVertex)
    {
        OutputMetrics[candidateIndex] = float2(3.402823e+38f, 0.0f);
        return;
    }

    float3 keepPosition = Positions[candidate.KeepVertex];
    float3 dropPosition = Positions[candidate.DropVertex];
    float edgeLength = length(keepPosition - dropPosition);
    if (edgeLength <= 1.0e-6f)
    {
        OutputMetrics[candidateIndex] = float2(3.402823e+38f, 0.0f);
        return;
    }

    float referenceEdgeLength = candidate.ReferenceEdgeLength > 1.0e-6f
        ? candidate.ReferenceEdgeLength
        : max(candidate.LocalMedianEdgeLength, 1.0e-6f);

    float score = edgeLength / max(referenceEdgeLength, 1.0e-6f);
    if (candidate.LocalMedianEdgeLength > 1.0e-6f)
    {
        score = min(score, edgeLength / candidate.LocalMedianEdgeLength);
    }

    if (HasNormals != 0u)
    {
        float3 keepNormal = Normals[candidate.KeepVertex].xyz;
        float3 dropNormal = Normals[candidate.DropVertex].xyz;
        float keepLengthSq = SafeLengthSq(keepNormal);
        float dropLengthSq = SafeLengthSq(dropNormal);
        if (keepLengthSq > 1.0e-10f && dropLengthSq > 1.0e-10f)
        {
            keepNormal *= rsqrt(keepLengthSq);
            dropNormal *= rsqrt(dropLengthSq);
            score += (1.0f - saturate(dot(keepNormal, dropNormal))) * 0.30f;
        }
    }

    score += min(0.18f, candidate.BoundaryPenaltyScale * candidate.BoundaryPressure);
    if ((candidate.Flags & 1u) != 0u)
        score += 0.05f;
    if ((candidate.Flags & 6u) != 0u)
        score += candidate.NearBodyPenalty;

    OutputMetrics[candidateIndex] = float2(score, 1.0f);
}
""";


    public bool TryBuildBodyProximityMask(ReadOnlySpan<Vector3> queryPositions, ReadOnlySpan<GpuBodyTriangle> bodyTriangles, float maxDistance, out bool[] mask)
    {
        mask = [];
        if (queryPositions.IsEmpty || bodyTriangles.IsEmpty || maxDistance <= 0f)
            return false;

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return false;

        _queue.Wait();
        state.ContextLock.Wait();
        try
        {
            return ExecuteBodyProximityMask(state, queryPositions, bodyTriangles, maxDistance, out mask);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "GPU body proximity mask build failed; falling back to CPU analysis.");
            mask = [];
            return false;
        }
        finally
        {
            state.ContextLock.Release();
            _queue.Release();
        }
    }

    public bool TryBuildAttributeProtectionMask(ReadOnlySpan<AttributePair> pairs, int vertexCount, ReadOnlySpan<Vector3> normals, ReadOnlySpan<Vector2> uvs, ReadOnlySpan<GpuSkinningVertex> skinning, out bool[] mask)
    {
        mask = [];
        if (pairs.IsEmpty || vertexCount <= 0)
            return false;

        var state = _sharedDeviceService.TryGetState();
        if (state == null)
            return false;

        _queue.Wait();
        state.ContextLock.Wait();
        try
        {
            return ExecuteAttributeProtectionMask(state, pairs, vertexCount, normals, uvs, skinning, out mask);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "GPU attribute protection mask build failed; falling back to CPU analysis.");
            mask = [];
            return false;
        }
        finally
        {
            state.ContextLock.Release();
            _queue.Release();
        }
    }

    private bool ExecuteBodyProximityMask(D3D11SharedDeviceService.State state, ReadOnlySpan<Vector3> queryPositions, ReadOnlySpan<GpuBodyTriangle> bodyTriangles, float maxDistance, out bool[] mask)
    {
        mask = [];

        int queryCount = queryPositions.Length;
        int triangleCount = bodyTriangles.Length;
        if (queryCount <= 0 || triangleCount <= 0)
            return false;

        var shaderBytecode = GetMeshShaderBytecode("mesh-body-proximity-v1");
        using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

        using var queryBuffer = state.Device.CreateBuffer(queryPositions,
            BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            structureByteStride: (uint)Marshal.SizeOf<Vector3>());
        using var querySrv = CreateStructuredBufferSrv(state.Device, queryBuffer, queryCount);

        using var bodyBuffer = state.Device.CreateBuffer(bodyTriangles,
            BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            structureByteStride: (uint)Marshal.SizeOf<GpuBodyTriangle>());
        using var bodySrv = CreateStructuredBufferSrv(state.Device, bodyBuffer, triangleCount);

        uint outputByteWidth = checked((uint)(queryCount * sizeof(uint)));
        using var outputBuffer = state.Device.CreateBuffer(
            outputByteWidth,
            BindFlags.UnorderedAccess | BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            sizeof(uint));
        using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, queryCount);
        using var stagingBuffer = state.Device.CreateBuffer(
            outputByteWidth,
            BindFlags.None,
            ResourceUsage.Staging,
            CpuAccessFlags.Read,
            ResourceOptionFlags.None,
            0);

        using var constants = CreateConstantBufferGeneric(state.Device, new BodyProximityConstants((uint)queryCount, (uint)triangleCount, maxDistance * maxDistance, 0u));

        state.Context.CSSetShader(computeShader);
        state.Context.CSSetConstantBuffer(0, constants);
        state.Context.CSSetShaderResource(0, querySrv);
        state.Context.CSSetShaderResource(1, bodySrv);
        state.Context.CSSetUnorderedAccessView(0, outputUav);
        state.Context.Dispatch((uint)((queryCount + 63) / 64), 1, 1);
        state.Context.Flush();
        state.Context.CopyResource(stagingBuffer, outputBuffer);

        var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
        try
        {
            var result = new bool[queryCount];
            unsafe
            {
                var rawSpan = new ReadOnlySpan<uint>((void*)mapped.DataPointer, queryCount);
                for (int i = 0; i < rawSpan.Length; i++)
                    result[i] = rawSpan[i] != 0;
            }

            mask = result;
            return true;
        }
        finally
        {
            state.Context.Unmap(stagingBuffer, 0);
            state.Context.CSSetShader(null);
            state.Context.CSSetConstantBuffer(0, null);
            state.Context.CSSetShaderResource(0, null);
            state.Context.CSSetShaderResource(1, null);
            state.Context.CSSetUnorderedAccessView(0, null);
        }
    }

    private bool ExecuteAttributeProtectionMask(D3D11SharedDeviceService.State state, ReadOnlySpan<AttributePair> pairs, int vertexCount, ReadOnlySpan<Vector3> normals, ReadOnlySpan<Vector2> uvs, ReadOnlySpan<GpuSkinningVertex> skinning, out bool[] mask)
    {
        mask = [];
        if (pairs.IsEmpty || vertexCount <= 0)
            return false;

        var shaderBytecode = GetMeshShaderBytecode("mesh-attribute-protection-v1");
        using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

        using var pairBuffer = state.Device.CreateBuffer(pairs,
            BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            structureByteStride: (uint)Marshal.SizeOf<AttributePair>());
        using var pairSrv = CreateStructuredBufferSrv(state.Device, pairBuffer, pairs.Length);

        ID3D11Buffer? uvBuffer = null;
        ID3D11ShaderResourceView? uvSrv = null;
        ID3D11Buffer? normalBuffer = null;
        ID3D11ShaderResourceView? normalSrv = null;
        ID3D11Buffer? skinBuffer = null;
        ID3D11ShaderResourceView? skinSrv = null;
        try
        {
            if (!uvs.IsEmpty)
            {
                uvBuffer = state.Device.CreateBuffer(uvs,
                    BindFlags.ShaderResource,
                    ResourceUsage.Default,
                    CpuAccessFlags.None,
                    ResourceOptionFlags.BufferStructured,
                    structureByteStride: (uint)Marshal.SizeOf<Vector2>());
                uvSrv = CreateStructuredBufferSrv(state.Device, uvBuffer, uvs.Length);
            }

            if (!normals.IsEmpty)
            {
                var normalData = new Vector4[normals.Length];
                for (int i = 0; i < normals.Length; i++)
                    normalData[i] = new Vector4(normals[i], 0f);

                normalBuffer = state.Device.CreateBuffer(normalData,
                    BindFlags.ShaderResource,
                    ResourceUsage.Default,
                    CpuAccessFlags.None,
                    ResourceOptionFlags.BufferStructured,
                    structureByteStride: (uint)Marshal.SizeOf<Vector4>());
                normalSrv = CreateStructuredBufferSrv(state.Device, normalBuffer, normalData.Length);
            }

            if (!skinning.IsEmpty)
            {
                skinBuffer = state.Device.CreateBuffer(skinning,
                    BindFlags.ShaderResource,
                    ResourceUsage.Default,
                    CpuAccessFlags.None,
                    ResourceOptionFlags.BufferStructured,
                    structureByteStride: (uint)Marshal.SizeOf<GpuSkinningVertex>());
                skinSrv = CreateStructuredBufferSrv(state.Device, skinBuffer, skinning.Length);
            }

            uint outputByteWidth = checked((uint)(vertexCount * sizeof(uint)));
            using var outputBuffer = state.Device.CreateBuffer(
                outputByteWidth,
                BindFlags.UnorderedAccess | BindFlags.ShaderResource,
                ResourceUsage.Default,
                CpuAccessFlags.None,
                ResourceOptionFlags.BufferStructured,
                sizeof(uint));
            using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, vertexCount);
            using var stagingBuffer = state.Device.CreateBuffer(
                outputByteWidth,
                BindFlags.None,
                ResourceUsage.Staging,
                CpuAccessFlags.Read,
                ResourceOptionFlags.None,
                0);

            using var constants = CreateConstantBufferGeneric(state.Device, new AttributeProtectionConstants(
                (uint)pairs.Length,
                (uint)vertexCount,
                uvs.IsEmpty ? 0u : 1u,
                normals.IsEmpty ? 0u : 1u,
                0.02f * 0.02f,
                0.85f,
                0.5f,
                skinning.IsEmpty ? 0u : 1u));

            state.Context.CSSetShader(computeShader);
            state.Context.CSSetConstantBuffer(0, constants);
            state.Context.CSSetShaderResource(0, pairSrv);
            state.Context.CSSetShaderResource(1, uvSrv);
            state.Context.CSSetShaderResource(2, normalSrv);
            state.Context.CSSetShaderResource(3, skinSrv);
            state.Context.CSSetUnorderedAccessView(0, outputUav);
            state.Context.Dispatch((uint)((pairs.Length + 63) / 64), 1, 1);
            state.Context.Flush();
            state.Context.CopyResource(stagingBuffer, outputBuffer);

            var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
            try
            {
                var result = new bool[vertexCount];
                unsafe
                {
                    var rawSpan = new ReadOnlySpan<uint>((void*)mapped.DataPointer, vertexCount);
                    for (int i = 0; i < rawSpan.Length; i++)
                        result[i] = rawSpan[i] != 0;
                }

                mask = result;
                return true;
            }
            finally
            {
                state.Context.Unmap(stagingBuffer, 0);
                state.Context.CSSetShader(null);
                state.Context.CSSetConstantBuffer(0, null);
                state.Context.CSSetShaderResource(0, null);
                state.Context.CSSetShaderResource(1, null);
                state.Context.CSSetShaderResource(2, null);
                state.Context.CSSetShaderResource(3, null);
                state.Context.CSSetUnorderedAccessView(0, null);
            }
        }
        finally
        {
            skinSrv?.Dispose();
            skinBuffer?.Dispose();
            normalSrv?.Dispose();
            normalBuffer?.Dispose();
            uvSrv?.Dispose();
            uvBuffer?.Dispose();
        }
    }


public bool TryExpandProtectedVertexMask(ReadOnlySpan<uint> indices, ReadOnlySpan<bool> seedMask, int vertexCount, out bool[] mask)
{
    mask = [];
    if (indices.Length < 3 || indices.Length % 3 != 0 || seedMask.IsEmpty || vertexCount <= 0)
        return false;

    var state = _sharedDeviceService.TryGetState();
    if (state == null)
        return false;

    _queue.Wait();
    state.ContextLock.Wait();
    try
    {
        return ExecuteProtectedExpandMask(state, indices, seedMask, vertexCount, out mask);
    }
    catch (Exception ex)
    {
        _logger.LogDebug(ex, "GPU protected vertex expansion failed; falling back to CPU expansion.");
        mask = [];
        return false;
    }
    finally
    {
        state.ContextLock.Release();
        _queue.Release();
    }
}

public bool TryComputeAverageEdgeLength(ReadOnlySpan<uint> indices, ReadOnlySpan<Vector3> positions, out float averageEdgeLength)
{
    averageEdgeLength = 0f;
    if (indices.Length < 3 || indices.Length % 3 != 0 || positions.IsEmpty)
        return false;

    var state = _sharedDeviceService.TryGetState();
    if (state == null)
        return false;

    _queue.Wait();
    state.ContextLock.Wait();
    try
    {
        return ExecuteAverageEdgeLength(state, indices, positions, out averageEdgeLength);
    }
    catch (Exception ex)
    {
        _logger.LogDebug(ex, "GPU average edge length compute failed; falling back to CPU edge metrics.");
        averageEdgeLength = 0f;
        return false;
    }
    finally
    {
        state.ContextLock.Release();
        _queue.Release();
    }
}

public bool TryScoreCollapseCandidates(ReadOnlySpan<GpuCollapseCandidate> candidates, ReadOnlySpan<Vector3> positions, ReadOnlySpan<Vector3> normals, out CollapseCandidateScore[] scores)
{
    scores = [];
    if (candidates.IsEmpty || positions.IsEmpty)
        return false;

    var state = _sharedDeviceService.TryGetState();
    if (state == null)
        return false;

    _queue.Wait();
    state.ContextLock.Wait();
    try
    {
        return ExecuteCollapseCandidateScores(state, candidates, positions, normals, out scores);
    }
    catch (Exception ex)
    {
        _logger.LogDebug(ex, "GPU collapse candidate scoring failed; falling back to CPU candidate ordering.");
        scores = [];
        return false;
    }
    finally
    {
        state.ContextLock.Release();
        _queue.Release();
    }
}

private bool ExecuteProtectedExpandMask(D3D11SharedDeviceService.State state, ReadOnlySpan<uint> indices, ReadOnlySpan<bool> seedMask, int vertexCount, out bool[] mask)
{
    mask = [];
    int triangleCount = indices.Length / 3;
    if (triangleCount <= 0 || vertexCount <= 0)
        return false;

    var shaderBytecode = GetMeshShaderBytecode("mesh-protected-expand-v1");
    using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

    var seedValues = new uint[vertexCount];
    int seedLimit = Math.Min(vertexCount, seedMask.Length);
    for (int i = 0; i < seedLimit; i++)
    {
        if (seedMask[i])
            seedValues[i] = 1u;
    }

    using var seedBuffer = state.Device.CreateBuffer(seedValues,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        sizeof(uint));
    using var seedSrv = CreateStructuredBufferSrv(state.Device, seedBuffer, seedValues.Length);

    using var indexBuffer = state.Device.CreateBuffer(indices,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        sizeof(uint));
    using var indexSrv = CreateStructuredBufferSrv(state.Device, indexBuffer, indices.Length);

    uint outputByteWidth = checked((uint)(vertexCount * sizeof(uint)));
    using var outputBuffer = state.Device.CreateBuffer(
        outputByteWidth,
        BindFlags.UnorderedAccess | BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        sizeof(uint));
    using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, vertexCount);
    using var stagingBuffer = state.Device.CreateBuffer(
        outputByteWidth,
        BindFlags.None,
        ResourceUsage.Staging,
        CpuAccessFlags.Read,
        ResourceOptionFlags.None,
        0);

    using var constants = CreateConstantBufferGeneric(state.Device, new ProtectedExpandConstants((uint)vertexCount, (uint)triangleCount, 0u, 0u));

    state.Context.CSSetShader(computeShader);
    state.Context.CSSetConstantBuffer(0, constants);
    state.Context.CSSetShaderResource(0, seedSrv);
    state.Context.CSSetShaderResource(1, indexSrv);
    state.Context.CSSetUnorderedAccessView(0, outputUav);
    state.Context.Dispatch((uint)((triangleCount + 63) / 64), 1, 1);
    state.Context.Flush();
    state.Context.CopyResource(stagingBuffer, outputBuffer);

    var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
    try
    {
        var result = new bool[vertexCount];
        unsafe
        {
            var rawSpan = new ReadOnlySpan<uint>((void*)mapped.DataPointer, vertexCount);
            for (int i = 0; i < rawSpan.Length; i++)
                result[i] = rawSpan[i] != 0 || (i < seedLimit && seedMask[i]);
        }

        mask = result;
        return true;
    }
    finally
    {
        state.Context.Unmap(stagingBuffer, 0);
        state.Context.CSSetShader(null);
        state.Context.CSSetConstantBuffer(0, null);
        state.Context.CSSetShaderResource(0, null);
        state.Context.CSSetShaderResource(1, null);
        state.Context.CSSetUnorderedAccessView(0, null);
    }
}

private bool ExecuteAverageEdgeLength(D3D11SharedDeviceService.State state, ReadOnlySpan<uint> indices, ReadOnlySpan<Vector3> positions, out float averageEdgeLength)
{
    averageEdgeLength = 0f;
    int triangleCount = indices.Length / 3;
    if (triangleCount <= 0 || positions.IsEmpty)
        return false;

    var shaderBytecode = GetMeshShaderBytecode("mesh-average-edge-length-v1");
    using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

    using var indexBuffer = state.Device.CreateBuffer(indices,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        sizeof(uint));
    using var indexSrv = CreateStructuredBufferSrv(state.Device, indexBuffer, indices.Length);

    using var positionBuffer = state.Device.CreateBuffer(positions,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        structureByteStride: (uint)Marshal.SizeOf<Vector3>());
    using var positionSrv = CreateStructuredBufferSrv(state.Device, positionBuffer, positions.Length);

    uint stride = (uint)Marshal.SizeOf<AverageEdgeMetric>();
    uint outputByteWidth = checked((uint)(triangleCount * (int)stride));
    using var outputBuffer = state.Device.CreateBuffer(
        outputByteWidth,
        BindFlags.UnorderedAccess | BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        stride);
    using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, triangleCount);
    using var stagingBuffer = state.Device.CreateBuffer(
        outputByteWidth,
        BindFlags.None,
        ResourceUsage.Staging,
        CpuAccessFlags.Read,
        ResourceOptionFlags.None,
        0);

    using var constants = CreateConstantBufferGeneric(state.Device, new AverageEdgeLengthConstants((uint)triangleCount, (uint)positions.Length, 0u, 0u));

    state.Context.CSSetShader(computeShader);
    state.Context.CSSetConstantBuffer(0, constants);
    state.Context.CSSetShaderResource(0, indexSrv);
    state.Context.CSSetShaderResource(1, positionSrv);
    state.Context.CSSetUnorderedAccessView(0, outputUav);
    state.Context.Dispatch((uint)((triangleCount + 63) / 64), 1, 1);
    state.Context.Flush();
    state.Context.CopyResource(stagingBuffer, outputBuffer);

    var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
    try
    {
        double edgeSum = 0d;
        double edgeCount = 0d;
        unsafe
        {
            var rawSpan = new ReadOnlySpan<AverageEdgeMetric>((void*)mapped.DataPointer, triangleCount);
            for (int i = 0; i < rawSpan.Length; i++)
            {
                edgeSum += rawSpan[i].EdgeSum;
                edgeCount += rawSpan[i].EdgeCount;
            }
        }

        if (edgeCount <= 0d)
            return false;

        averageEdgeLength = (float)(edgeSum / edgeCount);
        return averageEdgeLength > 0f;
    }
    finally
    {
        state.Context.Unmap(stagingBuffer, 0);
        state.Context.CSSetShader(null);
        state.Context.CSSetConstantBuffer(0, null);
        state.Context.CSSetShaderResource(0, null);
        state.Context.CSSetShaderResource(1, null);
        state.Context.CSSetUnorderedAccessView(0, null);
    }
}

private bool ExecuteCollapseCandidateScores(D3D11SharedDeviceService.State state, ReadOnlySpan<GpuCollapseCandidate> candidates, ReadOnlySpan<Vector3> positions, ReadOnlySpan<Vector3> normals, out CollapseCandidateScore[] scores)
{
    scores = [];
    int candidateCount = candidates.Length;
    if (candidateCount <= 0 || positions.IsEmpty)
        return false;

    var shaderBytecode = GetMeshShaderBytecode("mesh-collapse-candidate-score-v2");
    using var computeShader = state.Device.CreateComputeShader(shaderBytecode);

    using var candidateBuffer = state.Device.CreateBuffer(candidates,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        structureByteStride: (uint)Marshal.SizeOf<GpuCollapseCandidate>());
    using var candidateSrv = CreateStructuredBufferSrv(state.Device, candidateBuffer, candidateCount);

    using var positionBuffer = state.Device.CreateBuffer(positions,
        BindFlags.ShaderResource,
        ResourceUsage.Default,
        CpuAccessFlags.None,
        ResourceOptionFlags.BufferStructured,
        structureByteStride: (uint)Marshal.SizeOf<Vector3>());
    using var positionSrv = CreateStructuredBufferSrv(state.Device, positionBuffer, positions.Length);

    ID3D11Buffer? normalBuffer = null;
    ID3D11ShaderResourceView? normalSrv = null;
    try
    {
        if (!normals.IsEmpty)
        {
            var expandedNormals = new Vector4[Math.Max(normals.Length, positions.Length)];
            int limit = Math.Min(expandedNormals.Length, normals.Length);
            for (int i = 0; i < limit; i++)
                expandedNormals[i] = new Vector4(normals[i], 0f);

            normalBuffer = state.Device.CreateBuffer(expandedNormals,
                BindFlags.ShaderResource,
                ResourceUsage.Default,
                CpuAccessFlags.None,
                ResourceOptionFlags.BufferStructured,
                structureByteStride: (uint)Marshal.SizeOf<Vector4>());
            normalSrv = CreateStructuredBufferSrv(state.Device, normalBuffer, expandedNormals.Length);
        }

        uint stride = (uint)Marshal.SizeOf<CollapseCandidateScoreRaw>();
        uint outputByteWidth = checked((uint)(candidateCount * (int)stride));
        using var outputBuffer = state.Device.CreateBuffer(
            outputByteWidth,
            BindFlags.UnorderedAccess | BindFlags.ShaderResource,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.BufferStructured,
            stride);
        using var outputUav = CreateStructuredBufferUav(state.Device, outputBuffer, candidateCount);
        using var stagingBuffer = state.Device.CreateBuffer(
            outputByteWidth,
            BindFlags.None,
            ResourceUsage.Staging,
            CpuAccessFlags.Read,
            ResourceOptionFlags.None,
            0);

        using var constants = CreateConstantBufferGeneric(state.Device, new CollapseCandidateConstants((uint)candidateCount, (uint)positions.Length, normals.IsEmpty ? 0u : 1u, 0u));

        state.Context.CSSetShader(computeShader);
        state.Context.CSSetConstantBuffer(0, constants);
        state.Context.CSSetShaderResource(0, candidateSrv);
        state.Context.CSSetShaderResource(1, positionSrv);
        state.Context.CSSetShaderResource(2, normalSrv);
        state.Context.CSSetUnorderedAccessView(0, outputUav);
        state.Context.Dispatch((uint)((candidateCount + 63) / 64), 1, 1);
        state.Context.Flush();
        state.Context.CopyResource(stagingBuffer, outputBuffer);

        var mapped = state.Context.Map(stagingBuffer, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
        try
        {
            var result = new CollapseCandidateScore[candidateCount];
            unsafe
            {
                var rawSpan = new ReadOnlySpan<CollapseCandidateScoreRaw>((void*)mapped.DataPointer, candidateCount);
                for (int i = 0; i < rawSpan.Length; i++)
                    result[i] = new CollapseCandidateScore(rawSpan[i].Score, rawSpan[i].Eligible != 0f);
            }

            scores = result;
            return true;
        }
        finally
        {
            state.Context.Unmap(stagingBuffer, 0);
            state.Context.CSSetShader(null);
            state.Context.CSSetConstantBuffer(0, null);
            state.Context.CSSetShaderResource(0, null);
            state.Context.CSSetShaderResource(1, null);
            state.Context.CSSetShaderResource(2, null);
            state.Context.CSSetUnorderedAccessView(0, null);
        }
    }
    finally
    {
        normalSrv?.Dispose();
        normalBuffer?.Dispose();
    }
}

    private static ID3D11Buffer CreateConstantBufferGeneric<T>(ID3D11Device device, T constants)
        where T : unmanaged
    {
        var description = new BufferDescription(
            (uint)Marshal.SizeOf<T>(),
            BindFlags.ConstantBuffer,
            ResourceUsage.Default,
            CpuAccessFlags.None,
            ResourceOptionFlags.None,
            0);

        unsafe
        {
            var initialData = new SubresourceData((IntPtr)(&constants), 0, 0);
            return device.CreateBuffer(description, initialData);
        }
    }

    private static byte[] CompileCollapseCandidateScoreShader()
    {
        var hresult = TryCompile(CollapseCandidateScoreShaderSource, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 collapse candidate score shader compilation failed with HRESULT 0x{hresult:X8}."
                : errors);

        return bytecode;
    }

    private static byte[] CompileBodyProximityShader()
    {
        var hresult = TryCompile(BodyProximityShaderSource, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 body proximity shader compilation failed with HRESULT 0x{hresult:X8}."
                : errors);

        return bytecode;
    }

    private static byte[] CompileAttributeProtectionShader()
    {
        var hresult = TryCompile(AttributeProtectionShaderSource, out var bytecode, out var errors);
        if (hresult < 0 || bytecode == null)
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
                ? $"D3D11 attribute protection shader compilation failed with HRESULT 0x{hresult:X8}."
                : errors);

        return bytecode;
    }


private static byte[] CompileProtectedExpandShader()
{
    var hresult = TryCompile(ProtectedExpandShaderSource, out var bytecode, out var errors);
    if (hresult < 0 || bytecode == null)
        throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
            ? $"D3D11 protected expand shader compilation failed with HRESULT 0x{hresult:X8}."
            : errors);

    return bytecode;
}

private static byte[] CompileAverageEdgeLengthShader()
{
    var hresult = TryCompile(AverageEdgeLengthShaderSource, out var bytecode, out var errors);
    if (hresult < 0 || bytecode == null)
        throw new InvalidOperationException(string.IsNullOrWhiteSpace(errors)
            ? $"D3D11 average edge shader compilation failed with HRESULT 0x{hresult:X8}."
            : errors);

    return bytecode;
}

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct GpuBodyTriangle(Vector3 A, Vector3 B, Vector3 C, Vector3 Min, Vector3 Max);

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct GpuSkinningVertex(uint Bone0, uint Bone1, uint Bone2, uint Bone3, Vector4 Weights);

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct AttributePair(uint Left, uint Right);

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct GpuCollapseCandidate(uint KeepVertex, uint DropVertex, float ReferenceEdgeLength, float LocalMedianEdgeLength, uint BoundaryPressure, uint Flags, float BoundaryPenaltyScale, float NearBodyPenalty);

    public readonly record struct CollapseCandidateScore(float Score, bool Eligible);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct CollapseCandidateScoreRaw(float Score, float Eligible);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct CollapseCandidateConstants(uint CandidateCount, uint PositionCount, uint HasNormals, uint Padding0);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct AverageEdgeMetric(float EdgeSum, float EdgeCount);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct ProtectedExpandConstants(uint VertexCount, uint TriangleCount, uint Padding0, uint Padding1);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct AverageEdgeLengthConstants(uint TriangleCount, uint PositionCount, uint Padding0, uint Padding1);

[StructLayout(LayoutKind.Sequential)]
    private readonly record struct BodyProximityConstants(uint QueryCount, uint TriangleCount, float ThresholdSq, uint Padding);

    [StructLayout(LayoutKind.Sequential)]
    private readonly record struct AttributeProtectionConstants(uint PairCount, uint VertexCount, uint HasUvs, uint HasNormals, float UvThresholdSq, float BoneOverlapThreshold, float NormalThresholdCos, uint HasSkinning);
}
