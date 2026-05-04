using System;
using System.Runtime.InteropServices;

namespace RavaSync.Services.Optimisation.Reduction
{
    [Flags]
    public enum FfxivAttributeFlags : uint
    {
        None = 0,
        Normal = 1u << 0,
        Tangent1 = 1u << 1,
        Tangent2 = 1u << 2,
        Color = 1u << 3,
        BoneWeights = 1u << 4,
        PositionW = 1u << 5,
        NormalW = 1u << 6,
        Uv0 = 1u << 7,
        Uv1 = 1u << 8,
        Uv2 = 1u << 9,
        Uv3 = 1u << 10,
        Color1 = 1u << 11,
    }

    [StructLayout(LayoutKind.Sequential)]
    public readonly struct FfxivVertexAttribute : IEquatable<FfxivVertexAttribute>, IInterpolable<FfxivVertexAttribute>
    {
        public readonly Vector3F normal;
        public readonly Vector4F tangent1;
        public readonly Vector4F tangent2;
        public readonly Vector2F uv0;
        public readonly Vector2F uv1;
        public readonly Vector2F uv2;
        public readonly Vector2F uv3;
        public readonly Vector4F color;
        public readonly Vector4F color1;
        public readonly BoneWeight boneWeight;
        public readonly float positionW;
        public readonly float normalW;
        public readonly FfxivAttributeFlags flags;

        public FfxivVertexAttribute(
            FfxivAttributeFlags flags,
            Vector3F normal,
            Vector4F tangent1,
            Vector4F tangent2,
            Vector2F uv0,
            Vector2F uv1,
            Vector2F uv2,
            Vector2F uv3,
            Vector4F color,
            Vector4F color1,
            BoneWeight boneWeight,
            float positionW,
            float normalW)
        {
            this.flags = flags;
            this.normal = normal;
            this.tangent1 = tangent1;
            this.tangent2 = tangent2;
            this.uv0 = uv0;
            this.uv1 = uv1;
            this.uv2 = uv2;
            this.uv3 = uv3;
            this.color = color;
            this.color1 = color1;
            this.boneWeight = boneWeight;
            this.positionW = positionW;
            this.normalW = normalW;
        }

        public FfxivVertexAttribute Interpolate(FfxivVertexAttribute other, double ratio)
        {
            var t = (float)ratio;
            var inv = 1f - t;
            var combinedFlags = flags | other.flags;

            var normal = (combinedFlags & FfxivAttributeFlags.Normal) != 0
                ? NormalizeVector3(new Vector3F(
                    (this.normal.x * inv) + (other.normal.x * t),
                    (this.normal.y * inv) + (other.normal.y * t),
                    (this.normal.z * inv) + (other.normal.z * t)))
                : default;

            var tangent1 = (combinedFlags & FfxivAttributeFlags.Tangent1) != 0
                ? BlendTangent(this.tangent1, other.tangent1, t)
                : default;

            var tangent2 = (combinedFlags & FfxivAttributeFlags.Tangent2) != 0
                ? BlendTangent(this.tangent2, other.tangent2, t)
                : default;

            var uv0 = (combinedFlags & FfxivAttributeFlags.Uv0) != 0
                ? Vector2F.LerpUnclamped(this.uv0, other.uv0, t)
                : default;

            var uv1 = (combinedFlags & FfxivAttributeFlags.Uv1) != 0
                ? Vector2F.LerpUnclamped(this.uv1, other.uv1, t)
                : default;

            var uv2 = (combinedFlags & FfxivAttributeFlags.Uv2) != 0
                ? Vector2F.LerpUnclamped(this.uv2, other.uv2, t)
                : default;

            var uv3 = (combinedFlags & FfxivAttributeFlags.Uv3) != 0
                ? Vector2F.LerpUnclamped(this.uv3, other.uv3, t)
                : default;

            var color = (combinedFlags & FfxivAttributeFlags.Color) != 0
                ? new Vector4F(
                    (this.color.x * inv) + (other.color.x * t),
                    (this.color.y * inv) + (other.color.y * t),
                    (this.color.z * inv) + (other.color.z * t),
                    (this.color.w * inv) + (other.color.w * t))
                : default;

            var color1 = (combinedFlags & FfxivAttributeFlags.Color1) != 0
                ? new Vector4F(
                    (this.color1.x * inv) + (other.color1.x * t),
                    (this.color1.y * inv) + (other.color1.y * t),
                    (this.color1.z * inv) + (other.color1.z * t),
                    (this.color1.w * inv) + (other.color1.w * t))
                : default;

            var boneWeight = (combinedFlags & FfxivAttributeFlags.BoneWeights) != 0
                ? BlendBoneWeights(this.boneWeight, other.boneWeight, t)
                : default;

            var positionW = (combinedFlags & FfxivAttributeFlags.PositionW) != 0
                ? (this.positionW * inv) + (other.positionW * t)
                : 0f;

            var normalW = (combinedFlags & FfxivAttributeFlags.NormalW) != 0
                ? (this.normalW * inv) + (other.normalW * t)
                : 0f;

            return new FfxivVertexAttribute(
                combinedFlags,
                normal,
                tangent1,
                tangent2,
                uv0,
                uv1,
                uv2,
                uv3,
                color,
                color1,
                boneWeight,
                positionW,
                normalW);
        }

        public bool Equals(FfxivVertexAttribute other)
        {
            if (flags != other.flags)
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Normal) != 0 && !normal.Equals(other.normal))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Tangent1) != 0 && !tangent1.Equals(other.tangent1))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Tangent2) != 0 && !tangent2.Equals(other.tangent2))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Uv0) != 0 && !uv0.Equals(other.uv0))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Uv1) != 0 && !uv1.Equals(other.uv1))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Uv2) != 0 && !uv2.Equals(other.uv2))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Uv3) != 0 && !uv3.Equals(other.uv3))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Color) != 0 && !color.Equals(other.color))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.Color1) != 0 && !color1.Equals(other.color1))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.BoneWeights) != 0 && !boneWeight.Equals(other.boneWeight))
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.PositionW) != 0 && positionW != other.positionW)
            {
                return false;
            }

            if ((flags & FfxivAttributeFlags.NormalW) != 0 && normalW != other.normalW)
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object? obj)
            => obj is FfxivVertexAttribute other && Equals(other);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(normal);
            hash.Add(tangent1);
            hash.Add(tangent2);
            hash.Add(uv0);
            hash.Add(uv1);
            hash.Add(uv2);
            hash.Add(uv3);
            hash.Add(color);
            hash.Add(color1);
            hash.Add(boneWeight);
            hash.Add(positionW);
            hash.Add(normalW);
            hash.Add(flags);
            return hash.ToHashCode();
        }

        private static Vector3F NormalizeVector3(in Vector3F value)
        {
            var length = Vector3F.Magnitude(value);
            return length > 0f ? value / length : value;
        }

        private static Vector4F BlendTangent(in Vector4F a, in Vector4F b, float t)
        {
            var inv = 1f - t;
            var blended = new Vector3F(
                (a.x * inv) + (b.x * t),
                (a.y * inv) + (b.y * t),
                (a.z * inv) + (b.z * t));
            blended = NormalizeVector3(blended);

            var w = t >= 0.5f ? b.w : a.w;
            if (w != 0f)
            {
                w = w >= 0f ? 1f : -1f;
            }

            return new Vector4F(blended.x, blended.y, blended.z, w);
        }

        private static BoneWeight BlendBoneWeights(in BoneWeight a, in BoneWeight b, float ratio)
        {
            Span<int> indices = stackalloc int[8];
            Span<float> weights = stackalloc float[8];
            var count = 0;

            static void AddWeight(Span<int> indices, Span<float> weights, ref int count, int index, float weight)
            {
                if (weight <= 0f)
                {
                    return;
                }

                for (var i = 0; i < count; i++)
                {
                    if (indices[i] == index)
                    {
                        weights[i] += weight;
                        return;
                    }
                }

                if (count < indices.Length)
                {
                    indices[count] = index;
                    weights[count] = weight;
                    count++;
                }
            }

            var inv = 1f - ratio;
            var sumA = a.weight0 + a.weight1 + a.weight2 + a.weight3;
            var sumB = b.weight0 + b.weight1 + b.weight2 + b.weight3;
            var targetSum = (sumA * inv) + (sumB * ratio);
            AddWeight(indices, weights, ref count, a.index0, a.weight0 * inv);
            AddWeight(indices, weights, ref count, a.index1, a.weight1 * inv);
            AddWeight(indices, weights, ref count, a.index2, a.weight2 * inv);
            AddWeight(indices, weights, ref count, a.index3, a.weight3 * inv);
            AddWeight(indices, weights, ref count, b.index0, b.weight0 * ratio);
            AddWeight(indices, weights, ref count, b.index1, b.weight1 * ratio);
            AddWeight(indices, weights, ref count, b.index2, b.weight2 * ratio);
            AddWeight(indices, weights, ref count, b.index3, b.weight3 * ratio);

            if (count == 0)
            {
                return a;
            }

            Span<int> topIndices = stackalloc int[4];
            Span<float> topWeights = stackalloc float[4];
            for (var i = 0; i < 4; i++)
            {
                topIndices[i] = -1;
                topWeights[i] = 0f;
            }

            for (var i = 0; i < count; i++)
            {
                var weight = weights[i];
                var index = indices[i];
                for (var slot = 0; slot < 4; slot++)
                {
                    if (weight > topWeights[slot])
                    {
                        for (var shift = 3; shift > slot; shift--)
                        {
                            topWeights[shift] = topWeights[shift - 1];
                            topIndices[shift] = topIndices[shift - 1];
                        }

                        topWeights[slot] = weight;
                        topIndices[slot] = index;
                        break;
                    }
                }
            }

            var sum = topWeights[0] + topWeights[1] + topWeights[2] + topWeights[3];
            if (sum > 0f)
            {
                var scale = targetSum > 0f ? targetSum / sum : 0f;
                for (var i = 0; i < 4; i++)
                {
                    topWeights[i] *= scale;
                }
            }

            return new BoneWeight(
                topIndices[0] < 0 ? 0 : topIndices[0],
                topIndices[1] < 0 ? 0 : topIndices[1],
                topIndices[2] < 0 ? 0 : topIndices[2],
                topIndices[3] < 0 ? 0 : topIndices[3],
                topWeights[0],
                topWeights[1],
                topWeights[2],
                topWeights[3]);
        }
    }
}