using System;

namespace RavaSync.Services.Optimisation.Reduction
{
    public readonly struct Vector4F : IEquatable<Vector4F>, IInterpolable<Vector4F>
    {
        public readonly float x;
        public readonly float y;
        public readonly float z;
        public readonly float w;

        public Vector4F(float x, float y, float z, float w)
        {
            this.x = x;
            this.y = y;
            this.z = z;
            this.w = w;
        }

        public float this[int index]
        {
            get
            {
                switch (index)
                {
                    case 0: return x;
                    case 1: return y;
                    case 2: return z;
                    case 3: return w;
                    default:
                        throw new IndexOutOfRangeException("Invalid Vector4F index!");
                }
            }
        }

        public override int GetHashCode()
        {
            return Vector4FComparer.Default.GetHashCode(this);
        }

        public override bool Equals(object other)
        {
            if (!(other is Vector4F))
            {
                return false;
            }

            return Equals((Vector4F)other);
        }

        public bool Equals(Vector4F other)
        {
            return Vector4FComparer.Default.Equals(this, other);
        }

        public static Vector4F operator +(in Vector4F a, in Vector4F b)
            => new(a.x + b.x, a.y + b.y, a.z + b.z, a.w + b.w);

        public static Vector4F operator -(in Vector4F a, in Vector4F b)
            => new(a.x - b.x, a.y - b.y, a.z - b.z, a.w - b.w);

        public static Vector4F operator *(in Vector4F a, float d)
            => new(a.x * d, a.y * d, a.z * d, a.w * d);

        public static Vector4F operator *(float d, in Vector4F a)
            => new(a.x * d, a.y * d, a.z * d, a.w * d);

        public static Vector4F operator /(in Vector4F a, float d)
            => new(MathUtils.DivideSafe(a.x, d), MathUtils.DivideSafe(a.y, d), MathUtils.DivideSafe(a.z, d), MathUtils.DivideSafe(a.w, d));

        public static bool operator ==(in Vector4F lhs, in Vector4F rhs)
            => Vector4FComparer.Default.Equals(lhs, rhs);

        public static bool operator !=(in Vector4F lhs, in Vector4F rhs)
            => !Vector4FComparer.Default.Equals(lhs, rhs);

        public static float Dot(in Vector4F lhs, in Vector4F rhs)
            => (lhs.x * rhs.x) + (lhs.y * rhs.y) + (lhs.z * rhs.z) + (lhs.w * rhs.w);

        public Vector4F Interpolate(Vector4F other, double ratio)
        {
            var t = (float)ratio;
            var inv = 1f - t;
            return new Vector4F(
                (x * inv) + (other.x * t),
                (y * inv) + (other.y * t),
                (z * inv) + (other.z * t),
                (w * inv) + (other.w * t));
        }
    }
}