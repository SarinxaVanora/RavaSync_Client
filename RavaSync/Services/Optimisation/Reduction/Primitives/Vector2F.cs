using System;

namespace RavaSync.Services.Optimisation.Reduction
{
    public readonly struct Vector2F : IEquatable<Vector2F>, IInterpolable<Vector2F>
    {
        public readonly float x;
        public readonly float y;

        
        public float this[int index]
        {
            get
            {
                switch (index)
                {
                    case 0: return x;
                    case 1: return y;
                    default:
                        throw new IndexOutOfRangeException("Invalid Vector2 index!");
                }
            }
        }

        
        public Vector2F(float x, float y) { this.x = x; this.y = y; }

        
        public static Vector2F Lerp(Vector2F a, Vector2F b, float t)
        {
            t = MathF.Clamp(t, 0, 1);
            return new Vector2F(
                a.x + (b.x - a.x) * t,
                a.y + (b.y - a.y) * t
            );
        }

        
        public static Vector2F LerpUnclamped(Vector2F a, Vector2F b, float t)
        {
            return new Vector2F(
                a.x + (b.x - a.x) * t,
                a.y + (b.y - a.y) * t
            );
        }

        
        public static Vector2F MoveTowards(Vector2F current, Vector2F target, float maxDistanceDelta)
        {
            
            float toVector_x = target.x - current.x;
            float toVector_y = target.y - current.y;

            float sqDist = toVector_x * toVector_x + toVector_y * toVector_y;

            if (sqDist == 0 || (maxDistanceDelta >= 0 && sqDist <= maxDistanceDelta * maxDistanceDelta))
            {
                return target;
            }

            float dist = MathF.Sqrt(sqDist);

            return new Vector2F(current.x + toVector_x / dist * maxDistanceDelta,
                current.y + toVector_y / dist * maxDistanceDelta);
        }

        
        public static Vector2F Scale(Vector2F a, Vector2F b) { return new Vector2F(a.x * b.x, a.y * b.y); }

        public static Vector2F Normalize(in Vector2F value)
        {
            float mag = Magnitude(in value);
            if (mag > K_EPSILON)
            {
                return value / mag;
            }
            else
            {
                return Zero;
            }
        }

        public Vector2F Normalize() => Normalize(in this);

        public static float SqrMagnitude(in Vector2F a) => a.x * a.x + a.y * a.y;

        
        
        
        public float SqrMagnitude() => SqrMagnitude(in this);

        public static float Magnitude(in Vector2F vector) => (float)Math.Sqrt(SqrMagnitude(in vector));

        public float Magnitude() => Magnitude(this);

        
        public override int GetHashCode()
        {
            return x.GetHashCode() ^ (y.GetHashCode() << 2);
        }

        
        public override bool Equals(object other)
        {
            if (!(other is Vector2F))
            {
                return false;
            }

            return Equals((Vector2F)other);
        }


        public bool Equals(Vector2F other)
        {
            return Vector2FComparer.Default.Equals(this, other);
            
        }

        public static Vector2F Reflect(Vector2F inDirection, Vector2F inNormal)
        {
            float factor = -2F * Dot(inNormal, inDirection);
            return new Vector2F(factor * inNormal.x + inDirection.x, factor * inNormal.y + inDirection.y);
        }

        public static Vector2F Perpendicular(Vector2F inDirection)
        {
            return new Vector2F(-inDirection.y, inDirection.x);
        }

        
        
        
        
        
        
        public static float Dot(Vector2F lhs, Vector2F rhs) { return lhs.x * rhs.x + lhs.y * rhs.y; }

        
        
        
        
        
        
        public static float AngleRadians(Vector2F from, Vector2F to)
        {
            
            float denominator = MathF.Sqrt(from.SqrMagnitude() * to.SqrMagnitude());
            if (denominator < K_EPSILON_NORMAL_SQRT)
            {
                return 0F;
            }

            float dot = MathF.Clamp(Dot(from, to) / denominator, -1F, 1F);
            return MathF.Acos(dot);
        }

        public static float AngleDegrees(Vector2F from, Vector2F to)
        {
            return AngleRadians(from, to) / MathF.PI * 180f;
        }

        
        
        
        
        
        
        public static float SignedAngle(Vector2F from, Vector2F to)
        {
            float unsigned_angle = AngleDegrees(from, to);
            float sign = MathF.Sign(from.x * to.y - from.y * to.x);
            return unsigned_angle * sign;
        }

        
        
        
        
        
        
        public static float Distance(Vector2F a, Vector2F b)
        {
            float diff_x = a.x - b.x;
            float diff_y = a.y - b.y;
            return MathF.Sqrt(diff_x * diff_x + diff_y * diff_y);
        }

        
        
        
        
        
        
        public static Vector2F ClampMagnitude(Vector2F vector, float maxLength)
        {
            float sqrMagnitude = vector.SqrMagnitude();
            if (sqrMagnitude > maxLength * maxLength)
            {
                float mag = MathF.Sqrt(sqrMagnitude);

                
                
                
                float normalized_x = vector.x / mag;
                float normalized_y = vector.y / mag;
                return new Vector2F(normalized_x * maxLength,
                    normalized_y * maxLength);
            }
            return vector;
        }

        
        
        
        
        
        
        public static Vector2F Min(Vector2F lhs, Vector2F rhs) { return new Vector2F(MathF.Min(lhs.x, rhs.x), MathF.Min(lhs.y, rhs.y)); }

        
        
        
        
        
        
        public static Vector2F Max(Vector2F lhs, Vector2F rhs) { return new Vector2F(MathF.Max(lhs.x, rhs.x), MathF.Max(lhs.y, rhs.y)); }

        public Vector2F Interpolate(Vector2F other, double ratio) => this * ratio + other * (1 - ratio);

        
        
        
        
        
        
        public static Vector2F operator +(Vector2F a, Vector2F b) { return new Vector2F(a.x + b.x, a.y + b.y); }

        
        
        
        
        
        
        public static Vector2F operator -(Vector2F a, Vector2F b) { return new Vector2F(a.x - b.x, a.y - b.y); }

        
        
        
        
        
        
        public static Vector2F operator *(Vector2F a, Vector2F b) { return new Vector2F(a.x * b.x, a.y * b.y); }

        
        
        
        
        
        
        public static Vector2F operator /(Vector2F a, Vector2F b) { return new Vector2F(a.x / b.x, a.y / b.y); }

        
        
        
        
        
        public static Vector2F operator -(Vector2F a) { return new Vector2F(-a.x, -a.y); }

        
        
        
        
        
        
        public static Vector2F operator *(Vector2F a, float d) { return new Vector2F(a.x * d, a.y * d); }

        public static Vector2 operator *(Vector2F a, double d) { return new Vector2(a.x * d, a.y * d); }

        
        
        
        
        
        
        public static Vector2F operator *(float d, Vector2F a) { return new Vector2F(a.x * d, a.y * d); }

        public static Vector2 operator *(double d, Vector2F a) { return new Vector2(a.x * d, a.y * d); }

        
        
        
        
        
        
        public static Vector2F operator /(Vector2F a, float d) { return new Vector2F(a.x / d, a.y / d); }

        
        
        
        
        
        
        public static bool operator ==(Vector2F lhs, Vector2F rhs)
        {
            
            float diff_x = lhs.x - rhs.x;
            float diff_y = lhs.y - rhs.y;
            return (diff_x * diff_x + diff_y * diff_y) < K_EPSILON * K_EPSILON;
        }

        
        
        
        
        
        
        public static bool operator !=(Vector2F lhs, Vector2F rhs)
        {
            
            return !(lhs == rhs);
        }

        
        
        
        
        public static implicit operator Vector2F(Vector3F v)
        {
            return new Vector2F(v.x, v.y);
        }

        
        
        
        
        public static implicit operator Vector3(Vector2F v)
        {
            return new Vector3(v.x, v.y, 0);
        }

        public static readonly Vector2F zeroVector = new Vector2F(0F, 0F);
        public static readonly Vector2F oneVector = new Vector2F(1F, 1F);
        public static readonly Vector2F upVector = new Vector2F(0F, 1F);
        public static readonly Vector2F downVector = new Vector2F(0F, -1F);
        public static readonly Vector2F leftVector = new Vector2F(-1F, 0F);
        public static readonly Vector2F rightVector = new Vector2F(1F, 0F);
        public static readonly Vector2F positiveInfinityVector = new Vector2F(float.PositiveInfinity, float.PositiveInfinity);
        public static readonly Vector2F negativeInfinityVector = new Vector2F(float.NegativeInfinity, float.NegativeInfinity);

        public static Vector2F Zero => zeroVector;

        public static Vector2F One => oneVector;

        public static Vector2F Up => upVector;

        public static Vector2F Down => downVector;

        public static Vector2F Left => leftVector;

        public static Vector2F Right => rightVector;

        public static Vector2F PositiveInfinity => positiveInfinityVector;

        public static Vector2F NegativeInfinity => negativeInfinityVector;

        public const float K_EPSILON = 0.00001F;

        public const float K_EPSILON_NORMAL_SQRT = 1e-15f;
    }
}