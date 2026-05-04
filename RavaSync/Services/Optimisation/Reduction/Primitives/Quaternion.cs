using System;
using System.Runtime.InteropServices;

namespace RavaSync.Services.Optimisation.Reduction
{
    [StructLayout(LayoutKind.Sequential)]
    public partial struct Quaternion : IEquatable<Quaternion>
    {
        private const double radToDeg = 180.0 / Math.PI;
        private const double degToRad = Math.PI / 180.0;

        public const double kEpsilon = 1E-20; 

        public Vector3 xyz
        {
            set
            {
                x = value.x;
                y = value.y;
                z = value.z;
            }
            get => new Vector3(x, y, z);
        }

        public double x;

        public double y;

        public double z;

        public double w;

        public double this[int index]
        {
            get
            {
                switch (index)
                {
                    case 0:
                        return x;
                    case 1:
                        return y;
                    case 2:
                        return z;
                    case 3:
                        return w;
                    default:
                        throw new IndexOutOfRangeException("Invalid Quaternion index: " + index + ", can use only 0,1,2,3");
                }
            }
            set
            {
                switch (index)
                {
                    case 0:
                        x = value;
                        break;
                    case 1:
                        y = value;
                        break;
                    case 2:
                        z = value;
                        break;
                    case 3:
                        w = value;
                        break;
                    default:
                        throw new IndexOutOfRangeException("Invalid Quaternion index: " + index + ", can use only 0,1,2,3");
                }
            }
        }
        
        
        
        public static Quaternion identity => new Quaternion(0, 0, 0, 1);

        
        
        
        
        public double Length => (double)System.Math.Sqrt(x * x + y * y + z * z + w * w);

        
        
        
        public double LengthSquared => x * x + y * y + z * z + w * w;

        
        
        
        
        
        
        
        public Quaternion(double x, double y, double z, double w)
        {
            this.x = x;
            this.y = y;
            this.z = z;
            this.w = w;
        }

        
        
        
        
        
        public Quaternion(Vector3 v, double w)
        {
            x = v.x;
            y = v.y;
            z = v.z;
            this.w = w;
        }

        
        
        
        
        
        
        
        public void Set(double new_x, double new_y, double new_z, double new_w)
        {
            x = new_x;
            y = new_y;
            z = new_z;
            w = new_w;
        }

        
        
        
        public static Quaternion Normalize(Quaternion q)
        {
            double mag = Math.Sqrt(Dot(q, q));

            if (mag < kEpsilon)
            {
                return Quaternion.identity;
            }

            return new Quaternion(q.x / mag, q.y / mag, q.z / mag, q.w / mag);
        }

        
        
        
        
        
        public void Normalize()
        {
            this = Normalize(this);
        }

        
        
        
        
        
        public static double Dot(Quaternion a, Quaternion b)
        {
            return a.x * b.x + a.y * b.y + a.z * b.z + a.w * b.w;
        }

        
        
        
        
        
        public static Quaternion AngleAxis(double angle, Vector3 axis)
        {
            return Quaternion.AngleAxis(angle, ref axis);
        }

        private static Quaternion AngleAxis(double degress, ref Vector3 axis)
        {
            if (axis.LengthSquared == 0.0)
            {
                return identity;
            }

            Quaternion result = identity;
            double radians = degress * degToRad;
            radians *= 0.5;
            axis = axis.Normalized;
            axis = axis * Math.Sin(radians);
            result.x = axis.x;
            result.y = axis.y;
            result.z = axis.z;
            result.w = Math.Cos(radians);

            return Normalize(result);
        }

        public void ToAngleAxis(out double angle, out Vector3 axis)
        {
            Quaternion.ToAxisAngleRad(this, out axis, out angle);
            angle *= radToDeg;
        }

        
        
        
        
        
        public static Quaternion FromToRotation(Vector3 fromDirection, Vector3 toDirection)
        {
            return RotateTowards(LookRotation(fromDirection), LookRotation(toDirection), double.MaxValue);
        }

        
        
        
        
        
        public void SetFromToRotation(Vector3 fromDirection, Vector3 toDirection)
        {
            this = Quaternion.FromToRotation(fromDirection, toDirection);
        }

        
        
        
        
        
        public static Quaternion LookRotation(Vector3 forward, Vector3 upwards)
        {
            return Quaternion.LookRotation(ref forward, ref upwards);
        }

        public static Quaternion LookRotation(Vector3 forward)
        {
            Vector3 up = new Vector3(1, 0, 0);
            return Quaternion.LookRotation(ref forward, ref up);
        }

        private static Quaternion LookRotation(ref Vector3 forward, ref Vector3 up)
        {
            forward = Vector3.Normalize(forward);
            Vector3 right = Vector3.Normalize(Vector3.Cross(up, forward));
            up = Vector3.Cross(forward, right);
            double m00 = right.x;
            double m01 = right.y;
            double m02 = right.z;
            double m10 = up.x;
            double m11 = up.y;
            double m12 = up.z;
            double m20 = forward.x;
            double m21 = forward.y;
            double m22 = forward.z;

            double num8 = (m00 + m11) + m22;
            Quaternion quaternion = new Quaternion();
            if (num8 > 0)
            {
                double num = Math.Sqrt(num8 + 1);
                quaternion.w = num * 0.5;
                num = 0.5 / num;
                quaternion.x = (m12 - m21) * num;
                quaternion.y = (m20 - m02) * num;
                quaternion.z = (m01 - m10) * num;
                return quaternion;
            }
            if ((m00 >= m11) && (m00 >= m22))
            {
                double num7 = Math.Sqrt(((1 + m00) - m11) - m22);
                double num4 = 0.5 / num7;
                quaternion.x = 0.5 * num7;
                quaternion.y = (m01 + m10) * num4;
                quaternion.z = (m02 + m20) * num4;
                quaternion.w = (m12 - m21) * num4;
                return quaternion;
            }
            if (m11 > m22)
            {
                double num6 = Math.Sqrt(((1 + m11) - m00) - m22);
                double num3 = 0.5 / num6;
                quaternion.x = (m10 + m01) * num3;
                quaternion.y = 0.5 * num6;
                quaternion.z = (m21 + m12) * num3;
                quaternion.w = (m20 - m02) * num3;
                return quaternion;
            }
            double num5 = Math.Sqrt(((1 + m22) - m00) - m11);
            double num2 = 0.5 / num5;
            quaternion.x = (m20 + m02) * num2;
            quaternion.y = (m21 + m12) * num2;
            quaternion.z = 0.5 * num5;
            quaternion.w = (m01 - m10) * num2;
            return quaternion;
        }

        public void SetLookRotation(Vector3 view)
        {
            Vector3 up = new Vector3(1, 0, 0);
            SetLookRotation(view, up);
        }

        
        
        
        
        
        public void SetLookRotation(Vector3 view, Vector3 up)
        {
            this = Quaternion.LookRotation(view, up);
        }

        
        
        
        
        
        
        public static Quaternion Slerp(Quaternion a, Quaternion b, double t)
        {
            return Quaternion.Slerp(ref a, ref b, t);
        }

        private static Quaternion Slerp(ref Quaternion a, ref Quaternion b, double t)
        {
            if (t > 1)
            {
                t = 1;
            }

            if (t < 0)
            {
                t = 0;
            }

            return SlerpUnclamped(ref a, ref b, t);
        }

        
        
        
        
        
        
        public static Quaternion SlerpUnclamped(Quaternion a, Quaternion b, double t)
        {

            return Quaternion.SlerpUnclamped(ref a, ref b, t);
        }
        private static Quaternion SlerpUnclamped(ref Quaternion a, ref Quaternion b, double t)
        {
            
            if (a.LengthSquared == 0.0)
            {
                if (b.LengthSquared == 0.0)
                {
                    return identity;
                }
                return b;
            }
            else if (b.LengthSquared == 0.0)
            {
                return a;
            }

            double cosHalfAngle = a.w * b.w + Vector3.Dot(a.xyz, b.xyz);

            if (cosHalfAngle >= 1.0 || cosHalfAngle <= -1.0)
            {
                
                return a;
            }
            else if (cosHalfAngle < 0.0)
            {
                b.xyz = -b.xyz;
                b.w = -b.w;
                cosHalfAngle = -cosHalfAngle;
            }

            double blendA;
            double blendB;
            if (cosHalfAngle < 0.99)
            {
                
                double halfAngle = Math.Acos(cosHalfAngle);
                double sinHalfAngle = Math.Sin(halfAngle);
                double oneOverSinHalfAngle = 1.0 / sinHalfAngle;
                blendA = Math.Sin(halfAngle * (1.0 - t)) * oneOverSinHalfAngle;
                blendB = Math.Sin(halfAngle * t) * oneOverSinHalfAngle;
            }
            else
            {
                
                blendA = 1.0f - t;
                blendB = t;
            }

            Quaternion result = new Quaternion(blendA * a.xyz + blendB * b.xyz, blendA * a.w + blendB * b.w);
            if (result.LengthSquared > 0.0)
            {
                return Normalize(result);
            }
            else
            {
                return identity;
            }
        }

        
        
        
        
        
        
        public static Quaternion Lerp(Quaternion a, Quaternion b, double t)
        {
            if (t > 1)
            {
                t = 1;
            }

            if (t < 0)
            {
                t = 0;
            }

            return Slerp(ref a, ref b, t); 
        }

        
        
        
        
        
        
        public static Quaternion LerpUnclamped(Quaternion a, Quaternion b, double t)
        {
            return Slerp(ref a, ref b, t);
        }

        
        
        
        
        
        
        public static Quaternion RotateTowards(Quaternion from, Quaternion to, double maxDegreesDelta)
        {
            double num = Quaternion.Angle(from, to);
            if (num == 0)
            {
                return to;
            }
            double t = Math.Min(1, maxDegreesDelta / num);
            return Quaternion.SlerpUnclamped(from, to, t);
        }

        
        
        
        
        public static Quaternion Inverse(Quaternion rotation)
        {
            double lengthSq = rotation.LengthSquared;
            if (lengthSq != 0.0)
            {
                double i = 1.0 / lengthSq;
                return new Quaternion(rotation.xyz * -i, rotation.w * i);
            }
            return rotation;
        }

        
        
        
        
        public override string ToString()
        {
            return $"{x}, {y}, {z}, {w}";
        }

        
        
        
        
        public string ToString(string format)
        {
            return string.Format("({0}, {1}, {2}, {3})", x.ToString(format), y.ToString(format), z.ToString(format), w.ToString(format));
        }

        
        
        
        
        
        public static double Angle(Quaternion a, Quaternion b)
        {
            double f = Quaternion.Dot(a, b);
            return Math.Acos(Math.Min(Math.Abs(f), 1)) * 2 * radToDeg;
        }

        
        
        
        
        
        
        public static Quaternion Euler(double x, double y, double z)
        {
            return Quaternion.FromEulerRad(new Vector3((double)x, (double)y, (double)z) * degToRad);
        }

        
        
        
        
        public static Quaternion Euler(Vector3 euler)
        {
            return Quaternion.FromEulerRad(euler * degToRad);
        }

        private static double NormalizeAngle(double angle)
        {
            while (angle > 360)
            {
                angle -= 360;
            }

            while (angle < 0)
            {
                angle += 360;
            }

            return angle;
        }

        private static Quaternion FromEulerRad(Vector3 euler)
        {
            double yaw = euler.x;
            double pitch = euler.y;
            double roll = euler.z;
            double rollOver2 = roll * 0.5;
            double sinRollOver2 = (double)System.Math.Sin((double)rollOver2);
            double cosRollOver2 = (double)System.Math.Cos((double)rollOver2);
            double pitchOver2 = pitch * 0.5;
            double sinPitchOver2 = (double)System.Math.Sin((double)pitchOver2);
            double cosPitchOver2 = (double)System.Math.Cos((double)pitchOver2);
            double yawOver2 = yaw * 0.5;
            double sinYawOver2 = (double)System.Math.Sin((double)yawOver2);
            double cosYawOver2 = (double)System.Math.Cos((double)yawOver2);
            Quaternion result;
            result.x = cosYawOver2 * cosPitchOver2 * cosRollOver2 + sinYawOver2 * sinPitchOver2 * sinRollOver2;
            result.y = cosYawOver2 * cosPitchOver2 * sinRollOver2 - sinYawOver2 * sinPitchOver2 * cosRollOver2;
            result.z = cosYawOver2 * sinPitchOver2 * cosRollOver2 + sinYawOver2 * cosPitchOver2 * sinRollOver2;
            result.w = sinYawOver2 * cosPitchOver2 * cosRollOver2 - cosYawOver2 * sinPitchOver2 * sinRollOver2;
            return result;
        }

        private static void ToAxisAngleRad(Quaternion q, out Vector3 axis, out double angle)
        {
            if (System.Math.Abs(q.w) > 1.0)
            {
                q.Normalize();
            }

            angle = 2.0f * (double)System.Math.Acos(q.w); 
            double den = (double)System.Math.Sqrt(1.0 - q.w * q.w);
            if (den > 0.0001)
            {
                axis = q.xyz / den;
            }
            else
            {
                
                
                axis = new Vector3(1, 0, 0);
            }
        }

        public override int GetHashCode()
        {
            return x.GetHashCode() ^ y.GetHashCode() << 2 ^ z.GetHashCode() >> 2 ^ w.GetHashCode() >> 1;
        }
        public override bool Equals(object other)
        {
            if (!(other is Quaternion))
            {
                return false;
            }
            Quaternion quaternion = (Quaternion)other;
            return x.Equals(quaternion.x) && y.Equals(quaternion.y) && z.Equals(quaternion.z) && w.Equals(quaternion.w);
        }

        public bool Equals(Quaternion other)
        {
            return x.Equals(other.x) && y.Equals(other.y) && z.Equals(other.z) && w.Equals(other.w);
        }

        public static Quaternion operator *(Quaternion lhs, Quaternion rhs)
        {
            return new Quaternion(lhs.w * rhs.x + lhs.x * rhs.w + lhs.y * rhs.z - lhs.z * rhs.y, lhs.w * rhs.y + lhs.y * rhs.w + lhs.z * rhs.x - lhs.x * rhs.z, lhs.w * rhs.z + lhs.z * rhs.w + lhs.x * rhs.y - lhs.y * rhs.x, lhs.w * rhs.w - lhs.x * rhs.x - lhs.y * rhs.y - lhs.z * rhs.z);
        }

        public static Vector3 operator *(Quaternion rotation, Vector3 point)
        {
            double num = rotation.x * 2;
            double num2 = rotation.y * 2;
            double num3 = rotation.z * 2;
            double num4 = rotation.x * num;
            double num5 = rotation.y * num2;
            double num6 = rotation.z * num3;
            double num7 = rotation.x * num2;
            double num8 = rotation.x * num3;
            double num9 = rotation.y * num3;
            double num10 = rotation.w * num;
            double num11 = rotation.w * num2;
            double num12 = rotation.w * num3;

            return new Vector3(
                (1 - (num5 + num6)) * point.x + (num7 - num12) * point.y + (num8 + num11) * point.z,
                (num7 + num12) * point.x + (1 - (num4 + num6)) * point.y + (num9 - num10) * point.z,
                (num8 - num11) * point.x + (num9 + num10) * point.y + (1 - (num4 + num5)) * point.z);
        }

        public static bool operator ==(Quaternion lhs, Quaternion rhs)
        {
            return Quaternion.Dot(lhs, rhs) > 0.999999999;
        }

        public static bool operator !=(Quaternion lhs, Quaternion rhs)
        {
            return Quaternion.Dot(lhs, rhs) <= 0.999999999;
        }
    }
}