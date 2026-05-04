using System.Collections.Generic;

namespace RavaSync.Services.Optimisation.Reduction
{
    public class Vector4FComparer : IEqualityComparer<Vector4F>
    {
        private static Vector4FComparer? _instance;
        public static Vector4FComparer Default => _instance ??= new Vector4FComparer(0.0001f);

        private readonly float _tolerance;

        public Vector4FComparer(float tolerance)
        {
            _tolerance = tolerance;
        }

        public bool Equals(Vector4F x, Vector4F y)
        {
            return (int)(x.x / _tolerance) == (int)(y.x / _tolerance)
                && (int)(x.y / _tolerance) == (int)(y.y / _tolerance)
                && (int)(x.z / _tolerance) == (int)(y.z / _tolerance)
                && (int)(x.w / _tolerance) == (int)(y.w / _tolerance);
        }

        public int GetHashCode(Vector4F obj)
        {
            return (int)(obj.x / _tolerance)
                ^ ((int)(obj.y / _tolerance) << 2)
                ^ ((int)(obj.z / _tolerance) >> 2)
                ^ ((int)(obj.w / _tolerance) << 1);
        }
    }
}