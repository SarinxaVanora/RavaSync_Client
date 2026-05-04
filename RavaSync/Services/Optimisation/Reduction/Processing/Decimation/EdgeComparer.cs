using System.Collections.Generic;

namespace RavaSync.Services.Optimisation.Reduction
{
    public partial class DecimateModifier
    {
        private class EdgeComparer : IComparer<EdgeCollapse>
        {
            public int Compare(EdgeCollapse x, EdgeCollapse y)
            {
                return x.CompareTo(y);
            }
        }
    }
}