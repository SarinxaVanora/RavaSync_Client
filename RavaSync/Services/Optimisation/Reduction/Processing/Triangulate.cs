using System;

namespace RavaSync.Services.Optimisation.Reduction
{
    public class TriangulateModifier
    {
        public void Run(ConnectedMesh mesh)
        {
            for (int i = 0; i < mesh.nodes.Length; i++)
            {
                int edgeCount = 0;
                int relative = i;
                while ((relative = mesh.nodes[relative].relative) != i) 
                {
                    edgeCount++;
                }

                if (edgeCount > 2)
                {
                    throw new Exception("Mesh has polygons of dimension 4 or greater");
                }
            }

            
        }
    }
}