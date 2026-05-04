using System.Collections.Generic;
using System.Diagnostics;

namespace RavaSync.Services.Optimisation.Reduction
{
    
    
    
    
    
    public class OptimisedMeshBuffer
    {
        public Vector3[] positions;
        public int[] triangles;
        public MeshGroup[] groups;
        public MetaAttributeList attributes;
        public AttributeDefinition[] attributeDefinitions;

        [Conditional("DEBUG")]
        public void CheckLengths()
        {
            
            
            
            
            
            
            
        }

        public ConnectedMesh ToConnectedMesh()
        {
            CheckLengths();

            ConnectedMesh connectedMesh = new ConnectedMesh
            {
                groups = groups
            };

            connectedMesh.positions = positions;
            connectedMesh.attributes = attributes;
            connectedMesh.attributeDefinitions = attributeDefinitions;

            
            ConnectedMesh.Node[] nodes = new ConnectedMesh.Node[triangles.Length];
            Dictionary<int, List<int>> vertexToNodes = new Dictionary<int, List<int>>();
            for (int i = 0; i < triangles.Length; i += 3)
            {
                ConnectedMesh.Node A = new ConnectedMesh.Node();
                ConnectedMesh.Node B = new ConnectedMesh.Node();
                ConnectedMesh.Node C = new ConnectedMesh.Node();

                A.position = triangles[i];
                B.position = triangles[i + 1];
                C.position = triangles[i + 2];

                A.attribute = triangles[i];
                B.attribute = triangles[i + 1];
                C.attribute = triangles[i + 2];

                A.relative = i + 1; 
                B.relative = i + 2; 
                C.relative = i; 

                if (!vertexToNodes.ContainsKey(A.position))
                {
                    vertexToNodes.Add(A.position, new List<int>());
                }

                if (!vertexToNodes.ContainsKey(B.position))
                {
                    vertexToNodes.Add(B.position, new List<int>());
                }

                if (!vertexToNodes.ContainsKey(C.position))
                {
                    vertexToNodes.Add(C.position, new List<int>());
                }

                vertexToNodes[A.position].Add(i);
                vertexToNodes[B.position].Add(i + 1);
                vertexToNodes[C.position].Add(i + 2);

                nodes[i] = A;
                nodes[i + 1] = B;
                nodes[i + 2] = C;

                connectedMesh._faceCount++;
            }

            
            foreach (KeyValuePair<int, List<int>> pair in vertexToNodes)
            {
                int previousSibling = -1;
                int firstSibling = -1;
                foreach (int node in pair.Value)
                {
                    if (firstSibling != -1)
                    {
                        nodes[node].sibling = previousSibling;
                    }
                    else
                    {
                        firstSibling = node;
                    }
                    previousSibling = node;
                }
                nodes[firstSibling].sibling = previousSibling;
            }

            connectedMesh.nodes = nodes;

            Debug.Assert(connectedMesh.Check());

            return connectedMesh;
        }
    }
}