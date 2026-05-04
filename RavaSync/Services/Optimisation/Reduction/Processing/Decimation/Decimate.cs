using System.Diagnostics;

namespace RavaSync.Services.Optimisation.Reduction
{
    public partial class DecimateModifier
    {
        
        public readonly record struct DecimationSettings(
            bool UpdateFarNeighbors,
            bool UpdateMinsOnCollapse,
            float MergeNormalsThresholdDegrees,
            float NormalSimilarityThresholdDegrees,
            float CollapseToMidpointPenalty,
            bool CollapseToEndpointsOnly,
            float UvSimilarityThreshold,
            float UvSeamAngleCos,
            bool BlockUvSeamVertices,
            float BoneWeightSimilarityThreshold,
            bool LimitCollapseEdgeLength,
            float MaxCollapseEdgeLength,
            bool AllowBoundaryCollapses,
            float BodyCollisionPenetrationFactor);

        private static readonly DecimationSettings _defaultSettings = new(
            UpdateFarNeighbors: false,
            UpdateMinsOnCollapse: true,
            MergeNormalsThresholdDegrees: 90f,
            NormalSimilarityThresholdDegrees: 60f,
            CollapseToMidpointPenalty: 0.4716252f,
            CollapseToEndpointsOnly: false,
            UvSimilarityThreshold: 0.02f,
            UvSeamAngleCos: 0.99f,
            BlockUvSeamVertices: true,
            BoneWeightSimilarityThreshold: 0.85f,
            LimitCollapseEdgeLength: false,
            MaxCollapseEdgeLength: float.PositiveInfinity,
            AllowBoundaryCollapses: false,
            BodyCollisionPenetrationFactor: 0.75f);

        
        private const double _DeterminantEpsilon = 0.001f;
        private const float _MinTriangleAreaRatio = 0.05f;
        private const float _UvDirEpsilonSq = 1e-12f;
        private const double _OFFSET_HARD = 1e6;
        private const double _OFFSET_NOCOLLAPSE = 1e300;

        
        private ConnectedMesh _mesh;
        private SymmetricMatrix[] _matrices;
        private FastHashSet<EdgeCollapse> _pairs;
        private LinkedHashSet<EdgeCollapse> _mins;
        private int _lastProgress = int.MinValue;
        private int _initialTriangleCount;
        private DecimationSettings _settings;
        private float _mergeNormalsThresholdCos;
        private float _normalSimilarityThresholdCos;
        private int _evaluatedEdges;
        private int _collapsedEdges;
        private int _rejectedBoneWeights;
        private int _rejectedTopology;
        private int _rejectedInversion;
        private int _rejectedDegenerate;
        private int _rejectedArea;
        private int _rejectedFlip;
        private int _rejectedBodyCollision;
        private float[]? _bodyDistanceSq;
        private float _bodyDistanceThresholdSq;
        private Func<Vector3, float>? _bodyDistanceSqEvaluator;
        private bool[]? _protectedVertices;

        public ConnectedMesh Mesh => _mesh;
        public DecimationSettings Settings => _settings;

        public DecimateModifier()
        {
            _mesh = default!;
            _matrices = default!;
            _pairs = default!;
            _mins = default!;
            SetSettings(_defaultSettings);
        }

        public static DecimationSettings CreateDefaultSettings()
        {
            return _defaultSettings;
        }

        public void SetSettings(DecimationSettings settings)
        {
            _settings = settings;
            _mergeNormalsThresholdCos = MathF.Cos(_settings.MergeNormalsThresholdDegrees * MathF.PI / 180f);
            _normalSimilarityThresholdCos = MathF.Cos(_settings.NormalSimilarityThresholdDegrees * MathF.PI / 180f);
        }

        public DecimationStats GetStats()
            => new DecimationStats(
                _evaluatedEdges,
                _collapsedEdges,
                _rejectedBoneWeights,
                _rejectedTopology,
                _rejectedInversion,
                _rejectedDegenerate,
                _rejectedArea,
                _rejectedFlip,
                _rejectedBodyCollision);

        public void SetBodyCollision(float[]? bodyDistanceSq, float bodyDistanceThresholdSq, Func<Vector3, float>? bodyDistanceSqEvaluator = null)
        {
            _bodyDistanceSq = bodyDistanceSq;
            _bodyDistanceThresholdSq = bodyDistanceThresholdSq;
            _bodyDistanceSqEvaluator = bodyDistanceSqEvaluator;
        }

        public void SetProtectedVertices(bool[]? protectedVertices)
        {
            _protectedVertices = protectedVertices;
        }

        public void Initialize(ConnectedMesh mesh)
        {
            _mesh = mesh;
            ResetStats();

            _initialTriangleCount = mesh.FaceCount;

            _matrices = new SymmetricMatrix[mesh.positions.Length];
            _pairs = new FastHashSet<EdgeCollapse>();
            _mins = new LinkedHashSet<EdgeCollapse>();

            InitializePairs();

            for (int p = 0; p < _mesh.PositionToNode.Length; p++)
            {
                if (_mesh.PositionToNode[p] != -1)
                    CalculateQuadric(p);
            }

            foreach (EdgeCollapse pair in _pairs)
            {
                CalculateError(pair);
            }
        }

        public void DecimateToError(float maximumError)
        {
            while (GetPairWithMinimumError().error <= maximumError && _pairs.Count > 0)
            {
                Iterate();
            }
        }

        public void DecimateToRatio(float targetTriangleRatio)
        {
            targetTriangleRatio = MathF.Clamp(targetTriangleRatio, 0f, 1f);
            DecimateToPolycount((int)MathF.Round(targetTriangleRatio * _mesh.FaceCount));
        }

        public void DecimatePolycount(int polycount)
        {
            DecimateToPolycount((int)MathF.Round(_mesh.FaceCount - polycount));
        }

        public void DecimateToPolycount(int targetTriangleCount)
        {
            while (_mesh.FaceCount > targetTriangleCount && _pairs.Count > 0)
            {
                Iterate();

                int progress = (int)MathF.Round(100f * (_initialTriangleCount - _mesh.FaceCount) / (_initialTriangleCount - targetTriangleCount));
                if (progress >= _lastProgress + 10)
                {
                    _lastProgress = progress;
                }
            }
        }

        public void Iterate()
        {
            EdgeCollapse pair = GetPairWithMinimumError();
            while (pair != null && pair.error >= _OFFSET_NOCOLLAPSE)
            {
                _pairs.Remove(pair);
                _mins.Remove(pair);
                pair = GetPairWithMinimumError();
            }

            if (pair == null)
                return;

            Debug.Assert(_mesh.CheckEdge(_mesh.PositionToNode[pair.posA], _mesh.PositionToNode[pair.posB]));

            _pairs.Remove(pair);
            _mins.Remove(pair);

            CollapseEdge(pair);
        }

        public double GetMinimumError()
        {
            return GetPairWithMinimumError()?.error ?? double.PositiveInfinity;
        }

        private EdgeCollapse GetPairWithMinimumError()
        {
            if (_mins.Count == 0)
                ComputeMins();

            LinkedHashSet<EdgeCollapse>.LinkedHashNode<EdgeCollapse> edge = _mins.First;

            return edge?.Value;
        }

        private int MinsCount => MathF.Clamp(500, 0, _pairs.Count);

        private void ComputeMins()
        {
            var nullCount = 0;
            foreach (var pair in _pairs)
            {
                if (pair is null)
                {
                    nullCount++;
                }
            }

            if (nullCount > 0)
            {
                Trace.TraceWarning($"ReductionCore.Decimator: _pairs contained {nullCount} null edge(s) in ComputeMins; skipping nulls.");
            }

            _mins = new LinkedHashSet<EdgeCollapse>(_pairs.Where(pair => pair != null).OrderBy(x => x).Take(MinsCount));
        }

        private void InitializePairs()
        {
            _pairs.Clear();
            _mins.Clear();

            for (int p = 0; p < _mesh.PositionToNode.Length; p++)
            {
                int nodeIndex = _mesh.PositionToNode[p];
                if (nodeIndex < 0)
                {
                    continue;
                }

                int sibling = nodeIndex;
                do
                {
                    int firstRelative = _mesh.nodes[sibling].relative;
                    int secondRelative = _mesh.nodes[firstRelative].relative;

                    EdgeCollapse pair = new EdgeCollapse(_mesh.nodes[firstRelative].position, _mesh.nodes[secondRelative].position);

                    _pairs.Add(pair);

                    Debug.Assert(_mesh.CheckEdge(_mesh.PositionToNode[pair.posA], _mesh.PositionToNode[pair.posB]));

                } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);
            }
        }

        private void CalculateQuadric(int position)
        {
            int nodeIndex = _mesh.PositionToNode[position];

            Debug.Assert(nodeIndex >= 0);
            Debug.Assert(!_mesh.nodes[nodeIndex].IsRemoved);

            SymmetricMatrix symmetricMatrix = new SymmetricMatrix();

            int sibling = nodeIndex;
            do
            {
                Debug.Assert(_mesh.CheckRelatives(sibling));

                Vector3 faceNormal = _mesh.GetFaceNormal(sibling);
                double dot = Vector3.Dot(-faceNormal, _mesh.positions[_mesh.nodes[sibling].position]);
                symmetricMatrix += new SymmetricMatrix(faceNormal.x, faceNormal.y, faceNormal.z, dot);

            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);

            _matrices[position] = symmetricMatrix;
        }

        private readonly HashSet<int> _adjacentEdges = new HashSet<int>();
        private readonly HashSet<int> _adjacentEdgesA = new HashSet<int>();
        private readonly HashSet<int> _adjacentEdgesB = new HashSet<int>();

        private IEnumerable<int> GetAdjacentPositions(int nodeIndex, int nodeAvoid)
        {
            _adjacentEdges.Clear();

            int posToAvoid = _mesh.nodes[nodeAvoid].position;

            int sibling = nodeIndex;
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    if (_mesh.nodes[relative].position != posToAvoid)
                    {
                        _adjacentEdges.Add(_mesh.nodes[relative].position);
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);

            return _adjacentEdges;
        }

        private void FillAdjacentPositions(int nodeIndex, int nodeAvoid, HashSet<int> output)
        {
            output.Clear();

            int posToAvoid = _mesh.nodes[nodeAvoid].position;

            int sibling = nodeIndex;
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    if (_mesh.nodes[relative].position != posToAvoid)
                    {
                        output.Add(_mesh.nodes[relative].position);
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);
        }

        private void FillAdjacentPositionsByPos(int nodeIndex, int posToAvoid, HashSet<int> output)
        {
            output.Clear();

            int sibling = nodeIndex;
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    int pos = _mesh.nodes[relative].position;
                    if (pos != posToAvoid)
                    {
                        output.Add(pos);
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);
        }

        private double GetEdgeTopo(EdgeCollapse edge)
        {
            if (edge.Weight == -1)
            {
                edge.SetWeight(_mesh.GetEdgeTopo(_mesh.PositionToNode[edge.posA], _mesh.PositionToNode[edge.posB]));
            }
            return edge.Weight;
        }

        public static bool UseEdgeLength = true;

        private void CalculateError(EdgeCollapse pair)
        {
            Debug.Assert(_mesh.CheckEdge(_mesh.PositionToNode[pair.posA], _mesh.PositionToNode[pair.posB]));

            Vector3 posA = _mesh.positions[pair.posA];
            Vector3 posB = _mesh.positions[pair.posB];
            _evaluatedEdges++;

            if (ShouldBlockBoneWeightCollapse(pair.posA, pair.posB))
            {
                _rejectedBoneWeights++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }
            if (ShouldBlockNormalCollapse(pair.posA, pair.posB))
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }
            if (ShouldBlockUvCollapse(pair.posA, pair.posB))
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }
            if (IsProtectedVertex(pair.posA) || IsProtectedVertex(pair.posB))
            {
                _rejectedBodyCollision++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }

            var edgeTopo = GetEdgeTopo(pair);
            if (edgeTopo > 0d && !_settings.AllowBoundaryCollapses)
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }
            Vector3 posC = (posB + posA) / 2;

            int nodeA = _mesh.PositionToNode[pair.posA];
            int nodeB = _mesh.PositionToNode[pair.posB];
            if (!CollapsePreservesTopology(pair))
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }
            if (!_settings.AllowBoundaryCollapses && (IsBoundaryVertex(nodeA) || IsBoundaryVertex(nodeB)))
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }

            double errorCollapseToO;
            Vector3 posO = Vector3.PositiveInfinity;

            
            
            
            

            SymmetricMatrix q = _matrices[pair.posA] + _matrices[pair.posB];
            double det = q.DeterminantXYZ();

            if (det > _DeterminantEpsilon || det < -_DeterminantEpsilon)
            {
                posO = new Vector3(
                    -1d / det * q.DeterminantX(),
                    +1d / det * q.DeterminantY(),
                    -1d / det * q.DeterminantZ());
                errorCollapseToO = ComputeVertexError(q, posO.x, posO.y, posO.z);
            }
            else
            {
                errorCollapseToO = _OFFSET_NOCOLLAPSE;
            }

            double errorCollapseToA = ComputeVertexError(q, posA.x, posA.y, posA.z);
            double errorCollapseToB = ComputeVertexError(q, posB.x, posB.y, posB.z);
            double errorCollapseToC = ComputeVertexError(q, posC.x, posC.y, posC.z);

            int pA = _mesh.nodes[nodeA].position;
            int pB = _mesh.nodes[nodeB].position;

            
            
            double length = (posB - posA).Length;
            if (_settings.LimitCollapseEdgeLength && length > _settings.MaxCollapseEdgeLength)
            {
                _rejectedTopology++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }

            foreach (int pD in GetAdjacentPositions(nodeA, nodeB))
            {
                Vector3 posD = _mesh.positions[pD];
                EdgeCollapse edge = new EdgeCollapse(pA, pD);
                if (_pairs.TryGetValue(edge, out EdgeCollapse realEdge))
                {
                    double weight = GetEdgeTopo(realEdge);
                    errorCollapseToB += weight * length * ComputeLineicError(posB, posD, posA);
                    errorCollapseToC += weight * length * ComputeLineicError(posC, posD, posA);
                }
            }

            foreach (int pD in GetAdjacentPositions(nodeB, nodeA))
            {
                Vector3 posD = _mesh.positions[pD];
                EdgeCollapse edge = new EdgeCollapse(pB, pD);
                if (_pairs.TryGetValue(edge, out EdgeCollapse realEdge))
                {
                    double weight = GetEdgeTopo(realEdge);
                    errorCollapseToA += weight * length * ComputeLineicError(posA, posD, posB);
                    errorCollapseToC += weight * length * ComputeLineicError(posC, posD, posB);
                }
            }

            errorCollapseToC *= _settings.CollapseToMidpointPenalty;

            if (_settings.CollapseToEndpointsOnly)
            {
                errorCollapseToO = _OFFSET_NOCOLLAPSE;
                errorCollapseToC = _OFFSET_NOCOLLAPSE;
            }

            if (_settings.CollapseToEndpointsOnly && _bodyDistanceSq != null && _bodyDistanceThresholdSq > 0f)
            {
                var hasA = TryGetBodyDistanceSq(pair.posA, out var distASq);
                var hasB = TryGetBodyDistanceSq(pair.posB, out var distBSq);
                var nearA = hasA && distASq <= _bodyDistanceThresholdSq;
                var nearB = hasB && distBSq <= _bodyDistanceThresholdSq;

                if (nearA && nearB)
                {
                    if (distASq > distBSq)
                    {
                        errorCollapseToB = _OFFSET_NOCOLLAPSE;
                    }
                    else if (distBSq > distASq)
                    {
                        errorCollapseToA = _OFFSET_NOCOLLAPSE;
                    }
                    else
                    {
                        errorCollapseToA = _OFFSET_NOCOLLAPSE;
                        errorCollapseToB = _OFFSET_NOCOLLAPSE;
                    }
                }
                else
                {
                    if (nearA)
                    {
                        errorCollapseToA = _OFFSET_NOCOLLAPSE;
                    }

                    if (nearB)
                    {
                        errorCollapseToB = _OFFSET_NOCOLLAPSE;
                    }
                }

                if (hasA && hasB)
                {
                    if (distASq > distBSq)
                    {
                        errorCollapseToB = _OFFSET_NOCOLLAPSE;
                    }
                    else if (distBSq > distASq)
                    {
                        errorCollapseToA = _OFFSET_NOCOLLAPSE;
                    }
                }

                if (errorCollapseToA >= _OFFSET_NOCOLLAPSE && errorCollapseToB >= _OFFSET_NOCOLLAPSE)
                {
                    _rejectedBodyCollision++;
                    pair.error = _OFFSET_NOCOLLAPSE;
                    return;
                }
            }

            if (!_settings.CollapseToEndpointsOnly && IsPointNearBody((posA + posB) * 0.5))
            {
                _rejectedBodyCollision++;
                pair.error = _OFFSET_NOCOLLAPSE;
                return;
            }

            MathUtils.SelectMin(
                errorCollapseToO, errorCollapseToA, errorCollapseToB, errorCollapseToC,
                posO, posA, posB, posC,
                out pair.error, out pair.result);

            pair.error = Math.Max(0d, pair.error);

            if (!CollapseWillInvert(pair))
            {
                pair.error = _OFFSET_NOCOLLAPSE;
            }

            
        }

        private bool CollapsePreservesTopology(EdgeCollapse edge)
        {
            int nodeIndexA = _mesh.PositionToNode[edge.posA];
            int nodeIndexB = _mesh.PositionToNode[edge.posB];
            if (nodeIndexA < 0 || nodeIndexB < 0)
            {
                return true;
            }

            FillAdjacentPositions(nodeIndexA, nodeIndexB, _adjacentEdgesA);
            FillAdjacentPositions(nodeIndexB, nodeIndexA, _adjacentEdgesB);

            int shared = 0;
            foreach (var neighbor in _adjacentEdgesA)
            {
                if (_adjacentEdgesB.Contains(neighbor))
                {
                    shared++;
                    if (shared > 2)
                    {
                        return false;
                    }
                }
            }

            return _settings.AllowBoundaryCollapses ? shared >= 1 : shared == 2;
        }

        private bool IsBoundaryVertex(int nodeIndex)
        {
            if (nodeIndex < 0)
            {
                return false;
            }

            int sibling = nodeIndex;
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    if (_mesh.GetEdgeTopo(sibling, relative) >= ConnectedMesh.EdgeBorderPenalty)
                    {
                        return true;
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);

            return false;
        }

        private bool ShouldBlockBoneWeightCollapse(int posA, int posB)
        {
            if (_mesh.attributes is not MetaAttributeList<FfxivVertexAttribute> attrList)
            {
                return false;
            }

            int nodeA = _mesh.PositionToNode[posA];
            int nodeB = _mesh.PositionToNode[posB];
            if (nodeA < 0 || nodeB < 0)
            {
                return false;
            }

            bool hasWeights = false;
            int siblingA = nodeA;
            do
            {
                var attrA = (MetaAttribute<FfxivVertexAttribute>)attrList[_mesh.nodes[siblingA].attribute];
                if ((attrA.attr0.flags & FfxivAttributeFlags.BoneWeights) != 0)
                {
                    hasWeights = true;
                    int siblingB = nodeB;
                    do
                    {
                        var attrB = (MetaAttribute<FfxivVertexAttribute>)attrList[_mesh.nodes[siblingB].attribute];
                        if ((attrB.attr0.flags & FfxivAttributeFlags.BoneWeights) != 0
                            && HasMatchingDominantBone(attrA.attr0.boneWeight, attrB.attr0.boneWeight)
                            && GetBoneWeightOverlapNormalized(attrA.attr0.boneWeight, attrB.attr0.boneWeight) >= _settings.BoneWeightSimilarityThreshold)
                        {
                            return false;
                        }
                    } while ((siblingB = _mesh.nodes[siblingB].sibling) != nodeB);
                }
            } while ((siblingA = _mesh.nodes[siblingA].sibling) != nodeA);

            return hasWeights;
        }

        private bool ShouldBlockUvCollapse(int posA, int posB)
        {
            if (_mesh.attributes is not MetaAttributeList<FfxivVertexAttribute> attrList)
            {
                return false;
            }

            var attrA = ((MetaAttribute<FfxivVertexAttribute>)attrList[posA]).attr0;
            var attrB = ((MetaAttribute<FfxivVertexAttribute>)attrList[posB]).attr0;
            var flags = attrA.flags | attrB.flags;
            if ((flags & FfxivAttributeFlags.Uv0) == 0)
            {
                return false;
            }

            var isSeam = IsUvSeamEdge(attrA.uv0, attrB.uv0);
            if (!isSeam)
            {
                if (_settings.BlockUvSeamVertices && (HasUvSeamAtVertex(posA, posB, attrList, attrA) || HasUvSeamAtVertex(posB, posA, attrList, attrB)))
                {
                    return true;
                }

                return false;
            }

            if (!CheckUvSeamAngleAtVertex(posA, posB, attrList, attrA, attrB))
            {
                return true;
            }

            if (!CheckUvSeamAngleAtVertex(posB, posA, attrList, attrB, attrA))
            {
                return true;
            }

            return false;
        }

        private bool ShouldBlockNormalCollapse(int posA, int posB)
        {
            if (_mesh.attributes is not MetaAttributeList<FfxivVertexAttribute> attrList)
            {
                return false;
            }

            var attrA = ((MetaAttribute<FfxivVertexAttribute>)attrList[posA]).attr0;
            var attrB = ((MetaAttribute<FfxivVertexAttribute>)attrList[posB]).attr0;
            if ((attrA.flags & FfxivAttributeFlags.Normal) == 0 || (attrB.flags & FfxivAttributeFlags.Normal) == 0)
            {
                return false;
            }

            var dot = Vector3F.Dot(attrA.normal, attrB.normal);
            return dot < _normalSimilarityThresholdCos;
        }

        private static float UvDistanceSq(in Vector2F a, in Vector2F b)
        {
            var dx = a.x - b.x;
            var dy = a.y - b.y;
            return (dx * dx) + (dy * dy);
        }

        private bool IsUvSeamEdge(in Vector2F uvA, in Vector2F uvB)
        {
            var thresholdSq = _settings.UvSimilarityThreshold * _settings.UvSimilarityThreshold;
            return UvDistanceSq(uvA, uvB) > thresholdSq;
        }

        private bool HasUvSeamAtVertex(int posCenter, int posExclude, MetaAttributeList<FfxivVertexAttribute> attrList, in FfxivVertexAttribute attrCenter)
        {
            int nodeCenter = _mesh.PositionToNode[posCenter];
            if (nodeCenter < 0)
            {
                return false;
            }

            FillAdjacentPositionsByPos(nodeCenter, posExclude, _adjacentEdges);
            foreach (int neighborPos in _adjacentEdges)
            {
                var attrNeighbor = ((MetaAttribute<FfxivVertexAttribute>)attrList[neighborPos]).attr0;
                if (((attrNeighbor.flags | attrCenter.flags) & FfxivAttributeFlags.Uv0) == 0)
                {
                    continue;
                }

                if (IsUvSeamEdge(attrCenter.uv0, attrNeighbor.uv0))
                {
                    return true;
                }
            }

            return false;
        }

        private bool CheckUvSeamAngleAtVertex(int posCenter, int posOther, MetaAttributeList<FfxivVertexAttribute> attrList, in FfxivVertexAttribute attrCenter, in FfxivVertexAttribute attrOther)
        {
            int nodeCenter = _mesh.PositionToNode[posCenter];
            if (nodeCenter < 0)
            {
                return true;
            }

            FillAdjacentPositionsByPos(nodeCenter, posOther, _adjacentEdges);

            int seamEdges = 1;
            int otherSeamPos = -1;

            foreach (int neighborPos in _adjacentEdges)
            {
                var attrNeighbor = ((MetaAttribute<FfxivVertexAttribute>)attrList[neighborPos]).attr0;
                if (((attrNeighbor.flags | attrCenter.flags) & FfxivAttributeFlags.Uv0) == 0)
                {
                    continue;
                }

                if (IsUvSeamEdge(attrCenter.uv0, attrNeighbor.uv0))
                {
                    seamEdges++;
                    otherSeamPos = neighborPos;
                    if (seamEdges > 2)
                    {
                        return false;
                    }
                }
            }

            if (otherSeamPos < 0)
            {
                return true;
            }

            var attrOtherSeam = ((MetaAttribute<FfxivVertexAttribute>)attrList[otherSeamPos]).attr0;
            if (!TryNormalizeUvDirection(attrCenter.uv0, attrOther.uv0, out var dir1)
                || !TryNormalizeUvDirection(attrCenter.uv0, attrOtherSeam.uv0, out var dir2))
            {
                return false;
            }

            var dot = (dir1.x * dir2.x) + (dir1.y * dir2.y);
            return dot >= _settings.UvSeamAngleCos;
        }

        private static bool TryNormalizeUvDirection(in Vector2F from, in Vector2F to, out Vector2F direction)
        {
            var dx = to.x - from.x;
            var dy = to.y - from.y;
            var lenSq = (dx * dx) + (dy * dy);
            if (lenSq <= _UvDirEpsilonSq)
            {
                direction = default;
                return false;
            }

            var invLen = 1f / MathF.Sqrt(lenSq);
            direction = new Vector2F(dx * invLen, dy * invLen);
            return true;
        }

        private bool TryGetBodyDistanceSq(int pos, out float distanceSq)
        {
            distanceSq = float.NaN;
            if (_bodyDistanceSq == null)
            {
                return false;
            }

            if ((uint)pos >= (uint)_bodyDistanceSq.Length)
            {
                return false;
            }

            distanceSq = _bodyDistanceSq[pos];
            return !float.IsNaN(distanceSq);
        }

        private static float GetBoneWeightOverlapNormalized(in BoneWeight a, in BoneWeight b)
        {
            var overlap = GetBoneWeightOverlap(a, b);
            var sumA = GetBoneWeightSum(a);
            var sumB = GetBoneWeightSum(b);
            var denom = MathF.Max(sumA, sumB);
            if (denom <= 1e-6f)
            {
                return 1f;
            }

            return overlap / denom;
        }

        private static bool HasMatchingDominantBone(in BoneWeight a, in BoneWeight b)
        {
            var dominantA = GetDominantBoneIndex(a);
            if (dominantA < 0)
            {
                return true;
            }

            var dominantB = GetDominantBoneIndex(b);
            if (dominantB < 0)
            {
                return true;
            }

            return dominantA == dominantB;
        }

        private static int GetDominantBoneIndex(in BoneWeight weight)
        {
            var max = weight.weight0;
            var index = weight.index0;

            if (weight.weight1 > max)
            {
                max = weight.weight1;
                index = weight.index1;
            }
            if (weight.weight2 > max)
            {
                max = weight.weight2;
                index = weight.index2;
            }
            if (weight.weight3 > max)
            {
                max = weight.weight3;
                index = weight.index3;
            }

            return max > 0f ? index : -1;
        }

        private static float GetBoneWeightOverlap(in BoneWeight a, in BoneWeight b)
        {
            float overlap = 0f;
            AddSharedWeight(a.index0, a.weight0, b, ref overlap);
            AddSharedWeight(a.index1, a.weight1, b, ref overlap);
            AddSharedWeight(a.index2, a.weight2, b, ref overlap);
            AddSharedWeight(a.index3, a.weight3, b, ref overlap);
            return overlap;
        }

        private static float GetBoneWeightSum(in BoneWeight weight)
            => weight.weight0 + weight.weight1 + weight.weight2 + weight.weight3;

        private static void AddSharedWeight(int index, float weight, in BoneWeight other, ref float overlap)
        {
            if (weight <= 0f)
            {
                return;
            }

            if (index == other.index0)
            {
                overlap += MathF.Min(weight, other.weight0);
            }
            else if (index == other.index1)
            {
                overlap += MathF.Min(weight, other.weight1);
            }
            else if (index == other.index2)
            {
                overlap += MathF.Min(weight, other.weight2);
            }
            else if (index == other.index3)
            {
                overlap += MathF.Min(weight, other.weight3);
            }
        }

        public bool CollapseWillInvert(EdgeCollapse edge)
        {
            int nodeIndexA = _mesh.PositionToNode[edge.posA];
            int nodeIndexB = _mesh.PositionToNode[edge.posB];
            if ((uint)nodeIndexA >= (uint)_mesh.nodes.Length || (uint)nodeIndexB >= (uint)_mesh.nodes.Length)
            {
                _rejectedTopology++;
                _rejectedInversion++;
                return false;
            }

            Vector3 positionA = _mesh.positions[edge.posA];
            Vector3 positionB = _mesh.positions[edge.posB];
            var minAreaRatioSq = _MinTriangleAreaRatio * _MinTriangleAreaRatio;

            if (!ValidateCollapsedNodeRing(nodeIndexA, edge.posB, positionA, edge.result, minAreaRatioSq))
            {
                return false;
            }

            if (!ValidateCollapsedNodeRing(nodeIndexB, edge.posA, positionB, edge.result, minAreaRatioSq))
            {
                return false;
            }

            return true;
        }

        private bool ValidateCollapsedNodeRing(int startNodeIndex, int oppositePosition, in Vector3 collapseSourcePosition, in Vector3 collapseResult, float minAreaRatioSq)
        {
            int sibling = startNodeIndex;
            int guard = 0;
            int maxIterations = _mesh.nodes.Length;
            do
            {
                if ((uint)sibling >= (uint)_mesh.nodes.Length || _mesh.nodes[sibling].IsRemoved)
                {
                    _rejectedTopology++;
                    _rejectedInversion++;
                    return false;
                }

                if (!TryGetFaceVerticesForSibling(sibling, out int posC, out int posD))
                {
                    _rejectedTopology++;
                    _rejectedInversion++;
                    return false;
                }

                if (posC != oppositePosition && posD != oppositePosition
                    && !ValidateCollapsedTriangle(collapseSourcePosition, collapseResult, posC, posD, minAreaRatioSq))
                {
                    return false;
                }

                sibling = _mesh.nodes[sibling].sibling;
                guard++;
                if (guard > maxIterations)
                {
                    _rejectedTopology++;
                    _rejectedInversion++;
                    return false;
                }
            } while (sibling != startNodeIndex);

            return true;
        }

        private bool TryGetFaceVerticesForSibling(int siblingIndex, out int posC, out int posD)
        {
            posC = -1;
            posD = -1;

            if ((uint)siblingIndex >= (uint)_mesh.nodes.Length)
            {
                return false;
            }

            int relativeA = _mesh.nodes[siblingIndex].relative;
            if ((uint)relativeA >= (uint)_mesh.nodes.Length || _mesh.nodes[relativeA].IsRemoved)
            {
                return false;
            }

            int relativeB = _mesh.nodes[relativeA].relative;
            if ((uint)relativeB >= (uint)_mesh.nodes.Length || _mesh.nodes[relativeB].IsRemoved)
            {
                return false;
            }

            int relativeC = _mesh.nodes[relativeB].relative;
            if (relativeC != siblingIndex)
            {
                return false;
            }

            posC = _mesh.nodes[relativeA].position;
            posD = _mesh.nodes[relativeB].position;

            return (uint)posC < (uint)_mesh.positions.Length && (uint)posD < (uint)_mesh.positions.Length;
        }

        private bool ValidateCollapsedTriangle(in Vector3 collapseSourcePosition, in Vector3 collapseResult, int posC, int posD, float minAreaRatioSq)
        {
            Vector3F edgeAC = _mesh.positions[posC] - collapseSourcePosition;
            Vector3F edgeAD = _mesh.positions[posD] - collapseSourcePosition;
            Vector3F edgeCD = _mesh.positions[posD] - _mesh.positions[posC];
            var normalBefore = Vector3F.Cross(edgeAC, edgeAD);

            Vector3F edgeRC = _mesh.positions[posC] - collapseResult;
            Vector3F edgeRD = _mesh.positions[posD] - collapseResult;
            var normalAfter = Vector3F.Cross(edgeRC, edgeRD);
            if (ShouldRejectBodyTriangle(collapseResult, _mesh.positions[posC], _mesh.positions[posD]))
            {
                _rejectedBodyCollision++;
                return false;
            }

            if (IsDegenerateTriangle(edgeAC, edgeAD, edgeCD, normalBefore)
                || IsDegenerateTriangle(edgeRC, edgeRD, edgeCD, normalAfter))
            {
                _rejectedDegenerate++;
                _rejectedInversion++;
                return false;
            }

            if (normalAfter.SqrMagnitude < normalBefore.SqrMagnitude * minAreaRatioSq)
            {
                _rejectedArea++;
                _rejectedInversion++;
                return false;
            }

            var dot = Vector3F.Dot(normalBefore, normalAfter);
            if (dot <= 0f)
            {
                _rejectedFlip++;
                _rejectedInversion++;
                return false;
            }

            return true;
        }

        
        
        
        
        
        
        
        
        
        
        
        
        private double ComputeLineicError(in Vector3 A, in Vector3 B, in Vector3 X)
        {
            return Vector3.DistancePointLine(X, A, B);
        }

        private double ComputeVertexError(in SymmetricMatrix q, double x, double y, double z)
        {
            return q.m0 * x * x + 2 * q.m1 * x * y + 2 * q.m2 * x * z + 2 * q.m3 * x
                 + q.m4 * y * y + 2 * q.m5 * y * z + 2 * q.m6 * y
                 + q.m7 * z * z + 2 * q.m8 * z
                 + q.m9;
        }

        private void InterpolateAttributes(EdgeCollapse pair)
        {
            int posA = pair.posA;
            int posB = pair.posB;

            int nodeIndexA = _mesh.PositionToNode[posA];
            int nodeIndexB = _mesh.PositionToNode[posB];

            Vector3 positionA = _mesh.positions[posA];
            Vector3 positionB = _mesh.positions[posB];

            HashSet<int> procAttributes = new HashSet<int>();

            Vector3 positionN = pair.result;
            double AN = Vector3.Magnitude(positionA - positionN);
            double BN = Vector3.Magnitude(positionB - positionN);
            double ratio = MathUtils.DivideSafe(AN, AN + BN);

            







            


            int siblingOfA = nodeIndexA;
            do 
            {
                int relativeOfA = siblingOfA;
                do 
                {
                    if (_mesh.nodes[relativeOfA].position == posB)
                    {
                        if (!procAttributes.Add(_mesh.nodes[siblingOfA].attribute))
                            continue;

                        if (!procAttributes.Add(_mesh.nodes[relativeOfA].attribute))
                            continue;

                        if (_mesh.attributes != null && _mesh.attributeDefinitions.Length > 0)
                        {
                            IMetaAttribute attributeA = _mesh.attributes[_mesh.nodes[siblingOfA].attribute];
                            IMetaAttribute attributeB = _mesh.attributes[_mesh.nodes[relativeOfA].attribute];

                            for (int i = 0; i < _mesh.attributeDefinitions.Length; i++)
                            {
                                if (_mesh.attributeDefinitions[i].type == AttributeType.Normals)
                                {
                                    Vector3F normalA = attributeA.Get<Vector3F>(i);
                                    Vector3F normalB = attributeB.Get<Vector3F>(i);

                                    float dot = Vector3F.Dot(normalA, normalB);

                                    if (dot < _mergeNormalsThresholdCos)
                                    {
                                        continue;
                                    }
                                }

                                _mesh.attributes.Interpolate(i, _mesh.nodes[siblingOfA].attribute, _mesh.nodes[relativeOfA].attribute, ratio);
                            }
                        }
                    }
                } while ((relativeOfA = _mesh.nodes[relativeOfA].relative) != siblingOfA);

            } while ((siblingOfA = _mesh.nodes[siblingOfA].sibling) != nodeIndexA);


            














        }

        private readonly Dictionary<IMetaAttribute, int> _uniqueAttributes = new Dictionary<IMetaAttribute, int>();

        private void MergeAttributes(int nodeIndex)
        {
            if (_mesh.attributeDefinitions.Length == 0)
                return;

            _uniqueAttributes.Clear();

            int sibling = nodeIndex;
            do
            {
                _uniqueAttributes.TryAdd(_mesh.attributes[_mesh.nodes[sibling].attribute], _mesh.nodes[sibling].attribute);
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);

            sibling = nodeIndex;
            do
            {
                _mesh.nodes[sibling].attribute = _uniqueAttributes[_mesh.attributes[_mesh.nodes[sibling].attribute]];
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndex);
        }

        private readonly HashSet<EdgeCollapse> _edgeToRefresh = new HashSet<EdgeCollapse>();
        private readonly HashSet<int> _positionsToRemove = new HashSet<int>();

        private void CollapseEdge(EdgeCollapse pair)
        {
            _collapsedEdges++;
            int nodeIndexA = _mesh.PositionToNode[pair.posA];
            int nodeIndexB = _mesh.PositionToNode[pair.posB];

            int posA = pair.posA;
            int posB = pair.posB;

            
            int sibling = nodeIndexA;
            
            
            _positionsToRemove.Clear();
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    int posC = _mesh.nodes[relative].position;
                    if (!_positionsToRemove.Add(posC))
                    {
                        continue;
                    }
                    EdgeCollapse pairAC = new EdgeCollapse(posA, posC);
                    
                    if (_pairs.Remove(pairAC))
                    {
                        _mins.Remove(pairAC);
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndexA);

            
            sibling = nodeIndexB;
            _positionsToRemove.Clear();
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    int posC = _mesh.nodes[relative].position;
                    if (!_positionsToRemove.Add(posC))
                    {
                        continue;
                    }
                    EdgeCollapse pairBC = new EdgeCollapse(posB, posC);
                    if (_pairs.Remove(pairBC))
                    {
                        _mins.Remove(pairBC);
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != nodeIndexB);

            
            InterpolateAttributes(pair);

            
            int validNode = _mesh.CollapseEdge(nodeIndexA, nodeIndexB);

            
            if (validNode < 0)
            {
                return;
            }

            posA = _mesh.nodes[validNode].position;

            _mesh.positions[posA] = pair.result;

            MergeAttributes(validNode);

            CalculateQuadric(posA);

            _edgeToRefresh.Clear();

            sibling = validNode;
            do
            {
                for (int relative = sibling; (relative = _mesh.nodes[relative].relative) != sibling;)
                {
                    int posC = _mesh.nodes[relative].position;
                    _edgeToRefresh.Add(new EdgeCollapse(posA, posC));

                    if (_settings.UpdateFarNeighbors)
                    {
                        int sibling2 = relative;
                        while ((sibling2 = _mesh.nodes[sibling2].sibling) != relative)
                        {
                            int relative2 = sibling2;
                            while ((relative2 = _mesh.nodes[relative2].relative) != sibling2)
                            {
                                int posD = _mesh.nodes[relative2].position;
                                if (posD != posC)
                                {
                                    _edgeToRefresh.Add(new EdgeCollapse(posC, posD));
                                }
                            }
                        }
                    }
                }
            } while ((sibling = _mesh.nodes[sibling].sibling) != validNode);

            foreach (EdgeCollapse edge in _edgeToRefresh)
            {
                CalculateQuadric(edge.posB);
                edge.SetWeight(-1);
                _pairs.Remove(edge);
                _pairs.Add(edge);
            }

            foreach (EdgeCollapse edge in _edgeToRefresh)
            {
                CalculateError(edge);
                _mins.Remove(edge);
                if (_settings.UpdateMinsOnCollapse)
                {
                    _mins.AddMin(edge);
                }
            }
        }

        private void ResetStats()
        {
            _evaluatedEdges = 0;
            _collapsedEdges = 0;
            _rejectedBoneWeights = 0;
            _rejectedTopology = 0;
            _rejectedInversion = 0;
            _rejectedDegenerate = 0;
            _rejectedArea = 0;
            _rejectedFlip = 0;
            _rejectedBodyCollision = 0;
        }

        private bool IsPointNearBody(in Vector3 point)
        {
            if (_bodyDistanceSqEvaluator == null || _bodyDistanceThresholdSq <= 0f)
            {
                return false;
            }

            var sq = _bodyDistanceSqEvaluator(point);
            return !float.IsNaN(sq) && sq <= _bodyDistanceThresholdSq;
        }

        private bool IsPointNearBody(in Vector3 point, float thresholdSq)
        {
            if (_bodyDistanceSqEvaluator == null || thresholdSq <= 0f)
            {
                return false;
            }

            var sq = _bodyDistanceSqEvaluator(point);
            return !float.IsNaN(sq) && sq <= thresholdSq;
        }

        private bool ShouldRejectBodyTriangle(in Vector3 a, in Vector3 b, in Vector3 c)
        {
            if (_bodyDistanceSqEvaluator == null || _bodyDistanceThresholdSq <= 0f)
            {
                return false;
            }

            var centroid = (a + b + c) / 3d;
            if (!_settings.CollapseToEndpointsOnly)
            {
                return IsPointNearBody(centroid);
            }

            var penetrationFactor = MathF.Max(0f, _settings.BodyCollisionPenetrationFactor);
            var penetrationThresholdSq = _bodyDistanceThresholdSq * penetrationFactor * penetrationFactor;
            if (IsPointNearBody(centroid, penetrationThresholdSq))
            {
                return true;
            }

            var ab = (a + b) * 0.5;
            var bc = (b + c) * 0.5;
            var ca = (c + a) * 0.5;
            return IsPointNearBody(ab, penetrationThresholdSq)
                || IsPointNearBody(bc, penetrationThresholdSq)
                || IsPointNearBody(ca, penetrationThresholdSq);
        }

        private bool IsProtectedVertex(int pos)
        {
            if (_protectedVertices == null)
            {
                return false;
            }

            return (uint)pos < (uint)_protectedVertices.Length && _protectedVertices[pos];
        }

        private static bool IsDegenerateTriangle(in Vector3F edge0, in Vector3F edge1, in Vector3F edge2, in Vector3F normal)
        {
            var maxEdgeSq = MathF.Max(edge0.SqrMagnitude, MathF.Max(edge1.SqrMagnitude, edge2.SqrMagnitude));
            if (maxEdgeSq <= 0f)
            {
                return true;
            }

            var minNormalSq = (float)(_DeterminantEpsilon * _DeterminantEpsilon) * maxEdgeSq * maxEdgeSq;
            return normal.SqrMagnitude <= minNormalSq;
        }
    }

    public readonly record struct DecimationStats(
        int EvaluatedEdges,
        int CollapsedEdges,
        int RejectedBoneWeights,
        int RejectedTopology,
        int RejectedInversion,
        int RejectedDegenerate,
        int RejectedArea,
        int RejectedFlip,
        int RejectedBodyCollision);
}