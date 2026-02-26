using FFXIVClientStructs.FFXIV.Client.Game.Character;
using FFXIVClientStructs.FFXIV.Client.Graphics.Scene;
using FFXIVClientStructs.Havok.Animation;
using FFXIVClientStructs.Havok.Common.Base.Types;
using FFXIVClientStructs.Havok.Common.Serialize.Util;
using RavaSync.FileCache;
using RavaSync.Interop.GameModel;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;

namespace RavaSync.Services;

public sealed class XivDataAnalyzer
{
    private readonly ILogger<XivDataAnalyzer> _logger;
    private readonly FileCacheManager _fileCacheManager;
    private readonly XivDataStorageService _configService;
    private readonly HashSet<string> _failedCalculatedTris = new(StringComparer.Ordinal);

    // Throttle config save requests during burst tri analysis
    private DateTime _nextTriangleConfigSaveUtc = DateTime.MinValue;
    private static readonly TimeSpan TriangleConfigSaveThrottle = TimeSpan.FromSeconds(2);

    public XivDataAnalyzer(ILogger<XivDataAnalyzer> logger, FileCacheManager fileCacheManager,
        XivDataStorageService configService)
    {
        _logger = logger;
        _fileCacheManager = fileCacheManager;
        _configService = configService;
    }

    public unsafe Dictionary<string, List<ushort>>? GetSkeletonBoneIndices(GameObjectHandler handler)
    {
        if (handler == null) return null;
        if (handler.Address == nint.Zero) return null;

        var character = (Character*)handler.Address;
        if (character == null) return null;

        var drawObject = character->GameObject.DrawObject;
        if (drawObject == null) return null;

        var chara = (CharacterBase*)drawObject;
        if (chara == null) return null;

        if (chara->GetModelType() != CharacterBase.ModelType.Human) return null;

        if (chara->Skeleton == null) return null;

        var skeleton = chara->Skeleton;
        var resHandles = skeleton->SkeletonResourceHandles;
        if (resHandles == null) return null;

        var partialCount = skeleton->PartialSkeletonCount;
        if (partialCount <= 0 || partialCount > 64) return null;

        Dictionary<string, List<ushort>> outputIndices = [];

        try
        {
            for (int i = 0; i < partialCount; i++)
            {
                var handle = *(resHandles + i);
                _logger.LogTrace("Iterating over SkeletonResourceHandle #{i}:{x}", i, ((nint)handle).ToString("X"));

                if ((nint)handle == nint.Zero) continue;

                var curBones = handle->BoneCount;
                if (curBones <= 0 || curBones > 4096) continue;

                if (handle->FileName.Length > 1024) continue;

                var skeletonName = handle->FileName.ToString();
                if (string.IsNullOrEmpty(skeletonName)) continue;

                if (handle->HavokSkeleton == null) continue;

                var list = new List<ushort>((int)curBones);

                for (ushort boneIdx = 0; boneIdx < curBones; boneIdx++)
                {
                    var boneName = handle->HavokSkeleton->Bones[boneIdx].Name.String;
                    if (boneName == null) continue;

                    list.Add((ushort)(boneIdx + 1));
                }

                if (list.Count > 0)
                    outputIndices[skeletonName] = list;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not process skeleton data");
        }

        return (outputIndices.Count != 0 && outputIndices.Values.All(u => u.Count > 0)) ? outputIndices : null;
    }

    public unsafe Dictionary<string, List<ushort>>? GetBoneIndicesFromPap(string hash)
    {
        if (_configService.Current.BonesDictionary.TryGetValue(hash, out var bones)) return bones;

        var output = new Dictionary<string, List<ushort>>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var cacheEntity = _fileCacheManager.GetFileCacheByHash(hash);
            if (cacheEntity == null)
            {
                _logger.LogDebug("No cache entity for PAP hash {hash}, treating as unverifiable", hash);
                return output;
            }

            var filePath = cacheEntity.ResolvedFilepath;
            if (!File.Exists(filePath))
            {
                _logger.LogDebug("PAP file missing on disk for {hash} at {path}, treating as unverifiable", hash, filePath);
                return output;
            }

            using var reader = new BinaryReader(File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read));

            var streamLen = reader.BaseStream.Length;
            if (streamLen < 64) return output;

            reader.ReadInt32();
            reader.ReadInt32();
            reader.ReadInt16();
            reader.ReadInt16();

            var type = reader.ReadByte();
            // We only validate human-style PAP for player safety.
            if (type != 0) return output;

            reader.ReadByte();
            reader.ReadInt32();

            var havokPosition = reader.ReadInt32();
            var footerPosition = reader.ReadInt32();

            if (havokPosition <= 0 || footerPosition <= 0) return output;
            if (footerPosition <= havokPosition) return output;
            if (havokPosition >= streamLen || footerPosition > streamLen) return output;

            var havokDataSize = footerPosition - havokPosition;
            if (havokDataSize <= 8) return output;

            const int maxHavokBytes = 32 * 1024 * 1024;
            if (havokDataSize > maxHavokBytes) return output;

            reader.BaseStream.Position = havokPosition;
            var havokData = reader.ReadBytes(havokDataSize);
            if (havokData.Length != havokDataSize) return output;

            if (havokData.Length < 16)
                return output;

            var tempHavokDataPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) + ".hkx";
            var tempHavokDataPathAnsi = Marshal.StringToHGlobalAnsi(tempHavokDataPath);

            try
            {
                File.WriteAllBytes(tempHavokDataPath, havokData);

                var loadoptions = stackalloc hkSerializeUtil.LoadOptions[1];
                loadoptions->TypeInfoRegistry = hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry();
                loadoptions->ClassNameRegistry = hkBuiltinTypeRegistry.Instance()->GetClassNameRegistry();
                loadoptions->Flags = new hkFlags<hkSerializeUtil.LoadOptionBits, int>
                {
                    Storage = (int)(hkSerializeUtil.LoadOptionBits.Default)
                };

                var resource = hkSerializeUtil.LoadFromFile((byte*)tempHavokDataPathAnsi, null, loadoptions);
                if (resource == null) return output;

                var rootLevelName = @"hkRootLevelContainer"u8;
                fixed (byte* n1 = rootLevelName)
                {
                    var container = (hkRootLevelContainer*)resource->GetContentsPointer(n1, hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry());
                    if (container == null) return output;

                    var animationName = @"hkaAnimationContainer"u8;
                    fixed (byte* n2 = animationName)
                    {
                        var animContainer = (hkaAnimationContainer*)container->findObjectByName(n2, null);
                        if (animContainer == null) return output;

                        var bindingCount = animContainer->Bindings.Length;
                        if (bindingCount < 0 || bindingCount > 256) return output;

                        for (int i = 0; i < bindingCount; i++)
                        {
                            var binding = animContainer->Bindings[i].ptr;
                            if (binding == null) continue;

                            var sklNamePtr = binding->OriginalSkeletonName.String;
                            if (sklNamePtr == null) continue;

                            var boneTransform = binding->TransformTrackToBoneIndices;
                            if (boneTransform.Length <= 0 || boneTransform.Length > 8192) continue;

                            string name = sklNamePtr + "_" + i;
                            output[name] = [];

                            for (int boneIdx = 0; boneIdx < boneTransform.Length; boneIdx++)
                            {
                                output[name].Add((ushort)boneTransform[boneIdx]);
                            }

                            output[name].Sort();
                        }
                    }
                }

                return output;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not load havok data from PAP {path}", filePath);
                return output;
            }
            finally
            {
                Marshal.FreeHGlobal(tempHavokDataPathAnsi);
                try { File.Delete(tempHavokDataPath); } catch { }
            }
        }
        finally
        {
            // Cache even failures (empty) to avoid repeatedly hitting Havok for the same hash.
            _configService.Current.BonesDictionary[hash] = output;
            _configService.Save();
        }
    }

    public async Task<long> GetTrianglesByHash(string hash)
    {
        if (_configService.Current.TriangleDictionary.TryGetValue(hash, out var cachedTris) && cachedTris > 0)
            return cachedTris;

        if (_failedCalculatedTris.Contains(hash))
            return 0;

        var path = _fileCacheManager.GetFileCacheByHash(hash);
        if (path == null || !path.ResolvedFilepath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase))
            return 0;

        var filePath = path.ResolvedFilepath;

        try
        {
            _logger.LogDebug("Detected Model File {path}, calculating Tris", filePath);
            var file = new MdlFile(filePath);
            if (file.LodCount <= 0)
            {
                _failedCalculatedTris.Add(hash);
                _configService.Current.TriangleDictionary[hash] = 0;
                RequestTriangleConfigSave();
                return 0;
            }

            long tris = 0;
            for (int i = 0; i < file.LodCount; i++)
            {
                try
                {
                    var meshIdx = file.Lods[i].MeshIndex;
                    var meshCnt = file.Lods[i].MeshCount;
                    long indexCountSum = 0;
                    int meshEnd = meshIdx + meshCnt;
                    for (int m = meshIdx; m < meshEnd; m++)
                    {
                        indexCountSum += file.Meshes[m].IndexCount;
                    }

                    tris = indexCountSum / 3;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Could not load lod mesh {mesh} from path {path}", i, filePath);
                    continue;
                }

                if (tris > 0)
                {
                    _logger.LogDebug("TriAnalysis: {filePath} => {tris} triangles", filePath, tris);
                    _configService.Current.TriangleDictionary[hash] = tris;
                    RequestTriangleConfigSave();
                    break;
                }
            }

            return tris;
        }
        catch (Exception e)
        {

            _logger.LogDebug(e,"Could not parse file {file} while calculating triangles; marking tri count as 0 for this hash",filePath);

            _failedCalculatedTris.Add(hash);
            _configService.Current.TriangleDictionary[hash] = 0;
            RequestTriangleConfigSave();

            return 0;
        }
    }

    private void RequestTriangleConfigSave()
    {
        var now = DateTime.UtcNow;
        if (now < _nextTriangleConfigSaveUtc) return;

        _nextTriangleConfigSaveUtc = now + TriangleConfigSaveThrottle;
        _configService.Save();
    }
}
