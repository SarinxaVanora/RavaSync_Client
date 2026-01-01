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
    private readonly List<string> _failedCalculatedTris = [];

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



    //public unsafe Dictionary<string, List<ushort>>? GetSkeletonBoneIndices(GameObjectHandler handler)
    //{
    //    if (handler.Address == nint.Zero) return null;
    //    var chara = (CharacterBase*)(((Character*)handler.Address)->GameObject.DrawObject);
    //    if (chara->GetModelType() != CharacterBase.ModelType.Human) return null;
    //    var resHandles = chara->Skeleton->SkeletonResourceHandles;
    //    Dictionary<string, List<ushort>> outputIndices = [];
    //    try
    //    {
    //        for (int i = 0; i < chara->Skeleton->PartialSkeletonCount; i++)
    //        {
    //            var handle = *(resHandles + i);
    //            _logger.LogTrace("Iterating over SkeletonResourceHandle #{i}:{x}", i, ((nint)handle).ToString("X"));
    //            if ((nint)handle == nint.Zero) continue;
    //            var curBones = handle->BoneCount;
    //            // this is unrealistic, the filename shouldn't ever be that long
    //            if (handle->FileName.Length > 1024) continue;
    //            var skeletonName = handle->FileName.ToString();
    //            if (string.IsNullOrEmpty(skeletonName)) continue;
    //            outputIndices[skeletonName] = new();
    //            for (ushort boneIdx = 0; boneIdx < curBones; boneIdx++)
    //            {
    //                var boneName = handle->HavokSkeleton->Bones[boneIdx].Name.String;
    //                if (boneName == null) continue;
    //                outputIndices[skeletonName].Add((ushort)(boneIdx + 1));
    //            }
    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        _logger.LogWarning(ex, "Could not process skeleton data");
    //    }

    //    return (outputIndices.Count != 0 && outputIndices.Values.All(u => u.Count > 0)) ? outputIndices : null;
    //}

    //public unsafe Dictionary<string, List<ushort>>? GetBoneIndicesFromPap(string hash)
    //{
    //    if (_configService.Current.BonesDictionary.TryGetValue(hash, out var bones)) return bones;

    //    var cacheEntity = _fileCacheManager.GetFileCacheByHash(hash);
    //    if (cacheEntity == null) return null;

    //    using BinaryReader reader = new BinaryReader(File.Open(cacheEntity.ResolvedFilepath, FileMode.Open, FileAccess.Read, FileShare.Read));

    //    // most of this shit is from vfxeditor, surely nothing will change in the pap format :copium:
    //    reader.ReadInt32(); // ignore
    //    reader.ReadInt32(); // ignore
    //    reader.ReadInt16(); // read 2 (num animations)
    //    reader.ReadInt16(); // read 2 (modelid)
    //    var type = reader.ReadByte();// read 1 (type)
    //    if (type != 0) return null; // it's not human, just ignore it, whatever

    //    reader.ReadByte(); // read 1 (variant)
    //    reader.ReadInt32(); // ignore
    //    var havokPosition = reader.ReadInt32();
    //    var footerPosition = reader.ReadInt32();
    //    var havokDataSize = footerPosition - havokPosition;
    //    reader.BaseStream.Position = havokPosition;
    //    var havokData = reader.ReadBytes(havokDataSize);
    //    if (havokData.Length <= 8) return null; // no havok data

    //    var output = new Dictionary<string, List<ushort>>(StringComparer.OrdinalIgnoreCase);
    //    var tempHavokDataPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) + ".hkx";
    //    var tempHavokDataPathAnsi = Marshal.StringToHGlobalAnsi(tempHavokDataPath);

    //    try
    //    {
    //        File.WriteAllBytes(tempHavokDataPath, havokData);

    //        var loadoptions = stackalloc hkSerializeUtil.LoadOptions[1];
    //        loadoptions->TypeInfoRegistry = hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry();
    //        loadoptions->ClassNameRegistry = hkBuiltinTypeRegistry.Instance()->GetClassNameRegistry();
    //        loadoptions->Flags = new hkFlags<hkSerializeUtil.LoadOptionBits, int>
    //        {
    //            Storage = (int)(hkSerializeUtil.LoadOptionBits.Default)
    //        };

    //        var resource = hkSerializeUtil.LoadFromFile((byte*)tempHavokDataPathAnsi, null, loadoptions);
    //        if (resource == null)
    //        {
    //            throw new InvalidOperationException("Resource was null after loading");
    //        }

    //        var rootLevelName = @"hkRootLevelContainer"u8;
    //        fixed (byte* n1 = rootLevelName)
    //        {
    //            var container = (hkRootLevelContainer*)resource->GetContentsPointer(n1, hkBuiltinTypeRegistry.Instance()->GetTypeInfoRegistry());
    //            var animationName = @"hkaAnimationContainer"u8;
    //            fixed (byte* n2 = animationName)
    //            {
    //                var animContainer = (hkaAnimationContainer*)container->findObjectByName(n2, null);
    //                for (int i = 0; i < animContainer->Bindings.Length; i++)
    //                {
    //                    var binding = animContainer->Bindings[i].ptr;
    //                    var boneTransform = binding->TransformTrackToBoneIndices;
    //                    string name = binding->OriginalSkeletonName.String! + "_" + i;
    //                    output[name] = [];
    //                    for (int boneIdx = 0; boneIdx < boneTransform.Length; boneIdx++)
    //                    {
    //                        output[name].Add((ushort)boneTransform[boneIdx]);
    //                    }
    //                    output[name].Sort();
    //                }

    //            }
    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        _logger.LogWarning(ex, "Could not load havok file in {path}", tempHavokDataPath);
    //    }
    //    finally
    //    {
    //        Marshal.FreeHGlobal(tempHavokDataPathAnsi);
    //        File.Delete(tempHavokDataPath);
    //    }

    //    _configService.Current.BonesDictionary[hash] = output;
    //    _configService.Save();
    //    return output;
    //}

    public async Task<long> GetTrianglesByHash(string hash)
    {
        if (_configService.Current.TriangleDictionary.TryGetValue(hash, out var cachedTris) && cachedTris > 0)
            return cachedTris;

        if (_failedCalculatedTris.Contains(hash, StringComparer.Ordinal))
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
                _configService.Save();
                return 0;
            }

            long tris = 0;
            for (int i = 0; i < file.LodCount; i++)
            {
                try
                {
                    var meshIdx = file.Lods[i].MeshIndex;
                    var meshCnt = file.Lods[i].MeshCount;
                    tris = file.Meshes.Skip(meshIdx).Take(meshCnt).Sum(p => p.IndexCount) / 3;
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
                    _configService.Save();
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
            _configService.Save();

            return 0;
            //_logger.LogWarning(e,
            //    "Could not parse file {file}, treating cache entry as corrupt and deleting it",
            //    filePath);

            //try
            //{
            //    var entity = _fileCacheManager.GetFileCacheByHash(hash);
            //    if (entity != null)
            //    {
            //        try
            //        {
            //            _fileCacheManager.RemoveHashedFile(entity.Hash, entity.PrefixedFilePath);
            //        }
            //        catch (Exception exRemove)
            //        {
            //            _logger.LogDebug(exRemove,
            //                "Error while removing broken cache entry for {file}",
            //                filePath);
            //        }

            //        try
            //        {
            //            // Then delete the actual file on disk
            //            if (System.IO.File.Exists(entity.ResolvedFilepath))
            //                System.IO.File.Delete(entity.ResolvedFilepath);
            //        }
            //        catch (Exception exDelete)
            //        {
            //            _logger.LogDebug(exDelete,
            //                "Error while deleting broken cache file {file}",
            //                entity.ResolvedFilepath);
            //        }
            //    }
            //}
            //catch (Exception exOuter)
            //{
            //    _logger.LogDebug(exOuter,
            //        "Error during cache repair for corrupt model file {file}",
            //        filePath);
            //}
            //return 0;
        }
    }
}
