using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Services;

namespace RavaSync.Utils;

public sealed class CharacterRavaSidecarUtility
{
    private const string RootPropertyName = "__ravasync";
    private const string RawCarrierPropertyName = "__ravasync_raw";
    private const string VersionPropertyName = "v";
    private const string PerformancePropertyName = "performance";
    private const string SyncManifestPropertyName = "syncManifest";
    private const int CurrentVersion = 1;

    private sealed record PerformancePayload(int v, long vb, long tr);
    public sealed record SyncManifestAsset(int ok, string gp, string h, string fs, string k, string c);
    public sealed record SyncManifestPayload(int v, string pf, string af, string mf, int total, int critical, SyncManifestAsset[] assets);

    public bool TryEmbedPerformance(CharacterData? charaData, long vramBytes, long triangles)
    {
        if (charaData == null)
            return false;

        var payload = new PerformancePayload(CurrentVersion, Math.Max(0, vramBytes), Math.Max(0, triangles));
        var payloadBase64 = EncodePayload(payload);
        return TryEmbedPayload(charaData, PerformancePropertyName, payloadBase64);
    }

    public bool TryExtractPerformance(CharacterData? charaData, out long vramBytes, out long triangles)
    {
        vramBytes = 0;
        triangles = 0;

        if (!TryExtractPayload<PerformancePayload>(charaData, PerformancePropertyName, p => p?.v == CurrentVersion, out var payload))
            return false;

        vramBytes = Math.Max(0, payload?.vb ?? 0);
        triangles = Math.Max(0, payload?.tr ?? 0);
        return true;
    }

    public bool TryEmbedSyncManifest(CharacterData? charaData, out SyncManifestPayload? manifest)
    {
        manifest = null;
        if (charaData == null)
            return false;

        manifest = BuildManifest(charaData);
        if (manifest.assets.Length == 0)
            return false;

        return TryEmbedPayload(charaData, SyncManifestPropertyName, EncodePayload(manifest));
    }

    public bool TryExtractSyncManifest(CharacterData? charaData, out SyncManifestPayload? manifest)
        => TryExtractPayload(charaData, SyncManifestPropertyName, p => p?.v == CurrentVersion, out manifest);

    public bool StripSidecar(CharacterData? charaData)
    {
        if (charaData == null)
            return false;

        var changed = false;
        if (TryStripCarrier(charaData.PetNamesData ?? string.Empty, out var strippedPetNamesData))
        {
            charaData.PetNamesData = strippedPetNamesData;
            changed = true;
        }

        if (TryStripCarrier(charaData.MoodlesData ?? string.Empty, out var strippedMoodlesData))
        {
            charaData.MoodlesData = strippedMoodlesData;
            changed = true;
        }

        return changed;
    }

    public SyncManifestPayload BuildManifest(CharacterData charaData)
    {
        var assets = new List<SyncManifestAsset>();
        foreach (var objectFiles in charaData.FileReplacements.OrderBy(k => (int)k.Key))
        {
            foreach (var file in objectFiles.Value ?? [])
            {
                var hash = file.Hash ?? string.Empty;
                var fileSwap = file.FileSwapPath ?? string.Empty;
                foreach (var gamePathRaw in file.GamePaths ?? [])
                {
                    var gamePath = NormalizeGamePath(gamePathRaw);
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    var kind = ClassifyKind(gamePath, hash, fileSwap);
                    var criticality = ClassifyCriticality(gamePath, kind);
                    assets.Add(new SyncManifestAsset((int)objectFiles.Key, gamePath, hash, fileSwap, kind, criticality));
                }
            }
        }

        var orderedAssets = assets
            .GroupBy(a => $"{a.ok}|{a.gp}|{a.h}|{a.fs}", StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .OrderBy(a => a.ok)
            .ThenBy(a => a.gp, StringComparer.OrdinalIgnoreCase)
            .ThenBy(a => a.h, StringComparer.OrdinalIgnoreCase)
            .ThenBy(a => a.fs, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var critical = orderedAssets.Count(a => !string.Equals(a.c, "Normal", StringComparison.Ordinal));
        var payloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(charaData);
        var assetFingerprint = ComputeManifestAssetFingerprint(orderedAssets);
        var manifestFingerprint = PairApplyUtilities.ComputePathSetFingerprint(orderedAssets.Select(a => $"{a.ok}:{a.gp}:{a.h}:{a.fs}"));
        return new SyncManifestPayload(CurrentVersion, payloadFingerprint, assetFingerprint, manifestFingerprint, orderedAssets.Length, critical, orderedAssets);
    }

    public int MergeManifestIntoCharacterData(CharacterData? charaData, SyncManifestPayload? manifest)
    {
        if (charaData == null || manifest?.assets == null || manifest.assets.Length == 0)
            return 0;

        var added = 0;
        foreach (var asset in manifest.assets)
        {
            if (!Enum.IsDefined(typeof(ObjectKind), asset.ok))
                continue;

            var objectKind = (ObjectKind)asset.ok;
            var gamePath = NormalizeGamePath(asset.gp);
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            var hash = asset.h ?? string.Empty;
            var fileSwap = asset.fs ?? string.Empty;
            if (string.IsNullOrWhiteSpace(hash) && string.IsNullOrWhiteSpace(fileSwap))
                continue;

            if (!charaData.FileReplacements.TryGetValue(objectKind, out var replacements) || replacements == null)
            {
                replacements = [];
                charaData.FileReplacements[objectKind] = replacements;
            }

            var existing = replacements.FirstOrDefault(r => string.Equals(r.Hash ?? string.Empty, hash, StringComparison.OrdinalIgnoreCase)
                && string.Equals(r.FileSwapPath ?? string.Empty, fileSwap, StringComparison.OrdinalIgnoreCase));

            if (existing != null)
            {
                var gamePaths = (existing.GamePaths ?? []).ToList();
                if (!gamePaths.Any(p => string.Equals(NormalizeGamePath(p), gamePath, StringComparison.OrdinalIgnoreCase)))
                {
                    gamePaths.Add(gamePath);
                    existing.GamePaths = gamePaths.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
                    added++;
                }

                continue;
            }

            replacements.Add(new FileReplacementData { Hash = hash, FileSwapPath = fileSwap, GamePaths = [gamePath] });
            added++;
        }

        return added;
    }

    private static bool TryEmbedPayload(CharacterData charaData, string payloadName, string payloadBase64)
    {
        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryEmbedIntoCarrier(petNamesData, payloadName, payloadBase64, allowEmptyCarrier: true, out var updatedPetNamesData))
        {
            charaData.PetNamesData = updatedPetNamesData;
            return true;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryEmbedIntoCarrier(moodlesData, payloadName, payloadBase64, allowEmptyCarrier: false, out var updatedMoodlesData))
        {
            charaData.MoodlesData = updatedMoodlesData;
            return true;
        }

        return false;
    }

    private static bool TryExtractPayload<TPayload>(CharacterData? charaData, string payloadName, Func<TPayload?, bool> isValid, out TPayload? payload)
    {
        payload = default;
        if (charaData == null)
            return false;

        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryExtractFromCarrier(petNamesData, payloadName, isValid, out var sanitizedPetNamesData, out payload))
        {
            charaData.PetNamesData = sanitizedPetNamesData;
            return payload != null;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryExtractFromCarrier(moodlesData, payloadName, isValid, out var sanitizedMoodlesData, out payload))
        {
            charaData.MoodlesData = sanitizedMoodlesData;
            return payload != null;
        }

        return false;
    }

    private static bool TryEmbedIntoCarrier(string carrier, string payloadName, string payloadBase64, bool allowEmptyCarrier, out string updatedCarrier)
    {
        updatedCarrier = carrier ?? string.Empty;
        JsonObject root;

        if (!string.IsNullOrEmpty(updatedCarrier))
        {
            root = ParseJsonObject(updatedCarrier) ?? new JsonObject
            {
                [RawCarrierPropertyName] = Convert.ToBase64String(Encoding.UTF8.GetBytes(updatedCarrier))
            };
        }
        else
        {
            if (!allowEmptyCarrier)
                return false;

            root = [];
        }

        var sidecar = EnsureSidecarRoot(root);
        sidecar[VersionPropertyName] = CurrentVersion;
        sidecar[payloadName] = payloadBase64;
        updatedCarrier = root.ToJsonString();
        return true;
    }

    private static bool TryExtractFromCarrier<TPayload>(string carrier, string payloadName, Func<TPayload?, bool> isValid, out string sanitizedCarrier, out TPayload? payload)
    {
        sanitizedCarrier = carrier ?? string.Empty;
        payload = default;
        if (string.IsNullOrWhiteSpace(sanitizedCarrier))
            return false;

        var root = ParseJsonObject(sanitizedCarrier);
        if (root == null || root[RootPropertyName] is not JsonObject sidecar)
            return false;

        var encoded = sidecar[payloadName]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(encoded))
            return false;

        try
        {
            payload = JsonSerializer.Deserialize<TPayload>(Encoding.UTF8.GetString(Convert.FromBase64String(encoded)));
        }
        catch
        {
            payload = default;
            return false;
        }

        if (!isValid(payload))
        {
            payload = default;
            return false;
        }

        sidecar.Remove(payloadName);
        CleanupSidecarAfterRemoval(root, sidecar);
        sanitizedCarrier = SanitizeCarrier(root);
        return true;
    }

    private static bool TryStripCarrier(string carrier, out string strippedCarrier)
    {
        strippedCarrier = carrier ?? string.Empty;
        if (string.IsNullOrWhiteSpace(strippedCarrier))
            return false;

        var root = ParseJsonObject(strippedCarrier);
        if (root == null || !root.ContainsKey(RootPropertyName))
            return false;

        root.Remove(RootPropertyName);
        strippedCarrier = SanitizeCarrier(root);
        return true;
    }

    private static JsonObject EnsureSidecarRoot(JsonObject root)
    {
        if (root[RootPropertyName] is JsonObject sidecar)
            return sidecar;

        sidecar = [];
        root[RootPropertyName] = sidecar;
        return sidecar;
    }

    private static void CleanupSidecarAfterRemoval(JsonObject root, JsonObject sidecar)
    {
        var remainingPayloadKeys = sidecar.Where(k => !string.Equals(k.Key, VersionPropertyName, StringComparison.Ordinal)).ToList();
        if (remainingPayloadKeys.Count == 0)
            root.Remove(RootPropertyName);
    }

    private static string SanitizeCarrier(JsonObject root)
    {
        if (root[RawCarrierPropertyName] is JsonNode rawNode && !root.Any(k => !string.Equals(k.Key, RawCarrierPropertyName, StringComparison.Ordinal)))
        {
            try
            {
                var rawEncoded = rawNode.GetValue<string>();
                return string.IsNullOrWhiteSpace(rawEncoded) ? string.Empty : Encoding.UTF8.GetString(Convert.FromBase64String(rawEncoded));
            }
            catch
            {
                return string.Empty;
            }
        }

        return root.Count == 0 ? string.Empty : root.ToJsonString();
    }

    private static string EncodePayload<TPayload>(TPayload payload) => Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload)));

    private static JsonObject? ParseJsonObject(string value)
    {
        try
        {
            return JsonNode.Parse(value) as JsonObject;
        }
        catch
        {
            return null;
        }
    }

    private static string NormalizeGamePath(string? value) => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Replace('\\', '/').Trim().ToLowerInvariant();

    private static string ClassifyKind(string gamePath, string hash, string fileSwap)
    {
        if (string.IsNullOrWhiteSpace(hash) && !string.IsNullOrWhiteSpace(fileSwap))
            return "FileSwap";

        var extension = System.IO.Path.GetExtension(gamePath);
        return extension.ToLowerInvariant() switch
        {
            ".mdl" => "Model",
            ".mtrl" => "Material",
            ".tex" => "Texture",
            ".atex" => "Vfx",
            ".pap" => "Animation",
            ".tmb" or ".tmb2" => "Timeline",
            ".avfx" or ".shpk" or ".eid" or ".skp" => "Vfx",
            ".scd" => "Sound",
            ".sklb" => "Skeleton",
            ".phyb" or ".pbd" => "Physics",
            _ => "Unknown",
        };
    }

    private static string ClassifyCriticality(string gamePath, string kind)
    {
        if (PairApplyUtilities.IsAnimationCriticalGamePath(gamePath))
            return "AnimationCritical";

        if (PairApplyUtilities.IsVfxCriticalGamePath(gamePath))
            return "VfxCritical";

        if (PairApplyUtilities.IsVfxModelSupportGamePath(gamePath))
            return "VfxSupportCritical";

        return kind is "Model" or "Material" or "Texture" ? "AppearanceCritical" : "Normal";
    }

    private static string ComputeManifestAssetFingerprint(IEnumerable<SyncManifestAsset> assets)
        => PairApplyUtilities.ComputePathSetFingerprint(assets.Select(a => $"{a.ok}:{a.gp}:{a.h}:{a.fs}:{a.k}:{a.c}"));
}
