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

public static class CharacterSyncManifestUtility
{
    private const string CarrierPropertyName = "__ravasync_sync_manifest";
    private const string RawCarrierPropertyName = "__ravasync_raw";
    private const int CurrentVersion = 1;

    public sealed record SyncManifestAsset(int ok, string gp, string h, string fs, string k, string c);
    public sealed record SyncManifestPayload(int v, string pf, string af, string mf, int total, int critical, SyncManifestAsset[] assets);

    public static bool TryEmbed(CharacterData? charaData, out SyncManifestPayload? manifest)
    {
        manifest = null;
        if (charaData == null)
            return false;

        manifest = BuildManifest(charaData);
        if (manifest.assets.Length == 0)
            return false;

        var payloadBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(manifest)));
        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryEmbedIntoJsonCarrier(petNamesData, payloadBase64, allowEmptyCarrier: true, out var updatedPetNamesData))
        {
            charaData.PetNamesData = updatedPetNamesData;
            return true;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryEmbedIntoJsonCarrier(moodlesData, payloadBase64, allowEmptyCarrier: false, out var updatedMoodlesData))
        {
            charaData.MoodlesData = updatedMoodlesData;
            return true;
        }

        return false;
    }

    public static bool TryExtract(CharacterData? charaData, out SyncManifestPayload? manifest)
    {
        manifest = null;
        if (charaData == null)
            return false;

        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryExtractFromJsonCarrier(petNamesData, out var sanitizedPetNamesData, out manifest))
        {
            charaData.PetNamesData = sanitizedPetNamesData;
            return manifest != null;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryExtractFromJsonCarrier(moodlesData, out var sanitizedMoodlesData, out manifest))
        {
            charaData.MoodlesData = sanitizedMoodlesData;
            return manifest != null;
        }

        return false;
    }

    public static SyncManifestPayload BuildManifest(CharacterData charaData)
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

    public static int MergeManifestIntoCharacterData(CharacterData? charaData, SyncManifestPayload? manifest)
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

    private static bool TryEmbedIntoJsonCarrier(string carrier, string payloadBase64, bool allowEmptyCarrier, out string updatedCarrier)
    {
        updatedCarrier = carrier ?? string.Empty;
        if (!string.IsNullOrEmpty(updatedCarrier))
        {
            var parsed = ParseJsonObject(updatedCarrier);
            if (parsed != null)
            {
                parsed[CarrierPropertyName] = payloadBase64;
                updatedCarrier = parsed.ToJsonString();
                return true;
            }

            var wrapped = new JsonObject
            {
                [CarrierPropertyName] = payloadBase64,
                [RawCarrierPropertyName] = Convert.ToBase64String(Encoding.UTF8.GetBytes(updatedCarrier))
            };

            updatedCarrier = wrapped.ToJsonString();
            return true;
        }

        if (!allowEmptyCarrier)
            return false;

        var root = new JsonObject { [CarrierPropertyName] = payloadBase64 };
        updatedCarrier = root.ToJsonString();
        return true;
    }

    private static bool TryExtractFromJsonCarrier(string carrier, out string sanitizedCarrier, out SyncManifestPayload? manifest)
    {
        sanitizedCarrier = carrier ?? string.Empty;
        manifest = null;
        if (string.IsNullOrWhiteSpace(sanitizedCarrier))
            return false;

        var root = ParseJsonObject(sanitizedCarrier);
        if (root == null)
            return false;

        var encoded = root[CarrierPropertyName]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(encoded))
            return false;

        try
        {
            var json = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
            manifest = JsonSerializer.Deserialize<SyncManifestPayload>(json);
        }
        catch
        {
            manifest = null;
            return false;
        }

        if (manifest == null || manifest.v != CurrentVersion)
            return false;

        root.Remove(CarrierPropertyName);
        if (root[RawCarrierPropertyName] is JsonNode rawNode && !root.Any(k => k.Key != RawCarrierPropertyName))
        {
            try
            {
                var rawEncoded = rawNode.GetValue<string>();
                sanitizedCarrier = string.IsNullOrWhiteSpace(rawEncoded) ? string.Empty : Encoding.UTF8.GetString(Convert.FromBase64String(rawEncoded));
            }
            catch
            {
                sanitizedCarrier = string.Empty;
            }
        }
        else
        {
            sanitizedCarrier = root.Count == 0 ? string.Empty : root.ToJsonString();
        }

        return true;
    }

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
}
