using System;

namespace RavaSync.Utils;

internal readonly record struct TextureContentProfile(
    bool IsCharacterTexture,
    bool IsEquipmentTexture,
    bool IsUiOrOverlaySensitive,
    bool IsFaceOrEyeSensitive,
    bool IsHairOrFurSensitive,
    bool IsSkinOrBody,
    bool IsStrictNormalMap,
    bool IsMaterialMask,
    bool IsDefinitelyOpaqueDiffuse,
    bool IsAlphaSensitive,
    bool PreserveHighDetail,
    bool ShouldUsePremiumColorTarget,
    int PreferredMaxDimension)
{
    public static TextureContentProfile Analyze(string? path)
    {
        string s = Normalize(path);

        bool isCharacterTexture = s.StartsWith("chara/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/chara/", StringComparison.OrdinalIgnoreCase);

        bool isEquipmentTexture = s.Contains("/equipment/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/accessory/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/weapon/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/demihuman/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/monster/", StringComparison.OrdinalIgnoreCase);

        bool isUiOrOverlaySensitive = s.Contains("/ui/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("icon", StringComparison.OrdinalIgnoreCase)
            || s.Contains("decal", StringComparison.OrdinalIgnoreCase)
            || s.Contains("overlay", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tattoo", StringComparison.OrdinalIgnoreCase)
            || s.Contains("makeup", StringComparison.OrdinalIgnoreCase)
            || s.Contains("sticker", StringComparison.OrdinalIgnoreCase)
            || s.Contains("paint", StringComparison.OrdinalIgnoreCase)
            || s.Contains("alpha", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_a.", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_id.", StringComparison.OrdinalIgnoreCase);

        bool isFaceOrEyeSensitive = s.Contains("face", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_fac_", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_iri_", StringComparison.OrdinalIgnoreCase)
            || s.Contains("eye", StringComparison.OrdinalIgnoreCase)
            || s.Contains("iris", StringComparison.OrdinalIgnoreCase)
            || s.Contains("lash", StringComparison.OrdinalIgnoreCase)
            || s.Contains("brow", StringComparison.OrdinalIgnoreCase)
            || s.Contains("lip", StringComparison.OrdinalIgnoreCase)
            || s.Contains("mouth", StringComparison.OrdinalIgnoreCase);

        bool isHairOrFurSensitive = s.Contains("hair", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tail", StringComparison.OrdinalIgnoreCase)
            || s.Contains("ear", StringComparison.OrdinalIgnoreCase)
            || s.Contains("feather", StringComparison.OrdinalIgnoreCase)
            || s.Contains("fur", StringComparison.OrdinalIgnoreCase);

        bool isSkinOrBody = s.Contains("skin", StringComparison.OrdinalIgnoreCase)
            || s.Contains("body", StringComparison.OrdinalIgnoreCase)
            || s.Contains("hand", StringComparison.OrdinalIgnoreCase)
            || s.Contains("leg", StringComparison.OrdinalIgnoreCase)
            || s.Contains("arm", StringComparison.OrdinalIgnoreCase);

        bool isStrictNormalMap = s.EndsWith("_n.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/normal/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_normal", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_nor", StringComparison.OrdinalIgnoreCase);

        bool isMaterialMask = s.EndsWith("_m.tex", StringComparison.OrdinalIgnoreCase)
            || s.EndsWith("_s.tex", StringComparison.OrdinalIgnoreCase)
            || s.EndsWith("_r.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_mask", StringComparison.OrdinalIgnoreCase)
            || s.Contains("spec", StringComparison.OrdinalIgnoreCase)
            || s.Contains("rough", StringComparison.OrdinalIgnoreCase)
            || s.Contains("metal", StringComparison.OrdinalIgnoreCase)
            || s.Contains("occlusion", StringComparison.OrdinalIgnoreCase)
            || s.Contains("multi", StringComparison.OrdinalIgnoreCase);

        bool looksDiffuse = s.EndsWith("_d.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_diff", StringComparison.OrdinalIgnoreCase)
            || s.Contains("diffuse", StringComparison.OrdinalIgnoreCase)
            || s.Contains("albedo", StringComparison.OrdinalIgnoreCase)
            || s.Contains("basecolor", StringComparison.OrdinalIgnoreCase)
            || s.Contains("color", StringComparison.OrdinalIgnoreCase);

        bool maybeAlphaSensitive = isUiOrOverlaySensitive
            || isFaceOrEyeSensitive
            || isHairOrFurSensitive
            || s.Contains("mask", StringComparison.OrdinalIgnoreCase)
            || s.Contains("glass", StringComparison.OrdinalIgnoreCase)
            || s.Contains("lace", StringComparison.OrdinalIgnoreCase)
            || s.Contains("fishnet", StringComparison.OrdinalIgnoreCase)
            || s.Contains("transparent", StringComparison.OrdinalIgnoreCase)
            || s.Contains("sheer", StringComparison.OrdinalIgnoreCase);

        bool isDefinitelyOpaqueDiffuse = looksDiffuse && !maybeAlphaSensitive;
        bool isAlphaSensitive = isUiOrOverlaySensitive || isFaceOrEyeSensitive || isHairOrFurSensitive || maybeAlphaSensitive;
        bool preserveHighDetail = isUiOrOverlaySensitive || isFaceOrEyeSensitive || isHairOrFurSensitive;
        bool shouldUsePremiumColorTarget = isUiOrOverlaySensitive || isFaceOrEyeSensitive || isHairOrFurSensitive || isSkinOrBody || isCharacterTexture || isEquipmentTexture;

        int preferredMaxDimension = preserveHighDetail ? 4096 : 0;

        return new TextureContentProfile(
            isCharacterTexture,
            isEquipmentTexture,
            isUiOrOverlaySensitive,
            isFaceOrEyeSensitive,
            isHairOrFurSensitive,
            isSkinOrBody,
            isStrictNormalMap,
            isMaterialMask,
            isDefinitelyOpaqueDiffuse,
            isAlphaSensitive,
            preserveHighDetail,
            shouldUsePremiumColorTarget,
            preferredMaxDimension);
    }

    private static string Normalize(string? s)
        => (s ?? string.Empty).Replace('\\', '/').ToLowerInvariant();
}
