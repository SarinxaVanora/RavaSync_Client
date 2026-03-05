using Lumina.Data.Files;
using System;

namespace RavaSync.Services;

internal static class TextureCompressionPlanner
{
    internal enum Target
    {
        None = 0,
        BC1,
        BC3,
        BC4,
        BC5,
        BC7,
    }

    public static bool TryChooseTarget(string? hintPath, TexFile.TextureFormat srcFormat, out Target target)
    {
        target = Target.None;

        var hint = Normalize(hintPath);

        bool isRgba32 =
            srcFormat == TexFile.TextureFormat.B8G8R8A8 ||
            srcFormat == TexFile.TextureFormat.A8R8G8B8;

        if (!isRgba32)
            return false;

        // Normals
        if (LooksLikeNormalStrict(hint))
        {
            target = Target.BC3;
            return true;
        }

        // Equipment
        if (IsEquipmentTexture(hint))
        {
            target = Target.BC7;
            return true;
        }

        // UI / decals / overlays etc
        if (LooksAlphaSensitive(hint))
        {
            target = Target.BC7;
            return true;
        }

        // Character textures: safe default
        if (IsCharacterTexture(hint))
        {
            target = Target.BC7;
            return true;
        }

        // Non-equipment opaque diffuse can use BC1.
        if (LooksDefinitelyOpaqueDiffuse(hint))
        {
            target = Target.BC1;
            return true;
        }

        // Default safe choice.
        target = Target.BC7;
        return true;
    }

    private static bool IsCharacterTexture(string s)
        => s.StartsWith("chara/", StringComparison.OrdinalIgnoreCase)
        || s.Contains("/chara/", StringComparison.OrdinalIgnoreCase);

    private static bool IsEquipmentTexture(string s)
        => s.Contains("/equipment/", StringComparison.OrdinalIgnoreCase)
        || s.Contains("chara/equipment/", StringComparison.OrdinalIgnoreCase);

    private static bool LooksAlphaSensitive(string s)
    {
        return s.Contains("/ui/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("icon", StringComparison.OrdinalIgnoreCase)
            || s.Contains("decal", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tattoo", StringComparison.OrdinalIgnoreCase)
            || s.Contains("makeup", StringComparison.OrdinalIgnoreCase)
            || s.Contains("overlay", StringComparison.OrdinalIgnoreCase)
            || s.Contains("mask", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_m.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_id.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_a.", StringComparison.OrdinalIgnoreCase)
            || s.Contains("alpha", StringComparison.OrdinalIgnoreCase)
            || s.Contains("hair", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tail", StringComparison.OrdinalIgnoreCase)
            || s.Contains("feather", StringComparison.OrdinalIgnoreCase);
    }

    private static bool LooksLikeNormalStrict(string s)
    {
        return s.EndsWith("_n.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("/normal/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_normal", StringComparison.OrdinalIgnoreCase);
    }

    private static bool LooksDefinitelyOpaqueDiffuse(string s)
    {
        bool looksDiffuse =
            s.EndsWith("_d.tex", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_diff", StringComparison.OrdinalIgnoreCase)
            || s.Contains("diffuse", StringComparison.OrdinalIgnoreCase)
            || s.Contains("albedo", StringComparison.OrdinalIgnoreCase)
            || s.Contains("basecolor", StringComparison.OrdinalIgnoreCase);

        if (!looksDiffuse) return false;

        bool maybeAlpha =
            s.Contains("/ui/", StringComparison.OrdinalIgnoreCase)
            || s.Contains("icon", StringComparison.OrdinalIgnoreCase)
            || s.Contains("decal", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tattoo", StringComparison.OrdinalIgnoreCase)
            || s.Contains("makeup", StringComparison.OrdinalIgnoreCase)
            || s.Contains("overlay", StringComparison.OrdinalIgnoreCase)
            || s.Contains("mask", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_m.", StringComparison.OrdinalIgnoreCase)
            || s.Contains("_a.", StringComparison.OrdinalIgnoreCase)
            || s.Contains("alpha", StringComparison.OrdinalIgnoreCase)
            || s.Contains("hair", StringComparison.OrdinalIgnoreCase)
            || s.Contains("tail", StringComparison.OrdinalIgnoreCase)
            || s.Contains("feather", StringComparison.OrdinalIgnoreCase);

        return !maybeAlpha;
    }

    public static bool TryParseFormat(string? fmt, out TexFile.TextureFormat parsed)
    {
        parsed = default;
        if (string.IsNullOrWhiteSpace(fmt)) return false;

        // Your analysis format strings are like "BC7", "A8R8G8B8", etc.
        return Enum.TryParse(fmt.Trim(), ignoreCase: true, out parsed);
    }

    private static string Normalize(string? s)
        => (s ?? string.Empty).Replace('\\', '/').ToLowerInvariant();

    private static bool LooksLikeNormal(string s)
        => s.Contains("_n.tex", StringComparison.OrdinalIgnoreCase)
        || s.Contains("/normal", StringComparison.OrdinalIgnoreCase)
        || s.Contains("_nor", StringComparison.OrdinalIgnoreCase);

    private static bool LooksLikeMask(string s)
        => s.Contains("_m.tex", StringComparison.OrdinalIgnoreCase)
        || s.Contains("_mask", StringComparison.OrdinalIgnoreCase)
        || s.Contains("_s.tex", StringComparison.OrdinalIgnoreCase)
        || s.Contains("_r.tex", StringComparison.OrdinalIgnoreCase);

    private static bool LooksLikeAlpha(string s)
        => s.Contains("/ui/", StringComparison.OrdinalIgnoreCase)
        || s.Contains("icon", StringComparison.OrdinalIgnoreCase)
        || s.Contains("decal", StringComparison.OrdinalIgnoreCase)
        || s.Contains("_a.", StringComparison.OrdinalIgnoreCase);
}