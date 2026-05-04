using Lumina.Data.Files;
using RavaSync.Utils;
using System;

namespace RavaSync.Services.Optimisation;

internal static class TextureCompressionPlanner
{
    internal enum Target
    {
        None = 0,
        BC1,
        BC3,
        BC5,
        BC7,
    }

    public static bool TryChooseTarget(string? hintPath, TexFile.TextureFormat srcFormat, out Target target)
    {
        target = Target.None;

        bool isRgba32 = srcFormat == TexFile.TextureFormat.B8G8R8A8 || srcFormat == TexFile.TextureFormat.A8R8G8B8;
        if (!isRgba32)
            return false;

        return TryChooseTargetForPath(hintPath, out target);
    }

    public static bool TryChooseTargetForPath(string? hintPath, out Target target)
    {
        target = Target.None;

        var profile = TextureContentProfile.Analyze(hintPath);
        if (profile.IsStrictNormalMap)
        {
            target = Target.BC3;
            return true;
        }

        if (profile.IsMaterialMask)
        {
            target = Target.BC3;
            return true;
        }

        if (profile.IsDefinitelyOpaqueDiffuse)
        {
            target = Target.BC1;
            return true;
        }

        if (profile.ShouldUsePremiumColorTarget)
        {
            target = Target.BC7;
            return true;
        }

        target = Target.BC7;
        return true;
    }

    public static bool TryParseFormat(string? fmt, out TexFile.TextureFormat parsed)
    {
        parsed = default;
        if (string.IsNullOrWhiteSpace(fmt)) return false;
        return Enum.TryParse(fmt.Trim(), ignoreCase: true, out parsed);
    }
}
