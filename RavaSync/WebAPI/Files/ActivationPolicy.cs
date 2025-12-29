using System;
using System.Linq;

namespace RavaSync.WebAPI.Files
{
    public static class ActivationPolicy
    {
        private static readonly string[] HardDelayExts =
            [".pap", ".tmb", ".tmb2", ".sklb", ".phyb", ".scd", ".avfx"];

        private static readonly string[] SoftDelayExts =
            [".mdl", ".mtrl"];

        public static bool IsHardDelayed(string path)
            => HardDelayExts.Any(ext => path.EndsWith(ext, StringComparison.OrdinalIgnoreCase));

        public static bool IsSoftDelayed(string path)
            => SoftDelayExts.Any(ext => path.EndsWith(ext, StringComparison.OrdinalIgnoreCase));
    }

    public sealed record PendingFile(
            string QuarantinePath,
            string FinalPath,
            string FileHash,
            bool HardDelay,
            bool SoftDelay,
            nint? ActorAddress
        );
}
