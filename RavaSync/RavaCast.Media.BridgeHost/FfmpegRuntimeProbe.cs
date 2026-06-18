namespace RavaCast.Media.BridgeHost;

internal static class FfmpegRuntimeProbe
{
    public static string Describe()
    {
        var ffmpeg = FindExecutable("ffmpeg");
        var ffprobe = FindExecutable("ffprobe");
        if (!string.IsNullOrWhiteSpace(ffmpeg))
            return "FFmpeg found for Direct Stream v2 live video: " + ffmpeg + (string.IsNullOrWhiteSpace(ffprobe) ? string.Empty : "; ffprobe: " + ffprobe) + ".";
        return "FFmpeg was not found. Direct Stream v2 transport can negotiate, but live H.264 encode/decode needs ffmpeg beside BridgeHost, copied from RavaCast.Media.Runtime/<rid>/native, or available in PATH.";
    }

    public static bool TryFindFfmpeg(out string path, out string detail)
    {
        path = FindExecutable("ffmpeg");
        if (!string.IsNullOrWhiteSpace(path))
        {
            detail = "FFmpeg found: " + path;
            return true;
        }

        detail = "FFmpeg was not found beside BridgeHost or in PATH. For packaged builds, put ffmpeg in RavaCast.Media.Runtime/<rid>/native so MSBuild bundles it automatically.";
        return false;
    }

    private static string FindExecutable(string name)
    {
        try
        {
            var exe = OperatingSystem.IsWindows() ? name + ".exe" : name;
            var local = Path.Combine(AppContext.BaseDirectory, exe);
            if (File.Exists(local)) return local;

            var paths = (Environment.GetEnvironmentVariable("PATH") ?? string.Empty).Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var path in paths)
            {
                var candidate = Path.Combine(path, exe);
                if (File.Exists(candidate)) return candidate;
            }
        }
        catch { }

        return string.Empty;
    }
}
