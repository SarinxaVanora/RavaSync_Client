using System.Reflection;

namespace RavaSync.Utils;

public static class PluginVersion
{
    private static readonly Lazy<Version> CurrentVersion = new(ResolveCurrentVersion);

    public static Version Current => CurrentVersion.Value;

    public static string CurrentText => Format(Current);

    public static string Format(Version? version)
    {
        if (version == null)
            return "unknown";

        if (version.Revision >= 0)
            return version.ToString(4);

        if (version.Build >= 0)
            return version.ToString(3);

        return version.ToString(2);
    }

    private static Version ResolveCurrentVersion()
    {
        var assembly = Assembly.GetExecutingAssembly();

        if (TryParseVersionPrefix(assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion, out var informationalVersion))
            return informationalVersion;

        if (TryParseVersionPrefix(assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version, out var fileVersion))
            return fileVersion;

        return assembly.GetName().Version ?? new Version(0, 0, 0, 0);
    }

    private static bool TryParseVersionPrefix(string? value, out Version version)
    {
        version = new Version(0, 0, 0, 0);

        if (string.IsNullOrWhiteSpace(value))
            return false;

        var end = 0;
        while (end < value.Length && (char.IsDigit(value[end]) || value[end] == '.'))
            end++;

        if (end == 0)
            return false;

        var versionText = value[..end].TrimEnd('.');
        if (!Version.TryParse(versionText, out var parsed) || parsed == null)
            return false;

        version = parsed;
        return true;
    }
}
