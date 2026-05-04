using RavaSync.API.Data;

namespace RavaSync.PlayerData.Data;

public class FileReplacementDataComparer : IEqualityComparer<FileReplacementData>
{
    private static readonly FileReplacementDataComparer _instance = new();

    private FileReplacementDataComparer()
    { }

    public static FileReplacementDataComparer Instance => _instance;

    public bool Equals(FileReplacementData? x, FileReplacementData? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x == null || y == null) return false;

        return string.Equals(x.Hash ?? string.Empty, y.Hash ?? string.Empty, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.FileSwapPath ?? string.Empty, y.FileSwapPath ?? string.Empty, StringComparison.OrdinalIgnoreCase)
            && ComparePathSets(x.GamePaths, y.GamePaths);
    }

    public int GetHashCode(FileReplacementData obj)
    {
        return HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Hash ?? string.Empty),
            StringComparer.OrdinalIgnoreCase.GetHashCode(obj.FileSwapPath ?? string.Empty),
            GetOrderIndependentPathHashCode(obj.GamePaths));
    }

    private static bool ComparePathSets(IEnumerable<string>? first, IEnumerable<string>? second)
    {
        var firstSet = BuildPathSet(first);
        var secondSet = BuildPathSet(second);

        return firstSet.SetEquals(secondSet);
    }

    private static HashSet<string> BuildPathSet(IEnumerable<string>? source)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (source == null) return result;

        foreach (var path in source)
        {
            if (!string.IsNullOrWhiteSpace(path))
                result.Add(path.Replace('\\', '/').Trim());
        }

        return result;
    }

    private static int GetOrderIndependentPathHashCode(IEnumerable<string>? source)
    {
        var hash = 0;
        foreach (var path in BuildPathSet(source))
            hash = unchecked(hash + StringComparer.OrdinalIgnoreCase.GetHashCode(path));

        return hash;
    }
}
