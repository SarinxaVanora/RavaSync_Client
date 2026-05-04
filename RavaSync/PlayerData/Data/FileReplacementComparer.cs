namespace RavaSync.PlayerData.Data;

public class FileReplacementComparer : IEqualityComparer<FileReplacement>
{
    private static readonly FileReplacementComparer _instance = new();

    private FileReplacementComparer()
    { }

    public static FileReplacementComparer Instance => _instance;

    public bool Equals(FileReplacement? x, FileReplacement? y)
    {
        if (x == null || y == null) return false;
        return string.Equals(x.ResolvedPath, y.ResolvedPath, StringComparison.OrdinalIgnoreCase)
            && CompareLists(x.GamePaths, y.GamePaths);
    }

    public int GetHashCode(FileReplacement obj)
    {
        return HashCode.Combine(obj.ResolvedPath.GetHashCode(StringComparison.OrdinalIgnoreCase), GetOrderIndependentHashCode(obj.GamePaths));
    }

    private static bool CompareLists(HashSet<string> list1, HashSet<string> list2)
    {
        return list1.Count == list2.Count
            && list1.All(v => list2.Contains(v, StringComparer.OrdinalIgnoreCase));
    }

    private static int GetOrderIndependentHashCode(IEnumerable<string> source)
    {
        int hash = 0;
        foreach (var element in source)
        {
            if (string.IsNullOrEmpty(element)) continue;
            hash = unchecked(hash + StringComparer.OrdinalIgnoreCase.GetHashCode(element));
        }
        return hash;
    }
}
