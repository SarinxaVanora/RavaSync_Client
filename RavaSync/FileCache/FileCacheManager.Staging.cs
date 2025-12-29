using System.Collections.Concurrent;

namespace RavaSync.FileCache
{
    // staging lives here so the main cache file stays clean
    public partial class FileCacheManager
    {
        // hash -> finalPath for files downloaded to quarantine and not yet activated
        private readonly ConcurrentDictionary<string, string> _stagedByHash
            = new(StringComparer.OrdinalIgnoreCase);

        public void StageFile(string hash, string finalPath)
        {
            if (string.IsNullOrWhiteSpace(hash) || string.IsNullOrWhiteSpace(finalPath)) return;
            _stagedByHash[hash] = finalPath;
        }

        public void UnstageFile(string hash)
        {
            if (string.IsNullOrWhiteSpace(hash)) return;
            _stagedByHash.TryRemove(hash, out _);
        }

        public bool IsHashStaged(string hash)
        {
            if (string.IsNullOrWhiteSpace(hash)) return false;
            return _stagedByHash.ContainsKey(hash);
        }
    }
}
