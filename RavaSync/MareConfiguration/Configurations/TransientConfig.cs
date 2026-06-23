using RavaSync.API.Data.Enum;

namespace RavaSync.MareConfiguration.Configurations;

public class TransientConfig : IMareConfiguration
{
    public Dictionary<string, TransientPlayerConfig> TransientConfigs { get; set; } = [];
    public int SelectedScopeMode { get; set; } = (int)Services.Mediator.ScopeMode.Everyone;
    public int Version { get; set; } = 3; //was 1

    public class TransientPlayerConfig
    {
        public List<string> GlobalPersistentCache { get; set; } = [];
        public Dictionary<uint, List<string>> JobSpecificCache { get; set; } = [];
        public Dictionary<uint, List<string>> JobSpecificPetCache { get; set; } = [];
        public List<string> MinionOrMountPersistentCache { get; set; } = [];
        public List<string> CompanionPersistentCache { get; set; } = [];
        public bool AutoRecordEmotes { get; set; } = false;
        public List<string> AutoRecordedEmoteKeys { get; set; } = new();
        public List<string> AutoRecordedFilePaths { get; set; } = new();
        public List<string> PersistentResolvedFilePaths { get; set; } = new();
        public Dictionary<string, string> PersistentResolvedFilePathByGamePath { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public string StartupManifestPrimeFingerprint { get; set; } = string.Empty;

        public TransientPlayerConfig()
        {

        }

        public bool Canonicalize()
        {
            lock (this)
            {
                var changed = false;

                changed |= CanonicalizeList(GlobalPersistentCache);
                changed |= CanonicalizeList(MinionOrMountPersistentCache);
                changed |= CanonicalizeList(CompanionPersistentCache);
                changed |= CanonicalizeList(AutoRecordedEmoteKeys);
                changed |= CanonicalizeResolvedFilePathList(AutoRecordedFilePaths);
                changed |= CanonicalizeResolvedFilePathList(PersistentResolvedFilePaths);
                changed |= CanonicalizeResolvedFilePathMap(PersistentResolvedFilePathByGamePath);

                foreach (var jobId in JobSpecificCache.Keys.ToList())
                {
                    var list = JobSpecificCache[jobId] ?? [];
                    if (JobSpecificCache[jobId] == null)
                    {
                        JobSpecificCache[jobId] = list;
                        changed = true;
                    }

                    changed |= CanonicalizeList(list);
                    if (list.Count == 0)
                    {
                        JobSpecificCache.Remove(jobId);
                        changed = true;
                    }
                }

                foreach (var jobId in JobSpecificPetCache.Keys.ToList())
                {
                    var list = JobSpecificPetCache[jobId] ?? [];
                    if (JobSpecificPetCache[jobId] == null)
                    {
                        JobSpecificPetCache[jobId] = list;
                        changed = true;
                    }

                    changed |= CanonicalizeList(list);
                    if (list.Count == 0)
                    {
                        JobSpecificPetCache.Remove(jobId);
                        changed = true;
                    }
                }

                changed |= MoveSummonedActorPetJobCacheToPlayerJobCache();
                changed |= MoveObjectScopedPathsOutOfPlayerCaches();
                changed |= MovePlayerActorPathsOutOfObjectScopedCaches();

                var globalSeen = new HashSet<string>(GlobalPersistentCache, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in JobSpecificCache.OrderBy(k => k.Key).ToList())
                {
                    var list = kvp.Value ?? [];
                    var removed = list.RemoveAll(path => globalSeen.Contains(path));
                    if (removed > 0)
                        changed = true;

                    if (list.Count == 0)
                    {
                        JobSpecificCache.Remove(kvp.Key);
                        changed = true;
                    }
                    else if (!ReferenceEquals(JobSpecificCache[kvp.Key], list))
                    {
                        JobSpecificCache[kvp.Key] = list;
                        changed = true;
                    }
                }

                changed |= CanonicalizeList(GlobalPersistentCache);
                changed |= CanonicalizeList(MinionOrMountPersistentCache);
                changed |= CanonicalizeList(CompanionPersistentCache);
                changed |= CanonicalizeResolvedFilePathList(AutoRecordedFilePaths);
                changed |= CanonicalizeResolvedFilePathList(PersistentResolvedFilePaths);
                changed |= CanonicalizeResolvedFilePathMap(PersistentResolvedFilePathByGamePath);

                return changed;
            }
        }

        public bool ContainsPersistentResolvedFilePath(string? resolvedFilePath)
        {
            lock (this)
            {
                var normalizedPath = NormalizeResolvedFilePath(resolvedFilePath ?? string.Empty);
                if (string.IsNullOrWhiteSpace(normalizedPath) || normalizedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                    return false;

                return PersistentResolvedFilePaths.Contains(normalizedPath, StringComparer.OrdinalIgnoreCase)
                    || AutoRecordedFilePaths.Contains(normalizedPath, StringComparer.OrdinalIgnoreCase);
            }
        }

        public bool RegisterPersistentResolvedFilePath(string? resolvedFilePath)
        {
            lock (this)
            {
                var normalizedPath = NormalizeResolvedFilePath(resolvedFilePath ?? string.Empty);
                if (string.IsNullOrWhiteSpace(normalizedPath) || normalizedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                    return false;

                var existingIndex = PersistentResolvedFilePaths.FindIndex(path => string.Equals(path, normalizedPath, StringComparison.OrdinalIgnoreCase));
                if (existingIndex >= 0)
                {
                    if (string.Equals(PersistentResolvedFilePaths[existingIndex], normalizedPath, StringComparison.Ordinal))
                        return false;

                    PersistentResolvedFilePaths[existingIndex] = normalizedPath;
                    return true;
                }

                PersistentResolvedFilePaths.Add(normalizedPath);
                return true;
            }
        }

        public bool RegisterPersistentResolvedFilePath(string? gamePath, string? resolvedFilePath)
        {
            lock (this)
            {
                var changed = RegisterPersistentResolvedFilePath(resolvedFilePath);
                var normalizedGamePath = NormalizePath(gamePath ?? string.Empty);
                var normalizedResolvedPath = NormalizeResolvedFilePath(resolvedFilePath ?? string.Empty);

                if (string.IsNullOrWhiteSpace(normalizedGamePath)
                    || string.IsNullOrWhiteSpace(normalizedResolvedPath)
                    || normalizedResolvedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                {
                    return changed;
                }

                if (!PersistentResolvedFilePathByGamePath.TryGetValue(normalizedGamePath, out var existing)
                    || !string.Equals(existing, normalizedResolvedPath, StringComparison.Ordinal))
                {
                    PersistentResolvedFilePathByGamePath[normalizedGamePath] = normalizedResolvedPath;
                    changed = true;
                }

                return changed;
            }
        }

        public bool TryGetPersistentResolvedFilePath(string? gamePath, out string resolvedFilePath)
        {
            lock (this)
            {
                resolvedFilePath = string.Empty;
                var normalizedGamePath = NormalizePath(gamePath ?? string.Empty);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    return false;

                if (PersistentResolvedFilePathByGamePath.TryGetValue(normalizedGamePath, out var mapped)
                    && !string.IsNullOrWhiteSpace(mapped)
                    && !mapped.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                {
                    resolvedFilePath = mapped;
                    return true;
                }

                return false;
            }
        }

        public int RemovePath(string gamePath, ObjectKind objectKind)
        {
            lock (this)
            {
                var normalizedGamePath = NormalizePath(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    return 0;

                int removedEntries = 0;
                if (objectKind == ObjectKind.Player)
                {
                    removedEntries += GlobalPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    foreach (var kvp in JobSpecificCache.ToList())
                    {
                        removedEntries += kvp.Value.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    }
                }

                if (objectKind == ObjectKind.Pet)
                {
                    foreach (var kvp in JobSpecificPetCache.ToList())
                    {
                        removedEntries += kvp.Value.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    }
                }

                if (objectKind == ObjectKind.MinionOrMount)
                    removedEntries += MinionOrMountPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));

                if (objectKind == ObjectKind.Companion)
                    removedEntries += CompanionPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));

                if (removedEntries > 0)
                    Canonicalize();

                return removedEntries;
            }
        }

        public void AddOrElevate(uint jobId, string gamePath)
        {
            lock (this)
            {
                var normalizedGamePath = NormalizePath(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath) || jobId == 0)
                    return;

                GlobalPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                RemoveFromObjectScopedCaches(normalizedGamePath);

                if (!JobSpecificCache.TryGetValue(jobId, out var jobCache) || jobCache == null)
                {
                    JobSpecificCache[jobId] = jobCache = [];
                }

                if (!jobCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                    jobCache.Add(normalizedGamePath);

                Canonicalize();
            }
        }

        public void SetPathScope(uint? jobId, string gamePath)
        {
            lock (this)
            {
                var normalizedGamePath = NormalizePath(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    return;

                GlobalPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                foreach (var kvp in JobSpecificCache.ToList())
                {
                    kvp.Value?.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                }

                RemoveFromObjectScopedCaches(normalizedGamePath);

                if (jobId.HasValue && jobId.Value != 0)
                {
                    if (!JobSpecificCache.TryGetValue(jobId.Value, out var jobCache) || jobCache == null)
                    {
                        JobSpecificCache[jobId.Value] = jobCache = [];
                    }

                    if (!jobCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                        jobCache.Add(normalizedGamePath);
                }
                else
                {
                    if (!GlobalPersistentCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                        GlobalPersistentCache.Add(normalizedGamePath);
                }

                Canonicalize();
            }
        }

        public void SetPetPathScope(uint jobId, string gamePath)
        {
            lock (this)
            {
                var normalizedGamePath = NormalizePath(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    return;

                if (jobId == 0)
                    return;

                if (IsLikelyPlayerJobSummonedActorPath(normalizedGamePath))
                {
                    AddOrElevate(jobId, normalizedGamePath);
                    return;
                }

                if (!JobSpecificPetCache.TryGetValue(jobId, out var jobCache) || jobCache == null)
                {
                    JobSpecificPetCache[jobId] = jobCache = [];
                }

                if (!jobCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                    jobCache.Add(normalizedGamePath);

                Canonicalize();
            }
        }

        public bool SetObjectPathScope(ObjectKind objectKind, string gamePath)
        {
            if (objectKind == ObjectKind.Player)
            {
                SetPathScope(null, gamePath);
                return true;
            }

            lock (this)
            {
                var normalizedGamePath = NormalizePath(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    return false;

                var removedFromPlayerScopes = RemoveFromPlayerScopedCaches(normalizedGamePath);

                var cache = objectKind switch
                {
                    ObjectKind.MinionOrMount => MinionOrMountPersistentCache,
                    ObjectKind.Companion => CompanionPersistentCache,
                    _ => null,
                };

                if (cache == null)
                    return false;

                if (cache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                {
                    if (removedFromPlayerScopes <= 0)
                        return false;

                    Canonicalize();
                    return true;
                }

                cache.Add(normalizedGamePath);
                Canonicalize();
                return true;
            }
        }


        private bool MoveSummonedActorPetJobCacheToPlayerJobCache()
        {
            if (JobSpecificPetCache.Count == 0)
                return false;

            var changed = false;
            foreach (var kvp in JobSpecificPetCache.ToList())
            {
                var petList = kvp.Value;
                if (petList == null || petList.Count == 0)
                    continue;

                foreach (var path in petList.ToArray())
                {
                    var normalizedGamePath = NormalizePath(path);
                    if (string.IsNullOrWhiteSpace(normalizedGamePath) || !IsLikelyPlayerJobSummonedActorPath(normalizedGamePath))
                        continue;

                    petList.RemoveAll(value => string.Equals(value, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    if (!JobSpecificCache.TryGetValue(kvp.Key, out var jobList) || jobList == null)
                    {
                        JobSpecificCache[kvp.Key] = jobList = [];
                    }

                    if (!jobList.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                        jobList.Add(normalizedGamePath);

                    changed = true;
                }

                if (petList.Count == 0)
                {
                    JobSpecificPetCache.Remove(kvp.Key);
                    changed = true;
                }
            }

            return changed;
        }

        private bool MoveObjectScopedPathsOutOfPlayerCaches()
        {
            var changed = false;
            changed |= MoveObjectScopedPathsOutOfList(GlobalPersistentCache);

            foreach (var kvp in JobSpecificCache.ToList())
            {
                if (kvp.Value == null)
                    continue;

                changed |= MoveObjectScopedPathsOutOfList(kvp.Value);
                if (kvp.Value.Count == 0)
                {
                    JobSpecificCache.Remove(kvp.Key);
                    changed = true;
                }
            }

            return changed;
        }

        private bool MoveObjectScopedPathsOutOfList(List<string> source)
        {
            if (source == null || source.Count == 0)
                return false;

            var changed = false;
            foreach (var path in source.ToArray())
            {
                var normalizedGamePath = NormalizePath(path);
                if (string.IsNullOrWhiteSpace(normalizedGamePath))
                    continue;

                if (IsPlayerActorPersistentPath(normalizedGamePath))
                    continue;

                if (IsLikelyMinionOrMountPersistentPath(normalizedGamePath))
                {
                    source.RemoveAll(value => string.Equals(value, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    if (!MinionOrMountPersistentCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                        MinionOrMountPersistentCache.Add(normalizedGamePath);

                    changed = true;
                    continue;
                }

                if (IsLikelyCompanionPersistentPath(normalizedGamePath))
                {
                    source.RemoveAll(value => string.Equals(value, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                    if (!CompanionPersistentCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                        CompanionPersistentCache.Add(normalizedGamePath);

                    changed = true;
                }
            }

            return changed;
        }

        private bool MovePlayerActorPathsOutOfObjectScopedCaches()
        {
            var changed = false;
            changed |= MovePlayerActorPathsOutOfObjectScopedList(MinionOrMountPersistentCache);
            changed |= MovePlayerActorPathsOutOfObjectScopedList(CompanionPersistentCache);
            return changed;
        }

        private bool MovePlayerActorPathsOutOfObjectScopedList(List<string> source)
        {
            if (source == null || source.Count == 0)
                return false;

            var changed = false;
            foreach (var path in source.ToArray())
            {
                var normalizedGamePath = NormalizePath(path);
                if (string.IsNullOrWhiteSpace(normalizedGamePath) || !IsPlayerActorPersistentPath(normalizedGamePath))
                    continue;

                source.RemoveAll(value => string.Equals(value, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
                if (!GlobalPersistentCache.Contains(normalizedGamePath, StringComparer.OrdinalIgnoreCase))
                    GlobalPersistentCache.Add(normalizedGamePath);

                changed = true;
            }

            return changed;
        }

        private int RemoveFromPlayerScopedCaches(string normalizedGamePath)
        {
            var removed = GlobalPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
            foreach (var kvp in JobSpecificCache.ToList())
                removed += kvp.Value?.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase)) ?? 0;

            return removed;
        }

        private int RemoveFromObjectScopedCaches(string normalizedGamePath)
        {
            var removed = MinionOrMountPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
            removed += CompanionPersistentCache.RemoveAll(path => string.Equals(path, normalizedGamePath, StringComparison.OrdinalIgnoreCase));
            return removed;
        }


        private static bool IsLikelyPlayerJobSummonedActorPath(string? path)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            return normalized.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("chara/action/mon_sp/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("vfx/monster/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("vfx/pop/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("vfx/action/", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("sound/vfx/ability/", StringComparison.OrdinalIgnoreCase)
                || normalized.Contains("/ability/", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsPlayerActorPersistentPath(string? path)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            return normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsLikelyMinionOrMountPersistentPath(string? path)
            => ContainsPersistentObjectToken(path, "mount", "mounts", "minion", "minions");

        private static bool IsLikelyCompanionPersistentPath(string? path)
            => ContainsPersistentObjectToken(path, "companion", "companions", "chocobo", "buddy");

        private static bool ContainsPersistentObjectToken(string? path, params string[] tokens)
        {
            var normalized = NormalizePath(path ?? string.Empty);
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            foreach (var token in tokens)
            {
                if (System.Text.RegularExpressions.Regex.IsMatch(
                    normalized,
                    $"(^|[^a-z0-9]){System.Text.RegularExpressions.Regex.Escape(token)}([^a-z0-9]|$)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool CanonicalizeResolvedFilePathMap(Dictionary<string, string>? values)
        {
            if (values == null)
                return false;

            var changed = false;
            var next = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in values.ToArray())
            {
                var normalizedGamePath = NormalizePath(kvp.Key ?? string.Empty);
                var normalizedResolvedPath = NormalizeResolvedFilePath(kvp.Value ?? string.Empty);
                if (string.IsNullOrWhiteSpace(normalizedGamePath)
                    || string.IsNullOrWhiteSpace(normalizedResolvedPath)
                    || normalizedResolvedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase))
                {
                    changed = true;
                    continue;
                }

                if (next.TryGetValue(normalizedGamePath, out var existing)
                    && string.Equals(existing, normalizedResolvedPath, StringComparison.OrdinalIgnoreCase))
                {
                    changed = true;
                    continue;
                }

                if (!string.Equals(kvp.Key, normalizedGamePath, StringComparison.Ordinal)
                    || !string.Equals(kvp.Value, normalizedResolvedPath, StringComparison.Ordinal))
                {
                    changed = true;
                }

                next[normalizedGamePath] = normalizedResolvedPath;
            }

            if (changed || next.Count != values.Count)
            {
                values.Clear();
                foreach (var kvp in next.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                    values[kvp.Key] = kvp.Value;

                return true;
            }

            return false;
        }

        private static bool CanonicalizeResolvedFilePathList(List<string>? values)
        {
            if (values == null || values.Count == 0)
                return false;

            var changed = false;
            var next = new List<string>(values.Count);
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var value in values.ToArray())
            {
                var normalizedValue = NormalizeResolvedFilePath(value);
                if (string.IsNullOrWhiteSpace(normalizedValue))
                {
                    changed = true;
                    continue;
                }

                if (!seen.Add(normalizedValue))
                {
                    changed = true;
                    continue;
                }

                if (!string.Equals(value, normalizedValue, StringComparison.Ordinal))
                    changed = true;

                next.Add(normalizedValue);
            }

            if (changed || next.Count != values.Count)
            {
                values.Clear();
                values.AddRange(next);
                return true;
            }

            return false;
        }

        private static bool CanonicalizeList(List<string>? values)
        {
            if (values == null || values.Count == 0)
                return false;

            var changed = false;
            var next = new List<string>(values.Count);
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var value in values.ToArray())
            {
                var normalizedValue = NormalizePath(value);
                if (string.IsNullOrWhiteSpace(normalizedValue))
                {
                    changed = true;
                    continue;
                }

                if (!seen.Add(normalizedValue))
                {
                    changed = true;
                    continue;
                }

                if (!string.Equals(value, normalizedValue, StringComparison.Ordinal))
                    changed = true;

                next.Add(normalizedValue);
            }

            if (changed || next.Count != values.Count)
            {
                values.Clear();
                values.AddRange(next);
                return true;
            }

            return false;
        }

        private static string NormalizeResolvedFilePath(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            return value.Replace('\\', '/').Trim();
        }

        private static string NormalizePath(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            return value.ToLowerInvariant().Replace('\\', '/');
        }
    }
}
