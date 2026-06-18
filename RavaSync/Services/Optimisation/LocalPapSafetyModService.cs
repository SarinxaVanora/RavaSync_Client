using Microsoft.Extensions.Logging;
using Penumbra.Api.Enums;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.PlayerData.Services;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace RavaSync.Services.Optimisation;

public sealed class LocalPapSafetyModService
{
    public const string RuntimeModRelativeDirectory = "RavaSync Converted Animations";
    private const string RuntimeModDisplayName = "RavaSync Converted Animations";
    private const int RuntimePriorityFloor = 1000000;

    public sealed record SanitizedPapOverride(string OriginalResolvedPath, string OriginalHash, string EffectivePath, string EffectiveHash, string[] GamePaths, string Reason);

    public sealed record ManifestPapSource(string RelativeModDirectory, string DisplayModName, string SourceGroupName, string SourceGroupType, string OptionDisplayName, string ResolvedPath, string Hash, int Priority, string[] GamePaths);

    public sealed record ManifestSupportSource(string ResolvedPath, string Hash, int Priority, string[] GamePaths);

    private sealed record CurrentManifestGroupAliases(HashSet<string> RawAliases, HashSet<string> NormalizedAliases, HashSet<string> LooseAliases);

    private sealed record ManifestPapCandidate(string RelativeModDirectory, string DisplayModName, string SourceGroupName, string SourceGroupType, string OptionDisplayName, string ResolvedPath, int Priority, string[] GamePaths);

    private sealed record ManifestSupportCandidate(string ResolvedPath, int Priority, string[] GamePaths);

    private sealed class DesiredRuntimeGroup
    {
        public string Name { get; init; } = string.Empty;
        public string Type { get; set; } = "Single";
        public string ManifestFileName { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public Dictionary<string, DesiredRuntimeOption> Options { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> SelectedOptionNames { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class DesiredRuntimeOption
    {
        public string Name { get; init; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public Dictionary<string, string> Files { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class SourceRuntimeGroupSelectionState
    {
        public HashSet<string> SourceBackedGroupNames { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, HashSet<string>> SelectedOptionsByGroup { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private readonly ILogger<LocalPapSafetyModService> _logger;
    private readonly IpcManager _ipcManager;
    private readonly FileCacheManager _fileCacheManager;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true,
    };

    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly object _appliedStateLock = new();
    private readonly object _localCollectionSettingsCacheLock = new();
    private string? _lastAppliedRuntimeFingerprint;
    private IpcCallerPenumbra.PenumbraCollectionModSettings? _cachedLocalPlayerCollectionSettings;
    private DateTime _cachedLocalPlayerCollectionSettingsUntilUtc = DateTime.MinValue;
    private static readonly TimeSpan LocalPlayerCollectionSettingsCacheDuration = TimeSpan.FromMilliseconds(750);
    private const int ManifestScanYieldStride = 4;
    private const int ModScanYieldStride = 8;

    public LocalPapSafetyModService(
        ILogger<LocalPapSafetyModService> logger,
        IpcManager ipcManager,
        FileCacheManager fileCacheManager,
        DalamudUtilService dalamudUtil)
    {
        _logger = logger;
        _ipcManager = ipcManager;
        _fileCacheManager = fileCacheManager;
        _dalamudUtil = dalamudUtil;
    }

    public static bool IsManagedRuntimeModIdentifier(string? modIdentifier)
    {
        if (string.IsNullOrWhiteSpace(modIdentifier))
            return false;

        var normalized = modIdentifier.Replace('/', '\\').Trim().Trim('\\');
        if (normalized.EndsWith(RuntimeModRelativeDirectory, StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, RuntimeModRelativeDirectory, StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, RuntimeModDisplayName, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var fileName = Path.GetFileName(normalized);
        return string.Equals(fileName, RuntimeModRelativeDirectory, StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, RuntimeModDisplayName, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsRavaSyncInternalTemporaryModIdentifier(string? modIdentifier)
    {
        if (string.IsNullOrWhiteSpace(modIdentifier))
            return false;

        var normalized = modIdentifier.Replace('/', '\\').Trim().Trim('\\');
        var fileName = Path.GetFileName(normalized);

        return string.Equals(fileName, "MareChara_Files", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, "MareChara_Files_A", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, "MareChara_Files_B", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, "MareChara_Meta", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, IpcCallerPenumbra.MountMusicTemporaryModName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(fileName, "RavaSync_AsyncLoadSupport", StringComparison.OrdinalIgnoreCase);
    }

    public void InvalidateLocalPlayerCollectionSettingsCache()
    {
        lock (_localCollectionSettingsCacheLock)
        {
            _cachedLocalPlayerCollectionSettings = null;
            _cachedLocalPlayerCollectionSettingsUntilUtc = DateTime.MinValue;
        }
    }

    public bool IsManagedRuntimeModIdentifierForCurrentRoot(string? modIdentifier)
    {
        if (IsManagedRuntimeModIdentifier(modIdentifier))
            return true;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        return !string.IsNullOrWhiteSpace(modDirectory) && IsRuntimeModKey(modDirectory, modIdentifier ?? string.Empty);
    }

    public bool IsManagedRuntimePapPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory))
            return false;

        var normalizedPath = NormalizeFullPath(path);
        var runtimeRoot = Path.GetFullPath(Path.Combine(modDirectory, RuntimeModRelativeDirectory));
        var normalizedRoot = NormalizeFullPath(runtimeRoot);
        return normalizedPath.StartsWith(normalizedRoot + "/", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalizedPath, normalizedRoot, StringComparison.OrdinalIgnoreCase);
    }

    public bool ModMayContainHumanAnimationPapPayload(string? modIdentifier, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(modIdentifier))
            return true;

        if (IsManagedRuntimeModIdentifierForCurrentRoot(modIdentifier))
            return false;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return false;

        try
        {
            var modPath = ResolveModDirectory(modDirectory, modIdentifier);
            if (!Directory.Exists(modPath))
                return false;

            foreach (var manifestPath in EnumerateTopLevelPenumbraManifestFiles(modPath))
            {
                token.ThrowIfCancellationRequested();

                if (ManifestTextSuggestsAnimationPap(manifestPath))
                    return true;
            }

            return false;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Could not cheaply inspect Penumbra mod {mod} for animation PAP payloads; allowing runtime PAP refresh", modIdentifier);
            return true;
        }
    }

    private static IEnumerable<string> EnumerateTopLevelPenumbraManifestFiles(string modPath)
    {
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
            yield return defaultManifest;

        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
            yield return groupManifest;

        var legacyMeta = Path.Combine(modPath, "meta.json");
        if (File.Exists(legacyMeta))
            yield return legacyMeta;
    }

    private static bool ManifestTextSuggestsAnimationPap(string manifestPath)
    {
        if (!File.Exists(manifestPath))
            return false;

        var info = new FileInfo(manifestPath);
        if (info.Length <= 0)
            return false;

        // Very large manifests are rare; avoid spending time parsing them here and preserve safety.
        if (info.Length > 4 * 1024 * 1024)
            return true;

        using var stream = OpenReadShared(manifestPath);
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var text = reader.ReadToEnd();

        return text.Contains(".pap", StringComparison.OrdinalIgnoreCase);
    }

    public async Task<IpcCallerPenumbra.PenumbraCollectionModSettings?> TryGetLocalPlayerCollectionSettingsAsync(CancellationToken token)
    {
        if (!_ipcManager.Penumbra.APIAvailable)
            return null;

        var now = DateTime.UtcNow;
        lock (_localCollectionSettingsCacheLock)
        {
            if (_cachedLocalPlayerCollectionSettings != null && now <= _cachedLocalPlayerCollectionSettingsUntilUtc)
                return _cachedLocalPlayerCollectionSettings;
        }

        var localPlayer = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            var ptr = _dalamudUtil.GetPlayerPtr();
            if (ptr == nint.Zero)
                return (Address: nint.Zero, ObjectIndex: -1);

            var obj = _dalamudUtil.CreateGameObject(ptr);
            return (Address: ptr, ObjectIndex: obj?.ObjectIndex ?? -1);
        }).ConfigureAwait(false);

        if (token.IsCancellationRequested)
            return null;

        if (localPlayer.Address == nint.Zero || localPlayer.ObjectIndex < 0)
            return null;

        var result = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(_logger, localPlayer.ObjectIndex).ConfigureAwait(false);
        if (result != null)
        {
            lock (_localCollectionSettingsCacheLock)
            {
                _cachedLocalPlayerCollectionSettings = result;
                _cachedLocalPlayerCollectionSettingsUntilUtc = DateTime.UtcNow + LocalPlayerCollectionSettingsCacheDuration;
            }
        }

        return result;
    }

    public async Task<IReadOnlyDictionary<string, ManifestPapSource>> ResolveSelectedHumanPapSourcesAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, CancellationToken token)
    {
        IReadOnlyDictionary<string, ManifestPapSource> empty = new Dictionary<string, ManifestPapSource>(StringComparer.OrdinalIgnoreCase);

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return empty;

        var winnersByGamePath = new Dictionary<string, ManifestPapCandidate>(StringComparer.OrdinalIgnoreCase);
        var scannedMods = 0;

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (++scannedMods % ModScanYieldStride == 0)
                await Task.Yield();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var manifestEntries = await LoadSelectedManifestPapCandidatesAsync(modDirectory, mod.Key, modPath, mod.Value.Settings, mod.Value.Priority, token).ConfigureAwait(false);

            foreach (var candidate in manifestEntries)
            {
                token.ThrowIfCancellationRequested();

                foreach (var gamePath in candidate.GamePaths)
                {
                    if (!XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath(gamePath))
                        continue;

                    if (!winnersByGamePath.TryGetValue(gamePath, out var existing) || candidate.Priority >= existing.Priority)
                    {
                        winnersByGamePath[gamePath] = candidate with { GamePaths = [gamePath] };
                    }
                }
            }
        }

        if (winnersByGamePath.Count == 0)
            return empty;

        var pathHashes = _fileCacheManager.GetFileHashesByPaths(winnersByGamePath.Values.Select(v => v.ResolvedPath).Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
        var output = new Dictionary<string, ManifestPapSource>(StringComparer.OrdinalIgnoreCase);
        var emitted = 0;

        foreach (var kvp in winnersByGamePath)
        {
            token.ThrowIfCancellationRequested();

            if (++emitted % ModScanYieldStride == 0)
                await Task.Yield();

            var hash = pathHashes.TryGetValue(kvp.Value.ResolvedPath, out var resolvedHash)
                ? resolvedHash ?? string.Empty
                : string.Empty;
            if (string.IsNullOrWhiteSpace(hash))
                continue;

            output[kvp.Key] = new ManifestPapSource(kvp.Value.RelativeModDirectory, kvp.Value.DisplayModName, kvp.Value.SourceGroupName, kvp.Value.SourceGroupType, kvp.Value.OptionDisplayName, kvp.Value.ResolvedPath, hash, kvp.Value.Priority, kvp.Value.GamePaths);
        }

        return output;
    }

    public async Task<IReadOnlyCollection<ManifestSupportSource>> ResolveSelectedAnimationSupportFilesAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, CancellationToken token)
    {
        IReadOnlyCollection<ManifestSupportSource> empty = Array.Empty<ManifestSupportSource>();

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return empty;

        var winnersByGamePath = new Dictionary<string, ManifestSupportCandidate>(StringComparer.OrdinalIgnoreCase);
        var scannedMods = 0;

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (++scannedMods % ModScanYieldStride == 0)
                await Task.Yield();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var manifestSuggestsAnimation = false;
            var scannedManifests = 0;

            foreach (var manifestPath in EnumerateTopLevelPenumbraManifestFiles(modPath))
            {
                token.ThrowIfCancellationRequested();

                if (++scannedManifests % ManifestScanYieldStride == 0)
                    await Task.Yield();

                if (ManifestTextSuggestsAnimationPap(manifestPath))
                {
                    manifestSuggestsAnimation = true;
                    break;
                }
            }

            if (!manifestSuggestsAnimation)
                continue;

            var manifestEntries = await LoadSelectedManifestAnimationSupportCandidatesAsync(modPath, mod.Value.Settings, mod.Value.Priority, token).ConfigureAwait(false);

            if (!manifestEntries.HasSelectedHumanAnimationPap)
                continue;

            foreach (var candidate in manifestEntries.Candidates)
            {
                token.ThrowIfCancellationRequested();

                foreach (var gamePath in candidate.GamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    if (!winnersByGamePath.TryGetValue(gamePath, out var existing) || candidate.Priority >= existing.Priority)
                    {
                        winnersByGamePath[gamePath] = candidate with { GamePaths = [gamePath] };
                    }
                }
            }
        }

        if (winnersByGamePath.Count == 0)
            return empty;

        var pathHashes = _fileCacheManager.GetFileHashesByPaths(winnersByGamePath.Values.Select(v => v.ResolvedPath).Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
        var output = new List<ManifestSupportSource>(winnersByGamePath.Count);
        var emitted = 0;

        foreach (var kvp in winnersByGamePath)
        {
            token.ThrowIfCancellationRequested();

            if (++emitted % ModScanYieldStride == 0)
                await Task.Yield();

            var hash = pathHashes.TryGetValue(kvp.Value.ResolvedPath, out var resolvedHash)
                ? resolvedHash ?? string.Empty
                : string.Empty;

            if (string.IsNullOrWhiteSpace(hash))
                continue;

            output.Add(new ManifestSupportSource(kvp.Value.ResolvedPath, hash, kvp.Value.Priority, kvp.Value.GamePaths));
        }

        return output;
    }


    public async Task<IReadOnlyCollection<ManifestSupportSource>> ResolveSelectedFullMountModFilesAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, IReadOnlyCollection<string> activeMountIds, CancellationToken token)
    {
        IReadOnlyCollection<ManifestSupportSource> empty = Array.Empty<ManifestSupportSource>();

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || activeMountIds == null || activeMountIds.Count == 0 || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return empty;

        var normalizedMountIds = new HashSet<string>(activeMountIds.Select(NormalizeMountActorId).Where(static id => !string.IsNullOrWhiteSpace(id)), StringComparer.OrdinalIgnoreCase);
        if (normalizedMountIds.Count == 0)
            return empty;

        var winnersByGamePath = new Dictionary<string, ManifestSupportCandidate>(StringComparer.OrdinalIgnoreCase);
        var scannedMods = 0;
        var matchedMods = 0;

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (++scannedMods % ModScanYieldStride == 0)
                await Task.Yield();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var selectedEntries = await LoadSelectedManifestAllFileCandidatesAsync(modPath, mod.Value.Settings, mod.Value.Priority, token).ConfigureAwait(false);
            if (selectedEntries.Count == 0)
                continue;

            if (!selectedEntries.Any(entry => entry.GamePaths.Any(gamePath => IsActiveMountModGamePath(gamePath, normalizedMountIds))))
                continue;

            // Only use the "send the whole selected mount mod" safety net for real mounted/rider
            // replacements. Minions also live in ObjectKind.MinionOrMount and often use chara/monster
            // paths, but they do not have human mt_<id> rider PAPs. Without this guard, a single
            // minion summon can match a broad selected pack and send far more than the active minion.
            if (!selectedEntries.Any(entry => entry.GamePaths.Any(gamePath => IsActiveMountRiderGamePath(gamePath, normalizedMountIds))))
                continue;

            matchedMods++;
            foreach (var candidate in selectedEntries)
            {
                token.ThrowIfCancellationRequested();

                foreach (var gamePath in candidate.GamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    if (!winnersByGamePath.TryGetValue(gamePath, out var existing) || candidate.Priority >= existing.Priority)
                        winnersByGamePath[gamePath] = candidate with { GamePaths = [gamePath] };
                }
            }
        }

        if (winnersByGamePath.Count == 0)
            return empty;

        var pathHashes = _fileCacheManager.GetFileHashesByPaths(winnersByGamePath.Values.Select(static v => v.ResolvedPath).Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
        var output = new List<ManifestSupportSource>(winnersByGamePath.Count);
        var emitted = 0;

        foreach (var kvp in winnersByGamePath)
        {
            token.ThrowIfCancellationRequested();

            if (++emitted % ModScanYieldStride == 0)
                await Task.Yield();

            var hash = pathHashes.TryGetValue(kvp.Value.ResolvedPath, out var resolvedHash)
                ? resolvedHash ?? string.Empty
                : string.Empty;

            if (string.IsNullOrWhiteSpace(hash))
                continue;

            output.Add(new ManifestSupportSource(kvp.Value.ResolvedPath, hash, kvp.Value.Priority, kvp.Value.GamePaths));
        }

        if (output.Count > 0)
            _logger.LogDebug("Resolved {count} selected file(s) from {mods} active mount mod(s) for mount actor(s) {mounts}", output.Count, matchedMods, string.Join(",", normalizedMountIds.OrderBy(static id => id, StringComparer.OrdinalIgnoreCase)));

        return output;
    }

    public async Task<IReadOnlyCollection<ManifestSupportSource>> ResolveSelectedMountMusicFilesAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, CancellationToken token)
    {
        IReadOnlyCollection<ManifestSupportSource> empty = Array.Empty<ManifestSupportSource>();

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return empty;

        var winnersByGamePath = new Dictionary<string, ManifestSupportCandidate>(StringComparer.OrdinalIgnoreCase);
        var scannedMods = 0;

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (++scannedMods % ModScanYieldStride == 0)
                await Task.Yield();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var manifestEntries = await LoadSelectedManifestMountMusicCandidatesAsync(modPath, mod.Value.Settings, mod.Value.Priority, token).ConfigureAwait(false);
            foreach (var candidate in manifestEntries)
            {
                token.ThrowIfCancellationRequested();

                foreach (var gamePath in candidate.GamePaths)
                {
                    if (string.IsNullOrWhiteSpace(gamePath))
                        continue;

                    if (!winnersByGamePath.TryGetValue(gamePath, out var existing) || candidate.Priority >= existing.Priority)
                        winnersByGamePath[gamePath] = candidate with { GamePaths = [gamePath] };
                }
            }
        }

        if (winnersByGamePath.Count == 0)
            return empty;

        var pathHashes = _fileCacheManager.GetFileHashesByPaths(winnersByGamePath.Values.Select(v => v.ResolvedPath).Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
        var output = new List<ManifestSupportSource>(winnersByGamePath.Count);
        var emitted = 0;

        foreach (var kvp in winnersByGamePath)
        {
            token.ThrowIfCancellationRequested();

            if (++emitted % ModScanYieldStride == 0)
                await Task.Yield();

            var hash = pathHashes.TryGetValue(kvp.Value.ResolvedPath, out var resolvedHash)
                ? resolvedHash ?? string.Empty
                : string.Empty;

            if (string.IsNullOrWhiteSpace(hash))
                continue;

            output.Add(new ManifestSupportSource(kvp.Value.ResolvedPath, hash, kvp.Value.Priority, kvp.Value.GamePaths));
        }

        return output;
    }

    public async Task<bool> SyncRuntimeModAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, IReadOnlyDictionary<string, ManifestPapSource> selectedSourcesByGamePath, IReadOnlyCollection<SanitizedPapOverride> desiredOverrides, CancellationToken token)
    {
        if (collectionState == null || !_ipcManager.Penumbra.APIAvailable)
            return false;

        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return false;

        await _syncGate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            token.ThrowIfCancellationRequested();

            var runtimeModPath = Path.GetFullPath(Path.Combine(modDirectory, RuntimeModRelativeDirectory));
            Directory.CreateDirectory(runtimeModPath);

            bool contentChanged = false;
            contentChanged |= WriteRuntimeMetaFiles(runtimeModPath);

            var existingGroups = LoadExistingRuntimeGroups(runtimeModPath);
            var desiredGroups = CloneDesiredGroups(existingGroups);
            foreach (var desired in desiredOverrides.OrderBy(d => d.GamePaths.FirstOrDefault() ?? d.OriginalResolvedPath, StringComparer.OrdinalIgnoreCase))
            {
                token.ThrowIfCancellationRequested();


                var gamePaths = desired.GamePaths.Where(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(g => g, StringComparer.OrdinalIgnoreCase).ToArray();
                if (gamePaths.Length == 0)
                    continue;

                var sourceInfo = ResolveSourceInfoForOverride(selectedSourcesByGamePath, desired, gamePaths);
                var groupName = BuildRuntimeGroupName(sourceInfo.DisplayModName, sourceInfo.SourceGroupName);
                var optionName = BuildRuntimeOptionName(sourceInfo.OptionDisplayName);

                if (!desiredGroups.TryGetValue(groupName, out var group))
                {
                    group = new DesiredRuntimeGroup
                    {
                        Name = groupName,
                        Type = "Multi",
                        Description = string.Empty,
                    };
                    desiredGroups[groupName] = group;
                }
                else
                {
                    group.Type = "Multi";
                }

                if (!group.Options.TryGetValue(optionName, out var option))
                {
                    option = new DesiredRuntimeOption
                    {
                        Name = optionName,
                        Description = string.Empty,
                    };
                    group.Options[optionName] = option;
                }
                else if (!string.IsNullOrWhiteSpace(option.Description))
                {
                    option.Description = string.Empty;
                }

                contentChanged |= CopySanitizedPapIntoRuntimeMod(runtimeModPath, groupName, optionName, gamePaths, desired.EffectivePath, out var relativeFilePath);
                if (string.IsNullOrWhiteSpace(relativeFilePath))
                    continue;

                foreach (var gamePath in gamePaths)
                    option.Files[NormalizeGamePath(gamePath)] = relativeFilePath.Replace('\\', '/');
            }

            var sourceRuntimeState = await ResolveSourceRuntimeGroupSelectionStateAsync(collectionState, token).ConfigureAwait(false);
            AddExistingRuntimeGroupsBackedBySourceMods(sourceRuntimeState.SourceBackedGroupNames, desiredGroups.Keys, collectionState, token);

            foreach (var sourceBackedGroupName in sourceRuntimeState.SourceBackedGroupNames)
            {
                if (desiredGroups.TryGetValue(sourceBackedGroupName, out var group))
                    group.SelectedOptionNames.Clear();
            }

            foreach (var mirroredGroup in sourceRuntimeState.SelectedOptionsByGroup)
            {
                if (!desiredGroups.TryGetValue(mirroredGroup.Key, out var group))
                    continue;

                foreach (var optionName in mirroredGroup.Value)
                {
                    if (group.Options.ContainsKey(optionName))
                        group.SelectedOptionNames.Add(optionName);
                }
            }

            RemoveGeneratedOffFallbackOptions(desiredGroups);

            var desiredGroupSelections = desiredGroups
                .Where(kvp => kvp.Value.SelectedOptionNames.Count > 0)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => (IReadOnlyList<string>)kvp.Value.SelectedOptionNames.OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToArray(),
                    StringComparer.OrdinalIgnoreCase);

            var desiredGroupFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var group in desiredGroups.Values.OrderBy(g => g.Name, StringComparer.OrdinalIgnoreCase))
            {
                token.ThrowIfCancellationRequested();

                if (group.Options.Count == 0)
                    continue;

                var groupFileName = ResolveRuntimeGroupFileName(group.Name, group.ManifestFileName);
                group.ManifestFileName = groupFileName;
                desiredGroupFiles.Add(groupFileName);
                contentChanged |= WriteRuntimeGroupManifest(Path.Combine(runtimeModPath, groupFileName), group);
            }

            contentChanged |= DeleteStaleRuntimeGroupFiles(runtimeModPath, desiredGroupFiles);

            var desiredFingerprint = BuildRuntimeFingerprint(selectedSourcesByGamePath, desiredGroupSelections, desiredOverrides);
            bool fingerprintChanged;
            lock (_appliedStateLock)
            {
                fingerprintChanged = !string.Equals(_lastAppliedRuntimeFingerprint, desiredFingerprint, StringComparison.Ordinal);
            }

            var runtimeModKey = await EnsureRuntimeModRegisteredAsync(collectionState, runtimeModPath, token).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(runtimeModKey))
            {
                _logger.LogWarning("Runtime converted animations mod could not be located after registration attempt for {path}; skipping redraw request", runtimeModPath);
                return false;
            }

            IpcCallerPenumbra.PenumbraCollectionModSettings runtimeCollectionState = collectionState;
            if (!runtimeCollectionState.Mods.ContainsKey(runtimeModKey))
            {
                runtimeCollectionState = await TryGetLocalPlayerCollectionSettingsAsync(token).ConfigureAwait(false) ?? collectionState;
            }

            runtimeCollectionState.Mods.TryGetValue(runtimeModKey, out var runtimeState);
            var runtimeStateChanged = false;

            if (contentChanged)
            {
                var reloadResult = await _ipcManager.Penumbra.ReloadModAsync(_logger, runtimeModKey).ConfigureAwait(false);
                if (reloadResult != PenumbraApiEc.Success && reloadResult != PenumbraApiEc.NothingChanged)
                {
                    _logger.LogWarning("Failed to reload runtime converted animations mod {mod}: {result}", runtimeModKey, reloadResult);
                }

                runtimeCollectionState = await TryGetLocalPlayerCollectionSettingsAsync(token).ConfigureAwait(false) ?? runtimeCollectionState;
                runtimeCollectionState.Mods.TryGetValue(runtimeModKey, out runtimeState);
            }

            var shouldEnable = desiredGroups.Values.Any(g => g.Options.Count > 0);
            var currentlyEnabled = runtimeState?.Enabled ?? false;
            if (shouldEnable != currentlyEnabled)
            {
                var enableResult = await _ipcManager.Penumbra.SetModStateAsync(_logger, runtimeCollectionState.CollectionId, runtimeModKey, shouldEnable).ConfigureAwait(false);
                if (enableResult != PenumbraApiEc.Success && enableResult != PenumbraApiEc.NothingChanged)
                {
                    _logger.LogWarning("Failed to set runtime converted animations mod enabled={enabled} in collection {collection}: {result}", shouldEnable, runtimeCollectionState.CollectionName, enableResult);
                }
                else if (enableResult == PenumbraApiEc.Success)
                {
                    runtimeStateChanged = true;
                }
            }

            var currentPriority = runtimeState?.Priority ?? int.MinValue;
            if (shouldEnable && currentPriority < RuntimePriorityFloor)
            {
                var priorityResult = await _ipcManager.Penumbra.SetModPriorityAsync(_logger, runtimeCollectionState.CollectionId, runtimeModKey, RuntimePriorityFloor).ConfigureAwait(false);
                if (priorityResult != PenumbraApiEc.Success && priorityResult != PenumbraApiEc.NothingChanged)
                {
                    _logger.LogWarning("Failed to raise runtime converted animations mod priority in collection {collection}: {result}", runtimeCollectionState.CollectionName, priorityResult);
                }
                else if (priorityResult == PenumbraApiEc.Success)
                {
                    runtimeStateChanged = true;
                }
            }

            if (runtimeStateChanged)
            {
                runtimeCollectionState = await TryGetLocalPlayerCollectionSettingsAsync(token).ConfigureAwait(false) ?? runtimeCollectionState;
                runtimeCollectionState.Mods.TryGetValue(runtimeModKey, out runtimeState);
            }

            var currentSelections = runtimeState?.Settings ?? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            {
                foreach (var group in desiredGroups.Values.OrderBy(g => g.Name, StringComparer.OrdinalIgnoreCase))
                {
                    token.ThrowIfCancellationRequested();

                    if (group.Options.Count == 0)
                        continue;

                    if (!sourceRuntimeState.SourceBackedGroupNames.Contains(group.Name))
                        continue;

                    var desiredForGroup = group.SelectedOptionNames
                        .Where(group.Options.ContainsKey)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(o => o, StringComparer.OrdinalIgnoreCase)
                        .ToArray();

                    currentSelections.TryGetValue(group.Name, out var currentForGroupList);
                    var currentForGroup = (currentForGroupList ?? [])
                        .Where(group.Options.ContainsKey)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(o => o, StringComparer.OrdinalIgnoreCase)
                        .ToArray();

                    if (desiredForGroup.SequenceEqual(currentForGroup, StringComparer.OrdinalIgnoreCase))
                        continue;

                    var result = await _ipcManager.Penumbra.SetModSettingsAsync(_logger, runtimeCollectionState.CollectionId, runtimeModKey, group.Name, desiredForGroup).ConfigureAwait(false);
                    if (result != PenumbraApiEc.Success && result != PenumbraApiEc.NothingChanged)
                    {
                        _logger.LogWarning("Failed to set runtime converted animation group {group} selections [{options}]: {result}", group.Name, string.Join(", ", desiredForGroup), result);
                    }
                    else
                    {
                        currentSelections[group.Name] = desiredForGroup.ToList();

                        if (result == PenumbraApiEc.Success)
                            runtimeStateChanged = true;
                    }
                }
            }

            if (contentChanged || fingerprintChanged || runtimeStateChanged)
            {
                lock (_appliedStateLock)
                {
                    _lastAppliedRuntimeFingerprint = desiredFingerprint;
                }
            }

            // A fingerprint-only change still means the source Penumbra selection changed. The generated
            // runtime conversion mod may not need rebuilding, but the local player still needs a redraw so
            // the newly-selected PAP is reflected immediately, especially while an emote is already active.
            return contentChanged || fingerprintChanged || runtimeStateChanged;
        }
        finally
        {
            _syncGate.Release();
        }
    }


    private async Task<List<ManifestSupportCandidate>> LoadSelectedManifestAllFileCandidatesAsync(string modPath, Dictionary<string, List<string>> selectedSettings, int priority, CancellationToken token)
    {
        var output = new List<ManifestSupportCandidate>();

        bool loadedCurrentManifest = false;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            loadedCurrentManifest = true;
            ImportCurrentManifestAllFiles(output, modPath, priority, defaultManifest, token);
            await Task.Yield();
        }

        var scannedGroups = 0;
        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            loadedCurrentManifest = true;

            ImportCurrentGroupManifestAllFiles(output, modPath, selectedSettings, priority, groupManifest, token);

            if (++scannedGroups % ManifestScanYieldStride == 0)
                await Task.Yield();
        }

        if (!loadedCurrentManifest)
        {
            var legacyMeta = Path.Combine(modPath, "meta.json");
            if (File.Exists(legacyMeta))
            {
                ImportLegacyMetaManifestAllFiles(output, modPath, selectedSettings, priority, legacyMeta, token);
                await Task.Yield();
            }
        }

        return output;
    }

    private void ImportCurrentManifestAllFiles(List<ManifestSupportCandidate> output, string modPath, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        AddAllManifestFiles(output, modPath, priority, doc.RootElement, "Files");
    }

    private void ImportCurrentGroupManifestAllFiles(List<ManifestSupportCandidate> output, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        var selectedOptions = GetSelectedCurrentOptionNames(root, selectedSettings, manifestPath);
        if (selectedOptions.Count == 0)
            return;

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var optionName = optionNameElement.GetString() ?? string.Empty;
            if (!selectedOptions.Contains(optionName))
                continue;

            AddAllManifestFiles(output, modPath, priority, option, "Files");
        }
    }

    private void ImportLegacyMetaManifestAllFiles(List<ManifestSupportCandidate> output, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string metaPath, CancellationToken token)
    {
        using var stream = OpenReadShared(metaPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        AddAllManifestFiles(output, modPath, priority, root, "Files");
        AddAllManifestFiles(output, modPath, priority, root, "OptionFiles");

        if (!root.TryGetProperty("Groups", out var groups) || groups.ValueKind != JsonValueKind.Object)
            return;

        foreach (var group in groups.EnumerateObject())
        {
            token.ThrowIfCancellationRequested();
            var groupRoot = group.Value;
            var groupName = groupRoot.TryGetProperty("GroupName", out var groupNameElement)
                ? groupNameElement.GetString() ?? group.Name
                : group.Name;

            if (!selectedSettings.TryGetValue(groupName, out var selectedOptions) || selectedOptions == null || selectedOptions.Count == 0)
                continue;

            if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var option in options.EnumerateArray())
            {
                token.ThrowIfCancellationRequested();
                var optionName = option.TryGetProperty("OptionName", out var optionNameElement)
                    ? optionNameElement.GetString() ?? string.Empty
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(optionName) || !selectedOptions.Any(s => string.Equals(s, optionName, StringComparison.OrdinalIgnoreCase)))
                    continue;

                AddAllManifestFiles(output, modPath, priority, option, "OptionFiles");
                AddAllManifestFiles(output, modPath, priority, option, "Files");
            }
        }
    }

    private void AddAllManifestFiles(List<ManifestSupportCandidate> output, string modPath, int priority, JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var files) || files.ValueKind != JsonValueKind.Object)
            return;

        foreach (var property in files.EnumerateObject())
        {
            var gamePath = NormalizeGamePath(property.Name);
            if (string.IsNullOrWhiteSpace(gamePath) || !IsAllowedManifestGamePath(gamePath) || IsMountMusicLikeGamePath(gamePath))
                continue;

            var relativeValue = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(relativeValue))
                continue;

            var resolvedPath = ResolveManifestFilePath(modPath, relativeValue);
            if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                continue;

            output.Add(new ManifestSupportCandidate(resolvedPath, priority, [gamePath]));
        }
    }


    private static bool IsMountMusicLikeGamePath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var normalized = NormalizeGamePath(gamePath);
        return normalized.StartsWith("music/", StringComparison.OrdinalIgnoreCase)
            && string.Equals(Path.GetExtension(normalized), ".scd", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsAllowedManifestGamePath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var extension = Path.GetExtension(gamePath);
        return !string.IsNullOrWhiteSpace(extension)
            && (CacheMonitor.AllowedFileExtensions.Contains(extension, StringComparer.OrdinalIgnoreCase)
                || string.Equals(extension, ".atch", StringComparison.OrdinalIgnoreCase));
    }

    private static bool IsActiveMountModGamePath(string? gamePath, HashSet<string> activeMountIds)
    {
        if (string.IsNullOrWhiteSpace(gamePath) || activeMountIds.Count == 0)
            return false;

        var normalized = NormalizeGamePath(gamePath);
        foreach (var mountId in activeMountIds)
        {
            if (normalized.Contains($"/monster/{mountId}/", StringComparison.OrdinalIgnoreCase)
                || normalized.Contains($"/mt_{mountId}/", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }


    private static bool IsActiveMountRiderGamePath(string? gamePath, HashSet<string> activeMountIds)
    {
        if (string.IsNullOrWhiteSpace(gamePath) || activeMountIds.Count == 0)
            return false;

        var normalized = NormalizeGamePath(gamePath);
        if (!normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase) || !normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase))
            return false;

        foreach (var mountId in activeMountIds)
        {
            if (normalized.Contains($"/mt_{mountId}/", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static string NormalizeMountActorId(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.Replace('\\', '/').Trim().ToLowerInvariant();
        var match = System.Text.RegularExpressions.Regex.Match(normalized, @"(?:^|/|_)((?:m|d)\d{4})(?:/|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        return match.Success ? match.Groups[1].Value.ToLowerInvariant() : normalized.Trim('_', '/');
    }

    private async Task<List<ManifestSupportCandidate>> LoadSelectedManifestMountMusicCandidatesAsync(string modPath, Dictionary<string, List<string>> selectedSettings, int priority, CancellationToken token)
    {
        var output = new List<ManifestSupportCandidate>();

        bool loadedCurrentManifest = false;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            loadedCurrentManifest = true;
            ImportCurrentManifestMountMusicFiles(output, modPath, priority, defaultManifest, token);
            await Task.Yield();
        }

        var scannedGroups = 0;
        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            loadedCurrentManifest = true;

            ImportCurrentGroupManifestMountMusicFiles(output, modPath, selectedSettings, priority, groupManifest, token);

            if (++scannedGroups % ManifestScanYieldStride == 0)
                await Task.Yield();
        }

        if (!loadedCurrentManifest)
        {
            var legacyMeta = Path.Combine(modPath, "meta.json");
            if (File.Exists(legacyMeta))
            {
                ImportLegacyMetaManifestMountMusicFiles(output, modPath, selectedSettings, priority, legacyMeta, token);
                await Task.Yield();
            }
        }

        return output;
    }

    private void ImportCurrentManifestMountMusicFiles(List<ManifestSupportCandidate> output, string modPath, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        AddMountMusicManifestFiles(output, modPath, priority, doc.RootElement, "Files");
    }

    private void ImportCurrentGroupManifestMountMusicFiles(List<ManifestSupportCandidate> output, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        var selectedOptions = GetSelectedCurrentOptionNames(root, selectedSettings, manifestPath);
        if (selectedOptions.Count == 0)
            return;

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();

            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var optionName = optionNameElement.GetString() ?? string.Empty;
            if (!selectedOptions.Contains(optionName))
                continue;

            AddMountMusicManifestFiles(output, modPath, priority, option, "Files");
        }
    }

    private void ImportLegacyMetaManifestMountMusicFiles(List<ManifestSupportCandidate> output, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string metaPath, CancellationToken token)
    {
        using var stream = OpenReadShared(metaPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        AddMountMusicManifestFiles(output, modPath, priority, root, "Files");
        AddMountMusicManifestFiles(output, modPath, priority, root, "OptionFiles");

        if (!root.TryGetProperty("Groups", out var groups) || groups.ValueKind != JsonValueKind.Object)
            return;

        foreach (var group in groups.EnumerateObject())
        {
            token.ThrowIfCancellationRequested();

            var groupRoot = group.Value;
            var groupName = groupRoot.TryGetProperty("GroupName", out var groupNameElement)
                ? groupNameElement.GetString() ?? group.Name
                : group.Name;

            if (!selectedSettings.TryGetValue(groupName, out var selectedOptions) || selectedOptions == null || selectedOptions.Count == 0)
                continue;

            if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var option in options.EnumerateArray())
            {
                token.ThrowIfCancellationRequested();

                var optionName = option.TryGetProperty("OptionName", out var optionNameElement)
                    ? optionNameElement.GetString() ?? string.Empty
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(optionName) || !selectedOptions.Any(s => string.Equals(s, optionName, StringComparison.OrdinalIgnoreCase)))
                    continue;

                AddMountMusicManifestFiles(output, modPath, priority, option, "OptionFiles");
                AddMountMusicManifestFiles(output, modPath, priority, option, "Files");
            }
        }
    }

    private void AddMountMusicManifestFiles(List<ManifestSupportCandidate> output, string modPath, int priority, JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var files) || files.ValueKind != JsonValueKind.Object)
            return;

        foreach (var property in files.EnumerateObject())
        {
            var gamePath = NormalizeGamePath(property.Name);
            if (!PairApplyUtilities.IsMountMusicGamePath(gamePath))
                continue;

            var relativeValue = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(relativeValue))
                continue;

            var resolvedPath = ResolveManifestFilePath(modPath, relativeValue);
            if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                continue;

            output.Add(new ManifestSupportCandidate(resolvedPath, priority, [gamePath]));
        }
    }

    private async Task<(List<ManifestSupportCandidate> Candidates, bool HasSelectedHumanAnimationPap)> LoadSelectedManifestAnimationSupportCandidatesAsync(string modPath, Dictionary<string, List<string>> selectedSettings, int priority, CancellationToken token)
    {
        var output = new List<ManifestSupportCandidate>();
        var hasSelectedHumanAnimationPap = false;

        bool loadedCurrentManifest = false;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            loadedCurrentManifest = true;
            ImportCurrentManifestAnimationSupportFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, defaultManifest, token);
            await Task.Yield();
        }

        var scannedGroups = 0;
        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            loadedCurrentManifest = true;

            ImportCurrentGroupManifestAnimationSupportFiles(output, ref hasSelectedHumanAnimationPap, modPath, selectedSettings, priority, groupManifest, token);

            if (++scannedGroups % ManifestScanYieldStride == 0)
                await Task.Yield();
        }

        if (!loadedCurrentManifest)
        {
            var legacyMeta = Path.Combine(modPath, "meta.json");
            if (File.Exists(legacyMeta))
            {
                ImportLegacyMetaManifestAnimationSupportFiles(output, ref hasSelectedHumanAnimationPap, modPath, selectedSettings, priority, legacyMeta, token);
                await Task.Yield();
            }
        }

        return (output, hasSelectedHumanAnimationPap);
    }

    private void ImportCurrentManifestAnimationSupportFiles(List<ManifestSupportCandidate> output, ref bool hasSelectedHumanAnimationPap, string modPath, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, doc.RootElement, "Files");
    }

    private void ImportCurrentGroupManifestAnimationSupportFiles(List<ManifestSupportCandidate> output, ref bool hasSelectedHumanAnimationPap, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        var selectedOptions = GetSelectedCurrentOptionNames(root, selectedSettings, manifestPath);
        if (selectedOptions.Count == 0)
            return;

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();

            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var optionName = optionNameElement.GetString() ?? string.Empty;
            if (!selectedOptions.Contains(optionName))
                continue;

            AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, option, "Files");
        }
    }

    private void ImportLegacyMetaManifestAnimationSupportFiles(List<ManifestSupportCandidate> output, ref bool hasSelectedHumanAnimationPap, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, string metaPath, CancellationToken token)
    {
        using var stream = OpenReadShared(metaPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, root, "Files");
        AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, root, "OptionFiles");

        if (!root.TryGetProperty("Groups", out var groups) || groups.ValueKind != JsonValueKind.Object)
            return;

        foreach (var group in groups.EnumerateObject())
        {
            token.ThrowIfCancellationRequested();

            var groupRoot = group.Value;
            var groupName = groupRoot.TryGetProperty("GroupName", out var groupNameElement)
                ? groupNameElement.GetString() ?? group.Name
                : group.Name;

            if (!selectedSettings.TryGetValue(groupName, out var selectedOptions) || selectedOptions == null || selectedOptions.Count == 0)
                continue;

            if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var option in options.EnumerateArray())
            {
                token.ThrowIfCancellationRequested();

                var optionName = option.TryGetProperty("OptionName", out var optionNameElement)
                    ? optionNameElement.GetString() ?? string.Empty
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(optionName) || !selectedOptions.Any(s => string.Equals(s, optionName, StringComparison.OrdinalIgnoreCase)))
                    continue;

                AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, option, "OptionFiles");
                AddAnimationSupportManifestFiles(output, ref hasSelectedHumanAnimationPap, modPath, priority, option, "Files");
            }
        }
    }

    private void AddAnimationSupportManifestFiles(List<ManifestSupportCandidate> output, ref bool hasSelectedHumanAnimationPap, string modPath, int priority, JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var files) || files.ValueKind != JsonValueKind.Object)
            return;

        foreach (var property in files.EnumerateObject())
        {
            var gamePath = NormalizeGamePath(property.Name);

            if (XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath(gamePath))
            {
                hasSelectedHumanAnimationPap = true;
                continue;
            }

            if (!IsAnimationModelSupportGamePath(gamePath))
                continue;

            var relativeValue = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(relativeValue))
                continue;

            var resolvedPath = ResolveManifestFilePath(modPath, relativeValue);
            if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                continue;

            output.Add(new ManifestSupportCandidate(resolvedPath, priority, [gamePath]));
        }
    }

    private static bool IsAnimationModelSupportGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var normalized = NormalizeGamePath(gamePath);
        if (!(normalized.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".scd", StringComparison.OrdinalIgnoreCase)
            || normalized.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        // These are support assets commonly referenced by animation/VFX payloads.
        // They need to be present in the pushed temp mod before Penumbra's async loader
        // asks for them, otherwise the first async load can fail even though the main PAP/TMB exists.
        return normalized.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("music/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("bgm/", StringComparison.OrdinalIgnoreCase)
            || IsHumanObjectAnimationSupportGamePath(normalized);
    }

    private static bool IsHumanObjectAnimationSupportGamePath(string normalizedGamePath)
    {
        if (!normalizedGamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase))
            return false;

        return normalizedGamePath.Contains("/obj/hair/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.Contains("/obj/face/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.Contains("/obj/tail/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.Contains("/obj/zear/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.Contains("/obj/ear/", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<List<ManifestPapCandidate>> LoadSelectedManifestPapCandidatesAsync(string modDirectoryRoot, string relativeModDirectory, string modPath, Dictionary<string, List<string>> selectedSettings, int priority, CancellationToken token)
    {
        var output = new List<ManifestPapCandidate>();
        var displayName = ReadModDisplayName(modPath);

        bool loadedCurrentManifest = false;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            loadedCurrentManifest = true;
            ImportCurrentManifestFiles(output, modPath, relativeModDirectory, displayName, "Default", priority, defaultManifest, token);
            await Task.Yield();
        }

        var scannedGroups = 0;
        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            loadedCurrentManifest = true;

            ImportCurrentGroupManifestFiles(output, modPath, relativeModDirectory, displayName, selectedSettings, priority, groupManifest, token);

            if (++scannedGroups % ManifestScanYieldStride == 0)
                await Task.Yield();
        }

        if (!loadedCurrentManifest)
        {
            var legacyMeta = Path.Combine(modPath, "meta.json");
            if (File.Exists(legacyMeta))
            {
                ImportLegacyMetaManifestFiles(output, modPath, relativeModDirectory, displayName, selectedSettings, priority, legacyMeta, token);
                await Task.Yield();
            }
        }

        return output;
    }

    private void ImportCurrentManifestFiles(List<ManifestPapCandidate> output, string modPath, string relativeModDirectory, string displayName, string optionDisplayName, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        AddManifestFiles(output, modPath, relativeModDirectory, displayName, "Default", "Single", optionDisplayName, priority, doc.RootElement, "Files");
    }

    private void ImportCurrentGroupManifestFiles(List<ManifestPapCandidate> output, string modPath, string relativeModDirectory, string displayName, Dictionary<string, List<string>> selectedSettings, int priority, string manifestPath, CancellationToken token)
    {
        using var stream = OpenReadShared(manifestPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        var selectedOptions = GetSelectedCurrentOptionNames(root, selectedSettings, manifestPath);
        if (selectedOptions.Count == 0)
            return;

        if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return;

        var groupName = root.TryGetProperty("Name", out var groupNameElement)
            ? groupNameElement.GetString() ?? string.Empty
            : string.Empty;
        var groupType = root.TryGetProperty("Type", out var groupTypeElement)
            ? groupTypeElement.GetString() ?? string.Empty
            : string.Empty;

        foreach (var option in options.EnumerateArray())
        {
            token.ThrowIfCancellationRequested();
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var optionName = optionNameElement.GetString() ?? string.Empty;
            if (!selectedOptions.Contains(optionName))
                continue;

            AddManifestFiles(output, modPath, relativeModDirectory, displayName, groupName, groupType, optionName, priority, option, "Files");
        }
    }

    private void ImportLegacyMetaManifestFiles(List<ManifestPapCandidate> output, string modPath, string relativeModDirectory, string displayName, Dictionary<string, List<string>> selectedSettings, int priority, string metaPath, CancellationToken token)
    {
        using var stream = OpenReadShared(metaPath);
        using var doc = JsonDocument.Parse(stream);
        var root = doc.RootElement;

        AddManifestFiles(output, modPath, relativeModDirectory, displayName, "Default", "Single", "Default", priority, root, "Files");
        AddManifestFiles(output, modPath, relativeModDirectory, displayName, "Default", "Single", "Default", priority, root, "OptionFiles");

        if (!root.TryGetProperty("Groups", out var groups) || groups.ValueKind != JsonValueKind.Object)
            return;

        foreach (var group in groups.EnumerateObject())
        {
            token.ThrowIfCancellationRequested();
            var groupRoot = group.Value;
            var groupName = groupRoot.TryGetProperty("GroupName", out var groupNameElement)
                ? groupNameElement.GetString() ?? group.Name
                : group.Name;
            var groupType = groupRoot.TryGetProperty("Type", out var groupTypeElement)
                ? groupTypeElement.GetString() ?? string.Empty
                : string.Empty;

            if (!selectedSettings.TryGetValue(groupName, out var selectedOptions) || selectedOptions == null || selectedOptions.Count == 0)
                continue;

            if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var option in options.EnumerateArray())
            {
                token.ThrowIfCancellationRequested();
                var optionName = option.TryGetProperty("OptionName", out var optionNameElement)
                    ? optionNameElement.GetString() ?? string.Empty
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(optionName) || !selectedOptions.Any(s => string.Equals(s, optionName, StringComparison.OrdinalIgnoreCase)))
                    continue;

                AddManifestFiles(output, modPath, relativeModDirectory, displayName, groupName, groupType, optionName, priority, option, "OptionFiles");
                AddManifestFiles(output, modPath, relativeModDirectory, displayName, groupName, groupType, optionName, priority, option, "Files");
            }
        }
    }

    private static HashSet<string> GetSelectedCurrentOptionNames(JsonElement groupRoot, Dictionary<string, List<string>> selectedSettings, string manifestPath)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
            return output;

        var optionNames = new List<string>();
        foreach (var option in options.EnumerateArray())
        {
            if (!option.TryGetProperty("Name", out var optionNameElement))
                continue;

            var name = optionNameElement.GetString();
            if (!string.IsNullOrWhiteSpace(name))
                optionNames.Add(name);
        }

        if (optionNames.Count == 0)
            return output;

        var groupType = groupRoot.TryGetProperty("Type", out var typeElement)
            ? typeElement.GetString() ?? string.Empty
            : string.Empty;

        if (TryGetSelectedCurrentSettingsForGroup(groupRoot, manifestPath, selectedSettings, out var selected) && selected != null)
        {
            AddSelectedCurrentOptionNamesFromSettingValues(output, optionNames, groupType, selected);
            RemoveInactiveCurrentOptionNames(output);
            return output;
        }

        AddDefaultCurrentOptionNames(output, optionNames, groupType, groupRoot);
        RemoveInactiveCurrentOptionNames(output);
        return output;
    }

    private static bool TryGetSelectedCurrentSettingsForGroup(JsonElement groupRoot, string manifestPath, Dictionary<string, List<string>> selectedSettings, out List<string>? selected)
    {
        selected = null;
        if (selectedSettings == null || selectedSettings.Count == 0)
            return false;

        var aliases = BuildCurrentManifestGroupAliases(groupRoot, manifestPath);
        foreach (var alias in aliases.RawAliases)
        {
            if (selectedSettings.TryGetValue(alias, out selected))
                return true;
        }

        foreach (var kvp in selectedSettings)
        {
            var key = kvp.Key ?? string.Empty;
            if (aliases.NormalizedAliases.Contains(NormalizeGamePath(key))
                || aliases.LooseAliases.Contains(NormalizeCurrentSelectionKey(key)))
            {
                selected = kvp.Value;
                return true;
            }
        }

        return false;
    }

    private static CurrentManifestGroupAliases BuildCurrentManifestGroupAliases(JsonElement groupRoot, string manifestPath)
    {
        var rawAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var normalizedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var looseAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void AddAlias(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            var trimmed = value.Trim();
            rawAliases.Add(trimmed);
            normalizedAliases.Add(NormalizeGamePath(trimmed));
            looseAliases.Add(NormalizeCurrentSelectionKey(trimmed));
        }

        var groupName = groupRoot.TryGetProperty("Name", out var groupNameElement)
            ? groupNameElement.GetString() ?? string.Empty
            : string.Empty;
        AddAlias(groupName);

        var manifestFileName = Path.GetFileName(manifestPath);
        var manifestStem = Path.GetFileNameWithoutExtension(manifestPath);
        AddAlias(manifestFileName);
        AddAlias(manifestStem);

        var normalizedStem = NormalizeGamePath(manifestStem);
        var match = System.Text.RegularExpressions.Regex.Match(normalizedStem, @"^group[_\- ]*(\d+)(?:[_\- ]+(.*))?$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (match.Success)
        {
            var numberText = match.Groups[1].Value;
            AddAlias(numberText);
            if (int.TryParse(numberText, out var number))
            {
                AddAlias(number.ToString());
                AddAlias($"group_{number}");
                AddAlias($"group {number}");
            }

            AddAlias($"group_{numberText}");
            AddAlias($"group {numberText}");

            var suffix = match.Groups.Count > 2 ? match.Groups[2].Value : string.Empty;
            if (!string.IsNullOrWhiteSpace(suffix))
            {
                AddAlias(suffix);
                AddAlias(suffix.Replace('_', ' ').Replace('-', ' '));
                AddAlias($"{numberText}_{suffix}");
                AddAlias($"{numberText} {suffix.Replace('_', ' ').Replace('-', ' ')}");

                if (int.TryParse(numberText, out var numericSuffixNumber))
                {
                    AddAlias($"{numericSuffixNumber}_{suffix}");
                    AddAlias($"{numericSuffixNumber} {suffix.Replace('_', ' ').Replace('-', ' ')}");
                }
            }
        }

        return new CurrentManifestGroupAliases(rawAliases, normalizedAliases, looseAliases);
    }

    private static void AddDefaultCurrentOptionNames(HashSet<string> output, IReadOnlyList<string> optionNames, string groupType, JsonElement groupRoot)
    {
        if (!groupRoot.TryGetProperty("DefaultSettings", out var defaultSettings)
            || defaultSettings.ValueKind != JsonValueKind.Number
            || !defaultSettings.TryGetInt32(out var defaultValue))
        {
            return;
        }

        if (string.Equals(groupType, "Single", StringComparison.OrdinalIgnoreCase))
        {
            if (defaultValue >= 0 && defaultValue < optionNames.Count)
                output.Add(optionNames[defaultValue]);
            return;
        }

        if (string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase))
        {
            if (defaultValue <= 0)
                return;

            for (var i = 0; i < optionNames.Count && i < 31; i++)
            {
                if ((defaultValue & (1 << i)) != 0)
                    output.Add(optionNames[i]);
            }

            return;
        }

        if (defaultValue >= 0 && defaultValue < optionNames.Count)
            output.Add(optionNames[defaultValue]);
    }

    private static void AddSelectedCurrentOptionNamesFromSettingValues(HashSet<string> output, IReadOnlyList<string> optionNames, string groupType, IEnumerable<string>? selectedValues)
    {
        if (selectedValues == null || optionNames.Count == 0)
            return;

        var values = EnumerateCurrentSelectedSettingValues(selectedValues).ToList();
        if (values.Count == 0)
            return;

        if (TryAddCurrentBooleanSelectedOptionNames(output, optionNames, values))
            return;

        var isMultiGroup = string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase);
        foreach (var value in values)
        {
            if (TryAddCurrentSelectedOptionNameByAlias(output, optionNames, value))
                continue;

            var assignment = SplitCurrentSelectionAssignment(value);
            if (!string.IsNullOrWhiteSpace(assignment.Name) && TryParseCurrentSelectionBoolean(assignment.Value, out var enabled))
            {
                if (enabled)
                    TryAddCurrentSelectedOptionNameByAlias(output, optionNames, assignment.Name);

                continue;
            }

            if (!int.TryParse(value, out var numericValue))
                continue;

            if (isMultiGroup && values.Count == 1 && numericValue > 0)
            {
                var addedFromBitmask = false;
                for (var i = 0; i < optionNames.Count && i < 31; i++)
                {
                    if ((numericValue & (1 << i)) == 0)
                        continue;

                    output.Add(optionNames[i]);
                    addedFromBitmask = true;
                }

                if (addedFromBitmask)
                    continue;
            }

            if (isMultiGroup && numericValue == 0)
                continue;

            if (numericValue >= 0 && numericValue < optionNames.Count)
            {
                output.Add(optionNames[numericValue]);
                continue;
            }

            if (numericValue > 0 && isMultiGroup)
            {
                for (var i = 0; i < optionNames.Count && i < 31; i++)
                {
                    if ((numericValue & (1 << i)) != 0)
                        output.Add(optionNames[i]);
                }
            }
        }
    }

    private static IEnumerable<string> EnumerateCurrentSelectedSettingValues(IEnumerable<string> selectedValues)
    {
        var emitted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var rawValue in selectedValues)
        {
            if (string.IsNullOrWhiteSpace(rawValue))
                continue;

            foreach (var expanded in ExpandCurrentSelectedSettingValue(rawValue))
            {
                var value = expanded.Trim().Trim('"', '\'', '[', ']');
                if (!string.IsNullOrWhiteSpace(value) && emitted.Add(value))
                    yield return value;
            }
        }
    }

    private static IEnumerable<string> ExpandCurrentSelectedSettingValue(string rawValue)
    {
        var value = rawValue.Trim();
        if (string.IsNullOrWhiteSpace(value))
            yield break;

        yield return value;

        var trimmed = value.Trim().Trim('[', ']');
        foreach (var part in trimmed.Split(['\0', '\r', '\n', ';', ',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!string.IsNullOrWhiteSpace(part))
                yield return part;
        }
    }

    private static bool TryAddCurrentBooleanSelectedOptionNames(HashSet<string> output, IReadOnlyList<string> optionNames, IReadOnlyList<string> values)
    {
        if (values.Count != optionNames.Count)
            return false;

        var parsed = new bool[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            if (!TryParseCurrentSelectionBoolean(values[i], out parsed[i]))
                return false;
        }

        for (var i = 0; i < parsed.Length; i++)
        {
            if (parsed[i])
                output.Add(optionNames[i]);
        }

        return true;
    }

    private static bool TryParseCurrentSelectionBoolean(string? value, out bool result)
    {
        result = false;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalized = NormalizeCurrentSelectionKey(value);
        if (normalized is "true" or "enabled" or "enable" or "on" or "yes" or "y")
        {
            result = true;
            return true;
        }

        if (normalized is "false" or "disabled" or "disable" or "off" or "no" or "n" or "none" or "null")
        {
            result = false;
            return true;
        }

        return false;
    }

    private static (string Name, string Value) SplitCurrentSelectionAssignment(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return (string.Empty, string.Empty);

        var separatorIndex = value.IndexOf('=');
        if (separatorIndex < 0)
            separatorIndex = value.IndexOf(':');

        if (separatorIndex <= 0 || separatorIndex >= value.Length - 1)
            return (string.Empty, string.Empty);

        return (value[..separatorIndex].Trim().Trim('"', '\''), value[(separatorIndex + 1)..].Trim().Trim('"', '\''));
    }

    private static bool TryAddCurrentSelectedOptionNameByAlias(HashSet<string> output, IReadOnlyList<string> optionNames, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalizedValue = NormalizeGamePath(value);
        var looseValue = NormalizeCurrentSelectionKey(value);
        foreach (var optionName in optionNames)
        {
            if (string.Equals(optionName, value, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizeGamePath(optionName), normalizedValue, StringComparison.OrdinalIgnoreCase)
                || string.Equals(NormalizeCurrentSelectionKey(optionName), looseValue, StringComparison.OrdinalIgnoreCase))
            {
                output.Add(optionName);
                return true;
            }
        }

        return false;
    }

    private static void RemoveInactiveCurrentOptionNames(HashSet<string> optionNames)
    {
        if (optionNames.Count == 0)
            return;

        optionNames.RemoveWhere(IsInactiveCurrentOptionName);
    }

    private static bool IsInactiveCurrentOptionName(string? optionName)
    {
        if (string.IsNullOrWhiteSpace(optionName))
            return false;

        return NormalizeCurrentSelectionKey(optionName) is "off" or "none" or "no" or "false" or "null" or "disabled" or "disable" or "inactive" or "notactive";
    }

    private static string NormalizeCurrentSelectionKey(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = NormalizeGamePath(value);
        return System.Text.RegularExpressions.Regex.Replace(normalized, @"[^a-z0-9]+", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            .ToLowerInvariant();
    }

    private void AddManifestFiles(List<ManifestPapCandidate> output, string modPath, string relativeModDirectory, string displayName, string sourceGroupName, string sourceGroupType, string optionDisplayName, int priority, JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var files) || files.ValueKind != JsonValueKind.Object)
            return;

        foreach (var property in files.EnumerateObject())
        {
            if (!XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath(property.Name))
                continue;

            var relativeValue = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()
                : null;
            if (string.IsNullOrWhiteSpace(relativeValue))
                continue;

            var resolvedPath = ResolveManifestFilePath(modPath, relativeValue);
            if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                continue;

            output.Add(new ManifestPapCandidate(
                NormalizeRelativeDirectory(relativeModDirectory),
                string.IsNullOrWhiteSpace(displayName) ? Path.GetFileName(modPath) : displayName,
                string.IsNullOrWhiteSpace(sourceGroupName) ? "Default" : sourceGroupName,
                NormalizeRuntimeGroupType(sourceGroupType),
                string.IsNullOrWhiteSpace(optionDisplayName) ? "Default" : optionDisplayName,
                resolvedPath,
                priority,
                [NormalizeGamePath(property.Name)]));
        }
    }

    private static string ResolveManifestFilePath(string modPath, string? manifestValue)
    {
        if (string.IsNullOrWhiteSpace(manifestValue))
            return string.Empty;

        var normalized = manifestValue.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar);
        var combined = Path.IsPathRooted(normalized)
            ? normalized
            : Path.Combine(modPath, normalized);

        try
        {
            return Path.GetFullPath(combined);
        }
        catch
        {
            return combined;
        }
    }

    private static string ResolveModDirectory(string modDirectoryRoot, string modKey)
    {
        if (string.IsNullOrWhiteSpace(modKey))
            return modDirectoryRoot;

        if (Path.IsPathRooted(modKey))
            return Path.GetFullPath(modKey);

        return Path.GetFullPath(Path.Combine(modDirectoryRoot, modKey));
    }

    private static string ReadModDisplayName(string modPath)
    {
        var metaPath = Path.Combine(modPath, "meta.json");
        if (!File.Exists(metaPath))
            return Path.GetFileName(modPath);

        try
        {
            using var stream = OpenReadShared(metaPath);
            using var doc = JsonDocument.Parse(stream);
            if (doc.RootElement.TryGetProperty("Name", out var nameElement))
            {
                var name = nameElement.GetString();
                if (!string.IsNullOrWhiteSpace(name))
                    return name;
            }
        }
        catch
        {
            // fall back below
        }

        return Path.GetFileName(modPath);
    }

    private static string NormalizeRelativeDirectory(string? relativeDirectory)
        => string.IsNullOrWhiteSpace(relativeDirectory)
            ? string.Empty
            : relativeDirectory.Replace('/', '\\').Trim().Trim('\\');

    private static string NormalizeFullPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        try
        {
            return Path.GetFullPath(path).Replace('\\', '/').TrimEnd('/');
        }
        catch
        {
            return path.Replace('\\', '/').TrimEnd('/');
        }
    }

    private static string NormalizeGamePath(string? gamePath)
        => string.IsNullOrWhiteSpace(gamePath)
            ? string.Empty
            : gamePath.Replace('\\', '/').Trim().ToLowerInvariant();

    private Dictionary<string, DesiredRuntimeGroup> LoadExistingRuntimeGroups(string runtimeModPath)
    {
        var output = new Dictionary<string, DesiredRuntimeGroup>(StringComparer.OrdinalIgnoreCase);
        if (!Directory.Exists(runtimeModPath))
            return output;

        foreach (var groupFile in Directory.EnumerateFiles(runtimeModPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            try
            {
                using var stream = OpenReadShared(groupFile);
                using var doc = JsonDocument.Parse(stream);
                var root = doc.RootElement;
                var groupName = root.TryGetProperty("Name", out var nameElement)
                    ? nameElement.GetString() ?? string.Empty
                    : string.Empty;
                if (string.IsNullOrWhiteSpace(groupName))
                    continue;

                var group = new DesiredRuntimeGroup
                {
                    Name = groupName,
                    Type = root.TryGetProperty("Type", out var typeElement)
                        ? NormalizeRuntimeGroupType(typeElement.GetString())
                        : "Single",
                    ManifestFileName = Path.GetFileName(groupFile),
                    Description = root.TryGetProperty("Description", out var descriptionElement)
                        ? descriptionElement.GetString() ?? string.Empty
                        : string.Empty,
                };

                if (root.TryGetProperty("Options", out var options) && options.ValueKind == JsonValueKind.Array)
                {
                    foreach (var optionElement in options.EnumerateArray())
                    {
                        if (!optionElement.TryGetProperty("Name", out var optionNameElement))
                            continue;

                        var optionName = optionNameElement.GetString() ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(optionName))
                            continue;

                        var option = new DesiredRuntimeOption
                        {
                            Name = optionName,
                            Description = optionElement.TryGetProperty("Description", out var optionDescriptionElement)
                                ? optionDescriptionElement.GetString() ?? string.Empty
                                : string.Empty,
                        };

                        if (optionElement.TryGetProperty("Files", out var filesElement) && filesElement.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var fileProperty in filesElement.EnumerateObject())
                            {
                                var fileValue = fileProperty.Value.ValueKind == JsonValueKind.String
                                    ? fileProperty.Value.GetString()
                                    : null;
                                if (string.IsNullOrWhiteSpace(fileValue))
                                    continue;

                                option.Files[NormalizeGamePath(fileProperty.Name)] = fileValue.Replace('\\', '/');
                            }
                        }

                        group.Options[option.Name] = option;
                    }
                }

                if (output.TryGetValue(group.Name, out var existing)
                    && !ShouldPreferRuntimeGroupFileName(group.ManifestFileName, existing.ManifestFileName))
                {
                    continue;
                }

                output[group.Name] = group;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Ignoring malformed generated runtime group manifest {file}", groupFile);
            }
        }

        return output;
    }

    private static Dictionary<string, DesiredRuntimeGroup> CloneDesiredGroups(IReadOnlyDictionary<string, DesiredRuntimeGroup> existingGroups)
    {
        var output = new Dictionary<string, DesiredRuntimeGroup>(StringComparer.OrdinalIgnoreCase);
        foreach (var group in existingGroups.Values)
        {
            var clone = new DesiredRuntimeGroup
            {
                Name = group.Name,
                Type = "Multi",
                ManifestFileName = group.ManifestFileName,
                Description = string.Empty,
            };

            foreach (var option in group.Options.Values)
            {
                var optionClone = new DesiredRuntimeOption
                {
                    Name = option.Name,
                    Description = string.Empty,
                };

                foreach (var file in option.Files)
                    optionClone.Files[file.Key] = file.Value;

                clone.Options[optionClone.Name] = optionClone;
            }

            output[clone.Name] = clone;
        }

        return output;
    }

    private static (string DisplayModName, string SourceGroupName, string SourceGroupType, string OptionDisplayName) ResolveSourceInfoForOverride(
        IReadOnlyDictionary<string, ManifestPapSource> selectedSourcesByGamePath,
        SanitizedPapOverride desired,
        string[] gamePaths)
    {
        foreach (var gamePath in gamePaths)
        {
            if (selectedSourcesByGamePath.TryGetValue(gamePath, out var source))
            {
                return (source.DisplayModName, source.SourceGroupName, source.SourceGroupType, source.OptionDisplayName);
            }
        }

        return (Path.GetFileName(Path.GetDirectoryName(desired.OriginalResolvedPath) ?? desired.OriginalResolvedPath), "Default", "Single", "Converted");
    }

    private static string BuildRuntimeGroupName(string displayModName, string? sourceGroupName)
    {
        var modName = string.IsNullOrWhiteSpace(displayModName) ? "Unknown Mod" : displayModName.Trim();
        if (string.IsNullOrWhiteSpace(modName))
            modName = "Unknown Mod";

        var groupName = string.IsNullOrWhiteSpace(sourceGroupName) ? "Default" : sourceGroupName.Trim();
        if (string.IsNullOrWhiteSpace(groupName) || string.Equals(groupName, "Default", StringComparison.OrdinalIgnoreCase))
            return modName;

        return $"{modName} / {groupName}";
    }

    private static string BuildRuntimeGroupFileName(string groupName)
        => "group_" + SanitizeFileSegment(groupName) + ".json";

    private static string ResolveRuntimeGroupFileName(string groupName, string? existingFileName)
    {
        var fileName = Path.GetFileName(existingFileName ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(fileName)
            && fileName.StartsWith("group_", StringComparison.OrdinalIgnoreCase)
            && fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            return fileName;
        }

        return BuildRuntimeGroupFileName(groupName);
    }

    private static bool ShouldPreferRuntimeGroupFileName(string candidateFileName, string existingFileName)
    {
        var candidateIsPenumbraNamed = IsPenumbraNumberedGroupFileName(candidateFileName);
        var existingIsPenumbraNamed = IsPenumbraNumberedGroupFileName(existingFileName);
        if (candidateIsPenumbraNamed != existingIsPenumbraNamed)
            return candidateIsPenumbraNamed;

        return false;
    }

    private static bool IsPenumbraNumberedGroupFileName(string? fileName)
    {
        var name = Path.GetFileName(fileName ?? string.Empty);
        if (!name.StartsWith("group_", StringComparison.OrdinalIgnoreCase)
            || !name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var core = name.Substring("group_".Length, name.Length - "group_".Length - ".json".Length);
        return core.Length > 4
            && char.IsDigit(core[0])
            && char.IsDigit(core[1])
            && char.IsDigit(core[2])
            && core[3] == '_';
    }

    private static string NormalizeRuntimeGroupType(string? groupType)
        => string.Equals(groupType, "Multi", StringComparison.OrdinalIgnoreCase) ? "Multi" : "Single";

    private static void RemoveGeneratedOffFallbackOptions(Dictionary<string, DesiredRuntimeGroup> desiredGroups)
    {
        foreach (var group in desiredGroups.Values)
        {
            if (!group.Options.TryGetValue("Off", out var option))
                continue;

            if (option.Files.Count != 0)
                continue;

            if (!string.Equals(option.Description, $"Off state for {group.Name} converted animations.", StringComparison.Ordinal))
                continue;

            group.Options.Remove("Off");
            group.SelectedOptionNames.Remove("Off");
        }
    }

    private void AddExistingRuntimeGroupsBackedBySourceMods(HashSet<string> output, IEnumerable<string> runtimeGroupNames, IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, CancellationToken token)
    {
        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return;

        var existingRuntimeGroups = runtimeGroupNames
            .Where(g => !string.IsNullOrWhiteSpace(g))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (existingRuntimeGroups.Length == 0)
            return;

        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var displayName = ReadModDisplayName(modPath);
            if (string.IsNullOrWhiteSpace(displayName))
                displayName = Path.GetFileName(modPath);

            foreach (var runtimeGroupName in existingRuntimeGroups)
            {
                if (string.Equals(runtimeGroupName, displayName, StringComparison.OrdinalIgnoreCase)
                    || runtimeGroupName.StartsWith(displayName + " / ", StringComparison.OrdinalIgnoreCase))
                {
                    output.Add(runtimeGroupName);
                }
            }
        }
    }

    private async Task<SourceRuntimeGroupSelectionState> ResolveSourceRuntimeGroupSelectionStateAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, CancellationToken token)
    {
        var state = new SourceRuntimeGroupSelectionState();
        var modDirectory = _ipcManager.Penumbra.ModDirectory;
        if (collectionState == null || string.IsNullOrWhiteSpace(modDirectory) || !Directory.Exists(modDirectory))
            return state;

        var scannedMods = 0;
        foreach (var mod in collectionState.Mods)
        {
            token.ThrowIfCancellationRequested();

            if (++scannedMods % ModScanYieldStride == 0)
                await Task.Yield();

            if (!mod.Value.Enabled || mod.Value.Temporary)
                continue;

            if (IsRuntimeModKey(modDirectory, mod.Key))
                continue;

            var modPath = ResolveModDirectory(modDirectory, mod.Key);
            if (!Directory.Exists(modPath))
                continue;

            var displayName = ReadModDisplayName(modPath);
            AddSourceBackedRuntimeGroups(state.SourceBackedGroupNames, modPath, displayName, token);

            var manifestEntries = await LoadSelectedManifestPapCandidatesAsync(modDirectory, mod.Key, modPath, mod.Value.Settings, mod.Value.Priority, token).ConfigureAwait(false);

            foreach (var candidate in manifestEntries)
            {
                token.ThrowIfCancellationRequested();

                if (!candidate.GamePaths.Any(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath))
                    continue;

                var runtimeGroupName = BuildRuntimeGroupName(candidate.DisplayModName, candidate.SourceGroupName);
                var runtimeOptionName = BuildRuntimeOptionName(candidate.OptionDisplayName);
                if (!state.SelectedOptionsByGroup.TryGetValue(runtimeGroupName, out var selectedOptions))
                {
                    selectedOptions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    state.SelectedOptionsByGroup[runtimeGroupName] = selectedOptions;
                }

                selectedOptions.Add(runtimeOptionName);
            }
        }

        return state;
    }

    private void AddSourceBackedRuntimeGroups(HashSet<string> output, string modPath, string displayName, CancellationToken token)
    {
        bool loadedCurrentManifest = false;
        var defaultManifest = Path.Combine(modPath, "default_mod.json");
        if (File.Exists(defaultManifest))
        {
            loadedCurrentManifest = true;
            try
            {
                using var stream = OpenReadShared(defaultManifest);
                using var doc = JsonDocument.Parse(stream);
                if (ContainsHumanPlayerAnimationManifestFiles(doc.RootElement, "Files"))
                    output.Add(BuildRuntimeGroupName(displayName, "Default"));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Ignoring malformed default manifest while resolving source-backed runtime groups {file}", defaultManifest);
            }
        }

        foreach (var groupManifest in Directory.EnumerateFiles(modPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            token.ThrowIfCancellationRequested();
            loadedCurrentManifest = true;

            try
            {
                using var stream = OpenReadShared(groupManifest);
                using var doc = JsonDocument.Parse(stream);
                var root = doc.RootElement;

                var groupName = root.TryGetProperty("Name", out var groupNameElement)
                    ? groupNameElement.GetString() ?? string.Empty
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(groupName))
                    continue;

                if (!root.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var option in options.EnumerateArray())
                {
                    if (ContainsHumanPlayerAnimationManifestFiles(option, "Files"))
                    {
                        output.Add(BuildRuntimeGroupName(displayName, groupName));
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Ignoring malformed group manifest while resolving source-backed runtime groups {file}", groupManifest);
            }
        }

        if (loadedCurrentManifest)
            return;

        var legacyMeta = Path.Combine(modPath, "meta.json");
        if (!File.Exists(legacyMeta))
            return;

        try
        {
            using var stream = OpenReadShared(legacyMeta);
            using var doc = JsonDocument.Parse(stream);
            var root = doc.RootElement;

            if (ContainsHumanPlayerAnimationManifestFiles(root, "Files") || ContainsHumanPlayerAnimationManifestFiles(root, "OptionFiles"))
                output.Add(BuildRuntimeGroupName(displayName, "Default"));

            if (!root.TryGetProperty("Groups", out var groups) || groups.ValueKind != JsonValueKind.Object)
                return;

            foreach (var group in groups.EnumerateObject())
            {
                token.ThrowIfCancellationRequested();

                var groupRoot = group.Value;
                var groupName = groupRoot.TryGetProperty("GroupName", out var groupNameElement)
                    ? groupNameElement.GetString() ?? group.Name
                    : group.Name;

                if (!groupRoot.TryGetProperty("Options", out var options) || options.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var option in options.EnumerateArray())
                {
                    if (ContainsHumanPlayerAnimationManifestFiles(option, "OptionFiles")
                        || ContainsHumanPlayerAnimationManifestFiles(option, "Files"))
                    {
                        output.Add(BuildRuntimeGroupName(displayName, groupName));
                        break;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Ignoring malformed legacy meta while resolving source-backed runtime groups {file}", legacyMeta);
        }
    }

    private static bool ContainsHumanPlayerAnimationManifestFiles(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var files) || files.ValueKind != JsonValueKind.Object)
            return false;

        foreach (var property in files.EnumerateObject())
        {
            if (XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath(property.Name))
                return true;
        }

        return false;
    }

    private static string BuildRuntimeOptionName(string optionDisplayName)
    {
        var optionPart = string.IsNullOrWhiteSpace(optionDisplayName) ? "Default" : optionDisplayName.Trim();
        return string.IsNullOrWhiteSpace(optionPart) ? "Default" : optionPart;
    }

    private static string SanitizeFileSegment(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "default";

        var invalid = Path.GetInvalidFileNameChars().ToHashSet();
        var chars = value.Select(c => invalid.Contains(c) ? '_' : c).ToArray();
        var sanitized = new string(chars).Trim();
        return string.IsNullOrWhiteSpace(sanitized) ? "default" : sanitized;
    }

    private bool CopySanitizedPapIntoRuntimeMod(string runtimeModPath, string groupName, string optionName, string[] gamePaths, string effectivePath, out string relativePath)
    {
        relativePath = string.Empty;

        if (string.IsNullOrWhiteSpace(effectivePath) || !File.Exists(effectivePath))
            return false;

        var fileName = string.IsNullOrWhiteSpace(Path.GetFileName(effectivePath)) ? "sanitized.pap" : Path.GetFileName(effectivePath);
        var primaryGamePath = gamePaths.OrderBy(g => g, StringComparer.OrdinalIgnoreCase).FirstOrDefault() ?? Path.GetFileNameWithoutExtension(fileName);
        var gamePathSegment = SanitizeFileSegment(primaryGamePath.Replace('/', '_').Replace('\\', '_'));
        relativePath = Path.Combine("files", SanitizeFileSegment(groupName), SanitizeFileSegment(optionName), gamePathSegment, fileName).Replace('\\', '/');
        var targetPath = Path.Combine(runtimeModPath, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);
        return CopyFileIfChanged(effectivePath, targetPath);
    }

    private bool WriteRuntimeGroupManifest(string groupFilePath, DesiredRuntimeGroup group)
    {
        var orderedOptions = group.Options.Values
            .OrderBy(o => o.Name, StringComparer.OrdinalIgnoreCase)
            .Select(o => new RuntimeGroupOption
            {
                Name = o.Name,
                Description = string.Empty,
                Priority = RuntimePriorityFloor,
                Files = new Dictionary<string, string>(o.Files, StringComparer.OrdinalIgnoreCase),
            })
            .ToList();

        var manifest = new RuntimeGroupManifest
        {
            Name = group.Name,
            Type = "Multi",
            Priority = RuntimePriorityFloor,
            DefaultSettings = 0,
            Description = string.Empty,
            Options = orderedOptions,
        };

        return WriteTextIfChanged(groupFilePath, JsonSerializer.Serialize(manifest, _jsonOptions));
    }

    private static bool DeleteStaleRuntimeGroupFiles(string runtimeModPath, HashSet<string> desiredGroupFiles)
    {
        bool changed = false;
        foreach (var groupFile in Directory.EnumerateFiles(runtimeModPath, "group_*.json", SearchOption.TopDirectoryOnly))
        {
            var fileName = Path.GetFileName(groupFile);
            if (desiredGroupFiles.Contains(fileName))
                continue;

            File.Delete(groupFile);
            changed = true;
        }

        return changed;
    }

    private bool WriteRuntimeMetaFiles(string runtimeModPath)
    {
        var metaPath = Path.Combine(runtimeModPath, "meta.json");
        var defaultModPath = Path.Combine(runtimeModPath, "default_mod.json");

        var meta = new
        {
            FileVersion = 3,
            Name = RuntimeModDisplayName,
            Author = "RavaSync",
            Description = "Auto-generated converted animation overrides for the local player.",
            Version = "1.0",
            Website = string.Empty,
        };

        var defaultMod = new
        {
            Files = new Dictionary<string, string>(),
            FileSwaps = new Dictionary<string, string>(),
        };

        bool changed = false;
        changed |= WriteTextIfChanged(metaPath, JsonSerializer.Serialize(meta, _jsonOptions));
        changed |= WriteTextIfChanged(defaultModPath, JsonSerializer.Serialize(defaultMod, _jsonOptions));
        return changed;
    }

    private async Task<string> EnsureRuntimeModRegisteredAsync(IpcCallerPenumbra.PenumbraCollectionModSettings collectionState, string runtimeModPath, CancellationToken token)
    {
        var currentKey = FindRuntimeModKey(collectionState);
        if (!string.IsNullOrWhiteSpace(currentKey))
            return currentKey;

        var addResult = await _ipcManager.Penumbra.AddModAsync(_logger, runtimeModPath).ConfigureAwait(false);
        var addAccepted = addResult == PenumbraApiEc.Success || addResult == PenumbraApiEc.NothingChanged;

        if (!addAccepted)
        {
            _logger.LogWarning("Failed to add runtime converted animations mod from {path}: {result}", runtimeModPath, addResult);
            return string.Empty;
        }

        for (var attempt = 0; attempt < 10; attempt++)
        {
            token.ThrowIfCancellationRequested();

            var refreshed = await TryGetLocalPlayerCollectionSettingsAsync(token).ConfigureAwait(false);
            currentKey = FindRuntimeModKey(refreshed);
            if (!string.IsNullOrWhiteSpace(currentKey))
                return currentKey;

            await Task.Delay(100, token).ConfigureAwait(false);
        }

        _logger.LogDebug("Runtime converted animations mod was not visible in collection settings after AddMod; using runtime path as mod key: {path}", runtimeModPath);
        return runtimeModPath;
    }

    private bool IsRuntimeModKey(string modDirectoryRoot, string modKey)
    {
        var expectedRelative = NormalizeRelativeDirectory(RuntimeModRelativeDirectory);
        var normalizedRelativeKey = NormalizeRelativeDirectory(modKey);
        if (string.Equals(normalizedRelativeKey, expectedRelative, StringComparison.OrdinalIgnoreCase)
            || normalizedRelativeKey.EndsWith(expectedRelative, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(modDirectoryRoot))
        {
            var expectedFull = NormalizeFullPath(Path.Combine(modDirectoryRoot, RuntimeModRelativeDirectory));
            var actualFull = NormalizeFullPath(Path.IsPathRooted(modKey) ? modKey : Path.Combine(modDirectoryRoot, modKey));
            if (string.Equals(actualFull, expectedFull, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private string FindRuntimeModKey(IpcCallerPenumbra.PenumbraCollectionModSettings? collectionState)
    {
        if (collectionState == null)
            return string.Empty;

        var modDirectoryRoot = _ipcManager.Penumbra.ModDirectory ?? string.Empty;
        foreach (var mod in collectionState.Mods.Keys)
        {
            if (IsRuntimeModKey(modDirectoryRoot, mod))
                return mod;
        }

        return string.Empty;
    }

    private static string BuildRuntimeFingerprint(IReadOnlyDictionary<string, ManifestPapSource> selectedSourcesByGamePath, IReadOnlyDictionary<string, IReadOnlyList<string>> desiredSelectionsByGroup, IReadOnlyCollection<SanitizedPapOverride> desiredOverrides)
    {
        var builder = new StringBuilder();

        foreach (var item in selectedSourcesByGamePath.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
        {
            builder.Append("SRC|")
                .Append(item.Key).Append('|')
                .Append(item.Value.RelativeModDirectory).Append('|')
                .Append(item.Value.DisplayModName).Append('|')
                .Append(item.Value.OptionDisplayName).Append('|')
                .Append(item.Value.ResolvedPath).Append('|')
                .Append(item.Value.Hash).Append('|')
                .Append(item.Value.Priority)
                .AppendLine();
        }

        foreach (var item in desiredSelectionsByGroup.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
        {
            builder.Append("SEL|")
                .Append(item.Key).Append('|')
                .Append(string.Join(";", item.Value.OrderBy(v => v, StringComparer.OrdinalIgnoreCase)))
                .AppendLine();
        }

        foreach (var item in desiredOverrides.OrderBy(d => d.OriginalResolvedPath, StringComparer.OrdinalIgnoreCase))
        {
            builder.Append("OVR|")
                .Append(item.OriginalResolvedPath).Append('|')
                .Append(item.EffectivePath).Append('|')
                .Append(item.EffectiveHash).Append('|')
                .Append(string.Join(";", item.GamePaths.OrderBy(g => g, StringComparer.OrdinalIgnoreCase)))
                .AppendLine();
        }

        return Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(builder.ToString())));
    }

    private static FileStream OpenReadShared(string path)
        => new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

    private static bool CopyFileIfChanged(string sourcePath, string targetPath)
    {
        var sourceInfo = new FileInfo(sourcePath);
        var targetInfo = new FileInfo(targetPath);
        if (targetInfo.Exists && targetInfo.Length == sourceInfo.Length)
        {
            var sourceBytes = ReadAllBytesShared(sourcePath);
            var targetBytes = ReadAllBytesShared(targetPath);
            if (sourceBytes.AsSpan().SequenceEqual(targetBytes))
                return false;
        }

        var tempPath = targetPath + ".tmp";
        File.Copy(sourcePath, tempPath, overwrite: true);
        ReplaceFileWithRetry(tempPath, targetPath);
        return true;
    }

    private static bool WriteTextIfChanged(string path, string content)
    {
        if (File.Exists(path))
        {
            var existing = ReadAllTextShared(path);
            if (string.Equals(existing, content, StringComparison.Ordinal))
                return false;
        }

        var tempPath = path + ".tmp";
        File.WriteAllText(tempPath, content, Encoding.UTF8);
        ReplaceFileWithRetry(tempPath, path);
        return true;
    }

    private static byte[] ReadAllBytesShared(string path)
    {
        using var stream = OpenReadShared(path);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);
        return memory.ToArray();
    }

    private static string ReadAllTextShared(string path)
    {
        using var stream = OpenReadShared(path);
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        return reader.ReadToEnd();
    }

    private static void ReplaceFileWithRetry(string tempPath, string destinationPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);
        Exception? last = null;
        for (var attempt = 0; attempt < 10; attempt++)
        {
            try
            {
                if (File.Exists(destinationPath))
                    File.Delete(destinationPath);

                File.Move(tempPath, destinationPath, overwrite: true);
                return;
            }
            catch (IOException ex)
            {
                last = ex;
                Thread.Sleep(40 * (attempt + 1));
            }
            catch (UnauthorizedAccessException ex)
            {
                last = ex;
                Thread.Sleep(40 * (attempt + 1));
            }
        }

        if (File.Exists(tempPath))
            File.Delete(tempPath);

        throw last ?? new IOException($"Failed to replace file {destinationPath}");
    }

    private sealed class RuntimeGroupManifest
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = "Multi";
        public int Priority { get; set; }
        public int DefaultSettings { get; set; }
        public string Description { get; set; } = string.Empty;
        public List<RuntimeGroupOption> Options { get; set; } = [];
    }

    private sealed class RuntimeGroupOption
    {
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public int Priority { get; set; }
        public Dictionary<string, string> Files { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }
}
