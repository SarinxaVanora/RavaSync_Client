using System.Text;
using System.Text.RegularExpressions;
using FFXIVClientStructs.FFXIV.Client.Game.Character;
using Microsoft.Extensions.Logging;
using RavaSync.Interop.GameModel;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.Optimisation;
using System.Collections.Concurrent;
using System.Linq;
using CharacterData = RavaSync.PlayerData.Data.CharacterData;

namespace RavaSync.PlayerData.Factories;

public class PlayerDataFactory : IMediatorSubscriber
{
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileCacheManager _fileCacheManager;
    private readonly IpcManager _ipcManager;
    private readonly ILogger<PlayerDataFactory> _logger;
    private readonly PerformanceCollectorService _performanceCollector;
    private readonly PapSanitisationService _papSanitisationService;
    private readonly LocalPapSafetyModService _localPapSafetyModService;
    private readonly MareMediator _mareMediator;
    private readonly TransientResourceManager _transientResourceManager;

    private sealed record PapSessionDecision(string OverrideKey, string OriginalHash, string SkeletonFingerprint, PapRewriteStatus Status, string EffectiveHash, string EffectivePath, DateTimeOffset UpdatedUtc, string Reason);
    private sealed record StaticSupportDependencyCacheEntry(string Extension, long Length, DateTime LastWriteUtc, string[] DirectGamePaths, string[] RelativeMaterialFileNames)
    {
        public bool Matches(string extension, long length, DateTime lastWriteUtc)
            => string.Equals(Extension, extension, StringComparison.OrdinalIgnoreCase)
                && Length == length
                && LastWriteUtc == lastWriteUtc;
    }

    private readonly ConcurrentDictionary<string, PapSessionDecision> _playerPapDecisionsByKey = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StaticSupportDependencyCacheEntry> _staticSupportDependencyCacheByResolvedPath = new(StringComparer.OrdinalIgnoreCase);
    private const int ReverseResolveBatchSize = 192;
    private const int CharacterBuildYieldEvery = 128;
    private const int StaticSupportDependencyCacheSoftLimit = 2048;
    private static readonly TimeSpan SupportSweepBreatherDelay = TimeSpan.FromMilliseconds(1);


    public PlayerDataFactory(ILogger<PlayerDataFactory> logger, DalamudUtilService dalamudUtil, IpcManager ipcManager,
        TransientResourceManager transientResourceManager, FileCacheManager fileReplacementFactory,
        PerformanceCollectorService performanceCollector, XivDataAnalyzer modelAnalyzer, PapSanitisationService papSanitizationService,
        LocalPapSafetyModService localPapSafetyModService, MareMediator mareMediator)
    {
        _logger = logger;
        _dalamudUtil = dalamudUtil;
        _ipcManager = ipcManager;
        _transientResourceManager = transientResourceManager;
        _fileCacheManager = fileReplacementFactory;
        _performanceCollector = performanceCollector;
        _papSanitisationService = papSanitizationService;
        _localPapSafetyModService = localPapSafetyModService;
        _mareMediator = mareMediator;

        _mareMediator.Subscribe<PenumbraFileCacheChangedMessage>(this, _ => InvalidatePapSessionCaches());
        _mareMediator.Subscribe<PenumbraModSettingChangedMessage>(this, _ => InvalidatePapSessionCaches());
        _mareMediator.Subscribe<DisconnectedMessage>(this, _ => InvalidatePapSessionCaches());
        _mareMediator.Subscribe<DalamudLogoutMessage>(this, _ => InvalidatePapSessionCaches());

        _logger.LogTrace("Creating {this}", nameof(PlayerDataFactory));
    }

    public MareMediator Mediator => _mareMediator;

    private void InvalidatePapSessionCaches()
    {
        _playerPapDecisionsByKey.Clear();
    }


    public Task<CharacterDataFragment?> BuildCharacterData(GameObjectHandler playerRelatedObject, CancellationToken token)
    {
        return BuildCharacterData(playerRelatedObject, token, null);
    }

    public async Task<CharacterDataFragment?> BuildCharacterData(GameObjectHandler playerRelatedObject, CancellationToken token, string? reason)
    {
        if (!_ipcManager.Initialized)
        {
            throw new InvalidOperationException("Penumbra or Glamourer is not connected");
        }

        if (playerRelatedObject == null) return null;

        if (playerRelatedObject.Address == IntPtr.Zero)
        {
            _logger.LogTrace("Pointer was zero for {objectKind}", playerRelatedObject.ObjectKind);
            return null;
        }

        try
        {
            if (_performanceCollector.Enabled)
            {
                return await _performanceCollector.LogPerformance(this, $"CreateCharacterData>{playerRelatedObject.ObjectKind}", async () =>
                {
                    return await CreateCharacterData(playerRelatedObject, token, reason).ConfigureAwait(false);
                }).ConfigureAwait(true);
            }
            return await CreateCharacterData(playerRelatedObject, token, reason).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Cancelled creating Character data for {object}", playerRelatedObject);
            throw;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Failed to create {object} data", playerRelatedObject);
        }

        return null;
    }

    private async Task<CharacterDataFragment> CreateCharacterData(GameObjectHandler playerRelatedObject, CancellationToken ct, string? reason = null)
    {
        var objectKind = playerRelatedObject.ObjectKind;
        CharacterDataFragment fragment = objectKind == ObjectKind.Player ? new CharacterDataFragmentPlayer() : new();
        _logger.LogDebug("Building character data for {obj}, reason={reason}",
            playerRelatedObject,
            string.IsNullOrWhiteSpace(reason) ? "<none>" : reason);

        var lightweightBuild = IsLightweightBuildReasonSet(reason);
        var drawingWaitMs = lightweightBuild ? 1500 : 5000;
        await _dalamudUtil.WaitWhileCharacterIsDrawing(_logger, playerRelatedObject, Guid.NewGuid(), drawingWaitMs, ct: ct).ConfigureAwait(false);
        int totalWaitTime = lightweightBuild ? 1500 : 5000;

        var gameObj = await _dalamudUtil.CreateGameObjectAsync(playerRelatedObject.Address).ConfigureAwait(false);

        while (totalWaitTime > 0)
        {
            ct.ThrowIfCancellationRequested();

            bool present = false;

            try
            {

                gameObj ??= await _dalamudUtil.CreateGameObjectAsync(playerRelatedObject.Address).ConfigureAwait(false);

                if (gameObj != null)
                    present = await _dalamudUtil.IsObjectPresentAsync(gameObj).ConfigureAwait(false);
            }
            catch
            {
                gameObj = null;
                present = false;
            }

            if (present)
                break;

            if ((totalWaitTime % 500) == 0)
                _logger.LogTrace("Character is null but it shouldn't be, waiting");
            await Task.Delay(50, ct).ConfigureAwait(false);
            totalWaitTime -= 50;
        }


        ct.ThrowIfCancellationRequested();

        // Start IPC tasks early so they overlap with resolving + hashing
        Task<string> getHeelsOffset = _ipcManager.Heels.GetOffsetAsync();
        Task<string> getGlamourerData = _ipcManager.Glamourer.GetCharacterCustomizationAsync(playerRelatedObject.Address);
        Task<string?> getCustomizeData = _ipcManager.CustomizePlus.GetScaleAsync(playerRelatedObject.Address);
        Task<string> getHonorificTitle = _ipcManager.Honorific.GetTitle();

        Task<string?> getMoodles = objectKind == ObjectKind.Player ? _ipcManager.Moodles.GetStatusAsync(playerRelatedObject.Address) : Task.FromResult<string?>(null);

        DateTime start = DateTime.UtcNow;

        var resolvedPaths = await ResolvePenumbraCharacterDataWithRetriesAsync(playerRelatedObject, objectKind, ct, reason).ConfigureAwait(false);
        if (resolvedPaths == null) throw new InvalidOperationException("Penumbra returned null data after retry");

        ct.ThrowIfCancellationRequested();

        fragment.FileReplacements =
                new HashSet<FileReplacement>(resolvedPaths.Select(c => new FileReplacement([.. c.Value], c.Key)), FileReplacementComparer.Instance)
                .Where(p => p.HasFileReplacement).ToHashSet(FileReplacementComparer.Instance);
        fragment.FileReplacements.RemoveWhere(c => c.GamePaths.Any(g => !CacheMonitor.AllowedFileExtensions.Any(e => g.EndsWith(e, StringComparison.OrdinalIgnoreCase))));

        ct.ThrowIfCancellationRequested();

        ct.ThrowIfCancellationRequested();

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("== Static Replacements ==");
            foreach (var replacement in fragment.FileReplacements
                         .Where(i => i.HasFileReplacement)
                         .OrderBy(i => i.GamePaths.First(), StringComparer.OrdinalIgnoreCase))
            {
                _logger.LogDebug("=> {repl}", replacement);
                ct.ThrowIfCancellationRequested();
            }
        }


        await AddStaticSupportDependencyReplacementsAsync(fragment, ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        await _transientResourceManager.WaitForRecording(ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        // if it's pet then it's summoner, if it's summoner we actually want to keep all filereplacements alive at all times
        // or we get into redraw city for every change and nothing works properly
        if (objectKind == ObjectKind.Pet)
        {
            foreach (var item in fragment.FileReplacements.Where(i => i.HasFileReplacement).SelectMany(p => p.GamePaths))
            {
                if (_transientResourceManager.AddTransientResource(objectKind, item))
                {
                    _logger.LogDebug("Marking static {item} for Pet as transient", item);
                }
            }

            _logger.LogTrace("Clearing {count} Static Replacements for Pet", fragment.FileReplacements.Count);
            fragment.FileReplacements.Clear();
        }

        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("Handling transient update for {obj}", playerRelatedObject);

        // remove all potentially gathered paths from the transient resource manager that are resolved through static resolving
        var staticResolvedGamePaths = fragment.FileReplacements.SelectMany(c => c.GamePaths).ToList();
        _transientResourceManager.ClearTransientPaths(objectKind, staticResolvedGamePaths);

        // get all remaining paths and resolve them
        var transientPaths = ManageSemiTransientData(objectKind, staticResolvedGamePaths);
        var resolvedTransientPaths = await ResolveTransientReplacementsForBuildAsync(objectKind, transientPaths, reason, ct).ConfigureAwait(false);
        var transientReplacements = resolvedTransientPaths.Select(c => new FileReplacement([.. c.Value], c.Key)).ToList();
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("== Transient Replacements ==");
            foreach (var replacement in transientReplacements.OrderBy(f => f.ResolvedPath, StringComparer.Ordinal))
            {
                _logger.LogDebug("=> {repl}", replacement);
                fragment.FileReplacements.Add(replacement);
            }
        }
        else
        {
            foreach (var replacement in transientReplacements)
                fragment.FileReplacements.Add(replacement);
        }


        // clean up all semi transient resources that don't have any file replacement (aka null resolve)
        _transientResourceManager.CleanUpSemiTransientResources(objectKind, [.. fragment.FileReplacements]);

        ct.ThrowIfCancellationRequested();

        // make sure we only return data that actually has file replacements
        fragment.FileReplacements = new HashSet<FileReplacement>(fragment.FileReplacements.Where(v => v.HasFileReplacement).OrderBy(v => v.ResolvedPath, StringComparer.Ordinal), FileReplacementComparer.Instance);

        // gather up data from ipc
        fragment.GlamourerString = await getGlamourerData.ConfigureAwait(false);
        _logger.LogDebug("Glamourer is now: {data}", fragment.GlamourerString);

        var customizeScale = await getCustomizeData.ConfigureAwait(false);
        fragment.CustomizePlusScale = customizeScale ?? string.Empty;
        _logger.LogDebug("Customize is now: {data}", fragment.CustomizePlusScale);

        if (objectKind == ObjectKind.Player)
        {
            var playerFragment = (fragment as CharacterDataFragmentPlayer)!;
            playerFragment.ManipulationString = _ipcManager.Penumbra.GetMetaManipulations();

            playerFragment!.HonorificData = await getHonorificTitle.ConfigureAwait(false);
            _logger.LogDebug("Honorific is now: {data}", playerFragment!.HonorificData);

            playerFragment!.HeelsData = await getHeelsOffset.ConfigureAwait(false);
            _logger.LogDebug("Heels is now: {heels}", playerFragment!.HeelsData);

            playerFragment!.MoodlesData = await getMoodles.ConfigureAwait(false) ?? string.Empty;
            _logger.LogDebug("Moodles is now: {moodles}", playerFragment!.MoodlesData);

            playerFragment!.PetNamesData = _ipcManager.PetNames.GetLocalNames();
            _logger.LogDebug("Pet Nicknames is now: {petnames}", playerFragment!.PetNamesData);
        }


        ct.ThrowIfCancellationRequested();

        var toCompute = fragment.FileReplacements.Where(f => !f.IsFileSwap).ToArray();
        _logger.LogDebug("Getting Hashes for {amount} Files", toCompute.Length);
        var computedPaths = _fileCacheManager.GetFileCachesByPaths(toCompute.Select(c => c.ResolvedPath).ToArray());
        for (var i = 0; i < toCompute.Length; i++)
        {
            ct.ThrowIfCancellationRequested();
            var file = toCompute[i];
            file.Hash = computedPaths[file.ResolvedPath]?.Hash ?? string.Empty;

            if ((i + 1) % CharacterBuildYieldEvery == 0)
                await Task.Yield();
        }
        var removed = fragment.FileReplacements.RemoveWhere(f => !f.IsFileSwap && string.IsNullOrEmpty(f.Hash));
        if (removed > 0)
        {
            _logger.LogDebug("Removed {amount} of invalid files", removed);
        }

        ct.ThrowIfCancellationRequested();

        _logger.LogInformation("Building character data for {obj} took {time}ms", objectKind, TimeSpan.FromTicks(DateTime.UtcNow.Ticks - start.Ticks).TotalMilliseconds);

        return fragment;
    }

    private async Task<Dictionary<string, HashSet<string>>?> ResolvePenumbraCharacterDataWithRetriesAsync(GameObjectHandler playerRelatedObject, ObjectKind objectKind, CancellationToken ct, string? reason)
    {
        var maxAttempts = objectKind == ObjectKind.Player ? 5 : 2;
        var retryDelayMs = objectKind == ObjectKind.Player ? 80 : 50;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            if (objectKind == ObjectKind.Player && playerRelatedObject.Address == nint.Zero)
                await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);

            var resolvedPaths = await _ipcManager.Penumbra.GetCharacterData(_logger, playerRelatedObject).ConfigureAwait(false);
            if (resolvedPaths != null)
                return resolvedPaths;

            if (attempt == maxAttempts)
                break;

            _logger.LogTrace(
                "Penumbra returned null data for {obj} on attempt {attempt}/{maxAttempts}, reason={reason}; retrying after a framework tick",
                playerRelatedObject,
                attempt,
                maxAttempts,
                string.IsNullOrWhiteSpace(reason) ? "<none>" : reason);

            await Task.Delay(retryDelayMs, ct).ConfigureAwait(false);
            await _dalamudUtil.RunOnFrameworkThread(() => { }).ConfigureAwait(false);
        }

        return null;
    }

    private async Task AddStaticSupportDependencyReplacementsAsync(CharacterDataFragment fragment, CancellationToken ct)
    {
        if (fragment.FileReplacements.Count == 0)
            return;

        const int maxDependencyPasses = 4;
        var scannedLocalFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var knownGamePaths = fragment.FileReplacements
            .SelectMany(static replacement => replacement.GamePaths)
            .Select(NormalizeStaticSupportGamePath)
            .Where(static path => !string.IsNullOrWhiteSpace(path))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var pendingGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var addedCount = 0;

        for (var pass = 0; pass < maxDependencyPasses; pass++)
        {
            ct.ThrowIfCancellationRequested();

            foreach (var replacement in fragment.FileReplacements.ToArray())
            {
                ct.ThrowIfCancellationRequested();

                var resolvedPath = NormalizeStaticSupportResolvedPath(replacement.ResolvedPath);
                if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                    continue;

                if (!scannedLocalFiles.Add(resolvedPath))
                    continue;

                foreach (var dependencyPath in GetCachedStaticSupportDependencyGamePaths(resolvedPath, replacement.GamePaths))
                {
                    var normalizedDependencyPath = NormalizeStaticSupportGamePath(dependencyPath);
                    if (!IsStaticSupportDependencyPath(normalizedDependencyPath))
                        continue;

                    if (knownGamePaths.Contains(normalizedDependencyPath))
                        continue;

                    pendingGamePaths.Add(normalizedDependencyPath);
                }
            }

            if (pendingGamePaths.Count == 0)
                break;

            var pathsToResolve = pendingGamePaths.ToHashSet(StringComparer.OrdinalIgnoreCase);
            pendingGamePaths.Clear();

            var resolvedDependencies = await GetFileReplacementsFromPaths(pathsToResolve, new HashSet<string>(StringComparer.OrdinalIgnoreCase), ct).ConfigureAwait(false);
            var passAdded = 0;

            foreach (var resolvedDependency in resolvedDependencies.OrderBy(static kvp => kvp.Key, StringComparer.OrdinalIgnoreCase))
            {
                ct.ThrowIfCancellationRequested();

                var resolvedFilePath = NormalizeStaticSupportResolvedPath(resolvedDependency.Key);
                if (string.IsNullOrWhiteSpace(resolvedFilePath) || !File.Exists(resolvedFilePath))
                    continue;

                var dependencyGamePaths = resolvedDependency.Value
                    .Select(NormalizeStaticSupportGamePath)
                    .Where(path => pathsToResolve.Contains(path) && IsStaticSupportDependencyPath(path))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                if (dependencyGamePaths.Length == 0)
                    continue;

                var dependencyReplacement = new FileReplacement(dependencyGamePaths, resolvedFilePath);
                if (!dependencyReplacement.HasFileReplacement)
                    continue;

                foreach (var gamePath in dependencyReplacement.GamePaths)
                    knownGamePaths.Add(gamePath);

                if (fragment.FileReplacements.Add(dependencyReplacement))
                {
                    passAdded++;
                    addedCount++;
                }
            }

            if (passAdded == 0)
                break;

            await Task.Yield();
        }

        if (addedCount > 0)
        {
            _logger.LogDebug("Added {count} static support dependency replacement(s) from active model/material files", addedCount);
        }
    }

    private IEnumerable<string> GetCachedStaticSupportDependencyGamePaths(string resolvedPath, IEnumerable<string> gamePaths)
    {
        var normalizedResolvedPath = NormalizeStaticSupportResolvedPath(resolvedPath);
        if (!TryGetStaticSupportFileIdentity(normalizedResolvedPath, out var extension, out var length, out var lastWriteUtc))
            yield break;

        if (!_staticSupportDependencyCacheByResolvedPath.TryGetValue(normalizedResolvedPath, out var cacheEntry)
            || !cacheEntry.Matches(extension, length, lastWriteUtc))
        {
            cacheEntry = ExtractStaticSupportDependencyCacheEntry(normalizedResolvedPath, extension, length, lastWriteUtc);
            _staticSupportDependencyCacheByResolvedPath[normalizedResolvedPath] = cacheEntry;
            TrimStaticSupportDependencyCacheIfNeeded();
        }

        foreach (var dependencyPath in cacheEntry.DirectGamePaths)
            yield return dependencyPath;

        if (cacheEntry.RelativeMaterialFileNames.Length == 0)
            yield break;

        foreach (var materialFileName in cacheEntry.RelativeMaterialFileNames)
        {
            foreach (var modelGamePath in gamePaths)
            {
                var derived = TryDeriveMaterialPathFromModelPath(modelGamePath, materialFileName);
                if (!string.IsNullOrWhiteSpace(derived))
                    yield return derived;
            }
        }
    }

    private static bool TryGetStaticSupportFileIdentity(string resolvedPath, out string extension, out long length, out DateTime lastWriteUtc)
    {
        extension = string.Empty;
        length = 0;
        lastWriteUtc = default;

        if (string.IsNullOrWhiteSpace(resolvedPath))
            return false;

        try
        {
            var fileInfo = new FileInfo(resolvedPath);
            if (!fileInfo.Exists)
                return false;

            extension = fileInfo.Extension;
            if (!string.Equals(extension, ".mdl", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(extension, ".mtrl", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            length = fileInfo.Length;
            lastWriteUtc = fileInfo.LastWriteTimeUtc;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static StaticSupportDependencyCacheEntry ExtractStaticSupportDependencyCacheEntry(string resolvedPath, string extension, long length, DateTime lastWriteUtc)
    {
        if (string.Equals(extension, ".mdl", StringComparison.OrdinalIgnoreCase))
            return ExtractModelDependencyCacheEntry(resolvedPath, extension, length, lastWriteUtc);

        if (string.Equals(extension, ".mtrl", StringComparison.OrdinalIgnoreCase))
            return ExtractMaterialDependencyCacheEntry(resolvedPath, extension, length, lastWriteUtc);

        return new StaticSupportDependencyCacheEntry(extension, length, lastWriteUtc, [], []);
    }

    private static StaticSupportDependencyCacheEntry ExtractModelDependencyCacheEntry(string resolvedPath, string extension, long length, DateTime lastWriteUtc)
    {
        var directGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var relativeMaterialFileNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        MdlFile mdl;
        try
        {
            mdl = new MdlFile(resolvedPath);
        }
        catch
        {
            return new StaticSupportDependencyCacheEntry(extension, length, lastWriteUtc, [], []);
        }

        foreach (var materialPath in mdl.MaterialStrings ?? [])
        {
            var normalized = NormalizeStaticSupportGamePath(materialPath);
            if (IsStaticSupportDependencyPath(normalized))
            {
                directGamePaths.Add(normalized);
                continue;
            }

            if (!string.IsNullOrWhiteSpace(normalized) && normalized.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase))
                relativeMaterialFileNames.Add(normalized);
        }

        return new StaticSupportDependencyCacheEntry(
            extension,
            length,
            lastWriteUtc,
            directGamePaths.OrderBy(static path => path, StringComparer.OrdinalIgnoreCase).ToArray(),
            relativeMaterialFileNames.OrderBy(static path => path, StringComparer.OrdinalIgnoreCase).ToArray());
    }

    private static StaticSupportDependencyCacheEntry ExtractMaterialDependencyCacheEntry(string resolvedPath, string extension, long length, DateTime lastWriteUtc)
    {
        byte[] bytes;
        try
        {
            bytes = File.ReadAllBytes(resolvedPath);
        }
        catch
        {
            return new StaticSupportDependencyCacheEntry(extension, length, lastWriteUtc, [], []);
        }

        var texturePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in StaticMaterialTexturePathRegex.Matches(Encoding.UTF8.GetString(bytes)))
        {
            var normalized = NormalizeStaticSupportGamePath(match.Groups[1].Value);
            if (IsStaticSupportDependencyPath(normalized))
                texturePaths.Add(normalized);
        }

        return new StaticSupportDependencyCacheEntry(
            extension,
            length,
            lastWriteUtc,
            texturePaths.OrderBy(static path => path, StringComparer.OrdinalIgnoreCase).ToArray(),
            []);
    }

    private void TrimStaticSupportDependencyCacheIfNeeded()
    {
        if (_staticSupportDependencyCacheByResolvedPath.Count <= StaticSupportDependencyCacheSoftLimit * 2)
            return;

        _logger.LogDebug("Clearing static support dependency cache after it grew to {count} entries", _staticSupportDependencyCacheByResolvedPath.Count);
        _staticSupportDependencyCacheByResolvedPath.Clear();
    }

    private static string TryDeriveMaterialPathFromModelPath(string modelGamePath, string materialFileName)
    {
        var normalizedModelPath = NormalizeStaticSupportGamePath(modelGamePath);
        var normalizedMaterialFileName = NormalizeStaticSupportGamePath(materialFileName);

        if (string.IsNullOrWhiteSpace(normalizedModelPath)
            || string.IsNullOrWhiteSpace(normalizedMaterialFileName)
            || !normalizedModelPath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || !normalizedMaterialFileName.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        var modelMarker = "/model/";
        var markerIndex = normalizedModelPath.IndexOf(modelMarker, StringComparison.OrdinalIgnoreCase);
        if (markerIndex < 0)
            return string.Empty;

        var root = normalizedModelPath[..markerIndex];
        var fileName = Path.GetFileName(normalizedMaterialFileName);
        if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(fileName))
            return string.Empty;

        return root + "/material/v0001/" + fileName;
    }

    private static bool IsStaticSupportDependencyPath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeStaticSupportGamePath(string? value)
    {
        var normalized = CharacterDataPushSanitizer.NormalizeGamePathForPush(value);
        while (normalized.StartsWith("/", StringComparison.Ordinal))
            normalized = normalized[1..];

        return normalized;
    }

    private static string NormalizeStaticSupportResolvedPath(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : value.Replace('\\', '/').Trim().Trim('\0');

    private static readonly Regex StaticMaterialTexturePathRegex = new(@"(?:^|[\0\s\""'])/?((?:chara|common|bgcommon|bg|cut|vfx|shader|ui|font|game_script|sound|music|exd)/[a-z0-9_ '+&,\.\-\{\}/]+\.tex)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.ECMAScript);


    private static bool IsLightweightBuildReasonSet(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        var reasons = reason.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return reasons.Length > 0 && reasons.All(IsLightweightBuildReason);
    }

    private static bool IsLightweightBuildReason(string reason)
    {
        return string.Equals(reason, "GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || string.Equals(reason, "ImmediateFollowUp:GameObject:TransientResourceChanged", StringComparison.Ordinal)
            || string.Equals(reason, "Glamourer:PlayerFallback", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraRedraw", StringComparison.Ordinal)
            || string.Equals(reason, "GameObject:PenumbraEndRedraw", StringComparison.Ordinal)
            || string.Equals(reason, "PenumbraFileCacheChanged", StringComparison.Ordinal)
            || reason.StartsWith("ImmediateFollowUp:Connected:", StringComparison.Ordinal)
            || reason.StartsWith("Connected:", StringComparison.Ordinal)
            || reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
            || reason.StartsWith("GameObject:SemanticDiff", StringComparison.Ordinal)
            || reason.StartsWith("CustomizePlus:", StringComparison.Ordinal)
            || reason.StartsWith("ClassJobChanged:", StringComparison.Ordinal);
    }



    private static bool ShouldForceFullLiveTransientResolveForBuild(string? reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        var reasons = reason.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return reasons.Any(ShouldForceFullLiveTransientResolveForReason);
    }

    private static bool ShouldForceFullLiveTransientResolveForReason(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
            return false;

        // Raw Penumbra selection changes can swap the physical file behind an existing transient game path
        // before the targeted manifest refresh has finished. Keep only that immediate safety build live.
        // The follow-up PenumbraModSettingChanged:TransientManifest build can trust transient.json.
        if (reason.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
            && !reason.Contains(":TransientManifest", StringComparison.Ordinal))
        {
            return true;
        }

        // Directory changes can invalidate the physical resolved-file map. Startup/targeted manifest prime will
        // reseed transient.json afterwards, but the immediate build should be conservative.
        if (reason.StartsWith("PenumbraDirectoryChanged", StringComparison.Ordinal))
            return true;

        return false;
    }



    public async Task<bool> RefreshLocalPlayerConvertedAnimationPackAsync(GameObjectHandler playerRelatedObject, CancellationToken ct)
    {
        if (playerRelatedObject == null
            || playerRelatedObject.ObjectKind != ObjectKind.Player
            || playerRelatedObject.Address == IntPtr.Zero
            || !_ipcManager.Initialized)
        {
            return false;
        }

        var collectionState = await _localPapSafetyModService.TryGetLocalPlayerCollectionSettingsAsync(ct).ConfigureAwait(false);
        if (collectionState == null)
            return false;

        var selectedSourcesByGamePath = await _localPapSafetyModService.ResolveSelectedHumanPapSourcesAsync(collectionState, ct).ConfigureAwait(false);
        if (selectedSourcesByGamePath.Count == 0)
        {
            var runtimeChanged = await _localPapSafetyModService.SyncRuntimeModAsync(
                collectionState,
                selectedSourcesByGamePath,
                Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>(),
                ct).ConfigureAwait(false);

            if (runtimeChanged)
                await RequestLocalPlayerRedrawForRuntimePapChangeAsync(playerRelatedObject, ct).ConfigureAwait(false);

            return runtimeChanged;
        }

        var targetSkeletons = await _dalamudUtil.RunOnFrameworkThread(() => _papSanitisationService.GetTargetSkeletonSnapshots(playerRelatedObject)).ConfigureAwait(false);
        var canRewriteAgainstCurrentSkeleton = targetSkeletons != null && targetSkeletons.Count > 0;
        var currentSkeletonFingerprint = canRewriteAgainstCurrentSkeleton
            ? _papSanitisationService.GetTargetSkeletonFingerprint(targetSkeletons!)
            : string.Empty;

        List<LocalPapSafetyModService.SanitizedPapOverride> sanitizedOverrides = [];

        var candidates = selectedSourcesByGamePath.Values
            .Where(s => !string.IsNullOrWhiteSpace(s.ResolvedPath)
                && !string.IsNullOrWhiteSpace(s.Hash)
                && s.GamePaths.Any(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath))
            .GroupBy(s => s.ResolvedPath + "|" + s.Hash, StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.First().ResolvedPath, StringComparer.OrdinalIgnoreCase);

        foreach (var group in candidates)
        {
            ct.ThrowIfCancellationRequested();

            var source = group.First();
            var gamePaths = group
                .SelectMany(s => s.GamePaths)
                .Where(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(g => g, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            if (gamePaths.Length == 0)
                continue;

            var replacement = new FileReplacement(gamePaths, source.ResolvedPath)
            {
                Hash = source.Hash,
            };

            if (TryGetReusablePapDecision(replacement, currentSkeletonFingerprint, out var reusedDecision))
            {
                if (reusedDecision.Status == PapRewriteStatus.Sanitized
                    && TryCreateSanitizedPapOverride(replacement, reusedDecision.EffectivePath, reusedDecision.EffectiveHash, reusedDecision.Reason, out var reusedOverride))
                {
                    sanitizedOverrides.Add(reusedOverride);
                }

                continue;
            }

            if (!canRewriteAgainstCurrentSkeleton)
                continue;

            var rewrite = await _papSanitisationService.RewritePapForTargetAsync(replacement.Hash, targetSkeletons!, ct).ConfigureAwait(false);
            RegisterPlayerPapDecision(replacement.Hash, currentSkeletonFingerprint, rewrite);

            if (rewrite.Status == PapRewriteStatus.Sanitized
                && TryCreateSanitizedPapOverride(replacement, rewrite.EffectivePath, rewrite.EffectiveHash, rewrite.Reason, out var sanitizedOverride))
            {
                sanitizedOverrides.Add(sanitizedOverride);
            }

            await Task.Yield();
        }

        var sanitizedRuntimeChanged = await _localPapSafetyModService.SyncRuntimeModAsync(collectionState, selectedSourcesByGamePath, sanitizedOverrides, ct).ConfigureAwait(false);
        if (sanitizedRuntimeChanged)
            await RequestLocalPlayerRedrawForRuntimePapChangeAsync(playerRelatedObject, ct).ConfigureAwait(false);

        return sanitizedRuntimeChanged;
    }

    private async Task RequestLocalPlayerRedrawForRuntimePapChangeAsync(GameObjectHandler playerRelatedObject, CancellationToken ct)
    {
        if (playerRelatedObject.ObjectKind != ObjectKind.Player || playerRelatedObject.Address == IntPtr.Zero)
            return;

        await Task.Delay(50, ct).ConfigureAwait(false);

        if (playerRelatedObject.Address == IntPtr.Zero)
            return;

        _mareMediator.Publish(new ArmRequestedPlayerPublishAfterRedrawMessage(playerRelatedObject.Address));
        _mareMediator.Publish(new PenumbraRedrawAddressMessage(playerRelatedObject.Address));
    }

    private static bool TryCreateSanitizedPapOverride(
        FileReplacement replacement,
        string? effectivePath,
        string? effectiveHash,
        string? reason,
        out LocalPapSafetyModService.SanitizedPapOverride sanitizedOverride)
    {
        sanitizedOverride = default!;

        if (replacement == null
            || string.IsNullOrWhiteSpace(replacement.ResolvedPath)
            || string.IsNullOrWhiteSpace(replacement.Hash)
            || string.IsNullOrWhiteSpace(effectivePath)
            || string.IsNullOrWhiteSpace(effectiveHash)
            || !File.Exists(effectivePath))
        {
            return false;
        }

        var gamePaths = replacement.GamePaths
            .Where(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (gamePaths.Length == 0)
            return false;

        sanitizedOverride = new LocalPapSafetyModService.SanitizedPapOverride(
            replacement.ResolvedPath,
            replacement.Hash,
            effectivePath,
            effectiveHash,
            gamePaths,
            reason ?? "Using sanitized PAP variant in the converted animation pack");

        return true;
    }

    private bool TryGetReusablePapDecision(FileReplacement replacement, string skeletonFingerprint, out PapSessionDecision reusedDecision)
    {
        reusedDecision = default!;

        if (replacement == null
            || string.IsNullOrWhiteSpace(replacement.Hash)
            || string.IsNullOrWhiteSpace(skeletonFingerprint))
        {
            return false;
        }

        var overrideKey = CreatePapDecisionKey(replacement.Hash, skeletonFingerprint);
        if (!_playerPapDecisionsByKey.TryGetValue(overrideKey, out var knownDecision))
            return false;

        if (!IsReusablePapDecision(knownDecision))
        {
            _playerPapDecisionsByKey.TryRemove(overrideKey, out _);
            return false;
        }

        reusedDecision = knownDecision;
        return true;
    }

    private void RegisterPlayerPapDecision(string originalHash, string skeletonFingerprint, PapRewriteResult rewrite)
    {
        if (string.IsNullOrWhiteSpace(originalHash)
            || string.IsNullOrWhiteSpace(skeletonFingerprint))
        {
            return;
        }

        var overrideKey = CreatePapDecisionKey(originalHash, skeletonFingerprint);
        _playerPapDecisionsByKey[overrideKey] = new PapSessionDecision(
            overrideKey,
            originalHash,
            skeletonFingerprint,
            rewrite.Status,
            rewrite.EffectiveHash,
            rewrite.EffectivePath,
            DateTimeOffset.UtcNow,
            rewrite.Reason ?? "Previously evaluated PAP safety decision");
    }

    private static string CreatePapDecisionKey(string originalHash, string skeletonFingerprint)
        => originalHash + "|" + skeletonFingerprint;

    private static bool IsReusablePapDecision(PapSessionDecision decision)
    {
        if (decision == null || string.IsNullOrWhiteSpace(decision.SkeletonFingerprint))
            return false;

        return decision.Status switch
        {
            PapRewriteStatus.Blocked => true,
            PapRewriteStatus.OriginalFallback => true,
            PapRewriteStatus.OriginalSafe => !string.IsNullOrWhiteSpace(decision.EffectiveHash)
                && !string.IsNullOrWhiteSpace(decision.EffectivePath)
                && File.Exists(decision.EffectivePath),
            PapRewriteStatus.Sanitized => !string.IsNullOrWhiteSpace(decision.EffectiveHash)
                && !string.IsNullOrWhiteSpace(decision.EffectivePath)
                && File.Exists(decision.EffectivePath),
            _ => false,
        };
    }


    private async Task<IReadOnlyDictionary<string, string[]>> GetFileReplacementsFromPaths(HashSet<string> forwardResolve, HashSet<string> reverseResolve, CancellationToken ct = default)
    {
        var forwardPaths = forwardResolve.ToArray();
        var reversePaths = reverseResolve.ToArray();

        Dictionary<string, List<string>> resolvedPaths = new(StringComparer.OrdinalIgnoreCase);

        static string NormalizeResolvedFilePathForBuild(string value)
            => string.IsNullOrWhiteSpace(value)
                ? string.Empty
                : value.Replace('\\', '/').Trim();

        static string NormalizeGamePathForBuild(string value)
            => string.IsNullOrWhiteSpace(value)
                ? string.Empty
                : value.Replace('\\', '/').Trim().ToLowerInvariant();

        static void MergeForwardResolved(Dictionary<string, List<string>> target, string[] requestedGamePaths, string[] resolvedFilePaths)
        {
            for (int i = 0; i < requestedGamePaths.Length; i++)
            {
                var filePath = NormalizeResolvedFilePathForBuild(resolvedFilePaths[i]);
                if (string.IsNullOrEmpty(filePath))
                    continue;

                var gamePath = NormalizeGamePathForBuild(requestedGamePaths[i]);
                if (string.IsNullOrEmpty(gamePath))
                    continue;

                if (!target.TryGetValue(filePath, out var list))
                    target[filePath] = list = new List<string>(1);

                list.Add(gamePath);
            }
        }

        static void MergeReverseResolved(Dictionary<string, List<string>> target, string[] requestedFilePaths, string[][] resolvedGamePaths)
        {
            for (int i = 0; i < requestedFilePaths.Length; i++)
            {
                var filePath = NormalizeResolvedFilePathForBuild(requestedFilePaths[i]);
                if (string.IsNullOrEmpty(filePath))
                    continue;

                if (!target.TryGetValue(filePath, out var list))
                    target[filePath] = list = new List<string>(resolvedGamePaths[i].Length);

                foreach (var gamePath in resolvedGamePaths[i])
                {
                    var normalizedGamePath = NormalizeGamePathForBuild(gamePath);
                    if (!string.IsNullOrEmpty(normalizedGamePath))
                        list.Add(normalizedGamePath);
                }
            }
        }

        if (forwardPaths.Length > 0)
        {
            var (forward, _) = await _ipcManager.Penumbra.ResolvePathsAsync(forwardPaths, []).ConfigureAwait(false);
            MergeForwardResolved(resolvedPaths, forwardPaths, forward);
        }

        if (reversePaths.Length > 0)
        {
            for (int i = 0; i < reversePaths.Length; i += ReverseResolveBatchSize)
            {
                var batchSize = Math.Min(ReverseResolveBatchSize, reversePaths.Length - i);
                var batch = new string[batchSize];
                Array.Copy(reversePaths, i, batch, 0, batchSize);

                var (_, reverse) = await _ipcManager.Penumbra.ResolvePathsAsync([], batch).ConfigureAwait(false);
                MergeReverseResolved(resolvedPaths, batch, reverse);

                if (i + ReverseResolveBatchSize < reversePaths.Length)
                {
                    await Task.Delay(SupportSweepBreatherDelay, ct).ConfigureAwait(false);
                }
            }
        }

        Dictionary<string, string[]> output = new(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in resolvedPaths)
            output[kvp.Key] = kvp.Value.ToArray();

        return output.AsReadOnly();
    }

    private async Task<IReadOnlyDictionary<string, string[]>> ResolveTransientReplacementsForBuildAsync(ObjectKind objectKind, HashSet<string> transientPaths, string? reason, CancellationToken ct)
    {
        if (transientPaths.Count == 0)
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase).AsReadOnly();

        if (ShouldForceFullLiveTransientResolveForBuild(reason))
        {
            var liveResolved = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
            return MergeKnownTransientResolvedPathFallbacks(objectKind, transientPaths, liveResolved);
        }

        var knownResolved = _transientResourceManager.GetKnownResolvedFilePaths(objectKind, transientPaths);
        if (knownResolved.Count == 0)
        {
            var liveResolved = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
            return MergeKnownTransientResolvedPathFallbacks(objectKind, transientPaths, liveResolved);
        }

        var pathsNeedingLiveResolve = transientPaths
            .Where(path => !knownResolved.ContainsKey(path))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var output = BuildResolvedFilePathMapFromKnownTransientPaths(knownResolved);

        if (pathsNeedingLiveResolve.Count > 0)
        {
            var liveMissingResolved = await GetFileReplacementsFromPaths(pathsNeedingLiveResolve, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
            MergeResolvedFilePathMap(output, liveMissingResolved);
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug(
                "Used transient.json resolve map for {known}/{total} {objectKind} path(s); live-resolved {live} missing path(s) for reason {reason}",
                knownResolved.Count,
                transientPaths.Count,
                objectKind,
                pathsNeedingLiveResolve.Count,
                reason ?? "<none>");
        }

        return output.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Distinct(StringComparer.OrdinalIgnoreCase).ToArray(), StringComparer.OrdinalIgnoreCase).AsReadOnly();
    }

    private static Dictionary<string, List<string>> BuildResolvedFilePathMapFromKnownTransientPaths(IReadOnlyDictionary<string, string> knownResolved)
    {
        var output = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in knownResolved)
        {
            var gamePath = NormalizeTransientGamePathForBuild(kvp.Key);
            var resolvedFilePath = NormalizeTransientResolvedPathForBuild(kvp.Value);
            if (string.IsNullOrWhiteSpace(gamePath) || string.IsNullOrWhiteSpace(resolvedFilePath))
                continue;

            if (!output.TryGetValue(resolvedFilePath, out var gamePaths))
                output[resolvedFilePath] = gamePaths = [];

            gamePaths.Add(gamePath);
        }

        return output;
    }

    private static void MergeResolvedFilePathMap(Dictionary<string, List<string>> output, IReadOnlyDictionary<string, string[]> resolved)
    {
        foreach (var kvp in resolved)
        {
            var resolvedFilePath = NormalizeTransientResolvedPathForBuild(kvp.Key);
            if (string.IsNullOrWhiteSpace(resolvedFilePath))
                continue;

            if (!output.TryGetValue(resolvedFilePath, out var gamePaths))
                output[resolvedFilePath] = gamePaths = [];

            foreach (var gamePath in kvp.Value)
            {
                var normalizedGamePath = NormalizeTransientGamePathForBuild(gamePath);
                if (!string.IsNullOrWhiteSpace(normalizedGamePath))
                    gamePaths.Add(normalizedGamePath);
            }
        }
    }

    private static string NormalizeTransientGamePathForBuild(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : value.Replace('\\', '/').Trim().ToLowerInvariant();

    private static string NormalizeTransientResolvedPathForBuild(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : value.Replace('\\', '/').Trim();

    private IReadOnlyDictionary<string, string[]> MergeKnownTransientResolvedPathFallbacks(ObjectKind objectKind, HashSet<string> requestedGamePaths, IReadOnlyDictionary<string, string[]> resolvedTransientPaths)
    {
        var knownResolvedPaths = _transientResourceManager.GetKnownResolvedFilePaths(objectKind, requestedGamePaths);
        if (knownResolvedPaths.Count == 0)
            return resolvedTransientPaths;

        var output = resolvedTransientPaths.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.ToList(),
            StringComparer.OrdinalIgnoreCase);

        static string? FindExistingOutputKey(Dictionary<string, List<string>> output, string resolvedFilePath)
            => output.Keys.FirstOrDefault(key => string.Equals(key, resolvedFilePath, StringComparison.OrdinalIgnoreCase));

        var fallbackCount = 0;
        var correctedPathCount = 0;
        var manifestOverrideCount = 0;
        foreach (var kvp in knownResolvedPaths)
        {
            var gamePath = kvp.Key;
            var resolvedFilePath = kvp.Value;

            if (string.IsNullOrWhiteSpace(gamePath)
                || string.IsNullOrWhiteSpace(resolvedFilePath)
                || !File.Exists(resolvedFilePath))
            {
                continue;
            }

            var outputKey = FindExistingOutputKey(output, resolvedFilePath);
            if (!string.IsNullOrWhiteSpace(outputKey)
                && !string.Equals(outputKey, resolvedFilePath, StringComparison.Ordinal))
            {
                var existingGamePaths = output[outputKey];
                output.Remove(outputKey);
                if (output.TryGetValue(resolvedFilePath, out var alreadyCorrectedGamePaths))
                {
                    foreach (var existingGamePath in existingGamePaths)
                    {
                        if (!alreadyCorrectedGamePaths.Contains(existingGamePath, StringComparer.OrdinalIgnoreCase))
                            alreadyCorrectedGamePaths.Add(existingGamePath);
                    }
                }
                else
                {
                    output[resolvedFilePath] = existingGamePaths;
                }

                outputKey = resolvedFilePath;
                correctedPathCount++;
            }

            if (string.IsNullOrWhiteSpace(outputKey))
            {
                outputKey = resolvedFilePath;
                output[outputKey] = [];
            }

            foreach (var otherKey in output.Keys.Where(key => !string.Equals(key, outputKey, StringComparison.OrdinalIgnoreCase)).ToList())
            {
                var otherGamePaths = output[otherKey];
                var removed = otherGamePaths.RemoveAll(path => string.Equals(path, gamePath, StringComparison.OrdinalIgnoreCase));
                if (removed > 0)
                    manifestOverrideCount += removed;

                if (otherGamePaths.Count == 0)
                    output.Remove(otherKey);
            }

            var gamePaths = output[outputKey];
            if (!gamePaths.Contains(gamePath, StringComparer.OrdinalIgnoreCase))
            {
                gamePaths.Add(gamePath);
                fallbackCount++;
            }
        }

        if (fallbackCount > 0 || correctedPathCount > 0 || manifestOverrideCount > 0)
        {
            _logger.LogDebug(
                "Applied {count} known transient manifest file path fallback(s), corrected {corrected} resolved path casing issue(s), and overrode {overridden} forward-resolved transient path(s)",
                fallbackCount, correctedPathCount, manifestOverrideCount);
        }

        return output.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Distinct(StringComparer.OrdinalIgnoreCase).ToArray(), StringComparer.OrdinalIgnoreCase).AsReadOnly();
    }

    private HashSet<string> ManageSemiTransientData(ObjectKind objectKind, IReadOnlyCollection<string>? staticResolvedGamePaths)
    {
        var pendingObjectTransients = IsSummonedObjectKind(objectKind)
            ? _transientResourceManager.GetPendingTransientResources(objectKind)
            : new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        HashSet<string> pathsToResolve = new(StringComparer.OrdinalIgnoreCase);
        foreach (var path in _transientResourceManager.PrepareTransientResourcesForBuild(objectKind))
        {
            if (!string.IsNullOrEmpty(path))
                pathsToResolve.Add(path);
        }

        if (IsSummonedObjectKind(objectKind))
            pathsToResolve = FilterSummonedObjectTransientsToCurrentActor(objectKind, pathsToResolve, pendingObjectTransients, staticResolvedGamePaths);

        return pathsToResolve;
    }

    private HashSet<string> FilterSummonedObjectTransientsToCurrentActor(ObjectKind objectKind, HashSet<string> pathsToResolve, HashSet<string> pendingObjectTransients, IReadOnlyCollection<string>? staticResolvedGamePaths)
    {
        if (pathsToResolve.Count == 0)
            return pathsToResolve;

        var activeScopes = BuildSummonedObjectPathScopes(staticResolvedGamePaths);
        if (activeScopes.Count == 0)
        {
            if (pendingObjectTransients.Count == 0)
            {
                _logger.LogTrace("Suppressing {count} persistent {objectKind} transient path(s) because the current summoned actor has no active static scope", pathsToResolve.Count, objectKind);
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            _logger.LogDebug("Using {count} pending {objectKind} transient path(s) only; current summoned actor has no active static scope", pendingObjectTransients.Count, objectKind);
            return pendingObjectTransients;
        }

        var filtered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in pathsToResolve)
        {
            if (pendingObjectTransients.Contains(path) || BelongsToActiveSummonedObjectScope(path, activeScopes))
                filtered.Add(path);
        }

        var removed = pathsToResolve.Count - filtered.Count;
        if (removed > 0)
        {
            _logger.LogDebug(
                "Filtered {removed}/{total} persisted {objectKind} transient path(s) out of this build because they belong to a different summoned actor",
                removed,
                pathsToResolve.Count,
                objectKind);
        }

        return filtered;
    }

    private static bool IsSummonedObjectKind(ObjectKind objectKind)
        => objectKind == ObjectKind.MinionOrMount || objectKind == ObjectKind.Companion;

    private static bool BelongsToActiveSummonedObjectScope(string path, HashSet<string> activeScopes)
    {
        var normalized = NormalizeTransientGamePathForBuild(path);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        foreach (var scope in GetSummonedObjectPathScopes(normalized))
        {
            if (activeScopes.Contains(scope))
                return true;
        }

        return activeScopes.Any(scope => normalized.StartsWith(scope, StringComparison.OrdinalIgnoreCase));
    }

    private static HashSet<string> BuildSummonedObjectPathScopes(IEnumerable<string>? gamePaths)
    {
        var scopes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (gamePaths == null)
            return scopes;

        foreach (var path in gamePaths)
        {
            foreach (var scope in GetSummonedObjectPathScopes(path))
                scopes.Add(scope);
        }

        return scopes;
    }

    private static IEnumerable<string> GetSummonedObjectPathScopes(string? path)
    {
        var normalized = NormalizeTransientGamePathForBuild(path);
        if (string.IsNullOrWhiteSpace(normalized))
            yield break;

        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (segments.Length == 0)
            yield break;

        for (var i = 0; i < segments.Length - 2; i++)
        {
            if (!string.Equals(segments[i], "chara", StringComparison.OrdinalIgnoreCase))
                continue;

            var category = segments[i + 1];
            if (!IsSummonedObjectActorCategory(category))
                continue;

            var actorScopeEnd = i + 2;
            var bodyScopeEnd = -1;
            for (var j = actorScopeEnd + 1; j < segments.Length - 2; j++)
            {
                if (string.Equals(segments[j], "obj", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(segments[j + 1], "body", StringComparison.OrdinalIgnoreCase))
                {
                    bodyScopeEnd = j + 2;
                    break;
                }
            }

            if (bodyScopeEnd >= 0)
                yield return string.Join('/', segments.Take(bodyScopeEnd + 1)) + "/";

            yield return string.Join('/', segments.Take(actorScopeEnd + 1)) + "/";
            yield break;
        }
    }

    private static bool IsSummonedObjectActorCategory(string category)
        => string.Equals(category, "monster", StringComparison.OrdinalIgnoreCase)
            || string.Equals(category, "demihuman", StringComparison.OrdinalIgnoreCase)
            || string.Equals(category, "minion", StringComparison.OrdinalIgnoreCase)
            || string.Equals(category, "weapon", StringComparison.OrdinalIgnoreCase);
}
