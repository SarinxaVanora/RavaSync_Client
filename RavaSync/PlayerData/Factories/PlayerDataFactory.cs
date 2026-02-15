using FFXIVClientStructs.FFXIV.Client.Game.Character;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Data;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using System.Collections.Concurrent;
using System.Linq;
using CharacterData = RavaSync.PlayerData.Data.CharacterData;

namespace RavaSync.PlayerData.Factories;

public class PlayerDataFactory
{
    private readonly DalamudUtilService _dalamudUtil;
    private readonly FileCacheManager _fileCacheManager;
    private readonly IpcManager _ipcManager;
    private readonly ILogger<PlayerDataFactory> _logger;
    private readonly PerformanceCollectorService _performanceCollector;
    private readonly XivDataAnalyzer _modelAnalyzer;
    private readonly MareMediator _mareMediator;
    private readonly TransientResourceManager _transientResourceManager;
    private readonly ConcurrentDictionary<string, bool> _papBoneValidationCache = new(StringComparer.Ordinal);

    public PlayerDataFactory(ILogger<PlayerDataFactory> logger, DalamudUtilService dalamudUtil, IpcManager ipcManager,
        TransientResourceManager transientResourceManager, FileCacheManager fileReplacementFactory,
        PerformanceCollectorService performanceCollector, XivDataAnalyzer modelAnalyzer, MareMediator mareMediator)
    {
        _logger = logger;
        _dalamudUtil = dalamudUtil;
        _ipcManager = ipcManager;
        _transientResourceManager = transientResourceManager;
        _fileCacheManager = fileReplacementFactory;
        _performanceCollector = performanceCollector;
        _modelAnalyzer = modelAnalyzer;
        _mareMediator = mareMediator;
        _logger.LogTrace("Creating {this}", nameof(PlayerDataFactory));
    }

    public async Task<CharacterDataFragment?> BuildCharacterData(GameObjectHandler playerRelatedObject, CancellationToken token)
    {
        if (!_ipcManager.Initialized)
        {
            throw new InvalidOperationException("Penumbra or Glamourer is not connected");
        }

        if (playerRelatedObject == null) return null;

        bool pointerIsZero = true;

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
                    return await CreateCharacterData(playerRelatedObject, token).ConfigureAwait(false);
                }).ConfigureAwait(true);
            }
            return await CreateCharacterData(playerRelatedObject, token).ConfigureAwait(true);
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

    private async Task<CharacterDataFragment> CreateCharacterData(GameObjectHandler playerRelatedObject, CancellationToken ct)
    {
        var objectKind = playerRelatedObject.ObjectKind;
        CharacterDataFragment fragment = objectKind == ObjectKind.Player ? new CharacterDataFragmentPlayer() : new();

        _logger.LogDebug("Building character data for {obj}", playerRelatedObject);

        await _dalamudUtil.WaitWhileCharacterIsDrawing(_logger, playerRelatedObject, Guid.NewGuid(), 30000, ct: ct).ConfigureAwait(false);
        int totalWaitTime = 10000;

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

        Dictionary<string, List<ushort>>? boneIndices =
            objectKind != ObjectKind.Player
            ? null
            : await _dalamudUtil.RunOnFrameworkThread(() => _modelAnalyzer.GetSkeletonBoneIndices(playerRelatedObject)).ConfigureAwait(false);

        DateTime start = DateTime.UtcNow;

        // penumbra call, it's currently broken
        Dictionary<string, HashSet<string>>? resolvedPaths;

        resolvedPaths = (await _ipcManager.Penumbra.GetCharacterData(_logger, playerRelatedObject).ConfigureAwait(false));
        if (resolvedPaths == null) throw new InvalidOperationException("Penumbra returned null data");

        ct.ThrowIfCancellationRequested();

        fragment.FileReplacements =
                new HashSet<FileReplacement>(resolvedPaths.Select(c => new FileReplacement([.. c.Value], c.Key)), FileReplacementComparer.Instance)
                .Where(p => p.HasFileReplacement).ToHashSet();
        fragment.FileReplacements.RemoveWhere(c => c.GamePaths.Any(g => !CacheMonitor.AllowedFileExtensions.Any(e => g.EndsWith(e, StringComparison.OrdinalIgnoreCase))));

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


        await _transientResourceManager.WaitForRecording(ct).ConfigureAwait(false);

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
        _transientResourceManager.ClearTransientPaths(objectKind, fragment.FileReplacements.SelectMany(c => c.GamePaths).ToList());

        // get all remaining paths and resolve them
        var transientPaths = ManageSemiTransientData(objectKind);
        var resolvedTransientPaths = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal)).ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("== Transient Replacements ==");
            foreach (var replacement in resolvedTransientPaths
                         .Select(c => new FileReplacement([.. c.Value], c.Key))
                         .OrderBy(f => f.ResolvedPath, StringComparer.Ordinal))
            {
                _logger.LogDebug("=> {repl}", replacement);
                fragment.FileReplacements.Add(replacement);
            }
        }
        else
        {
            foreach (var replacement in resolvedTransientPaths.Select(c => new FileReplacement([.. c.Value], c.Key)))
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
        foreach (var file in toCompute)
        {
            ct.ThrowIfCancellationRequested();
            file.Hash = computedPaths[file.ResolvedPath]?.Hash ?? string.Empty;
        }
        var removed = fragment.FileReplacements.RemoveWhere(f => !f.IsFileSwap && string.IsNullOrEmpty(f.Hash));
        if (removed > 0)
        {
            _logger.LogDebug("Removed {amount} of invalid files", removed);
        }

        ct.ThrowIfCancellationRequested();

        if (objectKind == ObjectKind.Player)
        {
            try
            {
                await VerifyPlayerAnimationBones(boneIndices, (fragment as CharacterDataFragmentPlayer)!, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException e)
            {
                _logger.LogDebug(e, "Cancelled during player animation verification");
                throw;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Failed to verify player animations, continuing without further verification");
            }
        }

        _logger.LogInformation("Building character data for {obj} took {time}ms", objectKind, TimeSpan.FromTicks(DateTime.UtcNow.Ticks - start.Ticks).TotalMilliseconds);

        return fragment;
    }

    private async Task VerifyPlayerAnimationBones(Dictionary<string, List<ushort>>? boneIndices, CharacterDataFragmentPlayer fragment, CancellationToken ct)
    {
        if (boneIndices == null) return;

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            foreach (var kvp in boneIndices)
            {
                _logger.LogDebug("Found {skellyname} ({idx} bone indices) on player: {bones}",
                    kvp.Key,
                    kvp.Value.Any() ? kvp.Value.Max() : 0,
                    string.Join(',', kvp.Value));
            }
        }

        if (boneIndices.All(u => u.Value.Count == 0)) return;

        int noValidationFailed = 0;
        foreach (var file in fragment.FileReplacements.Where(f => !f.IsFileSwap && f.GamePaths.First().EndsWith("pap", StringComparison.OrdinalIgnoreCase)).ToList())
        {
            ct.ThrowIfCancellationRequested();

            // If we've already validated this exact file hash before, reuse the result.
            if (!string.IsNullOrEmpty(file.Hash) && _papBoneValidationCache.TryGetValue(file.Hash, out var cachedOk))
            {
                if (!cachedOk)
                {
                    noValidationFailed++;
                    _logger.LogDebug("Removing {file} from sent file replacements and transient data (cached invalid)", file.ResolvedPath);
                    fragment.FileReplacements.Remove(file);
                    foreach (var gamePath in file.GamePaths)
                    {
                        _transientResourceManager.RemoveTransientResource(ObjectKind.Player, gamePath);
                    }
                }

                continue;
            }

            var skeletonIndices = await _dalamudUtil.RunOnFrameworkThread(() => _modelAnalyzer.GetBoneIndicesFromPap(file.Hash)).ConfigureAwait(false);

            bool validationFailed = false;

            if (skeletonIndices == null || skeletonIndices.Count == 0 || skeletonIndices.All(k => k.Value == null || k.Value.Count == 0))
            {
                _logger.LogDebug("Could not verify bone indices for {path} (unverifiable/empty). Allowing, but cannot guarantee safety.", file.ResolvedPath);

                if (!string.IsNullOrEmpty(file.Hash))
                    _papBoneValidationCache[file.Hash] = true;

                continue;
            }

            ushort maxPlayerIndex = 0;
            foreach (var list in boneIndices.Values)
            {
                if (list == null || list.Count == 0) continue;
                var localMax = list.Max();
                if (localMax > maxPlayerIndex)
                    maxPlayerIndex = localMax;
            }

            // 105 is the maximum vanilla skellington spoopy bone index
            if (skeletonIndices.All(k => (k.Value.Count == 0 ? 0 : k.Value.Max()) <= 105))
            {
                _logger.LogTrace("All indices of {path} are <= 105, ignoring", file.ResolvedPath);

                if (!string.IsNullOrEmpty(file.Hash))
                    _papBoneValidationCache[file.Hash] = true;

                continue;
            }

            _logger.LogDebug("Verifying bone indices for {path}, found {x} skeletons", file.ResolvedPath, skeletonIndices.Count);

            foreach (var kvp in skeletonIndices)
            {
                if (kvp.Value == null || kvp.Value.Count == 0) continue;

                var maxAnimIndex = kvp.Value.Max();
                if (maxAnimIndex > maxPlayerIndex)
                {
                    _logger.LogWarning(
                        "Found more bone indices on the animation {path} skeleton {skl} (max indice {idx}) than on any player related skeleton (max indice {idx2})",
                        file.ResolvedPath, kvp.Key, maxAnimIndex, maxPlayerIndex);

                    validationFailed = true;
                    break;
                }
            }

            if (validationFailed)
            {
                noValidationFailed++;
                _logger.LogDebug("Removing {file} from sent file replacements and transient data", file.ResolvedPath);
                fragment.FileReplacements.Remove(file);
                foreach (var gamePath in file.GamePaths)
                {
                    _transientResourceManager.RemoveTransientResource(ObjectKind.Player, gamePath);
                }
            }

            if (!string.IsNullOrEmpty(file.Hash))
                _papBoneValidationCache[file.Hash] = !validationFailed;
        }

        if (noValidationFailed > 0)
        {
            _mareMediator.Publish(new NotificationMessage("Invalid Skeleton Setup",
                $"Your client is attempting to send {noValidationFailed} animation files with invalid bone data. Those animation files have been removed from your sent data. " +
                $"Verify that you are using the correct skeleton for those animation files (Check /xllog for more information).",
                NotificationType.Warning, TimeSpan.FromSeconds(10)));
        }
    }

    private async Task<IReadOnlyDictionary<string, string[]>> GetFileReplacementsFromPaths(HashSet<string> forwardResolve, HashSet<string> reverseResolve)
    {
        var forwardPaths = forwardResolve.ToArray();
        var reversePaths = reverseResolve.ToArray();

        Dictionary<string, List<string>> resolvedPaths = new(StringComparer.OrdinalIgnoreCase);

        var (forward, reverse) = await _ipcManager.Penumbra.ResolvePathsAsync(forwardPaths, reversePaths).ConfigureAwait(false);

        for (int i = 0; i < forwardPaths.Length; i++)
        {
            var filePath = forward[i];
            if (string.IsNullOrEmpty(filePath)) continue;

            filePath = filePath.ToLowerInvariant();
            var gp = forwardPaths[i].ToLowerInvariant();

            if (!resolvedPaths.TryGetValue(filePath, out var list))
                resolvedPaths[filePath] = list = new List<string>(1);

            list.Add(gp);
        }

        for (int i = 0; i < reversePaths.Length; i++)
        {
            var filePath = reversePaths[i];
            if (string.IsNullOrEmpty(filePath)) continue;

            filePath = filePath.ToLowerInvariant();

            if (!resolvedPaths.TryGetValue(filePath, out var list))
                resolvedPaths[filePath] = list = new List<string>(reverse[i].Length);

            foreach (var g in reverse[i])
            {
                if (!string.IsNullOrEmpty(g))
                    list.Add(g.ToLowerInvariant());
            }
        }

        // Build final dictionary without extra LINQ allocations
        Dictionary<string, string[]> output = new(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in resolvedPaths)
            output[kvp.Key] = kvp.Value.ToArray();

        return output.AsReadOnly();
    }


    private HashSet<string> ManageSemiTransientData(ObjectKind objectKind)
    {
        if (_transientResourceManager.HasPendingTransients(objectKind))
        {
            _transientResourceManager.PersistTransientResources(objectKind);
        }

        HashSet<string> pathsToResolve = new(StringComparer.Ordinal);
        foreach (var path in _transientResourceManager.GetSemiTransientResources(objectKind))
        {
            if (!string.IsNullOrEmpty(path))
                pathsToResolve.Add(path);
        }

        return pathsToResolve;

    }
}