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
using RavaSync.PlayerData.Services;
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

    private sealed record StaticSupportDependencyExpansionCacheEntry(string Signature, FileReplacement[] Replacements);
    private sealed record StaticBuildSnapshot(ObjectKind ObjectKind, FileReplacement[] Replacements, DateTimeOffset UpdatedUtc);
    private sealed record BuildHashCacheEntry(string Hash, long Length, DateTime LastWriteUtc);
    private sealed record StaticResolvedBuildCacheEntry(string Signature, FileReplacement[] Replacements, DateTimeOffset UpdatedUtc);
    private sealed record TransientResolvedBuildCacheEntry(ObjectKind ObjectKind, string Signature, FileReplacement[] Replacements, DateTimeOffset UpdatedUtc);

    private readonly ConcurrentDictionary<string, PapSessionDecision> _playerPapDecisionsByKey = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, PapRewriteResult> _playerPapTargetIndependentDecisionsByHash = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StaticSupportDependencyCacheEntry> _staticSupportDependencyCacheByResolvedPath = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, StaticSupportDependencyExpansionCacheEntry> _staticSupportExpansionCacheBySignature = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<ObjectKind, StaticBuildSnapshot> _lastStaticBuildSnapshotByKind = new();
    private readonly ConcurrentDictionary<ObjectKind, StaticResolvedBuildCacheEntry> _staticResolvedBuildCacheByKind = new();
    private readonly ConcurrentDictionary<ObjectKind, TransientResolvedBuildCacheEntry> _transientResolvedBuildCacheByKind = new();
    private readonly ConcurrentDictionary<string, BuildHashCacheEntry> _buildHashCacheByResolvedPath = new(StringComparer.OrdinalIgnoreCase);
    private static readonly HashSet<string> AllowedBuildGamePathExtensions = new(CacheMonitor.AllowedFileExtensions, StringComparer.OrdinalIgnoreCase);
    private static readonly Regex HumanModelPathRegex = new(@"(?:^|/)chara/human/(c\d{4})/", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.ECMAScript);
    private const int ReverseResolveBatchSize = 192;
    private const int CharacterBuildYieldEvery = 128;
    private const int StaticSupportDependencyCacheSoftLimit = 2048;
    private const int HumanRaceFamilyCollapseMinimumVariants = 3;
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

        _mareMediator.Subscribe<PenumbraFileCacheChangedMessage>(this, _ => InvalidateLiveBuildSnapshots());
        _mareMediator.Subscribe<PenumbraModSettingChangedMessage>(this, _ => InvalidateLiveBuildSnapshots());
        _mareMediator.Subscribe<PenumbraDirectoryChangedMessage>(this, _ => InvalidateVolatilePlayerDataCaches());
        _mareMediator.Subscribe<ClassJobChangedMessage>(this, _ => InvalidateLiveBuildSnapshots());
        _mareMediator.Subscribe<PenumbraResourceLoadMessage>(this, _ => InvalidateTransientResolveCaches());
        _mareMediator.Subscribe<ObservedSupportResourceMessage>(this, _ => InvalidateTransientResolveCaches());
        _mareMediator.Subscribe<TransientResourceChangedMessage>(this, _ => InvalidateTransientResolveCaches());
        _mareMediator.Subscribe<PenumbraStartRedrawMessage>(this, _ => InvalidateTransientResolveCaches());
        _mareMediator.Subscribe<PenumbraEndRedrawMessage>(this, _ => InvalidateTransientResolveCaches());
        _mareMediator.Subscribe<DisconnectedMessage>(this, _ => InvalidateVolatilePlayerDataCaches());
        _mareMediator.Subscribe<DalamudLogoutMessage>(this, _ => InvalidateVolatilePlayerDataCaches());

        _logger.LogTrace("Creating {this}", nameof(PlayerDataFactory));
    }

    public MareMediator Mediator => _mareMediator;

    private string _lastKnownPlayerPapSkeletonFingerprint = string.Empty;

    private void InvalidateLiveBuildSnapshots()
    {
        _lastStaticBuildSnapshotByKind.Clear();
        _staticResolvedBuildCacheByKind.Clear();
        InvalidateTransientResolveCaches();
    }

    private void InvalidateTransientResolveCaches()
    {
        _transientResolvedBuildCacheByKind.Clear();
    }

    private void InvalidateVolatilePlayerDataCaches()
    {
        _playerPapDecisionsByKey.Clear();
        _playerPapTargetIndependentDecisionsByHash.Clear();
        _lastKnownPlayerPapSkeletonFingerprint = string.Empty;
        InvalidateLiveBuildSnapshots();
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

        var usedCachedStaticReplacements = TryGetCachedStaticResolvedBuildReplacements(objectKind, resolvedPaths, out var cachedStaticReplacements);
        fragment.FileReplacements = usedCachedStaticReplacements
            ? new HashSet<FileReplacement>(cachedStaticReplacements, FileReplacementComparer.Instance)
            : BuildStaticFileReplacements(resolvedPaths);

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

        if (!usedCachedStaticReplacements)
        {
            await AddStaticSupportDependencyReplacementsAsync(fragment, ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
        }

        var staticReplacementReferences = fragment.FileReplacements.ToArray();

        await _transientResourceManager.WaitForRecording(ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        // Persistent Summoner-style pets need their static replacements kept alive as transients,
        // otherwise every minor pet change can turn into redraw city. Short-lived combat summons
        // such as DRK Living Shadow are different: promoting every spawned-actor replacement into
        // the persistent Pet transient bucket during summon creation can stall the local client.
        if (objectKind == ObjectKind.Pet)
        {
            if (_dalamudUtil.ShouldTreatPetAsShortLivedCombatSummonForCurrentJob)
            {
                _logger.LogTrace("Keeping {count} Pet static replacements static for short-lived combat summon; skipping persistent Pet transient promotion", fragment.FileReplacements.Count);
            }
            else
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
        }

        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("Handling transient update for {obj}", playerRelatedObject);

        if (objectKind == ObjectKind.Player && _transientResourceManager.HasDirtyTransientManifestForNextBuild)
        {
            var isLocalPlayerBuild = await _dalamudUtil.RunOnFrameworkThread(() => _dalamudUtil.GetPlayerPtr() == playerRelatedObject.Address).ConfigureAwait(false);
            if (isLocalPlayerBuild)
            {
                await _transientResourceManager.EnsureTransientManifestStateForCurrentBuildAsync(objectKind, reason, ct).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
            }
            else
            {
                _logger.LogTrace("Skipping dirty transient manifest refresh for non-local player build {obj}", playerRelatedObject);
            }
        }
        ct.ThrowIfCancellationRequested();

        // remove all potentially gathered paths from the transient resource manager that are resolved through static resolving
        var staticResolvedGamePaths = fragment.FileReplacements.SelectMany(c => c.GamePaths).ToList();
        _transientResourceManager.ClearTransientPaths(objectKind, staticResolvedGamePaths);

        // get all remaining paths and resolve them
        var transientPaths = ManageSemiTransientData(objectKind, staticResolvedGamePaths, playerRelatedObject, staticReplacementReferences);
        var transientReplacements = await ResolveTransientFileReplacementsForBuildAsync(objectKind, transientPaths, reason, ct).ConfigureAwait(false);
        var transientReplacementReferences = transientReplacements.ToArray();
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

        await AddSenderSelectedFullMountModReplacementsAsync(fragment, objectKind, playerRelatedObject, ct).ConfigureAwait(false);
        await AddSenderDefaultCollectionMountMusicReplacementsAsync(fragment, objectKind, ct).ConfigureAwait(false);

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
        await PopulateReplacementHashesAsync(toCompute, ct).ConfigureAwait(false);
        var removed = fragment.FileReplacements.RemoveWhere(f => !f.IsFileSwap && string.IsNullOrEmpty(f.Hash));
        if (removed > 0)
        {
            _logger.LogDebug("Removed {amount} of invalid files", removed);
        }

        RememberStaticBuildSnapshot(objectKind, staticReplacementReferences);
        RememberStaticResolvedBuildCache(objectKind, resolvedPaths, staticReplacementReferences);
        RememberTransientResolvedBuildCache(objectKind, transientPaths, transientReplacementReferences);

        ct.ThrowIfCancellationRequested();

        _logger.LogInformation("Building character data for {obj} took {time}ms (reason={reason})", objectKind, TimeSpan.FromTicks(DateTime.UtcNow.Ticks - start.Ticks).TotalMilliseconds, string.IsNullOrWhiteSpace(reason) ? "<none>" : reason);

        return fragment;
    }


    private async Task AddSenderSelectedFullMountModReplacementsAsync(CharacterDataFragment fragment, ObjectKind objectKind, GameObjectHandler playerRelatedObject, CancellationToken ct)
    {
        if (objectKind != ObjectKind.MinionOrMount)
            return;

        if (!_ipcManager.Penumbra.APIAvailable)
            return;

        var activeMountIds = ExtractActiveMountActorIdsFromFragment(fragment);
        if (activeMountIds.Count == 0)
            return;

        var collectionStates = new List<IpcCallerPenumbra.PenumbraCollectionModSettings>();
        var seenCollectionIds = new HashSet<Guid>();

        if (playerRelatedObject != null)
        {
            var mountObjectIndex = await _dalamudUtil.RunOnFrameworkThread(() => playerRelatedObject.GetGameObject()?.ObjectIndex ?? -1).ConfigureAwait(false);
            if (mountObjectIndex >= 0)
            {
                var mountObjectCollectionState = await _ipcManager.Penumbra.GetLocalPlayerCollectionModSettingsAsync(_logger, mountObjectIndex).ConfigureAwait(false);
                if (mountObjectCollectionState != null && mountObjectCollectionState.CollectionId != Guid.Empty && seenCollectionIds.Add(mountObjectCollectionState.CollectionId))
                    collectionStates.Add(mountObjectCollectionState);
            }
        }

        var localPlayerCollectionState = await _localPapSafetyModService.TryGetLocalPlayerCollectionSettingsAsync(ct).ConfigureAwait(false);
        if (localPlayerCollectionState != null && localPlayerCollectionState.CollectionId != Guid.Empty && seenCollectionIds.Add(localPlayerCollectionState.CollectionId))
            collectionStates.Add(localPlayerCollectionState);

        var defaultCollectionState = await _ipcManager.Penumbra.GetDefaultCollectionModSettingsAsync(_logger).ConfigureAwait(false);
        if (defaultCollectionState != null && defaultCollectionState.CollectionId != Guid.Empty && seenCollectionIds.Add(defaultCollectionState.CollectionId))
            collectionStates.Add(defaultCollectionState);

        if (collectionStates.Count == 0)
            return;

        var added = 0;
        foreach (var collectionState in collectionStates)
        {
            ct.ThrowIfCancellationRequested();

            var selectedMountModFiles = await _localPapSafetyModService.ResolveSelectedFullMountModFilesAsync(collectionState, activeMountIds, ct).ConfigureAwait(false);
            if (selectedMountModFiles.Count == 0)
                continue;

            foreach (var source in selectedMountModFiles.OrderBy(static source => source.ResolvedPath, StringComparer.OrdinalIgnoreCase))
            {
                ct.ThrowIfCancellationRequested();

                var replacement = new FileReplacement(source.GamePaths, source.ResolvedPath)
                {
                    Hash = source.Hash,
                };

                if (!replacement.HasFileReplacement || string.IsNullOrWhiteSpace(replacement.Hash))
                    continue;

                if (fragment.FileReplacements.Add(replacement))
                    added++;
            }
        }

        if (added > 0)
            _logger.LogDebug("Added {count} selected active mount mod file(s) for mount actor(s) {mounts}", added, string.Join(",", activeMountIds.OrderBy(static id => id, StringComparer.OrdinalIgnoreCase)));
    }

    private static HashSet<string> ExtractActiveMountActorIdsFromFragment(CharacterDataFragment fragment)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (fragment?.FileReplacements == null || fragment.FileReplacements.Count == 0)
            return output;

        foreach (var gamePath in fragment.FileReplacements.SelectMany(static replacement => replacement.GamePaths))
        {
            if (TryExtractMountActorIdFromGamePath(gamePath, out var mountId))
                output.Add(mountId);
        }

        return output;
    }

    private static bool TryExtractMountActorIdFromGamePath(string? gamePath, out string mountId)
    {
        mountId = string.Empty;
        var normalized = NormalizeTransientGamePathForBuild(gamePath);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        var match = Regex.Match(normalized, @"(?:^|/)chara/monster/((?:m|d)\d{4})(?:/|$)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        mountId = match.Groups[1].Value.ToLowerInvariant();
        return !string.IsNullOrWhiteSpace(mountId);
    }

    private async Task AddSenderDefaultCollectionMountMusicReplacementsAsync(CharacterDataFragment fragment, ObjectKind objectKind, CancellationToken ct)
    {
        if (objectKind != ObjectKind.MinionOrMount)
            return;

        if (!_ipcManager.Penumbra.APIAvailable)
            return;

        var defaultCollectionState = await _ipcManager.Penumbra.GetDefaultCollectionModSettingsAsync(_logger).ConfigureAwait(false);
        if (defaultCollectionState == null)
            return;

        var selectedMountMusic = await _localPapSafetyModService.ResolveSelectedMountMusicFilesAsync(defaultCollectionState, ct).ConfigureAwait(false);
        if (selectedMountMusic.Count == 0)
            return;

        foreach (var source in selectedMountMusic.OrderBy(static source => source.ResolvedPath, StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();

            var replacement = new FileReplacement(source.GamePaths, source.ResolvedPath)
            {
                Hash = source.Hash,
            };

            if (!replacement.HasFileReplacement || string.IsNullOrWhiteSpace(replacement.Hash))
                continue;

            if (fragment.FileReplacements.Add(replacement))
            {
                _logger.LogTrace("Added sender Default collection mount music replacement {gamePath} => {path}", string.Join(",", replacement.GamePaths), replacement.ResolvedPath);
            }
        }
    }

    public async Task<bool> TryBuildPlayerTransientOnlyFragmentFromSnapshotAsync(CharacterDataFragmentPlayer fragment, string? reason, CancellationToken ct)
    {
        if (fragment == null)
            return false;

        if (!_lastStaticBuildSnapshotByKind.TryGetValue(ObjectKind.Player, out var snapshot)
            || snapshot.Replacements.Length == 0)
        {
            return false;
        }

        var start = DateTime.UtcNow;
        fragment.FileReplacements = new HashSet<FileReplacement>(
            snapshot.Replacements.Select(static replacement => CloneFileReplacementWithHash(replacement)),
            FileReplacementComparer.Instance);

        await _transientResourceManager.WaitForRecording(ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        var staticResolvedGamePaths = fragment.FileReplacements.SelectMany(static replacement => replacement.GamePaths).ToList();
        _transientResourceManager.ClearTransientPaths(ObjectKind.Player, staticResolvedGamePaths);

        var transientPaths = ManageSemiTransientData(ObjectKind.Player, staticResolvedGamePaths);
        var transientReplacements = await ResolveTransientFileReplacementsForBuildAsync(ObjectKind.Player, transientPaths, reason, ct).ConfigureAwait(false);
        var transientReplacementReferences = transientReplacements.ToArray();

        foreach (var replacement in transientReplacements)
        {
            ct.ThrowIfCancellationRequested();
            if (replacement.HasFileReplacement)
                fragment.FileReplacements.Add(replacement);
        }

        _transientResourceManager.CleanUpSemiTransientResources(ObjectKind.Player, [.. fragment.FileReplacements]);
        fragment.FileReplacements = new HashSet<FileReplacement>(
            fragment.FileReplacements.Where(static replacement => replacement.HasFileReplacement).OrderBy(static replacement => replacement.ResolvedPath, StringComparer.Ordinal),
            FileReplacementComparer.Instance);

        var toCompute = fragment.FileReplacements
            .Where(static replacement => !replacement.IsFileSwap && string.IsNullOrWhiteSpace(replacement.Hash))
            .ToArray();
        await PopulateReplacementHashesAsync(toCompute, ct).ConfigureAwait(false);

        var removed = fragment.FileReplacements.RemoveWhere(static replacement => !replacement.IsFileSwap && string.IsNullOrEmpty(replacement.Hash));
        if (removed > 0)
            _logger.LogDebug("Removed {amount} invalid transient-only player file(s)", removed);

        RememberTransientResolvedBuildCache(ObjectKind.Player, transientPaths, transientReplacementReferences);

        _logger.LogInformation("Building transient-only character data for Player took {time}ms", TimeSpan.FromTicks(DateTime.UtcNow.Ticks - start.Ticks).TotalMilliseconds);
        return true;
    }

    private HashSet<FileReplacement> BuildStaticFileReplacements(IReadOnlyDictionary<string, HashSet<string>> resolvedPaths)
    {
        var replacements = new HashSet<FileReplacement>(FileReplacementComparer.Instance);
        foreach (var resolvedPath in resolvedPaths)
        {
            var replacement = new FileReplacement([.. resolvedPath.Value], resolvedPath.Key);
            if (replacement.HasFileReplacement && replacement.GamePaths.All(IsAllowedBuildGamePath))
                replacements.Add(replacement);
        }

        return replacements;
    }

    private bool TryGetCachedStaticResolvedBuildReplacements(ObjectKind objectKind, IReadOnlyDictionary<string, HashSet<string>> resolvedPaths, out FileReplacement[] replacements)
    {
        replacements = [];
        if (!TryBuildStaticResolvedBuildSignature(resolvedPaths, out var signature))
            return false;

        if (!_staticResolvedBuildCacheByKind.TryGetValue(objectKind, out var cacheEntry)
            || !string.Equals(cacheEntry.Signature, signature, StringComparison.Ordinal))
        {
            return false;
        }

        if (!TryCloneCachedReplacementsWithValidatedHashes(cacheEntry.Replacements, out replacements))
        {
            _staticResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            replacements = [];
            return false;
        }

        return true;
    }

    private void RememberStaticResolvedBuildCache(ObjectKind objectKind, IReadOnlyDictionary<string, HashSet<string>> resolvedPaths, FileReplacement[] staticReplacementReferences)
    {
        if (staticReplacementReferences == null || staticReplacementReferences.Length == 0)
        {
            _staticResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            return;
        }

        if (!TryBuildStaticResolvedBuildSignature(resolvedPaths, out var signature))
            return;

        var replacements = staticReplacementReferences
            .Where(static replacement => replacement.HasFileReplacement
                && (replacement.IsFileSwap || !string.IsNullOrWhiteSpace(replacement.Hash)))
            .OrderBy(static replacement => replacement.ResolvedPath, StringComparer.OrdinalIgnoreCase)
            .Select(static replacement => CloneFileReplacementWithHash(replacement))
            .ToArray();

        if (replacements.Length == 0)
        {
            _staticResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            return;
        }

        _staticResolvedBuildCacheByKind[objectKind] = new StaticResolvedBuildCacheEntry(signature, replacements, DateTimeOffset.UtcNow);
    }

    private async Task<List<FileReplacement>> ResolveTransientFileReplacementsForBuildAsync(ObjectKind objectKind, HashSet<string> transientPaths, string? reason, CancellationToken ct)
    {
        if (transientPaths.Count == 0)
            return [];

        if (!ShouldBypassTransientResolvedBuildCache(objectKind, reason)
            && TryGetCachedTransientResolvedBuildReplacements(objectKind, transientPaths, out var cachedReplacements))
        {
            return cachedReplacements.ToList();
        }

        var resolvedTransientPaths = await ResolveTransientReplacementsForBuildAsync(objectKind, transientPaths, reason, ct).ConfigureAwait(false);
        var transientReplacements = new List<FileReplacement>(resolvedTransientPaths.Count);
        foreach (var resolvedTransientPath in resolvedTransientPaths)
        {
            ct.ThrowIfCancellationRequested();
            var replacement = new FileReplacement([.. resolvedTransientPath.Value], resolvedTransientPath.Key);
            if (replacement.HasFileReplacement)
                transientReplacements.Add(replacement);
        }

        return transientReplacements;
    }

    private bool TryGetCachedTransientResolvedBuildReplacements(ObjectKind objectKind, HashSet<string> transientPaths, out FileReplacement[] replacements)
    {
        replacements = [];
        if (!TryBuildTransientResolvedBuildSignature(transientPaths, out var signature))
            return false;

        if (!_transientResolvedBuildCacheByKind.TryGetValue(objectKind, out var cacheEntry)
            || cacheEntry.ObjectKind != objectKind
            || !string.Equals(cacheEntry.Signature, signature, StringComparison.Ordinal))
        {
            return false;
        }

        if (!TryCloneCachedReplacementsWithValidatedHashes(cacheEntry.Replacements, out replacements))
        {
            _transientResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            replacements = [];
            return false;
        }

        return true;
    }

    private void RememberTransientResolvedBuildCache(ObjectKind objectKind, HashSet<string> transientPaths, FileReplacement[] transientReplacementReferences)
    {
        if (transientReplacementReferences == null || transientReplacementReferences.Length == 0)
        {
            _transientResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            return;
        }

        if (!TryBuildTransientResolvedBuildSignature(transientPaths, out var signature))
            return;

        var replacements = transientReplacementReferences
            .Where(static replacement => replacement.HasFileReplacement
                && (replacement.IsFileSwap || !string.IsNullOrWhiteSpace(replacement.Hash)))
            .OrderBy(static replacement => replacement.ResolvedPath, StringComparer.OrdinalIgnoreCase)
            .Select(static replacement => CloneFileReplacementWithHash(replacement))
            .ToArray();

        if (replacements.Length == 0)
        {
            _transientResolvedBuildCacheByKind.TryRemove(objectKind, out _);
            return;
        }

        _transientResolvedBuildCacheByKind[objectKind] = new TransientResolvedBuildCacheEntry(objectKind, signature, replacements, DateTimeOffset.UtcNow);
    }

    private bool TryCloneCachedReplacementsWithValidatedHashes(FileReplacement[] source, out FileReplacement[] clones)
    {
        clones = [];
        if (source == null || source.Length == 0)
            return false;

        var output = new FileReplacement[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            var replacement = source[i];
            if (replacement == null || !replacement.HasFileReplacement)
                return false;

            var clone = CloneFileReplacementWithHash(replacement);
            if (!clone.IsFileSwap)
            {
                if (string.IsNullOrWhiteSpace(clone.Hash)
                    || !TryGetCachedBuildHash(clone.ResolvedPath, out var currentHash)
                    || !string.Equals(currentHash, clone.Hash, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            output[i] = clone;
        }

        clones = output;
        return true;
    }

    private static bool TryBuildStaticResolvedBuildSignature(IReadOnlyDictionary<string, HashSet<string>> resolvedPaths, out string signature)
    {
        signature = string.Empty;
        if (resolvedPaths == null || resolvedPaths.Count == 0)
            return false;

        var sb = new StringBuilder(resolvedPaths.Count * 96);
        sb.Append("static-resolved-v1|");
        foreach (var kvp in resolvedPaths.OrderBy(static kvp => NormalizeStaticSupportResolvedPath(kvp.Key), StringComparer.OrdinalIgnoreCase))
        {
            var resolvedPath = NormalizeStaticSupportResolvedPath(kvp.Key);
            if (string.IsNullOrWhiteSpace(resolvedPath))
                continue;

            sb.Append("r:").Append(resolvedPath).Append('|');
            foreach (var gamePath in kvp.Value
                         .Select(NormalizeStaticSupportGamePath)
                         .Where(static path => !string.IsNullOrWhiteSpace(path))
                         .Distinct(StringComparer.OrdinalIgnoreCase)
                         .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase))
            {
                sb.Append("g:").Append(gamePath).Append('|');
            }
        }

        signature = sb.ToString();
        return signature.Length > "static-resolved-v1|".Length;
    }

    private static bool TryBuildTransientResolvedBuildSignature(HashSet<string> transientPaths, out string signature)
    {
        signature = string.Empty;
        if (transientPaths == null || transientPaths.Count == 0)
            return false;

        var sb = new StringBuilder(transientPaths.Count * 64);
        sb.Append("transient-resolved-v1|");
        foreach (var path in transientPaths
                     .Select(NormalizeTransientGamePathForBuild)
                     .Where(static path => !string.IsNullOrWhiteSpace(path))
                     .Distinct(StringComparer.OrdinalIgnoreCase)
                     .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append(path).Append('|');
        }

        signature = sb.ToString();
        return signature.Length > "transient-resolved-v1|".Length;
    }

    private static bool ShouldBypassTransientResolvedBuildCache(ObjectKind objectKind, string? reason)
    {
        if (ShouldForceFullLiveTransientResolveForBuild(reason))
            return true;

        if (string.IsNullOrWhiteSpace(reason))
            return false;

        var reasons = reason.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var item in reasons)
        {
            if (item.Contains("TransientResourceChanged", StringComparison.Ordinal)
                || item.Contains("PenumbraRedraw", StringComparison.Ordinal)
                || item.Contains("PenumbraEndRedraw", StringComparison.Ordinal)
                || item.StartsWith("Connected:", StringComparison.Ordinal)
                || item.StartsWith("ImmediateFollowUp:Connected:", StringComparison.Ordinal)
                || item.StartsWith("PenumbraFileCacheChanged", StringComparison.Ordinal)
                || item.StartsWith("PenumbraModSettingChanged", StringComparison.Ordinal)
                || item.StartsWith("ClassJobChanged:", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private void RememberStaticBuildSnapshot(ObjectKind objectKind, FileReplacement[] staticReplacementReferences)
    {
        if (objectKind != ObjectKind.Player || staticReplacementReferences == null || staticReplacementReferences.Length == 0)
            return;

        var replacements = staticReplacementReferences
            .Where(static replacement => replacement.HasFileReplacement
                && (replacement.IsFileSwap || !string.IsNullOrWhiteSpace(replacement.Hash)))
            .OrderBy(static replacement => replacement.ResolvedPath, StringComparer.OrdinalIgnoreCase)
            .Select(static replacement => CloneFileReplacementWithHash(replacement))
            .ToArray();

        if (replacements.Length == 0)
        {
            _lastStaticBuildSnapshotByKind.TryRemove(objectKind, out _);
            return;
        }

        _lastStaticBuildSnapshotByKind[objectKind] = new StaticBuildSnapshot(objectKind, replacements, DateTimeOffset.UtcNow);
    }

    private async Task PopulateReplacementHashesAsync(FileReplacement[] replacements, CancellationToken ct)
    {
        if (replacements.Length == 0)
            return;

        var pathsNeedingLookup = new List<string>();
        var seenMissing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var localHashByResolvedPath = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < replacements.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var replacement = replacements[i];
            var resolvedPath = replacement.ResolvedPath;
            if (string.IsNullOrWhiteSpace(resolvedPath))
                continue;

            if (localHashByResolvedPath.TryGetValue(resolvedPath, out var localHash))
            {
                replacement.Hash = localHash;
            }
            else if (TryGetCachedBuildHash(resolvedPath, out var cachedHash))
            {
                replacement.Hash = cachedHash;
                localHashByResolvedPath[resolvedPath] = cachedHash;
            }
            else
            {
                replacement.Hash = string.Empty;
                if (seenMissing.Add(resolvedPath))
                    pathsNeedingLookup.Add(resolvedPath);
            }

            if ((i + 1) % CharacterBuildYieldEvery == 0)
                await Task.Yield();
        }

        if (pathsNeedingLookup.Count == 0)
            return;

        var computedHashes = _fileCacheManager.GetFileHashesByPaths(pathsNeedingLookup.ToArray());
        foreach (var item in computedHashes)
        {
            if (string.IsNullOrWhiteSpace(item.Key) || string.IsNullOrWhiteSpace(item.Value))
                continue;

            localHashByResolvedPath[item.Key] = item.Value;
            RememberBuildHash(item.Key, item.Value);
        }

        for (var i = 0; i < replacements.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var replacement = replacements[i];
            if (!string.IsNullOrWhiteSpace(replacement.Hash))
                continue;

            if (!string.IsNullOrWhiteSpace(replacement.ResolvedPath)
                && localHashByResolvedPath.TryGetValue(replacement.ResolvedPath, out var hash)
                && !string.IsNullOrWhiteSpace(hash))
            {
                replacement.Hash = hash;
            }

            if ((i + 1) % CharacterBuildYieldEvery == 0)
                await Task.Yield();
        }
    }

    private bool TryGetCachedBuildHash(string? resolvedPath, out string hash)
    {
        hash = string.Empty;
        var normalizedPath = NormalizeStaticSupportResolvedPath(resolvedPath);
        if (string.IsNullOrWhiteSpace(normalizedPath))
            return false;

        if (!TryGetBuildFileIdentity(normalizedPath, out var length, out var lastWriteUtc))
        {
            _buildHashCacheByResolvedPath.TryRemove(normalizedPath, out _);
            return false;
        }

        if (!_buildHashCacheByResolvedPath.TryGetValue(normalizedPath, out var cacheEntry)
            || cacheEntry.Length != length
            || cacheEntry.LastWriteUtc != lastWriteUtc
            || string.IsNullOrWhiteSpace(cacheEntry.Hash))
        {
            return false;
        }

        hash = cacheEntry.Hash;
        return true;
    }

    private void RememberBuildHash(string? resolvedPath, string? hash)
    {
        var normalizedPath = NormalizeStaticSupportResolvedPath(resolvedPath);
        if (string.IsNullOrWhiteSpace(normalizedPath) || string.IsNullOrWhiteSpace(hash))
            return;

        if (!TryGetBuildFileIdentity(normalizedPath, out var length, out var lastWriteUtc))
            return;

        _buildHashCacheByResolvedPath[normalizedPath] = new BuildHashCacheEntry(hash, length, lastWriteUtc);
    }

    private static bool TryGetBuildFileIdentity(string resolvedPath, out long length, out DateTime lastWriteUtc)
    {
        length = 0;
        lastWriteUtc = default;

        try
        {
            var fileInfo = new FileInfo(resolvedPath);
            if (!fileInfo.Exists)
                return false;

            length = fileInfo.Length;
            lastWriteUtc = fileInfo.LastWriteTimeUtc;
            return true;
        }
        catch
        {
            return false;
        }
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

    private Task AddStaticSupportDependencyReplacementsAsync(CharacterDataFragment fragment, CancellationToken ct)
    {
        if (fragment.FileReplacements.Count == 0)
            return Task.CompletedTask;

        if (!HasStaticSupportDependencySource(fragment.FileReplacements))
            return Task.CompletedTask;

        if (!TryBuildStaticSupportDependencyExpansionSignature(fragment, out var cacheSignature))
            return Task.CompletedTask;

        if (TryApplyCachedStaticSupportDependencyExpansion(fragment, cacheSignature, ct))
            return Task.CompletedTask;

        _staticSupportExpansionCacheBySignature[cacheSignature] = new StaticSupportDependencyExpansionCacheEntry(
            cacheSignature,
            Array.Empty<FileReplacement>());

        return Task.CompletedTask;
    }

    private static bool HasStaticSupportDependencySource(IEnumerable<FileReplacement> replacements)
    {
        foreach (var replacement in replacements)
        {
            var resolvedExtension = Path.GetExtension(replacement.ResolvedPath);
            if (string.Equals(resolvedExtension, ".mdl", StringComparison.OrdinalIgnoreCase)
                || string.Equals(resolvedExtension, ".mtrl", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            foreach (var gamePath in replacement.GamePaths)
            {
                var gameExtension = Path.GetExtension(gamePath.Replace('\\', '/'));
                if (string.Equals(gameExtension, ".mdl", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(gameExtension, ".mtrl", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private bool TryApplyCachedStaticSupportDependencyExpansion(CharacterDataFragment fragment, string signature, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(signature))
            return false;

        if (!_staticSupportExpansionCacheBySignature.TryGetValue(signature, out var cacheEntry)
            || !string.Equals(cacheEntry.Signature, signature, StringComparison.Ordinal))
        {
            return false;
        }

        var added = 0;
        foreach (var cachedReplacement in cacheEntry.Replacements)
        {
            ct.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(cachedReplacement.ResolvedPath))
                continue;

            if (fragment.FileReplacements.Add(CloneFileReplacementWithoutHash(cachedReplacement)))
                added++;
        }

        if (added > 0 && _logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Reused {count} cached static support dependency replacement(s)", added);

        return true;
    }

    private static bool TryBuildStaticSupportDependencyExpansionSignature(CharacterDataFragment fragment, out string signature)
    {
        signature = string.Empty;
        if (fragment?.FileReplacements == null || fragment.FileReplacements.Count == 0)
            return false;

        var dependencySourceCount = 0;
        var sb = new StringBuilder(fragment.FileReplacements.Count * 96);
        sb.Append("static-support-declared-v3|");

        foreach (var replacement in fragment.FileReplacements
                     .OrderBy(static replacement => replacement.ResolvedPath, StringComparer.OrdinalIgnoreCase))
        {
            var resolvedPath = NormalizeStaticSupportResolvedPath(replacement.ResolvedPath);
            if (string.IsNullOrWhiteSpace(resolvedPath))
                continue;

            var resolvedExtension = Path.GetExtension(resolvedPath);
            var hasDependencySource = string.Equals(resolvedExtension, ".mdl", StringComparison.OrdinalIgnoreCase)
                || string.Equals(resolvedExtension, ".mtrl", StringComparison.OrdinalIgnoreCase)
                || replacement.GamePaths.Any(static gamePath =>
                {
                    var gameExtension = Path.GetExtension((gamePath ?? string.Empty).Replace('\\', '/'));
                    return string.Equals(gameExtension, ".mdl", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(gameExtension, ".mtrl", StringComparison.OrdinalIgnoreCase);
                });

            if (!hasDependencySource)
                continue;

            dependencySourceCount++;
            sb.Append("r:").Append(resolvedPath).Append('|');

            foreach (var gamePath in replacement.GamePaths
                         .Select(NormalizeStaticSupportGamePath)
                         .Where(static path => !string.IsNullOrWhiteSpace(path))
                         .Distinct(StringComparer.OrdinalIgnoreCase)
                         .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase))
            {
                sb.Append("g:").Append(gamePath).Append('|');
            }
        }

        if (dependencySourceCount == 0)
            return false;

        signature = sb.ToString();
        return true;
    }

    private static FileReplacement CloneFileReplacementWithoutHash(FileReplacement replacement)
        => new([.. replacement.GamePaths], replacement.ResolvedPath);

    private static FileReplacement CloneFileReplacementWithHash(FileReplacement replacement)
        => new([.. replacement.GamePaths], replacement.ResolvedPath) { Hash = replacement.Hash };

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


    private static bool IsAllowedBuildGamePath(string? gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return AllowedBuildGamePathExtensions.Contains(Path.GetExtension(gamePath.Replace('\\', '/')));
    }


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

        // Penumbra selection changes are now handled by the targeted transient-manifest refresh.
        // Do not live-resolve the full transient set during the player build; that is exactly the
        // expensive path that makes emote/VFX option changes feel sluggish. The manifest path is
        // event-invalidated rather than TTL-based, so accuracy still comes from the mod-setting event.

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
        var candidates = BuildRuntimePapCandidates(selectedSourcesByGamePath);

        if (candidates.Count == 0)
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

        if (TryBuildRuntimePapOverridesFromCachedDecisions(candidates, out var cachedSanitizedOverrides))
        {
            var cachedRuntimeChanged = await _localPapSafetyModService.SyncRuntimeModAsync(collectionState, selectedSourcesByGamePath, cachedSanitizedOverrides, ct).ConfigureAwait(false);
            if (cachedRuntimeChanged)
                await RequestLocalPlayerRedrawForRuntimePapChangeAsync(playerRelatedObject, ct).ConfigureAwait(false);

            return cachedRuntimeChanged;
        }

        var targetIndependentResult = await TryBuildRuntimePapOverridesFromTargetIndependentDecisionsAsync(candidates, ct).ConfigureAwait(false);
        if (targetIndependentResult.Success)
        {
            var targetIndependentRuntimeChanged = await _localPapSafetyModService.SyncRuntimeModAsync(collectionState, selectedSourcesByGamePath, targetIndependentResult.SanitizedOverrides, ct).ConfigureAwait(false);
            if (targetIndependentRuntimeChanged)
                await RequestLocalPlayerRedrawForRuntimePapChangeAsync(playerRelatedObject, ct).ConfigureAwait(false);

            return targetIndependentRuntimeChanged;
        }

        var targetSkeletons = await _dalamudUtil.RunOnFrameworkThread(() => _papSanitisationService.GetTargetSkeletonSnapshots(playerRelatedObject)).ConfigureAwait(false);
        var canRewriteAgainstCurrentSkeleton = targetSkeletons != null && targetSkeletons.Count > 0;
        var currentSkeletonFingerprint = canRewriteAgainstCurrentSkeleton
            ? _papSanitisationService.GetTargetSkeletonFingerprint(targetSkeletons!)
            : string.Empty;

        if (!string.IsNullOrWhiteSpace(currentSkeletonFingerprint))
            _lastKnownPlayerPapSkeletonFingerprint = currentSkeletonFingerprint;

        List<LocalPapSafetyModService.SanitizedPapOverride> sanitizedOverrides = [];

        foreach (var replacement in candidates)
        {
            ct.ThrowIfCancellationRequested();

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

    private static List<FileReplacement> BuildRuntimePapCandidates(IReadOnlyDictionary<string, LocalPapSafetyModService.ManifestPapSource> selectedSourcesByGamePath)
    {
        var output = new List<FileReplacement>();
        if (selectedSourcesByGamePath.Count == 0)
            return output;

        var candidates = selectedSourcesByGamePath.Values
            .Where(s => !string.IsNullOrWhiteSpace(s.ResolvedPath)
                && !string.IsNullOrWhiteSpace(s.Hash)
                && s.GamePaths.Any(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath))
            .GroupBy(s => s.ResolvedPath + "|" + s.Hash, StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.First().ResolvedPath, StringComparer.OrdinalIgnoreCase);

        foreach (var group in candidates)
        {
            var source = group.First();
            var gamePaths = group
                .SelectMany(s => s.GamePaths)
                .Where(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(g => g, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            if (gamePaths.Length == 0)
                continue;

            output.Add(new FileReplacement(gamePaths, source.ResolvedPath)
            {
                Hash = source.Hash,
            });
        }

        return output;
    }

    private bool TryBuildRuntimePapOverridesFromCachedDecisions(IReadOnlyList<FileReplacement> candidates, out LocalPapSafetyModService.SanitizedPapOverride[] sanitizedOverrides)
    {
        sanitizedOverrides = Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>();

        if (candidates.Count == 0 || string.IsNullOrWhiteSpace(_lastKnownPlayerPapSkeletonFingerprint))
            return false;

        var overrides = new List<LocalPapSafetyModService.SanitizedPapOverride>();
        foreach (var replacement in candidates)
        {
            if (!TryGetReusablePapDecision(replacement, _lastKnownPlayerPapSkeletonFingerprint, out var reusedDecision))
                return false;

            if (reusedDecision.Status == PapRewriteStatus.Sanitized
                && TryCreateSanitizedPapOverride(replacement, reusedDecision.EffectivePath, reusedDecision.EffectiveHash, reusedDecision.Reason, out var reusedOverride))
            {
                overrides.Add(reusedOverride);
            }
        }

        sanitizedOverrides = overrides.ToArray();
        return true;
    }

    private async Task<(bool Success, LocalPapSafetyModService.SanitizedPapOverride[] SanitizedOverrides)> TryBuildRuntimePapOverridesFromTargetIndependentDecisionsAsync(IReadOnlyList<FileReplacement> candidates, CancellationToken ct)
    {
        if (candidates.Count == 0)
            return (true, Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>());

        var overrides = new List<LocalPapSafetyModService.SanitizedPapOverride>();
        foreach (var replacement in candidates)
        {
            ct.ThrowIfCancellationRequested();

            if (replacement == null || string.IsNullOrWhiteSpace(replacement.Hash))
                return (false, Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>());

            if (!TryGetReusableTargetIndependentPapDecision(replacement.Hash, out var decision))
            {
                var evaluated = await _papSanitisationService.TryGetTargetIndependentRewriteResultAsync(replacement.Hash, ct).ConfigureAwait(false);
                if (evaluated == null)
                    return (false, Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>());

                _playerPapTargetIndependentDecisionsByHash[replacement.Hash] = evaluated;
                decision = evaluated;
            }

            if (decision.Status == PapRewriteStatus.Sanitized)
            {
                if (TryCreateSanitizedPapOverride(replacement, decision.EffectivePath, decision.EffectiveHash, decision.Reason, out var sanitizedOverride))
                {
                    overrides.Add(sanitizedOverride);
                    continue;
                }

                return (false, Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>());
            }

            if (decision.Status is PapRewriteStatus.OriginalSafe or PapRewriteStatus.OriginalFallback or PapRewriteStatus.Blocked)
                continue;

            return (false, Array.Empty<LocalPapSafetyModService.SanitizedPapOverride>());
        }

        return (true, overrides.ToArray());
    }

    private bool TryGetReusableTargetIndependentPapDecision(string hash, out PapRewriteResult decision)
    {
        decision = default!;
        if (string.IsNullOrWhiteSpace(hash))
            return false;

        if (!_playerPapTargetIndependentDecisionsByHash.TryGetValue(hash, out var cached))
            return false;

        if (!IsReusablePapRewriteResult(cached))
        {
            _playerPapTargetIndependentDecisionsByHash.TryRemove(hash, out _);
            return false;
        }

        decision = cached;
        return true;
    }

    private static bool IsReusablePapRewriteResult(PapRewriteResult result)
    {
        return result.Status switch
        {
            PapRewriteStatus.Blocked => true,
            PapRewriteStatus.OriginalFallback => true,
            PapRewriteStatus.OriginalSafe => !string.IsNullOrWhiteSpace(result.EffectivePath) && File.Exists(result.EffectivePath),
            PapRewriteStatus.Sanitized => !string.IsNullOrWhiteSpace(result.EffectivePath) && File.Exists(result.EffectivePath),
            _ => false,
        };
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

        if (objectKind == ObjectKind.Player && _ipcManager.Penumbra.APIAvailable)
            return await ResolvePlayerTransientReplacementsForBuildAsync(transientPaths, reason, ct).ConfigureAwait(false);

        if (ShouldForceFullLiveTransientResolveForBuild(reason))
        {
            var liveResolved = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
            return MergeKnownTransientResolvedPathFallbacks(objectKind, transientPaths, liveResolved);
        }

        var knownResolved = _transientResourceManager.GetKnownResolvedFilePaths(objectKind, transientPaths, validateExists: false);
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

    private async Task<IReadOnlyDictionary<string, string[]>> ResolvePlayerTransientReplacementsForBuildAsync(HashSet<string> transientPaths, string? reason, CancellationToken ct)
    {
        var liveResolved = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
        var output = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var seenGamePathsByResolvedPath = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in liveResolved)
        {
            var resolvedPath = NormalizeTransientResolvedPathForBuild(kvp.Key);
            if (string.IsNullOrWhiteSpace(resolvedPath)
                || resolvedPath.StartsWith("fileswap|", StringComparison.OrdinalIgnoreCase)
                || !File.Exists(resolvedPath))
            {
                continue;
            }

            var normalizedResolvedAsGamePath = NormalizeTransientGamePathForBuild(resolvedPath);

            foreach (var gamePath in kvp.Value)
            {
                var normalizedGamePath = NormalizeTransientGamePathForBuild(gamePath);
                if (string.IsNullOrWhiteSpace(normalizedGamePath)
                    || string.Equals(normalizedGamePath, normalizedResolvedAsGamePath, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!output.TryGetValue(resolvedPath, out var gamePaths))
                {
                    output[resolvedPath] = gamePaths = [];
                    seenGamePathsByResolvedPath[resolvedPath] = [];
                }

                if (seenGamePathsByResolvedPath[resolvedPath].Add(normalizedGamePath))
                    gamePaths.Add(normalizedGamePath);

                _transientResourceManager.RegisterKnownTransientFilePath(normalizedGamePath, resolvedPath);
            }
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            var activeGamePaths = output
                .SelectMany(kvp => kvp.Value.Select(NormalizeTransientGamePathForBuild))
                .Where(path => !string.IsNullOrWhiteSpace(path))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var skipped = transientPaths
                .Select(NormalizeTransientGamePathForBuild)
                .Count(path => !string.IsNullOrWhiteSpace(path) && !activeGamePaths.Contains(path));

            _logger.LogDebug(
                "Resolved player transient paths against current Penumbra state for reason {reason}; active={active}, skippedCached={skipped}, requested={requested}",
                reason ?? "<none>",
                activeGamePaths.Count,
                skipped,
                transientPaths.Count);
        }

        return output.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray(), StringComparer.OrdinalIgnoreCase).AsReadOnly();
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

        var canonicalOutputKeyByResolvedPath = output.Keys.ToDictionary(static key => key, static key => key, StringComparer.OrdinalIgnoreCase);
        var owningResolvedPathByGamePath = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in output)
        {
            foreach (var gamePath in kvp.Value)
            {
                var normalizedGamePath = NormalizeTransientGamePathForBuild(gamePath);
                if (!string.IsNullOrWhiteSpace(normalizedGamePath) && !owningResolvedPathByGamePath.ContainsKey(normalizedGamePath))
                    owningResolvedPathByGamePath[normalizedGamePath] = kvp.Key;
            }
        }

        var fallbackCount = 0;
        var correctedPathCount = 0;
        var manifestOverrideCount = 0;
        foreach (var kvp in knownResolvedPaths)
        {
            var gamePath = NormalizeTransientGamePathForBuild(kvp.Key);
            var resolvedFilePath = NormalizeTransientResolvedPathForBuild(kvp.Value);

            if (string.IsNullOrWhiteSpace(gamePath)
                || string.IsNullOrWhiteSpace(resolvedFilePath)
                || !File.Exists(resolvedFilePath))
            {
                continue;
            }

            canonicalOutputKeyByResolvedPath.TryGetValue(resolvedFilePath, out var outputKey);
            if (!string.IsNullOrWhiteSpace(outputKey)
                && !string.Equals(outputKey, resolvedFilePath, StringComparison.Ordinal))
            {
                var existingGamePaths = output[outputKey];
                output.Remove(outputKey);
                canonicalOutputKeyByResolvedPath.Remove(outputKey);

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
                    canonicalOutputKeyByResolvedPath[resolvedFilePath] = resolvedFilePath;
                }

                foreach (var existingGamePath in existingGamePaths)
                    owningResolvedPathByGamePath[NormalizeTransientGamePathForBuild(existingGamePath)] = resolvedFilePath;

                outputKey = resolvedFilePath;
                correctedPathCount++;
            }

            if (string.IsNullOrWhiteSpace(outputKey))
            {
                outputKey = resolvedFilePath;
                output[outputKey] = [];
                canonicalOutputKeyByResolvedPath[outputKey] = outputKey;
            }

            if (owningResolvedPathByGamePath.TryGetValue(gamePath, out var otherKey)
                && !string.Equals(otherKey, outputKey, StringComparison.OrdinalIgnoreCase)
                && output.TryGetValue(otherKey, out var otherGamePaths))
            {
                var removed = otherGamePaths.RemoveAll(path => string.Equals(path, gamePath, StringComparison.OrdinalIgnoreCase));
                if (removed > 0)
                    manifestOverrideCount += removed;

                if (otherGamePaths.Count == 0)
                {
                    output.Remove(otherKey);
                    canonicalOutputKeyByResolvedPath.Remove(otherKey);
                }
            }

            var gamePaths = output[outputKey];
            if (!gamePaths.Contains(gamePath, StringComparer.OrdinalIgnoreCase))
            {
                gamePaths.Add(gamePath);
                fallbackCount++;
            }

            owningResolvedPathByGamePath[gamePath] = outputKey;
        }

        if (fallbackCount > 0 || correctedPathCount > 0 || manifestOverrideCount > 0)
        {
            _logger.LogDebug(
                "Applied {count} known transient manifest file path fallback(s), corrected {corrected} resolved path casing issue(s), and overrode {overridden} forward-resolved transient path(s)",
                fallbackCount, correctedPathCount, manifestOverrideCount);
        }

        return output.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Distinct(StringComparer.OrdinalIgnoreCase).ToArray(), StringComparer.OrdinalIgnoreCase).AsReadOnly();
    }

    private HashSet<string> ManageSemiTransientData(ObjectKind objectKind, IReadOnlyCollection<string>? staticResolvedGamePaths, GameObjectHandler? playerRelatedObject = null, IReadOnlyCollection<FileReplacement>? staticReplacementReferences = null)
    {
        var pendingObjectTransients = IsSummonedObjectKind(objectKind)
            ? _transientResourceManager.GetPendingTransientResources(objectKind)
            : new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var liveObservedPlayerTransients = objectKind == ObjectKind.Player
            ? _transientResourceManager.GetPendingTransientResources(ObjectKind.Player)
            : new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        HashSet<string> pathsToResolve = new(StringComparer.OrdinalIgnoreCase);
        foreach (var path in _transientResourceManager.PrepareTransientResourcesForBuild(objectKind))
        {
            if (!string.IsNullOrEmpty(path))
                pathsToResolve.Add(path);
        }

        if (objectKind == ObjectKind.Player)
            pathsToResolve = FilterPlayerTransientsToCurrentHumanRaceFamilies(pathsToResolve, staticResolvedGamePaths, liveObservedPlayerTransients, playerRelatedObject);

        if (IsSummonedObjectKind(objectKind))
            pathsToResolve = FilterSummonedObjectTransientsToCurrentActor(objectKind, pathsToResolve, pendingObjectTransients, staticResolvedGamePaths, staticReplacementReferences);

        return pathsToResolve;
    }

    private HashSet<string> FilterPlayerTransientsToCurrentHumanRaceFamilies(HashSet<string> pathsToResolve, IReadOnlyCollection<string>? staticResolvedGamePaths, HashSet<string> liveObservedPlayerTransients, GameObjectHandler? playerRelatedObject)
    {
        if (pathsToResolve.Count == 0)
            return pathsToResolve;

        var activeHumanCodes = BuildCurrentHumanModelCodeSet(staticResolvedGamePaths, playerRelatedObject);
        if (activeHumanCodes.Count == 0)
            return pathsToResolve;

        var protectedPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (staticResolvedGamePaths != null)
        {
            foreach (var path in staticResolvedGamePaths)
            {
                if (!string.IsNullOrWhiteSpace(path))
                    protectedPaths.Add(path);
            }
        }

        foreach (var path in liveObservedPlayerTransients)
        {
            if (!string.IsNullOrWhiteSpace(path))
                protectedPaths.Add(path);
        }

        var humanFamilies = new Dictionary<string, List<(string Path, string Code)>>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in pathsToResolve)
        {
            if (!TryGetHumanModelPathFamily(path, out var code, out var familyKey))
                continue;

            if (!humanFamilies.TryGetValue(familyKey, out var family))
                humanFamilies[familyKey] = family = [];

            family.Add((path, code));
        }

        if (humanFamilies.Count == 0)
            return pathsToResolve;

        var filtered = new HashSet<string>(pathsToResolve, StringComparer.OrdinalIgnoreCase);
        var removed = 0;
        var collapsedFamilies = 0;

        foreach (var family in humanFamilies.Values)
        {
            var distinctCodes = family.Select(static item => item.Code).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            if (distinctCodes.Length < HumanRaceFamilyCollapseMinimumVariants)
                continue;

            if (IsCompatibleHumanAnimationFamily(family))
            {
                // Animation/transient PAP families can legitimately rely on sibling human race aliases/fallbacks.
                // Do not collapse those game paths to the current race here; just preserve the entries that the
                // normal transient/cache path already collected. This avoids expensive selected-option expansion
                // while keeping locally working idle/emote animations available to receivers.
                continue;
            }

            var keepCodes = new HashSet<string>(activeHumanCodes, StringComparer.OrdinalIgnoreCase);
            var hasActiveVariant = family.Any(item => keepCodes.Contains(item.Code));

            if (!hasActiveVariant)
                continue;

            var familyRemoved = 0;
            foreach (var item in family)
            {
                if (keepCodes.Contains(item.Code) || protectedPaths.Contains(item.Path))
                    continue;

                if (filtered.Remove(item.Path))
                    familyRemoved++;
            }

            if (familyRemoved > 0)
            {
                removed += familyRemoved;
                collapsedFamilies++;
            }
        }

        if (removed > 0)
        {
            _logger.LogDebug(
                "Collapsed {removed}/{total} player transient human race-variant path(s) across {families} family/families for current/compatible model(s) {models}",
                removed,
                pathsToResolve.Count,
                collapsedFamilies,
                string.Join(",", activeHumanCodes.OrderBy(static code => code, StringComparer.OrdinalIgnoreCase)));
        }

        return filtered;
    }

    private static HashSet<string> BuildCurrentHumanModelCodeSet(IReadOnlyCollection<string>? staticResolvedGamePaths, GameObjectHandler? playerRelatedObject)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (playerRelatedObject != null && TryGetExpectedHumanModelCode(playerRelatedObject.RaceId, playerRelatedObject.TribeId, playerRelatedObject.Gender, out var expectedCode))
            result.Add(expectedCode);

        // Static paths are only used as a fallback when we genuinely do not have a player object to read from.
        // When we do have the object, treating every static mod path as an "active race" defeats the stripper
        // and can also hide the c0101-compatible animation fallback case.
        if (result.Count == 0 && staticResolvedGamePaths != null)
        {
            foreach (var path in staticResolvedGamePaths)
            {
                if (TryGetHumanModelCode(path, out var code))
                    result.Add(code);
            }
        }

        return result;
    }

    private static bool TryGetHumanModelPathFamily(string? path, out string code, out string familyKey)
    {
        code = string.Empty;
        familyKey = string.Empty;

        if (string.IsNullOrWhiteSpace(path))
            return false;

        var normalized = path.Replace('\\', '/').Trim();
        var match = HumanModelPathRegex.Match(normalized);
        if (!match.Success)
            return false;

        code = match.Groups[1].Value.ToLowerInvariant();
        familyKey = normalized.Remove(match.Groups[1].Index, match.Groups[1].Length).Insert(match.Groups[1].Index, "c####");
        return true;
    }

    private static bool TryGetHumanModelCode(string? path, out string code)
    {
        code = string.Empty;
        return TryGetHumanModelPathFamily(path, out code, out _);
    }

    private static bool IsCompatibleHumanAnimationFamily(IEnumerable<(string Path, string Code)> family)
    {
        return family.Any(static item => IsCompatibleHumanAnimationPath(item.Path));
    }

    private static bool IsCompatibleHumanAnimationPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var normalized = path.Replace('\\', '/').Trim();
        return normalized.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            && normalized.Contains("/animation/", StringComparison.OrdinalIgnoreCase)
            && normalized.EndsWith(".pap", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetExpectedHumanModelCode(byte raceId, byte tribeId, byte gender, out string code)
    {
        code = string.Empty;
        var isFemale = gender == 1;

        code = raceId switch
        {
            1 => tribeId <= 2 ? (isFemale ? "c0201" : "c0101") : (isFemale ? "c0401" : "c0301"),
            2 => isFemale ? "c0601" : "c0501",
            3 => isFemale ? "c0801" : "c0701",
            4 => isFemale ? "c1001" : "c0901",
            5 => isFemale ? "c1201" : "c1101",
            6 => isFemale ? "c1401" : "c1301",
            7 => isFemale ? "c1801" : "c1501",
            8 => isFemale ? "c1601" : "c1701",
            _ => string.Empty,
        };

        return !string.IsNullOrWhiteSpace(code);
    }

    private HashSet<string> FilterSummonedObjectTransientsToCurrentActor(ObjectKind objectKind, HashSet<string> pathsToResolve, HashSet<string> pendingObjectTransients, IReadOnlyCollection<string>? staticResolvedGamePaths, IReadOnlyCollection<FileReplacement>? staticReplacementReferences)
    {
        if (pathsToResolve.Count == 0)
            return pathsToResolve;

        var activeScopes = BuildSummonedObjectPathScopes(staticResolvedGamePaths);
        var activeResolvedRoots = BuildActiveSummonedObjectResolvedRoots(staticReplacementReferences);
        var scopedPendingObjectTransients = FilterPendingSummonedObjectTransientsToActiveScopes(pendingObjectTransients, activeScopes);

        if (activeScopes.Count == 0 && activeResolvedRoots.Count == 0)
        {
            _logger.LogTrace(
                "Suppressing {count} persisted {objectKind} transient path(s) because the current summoned actor has no provable top-priority active scope/root",
                pathsToResolve.Count,
                objectKind);
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        if (activeResolvedRoots.Count == 0)
        {
            if (scopedPendingObjectTransients.Count == 0)
            {
                _logger.LogTrace(
                    "Suppressing {count} persisted {objectKind} transient path(s) because only the active actor scope was known and no pending transient matched it",
                    pathsToResolve.Count,
                    objectKind);
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            _logger.LogDebug(
                "Using {count}/{pendingTotal} pending {objectKind} transient path(s) matching the current summoned actor scope only; no active resolved root was provable",
                scopedPendingObjectTransients.Count,
                pendingObjectTransients.Count,
                objectKind);
            return scopedPendingObjectTransients;
        }

        var knownResolvedPaths = _transientResourceManager.GetKnownResolvedFilePaths(objectKind, pathsToResolve, validateExists: false);
        var filtered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in pathsToResolve)
        {
            if (pendingObjectTransients.Contains(path))
            {
                if (activeScopes.Count == 0 || scopedPendingObjectTransients.Contains(path))
                    filtered.Add(path);
                continue;
            }

            if (!knownResolvedPaths.TryGetValue(path, out var resolvedPath))
                continue;

            if (!ResolvedPathBelongsToActiveRoot(resolvedPath, activeResolvedRoots))
                continue;

            if (activeScopes.Count == 0
                || BelongsToActiveSummonedObjectScope(path, activeScopes)
                || ResolvedPathContainsActiveSummonedObjectScope(resolvedPath, activeScopes))
            {
                filtered.Add(path);
            }
        }

        var removed = pathsToResolve.Count - filtered.Count;
        if (removed > 0)
        {
            _logger.LogDebug(
                "Filtered {removed}/{total} persisted {objectKind} transient path(s) out of this build because they were not from the current top-priority summoned actor source",
                removed,
                pathsToResolve.Count,
                objectKind);
        }

        return filtered;
    }

    private static HashSet<string> FilterPendingSummonedObjectTransientsToActiveScopes(HashSet<string> pendingObjectTransients, HashSet<string> activeScopes)
    {
        if (pendingObjectTransients.Count == 0 || activeScopes.Count == 0)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var filtered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in pendingObjectTransients)
        {
            if (BelongsToActiveSummonedObjectScope(path, activeScopes))
                filtered.Add(path);
        }

        return filtered;
    }

    private static HashSet<string> BuildActiveSummonedObjectResolvedRoots(IReadOnlyCollection<FileReplacement>? staticReplacementReferences)
    {
        var roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (staticReplacementReferences == null)
            return roots;

        foreach (var replacement in staticReplacementReferences)
        {
            if (replacement == null || !replacement.HasFileReplacement)
                continue;

            foreach (var gamePath in replacement.GamePaths)
            {
                if (TryBuildResolvedGamePathRoot(replacement.ResolvedPath, gamePath, out var root))
                    roots.Add(root);
            }
        }

        return roots;
    }

    private static bool TryBuildResolvedGamePathRoot(string? resolvedPath, string? gamePath, out string root)
    {
        root = string.Empty;

        var normalizedResolved = NormalizeTransientGamePathForBuild(resolvedPath);
        var normalizedGamePath = NormalizeTransientGamePathForBuild(gamePath);
        if (string.IsNullOrWhiteSpace(normalizedResolved) || string.IsNullOrWhiteSpace(normalizedGamePath))
            return false;

        if (!normalizedResolved.EndsWith(normalizedGamePath, StringComparison.OrdinalIgnoreCase))
            return false;

        root = normalizedResolved[..^normalizedGamePath.Length];
        return !string.IsNullOrWhiteSpace(root);
    }

    private static bool ResolvedPathBelongsToActiveRoot(string? resolvedPath, HashSet<string> activeResolvedRoots)
    {
        var normalizedResolved = NormalizeTransientGamePathForBuild(resolvedPath);
        if (string.IsNullOrWhiteSpace(normalizedResolved) || activeResolvedRoots.Count == 0)
            return false;

        return activeResolvedRoots.Any(root => normalizedResolved.StartsWith(root, StringComparison.OrdinalIgnoreCase));
    }

    private static bool ResolvedPathContainsActiveSummonedObjectScope(string? resolvedPath, HashSet<string> activeScopes)
    {
        var normalizedResolved = NormalizeTransientGamePathForBuild(resolvedPath);
        if (string.IsNullOrWhiteSpace(normalizedResolved) || activeScopes.Count == 0)
            return false;

        foreach (var scope in activeScopes)
        {
            var normalizedScope = NormalizeTransientGamePathForBuild(scope).TrimStart('/');
            if (string.IsNullOrWhiteSpace(normalizedScope))
                continue;

            if (normalizedResolved.Contains("/" + normalizedScope, StringComparison.OrdinalIgnoreCase)
                || normalizedResolved.StartsWith(normalizedScope, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
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
