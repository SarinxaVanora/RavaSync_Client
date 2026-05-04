using System.Text;
using System.Text.RegularExpressions;
using FFXIVClientStructs.FFXIV.Client.Game.Character;
using Microsoft.Extensions.Logging;
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
    private readonly XivDataAnalyzer _modelAnalyzer;
    private readonly PapSanitisationService _papSanitisationService;
    private readonly LocalPapSafetyModService _localPapSafetyModService;
    private readonly MareMediator _mareMediator;
    private readonly TransientResourceManager _transientResourceManager;

    private sealed record PapSessionDecision(string OverrideKey, string OriginalHash, string SkeletonFingerprint, PapRewriteStatus Status, string EffectiveHash, string EffectivePath, DateTimeOffset UpdatedUtc, string Reason);

    private readonly ConcurrentDictionary<string, PapSessionDecision> _playerPapDecisionsByKey = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string[]> _initialCrawlSupportFilesByRoot = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, IReadOnlyDictionary<string, string[]>> _initialCrawlSupportReverseResolveCache = new(StringComparer.Ordinal);
    private const int ReverseResolveBatchSize = 64;
    private const int SupportSweepDirectoryPauseStride = 24;
    private const int SupportSweepFilePauseStride = 128;
    private static readonly TimeSpan SupportSweepBreatherDelay = TimeSpan.FromMilliseconds(2);

    private const int EmbeddedDependencyResolveDepth = 3;
    private const long MaxEmbeddedDependencyScanBytes = 8L * 1024L * 1024L;
    
    private readonly ConcurrentDictionary<string, EmbeddedDependencyScanCacheEntry> _embeddedDependencyScanCache = new(StringComparer.OrdinalIgnoreCase);
    private sealed record EmbeddedDependencyScanCacheEntry(long Length, DateTime LastWriteUtc, HashSet<string> GamePaths);


    private static readonly Regex EmbeddedDependencyPathRegex = new(
        @"(?i)\b(?:chara|vfx|bgcommon|sound|ui|shader|common)/(?:[a-z0-9_\-./]+)\.(?:mdl|mtrl|tex|atex|avfx|pap|tmb2?|eid|skp|shpk|scd)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

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
        _modelAnalyzer = modelAnalyzer;
        _papSanitisationService = papSanitizationService;
        _localPapSafetyModService = localPapSafetyModService;
        _mareMediator = mareMediator;

        _mareMediator.Subscribe<PenumbraFileCacheChangedMessage>(this, _ => InvalidatePapSessionCaches());
        _mareMediator.Subscribe<DisconnectedMessage>(this, _ => InvalidatePapSessionCaches());
        _mareMediator.Subscribe<DalamudLogoutMessage>(this, _ => InvalidatePapSessionCaches());

        _logger.LogTrace("Creating {this}", nameof(PlayerDataFactory));
    }

    public MareMediator Mediator => _mareMediator;

    private void InvalidatePapSessionCaches()
    {
        ClearInitialCrawlSupportSweepCache();
        _initialCrawlSupportReverseResolveCache.Clear();
        _embeddedDependencyScanCache.Clear();
        _playerPapDecisionsByKey.Clear();
    }

    public async Task<CharacterDataFragment?> BuildCharacterData(GameObjectHandler playerRelatedObject, CancellationToken token)
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

        DateTime start = DateTime.UtcNow;

        // penumbra call, it's currently broken
        Dictionary<string, HashSet<string>>? resolvedPaths;

        resolvedPaths = (await _ipcManager.Penumbra.GetCharacterData(_logger, playerRelatedObject).ConfigureAwait(false));
        if (resolvedPaths == null) throw new InvalidOperationException("Penumbra returned null data");

        ct.ThrowIfCancellationRequested();

        fragment.FileReplacements =
                new HashSet<FileReplacement>(resolvedPaths.Select(c => new FileReplacement([.. c.Value], c.Key)), FileReplacementComparer.Instance)
                .Where(p => p.HasFileReplacement).ToHashSet(FileReplacementComparer.Instance);
        fragment.FileReplacements.RemoveWhere(c => c.GamePaths.Any(g => !CacheMonitor.AllowedFileExtensions.Any(e => g.EndsWith(e, StringComparison.OrdinalIgnoreCase))));

        ct.ThrowIfCancellationRequested();

        if (objectKind == ObjectKind.Player)
        {
            await AddSelectedAnimationSupportReplacementsAsync(fragment, ct).ConfigureAwait(false);
        }

        ct.ThrowIfCancellationRequested();

        if (objectKind != ObjectKind.Pet)
        {
            await AddEmbeddedPenumbraSubResourceReplacementsAsync(fragment, ct).ConfigureAwait(false);
            await AddInitialCrawlSupportSweepReplacementsAsync(
                fragment,
                fragment.FileReplacements.ToArray(),
                objectKind,
                persistAsTransientHints: false,
                source: "static",
                ct: ct).ConfigureAwait(false);
        }

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

        if (objectKind == ObjectKind.Player)
        {
            await _transientResourceManager.EnsureManifestPrimeAsync("PlayerDataFactory.CreateCharacterData", ct).ConfigureAwait(false);
        }

        _logger.LogDebug("Handling transient update for {obj}", playerRelatedObject);

        // remove all potentially gathered paths from the transient resource manager that are resolved through static resolving
        _transientResourceManager.ClearTransientPaths(objectKind, fragment.FileReplacements.SelectMany(c => c.GamePaths).ToList());

        // get all remaining paths and resolve them
        var transientPaths = ManageSemiTransientData(objectKind);
        var resolvedTransientPaths = await GetFileReplacementsFromPaths(transientPaths, new HashSet<string>(StringComparer.Ordinal), ct).ConfigureAwait(false);
        var transientReplacements = resolvedTransientPaths.Select(c => new FileReplacement([.. c.Value], c.Key)).ToList();
        await AddInitialCrawlSupportSweepReplacementsAsync(
            fragment,
            transientReplacements.ToArray(),
            objectKind,
            persistAsTransientHints: true,
            source: "transient",
            ct: ct,
            appendTarget: transientReplacements).ConfigureAwait(false);
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

        _logger.LogInformation("Building character data for {obj} took {time}ms", objectKind, TimeSpan.FromTicks(DateTime.UtcNow.Ticks - start.Ticks).TotalMilliseconds);

        return fragment;
    }

    private async Task AddSelectedAnimationSupportReplacementsAsync(CharacterDataFragment fragment, CancellationToken ct)
    {
        if (!fragment.FileReplacements.Any(r => r.GamePaths.Any(XivSkeletonIdentity.IsHumanPlayerAnimationPapGamePath)))
            return;

        var collectionState = await _localPapSafetyModService.TryGetLocalPlayerCollectionSettingsAsync(ct).ConfigureAwait(false);
        if (collectionState == null)
            return;

        var selectedSupportFiles = await _localPapSafetyModService.ResolveSelectedAnimationSupportFilesAsync(collectionState, ct).ConfigureAwait(false);
        if (selectedSupportFiles.Count == 0)
            return;

        foreach (var support in selectedSupportFiles.OrderBy(s => s.GamePaths.FirstOrDefault() ?? s.ResolvedPath, StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            UpsertSelectedAnimationSupportReplacement(fragment, support);
        }
    }

    private void UpsertSelectedAnimationSupportReplacement(CharacterDataFragment fragment, LocalPapSafetyModService.ManifestSupportSource support)
    {
        if (string.IsNullOrWhiteSpace(support.ResolvedPath) || !File.Exists(support.ResolvedPath))
            return;

        var supportGamePaths = support.GamePaths
            .Where(g => !string.IsNullOrWhiteSpace(g))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (supportGamePaths.Length == 0)
            return;

        var supportGamePathSet = supportGamePaths.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var conflictingExistingReplacements = fragment.FileReplacements
            .Where(existing => existing.GamePaths.Any(supportGamePathSet.Contains))
            .ToArray();

        foreach (var existing in conflictingExistingReplacements)
        {
            fragment.FileReplacements.Remove(existing);

            var remainingGamePaths = existing.GamePaths
                .Where(g => !supportGamePathSet.Contains(g))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            if (remainingGamePaths.Length > 0)
            {
                fragment.FileReplacements.Add(new FileReplacement(remainingGamePaths, existing.ResolvedPath)
                {
                    Hash = existing.Hash,
                });
            }
        }

        if (fragment.FileReplacements.Add(new FileReplacement(supportGamePaths, support.ResolvedPath)))
        {
            _logger.LogDebug("Upserted selected animation support replacement {path} for [{gamePaths}]",
                support.ResolvedPath,
                string.Join(", ", supportGamePaths));
        }
    }

    public async Task RefreshLocalPlayerConvertedAnimationPackAsync(GameObjectHandler playerRelatedObject, CancellationToken ct)
    {
        if (playerRelatedObject == null
            || playerRelatedObject.ObjectKind != ObjectKind.Player
            || playerRelatedObject.Address == IntPtr.Zero
            || !_ipcManager.Initialized)
        {
            return;
        }

        var collectionState = await _localPapSafetyModService.TryGetLocalPlayerCollectionSettingsAsync(ct).ConfigureAwait(false);
        if (collectionState == null)
            return;

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

            return;
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

    private async Task AddEmbeddedPenumbraSubResourceReplacementsAsync(CharacterDataFragment fragment, CancellationToken ct)
    {
        if (fragment.FileReplacements.Count == 0)
            return;

        var knownGamePaths = new HashSet<string>(
            fragment.FileReplacements.SelectMany(r => r.GamePaths),
            StringComparer.OrdinalIgnoreCase);

        var frontier = fragment.FileReplacements
            .Where(IsEmbeddedDependencyContainerReplacement)
            .ToArray();

        for (int depth = 0; depth < EmbeddedDependencyResolveDepth && frontier.Length > 0; depth++)
        {
            ct.ThrowIfCancellationRequested();

            var dependencyGamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var replacement in frontier)
            {
                ct.ThrowIfCancellationRequested();

                foreach (var dependencyGamePath in await ExtractEmbeddedDependencyGamePathsAsync(replacement.ResolvedPath, ct).ConfigureAwait(false))
                {
                    if (knownGamePaths.Contains(dependencyGamePath))
                        continue;

                    dependencyGamePaths.Add(dependencyGamePath);
                }
            }

            if (dependencyGamePaths.Count == 0)
                break;

            var resolvedDependencies = await GetFileReplacementsFromPaths(
                dependencyGamePaths,
                new HashSet<string>(StringComparer.Ordinal),
                ct).ConfigureAwait(false);

            var nextFrontier = new List<FileReplacement>();

            foreach (var kvp in resolvedDependencies)
            {
                ct.ThrowIfCancellationRequested();

                var resolvedPath = kvp.Key;
                if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                    continue;

                var gamePaths = kvp.Value
                    .Select(NormalizeEmbeddedDependencyGamePath)
                    .Where(IsEmbeddedDependencyGamePath)
                    .Where(g => !knownGamePaths.Contains(g))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                if (gamePaths.Length == 0)
                    continue;

                var replacement = new FileReplacement(gamePaths, resolvedPath);
                if (!replacement.HasFileReplacement || replacement.IsFileSwap)
                    continue;

                foreach (var gamePath in replacement.GamePaths)
                    knownGamePaths.Add(gamePath);

                if (!fragment.FileReplacements.Add(replacement))
                    continue;

                _logger.LogDebug(
                    "Added embedded Penumbra sub-resource dependency {resolvedPath} for {gamePaths}",
                    replacement.ResolvedPath,
                    string.Join(", ", replacement.GamePaths));

                if (IsEmbeddedDependencyContainerReplacement(replacement))
                    nextFrontier.Add(replacement);
            }

            var unresolved = dependencyGamePaths
                .Where(g => !knownGamePaths.Contains(g))
                .ToArray();

            foreach (var gamePath in unresolved)
            {
                _logger.LogTrace(
                    "Embedded Penumbra sub-resource {gamePath} was referenced by a resolved file but did not forward-resolve through Penumbra",
                    gamePath);
            }

            frontier = nextFrontier.ToArray();
        }
    }

    private async Task<HashSet<string>> ExtractEmbeddedDependencyGamePathsAsync(string resolvedPath, CancellationToken ct)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(resolvedPath) || !IsEmbeddedDependencyContainerPath(resolvedPath) || !File.Exists(resolvedPath))
            return output;

        try
        {
            var info = new FileInfo(resolvedPath);
            if (info.Length <= 0 || info.Length > MaxEmbeddedDependencyScanBytes)
                return output;

            var cacheKey = resolvedPath.Replace('\\', '/').ToLowerInvariant();
            var lastWriteUtc = info.LastWriteTimeUtc;

            if (_embeddedDependencyScanCache.TryGetValue(cacheKey, out var cached)
                && cached.Length == info.Length
                && cached.LastWriteUtc == lastWriteUtc)
            {
                return new HashSet<string>(cached.GamePaths, StringComparer.OrdinalIgnoreCase);
            }

            var bytes = await File.ReadAllBytesAsync(resolvedPath, ct).ConfigureAwait(false);
            var text = Encoding.UTF8.GetString(bytes);

            foreach (Match match in EmbeddedDependencyPathRegex.Matches(text))
            {
                ct.ThrowIfCancellationRequested();

                var gamePath = NormalizeEmbeddedDependencyGamePath(match.Value);
                if (IsEmbeddedDependencyGamePath(gamePath))
                    output.Add(gamePath);
            }

            _embeddedDependencyScanCache[cacheKey] = new EmbeddedDependencyScanCacheEntry(
                info.Length,
                lastWriteUtc,
                new HashSet<string>(output, StringComparer.OrdinalIgnoreCase));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Best-effort dependency discovery only. Do not break character data creation.
        }

        return output;
    }

    private static bool IsEmbeddedDependencyContainerReplacement(FileReplacement replacement)
    {
        if (replacement == null || replacement.IsFileSwap || string.IsNullOrWhiteSpace(replacement.ResolvedPath))
            return false;

        return IsEmbeddedDependencyContainerPath(replacement.ResolvedPath);
    }

    private static bool IsEmbeddedDependencyContainerPath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        return 
            path.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".skp", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeEmbeddedDependencyGamePath(string path)
    {
        var normalized = (path ?? string.Empty)
            .Replace('\\', '/')
            .Trim()
            .Trim('\0')
            .ToLowerInvariant();

        while (normalized.StartsWith("./", StringComparison.Ordinal))
            normalized = normalized[2..];

        return normalized;
    }

    private static bool IsEmbeddedDependencyGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        var path = NormalizeEmbeddedDependencyGamePath(gamePath);

        var hasAllowedRoot = path.StartsWith("chara/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("bgcommon/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("ui/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("shader/", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("common/", StringComparison.OrdinalIgnoreCase);

        if (!hasAllowedRoot)
            return false;

        return path.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".skp", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".scd", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<IReadOnlyDictionary<string, string[]>> GetFileReplacementsFromPaths(HashSet<string> forwardResolve, HashSet<string> reverseResolve, CancellationToken ct = default)
    {
        var forwardPaths = forwardResolve.ToArray();
        var reversePaths = reverseResolve.ToArray();

        Dictionary<string, List<string>> resolvedPaths = new(StringComparer.OrdinalIgnoreCase);

        static void MergeForwardResolved(Dictionary<string, List<string>> target, string[] requestedGamePaths, string[] resolvedFilePaths)
        {
            for (int i = 0; i < requestedGamePaths.Length; i++)
            {
                var filePath = resolvedFilePaths[i];
                if (string.IsNullOrEmpty(filePath))
                    continue;

                filePath = filePath.ToLowerInvariant();
                var gamePath = requestedGamePaths[i].ToLowerInvariant();

                if (!target.TryGetValue(filePath, out var list))
                    target[filePath] = list = new List<string>(1);

                list.Add(gamePath);
            }
        }

        static void MergeReverseResolved(Dictionary<string, List<string>> target, string[] requestedFilePaths, string[][] resolvedGamePaths)
        {
            for (int i = 0; i < requestedFilePaths.Length; i++)
            {
                var filePath = requestedFilePaths[i];
                if (string.IsNullOrEmpty(filePath))
                    continue;

                filePath = filePath.ToLowerInvariant();

                if (!target.TryGetValue(filePath, out var list))
                    target[filePath] = list = new List<string>(resolvedGamePaths[i].Length);

                foreach (var gamePath in resolvedGamePaths[i])
                {
                    if (!string.IsNullOrEmpty(gamePath))
                        list.Add(gamePath.ToLowerInvariant());
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
                var batch = reversePaths.Skip(i).Take(ReverseResolveBatchSize).ToArray();
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

    private async Task<IReadOnlyDictionary<string, string[]>> GetOrResolveInitialCrawlSupportSweepPathsAsync(HashSet<string> reverseSweepPaths, string source, ObjectKind objectKind, CancellationToken ct)
    {
        var cacheKey = CreateInitialCrawlSupportReverseResolveCacheKey(reverseSweepPaths);

        if (_initialCrawlSupportReverseResolveCache.TryGetValue(cacheKey, out var cached))
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace("Reusing cached reverse-resolved {count} {source} support files for {obj}", reverseSweepPaths.Count, source, objectKind);

            return cached;
        }

        _logger.LogDebug("Reverse-resolving {count} {source} support files for {obj}", reverseSweepPaths.Count, source, objectKind);

        var resolved = await GetFileReplacementsFromPaths(
            new HashSet<string>(StringComparer.Ordinal),
            reverseSweepPaths,
            ct).ConfigureAwait(false);

        if (_initialCrawlSupportReverseResolveCache.Count > 32)
            _initialCrawlSupportReverseResolveCache.Clear();

        _initialCrawlSupportReverseResolveCache[cacheKey] = resolved;
        return resolved;
    }

    private static string CreateInitialCrawlSupportReverseResolveCacheKey(HashSet<string> reverseSweepPaths)
    {
        if (reverseSweepPaths.Count == 0)
            return "0";

        return reverseSweepPaths.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + "|"
            + string.Join("|", reverseSweepPaths.OrderBy(p => p, StringComparer.OrdinalIgnoreCase));
    }

    private async Task AddInitialCrawlSupportSweepReplacementsAsync(CharacterDataFragment fragment, IEnumerable<FileReplacement> seedReplacements, ObjectKind objectKind, bool persistAsTransientHints, string source, CancellationToken ct, List<FileReplacement>? appendTarget = null)
    {
        var seeds = seedReplacements
            .Where(IsInitialCrawlSupportSweepCandidate)
            .ToArray();

        if (seeds.Length == 0)
            return;

        var reverseSweepPaths = await CollectInitialCrawlSupportSweepPathsAsync(seeds, ct).ConfigureAwait(false);
        if (reverseSweepPaths.Count == 0)
            return;

        var sweptSupportPaths = await GetOrResolveInitialCrawlSupportSweepPathsAsync(reverseSweepPaths, source, objectKind, ct).ConfigureAwait(false);

        var sweptSupportMap = new Dictionary<string, string[]>(sweptSupportPaths, StringComparer.OrdinalIgnoreCase);
        int synthesizedSupportPaths = 0;

        foreach (var filePath in reverseSweepPaths)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                continue;

            if (sweptSupportMap.TryGetValue(filePath, out var resolvedGamePaths)
                && resolvedGamePaths.Any(g => !string.IsNullOrWhiteSpace(g) && IsInitialCrawlSupportGamePath(g)))
            {
                continue;
            }

            if (!TryDeriveMirroredGamePathFromResolvedFile(filePath, out var mirroredGamePath))
                continue;

            if (!IsInitialCrawlSupportGamePath(mirroredGamePath))
                continue;

            sweptSupportMap[filePath] = [mirroredGamePath];
            synthesizedSupportPaths++;
        }

        if (synthesizedSupportPaths > 0)
        {
            _logger.LogDebug("Synthesized {count} mirrored {source} support game paths for {obj}", synthesizedSupportPaths, source, objectKind);
        }

        bool addedTransientHints = false;
        int addedReplacements = 0;

        foreach (var kvp in sweptSupportMap)
        {
            var filteredGamePaths = kvp.Value
                .Where(g => !string.IsNullOrWhiteSpace(g) && IsInitialCrawlSupportGamePath(g))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            if (filteredGamePaths.Length == 0)
                continue;

            if (persistAsTransientHints)
            {
                foreach (var gamePath in filteredGamePaths)
                {
                    if (_transientResourceManager.AddTransientResource(objectKind, gamePath))
                        addedTransientHints = true;
                }
            }

            var replacement = new FileReplacement(filteredGamePaths, kvp.Key);
            if (appendTarget != null)
                appendTarget.Add(replacement);
            else
                fragment.FileReplacements.Add(replacement);

            addedReplacements++;
        }

        if (addedTransientHints)
        {
            _logger.LogDebug("Persisting {source} support transient hints for {obj}", source, objectKind);
            _transientResourceManager.PersistTransientResources(objectKind);
        }

        if (addedReplacements > 0)
        {
            _logger.LogDebug("Added {count} {source} support replacements for {obj}", addedReplacements, source, objectKind);
        }
    }

    private async Task<HashSet<string>> CollectInitialCrawlSupportSweepPathsAsync(IEnumerable<FileReplacement> replacements, CancellationToken ct)
    {
        var output = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visitedRoots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var replacement in replacements)
        {
            ct.ThrowIfCancellationRequested();

            if (!IsInitialCrawlSupportSweepCandidate(replacement))
                continue;

            var resolvedPath = replacement.ResolvedPath;
            if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
                continue;

            foreach (var root in GetInitialCrawlSupportSweepRoots(replacement))
            {
                ct.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
                    continue;

                var normalizedRoot = root.Replace('\\', '/').ToLowerInvariant();
                if (!visitedRoots.Add(normalizedRoot))
                    continue;

                foreach (var file in await GetOrEnumerateInitialCrawlSupportFilesForRootAsync(root, normalizedRoot, ct).ConfigureAwait(false))
                {
                    output.Add(file);
                }
            }
        }

        return output;
    }

    private async Task<string[]> GetOrEnumerateInitialCrawlSupportFilesForRootAsync(string root, string normalizedRoot, CancellationToken ct)
    {
        if (_initialCrawlSupportFilesByRoot.TryGetValue(normalizedRoot, out var cachedFiles))
            return cachedFiles;

        var enumeratedFiles = await EnumerateInitialCrawlSupportFilesAsync(root, ct).ConfigureAwait(false);
        _initialCrawlSupportFilesByRoot[normalizedRoot] = enumeratedFiles;
        return enumeratedFiles;
    }

    private void ClearInitialCrawlSupportSweepCache()
    {
        _initialCrawlSupportFilesByRoot.Clear();
    }

    private static async Task<string[]> EnumerateInitialCrawlSupportFilesAsync(string root, CancellationToken ct)
    {
        Stack<string> pendingDirectories = new();
        pendingDirectories.Push(root);
        List<string> files = [];
        int visitedDirectoryCount = 0;
        int collectedFileCount = 0;

        while (pendingDirectories.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            var currentDirectory = pendingDirectories.Pop();
            visitedDirectoryCount++;

            IEnumerable<string> subDirectories;
            try
            {
                subDirectories = Directory.EnumerateDirectories(currentDirectory);
            }
            catch (Exception)
            {
                continue;
            }

            foreach (var directory in subDirectories)
            {
                pendingDirectories.Push(directory);
            }

            foreach (var file in EnumerateInitialCrawlSupportFilesInDirectory(currentDirectory))
            {
                ct.ThrowIfCancellationRequested();
                files.Add(file);
                collectedFileCount++;

                if ((collectedFileCount % SupportSweepFilePauseStride) == 0)
                {
                    await Task.Delay(SupportSweepBreatherDelay, ct).ConfigureAwait(false);
                }
            }

            if ((visitedDirectoryCount % SupportSweepDirectoryPauseStride) == 0)
            {
                await Task.Delay(SupportSweepBreatherDelay, ct).ConfigureAwait(false);
            }
        }

        return files.ToArray();
    }

    private static IEnumerable<string> EnumerateInitialCrawlSupportFilesInDirectory(string directory)
    {
        foreach (var pattern in InitialCrawlSupportFilePatterns)
        {
            IEnumerable<string> files;
            try
            {
                files = Directory.EnumerateFiles(directory, pattern, SearchOption.TopDirectoryOnly);
            }
            catch (Exception)
            {
                continue;
            }

            foreach (var file in files)
            {
                yield return file.Replace('\\', '/').ToLowerInvariant();
            }
        }
    }

    private static bool IsInitialCrawlSupportSweepCandidate(FileReplacement replacement)
    {
        if (replacement == null || replacement.IsFileSwap || string.IsNullOrWhiteSpace(replacement.ResolvedPath))
            return false;

        foreach (var gamePath in replacement.GamePaths)
        {
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            if (IsInitialCrawlSupportSweepGamePath(gamePath))
                return true;
        }

        return false;
    }

    private static bool IsInitialCrawlSupportSweepGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return gamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || gamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryDeriveMirroredGamePathFromResolvedFile(string filePath, out string gamePath)
    {
        gamePath = string.Empty;
        if (string.IsNullOrWhiteSpace(filePath))
            return false;

        var normalized = filePath.Replace('\\', '/');

        static int LastMarkerIndex(string value, string marker)
            => value.LastIndexOf(marker, StringComparison.OrdinalIgnoreCase);

        var idx = LastMarkerIndex(normalized, "/bgcommon/vfx/");
        if (idx < 0)
            idx = LastMarkerIndex(normalized, "/chara/");
        if (idx < 0)
            idx = LastMarkerIndex(normalized, "/vfx/");
        if (idx < 0)
            idx = LastMarkerIndex(normalized, "/sound/");
        if (idx < 0)
            idx = LastMarkerIndex(normalized, "/ui/");
        if (idx < 0)
            idx = LastMarkerIndex(normalized, "/shader/");

        if (idx < 0)
            return false;

        gamePath = normalized[(idx + 1)..].ToLowerInvariant();
        return !string.IsNullOrWhiteSpace(gamePath);
    }

    private static bool IsInitialCrawlSupportGamePath(string gamePath)
    {
        if (string.IsNullOrWhiteSpace(gamePath))
            return false;

        return gamePath.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".phy", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".skp", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
            || gamePath.EndsWith(".scd", StringComparison.OrdinalIgnoreCase);
    }

    private static readonly string[] InitialCrawlSupportFilePatterns =
    [
        "*.mdl",
        "*.mtrl",
        "*.tex",
        "*.atex",
        "*.avfx",
        "*.pap",
        "*.tmb",
        "*.tmb2",
        "*.eid",
        "*.sklb",
        "*.phy",
        "*.phyb",
        "*.pbd",
        "*.skp",
        "*.shpk",
        "*.scd",
    ];

    private static IEnumerable<string> GetInitialCrawlSupportSweepRoots(FileReplacement replacement)
    {
        var resolvedPath = replacement.ResolvedPath;
        if (string.IsNullOrWhiteSpace(resolvedPath) || !File.Exists(resolvedPath))
            yield break;

        var normalizedResolvedPath = resolvedPath.Replace('\\', '/').ToLowerInvariant();
        var immediateDirectory = Path.GetDirectoryName(resolvedPath);
        if (!string.IsNullOrWhiteSpace(immediateDirectory))
            yield return immediateDirectory;

        foreach (var gamePath in replacement.GamePaths)
        {
            if (string.IsNullOrWhiteSpace(gamePath))
                continue;

            var normalizedGamePath = gamePath.Replace('\\', '/').ToLowerInvariant();
            string? root = TryGetInitialCrawlEntityRoot(normalizedResolvedPath, normalizedGamePath);
            if (!string.IsNullOrWhiteSpace(root))
                yield return root;

            foreach (var soundRoot in GetInitialCrawlSoundSupportRoots(normalizedResolvedPath, normalizedGamePath))
                yield return soundRoot;
        }
    }

    private static IEnumerable<string> GetInitialCrawlSoundSupportRoots(string normalizedResolvedPath, string normalizedGamePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedResolvedPath) || string.IsNullOrWhiteSpace(normalizedGamePath))
            yield break;

        if (!ShouldScanInitialCrawlSoundSupportFolder(normalizedGamePath))
            yield break;

        var modRoot = TryGetInitialCrawlResolvedModRoot(normalizedResolvedPath);
        if (string.IsNullOrWhiteSpace(modRoot))
            yield break;

        var soundRoot = Path.Combine(modRoot, "sound");
        if (Directory.Exists(soundRoot))
            yield return soundRoot;
    }

    private static bool ShouldScanInitialCrawlSoundSupportFolder(string normalizedGamePath)
    {
        if (string.IsNullOrWhiteSpace(normalizedGamePath))
            return false;

        return normalizedGamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/equipment/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/accessory/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/weapon/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/human/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/minion/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/monster/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/demihuman/", StringComparison.OrdinalIgnoreCase);
    }

    private static string? TryGetInitialCrawlResolvedModRoot(string normalizedResolvedPath)
    {
        if (string.IsNullOrWhiteSpace(normalizedResolvedPath))
            return null;

        string[] markers =
        [
            "/chara/",
            "/vfx/",
            "/bgcommon/",
            "/sound/",
            "/ui/",
            "/shader/",
            "/common/",
        ];

        var bestIdx = -1;
        foreach (var marker in markers)
        {
            var idx = normalizedResolvedPath.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
                continue;

            if (bestIdx < 0 || idx < bestIdx)
                bestIdx = idx;
        }

        if (bestIdx <= 0)
            return null;

        var root = normalizedResolvedPath[..bestIdx].Replace('/', Path.DirectorySeparatorChar);
        return Directory.Exists(root) ? root : null;
    }

    private static string? TryGetInitialCrawlEntityRoot(string normalizedResolvedPath, string normalizedGamePath)
    {
        static IEnumerable<string> EnumerateEntityRootCandidates(string gamePath, string prefix, int fallbackSegmentCount)
        {
            if (!gamePath.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                yield break;

            var parts = gamePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < fallbackSegmentCount)
                yield break;

            for (int i = 0; i <= parts.Length - 3; i++)
            {
                if (!parts[i].Equals("obj", StringComparison.OrdinalIgnoreCase))
                    continue;

                var scopeType = parts[i + 1];
                if (!scopeType.Equals("body", StringComparison.OrdinalIgnoreCase)
                    && !scopeType.Equals("face", StringComparison.OrdinalIgnoreCase)
                    && !scopeType.Equals("tail", StringComparison.OrdinalIgnoreCase)
                    && !scopeType.Equals("met", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                yield return string.Join('/', parts.Take(i + 3));
                break;
            }

            yield return string.Join('/', parts.Take(fallbackSegmentCount));
        }

        static string? MatchEntityRoot(string resolvedPath, string gamePath, string prefix, int fallbackSegmentCount)
        {
            foreach (var entityRoot in EnumerateEntityRootCandidates(gamePath, prefix, fallbackSegmentCount)
                .Distinct(StringComparer.OrdinalIgnoreCase))
            {
                var marker = "/" + entityRoot + "/";
                var idx = resolvedPath.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
                if (idx < 0)
                    continue;

                return resolvedPath[..(idx + marker.Length - 1)].Replace('/', Path.DirectorySeparatorChar);
            }

            return null;
        }

        if (normalizedGamePath.StartsWith("vfx/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("bgcommon/vfx/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("sound/", StringComparison.OrdinalIgnoreCase)
            || normalizedGamePath.StartsWith("chara/action/", StringComparison.OrdinalIgnoreCase))
        {
            return Path.GetDirectoryName(normalizedResolvedPath.Replace('/', Path.DirectorySeparatorChar));
        }

        return MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/equipment/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/accessory/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/weapon/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/human/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/minion/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/monster/", 3)
            ?? MatchEntityRoot(normalizedResolvedPath, normalizedGamePath, "chara/demihuman/", 3);
    }

    private static bool IsInitialCrawlSupportFile(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        return path.EndsWith(".mdl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".mtrl", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".atex", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".avfx", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".pap", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".tmb2", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".eid", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".sklb", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".phy", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".phyb", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".pbd", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".skp", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".shpk", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".scd", StringComparison.OrdinalIgnoreCase);
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
