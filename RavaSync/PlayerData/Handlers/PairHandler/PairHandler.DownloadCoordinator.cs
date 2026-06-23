using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private sealed class DownloadCoordinator : CoordinatorBase
    {
        private const int PairUploadWaitPollMs = 100;
        private const int PairUploadWaitLogMs = 1000;
        private const int PairUploadWaitUiRefreshMs = 250;
        private const string PairUploadWaitStatusKey = "__pair_upload_wait";
        private const int PairDownloadAttemptLimit = 3;

        public DownloadCoordinator(PairHandler owner) : base(owner)
        {
        }

        public async Task<PairSyncCommitResult> DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
        {
                var initialBlock = GetPairSyncExecutionBlockReason("prepare/download start");
                if (initialBlock != null)
                    return initialBlock;

                var prepareResult = await PrepareRequiredFilesForPairSyncAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, assetPlan, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);

                    if (prepareResult.Failure != null)
                        return prepareResult.Failure;

                    if (!updateModdedPaths
                        && (Pair.LastAppliedApproximateVRAMBytes < 0 || Pair.LastAppliedDataTris < 0)
                        && !await _playerPerformanceService.CheckBothThresholds(Owner, charaData).ConfigureAwait(false))
                    {
                        return PairSyncCommitResult.ThresholdBlocked("performance thresholds blocked application before commit");
                    }

                    downloadToken.ThrowIfCancellationRequested();

                    var waitFailure = await WaitForApplicationCommitSlotAsync(applicationBase, downloadToken).ConfigureAwait(false);
                    if (waitFailure != null)
                        return waitFailure;

                var commitStart = await StartPairSyncApplicationCommitAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, prepareResult.ModdedPaths, prepareResult.AssetPlan, prepareResult.DownloadedAny, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);
                return commitStart;
            }

            private async Task<(PairSyncCommitResult? Failure, Dictionary<(string GamePath, string? Hash), string> ModdedPaths, PairSyncAssetPlan AssetPlan, bool DownloadedAny)> PrepareRequiredFilesForPairSyncAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
            {
                var moddedPaths = assetPlan.ModdedPaths.Count > 0
                    ? new Dictionary<(string GamePath, string? Hash), string>(assetPlan.ModdedPaths)
                    : [];

                var downloadedAny = false;
                FileDownloadManager? activeDownloadManager = null;

                if (!requiresFileReadyGate)
                    return (null, moddedPaths, PairSyncAssetPlan.Empty, downloadedAny);

                List<FileReplacementData> RecalculateMissing()
                {
                    var recalculated = _modPathResolver.Calculate(applicationBase, charaData, out moddedPaths, downloadToken, Pair.EffectiveScreenShakeEnabled);
                    return DropSessionFailedMissingFiles(recalculated, applicationBase, "recalculate");
                }

                var toDownloadReplacements = assetPlan.HasAssets || assetPlan.HasMissingFiles
                    ? DropSessionFailedMissingFiles(assetPlan.MissingFiles.ToList(), applicationBase, "initial asset plan")
                    : RecalculateMissing();
                var previousMissingCount = toDownloadReplacements.Count;
                var appearancePreviewApplied = false;

                for (var attempts = 1; attempts <= PairDownloadAttemptLimit && toDownloadReplacements.Count > 0 && !downloadToken.IsCancellationRequested; attempts++)
                {
                    var block = GetPairSyncExecutionBlockReason($"download attempt {attempts}");
                    if (block != null)
                        return (block, moddedPaths, assetPlan, downloadedAny);

                    if (_pairDownloadTask != null && !_pairDownloadTask.IsCompleted)
                    {
                        try
                        {
                            await _pairDownloadTask.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            ClearPairSyncDownloadStatus();
                            return (PairSyncCommitResult.Cancelled("prior download task was cancelled"), moddedPaths, assetPlan, downloadedAny);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "[BASE-{appBase}] Prior download task failed before preparing next sync for {player}",
                                applicationBase, PlayerName);
                            _downloadManager?.ClearDownload();
                            _pairDownloadTask = null;
                            ClearPairSyncDownloadStatus();
                            return (PairSyncCommitResult.DownloadFailed("prior download task failed", retryImmediately: true), moddedPaths, assetPlan, downloadedAny);
                        }
                    }

                    toDownloadReplacements = PairApplyUtilities.DeduplicateReplacementsByHash(toDownloadReplacements);

                    // Download the pair's currently-applicable payload as one unit.
                    // The old appearance-first staging could apply one small payload, then publish/apply
                    // one or two follow-up payloads a moment later, which made single-pair room entry feel
                    // like the character was loading in pieces.
                    var replacementsThisAttempt = toDownloadReplacements;
                    var appearanceFirstStage = false;

                    var uploadWaitBlock = await WaitForPairUploadsToFinishBeforeDownloadAsync(applicationBase, replacementsThisAttempt, charaData.DataHash.Value, downloadToken).ConfigureAwait(false);
                    if (uploadWaitBlock != null)
                        return (uploadWaitBlock, moddedPaths, assetPlan, downloadedAny);

                    SyncStorm.RegisterDownloadQueued();
                    var downloadLaneQueued = true;
                    var downloadLane = SyncStorm.IsActive ? StormDownloadSemaphore : NormalDownloadSemaphore;
                    var downloadLaneAcquired = false;

                    try
                    {
                        await downloadLane.WaitAsync(downloadToken).ConfigureAwait(false);
                        downloadLaneAcquired = true;
                        downloadLaneQueued = false;
                        SyncStorm.RegisterDownloadStarted();

                        var queueBlock = GetPairSyncExecutionBlockReason($"download lane acquired attempt {attempts}");
                        if (queueBlock != null)
                            return (queueBlock, moddedPaths, assetPlan, downloadedAny);

                        Mediator.Publish(new EventMessage(new Event(
                            PlayerName,
                            Pair.UserData,
                            nameof(PairHandler),
                            EventSeverity.Informational,
                            appearanceFirstStage
                                ? $"Starting appearance-first download for {replacementsThisAttempt.Count}/{toDownloadReplacements.Count} files (attempt {attempts}/{PairDownloadAttemptLimit})"
                                : $"Starting download for {toDownloadReplacements.Count} files (attempt {attempts}/{PairDownloadAttemptLimit})")));

                        var downloadManager = EnsureDownloadManager();
                        activeDownloadManager = downloadManager;
                        downloadManager.ConfigureFileRepairContext(Pair.UserData.UID, Pair.Ident, charaData.DataHash.Value);

                        var toDownloadFiles = await downloadManager
                            .InitiateDownloadList(_charaHandler!, replacementsThisAttempt, downloadToken)
                            .ConfigureAwait(false);

                        if (toDownloadFiles != null && toDownloadFiles.Count > 0)
                            downloadedAny = true;

                        if (toDownloadFiles == null || toDownloadFiles.Count == 0)
                        {
                            var recalculatedMissing = RecalculateMissing();
                            recalculatedMissing = DropCentralRepairFailedMissingFiles(activeDownloadManager, recalculatedMissing, applicationBase);

                            if (recalculatedMissing.Count == 0)
                            {
                                _downloadManager?.ClearDownload();
                                _pairDownloadTask = null;
                                ClearPairSyncDownloadStatus();
                                break;
                            }

                            _downloadManager?.ClearDownload();
                            _pairDownloadTask = null;

                            if (downloadLaneAcquired)
                            {
                                downloadLane.Release();
                                downloadLaneAcquired = false;
                                SyncStorm.RegisterDownloadFinished();
                            }

                            if (appearanceFirstStage && !appearancePreviewApplied)
                            {
                                toDownloadReplacements = recalculatedMissing;
                                var appliedPreview = await TryApplyAppearanceFirstPreviewAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, toDownloadReplacements, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);
                                appearancePreviewApplied |= appliedPreview;
                                previousMissingCount = toDownloadReplacements.Count;
                                continue;
                            }

                            var delayMs = Math.Min(4000, 500 * attempts);
                            await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);

                            previousMissingCount = recalculatedMissing.Count;
                            toDownloadReplacements = recalculatedMissing;
                            continue;
                        }

                        if (!_playerPerformanceService
                                .ComputeAndAutoPauseOnVRAMUsageThresholds(Owner, charaData, toDownloadFiles, stableDataHash: charaData.DataHash.Value))
                        {
                            _downloadManager?.ClearDownload();
                            ClearPairSyncDownloadStatus();
                            return (PairSyncCommitResult.ThresholdBlocked("VRAM/data thresholds blocked download/apply"), moddedPaths, assetPlan, downloadedAny);
                        }

                        var downloadBlock = GetPairSyncExecutionBlockReason($"download files attempt {attempts}");
                        if (downloadBlock != null)
                        {
                            _downloadManager?.ClearDownload();
                            ClearPairSyncDownloadStatus();
                            return (downloadBlock, moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, toDownloadReplacements, charaData), downloadedAny);
                        }

                        _pairDownloadTask = downloadManager.DownloadFiles(_charaHandler!, replacementsThisAttempt, downloadToken);

                        try
                        {
                            await _pairDownloadTask.ConfigureAwait(false);
                            ClearPairSyncDownloadStatus();
                        }
                        catch (OperationCanceledException)
                        {
                            ClearPairSyncDownloadStatus();
                            return (PairSyncCommitResult.Cancelled("download task was cancelled"), moddedPaths, assetPlan, downloadedAny);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "[BASE-{appBase}] Download task failed on attempt {attempt}/{maxAttempts} for {player}",
                                applicationBase, attempts, PairDownloadAttemptLimit, PlayerName);

                            ClearPairSyncDownloadStatus();

                            _downloadManager?.ClearDownload();
                            _pairDownloadTask = null;

                            if (downloadLaneAcquired)
                            {
                                downloadLane.Release();
                                downloadLaneAcquired = false;
                                SyncStorm.RegisterDownloadFinished();
                            }

                            var delayMs = Math.Min(15000, 2000 * attempts);
                            await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);

                            toDownloadReplacements = RecalculateMissing();
                            toDownloadReplacements = DropCentralRepairFailedMissingFiles(activeDownloadManager, toDownloadReplacements, applicationBase);
                            previousMissingCount = toDownloadReplacements.Count;
                            continue;
                        }

                        downloadToken.ThrowIfCancellationRequested();

                        toDownloadReplacements = RecalculateMissing();
                        toDownloadReplacements = DropCentralRepairFailedMissingFiles(activeDownloadManager, toDownloadReplacements, applicationBase);

                        if (appearanceFirstStage && !appearancePreviewApplied)
                        {
                            if (downloadLaneAcquired)
                            {
                                downloadLane.Release();
                                downloadLaneAcquired = false;
                                SyncStorm.RegisterDownloadFinished();
                            }

                            var appliedPreview = await TryApplyAppearanceFirstPreviewAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, toDownloadReplacements, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);
                            appearancePreviewApplied |= appliedPreview;
                            previousMissingCount = toDownloadReplacements.Count;
                            continue;
                        }

                        if (toDownloadReplacements.Count > 0 && toDownloadReplacements.Count == previousMissingCount && Logger.IsEnabled(LogLevel.Debug))
                        {
                            Logger.LogDebug("[BASE-{appBase}] Missing count unchanged after download attempt {attempt}/{maxAttempts} for {player}: still {count}",
                                applicationBase, attempts, PairDownloadAttemptLimit, PlayerName, toDownloadReplacements.Count);
                        }

                        previousMissingCount = toDownloadReplacements.Count;
                    }
                    finally
                    {
                        if (downloadLaneQueued)
                            SyncStorm.RegisterDownloadQueueCancelled();

                        if (downloadLaneAcquired)
                        {
                            downloadLane.Release();
                            SyncStorm.RegisterDownloadFinished();
                        }
                    }
                }

                if (downloadToken.IsCancellationRequested)
                    return (PairSyncCommitResult.Cancelled("download token was cancelled"), moddedPaths, assetPlan, downloadedAny);

                if (toDownloadReplacements.Count > 0)
                {
                    var exhaustedHashes = toDownloadReplacements
                        .Select(static f => f.Hash)
                        .Where(static h => !string.IsNullOrWhiteSpace(h))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    var repairRequests = (activeDownloadManager ?? _downloadManager)?.RequestCentralFileRepair(exhaustedHashes, "download attempts exhausted") ?? 0;

                    Logger.LogWarning(
                        "[BASE-{appBase}] Download attempts exhausted for {player}; keeping {count} unresolved hash(es) retryable instead of session-skipping them; repairRequests={repairRequests}: {hashes}",
                        applicationBase,
                        PlayerName,
                        exhaustedHashes.Count,
                        repairRequests,
                        string.Join(", ", exhaustedHashes.Take(20)));
                }

                toDownloadReplacements = DropSessionFailedMissingFiles(toDownloadReplacements, applicationBase, "post-exhaustion");
                toDownloadReplacements = DropCentralRepairFailedMissingFiles(activeDownloadManager ?? _downloadManager, toDownloadReplacements, applicationBase);

                if (toDownloadReplacements.Count > 0)
                {
                    Mediator.Publish(new EventMessage(new Event(
                        PlayerName,
                        Pair.UserData,
                        nameof(PairHandler),
                        EventSeverity.Warning,
                        appearancePreviewApplied
                            ? $"RavaSync: {toDownloadReplacements.Count} non-appearance file(s) could not be downloaded; keeping the appearance-first preview pending retry."
                            : $"RavaSync: {toDownloadReplacements.Count} files could not be downloaded; not applying partial appearance.")));

                    var retryImmediately = lifecycleRedrawRequestedFromPlan || !_hasRetriedAfterMissingDownload;
                    if (!_hasRetriedAfterMissingDownload)
                    {
                        _hasRetriedAfterMissingDownload = true;

                        Logger.LogInformation(
                            "[BASE-{appBase}] Self-heal: requesting sync retry for {name} after missing-files detected",
                            applicationBase,
                            PlayerName);
                    }
                    else if (lifecycleRedrawRequestedFromPlan)
                    {
                        Logger.LogInformation(
                            "[BASE-{appBase}] Lifecycle replay for {name} is still waiting for {count} required files; keeping replay pending instead of applying partial vanilla state",
                            applicationBase,
                            PlayerName,
                            toDownloadReplacements.Count);
                    }

                    if (lifecycleRedrawRequestedFromPlan)
                        MarkInitialApplyRequired();

                    ScheduleMissingFileSelfHealRetry(charaData, toDownloadReplacements.Select(static r => r.Hash).Where(static h => !string.IsNullOrWhiteSpace(h)).ToList(), immediate: false);

                    return (PairSyncCommitResult.MissingFiles(
                        $"{toDownloadReplacements.Count} required files remained missing after download attempts",
                        retryImmediately), moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, toDownloadReplacements, charaData), downloadedAny);
                }

            if (!await _playerPerformanceService.CheckBothThresholds(Owner, charaData).ConfigureAwait(false))
                return (PairSyncCommitResult.ThresholdBlocked("performance thresholds blocked application after downloads"), moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, toDownloadReplacements, charaData), downloadedAny);

            return (null, moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, [], charaData), downloadedAny);
        }

        private List<FileReplacementData> DropSessionFailedMissingFiles(List<FileReplacementData> missingFiles, Guid applicationBase, string phase)
        {
            if (missingFiles.Count == 0)
                return missingFiles;

            var kept = new List<FileReplacementData>(missingFiles.Count);
            var dropped = new List<string>();

            foreach (var missing in missingFiles)
            {
                var hash = missing.Hash;
                if (!string.IsNullOrWhiteSpace(hash) && FileDownloadManager.HasSessionDownloadFailedHash(hash))
                {
                    dropped.Add(hash);
                    continue;
                }

                kept.Add(missing);
            }

            if (dropped.Count == 0)
                return missingFiles;

            Logger.LogDebug(
                "[BASE-{appBase}] Skipping {count} hash(es) already exhausted earlier this session during {phase} for {player}: {hashes}",
                applicationBase,
                dropped.Distinct(StringComparer.OrdinalIgnoreCase).Count(),
                phase,
                PlayerName,
                string.Join(", ", dropped.Distinct(StringComparer.OrdinalIgnoreCase).Take(20)));

            return kept;
        }

        private List<FileReplacementData> DropCentralRepairFailedMissingFiles(FileDownloadManager? downloadManager, List<FileReplacementData> missingFiles, Guid applicationBase)
        {
            if (downloadManager == null || missingFiles.Count == 0)
                return missingFiles;

            var exhausted = missingFiles
                .Select(static f => f.Hash)
                .Where(h => !string.IsNullOrWhiteSpace(h) && downloadManager.HasCentralFileRepairFailedHash(h))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (exhausted.Count > 0)
            {
                Logger.LogWarning(
                    "[BASE-{appBase}] Central B2 repair is exhausted for {count} file(s) for {player}; keeping them in the missing set so partial appearance is not applied: {hashes}",
                    applicationBase,
                    exhausted.Count,
                    PlayerName,
                    string.Join(", ", exhausted.Take(20)));
            }

            return missingFiles;
        }

        private async Task<bool> TryApplyAppearanceFirstPreviewAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, Dictionary<(string GamePath, string? Hash), string> moddedPaths, List<FileReplacementData> stillMissing, bool forceApplyModsForThisApply, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
        {
            if (moddedPaths.Count == 0)
                return false;

            var previewPlan = BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, stillMissing, charaData);
            if (!previewPlan.Entries.Any(IsAppearanceFirstAssetEntry))
                return false;

            var waitFailure = await WaitForApplicationCommitSlotAsync(applicationBase, downloadToken).ConfigureAwait(false);
            if (waitFailure != null)
            {
                Logger.LogTrace("[BASE-{appBase}] Appearance-first preview apply for {player} skipped: {reason}", applicationBase, PlayerName, waitFailure.Reason);
                return false;
            }

            Logger.LogDebug("[BASE-{appBase}] Applying appearance-first preview for {player}: {readyFiles} ready file(s), {missingFiles} remaining", applicationBase, PlayerName, previewPlan.Entries.Count, stillMissing.Count);

            await PaceRoomEntryCommitHandoffAsync(applicationBase, true, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);

            var previewLifecycleRedraw = lifecycleRedrawRequestedFromPlan || lifecycleApplyRequestedFromPlan;

            var previewResult = await ApplyCharacterDataAsync(
                applicationBase,
                charaData,
                updatedData,
                updateModdedPaths,
                updateManip,
                requiresFileReadyGate,
                moddedPaths,
                previewPlan,
                true,
                forceApplyModsForThisApply,
                lifecycleApplyRequestedFromPlan,
                previewLifecycleRedraw,
                downloadToken,
                authoritativeCommit: false,
                suppressNonLifecycleRedraw: !previewLifecycleRedraw).ConfigureAwait(false);

            if (!previewResult.Success)
            {
                Logger.LogTrace("[BASE-{appBase}] Appearance-first preview apply for {player} did not commit: {reason}", applicationBase, PlayerName, previewResult.Reason);
                return false;
            }

            return true;
        }

        private static List<FileReplacementData> SelectAppearanceFirstReplacements(List<FileReplacementData> replacements)
        {
            // Disabled deliberately: the appearance-first preview can only be safe if every
            // appearance-critical piece is already present before the preview apply happens
            // (face/ears/tail/skeleton/gear plus Glamourer/Customize+/Honorific/manipulations).
            // If even one critical file or metadata payload is still pending, the receiver can
            // briefly show broken actors with missing body parts.  Until that can be proven
            // end-to-end, download the full required set first and only then apply.
            return [];
        }

        private static bool IsAppearanceFirstReplacement(FileReplacementData replacement)
        {
            if (replacement.GamePaths == null)
                return false;

            foreach (var gamePath in replacement.GamePaths)
            {
                if (IsAppearanceFirstGamePath(gamePath))
                    return true;
            }

            return false;
        }

        private static bool IsAppearanceFirstAssetEntry(PairSyncAssetEntry entry)
            => entry.Criticality == PairSyncAssetCriticality.AppearanceCritical
               || entry.Kind is PairSyncAssetKind.Skeleton or PairSyncAssetKind.Physics
               || IsAppearanceFirstGamePath(entry.GamePath);

        private static bool IsAppearanceFirstGamePath(string? gamePath)
        {
            if (string.IsNullOrWhiteSpace(gamePath))
                return false;

            var normalized = gamePath.Replace('\\', '/').Trim();
            var ext = Path.GetExtension(normalized).ToLowerInvariant();

            if (ext is ".mdl" or ".mtrl" or ".tex" or ".sklb" or ".atch" or ".phyb" or ".pbd" or ".imc" or ".eqp" or ".eqdp" or ".est" or ".cmp" or ".gmp")
                return true;

            if (ext == ".shpk" && normalized.StartsWith("chara/", StringComparison.OrdinalIgnoreCase))
                return true;

            if (ext is ".pap" or ".tmb" or ".tmb2" or ".avfx" or ".atex" or ".scd" or ".shpk" or ".eid" or ".skp")
                return false;

            return normalized.Contains("/face/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/hair/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/tail/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/ear/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/body/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/skin/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/attachoffset/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/material/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/equipment/", StringComparison.OrdinalIgnoreCase)
                   || normalized.Contains("/accessory/", StringComparison.OrdinalIgnoreCase);
        }

        private async Task<PairSyncCommitResult?> WaitForPairUploadsToFinishBeforeDownloadAsync(Guid applicationBase, IReadOnlyCollection<FileReplacementData> toDownloadReplacements, string dataHash, CancellationToken downloadToken)
        {
            if (!Pair.IsUploadingRecently)
                return null;

            if (MissingFileMeshService.HasRecentMeshReadyForCharacterData(Pair.UserData?.UID, dataHash))
            {
                ClearPairSyncDownloadStatus();
                Logger.LogDebug(
                    "[BASE-{appBase}] Pair upload flag is active for {player}, but Mesh local-ready was proven for {hash}; continuing download/apply",
                    applicationBase,
                    PlayerName,
                    dataHash);
                return null;
            }

            if (AreRequiredDownloadHashesAvailableLocally(toDownloadReplacements))
            {
                ClearPairSyncDownloadStatus();
                Logger.LogDebug(
                    "[BASE-{appBase}] Pair upload flag is active for {player}, but all required hash(es) are already local; continuing apply",
                    applicationBase,
                    PlayerName);
                return null;
            }

            var startedTick = Environment.TickCount64;
            var lastLogTick = 0L;
            var lastUiTick = 0L;
            var lastLocalAvailabilityCheckTick = 0L;
            var localAvailabilityPollMs = IsWineRuntime ? 500 : PairUploadWaitPollMs;
            var fileCount = Math.Max(1, toDownloadReplacements?.Count ?? 0);

            PublishPairUploadWaitStatus(fileCount);

            Logger.LogDebug(
                "[BASE-{appBase}] Waiting for {player}'s upload to finish before downloading {count} file(s)",
                applicationBase,
                PlayerName,
                fileCount);

            while (Pair.IsUploadingRecently)
            {
                downloadToken.ThrowIfCancellationRequested();

                var block = GetPairSyncExecutionBlockReason("waiting for pair upload to finish before download");
                if (block != null)
                {
                    ClearPairSyncDownloadStatus();
                    return block;
                }

                var nowTick = Environment.TickCount64;
                if (unchecked(nowTick - lastLocalAvailabilityCheckTick) >= localAvailabilityPollMs
                    && AreRequiredDownloadHashesAvailableLocally(toDownloadReplacements))
                {
                    lastLocalAvailabilityCheckTick = nowTick;
                    ClearPairSyncDownloadStatus();
                    Logger.LogDebug(
                        "[BASE-{appBase}] Pair upload flag is still active for {player}, but all required hash(es) became local after {elapsed}ms; continuing apply",
                        applicationBase,
                        PlayerName,
                        unchecked(nowTick - startedTick));
                    return null;
                }

                if (MissingFileMeshService.HasRecentMeshReadyForCharacterData(Pair.UserData?.UID, dataHash))
                {
                    ClearPairSyncDownloadStatus();
                    Logger.LogDebug(
                        "[BASE-{appBase}] Pair upload flag is still active for {player}, but Mesh local-ready was proven after {elapsed}ms for {hash}; continuing download/apply",
                        applicationBase,
                        PlayerName,
                        unchecked(Environment.TickCount64 - startedTick),
                        dataHash);
                    return null;
                }


                if (unchecked(nowTick - lastLogTick) >= PairUploadWaitLogMs)
                {
                    lastLogTick = nowTick;
                    Logger.LogTrace(
                        "[BASE-{appBase}] Still waiting for {player}'s upload before downloading; waited {elapsed}ms",
                        applicationBase,
                        PlayerName,
                        unchecked(nowTick - startedTick));
                }

                if (unchecked(nowTick - lastUiTick) >= PairUploadWaitUiRefreshMs)
                {
                    lastUiTick = nowTick;
                    PublishPairUploadWaitStatus(fileCount);
                }

                await Task.Delay(PairUploadWaitPollMs, downloadToken).ConfigureAwait(false);
            }

            var postWaitBlock = GetPairSyncExecutionBlockReason("pair upload finished before download");
            if (postWaitBlock != null)
            {
                ClearPairSyncDownloadStatus();
                return postWaitBlock;
            }

            Logger.LogDebug(
                "[BASE-{appBase}] Pair upload finished for {player}; starting download after {elapsed}ms wait",
                applicationBase,
                PlayerName,
                unchecked(Environment.TickCount64 - startedTick));

            return null;
        }

        private bool AreRequiredDownloadHashesAvailableLocally(IReadOnlyCollection<FileReplacementData>? replacements)
        {
            if (replacements == null || replacements.Count == 0)
                return false;

            var checkedHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var sawHash = false;

            foreach (var replacement in replacements)
            {
                var hash = replacement?.Hash;
                if (string.IsNullOrWhiteSpace(hash) || !checkedHashes.Add(hash))
                    continue;

                sawHash = true;
                var cache = _fileDbManager.GetFileCacheByHash(hash);
                if (cache == null || string.IsNullOrWhiteSpace(cache.ResolvedFilepath) || !File.Exists(cache.ResolvedFilepath))
                    return false;
            }

            return sawHash;
        }

        private void PublishPairUploadWaitStatus(int fileCount)
        {
            var totalFiles = Math.Max(1, fileCount);

            Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.Downloading);
            Pair.SetCurrentDownloadSummary(new Pair.DownloadProgressSummary(
                HasAny: true,
                AnyDownloading: true,
                AnyLoading: false,
                TotalBytes: 0,
                TransferredBytes: 0,
                TotalFiles: totalFiles,
                TransferredFiles: 0));

            Pair.SetCurrentDownloadStatus(new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal)
            {
                [PairUploadWaitStatusKey] = new()
                {
                    DownloadStatus = DownloadStatus.WaitingForQueue,
                    TotalFiles = totalFiles,
                    TransferredFiles = 0,
                    TotalBytes = 0,
                    TransferredBytes = 0,
                }
            });

            ScheduleRefreshUi();
        }

        private async Task<PairSyncCommitResult?> WaitForApplicationCommitSlotAsync(Guid applicationBase, CancellationToken downloadToken)
        {
                CancellationToken? appToken;
                try
                {
                    appToken = _applicationCancellationTokenSource?.Token;
                }
                catch (ObjectDisposedException)
                {
                    return PairSyncCommitResult.Cancelled("application cancellation token source was disposed");
                }

                var lastWaitLog = 0L;

                while ((!_applicationTask?.IsCompleted ?? false)
                       && !downloadToken.IsCancellationRequested
                       && (appToken == null || !appToken.Value.IsCancellationRequested))
                {
                    var block = GetPairSyncExecutionBlockReason("waiting for application slot");
                    if (block != null)
                        return block;

                    var now = Environment.TickCount64;
                    if (now - lastWaitLog >= 1000)
                    {
                        lastWaitLog = now;
                        Logger.LogTrace("[BASE-{appBase}] Waiting for current pair application to finish for {player}",
                            applicationBase, PlayerName);
                    }

                    var pollDelayMs = IsWineRuntime
                        ? LinuxSmoothMode.ComputeMaintenancePollDelay(30, 100)
                        : 15;
                    await Task.Delay(pollDelayMs, downloadToken).ConfigureAwait(false);

                    try
                    {
                        appToken = _applicationCancellationTokenSource?.Token;
                    }
                    catch (ObjectDisposedException)
                    {
                        return PairSyncCommitResult.Cancelled("application cancellation token source was disposed while waiting");
                    }
                }

                if (downloadToken.IsCancellationRequested)
                    return PairSyncCommitResult.Cancelled("download token cancelled while waiting for application slot");

                if (appToken?.IsCancellationRequested ?? false)
                    return PairSyncCommitResult.Cancelled("application token cancelled while waiting for slot");

                return null;
            }

        private async Task PaceRoomEntryCommitHandoffAsync(Guid applicationBase, bool downloadedAny, bool lifecycleApplyRequestedFromPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken token)
        {
            var visibleLifecycleEntry = lifecycleApplyRequestedFromPlan || lifecycleRedrawRequestedFromPlan || IsRecentlyVisibleLifecycleReplay();
            if (!downloadedAny && !visibleLifecycleEntry)
                return;

            var jitterSeed = Math.Abs(PlayerNameHash?.GetHashCode(StringComparison.Ordinal) ?? applicationBase.GetHashCode());
            var smoothDelayMs = downloadedAny
                ? (SyncStorm.IsActive ? 25 + (jitterSeed % 35) : 8 + (jitterSeed % 15))
                : 0;

            if (visibleLifecycleEntry)
            {
                var backlog = SyncStorm.TotalBacklog;
                int lifecycleDelayMs;

                if (SyncStorm.IsActive && backlog >= 24)
                    lifecycleDelayMs = 180 + (jitterSeed % 220);
                else if (SyncStorm.IsActive && backlog >= 12)
                    lifecycleDelayMs = 120 + (jitterSeed % 140);
                else if (SyncStorm.IsActive)
                    lifecycleDelayMs = 65 + (jitterSeed % 90);
                else
                    lifecycleDelayMs = 18 + (jitterSeed % 28);

                if (IsWineRuntime)
                {
                    if (SyncStorm.IsActive && backlog >= 24)
                        lifecycleDelayMs += 120;
                    else if (SyncStorm.IsActive && backlog >= 12)
                        lifecycleDelayMs += 80;
                    else
                        lifecycleDelayMs += SyncStorm.IsActive ? 45 : 12;
                }

                smoothDelayMs = Math.Max(smoothDelayMs, lifecycleDelayMs);
            }

            if (smoothDelayMs > 0)
                await Task.Delay(smoothDelayMs, token).ConfigureAwait(false);
        }

        private async Task<PairSyncCommitResult> StartPairSyncApplicationCommitAsync(
            Guid applicationBase,
            CharacterData charaData,
            Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData,
            bool updateModdedPaths,
            bool updateManip,
            bool requiresFileReadyGate,
            Dictionary<(string GamePath, string? Hash), string> moddedPaths,
            PairSyncAssetPlan assetPlan,
            bool downloadedAny,
            bool forceApplyModsForThisApply,
            bool lifecycleApplyRequestedFromPlan,
            bool lifecycleRedrawRequestedFromPlan,
            CancellationToken downloadToken)
        {
                downloadToken.ThrowIfCancellationRequested();

                await PaceRoomEntryCommitHandoffAsync(applicationBase, downloadedAny, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);
                downloadToken.ThrowIfCancellationRequested();

                var block = GetPairSyncExecutionBlockReason("application commit handoff");
                if (block != null)
                    return block;

                if (_dalamudUtil.IsInGpose || _dalamudUtil.IsInCutscene || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
                    return PairSyncCommitResult.PluginsUnavailable("GPose/cutscene or Penumbra/Glamourer unavailable before commit handoff");

                downloadToken.ThrowIfCancellationRequested();

                var newCts = new CancellationTokenSource();
                var oldCts = Interlocked.Exchange(ref Owner._applicationCancellationTokenSource, newCts);
                oldCts?.CancelDispose();

                CancellationToken token;
                try
                {
                    token = newCts.Token;
                }
                catch (ObjectDisposedException)
                {
                    return PairSyncCommitResult.Cancelled("new application token was disposed before commit handoff");
                }

                var applicationTask = ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, assetPlan, downloadedAny, forceApplyModsForThisApply, lifecycleApplyRequestedFromPlan, lifecycleRedrawRequestedFromPlan, token);
                _pairSyncApplicationTask = applicationTask;
                _applicationTask = applicationTask;

                _ = applicationTask.ContinueWith(t =>
                {
                    try
                    {
                        _ = t.Exception;
                        Logger.LogWarning(t.Exception, "[BASE-{appBase}] Application task faulted for {player}", applicationBase, PlayerName);
                    }
                    catch
                    {
                    }
                }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);

                return PairSyncCommitResult.StartedCommit();
            }

            private PairSyncCommitResult? GetPairSyncExecutionBlockReason(string phase)
            {
                if (_lifetime.ApplicationStopping.IsCancellationRequested)
                    return PairSyncCommitResult.Cancelled($"application is stopping during {phase}");

                if (!IsVisible)
                    return PairSyncCommitResult.Hidden($"pair is not visible during {phase}");

                if (Pair.IsPaused)
                    return PairSyncCommitResult.Paused($"pair is paused during {phase}");

                if (Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
                    return PairSyncCommitResult.YieldedToOtherSync($"yielded to OtherSync during {phase}");

                if (_charaHandler == null || _charaHandler.Address == nint.Zero)
                    return PairSyncCommitResult.Hidden($"no visible actor handler during {phase}");

                var strictAddress = ResolveStrictVisiblePlayerAddress(_charaHandler.Address);
                if (strictAddress == nint.Zero || !IsExpectedPlayerAddress(strictAddress))
                    return PairSyncCommitResult.ActorChanged($"visible actor became unstable during {phase}", retryImmediately: false);

                return null;
            }

        private void ClearPairSyncDownloadStatus()
        {
            Pair.SetCurrentDownloadStatus(null);
            Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
            Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
        }

    }
}
