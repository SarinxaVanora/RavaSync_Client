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
        public DownloadCoordinator(PairHandler owner) : base(owner)
        {
        }

        public async Task<PairSyncCommitResult> DownloadAndApplyCharacterAsync(Guid applicationBase, CharacterData charaData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool updateModdedPaths, bool updateManip, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool forceApplyModsForThisApply, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
        {
                var initialBlock = GetPairSyncExecutionBlockReason("prepare/download start");
                if (initialBlock != null)
                    return initialBlock;

                var prepareResult = await PrepareRequiredFilesForPairSyncAsync(applicationBase, charaData, requiresFileReadyGate, assetPlan, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);

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

                var commitStart = await StartPairSyncApplicationCommitAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, prepareResult.ModdedPaths, prepareResult.AssetPlan, prepareResult.DownloadedAny, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, downloadToken).ConfigureAwait(false);
                return commitStart;
            }

            private async Task<(PairSyncCommitResult? Failure, Dictionary<(string GamePath, string? Hash), string> ModdedPaths, PairSyncAssetPlan AssetPlan, bool DownloadedAny)> PrepareRequiredFilesForPairSyncAsync(Guid applicationBase, CharacterData charaData, bool requiresFileReadyGate, PairSyncAssetPlan assetPlan, bool lifecycleRedrawRequestedFromPlan, CancellationToken downloadToken)
            {
                var moddedPaths = assetPlan.ModdedPaths.Count > 0
                    ? new Dictionary<(string GamePath, string? Hash), string>(assetPlan.ModdedPaths)
                    : [];

                var downloadedAny = false;

                if (!requiresFileReadyGate)
                    return (null, moddedPaths, PairSyncAssetPlan.Empty, downloadedAny);

                List<FileReplacementData> RecalculateMissing()
                    => _modPathResolver.Calculate(applicationBase, charaData, out moddedPaths, downloadToken);

                var toDownloadReplacements = assetPlan.HasAssets || assetPlan.HasMissingFiles
                    ? assetPlan.MissingFiles.ToList()
                    : RecalculateMissing();
                var previousMissingCount = toDownloadReplacements.Count;

                for (var attempts = 1; attempts <= 10 && toDownloadReplacements.Count > 0 && !downloadToken.IsCancellationRequested; attempts++)
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

                    Mediator.Publish(new EventMessage(new Event(
                        PlayerName,
                        Pair.UserData,
                        nameof(PairHandler),
                        EventSeverity.Informational,
                        $"Starting download for {toDownloadReplacements.Count} files (attempt {attempts}/10)")));

                    var downloadManager = EnsureDownloadManager();
                    downloadManager.ConfigureFileRepairContext(Pair.UserData.UID, Pair.Ident, charaData.DataHash.Value);

                    var toDownloadFiles = await downloadManager
                        .InitiateDownloadList(_charaHandler!, toDownloadReplacements, downloadToken)
                        .ConfigureAwait(false);

                    if (toDownloadFiles != null && toDownloadFiles.Count > 0)
                        downloadedAny = true;

                    if (toDownloadFiles == null || toDownloadFiles.Count == 0)
                    {
                        var recalculatedMissing = RecalculateMissing();

                        if (recalculatedMissing.Count == 0)
                            break;

                        _downloadManager?.ClearDownload();
                        _pairDownloadTask = null;

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

                    _pairDownloadTask = downloadManager.DownloadFiles(_charaHandler!, toDownloadReplacements, downloadToken);

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
                        Logger.LogWarning(ex, "[BASE-{appBase}] Download task failed on attempt {attempt}/10 for {player}",
                            applicationBase, attempts, PlayerName);

                        ClearPairSyncDownloadStatus();

                        _downloadManager?.ClearDownload();
                        _pairDownloadTask = null;

                        var delayMs = Math.Min(15000, 2000 * attempts);
                        await Task.Delay(delayMs, downloadToken).ConfigureAwait(false);

                        toDownloadReplacements = RecalculateMissing();
                        previousMissingCount = toDownloadReplacements.Count;
                        continue;
                    }

                    downloadToken.ThrowIfCancellationRequested();

                    toDownloadReplacements = RecalculateMissing();

                    if (toDownloadReplacements.Count > 0 && toDownloadReplacements.Count == previousMissingCount && Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("[BASE-{appBase}] Missing count unchanged after download attempt {attempt}/10 for {player}: still {count}",
                            applicationBase, attempts, PlayerName, toDownloadReplacements.Count);
                    }

                    previousMissingCount = toDownloadReplacements.Count;
                }

                if (downloadToken.IsCancellationRequested)
                    return (PairSyncCommitResult.Cancelled("download token was cancelled"), moddedPaths, assetPlan, downloadedAny);

                if (toDownloadReplacements.Count > 0)
                {
                    Mediator.Publish(new EventMessage(new Event(
                        PlayerName,
                        Pair.UserData,
                        nameof(PairHandler),
                        EventSeverity.Warning,
                        $"RavaSync: {toDownloadReplacements.Count} files could not be downloaded; not applying partial appearance.")));

                    var retryImmediately = lifecycleRedrawRequestedFromPlan || !_hasRetriedAfterMissingDownload;
                    if (!_hasRetriedAfterMissingDownload)
                    {
                        _hasRetriedAfterMissingDownload = true;

                        Logger.LogInformation(
                            "[BASE-{appBase}] Self-heal: requesting worker retry for {name} after missing-files detected",
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

                    return (PairSyncCommitResult.MissingFiles(
                        $"{toDownloadReplacements.Count} required files remained missing after download attempts",
                        retryImmediately), moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, toDownloadReplacements, charaData), downloadedAny);
                }

                if (!await _playerPerformanceService.CheckBothThresholds(Owner, charaData).ConfigureAwait(false))
                    return (PairSyncCommitResult.ThresholdBlocked("performance thresholds blocked application after downloads"), moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, toDownloadReplacements, charaData), downloadedAny);

                return (null, moddedPaths, BuildPairSyncAssetPlanFromResolvedPaths(moddedPaths, [], charaData), downloadedAny);
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

                    await Task.Delay(15, downloadToken).ConfigureAwait(false);

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
            bool lifecycleRedrawRequestedFromPlan,
            CancellationToken downloadToken)
        {
                if (downloadedAny)
                {
                    var jitterSeed = Math.Abs(PlayerNameHash?.GetHashCode(StringComparison.Ordinal) ?? applicationBase.GetHashCode());
                    var smoothDelayMs = SyncStorm.IsActive ? 45 + (jitterSeed % 35) : 15 + (jitterSeed % 20);
                    await Task.Delay(smoothDelayMs, downloadToken).ConfigureAwait(false);
                }

                var block = GetPairSyncExecutionBlockReason("application commit handoff");
                if (block != null)
                    return block;

                if (_dalamudUtil.IsInGpose || _dalamudUtil.IsInCutscene || !_ipcManager.Penumbra.APIAvailable || !_ipcManager.Glamourer.APIAvailable)
                    return PairSyncCommitResult.PluginsUnavailable("GPose/cutscene or Penumbra/Glamourer unavailable before commit handoff");

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

                var applicationTask = ApplyCharacterDataAsync(applicationBase, charaData, updatedData, updateModdedPaths, updateManip, requiresFileReadyGate, moddedPaths, assetPlan, downloadedAny, forceApplyModsForThisApply, lifecycleRedrawRequestedFromPlan, token);
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

                if (_localOtherSyncDecisionYield && !Pair.EffectiveOverrideOtherSync)
                    return PairSyncCommitResult.YieldedToOtherSync($"local OtherSync gate yielded during {phase}");

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
