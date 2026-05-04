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
    public override string ToString()
    {
        var hasChar = (_charaHandler?.Address ?? nint.Zero) != nint.Zero || _lastAssignedPlayerAddress != nint.Zero;

        return Pair == null
            ? base.ToString() ?? string.Empty
            : Pair.UserData.AliasOrUID + ":" + PlayerName + ":" + (hasChar ? "HasChar" : "NoChar");
    }

    private void CancelDownloadCancellationDeferred()
    {
        var oldCts = Interlocked.Exchange(ref _downloadCancellationTokenSource, null);
        if (oldCts == null)
            return;

        _ = Task.Run(() =>
        {
            try
            {
                oldCts.CancelDispose();
            }
            catch (Exception ex)
            {
                Logger.LogTrace(ex, "Deferred download CTS cancellation failed for {pair}", Pair.UserData.AliasOrUID);
            }
        });
    }

    internal void SetUploading(bool isUploading = true)
    {
        Logger.LogTrace("Setting {this} uploading {uploading}", this, isUploading);

        if (isUploading && Pair.AutoPausedByOtherSync && !Pair.EffectiveOverrideOtherSync)
        {
            isUploading = false;
        }

        if (isUploading && Pair.RemoteOtherSyncOverrideActive && Pair.RemoteOtherSyncYield && !Pair.EffectiveOverrideOtherSync)
        {
            isUploading = false;
        }

        Pair?.SetUploadState(isUploading);

        if (_charaHandler != null)
        {
            try
            {
                var go = _charaHandler.GetGameObject();
                if (go == null) return;

                if (go.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc)
                    return;

                var expected = _dalamudUtil.GetPlayerCharacterFromCachedTableByIdent(Pair.Ident);
                if (expected != nint.Zero && expected != _charaHandler.Address)
                    return;

                Mediator.Publish(new PlayerUploadingMessage(_charaHandler, isUploading));
            }
            catch
            {
                // Best effort
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _syncWorker?.Dispose();
        _syncWorker = null;

        SetUploading(isUploading: false);
        lock (_refreshUiGate)
        {
            _refreshUiPending = false;
            _refreshUiPublishTick = 0;
        }
        var name = PlayerName;
        var skipHeavyRestore = _disposeRestoreAlreadyQueued;
        Logger.LogDebug("Disposing {name} ({user})", name, Pair);
        try
        {
            Guid applicationId = Guid.NewGuid();
            _applicationCancellationTokenSource?.CancelDispose();
            _applicationCancellationTokenSource = null;
            _deferredTempFilesModCleanupCts?.CancelDispose();
            _deferredTempFilesModCleanupCts = null;
            _downloadCancellationTokenSource?.CancelDispose();
            _downloadCancellationTokenSource = null;
            DisposeDownloadManager();

            try
            {
                if (!string.IsNullOrEmpty(name))
                {
                    Mediator.Publish(new EventMessage(new Event(name, Pair.UserData, nameof(PairHandler), EventSeverity.Informational, "Disposing User")));
                }

                if (_lifetime.ApplicationStopping.IsCancellationRequested) return;

                if (skipHeavyRestore)
                {
                    Logger.LogDebug("[{applicationId}] Skipping synchronous dispose restore for {name}; paused/disconnect cleanup was already queued", applicationId, name);
                }
                else if (_dalamudUtil is { IsZoning: false, IsInCutscene: false } && !string.IsNullOrEmpty(name))
                {
                    Logger.LogTrace("[{applicationId}] Restoring state for {name} ({OnlineUser})", applicationId, name, Pair.UserPair);
                    Logger.LogDebug("[{applicationId}] Removing Temp Collection for {name} ({user})", applicationId, name, Pair.UserPair);
                    RemovePenumbraCollectionAsync(applicationId).GetAwaiter().GetResult();

                    if (!IsVisible)
                    {
                        Logger.LogDebug("[{applicationId}] Restoring Glamourer for {name} ({user})", applicationId, name, Pair.UserPair);
                        _ipcManager.Glamourer.RevertByNameAsync(Logger, name, applicationId).GetAwaiter().GetResult();
                    }
                    else
                    {
                        using var cts = new CancellationTokenSource();
                        cts.CancelAfter(TimeSpan.FromSeconds(60));

                        Logger.LogInformation("[{applicationId}] CachedData is null {isNull}, contains things: {contains}", applicationId, _cachedData == null, _cachedData?.FileReplacements.Any() ?? false);

                        foreach (KeyValuePair<ObjectKind, List<FileReplacementData>> item in _cachedData?.FileReplacements ?? [])
                        {
                            try
                            {
                                RevertCustomizationDataAsync(item.Key, name, applicationId, cts.Token).GetAwaiter().GetResult();
                            }
                            catch (InvalidOperationException ex)
                            {
                                Logger.LogWarning(ex, "Failed disposing player (not present anymore?)");
                                break;
                            }
                        }
                    }
                }
            }
            finally
            {
                _charaHandler?.Dispose();
                _charaHandler = null;
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogDebug("Disposal for {name} was cancelled (timeout or shutdown).", name);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error on disposal of {name}", name);
        }
        finally
        {
            PlayerName = null;
            _cachedData = null;
            ResetAppliedModTrackingState();
            Logger.LogDebug("Disposing {name} complete", name);
        }
    }

    private void ScheduleRefreshUi(bool immediate = false)
    {
        lock (_refreshUiGate)
        {
            var dueTick = immediate ? Environment.TickCount64 : Environment.TickCount64 + 50;
            if (!_refreshUiPending || dueTick < _refreshUiPublishTick)
                _refreshUiPublishTick = dueTick;

            _refreshUiPending = true;
        }
    }

    private void FlushScheduledRefreshUi()
    {
        var publish = false;
        lock (_refreshUiGate)
        {
            if (!_refreshUiPending)
                return;

            if (Environment.TickCount64 < _refreshUiPublishTick)
                return;

            _refreshUiPending = false;
            _refreshUiPublishTick = 0;
            publish = true;
        }

        if (publish)
            Mediator.Publish(new RefreshUiMessage());
    }

    private static Pair.DownloadProgressSummary SummarizeDownloadStatus(Dictionary<string, FileDownloadStatus>? src)
    {
        if (src == null || src.Count == 0)
            return Pair.DownloadProgressSummary.None;

        bool anyDownloading = false;
        bool anyLoading = false;
        long totalBytes = 0;
        long transferredBytes = 0;
        int totalFiles = 0;
        int transferredFiles = 0;

        foreach (var s in src.Values)
        {
            if (s == null)
                continue;

            switch (s.DownloadStatus)
            {
                case DownloadStatus.Downloading:
                case DownloadStatus.WaitingForQueue:
                case DownloadStatus.WaitingForSlot:
                    anyDownloading = true;
                    break;

                case DownloadStatus.Initializing:
                case DownloadStatus.Decompressing:
                    anyLoading = true;
                    break;
            }

            if (s.TotalBytes > 0) totalBytes += s.TotalBytes;
            if (s.TransferredBytes > 0) transferredBytes += s.TransferredBytes;
            if (s.TotalFiles > 0) totalFiles += s.TotalFiles;
            if (s.TransferredFiles > 0) transferredFiles += s.TransferredFiles;
        }

        if (!anyDownloading && !anyLoading && totalBytes <= 0 && totalFiles <= 0 && transferredBytes <= 0 && transferredFiles <= 0)
            return Pair.DownloadProgressSummary.None;

        return new Pair.DownloadProgressSummary(true, anyDownloading, anyLoading, totalBytes, transferredBytes, totalFiles, transferredFiles);
    }

    private static Dictionary<string, FileDownloadStatus> SnapshotStatus(Dictionary<string, FileDownloadStatus>? src)
    {
        if (src == null || src.Count == 0)
            return new Dictionary<string, FileDownloadStatus>(StringComparer.Ordinal);

        var dst = new Dictionary<string, FileDownloadStatus>(src.Count, StringComparer.Ordinal);

        foreach (var kv in src)
        {
            var s = kv.Value;
            if (s == null) continue;

            dst[kv.Key] = new FileDownloadStatus
            {
                DownloadStatus = s.DownloadStatus,
                TotalBytes = s.TotalBytes,
                TransferredBytes = s.TransferredBytes,
                TotalFiles = s.TotalFiles,
                TransferredFiles = s.TransferredFiles
            };
        }

        return dst;
    }
}
