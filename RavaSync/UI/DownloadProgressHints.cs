using System.Collections.Generic;
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.UI;

internal enum DownloadProgressPhase
{
    None = 0,
    CheckingCache = 1,
    Queued = 2,
    Downloading = 3,
    Decompressing = 4,
}

internal static class DownloadProgressHints
{
    public static DownloadProgressPhase GetPrimaryPhase(IEnumerable<FileDownloadStatus>? statuses)
    {
        if (statuses == null)
            return DownloadProgressPhase.None;

        var hasCheckingCache = false;
        var hasQueued = false;
        var hasDownloading = false;
        var hasDecompressing = false;

        foreach (var status in statuses)
        {
            if (status == null)
                continue;

            switch (status.DownloadStatus)
            {
                case DownloadStatus.Downloading:
                    hasDownloading = true;
                    break;

                case DownloadStatus.Decompressing:
                    hasDecompressing = true;
                    break;

                case DownloadStatus.WaitingForSlot:
                case DownloadStatus.WaitingForQueue:
                    hasQueued = true;
                    break;

                case DownloadStatus.Initializing:
                    hasCheckingCache = true;
                    break;
            }
        }

        return GetPrimaryPhase(hasCheckingCache, hasQueued, hasDownloading, hasDecompressing);
    }

    public static DownloadProgressPhase GetPrimaryPhase(int checkingCacheCount, int queuedCount, int downloadingCount, int decompressingCount)
    {
        return GetPrimaryPhase(
            checkingCacheCount > 0,
            queuedCount > 0,
            downloadingCount > 0,
            decompressingCount > 0);
    }

    private static DownloadProgressPhase GetPrimaryPhase(bool hasCheckingCache, bool hasQueued, bool hasDownloading, bool hasDecompressing)
    {
        if (hasDownloading)
            return DownloadProgressPhase.Downloading;

        if (hasDecompressing)
            return DownloadProgressPhase.Decompressing;

        if (hasQueued)
            return DownloadProgressPhase.Queued;

        if (hasCheckingCache)
            return DownloadProgressPhase.CheckingCache;

        return DownloadProgressPhase.None;
    }

    public static string GetPhaseLabel(DownloadProgressPhase phase)
    {
        return phase switch
        {
            DownloadProgressPhase.CheckingCache => "Checking Cache",
            DownloadProgressPhase.Queued => "Queued",
            DownloadProgressPhase.Downloading => "Downloading",
            DownloadProgressPhase.Decompressing => "Decompressing",
            _ => string.Empty,
        };
    }

    public static string BuildPhaseList(int checkingCacheCount, int queuedCount, int downloadingCount, int decompressingCount)
    {
        var phases = new List<string>(4);

        if (checkingCacheCount > 0)
            phases.Add(GetPhaseLabel(DownloadProgressPhase.CheckingCache));

        if (queuedCount > 0)
            phases.Add(GetPhaseLabel(DownloadProgressPhase.Queued));

        if (downloadingCount > 0)
            phases.Add(GetPhaseLabel(DownloadProgressPhase.Downloading));

        if (decompressingCount > 0)
            phases.Add(GetPhaseLabel(DownloadProgressPhase.Decompressing));

        return phases.Count == 0 ? string.Empty : string.Join("  •  ", phases);
    }


    public static string GetCompactPairTransferLabel(bool uploading, DownloadProgressPhase phase)
    {
        if (uploading)
            return "UP";

        return phase == DownloadProgressPhase.None ? string.Empty : "DL";
    }

    public static string FormatProgressText(DownloadProgressPhase phase, long transferredBytes, long totalBytes)
    {
        var label = GetPhaseLabel(phase);

        if (string.IsNullOrWhiteSpace(label))
            label = "Downloading";

        if (totalBytes > 0)
        {
            var done = Math.Clamp(transferredBytes, 0, totalBytes);
            return $"{label}  {UiSharedService.ByteToString(done, addSuffix: false)}/{UiSharedService.ByteToString(totalBytes)}";
        }

        return label;
    }
}
