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
    private sealed class OtherSyncCoordinator : CoordinatorBase
    {
        public OtherSyncCoordinator(PairHandler owner) : base(owner)
        {
        }

            public void BroadcastLocalOtherSyncYieldState(bool yieldToOtherSync, string owner)
            {
                owner ??= string.Empty;
                if (!yieldToOtherSync)
                    owner = string.Empty;

                _localOtherSyncDecisionYield = yieldToOtherSync;
                _localOtherSyncDecisionOwner = owner;

                if (_lastBroadcastYield.HasValue && _lastBroadcastYield.Value == yieldToOtherSync && string.Equals(_lastBroadcastOwner, owner, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                _lastBroadcastYield = yieldToOtherSync;
                _lastBroadcastOwner = owner;

                Mediator.Publish(new LocalOtherSyncYieldStateChangedMessage(Pair.UserData.UID, yieldToOtherSync, owner));
            }

            public void ReclaimFromOtherSync(bool requestApplyIfPossible, bool treatAsFirstVisible)
            {
                Pair.AutoPausedByOtherSync = false;
                Pair.AutoPausedByOtherSyncName = string.Empty;

                CancelPairSyncWork();

                Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
                Pair.SetCurrentDownloadStatus(null);

                if (treatAsFirstVisible)
                {
                    GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
                    ScheduleRefreshUi();
                    return;
                }

                if (_charaHandler != null && _charaHandler.Address != nint.Zero && !string.IsNullOrEmpty(PlayerName))
                {
                    // Do not promote received data into _cachedData here. The sync worker keeps
                    // desired, prepared, and actually-applied state separate.
                    ResetLiveApplicationState();
                    _forceApplyMods = true;
                    _redrawOnNextApplication = false;

                    _customizeIds.Clear();

                    ResetVisibilityTracking();
                    BeginVisibilityRecoveryWindow(Environment.TickCount64, isZoneTransition: true);
                    MarkInitialApplyRequired();
                    ResetOtherSyncCandidateState();

                    if (requestApplyIfPossible)
                    {
                        if (!IsVisible)
                        {
                            IsVisible = true;
                            SyncStorm.RegisterVisibleNow();
                        }

                        _ = Task.Run(() =>
                        {
                            try
                            {
                                Pair.ApplyLastReceivedData(forced: true);
                            }
                            catch (Exception ex)
                            {
                                Logger.LogWarning(ex, "Deferred reclaim apply dispatch failed for {pair}", Pair.UserData.AliasOrUID);
                            }
                        });
                    }

                    ScheduleRefreshUi();
                    return;
                }

                GoBackToVanillaState(revertLiveCustomizationState: true, waitForCompletion: false);
                ScheduleRefreshUi();
            }

            public void HandleOtherSyncReleased(bool requestApplyIfPossible)
            {
                var canSoftReclaim = _charaHandler != null && _charaHandler.Address != nint.Zero && IsVisible && !string.IsNullOrEmpty(PlayerName);
                ReclaimFromOtherSync(requestApplyIfPossible, treatAsFirstVisible: !canSoftReclaim);
            }

            public void EnterYieldedState(string owner)
            {
                owner ??= string.Empty;

                var alreadySameOwner =
                    Pair.AutoPausedByOtherSync &&
                    string.Equals(Pair.AutoPausedByOtherSyncName, owner, StringComparison.OrdinalIgnoreCase);

                if (alreadySameOwner)
                {
                    BroadcastLocalOtherSyncYieldState(yieldToOtherSync: true, owner: owner);
                    ScheduleRefreshUi(immediate: true);
                    return;
                }

                Pair.AutoPausedByOtherSync = true;
                Pair.AutoPausedByOtherSyncName = owner;

                Logger.LogDebug("OtherSync owner {owner} claimed {pair}; pausing RavaSync work without forcing vanilla teardown", owner, Pair.UserData.AliasOrUID);

                // OtherSync ownership is a coordination pause, not a visual teardown.
                // RavaSync should stop downloading/applying for this pair and let the owning
                // sync provider settle its own state, but it must not run the hard vanilla
                // reset path here or it can fight the provider that just took over.
                CancelPairSyncWork(clearDesired: false);
                Pair.SetVisibleTransferStatus(Pair.VisibleTransferIndicator.None);
                Pair.SetCurrentDownloadStatus(null);
                Pair.SetCurrentDownloadSummary(Pair.DownloadProgressSummary.None);
                SetUploading(isUploading: false);

                _initialApplyPending = false;
                _redrawOnNextApplication = false;
                _forceApplyMods = true;
                ResetOtherSyncCandidateState();
                BroadcastLocalOtherSyncYieldState(yieldToOtherSync: true, owner: owner);
                ScheduleRefreshUi(immediate: true);
            }
    }
}
