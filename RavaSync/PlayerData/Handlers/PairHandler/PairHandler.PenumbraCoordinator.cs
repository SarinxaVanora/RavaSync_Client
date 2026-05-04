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
    private sealed class PenumbraCoordinator : CoordinatorBase
    {
        public PenumbraCoordinator(PairHandler owner) : base(owner)
        {
        }

            public async Task<(bool Bound, bool Reassigned)> EnsurePenumbraCollectionBindingAsync(Guid applicationId)
            {
                var (objIndex, ownershipAddr, bindingValid) = await _dalamudUtil
                    .RunOnFrameworkThread(() =>
                    {
                        var obj = _charaHandler!.GetGameObject();
                        if (obj == null || obj.Address == nint.Zero)
                            return (-1, nint.Zero, false);

                        if (!IsExpectedPlayerAddress(obj.Address))
                            return (-1, nint.Zero, false);

                        var stablePlayerAddr = ResolveStablePlayerAddress(obj.Address);
                        if (stablePlayerAddr == nint.Zero || !IsExpectedPlayerAddress(stablePlayerAddr))
                            return (-1, nint.Zero, false);

                        return (obj.ObjectIndex, stablePlayerAddr, true);
                    })
                    .ConfigureAwait(false);

                if (!bindingValid || objIndex < 0)
                {
                    Logger.LogDebug("[{applicationId}] Aborting apply because the target binding for {player} is no longer verified",
                        applicationId, PlayerName);
                    _initialApplyPending = true;
                    return (false, false);
                }

                var nowUtc = DateTime.UtcNow;
                var firstAssign = !_lastAssignedObjectIndex.HasValue;
                var indexChanged = _lastAssignedObjectIndex != objIndex;
                var playerAddressChanged = ownershipAddr != nint.Zero && _lastAssignedPlayerAddress != ownershipAddr;
                var stale = (nowUtc - _lastAssignedCollectionAssignUtc) > TimeSpan.FromSeconds(20);

                var needsAssign = firstAssign
                    || indexChanged
                    || playerAddressChanged
                    || stale
                    || _penumbraCollection == Guid.Empty;

                if (!needsAssign)
                    return (true, false);

                if (nowUtc < _nextTempCollectionRetryNotBeforeUtc)
                {
                    _initialApplyPending = true;
                    return (false, false);
                }

                _lastAssignedCollectionAssignUtc = nowUtc;

                var oldIdx = _lastAssignedObjectIndex;

                await EnsurePenumbraCollectionAsync().ConfigureAwait(false);

                var ok = await _ipcManager.Penumbra.AssignTemporaryCollectionAsync(Logger, _penumbraCollection, objIndex).ConfigureAwait(false);

                if (!ok)
                {
                    Logger.LogDebug("[{applicationId}] Could not claim Penumbra temp collection for idx {idx}; requeueing apply after a short backoff to avoid partial state",
                        applicationId, objIndex);
                    _initialApplyPending = true;
                    _lastAssignedCollectionAssignUtc = DateTime.MinValue;
                    _nextTempCollectionRetryNotBeforeUtc = nowUtc.AddSeconds(1);
                    return (false, false);
                }

                _nextTempCollectionRetryNotBeforeUtc = DateTime.MinValue;
                _lastAssignedObjectIndex = objIndex;
                _lastAssignedPlayerAddress = ownershipAddr;

                if (indexChanged && oldIdx.HasValue && oldIdx.Value >= 0)
                {
                    Logger.LogDebug("[{applicationId}] ObjectIndex changed {oldIdx} -> {newIdx}; clearing stale state from the old slot if it is no longer owned by {ident}",
                        applicationId, oldIdx.Value, objIndex, Pair.Ident);
                    CleanupOldAssignedIndexIfNeeded(oldIdx, applicationId);
                }

                return (true, needsAssign);
            }


            public async Task EnsurePenumbraCollectionAsync()
            {
                if (_penumbraCollection != Guid.Empty)
                    return;

                _penumbraCollectionTask ??= _ipcManager.Penumbra.CreateTemporaryCollectionAsync(Logger, Pair.UserData.UID);
                _penumbraCollection = await _penumbraCollectionTask.ConfigureAwait(false);
            }


            public async Task RemovePenumbraCollectionAsync(Guid applicationId)
            {
                var coll = _penumbraCollection;
                var collTask = _penumbraCollectionTask;

                _penumbraCollection = Guid.Empty;
                _penumbraCollectionTask = null;

                if (coll == Guid.Empty && collTask != null)
                {
                    try
                    {
                        coll = await collTask.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogDebug(ex, "Temp collection creation task faulted while tearing down for {pair}", Pair);
                        return;
                    }
                }

                if (coll == Guid.Empty)
                    return;

                await _ipcManager.Penumbra.RemoveTemporaryCollectionAsync(Logger, applicationId, coll).ConfigureAwait(false);
            }
    }
}
