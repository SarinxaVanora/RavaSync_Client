using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dalamud.Game.ClientState.Objects.Types;
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
        private readonly ConcurrentDictionary<nint, (int ObjectIndex, nint Address, long LastRedrawTick)> _lastAssignedSummonedActorIndices = new();

        public PenumbraCoordinator(PairHandler owner) : base(owner)
        {
        }

        public void ClearSummonedActorBindingState()
        {
            _lastAssignedSummonedActorIndices.Clear();
        }

        public async Task<(bool Bound, bool Reassigned)> EnsurePenumbraCollectionBindingAsync(Guid applicationId)
            {
                var (objIndex, ownershipAddr, bindingValid) = await _dalamudUtil
                    .RunOnFrameworkThread(() =>
                    {
                        var obj = _charaHandler!.GetGameObject();
                        if (obj == null || obj.Address == nint.Zero)
                            return (-1, nint.Zero, false);

                        var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                        if (localPlayerAddress != nint.Zero && obj.Address == localPlayerAddress)
                        {
                            Logger.LogWarning("[{applicationId}] Refusing to bind remote pair {player}/{uid} to the local player object index {idx}",
                                applicationId, PlayerName, Pair.UserData.UID, obj.ObjectIndex);
                            return (-1, nint.Zero, false);
                        }

                        if (!IsExpectedPlayerAddress(obj.Address))
                            return (-1, nint.Zero, false);

                        var stablePlayerAddr = ResolveStablePlayerAddress(obj.Address);
                        if (stablePlayerAddr == nint.Zero || !IsExpectedPlayerAddress(stablePlayerAddr))
                            return (-1, nint.Zero, false);

                        if (localPlayerAddress != nint.Zero && stablePlayerAddr == localPlayerAddress)
                        {
                            Logger.LogWarning("[{applicationId}] Refusing to bind remote pair {player}/{uid} because the stable owner resolved to the local player",
                                applicationId, PlayerName, Pair.UserData.UID);
                            return (-1, nint.Zero, false);
                        }

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
                {
                    var liveBinding = await _ipcManager.Penumbra.TryGetObjectEffectiveCollectionMatchAsync(
                        Logger,
                        _penumbraCollection,
                        objIndex,
                        Pair.Ident,
                        ownershipAddr,
                        PlayerName ?? Pair.UserData.AliasOrUID).ConfigureAwait(false);

                    if (liveBinding.Checked && liveBinding.Matches)
                        return (true, false);

                    Logger.LogDebug(
                        "[{applicationId}] Cached Penumbra temp collection binding for {player}/{uid} was not live at idx {idx}: effective={effective}/{name}, expected={expected}; forcing receiver rebind",
                        applicationId,
                        PlayerName,
                        Pair.UserData.UID,
                        objIndex,
                        liveBinding.EffectiveCollectionId,
                        liveBinding.EffectiveCollectionName,
                        _penumbraCollection);

                    needsAssign = true;
                    _lastAssignedCollectionAssignUtc = DateTime.MinValue;
                }

                if (nowUtc < _nextTempCollectionRetryNotBeforeUtc)
                {
                    _initialApplyPending = true;
                    return (false, false);
                }

                _lastAssignedCollectionAssignUtc = nowUtc;

                var oldIdx = _lastAssignedObjectIndex;

                await EnsurePenumbraCollectionAsync().ConfigureAwait(false);

                var ok = await _ipcManager.Penumbra.AssignTemporaryCollectionToVerifiedCharacterAsync(
                    Logger,
                    _penumbraCollection,
                    objIndex,
                    Pair.Ident,
                    ownershipAddr,
                    PlayerName ?? Pair.UserData.AliasOrUID).ConfigureAwait(false);

                if (!ok)
                {
                    Logger.LogDebug("[{applicationId}] Could not claim Penumbra temp collection for idx {idx}; requeueing apply after a short backoff to avoid partial state",
                        applicationId, objIndex);
                    _initialApplyPending = true;
                    _lastAssignedCollectionAssignUtc = DateTime.MinValue;
                    _nextTempCollectionRetryNotBeforeUtc = nowUtc.AddSeconds(1);
                    return (false, false);
                }

                var verifiedLiveBinding = await _ipcManager.Penumbra.TryGetObjectEffectiveCollectionMatchAsync(
                    Logger,
                    _penumbraCollection,
                    objIndex,
                    Pair.Ident,
                    ownershipAddr,
                    PlayerName ?? Pair.UserData.AliasOrUID).ConfigureAwait(false);

                if (!verifiedLiveBinding.Checked || !verifiedLiveBinding.Matches)
                {
                    Logger.LogDebug(
                        "[{applicationId}] Penumbra accepted receiver collection assignment for {player}/{uid}, but the live effective collection at idx {idx} is {effective}/{name}, expected {expected}; retrying instead of committing vanilla state",
                        applicationId,
                        PlayerName,
                        Pair.UserData.UID,
                        objIndex,
                        verifiedLiveBinding.EffectiveCollectionId,
                        verifiedLiveBinding.EffectiveCollectionName,
                        _penumbraCollection);
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
                    Logger.LogDebug("[{applicationId}] ObjectIndex changed {oldIdx} -> {newIdx}; old object index cleanup intentionally skipped. Object indices are volatile and must never be cleared blindly.",
                        applicationId, oldIdx.Value, objIndex);
                }

                return (true, needsAssign);
            }


            public async Task<bool> EnsureOwnedObjectPenumbraCollectionBindingAsync(Guid applicationId, GameObjectHandler handler, ObjectKind objectKind)
            {
                if (objectKind == ObjectKind.Player)
                    return true;

                if (handler == null || handler.Address == nint.Zero)
                    return false;

                var binding = await _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    var playerAddress = ResolveStablePlayerAddress();
                    if (playerAddress == nint.Zero)
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    var expectedOwnedAddress = ResolveOwnedObjectAddressForRetry(playerAddress, objectKind);
                    if (expectedOwnedAddress == nint.Zero || expectedOwnedAddress != handler.Address)
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    var obj = handler.GetGameObject();
                    if (obj == null || obj.Address == nint.Zero || obj.Address != expectedOwnedAddress)
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                    if (localPlayerAddress != nint.Zero && obj.Address == localPlayerAddress)
                    {
                        Logger.LogWarning("[{applicationId}] Refusing to bind remote pair {player}/{uid} owned {objectKind} to the local player object index {idx}",
                            applicationId, PlayerName, Pair.UserData.UID, objectKind, obj.ObjectIndex);
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);
                    }

                    return (Index: obj.ObjectIndex, Address: obj.Address, DisplayName: handler.Name ?? string.Empty, Valid: obj.ObjectIndex >= 0);
                }).ConfigureAwait(false);

                if (!binding.Valid || binding.Index < 0 || binding.Address == nint.Zero)
                    return false;

                if (_lastAssignedOwnedObjectIndices.TryGetValue(objectKind, out var existing)
                    && existing.ObjectIndex == binding.Index
                    && existing.Address == binding.Address
                    && _penumbraCollection != Guid.Empty)
                {
                    return true;
                }

                await EnsurePenumbraCollectionAsync().ConfigureAwait(false);

                var ok = await _ipcManager.Penumbra.AssignTemporaryCollectionToVerifiedCharacterAsync(
                    Logger,
                    _penumbraCollection,
                    binding.Index,
                    string.Empty,
                    binding.Address,
                    string.IsNullOrWhiteSpace(binding.DisplayName) ? $"{PlayerName ?? Pair.UserData.AliasOrUID} {objectKind}" : binding.DisplayName).ConfigureAwait(false);

                if (!ok)
                {
                    Logger.LogDebug("[{applicationId}] Could not claim Penumbra temp collection for {player} owned {objectKind} idx {idx}; retrying when the object settles",
                        applicationId, PlayerName, objectKind, binding.Index);
                    _lastAssignedOwnedObjectIndices.Remove(objectKind);
                    return false;
                }

                _lastAssignedOwnedObjectIndices[objectKind] = (binding.Index, binding.Address);
                Logger.LogTrace("[{applicationId}] Bound RavaSync Penumbra temp collection {collection} to {player} owned {objectKind} idx {idx} addr {addr:X}",
                    applicationId, _penumbraCollection, PlayerName, objectKind, binding.Index, binding.Address);
                return true;
            }


            public async Task<bool> EnsureSummonedActorPenumbraCollectionBindingAsync(Guid applicationId, nint actorAddress, string reason)
            {
                if (actorAddress == nint.Zero)
                    return false;

                var binding = await _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    var playerAddress = ResolveStablePlayerAddress();
                    if (playerAddress == nint.Zero || actorAddress == playerAddress)
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    if (!_dalamudUtil.IsOwnedActorOfPlayer(playerAddress, actorAddress))
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    var obj = _dalamudUtil.CreateGameObject(actorAddress);
                    if (obj == null || obj.Address == nint.Zero || obj.Address != actorAddress || obj is not ICharacter)
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);

                    var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                    if (localPlayerAddress != nint.Zero && obj.Address == localPlayerAddress)
                    {
                        Logger.LogWarning("[{applicationId}] Refusing to bind remote pair {player}/{uid} summoned actor to the local player object index {idx}",
                            applicationId, PlayerName, Pair.UserData.UID, obj.ObjectIndex);
                        return (Index: -1, Address: nint.Zero, DisplayName: string.Empty, Valid: false);
                    }

                    return (Index: obj.ObjectIndex, Address: obj.Address, DisplayName: obj.Name.TextValue ?? string.Empty, Valid: obj.ObjectIndex >= 0);
                }).ConfigureAwait(false);

                if (!binding.Valid || binding.Index < 0 || binding.Address == nint.Zero)
                    return false;

                var nowTick = Environment.TickCount64;
                if (_lastAssignedSummonedActorIndices.TryGetValue(binding.Address, out var existing)
                    && existing.ObjectIndex == binding.Index
                    && existing.Address == binding.Address
                    && _penumbraCollection != Guid.Empty)
                {
                    return false;
                }

                await EnsurePenumbraCollectionAsync().ConfigureAwait(false);

                var ok = await _ipcManager.Penumbra.AssignTemporaryCollectionToVerifiedCharacterAsync(
                    Logger,
                    _penumbraCollection,
                    binding.Index,
                    string.Empty,
                    binding.Address,
                    string.IsNullOrWhiteSpace(binding.DisplayName) ? $"{PlayerName ?? Pair.UserData.AliasOrUID} {reason}" : binding.DisplayName).ConfigureAwait(false);

                if (!ok)
                {
                    Logger.LogDebug("[{applicationId}] Could not claim Penumbra temp collection for {player} {reason} idx {idx}; actor will retry on the next resource load",
                        applicationId, PlayerName, reason, binding.Index);
                    _lastAssignedSummonedActorIndices.TryRemove(binding.Address, out _);
                    return false;
                }

                _lastAssignedSummonedActorIndices[binding.Address] = (binding.Index, binding.Address, nowTick);
                Logger.LogTrace("[{applicationId}] Bound RavaSync Penumbra temp collection {collection} to {player} {reason} idx {idx} addr {addr:X}",
                    applicationId, _penumbraCollection, PlayerName, reason, binding.Index, binding.Address);
                return true;
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
