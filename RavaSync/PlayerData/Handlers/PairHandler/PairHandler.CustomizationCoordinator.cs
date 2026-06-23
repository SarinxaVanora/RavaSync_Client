using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glamourer.Api.Enums;
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
    private sealed class CustomizationCoordinator : CoordinatorBase
    {
        public CustomizationCoordinator(PairHandler owner) : base(owner)
        {
        }

            private const int OwnedObjectRetryInitialDelayMs = 35;
            private const int OwnedObjectRetryPollDelayMs = 75;
            private const int OwnedObjectRetryBackoffMs = 150;
            private const int ShortLivedPetRedrawSettleMs = 140;

            public void QueueOwnedObjectCustomizationRetry(ObjectKind objectKind)
            {
                if (objectKind == ObjectKind.Player) return;

                _pendingOwnedObjectCustomizationRetry.Add(objectKind);
                var dueTick = Environment.TickCount64 + OwnedObjectRetryInitialDelayMs;
                if (_nextOwnedObjectCustomizationRetryTick < 0 || _nextOwnedObjectCustomizationRetryTick > dueTick)
                    _nextOwnedObjectCustomizationRetryTick = dueTick;
            }

            public bool HasPendingOwnedObjectCustomizationPayload(CharacterData charaData, ObjectKind objectKind)
            {
                return charaData.FileReplacements.TryGetValue(objectKind, out var replacements) && replacements.Count > 0
                    || charaData.GlamourerData.TryGetValue(objectKind, out var glamourerString) && !string.IsNullOrEmpty(glamourerString)
                    || Pair.IsCustomizePlusEnabled && charaData.CustomizePlusData.TryGetValue(objectKind, out var customizeScale) && !string.IsNullOrEmpty(customizeScale);
            }

            public nint ResolveOwnedObjectAddressForRetry(nint playerAddress, ObjectKind objectKind)
            {
                return objectKind switch
                {
                    ObjectKind.MinionOrMount => _dalamudUtil.GetMinionOrMountPtr(playerAddress),
                    ObjectKind.Pet => _dalamudUtil.GetPetPtr(playerAddress),
                    ObjectKind.Companion => _dalamudUtil.GetCompanionPtr(playerAddress),
                    _ => nint.Zero
                };
            }

            public void ProcessPendingOwnedObjectCustomizationRetry(long nowTick)
            {
                if (_pendingOwnedObjectCustomizationRetry.Count == 0) return;
                if (_nextOwnedObjectCustomizationRetryTick >= 0 && nowTick < _nextOwnedObjectCustomizationRetryTick) return;
                if (!IsVisible || _cachedData == null || _charaHandler == null || _charaHandler.Address == nint.Zero) return;

                var playerAddress = ResolveStablePlayerAddress();
                if (playerAddress == nint.Zero) return;

                foreach (var objectKind in _pendingOwnedObjectCustomizationRetry.ToArray())
                {
                    if (!HasPendingOwnedObjectCustomizationPayload(_cachedData, objectKind))
                    {
                        _pendingOwnedObjectCustomizationRetry.Remove(objectKind);
                        continue;
                    }

                    if (ResolveOwnedObjectAddressForRetry(playerAddress, objectKind) == nint.Zero)
                        continue;

                    var retryChanges = BuildOwnedObjectRetryChanges(_cachedData, objectKind);
                    if (retryChanges.Count == 0)
                    {
                        _pendingOwnedObjectCustomizationRetry.Remove(objectKind);
                        continue;
                    }

                    _pendingOwnedObjectCustomizationRetry.Remove(objectKind);
                    _nextOwnedObjectCustomizationRetryTick = nowTick + OwnedObjectRetryPollDelayMs;

                    var appData = Guid.NewGuid();
                    var cachedData = _cachedData.DeepClone();
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var applied = await ApplyOwnedObjectCustomizationRetryAsync(appData, cachedData, objectKind, retryChanges, CancellationToken.None).ConfigureAwait(false);
                            if (!applied && IsVisible && _cachedData != null && HasPendingOwnedObjectCustomizationPayload(_cachedData, objectKind))
                                QueueOwnedObjectCustomizationRetry(objectKind);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "[BASE-{appBase}] Deferred owned-object customization retry failed for {player} {objectKind}", appData, PlayerName, objectKind);
                            if (IsVisible && _cachedData != null && HasPendingOwnedObjectCustomizationPayload(_cachedData, objectKind))
                                QueueOwnedObjectCustomizationRetry(objectKind);
                        }
                    });

                    return;
                }

                _nextOwnedObjectCustomizationRetryTick = nowTick + OwnedObjectRetryBackoffMs;
            }

            private HashSet<PlayerChanges> BuildOwnedObjectRetryChanges(CharacterData charaData, ObjectKind objectKind)
            {
                var changes = new HashSet<PlayerChanges>();
                if (objectKind == ObjectKind.Player)
                    return changes;

                if (charaData.FileReplacements.TryGetValue(objectKind, out var replacements)
                    && HasMeaningfulFileReplacements(replacements))
                {
                    changes.Add(PlayerChanges.ModFiles);
                    changes.Add(PlayerChanges.ForcedRedraw);
                }

                if (charaData.GlamourerData.TryGetValue(objectKind, out var glamourerString)
                    && !string.IsNullOrWhiteSpace(glamourerString))
                {
                    changes.Add(PlayerChanges.Glamourer);
                }

                if (Pair.IsCustomizePlusEnabled
                    && charaData.CustomizePlusData.TryGetValue(objectKind, out var customizeScale)
                    && !string.IsNullOrWhiteSpace(customizeScale))
                {
                    changes.Add(PlayerChanges.Customize);
                    changes.Add(PlayerChanges.ForcedRedraw);
                }

                return changes;
            }


            private static bool HasMeaningfulFileReplacements(List<FileReplacementData>? replacements)
                => replacements != null && replacements.Any(static replacement =>
                    !string.IsNullOrWhiteSpace(replacement.Hash)
                    || !string.IsNullOrWhiteSpace(replacement.FileSwapPath)
                    || (replacement.GamePaths?.Any(static gamePath => !string.IsNullOrWhiteSpace(gamePath)) ?? false));

            private async Task<bool> ApplyOwnedObjectCustomizationRetryAsync(Guid applicationId, CharacterData charaData, ObjectKind objectKind, HashSet<PlayerChanges> retryChanges, CancellationToken token)
            {
                if (objectKind == ObjectKind.Player || retryChanges.Count == 0)
                    return false;

                if (!await IsOwnedObjectRetryApplyReadyAsync(objectKind, token).ConfigureAwait(false))
                    return false;

                var bound = await EnsurePenumbraCollectionBindingAsync(applicationId).ConfigureAwait(false);
                if (!bound.Bound)
                    return false;

                if (!await IsOwnedObjectRetryApplyReadyAsync(objectKind, token).ConfigureAwait(false))
                    return false;

                var lightweightOwnedObjectFlags = ApplyFlag.Once | ApplyFlag.Equipment | ApplyFlag.Customization;
                var changeSet = new KeyValuePair<ObjectKind, HashSet<PlayerChanges>>(objectKind, retryChanges);
                await ApplyCustomizationDataAsync(applicationId, changeSet, charaData, false, false, false, false, lightweightOwnedObjectFlags, token).ConfigureAwait(false);
                return true;
            }

            private async Task<bool> IsOwnedObjectRetryApplyReadyAsync(ObjectKind objectKind, CancellationToken token)
            {
                if (objectKind == ObjectKind.Player)
                    return false;

                return await _dalamudUtil.RunOnFrameworkThread(() =>
                {
                    token.ThrowIfCancellationRequested();

                    if (!IsVisible || _charaHandler == null || _charaHandler.Address == nint.Zero)
                        return false;

                    var playerAddress = ResolveStablePlayerAddress();
                    return playerAddress != nint.Zero && ResolveOwnedObjectAddressForRetry(playerAddress, objectKind) != nint.Zero;
                }).ConfigureAwait(false);
            }

            private static bool ShouldDeferFileBackedOwnedObjectApplyUntilDrawSettled(ObjectKind objectKind)
                => objectKind == ObjectKind.Pet;

            private async Task<bool> IsOwnedObjectReadyForFileBackedApplyAsync(Guid applicationId, GameObjectHandler handler, ObjectKind objectKind, CancellationToken token)
            {
                if (objectKind == ObjectKind.Player)
                    return true;

                if (!ShouldDeferFileBackedOwnedObjectApplyUntilDrawSettled(objectKind))
                {
                    if (handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                    {
                        Logger.LogTrace("[{applicationId}] Applying file-backed owned-object state without waiting for draw settle for {handler}; draw state={draw}", applicationId, handler, handler.CurrentDrawCondition);
                    }

                    return true;
                }

                await handler.RefreshStateOnFrameworkAsync().ConfigureAwait(false);
                token.ThrowIfCancellationRequested();

                if (handler.Address == nint.Zero)
                    return false;

                if (handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                {
                    Logger.LogTrace("[{applicationId}] Deferring file-backed {objectKind} apply until summon draw settles for {handler}; draw state={draw}", applicationId, objectKind, handler, handler.CurrentDrawCondition);
                    return false;
                }

                await Task.Delay(ShortLivedPetRedrawSettleMs, token).ConfigureAwait(false);
                await handler.RefreshStateOnFrameworkAsync().ConfigureAwait(false);
                token.ThrowIfCancellationRequested();

                if (handler.Address == nint.Zero)
                    return false;

                if (handler.CurrentDrawCondition != GameObjectHandler.DrawCondition.None)
                {
                    Logger.LogTrace("[{applicationId}] Deferring file-backed {objectKind} apply after settle check for {handler}; draw state={draw}", applicationId, objectKind, handler, handler.CurrentDrawCondition);
                    return false;
                }

                return true;
            }


            private async Task RunOptionalPluginApplyAsync(Guid applicationId, string pluginName, ObjectKind objectKind, Func<Task> action)
            {
                try
                {
                    await action().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex, "[{applicationId}] Optional plugin {pluginName} apply failed for {pair}/{objectKind}; continuing core visibility apply", applicationId, pluginName, Pair.UserData.AliasOrUID, objectKind);
                }
            }

            public async Task<bool> ApplyCustomizationDataAsync(Guid applicationId, KeyValuePair<ObjectKind, HashSet<PlayerChanges>> changes, CharacterData charaData, bool allowPlayerRedraw, bool forceLightweightMetadataReapply, bool awaitPlayerGlamourerApply, bool waitForPlayerGlamourerDrawSettle, ApplyFlag glamourerApplyFlags, CancellationToken token)
            {
                if (PlayerCharacter == nint.Zero)
                {
                    LogVisibilityDiagnostic("CUSTOM skip pair={pair} app={app} kind={kind} reason=no-player-character", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                    if (changes.Key == ObjectKind.Player)
                        Logger.LogDebug("[{applicationId}] Cannot apply player customization for {pair}: receiver player actor is unavailable", applicationId, Pair.UserData.AliasOrUID);
                    return false;
                }
                var ptr = PlayerCharacter;

                var handler = changes.Key switch
                {
                    ObjectKind.Player => _charaHandler!,
                    ObjectKind.Companion => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetCompanionPtr(ptr), isWatched: false).ConfigureAwait(false),
                    ObjectKind.MinionOrMount => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetMinionOrMountPtr(ptr), isWatched: false).ConfigureAwait(false),
                    ObjectKind.Pet => await _gameObjectHandlerFactory.Create(changes.Key, () => _dalamudUtil.GetPetPtr(ptr), isWatched: false).ConfigureAwait(false),
                    _ => throw new NotSupportedException("ObjectKind not supported: " + changes.Key)
                };

                try
                {
                    if (handler.Address == nint.Zero)
                    {
                        LogVisibilityDiagnostic("CUSTOM skip pair={pair} app={app} kind={kind} reason=handler-address-zero", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                        if (changes.Key != ObjectKind.Player)
                            QueueOwnedObjectCustomizationRetry(changes.Key);
                        else
                            Logger.LogDebug("[{applicationId}] Cannot apply player customization for {pair}: handler address is unavailable", applicationId, Pair.UserData.AliasOrUID);

                        return false;
                    }

                    LogVisibilityDiagnostic("CUSTOM start pair={pair} app={app} kind={kind} handler={handler} address={address} changes={changes}",
                        Pair.UserData.AliasOrUID,
                        applicationId,
                        changes.Key,
                        handler,
                        handler.Address,
                        string.Join("|", changes.Value));
                    Logger.LogDebug("[{applicationId}] Applying Customization Data for {handler}", applicationId, handler);
                    var isFileBackedOwnedObjectApply = changes.Key != ObjectKind.Player
                        && (changes.Value.Contains(PlayerChanges.ModFiles) || changes.Value.Contains(PlayerChanges.ForcedRedraw));
                    if (changes.Key != ObjectKind.Player)
                    {
                        if (isFileBackedOwnedObjectApply)
                        {
                            if (!await EnsureOwnedObjectPenumbraCollectionBindingAsync(applicationId, handler, changes.Key).ConfigureAwait(false))
                            {
                                QueueOwnedObjectCustomizationRetry(changes.Key);
                                return false;
                            }

                            if (!await IsOwnedObjectReadyForFileBackedApplyAsync(applicationId, handler, changes.Key, token).ConfigureAwait(false))
                            {
                                QueueOwnedObjectCustomizationRetry(changes.Key);
                                return false;
                            }
                        }
                        else
                        {
                            var ownedObjectDrawWaitMs = IsWineRuntime ? 700 : (SyncStorm.IsActive ? 500 : 1500);
                            await _dalamudUtil.WaitWhileCharacterIsDrawing(Logger, handler, applicationId, ownedObjectDrawWaitMs, token).ConfigureAwait(false);
                            token.ThrowIfCancellationRequested();
                        }

                        _pendingOwnedObjectCustomizationRetry.Remove(changes.Key);
                    }

                    // Coalesce redraws for this handler into a single call at the end.
                    bool needsRedraw = false;
                    List<Task>? deferredMetadataTasks = null;
                    List<Action>? deferredMetadataCommitActions = null;

                    foreach (var change in changes.Value.OrderBy(p => (int)p))
                    {
                        LogVisibilityDiagnostic("CUSTOM step pair={pair} app={app} kind={kind} change={change} handler={handler} address={address}", Pair.UserData.AliasOrUID, applicationId, changes.Key, change, handler, handler.Address);
                        Logger.LogDebug("[{applicationId}] Processing {change} for {handler}", applicationId, change, handler);

                        switch (change)
                        {
                            case PlayerChanges.Customize:
                                await RunOptionalPluginApplyAsync(applicationId, "Customize+", changes.Key, async () =>
                                {
                                    if (!Pair.IsCustomizePlusEnabled)
                                    {
                                        LogVisibilityDiagnostic("CUSTOM Customize+ skip pair={pair} app={app} kind={kind} reason=disabled-by-pair", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                                        if (_customizeIds.TryGetValue(changes.Key, out var appliedId) && appliedId.HasValue)
                                        {
                                            await _ipcManager.CustomizePlus.RevertByIdAsync(appliedId.Value).ConfigureAwait(false);
                                            _customizeIds.Remove(changes.Key);
                                        }
                                        return;
                                    }

                                    if (charaData.CustomizePlusData.TryGetValue(changes.Key, out var customizePlusData))
                                    {
                                        LogVisibilityDiagnostic("CUSTOM Customize+ apply pair={pair} app={app} kind={kind} payloadLength={length}", Pair.UserData.AliasOrUID, applicationId, changes.Key, customizePlusData?.Length ?? 0);
                                        _customizeIds[changes.Key] = await _ipcManager.CustomizePlus
                                            .SetBodyScaleAsync(handler.Address, customizePlusData)
                                            .ConfigureAwait(false);
                                        LogVisibilityDiagnostic("CUSTOM Customize+ applied pair={pair} app={app} kind={kind} tempId={id}", Pair.UserData.AliasOrUID, applicationId, changes.Key, _customizeIds[changes.Key]);
                                    }
                                    else if (_customizeIds.TryGetValue(changes.Key, out var customizeId) && customizeId.HasValue)
                                    {
                                        LogVisibilityDiagnostic("CUSTOM Customize+ revert pair={pair} app={app} kind={kind} tempId={id}", Pair.UserData.AliasOrUID, applicationId, changes.Key, customizeId.Value);
                                        await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                                        _customizeIds.Remove(changes.Key);
                                    }
                                }).ConfigureAwait(false);
                                break;

                            case PlayerChanges.Heels:
                                await RunOptionalPluginApplyAsync(applicationId, "SimpleHeels", changes.Key, () => _ipcManager.Heels.SetOffsetForPlayerAsync(handler.Address, charaData.HeelsData)).ConfigureAwait(false);
                                break;

                            case PlayerChanges.Honorific:
                                if (forceLightweightMetadataReapply || ShouldApplyLightweightMetadata(_lastAppliedHonorificData, changes.Key, charaData.HonorificData))
                                {
                                    deferredMetadataTasks ??= [];
                                    deferredMetadataTasks.Add(RunOptionalPluginApplyAsync(applicationId, "Honorific", changes.Key, () => _ipcManager.Honorific.SetTitleAsync(handler.Address, charaData.HonorificData)));

                                    if (forceLightweightMetadataReapply)
                                    {
                                        deferredMetadataCommitActions ??= [];
                                        var metadataData = charaData.HonorificData ?? string.Empty;
                                        deferredMetadataCommitActions.Add(() => _lastAppliedHonorificData[changes.Key] = metadataData);
                                    }
                                }
                                break;

                            case PlayerChanges.Glamourer:
                                if (charaData.GlamourerData.TryGetValue(changes.Key, out var glamourerData))
                                {
                                    LogVisibilityDiagnostic("CUSTOM Glamourer apply-start pair={pair} app={app} kind={kind} payloadLength={length} flags={flags} await={await} waitDraw={waitDraw}",
                                        Pair.UserData.AliasOrUID,
                                        applicationId,
                                        changes.Key,
                                        glamourerData?.Length ?? 0,
                                        glamourerApplyFlags,
                                        awaitPlayerGlamourerApply && changes.Key == ObjectKind.Player,
                                        waitForPlayerGlamourerDrawSettle);
                                    var waitForThisGlamourerApply = awaitPlayerGlamourerApply && changes.Key == ObjectKind.Player;
                                    var glamourerApplied = await _ipcManager.Glamourer.ApplyAllAsync(Logger, handler, glamourerData, applicationId, token, fireAndForget: !waitForThisGlamourerApply, flags: glamourerApplyFlags, waitForDrawSettle: waitForThisGlamourerApply && waitForPlayerGlamourerDrawSettle).ConfigureAwait(false);
                                    LogVisibilityDiagnostic("CUSTOM Glamourer apply-result pair={pair} app={app} kind={kind} applied={applied} fireAndForget={fireAndForget}",
                                        Pair.UserData.AliasOrUID,
                                        applicationId,
                                        changes.Key,
                                        glamourerApplied,
                                        !waitForThisGlamourerApply);
                                    if (!glamourerApplied)
                                    {
                                        if (changes.Key == ObjectKind.Player && waitForThisGlamourerApply)
                                            throw new InvalidOperationException($"Glamourer did not accept/apply the receiver player state for {Pair.UserData.AliasOrUID}");

                                        if (changes.Key != ObjectKind.Player)
                                        {
                                            QueueOwnedObjectCustomizationRetry(changes.Key);
                                            return false;
                                        }
                                    }

                                    if (IsWineRuntime && changes.Key == ObjectKind.Player && waitForThisGlamourerApply)
                                        await Task.Delay(waitForPlayerGlamourerDrawSettle ? 120 : 25, token).ConfigureAwait(false);
                                }
                                else
                                {
                                    LogVisibilityDiagnostic("CUSTOM Glamourer skip pair={pair} app={app} kind={kind} reason=no-payload", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                                }
                                break;

                            case PlayerChanges.Moodles:
                                if (forceLightweightMetadataReapply || ShouldApplyLightweightMetadata(_lastAppliedMoodlesData, changes.Key, charaData.MoodlesData))
                                {
                                    deferredMetadataTasks ??= [];
                                    deferredMetadataTasks.Add(RunOptionalPluginApplyAsync(applicationId, "Moodles", changes.Key, () => string.IsNullOrEmpty(charaData.MoodlesData)
                                        ? _ipcManager.Moodles.RevertStatusAsync(handler.Address)
                                        : _ipcManager.Moodles.SetStatusAsync(handler.Address, charaData.MoodlesData)));

                                    if (forceLightweightMetadataReapply)
                                    {
                                        deferredMetadataCommitActions ??= [];
                                        var metadataData = charaData.MoodlesData ?? string.Empty;
                                        deferredMetadataCommitActions.Add(() => _lastAppliedMoodlesData[changes.Key] = metadataData);
                                    }
                                }
                                break;

                            case PlayerChanges.PetNames:
                                if (forceLightweightMetadataReapply || ShouldApplyLightweightMetadata(_lastAppliedPetNamesData, changes.Key, charaData.PetNamesData))
                                {
                                    deferredMetadataTasks ??= [];
                                    deferredMetadataTasks.Add(RunOptionalPluginApplyAsync(applicationId, "PetNames", changes.Key, () => _ipcManager.PetNames.SetPlayerData(handler.Address, charaData.PetNamesData)));

                                    if (forceLightweightMetadataReapply)
                                    {
                                        deferredMetadataCommitActions ??= [];
                                        var metadataData = charaData.PetNamesData ?? string.Empty;
                                        deferredMetadataCommitActions.Add(() => _lastAppliedPetNamesData[changes.Key] = metadataData);
                                    }
                                }
                                break;

                            case PlayerChanges.ForcedRedraw:
                                if (changes.Key != ObjectKind.Player || allowPlayerRedraw)
                                    needsRedraw = true;
                                break;

                            default:
                                break;
                        }

                        token.ThrowIfCancellationRequested();
                    }

                    if (deferredMetadataTasks is { Count: > 0 })
                    {
                        await Task.WhenAll(deferredMetadataTasks).ConfigureAwait(false);

                        if (deferredMetadataCommitActions is { Count: > 0 })
                        {
                            foreach (var commitAction in deferredMetadataCommitActions)
                                commitAction();
                        }

                        token.ThrowIfCancellationRequested();
                    }

                    if (needsRedraw)
                    {
                        LogVisibilityDiagnostic("CUSTOM redraw-needed pair={pair} app={app} kind={kind} allowPlayerRedraw={allowPlayerRedraw} fileBackedOwned={fileBacked}", Pair.UserData.AliasOrUID, applicationId, changes.Key, allowPlayerRedraw, isFileBackedOwnedObjectApply);
                        if (changes.Key == ObjectKind.Player)
                        {
                            LogVisibilityDiagnostic("CUSTOM complete pair={pair} app={app} kind={kind} result=success redrawDeferredToMainApply=true", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                            return true;
                        }

                        if (isFileBackedOwnedObjectApply)
                        {
                            LogVisibilityDiagnostic("CUSTOM redraw-direct start pair={pair} app={app} kind={kind}", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                            var directFired = await _ipcManager.Penumbra.RedrawDirectAndWaitAsync(Logger, handler, applicationId, token).ConfigureAwait(false);
                            LogVisibilityDiagnostic("CUSTOM redraw-direct result pair={pair} app={app} kind={kind} directFired={directFired}", Pair.UserData.AliasOrUID, applicationId, changes.Key, directFired);
                            if (!directFired)
                            {
                                LogVisibilityDiagnostic("CUSTOM redraw-fallback start pair={pair} app={app} kind={kind}", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                                await _ipcManager.Penumbra.RedrawAsync(Logger, handler, applicationId, token).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            LogVisibilityDiagnostic("CUSTOM redraw start pair={pair} app={app} kind={kind}", Pair.UserData.AliasOrUID, applicationId, changes.Key);
                            await _ipcManager.Penumbra.RedrawAsync(Logger, handler, applicationId, token).ConfigureAwait(false);
                        }
                    }

                    LogVisibilityDiagnostic("CUSTOM complete pair={pair} app={app} kind={kind} result=success needsRedraw={needsRedraw}", Pair.UserData.AliasOrUID, applicationId, changes.Key, needsRedraw);
                    return true;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    LogVisibilityDiagnostic("CUSTOM exception pair={pair} app={app} kind={kind} type={type} message={message}", Pair.UserData.AliasOrUID, applicationId, changes.Key, ex.GetType().Name, ex.Message);
                    throw;
                }
                finally
                {
                    if (handler != _charaHandler) handler.Dispose();
                }
            }

            public async Task RevertCustomizationDataAsync(ObjectKind objectKind, string name, Guid applicationId, CancellationToken cancelToken, nint addressOverride = 0, IReadOnlyDictionary<ObjectKind, Guid?>? customizeIdSnapshot = null)
            {
                nint address = addressOverride != nint.Zero ? addressOverride : ResolveStablePlayerAddress();
                if (address == nint.Zero) return;

                Logger.LogDebug("[{applicationId}] Reverting all Customization for {alias}/{name} {objectKind}", applicationId, Pair.UserData.AliasOrUID, name, objectKind);

                Guid? customizeId = null;
                if (customizeIdSnapshot != null && customizeIdSnapshot.TryGetValue(objectKind, out var snapshotCustomizeId) && snapshotCustomizeId.HasValue)
                {
                    customizeId = snapshotCustomizeId;
                }
                else if (_customizeIds.TryGetValue(objectKind, out var liveCustomizeId) && liveCustomizeId.HasValue)
                {
                    customizeId = liveCustomizeId;
                }

                _customizeIds.Remove(objectKind);

                if (objectKind == ObjectKind.Player)
                {
                    using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Player, () => address, isWatched: false).ConfigureAwait(false);
                    tempHandler.CompareNameAndThrow(name);
                    Logger.LogDebug("[{applicationId}] Restoring Customization and Equipment for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                    tempHandler.CompareNameAndThrow(name);
                    Logger.LogDebug("[{applicationId}] Restoring Heels for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.Heels.RestoreOffsetForPlayerAsync(address).ConfigureAwait(false);
                    tempHandler.CompareNameAndThrow(name);
                    Logger.LogDebug("[{applicationId}] Restoring C+ for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.CustomizePlus.RevertAsync(address).ConfigureAwait(false);
                    if (customizeId.HasValue)
                        await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                    tempHandler.CompareNameAndThrow(name);
                    Logger.LogDebug("[{applicationId}] Restoring Honorific for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.Honorific.ClearTitleAsync(address).ConfigureAwait(false);
                    Logger.LogDebug("[{applicationId}] Restoring Moodles for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.Moodles.RevertStatusAsync(address).ConfigureAwait(false);
                    Logger.LogDebug("[{applicationId}] Restoring Pet Nicknames for {alias}/{name}", applicationId, Pair.UserData.AliasOrUID, name);
                    await _ipcManager.PetNames.ClearPlayerData(address).ConfigureAwait(false);
                }
                else if (objectKind == ObjectKind.MinionOrMount)
                {
                    var minionOrMount = await _dalamudUtil.GetMinionOrMountAsync(address).ConfigureAwait(false);
                    if (minionOrMount != nint.Zero)
                    {
                        if (customizeId.HasValue)
                            await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                        using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.MinionOrMount, () => minionOrMount, isWatched: false).ConfigureAwait(false);
                        _ = _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken, fireAndForget: true);
                        var directFired = await _ipcManager.Penumbra.RedrawDirectAndWaitAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                        if (!directFired)
                            await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);

                    }
                }
                else if (objectKind == ObjectKind.Pet)
                {
                    var pet = await _dalamudUtil.GetPetAsync(address).ConfigureAwait(false);
                    if (pet != nint.Zero)
                    {
                        if (customizeId.HasValue)
                            await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                        using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Pet, () => pet, isWatched: false).ConfigureAwait(false);
                        await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                        await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                    }
                }
                else if (objectKind == ObjectKind.Companion)
                {
                    var companion = await _dalamudUtil.GetCompanionAsync(address).ConfigureAwait(false);
                    if (companion != nint.Zero)
                    {
                        if (customizeId.HasValue)
                            await _ipcManager.CustomizePlus.RevertByIdAsync(customizeId.Value).ConfigureAwait(false);
                        using GameObjectHandler tempHandler = await _gameObjectHandlerFactory.Create(ObjectKind.Companion, () => companion, isWatched: false).ConfigureAwait(false);
                        await _ipcManager.Glamourer.RevertAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                        await _ipcManager.Penumbra.RedrawAsync(Logger, tempHandler, applicationId, cancelToken).ConfigureAwait(false);
                    }
                }
            }
    }
}
