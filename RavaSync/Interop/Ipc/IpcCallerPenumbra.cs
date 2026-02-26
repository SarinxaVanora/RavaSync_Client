using Dalamud.Plugin;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using Penumbra.Api.Enums;
using Penumbra.Api.Helpers;
using Penumbra.Api.IpcSubscribers;
using System.Collections.Concurrent;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerPenumbra : DisposableMediatorSubscriberBase, IIpcCaller
{
    private readonly IDalamudPluginInterface _pi;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly MareMediator _mareMediator;
    private readonly RedrawManager _redrawManager;
    private bool _shownPenumbraUnavailable = false;
    private string? _penumbraModDirectory;

    private sealed class RedrawGate
    {
        public int InFlight;
        public int Pending;
    }

    private readonly ConcurrentDictionary<nint, RedrawGate> _redrawGates = new();

    public string? ModDirectory
    {
        get => _penumbraModDirectory;
        private set
        {
            if (!string.Equals(_penumbraModDirectory, value, StringComparison.Ordinal))
            {
                _penumbraModDirectory = value;
                _mareMediator.Publish(new PenumbraDirectoryChangedMessage(_penumbraModDirectory));
            }
        }
    }

    private readonly ConcurrentDictionary<int, TempCollectionAssignState> _tempCollectionAssignState = new();

    private sealed class TempCollectionAssignState
    {
        public Guid LastCollection;
        public int FailCount;
        public DateTime NextAttemptUtc;
        public DateTime LastWarnUtc;
        public DateTime LastForceUtc;
    }


    private readonly ConcurrentDictionary<IntPtr, bool> _penumbraRedrawRequests = new();

    private readonly EventSubscriber _penumbraDispose;
    private readonly EventSubscriber<nint, string, string> _penumbraGameObjectResourcePathResolved;
    private readonly EventSubscriber _penumbraInit;
    private readonly EventSubscriber<ModSettingChange, Guid, string, bool> _penumbraModSettingChanged;
    private readonly EventSubscriber<nint, int> _penumbraObjectIsRedrawn;

    private readonly AddTemporaryMod _penumbraAddTemporaryMod;
    private readonly AssignTemporaryCollection _penumbraAssignTemporaryCollection;
    private readonly ConvertTextureFile _penumbraConvertTextureFile;
    private readonly CreateTemporaryCollection _penumbraCreateNamedTemporaryCollection;
    private readonly GetEnabledState _penumbraEnabled;
    private readonly GetPlayerMetaManipulations _penumbraGetMetaManipulations;
    private readonly RedrawObject _penumbraRedraw;
    private readonly DeleteTemporaryCollection _penumbraRemoveTemporaryCollection;
    private readonly RemoveTemporaryMod _penumbraRemoveTemporaryMod;
    private readonly GetModDirectory _penumbraResolveModDir;
    private readonly ResolvePlayerPathsAsync _penumbraResolvePaths;
    private readonly GetGameObjectResourcePaths _penumbraResourcePaths;
    private Task<Guid>? _emptyCollectionTask;
    private Guid _emptyCollectionId = Guid.Empty;


    public IpcCallerPenumbra(ILogger<IpcCallerPenumbra> logger, IDalamudPluginInterface pi, DalamudUtilService dalamudUtil,
        MareMediator mareMediator, RedrawManager redrawManager) : base(logger, mareMediator)
    {
        _pi = pi;
        _dalamudUtil = dalamudUtil;
        _mareMediator = mareMediator;
        _redrawManager = redrawManager;
        _penumbraInit = Initialized.Subscriber(pi, PenumbraInit);
        _penumbraDispose = Disposed.Subscriber(pi, PenumbraDispose);
        _penumbraResolveModDir = new GetModDirectory(pi);
        _penumbraRedraw = new RedrawObject(pi);
        _penumbraObjectIsRedrawn = GameObjectRedrawn.Subscriber(pi, RedrawEvent);
        _penumbraGetMetaManipulations = new GetPlayerMetaManipulations(pi);
        _penumbraRemoveTemporaryMod = new RemoveTemporaryMod(pi);
        _penumbraAddTemporaryMod = new AddTemporaryMod(pi);
        _penumbraCreateNamedTemporaryCollection = new CreateTemporaryCollection(pi);
        _penumbraRemoveTemporaryCollection = new DeleteTemporaryCollection(pi);
        _penumbraAssignTemporaryCollection = new AssignTemporaryCollection(pi);
        _penumbraResolvePaths = new ResolvePlayerPathsAsync(pi);
        _penumbraEnabled = new GetEnabledState(pi);
        _penumbraConvertTextureFile = new ConvertTextureFile(pi);
        _penumbraResourcePaths = new GetGameObjectResourcePaths(pi);
        _penumbraModSettingChanged = ModSettingChanged.Subscriber(pi, OnPenumbraModSettingChanged);
        _penumbraGameObjectResourcePathResolved = GameObjectResourcePathResolved.Subscriber(pi, ResourceLoaded);
        CheckAPI();
        CheckModDirectory();




        Mediator.Subscribe<PenumbraRedrawCharacterMessage>(this, (msg) =>
        {
            _ = SafeIpc.TryRun(Logger, "Penumbra.Redraw.Coalesced", TimeSpan.FromSeconds(2), ct =>
            {
                return _redrawManager.ExternalPenumbraRedrawAsync(Logger, msg.Character, Guid.NewGuid(), c =>
                {
                    _penumbraRedraw.Invoke(c.ObjectIndex, RedrawType.Redraw);
                }, ct);
            });
        });


        Mediator.Subscribe<DalamudLoginMessage>(this, (msg) => _shownPenumbraUnavailable = false);
    }

    public bool APIAvailable { get; private set; } = false;

    public void CheckAPI()
    {
        bool penumbraAvailable = false;
        try
        {
            var penumbraVersion = (_pi.InstalledPlugins
                .FirstOrDefault(p => string.Equals(p.InternalName, "Penumbra", StringComparison.OrdinalIgnoreCase))
                ?.Version ?? new Version(0, 0, 0, 0));
            penumbraAvailable = penumbraVersion >= new Version(1, 2, 0, 22);
            try
            {
                penumbraAvailable &= _penumbraEnabled.Invoke();
            }
            catch
            {
                penumbraAvailable = false;
            }
            _shownPenumbraUnavailable = _shownPenumbraUnavailable && !penumbraAvailable;
            APIAvailable = penumbraAvailable;
        }
        catch
        {
            APIAvailable = penumbraAvailable;
        }
        finally
        {
            if (!penumbraAvailable && !_shownPenumbraUnavailable)
            {
                _shownPenumbraUnavailable = true;
                _mareMediator.Publish(new NotificationMessage("Penumbra inactive",
                    "Your Penumbra installation is not active or out of date. Update Penumbra and/or the Enable Mods setting in Penumbra to continue to use RavaSync. If you just updated Penumbra, ignore this message.",
                    NotificationType.Error));
            }
        }
    }

    public void CheckModDirectory()
    {
        if (!APIAvailable)
        {
            ModDirectory = string.Empty;
        }
        else
        {
            ModDirectory = _penumbraResolveModDir!.Invoke().ToLowerInvariant();
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        _redrawManager.Cancel();
        _penumbraGameObjectResourcePathResolved.Dispose();
        _penumbraDispose.Dispose();
        _penumbraInit.Dispose();
        _penumbraObjectIsRedrawn.Dispose();
        _penumbraModSettingChanged.Dispose();
    }
    private void OnPenumbraModSettingChanged(ModSettingChange change,Guid collectionId,string modName,bool inherited)
    {

        if (change == ModSettingChange.EnableState)
        {
            return;
        }

        _mareMediator.Publish(new PenumbraModSettingChangedMessage());
    }
    //public async Task AssignTemporaryCollectionAsync(ILogger logger, Guid collName, int idx)
    //{
    //    if (!APIAvailable) return;

    //    await SafeIpc.TryRun(Logger, "Penumbra.AssignTemporaryCollection", TimeSpan.FromSeconds(2), async ct =>
    //    {
    //        await _dalamudUtil.RunOnFrameworkThread(() =>
    //        {
    //            var retAssign = _penumbraAssignTemporaryCollection.Invoke(collName, idx, forceAssignment: true);
    //            logger.LogTrace("Assigning Temp Collection {collName} to index {idx}, Success: {ret}", collName, idx, retAssign);
    //            return collName;
    //        }).ConfigureAwait(false);
    //    }).ConfigureAwait(false);
    //}

    public async Task<bool> AssignTemporaryCollectionAsync(ILogger logger, Guid collName, int idx)
    {
        if (!APIAvailable) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignTemporaryCollection", TimeSpan.FromSeconds(2), async ct =>
        {
            assigned = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var now = DateTime.UtcNow;
                var state = _tempCollectionAssignState.GetOrAdd(idx, _ => new TempCollectionAssignState());

                if (state.LastCollection != collName)
                {
                    state.LastCollection = collName;
                    state.FailCount = 0;
                    state.NextAttemptUtc = DateTime.MinValue;
                    state.LastWarnUtc = DateTime.MinValue;
                    state.LastForceUtc = DateTime.MinValue;
                }

                if (now < state.NextAttemptUtc)
                    return false;

                var ec = _penumbraAssignTemporaryCollection.Invoke(collName, idx, forceAssignment: false);
                if (ec == PenumbraApiEc.Success)
                {
                    _tempCollectionAssignState.TryRemove(idx, out _);
                    logger.LogTrace("[Penumbra] Assigned temp collection {collection} to idx {idx} (polite)", collName, idx);
                    return true;
                }

                if ((now - state.LastForceUtc) >= TimeSpan.FromSeconds(3))
                {
                    state.LastForceUtc = now;

                    var ecForce = _penumbraAssignTemporaryCollection.Invoke(collName, idx, forceAssignment: true);
                    if (ecForce == PenumbraApiEc.Success)
                    {
                        _tempCollectionAssignState.TryRemove(idx, out _);
                        logger.LogTrace("[Penumbra] Assigned temp collection {collection} to idx {idx} (forced)", collName, idx);
                        return true;
                    }

                    ec = ecForce;
                }

                state.FailCount++;
                var delayMs = (int)Math.Min(30_000, 500 * Math.Pow(2, Math.Min(state.FailCount, 10)));
                state.NextAttemptUtc = now.AddMilliseconds(delayMs);

                if (now - state.LastWarnUtc > TimeSpan.FromMinutes(1))
                {
                    state.LastWarnUtc = now;
                    logger.LogWarning(
                        "[Penumbra] Temp collection assign failed for idx {idx} (ec={ec}). Another plugin may be controlling this actor. Backing off for {delay}ms.",
                        idx, ec, delayMs);
                }

                return false;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return assigned;
    }



    public async Task ConvertTextureFiles(ILogger logger, Dictionary<string, string[]> textures, IProgress<(string, int)> progress, CancellationToken token)
    {
        if (!APIAvailable) return;

        _mareMediator.Publish(new HaltScanMessage(nameof(ConvertTextureFiles)));
        int currentTexture = 0;
        foreach (var texture in textures)
        {
            if (token.IsCancellationRequested) break;

            progress.Report((texture.Key, ++currentTexture));
            logger.LogInformation("Converting Texture {path} to {type}", texture.Key, TextureType.Bc7Tex);

            var ok = await SafeIpc.TryRun(Logger, "Penumbra.ConvertTextureFile", TimeSpan.FromSeconds(30), async ct =>
            {
                var convertTask = _penumbraConvertTextureFile.Invoke(texture.Key, texture.Key, TextureType.Bc7Tex, mipMaps: true);
                await convertTask.ConfigureAwait(false);
            }).ConfigureAwait(false);

            if (ok && texture.Value.Any())
            {
                foreach (var duplicatedTexture in texture.Value)
                {
                    logger.LogInformation("Migrating duplicate {dup}", duplicatedTexture);
                    try
                    {
                        File.Copy(texture.Key, duplicatedTexture, overwrite: true);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to copy duplicate {dup}", duplicatedTexture);
                    }
                }
            }
        }
        _mareMediator.Publish(new ResumeScanMessage(nameof(ConvertTextureFiles)));

        await _dalamudUtil.RunOnFrameworkThread(async () =>
        {
            var gameObject = await _dalamudUtil.CreateGameObjectAsync(await _dalamudUtil.GetPlayerPointerAsync().ConfigureAwait(false)).ConfigureAwait(false);
            _penumbraRedraw.Invoke(gameObject!.ObjectIndex, setting: RedrawType.Redraw);
        }).ConfigureAwait(false);
    }

    public async Task<Guid> CreateTemporaryCollectionAsync(ILogger logger, string uid)
    {
        if (!APIAvailable) return Guid.Empty;

        Guid result = Guid.Empty;

        await SafeIpc.TryRun(Logger, "Penumbra.CreateTemporaryCollection", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                Guid collId;
                var collName = "RavaSync" + uid;
                PenumbraApiEc penEC = _penumbraCreateNamedTemporaryCollection.Invoke(uid, collName, out collId);
                logger.LogTrace("Creating Temp Collection {collName}, GUID: {collId}", collName, collId);
                if (penEC != PenumbraApiEc.Success)
                {
                    logger.LogError("Failed to create temporary collection for {collName} with error code {penEC}. Please include this line in any error reports", collName, penEC);
                    return Guid.Empty;
                }
                return collId;
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<Dictionary<string, HashSet<string>>?> GetCharacterData(ILogger logger, GameObjectHandler handler)
    {
        if (!APIAvailable) return null;

        Dictionary<string, HashSet<string>>? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetGameObjectResourcePaths", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                logger.LogTrace("Calling On IPC: Penumbra.GetGameObjectResourcePaths");
                var idx = handler.GetGameObject()?.ObjectIndex;
                if (idx == null) return null;
                return _penumbraResourcePaths.Invoke(idx.Value)[0];
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return result;
    }

    public string GetMetaManipulations()
    {
        if (!APIAvailable) return string.Empty;

        try
        {
            return _penumbraGetMetaManipulations.Invoke() ?? string.Empty;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Penumbra.GetPlayerMetaManipulations threw.");
            return string.Empty;
        }
    }

    public async Task RedrawAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning) return;

        // Coalesce redraws per-actor:
        var gate = _redrawGates.GetOrAdd(handler.Address, _ => new RedrawGate());

        if (Interlocked.Exchange(ref gate.InFlight, 1) == 1)
        {
            Interlocked.Exchange(ref gate.Pending, 1);
            logger.LogTrace("[{appid}] Redraw already in flight for {name}, marking pending", applicationId, handler.Name);
            return;
        }

        try
        {
            while (!token.IsCancellationRequested && !_dalamudUtil.IsZoning)
            {
                // Clear pending for this run.
                Interlocked.Exchange(ref gate.Pending, 0);
                try
                {
                    await _redrawManager.PenumbraRedrawInternalAsync(logger, handler, applicationId, chara =>
                    {
                        logger.LogDebug("[{appid}] Calling on IPC: PenumbraRedraw", applicationId);

                        try
                        {
                            _penumbraRedraw!.Invoke(chara.ObjectIndex, RedrawType.Redraw);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Penumbra.Redraw threw");
                        }

                    }, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    logger.LogDebug("[{appid}] Penumbra redraw cancelled for {name}", applicationId, handler.Name);
                    break;
                }

                if (Interlocked.CompareExchange(ref gate.Pending, 0, 0) != 1)
                    break;
            }
        }
        finally
        {
            Interlocked.Exchange(ref gate.InFlight, 0);
        }
    }



    public async Task RemoveTemporaryCollectionAsync(ILogger logger, Guid applicationId, Guid collId)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.RemoveTemporaryCollection", TimeSpan.FromSeconds(2), async ct =>
        {
            await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                logger.LogTrace("[{applicationId}] Removing temp collection for {collId}", applicationId, collId);
                var ret2 = _penumbraRemoveTemporaryCollection.Invoke(collId);
                logger.LogTrace("[{applicationId}] RemoveTemporaryCollection: {ret2}", applicationId, ret2);
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task<(string[] forward, string[][] reverse)> ResolvePathsAsync(string[] forward, string[] reverse)
    {
        return await _penumbraResolvePaths.Invoke(forward, reverse).ConfigureAwait(false);
    }

    public async Task SetManipulationDataAsync(ILogger logger, Guid applicationId, Guid collId, string manipulationData)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.SetManipulationData", TimeSpan.FromSeconds(2), ct =>
        {
            logger.LogTrace("[{applicationId}] Manip: {data}", applicationId, manipulationData);
            var retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, [], manipulationData, 0);
            logger.LogTrace("[{applicationId}] Setting temp meta mod for {collId}, Success: {ret}", applicationId, collId, retAdd);

            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }
    public async Task ClearManipulationDataAsync(ILogger logger, Guid applicationId, Guid collId)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.RemoveTemporaryMod(Meta)", TimeSpan.FromSeconds(2), ct =>
        {
            var retRem = _penumbraRemoveTemporaryMod.Invoke("MareChara_Meta", collId, 0);
            logger.LogTrace("[{applicationId}] Cleared meta (height) from temp collection {collId}, ret={ret}", applicationId, collId, retRem);

            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }


    public async Task SetTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, Dictionary<string, string> modPaths)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.SetTemporaryMods", TimeSpan.FromSeconds(2), ct =>
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                foreach (var mod in modPaths)
                {
                    logger.LogTrace("[{applicationId}] Change: {from} => {to}", applicationId, mod.Key, mod.Value);
                }
            }

            var retRemove = _penumbraRemoveTemporaryMod.Invoke("MareChara_Files", collId, 0);
            logger.LogTrace("[{applicationId}] Removing temp files mod for {collId}, Success: {ret}", applicationId, collId, retRemove);

            var retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Files", collId, modPaths, string.Empty, 0);
            logger.LogTrace("[{applicationId}] Setting temp files mod for {collId}, Success: {ret}", applicationId, collId, retAdd);

            return Task.CompletedTask;
        }).ConfigureAwait(false);
    }

    public async Task<Guid> GetOrCreateEmptyCollectionAsync(ILogger logger)
    {
        if (!APIAvailable) return Guid.Empty;
        if (_emptyCollectionId != Guid.Empty) return _emptyCollectionId;

        _emptyCollectionTask ??= CreateTemporaryCollectionAsync(logger, "_EMPTY");
        _emptyCollectionId = await _emptyCollectionTask.ConfigureAwait(false);
        return _emptyCollectionId;
    }

    public async Task<bool> AssignEmptyCollectionAsync(ILogger logger, int idx)
    {
        var empty = await GetOrCreateEmptyCollectionAsync(logger).ConfigureAwait(false);
        if (empty == Guid.Empty) return false;

        return await AssignTemporaryCollectionAsync(logger, empty, idx).ConfigureAwait(false);
    }


    private void RedrawEvent(IntPtr objectAddress, int objectTableIndex)
    {
        bool wasRequested = false;
        if (_penumbraRedrawRequests.TryGetValue(objectAddress, out var redrawRequest) && redrawRequest)
        {
            _penumbraRedrawRequests[objectAddress] = false;
        }
        else
        {
            _mareMediator.Publish(new PenumbraRedrawMessage(objectAddress, objectTableIndex, wasRequested));
        }
    }


    private void ResourceLoaded(IntPtr ptr, string arg1, string arg2)
    {
        if (ptr != IntPtr.Zero && string.Compare(arg1, arg2, ignoreCase: true, System.Globalization.CultureInfo.InvariantCulture) != 0)
        {
            _mareMediator.Publish(new PenumbraResourceLoadMessage(ptr, arg1, arg2));
        }
    }

    private void PenumbraDispose()
    {
        _redrawManager.Cancel();
        _mareMediator.Publish(new PenumbraDisposedMessage());
    }

    private void PenumbraInit()
    {
        APIAvailable = true;
        ModDirectory = _penumbraResolveModDir.Invoke();
        _mareMediator.Publish(new PenumbraInitializedMessage());
    }
}
