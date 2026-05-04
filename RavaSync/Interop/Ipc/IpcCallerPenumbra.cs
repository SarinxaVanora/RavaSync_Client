using Dalamud.Game.ClientState.Objects.SubKinds;
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
using Dalamud.Game.ClientState.Objects.Types;
using System.Diagnostics;
using RavaSync.Services.Optimisation;

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
        public long LastCompletedTick;
    }

    private sealed class PendingRedrawAck
    {
        public PendingRedrawAck(Guid id, nint address, int objectIndex)
        {
            Id = id;
            Address = address;
            ObjectIndex = objectIndex;
        }

        public Guid Id { get; }
        public nint Address { get; }
        public int ObjectIndex { get; }
        public TaskCompletionSource<bool> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly ConcurrentDictionary<nint, RedrawGate> _redrawGates = new();
    private readonly ConcurrentDictionary<Guid, PendingRedrawAck> _pendingRedrawAcks = new();
    private static readonly SemaphoreSlim PenumbraFrameworkIpcGate = new(1, 1);
    private static long _nextPenumbraFrameworkIpcTick;
    // Keep Penumbra framework IPC serialized, but do not add a near-frame of idle time
    // between every pair during cached room-entry applies. If hitches return, raise to 30.
    private const int PenumbraFrameworkIpcSpacingMs = 20;

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

    private readonly EventSubscriber _penumbraDispose;
    private readonly EventSubscriber<nint, string, string> _penumbraGameObjectResourcePathResolved;
    private readonly EventSubscriber _penumbraInit;
    private readonly EventSubscriber<ModSettingChange, Guid, string, bool> _penumbraModSettingChanged;
    private readonly EventSubscriber<nint, int> _penumbraObjectIsRedrawn;

    private readonly AddTemporaryMod _penumbraAddTemporaryMod;
    private readonly AssignTemporaryCollection _penumbraAssignTemporaryCollection;
    private readonly AddMod _penumbraAddMod;
    private readonly ConvertTextureFile _penumbraConvertTextureFile;
    private readonly CreateTemporaryCollection _penumbraCreateNamedTemporaryCollection;
    private readonly GetEnabledState _penumbraEnabled;
    private readonly GetCollectionForObject _penumbraGetCollectionForObject;
    private readonly GetAllModSettings _penumbraGetAllModSettings;
    private readonly GetPlayerMetaManipulations _penumbraGetMetaManipulations;
    private readonly RedrawObject _penumbraRedraw;
    private readonly ReloadMod _penumbraReloadMod;
    private readonly DeleteTemporaryCollection _penumbraRemoveTemporaryCollection;
    private readonly RemoveTemporaryMod _penumbraRemoveTemporaryMod;
    private readonly TrySetMod _penumbraTrySetMod;
    private readonly TrySetModPriority _penumbraTrySetModPriority;
    private readonly TrySetModSetting _penumbraTrySetModSetting;
    private readonly TrySetModSettings _penumbraTrySetModSettings;
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
        _penumbraReloadMod = new ReloadMod(pi);
        _penumbraObjectIsRedrawn = GameObjectRedrawn.Subscriber(pi, RedrawEvent);
        _penumbraGetMetaManipulations = new GetPlayerMetaManipulations(pi);
        _penumbraRemoveTemporaryMod = new RemoveTemporaryMod(pi);
        _penumbraAddTemporaryMod = new AddTemporaryMod(pi);
        _penumbraAddMod = new AddMod(pi);
        _penumbraCreateNamedTemporaryCollection = new CreateTemporaryCollection(pi);
        _penumbraRemoveTemporaryCollection = new DeleteTemporaryCollection(pi);
        _penumbraAssignTemporaryCollection = new AssignTemporaryCollection(pi);
        _penumbraResolvePaths = new ResolvePlayerPathsAsync(pi);
        _penumbraEnabled = new GetEnabledState(pi);
        _penumbraGetCollectionForObject = new GetCollectionForObject(pi);
        _penumbraGetAllModSettings = new GetAllModSettings(pi);
        _penumbraTrySetMod = new TrySetMod(pi);
        _penumbraTrySetModPriority = new TrySetModPriority(pi);
        _penumbraTrySetModSetting = new TrySetModSetting(pi);
        _penumbraTrySetModSettings = new TrySetModSettings(pi);
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
                }, ct, isExplicitRedraw: true);
            });
        });

        Mediator.Subscribe<PenumbraRedrawAddressMessage>(this, (msg) =>
        {
            _ = SafeIpc.TryRun(Logger, "Penumbra.Redraw.Address", TimeSpan.FromSeconds(2), async ct =>
            {
                var obj = await _dalamudUtil.CreateGameObjectAsync(msg.Address).ConfigureAwait(false);
                if (obj is not ICharacter character) return;
                await _redrawManager.ExternalPenumbraRedrawAsync(Logger, character, Guid.NewGuid(), c =>
                {
                    _penumbraRedraw.Invoke(c.ObjectIndex, RedrawType.Redraw);
                }, ct, isExplicitRedraw: true).ConfigureAwait(false);
            });
        });
    }


    public sealed record PenumbraModSettingState(bool Enabled, int Priority, Dictionary<string, List<string>> Settings, bool Inherited, bool Temporary);

    public sealed record PenumbraCollectionModSettings(Guid CollectionId, string CollectionName,
        Dictionary<string, PenumbraModSettingState> Mods);

    public async Task<PenumbraCollectionModSettings?> GetLocalPlayerCollectionModSettingsAsync(ILogger logger, int gameObjectIndex)
    {
        if (!APIAvailable || gameObjectIndex < 0) return null;

        PenumbraCollectionModSettings? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetLocalPlayerCollectionModSettings", TimeSpan.FromSeconds(2), async ct =>
        {
            var collectionInfo = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                return _penumbraGetCollectionForObject.Invoke(gameObjectIndex);
            }).ConfigureAwait(false);

            if (!collectionInfo.ObjectValid || collectionInfo.EffectiveCollection.Id == Guid.Empty)
            {
                logger.LogTrace("[Penumbra] No valid effective collection for object index {index}", gameObjectIndex);
                return;
            }

            var (ec, settings) = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                return _penumbraGetAllModSettings.Invoke(collectionInfo.EffectiveCollection.Id);
            }).ConfigureAwait(false);

            if (ec != PenumbraApiEc.Success || settings == null)
            {
                logger.LogDebug("[Penumbra] GetAllModSettings failed for collection {collection} ({ec})", collectionInfo.EffectiveCollection.Id, ec);
                return;
            }

            var mapped = new Dictionary<string, PenumbraModSettingState>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in settings)
            {
                mapped[kv.Key] = new PenumbraModSettingState(
                    kv.Value.Item1,
                    kv.Value.Item2,
                    kv.Value.Item3 ?? new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase),
                    kv.Value.Item4,
                    kv.Value.Item5);
            }

            result = new PenumbraCollectionModSettings(
                collectionInfo.EffectiveCollection.Id,
                collectionInfo.EffectiveCollection.Name ?? string.Empty,
                mapped);
        }).ConfigureAwait(false);

        return result;
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
    private void OnPenumbraModSettingChanged(ModSettingChange change, Guid collectionId, string modName, bool inherited)
    {
        if (LocalPapSafetyModService.IsManagedRuntimeModIdentifier(modName))
            return;

        _mareMediator.Publish(new PenumbraModSettingChangedMessage(collectionId, modName, inherited, change.ToString()));
    }

    private async Task<T> RunPacedPenumbraFrameworkIpcAsync<T>(ILogger logger, string operationName, Func<T> action, CancellationToken token, int warnAfterMs = 60)
    {
        await PenumbraFrameworkIpcGate.WaitAsync(token).ConfigureAwait(false);

        try
        {
            var delayMs = unchecked(Interlocked.Read(ref _nextPenumbraFrameworkIpcTick) - Environment.TickCount64);
            if (delayMs > 0 && delayMs < 1000)
                await Task.Delay((int)delayMs, token).ConfigureAwait(false);

            return await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    return action();
                }
                finally
                {
                    sw.Stop();
                    if (sw.ElapsedMilliseconds >= warnAfterMs)
                        logger.LogWarning("[Penumbra IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, sw.ElapsedMilliseconds);
                    else
                        logger.LogTrace("[Penumbra IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, sw.ElapsedMilliseconds);
                }
            }).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _nextPenumbraFrameworkIpcTick, Environment.TickCount64 + PenumbraFrameworkIpcSpacingMs);
            PenumbraFrameworkIpcGate.Release();
        }
    }

    private Task RunPacedPenumbraFrameworkIpcAsync(ILogger logger, string operationName, Action action, CancellationToken token, int warnAfterMs = 60)
        => RunPacedPenumbraFrameworkIpcAsync(logger, operationName, () =>
        {
            action();
            return true;
        }, token, warnAfterMs);

    public async Task<bool> AssignTemporaryCollectionAsync(ILogger logger, Guid collName, int idx)
    {
        if (!APIAvailable) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignTemporaryCollection", TimeSpan.FromSeconds(2), async ct =>
        {
            assigned = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AssignTemporaryCollection(force)", () =>
            {
                var ecForce = _penumbraAssignTemporaryCollection.Invoke(collName, idx, forceAssignment: true);
                if (ecForce == PenumbraApiEc.Success)
                {
                    logger.LogTrace("[Penumbra] Assigned temp collection {collection} to idx {idx} (forced)", collName, idx);
                    return true;
                }

                logger.LogDebug("[Penumbra] Failed to assign temp collection {collection} to idx {idx}, ret={ret}", collName, idx, ecForce);
                return false;
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return assigned;
    }

    public async Task ConvertTextureFiles(ILogger logger, Dictionary<string, string[]> textures, IProgress<(string, int)> progress, CancellationToken token)
    {
        var plan = new Dictionary<TextureType, Dictionary<string, string[]>>()
        {
            [TextureType.Bc7Tex] = textures ?? new Dictionary<string, string[]>(),
        };

        await ConvertTextureFiles(logger, plan, progress, token).ConfigureAwait(false);
    }
    public async Task ConvertTextureFiles(ILogger logger, Dictionary<TextureType, Dictionary<string, string[]>> texturesByTarget, IProgress<(string, int)> progress, CancellationToken token)
    {
        if (!APIAvailable) return;

        _mareMediator.Publish(new HaltScanMessage(nameof(ConvertTextureFiles)));

        static string Norm(string p) => (p ?? string.Empty).Replace('\\', '/');

        static bool ShouldUseMipMaps(string path)
        {
            var p = Norm(path);
            if (string.IsNullOrWhiteSpace(p)) return true;

            bool isUiLike =
                p.Contains("/ui/", StringComparison.OrdinalIgnoreCase) ||
                p.Contains("icon", StringComparison.OrdinalIgnoreCase) ||
                p.Contains("hud", StringComparison.OrdinalIgnoreCase);

            if (isUiLike)
                return false;

            return true;
        }

        try
        {
            int total = 0;
            foreach (var grp in texturesByTarget.Values)
                total += grp?.Count ?? 0;

            int current = 0;

            foreach (var kvType in texturesByTarget)
            {
                if (token.IsCancellationRequested) break;

                var type = kvType.Key;
                var textures = kvType.Value;
                if (textures == null || textures.Count == 0) continue;

                foreach (var texture in textures)
                {
                    if (token.IsCancellationRequested) break;

                    var path = texture.Key;
                    bool mipMaps = ShouldUseMipMaps(path);

                    progress.Report((path, ++current));

                    var ok = await SafeIpc.TryRun(Logger, "Penumbra.ConvertTextureFile", TimeSpan.FromSeconds(30), async ct =>
                    {
                        var convertTask = _penumbraConvertTextureFile.Invoke(path, path, type, mipMaps: mipMaps);
                        await convertTask.ConfigureAwait(false);
                    }).ConfigureAwait(false);

                    if (ok && texture.Value != null && texture.Value.Any())
                    {
                        foreach (var dup in texture.Value)
                        {
                            try
                            {
                                File.Copy(path, dup, overwrite: true);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Failed to copy duplicate {dup}", dup);
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            await FinalizeTextureWriteAsync(nameof(ConvertTextureFiles)).ConfigureAwait(false);
        }
    }

    public async Task FinalizeTextureWriteAsync(string source)
    {
        if (!APIAvailable) return;

        _mareMediator.Publish(new ResumeScanMessage(source));

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

    public async Task<bool> RedrawAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token, bool criticalRedraw = false)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning)
        {
            logger.LogTrace("[{appid}] Penumbra redraw skipped for {name}: APIAvailable={apiAvailable}, IsZoning={isZoning}", applicationId, handler.Name, APIAvailable, _dalamudUtil.IsZoning);
            return false;
        }

        var gate = _redrawGates.GetOrAdd(handler.Address, _ => new RedrawGate());

        if (!criticalRedraw)
        {
            var nowTick = Environment.TickCount64;
            var lastCompletedTick = Interlocked.Read(ref gate.LastCompletedTick);
            if (lastCompletedTick > 0 && unchecked(nowTick - lastCompletedTick) >= 0 && unchecked(nowTick - lastCompletedTick) < 350)
            {
                logger.LogTrace("[{appid}] Redraw completed very recently for {name}, coalescing late duplicate request", applicationId, handler.Name);
                return false;
            }

            if (Interlocked.Exchange(ref gate.InFlight, 1) == 1)
            {
                Interlocked.Exchange(ref gate.Pending, 1);
                logger.LogTrace("[{appid}] Redraw already scheduled/in flight for {name}, coalescing duplicate request", applicationId, handler.Name);
                return false;
            }
        }
        else
        {
            while (Interlocked.CompareExchange(ref gate.InFlight, 1, 0) != 0)
            {
                Interlocked.Exchange(ref gate.Pending, 1);
                await Task.Delay(15, token).ConfigureAwait(false);
            }
        }

        var redrawFired = false;

        try
        {
            Interlocked.Exchange(ref gate.Pending, 0);

            try
            {
                redrawFired = await _redrawManager.PenumbraRedrawInternalAsync(logger, handler, applicationId, chara =>
                {
                    logger.LogDebug("[{appid}] Calling on IPC: PenumbraRedraw", applicationId);
                    var frameworkStopwatch = Stopwatch.StartNew();

                    try
                    {
                        _penumbraRedraw!.Invoke(chara.ObjectIndex, RedrawType.Redraw);
                        redrawFired = true;
                    }
                    catch (Exception ex)
                    {
                        redrawFired = false;
                        logger.LogError(ex, "Penumbra.Redraw threw");
                    }
                    finally
                    {
                        frameworkStopwatch.Stop();
                        if (frameworkStopwatch.ElapsedMilliseconds >= 60)
                        {
                            logger.LogWarning("[{appid}] PenumbraRedraw for {name} took {elapsed}ms on framework", applicationId, handler.Name, frameworkStopwatch.ElapsedMilliseconds);
                        }
                    }

                }, token, isExplicitRedraw: true).ConfigureAwait(false) && redrawFired;
            }
            catch (OperationCanceledException)
            {
                logger.LogDebug("[{appid}] Penumbra redraw cancelled for {name}", applicationId, handler.Name);
                redrawFired = false;
            }

            return redrawFired;
        }
        finally
        {
            Interlocked.Exchange(ref gate.Pending, 0);
            if (redrawFired)
                Interlocked.Exchange(ref gate.LastCompletedTick, Environment.TickCount64);
            Interlocked.Exchange(ref gate.InFlight, 0);
        }
    }



    public async Task<bool> RedrawDirectAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning)
        {
            logger.LogTrace("[{appid}] Direct Penumbra redraw skipped for {name}: APIAvailable={apiAvailable}, IsZoning={isZoning}", applicationId, handler.Name, APIAvailable, _dalamudUtil.IsZoning);
            return false;
        }

        token.ThrowIfCancellationRequested();

        return await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            var frameworkStopwatch = Stopwatch.StartNew();
            try
            {
                if (handler.GetGameObject() is not ICharacter chara)
                {
                    logger.LogTrace("[{appid}] Direct Penumbra redraw skipped for {name}: no live character", applicationId, handler.Name);
                    return false;
                }

                logger.LogDebug("[{appid}] Calling on IPC: PenumbraRedrawDirect idx={idx} addr={addr:X}", applicationId, chara.ObjectIndex, (nint)chara.Address);
                _penumbraRedraw!.Invoke(chara.ObjectIndex, RedrawType.Redraw);
                return true;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{appid}] Direct Penumbra redraw failed for {name}", applicationId, handler.Name);
                return false;
            }
            finally
            {
                frameworkStopwatch.Stop();
                if (frameworkStopwatch.ElapsedMilliseconds >= 60)
                {
                    logger.LogWarning("[{appid}] PenumbraRedrawDirect for {name} took {elapsed}ms on framework", applicationId, handler.Name, frameworkStopwatch.ElapsedMilliseconds);
                }
            }
        }).ConfigureAwait(false);
    }

    public async Task<bool> RedrawDirectAndWaitAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, CancellationToken token)
    {
        if (!APIAvailable || _dalamudUtil.IsZoning)
        {
            logger.LogTrace("[{appid}] Confirmed direct Penumbra redraw skipped for {name}: APIAvailable={apiAvailable}, IsZoning={isZoning}", applicationId, handler.Name, APIAvailable, _dalamudUtil.IsZoning);
            return false;
        }

        token.ThrowIfCancellationRequested();

        PendingRedrawAck? pending = null;
        var invoked = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            var frameworkStopwatch = Stopwatch.StartNew();
            try
            {
                if (handler.GetGameObject() is not ICharacter chara)
                {
                    logger.LogTrace("[{appid}] Confirmed direct Penumbra redraw skipped for {name}: no live character", applicationId, handler.Name);
                    return false;
                }

                var address = (nint)chara.Address;
                var objectIndex = chara.ObjectIndex;
                pending = RegisterRedrawAckWaiter(address, objectIndex);

                logger.LogDebug("[{appid}] Calling on IPC: PenumbraRedrawDirectConfirmed idx={idx} addr={addr:X}", applicationId, objectIndex, address);
                _penumbraRedraw!.Invoke(objectIndex, RedrawType.Redraw);
                return true;
            }
            catch (Exception ex)
            {
                if (pending != null)
                {
                    _pendingRedrawAcks.TryRemove(pending.Id, out _);
                    pending = null;
                }

                logger.LogWarning(ex, "[{appid}] Confirmed direct Penumbra redraw failed for {name}", applicationId, handler.Name);
                return false;
            }
            finally
            {
                frameworkStopwatch.Stop();
                if (frameworkStopwatch.ElapsedMilliseconds >= 60)
                {
                    logger.LogWarning("[{appid}] PenumbraRedrawDirectConfirmed for {name} took {elapsed}ms on framework", applicationId, handler.Name, frameworkStopwatch.ElapsedMilliseconds);
                }
            }
        }).ConfigureAwait(false);

        if (!invoked || pending == null)
            return false;

        try
        {
            var timeoutTask = Task.Delay(TimeSpan.FromMilliseconds(900), token);
            var completed = await Task.WhenAny(pending.Completion.Task, timeoutTask).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();

            if (completed == pending.Completion.Task && await pending.Completion.Task.ConfigureAwait(false))
            {
                logger.LogDebug("[{appid}] Penumbra redraw acknowledged for {name}: idx={idx} addr={addr:X}", applicationId, handler.Name, pending.ObjectIndex, pending.Address);
                return true;
            }

            logger.LogTrace("[{appid}] Penumbra redraw acknowledgement timed out for {name}: idx={idx} addr={addr:X}", applicationId, handler.Name, pending.ObjectIndex, pending.Address);
            return false;
        }
        finally
        {
            _pendingRedrawAcks.TryRemove(pending.Id, out _);
        }
    }

    private PendingRedrawAck RegisterRedrawAckWaiter(nint address, int objectIndex)
    {
        var pending = new PendingRedrawAck(Guid.NewGuid(), address, objectIndex);
        _pendingRedrawAcks[pending.Id] = pending;
        return pending;
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

        await SafeIpc.TryRun(Logger, "Penumbra.SetManipulationData", TimeSpan.FromSeconds(2), async ct =>
        {
            logger.LogTrace("[{applicationId}] Manip: {data}", applicationId, manipulationData);

            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AddTemporaryMod(Meta)", () =>
            {
                var retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, [], manipulationData, 0);
                logger.LogTrace("[{applicationId}] Setting temp meta mod for {collId}, Success: {ret}", applicationId, collId, retAdd);

                if (retAdd == PenumbraApiEc.Success)
                    return;
                var retRem = _penumbraRemoveTemporaryMod.Invoke("MareChara_Meta", collId, 0);
                logger.LogTrace("[{applicationId}] Replace fallback: removing existing temp meta mod for {collId}, ret={ret}", applicationId, collId, retRem);

                retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, [], manipulationData, 0);
                logger.LogTrace("[{applicationId}] Replace fallback: setting temp meta mod for {collId}, Success: {ret}", applicationId, collId, retAdd);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }
    public async Task ClearManipulationDataAsync(ILogger logger, Guid applicationId, Guid collId)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.RemoveTemporaryMod(Meta)", TimeSpan.FromSeconds(2), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.RemoveTemporaryMod(Meta)", () =>
            {
                var retRem = _penumbraRemoveTemporaryMod.Invoke("MareChara_Meta", collId, 0);
                logger.LogTrace("[{applicationId}] Cleared meta (height) from temp collection {collId}, ret={ret}", applicationId, collId, retRem);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public Task<bool> SetTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, Dictionary<string, string> modPaths)
        => SetNamedTemporaryModsAsync(logger, applicationId, collId, "MareChara_Files", modPaths, 0);

    public Task<bool> SetNamedTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, string tempModName, Dictionary<string, string> modPaths)
        => SetNamedTemporaryModsAsync(logger, applicationId, collId, tempModName, modPaths, 0);

    public async Task<bool> SetNamedTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, string tempModName, Dictionary<string, string> modPaths, int priority)
    {
        if (!APIAvailable) return false;
        if (string.IsNullOrWhiteSpace(tempModName)) return false;

        var applied = false;

        var ipcOk = await SafeIpc.TryRun(Logger, "Penumbra.SetTemporaryMods", TimeSpan.FromSeconds(2), async ct =>
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                foreach (var mod in modPaths)
                {
                    logger.LogTrace("[{applicationId}] {tempModName}: {from} => {to}", applicationId, tempModName, mod.Key, mod.Value);
                }
            }

            var retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({tempModName})", () =>
            {
                var ret = _penumbraAddTemporaryMod.Invoke(tempModName, collId, modPaths, string.Empty, priority);
                logger.LogTrace("[{applicationId}] Setting temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, ret);
                return ret;
            }, ct).ConfigureAwait(false);

            if (retAdd != PenumbraApiEc.Success)
            {
                await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.RemoveTemporaryMod({tempModName})", () =>
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(tempModName, collId, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: removing temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, retRemove);
                }, ct).ConfigureAwait(false);

                retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({tempModName})Fallback", () =>
                {
                    var ret = _penumbraAddTemporaryMod.Invoke(tempModName, collId, modPaths, string.Empty, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: setting temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, ret);
                    return ret;
                }, ct).ConfigureAwait(false);
            }

            applied = retAdd == PenumbraApiEc.Success;
        }).ConfigureAwait(false);

        return ipcOk && applied;
    }

    public async Task ClearNamedTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, string tempModName, int priority = 0)
    {
        if (!APIAvailable) return;
        if (string.IsNullOrWhiteSpace(tempModName)) return;

        await SafeIpc.TryRun(Logger, "Penumbra.ClearTemporaryMods", TimeSpan.FromSeconds(2), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.ClearTemporaryMods({tempModName}@{priority})", () =>
            {
                var retRemove = _penumbraRemoveTemporaryMod.Invoke(tempModName, collId, priority);
                logger.LogTrace("[{applicationId}] Clearing temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, retRemove);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task ClearNamedTemporaryModsPriorityRangeAsync(ILogger logger, Guid applicationId, Guid collId, IReadOnlyCollection<string> tempModNames, int fromPriorityInclusive, int toPriorityInclusive, string? keepTempModName = null, int keepPriority = int.MinValue)
    {
        if (!APIAvailable) return;
        if (collId == Guid.Empty) return;
        if (tempModNames == null || tempModNames.Count == 0) return;

        var fromPriority = Math.Max(0, fromPriorityInclusive);
        var toPriority = Math.Max(fromPriority, toPriorityInclusive);

        await SafeIpc.TryRun(Logger, "Penumbra.ClearTemporaryModsPriorityRange", TimeSpan.FromSeconds(5), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.ClearTemporaryModsRange({fromPriority}-{toPriority})", () =>
            {
                var removed = 0;
                var attempted = 0;

                foreach (var tempModName in tempModNames)
                {
                    if (string.IsNullOrWhiteSpace(tempModName))
                        continue;

                    for (var priority = fromPriority; priority <= toPriority; priority++)
                    {
                        if (priority == keepPriority && string.Equals(tempModName, keepTempModName, StringComparison.Ordinal))
                            continue;

                        attempted++;

                        var retRemove = _penumbraRemoveTemporaryMod.Invoke(tempModName, collId, priority);
                        if (retRemove == PenumbraApiEc.Success)
                            removed++;
                    }
                }

                logger.LogDebug(
                    "[{applicationId}] Swept legacy temp files mods for {collId}: names=[{names}], priorities={from}-{to}, keep={keepName}@{keepPriority}, attempted={attempted}, removed={removed}",
                    applicationId,
                    collId,
                    string.Join(", ", tempModNames),
                    fromPriority,
                    toPriority,
                    keepTempModName ?? string.Empty,
                    keepPriority,
                    attempted,
                    removed);
            }, ct, warnAfterMs: 100).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task<PenumbraApiEc> AddModAsync(ILogger logger, string modDirectory)
    {
        if (!APIAvailable || string.IsNullOrWhiteSpace(modDirectory)) return PenumbraApiEc.UnknownError;

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.AddMod", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraAddMod.Invoke(modDirectory);
            logger.LogTrace("[Penumbra] AddMod {modDirectory} => {result}", modDirectory, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<PenumbraApiEc> ReloadModAsync(ILogger logger, string modDirectory, string modName = "")
    {
        if (!APIAvailable || string.IsNullOrWhiteSpace(modDirectory)) return PenumbraApiEc.UnknownError;

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.ReloadMod", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraReloadMod.Invoke(modDirectory, modName);
            logger.LogTrace("[Penumbra] ReloadMod {modDirectory} ({modName}) => {result}", modDirectory, modName, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<PenumbraApiEc> SetModStateAsync(ILogger logger, Guid collectionId, string modDirectory, bool enabled, string modName = "")
    {
        if (!APIAvailable || collectionId == Guid.Empty || string.IsNullOrWhiteSpace(modDirectory)) return PenumbraApiEc.UnknownError;

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.TrySetMod", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraTrySetMod.Invoke(collectionId, modDirectory, enabled, modName);
            logger.LogTrace("[Penumbra] TrySetMod {modDirectory} enabled={enabled} collection={collectionId} => {result}", modDirectory, enabled, collectionId, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<PenumbraApiEc> SetModPriorityAsync(ILogger logger, Guid collectionId, string modDirectory, int priority, string modName = "")
    {
        if (!APIAvailable || collectionId == Guid.Empty || string.IsNullOrWhiteSpace(modDirectory)) return PenumbraApiEc.UnknownError;

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.TrySetModPriority", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraTrySetModPriority.Invoke(collectionId, modDirectory, priority, modName);
            logger.LogTrace("[Penumbra] TrySetModPriority {modDirectory} priority={priority} collection={collectionId} => {result}", modDirectory, priority, collectionId, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<PenumbraApiEc> SetModSettingAsync(ILogger logger, Guid collectionId, string modDirectory, string optionGroupName, string optionName, string modName = "")
    {
        if (!APIAvailable || collectionId == Guid.Empty || string.IsNullOrWhiteSpace(modDirectory) || string.IsNullOrWhiteSpace(optionGroupName))
            return PenumbraApiEc.UnknownError;

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.TrySetModSetting", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraTrySetModSetting.Invoke(collectionId, modDirectory, optionGroupName, optionName, modName);
            logger.LogTrace("[Penumbra] TrySetModSetting {modDirectory} {group}={option} collection={collectionId} => {result}", modDirectory, optionGroupName, optionName, collectionId, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<PenumbraApiEc> SetModSettingsAsync(ILogger logger, Guid collectionId, string modDirectory, string optionGroupName, IReadOnlyCollection<string> optionNames, string modName = "")
    {
        if (!APIAvailable || collectionId == Guid.Empty || string.IsNullOrWhiteSpace(modDirectory) || string.IsNullOrWhiteSpace(optionGroupName))
            return PenumbraApiEc.UnknownError;

        var selectedOptions = optionNames
            .Where(o => !string.IsNullOrWhiteSpace(o))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        PenumbraApiEc result = PenumbraApiEc.UnknownError;
        await SafeIpc.TryRun(Logger, "Penumbra.TrySetModSettings", TimeSpan.FromSeconds(2), ct =>
        {
            result = _penumbraTrySetModSettings.Invoke(collectionId, modDirectory, optionGroupName, selectedOptions, modName);
            logger.LogTrace("[Penumbra] TrySetModSettings {modDirectory} {group}=[{options}] collection={collectionId} => {result}", modDirectory, optionGroupName, string.Join(", ", selectedOptions), collectionId, result);
            return Task.CompletedTask;
        }).ConfigureAwait(false);

        return result;
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
        var address = (nint)objectAddress;
        foreach (var waiter in _pendingRedrawAcks.Values)
        {
            if ((waiter.ObjectIndex >= 0 && waiter.ObjectIndex == objectTableIndex)
                || (waiter.Address != nint.Zero && waiter.Address == address))
            {
                waiter.Completion.TrySetResult(true);
            }
        }

        var wasRequested = _redrawManager.TryConsumeRequestedRedraw(objectAddress);
        _mareMediator.Publish(new PenumbraRedrawMessage(objectAddress, objectTableIndex, wasRequested));
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
