using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Plugin;
using Dalamud.Utility;
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
using RavaSync.Utils;

namespace RavaSync.Interop.Ipc;

public sealed class IpcCallerPenumbra : DisposableMediatorSubscriberBase, IIpcCaller
{
    public const string MountMusicTemporaryModName = "RavaSync - Mount music";

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
    private static readonly bool IsWineRuntime = SafeIsWine();
    private static readonly SemaphoreSlim PenumbraFrameworkIpcGate = new(1, 1);
    private static readonly SemaphoreSlim RavaSyncGlobalTemporaryCollectionWipeGate = new(1, 1);
    private static long _nextPenumbraFrameworkIpcTick;
    // Keep Penumbra framework IPC serialized so room-entry applies do not stack on one frame.
    // Windows keeps its original fixed spacing. Linux/Wine adapts only after an IPC call proves expensive.
    private const int PenumbraFrameworkIpcSpacingMs = 24;

    private static bool SafeIsWine()
    {
        try { return Util.IsWine(); }
        catch { return false; }
    }

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
    private readonly GetCollection _penumbraGetCollection;
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
    private readonly GetPlayerResourcePaths _penumbraPlayerResourcePaths;
    private readonly ConcurrentDictionary<Guid, string> _ravaSyncTemporaryCollections = new();


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
        _penumbraGetCollection = new GetCollection(pi);
        _penumbraGetAllModSettings = new GetAllModSettings(pi);
        _penumbraTrySetMod = new TrySetMod(pi);
        _penumbraTrySetModPriority = new TrySetModPriority(pi);
        _penumbraTrySetModSetting = new TrySetModSetting(pi);
        _penumbraTrySetModSettings = new TrySetModSettings(pi);
        _penumbraConvertTextureFile = new ConvertTextureFile(pi);
        _penumbraResourcePaths = new GetGameObjectResourcePaths(pi);
        _penumbraPlayerResourcePaths = new GetPlayerResourcePaths(pi);
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

    public async Task<(Guid Id, string Name)?> GetCollectionAsync(ILogger logger, ApiCollectionType type)
    {
        if (!APIAvailable) return null;

        (Guid Id, string Name)? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetCollection", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                return _penumbraGetCollection.Invoke(type);
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        if (result == null)
            logger.LogTrace("[Penumbra] GetCollection failed for {type}", type);

        return result;
    }

    public async Task<PenumbraCollectionModSettings?> GetCollectionModSettingsAsync(ILogger logger, Guid collectionId, string collectionName)
    {
        if (!APIAvailable || collectionId == Guid.Empty) return null;

        PenumbraCollectionModSettings? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetCollectionModSettings", TimeSpan.FromSeconds(2), async ct =>
        {
            var (ec, settings) = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                return _penumbraGetAllModSettings.Invoke(collectionId);
            }).ConfigureAwait(false);

            if (ec != PenumbraApiEc.Success || settings == null)
            {
                logger.LogDebug("[Penumbra] GetAllModSettings failed for collection {collection} ({ec})", collectionId, ec);
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

            result = new PenumbraCollectionModSettings(collectionId, collectionName ?? string.Empty, mapped);
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<bool> HasTemporaryModOnCollectionAsync(ILogger logger, Guid collectionId, string tempModName, int priority = 0)
    {
        if (!APIAvailable || collectionId == Guid.Empty || string.IsNullOrWhiteSpace(tempModName))
            return false;

        var settings = await GetCollectionModSettingsAsync(logger, collectionId, string.Empty).ConfigureAwait(false);
        if (settings?.Mods == null)
            return false;

        if (!settings.Mods.TryGetValue(tempModName, out var state))
            return false;

        return state.Enabled && state.Temporary && state.Priority == priority;
    }

    public async Task<PenumbraCollectionModSettings?> GetDefaultCollectionModSettingsAsync(ILogger logger)
    {
        var defaultCollection = await GetCollectionAsync(logger, ApiCollectionType.Default).ConfigureAwait(false);
        if (!defaultCollection.HasValue || defaultCollection.Value.Id == Guid.Empty)
            return null;

        return await GetCollectionModSettingsAsync(logger, defaultCollection.Value.Id, defaultCollection.Value.Name ?? string.Empty).ConfigureAwait(false);
    }


    public async Task<(bool Checked, bool Matches, Guid EffectiveCollectionId, string EffectiveCollectionName)> TryGetObjectEffectiveCollectionMatchAsync(ILogger logger, Guid expectedCollectionId, int idx, string? expectedIdent, nint expectedAddress, string? expectedDisplayName = null)
    {
        if (!APIAvailable || expectedCollectionId == Guid.Empty || idx < 0)
            return (false, false, Guid.Empty, string.Empty);

        (bool Checked, bool Matches, Guid EffectiveCollectionId, string EffectiveCollectionName) result = (false, false, Guid.Empty, string.Empty);

        await SafeIpc.TryRun(Logger, "Penumbra.GetCollectionForObject.Match", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.GetCollectionForObject(match)", () =>
            {
                if (!ValidateTemporaryCollectionAssignmentTargetOnFramework(logger, expectedCollectionId, idx, expectedIdent, expectedAddress, expectedDisplayName))
                    return (false, false, Guid.Empty, string.Empty);

                var collectionInfo = _penumbraGetCollectionForObject.Invoke(idx);
                if (!collectionInfo.ObjectValid)
                    return (true, false, Guid.Empty, string.Empty);

                var effectiveId = collectionInfo.EffectiveCollection.Id;
                var effectiveName = collectionInfo.EffectiveCollection.Name ?? string.Empty;
                return (true, effectiveId == expectedCollectionId, effectiveId, effectiveName);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return result;
    }

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
        if (LocalPapSafetyModService.IsManagedRuntimeModIdentifier(modName)
            || LocalPapSafetyModService.IsRavaSyncInternalTemporaryModIdentifier(modName))
        {
            return;
        }

        _mareMediator.Publish(new PenumbraModSettingChangedMessage(collectionId, modName, inherited, change.ToString()));
    }

    private async Task<T> RunPacedPenumbraFrameworkIpcAsync<T>(ILogger logger, string operationName, Func<T> action, CancellationToken token, int warnAfterMs = 60)
    {
        await PenumbraFrameworkIpcGate.WaitAsync(token).ConfigureAwait(false);
        long frameworkElapsedMs = 0;

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
                    frameworkElapsedMs = sw.ElapsedMilliseconds;
                    var effectiveWarnAfterMs = IsWineRuntime ? Math.Max(warnAfterMs, 250) : warnAfterMs;
                    if (frameworkElapsedMs >= effectiveWarnAfterMs)
                        logger.LogWarning("[Penumbra IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, frameworkElapsedMs);
                    else
                        logger.LogTrace("[Penumbra IPC HitchGuard] {operation} took {elapsed}ms on framework", operationName, frameworkElapsedMs);
                }
            }).ConfigureAwait(false);
        }
        finally
        {
            var spacingMs = LinuxSmoothMode.ComputeFrameworkIpcSpacing(PenumbraFrameworkIpcSpacingMs, frameworkElapsedMs, 240);
            Interlocked.Exchange(ref _nextPenumbraFrameworkIpcTick, Environment.TickCount64 + spacingMs);
            PenumbraFrameworkIpcGate.Release();
        }
    }

    private Task RunPacedPenumbraFrameworkIpcAsync(ILogger logger, string operationName, Action action, CancellationToken token, int warnAfterMs = 60)
        => RunPacedPenumbraFrameworkIpcAsync(logger, operationName, () =>
        {
            action();
            return true;
        }, token, warnAfterMs);


    public async Task<bool> AssignEmptyCollectionToVerifiedCharacterAsync(ILogger logger, int idx, string expectedIdent, nint expectedAddress, string? expectedDisplayName = null)
    {
        if (!APIAvailable) return false;
        if (idx < 0) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignEmptyCollectionVerified", TimeSpan.FromSeconds(10), async ct =>
        {
            assigned = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AssignEmptyCollectionVerified(force)", () =>
            {
                if (!ValidateTemporaryCollectionClearTargetOnFramework(logger, idx, expectedIdent, expectedAddress, expectedDisplayName))
                    return false;

                var ecForce = _penumbraAssignTemporaryCollection.Invoke(Guid.Empty, idx, forceAssignment: true);
                if (ecForce == PenumbraApiEc.Success)
                {
                    logger.LogTrace("[Penumbra] Cleared temporary collection assignment for verified idx {idx} ({name}/{ident})", idx, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
                    return true;
                }

                logger.LogDebug("[Penumbra] Failed to clear temporary collection assignment for idx {idx} ({name}/{ident}), ret={ret}", idx, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty, ecForce);
                return false;
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return assigned;
    }

    public async Task<bool> AssignEmptyCollectionIfAssignedCollectionMatchesAsync(ILogger logger, Guid applicationId, int idx, Guid expectedCollectionId, string? expectedDisplayName = null)
    {
        if (!APIAvailable) return false;
        if (idx < 0 || expectedCollectionId == Guid.Empty) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignEmptyCollectionIfCollectionMatches", TimeSpan.FromSeconds(10), async ct =>
        {
            assigned = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AssignEmptyCollectionIfCollectionMatches(force)", () =>
            {
                if (!_ravaSyncTemporaryCollections.ContainsKey(expectedCollectionId))
                    logger.LogDebug("[{applicationId}] Collection-match clear is using unregistered/stale collection {collection} at idx {idx}; continuing because teardown must remove captured receiver state", applicationId, expectedCollectionId, idx);

                if (IsProtectedUserCollectionOnFramework(expectedCollectionId, out var protectedReason))
                {
                    logger.LogWarning("[{applicationId}] Blocked collection-match clear for protected collection {collection} at idx {idx} ({reason})", applicationId, expectedCollectionId, idx, protectedReason);
                    return false;
                }

                ICharacter? target = null;
                try
                {
                    target = _dalamudUtil.GetCharacterFromObjectTableByIndex(idx);
                }
                catch
                {
                    target = null;
                }

                var targetAddress = target?.Address ?? nint.Zero;
                if (targetAddress != nint.Zero)
                {
                    var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                    if (localPlayerAddress != nint.Zero && targetAddress == localPlayerAddress)
                    {
                        logger.LogWarning("[{applicationId}] Blocked collection-match clear on local player at idx {idx}; expected remote {name}", applicationId, idx, expectedDisplayName ?? string.Empty);
                        return false;
                    }

                    var collectionInfo = _penumbraGetCollectionForObject.Invoke(idx);
                    if (!collectionInfo.ObjectValid || collectionInfo.EffectiveCollection.Id != expectedCollectionId)
                    {
                        logger.LogTrace("[{applicationId}] Collection-match clear skipped for idx {idx}; effective collection is {actual}, expected {expected}", applicationId, idx, collectionInfo.EffectiveCollection.Id, expectedCollectionId);
                        return false;
                    }
                }
                else
                {
                    logger.LogDebug("[{applicationId}] Clearing former RavaSync slot idx {idx} ({name}) even though no live object is present; this prevents stale assignment from reattaching on re-entry", applicationId, idx, expectedDisplayName ?? string.Empty);
                }

                var ecForce = _penumbraAssignTemporaryCollection.Invoke(Guid.Empty, idx, forceAssignment: true);
                if (ecForce == PenumbraApiEc.Success)
                {
                    logger.LogDebug("[{applicationId}] Cleared RavaSync temp collection {collection} from idx {idx} ({name}) by collection match", applicationId, expectedCollectionId, idx, expectedDisplayName ?? string.Empty);
                    return true;
                }

                logger.LogDebug("[{applicationId}] Failed collection-match clear of temp collection {collection} from idx {idx} ({name}), ret={ret}", applicationId, expectedCollectionId, idx, expectedDisplayName ?? string.Empty, ecForce);
                return false;
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return assigned;
    }


    public async Task<bool> AssignEmptyCollectionToSelfAsync(ILogger logger, int idx, nint expectedAddress, string? expectedDisplayName = null)
    {
        if (!APIAvailable) return false;
        if (idx < 0) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignEmptyCollectionSelf", TimeSpan.FromSeconds(10), async ct =>
        {
            assigned = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AssignEmptyCollectionSelf(force)", () =>
            {
                if (!ValidateTemporaryCollectionSelfClearTargetOnFramework(logger, idx, expectedAddress, expectedDisplayName))
                    return false;

                var ecForce = _penumbraAssignTemporaryCollection.Invoke(Guid.Empty, idx, forceAssignment: true);
                if (ecForce == PenumbraApiEc.Success)
                {
                    logger.LogTrace("[Penumbra] Cleared temporary collection assignment for self idx {idx} ({name})", idx, expectedDisplayName ?? string.Empty);
                    return true;
                }

                logger.LogDebug("[Penumbra] Failed to clear temporary collection assignment for self idx {idx} ({name}), ret={ret}", idx, expectedDisplayName ?? string.Empty, ecForce);
                return false;
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return assigned;
    }

    private bool ValidateTemporaryCollectionSelfClearTargetOnFramework(ILogger logger, int idx, nint expectedAddress, string? expectedDisplayName)
    {
        try
        {
            var target = _dalamudUtil.GetCharacterFromObjectTableByIndex(idx);
            var targetAddress = target?.Address ?? nint.Zero;

            if (targetAddress == nint.Zero)
            {
                logger.LogDebug("Skipped self temporary collection clear for {name}: idx {idx} no longer points at a valid player", expectedDisplayName ?? string.Empty, idx);
                return false;
            }

            var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
            var localPlayer = _dalamudUtil.GetPlayerCharacter();
            var localName = localPlayer?.Name.TextValue ?? string.Empty;
            var expectedName = string.IsNullOrWhiteSpace(expectedDisplayName) ? localName : expectedDisplayName;
            var targetName = target?.Name.TextValue ?? string.Empty;

            var isLocalPlayer = localPlayerAddress != nint.Zero && targetAddress == localPlayerAddress;
            var nameMatchesSelf = !string.IsNullOrWhiteSpace(expectedName) && string.Equals(targetName, expectedName, StringComparison.Ordinal);

            if (!isLocalPlayer && !nameMatchesSelf)
            {
                logger.LogWarning("Blocked self temporary collection clear at idx {idx}: target {targetName}/{addr:X} does not match local/self name {expectedName}", idx, targetName, targetAddress, expectedName);
                return false;
            }

            if (expectedAddress != nint.Zero && targetAddress != expectedAddress && !isLocalPlayer)
            {
                logger.LogTrace("Self temporary collection clear target for {name} moved from {oldAddr:X} to {newAddr:X}; accepting because this is the named GPose/local self", expectedName, expectedAddress, targetAddress);
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Blocked self temporary collection clear for idx {idx}; target validation failed", idx);
            return false;
        }
    }

    private bool ValidateTemporaryCollectionClearTargetOnFramework(ILogger logger, int idx, string? expectedIdent, nint expectedAddress, string? expectedDisplayName)
    {
        try
        {
            var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
            var target = _dalamudUtil.GetCharacterFromObjectTableByIndex(idx);
            var targetAddress = target?.Address ?? nint.Zero;

            if (targetAddress == nint.Zero)
            {
                logger.LogDebug("Skipped RavaSync receiver temporary collection clear for {name}/{ident}: idx {idx} no longer points at a valid player", expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty, idx);
                return false;
            }

            if (localPlayerAddress != nint.Zero && targetAddress == localPlayerAddress)
            {
                logger.LogWarning("Blocked RavaSync receiver temporary collection clear on the local player at idx {idx}; expected remote {name}/{ident}", idx, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
                return false;
            }

            var targetName = target?.Name.TextValue ?? string.Empty;
            var expectedName = expectedDisplayName ?? string.Empty;
            var nameMatches = !string.IsNullOrWhiteSpace(expectedName)
                && string.Equals(targetName, expectedName, StringComparison.OrdinalIgnoreCase);

            if (!string.IsNullOrWhiteSpace(expectedIdent) && !_dalamudUtil.AddressMatchesPlayerIdent(expectedIdent, targetAddress))
            {
                if (!nameMatches)
                {
                    logger.LogDebug("Skipped RavaSync receiver temporary collection clear for idx {idx}: target {targetName}/{addr:X} no longer matches expected {name}/{ident}", idx, targetName, targetAddress, expectedName, expectedIdent);
                    return false;
                }

                logger.LogDebug("Allowing RavaSync receiver temporary collection clear for idx {idx}: ident cache no longer matches {ident}, but live target name still matches {name}", idx, expectedIdent, expectedName);
            }

            if (expectedAddress != nint.Zero && targetAddress != expectedAddress)
            {
                logger.LogTrace("RavaSync receiver temporary collection clear target for {name}/{ident} moved from {oldAddr:X} to {newAddr:X}; accepting because ident still matches", expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty, expectedAddress, targetAddress);
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Blocked RavaSync receiver temporary collection clear for idx {idx}; target validation failed", idx);
            return false;
        }
    }

    public Task<bool> AssignTemporaryCollectionAsync(ILogger logger, Guid collName, int idx)
        => AssignTemporaryCollectionCoreAsync(logger, collName, idx, expectedIdent: null, expectedAddress: nint.Zero, expectedDisplayName: null, protectLocalPlayer: true);

    public Task<bool> AssignTemporaryCollectionToVerifiedCharacterAsync(ILogger logger, Guid collName, int idx, string expectedIdent, nint expectedAddress, string? expectedDisplayName = null)
        => AssignTemporaryCollectionCoreAsync(logger, collName, idx, expectedIdent, expectedAddress, expectedDisplayName, protectLocalPlayer: true);

    private async Task<bool> AssignTemporaryCollectionCoreAsync(ILogger logger, Guid collName, int idx, string? expectedIdent, nint expectedAddress, string? expectedDisplayName, bool protectLocalPlayer)
    {
        if (!APIAvailable) return false;
        if (collName == Guid.Empty || idx < 0) return false;

        bool assigned = false;

        await SafeIpc.TryRun(Logger, "Penumbra.AssignTemporaryCollection", TimeSpan.FromSeconds(10), async ct =>
        {
            assigned = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AssignTemporaryCollection(force)", () =>
            {
                if (protectLocalPlayer && !ValidateTemporaryCollectionAssignmentTargetOnFramework(logger, collName, idx, expectedIdent, expectedAddress, expectedDisplayName))
                    return false;

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

    private bool ValidateTemporaryCollectionAssignmentTargetOnFramework(ILogger logger, Guid collId, int idx, string? expectedIdent, nint expectedAddress, string? expectedDisplayName)
    {
        try
        {
            if (!_ravaSyncTemporaryCollections.ContainsKey(collId))
            {
                logger.LogWarning("Blocked RavaSync receiver temporary collection assignment for unregistered collection {collection} to idx {idx}; expected {name}/{ident}", collId, idx, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
                return false;
            }

            var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
            var targetObject = _dalamudUtil.GetObjectFromObjectTableByIndex(idx);
            var targetAddress = targetObject?.Address ?? nint.Zero;

            if (targetAddress == nint.Zero || targetObject is not ICharacter)
            {
                logger.LogDebug("Blocked RavaSync receiver temporary collection assignment for {name}/{ident}: idx {idx} no longer points at a valid character actor", expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty, idx);
                return false;
            }

            if (localPlayerAddress != nint.Zero && targetAddress == localPlayerAddress)
            {
                logger.LogWarning("Blocked RavaSync receiver temporary collection {collection} assignment to the local player at idx {idx}; expected remote {name}/{ident}", collId, idx, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
                return false;
            }

            if (!string.IsNullOrWhiteSpace(expectedIdent) && !_dalamudUtil.AddressMatchesPlayerIdent(expectedIdent, targetAddress))
            {
                logger.LogDebug("Blocked RavaSync receiver temporary collection {collection} assignment to idx {idx}: target addr {addr:X} no longer matches expected {name}/{ident}", collId, idx, targetAddress, expectedDisplayName ?? string.Empty, expectedIdent);
                return false;
            }

            if (expectedAddress != nint.Zero && targetAddress != expectedAddress)
            {
                if (string.IsNullOrWhiteSpace(expectedIdent))
                {
                    logger.LogDebug("Blocked RavaSync receiver temporary collection {collection} assignment to idx {idx}: target addr {addr:X} no longer matches expected actor addr {expectedAddr:X} for {name}", collId, idx, targetAddress, expectedAddress, expectedDisplayName ?? string.Empty);
                    return false;
                }

                logger.LogTrace("RavaSync receiver temporary collection assignment target for {name}/{ident} moved from {oldAddr:X} to {newAddr:X}; accepting because ident still matches", expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty, expectedAddress, targetAddress);
            }

            if (IsProtectedUserCollectionOnFramework(collId, out var protectedReason))
            {
                logger.LogWarning("Blocked RavaSync receiver temporary collection assignment using protected collection {collection} ({reason}) for {name}/{ident}", collId, protectedReason, expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Blocked RavaSync receiver temporary collection assignment for {name}/{ident}: validation failed", expectedDisplayName ?? string.Empty, expectedIdent ?? string.Empty);
            return false;
        }
    }

    private bool ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(ILogger logger, Guid applicationId, Guid collId, string operationName, string tempModName)
    {
        if (collId == Guid.Empty)
            return true;

        if (!LocalPapSafetyModService.IsRavaSyncInternalTemporaryModIdentifier(tempModName))
            return false;

        if (IsProtectedUserCollectionOnFramework(collId, out var protectedReason))
        {
            logger.LogWarning("[{applicationId}] Blocked {operation} for RavaSync temporary mod {tempModName} on protected collection {collection} ({reason}). Receiver data must never mutate Default/Local Player collections.", applicationId, operationName, tempModName, collId, protectedReason);
            return true;
        }

        if (!_ravaSyncTemporaryCollections.ContainsKey(collId))
        {
            logger.LogWarning("[{applicationId}] Blocked {operation} for RavaSync temporary mod {tempModName} on unregistered collection {collection}. Receiver temp mods may only target collections created by RavaSync for that apply.", applicationId, operationName, tempModName, collId);
            return true;
        }

        return false;
    }

    private bool ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(ILogger logger, Guid applicationId, Guid collId, string operationName, IEnumerable<string> tempModNames)
    {
        foreach (var tempModName in tempModNames)
        {
            if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, operationName, tempModName))
                return true;
        }

        return false;
    }

    private bool IsProtectedUserCollectionOnFramework(Guid collId, out string reason)
    {
        reason = string.Empty;

        if (collId == Guid.Empty)
            return false;

        try
        {
            var defaultCollection = _penumbraGetCollection.Invoke(ApiCollectionType.Default);
            if (defaultCollection.HasValue && defaultCollection.Value.Id != Guid.Empty && defaultCollection.Value.Id == collId)
            {
                reason = "Default";
                return true;
            }
        }
        catch
        {
            // If Penumbra cannot answer this during a shutdown/race, fall through to the local-player check.
        }

        try
        {
            var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
            if (localPlayerAddress == nint.Zero)
                return false;

            var localObj = _dalamudUtil.CreateGameObject(localPlayerAddress);
            var localIndex = localObj?.ObjectIndex ?? -1;
            if (localIndex < 0)
                return false;

            var localCollection = _penumbraGetCollectionForObject.Invoke(localIndex);
            if (localCollection.ObjectValid && localCollection.EffectiveCollection.Id != Guid.Empty && localCollection.EffectiveCollection.Id == collId)
            {
                reason = "LocalPlayer";
                return true;
            }
        }
        catch
        {
            // A failed safety lookup should not crash the IPC path. The caller still has registration checks.
        }

        return false;
    }

    private static bool IsRavaSyncTemporaryCollectionName(string? collectionName)
        => !string.IsNullOrWhiteSpace(collectionName)
            && collectionName.StartsWith("RavaSync", StringComparison.OrdinalIgnoreCase);

    private bool IsRavaSyncOwnedTemporaryCollectionOnFramework(Guid collectionId, string? collectionName)
    {
        if (collectionId == Guid.Empty)
            return false;

        if (_ravaSyncTemporaryCollections.ContainsKey(collectionId))
            return true;

        return IsRavaSyncTemporaryCollectionName(collectionName);
    }

    private bool DoesPlayerMatchTeardownTargetOnFramework((int ObjectIndex, nint Address, string Name) player, string? expectedIdent, string? expectedDisplayName, nint expectedAddress)
    {
        if (player.ObjectIndex < 0 || player.Address == nint.Zero)
            return false;

        var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
        if (localPlayerAddress != nint.Zero && player.Address == localPlayerAddress)
            return false;

        if (expectedAddress != nint.Zero && player.Address == expectedAddress)
            return true;

        if (!string.IsNullOrWhiteSpace(expectedIdent) && _dalamudUtil.AddressMatchesPlayerIdent(expectedIdent, player.Address))
            return true;

        return !string.IsNullOrWhiteSpace(expectedDisplayName)
            && string.Equals(player.Name, expectedDisplayName, StringComparison.OrdinalIgnoreCase);
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

        var playerAddress = await _dalamudUtil.GetPlayerPointerAsync().ConfigureAwait(false);
        if (playerAddress == nint.Zero)
            return;

        _mareMediator.Publish(new ArmRequestedPlayerPublishAfterRedrawMessage(playerAddress));

        var gameObject = await _dalamudUtil.CreateGameObjectAsync(playerAddress).ConfigureAwait(false);
        if (gameObject is ICharacter character)
        {
            await _redrawManager.ExternalPenumbraRedrawAsync(Logger, character, Guid.NewGuid(), c =>
            {
                _penumbraRedraw.Invoke(c.ObjectIndex, setting: RedrawType.Redraw);
            }, CancellationToken.None, isExplicitRedraw: true).ConfigureAwait(false);

            return;
        }

        await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            if (gameObject != null)
                _penumbraRedraw.Invoke(gameObject.ObjectIndex, setting: RedrawType.Redraw);
        }).ConfigureAwait(false);
    }
    public async Task<Guid> CreateTemporaryCollectionAsync(ILogger logger, string uid)
    {
        if (!APIAvailable) return Guid.Empty;

        Guid result = Guid.Empty;

        await SafeIpc.TryRun(Logger, "Penumbra.CreateTemporaryCollection", TimeSpan.FromSeconds(10), async ct =>
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

        if (result != Guid.Empty)
            _ravaSyncTemporaryCollections[result] = uid ?? string.Empty;

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
                var gameObj = handler.GetGameObject();
                if (gameObj == null && handler.ObjectKind == RavaSync.API.Data.Enum.ObjectKind.Player)
                {
                    var localPlayer = _dalamudUtil.GetPlayerPtr();
                    if (localPlayer != nint.Zero)
                        gameObj = _dalamudUtil.CreateGameObject(localPlayer);
                }

                var idx = gameObj?.ObjectIndex;
                if (idx == null) return null;

                var resourcePathSets = _penumbraResourcePaths.Invoke(idx.Value);
                if (resourcePathSets == null)
                    return null;

                try
                {
                    return resourcePathSets[0];
                }
                catch (ArgumentOutOfRangeException)
                {
                    return null;
                }
                catch (IndexOutOfRangeException)
                {
                    return null;
                }
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<Dictionary<string, HashSet<string>>?> GetGameObjectResourcePathsAsync(ILogger logger, nint gameObjectAddress)
    {
        if (!APIAvailable || gameObjectAddress == nint.Zero) return null;

        Dictionary<string, HashSet<string>>? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetGameObjectResourcePathsByAddress", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                try
                {
                    logger.LogTrace("Calling On IPC: Penumbra.GetGameObjectResourcePathsByAddress {address:X}", gameObjectAddress);
                    var gameObj = _dalamudUtil.CreateGameObject((IntPtr)gameObjectAddress);
                    var idx = gameObj?.ObjectIndex;
                    if (idx == null) return null;

                    var resourcePathSets = _penumbraResourcePaths.Invoke(idx.Value);
                    if (resourcePathSets == null)
                        return null;

                    try
                    {
                        return resourcePathSets[0];
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        return null;
                    }
                    catch (IndexOutOfRangeException)
                    {
                        return null;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogTrace(ex, "Failed to get Penumbra resource paths for game object {address:X}", gameObjectAddress);
                    return null;
                }
            }).ConfigureAwait(false);
        }).ConfigureAwait(false);

        return result;
    }

    public async Task<Dictionary<ushort, Dictionary<string, HashSet<string>>>?> GetPlayerResourcePathsAsync(ILogger logger)
    {
        if (!APIAvailable) return null;

        Dictionary<ushort, Dictionary<string, HashSet<string>>>? result = null;

        await SafeIpc.TryRun(Logger, "Penumbra.GetPlayerResourcePaths", TimeSpan.FromSeconds(2), async ct =>
        {
            result = await _dalamudUtil.RunOnFrameworkThread(() =>
            {
                logger.LogTrace("Calling On IPC: Penumbra.GetPlayerResourcePaths");
                return _penumbraPlayerResourcePaths.Invoke();
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
                        if (frameworkStopwatch.ElapsedMilliseconds >= (IsWineRuntime ? 250 : 60))
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
                if (frameworkStopwatch.ElapsedMilliseconds >= (IsWineRuntime ? 250 : 60))
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
                if (frameworkStopwatch.ElapsedMilliseconds >= (IsWineRuntime ? 250 : 60))
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
        if (collId == Guid.Empty) return;

        await SafeIpc.TryRun(Logger, "Penumbra.RemoveTemporaryCollection", TimeSpan.FromSeconds(10), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.RemoveTemporaryCollection", () =>
            {
                if (IsProtectedUserCollectionOnFramework(collId, out var protectedReason))
                {
                    logger.LogWarning("[{applicationId}] Blocked temporary collection removal for protected collection {collection} ({reason}). RavaSync will not mutate the local/default collection during cleanup.", applicationId, collId, protectedReason);
                    return;
                }

                logger.LogTrace("[{applicationId}] Removing temp collection for {collId}", applicationId, collId);
                var ret2 = _penumbraRemoveTemporaryCollection.Invoke(collId);
                if (ret2 == PenumbraApiEc.Success)
                    _ravaSyncTemporaryCollections.TryRemove(collId, out _);
                logger.LogTrace("[{applicationId}] RemoveTemporaryCollection: {ret2}", applicationId, ret2);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task<(string[] forward, string[][] reverse)> ResolvePathsAsync(string[] forward, string[] reverse)
    {
        return await _penumbraResolvePaths.Invoke(forward, reverse).ConfigureAwait(false);
    }

    public async Task SetManipulationDataAsync(ILogger logger, Guid applicationId, Guid collId, string manipulationData)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.SetManipulationData", TimeSpan.FromSeconds(10), async ct =>
        {
            if (await _dalamudUtil.RunOnFrameworkThread(() => ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetManipulationData", "MareChara_Meta")).ConfigureAwait(false))
                return;

            logger.LogTrace("[{applicationId}] Manip: {data}", applicationId, manipulationData);

            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.AddTemporaryMod(Meta)", () =>
            {
                if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetManipulationData", "MareChara_Meta"))
                    return;

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

        await SafeIpc.TryRun(Logger, "Penumbra.ClearManipulationData", TimeSpan.FromSeconds(10), async ct =>
        {
            if (await _dalamudUtil.RunOnFrameworkThread(() => ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearManipulationData", "MareChara_Meta")).ConfigureAwait(false))
                return;

            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.ClearManipulationData", () =>
            {
                if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearManipulationData", "MareChara_Meta"))
                    return;

                var emptyPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, emptyPaths, string.Empty, 0);
                if (retAdd != PenumbraApiEc.Success)
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke("MareChara_Meta", collId, 0);
                    logger.LogTrace("[{applicationId}] Clear meta fallback: removed temp meta mod from {collId}, ret={ret}", applicationId, collId, retRemove);
                    retAdd = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, emptyPaths, string.Empty, 0);
                }

                logger.LogTrace("[{applicationId}] Cleared meta by overwriting temp collection {collId} with empty manipulation, ret={ret}", applicationId, collId, retAdd);
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

        var ipcOk = await SafeIpc.TryRun(Logger, "Penumbra.SetTemporaryMods", TimeSpan.FromSeconds(10), async ct =>
        {
            if (await _dalamudUtil.RunOnFrameworkThread(() => ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetTemporaryMods", tempModName)).ConfigureAwait(false))
                return;

            if (string.Equals(tempModName, "RavaSync_AsyncLoadSupport", StringComparison.OrdinalIgnoreCase))
            {
                logger.LogWarning("Blocked attempt to install {tempModName} as a Penumbra temporary mod on collection {collId}; async support temp-mod shim is disabled", tempModName, collId);
                return;
            }

            if (logger.IsEnabled(LogLevel.Trace))
            {
                foreach (var mod in modPaths)
                {
                    logger.LogTrace("[{applicationId}] {tempModName}: {from} => {to}", applicationId, tempModName, mod.Key, mod.Value);
                }
            }

            var retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({tempModName})", () =>
            {
                if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetTemporaryMods", tempModName))
                    return PenumbraApiEc.UnknownError;

                var ret = _penumbraAddTemporaryMod.Invoke(tempModName, collId, modPaths, string.Empty, priority);
                logger.LogTrace("[{applicationId}] Setting temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, ret);
                return ret;
            }, ct).ConfigureAwait(false);

            if (retAdd != PenumbraApiEc.Success)
            {
                await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.RemoveTemporaryMod({tempModName})", () =>
                {
                    if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetTemporaryModsFallbackClear", tempModName))
                        return;

                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(tempModName, collId, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: removing temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, retRemove);
                }, ct).ConfigureAwait(false);

                retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({tempModName})Fallback", () =>
                {
                    if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.SetTemporaryModsFallbackAdd", tempModName))
                        return PenumbraApiEc.UnknownError;

                    var ret = _penumbraAddTemporaryMod.Invoke(tempModName, collId, modPaths, string.Empty, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: setting temp files mod {tempModName} for {collId} at priority {priority}, Success: {ret}", applicationId, tempModName, collId, priority, ret);
                    return ret;
                }, ct).ConfigureAwait(false);
            }

            applied = retAdd == PenumbraApiEc.Success;
        }).ConfigureAwait(false);

        return ipcOk && applied;
    }




    public async Task<bool> SetMountMusicTemporaryModOnDefaultCollectionAsync(ILogger logger, Guid applicationId, Dictionary<string, string> modPaths, int priority)
    {
        if (!APIAvailable) return false;
        if (modPaths == null || modPaths.Count == 0) return false;

        var defaultCollection = await GetCollectionAsync(logger, ApiCollectionType.Default).ConfigureAwait(false);
        if (!defaultCollection.HasValue || defaultCollection.Value.Id == Guid.Empty)
        {
            logger.LogDebug("[{applicationId}] Could not resolve Default collection for receiver mount music temp mod", applicationId);
            return false;
        }

        var collId = defaultCollection.Value.Id;
        var applied = false;

        var ipcOk = await SafeIpc.TryRun(Logger, "Penumbra.SetMountMusicTemporaryModOnDefaultCollection", TimeSpan.FromSeconds(10), async ct =>
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                foreach (var mod in modPaths)
                    logger.LogTrace("[{applicationId}] {tempModName}: {from} => {to}", applicationId, MountMusicTemporaryModName, mod.Key, mod.Value);
            }

            var retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({MountMusicTemporaryModName})", () =>
            {
                var ret = _penumbraAddTemporaryMod.Invoke(MountMusicTemporaryModName, collId, modPaths, string.Empty, priority);
                logger.LogTrace("[{applicationId}] Setting mount music temp mod {tempModName} on Default collection {collId} at priority {priority}, Success: {ret}", applicationId, MountMusicTemporaryModName, collId, priority, ret);
                return ret;
            }, ct).ConfigureAwait(false);

            if (retAdd != PenumbraApiEc.Success)
            {
                await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.RemoveTemporaryMod({MountMusicTemporaryModName})", () =>
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(MountMusicTemporaryModName, collId, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: removing mount music temp mod {tempModName} from Default collection {collId} at priority {priority}, Success: {ret}", applicationId, MountMusicTemporaryModName, collId, priority, retRemove);
                }, ct).ConfigureAwait(false);

                retAdd = await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.AddTemporaryMod({MountMusicTemporaryModName})Fallback", () =>
                {
                    var ret = _penumbraAddTemporaryMod.Invoke(MountMusicTemporaryModName, collId, modPaths, string.Empty, priority);
                    logger.LogTrace("[{applicationId}] Replace fallback: setting mount music temp mod {tempModName} on Default collection {collId} at priority {priority}, Success: {ret}", applicationId, MountMusicTemporaryModName, collId, priority, ret);
                    return ret;
                }, ct).ConfigureAwait(false);
            }

            applied = retAdd == PenumbraApiEc.Success;
        }).ConfigureAwait(false);

        return ipcOk && applied;
    }

    public async Task ClearMountMusicTemporaryModOnDefaultCollectionAsync(ILogger logger, Guid applicationId, int priority)
    {
        if (!APIAvailable) return;

        var defaultCollection = await GetCollectionAsync(logger, ApiCollectionType.Default).ConfigureAwait(false);
        if (!defaultCollection.HasValue || defaultCollection.Value.Id == Guid.Empty)
            return;

        var collId = defaultCollection.Value.Id;

        await SafeIpc.TryRun(Logger, "Penumbra.ClearMountMusicTemporaryModOnDefaultCollection", TimeSpan.FromSeconds(10), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.ClearTemporaryMods({MountMusicTemporaryModName}@{priority})", () =>
            {
                var emptyPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var retAdd = _penumbraAddTemporaryMod.Invoke(MountMusicTemporaryModName, collId, emptyPaths, string.Empty, priority);
                if (retAdd != PenumbraApiEc.Success)
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(MountMusicTemporaryModName, collId, priority);
                    logger.LogTrace("[{applicationId}] Clear mount music fallback: removed {tempModName}@{priority} from Default collection {collId}, ret={ret}", applicationId, MountMusicTemporaryModName, priority, collId, retRemove);
                    retAdd = _penumbraAddTemporaryMod.Invoke(MountMusicTemporaryModName, collId, emptyPaths, string.Empty, priority);
                }

                logger.LogTrace("[{applicationId}] Cleared mount music temp mod {tempModName}@{priority} on Default collection {collId} by overwriting with empty redirects, ret={ret}", applicationId, MountMusicTemporaryModName, priority, collId, retAdd);
            }, ct).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }



    private static readonly (string Name, int Priority)[] RavaSyncReceiverTempFileMods =
    [
        ("MareChara_Files", 100),
        ("MareChara_Files", 0),
        ("MareChara_Files_A", 100),
        ("MareChara_Files_A", 0),
        ("MareChara_Files_B", 100),
        ("MareChara_Files_B", 0),
    ];

    private void ForceRavaSyncTemporaryCollectionVanillaOnFramework(ILogger logger, Guid applicationId, Guid collId, string reason)
    {
        if (collId == Guid.Empty)
            return;

        if (IsProtectedUserCollectionOnFramework(collId, out var protectedReason))
        {
            logger.LogWarning("[{applicationId}] Blocked {reason} vanilla overwrite for protected collection {collection} ({protectedReason})", applicationId, reason, collId, protectedReason);
            return;
        }

        var emptyPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var (name, priority) in RavaSyncReceiverTempFileMods)
        {
            try
            {
                var retAdd = _penumbraAddTemporaryMod.Invoke(name, collId, emptyPaths, string.Empty, priority);
                if (retAdd != PenumbraApiEc.Success)
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(name, collId, priority);
                    logger.LogTrace("[{applicationId}] {reason}: fallback removed temp files mod {tempModName}@{priority} from {collId}, ret={ret}", applicationId, reason, name, priority, collId, retRemove);
                    retAdd = _penumbraAddTemporaryMod.Invoke(name, collId, emptyPaths, string.Empty, priority);
                }

                logger.LogTrace("[{applicationId}] {reason}: overwrote temp files mod {tempModName}@{priority} on {collId} with empty redirects, ret={ret}", applicationId, reason, name, priority, collId, retAdd);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "[{applicationId}] {reason}: failed vanilla overwrite for temp files mod {tempModName}@{priority} on {collId}", applicationId, reason, name, priority, collId);
            }
        }

        try
        {
            var retMeta = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, emptyPaths, string.Empty, 0);
            if (retMeta != PenumbraApiEc.Success)
            {
                var retRemoveMeta = _penumbraRemoveTemporaryMod.Invoke("MareChara_Meta", collId, 0);
                logger.LogTrace("[{applicationId}] {reason}: fallback removed temp meta mod from {collId}, ret={ret}", applicationId, reason, collId, retRemoveMeta);
                retMeta = _penumbraAddTemporaryMod.Invoke("MareChara_Meta", collId, emptyPaths, string.Empty, 0);
            }

            logger.LogTrace("[{applicationId}] {reason}: overwrote temp meta mod on {collId} with empty manipulation, ret={ret}", applicationId, reason, collId, retMeta);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "[{applicationId}] {reason}: failed vanilla overwrite for temp meta mod on {collId}", applicationId, reason, collId);
        }

        foreach (var priority in new[] { 100, 0 })
        {
            try
            {
                var retSupport = _penumbraRemoveTemporaryMod.Invoke("RavaSync_AsyncLoadSupport", collId, priority);
                logger.LogTrace("[{applicationId}] {reason}: removed async support temp mod @{priority} from {collId}, ret={ret}", applicationId, reason, priority, collId, retSupport);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "[{applicationId}] {reason}: failed removing async support temp mod @{priority} from {collId}", applicationId, reason, priority, collId);
            }
        }

        logger.LogDebug("[{applicationId}] {reason}: forced RavaSync temp collection {collId} to vanilla contents", applicationId, reason, collId);
    }

    public async Task ClearRavaSyncTemporaryModsFastAsync(ILogger logger, Guid applicationId, Guid collId)
    {
        if (!APIAvailable) return;
        if (collId == Guid.Empty) return;

        await SafeIpc.TryRun(Logger, "Penumbra.ForceRavaSyncTemporaryModsVanilla", TimeSpan.FromSeconds(5), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.ForceRavaSyncTemporaryModsVanilla", () =>
            {
                ForceRavaSyncTemporaryCollectionVanillaOnFramework(logger, applicationId, collId, "teardown");
            }, ct, warnAfterMs: 100).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public async Task ClearRavaSyncTemporaryCollectionForPlayerAsync(ILogger logger, Guid applicationId, string expectedIdent, string? expectedDisplayName, nint expectedAddress, int? lastObjectIndex, Guid capturedCollection, CancellationToken token = default)
    {
        if (!APIAvailable) return;

        await SafeIpc.TryRun(Logger, "Penumbra.RestoreRavaSyncTargetTemporaryCollectionToVanilla", TimeSpan.FromSeconds(5), async ct =>
        {
            await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.RestoreRavaSyncTargetTemporaryCollectionToVanilla", () =>
            {
                // Critical: make the collection itself vanilla first. Penumbra can keep rendering a
                // previously-assigned temp collection through despawn/re-entry; deleting or clearing the
                // object slot alone is not enough if the collection contents still contain redirects.
                if (capturedCollection != Guid.Empty)
                    ForceRavaSyncTemporaryCollectionVanillaOnFramework(logger, applicationId, capturedCollection, "targeted teardown captured collection");

                var vanillaCollectionsApplied = new HashSet<Guid>();

                void TryRestoreIndex(int idx, string source, bool allowNoLiveObject)
                {
                    if (idx < 0)
                        return;

                    try
                    {
                        var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                        var live = _dalamudUtil.GetCharacterFromObjectTableByIndex(idx);
                        var liveAddress = live?.Address ?? nint.Zero;

                        if (liveAddress != nint.Zero && localPlayerAddress != nint.Zero && liveAddress == localPlayerAddress)
                        {
                            logger.LogWarning("[{applicationId}] Targeted vanilla teardown blocked at idx {idx}; target is local player", applicationId, idx);
                            return;
                        }

                        if (liveAddress == nint.Zero && !allowNoLiveObject)
                            return;

                        Guid effectiveId = Guid.Empty;
                        string effectiveName = string.Empty;
                        var objectValid = false;

                        try
                        {
                            var collectionInfo = _penumbraGetCollectionForObject.Invoke(idx);
                            objectValid = collectionInfo.ObjectValid;
                            if (objectValid)
                            {
                                effectiveId = collectionInfo.EffectiveCollection.Id;
                                effectiveName = collectionInfo.EffectiveCollection.Name ?? string.Empty;
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogDebug(ex, "[{applicationId}] Targeted vanilla teardown could not read Penumbra collection for idx {idx} ({source})", applicationId, idx, source);
                        }

                        var matchesCapturedCollection = capturedCollection != Guid.Empty && effectiveId == capturedCollection;
                        var matchesRavaCollection = IsRavaSyncOwnedTemporaryCollectionOnFramework(effectiveId, effectiveName);

                        if (objectValid && !matchesCapturedCollection && !matchesRavaCollection)
                            return;

                        var vanillaCollection = effectiveId != Guid.Empty && (matchesCapturedCollection || matchesRavaCollection)
                            ? effectiveId
                            : capturedCollection;

                        if (vanillaCollection == Guid.Empty)
                            return;

                        if (vanillaCollectionsApplied.Add(vanillaCollection))
                            ForceRavaSyncTemporaryCollectionVanillaOnFramework(logger, applicationId, vanillaCollection, $"targeted teardown {source}");

                        // Assign the now-empty owned collection back to the actor/index and redraw. This actively
                        // replaces rendered redirects with an empty owned collection instead of hoping
                        // Guid.Empty/deletion invalidates Penumbra's rendered state in time.
                        var retAssign = _penumbraAssignTemporaryCollection.Invoke(vanillaCollection, idx, forceAssignment: true);
                        logger.LogDebug("[{applicationId}] Targeted vanilla teardown assigned empty RavaSync collection {collection} to idx={idx} source={source}, previous={previous}/{name}, ret={ret}", applicationId, vanillaCollection, idx, source, effectiveId, effectiveName, retAssign);

                        try
                        {
                            _penumbraRedraw.Invoke(idx, RedrawType.Redraw);
                        }
                        catch (Exception redrawEx)
                        {
                            logger.LogDebug(redrawEx, "[{applicationId}] Targeted vanilla teardown redraw failed for idx {idx}", applicationId, idx);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogDebug(ex, "[{applicationId}] Targeted vanilla teardown failed restoring idx {idx} ({source})", applicationId, idx, source);
                    }
                }

                if (lastObjectIndex.HasValue)
                    TryRestoreIndex(lastObjectIndex.Value, "last assigned", allowNoLiveObject: true);

                foreach (var player in _dalamudUtil.GetPlayerCharacterSnapshotsFromObjectTable())
                {
                    if (!DoesPlayerMatchTeardownTargetOnFramework(player, expectedIdent, expectedDisplayName, expectedAddress))
                        continue;

                    TryRestoreIndex(player.ObjectIndex, "live target", allowNoLiveObject: false);
                }
            }, ct, warnAfterMs: 100).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public void QueueRavaSyncGlobalTemporaryCollectionWipe(ILogger logger, string reason)
    {
        if (!APIAvailable)
            return;

        var applicationId = Guid.NewGuid();
        _ = Task.Run(async () =>
        {
            try
            {
                // Keep global wipes single-pass. Visibility/zone re-entry is generation-guarded
                // at the PairHandler level; a delayed global follow-up has no pair generation guard
                // and can wipe a freshly-applied actor after it becomes visible again.
                await WipeAllRavaSyncTemporaryCollectionsAsync(logger, applicationId, reason, CancellationToken.None, deleteCollections: true).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "[{applicationId}] Global RavaSync temporary collection wipe task failed for {reason}", applicationId, reason);
            }
        });
    }

    public async Task WipeAllRavaSyncTemporaryCollectionsAsync(ILogger logger, Guid applicationId, string reason, CancellationToken token = default, bool deleteCollections = false)
    {
        if (!APIAvailable)
            return;

        if (!await RavaSyncGlobalTemporaryCollectionWipeGate.WaitAsync(0, token).ConfigureAwait(false))
        {
            logger.LogDebug("[{applicationId}] Skipping duplicate global RavaSync temporary collection wipe for {reason}; another wipe is already running", applicationId, reason);
            return;
        }

        try
        {
            var initiallyTrackedCollections = _ravaSyncTemporaryCollections.Keys
                .Where(c => c != Guid.Empty)
                .Distinct()
                .ToArray();

            var collectionSet = initiallyTrackedCollections.ToHashSet();
            var liveAssignments = new List<(int Index, nint Address, string Name, Guid Collection)>();
            var discoveredCollections = new List<Guid>();

            await SafeIpc.TryRun(Logger, "Penumbra.ScanRavaSyncTemporaryCollections", TimeSpan.FromSeconds(5), async ct =>
            {
                var scanResult = await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.ScanRavaSyncTemporaryCollections", () =>
                {
                    var assignments = new List<(int Index, nint Address, string Name, Guid Collection)>();
                    var discovered = new List<Guid>();
                    var localPlayerAddress = _dalamudUtil.GetPlayerPtr();
                    var players = _dalamudUtil.GetPlayerCharacterSnapshotsFromObjectTable();

                    foreach (var player in players)
                    {
                        if (player.ObjectIndex < 0 || player.Address == nint.Zero)
                            continue;

                        if (localPlayerAddress != nint.Zero && player.Address == localPlayerAddress)
                            continue;

                        try
                        {
                            var collectionInfo = _penumbraGetCollectionForObject.Invoke(player.ObjectIndex);
                            if (!collectionInfo.ObjectValid)
                                continue;

                            var effectiveCollectionId = collectionInfo.EffectiveCollection.Id;
                            var effectiveCollectionName = collectionInfo.EffectiveCollection.Name ?? string.Empty;
                            if (!IsRavaSyncOwnedTemporaryCollectionOnFramework(effectiveCollectionId, effectiveCollectionName))
                                continue;

                            assignments.Add((player.ObjectIndex, player.Address, player.Name, effectiveCollectionId));
                            if (effectiveCollectionId != Guid.Empty)
                                discovered.Add(effectiveCollectionId);
                        }
                        catch (Exception ex)
                        {
                            logger.LogDebug(ex, "[{applicationId}] Failed checking Penumbra collection for object index {idx} during global wipe", applicationId, player.ObjectIndex);
                        }
                    }

                    return (Assignments: assignments, Discovered: discovered.Distinct().ToArray());
                }, ct, warnAfterMs: 80).ConfigureAwait(false);

                liveAssignments = scanResult.Assignments;
                discoveredCollections = scanResult.Discovered.ToList();
            }).ConfigureAwait(false);

            foreach (var discovered in discoveredCollections)
            {
                if (discovered != Guid.Empty)
                    collectionSet.Add(discovered);
            }

            var collections = collectionSet
                .Where(c => c != Guid.Empty)
                .Distinct()
                .ToArray();

            if (collections.Length == 0 && liveAssignments.Count == 0)
            {
                logger.LogTrace("[{applicationId}] Global RavaSync temporary collection wipe for {reason}: no tracked or live RavaSync collections", applicationId, reason);
                return;
            }

            logger.LogDebug("[{applicationId}] Global RavaSync temporary collection wipe for {reason}: tracked={trackedCount}, discovered={discoveredCount}, collections={collectionCount}, liveAssignments={assignmentCount}, deleteCollections={deleteCollections}", applicationId, reason, initiallyTrackedCollections.Length, discoveredCollections.Count, collections.Length, liveAssignments.Count, deleteCollections);

            foreach (var collectionId in collections)
            {
                token.ThrowIfCancellationRequested();

                await SafeIpc.TryRun(Logger, "Penumbra.ForceGlobalRavaSyncTemporaryCollectionVanilla", TimeSpan.FromSeconds(5), async ct =>
                {
                    await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.ForceGlobalRavaSyncTemporaryCollectionVanilla", () =>
                    {
                        ForceRavaSyncTemporaryCollectionVanillaOnFramework(logger, applicationId, collectionId, $"global wipe {reason}");
                    }, ct, warnAfterMs: 100).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }

            const int liveAssignmentBatchSize = 1;
            for (var start = 0; start < liveAssignments.Count; start += liveAssignmentBatchSize)
            {
                token.ThrowIfCancellationRequested();

                var batch = liveAssignments.Skip(start).Take(liveAssignmentBatchSize).ToArray();
                await SafeIpc.TryRun(Logger, "Penumbra.ClearRavaSyncTemporaryCollectionAssignments", TimeSpan.FromSeconds(5), async ct =>
                {
                    await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.ClearRavaSyncTemporaryCollectionAssignments", () =>
                    {
                        foreach (var item in batch)
                        {
                            try
                            {
                                var current = _penumbraGetCollectionForObject.Invoke(item.Index);
                                if (!current.ObjectValid)
                                    continue;

                                var effectiveCollectionId = current.EffectiveCollection.Id;
                                var effectiveCollectionName = current.EffectiveCollection.Name ?? string.Empty;
                                if (!IsRavaSyncOwnedTemporaryCollectionOnFramework(effectiveCollectionId, effectiveCollectionName))
                                    continue;

                                ForceRavaSyncTemporaryCollectionVanillaOnFramework(logger, applicationId, effectiveCollectionId, $"global live assignment {reason}");
                                var clearResult = _penumbraAssignTemporaryCollection.Invoke(effectiveCollectionId, item.Index, forceAssignment: true);
                                logger.LogDebug("[{applicationId}] Global wipe assigned empty RavaSync temp collection {collection}/{collectionName} to idx {idx} ({name}), ret={ret}", applicationId, effectiveCollectionId, effectiveCollectionName, item.Index, item.Name, clearResult);

                                if (clearResult == PenumbraApiEc.Success)
                                {
                                    collectionSet.Add(effectiveCollectionId);
                                    try
                                    {
                                        _penumbraRedraw.Invoke(item.Index, RedrawType.Redraw);
                                    }
                                    catch (Exception redrawEx)
                                    {
                                        logger.LogDebug(redrawEx, "[{applicationId}] Global wipe redraw failed for idx {idx} ({name})", applicationId, item.Index, item.Name);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.LogDebug(ex, "[{applicationId}] Failed clearing RavaSync temp collection assignment for idx {idx} ({name})", applicationId, item.Index, item.Name);
                            }
                        }
                    }, ct, warnAfterMs: 100).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }

            collections = collectionSet
                .Where(c => c != Guid.Empty)
                .Distinct()
                .ToArray();

            if (deleteCollections)
            {
                foreach (var collectionId in collections)
                {
                    token.ThrowIfCancellationRequested();

                    await SafeIpc.TryRun(Logger, "Penumbra.DeleteRavaSyncTemporaryCollection", TimeSpan.FromSeconds(5), async ct =>
                    {
                        await RunPacedPenumbraFrameworkIpcAsync(logger, "Penumbra.DeleteRavaSyncTemporaryCollection", () =>
                        {
                            if (IsProtectedUserCollectionOnFramework(collectionId, out var protectedReason))
                            {
                                logger.LogWarning("[{applicationId}] Global wipe blocked protected collection deletion for {collection} ({reason})", applicationId, collectionId, protectedReason);
                                return;
                            }

                            var result = _penumbraRemoveTemporaryCollection.Invoke(collectionId);
                            if (result == PenumbraApiEc.Success)
                                _ravaSyncTemporaryCollections.TryRemove(collectionId, out _);

                            logger.LogDebug("[{applicationId}] Global wipe deleted RavaSync temporary collection {collection}, ret={ret}", applicationId, collectionId, result);
                        }, ct, warnAfterMs: 100).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogDebug("[{applicationId}] Global RavaSync temporary collection wipe cancelled for {reason}", applicationId, reason);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "[{applicationId}] Global RavaSync temporary collection wipe failed for {reason}", applicationId, reason);
        }
        finally
        {
            RavaSyncGlobalTemporaryCollectionWipeGate.Release();
        }
    }

    public async Task ClearNamedTemporaryModsAsync(ILogger logger, Guid applicationId, Guid collId, string tempModName, int priority = 0)
    {
        if (!APIAvailable) return;
        if (string.IsNullOrWhiteSpace(tempModName)) return;

        await SafeIpc.TryRun(Logger, "Penumbra.ClearTemporaryMods", TimeSpan.FromSeconds(10), async ct =>
        {
            if (await _dalamudUtil.RunOnFrameworkThread(() => ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearTemporaryMods", tempModName)).ConfigureAwait(false))
                return;

            await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.ClearTemporaryMods({tempModName}@{priority})", () =>
            {
                if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearTemporaryMods", tempModName))
                    return;

                var emptyPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var retAdd = _penumbraAddTemporaryMod.Invoke(tempModName, collId, emptyPaths, string.Empty, priority);
                if (retAdd != PenumbraApiEc.Success)
                {
                    var retRemove = _penumbraRemoveTemporaryMod.Invoke(tempModName, collId, priority);
                    logger.LogTrace("[{applicationId}] Clear temp files fallback: removed {tempModName}@{priority} from {collId}, ret={ret}", applicationId, tempModName, priority, collId, retRemove);
                    retAdd = _penumbraAddTemporaryMod.Invoke(tempModName, collId, emptyPaths, string.Empty, priority);
                }

                logger.LogTrace("[{applicationId}] Cleared temp files mod {tempModName}@{priority} for {collId} by overwriting with empty redirects, ret={ret}", applicationId, tempModName, priority, collId, retAdd);
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

        await SafeIpc.TryRun(Logger, "Penumbra.ClearTemporaryModsPriorityRange", TimeSpan.FromSeconds(10), async ct =>
        {
            if (await _dalamudUtil.RunOnFrameworkThread(() => ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearTemporaryModsPriorityRange", tempModNames)).ConfigureAwait(false))
                return;

            await RunPacedPenumbraFrameworkIpcAsync(logger, $"Penumbra.ClearTemporaryModsRange({fromPriority}-{toPriority})", () =>
            {
                if (ShouldBlockRavaSyncTemporaryCollectionMutationOnFramework(logger, applicationId, collId, "Penumbra.ClearTemporaryModsPriorityRange", tempModNames))
                    return;

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
