
using Dalamud.Game.ClientState.Conditions;
using Dalamud.Game.ClientState.Objects.SubKinds;
using Dalamud.Game.ClientState.Objects.Types;
using Dalamud.Plugin.Services;
using Dalamud.Utility;
using FFXIVClientStructs.FFXIV.Client.Game;
using FFXIVClientStructs.FFXIV.Client.Game.Character;
using FFXIVClientStructs.FFXIV.Client.Game.Control;
using FFXIVClientStructs.FFXIV.Client.Game.Object;
using FFXIVClientStructs.FFXIV.Client.UI.Agent;
using Lumina.Excel.Sheets;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto.CharaData;
using RavaObjectKind = RavaSync.API.Data.Enum.ObjectKind;
using DalamudObjectKind = Dalamud.Game.ClientState.Objects.Enums.ObjectKind;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using GameObject = FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject;



namespace RavaSync.Services;

public class DalamudUtilService : IHostedService, IMediatorSubscriber
{
    // Some jobs expose actor-like combat helpers through the pet pointer.
    // Ninja shadow clone is intentionally ignored as a pet pointer entirely.
    private readonly List<uint> _classJobIdsIgnoredForPets = [30];
    private readonly HashSet<uint> _classJobIdsWithShortLivedCombatPets = [31, 32];
    // Arcanist/Summoner/Scholar plus MCH/DRK owned helpers can churn the pet pointer during
    // summon/dismiss/combat-helper casts. Their files are delivered through the player job
    // transient scope, so live pet-pointer churn should not force heavyweight Pet builds.
    private readonly HashSet<uint> _classJobIdsWithPlayerScopedSummonPetChurn = [26, 27, 28, 31, 32];
    private readonly IClientState _clientState;
    private readonly ICondition _condition;
    private readonly IDataManager _gameData;
    private readonly IGameConfig _gameConfig;
    private readonly IDutyState _dutyState;
    private readonly IFramework _framework;
    private readonly IGameGui _gameGui;
    private readonly ILogger<DalamudUtilService> _logger;
    private readonly IObjectTable _objectTable;
    private readonly PerformanceCollectorService _performanceCollector;
    private readonly MareConfigService _configService;
    private readonly IPartyList _partyList;
    private uint? _classJobId = 0;
    private DateTime _delayedFrameworkUpdateCheck = DateTime.UtcNow;
    private string _lastGlobalBlockPlayer = string.Empty;
    private string _lastGlobalBlockReason = string.Empty;
    private ushort _lastZone = 0;
    //private readonly Dictionary<string, (string Name, nint Address)> _playerCharas = new(StringComparer.Ordinal);
    private readonly List<string> _notUpdatedCharas = [];
    private bool _sentBetweenAreas = false;
    private Lazy<ulong> _cid;


    public readonly struct LivePlayerRef
    {
        public readonly string Ident;
        public readonly string Name;
        public readonly nint Address;
        public readonly uint EntityId;
        public readonly int ObjectIndex;
        public readonly int LastSeenScan;

        public LivePlayerRef(string ident, string name, nint address, uint entityId, int objectIndex, int lastSeenScan)
        {
            Ident = ident;
            Name = name;
            Address = address;
            EntityId = entityId;
            ObjectIndex = objectIndex;
            LastSeenScan = lastSeenScan;
        }

        public bool IsValid => !string.IsNullOrEmpty(Ident) && Address != nint.Zero;
    }

    private struct PlayerCharaInfo
    {
        public string Name;
        public nint Address;
        public uint EntityId;
        public int ObjectIndex;
        public int LastSeenScan;

        public PlayerCharaInfo(string name, nint address, uint entityId, int objectIndex, int lastSeenScan)
        {
            Name = name;
            Address = address;
            EntityId = entityId;
            ObjectIndex = objectIndex;
            LastSeenScan = lastSeenScan;
        }

        public LivePlayerRef ToLiveRef(string ident) => new(ident, Name, Address, EntityId, ObjectIndex, LastSeenScan);
    }

    private readonly Dictionary<string, PlayerCharaInfo> _playerCharas = new(StringComparer.Ordinal);

    private readonly Dictionary<nint, string> _identByAddress = new();
    private readonly Dictionary<string, nint> _addressBySessionId = new(StringComparer.Ordinal);

    private readonly Dictionary<ulong, string> _cidHashCache = new();
    private const int _cidHashCacheMax = 512;

    private enum SummonedActorCatalogKind : byte
    {
        Unknown = 0,
        Mount = 1,
        Minion = 2,
        Ambiguous = 3,
    }

    private readonly object _summonedActorCatalogLock = new();
    private Dictionary<uint, SummonedActorCatalogKind>? _summonedActorCatalogByModelId;

    private int _playerCharaScanId = 0;
    private DateTime _nextPlayerCharaScan = DateTime.MinValue;
    private DateTime _nextPlayerCharaPrune = DateTime.MinValue;
    private static readonly TimeSpan _playerCharaScanInterval = TimeSpan.FromMilliseconds(75);
    private static readonly TimeSpan _playerCharaPruneInterval = TimeSpan.FromSeconds(2);



    public DalamudUtilService(ILogger<DalamudUtilService> logger, IClientState clientState, IObjectTable objectTable, IFramework framework,
        IGameGui gameGui, ICondition condition, IDataManager gameData, ITargetManager targetManager, IGameConfig gameConfig,
        IDutyState dutyState, MareMediator mediator, PerformanceCollectorService performanceCollector,
        MareConfigService configService, IPartyList partyList)
    {
        _logger = logger;
        _clientState = clientState;
        _objectTable = objectTable;
        _framework = framework;
        _gameGui = gameGui;
        _condition = condition;
        _gameData = gameData;
        _gameConfig = gameConfig;
        _dutyState = dutyState;
        Mediator = mediator;
        _performanceCollector = performanceCollector;
        _configService = configService;
        _partyList = partyList;
        WorldData = new(() =>
        {
            return gameData.GetExcelSheet<Lumina.Excel.Sheets.World>(Dalamud.Game.ClientLanguage.English)!
                .Where(w => !w.Name.IsEmpty && w.DataCenter.RowId != 0 && (w.IsPublic || char.IsUpper(w.Name.ToString()[0])))
                .ToDictionary(w => (ushort)w.RowId, w => w.Name.ToString());
        });
        JobData = new(() =>
        {
            return gameData.GetExcelSheet<ClassJob>(Dalamud.Game.ClientLanguage.English)!
                .ToDictionary(k => k.RowId, k => k.NameEnglish.ToString());
        });
        TerritoryData = new(() =>
        {
            return gameData.GetExcelSheet<TerritoryType>(Dalamud.Game.ClientLanguage.English)!
            .Where(w => w.RowId != 0)
            .ToDictionary(w => w.RowId, w =>
            {
                StringBuilder sb = new();
                sb.Append(w.PlaceNameRegion.Value.Name);
                if (w.PlaceName.ValueNullable != null)
                {
                    sb.Append(" - ");
                    sb.Append(w.PlaceName.Value.Name);
                }
                return sb.ToString();
            });
        });
        MapData = new(() =>
        {
            return gameData.GetExcelSheet<Map>(Dalamud.Game.ClientLanguage.English)!
            .Where(w => w.RowId != 0)
            .ToDictionary(w => w.RowId, w =>
            {
                StringBuilder sb = new();
                sb.Append(w.PlaceNameRegion.Value.Name);
                if (w.PlaceName.ValueNullable != null)
                {
                    sb.Append(" - ");
                    sb.Append(w.PlaceName.Value.Name);
                }
                if (w.PlaceNameSub.ValueNullable != null && !string.IsNullOrEmpty(w.PlaceNameSub.Value.Name.ToString()))
                {
                    sb.Append(" - ");
                    sb.Append(w.PlaceNameSub.Value.Name);
                }
                return (w, sb.ToString());
            });
        });
        mediator.Subscribe<TargetPairMessage>(this, (msg) =>
        {
            if (clientState.IsPvP) return;
            var name = msg.Pair.PlayerName;
            if (string.IsNullOrEmpty(name)) return;
            var addr = _playerCharas.FirstOrDefault(f => string.Equals(f.Value.Name, name, StringComparison.Ordinal)).Value.Address;
            if (addr == nint.Zero) return;
            var useFocusTarget = _configService.Current.UseFocusTarget;
            _ = RunOnFrameworkThread(() =>
            {
                if (useFocusTarget)
                    targetManager.FocusTarget = CreateGameObject(addr);
                else
                    targetManager.Target = CreateGameObject(addr);
            }).ConfigureAwait(false);
        });
        IsWine = Util.IsWine();
        if (LinuxSmoothMode.IsActive)
            _logger.LogInformation("Linux Smooth Mode active: adaptive framework IPC pacing, restored pair download lanes and batched cache finalisation enabled");
        _cid = RebuildCID();
    }

    private Lazy<ulong> RebuildCID() =>  new(GetCID);

    public bool IsWine { get; init; }
    public HashSet<string> GetPartyNames()
    {
        // Raw names (no world suffix). We'll normalize later where we compare.
        var set = new HashSet<string>(StringComparer.Ordinal);

        // Length is your own party (up to 8).
        for (var i = 0; i < _partyList.Length; i++)
        {
            var m = _partyList[i];
            if (m == null) continue;

            // SeString → plain text
            var name = m.Name?.TextValue;
            if (!string.IsNullOrWhiteSpace(name))
                set.Add(name);
        }

        return set;
    }

    public HashSet<string> GetAllianceNames()
    {
        var set = new HashSet<string>(StringComparer.Ordinal);

        if (_partyList.IsAlliance)
        {
            // In an alliance, enumeration includes all alliance members (up to 24).
            foreach (var m in _partyList)
            {
                if (m == null) continue;
                var name = m.Name?.TextValue; // SeString → plain text
                if (!string.IsNullOrWhiteSpace(name))
                    set.Add(name);
            }
        }
        else
        {
            // Fallback: not in alliance, just return own party.
            for (var i = 0; i < _partyList.Length; i++)
            {
                var m = _partyList[i];
                if (m == null) continue;

                var name = m.Name?.TextValue;
                if (!string.IsNullOrWhiteSpace(name))
                    set.Add(name);
            }
        }

        return set;
    }


    public unsafe GameObject* GposeTarget
    {
        get => TargetSystem.Instance()->GPoseTarget;
        set => TargetSystem.Instance()->GPoseTarget = value;
    }

    private unsafe bool HasGposeTarget => GposeTarget != null;
    private unsafe int GPoseTargetIdx => !HasGposeTarget ? -1 : GposeTarget->ObjectIndex;

    public async Task<IGameObject?> GetGposeTargetGameObjectAsync()
    {
        if (!HasGposeTarget)
            return null;

        return await _framework.RunOnFrameworkThread(() => _objectTable[GPoseTargetIdx]).ConfigureAwait(true);
    }
    public bool IsAnythingDrawing { get; private set; } = false;
    public bool IsInCutscene { get; private set; } = false;
    public bool IsInGpose { get; private set; } = false;
    public bool IsLoggedIn { get; private set; }
    public bool IsOnFrameworkThread => _framework.IsInFrameworkUpdateThread;
    public bool IsZoning => _condition[ConditionFlag.BetweenAreas] || _condition[ConditionFlag.BetweenAreas51];
    public bool IsInCombatOrPerforming { get; private set; } = false;
    public bool IsInInstancedContent { get; private set; } = false;
    public bool HasModifiedGameFiles => _gameData.HasModifiedGameDataFiles;
    public uint ClassJobId => _classJobId!.Value;
    public Lazy<Dictionary<uint, string>> JobData { get; private set; }
    public Lazy<Dictionary<ushort, string>> WorldData { get; private set; }
    public Lazy<Dictionary<uint, string>> TerritoryData { get; private set; }
    public Lazy<Dictionary<uint, (Map Map, string MapName)>> MapData { get; private set; }
    public bool IsLodEnabled { get; private set; }
    public MareMediator Mediator { get; }
    public bool IsRoleplaying => _condition[ConditionFlag.RolePlaying];
    public bool ShouldIgnorePetForCurrentJob => _classJobIdsIgnoredForPets.Contains(_classJobId ?? 0);
    public bool ShouldTreatPetAsShortLivedCombatSummonForCurrentJob => _classJobIdsWithShortLivedCombatPets.Contains(_classJobId ?? 0);
    public bool ShouldRoutePetChurnThroughPlayerScopedSummonsForCurrentJob => _classJobIdsWithPlayerScopedSummonPetChurn.Contains(_classJobId ?? 0);
    public uint CurrentTerritoryId { get; private set; } = 0;

    public IGameObject? CreateGameObject(IntPtr reference)
    {
        EnsureIsOnFramework();
        return _objectTable.CreateObjectReference(reference);
    }

    public async Task<IGameObject?> CreateGameObjectAsync(IntPtr reference)
    {
        return await RunOnFrameworkThread(() => _objectTable.CreateObjectReference(reference)).ConfigureAwait(false);
    }

    public void EnsureIsOnFramework()
    {
        if (!_framework.IsInFrameworkUpdateThread) throw new InvalidOperationException("Can only be run on Framework");
    }

    public IGameObject? GetObjectFromObjectTableByIndex(int index)
    {
        EnsureIsOnFramework();
        if (index < 0) return null;

        try
        {
            return _objectTable[index];
        }
        catch
        {
            return null;
        }
    }

    public ICharacter? GetCharacterFromObjectTableByIndex(int index)
    {
        EnsureIsOnFramework();
        var objTableObj = GetObjectFromObjectTableByIndex(index);
        if (objTableObj == null || objTableObj.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc) return null;
        return (ICharacter)objTableObj;
    }

    public unsafe IntPtr GetCompanionPtr(IntPtr? playerPointer = null)
    {
        EnsureIsOnFramework();
        var mgr = CharacterManager.Instance();
        playerPointer ??= GetPlayerPtr();
        if (playerPointer == IntPtr.Zero || (IntPtr)mgr == IntPtr.Zero) return IntPtr.Zero;
        return (IntPtr)mgr->LookupBuddyByOwnerObject((BattleChara*)playerPointer);
    }


    public unsafe bool TryResolveOwnedActorKindForPlayer(IntPtr playerPointer, IntPtr actorPointer, out RavaObjectKind objectKind)
    {
        EnsureIsOnFramework();
        objectKind = RavaObjectKind.Player;

        if (playerPointer == IntPtr.Zero || actorPointer == IntPtr.Zero || actorPointer == playerPointer)
            return false;

        try
        {
            var mgr = CharacterManager.Instance();
            if ((IntPtr)mgr != IntPtr.Zero)
            {
                var pet = (IntPtr)mgr->LookupPetByOwnerObject((BattleChara*)playerPointer);
                if (pet != IntPtr.Zero && pet == actorPointer)
                {
                    objectKind = RavaObjectKind.Pet;
                    return true;
                }

                var companion = (IntPtr)mgr->LookupBuddyByOwnerObject((BattleChara*)playerPointer);
                if (companion != IntPtr.Zero && companion == actorPointer)
                {
                    objectKind = RavaObjectKind.Companion;
                    return true;
                }
            }
        }
        catch
        {
            // Fall through to the parent-character relationship check below.
        }

        try
        {
            var candidateCharacter = (Character*)actorPointer;
            var parent = candidateCharacter->GetParentCharacter();
            if ((IntPtr)parent == playerPointer)
            {
                objectKind = RavaObjectKind.MinionOrMount;
                return true;
            }
        }
        catch
        {
            return false;
        }

        return false;
    }

    public bool IsOwnedActorOfPlayer(IntPtr playerPointer, IntPtr actorPointer)
        => TryResolveOwnedActorKindForPlayer(playerPointer, actorPointer, out _);

    public async Task<IntPtr> GetCompanionAsync(IntPtr? playerPointer = null)
    {
        return await RunOnFrameworkThread(() => GetCompanionPtr(playerPointer)).ConfigureAwait(false);
    }

    public async Task<ICharacter?> GetGposeCharacterFromObjectTableByNameAsync(string name, bool onlyGposeCharacters = false)
    {
        return await RunOnFrameworkThread(() => GetGposeCharacterFromObjectTableByName(name, onlyGposeCharacters)).ConfigureAwait(false);
    }

    public ICharacter? GetGposeCharacterFromObjectTableByName(string name, bool onlyGposeCharacters = false)
    {
        EnsureIsOnFramework();
        return (ICharacter?)_objectTable
            .FirstOrDefault(i => (!onlyGposeCharacters || i.ObjectIndex >= 200) && string.Equals(i.Name.ToString(), name, StringComparison.Ordinal));
    }

    public IEnumerable<ICharacter?> GetGposeCharactersFromObjectTable()
    {
        return _objectTable.Where(o => o.ObjectIndex > 200 && o.ObjectKind == Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc).Cast<ICharacter>();
    }

    public List<(int ObjectIndex, nint Address, string Name)> GetPlayerCharacterSnapshotsFromObjectTable()
    {
        EnsureIsOnFramework();

        var output = new List<(int ObjectIndex, nint Address, string Name)>();

        foreach (var obj in _objectTable)
        {
            if (obj == null || !obj.IsValid())
                continue;

            if (obj.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc)
                continue;

            if (obj is not ICharacter character)
                continue;

            output.Add((character.ObjectIndex, character.Address, character.Name.TextValue ?? string.Empty));
        }

        return output;
    }

    public bool GetIsPlayerPresent()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer != null && _objectTable.LocalPlayer.IsValid();
    }

    public async Task<bool> GetIsPlayerPresentAsync()
    {
        return await RunOnFrameworkThread(GetIsPlayerPresent).ConfigureAwait(false);
    }

    public bool TryIsKnownMountActorId(string? actorId, out bool isMount)
    {
        isMount = false;

        if (!TryParseSummonedActorModelId(actorId, out var modelId))
            return false;

        var catalog = GetSummonedActorCatalogByModelId();
        if (!catalog.TryGetValue(modelId, out var kind))
            return false;

        if (kind == SummonedActorCatalogKind.Mount)
        {
            isMount = true;
            return true;
        }

        if (kind == SummonedActorCatalogKind.Minion)
        {
            isMount = false;
            return true;
        }

        return false;
    }

    private Dictionary<uint, SummonedActorCatalogKind> GetSummonedActorCatalogByModelId()
    {
        lock (_summonedActorCatalogLock)
        {
            if (_summonedActorCatalogByModelId != null)
                return _summonedActorCatalogByModelId;

            var mountModelIds = new HashSet<uint>();
            var minionModelIds = new HashSet<uint>();

            try
            {
                foreach (var mount in _gameData.GetExcelSheet<Mount>()!)
                {
                    var modelChara = mount.ModelChara.ValueNullable;
                    if (modelChara.HasValue && TryGetModelCharaModelId(modelChara.Value, out var modelId))
                        mountModelIds.Add(modelId);
                }

                foreach (var companion in _gameData.GetExcelSheet<Lumina.Excel.Sheets.Companion>()!)
                {
                    var modelChara = companion.Model.ValueNullable;
                    if (modelChara.HasValue && TryGetModelCharaModelId(modelChara.Value, out var modelId))
                        minionModelIds.Add(modelId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Unable to build Lumina mount/minion actor model catalogue; mount/minion build path will stay conservative");
                _summonedActorCatalogByModelId = new Dictionary<uint, SummonedActorCatalogKind>();
                return _summonedActorCatalogByModelId;
            }

            var catalog = new Dictionary<uint, SummonedActorCatalogKind>();
            foreach (var modelId in mountModelIds)
            {
                catalog[modelId] = minionModelIds.Contains(modelId)
                    ? SummonedActorCatalogKind.Ambiguous
                    : SummonedActorCatalogKind.Mount;
            }

            foreach (var modelId in minionModelIds)
            {
                if (catalog.ContainsKey(modelId))
                    catalog[modelId] = SummonedActorCatalogKind.Ambiguous;
                else
                    catalog[modelId] = SummonedActorCatalogKind.Minion;
            }

            _summonedActorCatalogByModelId = catalog;
            _logger.LogDebug("Built Lumina summoned actor catalogue with {mounts} mount model(s), {minions} minion model(s), {total} classified model id(s)", mountModelIds.Count, minionModelIds.Count, catalog.Count);
            return catalog;
        }
    }

    private static bool TryGetModelCharaModelId(ModelChara modelChara, out uint modelId)
    {
        modelId = Convert.ToUInt32(modelChara.Model);
        return modelId != 0;
    }

    private static bool TryParseSummonedActorModelId(string? actorId, out uint modelId)
    {
        modelId = 0;
        if (string.IsNullOrWhiteSpace(actorId))
            return false;

        var normalized = actorId.Trim().Replace('\\', '/').Trim('/').ToLowerInvariant();
        var slash = normalized.LastIndexOf('/');
        if (slash >= 0 && slash + 1 < normalized.Length)
            normalized = normalized[(slash + 1)..];

        if (normalized.Length < 2 || (normalized[0] != 'm' && normalized[0] != 'd'))
            return false;

        return uint.TryParse(normalized[1..], out modelId) && modelId != 0;
    }

    public unsafe IntPtr GetMinionOrMountPtr(IntPtr? playerPointer = null)
    {
        EnsureIsOnFramework();
        playerPointer ??= GetPlayerPtr();
        if (playerPointer == IntPtr.Zero) return IntPtr.Zero;

        var playerCharacter = (Character*)playerPointer;
        if (playerCharacter == null) return IntPtr.Zero;

        var companionObject = (IntPtr)playerCharacter->CompanionObject;
        if (IsOwnedMinionOrMount(playerPointer.Value, companionObject)) return companionObject;

        var playerObject = (GameObject*)playerPointer;
        if (playerObject == null) return IntPtr.Zero;

        var startIndex = Math.Max(0, playerObject->ObjectIndex + 1);
        var endIndex = Math.Min(199, playerObject->ObjectIndex + 8);
        for (var idx = startIndex; idx <= endIndex; idx++)
        {
            var candidate = _objectTable.GetObjectAddress(idx);
            if (IsOwnedMinionOrMount(playerPointer.Value, candidate)) return candidate;
        }

        for (var idx = 0; idx < 200; idx++)
        {
            var candidate = _objectTable.GetObjectAddress(idx);
            if (IsOwnedMinionOrMount(playerPointer.Value, candidate)) return candidate;
        }

        return IntPtr.Zero;
    }

    private static unsafe bool IsOwnedMinionOrMount(IntPtr playerPointer, IntPtr candidate)
    {
        if (playerPointer == IntPtr.Zero || candidate == IntPtr.Zero || candidate == playerPointer) return false;

        try
        {
            var candidateCharacter = (Character*)candidate;
            if (candidateCharacter == null) return false;

            var parent = candidateCharacter->GetParentCharacter();
            return (IntPtr)parent == playerPointer;
        }
        catch
        {
            return false;
        }
    }

    public async Task<IntPtr> GetMinionOrMountAsync(IntPtr? playerPointer = null)
    {
        return await RunOnFrameworkThread(() => GetMinionOrMountPtr(playerPointer)).ConfigureAwait(false);
    }

    public unsafe IntPtr GetPetPtr(IntPtr? playerPointer = null)
    {
        EnsureIsOnFramework();
        if (ShouldIgnorePetForCurrentJob) return IntPtr.Zero;
        var mgr = CharacterManager.Instance();
        playerPointer ??= GetPlayerPtr();
        if (playerPointer == IntPtr.Zero || (IntPtr)mgr == IntPtr.Zero) return IntPtr.Zero;
        return (IntPtr)mgr->LookupPetByOwnerObject((BattleChara*)playerPointer);
    }

    public async Task<IntPtr> GetPetAsync(IntPtr? playerPointer = null)
    {
        return await RunOnFrameworkThread(() => GetPetPtr(playerPointer)).ConfigureAwait(false);
    }

    public async Task<IPlayerCharacter> GetPlayerCharacterAsync()
    {
        return await RunOnFrameworkThread(GetPlayerCharacter).ConfigureAwait(false);
    }

    public IPlayerCharacter GetPlayerCharacter()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer!;
    }

    public string? GetIdentFromAddress(nint address)
    {
        EnsureIsOnFramework();

        if (address == nint.Zero)
            return null;

        if (!TryGetLivePlayerRefByAddress(address, out var livePlayer))
            return null;

        UpdateLivePlayerCache(livePlayer);
        return livePlayer.Ident;
    }

    public bool AddressMatchesPlayerIdent(string? characterIdent, nint address)
    {
        EnsureIsOnFramework();

        if (string.IsNullOrEmpty(characterIdent) || address == nint.Zero)
            return false;

        if (!TryGetLivePlayerRefByAddress(address, out var livePlayer))
            return false;

        if (!string.Equals(livePlayer.Ident, characterIdent, StringComparison.Ordinal))
            return false;

        UpdateLivePlayerCache(livePlayer);
        return true;
    }

    public bool AddressMatchesPlayerIdentCached(string? characterIdent, nint address)
    {
        EnsureIsOnFramework();

        if (string.IsNullOrEmpty(characterIdent) || address == nint.Zero)
            return false;

        return TryGetCachedLivePlayerRefByAddress(address, out var livePlayer)
            && string.Equals(livePlayer.Ident, characterIdent, StringComparison.Ordinal);
    }

    private IPlayerCharacter? TryGetLivePlayerCharacterByAddress(nint address)
    {
        EnsureIsOnFramework();

        if (address == nint.Zero)
            return null;

        try
        {
            for (var i = 0; i < 200; i++)
            {
                var chara = _objectTable[i] as IPlayerCharacter;
                if (chara == null || chara.Address == nint.Zero)
                    continue;

                if (chara.Address == address)
                    return chara;
            }
        }
        catch
        {
            // Object table access can race zone/object teardown. Treat it as missing.
        }

        return null;
    }

    private unsafe string? GetIdentFromLivePlayerCharacter(IPlayerCharacter chara)
    {
        EnsureIsOnFramework();

        if (chara.Address == nint.Zero)
            return null;

        try
        {
            var cid = ((BattleChara*)chara.Address)->Character.ContentId;
            if (cid == 0)
                return null;

            if (!_cidHashCache.TryGetValue(cid, out var ident))
            {
                ident = cid.GetHash256();

                if (_cidHashCache.Count > _cidHashCacheMax)
                    _cidHashCache.Clear();

                _cidHashCache[cid] = ident;
            }

            return string.IsNullOrEmpty(ident) ? null : ident;
        }
        catch
        {
            return null;
        }
    }

    private unsafe bool TryBuildLivePlayerRef(IPlayerCharacter chara, int scanId, out LivePlayerRef livePlayer)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        if (chara == null || chara.Address == nint.Zero)
            return false;

        try
        {
            if (chara.ObjectKind == (uint)ObjectKind.None)
                return false;

            var name = chara.Name.TextValue;
            if (string.IsNullOrEmpty(name))
                return false;

            var ident = GetIdentFromLivePlayerCharacter(chara);
            if (string.IsNullOrEmpty(ident))
                return false;

            var entityId = ((GameObject*)chara.Address)->EntityId;
            livePlayer = new LivePlayerRef(ident, name, chara.Address, entityId, chara.ObjectIndex, scanId);
            return true;
        }
        catch
        {
            livePlayer = default;
            return false;
        }
    }

    private bool TryGetCachedLivePlayerRefByAddress(nint address, out LivePlayerRef livePlayer)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        if (address == nint.Zero)
            return false;

        if (!_identByAddress.TryGetValue(address, out var ident) || string.IsNullOrEmpty(ident))
            return false;

        if (!_playerCharas.TryGetValue(ident, out var cached) || cached.Address != address)
            return false;

        livePlayer = cached.ToLiveRef(ident);
        return livePlayer.IsValid;
    }

    private bool TryGetCachedLivePlayerRefByIdent(string characterIdent, out LivePlayerRef livePlayer, nint fallbackAddress = 0)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        if (string.IsNullOrEmpty(characterIdent))
            return false;

        if (fallbackAddress != nint.Zero
            && TryGetCachedLivePlayerRefByAddress(fallbackAddress, out var fallbackLive)
            && string.Equals(fallbackLive.Ident, characterIdent, StringComparison.Ordinal))
        {
            livePlayer = fallbackLive;
            return true;
        }

        if (_playerCharas.TryGetValue(characterIdent, out var cached)
            && cached.Address != nint.Zero
            && TryGetCachedLivePlayerRefByAddress(cached.Address, out var cachedLive)
            && string.Equals(cachedLive.Ident, characterIdent, StringComparison.Ordinal))
        {
            livePlayer = cachedLive;
            return true;
        }

        return false;
    }

    private bool TryGetLivePlayerRefByAddress(nint address, out LivePlayerRef livePlayer)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        var chara = TryGetLivePlayerCharacterByAddress(address);
        if (chara == null)
            return false;

        return TryBuildLivePlayerRef(chara, _playerCharaScanId, out livePlayer);
    }

    private void UpdateLivePlayerCache(LivePlayerRef livePlayer)
    {
        EnsureIsOnFramework();

        if (!livePlayer.IsValid)
            return;

        _identByAddress[livePlayer.Address] = livePlayer.Ident;

        var sid = RavaSync.Services.Mesh.RavaSessionId.FromIdent(livePlayer.Ident);
        if (!string.IsNullOrEmpty(sid))
            _addressBySessionId[sid] = livePlayer.Address;

        _playerCharas[livePlayer.Ident] = new PlayerCharaInfo(
            livePlayer.Name,
            livePlayer.Address,
            livePlayer.EntityId,
            livePlayer.ObjectIndex,
            livePlayer.LastSeenScan);
    }

    private bool TryScanLivePlayerByIdent(string characterIdent, out LivePlayerRef livePlayer)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        if (string.IsNullOrEmpty(characterIdent))
            return false;

        try
        {
            var scanId = unchecked(++_playerCharaScanId);

            for (var i = 0; i < 200; i++)
            {
                var chara = _objectTable[i] as IPlayerCharacter;
                if (chara == null)
                    continue;

                if (!TryBuildLivePlayerRef(chara, scanId, out var candidate))
                    continue;

                UpdateLivePlayerCache(candidate);

                if (!string.Equals(candidate.Ident, characterIdent, StringComparison.Ordinal))
                    continue;

                livePlayer = candidate;
                return true;
            }
        }
        catch
        {
            livePlayer = default;
        }

        return false;
    }

    public bool TryResolveLivePlayer(string? characterIdent, out LivePlayerRef livePlayer, nint fallbackAddress = 0, bool forceLiveScan = false)
    {
        EnsureIsOnFramework();
        livePlayer = default;

        if (string.IsNullOrEmpty(characterIdent))
            return false;

        // Hot path rule: the normal resolver is cache-first/cache-only.
        // UpdatePlayerCharaCache() already performs one shared object-table scan on the
        // framework thread. Do not let every PairHandler/GameObjectHandler repeat that
        // same 0..200 object walk just because it has a fallback address.
        if (TryGetCachedLivePlayerRefByIdent(characterIdent, out livePlayer, fallbackAddress))
            return true;

        if (!forceLiveScan)
            return false;

        return TryScanLivePlayerByIdent(characterIdent, out livePlayer);
    }

    public IntPtr GetPlayerCharacterFromCachedTableByIdent(string characterName)
    {
        EnsureIsOnFramework();

        return TryResolveLivePlayer(characterName, out var livePlayer, forceLiveScan: false)
            ? livePlayer.Address
            : IntPtr.Zero;
    }

    public nint ResolvePlayerAddress(string? characterIdent, nint fallbackAddress = 0)
    {
        EnsureIsOnFramework();

        return TryResolveLivePlayer(characterIdent, out var livePlayer, fallbackAddress)
            ? livePlayer.Address
            : nint.Zero;
    }

    public unsafe IntPtr GetGameObjectAddressByEntityId(uint entityId)
    {
        EnsureIsOnFramework();

        for (int i = 0; i < 200; i++)
        {
            var obj = _objectTable[i];
            if (obj == null) continue;
            if (obj.ObjectKind != Dalamud.Game.ClientState.Objects.Enums.ObjectKind.Pc)
                continue;

            var gameObj = (GameObject*)obj.Address;
            if (gameObj->EntityId == entityId)
                return obj.Address;
        }

        return IntPtr.Zero;
    }

    public unsafe string? GetIdentFromEntityId(uint entityId)
    {
        EnsureIsOnFramework();

        var addr = GetGameObjectAddressByEntityId(entityId);
        if (addr == IntPtr.Zero) return null;
        return GetHashedCIDFromPlayerPointer(addr);
    }


    public string GetPlayerName()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer?.Name.ToString() ?? "--";
    }

    public async Task<string> GetPlayerNameAsync()
    {
        return await RunOnFrameworkThread(GetPlayerName).ConfigureAwait(false);
    }

    public async Task<ulong> GetCIDAsync()
    {
        return await RunOnFrameworkThread(GetCID).ConfigureAwait(false);
    }

    public unsafe ulong GetCID()
    {
        EnsureIsOnFramework();
        var playerChar = GetPlayerCharacter();
        return ((BattleChara*)playerChar.Address)->Character.ContentId;
    }

    public async Task<string> GetPlayerNameHashedAsync()
    {
        return await RunOnFrameworkThread(() => _cid.Value.GetHash256()).ConfigureAwait(false);
    }

    private unsafe static string GetHashedCIDFromPlayerPointer(nint ptr)
    {
        return ((BattleChara*)ptr)->Character.ContentId.GetHash256();
    }

    public IntPtr GetPlayerPtr()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer?.Address ?? IntPtr.Zero;
    }

    public async Task<IntPtr> GetPlayerPointerAsync()
    {
        return await RunOnFrameworkThread(GetPlayerPtr).ConfigureAwait(false);
    }

    public uint GetHomeWorldId()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer?.HomeWorld.RowId ?? 0;
    }

    public uint GetWorldId()
    {
        EnsureIsOnFramework();
        return _objectTable.LocalPlayer!.CurrentWorld.RowId;
    }

    public unsafe LocationInfo GetMapData()
    {
        EnsureIsOnFramework();
        var agentMap = AgentMap.Instance();
        var houseMan = HousingManager.Instance();
        uint serverId = 0;
        if (_objectTable.LocalPlayer == null) serverId = 0;
        else serverId = _objectTable.LocalPlayer.CurrentWorld.RowId;
        uint mapId = agentMap == null ? 0 : agentMap->CurrentMapId;
        uint territoryId = agentMap == null ? 0 : agentMap->CurrentTerritoryId;
        uint divisionId = houseMan == null ? 0 : (uint)(houseMan->GetCurrentDivision());
        uint wardId = houseMan == null ? 0 : (uint)(houseMan->GetCurrentWard() + 1);

        uint houseId = 0;
        var tempHouseId = houseMan == null ? 0 : (houseMan->GetCurrentPlot());
        if (tempHouseId < -1)
        {
            divisionId = tempHouseId == -127 ? 2 : (uint)1;
            tempHouseId = 100;
        }
        if (tempHouseId == -1) tempHouseId = 0;
        houseId = (uint)tempHouseId;
        if (houseId != 0)
        {
            territoryId = HousingManager.GetOriginalHouseTerritoryTypeId();
        }
        uint roomId = houseMan == null ? 0 : (uint)(houseMan->GetCurrentRoom());

        return new LocationInfo()
        {
            ServerId = serverId,
            MapId = mapId,
            TerritoryId = territoryId,
            DivisionId = divisionId,
            WardId = wardId,
            HouseId = houseId,
            RoomId = roomId
        };
    }

    public unsafe void SetMarkerAndOpenMap(Vector3 position, Map map)
    {
        EnsureIsOnFramework();
        var agentMap = AgentMap.Instance();
        if (agentMap == null) return;
        agentMap->OpenMapByMapId(map.RowId);
        agentMap->SetFlagMapMarker(map.TerritoryType.RowId, map.RowId, position);
    }

    public async Task<LocationInfo> GetMapDataAsync()
    {
        return await RunOnFrameworkThread(GetMapData).ConfigureAwait(false);
    }

    public async Task<uint> GetWorldIdAsync()
    {
        return await RunOnFrameworkThread(GetWorldId).ConfigureAwait(false);
    }

    public async Task<uint> GetHomeWorldIdAsync()
    {
        return await RunOnFrameworkThread(GetHomeWorldId).ConfigureAwait(false);
    }

    public unsafe bool IsGameObjectPresent(IntPtr key)
    {
        return _objectTable.Any(f => f.Address == key);
    }

    public bool IsObjectPresent(IGameObject? obj)
    {
        EnsureIsOnFramework();
        return obj != null && obj.IsValid();
    }

    public async Task<bool> IsObjectPresentAsync(IGameObject? obj)
    {
        return await RunOnFrameworkThread(() => IsObjectPresent(obj)).ConfigureAwait(false);
    }

    public async Task RunOnFrameworkThread(System.Action act, [CallerMemberName] string callerMember = "", [CallerFilePath] string callerFilePath = "", [CallerLineNumber] int callerLineNumber = 0)
    {
        if (_performanceCollector.Enabled)
        {
            var fileName = Path.GetFileNameWithoutExtension(callerFilePath);

            await _performanceCollector.LogPerformance(
                this,
                $"RunOnFrameworkThread:Act/{fileName}>{callerMember}:{callerLineNumber}",
                async () =>
                {
                    if (_framework.IsInFrameworkUpdateThread)
                    {
                        act();
                        return;
                    }

                    await _framework
                        .RunOnFrameworkThread(act)
                        .ConfigureAwait(false);
                }).ConfigureAwait(false);

            return;
        }

        if (_framework.IsInFrameworkUpdateThread)
        {
            act();
            return;
        }

        await _framework
            .RunOnFrameworkThread(act)
            .ConfigureAwait(false);
    }

    public async Task<T> RunOnFrameworkThread<T>(Func<T> func, [CallerMemberName] string callerMember = "", [CallerFilePath] string callerFilePath = "", [CallerLineNumber] int callerLineNumber = 0)
    {
        if (_performanceCollector.Enabled)
        {
            var fileName = Path.GetFileNameWithoutExtension(callerFilePath);

            return await _performanceCollector.LogPerformance(
                this,
                $"RunOnFramework:Func<{typeof(T)}>/{fileName}>{callerMember}:{callerLineNumber}",
                async () =>
                {
                    if (_framework.IsInFrameworkUpdateThread)
                        return func.Invoke();

                    return await _framework.RunOnFrameworkThread(func).ConfigureAwait(false);

                }).ConfigureAwait(false);
        }

        if (_framework.IsInFrameworkUpdateThread)
            return func.Invoke();

        return await _framework.RunOnFrameworkThread(func).ConfigureAwait(false);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting DalamudUtilService");
        _framework.Update += FrameworkOnUpdate;
        if (IsLoggedIn)
        {
            _classJobId = _objectTable.LocalPlayer!.ClassJob.RowId;
        }

        _logger.LogInformation("Started DalamudUtilService");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogTrace("Stopping {type}", GetType());

        Mediator.UnsubscribeAll(this);
        _framework.Update -= FrameworkOnUpdate;
        return Task.CompletedTask;
    }

    public async Task WaitWhileCharacterIsDrawing(ILogger logger, GameObjectHandler handler, Guid redrawId, int timeOut = 5000, CancellationToken? ct = null)
    {
        if (!_clientState.IsLoggedIn) return;

        var token = ct ?? CancellationToken.None;
        const int tick = 50;
        var curWaitTime = 0;
        var nextLogAtMs = 1000;

        try
        {
            logger.LogTrace("[{redrawId}] Starting wait for {handler} to draw", redrawId, handler);

            // First check: if we’re not drawing, don’t sleep at all.
            bool isDrawing = await handler
                .IsBeingDrawnRunOnFrameworkAsync()
                .ConfigureAwait(false);

            while (!token.IsCancellationRequested
                   && curWaitTime < timeOut
                   && isDrawing)
            {
                await Task.Delay(tick, token).ConfigureAwait(false);
                curWaitTime += tick;

                if (curWaitTime >= nextLogAtMs)
                {
                    logger.LogTrace(
                        "[{redrawId}] Waiting for {handler} to finish drawing ({curWaitTime}ms)",
                        redrawId,
                        handler,
                        curWaitTime);
                    nextLogAtMs += 1000;
                }

                if (curWaitTime >= timeOut)
                    break;

                isDrawing = await handler
                    .IsBeingDrawnRunOnFrameworkAsync()
                    .ConfigureAwait(false);
            }

            logger.LogTrace(
                "[{redrawId}] Finished drawing after {curWaitTime}ms",
                redrawId,
                curWaitTime);
        }
        catch (OperationCanceledException)
        {
            // normal, ignore
        }
        catch (Exception ex) when (ex is NullReferenceException or AccessViolationException)
        {
            logger.LogWarning(ex, "Error accessing {handler}, object does not exist anymore?", handler);
        }
    }

    public unsafe void WaitWhileGposeCharacterIsDrawing(IntPtr characterAddress, int timeOut = 5000)
    {
        Thread.Sleep(500);
        var obj = (GameObject*)characterAddress;
        const int tick = 50;
        int curWaitTime = 0;
        _logger.LogTrace("RenderFlags: {flags}", obj->RenderFlags.ToString("X"));
        while (obj->RenderFlags != 0x00 && curWaitTime < timeOut)
        {
            _logger.LogTrace($"Waiting for gpose actor to finish drawing");
            curWaitTime += tick;
            Thread.Sleep(tick);
        }

        Thread.Sleep(tick * 2);
    }

    public Vector2 WorldToScreen(IGameObject? obj)
    {
        if (obj == null) return Vector2.Zero;

        try
        {
            var addr = obj.Address;
            if (addr == IntPtr.Zero) return Vector2.Zero;

            var idx = obj.ObjectIndex;
            if (idx >= 0 && idx < _objectTable.Length)
            {
                var fresh = _objectTable[idx];
                if (fresh == null || fresh.Address == IntPtr.Zero)
                    return Vector2.Zero;

                obj = fresh;
            }

            return _gameGui.WorldToScreen(obj.Position, out var screenPos) ? screenPos : Vector2.Zero;
        }
        catch (AccessViolationException)
        {
            return Vector2.Zero;
        }
        catch
        {
            return Vector2.Zero;
        }
    }


    internal (string Name, nint Address) FindPlayerByNameHash(string ident, bool forceLiveScan = false)
    {
        EnsureIsOnFramework();

        return TryResolveLivePlayer(ident, out var livePlayer, forceLiveScan: forceLiveScan)
            ? (livePlayer.Name, livePlayer.Address)
            : default;
    }


    private void ClearPlayerCharaCacheForZoneSwitch()
    {
        _playerCharas.Clear();
        _identByAddress.Clear();
        _addressBySessionId.Clear();
        _nextPlayerCharaScan = DateTime.MinValue;
        _nextPlayerCharaPrune = DateTime.MinValue;
    }

    private void UpdatePlayerCharaCache()
    {
        var now = DateTime.UtcNow;

        if (now < _nextPlayerCharaScan)
            return;

        _nextPlayerCharaScan = now.Add(_playerCharaScanInterval);
        var scanId = unchecked(++_playerCharaScanId);

        // Rebuild the live resolver maps from the actual object table. This is the
        // discovery source only; pair handlers still validate the ident before using
        // any address as a live handle.
        _identByAddress.Clear();
        _addressBySessionId.Clear();

        try
        {
            for (var i = 0; i < 200; i++)
            {
                var chara = _objectTable[i] as IPlayerCharacter;
                if (chara == null)
                    continue;

                if (!TryBuildLivePlayerRef(chara, scanId, out var livePlayer))
                    continue;

                UpdateLivePlayerCache(livePlayer);
            }
        }
        catch
        {
            // Object table access can race zoning/teardown. Keep whatever was already
            // resolved this frame and let the next scan repair the cache.
        }

        if (now < _nextPlayerCharaPrune || _playerCharas.Count == 0)
            return;

        _nextPlayerCharaPrune = now.Add(_playerCharaPruneInterval);

        List<string>? toRemove = null;

        foreach (var kvp in _playerCharas)
        {
            if (kvp.Value.LastSeenScan == scanId)
                continue;

            toRemove ??= new List<string>(8);
            toRemove.Add(kvp.Key);
        }

        if (toRemove == null) return;

        foreach (var k in toRemove)
            _playerCharas.Remove(k);
    }
    private bool CheckIsInInstancedContent()
    {
        try
        {
            if (_dutyState.ContentFinderCondition.IsValid)
                return true;
        }
        catch
        {
            // Best-effort fallback below. Some transition frames can have no valid content row yet.
        }

        return _condition[ConditionFlag.BoundByDuty]
            || _condition[ConditionFlag.BoundByDuty56]
            || _condition[ConditionFlag.BoundByDuty95]
            || _condition[ConditionFlag.InDeepDungeon];
    }

    private void FrameworkOnUpdate(IFramework framework)
    {
        if (_performanceCollector.Enabled)
            _performanceCollector.LogPerformance(this, $"FrameworkOnUpdate", FrameworkOnUpdateInternal);
        else
            FrameworkOnUpdateInternal();
    }

    private unsafe void FrameworkOnUpdateInternal()
    {
        if ((_objectTable.LocalPlayer?.IsDead ?? false) && _condition[ConditionFlag.BoundByDuty])
        {
            return;
        }

        bool isNormalFrameworkUpdate = DateTime.UtcNow < _delayedFrameworkUpdateCheck.AddSeconds(1);

        System.Action update = () =>
        {
            IsAnythingDrawing = false;

            if (_performanceCollector.Enabled)
                _performanceCollector.LogPerformance(this, $"ObjTableToCharas", UpdatePlayerCharaCache);
            else
                UpdatePlayerCharaCache();

            if (!IsAnythingDrawing && !string.IsNullOrEmpty(_lastGlobalBlockPlayer))
            {
                _logger.LogTrace("Global draw block: END => {name}", _lastGlobalBlockPlayer);
                _lastGlobalBlockPlayer = string.Empty;
                _lastGlobalBlockReason = string.Empty;
            }

            if (_clientState.IsGPosing && !IsInGpose)
            {
                _logger.LogDebug("Gpose start");
                IsInGpose = true;
                Mediator.Publish(new GposeStartMessage());
            }
            else if (!_clientState.IsGPosing && IsInGpose)
            {
                _logger.LogDebug("Gpose end");
                IsInGpose = false;
                Mediator.Publish(new GposeEndMessage());
            }

            if ((_condition[ConditionFlag.Performing] || _condition[ConditionFlag.InCombat]) && !IsInCombatOrPerforming)
            {
                _logger.LogDebug("Combat/Performance start");
                IsInCombatOrPerforming = true;
                Mediator.Publish(new CombatOrPerformanceStartMessage());
                Mediator.Publish(new HaltScanMessage(nameof(IsInCombatOrPerforming)));
            }
            else if ((!_condition[ConditionFlag.Performing] && !_condition[ConditionFlag.InCombat]) && IsInCombatOrPerforming)
            {
                _logger.LogDebug("Combat/Performance end");
                IsInCombatOrPerforming = false;
                Mediator.Publish(new CombatOrPerformanceEndMessage());
                Mediator.Publish(new ResumeScanMessage(nameof(IsInCombatOrPerforming)));
            }

            var isInInstancedContent = CheckIsInInstancedContent();
            if (isInInstancedContent && !IsInInstancedContent)
            {
                _logger.LogDebug("Instanced content start");
                IsInInstancedContent = true;
                Mediator.Publish(new InstancedContentStartMessage());
            }
            else if (!isInInstancedContent && IsInInstancedContent)
            {
                _logger.LogDebug("Instanced content end");
                IsInInstancedContent = false;
                Mediator.Publish(new InstancedContentEndMessage());
            }

            if (_condition[ConditionFlag.WatchingCutscene] && !IsInCutscene)
            {
                _logger.LogDebug("Cutscene start");
                IsInCutscene = true;
                Mediator.Publish(new CutsceneStartMessage());
                Mediator.Publish(new HaltScanMessage(nameof(IsInCutscene)));
            }
            else if (!_condition[ConditionFlag.WatchingCutscene] && IsInCutscene)
            {
                _logger.LogDebug("Cutscene end");
                IsInCutscene = false;
                Mediator.Publish(new CutsceneEndMessage());
                Mediator.Publish(new ResumeScanMessage(nameof(IsInCutscene)));
            }

            if (IsInCutscene)
            {
                Mediator.Publish(new CutsceneFrameworkUpdateMessage());
                //return;
                //removed return to allow cutscene application of chracter data
            }

            if (_condition[ConditionFlag.BetweenAreas] || _condition[ConditionFlag.BetweenAreas51])
            {
                var zone = _clientState.TerritoryType;

                if (_lastZone != zone)
                {
                    _lastZone = (ushort)zone;
                    CurrentTerritoryId = zone;
                }

                if (!_sentBetweenAreas)
                {
                    _logger.LogDebug("Zone switch start for territory {territory}", zone);
                    _sentBetweenAreas = true;
                    ClearPlayerCharaCacheForZoneSwitch();
                    Mediator.Publish(new ZoneSwitchStartMessage());
                    Mediator.Publish(new HaltScanMessage(nameof(ConditionFlag.BetweenAreas)));
                }

                return;
            }

            if (_sentBetweenAreas)
            {
                var zone = _clientState.TerritoryType;
                if (_lastZone != zone)
                {
                    _lastZone = (ushort)zone;
                    CurrentTerritoryId = zone;
                }

                _logger.LogDebug("Zone switch end for territory {territory}", zone);
                _sentBetweenAreas = false;
                ClearPlayerCharaCacheForZoneSwitch();
                Mediator.Publish(new ZoneSwitchEndMessage());
                Mediator.Publish(new ResumeScanMessage(nameof(ConditionFlag.BetweenAreas)));
            }

            var localPlayer = _objectTable.LocalPlayer;
            var clientLoggedIn = _clientState.IsLoggedIn;

            if (!clientLoggedIn && IsLoggedIn)
            {
                _logger.LogDebug("Logged out");
                IsLoggedIn = false;
                Mediator.Publish(new DalamudLogoutMessage());
            }
            else if (clientLoggedIn && localPlayer != null && !IsLoggedIn)
            {
                _logger.LogDebug("Logged in");
                IsLoggedIn = true;
                _lastZone = (ushort)_clientState.TerritoryType;
                _cid = RebuildCID();
                Mediator.Publish(new DalamudLoginMessage());
            }

            if (clientLoggedIn && localPlayer != null)
            {
                _classJobId = localPlayer.ClassJob.RowId;
            }

            if (!IsInCombatOrPerforming)
                Mediator.Publish(new FrameworkUpdateMessage());

            Mediator.Publish(new PriorityFrameworkUpdateMessage());

            if (isNormalFrameworkUpdate)
                return;

            if (_gameConfig != null
                && _gameConfig.TryGet(Dalamud.Game.Config.SystemConfigOption.LodType_DX11, out bool lodEnabled))
            {
                IsLodEnabled = lodEnabled;
            }

            if (IsInCombatOrPerforming)
                Mediator.Publish(new FrameworkUpdateMessage());

            Mediator.Publish(new DelayedFrameworkUpdateMessage());

            _delayedFrameworkUpdateCheck = DateTime.UtcNow;
        };

        if (_performanceCollector.Enabled)
            _performanceCollector.LogPerformance(this, $"FrameworkOnUpdateInternal+{(isNormalFrameworkUpdate ? "Regular" : "Delayed")}", update);
        else
            update();
    }
    public bool TryGetRegisterableVenue(IGameGui gameGui, out VenueAddress address, out string denyReason)
    {
        address = null!;
        denyReason = string.Empty;

        var map = GetMapData();
        bool isInterior = map.HouseId != 0 || map.RoomId != 0;
        if (!isInterior)
        {
            denyReason = "Registration must be done from inside the house/room.";
            return false;
        }

        unsafe bool HasIndoorLayoutUi()
        {
            ReadOnlySpan<string> candidates = new[]
            {
            "HousingGoods",
            "HousingIndoorObjectSelect",
            "HousingLayout",
            "Housing",
        };

            foreach (var name in candidates)
            {
                nint addonPtr = gameGui.GetAddonByName(name, 1);
                if (addonPtr == nint.Zero) continue;
                var unit = (FFXIVClientStructs.FFXIV.Component.GUI.AtkUnitBase*)addonPtr;
                if (unit != null && unit->IsVisible) return true;
            }
            return false;
        }

        if (!HasIndoorLayoutUi())
        {
            denyReason = "Open Indoor Furnishings layout to prove edit permission.";
            return false;
        }

        string areaKind = (map.HouseId != 0 && map.RoomId == 0) ? "EstateInterior"
            : (map.HouseId == 0 && map.RoomId != 0) ? "ApartmentRoom"
            : "FreeCompanyRoom";

        string key = string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"{map.ServerId}:{map.TerritoryId}:{areaKind}:{map.WardId}:{map.HouseId}:{map.RoomId}");

        address = new VenueAddress
        {
            CanonicalKey = key,
            WorldId = map.ServerId,
            TerritoryId = map.TerritoryId,
            WardId = map.WardId,
            HouseId = map.HouseId,
            RoomId = map.RoomId,
            AreaKind = areaKind,
        };
        return true;
    }

    public string? GetIdentFromGameObject(IGameObject? gameObject)
    {
        EnsureIsOnFramework();

        if (gameObject == null || gameObject.Address == IntPtr.Zero)
            return null;

        if (_identByAddress.TryGetValue(gameObject.Address, out var ident) && !string.IsNullOrEmpty(ident))
            return ident;

        return GetIdentFromAddress(gameObject.Address);
    }

    public IGameObject? GetGameObjectBySessionId(string sessionId)
    {
        EnsureIsOnFramework();

        if (string.IsNullOrEmpty(sessionId)) return null;

        if (_addressBySessionId.TryGetValue(sessionId, out var addr) && addr != nint.Zero)
            return CreateGameObject((IntPtr)addr);

        return null;
    }

    public sealed class VenueAddress
    {
        public required string CanonicalKey { get; init; }
        public required uint WorldId { get; init; }
        public required uint TerritoryId { get; init; }
        public required uint WardId { get; init; }
        public required uint HouseId { get; init; }
        public required uint RoomId { get; init; }
        public required string AreaKind { get; init; }
    }

}