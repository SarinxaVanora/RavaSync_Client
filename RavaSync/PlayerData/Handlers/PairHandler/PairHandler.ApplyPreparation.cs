using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Utils;
using System;
using System.Linq;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private ApplyFrameworkSnapshot CaptureApplyFrameworkSnapshot()
    {
        var cachedData = _cachedData ?? new CharacterData();
        var cachedHash = _cachedData?.DataHash.Value ?? "NODATA";
        var cachedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_cachedData);
        var resolvedPlayerAddress = ResolveStablePlayerAddress();

        return new ApplyFrameworkSnapshot(_dalamudUtil.IsInCombatOrPerforming, _charaHandler != null, resolvedPlayerAddress, _dalamudUtil.IsInGpose, _ipcManager.Penumbra.APIAvailable, _ipcManager.Glamourer.APIAvailable, cachedHash, cachedPayloadFingerprint, cachedData, _forceApplyMods);
    }

    private ApplyPreparation PrepareApplyData(Guid applicationBase, CharacterData characterData, bool forceApplyCustomization, ApplyFrameworkSnapshot snapshot)
    {
        var newHash = characterData.DataHash.Value;
        var oldHash = snapshot.CachedHash;
        var newPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(characterData);
        var updatedData = characterData.CheckUpdatedData(applicationBase, snapshot.CachedData, Logger, this, forceApplyCustomization, snapshot.ForceApplyMods);

        PruneUnchangedForcedManipulationReapply(applicationBase, characterData, updatedData, snapshot.ForceApplyMods);

        var hasDiffMods = updatedData.Any(p => p.Value.Contains(PlayerChanges.ModManip) || p.Value.Contains(PlayerChanges.ModFiles));

        return new ApplyPreparation(newHash, oldHash, string.Equals(newHash, oldHash, StringComparison.Ordinal), string.Equals(newPayloadFingerprint, snapshot.CachedPayloadFingerprint, StringComparison.Ordinal), hasDiffMods, updatedData);
    }

    private void PruneUnchangedForcedManipulationReapply(Guid applicationBase, CharacterData characterData, Dictionary<ObjectKind, HashSet<PlayerChanges>> updatedData, bool forceApplyMods)
    {
        if (!forceApplyMods)
            return;

        if (!updatedData.TryGetValue(ObjectKind.Player, out var playerChanges))
            return;

        if (!playerChanges.Contains(PlayerChanges.ModManip))
            return;

        if (string.IsNullOrEmpty(_lastAppliedManipulationFingerprint))
            return;

        var effectiveManipulationData = Pair.GetEffectiveManipulationData(characterData.ManipulationData);
        var desiredManipulationFingerprint = PairApplyUtilities.ComputeManipulationFingerprint(effectiveManipulationData);

        if (!string.Equals(desiredManipulationFingerprint, _lastAppliedManipulationFingerprint, StringComparison.Ordinal))
            return;

        playerChanges.Remove(PlayerChanges.ModManip);

        Logger.LogDebug(
            "[BASE-{appBase}] Skipping forced ModManip reprocess because effective manipulation payload is already applied",
            applicationBase);
    }

    private bool IsApplyPreparationStillValid(ApplyFrameworkSnapshot snapshot)
    {
        var currentCachedHash = _cachedData?.DataHash.Value ?? "NODATA";
        var currentCachedPayloadFingerprint = PairApplyUtilities.ComputeCharacterDataPayloadFingerprint(_cachedData);

        return string.Equals(currentCachedHash, snapshot.CachedHash, StringComparison.Ordinal)
            && string.Equals(currentCachedPayloadFingerprint, snapshot.CachedPayloadFingerprint, StringComparison.Ordinal)
            && _forceApplyMods == snapshot.ForceApplyMods;
    }
}
