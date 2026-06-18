using Dalamud.Game.ClientState.Objects.Types;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.PlayerData.Handlers;
using RavaSync.PlayerData.Pairs;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using RavaSync.PlayerData.Services;

namespace RavaSync.Utils;

public static class VariousExtensions
{
    public static string ToByteString(this int bytes, bool addSuffix = true)
    {
        string[] suffix = ["B", "KiB", "MiB", "GiB", "TiB"];
        int i;
        double dblSByte = bytes;
        for (i = 0; i < suffix.Length && bytes >= 1024; i++, bytes /= 1024)
        {
            dblSByte = bytes / 1024.0;
        }

        return addSuffix ? $"{dblSByte:0.00} {suffix[i]}" : $"{dblSByte:0.00}";
    }

    public static string ToByteString(this long bytes, bool addSuffix = true)
    {
        string[] suffix = ["B", "KiB", "MiB", "GiB", "TiB"];
        int i;
        double dblSByte = bytes;
        for (i = 0; i < suffix.Length && bytes >= 1024; i++, bytes /= 1024)
        {
            dblSByte = bytes / 1024.0;
        }

        return addSuffix ? $"{dblSByte:0.00} {suffix[i]}" : $"{dblSByte:0.00}";
    }

    public static void CancelDispose(this CancellationTokenSource? cts)
    {
        try
        {
            cts?.Cancel();
            cts?.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // swallow it
        }
    }

    public static CancellationTokenSource CancelRecreate(this CancellationTokenSource? cts)
    {
        cts?.CancelDispose();
        return new CancellationTokenSource();
    }

    public static Dictionary<ObjectKind, HashSet<PlayerChanges>> CheckUpdatedData(this CharacterData newData, Guid applicationBase,
        CharacterData? oldData, ILogger logger, PairHandler cachedPlayer, bool forceApplyCustomization, bool forceApplyMods)
    {
        oldData ??= new();
        var charaDataToUpdate = new Dictionary<ObjectKind, HashSet<PlayerChanges>>();
        foreach (ObjectKind objectKind in Enum.GetValues<ObjectKind>())
        {
            charaDataToUpdate[objectKind] = [];
            oldData.FileReplacements.TryGetValue(objectKind, out var existingFileReplacements);
            newData.FileReplacements.TryGetValue(objectKind, out var newFileReplacements);
            oldData.GlamourerData.TryGetValue(objectKind, out var existingGlamourerData);
            newData.GlamourerData.TryGetValue(objectKind, out var newGlamourerData);

            bool hasNewButNotOldFileReplacements = newFileReplacements != null && existingFileReplacements == null;
            bool hasOldButNotNewFileReplacements = existingFileReplacements != null && newFileReplacements == null;

            bool hasNewButNotOldGlamourerData = newGlamourerData != null && existingGlamourerData == null;
            bool hasOldButNotNewGlamourerData = existingGlamourerData != null && newGlamourerData == null;

            bool hasNewAndOldFileReplacements = newFileReplacements != null && existingFileReplacements != null;
            bool hasNewAndOldGlamourerData = newGlamourerData != null && existingGlamourerData != null;

            if (hasNewButNotOldFileReplacements || hasOldButNotNewFileReplacements)
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (File replacement data arrived/removed: NewButNotOldFiles:{hasNewButNotOldFileReplacements}, OldButNotNewFiles:{hasOldButNotNewFileReplacements}) => {change}",
                    applicationBase, cachedPlayer, objectKind, hasNewButNotOldFileReplacements, hasOldButNotNewFileReplacements, PlayerChanges.ModFiles);

                charaDataToUpdate[objectKind].Add(PlayerChanges.ModFiles);
                if (ShouldForceOwnedObjectRedrawForFileChange(objectKind, existingFileReplacements, newFileReplacements))
                    charaDataToUpdate[objectKind].Add(PlayerChanges.ForcedRedraw);
            }
            else if (hasNewAndOldFileReplacements)
            {
                bool listsAreEqual = oldData.FileReplacements[objectKind].SequenceEqual(newData.FileReplacements[objectKind], PlayerData.Data.FileReplacementDataComparer.Instance);
                if (!listsAreEqual || forceApplyMods)
                {
                    logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (FileReplacements not equal) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.ModFiles);
                    charaDataToUpdate[objectKind].Add(PlayerChanges.ModFiles);
                    if (objectKind != ObjectKind.Player)
                    {
                        if (ShouldForceOwnedObjectRedrawForFileChange(objectKind, existingFileReplacements, newFileReplacements))
                            charaDataToUpdate[objectKind].Add(PlayerChanges.ForcedRedraw);
                    }
                    else
                    {
                        var existingFace = existingFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/face/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var existingHair = existingFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/hair/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var existingTail = existingFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/tail/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var newFace = newFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/face/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var newHair = newFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/hair/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var newTail = newFileReplacements.Where(g => g.GamePaths.Any(p => p.Contains("/tail/", StringComparison.OrdinalIgnoreCase)))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var existingTransients = existingFileReplacements.Where(g => g.GamePaths.Any(IsPlayerAssociatedTransientOrSupportPath))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var newTransients = newFileReplacements.Where(g => g.GamePaths.Any(IsPlayerAssociatedTransientOrSupportPath))
                            .OrderBy(g => string.IsNullOrEmpty(g.Hash) ? g.FileSwapPath : g.Hash, StringComparer.OrdinalIgnoreCase).ToList();
                        var existingWornEquipment = SelectPlayerWornEquipmentOrAccessoryReplacements(existingFileReplacements);
                        var newWornEquipment = SelectPlayerWornEquipmentOrAccessoryReplacements(newFileReplacements);

                        logger.LogTrace("[BASE-{appbase}] ExistingFace: {of}, NewFace: {fc}; ExistingHair: {eh}, NewHair: {nh}; ExistingTail: {et}, NewTail: {nt}; ExistingTransient: {etr}, NewTransient: {ntr}; ExistingWornGear: {ewg}, NewWornGear: {nwg}", applicationBase,
                            existingFace.Count, newFace.Count, existingHair.Count, newHair.Count, existingTail.Count, newTail.Count, existingTransients.Count, newTransients.Count, existingWornEquipment.Count, newWornEquipment.Count);
                        var differentFace = !existingFace.SequenceEqual(newFace, PlayerData.Data.FileReplacementDataComparer.Instance);
                        var differentHair = !existingHair.SequenceEqual(newHair, PlayerData.Data.FileReplacementDataComparer.Instance);
                        var differentTail = !existingTail.SequenceEqual(newTail, PlayerData.Data.FileReplacementDataComparer.Instance);
                        var differentTransients = !existingTransients.SequenceEqual(newTransients, PlayerData.Data.FileReplacementDataComparer.Instance);
                        var differentWornEquipment = !existingWornEquipment.SequenceEqual(newWornEquipment, PlayerData.Data.FileReplacementDataComparer.Instance);

                        if (differentWornEquipment)
                        {
                            logger.LogDebug("[BASE-{appbase}] Different worn equipment/accessory paths; applying receiver temp mod update without requesting a player redraw", applicationBase);
                            charaDataToUpdate[objectKind].Add(PlayerChanges.ModFiles);
                        }

                        if (differentTransients)
                        {
                            logger.LogDebug("[BASE-{appbase}] Different transient/animation/VFX paths: {transients}; redraw decision will be handled by the download-aware temp-content gate", applicationBase,
                                differentTransients);
                            charaDataToUpdate[objectKind].Add(PlayerChanges.ModFiles);
                        }

                        if (differentFace || differentHair || differentTail)
                        {
                            // Apply Glamourer and re-evaluate mod files so race swaps pull in the right textures.
                            charaDataToUpdate[objectKind].Add(PlayerChanges.Glamourer);
                            charaDataToUpdate[objectKind].Add(PlayerChanges.ModFiles);
                        }
                    }
                }
            }

            if (hasNewButNotOldGlamourerData || hasOldButNotNewGlamourerData)
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Glamourer data arrived/removed: NewButNotOldGlam:{hasNewButNotOldGlamourerData}, OldButNotNewGlam:{hasOldButNotNewGlamourerData}) => {change}",
                    applicationBase, cachedPlayer, objectKind, hasNewButNotOldGlamourerData, hasOldButNotNewGlamourerData, PlayerChanges.Glamourer);

                charaDataToUpdate[objectKind].Add(PlayerChanges.Glamourer);
                if (ShouldForceOwnedObjectRedrawForMetadataChange(objectKind, existingFileReplacements, newFileReplacements, oldData, newData))
                    charaDataToUpdate[objectKind].Add(PlayerChanges.ForcedRedraw);
            }
            else if (hasNewAndOldGlamourerData)
            {
                bool glamourerDataDifferent = !string.Equals(oldData.GlamourerData[objectKind], newData.GlamourerData[objectKind], StringComparison.Ordinal);
                if (glamourerDataDifferent || forceApplyCustomization)
                {
                    logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Glamourer different) => {change}",
                        applicationBase, cachedPlayer, objectKind, PlayerChanges.Glamourer);

                    charaDataToUpdate[objectKind].Add(PlayerChanges.Glamourer);
                }
            }

            oldData.CustomizePlusData.TryGetValue(objectKind, out var oldCustomizePlusData);
            newData.CustomizePlusData.TryGetValue(objectKind, out var newCustomizePlusData);

            oldCustomizePlusData ??= string.Empty;
            newCustomizePlusData ??= string.Empty;

            bool customizeDataDifferent = !string.Equals(oldCustomizePlusData, newCustomizePlusData, StringComparison.Ordinal);
            if (customizeDataDifferent || (forceApplyCustomization && !string.IsNullOrEmpty(newCustomizePlusData)))
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Diff customize data) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.Customize);
                charaDataToUpdate[objectKind].Add(PlayerChanges.Customize);
            }

            if (objectKind != ObjectKind.Player) continue;

            var oldEffectiveManipulationData = cachedPlayer.Pair.GetEffectiveManipulationData(oldData.ManipulationData);
            var newEffectiveManipulationData = cachedPlayer.Pair.GetEffectiveManipulationData(newData.ManipulationData);

            bool manipDataDifferent = !string.Equals(
                oldEffectiveManipulationData?.Trim() ?? string.Empty,
                newEffectiveManipulationData?.Trim() ?? string.Empty,
                StringComparison.Ordinal);

            if (manipDataDifferent || forceApplyMods)
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Manip data changed={changed}, forced={forced}) => {change}", applicationBase, cachedPlayer, objectKind, manipDataDifferent, forceApplyMods, PlayerChanges.ModManip);
                charaDataToUpdate[objectKind].Add(PlayerChanges.ModManip);

                if (manipDataDifferent)
                    charaDataToUpdate[objectKind].Add(PlayerChanges.ForcedRedraw);
            }

            bool heelsOffsetDifferent = !string.Equals(oldData.HeelsData, newData.HeelsData, StringComparison.Ordinal);
            if (heelsOffsetDifferent || (forceApplyCustomization && !string.IsNullOrEmpty(newData.HeelsData)))
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Diff heels data) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.Heels);
                charaDataToUpdate[objectKind].Add(PlayerChanges.Heels);
            }

            bool honorificDataDifferent = !string.Equals(oldData.HonorificData, newData.HonorificData, StringComparison.Ordinal);
            if (honorificDataDifferent || (forceApplyCustomization && !string.IsNullOrEmpty(newData.HonorificData)))
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Diff honorific data) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.Honorific);
                charaDataToUpdate[objectKind].Add(PlayerChanges.Honorific);
            }

            bool moodlesDataDifferent = !string.Equals(oldData.MoodlesData, newData.MoodlesData, StringComparison.Ordinal);
            if (moodlesDataDifferent || (forceApplyCustomization && !string.IsNullOrEmpty(newData.MoodlesData)))
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Diff moodles data) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.Moodles);
                charaDataToUpdate[objectKind].Add(PlayerChanges.Moodles);
            }

            bool petNamesDataDifferent = !string.Equals(oldData.PetNamesData, newData.PetNamesData, StringComparison.Ordinal);
            if (petNamesDataDifferent || (forceApplyCustomization && !string.IsNullOrEmpty(newData.PetNamesData)))
            {
                logger.LogDebug("[BASE-{appBase}] Updating {object}/{kind} (Diff petnames data) => {change}", applicationBase, cachedPlayer, objectKind, PlayerChanges.PetNames);
                charaDataToUpdate[objectKind].Add(PlayerChanges.PetNames);
            }
        }

        foreach (KeyValuePair<ObjectKind, HashSet<PlayerChanges>> data in charaDataToUpdate.ToList())
        {
            if (!data.Value.Any()) charaDataToUpdate.Remove(data.Key);
            else charaDataToUpdate[data.Key] = [.. data.Value.OrderByDescending(p => (int)p)];
        }

        return charaDataToUpdate;
    }


    private static List<FileReplacementData> SelectPlayerWornEquipmentOrAccessoryReplacements(List<FileReplacementData>? replacements)
    {
        if (replacements == null || replacements.Count == 0)
            return [];

        return replacements
            .Where(static replacement => replacement.GamePaths.Any(IsPlayerWornEquipmentOrAccessoryGamePath))
            .OrderBy(static replacement => string.IsNullOrEmpty(replacement.Hash) ? replacement.FileSwapPath : replacement.Hash, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static replacement => string.Join("|", replacement.GamePaths.OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)), StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool IsPlayerWornEquipmentOrAccessoryGamePath(string gamePath)
        => PairApplyUtilities.IsWornEquipmentOrAccessoryGamePath(gamePath);

    private static bool IsPlayerAssociatedTransientOrSupportPath(string gamePath)
        => PairApplyUtilities.IsTransientRedrawCriticalGamePath(gamePath)
            || PairApplyUtilities.IsVfxPropSupportGamePath(gamePath);

    private static bool ShouldForceOwnedObjectRedrawForFileChange(ObjectKind objectKind, List<FileReplacementData>? existingFileReplacements, List<FileReplacementData>? newFileReplacements)
    {
        if (objectKind == ObjectKind.Player)
            return false;

        return HasMeaningfulFileReplacements(existingFileReplacements)
            || HasMeaningfulFileReplacements(newFileReplacements);
    }

    private static bool ShouldForceOwnedObjectRedrawForMetadataChange(ObjectKind objectKind, List<FileReplacementData>? existingFileReplacements, List<FileReplacementData>? newFileReplacements, CharacterData oldData, CharacterData newData)
    {
        if (objectKind == ObjectKind.Player)
            return false;

        if (objectKind != ObjectKind.MinionOrMount)
            return true;

        return HasMeaningfulFileReplacements(existingFileReplacements)
            || HasMeaningfulFileReplacements(newFileReplacements)
            || HasMeaningfulCustomizePayload(oldData, objectKind)
            || HasMeaningfulCustomizePayload(newData, objectKind);
    }

    private static bool HasMeaningfulFileReplacements(List<FileReplacementData>? replacements)
        => replacements != null && replacements.Any(static replacement =>
            !string.IsNullOrWhiteSpace(replacement.Hash)
            || !string.IsNullOrWhiteSpace(replacement.FileSwapPath)
            || replacement.GamePaths.Any());

    private static bool HasMeaningfulCustomizePayload(CharacterData data, ObjectKind objectKind)
        => data.CustomizePlusData.TryGetValue(objectKind, out var payload)
            && !string.IsNullOrWhiteSpace(payload);

    public static T DeepClone<T>(this T obj)
    {
        return JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(obj))!;
    }

    public static unsafe int? ObjectTableIndex(this IGameObject? gameObject)
    {
        if (gameObject == null || gameObject.Address == IntPtr.Zero)
        {
            return null;
        }

        return ((FFXIVClientStructs.FFXIV.Client.Game.Object.GameObject*)gameObject.Address)->ObjectIndex;
    }
}