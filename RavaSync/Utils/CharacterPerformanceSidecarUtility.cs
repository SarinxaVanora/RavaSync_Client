using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using RavaSync.API.Data;

namespace RavaSync.Utils;

public sealed class CharacterPerformanceSidecarUtility
{
    private const string CarrierPropertyName = "__ravasync_perf";
    private const string RawCarrierPropertyName = "__ravasync_raw";
    private const int CurrentVersion = 1;

    private sealed record PerfPayload(int v, long vb, long tr);

    public bool TryEmbed(CharacterData? charaData, long vramBytes, long triangles)
    {
        if (charaData == null)
            return false;

        var payload = new PerfPayload(CurrentVersion, Math.Max(0, vramBytes), Math.Max(0, triangles));
        var payloadBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload)));

        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryEmbedIntoJsonCarrier(petNamesData, payloadBase64, allowEmptyCarrier: true, out var updatedPetNamesData))
        {
            charaData.PetNamesData = updatedPetNamesData;
            return true;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryEmbedIntoJsonCarrier(moodlesData, payloadBase64, allowEmptyCarrier: false, out var updatedMoodlesData))
        {
            charaData.MoodlesData = updatedMoodlesData;
            return true;
        }

        return false;
    }

    public bool TryExtract(CharacterData? charaData, out long vramBytes, out long triangles)
    {
        vramBytes = 0;
        triangles = 0;

        if (charaData == null)
            return false;

        var petNamesData = charaData.PetNamesData ?? string.Empty;
        if (TryExtractFromJsonCarrier(petNamesData, out var sanitizedPetNamesData, out vramBytes, out triangles))
        {
            charaData.PetNamesData = sanitizedPetNamesData;
            return true;
        }

        var moodlesData = charaData.MoodlesData ?? string.Empty;
        if (TryExtractFromJsonCarrier(moodlesData, out var sanitizedMoodlesData, out vramBytes, out triangles))
        {
            charaData.MoodlesData = sanitizedMoodlesData;
            return true;
        }

        return false;
    }

    private static bool TryEmbedIntoJsonCarrier(string carrier, string payloadBase64, bool allowEmptyCarrier, out string updatedCarrier)
    {
        updatedCarrier = carrier ?? string.Empty;

        if (!string.IsNullOrEmpty(updatedCarrier))
        {
            var parsed = ParseJsonObject(updatedCarrier);
            if (parsed != null)
            {
                parsed[CarrierPropertyName] = payloadBase64;
                updatedCarrier = parsed.ToJsonString();
                return true;
            }

            var wrapped = new JsonObject
            {
                [CarrierPropertyName] = payloadBase64,
                [RawCarrierPropertyName] = Convert.ToBase64String(Encoding.UTF8.GetBytes(updatedCarrier))
            };

            updatedCarrier = wrapped.ToJsonString();
            return true;
        }

        if (!allowEmptyCarrier)
            return false;

        var root = new JsonObject
        {
            [CarrierPropertyName] = payloadBase64
        };

        updatedCarrier = root.ToJsonString();
        return true;
    }

    private static bool TryExtractFromJsonCarrier(string carrier, out string sanitizedCarrier, out long vramBytes, out long triangles)
    {
        sanitizedCarrier = carrier ?? string.Empty;
        vramBytes = 0;
        triangles = 0;

        if (string.IsNullOrWhiteSpace(sanitizedCarrier))
            return false;

        var root = ParseJsonObject(sanitizedCarrier);
        if (root == null)
            return false;

        var encoded = root[CarrierPropertyName]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(encoded))
            return false;

        PerfPayload? payload;
        try
        {
            var json = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
            payload = JsonSerializer.Deserialize<PerfPayload>(json);
        }
        catch
        {
            return false;
        }

        if (payload == null || payload.v != CurrentVersion)
            return false;

        if (root[RawCarrierPropertyName] is JsonNode rawNode)
        {
            try
            {
                var rawEncoded = rawNode.GetValue<string>();
                sanitizedCarrier = string.IsNullOrWhiteSpace(rawEncoded)
                    ? string.Empty
                    : Encoding.UTF8.GetString(Convert.FromBase64String(rawEncoded));
            }
            catch
            {
                sanitizedCarrier = string.Empty;
            }
        }
        else
        {
            root.Remove(CarrierPropertyName);
            sanitizedCarrier = root.Count == 0 ? string.Empty : root.ToJsonString();
        }

        vramBytes = Math.Max(0, payload.vb);
        triangles = Math.Max(0, payload.tr);
        return true;
    }

    private static JsonObject? ParseJsonObject(string value)
    {
        try
        {
            var node = JsonNode.Parse(value);
            return node as JsonObject;
        }
        catch
        {
            return null;
        }
    }
}
