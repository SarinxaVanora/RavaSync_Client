using System;

namespace RavaSync.MareConfiguration.Models;

[Serializable]
public sealed class RavaCastSavedScreenPlacement
{
    public string Name { get; set; } = string.Empty;
    public string ScreenName { get; set; } = string.Empty;
    public ushort TerritoryId { get; set; } = 0;
    public float CentreX { get; set; }
    public float CentreY { get; set; }
    public float CentreZ { get; set; }
    public float Width { get; set; } = 3.0f;
    public float Height { get; set; } = 1.70f;
    public float YawRadians { get; set; }
    public float PitchRadians { get; set; }
    public DateTime UpdatedUtc { get; set; } = DateTime.UtcNow;
}
