namespace RavaSync.Themes;

[Serializable]
public sealed class Theme
{
    public int SchemaVersion { get; set; } = 1;
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";      
    public string? Inherits { get; set; }        

    public Colors Colors { get; set; } = new();
    public Typography Typography { get; set; } = new();
    public Effects Effects { get; set; } = new();
    public Assets Assets { get; set; } = new();
}

[Serializable]
public sealed class Colors
{
    public string Primary { get; set; } = "#8A2BE2";
    public string Accent { get; set; } = "#A855F7";
    public string Background { get; set; } = "#0B0612";
    public string BackgroundAlt { get; set; } = "#130A1F";
    public string Surface { get; set; } = "#1E132F";
    public string Text { get; set; } = "#FFFFFF";
    public string MutedText { get; set; } = "#C9B9E6";
    public string Success { get; set; } = "#33D17A";
    public string Warning { get; set; } = "#F5C542";
    public string Danger { get; set; } = "#FF4D6D";
    public string Info { get; set; } = "#6CC3FF";
    public string Border { get; set; } = "#2F2246";
    public string Highlight { get; set; } = "#B98AFF";
}

[Serializable]
public sealed class Typography
{
    public string UiFont { get; set; } = "Inter";
    public string DisplayFont { get; set; } = "Cinzel Decorative";
    public double BaseSize { get; set; } = 14;
    public double Scale { get; set; } = 1.0;
}

[Serializable]
public sealed class Effects
{
    public double GlowIntensity { get; set; } = 0.0;
    public double NoiseOpacity { get; set; } = 0.05;
    public bool Particles { get; set; } = false;
    public string ParticleStyle { get; set; } = "none";
    public bool RoundedUi { get; set; } = true;
}

[Serializable]
public sealed class Assets
{
    public string? BackgroundImage { get; set; }
    public string? Logo { get; set; }
    public string? Watermark { get; set; }
}
