using Dalamud.Bindings.ImGui;
using System;

namespace RavaSync.Services.RavaCast.Rendering;

/// <summary>
/// Represents the current renderable texture for RavaCast. TextureId is the ImGui/Dalamud-facing
/// handle so the world renderer does not need to know the details of the WebView2/WGC shared-texture frame source.
/// </summary>
public sealed record RavaCastTextureFrame(ImTextureID TextureId, int Width, int Height, long FrameIndex, DateTime CapturedUtc)
{
    public bool IsValid => Width > 0 && Height > 0;
}
