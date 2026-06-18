using System;

namespace RavaSync.Services.RavaCast.Rendering;

public sealed record RavaCastBackendStatus(bool IsAvailable, bool IsOpen, bool HasRenderableTexture, string BackendName, string StatusText, string? Detail = null)
{
    public static RavaCastBackendStatus Unavailable(string detail) => new(false, false, false, "None", "Backend unavailable", detail);
    public static RavaCastBackendStatus Idle(string backendName) => new(true, false, false, backendName, "Ready");
}
