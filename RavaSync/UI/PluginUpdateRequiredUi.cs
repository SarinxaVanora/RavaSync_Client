using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Windowing;
using RavaSync.Services;
using System.Numerics;

namespace RavaSync.UI;

public sealed class PluginUpdateRequiredUi : Window
{
    private readonly PluginUpdateGate _pluginUpdateGate;

    public string StatusText { get; set; } = string.Empty;

    public PluginUpdateRequiredUi(PluginUpdateGate pluginUpdateGate) : base("RavaSync Update Required###RavaSyncUpdateRequired")
    {
        _pluginUpdateGate = pluginUpdateGate;
        IsOpen = true;
        RespectCloseHotkey = false;
        AllowPinning = false;
        AllowClickthrough = false;
        Flags |= ImGuiWindowFlags.NoCollapse | ImGuiWindowFlags.NoDocking | ImGuiWindowFlags.AlwaysAutoResize;
        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(385, 0),
            MaximumSize = new Vector2(385, 900),
        };
    }

    public override void Draw()
    {
        var scale = ImGuiHelpers.GlobalScale;
        var contentW = MathF.Max(320f * scale, ImGui.GetContentRegionAvail().X);
        var dl = ImGui.GetWindowDrawList();
        var start = ImGui.GetCursorScreenPos();

        ImGui.BeginGroup();
        ImGui.Dummy(new Vector2(0, 8f * scale));

        DrawCenteredTitle("RavaSync update available", contentW, scale);
        ImGui.Spacing();

        ImGui.PushTextWrapPos(ImGui.GetCursorPosX() + contentW - (18f * scale));
        ImGui.TextColored(ImGuiColors.DalamudYellow, "Normal startup has been paused.");
        ImGui.TextWrapped("Dalamud has detected a newer RavaSync build. Update before connecting so your local plugin, cache, and sync pipeline stay on the expected version.");
        ImGui.PopTextWrapPos();

        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.TextDisabled($"Loaded version: {_pluginUpdateGate.CurrentVersionText}");
        ImGui.TextDisabled($"Reported update: {_pluginUpdateGate.ReportedUpdateVersionText}");
        ImGui.TextDisabled($"Search target: {_pluginUpdateGate.PluginSearchText}");

        if (!string.IsNullOrWhiteSpace(StatusText))
        {
            ImGui.Spacing();
            ImGui.TextWrapped(StatusText);
        }

        ImGui.Spacing();

        if (ImGui.Button("Update RavaSync", new Vector2(contentW, 38f * scale)))
            _pluginUpdateGate.OpenUpdater();

        ImGui.PushTextWrapPos(ImGui.GetCursorPosX() + contentW - (18f * scale));
        ImGui.TextWrapped("This opens Dalamud's plugin installer to the updateable plugins page. RavaSync will keep checking and finish loading once Dalamud reports that the update is installed.");
        ImGui.PopTextWrapPos();

        ImGui.Dummy(new Vector2(0, 8f * scale));
        ImGui.EndGroup();

        var end = ImGui.GetCursorScreenPos();
        var h = MathF.Max(0f, end.Y - start.Y);
        var bg = ImGui.GetColorU32(new Vector4(0.40f, 0.20f, 0.60f, 0.10f));
        var border = ImGui.GetColorU32(new Vector4(0.60f, 0.35f, 0.90f, 0.22f));
        dl.AddRectFilled(start, start + new Vector2(contentW, h), bg, 10f * scale);
        dl.AddRect(start, start + new Vector2(contentW, h), border, 10f * scale);
    }

    private static void DrawCenteredTitle(string text, float contentW, float scale)
    {
        var size = ImGui.CalcTextSize(text);
        ImGui.SetCursorPosX(MathF.Max(0f, (contentW - size.X) * 0.5f));
        ImGui.TextColored(new Vector4(0.90f, 0.70f, 1.00f, 1.00f), text);
    }
}
