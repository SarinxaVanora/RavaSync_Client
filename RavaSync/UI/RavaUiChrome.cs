using System;
using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility;
using System.Numerics;

namespace RavaSync.UI;

internal static class RavaUiChrome
{
    private static ChromePalette Palette => BuildPalette();

    private static ChromePalette BuildPalette()
    {
        var style = ImGui.GetStyle();

        var text = EnsureVisible(style.Colors[(int)ImGuiCol.Text], new Vector4(0.94f, 0.90f, 0.98f, 1.00f));
        var muted = EnsureVisible(style.Colors[(int)ImGuiCol.TextDisabled], new Vector4(0.72f, 0.66f, 0.80f, 0.92f));
        var window = EnsureVisible(style.Colors[(int)ImGuiCol.WindowBg], new Vector4(0.075f, 0.058f, 0.095f, 0.96f));
        var child = EnsureVisible(style.Colors[(int)ImGuiCol.ChildBg], new Vector4(0.090f, 0.064f, 0.118f, 0.72f));
        var frame = EnsureVisible(style.Colors[(int)ImGuiCol.FrameBg], new Vector4(0.130f, 0.090f, 0.170f, 0.88f));
        var border = EnsureVisible(style.Colors[(int)ImGuiCol.Border], new Vector4(0.56f, 0.30f, 0.82f, 0.42f));
        var button = EnsureVisible(style.Colors[(int)ImGuiCol.Button], new Vector4(0.210f, 0.115f, 0.300f, 0.88f));
        var buttonHover = EnsureVisible(style.Colors[(int)ImGuiCol.ButtonHovered], new Vector4(0.360f, 0.185f, 0.520f, 0.95f));
        var buttonActive = EnsureVisible(style.Colors[(int)ImGuiCol.ButtonActive], new Vector4(0.720f, 0.34f, 1.00f, 1.00f));
        var header = EnsureVisible(style.Colors[(int)ImGuiCol.Header], Mix(button, frame, 0.30f));
        var headerHover = EnsureVisible(style.Colors[(int)ImGuiCol.HeaderHovered], Mix(buttonHover, buttonActive, 0.25f));
        var headerActive = EnsureVisible(style.Colors[(int)ImGuiCol.HeaderActive], Mix(buttonActive, buttonHover, 0.20f));

        var accent = WithAlpha(buttonActive, 1f);
        var accentHot = WithAlpha(Brighten(Mix(buttonActive, buttonHover, 0.35f), 0.14f), 1f);
        var surface = WithAlpha(Mix(window, child, 0.25f), 0.96f);
        var surfaceRaised = WithAlpha(Mix(child, frame, 0.48f), 0.96f);
        var surfaceRaisedHot = WithAlpha(Mix(surfaceRaised, accent, 0.18f), 0.98f);
        var chromeBorder = WithAlpha(Mix(border, accent, 0.50f), 0.48f);
        var chromeBorderSoft = WithAlpha(Mix(border, accent, 0.35f), 0.22f);

        return new ChromePalette(
            Text: WithAlpha(text, 1f),
            MutedText: WithAlpha(muted, MathF.Max(0.88f, muted.W)),
            Accent: accent,
            AccentHot: accentHot,
            AccentSoft: WithAlpha(accent, 0.42f),
            Surface: surface,
            SurfaceRaised: surfaceRaised,
            SurfaceRaisedHot: surfaceRaisedHot,
            Border: chromeBorder,
            BorderSoft: chromeBorderSoft,
            Frame: WithAlpha(Mix(frame, surfaceRaised, 0.15f), MathF.Max(0.86f, frame.W)),
            FrameHovered: WithAlpha(Mix(style.Colors[(int)ImGuiCol.FrameBgHovered], accent, 0.16f), 0.92f),
            FrameActive: WithAlpha(Mix(style.Colors[(int)ImGuiCol.FrameBgActive], accent, 0.22f), 0.96f),
            Button: WithAlpha(Mix(button, surfaceRaised, 0.20f), MathF.Max(0.86f, button.W)),
            ButtonHovered: WithAlpha(Mix(buttonHover, accent, 0.16f), 0.95f),
            ButtonActive: WithAlpha(Mix(buttonActive, accentHot, 0.12f), 1.00f),
            Header: WithAlpha(Mix(header, surfaceRaised, 0.18f), 0.76f),
            HeaderHovered: WithAlpha(Mix(headerHover, accent, 0.15f), 0.82f),
            HeaderActive: WithAlpha(Mix(headerActive, accentHot, 0.10f), 0.92f));
    }


    public static Vector4 PanelBackground
    {
        get
        {
            var p = Palette;
            return WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.06f), 0.52f);
        }
    }

    public static Vector4 PanelBorder
    {
        get
        {
            var p = Palette;
            return WithAlpha(Mix(p.Border, p.Accent, 0.34f), 0.30f);
        }
    }

    public static Vector4 PanelText => Palette.Text;
    public static Vector4 PanelMutedText => Palette.MutedText;
    public static Vector4 PanelAccentText => Palette.AccentHot;

    public static IDisposable BeginScope()
    {
        var scale = ImGuiHelpers.GlobalScale;
        var p = Palette;
        var styleCount = 0;
        var colorCount = 0;

        void VarFloat(ImGuiStyleVar idx, float value)
        {
            ImGui.PushStyleVar(idx, value);
            styleCount++;
        }

        void VarVec(ImGuiStyleVar idx, Vector2 value)
        {
            ImGui.PushStyleVar(idx, value);
            styleCount++;
        }

        void Col(ImGuiCol idx, Vector4 value)
        {
            ImGui.PushStyleColor(idx, value);
            colorCount++;
        }

        VarVec(ImGuiStyleVar.WindowPadding, new Vector2(12f, 11f) * scale);
        VarVec(ImGuiStyleVar.FramePadding, new Vector2(10f, 5f) * scale);
        VarVec(ImGuiStyleVar.ItemSpacing, new Vector2(8f, 7f) * scale);
        VarVec(ImGuiStyleVar.ItemInnerSpacing, new Vector2(6f, 5f) * scale);
        VarVec(ImGuiStyleVar.CellPadding, new Vector2(8f, 5f) * scale);
        VarFloat(ImGuiStyleVar.IndentSpacing, 17f * scale);
        VarFloat(ImGuiStyleVar.ScrollbarSize, 12f * scale);
        VarFloat(ImGuiStyleVar.GrabMinSize, 12f * scale);
        VarFloat(ImGuiStyleVar.WindowRounding, 13f * scale);
        VarFloat(ImGuiStyleVar.ChildRounding, 11f * scale);
        VarFloat(ImGuiStyleVar.FrameRounding, 8f * scale);
        VarFloat(ImGuiStyleVar.PopupRounding, 10f * scale);
        VarFloat(ImGuiStyleVar.ScrollbarRounding, 10f * scale);
        VarFloat(ImGuiStyleVar.GrabRounding, 8f * scale);
        VarFloat(ImGuiStyleVar.TabRounding, 8f * scale);
        VarFloat(ImGuiStyleVar.WindowBorderSize, 1f * scale);
        VarFloat(ImGuiStyleVar.ChildBorderSize, 1f * scale);
        VarFloat(ImGuiStyleVar.PopupBorderSize, 1f * scale);
        VarFloat(ImGuiStyleVar.FrameBorderSize, 1f * scale);

        Col(ImGuiCol.Text, p.Text);
        Col(ImGuiCol.TextDisabled, p.MutedText);
        Col(ImGuiCol.WindowBg, p.Surface);
        Col(ImGuiCol.ChildBg, WithAlpha(p.SurfaceRaised, 0.72f));
        Col(ImGuiCol.PopupBg, WithAlpha(Darken(p.Surface, 0.025f), 0.98f));
        Col(ImGuiCol.Border, p.BorderSoft);
        Col(ImGuiCol.BorderShadow, new Vector4(0f, 0f, 0f, 0f));
        Col(ImGuiCol.FrameBg, p.Frame);
        Col(ImGuiCol.FrameBgHovered, p.FrameHovered);
        Col(ImGuiCol.FrameBgActive, p.FrameActive);
        Col(ImGuiCol.TitleBg, WithAlpha(Darken(p.Surface, 0.025f), 1f));
        Col(ImGuiCol.TitleBgActive, WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.20f), 1f));
        Col(ImGuiCol.TitleBgCollapsed, WithAlpha(Darken(p.Surface, 0.035f), 0.88f));
        Col(ImGuiCol.MenuBarBg, WithAlpha(Mix(p.Surface, p.SurfaceRaised, 0.30f), 0.92f));
        Col(ImGuiCol.ScrollbarBg, WithAlpha(Darken(p.Surface, 0.030f), 0.55f));
        Col(ImGuiCol.ScrollbarGrab, WithAlpha(Mix(p.Border, p.Accent, 0.25f), 0.65f));
        Col(ImGuiCol.ScrollbarGrabHovered, WithAlpha(Mix(p.Border, p.Accent, 0.55f), 0.82f));
        Col(ImGuiCol.ScrollbarGrabActive, WithAlpha(p.AccentHot, 0.96f));
        Col(ImGuiCol.CheckMark, p.AccentHot);
        Col(ImGuiCol.SliderGrab, p.Accent);
        Col(ImGuiCol.SliderGrabActive, p.AccentHot);
        Col(ImGuiCol.Button, p.Button);
        Col(ImGuiCol.ButtonHovered, p.ButtonHovered);
        Col(ImGuiCol.ButtonActive, p.ButtonActive);
        Col(ImGuiCol.Header, p.Header);
        Col(ImGuiCol.HeaderHovered, p.HeaderHovered);
        Col(ImGuiCol.HeaderActive, p.HeaderActive);
        Col(ImGuiCol.Separator, WithAlpha(p.Border, 0.30f));
        Col(ImGuiCol.SeparatorHovered, WithAlpha(p.Accent, 0.65f));
        Col(ImGuiCol.SeparatorActive, p.AccentHot);
        Col(ImGuiCol.ResizeGrip, WithAlpha(Mix(p.Border, p.Accent, 0.35f), 0.32f));
        Col(ImGuiCol.ResizeGripHovered, WithAlpha(Mix(p.Border, p.Accent, 0.65f), 0.62f));
        Col(ImGuiCol.ResizeGripActive, p.AccentHot);
        Col(ImGuiCol.Tab, WithAlpha(Mix(p.SurfaceRaised, p.Button, 0.22f), 0.92f));
        Col(ImGuiCol.TabHovered, WithAlpha(Mix(p.ButtonHovered, p.Accent, 0.25f), 0.95f));
        Col(ImGuiCol.TabActive, WithAlpha(Mix(p.ButtonActive, p.SurfaceRaised, 0.20f), 1.00f));
        Col(ImGuiCol.TabUnfocused, WithAlpha(Darken(p.SurfaceRaised, 0.020f), 0.88f));
        Col(ImGuiCol.TabUnfocusedActive, WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.15f), 0.92f));
        Col(ImGuiCol.TableHeaderBg, WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.10f), 0.86f));
        Col(ImGuiCol.TableBorderStrong, WithAlpha(p.Border, 0.42f));
        Col(ImGuiCol.TableBorderLight, WithAlpha(p.Border, 0.20f));
        Col(ImGuiCol.TableRowBg, WithAlpha(p.Surface, 0.22f));
        Col(ImGuiCol.TableRowBgAlt, WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.05f), 0.28f));
        Col(ImGuiCol.TextSelectedBg, WithAlpha(p.Accent, 0.38f));
        Col(ImGuiCol.DragDropTarget, p.AccentHot);
        Col(ImGuiCol.NavHighlight, WithAlpha(p.AccentHot, 0.72f));
        Col(ImGuiCol.ModalWindowDimBg, new Vector4(0.010f, 0.006f, 0.015f, 0.68f));

        return new Scope(() =>
        {
            ImGui.PopStyleColor(colorCount);
            ImGui.PopStyleVar(styleCount);
        });
    }

    public static void DrawHero(string title, string subtitle, string? rightText = null, Vector4? rightColor = null, Action? content = null, float contentHeight = 0f)
    {
        var scale = ImGuiHelpers.GlobalScale;
        var p = Palette;
        var draw = ImGui.GetWindowDrawList();
        var start = ImGui.GetCursorScreenPos();
        var width = MathF.Max(1f, ImGui.GetContentRegionAvail().X);
        var hasSubtitle = !string.IsNullOrWhiteSpace(subtitle);
        var hasContent = content is not null;
        var extraHeight = hasContent ? MathF.Max(contentHeight, 0f) : 0f;
        var baseHeight = hasSubtitle ? 58f : 44f;
        var height = (baseHeight * scale) + extraHeight;
        var pad = new Vector2(14f, hasSubtitle ? 9f : 10f) * scale;
        var end = start + new Vector2(width, height);

        draw.AddRectFilled(start, end, ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.07f), 0.92f)), 13f * scale);
        draw.AddRectFilledMultiColor(
            start,
            end,
            ImGui.GetColorU32(WithAlpha(Mix(p.Accent, p.SurfaceRaised, 0.25f), 0.24f)),
            ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.12f), 0.40f)),
            ImGui.GetColorU32(WithAlpha(p.Surface, 0.18f)),
            ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.20f), 0.22f)));
        draw.AddRect(start, end, ImGui.GetColorU32(WithAlpha(p.Border, 0.50f)), 13f * scale, ImDrawFlags.None, 1.25f * scale);

        ImGui.SetCursorScreenPos(start + pad);
        ImGui.TextUnformatted(title);
        if (hasSubtitle)
        {
            ImGui.PushStyleColor(ImGuiCol.Text, p.MutedText);
            ImGui.TextUnformatted(subtitle);
            ImGui.PopStyleColor();
        }

        if (hasContent)
        {
            var contentY = start.Y + pad.Y + ImGui.GetTextLineHeightWithSpacing() + (hasSubtitle ? ImGui.GetTextLineHeightWithSpacing() : 0f) + (2f * scale);
            ImGui.SetCursorScreenPos(new Vector2(start.X + pad.X, contentY));
            content!.Invoke();
        }

        if (!string.IsNullOrWhiteSpace(rightText))
        {
            var textSize = ImGui.CalcTextSize(rightText);
            var pillPad = new Vector2(10f, 4f) * scale;
            var pillSize = textSize + (pillPad * 2f);
            var pillY = hasContent ? start.Y + (10f * scale) : start.Y + (height - pillSize.Y) * 0.5f;
            var pillMin = new Vector2(end.X - pillSize.X - (12f * scale), pillY);
            var pillMax = pillMin + pillSize;
            var col = rightColor ?? p.Accent;
            draw.AddRectFilled(pillMin, pillMax, ImGui.GetColorU32(WithAlpha(col, 0.18f)), pillSize.Y * 0.5f);
            draw.AddRect(pillMin, pillMax, ImGui.GetColorU32(WithAlpha(col, 0.58f)), pillSize.Y * 0.5f);
            draw.AddText(pillMin + pillPad, ImGui.GetColorU32(col), rightText);
        }

        ImGui.SetCursorScreenPos(new Vector2(start.X, end.Y + (7f * scale)));
    }

    public static void DrawSectionTitle(string title, string? subtitle = null)
    {
        var scale = ImGuiHelpers.GlobalScale;
        var p = Palette;
        var draw = ImGui.GetWindowDrawList();
        var pos = ImGui.GetCursorScreenPos();
        var width = MathF.Max(1f, ImGui.GetContentRegionAvail().X);
        var hasSubtitle = !string.IsNullOrWhiteSpace(subtitle);
        var titleSize = ImGui.CalcTextSize(title);
        var subtitleSize = hasSubtitle ? ImGui.CalcTextSize(subtitle) : Vector2.Zero;
        var pad = new Vector2(12f, 7f) * scale;
        var height = MathF.Max((hasSubtitle ? 46f : 34f) * scale, titleSize.Y + subtitleSize.Y + pad.Y * 2f + (hasSubtitle ? 1f * scale : 0f));
        var end = pos + new Vector2(width, height);

        draw.AddRectFilled(pos, end, ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.06f), 0.82f)), 12f * scale);
        draw.AddRectFilledMultiColor(
            pos,
            end,
            ImGui.GetColorU32(WithAlpha(Mix(p.Accent, p.SurfaceRaised, 0.28f), 0.20f)),
            ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.10f), 0.26f)),
            ImGui.GetColorU32(WithAlpha(p.Surface, 0.14f)),
            ImGui.GetColorU32(WithAlpha(Mix(p.SurfaceRaised, p.Accent, 0.18f), 0.18f)));
        draw.AddRect(pos, end, ImGui.GetColorU32(WithAlpha(p.Border, 0.40f)), 12f * scale, ImDrawFlags.None, 1f * scale);

        var font = ImGui.GetFont();
        var titleFontSize = ImGui.GetFontSize() * 1.16f;
        var titlePos = pos + pad;
        draw.AddText(font, titleFontSize, titlePos, ImGui.GetColorU32(p.Text), title);

        if (hasSubtitle)
        {
            var subtitlePos = new Vector2(titlePos.X, titlePos.Y + titleFontSize + (2f * scale));
            draw.AddText(subtitlePos, ImGui.GetColorU32(WithAlpha(p.MutedText, 0.96f)), subtitle!);
        }

        ImGui.SetCursorScreenPos(new Vector2(pos.X, end.Y + (7f * scale)));
    }

    public static IDisposable BeginCard(string id, float height = 0f, bool border = true)
    {
        var scale = ImGuiHelpers.GlobalScale;
        var p = Palette;
        var styleCount = 0;
        var colorCount = 0;

        ImGui.PushStyleVar(ImGuiStyleVar.ChildRounding, 12f * scale); styleCount++;
        ImGui.PushStyleVar(ImGuiStyleVar.ChildBorderSize, border ? 1f * scale : 0f); styleCount++;
        ImGui.PushStyleVar(ImGuiStyleVar.WindowPadding, new Vector2(12f, 10f) * scale); styleCount++;
        ImGui.PushStyleColor(ImGuiCol.ChildBg, p.SurfaceRaised); colorCount++;
        ImGui.PushStyleColor(ImGuiCol.Border, p.BorderSoft); colorCount++;

        ImGui.BeginChild(id, new Vector2(0f, height), border, ImGuiWindowFlags.NoScrollbar | ImGuiWindowFlags.NoScrollWithMouse);

        return new Scope(() =>
        {
            ImGui.EndChild();
            ImGui.PopStyleColor(colorCount);
            ImGui.PopStyleVar(styleCount);
        });
    }

    public static bool DrawSegmentTab(string label, bool selected, float? width = null)
    {
        var p = Palette;
        var colorCount = 0;
        if (selected)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, WithAlpha(Mix(p.ButtonActive, p.Accent, 0.18f), 0.92f)); colorCount++;
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, WithAlpha(Mix(p.ButtonHovered, p.AccentHot, 0.20f), 0.98f)); colorCount++;
            ImGui.PushStyleColor(ImGuiCol.Text, Brighten(p.Text, 0.08f)); colorCount++;
        }
        else
        {
            ImGui.PushStyleColor(ImGuiCol.Button, WithAlpha(Mix(p.Button, p.SurfaceRaised, 0.30f), 0.74f)); colorCount++;
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, WithAlpha(Mix(p.ButtonHovered, p.Accent, 0.12f), 0.90f)); colorCount++;
            ImGui.PushStyleColor(ImGuiCol.Text, p.MutedText); colorCount++;
        }

        var pressed = width.HasValue ? ImGui.Button(label, new Vector2(width.Value, 0f)) : ImGui.Button(label);
        ImGui.PopStyleColor(colorCount);
        return pressed;
    }

    public static void DrawMutedText(string text)
    {
        ImGui.PushStyleColor(ImGuiCol.Text, Palette.MutedText);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
    }

    private static Vector4 EnsureVisible(Vector4 value, Vector4 fallback)
    {
        if (value.W <= 0.01f && value.X <= 0.01f && value.Y <= 0.01f && value.Z <= 0.01f)
            return fallback;

        return value;
    }

    private static Vector4 WithAlpha(Vector4 value, float alpha)
        => new(value.X, value.Y, value.Z, Math.Clamp(alpha, 0f, 1f));

    private static Vector4 Mix(Vector4 a, Vector4 b, float amount)
    {
        amount = Math.Clamp(amount, 0f, 1f);
        return new Vector4(
            a.X + ((b.X - a.X) * amount),
            a.Y + ((b.Y - a.Y) * amount),
            a.Z + ((b.Z - a.Z) * amount),
            a.W + ((b.W - a.W) * amount));
    }

    private static Vector4 Brighten(Vector4 value, float amount)
    {
        amount = Math.Clamp(amount, 0f, 1f);
        return new Vector4(
            value.X + ((1f - value.X) * amount),
            value.Y + ((1f - value.Y) * amount),
            value.Z + ((1f - value.Z) * amount),
            value.W);
    }

    private static Vector4 Darken(Vector4 value, float amount)
    {
        amount = Math.Clamp(amount, 0f, 1f);
        return new Vector4(
            value.X * (1f - amount),
            value.Y * (1f - amount),
            value.Z * (1f - amount),
            value.W);
    }

    private readonly record struct ChromePalette(
        Vector4 Text,
        Vector4 MutedText,
        Vector4 Accent,
        Vector4 AccentHot,
        Vector4 AccentSoft,
        Vector4 Surface,
        Vector4 SurfaceRaised,
        Vector4 SurfaceRaisedHot,
        Vector4 Border,
        Vector4 BorderSoft,
        Vector4 Frame,
        Vector4 FrameHovered,
        Vector4 FrameActive,
        Vector4 Button,
        Vector4 ButtonHovered,
        Vector4 ButtonActive,
        Vector4 Header,
        Vector4 HeaderHovered,
        Vector4 HeaderActive);

    private sealed class Scope : IDisposable
    {
        private readonly Action _onDispose;
        private bool _disposed;

        public Scope(Action onDispose) => _onDispose = onDispose;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _onDispose();
        }
    }
}
