using Dalamud.Bindings.ImGui;
using Dalamud.Game.ClientState.Objects;
using Dalamud.Game.ClientState.Objects.Enums;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Plugin.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using RavaSync.Services.RavaCast.WorldRender;
using FFXIVClientStructs.FFXIV.Client.Graphics.Scene;

namespace RavaSync.Services.RavaCast;

public sealed class RavaCastRenderer : IHostedService, IDisposable
{
    private readonly ILogger<RavaCastRenderer> _logger;
    private readonly IUiBuilder _uiBuilder;
    private readonly IGameGui _gameGui;
    private readonly RavaCastService _ravaCast;
    private readonly RavaCastBrowserSurface _surface;
    private readonly IObjectTable _objects;
    private readonly RavaCastWorldImageRenderer _worldImageRenderer;
    private bool _ownsUiHideOverride;
    private bool _previousDisableUserUiHide;
    private bool _previousDisableAutomaticUiHide;

    private Vector2 _lastBrowserMove = new(-1f, -1f);
    private long _lastBrowserMoveTick;
    private bool _lastBrowserMouseInside;
    private int _lastBrowserHeldMask;
    private int _lastBrowserClickMask;
    private long _lastBrowserClickTick;
    private bool _browserFocused;
    private bool _keyboardFocusPending;
    private bool _swallowRightClickUntilReleased;
    private string _browserKeyboardCapture = string.Empty;
    private string _browserKeyboardCaptureLast = string.Empty;
    private bool _enterWasDown;
    private bool _tabWasDown;
    private bool _escapeWasDown;
    private bool _backspaceWasDown;
    private bool _deleteWasDown;
    private bool _leftWasDown;
    private bool _rightWasDown;
    private bool _upWasDown;
    private bool _downWasDown;
    private bool _homeWasDown;
    private bool _endWasDown;
    private Guid _urlBarCastId;
    private string _urlBarText = string.Empty;
    private string _urlBarLastSynced = string.Empty;
    private bool _urlBarDirty;
    private string _worldBrowserError = string.Empty;

    public RavaCastRenderer(ILogger<RavaCastRenderer> logger, IUiBuilder uiBuilder, IGameGui gameGui, RavaCastService ravaCast, RavaCastBrowserSurface surface, IObjectTable objects, RavaCastWorldImageRenderer worldImageRenderer)
    {
        _logger = logger;
        _uiBuilder = uiBuilder;
        _gameGui = gameGui;
        _ravaCast = ravaCast;
        _surface = surface;
        _objects = objects;
        _worldImageRenderer = worldImageRenderer;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _uiBuilder.Draw += Draw;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _uiBuilder.Draw -= Draw;
        ClearBrowserFocus();
        SetRavaCastUiHideOverride(false);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _uiBuilder.Draw -= Draw;
        ClearBrowserFocus();
        SetRavaCastUiHideOverride(false);
    }

    private void Draw()
    {
        try
        {
            var current = _ravaCast.GetCurrentSession();
            SetRavaCastUiHideOverride(current is not null);
            if (current is null)
            {
                ClearBrowserFocus();
                return;
            }

            var state = _ravaCast.GetRenderState();
            if (state is null)
            {
                ClearBrowserFocus();
                return;
            }

            // Important: RavaCast must never fall back to a flat ImGui world quad for the actual
            // screen surface.  That old path is useful as a debug preview, but it cannot participate
            // in game depth, so it will always draw over characters/walls and its apparent shape can
            // drift as the camera moves.  The screen now renders only through the depth-tested world
            // renderer; if that renderer is unavailable, we simply leave the world surface invisible
            // and expose the failure through LastError/status instead of drawing the wrong thing.
            var frame = _surface.TryGetCurrentFrame();
            var displayPlane = state.Plane;
            var drewWorldImage = false;

            if (frame is not null && frame.IsValid)
                drewWorldImage = _worldImageRenderer.TryRender(displayPlane, frame, out var worldTexture) && DrawFullscreenWorldTexture(worldTexture);
            else
                drewWorldImage = _worldImageRenderer.TryRenderPlaceholder(displayPlane, out var placeholderTexture) && DrawFullscreenWorldTexture(placeholderTexture);

            DrawWorldBrowserSurface(current, displayPlane);
            // Draw happens every frame; expose renderer problems through LastError/status, not log spam.
        }
        catch
        {
            // Draw happens every frame; avoid repeated RavaCast log pressure here.
        }
    }

    private void SetRavaCastUiHideOverride(bool enabled)
    {
        if (enabled)
        {
            if (_ownsUiHideOverride) return;

            _previousDisableUserUiHide = _uiBuilder.DisableUserUiHide;
            _previousDisableAutomaticUiHide = _uiBuilder.DisableAutomaticUiHide;
            _uiBuilder.DisableUserUiHide = true;
            _uiBuilder.DisableAutomaticUiHide = true;
            _ownsUiHideOverride = true;
            return;
        }

        if (!_ownsUiHideOverride) return;

        _uiBuilder.DisableUserUiHide = _previousDisableUserUiHide;
        _uiBuilder.DisableAutomaticUiHide = _previousDisableAutomaticUiHide;
        _ownsUiHideOverride = false;
    }

    private static bool DrawFullscreenWorldTexture(ImTextureID texture)
    {
        if (texture.Handle == 0) return false;
        var viewport = GetViewportRect();
        if (!viewport.IsValid) return false;
        ImGui.GetBackgroundDrawList().AddImage(texture, viewport.Min, viewport.Max);
        return true;
    }

    private void DrawWorldBrowserSurface(RavaCastSessionView current, RavaCastPlane plane)
    {
        if (!TryProjectPlaneToScreen(plane, out var tl, out var tr, out var br, out var bl))
        {
            ClearBrowserFocus();
            return;
        }

        var bounds = ScreenRect.FromQuad(tl, tr, br, bl);
        if (!bounds.IsValid)
        {
            ClearBrowserFocus();
            return;
        }

        var inputAllowed = current.IsOwner || current.Mode == RavaCastMode.UrlShare;
        if (!inputAllowed || _ravaCast.WorldBrowserInputSuspended)
        {
            ClearBrowserFocus();
            return;
        }

        DrawWorldBrowserOverlay(current, bounds, tl, tr, br, bl);
    }

    private void DrawWorldBrowserOverlay(RavaCastSessionView current, ScreenRect bounds, Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl)
    {
        var viewport = GetViewportRect();
        bounds = new ScreenRect(Vector2.Max(bounds.Min, viewport.Min), Vector2.Min(bounds.Max, viewport.Max));
        if (!bounds.IsValid)
        {
            ClearBrowserFocus();
            return;
        }

        var mouse = ImGui.GetMousePos();
        var mouseInBrowserQuadBeforeOverlay = mouse.X >= bounds.Min.X && mouse.X <= bounds.Max.X && mouse.Y >= bounds.Min.Y && mouse.Y <= bounds.Max.Y && TryPointToQuadUv(mouse, tl, tr, br, bl, out _);
        var anyMouseClicked = ImGui.IsMouseClicked(ImGuiMouseButton.Left) || ImGui.IsMouseClicked(ImGuiMouseButton.Right) || ImGui.IsMouseClicked(ImGuiMouseButton.Middle);
        var anyMouseDown = ImGui.IsMouseDown(ImGuiMouseButton.Left) || ImGui.IsMouseDown(ImGuiMouseButton.Right) || ImGui.IsMouseDown(ImGuiMouseButton.Middle);

        // Global focus-loss gate: if the browser is armed and the user clicks/drags anywhere that
        // is not the projected browser quad, immediately give control back to the game. Using both
        // clicked and down catches the normal game-world case where a click can be consumed before
        // it reaches the transparent browser hit-test window.
        if (_browserFocused && (anyMouseClicked || anyMouseDown) && !mouseInBrowserQuadBeforeOverlay)
        {
            ClearBrowserFocus();
            return;
        }

        // The projected screen should behave like the old Browser Preview surface: ImGui only owns
        // the transparent hit-test/focus capture. The browser surface itself owns navigation/input;
        // ImGui only owns transparent hit-testing and keyboard capture.
        var flags = ImGuiWindowFlags.NoTitleBar
            | ImGuiWindowFlags.NoResize
            | ImGuiWindowFlags.NoMove
            | ImGuiWindowFlags.NoScrollbar
            | ImGuiWindowFlags.NoScrollWithMouse
            | ImGuiWindowFlags.NoSavedSettings
            | ImGuiWindowFlags.NoCollapse
            | ImGuiWindowFlags.NoBringToFrontOnFocus;

        ImGui.SetNextWindowPos(bounds.Min);
        ImGui.SetNextWindowSize(bounds.Size);
        ImGui.SetNextWindowBgAlpha(0f);
        if (!ImGui.Begin($"##ravacast_world_browser_surface_{current.CastId:N}", flags))
        {
            ImGui.End();
            return;
        }

        // Keep the transparent world-browser window as the active ImGui window while the browser is
        // armed. This is what lets the hidden text input capture keyboard/text instead of FFXIV also
        // seeing movement/menu hotkeys. Clicking outside clears _browserFocused before this point.
        if (_browserFocused)
            ImGui.SetWindowFocus();

        ImGui.SetCursorScreenPos(bounds.Min);
        ImGui.InvisibleButton("##ravacast_world_browser_input", bounds.Size, ImGuiButtonFlags.MouseButtonLeft | ImGuiButtonFlags.MouseButtonRight | ImGuiButtonFlags.MouseButtonMiddle);
        var browserInputHovered = ImGui.IsItemHovered();
        HandleWorldBrowserMouse(bounds, tl, tr, br, bl, bounds.Min.Y, browserInputHovered);
        DrawBrowserKeyboardCapture();

        ImGui.End();
    }

    private void DrawWorldBrowserToolbar(RavaCastSessionView current, ScreenRect bounds, float toolbarHeight)
    {
        var scale = ImGuiHelpers.GlobalScale;
        ImGui.SetCursorScreenPos(bounds.Min + new Vector2(5f * scale, 5f * scale));

        if (ImGui.Button("Back##ravacast_world_back", new Vector2(58f * scale, 24f * scale)))
        {
            ClearBrowserFocus();
            _ravaCast.GoBackCurrentBrowser();
        }

        ImGui.SameLine();
        if (ImGui.Button("Reload##ravacast_world_reload", new Vector2(70f * scale, 24f * scale)))
        {
            ClearBrowserFocus();
            _ravaCast.ReloadCurrentBrowser();
        }

        ImGui.SameLine();
        var goWidth = 48f * scale;
        var gap = 8f * scale;
        var urlWidth = Math.Max(80f * scale, bounds.Width - (58f + 70f + 48f) * scale - gap * 8f);
        ImGui.SetNextItemWidth(urlWidth);
        var beforeUrl = _urlBarText;
        var goPressed = ImGui.InputTextWithHint("##ravacast_world_url", "URL or search", ref _urlBarText, 1024, ImGuiInputTextFlags.EnterReturnsTrue);
        if (ImGui.IsItemActivated())
            ClearBrowserFocus();
        if (!string.Equals(beforeUrl, _urlBarText, StringComparison.Ordinal))
            _urlBarDirty = true;

        ImGui.SameLine();
        if (ImGui.Button("Go##ravacast_world_go", new Vector2(goWidth, 24f * scale)) || goPressed)
        {
            ClearBrowserFocus();
            NavigateWorldBrowser();
        }

        if (!string.IsNullOrWhiteSpace(_worldBrowserError))
        {
            var pos = new Vector2(bounds.Min.X + 6f * scale, bounds.Min.Y + toolbarHeight + 5f * scale);
            var size = ImGui.CalcTextSize(_worldBrowserError);
            var dl = ImGui.GetWindowDrawList();
            dl.AddRectFilled(pos - new Vector2(6f * scale, 4f * scale), pos + size + new Vector2(6f * scale, 4f * scale), 0xDD16061F, 5f * scale);
            dl.AddText(pos, ImGui.GetColorU32(ImGuiColors.DalamudRed), _worldBrowserError);
        }
    }

    private void NavigateWorldBrowser()
    {
        if (_ravaCast.NavigateCurrentBrowserFromText(_urlBarText, out var error))
        {
            _urlBarText = RavaCastService.NormaliseBrowserNavigationText(_urlBarText);
            _urlBarLastSynced = _urlBarText;
            _urlBarDirty = false;
            _worldBrowserError = string.Empty;
            return;
        }

        _worldBrowserError = error;
    }

    private void EnsureUrlBarState(RavaCastSessionView current)
    {
        var browserUrl = !string.IsNullOrWhiteSpace(_surface.CurrentUrl) ? _surface.CurrentUrl : current.Url;
        if (_urlBarCastId != current.CastId)
        {
            _urlBarCastId = current.CastId;
            _urlBarText = browserUrl;
            _urlBarLastSynced = browserUrl;
            _urlBarDirty = false;
            _worldBrowserError = string.Empty;
            return;
        }

        if (!_urlBarDirty && !string.Equals(_urlBarLastSynced, browserUrl, StringComparison.Ordinal))
        {
            _urlBarText = browserUrl;
            _urlBarLastSynced = browserUrl;
        }
    }

    private void HandleWorldBrowserMouse(ScreenRect bounds, Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl, float inputTop, bool browserInputHovered)
    {
        var io = ImGui.GetIO();
        var mouse = ImGui.GetMousePos();
        var mouseInInput = mouse.X >= bounds.Min.X && mouse.X <= bounds.Max.X && mouse.Y >= inputTop && mouse.Y <= bounds.Max.Y;
        var normalised = _lastBrowserMove.X < 0f ? new Vector2(0.5f, 0.5f) : _lastBrowserMove;
        var mouseInBrowserQuad = browserInputHovered && mouseInInput && TryPointToQuadUv(mouse, tl, tr, br, bl, out normalised);
        if (mouseInBrowserQuad)
            normalised.X = 1f - normalised.X;

        // Leaving the browser surface, or clicking another ImGui/window element drawn over it, means
        // the next interaction must deliberately focus it again. This keeps the screen widgets usable
        // after the browser has been armed with the right-click focus gate.
        if (!mouseInBrowserQuad)
        {
            if (_browserFocused && (ImGui.IsMouseClicked(ImGuiMouseButton.Left) || ImGui.IsMouseClicked(ImGuiMouseButton.Right) || ImGui.IsMouseClicked(ImGuiMouseButton.Middle)))
                ClearBrowserFocus();

            if (_lastBrowserMouseInside)
                _surface.SendBrowserMouse(Math.Clamp(_lastBrowserMove.X, 0f, 1f), Math.Clamp(_lastBrowserMove.Y, 0f, 1f), 0, 0, _lastBrowserHeldMask, 0, 0f, 0f, leaving: true, shift: io.KeyShift, ctrl: io.KeyCtrl, alt: io.KeyAlt);
            _lastBrowserMouseInside = false;
            return;
        }

        normalised = new Vector2(Math.Clamp(normalised.X, 0f, 1f), Math.Clamp(normalised.Y, 0f, 1f));
        _lastBrowserMouseInside = true;
        _worldBrowserError = string.Empty;

        // Require an explicit right-click to arm browser control.  The arming right-click is swallowed;
        // once focused, right-click behaves normally and reaches the browser/context menu.
        if (!_browserFocused)
        {
            _lastBrowserMove = normalised;
            if (!ImGui.IsMouseClicked(ImGuiMouseButton.Right))
                return;

            _browserKeyboardCapture = string.Empty;
            _browserKeyboardCaptureLast = string.Empty;
            _surface.SendBrowserFocus(true);
            _surface.SendBrowserMouse(normalised.X, normalised.Y, 0, 0, 0, 0, 0f, 0f, leaving: false, shift: io.KeyShift, ctrl: io.KeyCtrl, alt: io.KeyAlt);
            _browserFocused = true;
            _keyboardFocusPending = true;
            _swallowRightClickUntilReleased = true;
            _lastBrowserHeldMask = 0;
            _lastBrowserClickMask = 0;
            _lastBrowserClickTick = 0;
            return;
        }

        if (ImGui.IsKeyPressed(ImGuiKey.Escape))
        {
            ClearBrowserFocus();
            return;
        }

        if (_swallowRightClickUntilReleased)
        {
            _lastBrowserMove = normalised;
            _lastBrowserHeldMask &= ~2;
            if (ImGui.IsMouseDown(ImGuiMouseButton.Right) || ImGui.IsMouseReleased(ImGuiMouseButton.Right))
                return;
            _swallowRightClickUntilReleased = false;
        }

        // After the initial right-click has armed the browser, every real click inside the browser
        // surface should restore the hidden keyboard capture. Without this, clicking a WebView text
        // box can focus the browser visually while ImGui stops collecting typed characters.
        if (ImGui.IsMouseClicked(ImGuiMouseButton.Left) || ImGui.IsMouseClicked(ImGuiMouseButton.Right) || ImGui.IsMouseClicked(ImGuiMouseButton.Middle))
            _keyboardFocusPending = true;

        var heldMask = BuildMouseMask(ImGui.IsMouseDown(ImGuiMouseButton.Left), ImGui.IsMouseDown(ImGuiMouseButton.Right), ImGui.IsMouseDown(ImGuiMouseButton.Middle));
        var downMask = BuildMouseMask(ImGui.IsMouseClicked(ImGuiMouseButton.Left), ImGui.IsMouseClicked(ImGuiMouseButton.Right), ImGui.IsMouseClicked(ImGuiMouseButton.Middle));
        var upMask = BuildMouseMask(ImGui.IsMouseReleased(ImGuiMouseButton.Left), ImGui.IsMouseReleased(ImGuiMouseButton.Right), ImGui.IsMouseReleased(ImGuiMouseButton.Middle));
        var wheelY = io.MouseWheel;
        var wheelX = io.MouseWheelH;

        var now = Environment.TickCount64;
        var doubleMask = 0;
        if (downMask != 0 && _lastBrowserClickMask == downMask && now - _lastBrowserClickTick <= 450)
            doubleMask = downMask;
        if (downMask != 0)
        {
            _lastBrowserClickMask = downMask;
            _lastBrowserClickTick = now;
        }

        var moved = Vector2.DistanceSquared(normalised, _lastBrowserMove) > 0.0000005f && now - _lastBrowserMoveTick >= 4;
        var buttonChanged = downMask != 0 || upMask != 0 || heldMask != _lastBrowserHeldMask;
        var wheeled = Math.Abs(wheelY) > 0.01f || Math.Abs(wheelX) > 0.01f;

        if (moved || buttonChanged || wheeled)
        {
            _lastBrowserMoveTick = now;
            _lastBrowserMove = normalised;
            _lastBrowserHeldMask = heldMask;
            _surface.SendBrowserMouse(normalised.X, normalised.Y, downMask, upMask, heldMask, doubleMask, wheelX, wheelY, leaving: false, shift: io.KeyShift, ctrl: io.KeyCtrl, alt: io.KeyAlt);
        }
    }

    private void DrawBrowserKeyboardCapture()
    {
        if (!_browserFocused)
        {
            _browserKeyboardCapture = string.Empty;
            _browserKeyboardCaptureLast = string.Empty;
            ResetBrowserKeyState();
            return;
        }

        var io = ImGui.GetIO();
        var hiddenInputCursor = ImGui.GetCursorPos();
        ImGui.PushStyleVar(ImGuiStyleVar.Alpha, 0f);

        // Keep the capture field inside the transparent browser overlay window. The old preview lived
        // inside a normal ImGui window; this world overlay is tightly clipped to the screen bounds, so
        // parking the input far off-window can prevent it from becoming the active keyboard target.
        ImGui.SetCursorPos(new Vector2(1f, 1f));
        ImGui.SetNextItemWidth(1f);
        if (_keyboardFocusPending)
        {
            ImGui.SetKeyboardFocusHere();
            _keyboardFocusPending = false;
        }
        var captureChanged = ImGui.InputText("##ravacast_world_browser_keyboard_capture", ref _browserKeyboardCapture, 2048, ImGuiInputTextFlags.Password | ImGuiInputTextFlags.NoUndoRedo);
        var capturedText = _browserKeyboardCapture;
        ImGui.PopStyleVar();
        ImGui.SetCursorPos(hiddenInputCursor);

        if (captureChanged)
        {
            var textToSend = GetNewBrowserCaptureText(_browserKeyboardCaptureLast, capturedText);
            _browserKeyboardCaptureLast = capturedText;
            if (!string.IsNullOrEmpty(textToSend))
                _surface.SendTextInput(textToSend);

            if (_browserKeyboardCapture.Length > 1536)
            {
                _browserKeyboardCapture = string.Empty;
                _browserKeyboardCaptureLast = string.Empty;
            }
        }

        var shift = io.KeyShift;
        var ctrl = io.KeyCtrl;
        var alt = io.KeyAlt;
        SendBrowserKeyOnChange(ImGuiKey.Enter, ref _enterWasDown, 13, "\r", shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Tab, ref _tabWasDown, 9, "\t", shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Escape, ref _escapeWasDown, 27, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Backspace, ref _backspaceWasDown, 8, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Delete, ref _deleteWasDown, 46, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.LeftArrow, ref _leftWasDown, 37, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.RightArrow, ref _rightWasDown, 39, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.UpArrow, ref _upWasDown, 38, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.DownArrow, ref _downWasDown, 40, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.Home, ref _homeWasDown, 36, null, shift, ctrl, alt);
        SendBrowserKeyOnChange(ImGuiKey.End, ref _endWasDown, 35, null, shift, ctrl, alt);
    }

    private void ClearBrowserFocus()
    {
        var heldMask = _lastBrowserHeldMask;
        if (_lastBrowserMouseInside || heldMask != 0)
        {
            var x = Math.Clamp(_lastBrowserMove.X < 0f ? 0.5f : _lastBrowserMove.X, 0f, 1f);
            var y = Math.Clamp(_lastBrowserMove.Y < 0f ? 0.5f : _lastBrowserMove.Y, 0f, 1f);
            _surface.SendBrowserMouse(x, y, 0, 0, heldMask, 0, 0f, 0f, leaving: true, shift: false, ctrl: false, alt: false);
        }

        if (_browserFocused)
            _surface.SendBrowserFocus(false);
        _browserFocused = false;
        _swallowRightClickUntilReleased = false;
        _browserKeyboardCapture = string.Empty;
        _browserKeyboardCaptureLast = string.Empty;
        _lastBrowserHeldMask = 0;
        _lastBrowserMouseInside = false;
        ResetBrowserKeyState();
    }

    private static string GetNewBrowserCaptureText(string previous, string current)
    {
        if (string.IsNullOrEmpty(current)) return string.Empty;
        if (string.IsNullOrEmpty(previous)) return current;
        return current.StartsWith(previous, StringComparison.Ordinal) ? current[previous.Length..] : string.Empty;
    }

    private void SendBrowserKeyOnChange(ImGuiKey key, ref bool wasDown, int virtualKey, string? text, bool shift, bool ctrl, bool alt)
    {
        var down = ImGui.IsKeyDown(key);
        if (down != wasDown)
            _surface.SendBrowserKey(virtualKey, down, down ? text : null, shift, ctrl, alt);
        wasDown = down;
    }

    private void ResetBrowserKeyState()
    {
        _keyboardFocusPending = false;
        _enterWasDown = false;
        _tabWasDown = false;
        _escapeWasDown = false;
        _backspaceWasDown = false;
        _deleteWasDown = false;
        _leftWasDown = false;
        _rightWasDown = false;
        _upWasDown = false;
        _downWasDown = false;
        _homeWasDown = false;
        _endWasDown = false;
    }

    private static int BuildMouseMask(bool left, bool right, bool middle)
    {
        var mask = 0;
        if (left) mask |= 1;
        if (right) mask |= 2;
        if (middle) mask |= 4;
        return mask;
    }

    private bool TryProjectPlaneToScreen(RavaCastPlane plane, out Vector2 tl, out Vector2 tr, out Vector2 br, out Vector2 bl)
    {
        tl = tr = br = bl = Vector2.Zero;
        return Project(plane.TopLeft, out tl)
            && Project(plane.TopRight, out tr)
            && Project(plane.BottomRight, out br)
            && Project(plane.BottomLeft, out bl);
    }

    private static bool TryPointToQuadUv(Vector2 p, Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl, out Vector2 uv)
    {
        if (TryPointToTriangleUv(p, tl, tr, br, new Vector2(0f, 0f), new Vector2(1f, 0f), new Vector2(1f, 1f), out uv))
            return true;
        if (TryPointToTriangleUv(p, tl, br, bl, new Vector2(0f, 0f), new Vector2(1f, 1f), new Vector2(0f, 1f), out uv))
            return true;
        uv = Vector2.Zero;
        return false;
    }

    private static bool TryPointToTriangleUv(Vector2 p, Vector2 a, Vector2 b, Vector2 c, Vector2 auv, Vector2 buv, Vector2 cuv, out Vector2 uv)
    {
        var v0 = b - a;
        var v1 = c - a;
        var v2 = p - a;
        var den = v0.X * v1.Y - v1.X * v0.Y;
        if (Math.Abs(den) < 0.0001f)
        {
            uv = Vector2.Zero;
            return false;
        }

        var inv = 1f / den;
        var u = (v2.X * v1.Y - v1.X * v2.Y) * inv;
        var v = (v0.X * v2.Y - v2.X * v0.Y) * inv;
        var w = 1f - u - v;
        const float eps = -0.002f;
        if (u < eps || v < eps || w < eps)
        {
            uv = Vector2.Zero;
            return false;
        }

        uv = auv * w + buv * u + cuv * v;
        return true;
    }

    private readonly record struct ScreenRect(Vector2 Min, Vector2 Max)
    {
        public bool IsValid => Max.X > Min.X + 8f && Max.Y > Min.Y + 8f;
        public float Width => Max.X - Min.X;
        public float Height => Max.Y - Min.Y;
        public Vector2 Size => new(Width, Height);
        public bool Contains(Vector2 p, float pad = 0f)
            => p.X >= Min.X - pad && p.X <= Max.X + pad && p.Y >= Min.Y - pad && p.Y <= Max.Y + pad;

        public bool Intersects(ScreenRect other, float pad = 0f)
            => Min.X <= other.Max.X + pad && Max.X >= other.Min.X - pad && Min.Y <= other.Max.Y + pad && Max.Y >= other.Min.Y - pad;

        public static ScreenRect FromQuad(Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl)
        {
            var min = new Vector2(MathF.Min(MathF.Min(tl.X, tr.X), MathF.Min(br.X, bl.X)), MathF.Min(MathF.Min(tl.Y, tr.Y), MathF.Min(br.Y, bl.Y)));
            var max = new Vector2(MathF.Max(MathF.Max(tl.X, tr.X), MathF.Max(br.X, bl.X)), MathF.Max(MathF.Max(tl.Y, tr.Y), MathF.Max(br.Y, bl.Y)));
            return new ScreenRect(min, max);
        }
    }

    private static ScreenRect GetViewportRect()
    {
        var io = ImGui.GetIO();
        return new ScreenRect(Vector2.Zero, io.DisplaySize);
    }

    private static RavaCastPlane FitPlaneToAspect(RavaCastPlane plane, float textureAspect)
    {
        textureAspect = Math.Clamp(textureAspect, 0.25f, 4.0f);

        var centre = (plane.TopLeft + plane.TopRight + plane.BottomRight + plane.BottomLeft) / 4f;
        var rightVec = plane.TopRight - plane.TopLeft;
        var downVec = plane.BottomLeft - plane.TopLeft;
        var width = rightVec.Length();
        var height = downVec.Length();
        if (width <= 0.001f || height <= 0.001f)
            return plane;

        var right = Vector3.Normalize(rightVec);
        var down = Vector3.Normalize(downVec);
        var planeAspect = width / height;

        var fittedWidth = width;
        var fittedHeight = height;
        if (planeAspect > textureAspect)
            fittedWidth = height * textureAspect;
        else
            fittedHeight = width / textureAspect;

        var halfRight = right * (fittedWidth / 2f);
        var halfDown = down * (fittedHeight / 2f);
        return new RavaCastPlane(plane.TerritoryId, plane.ScreenName,
            centre - halfRight - halfDown,
            centre + halfRight - halfDown,
            centre + halfRight + halfDown,
            centre - halfRight + halfDown);
    }

    private static void DrawSubdividedImageQuad(ImDrawListPtr dl, ImTextureID textureId, Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl, ScreenRect bounds, IReadOnlyList<ScreenRect> occlusionRects)
    {
        // ImGui's AddImageQuad uses affine interpolation per quad. A single large quad shears badly as the camera moves.
        // Splitting it into a small grid keeps the mapped screen stable without touching the game render/depth path.
        var longest = MathF.Max(bounds.Width, bounds.Height);
        var cols = Math.Clamp((int)MathF.Ceiling(longest / 140f), 4, 14);
        var rows = Math.Clamp((int)MathF.Ceiling(cols / Math.Max(0.75f, bounds.Width / Math.Max(1f, bounds.Height))), 3, 10);

        for (var y = 0; y < rows; y++)
        {
            var v0 = (float)y / rows;
            var v1 = (float)(y + 1) / rows;
            for (var x = 0; x < cols; x++)
            {
                var u0 = (float)x / cols;
                var u1 = (float)(x + 1) / cols;

                var p00 = QuadPoint(tl, tr, br, bl, u0, v0);
                var p10 = QuadPoint(tl, tr, br, bl, u1, v0);
                var p11 = QuadPoint(tl, tr, br, bl, u1, v1);
                var p01 = QuadPoint(tl, tr, br, bl, u0, v1);
                var cell = ScreenRect.FromQuad(p00, p10, p11, p01);
                if (IntersectsAny(cell, occlusionRects))
                    continue;

                dl.AddImageQuad(textureId, p00, p10, p11, p01, new Vector2(u0, v0), new Vector2(u1, v0), new Vector2(u1, v1), new Vector2(u0, v1));
            }
        }
    }

    private static Vector2 QuadPoint(Vector2 tl, Vector2 tr, Vector2 br, Vector2 bl, float u, float v)
    {
        var top = Vector2.Lerp(tl, tr, u);
        var bottom = Vector2.Lerp(bl, br, u);
        return Vector2.Lerp(top, bottom, v);
    }

    private static void DrawCentredText(ImDrawListPtr dl, Vector2 centre, string text)
    {
        var lines = text.Split('\n');
        var y = centre.Y - (lines.Length * 9f * ImGuiHelpers.GlobalScale);
        foreach (var l in lines)
        {
            var size = ImGui.CalcTextSize(l);
            dl.AddText(new Vector2(centre.X - size.X / 2f, y), ImGui.GetColorU32(ImGuiColors.DalamudWhite), l);
            y += 18f * ImGuiHelpers.GlobalScale;
        }
    }

    private static bool IntersectsAny(ScreenRect cell, IReadOnlyList<ScreenRect> occlusionRects)
    {
        for (var i = 0; i < occlusionRects.Count; i++)
        {
            if (cell.Intersects(occlusionRects[i], 2f * ImGuiHelpers.GlobalScale))
                return true;
        }

        return false;
    }

    private List<ScreenRect> GetCharacterOcclusionRects(RavaCastPlane plane, ScreenRect screenRect)
    {
        var result = new List<ScreenRect>();
        if (!TryGetCameraPosition(out var cameraPos)) return result;

        var planeCentre = (plane.TopLeft + plane.TopRight + plane.BottomRight + plane.BottomLeft) / 4f;
        var screenDistance = Vector3.Distance(cameraPos, planeCentre);
        if (screenDistance <= 0.1f) return result;

        var padX = 6f * ImGuiHelpers.GlobalScale;
        var padY = 2f * ImGuiHelpers.GlobalScale;
        foreach (var obj in _objects)
        {
            if (obj is null || obj.ObjectKind != ObjectKind.Pc) continue;

            var pos = obj.Position;
            if (Vector3.DistanceSquared(pos, planeCentre) > 144f) continue;

            var actorDistance = Vector3.Distance(cameraPos, pos);
            if (actorDistance >= screenDistance - 0.20f) continue;

            var points = new[]
            {
                pos + new Vector3(0f, 0.10f, 0f),
                pos + new Vector3(0f, 0.85f, 0f),
                pos + new Vector3(0f, 1.55f, 0f),
                pos + new Vector3(0f, 1.95f, 0f),
            };

            var any = false;
            var min = new Vector2(float.MaxValue, float.MaxValue);
            var max = new Vector2(float.MinValue, float.MinValue);
            foreach (var p in points)
            {
                if (!Project(p, out var screen)) continue;
                any = true;
                min = Vector2.Min(min, screen);
                max = Vector2.Max(max, screen);
            }

            if (!any) continue;

            var height = MathF.Max(42f * ImGuiHelpers.GlobalScale, max.Y - min.Y);
            height = MathF.Min(height, screenRect.Height * 0.92f);
            var width = Math.Clamp(height * 0.22f, 20f * ImGuiHelpers.GlobalScale, MathF.Min(64f * ImGuiHelpers.GlobalScale, screenRect.Width * 0.18f));
            var cx = (min.X + max.X) / 2f;
            var rect = new ScreenRect(new Vector2(cx - width / 2f - padX, min.Y - padY), new Vector2(cx + width / 2f + padX, min.Y + height + padY));
            if (!rect.Intersects(screenRect))
                continue;

            var clipped = new ScreenRect(Vector2.Max(rect.Min, screenRect.Min), Vector2.Min(rect.Max, screenRect.Max));
            if (clipped.IsValid)
                result.Add(clipped);
        }

        return result;
    }

    private static unsafe bool TryGetCameraPosition(out Vector3 cameraPos)
    {
        cameraPos = Vector3.Zero;
        try
        {
            var camera = CameraManager.Instance()->CurrentCamera;
            if (camera is null) return false;
            cameraPos = new Vector3(camera->Position.X, camera->Position.Y, camera->Position.Z);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private bool Project(Vector3 world, out Vector2 screen)
        => _gameGui.WorldToScreen(world, out screen);
}
