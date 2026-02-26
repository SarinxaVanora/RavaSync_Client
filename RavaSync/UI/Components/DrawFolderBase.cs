using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility.Raii;
using RavaSync.PlayerData.Pairs;
using RavaSync.UI.Handlers;
using System.Collections.Immutable;

namespace RavaSync.UI.Components;

public abstract class DrawFolderBase : IDrawFolder
{
    public IImmutableList<DrawUserPair> DrawPairs { get; init; }
    protected readonly string _id;
    protected readonly IImmutableList<Pair> _allPairs;
    protected readonly TagHandler _tagHandler;
    protected readonly UiSharedService _uiSharedService;
    private float _menuWidth = -1;
    public int OnlinePairs => DrawPairs.Count(u => u.Pair.IsOnline);
    public int TotalPairs => _allPairs.Count;
    private bool _wasHovered = false;

    protected DrawFolderBase(string id, IImmutableList<DrawUserPair> drawPairs,
        IImmutableList<Pair> allPairs, TagHandler tagHandler, UiSharedService uiSharedService)
    {
        _id = id;
        DrawPairs = drawPairs;
        _allPairs = allPairs;
        _tagHandler = tagHandler;
        _uiSharedService = uiSharedService;
    }

    protected abstract bool RenderIfEmpty { get; }
    protected abstract bool RenderMenu { get; }

    public void Draw()
    {
        if (!RenderIfEmpty && !DrawPairs.Any()) return;

        using var id = ImRaii.PushId("folder_" + _id);
        var color = ImRaii.PushColor(ImGuiCol.ChildBg, ImGui.GetColorU32(ImGuiCol.FrameBgHovered), _wasHovered);
        var contentWidth = UiSharedService.GetWindowContentRegionWidth();
        using (ImRaii.Child("folder__" + _id, new System.Numerics.Vector2(contentWidth - ImGui.GetCursorPosX(), ImGui.GetFrameHeight())))
        {
            // draw opener
            var icon = _tagHandler.IsTagOpen(_id) ? FontAwesomeIcon.CaretDown : FontAwesomeIcon.CaretRight;

            ImGui.AlignTextToFramePadding();

            _uiSharedService.IconText(icon);
            if (ImGui.IsItemClicked())
            {
                _tagHandler.SetTagOpen(_id, !_tagHandler.IsTagOpen(_id));
            }

            ImGui.SameLine();
            var leftSideEnd = DrawIcon();

            ImGui.SameLine();
            var rightSideStart = DrawRightSideInternal();

            // draw name
            ImGui.SameLine(leftSideEnd);
            DrawName(rightSideStart - leftSideEnd);
        }

        _wasHovered = ImGui.IsItemHovered();

        color.Dispose();

        ImGui.Separator();

        // if opened draw content
        if (_tagHandler.IsTagOpen(_id))
        {
            using var indent = ImRaii.PushIndent(_uiSharedService.GetIconSize(FontAwesomeIcon.EllipsisV).X + ImGui.GetStyle().ItemSpacing.X, false);
            if (DrawPairs.Any())
            {
                foreach (var item in DrawPairs)
                {
                    item.DrawPairedClient();
                }
            }
            else
            {
                ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderBase.2a3f7724", "No users (online)"));
            }

            ImGui.Separator();
        }
    }

    protected abstract float DrawIcon();

    protected abstract void DrawMenu(float menuWidth);

    protected abstract void DrawName(float width);

    protected abstract float DrawRightSide(float currentRightSideX);

    private float DrawRightSideInternal()
    {
        var barButtonSize = _uiSharedService.GetIconButtonSize(FontAwesomeIcon.EllipsisV);
        var spacingX = ImGui.GetStyle().ItemSpacing.X;
        var windowEndX = ImGui.GetWindowContentRegionMin().X + UiSharedService.GetWindowContentRegionWidth();

        // Flyout Menu
        var rightSideStart = windowEndX - (RenderMenu ? (barButtonSize.X + spacingX) : spacingX);

        if (RenderMenu)
        {
            ImGui.SameLine(windowEndX - barButtonSize.X);
            if (_uiSharedService.IconButton(FontAwesomeIcon.EllipsisV))
            {
                ImGui.OpenPopup(_uiSharedService.L("UI.DrawFolderBase.d24cd894", "User Flyout Menu"));
            }
            if (ImGui.BeginPopup(_uiSharedService.L("UI.DrawFolderBase.d24cd894", "User Flyout Menu")))
            {
                _menuWidth = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;
                using (ImRaii.PushId($"buttons-{_id}")) DrawMenu(_menuWidth);
                ImGui.EndPopup();
            }
            else
            {
                _menuWidth = 0;
            }
        }

        return DrawRightSide(rightSideStart);
    }
}