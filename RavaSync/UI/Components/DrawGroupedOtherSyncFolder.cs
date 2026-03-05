using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.UI.Handlers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace RavaSync.UI.Components;

/// <summary>
/// Groups external-sync folders (Lightless/Snowcloak/Other) under one parent.
/// </summary>
public sealed class DrawGroupedOtherSyncFolder : IDrawFolder
{
    private readonly List<IDrawFolder> _children;
    private readonly TagHandler _tagHandler;
    private readonly string _id;
    private readonly string _displayName;

    public DrawGroupedOtherSyncFolder(string id, string displayName, IEnumerable<IDrawFolder> children, TagHandler tagHandler)
    {
        _id = id;
        _displayName = displayName;
        _children = children.Where(c => c != null).ToList();
        _tagHandler = tagHandler;
    }

    public int TotalPairs => _children.Sum(c => c.TotalPairs);

    public int OnlinePairs
    {
        get
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var online = 0;

            foreach (var folder in _children)
            {
                foreach (var entry in folder.DrawPairs)
                {
                    var uid = entry.Pair.UserData.UID;
                    if (!seen.Add(uid)) continue;

                    if (entry.Pair.IsOnline)
                        online++;
                }
            }

            return online;
        }
    }

    public IImmutableList<DrawUserPair> DrawPairs
        => throw new NotSupportedException("DrawGroupedOtherSyncFolder is a container; enumerate children instead.");

    public void Draw()
    {
        using var _ = ImRaii.PushId($"other-sync-group-{_id}");

        var open = _tagHandler.IsTagOpen(_id);

        using (ImRaii.PushStyle(ImGuiStyleVar.FramePadding,
                   new System.Numerics.Vector2(ImGui.GetStyle().FramePadding.X, 2f * ImGuiHelpers.GlobalScale)))
        {
            if (ImGui.CollapsingHeader(_displayName, open ? ImGuiTreeNodeFlags.DefaultOpen : ImGuiTreeNodeFlags.None))
            {
                if (!open) _tagHandler.SetTagOpen(_id, true);

                using var indent = ImRaii.PushIndent(14f * ImGuiHelpers.GlobalScale);
                foreach (var c in _children)
                    c.Draw();
            }
            else
            {
                if (open) _tagHandler.SetTagOpen(_id, false);
            }
        }

        ImGui.Separator();
    }
}