using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Extensions;
using RavaSync.PlayerData.Pairs;
using RavaSync.UI.Handlers;
using RavaSync.WebAPI;
using System.Collections.Immutable;

namespace RavaSync.UI.Components;

public class DrawFolderTag : DrawFolderBase
{
    private readonly ApiController _apiController;
    private readonly SelectPairForTagUi _selectPairForTagUi;
    private readonly PairManager _pairManager;

    public DrawFolderTag(string id, IImmutableList<DrawUserPair> drawPairs, IImmutableList<Pair> allPairs,
        TagHandler tagHandler, ApiController apiController, SelectPairForTagUi selectPairForTagUi, UiSharedService uiSharedService, PairManager pairManager)
        : base(id, drawPairs, allPairs, tagHandler, uiSharedService)
    {
        _apiController = apiController;
        _selectPairForTagUi = selectPairForTagUi;
        _pairManager = pairManager;
    }

    protected override bool RenderIfEmpty => _id switch
    {
        TagHandler.CustomUnpairedTag => false,
        TagHandler.CustomOnlineTag => false,
        TagHandler.CustomOfflineTag => false,
        TagHandler.CustomVisibleTag => false,
        TagHandler.CustomAllTag => true,
        TagHandler.CustomOfflineSyncshellTag => false,
        _ => true,
    };

    protected override bool RenderMenu => _id switch
    {
        TagHandler.CustomUnpairedTag => false,
        TagHandler.CustomOnlineTag => false,
        TagHandler.CustomOfflineTag => false,
        TagHandler.CustomVisibleTag => false,
        TagHandler.CustomAllTag => false,
        TagHandler.CustomOfflineSyncshellTag => false,
        _ => true,
    };

    private bool RenderPause => _id switch
    {
        TagHandler.CustomUnpairedTag => false,
        TagHandler.CustomOnlineTag => false,
        TagHandler.CustomOfflineTag => false,
        TagHandler.CustomVisibleTag => false,
        TagHandler.CustomAllTag => false,
        TagHandler.CustomOfflineSyncshellTag => false,
        _ => _allPairs.Count > 0,
    };

    private bool RenderCount => _id switch
    {
        TagHandler.CustomUnpairedTag => false,
        TagHandler.CustomOnlineTag => false,
        TagHandler.CustomOfflineTag => false,
        TagHandler.CustomVisibleTag => false,
        TagHandler.CustomAllTag => false,
        TagHandler.CustomOfflineSyncshellTag => false,
        _ => true
    };

    protected override float DrawIcon()
    {
        var icon = _id switch
        {
            TagHandler.CustomUnpairedTag => FontAwesomeIcon.ArrowsLeftRight,
            TagHandler.CustomOnlineTag => FontAwesomeIcon.Link,
            TagHandler.CustomOfflineTag => FontAwesomeIcon.Unlink,
            TagHandler.CustomOfflineSyncshellTag => FontAwesomeIcon.Unlink,
            TagHandler.CustomVisibleTag => FontAwesomeIcon.Eye,
            TagHandler.CustomAllTag => FontAwesomeIcon.User,
            _ => FontAwesomeIcon.Folder
        };

        ImGui.AlignTextToFramePadding();
        _uiSharedService.IconText(icon);

        if (RenderCount)
        {
            var style = ImGui.GetStyle();
            var onlinePairs = OnlinePairs;
            var totalPairs = TotalPairs;

            using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, style.ItemSpacing with { X = style.ItemSpacing.X / 2f }))
            {
                ImGui.SameLine();
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted($"[{onlinePairs}]");
            }

            UiSharedService.AttachToolTip($"{onlinePairs} online{Environment.NewLine}{totalPairs} total");
        }
        ImGui.SameLine();
        return ImGui.GetCursorPosX();
    }

    protected override void DrawMenu(float menuWidth)
    {
        ImGui.TextUnformatted(_uiSharedService.L("UI.DrawFolderTag.60c393ce", "Group Menu"));
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Users, _uiSharedService.L("UI.DrawFolderTag.8eda6a72", "Select Pairs"), menuWidth, true))
        {
            _selectPairForTagUi.Open(_id);
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderTag.f08bedd9", "Select Individual Pairs for this Pair Group"));
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.DrawFolderTag.2e09993a", "Delete Pair Group"), menuWidth, true) && UiSharedService.CtrlPressed())
        {
            _tagHandler.RemoveTag(_id);
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.DrawFolderTag.f060823b", "Hold CTRL to remove this Group permanently.") + Environment.NewLine +
            "Note: this will not unpair with users in this Group.");
    }

    protected override void DrawName(float width)
    {
        ImGui.AlignTextToFramePadding();

        string name = _id switch
        {
            TagHandler.CustomUnpairedTag => _uiSharedService.L("UI.DrawFolderTag.Name.OneSidedIndividualPairs", "One-sided Individual Pairs"),
            TagHandler.CustomOnlineTag => _uiSharedService.L("UI.DrawFolderTag.Name.OnlinePausedByYou", "Online / Paused by you"),
            TagHandler.CustomOfflineTag => _uiSharedService.L("UI.DrawFolderTag.Name.OfflinePausedByOther", "Offline / Paused by other"),
            TagHandler.CustomOfflineSyncshellTag => _uiSharedService.L("UI.DrawFolderTag.Name.OfflineSyncshellUsers", "Offline Syncshell Users"),
            TagHandler.CustomVisibleTag => _uiSharedService.L("UI.DrawFolderTag.Name.Visible", "Visible"),
            TagHandler.CustomAllTag => _uiSharedService.L("UI.DrawFolderTag.Name.Users", "Users"),
            _ => _id
        };

        ImGui.TextUnformatted(name);
    }

    protected override float DrawRightSide(float currentRightSideX)
    {
        if (!RenderPause) return currentRightSideX;

        bool allArePaused = true;
        for (int i = 0; i < _allPairs.Count; i++)
        {
            if (!_allPairs[i].UserPair!.OwnPermissions.IsPaused())
            {
                allArePaused = false;
                break;
            }
        }

        var pauseButton = allArePaused ? FontAwesomeIcon.Play : FontAwesomeIcon.Pause;
        var pauseButtonX = _uiSharedService.GetIconButtonSize(pauseButton).X;

        var buttonPauseOffset = currentRightSideX - pauseButtonX;
        ImGui.SameLine(buttonPauseOffset);
        if (_uiSharedService.IconButton(pauseButton))
        {
            if (allArePaused)
            {
                ResumeAllPairs(_allPairs);
            }
            else
            {
                PauseRemainingPairs(_allPairs);
            }
        }
        if (allArePaused)
        {
            UiSharedService.AttachToolTip($"Resume pairing with all pairs in {_id}");
        }
        else
        {
            UiSharedService.AttachToolTip($"Pause pairing with all pairs in {_id}");
        }

        return currentRightSideX;
    }

    private void PauseRemainingPairs(IEnumerable<Pair> availablePairs)
    {
        var pairs = availablePairs as ICollection<Pair> ?? availablePairs.ToList();
        var dict = new Dictionary<string, RavaSync.API.Data.Enum.UserPermissions>(pairs.Count, StringComparer.Ordinal);

        foreach (var g in pairs)
        {
            var perm = g.UserPair.OwnPermissions;
            perm.SetPaused(paused: true);
            dict[g.UserData.UID] = perm;
        }

        _ = _apiController.SetBulkPermissions(new(dict, new(StringComparer.Ordinal)))
            .ConfigureAwait(false);
    }

    private void ResumeAllPairs(IEnumerable<Pair> availablePairs)
    {
        var pairs = availablePairs as ICollection<Pair> ?? availablePairs.ToList();
        var dict = new Dictionary<string, RavaSync.API.Data.Enum.UserPermissions>(pairs.Count, StringComparer.Ordinal);

        foreach (var g in pairs)
        {
            var perm = g.UserPair.OwnPermissions;
            perm.SetPaused(paused: false);
            dict[g.UserData.UID] = perm;
        }

        _ = _apiController.SetBulkPermissions(new(dict, new(StringComparer.Ordinal)))
            .ConfigureAwait(false);
    }
}