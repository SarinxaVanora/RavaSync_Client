using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Dto.Group;
using RavaSync.MareConfiguration;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.UI;
using RavaSync.WebAPI.Files.Models;
using System.Numerics;

namespace RavaSync.UI.Handlers;

public class IdDisplayHandler
{
    private readonly MareConfigService _mareConfigService;
    private readonly MareMediator _mediator;
    private readonly ServerConfigurationManager _serverManager;
    private readonly UiSharedService _uiShared;
    private readonly Dictionary<string, bool> _showIdForEntry = new(StringComparer.Ordinal);
    private string _editComment = string.Empty;
    private string _editEntry = string.Empty;
    private bool _editIsUid = false;
    private string _lastMouseOverUid = string.Empty;
    private bool _popupShown = false;
    private DateTime? _popupTime;

    public IdDisplayHandler(MareMediator mediator, ServerConfigurationManager serverManager, MareConfigService mareConfigService, UiSharedService uiShared)
    {
        _mediator = mediator;
        _serverManager = serverManager;
        _mareConfigService = mareConfigService;
        _uiShared = uiShared;
    }

    public void DrawGroupText(string id, GroupFullInfoDto group, float textPosX, Func<float> editBoxWidth)
    {
        ImGui.SameLine(textPosX);
        (bool textIsUid, string playerText) = GetGroupText(group);
        if (!string.Equals(_editEntry, group.GID, StringComparison.Ordinal))
        {
            ImGui.AlignTextToFramePadding();

            using (ImRaii.PushFont(UiBuilder.MonoFont, textIsUid))
                ImGui.TextUnformatted(playerText);

            if (ImGui.IsItemClicked(ImGuiMouseButton.Left))
            {
                var prevState = textIsUid;
                if (_showIdForEntry.ContainsKey(group.GID))
                {
                    prevState = _showIdForEntry[group.GID];
                }
                _showIdForEntry[group.GID] = !prevState;
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Right))
            {
                if (_editIsUid)
                {
                    _serverManager.SetNoteForUid(_editEntry, _editComment, save: true);
                }
                else
                {
                    _serverManager.SetNoteForGid(_editEntry, _editComment, save: true);
                }

                _editComment = _serverManager.GetNoteForGid(group.GID) ?? string.Empty;
                _editEntry = group.GID;
                _editIsUid = false;
            }
        }
        else
        {
            ImGui.AlignTextToFramePadding();

            ImGui.SetNextItemWidth(editBoxWidth.Invoke());
            if (ImGui.InputTextWithHint("", _uiShared.L("UI.IdDisplayHandler.965c6daa", "Name/Notes"), ref _editComment, 255, ImGuiInputTextFlags.EnterReturnsTrue))
            {
                _serverManager.SetNoteForGid(group.GID, _editComment, save: true);
                _editEntry = string.Empty;
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Right))
            {
                _editEntry = string.Empty;
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.IdDisplayHandler.868a0c06", "Hit ENTER to save\nRight click to cancel"));
        }
    }

    public void DrawPairText(string id, Pair pair, float textPosX, Func<float> editBoxWidth)
    {
        ImGui.SameLine(textPosX);
        (bool textIsUid, string playerText) = GetPlayerText(pair);
        if (!string.Equals(_editEntry, pair.UserData.UID, StringComparison.Ordinal))
        {

            // ---- two-line cell (name + VRAM) ----

            // DO NOT center: we'll control Y manually
            float startY = ImGui.GetCursorPosY();
            float line = ImGui.GetTextLineHeight();
            float spacing = ImGui.GetStyle().ItemSpacing.Y;
            float twoLineMin = line * 2f + spacing * 0.5f;

            // 1) first line (name / uid / note)
            ImGui.SetCursorPosY(startY);
            using (ImRaii.PushFont(UiBuilder.MonoFont, textIsUid))
                ImGui.TextUnformatted(playerText);

            // 2) second line (VRAM) — always reserve the second row so height is consistent
            ImGui.SetCursorPosX(textPosX);
            ImGui.SetCursorPosY(startY + line + spacing * 0.25f);

            // If this pair is actively downloading/loading, show progress where VRAM normally sits.
            // Once downloads finish (PairHandler clears it), this naturally falls back to VRAM.
            var dl = pair.CurrentDownloadStatus;
            if (dl != null && dl.Count > 0)
            {
                // Snapshot defensively: the underlying dictionary updates during the pipeline.
                var snapshot = new List<FileDownloadStatus>(dl.Count);
                try
                {
                    foreach (var s in dl.Values)
                        snapshot.Add(s);
                }
                catch
                {
                    snapshot.Clear();
                }

                bool anyDownloading = false;
                bool anyLoading = false;

                long totalBytes = 0;
                long transferredBytes = 0;
                int totalFiles = 0;
                int transferredFiles = 0;

                foreach (var s in snapshot)
                {
                    switch (s.DownloadStatus)
                    {
                        case DownloadStatus.Downloading:
                        case DownloadStatus.WaitingForQueue:
                        case DownloadStatus.WaitingForSlot:
                            anyDownloading = true;
                            break;

                        case DownloadStatus.Initializing:
                        case DownloadStatus.Decompressing:
                            anyLoading = true;
                            break;
                    }

                    if (s.TotalBytes > 0) totalBytes += s.TotalBytes;
                    if (s.TransferredBytes > 0) transferredBytes += s.TransferredBytes;
                    if (s.TotalFiles > 0) totalFiles += s.TotalFiles;
                    if (s.TransferredFiles > 0) transferredFiles += s.TransferredFiles;
                }

                string label = anyLoading && !anyDownloading ? "Loading" : "DL";
                string text;

                if (totalBytes > 0)
                {
                    var pct = (double)transferredBytes * 100d / (double)totalBytes;
                    text = $"{label}: {UiSharedService.ByteToString(transferredBytes, addSuffix: true)}/{UiSharedService.ByteToString(totalBytes, addSuffix: true)} ({pct:0}%)";
                }
                else if (totalFiles > 0)
                {
                    var pct = (double)transferredFiles * 100d / (double)totalFiles;
                    text = $"{label}: {transferredFiles}/{totalFiles} files ({pct:0}%)";
                }
                else
                {
                    text = anyLoading ? "Loading files..." : "Downloading...";
                }

                ImGui.PushStyleColor(ImGuiCol.Text, anyLoading ? ImGuiColors.DalamudViolet : ImGuiColors.ParsedBlue);
                ImGui.TextUnformatted(text);
                ImGui.PopStyleColor();
            }
            else if (pair.LastAppliedApproximateVRAMBytes >= 0)
            {
                ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey);

                var vramText = UiSharedService.ByteToString(pair.LastAppliedApproximateVRAMBytes, addSuffix: true);
                var trisText = pair.LastAppliedDataTris > 0
                    ? pair.LastAppliedDataTris.ToString("N0")
                    : "—";

                var line1 = $"VRAM: {vramText} | Tris: {trisText}";

                float regionW = ImGui.GetContentRegionAvail().X;
                float lineH = ImGui.GetTextLineHeight();
                var start = ImGui.GetCursorScreenPos();

                ImGui.InvisibleButton("##pair_stats_line", new Vector2(MathF.Max(1f, regionW), lineH));

                float padX = 2f * ImGuiHelpers.GlobalScale;
                float innerW = MathF.Max(1f, regionW - (padX * 2f));
                float baseW = ImGui.CalcTextSize(line1).X;

                // Keep it readable but allow compact shrink
                float scale = baseW > 0f ? MathF.Min(1f, innerW / baseW) : 1f;
                scale = MathF.Max(0.78f, scale);

                var font = ImGui.GetFont();
                float fontSize = ImGui.GetFontSize() * scale;
                float drawW = baseW * scale;

                float x = start.X + padX;
                float y = start.Y + (lineH - fontSize) * 0.5f;

                var dl1 = ImGui.GetWindowDrawList();
                var rMin = start;
                var rMax = start + new Vector2(regionW, lineH);

                dl1.PushClipRect(rMin, rMax, true);
                dl1.AddText(font, fontSize, new Vector2(x, y), ImGui.GetColorU32(ImGuiCol.Text), line1);
                dl1.PopClipRect();

                ImGui.PopStyleColor();
            }
            else
            {
                ImGui.Dummy(new(0, line));
            }


            // 3) ensure the cell height is at least two lines so the whole table row grows
            float used = ImGui.GetCursorPosY() - startY;
            if (used < twoLineMin)
            {
                ImGui.SetCursorPosX(textPosX);
                ImGui.Dummy(new(0, twoLineMin - used));
            }

            if (ImGui.IsItemHovered())
            {
                if (!string.Equals(_lastMouseOverUid, id))
                {
                    _popupTime = DateTime.UtcNow.AddSeconds(_mareConfigService.Current.ProfileDelay);
                }

                _lastMouseOverUid = id;

                if (_popupTime > DateTime.UtcNow || !_mareConfigService.Current.ProfilesShow)
                {
                    ImGui.SetTooltip(_uiShared.L("UI.IdDisplayHandler.c8d0f471", "Left click to switch between UID display and nick") + Environment.NewLine
                        + "Right click to change nick for " + pair.UserData.AliasOrUID + Environment.NewLine
                        + _uiShared.L("UI.IdDisplayHandler.2f20db7a", "Middle Mouse Button to open their profile in a separate window"));
                }
                else if (_popupTime < DateTime.UtcNow && !_popupShown)
                {
                    _popupShown = true;
                    _mediator.Publish(new ProfilePopoutToggle(pair));
                }
            }
            else
            {
                if (string.Equals(_lastMouseOverUid, id))
                {
                    _mediator.Publish(new ProfilePopoutToggle(Pair: null));
                    _lastMouseOverUid = string.Empty;
                    _popupShown = false;
                }
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Left))
            {
                var prevState = textIsUid;
                if (_showIdForEntry.ContainsKey(pair.UserData.UID))
                {
                    prevState = _showIdForEntry[pair.UserData.UID];
                }
                _showIdForEntry[pair.UserData.UID] = !prevState;
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Right))
            {
                if (_editIsUid)
                {
                    _serverManager.SetNoteForUid(_editEntry, _editComment, save: true);
                }
                else
                {
                    _serverManager.SetNoteForGid(_editEntry, _editComment, save: true);
                }

                _editComment = pair.GetNote() ?? string.Empty;
                _editEntry = pair.UserData.UID;
                _editIsUid = true;
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Middle))
            {
                _mediator.Publish(new ProfileOpenStandaloneMessage(pair));
            }
        }
        else
        {
            ImGui.AlignTextToFramePadding();

            ImGui.SetNextItemWidth(editBoxWidth.Invoke());
            if (ImGui.InputTextWithHint("##" + pair.UserData.UID, _uiShared.L("UI.IdDisplayHandler.a3b5e868", "Nick/Notes"), ref _editComment, 255, ImGuiInputTextFlags.EnterReturnsTrue))
            {
                _serverManager.SetNoteForUid(pair.UserData.UID, _editComment);
                _serverManager.SaveNotes();
                _editEntry = string.Empty;
            }

            if (ImGui.IsItemClicked(ImGuiMouseButton.Right))
            {
                _editEntry = string.Empty;
            }
            UiSharedService.AttachToolTip(_uiShared.L("UI.IdDisplayHandler.868a0c06", "Hit ENTER to save\nRight click to cancel"));
        }
    }

    public (bool isGid, string text) GetGroupText(GroupFullInfoDto group)
    {
        var textIsGid = true;
        bool showUidInsteadOfName = ShowGidInsteadOfName(group);
        string? groupText = _serverManager.GetNoteForGid(group.GID);
        if (!showUidInsteadOfName && groupText != null)
        {
            if (string.IsNullOrEmpty(groupText))
            {
                groupText = group.GroupAliasOrGID;
            }
            else
            {
                textIsGid = false;
            }
        }
        else
        {
            groupText = group.GroupAliasOrGID;
        }

        return (textIsGid, groupText!);
    }

    public (bool isUid, string text) GetPlayerText(Pair pair)
    {
        var textIsUid = true;
        bool showUidInsteadOfName = ShowUidInsteadOfName(pair);
        string? playerText = _serverManager.GetNoteForUid(pair.UserData.UID);
        if (!showUidInsteadOfName && playerText != null)
        {
            if (string.IsNullOrEmpty(playerText))
            {
                playerText = pair.UserData.AliasOrUID;
            }
            else
            {
                textIsUid = false;
            }
        }
        else
        {
            playerText = pair.UserData.AliasOrUID;
        }

        if (_mareConfigService.Current.ShowCharacterNameInsteadOfNotesForVisible && pair.IsVisible && !showUidInsteadOfName)
        {
            playerText = pair.PlayerName;
            textIsUid = false;
            if (_mareConfigService.Current.PreferNotesOverNamesForVisible)
            {
                var note = pair.GetNote();
                if (note != null)
                {
                    playerText = note;
                }
            }
        }

        return (textIsUid, playerText!);
    }

    internal void Clear()
    {
        _editEntry = string.Empty;
        _editComment = string.Empty;
    }

    internal void OpenProfile(Pair entry)
    {
        _mediator.Publish(new ProfileOpenStandaloneMessage(entry));
    }

    private bool ShowGidInsteadOfName(GroupFullInfoDto group)
    {
        _showIdForEntry.TryGetValue(group.GID, out var showidInsteadOfName);

        return showidInsteadOfName;
    }

    private bool ShowUidInsteadOfName(Pair pair)
    {
        _showIdForEntry.TryGetValue(pair.UserData.UID, out var showidInsteadOfName);

        return showidInsteadOfName;
    }
}