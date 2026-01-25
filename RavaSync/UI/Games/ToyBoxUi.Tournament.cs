using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using System;
using System.Linq;
using System.Numerics;
using RavaSync.Services;

namespace RavaSync.UI;

public sealed partial class ToyBoxUi
{
    private int _tournamentHostMaxHp = 100;

    private void DrawTournament()
    {
        if (_activeGameId == Guid.Empty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No active tournament session selected.");
            return;
        }

        if (!_games.TryGetClientTournament(_activeGameId, out var view))
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state…");
            return;
        }

        var pub = view.Public;
        var priv = view.Private;

        if (pub is null && priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for host…");
            return;
        }

        var stage = pub?.Stage ?? TournamentStage.Lobby;

        bool isHostLobby = pub != null && string.Equals(view.MySessionId, pub.HostSessionId, StringComparison.Ordinal);

        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale }))
        {
            ImGui.TextUnformatted("Combat Tournament");
            ImGui.SameLine();
            DrawPill("Stage: " + stage, stage == TournamentStage.InProgress ? ImGuiColors.ParsedOrange : ImGuiColors.DalamudGrey);

            ImGui.SameLine();

            var label = isHostLobby ? "Close Lobby" : "Leave";

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignOutAlt, label, 120 * ImGuiHelpers.GlobalScale, true))
            {
                if (isHostLobby) _games.CloseTournamentLobby(_activeGameId);
                else _games.LeaveTournament(_activeGameId);

                _activeGameId = Guid.Empty;
                return;
            }
        }

        ImGui.Separator();

        if (!isHostLobby)
        {
            using (ImRaii.Child("tour_left_full", new Vector2(0, 0), false))
            {
                DrawTournamentBracket(pub);
            }

            return;
        }

        using var layout = ImRaii.Table("tournament_layout", 2, ImGuiTableFlags.SizingStretchProp);
        if (!layout) return;

        ImGui.TableSetupColumn("Left", ImGuiTableColumnFlags.WidthStretch, 0.72f);
        ImGui.TableSetupColumn("Right", ImGuiTableColumnFlags.WidthStretch, 0.28f);
        ImGui.TableNextRow();

        ImGui.TableNextColumn();
        using (ImRaii.Child("tour_left", new Vector2(0, 0), false))
        {
            DrawTournamentBracket(pub);
        }

        ImGui.TableNextColumn();
        using (ImRaii.Child("tour_right", new Vector2(0, 0), false))
        {
            DrawTournamentRightPanel(view, pub, priv);
        }
    }

    private void DrawTournamentBracket(TournamentStatePublic? pub)
    {
        if (pub == null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for bracket…");
            return;
        }

        if (pub.MatchInProgress && !string.IsNullOrEmpty(pub.ActiveFighterASessionId) && !string.IsNullOrEmpty(pub.ActiveFighterBSessionId))
        {
            ImGui.TextUnformatted("Active Match");
            ImGui.Separator();
            ImGui.TextUnformatted($"{pub.ActiveFighterAName} vs {pub.ActiveFighterBName}");
            ImGui.TextColored(ImGuiColors.DalamudGrey, $"HP: {pub.ActiveFighterAHp}/{pub.MaxHp}  -  {pub.ActiveFighterBHp}/{pub.MaxHp}");
            ImGuiHelpers.ScaledDummy(10);
        }

        ImGui.TextUnformatted("Bracket");
        ImGui.Separator();

        using var canvas = ImRaii.Child("tournament_bracket_tree_canvas",
            new Vector2(0, 0),
            false,
            ImGuiWindowFlags.HorizontalScrollbar);

        if (!canvas) return;

        DrawTournamentBracketTree(pub);
    }


    private void DrawTournamentBracketTree(TournamentStatePublic pub)
    {
        var rounds = pub.Rounds;
        if (rounds == null || rounds.Count == 0 || rounds[0].Matches.Count == 0)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No bracket yet.");
            return;
        }

        int roundCount = rounds.Count;
        int leafSlots = rounds[0].Matches.Count * 2;
        if (leafSlots < 2) leafSlots = 2;

        float s = ImGuiHelpers.GlobalScale;

        float boxW = 170f * s;
        float boxH = 38f * s;

        float leafGapX = 40f * s;
        float levelGapY = 72f * s;

        float marginX = 18f * s;
        float marginY = 18f * s;

        float totalW = marginX * 2 + leafSlots * boxW + (leafSlots - 1) * leafGapX;
        float totalH = marginY * 2 + (roundCount + 1) * levelGapY + boxH;

        var origin = ImGui.GetCursorScreenPos();
        var avail = ImGui.GetContentRegionAvail();
        var canvasSize = new Vector2(MathF.Max(avail.X, totalW), MathF.Max(avail.Y, totalH));

        ImGui.InvisibleButton("##bracket_tree_bg", canvasSize);

        float offsetX = 0f;
        if (avail.X > totalW)
            offsetX = (avail.X - totalW) * 0.5f;

        var drawOrigin = origin + new Vector2(offsetX, 0f);

        var dl = ImGui.GetWindowDrawList();
        dl.PushClipRect(origin, origin + canvasSize, true);

        float LeafX(int leafIndex)
        {
            return drawOrigin.X + marginX + leafIndex * (boxW + leafGapX) + boxW * 0.5f;
        }

        float LevelY(int level)
        {
            return drawOrigin.Y + marginY + (roundCount - level) * levelGapY;
        }

        string SlotName(int level, int slotIndex)
        {
            if (level == 0)
            {
                int m = slotIndex / 2;
                bool a = (slotIndex % 2) == 0;

                if (m < 0 || m >= rounds[0].Matches.Count) return "(TBD)";

                var match = rounds[0].Matches[m];

                var n = a ? match.FighterAName : match.FighterBName;
                if (string.IsNullOrWhiteSpace(n))
                    return a ? "(TBD)" : "(BYE)";

                return n;
            }

            int prevRound = level - 1;
            if (prevRound < 0 || prevRound >= rounds.Count) return "(TBD)";

            if (slotIndex < 0 || slotIndex >= rounds[prevRound].Matches.Count) return "(TBD)";

            var pm = rounds[prevRound].Matches[slotIndex];
            return string.IsNullOrWhiteSpace(pm.WinnerName) ? "(TBD)" : pm.WinnerName;
        }

        uint colLine = ImGui.GetColorU32(new Vector4(1f, 1f, 1f, 0.35f));
        uint colFill = ImGui.GetColorU32(new Vector4(0f, 0f, 0f, 0.35f));
        uint colBorder = ImGui.GetColorU32(new Vector4(1f, 1f, 1f, 0.30f));
        uint colActive = ImGui.GetColorU32(new Vector4(0.75f, 0.25f, 1f, 0.90f));
        uint colWinner = ImGui.GetColorU32(new Vector4(0.20f, 0.65f, 0.30f, 0.35f));

        // Precompute rects per level/slot so we can draw connectors cleanly.
        var rects = new System.Collections.Generic.List<System.Collections.Generic.List<(Vector2 Min, Vector2 Max, bool Active, bool HasWinner)>>();

        for (int level = 0; level <= roundCount; level++)
        {
            int slotCount = leafSlots >> level;
            if (slotCount < 1) slotCount = 1;

            var list = new System.Collections.Generic.List<(Vector2, Vector2, bool, bool)>(slotCount);

            for (int i = 0; i < slotCount; i++)
            {
                int span = 1 << level;
                int leafStart = i * span;
                int leafEnd = Math.Min(leafSlots - 1, leafStart + span - 1);

                float x = (LeafX(leafStart) + LeafX(leafEnd)) * 0.5f;
                float y = LevelY(level);

                var min = new Vector2(x - boxW * 0.5f, y);
                var max = min + new Vector2(boxW, boxH);

                bool isActive = pub.MatchInProgress
                    && level == pub.CurrentRoundIndex
                    && (i == pub.CurrentMatchIndex * 2 || i == pub.CurrentMatchIndex * 2 + 1);

                bool hasWinner = false;
                if (level > 0)
                {
                    int prevRound = level - 1;
                    if (prevRound >= 0 && prevRound < rounds.Count && i < rounds[prevRound].Matches.Count)
                        hasWinner = !string.IsNullOrEmpty(rounds[prevRound].Matches[i].WinnerSessionId);
                }

                list.Add((min, max, isActive, hasWinner));
            }

            rects.Add(list);
        }

        // Connectors: children (level) -> parent (level+1)
        for (int level = 0; level < roundCount; level++)
        {
            var kids = rects[level];
            var parents = rects[level + 1];

            for (int p = 0; p < parents.Count; p++)
            {
                int c0 = p * 2;
                int c1 = p * 2 + 1;
                if (c0 >= kids.Count) break;

                var parent = parents[p];
                var parentBottom = new Vector2((parent.Min.X + parent.Max.X) * 0.5f, parent.Max.Y);

                var k0 = kids[c0];
                var k0Top = new Vector2((k0.Min.X + k0.Max.X) * 0.5f, k0.Min.Y);

                Vector2 k1Top = k0Top;
                bool hasSecond = c1 < kids.Count;

                if (hasSecond)
                {
                    var k1 = kids[c1];
                    k1Top = new Vector2((k1.Min.X + k1.Max.X) * 0.5f, k1.Min.Y);
                }

                float joinY = (parentBottom.Y + k0Top.Y) * 0.5f;

                dl.AddLine(k0Top, new Vector2(k0Top.X, joinY), colLine, 2f * s);
                if (hasSecond)
                    dl.AddLine(k1Top, new Vector2(k1Top.X, joinY), colLine, 2f * s);

                float leftX = MathF.Min(k0Top.X, k1Top.X);
                float rightX = MathF.Max(k0Top.X, k1Top.X);
                dl.AddLine(new Vector2(leftX, joinY), new Vector2(rightX, joinY), colLine, 2f * s);

                float midX = (leftX + rightX) * 0.5f;
                dl.AddLine(new Vector2(midX, joinY), new Vector2(midX, parentBottom.Y), colLine, 2f * s);
                dl.AddLine(new Vector2(midX, parentBottom.Y), parentBottom, colLine, 2f * s);
            }
        }

        // Boxes + names
        for (int level = 0; level <= roundCount; level++)
        {
            int slotCount = rects[level].Count;

            for (int i = 0; i < slotCount; i++)
            {
                var r = rects[level][i];

                dl.AddRectFilled(r.Min, r.Max, r.HasWinner ? colWinner : colFill, 6f * s);
                dl.AddRect(r.Min, r.Max, r.Active ? colActive : colBorder, 6f * s, 0, 2f * s);

                var name = SlotName(level, i);

                // Slightly smaller text to keep boxes tidy
                var pad = 6f * s;
                var pos = r.Min + new Vector2(pad, pad);

                dl.AddText(pos, ImGui.GetColorU32(ImGuiColors.DalamudWhite), name);
            }
        }

        dl.PopClipRect();
    }
    private void DrawTournamentRightPanel(ToyBox.TournamentClientView view, TournamentStatePublic? pub, TournamentStatePrivate? priv)
    {
        bool isHost = pub != null && string.Equals(view.MySessionId, pub.HostSessionId, StringComparison.Ordinal);

        if (isHost)
        {
            ImGui.TextUnformatted("Host Controls");
            ImGui.Separator();

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.UserNinja, "Join as Fighter", 220 * ImGuiHelpers.GlobalScale, true))
                _games.JoinTournament(_activeGameId, TournamentRole.Fighter);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Eye, "Join as Spectator", 220 * ImGuiHelpers.GlobalScale, true))
                _games.JoinTournament(_activeGameId, TournamentRole.Spectator);

            ImGuiHelpers.ScaledDummy(8);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Random, "Randomize fighters", 220 * ImGuiHelpers.GlobalScale, true))
                _games.HostTournamentRandomizeFighters(_activeGameId);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.ProjectDiagram, "Build bracket", 220 * ImGuiHelpers.GlobalScale, true))
                _games.HostTournamentBuildBracket(_activeGameId);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Play, "Start next match", 220 * ImGuiHelpers.GlobalScale, true))
                _games.HostTournamentStartNextMatch(_activeGameId);

            if (pub != null && pub.MatchInProgress)
            {
                ImGuiHelpers.ScaledDummy(6);
                ImGui.TextUnformatted("Force win");
                ImGui.Separator();

                if (!string.IsNullOrEmpty(pub.ActiveFighterASessionId))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trophy, pub.ActiveFighterAName, 220 * ImGuiHelpers.GlobalScale, true))
                        _games.HostTournamentForceWin(_activeGameId, pub.ActiveFighterASessionId);
                }

                if (!string.IsNullOrEmpty(pub.ActiveFighterBSessionId))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trophy, pub.ActiveFighterBName, 220 * ImGuiHelpers.GlobalScale, true))
                        _games.HostTournamentForceWin(_activeGameId, pub.ActiveFighterBSessionId);
                }
            }

            ImGuiHelpers.ScaledDummy(12);
            ImGui.TextUnformatted("Fighters (order = bracket seeding)");
            ImGui.Separator();

            if (pub != null)
            {
                bool canEditRoles = pub.Stage == TournamentStage.Lobby;

                for (int i = 0; i < pub.Fighters.Count; i++)
                {
                    using var id = ImRaii.PushId("fighter_" + i);

                    var f = pub.Fighters[i];

                    ImGui.TextUnformatted(f.Name);

                    ImGui.SameLine();

                    if (ImGui.SmallButton("▲") && i > 0)
                        _games.HostTournamentMoveFighter(_activeGameId, i, -1);

                    ImGui.SameLine();

                    if (ImGui.SmallButton("▼") && i < pub.Fighters.Count - 1)
                        _games.HostTournamentMoveFighter(_activeGameId, i, +1);

                    ImGui.SameLine();

                    if (!canEditRoles)
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "(locked)");
                    }
                    else
                    {
                        if (ImGui.SmallButton("Spectate"))
                            _games.HostTournamentMoveFighterToSpectator(_activeGameId, f.SessionId, f.Name);
                    }
                }
            }

            return;
        }

        ImGui.TextUnformatted("Your Panel");
        ImGui.Separator();

        if (priv != null && priv.Role == TournamentRole.Spectator)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Spectating. Rolls are only counted for the two active fighters.");
            return;
        }

        ImGui.TextColored(ImGuiColors.DalamudGrey, "Fighters: roll /random or /dice. Only active match rolls count.");
    }
}
