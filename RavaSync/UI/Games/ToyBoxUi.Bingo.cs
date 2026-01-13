using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using static RavaSync.Services.ToyBox;

namespace RavaSync.UI;

public sealed partial class ToyBoxUi
{
    private Vector4 _bingoMarkerColor = new Vector4(0.75f, 0.35f, 1.00f, 1.00f);
    private string? _bingoSelectedClaimSessionId;

    private readonly Dictionary<(Guid GameId, int CardIndex), HashSet<int>> _bingoMarks = new();

    private int _bingoHostNumberInput = 0;
    private int _bingoHostPhase = 0;

    private int _bingoPotOneLine = 0;
    private int _bingoPotTwoLines = 0;
    private int _bingoPotFullHouse = 0;

    private readonly Dictionary<string, int> _bingoHostDesiredCards = new(StringComparer.Ordinal);

    private Guid _bingoPotSyncGameId = Guid.Empty;
    private int _bingoPotLastOneLine = int.MinValue;
    private int _bingoPotLastTwoLines = int.MinValue;
    private int _bingoPotLastFullHouse = int.MinValue;

    private static readonly int[][] BingoLines = BuildBingoLines();


    private void DrawBingo()
    {
        if (_activeGameId == Guid.Empty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No active bingo session selected.");
            return;
        }

        if (!_games.TryGetClientBingo(_activeGameId, out var view))
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state...");
            return;
        }

        var pub = view.Public;
        var priv = view.Private;

        if (pub is null && priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for host...");
            return;
        }

        float scale = ImGuiHelpers.GlobalScale;

        var phase = pub?.Phase ?? priv?.Phase ?? BingoPhase.OneLine;
        bool isHost = pub != null && pub.HostSessionId == view.MySessionId;

        // Header row
        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * scale }))
        {
            ImGui.TextUnformatted("Bingo");
            ImGui.SameLine();

            DrawPill("Phase: " + BingoPhaseLabel(phase), PhaseColor(phase));

            if (pub != null)
            {
                ImGui.SameLine();
                DrawPill(pub.CurrentNumber > 0 ? $"Current: {pub.CurrentNumber}" : "Current: -", ImGuiColors.ParsedGreen);

                ImGui.SameLine();
                int pot = phase switch
                {
                    BingoPhase.OneLine => pub.PotOneLine,
                    BingoPhase.TwoLines => pub.PotTwoLines,
                    _ => pub.PotFullHouse
                };
                DrawPill(pot > 0 ? $"Pot: {pot}" : "Pot: -", ImGuiColors.DalamudGrey);
            }

            // Marker colour (local)
            if (priv != null)
            {
                ImGui.SameLine();
                ImGui.AlignTextToFramePadding();
                ImGui.TextUnformatted("Marker Colour");
                ImGui.SameLine();

                _bingoMarkerColor = UnpackArgb(priv.MarkerColorArgb);
                ImGui.ColorEdit4("##bingo_marker_top", ref _bingoMarkerColor,
                    ImGuiColorEditFlags.NoInputs | ImGuiColorEditFlags.AlphaBar | ImGuiColorEditFlags.PickerHueWheel);

                int newArgb = PackArgb(_bingoMarkerColor);
                if (newArgb != priv.MarkerColorArgb)
                    _games.SetLocalBingoMarker(view.GameId, newArgb);
            }

            ImGui.SameLine();
            var leaveLabel = isHost ? "Close Lobby" : "Leave";

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignOutAlt, leaveLabel, 120 * scale, true))
            {
                if (isHost) _games.CloseBingoLobby(_activeGameId);
                else _games.LeaveBingo(_activeGameId);

                _activeGameId = Guid.Empty;
                return;
            }
        }

        ImGui.Separator();

        using (ImRaii.Child("bingo_main", new Vector2(0, 0), false))
        {
            // Winner banner
            if (pub != null)
            {
                DrawBingoWinnerBanner(pub, phase);
                ImGuiHelpers.ScaledDummy(8);
            }

            // Called numbers
            DrawBingoCalledNumbers(pub);

            ImGuiHelpers.ScaledDummy(10);

            // Controls
            if (isHost) DrawBingoHostControlsInline(view);
            else DrawBingoPlayerControlsInline(view, phase);

            ImGuiHelpers.ScaledDummy(10);

            // Cards
            if (!isHost)
                DrawBingoCardsGrid(view);
        }
    }

    private void DrawBingoWinnerBanner(BingoStatePublic pub, BingoPhase phase)
    {
        if (pub.Winners == null || pub.Winners.Count == 0) return;

        var winners = pub.Winners.Where(w => w.Phase == phase).ToList();
        if (winners.Count == 0) return;

        float scale = ImGuiHelpers.GlobalScale;

        static int PhasePot(BingoStatePublic p, BingoPhase ph) => ph switch
        {
            BingoPhase.OneLine => p.PotOneLine,
            BingoPhase.TwoLines => p.PotTwoLines,
            BingoPhase.FullHouse => p.PotFullHouse,
            _ => 0,
        };

        var label = BingoPhaseLabel(phase);
        var pot = PhasePot(pub, phase);

        string msg;
        if (winners.Count == 1)
        {
            msg = $"{winners[0].Name} won {label}! Prize - {pot}";
        }
        else
        {
            var names = string.Join(", ", winners.Take(3).Select(w => w.Name));
            if (winners.Count > 3) names += $" +{winners.Count - 3}";
            msg = $"{names} won {label}! Prize - {pot}";
        }

        using (ImRaii.Child("bingo_winner_banner", new Vector2(0, 52f * scale), false, ImGuiWindowFlags.NoScrollbar | ImGuiWindowFlags.NoScrollWithMouse))
        {
            var dl = ImGui.GetWindowDrawList();
            var p0 = ImGui.GetWindowPos();
            var sz = ImGui.GetWindowSize();
            var p1 = p0 + sz;

            Vector4 purple = new(0.75f, 0.35f, 1.00f, 1.00f);
            dl.AddRectFilled(p0, p1, ImGui.GetColorU32(new Vector4(0f, 0f, 0f, 0.55f)), 14f * scale);
            dl.AddRect(p0, p1, ImGui.GetColorU32(new Vector4(purple.X, purple.Y, purple.Z, 0.55f)),
                14f * scale, 0, 2.0f * scale);

            const float baseFontScale = 1.35f;
            ImGui.SetWindowFontScale(baseFontScale);

            var textSize = ImGui.CalcTextSize(msg);
            var textPos = new Vector2(
                p0.X + (sz.X - textSize.X) * 0.5f,
                p0.Y + (sz.Y - textSize.Y) * 0.5f
            );

            var main = ImGui.GetColorU32(new Vector4(0.35f, 1.00f, 0.45f, 1.00f));
            var shadow = ImGui.GetColorU32(new Vector4(0.00f, 0.00f, 0.00f, 0.75f));

            var sh = new Vector2(2f, 2f) * scale;
            dl.AddText(textPos + sh, shadow, msg);
            dl.AddText(textPos, main, msg);

            ImGui.SetWindowFontScale(1.0f);
        }
    }


    void DrawBingoCalledNumbers(BingoStatePublic? pub)
    {
        float scale = ImGuiHelpers.GlobalScale;

        using (ImRaii.Child("bingo_called_panel", new Vector2(0, 56f * scale), true))
        {
            ImGui.AlignTextToFramePadding();
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Called:");
            ImGui.SameLine();

            if (pub?.CalledNumbers == null || pub.CalledNumbers.Count == 0)
            {
                ImGui.TextColored(ImGuiColors.DalamudGrey, "-");
            }
            else
            {
                var sorted = pub.CalledNumbers.OrderBy(n => n);
                ImGui.TextWrapped(string.Join("  ", sorted));
            }
        }
    }

    private void DrawBingoPlayerControlsInline(BingoClientView view, BingoPhase phase)
    {
        var pub = view.Public;
        var priv = view.Private;
        if (pub is null || priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state...");
            return;
        }

        float scale = ImGuiHelpers.GlobalScale;

        bool waiting = pub.PendingClaims != null
            && pub.PendingClaims.Any(c => c.SessionId == view.MySessionId && c.Phase == pub.Phase);

        bool eligible = false;
        for (int i = 0; i < priv.YourCards.Count; i++)
        {
            if (IsEligibleForPhase(GetMarks(view.GameId, i), pub.Phase))
            {
                eligible = true;
                break;
            }
        }

        bool disableCall = waiting || !eligible;

        // "Light up" when available
        if (!disableCall)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ImGuiColors.ParsedGreen);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ImGuiColors.ParsedGreen);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ImGuiColors.ParsedGreen);
        }

        ImGui.BeginDisabled(disableCall);

        string btnText = $"BINGO! - {BingoPhaseLabel(pub.Phase)}";
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.FlagCheckered, btnText, 240f * scale, true))
            _games.BingoCallBingo(view.GameId, pub.Phase);

        ImGui.EndDisabled();

        if (!disableCall)
            ImGui.PopStyleColor(3);

        if (waiting)
        {
            ImGui.SameLine();
            DrawPill("Waiting for host...", ImGuiColors.ParsedOrange);
        }
        else if (view.LastClaimResult != null
            && view.LastClaimResult.PlayerSessionId == view.MySessionId
            && view.LastClaimResult.Phase == pub.Phase)
        {
            ImGui.SameLine();
            var r = view.LastClaimResult;
            var col = r.Approved ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudRed;
            DrawPill(r.Message, col);
        }
    }

    private void DrawBingoHostControlsInline(BingoClientView view)
    {
        var pub = view.Public;
        if (pub is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state...");
            return;
        }

        float scale = ImGuiHelpers.GlobalScale;

        DrawSectionHeader("Host controls");
        ImGui.Separator();

        var claims = pub.PendingClaims;
        bool hasClaims = claims != null && claims.Count > 0;

        if (hasClaims)
        {
            if (_bingoSelectedClaimSessionId == null || !claims!.Any(c => c.SessionId == _bingoSelectedClaimSessionId))
                _bingoSelectedClaimSessionId = claims!.OrderByDescending(c => c.ClaimedTicks).First().SessionId;
        }
        else
        {
            _bingoSelectedClaimSessionId = null;
        }

        float availW = ImGui.GetContentRegionAvail().X;
        float gapW = 14f * scale;

        float leftW = 420f * scale;
        float rightW = MathF.Max(0, availW - leftW - gapW);

        float topRowH = 420f * scale;

        // Left: phase + pots + roll
        using (ImRaii.Child("bingo_host_left", new Vector2(leftW, topRowH), false,
                   ImGuiWindowFlags.NoScrollbar | ImGuiWindowFlags.NoScrollWithMouse))
        {
            string[] phaseLabels = ["One line", "Two lines", "Full house"];
            _bingoHostPhase = (int)pub.Phase;

            ImGui.SetNextItemWidth(220f * scale);
            if (ImGui.Combo("Phase", ref _bingoHostPhase, phaseLabels, phaseLabels.Length))
                _games.HostBingoSetPhase(view.GameId, (BingoPhase)_bingoHostPhase);

            bool canAdvance = pub.Phase != BingoPhase.FullHouse && pub.Winners != null && pub.Winners.Any(w => w.Phase == pub.Phase);
            ImGui.SameLine();
            ImGui.BeginDisabled(!canAdvance);
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.StepForward, "Next phase", 120f * scale, true))
                _games.HostBingoAdvancePhase(view.GameId);
            ImGui.EndDisabled();

            ImGuiHelpers.ScaledDummy(10);

            DrawSectionHeader("Pots");
            ImGui.Separator();

            if (_bingoPotSyncGameId != view.GameId ||
                _bingoPotLastOneLine != pub.PotOneLine ||
                _bingoPotLastTwoLines != pub.PotTwoLines ||
                _bingoPotLastFullHouse != pub.PotFullHouse)
            {
                _bingoPotSyncGameId = view.GameId;
                _bingoPotOneLine = pub.PotOneLine;
                _bingoPotTwoLines = pub.PotTwoLines;
                _bingoPotFullHouse = pub.PotFullHouse;

                _bingoPotLastOneLine = pub.PotOneLine;
                _bingoPotLastTwoLines = pub.PotTwoLines;
                _bingoPotLastFullHouse = pub.PotFullHouse;
            }

            ImGui.SetNextItemWidth(120f * scale);
            ImGui.InputInt("One line", ref _bingoPotOneLine);
            ImGui.SetNextItemWidth(120f * scale);
            ImGui.InputInt("Two lines", ref _bingoPotTwoLines);
            ImGui.SetNextItemWidth(120f * scale);
            ImGui.InputInt("Full house", ref _bingoPotFullHouse);

            _bingoPotOneLine = Math.Max(0, _bingoPotOneLine);
            _bingoPotTwoLines = Math.Max(0, _bingoPotTwoLines);
            _bingoPotFullHouse = Math.Max(0, _bingoPotFullHouse);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, "Apply pots", 140f * scale, true))
            {
                _games.HostBingoSetPots(view.GameId, _bingoPotOneLine, _bingoPotTwoLines, _bingoPotFullHouse);
                _bingoPotLastOneLine = _bingoPotOneLine;
                _bingoPotLastTwoLines = _bingoPotTwoLines;
                _bingoPotLastFullHouse = _bingoPotFullHouse;
            }

            ImGuiHelpers.ScaledDummy(8);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Dice, "Roll next number", 180f * scale, true))
                _games.HostBingoRollRandom(view.GameId);
        }

        // Right: claim station
        if (hasClaims && rightW > 220f * scale)
        {
            ImGui.SameLine(0, gapW);

            using (ImRaii.Child("bingo_claim_station", new Vector2(rightW, topRowH), true))
            {
                var sel = claims!.FirstOrDefault(c => c.SessionId == _bingoSelectedClaimSessionId) ?? claims!.First();

                ImGui.TextColored(ImGuiColors.DalamudGrey, $"{sel.Name} — {BingoPhaseLabel(sel.Phase)}");
                ImGuiHelpers.ScaledDummy(4);

                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, "Approve", 120f * scale, true))
                    _games.HostBingoApproveClaim(view.GameId, sel.SessionId, sel.Phase);

                ImGui.SameLine();
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Times, "Deny", 120f * scale, true))
                    _games.HostBingoDenyClaim(view.GameId, sel.SessionId, "Not valid for this phase.");

                ImGuiHelpers.ScaledDummy(3);

                if (_games.TryGetBingoClaimPreview(view.GameId, sel.SessionId, sel.Phase, out var preview))
                {
                    DrawBingoCard(
                        view.GameId,
                        preview.CardIndex,
                        preview.Card,
                        pub.CalledNumbers,
                        preview.MarkerColorArgb,
                        allowMarking: false,
                        highlightIndices: preview.MatchedIndices,
                        showHeader: false
                    );
                }
                else
                {
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for preview...");
                }
            }
        }

        ImGuiHelpers.ScaledDummy(12);

        DrawSectionHeader("Players");
        ImGui.Separator();

        foreach (var p in pub.Players.OrderBy(p => p.Name))
        {
            using var id = ImRaii.PushId("bingo_player_" + p.SessionId);

            ImGui.TextUnformatted(p.Name);
            ImGui.SameLine();

            int desired = _bingoHostDesiredCards.TryGetValue(p.SessionId, out var v) ? v : p.CardCount;
            ImGui.SetNextItemWidth(140f * scale);
            if (ImGui.SliderInt("##cards", ref desired, 1, 10))
                _bingoHostDesiredCards[p.SessionId] = desired;

            ImGui.SameLine();
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Magic, "Deal", 90f * scale, true))
                _games.HostBingoSetPlayerCards(view.GameId, p.SessionId, desired);

            ImGui.SameLine();
            ImGui.TextColored(ImGuiColors.DalamudGrey, $"  (current: {p.CardCount})");

            if (p.HasPendingClaim)
            {
                ImGui.SameLine();
                var claim = pub.PendingClaims?.FirstOrDefault(c => c.SessionId == p.SessionId);
                if (claim != null)
                    DrawPill("CLAIM: " + BingoPhaseLabel(claim.Phase), ImGuiColors.ParsedOrange);
                else
                    DrawPill("CLAIM", ImGuiColors.ParsedOrange);
            }
        }
    }


    private void DrawBingoCardsGrid(BingoClientView view)
    {
        var pub = view.Public;
        var priv = view.Private;
        float scale = ImGuiHelpers.GlobalScale;

        if (pub is null || priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state...");
            return;
        }

        DrawSectionHeader($"Your cards ({priv.YourCards.Count})");
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(4);

        int cols = DetermineCardColumns(priv.YourCards.Count);
        float availX = ImGui.GetContentRegionAvail().X;
        float cardW = Math.Max(220f * scale, (availX - (cols - 1) * 10f * scale) / cols);

        for (int i = 0; i < priv.YourCards.Count; i++)
        {
            if (i % cols != 0) ImGui.SameLine();
            using (ImRaii.Child($"bingo_card_{i}", new Vector2(cardW, 310f * scale), true))
            {
                DrawBingoCard(view.GameId, i, priv.YourCards[i], pub.CalledNumbers, priv.MarkerColorArgb, allowMarking: true, highlightIndices: null);
            }
        }
    }




    private void DrawBingoCard(Guid gameId, int cardIndex, BingoCard card, IReadOnlyList<int> calledNumbers,
        int markerColorArgb, bool allowMarking, IReadOnlyList<int>? highlightIndices, bool showHeader = true)
    {
        float scale = ImGuiHelpers.GlobalScale;
        if (card?.Numbers == null || card.Numbers.Count != 25)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Invalid card.");
            return;
        }

        var marked = GetMarks(gameId, cardIndex);
        var calledSet = calledNumbers != null ? new HashSet<int>(calledNumbers) : null;
        var highlight = highlightIndices != null ? new HashSet<int>(highlightIndices) : null;
        
        if (showHeader)
        {
            ImGui.TextUnformatted($"Card #{cardIndex + 1}");
            ImGuiHelpers.ScaledDummy(6);
        }
        
        float cell = 44f * scale;
        float gap = 4f * scale;
        var dl = ImGui.GetWindowDrawList();

        var markerCol = UnpackArgb(markerColorArgb);
        uint markerFill = ImGui.GetColorU32(new Vector4(markerCol.X, markerCol.Y, markerCol.Z, 0.22f));
        uint markerBorder = ImGui.GetColorU32(new Vector4(markerCol.X, markerCol.Y, markerCol.Z, 0.65f));

        float gridW = (cell * 5f) + (gap * 4f);
        float baseX = ImGui.GetCursorPosX();
        float availW = ImGui.GetContentRegionAvail().X;
        float centeredX = baseX + MathF.Max(0f, (availW - gridW) * 0.5f);

        for (int r = 0; r < 5; r++)
        {
            ImGui.SetCursorPosX(centeredX);

            for (int c = 0; c < 5; c++)
            {
                int idx = r * 5 + c;
                int num = card.Numbers[idx];
                bool userMarked = marked.Contains(idx);
                bool isCalled = calledSet != null && calledSet.Contains(num);
                bool hi = highlight != null && highlight.Contains(idx);

                using var id = ImRaii.PushId($"bingo_{gameId}_{cardIndex}_{idx}");

                var p0 = ImGui.GetCursorScreenPos();
                var p1 = p0 + new Vector2(cell, cell);

                Vector4 purple = new Vector4(0.75f, 0.35f, 1.00f, 1.00f);
                uint glow = ImGui.GetColorU32(new Vector4(purple.X, purple.Y, purple.Z, 0.20f));
                uint border = ImGui.GetColorU32(new Vector4(purple.X, purple.Y, purple.Z, 0.70f));

                float rowA = (r % 2 == 0) ? 0.18f : 0.12f;
                uint bg = ImGui.GetColorU32(new Vector4(0.08f, 0.08f, 0.10f, 0.90f + rowA * 0.0f));

                dl.AddRectFilled(p0, p1, bg, 10f * scale);

                dl.AddRect(p0 - new Vector2(1.5f, 1.5f) * scale, p1 + new Vector2(1.5f, 1.5f) * scale, glow, 11f * scale, 0, 3.0f * scale);
                dl.AddRect(p0, p1, border, 10f * scale, 0, 1.8f * scale);

                if (userMarked)
                {
                    dl.AddRectFilled(p0 + new Vector2(3, 3) * scale, p1 - new Vector2(3, 3) * scale, markerFill, 8f * scale);
                    dl.AddRect(p0 + new Vector2(3, 3) * scale, p1 - new Vector2(3, 3) * scale, markerBorder, 8f * scale, 0, 1.5f * scale);
                }

                if (hi)
                {
                    uint hiCol = ImGui.GetColorU32(new Vector4(1f, 0.8f, 0.2f, 0.85f));
                    dl.AddRect(p0 + new Vector2(1, 1) * scale, p1 - new Vector2(1, 1) * scale, hiCol, 10f * scale, 0, 2.5f * scale);
                }

                ImGui.InvisibleButton("##cell", new Vector2(cell, cell));
                if (allowMarking && ImGui.IsItemClicked(ImGuiMouseButton.Left))
                {
                    // Only allow marking called numbers (but always allow unmark)
                    if (userMarked || isCalled)
                    {
                        if (!marked.Add(idx))
                            marked.Remove(idx);
                    }
                }

                var text = num.ToString();
                var ts = ImGui.CalcTextSize(text);
                var tp = p0 + (new Vector2(cell, cell) - ts) / 2f;

                uint txtCol = ImGui.GetColorU32(new Vector4(0.90f, 0.90f, 0.94f, 1.00f));
                if (userMarked)
                    txtCol = ImGui.GetColorU32(ImGuiColors.ParsedGreen);
                if (hi && !allowMarking)
                    txtCol = ImGui.GetColorU32(new Vector4(1f, 0.85f, 0.25f, 1f));

                dl.AddText(tp + new Vector2(1, 1) * scale, ImGui.GetColorU32(new Vector4(0f, 0f, 0f, 0.60f)), text);
                dl.AddText(tp, txtCol, text);

                if (c < 4) ImGui.SameLine(0, gap);
            }

            ImGuiHelpers.ScaledDummy(1);
        }
    }

    private HashSet<int> GetMarks(Guid gameId, int cardIndex)
    {
        var key = (gameId, cardIndex);
        if (!_bingoMarks.TryGetValue(key, out var set))
        {
            set = new HashSet<int>();
            _bingoMarks[key] = set;
        }

        return set;
    }

    private static int DetermineCardColumns(int cardCount)
    {
        if (cardCount <= 1) return 1;
        if (cardCount <= 4) return 2;
        if (cardCount <= 6) return 3;
        if (cardCount <= 9) return 3;
        return 4;
    }

    private static string BingoPhaseLabel(BingoPhase phase) => phase switch
    {
        BingoPhase.OneLine => "One line",
        BingoPhase.TwoLines => "Two lines",
        _ => "Full house"
    };

    private static Vector4 PhaseColor(BingoPhase phase) => phase switch
    {
        BingoPhase.OneLine => ImGuiColors.ParsedOrange,
        BingoPhase.TwoLines => new Vector4(0.55f, 0.75f, 1.0f, 1.0f),
        _ => ImGuiColors.ParsedGreen
    };

    private static int RollUncalled(IReadOnlyList<int> called)
    {
        var set = called != null ? new HashSet<int>(called) : new HashSet<int>();
        var avail = new List<int>(49);
        for (int i = 1; i <= 49; i++)
            if (!set.Contains(i)) avail.Add(i);

        if (avail.Count == 0) return 0;
        return avail[Random.Shared.Next(avail.Count)];
    }

    private static int[][] BuildBingoLines()
    {
        var lines = new List<int[]>(12);

        for (int r = 0; r < 5; r++)
            lines.Add([r * 5 + 0, r * 5 + 1, r * 5 + 2, r * 5 + 3, r * 5 + 4]);

        for (int c = 0; c < 5; c++)
            lines.Add([0 * 5 + c, 1 * 5 + c, 2 * 5 + c, 3 * 5 + c, 4 * 5 + c]);

        lines.Add([0, 6, 12, 18, 24]);
        lines.Add([4, 8, 12, 16, 20]);

        return lines.ToArray();
    }



    private static int CountCompletedLines(HashSet<int> marked)
    {
        int count = 0;
        foreach (var line in BingoLines)
        {
            bool ok = true;
            for (int i = 0; i < line.Length; i++)
            {
                if (!marked.Contains(line[i]))
                {
                    ok = false;
                    break;
                }
            }

            if (ok) count++;
        }

        return count;
    }

    private static bool IsEligibleForPhase(HashSet<int> marked, BingoPhase phase)
    {
        return phase switch
        {
            BingoPhase.OneLine => CountCompletedLines(marked) >= 1,
            BingoPhase.TwoLines => CountCompletedLines(marked) >= 2,
            BingoPhase.FullHouse => marked.Count >= 25,
            _ => false,
        };
    }
    private static int PackArgb(Vector4 col)
    {
        int a = (int)Math.Round(Math.Clamp(col.W, 0f, 1f) * 255f);
        int r = (int)Math.Round(Math.Clamp(col.X, 0f, 1f) * 255f);
        int g = (int)Math.Round(Math.Clamp(col.Y, 0f, 1f) * 255f);
        int b = (int)Math.Round(Math.Clamp(col.Z, 0f, 1f) * 255f);

        return (a << 24) | (r << 16) | (g << 8) | b;
    }

    private static Vector4 UnpackArgb(int argb)
    {
        float a = ((argb >> 24) & 0xFF) / 255f;
        float r = ((argb >> 16) & 0xFF) / 255f;
        float g = ((argb >> 8) & 0xFF) / 255f;
        float b = (argb & 0xFF) / 255f;
        return new Vector4(r, g, b, a);
    }
}
