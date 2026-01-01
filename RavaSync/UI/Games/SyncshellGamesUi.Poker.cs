using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Microsoft.Extensions.Logging;
using RavaSync.API.Dto.Group;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Dalamud.Interface.Textures.TextureWraps;
using System.IO;
using System.Numerics;

namespace RavaSync.UI;

public sealed partial class SyncshellGamesUi
{
    private int _pokerRaiseTo = 0;

    private int _pokerHostBuyIn = 0;
    private int _pokerHostSmallBlind = 10000;
    private int _pokerHostBigBlind = 20000;
    
    private int _pokerRaiseBy;
    private int _pokerRaiseByMinLast = -1;
    private int _pokerRaiseByMaxLast = -1;



    private void DrawPoker()
    {
        
        if (_activeGameId == Guid.Empty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No active poker session selected.");
            return;
        }

        if (!_games.TryGetClientPoker(_activeGameId, out var view))
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for state…");
            return;
        }

        var pub = view.Public;
        var priv = view.Private;
        bool isHost = pub != null && pub.HostSessionId == view.MySessionId;

        if (pub is null && priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for host…");
            return;
        }

        float scale = ImGuiHelpers.GlobalScale;
        var stage = pub?.Stage ?? priv?.Stage ?? PokerStage.Lobby;

        // Header row (phase, blinds, buy-in, leave)
        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * scale }))
        {
            ImGui.TextUnformatted("Poker");
            ImGui.SameLine();

            DrawPill("Phase: " + PokerPhaseLabel(stage),
                stage == PokerStage.Preflop ? ImGuiColors.ParsedOrange : ImGuiColors.DalamudGrey);

            if (pub != null)
            {
                ImGui.SameLine();
                DrawPill($"Blinds: {pub.SmallBlind}/{pub.BigBlind}", ImGuiColors.DalamudGrey);

                if (pub.TableBuyIn > 0)
                {
                    ImGui.SameLine();
                    DrawPill($"Buy-in: {pub.TableBuyIn}", ImGuiColors.DalamudGrey);
                    ImGui.SameLine();
                    DrawPill($"Total pot: {pub?.PotTotal ?? 0}", ImGuiColors.ParsedGreen);
                }
                
                
                if (!string.IsNullOrEmpty(pub.CurrentTurnSessionId))
                {
                    ImGui.SameLine();
                    bool isMyTurn = pub.CurrentTurnSessionId == view.MySessionId;
                    string turnName = isMyTurn ? "You" : ResolvePokerName(pub, pub.CurrentTurnSessionId);
                    DrawPill($"Turn: {turnName}", isMyTurn ? ImGuiColors.ParsedGreen : ImGuiColors.ParsedOrange);
                }
            }

            ImGui.SameLine();
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignOutAlt, "Leave", 120 * scale, true))
            {
                _games.LeavePoker(_activeGameId);
                return;
            }
        }

        ImGui.Separator();

        float sideW = 420f * scale;

        if (ImGui.BeginTable("poker_layout", 2, ImGuiTableFlags.SizingStretchProp))
        {
            try
            {
                ImGui.TableSetupColumn("Table", ImGuiTableColumnFlags.WidthStretch);
                ImGui.TableSetupColumn("Side", ImGuiTableColumnFlags.WidthFixed, sideW);

                ImGui.TableNextRow();

                // LEFT: TABLE
                ImGui.TableNextColumn();
                using (ImRaii.Child("poker_left", new Vector2(0, 0), false))
                {
                    using (ImRaii.Child("poker_felt", new Vector2(0, 0), true, ImGuiWindowFlags.NoScrollbar))
                    {
                        var feltDl = ImGui.GetWindowDrawList();
                        var p0 = ImGui.GetWindowPos();
                        var sz = ImGui.GetWindowSize();
                        var p1 = p0 + sz;

                        ImGui.SetWindowFontScale(1.0f);

                        feltDl.AddRectFilled(p0, p1, ImGui.GetColorU32(new Vector4(0.05f, 0.10f, 0.08f, 0.75f)), 18f * scale);
                        feltDl.AddRect(p0, p1, ImGui.GetColorU32(new Vector4(0.60f, 0.60f, 0.70f, 0.30f)), 18f * scale, 0, 2f);

                        float safeTop = 52f * scale;
                        float safePad = 14f * scale;

                        float availX = ImGui.GetContentRegionAvail().X;
                        float availY = ImGui.GetContentRegionAvail().Y;

                        float lineH = ImGui.CalcTextSize("Ay").Y;
                        float innerPadY = 8f * scale;
                        float lineGap = 4f * scale;
                        float seatHMin = (innerPadY * 2f) + (lineH * 3f) + (lineGap * 2f);

                        float seatHMax = 118f * scale;
                        if (seatHMin > seatHMax) seatHMin = seatHMax;

                        float seatHEst = Math.Clamp(sz.Y * 0.12f, seatHMin, seatHMax);
                        float seatBand = seatHEst + 26f * scale;

                        float boardAreaY = Math.Max(180f * scale, availY - safeTop - seatBand * 2f);
                        float boardCardH = ComputeBoardCardHeightResponsive(availX, boardAreaY, scale);

                        float cardW = boardCardH * (2f / 3f);
                        float spacing = 10f * scale;
                        float idealTotalW = 5 * cardW + 4 * spacing;

                        ImGuiHelpers.ScaledDummy(6);

                        float freeY = Math.Max(0, availY - safeTop - seatBand * 2f - boardCardH);
                        float boardYOffset = seatBand + freeY * 0.45f;

                        ImGui.SetCursorPosY(ImGui.GetCursorPosY() + safeTop + boardYOffset);

                        float step = cardW * 0.70f;
                        float stackedTotalW = cardW + 4 * step;

                        float xPad = Math.Max(0, (availX - stackedTotalW) / 2f);
                        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + xPad);

                        var boardTop = ImGui.GetCursorScreenPos();
                        bool hideMissing = stage != PokerStage.Showdown && stage != PokerStage.RoundOver;

                        DrawCardSlotsStackedSized(5, pub?.BoardCards ?? [], hideMissing, boardCardH, step);

                        if (pub != null)
                        {
                            var fg = ImGui.GetForegroundDrawList();
                            DrawPokerSeatsOverlay(fg, p0, sz, pub, view.MySessionId, boardTop.Y, boardTop.Y + boardCardH);
                        }
                        if (pub != null && pub.Stage == PokerStage.RoundOver && pub.LastHandResults != null && pub.LastHandResults.Count > 0)
                        {
                            var top = pub.LastHandResults[0];
                            var dl = ImGui.GetWindowDrawList();

                            string line1 = top.Name;
                            string line2 = $"won the pot of {pub.PotTotal}";
                            string line3 = $"with {top.HandLabel}";

                            const float prevFontScale = 1.0f;

                            float nameScale = 1.55f;
                            float infoScale = 1.18f;

                            Vector2 t1, t2, t3;
                            try
                            {
                                ImGui.SetWindowFontScale(nameScale);
                                t1 = ImGui.CalcTextSize(line1);

                                ImGui.SetWindowFontScale(infoScale);
                                t2 = ImGui.CalcTextSize(line2);
                                t3 = ImGui.CalcTextSize(line3);
                            }
                            finally
                            {
                                ImGui.SetWindowFontScale(prevFontScale);
                            }

                            float pad = 16f * scale;
                            float w = MathF.Max(t1.X, MathF.Max(t2.X, t3.X)) + pad * 2f;
                            float h = (t1.Y + t2.Y + t3.Y) + pad * 2.4f;

                            float centerX = p0.X + sz.X * 0.5f;

                            float aboveCenterY = boardTop.Y - (h * 0.5f) - (10f * scale);
                            float belowCenterY = boardTop.Y + boardCardH + (h * 0.5f) + (10f * scale);

                            float minY = p0.Y + safePad;
                            float maxY = (p0.Y + sz.Y) - safePad;

                            float centerY;
                            if (aboveCenterY - h * 0.5f >= minY)
                                centerY = aboveCenterY;
                            else if (belowCenterY + h * 0.5f <= maxY)
                                centerY = belowCenterY;
                            else
                                centerY = p0.Y + sz.Y * 0.5f;

                            var center = new Vector2(centerX, centerY);
                            var rMin = center - new Vector2(w, h) * 0.5f;
                            var rMax = center + new Vector2(w, h) * 0.5f;

                            dl.AddRectFilled(rMin, rMax, ImGui.GetColorU32(new Vector4(0, 0, 0, 0.68f)), 16f * scale);
                            dl.AddRect(rMin, rMax, ImGui.GetColorU32(new Vector4(1, 1, 1, 0.20f)), 16f * scale, 0, 2f);

                            float y = rMin.Y + pad;

                            try
                            {
                                ImGui.SetWindowFontScale(nameScale);
                                dl.AddText(new Vector2(center.X - t1.X * 0.5f, y), ImGui.GetColorU32(ImGuiColors.ParsedGreen), line1);
                                y += t1.Y + (6f * scale);

                                ImGui.SetWindowFontScale(infoScale);
                                dl.AddText(new Vector2(center.X - t2.X * 0.5f, y), ImGui.GetColorU32(ImGuiColors.DalamudGrey), line2);
                                y += t2.Y + (4f * scale);

                                dl.AddText(new Vector2(center.X - t3.X * 0.5f, y), ImGui.GetColorU32(ImGuiColors.DalamudGrey), line3);
                            }
                            finally
                            {
                                ImGui.SetWindowFontScale(prevFontScale);
                            }
                        }

                    }
                }

                // RIGHT: YOU / ACTIONS (+ HOST CONTROLS FOR HOST)
                ImGui.TableNextColumn();
                using (ImRaii.Child("poker_side", new Vector2(0, 0), true))
                {
                    if (pub != null && pub.HostSessionId == view.MySessionId)
                    {
                        DrawSectionHeader("Host");

                        ImGui.SetNextItemWidth(-1);
                        ImGui.InputInt("Table buy-in", ref _pokerHostBuyIn);
                        if (_pokerHostBuyIn < 1) _pokerHostBuyIn = 1;

                        ImGui.SetNextItemWidth(-1);
                        ImGui.InputInt("Small blind", ref _pokerHostSmallBlind);
                        if (_pokerHostSmallBlind < 1) _pokerHostSmallBlind = 1;

                        ImGui.SetNextItemWidth(-1);
                        ImGui.InputInt("Big blind", ref _pokerHostBigBlind);
                        if (_pokerHostBigBlind < _pokerHostSmallBlind) _pokerHostBigBlind = _pokerHostSmallBlind;

                        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { Y = 6f * scale }))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, "Apply table settings", -1, true))
                            {
                                _games.HostPokerConfigure(_activeGameId, _pokerHostBuyIn, _pokerHostSmallBlind, _pokerHostBigBlind);
                            }

                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, "Start hand", -1, true))
                            {
                                _games.HostPokerConfigure(_activeGameId, _pokerHostBuyIn, _pokerHostSmallBlind, _pokerHostBigBlind);
                                _games.HostPokerStartHand(_activeGameId, _pokerHostBuyIn);
                            }
                        }

                        ImGuiHelpers.ScaledDummy(8);
                        ImGui.Separator();
                        ImGuiHelpers.ScaledDummy(8);
                    }

                    DrawSectionHeader("You");

                    float handCardH = 240f * scale;

                    if (priv != null && priv.YourHoleCards != null && priv.YourHoleCards.Count > 0)
                    {
                        DrawCardRowInlineSized(priv.YourHoleCards.ToArray(), showAsHidden: false, cardH: handCardH);
                    }
                    else
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, stage == PokerStage.Lobby ? "Waiting for the host to deal…" : "No hand yet.");
                    }

                    if (pub != null)
                    {
                        var mePub = pub.Players.FirstOrDefault(x => x.SessionId == view.MySessionId);
                        if (mePub != null)
                            ImGui.TextColored(ImGuiColors.DalamudGrey, $"Chips: {mePub.Stack}   |   In: {mePub.CommittedThisHand}");
                    }

                    ImGuiHelpers.ScaledDummy(10);
                    DrawSectionHeader("Actions");

                    if (priv == null)
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for host…");
                        return;
                    }

                    // Reveal option at showdown
                    if (pub != null && pub.Stage == PokerStage.Showdown)
                    {
                        if (priv.YourHoleCards != null && priv.YourHoleCards.Count == 2)
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Eye, "Reveal your hand", -1, true))
                                _games.PokerReveal(_activeGameId, priv.YourHoleCards);

                            ImGuiHelpers.ScaledDummy(6);
                        }
                    }

                    if (pub != null && pub.Stage == PokerStage.RoundOver)
                    {
                        ImGui.Spacing();
                        ImGui.Separator();
                        ImGui.TextUnformatted("Hand complete");

                        var top = pub.LastHandResults?.FirstOrDefault();
                        if (top != null)
                        {
                            ImGui.TextColored(ImGuiColors.ParsedGreen, $"{top.Name} won {top.WonAmount}");
                            ImGui.TextDisabled($"With: {top.HandLabel}");
                            ImGui.TextDisabled($"Total pot: {pub.PotTotal}");
                        }
                        else
                        {
                            ImGui.TextDisabled("Hand complete.");
                        }

                        ImGui.Spacing();

                        if (isHost)
                        {
                            int buyIn = Math.Max(1, pub.TableBuyIn > 0 ? pub.TableBuyIn : _pokerHostBuyIn);

                            if (ImGui.Button("Start next hand", new Vector2(-1, 0)))
                                _games.HostPokerStartHand(_activeGameId, buyIn);
                        }
                        else
                        {
                            ImGui.TextDisabled("Waiting for the host to start the next hand...");
                        }

                        return;
                    }


                    if (!priv.IsYourTurn)
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting…");
                        return;
                    }

                    var allowed = priv.AllowedActions ?? [];

                    using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 8f * scale, Y = 6f * scale }))
                    {
                        if (allowed.Contains(PokerActionKind.Fold))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Times, "Fold", -1, true))
                            {
                                _games.PokerAction(_activeGameId, PokerActionKind.Fold);
                                return;
                            }
                        }

                        if (allowed.Contains(PokerActionKind.Check))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, "Check", -1, true))
                            {
                                _games.PokerAction(_activeGameId, PokerActionKind.Check);
                                return;
                            }
                        }

                        if (allowed.Contains(PokerActionKind.Call))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Phone, $"Call ({priv.CallAmount})", -1, true))
                            {
                                _games.PokerAction(_activeGameId, PokerActionKind.Call);
                                return;
                            }
                        }

                        if (allowed.Contains(PokerActionKind.Raise))
                        {
                            int currentBet = pub?.CurrentBet ?? 0;

                            int minRaiseTo = Math.Max(priv.MinRaiseTo, 0);
                            int maxRaiseTo = Math.Max(priv.MaxRaiseTo, minRaiseTo);

                            int minBy = Math.Max(0, minRaiseTo - currentBet);
                            int maxBy = Math.Max(minBy, maxRaiseTo - currentBet);

                            if (_pokerRaiseByMinLast != minBy || _pokerRaiseByMaxLast != maxBy)
                            {
                                _pokerRaiseByMinLast = minBy;
                                _pokerRaiseByMaxLast = maxBy;
                                _pokerRaiseBy = minBy;
                            }

                            if (_pokerRaiseBy < minBy) _pokerRaiseBy = minBy;
                            if (_pokerRaiseBy > maxBy) _pokerRaiseBy = maxBy;

                            int raiseTo = currentBet + _pokerRaiseBy;

                            ImGui.TextColored(ImGuiColors.DalamudGrey, $"Raise by:  (min {minBy} / max {maxBy})");
                            ImGui.SetNextItemWidth(-1);
                            ImGui.InputInt("##PokerRaiseBy", ref _pokerRaiseBy);

                            if (_pokerRaiseBy < minBy) _pokerRaiseBy = minBy;
                            if (_pokerRaiseBy > maxBy) _pokerRaiseBy = maxBy;

                            raiseTo = currentBet + _pokerRaiseBy;

                            ImGui.TextDisabled($"→ Raise to: {raiseTo}");

                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.LevelUpAlt, "Raise", -1, true))
                            {
                                _games.PokerAction(_activeGameId, PokerActionKind.Raise, raiseTo);
                                return;
                            }
                        }
                        else
                        {
                            _pokerRaiseByMinLast = -1;
                            _pokerRaiseByMaxLast = -1;
                        }



                        if (allowed.Contains(PokerActionKind.AllIn))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Bomb, "All-in", -1, true))
                            {
                                _games.PokerAction(_activeGameId, PokerActionKind.AllIn);
                                return;
                            }
                        }
                        
                        if (pub.Stage == PokerStage.RoundOver && pub.LastHandResults != null && pub.LastHandResults.Count > 0)
                        {
                            ImGui.Separator();
                            ImGui.TextUnformatted("Hand result");

                            foreach (var r in pub.LastHandResults)
                            {
                                ImGui.TextUnformatted($"{r.Name} won {r.WonAmount}  ({r.HandLabel})");
                            }
                        }
                    }
                }
            }
            finally
            {
                ImGui.EndTable();
            }
        }
    }

private void DrawPokerSeatsOverlay(ImDrawListPtr dl, Vector2 p0, Vector2 size, PokerStatePublic pub, string mySessionId, float boardBandTop, float boardBandBottom)
    {
        float scale = ImGuiHelpers.GlobalScale;

        var players = pub.Players ?? [];
        if (players.Count == 0) return;

        var revealLookup = (pub.Reveals ?? new List<PokerRevealPublic>())
            .Where(r => r.HoleCards != null && r.HoleCards.Count == 2)
            .ToDictionary(r => r.SessionId, r => r.HoleCards, StringComparer.Ordinal);

        float pad = 16f * scale;

        float topEdgeY = p0.Y + pad;
        float bottomEdgeY = p0.Y + size.Y - pad;

        float FitY(float y, float seatH)
        {
            if (y + seatH < boardBandTop - pad) return y;
            if (y > boardBandBottom + pad) return y;

            float up = Math.Max(topEdgeY, boardBandTop - seatH - pad);
            float down = Math.Min(bottomEdgeY - seatH, boardBandBottom + pad);

            if (Math.Abs(y - up) < Math.Abs(down - y)) return up;
            return down;
        }

        float lineH = ImGui.GetTextLineHeight();
        float topPad = 10f * scale;
        float bottomPad = 10f * scale;
        float lineGap = 4f * scale;

        float seatH = topPad + bottomPad + (lineH * 3f) + (lineGap * 2f);
        seatH = Math.Clamp(seatH, 78f * scale, 110f * scale);

        float leftPad = 12f * scale;
        float rightPad = 12f * scale;

        float RequiredSeatW(PokerPlayerPublic pl)
        {
            var l1 = pl.Name ?? "";
            var l2 = $"Bet: {pl.CommittedThisStreet}";
            var l3 = $"Chips: {pl.Stack}";

            float w1 = ImGui.CalcTextSize(l1).X;
            float w2 = ImGui.CalcTextSize(l2).X;
            float w3 = ImGui.CalcTextSize(l3).X;

            float w = Math.Max(w1, Math.Max(w2, w3));
            return w + leftPad + rightPad + (18f * scale);
        }

        float baseSeatW = size.X * 0.26f;
        float minSeatW = 240f * scale;
        float maxSeatW = 360f * scale;

        float seatW = Math.Max(baseSeatW, players.Take(8).Max(RequiredSeatW));
        seatW = Math.Clamp(seatW, minSeatW, maxSeatW);

        if (players.Count >= 6)
            seatW = Math.Clamp(seatW * 0.92f, 220f * scale, 340f * scale);

        var seatSize = new Vector2(seatW, seatH);

        float leftX = p0.X + pad;
        float rightX = p0.X + size.X - seatW - pad;
        float centerX = p0.X + (size.X - seatW) / 2f;

        float topY = topEdgeY;
        float bottomY = Math.Min(bottomEdgeY - seatH, p0.Y + size.Y - seatH - pad);
        float midY = p0.Y + (size.Y - seatH) / 2f;

        List<Vector2> seats;

        if (players.Count <= 2)
        {
            seats = [new(centerX, bottomY), new(centerX, topY)];
        }
        else if (players.Count == 3)
        {
            seats = [new(centerX, bottomY), new(leftX, FitY(midY, seatH)), new(rightX, FitY(midY, seatH))];
        }
        else if (players.Count == 4)
        {
            seats = [new(centerX, bottomY), new(leftX, FitY(midY, seatH)), new(centerX, topY), new(rightX, FitY(midY, seatH))];
        }
        else
        {
            seats =
            [
                new(centerX, bottomY),
            new(leftX,  FitY(bottomY - seatH * 0.15f, seatH)),
            new(rightX, FitY(bottomY - seatH * 0.15f, seatH)),
            new(centerX, topY),
            new(leftX,  FitY(topY + seatH * 0.15f, seatH)),
            new(rightX, FitY(topY + seatH * 0.15f, seatH)),
            new(leftX,  FitY(midY, seatH)),
            new(rightX, FitY(midY, seatH)),
        ];
        }

        static string Trunc(string s, float maxW)
        {
            if (string.IsNullOrEmpty(s)) return s;
            if (ImGui.CalcTextSize(s).X <= maxW) return s;

            const string ell = "…";
            float ellW = ImGui.CalcTextSize(ell).X;

            int lo = 0;
            int hi = s.Length;

            while (lo < hi)
            {
                int mid = (lo + hi + 1) / 2;
                string t = s.Substring(0, mid) + ell;
                if (ImGui.CalcTextSize(t).X <= maxW) lo = mid;
                else hi = mid - 1;
            }

            return s.Substring(0, lo) + ell;
        }

        int count = Math.Min(players.Count, seats.Count);

        for (int i = 0; i < count; i++)
        {
            var pl = players[i];
            var pos = seats[i];

            bool isMe = pl.SessionId == mySessionId;
            bool isTurn = pub.CurrentTurnSessionId == pl.SessionId;

            uint bg = ImGui.GetColorU32(new Vector4(0.08f, 0.08f, 0.10f, 0.62f));
            uint border = ImGui.GetColorU32(new Vector4(0.75f, 0.75f, 0.85f, 0.30f));

            if (isMe)
            {
                bg = ImGui.GetColorU32(new Vector4(0.10f, 0.16f, 0.10f, 0.75f));
                border = ImGui.GetColorU32(new Vector4(0.35f, 0.95f, 0.45f, 0.55f));
            }

            if (isTurn)
                border = ImGui.GetColorU32(new Vector4(1.00f, 0.70f, 0.20f, 0.85f));

            dl.AddRectFilled(pos, pos + seatSize, bg, 12f * scale);
            dl.AddRect(pos, pos + seatSize, border, 12f * scale, 0, 2f);

            string tag = pl.Folded ? "FOLD" : (pl.AllIn ? "ALL-IN" : "");
            float tagW = string.IsNullOrEmpty(tag) ? 0f : (ImGui.CalcTextSize(tag).X + 8f * scale);

            var clipMin = pos + new Vector2(leftPad, topPad);
            var clipMax = pos + new Vector2(seatW - rightPad, seatH - bottomPad);
            dl.PushClipRect(clipMin, clipMax, true);

            var textPos = pos + new Vector2(leftPad, topPad);

            float nameMaxW = (seatW - leftPad - rightPad) - tagW;
            string name = Trunc(pl.Name ?? "", Math.Max(40f * scale, nameMaxW));

            dl.AddText(textPos,
                ImGui.GetColorU32(isMe ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudGrey),
                name);

            if (!string.IsNullOrEmpty(tag))
            {
                var tagPos = new Vector2(pos.X + seatW - rightPad - ImGui.CalcTextSize(tag).X, textPos.Y);
                var col = pl.Folded ? ImGuiColors.DalamudRed : ImGuiColors.ParsedOrange;
                dl.AddText(tagPos, ImGui.GetColorU32(col), tag);
            }

            var line2 = textPos + new Vector2(0, lineH + lineGap);
            dl.AddText(line2, ImGui.GetColorU32(ImGuiColors.DalamudGrey), $"Bet: {pl.CommittedThisStreet}");

            var line3 = line2 + new Vector2(0, lineH + lineGap);
            dl.AddText(line3, ImGui.GetColorU32(ImGuiColors.DalamudGrey), $"Chips: {pl.Stack}");

            dl.PopClipRect();

            bool hasHole = revealLookup.TryGetValue(pl.SessionId, out var hole) && hole != null && hole.Count == 2;
            bool shouldShowReveals = (pub.Stage == PokerStage.Showdown || pub.Stage == PokerStage.RoundOver) && hasHole;

            if (shouldShowReveals)
            {
                float miniH = Math.Clamp(seatH * 1.15f, 90f * scale, 124f * scale);
                float miniW = miniH * (2f / 3f);
                float overlap = miniW * 0.35f;

                float totalW = miniW + (miniW - overlap);

                float y = pos.Y - miniH - (8f * scale);
                if (y < p0.Y + pad) y = pos.Y + seatH + (8f * scale);

                float x = pos.X + (seatW - totalW) / 2f;

                var c1 = new Vector2(x, y);
                var c2 = new Vector2(x + (miniW - overlap), y);

                DrawSingleCardAt(dl, c1, new Vector2(miniW, miniH), hole![0], false);
                DrawSingleCardAt(dl, c2, new Vector2(miniW, miniH), hole![1], false);
            }
        }
    }

private float ComputeBoardCardHeightResponsive(float availX, float boardAreaY, float scale)
    {
        // width-driven sizing so board always fits on different resolutions
        const float aspect = 2f / 3f; // cardW = H * aspect
        float spacing = 10f * scale;

        float widthLimitedH = (availX - 4 * spacing) / (5f * aspect);

        float heightLimitedH = boardAreaY;

        float h = Math.Min(widthLimitedH, heightLimitedH);

        return Math.Clamp(h, 190f * scale, 320f * scale);
    }

private static string ResolvePokerName(PokerStatePublic pub, string sessionId)
    {
        if (string.IsNullOrEmpty(sessionId)) return "-";
        return pub.Players.FirstOrDefault(p => string.Equals(p.SessionId, sessionId, StringComparison.Ordinal))?.Name
            ?? sessionId;
    }

    private static string PokerPhaseLabel(PokerStage stage) => stage switch
    {
        PokerStage.Lobby => "Waiting for players",
        PokerStage.Preflop => "Betting (before table cards)",
        PokerStage.Flop => "Betting (after 3 table cards)",
        PokerStage.Turn => "Betting (after 4 table cards)",
        PokerStage.River => "Betting (after 5 table cards)",
        PokerStage.Showdown => "Reveal hands",
        PokerStage.RoundOver => "Hand complete",
        _ => stage.ToString()
    };

    private static string PokerPhaseShort(PokerStage stage) => stage switch
    {
        PokerStage.Lobby => "Lobby",
        PokerStage.Preflop => "Before table cards",
        PokerStage.Flop => "3 table cards",
        PokerStage.Turn => "4 table cards",
        PokerStage.River => "5 table cards",
        PokerStage.Showdown => "Reveal",
        PokerStage.RoundOver => "Done",
        _ => stage.ToString()
    };
}
