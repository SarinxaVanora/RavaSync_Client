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

public sealed partial class ToyBoxUi
{
    private int _betAmount = 1000;

    private void DrawBlackjack()
    {
        if (_activeGameId == Guid.Empty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.304ad345", "No active blackjack session selected."));
            return;
        }

        if (!_games.TryGetClientBlackjack(_activeGameId, out var view))
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.31e6b175", "Waiting for state…"));
            return;
        }

        var pub = view.Public;
        var priv = view.Private;

        if (pub is null && priv is null)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.9d36327a", "Waiting for host…"));
            return;
        }

        float scale = ImGuiHelpers.GlobalScale;
        var stage = priv?.Stage ?? pub?.Stage ?? BjStage.Lobby;
        bool isHost = pub != null && string.Equals(view.MySessionId, pub.HostSessionId, StringComparison.Ordinal);

        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * scale }))
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.ToyBoxUiBlackjack.3f468208", "Blackjack"));
            ImGui.SameLine();

            DrawPill("Stage: " + BlackjackStageLabel(stage), stage == BjStage.Playing ? ImGuiColors.ParsedOrange : ImGuiColors.DalamudGrey);

            if (pub != null && !string.IsNullOrEmpty(pub.CurrentTurnSessionId))
            {
                ImGui.SameLine();
                bool isMyTurn = string.Equals(pub.CurrentTurnSessionId, view.MySessionId, StringComparison.Ordinal);
                string turnName = isMyTurn ? "You" : ResolveBlackjackName(pub, pub.CurrentTurnSessionId);
                DrawPill("Turn: " + turnName, isMyTurn ? ImGuiColors.ParsedGreen : ImGuiColors.ParsedOrange);
            }
            else if (pub != null && stage == BjStage.Playing)
            {
                ImGui.SameLine();
                DrawPill("Turn: Dealer", ImGuiColors.ParsedOrange);
            }

            ImGui.SameLine();
            var label = isHost ? "Close Lobby" : "Leave";

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignOutAlt, label, 120 * scale, true))
            {
                if (isHost) _games.CloseBJLobby(_activeGameId);
                else _games.LeaveBlackjack(_activeGameId);

                _activeGameId = Guid.Empty;
                return;
            }
        }

        ImGui.Separator();

        float sideW = 380f * scale;

        if (ImGui.BeginTable(_uiSharedService.L("UI.ToyBoxUiBlackjack.2ec86f25", "bj_open_layout"), 2, ImGuiTableFlags.SizingStretchProp))
        {
            try
            {
                ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.0424f6e7", "Table"), ImGuiTableColumnFlags.WidthStretch);
                ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.26528381", "Side"), ImGuiTableColumnFlags.WidthFixed, sideW);

                ImGui.TableNextRow();

                ImGui.TableNextColumn();
                using (ImRaii.Child("bj_felt_outer", new Vector2(0, 0), false))
                {
                    using (ImRaii.Child("bj_felt", new Vector2(0, 0), true, ImGuiWindowFlags.NoScrollbar))
                    {
                        if (pub != null)
                            DrawBlackjackTable(pub, view, stage);
                        else
                            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.44fc9136", "No table state yet."));
                    }
                }

                ImGui.TableNextColumn();
                using (ImRaii.Child("bj_side", new Vector2(0, 0), true))
                {
                    if (isHost)
                        DrawBlackjackHostPanel(pub, priv, view, stage);
                    else
                        DrawBlackjackPlayerPanel(pub, priv, view, stage);
                }
            }
            finally
            {
                ImGui.EndTable();
            }
        }
    }

    private void DrawBlackjackTable(BjStatePublic pub, ToyBox.BlackjackClientView view, BjStage stage)
    {
        float scale = ImGuiHelpers.GlobalScale;
        var dl = ImGui.GetWindowDrawList();
        var p0 = ImGui.GetWindowPos();
        var sz = ImGui.GetWindowSize();
        var p1 = p0 + sz;

        dl.AddRectFilled(p0, p1, ImGui.GetColorU32(new Vector4(0.05f, 0.10f, 0.08f, 0.75f)), 18f * scale);
        dl.AddRect(p0, p1, ImGui.GetColorU32(new Vector4(0.60f, 0.60f, 0.70f, 0.30f)), 18f * scale, 0, 2f);

        float availX = ImGui.GetContentRegionAvail().X;
        float availY = ImGui.GetContentRegionAvail().Y;

        List<byte> dealerCards = [];
        if (pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 0)
            dealerCards = pub.DealerCardsReveal;
        else if (stage == BjStage.Playing || stage == BjStage.Results)
            dealerCards = [pub.DealerUpCard];

        float dealerCardH = ComputeBlackjackDealerCardHeight(availX, availY, scale, dealerCards.Count);
        float dealerCardW = dealerCardH * (2f / 3f);
        float dealerStep = dealerCards.Count <= 1 ? dealerCardW : Math.Clamp(dealerCardW * 0.72f, 54f * scale, dealerCardW + 10f * scale);
        float dealerTotalW = dealerCards.Count <= 0 ? 0f : dealerCardW + (dealerCards.Count - 1) * dealerStep;

        float dealerY = Math.Max(86f * scale, (availY - dealerCardH) * 0.46f);
        ImGui.SetCursorPos(new Vector2(Math.Max(0, (availX - dealerTotalW) * 0.5f), dealerY));
        var dealerTop = ImGui.GetCursorScreenPos();

        if (dealerCards.Count > 0)
        {
            var cardSize = new Vector2(dealerCardW, dealerCardH);
            for (int i = 0; i < dealerCards.Count; i++)
            {
                var cardPos = dealerTop + new Vector2(i * dealerStep, 0);
                DrawSingleCardAt(dl, cardPos, cardSize, dealerCards[i], false);
            }
            ImGui.Dummy(new Vector2(dealerTotalW, dealerCardH));

            int dealerTotal = BlackjackTotal(dealerCards);
            string dealerText = pub.Stage == BjStage.Results || (pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 1)
                ? $"Dealer  •  Total: {dealerTotal}"
                : "Dealer";

            var ts = ImGui.CalcTextSize(dealerText);
            dl.AddText(new Vector2(p0.X + (sz.X - ts.X) * 0.5f, dealerTop.Y - ts.Y - 8f * scale), ImGui.GetColorU32(ImGuiColors.DalamudGrey), dealerText);
        }
        else
        {
            string waiting = stage == BjStage.Lobby ? "Waiting for players" : "Waiting for deal";
            var ts = ImGui.CalcTextSize(waiting);
            dl.AddText(new Vector2(p0.X + (sz.X - ts.X) * 0.5f, p0.Y + sz.Y * 0.48f), ImGui.GetColorU32(ImGuiColors.DalamudGrey), waiting);
        }

        DrawBlackjackSeatsOverlay(ImGui.GetForegroundDrawList(), p0, sz, pub, view.MySessionId, dealerTop.Y, dealerTop.Y + Math.Max(dealerCardH, 1f));

        if (pub.Stage == BjStage.Results && pub.Results != null && pub.Results.Count > 0)
            DrawBlackjackResultsOverlay(dl, p0, sz, pub);
    }

    private float ComputeBlackjackDealerCardHeight(float availX, float availY, float scale, int cardCount)
    {
        const float aspect = 2f / 3f;
        int count = Math.Max(1, cardCount);
        float stepFactor = count <= 1 ? 1f : 0.72f;
        float widthUnits = aspect * (1f + (count - 1) * stepFactor);
        float widthLimitedH = availX / Math.Max(widthUnits, 0.01f);
        float heightLimitedH = availY * 0.34f;
        return Math.Clamp(Math.Min(widthLimitedH, heightLimitedH), 150f * scale, 270f * scale);
    }

    private void DrawBlackjackSeatsOverlay(ImDrawListPtr dl, Vector2 p0, Vector2 size, BjStatePublic pub, string mySessionId, float dealerBandTop, float dealerBandBottom)
    {
        float scale = ImGuiHelpers.GlobalScale;
        var players = pub.Players ?? [];
        if (players.Count == 0) return;

        float pad = 16f * scale;
        float topEdgeY = p0.Y + pad;
        float bottomEdgeY = p0.Y + size.Y - pad;

        float lineH = ImGui.GetTextLineHeight();
        float topPad = 10f * scale;
        float bottomPad = 10f * scale;
        float lineGap = 4f * scale;
        float seatH = Math.Clamp(topPad + bottomPad + lineH * 3f + lineGap * 2f, 78f * scale, 108f * scale);

        float leftPad = 12f * scale;
        float rightPad = 12f * scale;

        float RequiredSeatW(BjPlayerPublic pl)
        {
            var l1 = pl.Name ?? "";
            var l2 = $"Bet: {pl.Bet}";
            var l3 = BlackjackPlayerState(pl);
            float w = Math.Max(ImGui.CalcTextSize(l1).X, Math.Max(ImGui.CalcTextSize(l2).X, ImGui.CalcTextSize(l3).X));
            return w + leftPad + rightPad + 18f * scale;
        }

        float seatW = Math.Max(size.X * 0.24f, players.Take(8).Max(RequiredSeatW));
        seatW = Math.Clamp(seatW, 220f * scale, 340f * scale);
        if (players.Count >= 6)
            seatW = Math.Clamp(seatW * 0.92f, 205f * scale, 318f * scale);

        var seatSize = new Vector2(seatW, seatH);
        float leftX = p0.X + pad;
        float rightX = p0.X + size.X - seatW - pad;
        float centerX = p0.X + (size.X - seatW) / 2f;
        float topY = topEdgeY;
        float bottomY = Math.Min(bottomEdgeY - seatH, p0.Y + size.Y - seatH - pad);
        float midY = p0.Y + (size.Y - seatH) / 2f;

        float FitY(float y)
        {
            if (y + seatH < dealerBandTop - pad) return y;
            if (y > dealerBandBottom + pad) return y;

            float up = Math.Max(topEdgeY, dealerBandTop - seatH - pad);
            float down = Math.Min(bottomEdgeY - seatH, dealerBandBottom + pad);
            return Math.Abs(y - up) < Math.Abs(down - y) ? up : down;
        }

        List<Vector2> seats;
        if (players.Count <= 2)
        {
            seats = [new(centerX, bottomY), new(centerX, topY)];
        }
        else if (players.Count == 3)
        {
            seats = [new(centerX, bottomY), new(leftX, FitY(midY)), new(rightX, FitY(midY))];
        }
        else if (players.Count == 4)
        {
            seats = [new(centerX, bottomY), new(leftX, FitY(midY)), new(centerX, topY), new(rightX, FitY(midY))];
        }
        else
        {
            seats =
            [
                new(centerX, bottomY),
                new(leftX, FitY(bottomY - seatH * 0.15f)),
                new(rightX, FitY(bottomY - seatH * 0.15f)),
                new(centerX, topY),
                new(leftX, FitY(topY + seatH * 0.15f)),
                new(rightX, FitY(topY + seatH * 0.15f)),
                new(leftX, FitY(midY)),
                new(rightX, FitY(midY)),
            ];
        }

        static string Trunc(string s, float maxW)
        {
            if (string.IsNullOrEmpty(s)) return s;
            if (ImGui.CalcTextSize(s).X <= maxW) return s;

            const string ell = "…";
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
            else if (pl.Bust)
                border = ImGui.GetColorU32(new Vector4(0.95f, 0.25f, 0.25f, 0.55f));
            else if (pl.Done)
                border = ImGui.GetColorU32(new Vector4(0.35f, 0.95f, 0.45f, 0.40f));

            dl.AddRectFilled(pos, pos + seatSize, bg, 12f * scale);
            dl.AddRect(pos, pos + seatSize, border, 12f * scale, 0, 2f);

            var cards = pl.Cards ?? [];
            if (cards.Count > 0)
            {
                float cardH = Math.Clamp(seatH * 1.12f, 86f * scale, 122f * scale);
                float cardW = cardH * (2f / 3f);
                float overlap = cardW * 0.42f;
                float step = Math.Max(18f * scale, cardW - overlap);
                float totalW = cardW + (cards.Count - 1) * step;

                float y = pos.Y - cardH - 8f * scale;
                if (y < p0.Y + pad) y = pos.Y + seatH + 8f * scale;

                float x = pos.X + (seatW - totalW) / 2f;
                x = Math.Clamp(x, p0.X + pad, p0.X + size.X - totalW - pad);

                byte? hoveredCard = null;
                Vector2 hoveredPos = default;
                var cardSize = new Vector2(cardW, cardH);

                for (int c = 0; c < cards.Count; c++)
                {
                    var cardPos = new Vector2(x + c * step, y);
                    DrawSingleCardAt(dl, cardPos, cardSize, cards[c], false, hoverGrow: 1f);

                    if (ImGui.IsMouseHoveringRect(cardPos, cardPos + cardSize))
                    {
                        hoveredCard = cards[c];
                        hoveredPos = cardPos;
                    }
                }

                if (hoveredCard.HasValue)
                    DrawHoveredCardOverlay(hoveredPos, cardSize, hoveredCard.Value, 2.85f);
            }

            string state = BlackjackPlayerState(pl);
            string totalLine = cards.Count > 0 ? $"Total: {pl.Total}" : (pl.BetConfirmed ? "Waiting for deal" : "Waiting for bet");

            var clipMin = pos + new Vector2(leftPad, topPad);
            var clipMax = pos + new Vector2(seatW - rightPad, seatH - bottomPad);
            dl.PushClipRect(clipMin, clipMax, true);

            var textPos = pos + new Vector2(leftPad, topPad);
            string name = Trunc(pl.Name ?? "", Math.Max(40f * scale, seatW - leftPad - rightPad));
            dl.AddText(textPos, ImGui.GetColorU32(isMe ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudGrey), name);

            var line2 = textPos + new Vector2(0, lineH + lineGap);
            dl.AddText(line2, ImGui.GetColorU32(ImGuiColors.DalamudGrey), $"Bet: {pl.Bet}");

            var line3 = line2 + new Vector2(0, lineH + lineGap);
            Vector4 stateCol = pl.Bust ? ImGuiColors.DalamudRed : (pl.Done ? ImGuiColors.ParsedGreen : (isTurn ? ImGuiColors.ParsedOrange : ImGuiColors.DalamudGrey));
            dl.AddText(line3, ImGui.GetColorU32(stateCol), cards.Count > 0 ? $"{state}  •  {totalLine}" : totalLine);

            dl.PopClipRect();
        }
    }

    private void DrawBlackjackResultsOverlay(ImDrawListPtr dl, Vector2 p0, Vector2 size, BjStatePublic pub)
    {
        if (pub.Results == null || pub.Results.Count == 0) return;

        float scale = ImGuiHelpers.GlobalScale;
        var winners = pub.Results
            .Where(r => r.Outcome == BjOutcome.Win)
            .OrderByDescending(r => r.Payout)
            .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        string line1;
        string line2;
        Vector4 line1Color;
        Vector4 line2Color;

        if (winners.Count > 0)
        {
            var top = winners[0];
            line1 = top.Name;
            line2 = $"won {top.Payout}";
            line1Color = ImGuiColors.ParsedGreen;
            line2Color = ImGuiColors.ParsedGreen;
        }
        else if (BlackjackDealerWon(pub))
        {
            line1 = "Dealer / Table";
            line2 = "wins the round";
            line1Color = ImGuiColors.ParsedOrange;
            line2Color = ImGuiColors.ParsedOrange;
        }
        else
        {
            line1 = "Push";
            line2 = "No winner";
            line1Color = ImGuiColors.DalamudGrey;
            line2Color = ImGuiColors.DalamudGrey;
        }

        string line3 = "Round complete";

        Vector2 t1 = ImGui.CalcTextSize(line1);
        Vector2 t2 = ImGui.CalcTextSize(line2);
        Vector2 t3 = ImGui.CalcTextSize(line3);

        float pad = 16f * scale;
        float w = MathF.Max(t1.X, MathF.Max(t2.X, t3.X)) + pad * 2f;
        float h = t1.Y + t2.Y + t3.Y + pad * 2.4f;
        var center = new Vector2(p0.X + size.X * 0.5f, p0.Y + size.Y * 0.5f);
        var rMin = center - new Vector2(w, h) * 0.5f;
        var rMax = center + new Vector2(w, h) * 0.5f;

        dl.AddRectFilled(rMin, rMax, ImGui.GetColorU32(new Vector4(0, 0, 0, 0.68f)), 16f * scale);
        dl.AddRect(rMin, rMax, ImGui.GetColorU32(new Vector4(1, 1, 1, 0.20f)), 16f * scale, 0, 2f);

        float y = rMin.Y + pad;
        dl.AddText(new Vector2(center.X - t1.X * 0.5f, y), ImGui.GetColorU32(line1Color), line1);
        y += t1.Y + 6f * scale;
        dl.AddText(new Vector2(center.X - t2.X * 0.5f, y), ImGui.GetColorU32(line2Color), line2);
        y += t2.Y + 4f * scale;
        dl.AddText(new Vector2(center.X - t3.X * 0.5f, y), ImGui.GetColorU32(ImGuiColors.DalamudGrey), line3);
    }

    private static bool BlackjackDealerWon(BjStatePublic pub)
    {
        var results = pub.Results ?? [];
        return results.Count > 0 && results.Any(r => r.Outcome == BjOutcome.Lose || r.Outcome == BjOutcome.Bust) && results.All(r => r.Outcome != BjOutcome.Win);
    }

    private void DrawBlackjackHostPanel(BjStatePublic? pub, BjStatePrivate? priv, ToyBox.BlackjackClientView view, BjStage stage)
    {
        DrawSectionHeader("Dealer / Table");

        if (pub != null)
        {
            List<byte> dealerCards = [];
            if (pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 0)
                dealerCards = pub.DealerCardsReveal;
            else if ((pub.Stage == BjStage.Playing || pub.Stage == BjStage.Results) && pub.DealerUpCard != 0)
                dealerCards = [pub.DealerUpCard];

            if (dealerCards.Count > 0)
            {
                DrawCardRowInlineSized(dealerCards.ToArray(), showAsHidden: false, cardH: 170f * ImGuiHelpers.GlobalScale);

                bool totalVisible = pub.Stage == BjStage.Results || (pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 1);
                if (totalVisible)
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Dealer total: " + BlackjackTotal(dealerCards));
                else
                    ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.63cabf52", "Dealer up card shown."));
            }
            else
            {
                ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.93ab21bd", "No cards yet."));
            }
        }
        else
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.44fc9136", "No table state yet."));
        }

        ImGuiHelpers.ScaledDummy(12);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(8);

        DrawSectionHeader("Host");

        ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.1a7327d6", "Dealer controls"));
        ImGuiHelpers.ScaledDummy(8);

        bool canStartBetting = stage == BjStage.Lobby || stage == BjStage.Results;
        bool anyPlayers = pub != null && pub.Players.Count > 0;
        bool allReady = anyPlayers && pub != null && pub.Players.All(p => p.BetConfirmed);
        bool canDealAndPlay = stage == BjStage.Betting && anyPlayers && allReady;
        bool dealerTurn = pub != null && stage == BjStage.Playing && string.IsNullOrEmpty(pub.CurrentTurnSessionId);
        bool canNextRound = stage == BjStage.Results;

        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale, Y = 6f * ImGuiHelpers.GlobalScale }))
        {
            using (ImRaii.Disabled(!canStartBetting))
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Coins, _uiSharedService.L("UI.ToyBoxUi.Blackjack.ed509afe", "Start Betting"), -1, true))
                    _games.HostBlackjackStartBetting(_activeGameId);
            }

            using (ImRaii.Disabled(!canDealAndPlay))
            {
                if (stage == BjStage.Betting && pub != null)
                {
                    if (!anyPlayers)
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.777d81b8", "Waiting for players."));
                    }
                    else if (!allReady)
                    {
                        var waiting = pub.Players.Where(p => !p.BetConfirmed).Select(p => p.Name).ToArray();
                        if (waiting.Length > 0)
                            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.cc277797", "Waiting on: ") + string.Join(", ", waiting));
                    }
                }

                if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, _uiSharedService.L("UI.ToyBoxUi.Blackjack.1a36d28a", "Deal & Play"), -1, true))
                    _games.HostBlackjackDealAndPlay(_activeGameId);
            }

            using (ImRaii.Disabled(!dealerTurn))
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Magic, _uiSharedService.L("UI.ToyBoxUi.Blackjack.b7f0120b", "Draw dealer card"), -1, true))
                    _games.HostBlackjackDealerDraw(_activeGameId);
            }

            using (ImRaii.Disabled(!canNextRound))
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StepForward, _uiSharedService.L("UI.ToyBoxUi.Blackjack.b8cb97f5", "Next Round"), -1, true))
                    _games.HostBlackjackNextRound(_activeGameId);
            }
        }

        ImGuiHelpers.ScaledDummy(10);
        DrawSectionHeader("Status");
        ImGui.TextColored(ImGuiColors.DalamudGrey, $"Stage: {BlackjackStageLabel(stage)}");
        if (pub != null)
            ImGui.TextColored(ImGuiColors.DalamudGrey, "Players at table: " + pub.Players.Count);

        if (pub != null && pub.Stage == BjStage.Results && pub.Results != null && pub.Results.Count > 0)
        {
            ImGuiHelpers.ScaledDummy(10);
            DrawBlackjackResultsTable(pub, pub.HostSessionId);
        }
    }

    private void DrawBlackjackPlayerPanel(BjStatePublic? pub, BjStatePrivate? priv, ToyBox.BlackjackClientView view, BjStage stage, bool showResults = true)
    {
        DrawSectionHeader("You");

        var publicMe = pub?.Players.FirstOrDefault(x => string.Equals(x.SessionId, view.MySessionId, StringComparison.Ordinal));
        var visibleCards = publicMe?.Cards;
        if ((visibleCards == null || visibleCards.Count == 0) && priv != null && priv.YourCards.Count > 0)
            visibleCards = priv.YourCards;

        if (visibleCards != null && visibleCards.Count > 0)
        {
            DrawCardRowInlineSized(visibleCards.ToArray(), showAsHidden: false, cardH: 220f * ImGuiHelpers.GlobalScale);
            int total = publicMe?.Total ?? priv?.YourTotal ?? BlackjackTotal(visibleCards);
            bool bust = publicMe?.Bust ?? priv?.YourBust ?? total > 21;

            if (bust)
                ImGui.TextColored(ImGuiColors.DalamudRed, _uiSharedService.L("UI.ToyBoxUi.Blackjack.f4152ff0", "BUST (") + total + ")");
            else
                ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.445185d3", "Total: ") + total + (priv != null && priv.IsYourTurn ? _uiSharedService.L("UI.ToyBoxUi.Blackjack.234979c8", "  |  Your turn") : ""));
        }
        else
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.93ab21bd", "No cards yet."));
        }

        ImGuiHelpers.ScaledDummy(10);

        if (stage == BjStage.Betting)
        {
            DrawSectionHeader("Betting");

            ImGui.SetNextItemWidth(-1);
            ImGui.InputInt("##BetAmount", ref _betAmount);

            if (publicMe != null)
                ImGui.TextColored(publicMe.BetConfirmed ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudGrey, "Your bet: " + publicMe.Bet + (publicMe.BetConfirmed ? " (confirmed)" : ""));

            ImGuiHelpers.ScaledDummy(6);

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, _uiSharedService.L("UI.ToyBoxUi.Blackjack.1e7acc43", "Confirm bet and play"), -1, true))
                _games.BlackjackConfirmBet(_activeGameId, Math.Max(0, _betAmount));

            return;
        }

        if (stage == BjStage.Playing && priv != null)
        {
            DrawSectionHeader("Actions");

            if (priv.IsYourTurn)
            {
                using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale, Y = 6f * ImGuiHelpers.GlobalScale }))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Plus, _uiSharedService.L("UI.ToyBoxUi.Blackjack.b139714b", "Hit"), -1, true))
                        _games.BlackjackAction(_activeGameId, BjActionKind.Hit);

                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.HandPaper, _uiSharedService.L("UI.ToyBoxUi.Blackjack.b0247e6a", "Stick"), -1, true))
                        _games.BlackjackAction(_activeGameId, BjActionKind.Stick);
                }
            }
            else
            {
                ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.bda02835", "Waiting for your turn…"));
            }
        }

        if (showResults && stage == BjStage.Results)
        {
            DrawSectionHeader("Status");
            ImGui.TextColored(ImGuiColors.DalamudGrey, _uiSharedService.L("UI.ToyBoxUi.Blackjack.d99b875f", "Round complete. Host will start the next one."));

            if (pub != null && pub.Results != null && pub.Results.Count > 0)
            {
                ImGuiHelpers.ScaledDummy(10);
                DrawBlackjackResultsTable(pub, view.MySessionId);
            }
        }
    }

    private void DrawBlackjackResultsTable(BjStatePublic pub, string mySessionId)
    {
        DrawSectionHeader("Results");

        if (ImGui.BeginTable(_uiSharedService.L("UI.ToyBoxUiBlackjack.bcb389bc", "bj_results"), 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.SizingStretchProp))
        {
            ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.e53407cf", "Player"), ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.d3f06106", "Outcome"), ImGuiTableColumnFlags.WidthFixed, 82 * ImGuiHelpers.GlobalScale);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.40880286", "Bet"), ImGuiTableColumnFlags.WidthFixed, 72 * ImGuiHelpers.GlobalScale);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.ToyBoxUiBlackjack.9bb39694", "Payout"), ImGuiTableColumnFlags.WidthFixed, 78 * ImGuiHelpers.GlobalScale);
            ImGui.TableHeadersRow();

            if (BlackjackDealerWon(pub))
            {
                ImGui.TableNextRow();

                ImGui.TableNextColumn();
                ImGui.TextColored(ImGuiColors.ParsedOrange, "Dealer / Table");

                ImGui.TableNextColumn();
                ImGui.TextColored(ImGuiColors.ParsedOrange, "Win");

                ImGui.TableNextColumn();
                ImGui.TextUnformatted("-");

                ImGui.TableNextColumn();
                ImGui.TextUnformatted("-");
            }

            foreach (var r in pub.Results ?? [])
            {
                ImGui.TableNextRow();

                ImGui.TableNextColumn();
                bool isMe = r.SessionId == mySessionId;
                if (isMe) ImGui.TextColored(ImGuiColors.ParsedGreen, r.Name);
                else ImGui.TextUnformatted(r.Name);

                ImGui.TableNextColumn();
                ImGui.TextColored(OutcomeColor(r.Outcome.ToString()), r.Outcome.ToString());

                ImGui.TableNextColumn();
                ImGui.TextUnformatted(r.Bet.ToString());

                ImGui.TableNextColumn();
                ImGui.TextUnformatted(r.Payout.ToString());
            }

            ImGui.EndTable();
        }
    }

    private void DrawCardRow(byte[] cards, bool showAsHidden)
    {
        if (cards == null || cards.Length == 0)
            return;
        DrawCardStack(cards, showAsHidden);
    }

    private void DrawCardStack(byte[] cards, bool showAsHidden)
    {
        EnsureCardTextures();

        float scale = ImGuiHelpers.GlobalScale;

        var avail = ImGui.GetContentRegionAvail();
        float targetH = avail.Y > 1 ? (avail.Y * 0.55f) : (360f * scale);
        float cardH = Math.Clamp(targetH, 280f * scale, 440f * scale);

        float cardW = cardH * (2f / 3f);
        float step = Math.Clamp(cardW * 0.33f, 34f * scale, 72f * scale);

        float maxWidth = avail.X > 1 ? avail.X : (cardW + step * (cards.Length - 1));
        if (cards.Length > 1)
        {
            float needed = cardW + step * (cards.Length - 1);
            if (needed > maxWidth)
            {
                float fitStep = (maxWidth - cardW) / (cards.Length - 1);
                step = Math.Clamp(fitStep, 10f * scale, step);
            }
        }

        var p = ImGui.GetCursorScreenPos();
        var dl = ImGui.GetWindowDrawList();

        float totalW = cardW + (cards.Length - 1) * step;
        var size = new Vector2(totalW, cardH);

        for (int i = 0; i < cards.Length; i++)
        {
            var cardPos = p + new Vector2(i * step, 0);
            DrawSingleCardAt(dl, cardPos, new Vector2(cardW, cardH), cards[i], showAsHidden);
        }

        ImGui.Dummy(size);
    }

    private static int BlackjackTotal(System.Collections.Generic.IReadOnlyList<byte> cards)
    {
        int total = 0;
        int aces = 0;

        for (int i = 0; i < cards.Count; i++)
        {
            var c = cards[i];
            int rank = c % 13;

            if (rank == 0) { total += 11; aces++; }
            else if (rank >= 10) total += 10;
            else total += rank + 1;
        }

        while (total > 21 && aces > 0)
        {
            total -= 10;
            aces--;
        }

        return total;
    }

    private static string ResolveBlackjackName(BjStatePublic pub, string sessionId)
    {
        if (string.IsNullOrEmpty(sessionId)) return "-";
        return pub.Players.FirstOrDefault(p => string.Equals(p.SessionId, sessionId, StringComparison.Ordinal))?.Name ?? sessionId;
    }

    private static string BlackjackStageLabel(BjStage stage) => stage switch
    {
        BjStage.Lobby => "Lobby",
        BjStage.Betting => "Betting",
        BjStage.Playing => "Playing",
        BjStage.Results => "Results",
        _ => stage.ToString()
    };

    private static string BlackjackPlayerState(BjPlayerPublic p)
    {
        if (p.Bust) return "BUST";
        if (p.Done) return "DONE";
        if (p.Cards != null && p.Cards.Count > 0) return "PLAYING";
        if (p.BetConfirmed) return "READY";
        return "WAITING";
    }

    private static Vector4 OutcomeColor(string outcome)
    {
        if (string.Equals(outcome, "Win", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedGreen;
        if (string.Equals(outcome, "Push", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedOrange;
        return ImGuiColors.DalamudRed;
    }
}
