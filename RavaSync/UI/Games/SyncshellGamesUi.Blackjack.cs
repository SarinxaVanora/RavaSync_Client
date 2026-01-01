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
    private int _betAmount = 1000;

private void DrawBlackjack()
    {
        if (_activeGameId == Guid.Empty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No active blackjack session selected.");
            return;
        }

        if (!_games.TryGetClientBlackjack(_activeGameId, out var view))
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

        var stage = priv?.Stage ?? pub?.Stage ?? BjStage.Lobby;

        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale }))
        {
            ImGui.TextUnformatted("Blackjack");
            ImGui.SameLine();

            DrawPill("Stage: " + stage, stage == BjStage.Playing ? ImGuiColors.ParsedOrange : ImGuiColors.DalamudGrey);

            ImGui.SameLine();

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignOutAlt, "Leave", 120 * ImGuiHelpers.GlobalScale, true))
            {
                _games.LeaveBlackjack(_activeGameId);
                return;
            }
        }

        ImGui.Separator();

        using var bjLayoutTable = ImRaii.Table("bj_layout", 2, ImGuiTableFlags.SizingStretchProp);
        if (bjLayoutTable)
        {
            ImGui.TableSetupColumn("Left", ImGuiTableColumnFlags.WidthStretch, 0.62f);
            ImGui.TableSetupColumn("Right", ImGuiTableColumnFlags.WidthStretch, 0.38f);

            ImGui.TableNextRow();

            ImGui.TableNextColumn();
            using (ImRaii.Child("bj_left", new Vector2(0, 0), false))
            {
                DrawSectionHeader("Dealer");

                var dealerUp = priv?.DealerUpCard ?? pub?.DealerUpCard ?? (byte)0;

                if (pub != null && pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 0)
                {
                    DrawCardRow(pub.DealerCardsReveal.ToArray(), showAsHidden: false);

                    if (pub.Stage == BjStage.Results)
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "Total: " + pub.DealerTotalReveal);
                }
                else
                {
                    DrawCardRow([dealerUp], showAsHidden: false);
                }
                ImGuiHelpers.ScaledDummy(10);

                DrawSectionHeader("Table");

                if (pub != null)
                {
                    if (ImGui.BeginTable("bj_table_players", 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.SizingStretchProp))
                    {
                        ImGui.TableSetupColumn("Player", ImGuiTableColumnFlags.WidthStretch);
                        ImGui.TableSetupColumn("Bet", ImGuiTableColumnFlags.WidthFixed, 80 * ImGuiHelpers.GlobalScale);
                        ImGui.TableSetupColumn("State", ImGuiTableColumnFlags.WidthFixed, 90 * ImGuiHelpers.GlobalScale);
                        ImGui.TableSetupColumn("Ready", ImGuiTableColumnFlags.WidthFixed, 70 * ImGuiHelpers.GlobalScale);
                        ImGui.TableHeadersRow();

                        foreach (var p in pub.Players)
                        {
                            string state = p.Bust ? "BUST" : (p.Done ? "DONE" : "PLAYING");

                            ImGui.TableNextRow();

                            ImGui.TableNextColumn();
                            bool isMe = p.SessionId == view.MySessionId;
                            if (isMe) ImGui.TextColored(ImGuiColors.ParsedGreen, p.Name);
                            else ImGui.TextUnformatted(p.Name);

                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(p.Bet.ToString());

                            ImGui.TableNextColumn();
                            if (p.Bust) ImGui.TextColored(ImGuiColors.DalamudRed, state);
                            else if (p.Done) ImGui.TextColored(ImGuiColors.ParsedGreen, state);
                            else ImGui.TextColored(ImGuiColors.DalamudGrey, state);

                            ImGui.TableNextColumn();
                            if (p.BetConfirmed) ImGui.TextColored(ImGuiColors.ParsedGreen, "Yes");
                            else ImGui.TextColored(ImGuiColors.DalamudGrey, "No");
                        }

                        ImGui.EndTable();
                    }
                }
                else
                {
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "No table state yet.");
                }

                if (pub != null && pub.Stage == BjStage.Results && pub.Results != null)
                {
                    ImGuiHelpers.ScaledDummy(10);
                    DrawSectionHeader("Results");

                    if (ImGui.BeginTable("bj_results", 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.SizingStretchProp))
                    {
                        ImGui.TableSetupColumn("Player", ImGuiTableColumnFlags.WidthStretch);
                        ImGui.TableSetupColumn("Outcome", ImGuiTableColumnFlags.WidthFixed, 90 * ImGuiHelpers.GlobalScale);
                        ImGui.TableSetupColumn("Bet", ImGuiTableColumnFlags.WidthFixed, 80 * ImGuiHelpers.GlobalScale);
                        ImGui.TableSetupColumn("Payout", ImGuiTableColumnFlags.WidthFixed, 90 * ImGuiHelpers.GlobalScale);
                        ImGui.TableHeadersRow();

                        foreach (var r in pub.Results)
                        {
                            ImGui.TableNextRow();

                            ImGui.TableNextColumn();
                            bool isMe = r.SessionId == view.MySessionId;
                            if (isMe) ImGui.TextColored(ImGuiColors.ParsedGreen, r.Name);
                            else ImGui.TextUnformatted(r.Name);

                            ImGui.TableNextColumn();
                            var col = OutcomeColor(r.Outcome.ToString());
                            ImGui.TextColored(col, r.Outcome.ToString());

                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(r.Bet.ToString());

                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(r.Payout.ToString());
                        }

                        ImGui.EndTable();
                    }
                }
            }

            ImGui.TableNextColumn();
            using (ImRaii.Child("bj_right", new Vector2(0, 0), true))
            {
                bool isHost = pub != null && string.Equals(view.MySessionId, pub.HostSessionId, StringComparison.Ordinal);

                if (isHost)
                {
                    DrawSectionHeader("Host");

                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Dealer controls");
                    ImGuiHelpers.ScaledDummy(8);

                    bool canStartBetting = stage == BjStage.Lobby || stage == BjStage.Results;
                    bool anyPlayers = pub != null && pub.Players.Any(p => p.SessionId != pub.HostSessionId);

                    bool allReady = pub != null && pub.Players
                        .Where(p => p.SessionId != pub.HostSessionId)
                        .All(p => p.BetConfirmed);

                    bool canDealAndPlay = stage == BjStage.Betting && anyPlayers && allReady;
                    bool canNextRound = stage == BjStage.Results;

                    using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale }))
                    {
                        using (ImRaii.Disabled(!canStartBetting))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Coins, "Start Betting", -1, true))
                                _games.HostBlackjackStartBetting(_activeGameId);
                        }

                        ImGuiHelpers.ScaledDummy(6);

                        using (ImRaii.Disabled(!canDealAndPlay))
                        {
                            if (stage == BjStage.Betting && pub != null && !allReady)
                            {
                                var waiting = pub.Players
                                    .Where(p => p.SessionId != pub.HostSessionId && !p.BetConfirmed)
                                    .Select(p => p.Name)
                                    .ToArray();

                                if (waiting.Length > 0)
                                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting on: " + string.Join(", ", waiting));
                            }
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, "Deal & Play", -1, true))
                                _games.HostBlackjackDealAndPlay(_activeGameId);
                        }

                        ImGuiHelpers.ScaledDummy(6);

                        bool dealerTurn = pub != null && stage == BjStage.Playing && string.IsNullOrEmpty(pub.CurrentTurnSessionId);

                        ImGuiHelpers.ScaledDummy(6);

                        using (ImRaii.Disabled(!dealerTurn))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Magic, "Draw dealer card", -1, true))
                                _games.HostBlackjackDealerDraw(_activeGameId);
                        }

                        ImGuiHelpers.ScaledDummy(6);

                        using (ImRaii.Disabled(!canNextRound))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.StepForward, "Next Round", -1, true))
                                _games.HostBlackjackNextRound(_activeGameId);
                        }
                    }

                    ImGuiHelpers.ScaledDummy(10);

                    DrawSectionHeader("Status");
                    ImGui.TextColored(ImGuiColors.DalamudGrey, $"Stage: {stage}");

                    return; 
                }

                DrawSectionHeader("You");

                if (priv != null && priv.YourCards.Count > 0)
                {
                    DrawCardRow(priv.YourCards.ToArray(), showAsHidden: false);

                    if (priv.YourBust)
                        ImGui.TextColored(ImGuiColors.DalamudRed, "BUST (" + priv.YourTotal + ")");
                    else
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "Total: " + priv.YourTotal + (priv.IsYourTurn ? "  |  Your turn" : ""));
                }
                else
                {
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "No cards yet.");
                }

                ImGuiHelpers.ScaledDummy(10);

                if (stage == BjStage.Betting)
                {
                    DrawSectionHeader("Betting");

                    ImGui.SetNextItemWidth(-1);
                    ImGui.InputInt("##BetAmount", ref _betAmount);

                    if (pub != null)
                    {
                        var me = pub.Players.FirstOrDefault(x => x.SessionId == view.MySessionId);
                        if (me != null)
                            ImGui.TextColored(me.BetConfirmed ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudGrey,
                                "Your bet: " + me.Bet + (me.BetConfirmed ? " (confirmed)" : ""));
                    }

                    ImGuiHelpers.ScaledDummy(6);

                    using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale }))
                    {
                        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, "Confirm bet and play", -1, true))
                            _games.BlackjackConfirmBet(_activeGameId, Math.Max(0, _betAmount));
                    }

                    return;
                }

                if (stage == BjStage.Playing && priv != null)
                {
                    DrawSectionHeader("Actions");

                    if (priv.IsYourTurn)
                    {
                        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 10f * ImGuiHelpers.GlobalScale }))
                        {
                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Plus, "Hit", -1, true))
                                _games.BlackjackAction(_activeGameId, BjActionKind.Hit);

                            ImGuiHelpers.ScaledDummy(6);

                            if (_uiSharedService.IconTextButton(FontAwesomeIcon.HandPaper, "Stick", -1, true))
                                _games.BlackjackAction(_activeGameId, BjActionKind.Stick);
                        }
                    }
                    else
                    {
                        ImGui.TextColored(ImGuiColors.DalamudGrey, "Waiting for your turn…");
                    }
                }

                if (stage == BjStage.Results)
                {
                    DrawSectionHeader("Status");
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Round complete. Host will start the next one.");
                }
            }
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

private static Vector4 OutcomeColor(string outcome)
    {
        if (string.Equals(outcome, "Win", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedGreen;
        if (string.Equals(outcome, "Push", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedOrange;
        return ImGuiColors.DalamudRed;
    }
}
