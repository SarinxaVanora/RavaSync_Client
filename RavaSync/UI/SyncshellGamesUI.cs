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

public sealed class SyncshellGamesUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiSharedService;
    private readonly PairManager _pairManager;
    private readonly SyncshellGameService _games;

    private IDalamudTextureWrap? _cardAtlas;
    private IDalamudTextureWrap? _cardBack;
    private bool _cardTexturesTried = false;

    private int _hostGameKind = 0;
    private Guid _activeGameId = Guid.Empty;

    private int _betAmount = 1000;
    private bool _selectHostTab = false;
    private bool _selectBlackjackTab = false;

    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public GroupFullInfoDto GroupFullInfo { get; private set; }

    public SyncshellGamesUi(ILogger<SyncshellGamesUi> logger, MareMediator mediator,
        UiSharedService uiSharedService, PairManager pairManager, SyncshellGameService games,
        GroupFullInfoDto groupFullInfo, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "Syncshell Games (" + groupFullInfo.GroupAliasOrGID + ")", performanceCollectorService)
    {
        GroupFullInfo = groupFullInfo;
        _uiSharedService = uiSharedService;
        _pairManager = pairManager;
        _games = games;

        IsOpen = true;
        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new(820, 520),
            MaximumSize = new(1200, 1200),
        };
    }

    protected override void DrawInternal()
    {
        GroupFullInfo = _pairManager.Groups[GroupFullInfo.Group];

        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted("Syncshell Games");

        ImGui.Separator();

        using var tabs = ImRaii.TabBar("syncshell_games_tabs_" + GroupFullInfo.GID);
        if (!tabs) return;

        // Force-select Host tab right after hosting
        var hostFlags = _selectHostTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var hostTab = ImRaii.TabItem("Host", hostFlags))
        {
            if (hostTab)
                DrawHost();
        }

        using (var invitesTab = ImRaii.TabItem("Invites"))
        {
            if (invitesTab)
                DrawInvites();
        }

        // Force-select Blackjack tab right after accepting an invite/joining
        var bjFlags = _selectBlackjackTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var blackjackTab = ImRaii.TabItem("Blackjack", bjFlags))
        {
            if (blackjackTab)
                DrawBlackjack();
        }

        _selectHostTab = false;
        _selectBlackjackTab = false;
    }


    private void DrawHost()
    {
        string[] kinds = ["Blackjack", "Poker (soon)", "Bingo (soon)"];
        ImGui.SetNextItemWidth(250 * ImGuiHelpers.GlobalScale);
        ImGui.Combo("Game Type", ref _hostGameKind, kinds, kinds.Length);

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Play, "Host game", 200 * ImGuiHelpers.GlobalScale, true))
        {
            if (_hostGameKind == 0)
            {
                _activeGameId = _games.HostBlackjack(GroupFullInfo);
                if (_activeGameId != Guid.Empty)
                    _selectHostTab = true;
            }
        }

        if (_activeGameId != Guid.Empty && _games.TryGetHostedBlackjack(_activeGameId, out var host))
        {
            ImGui.Separator();
            ImGui.TextUnformatted("Blackjack Host Controls");

            using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 8f * ImGuiHelpers.GlobalScale }))
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Coins, "Start Betting", 180 * ImGuiHelpers.GlobalScale, true))
                    _games.HostBlackjackStartBetting(_activeGameId);

                ImGui.SameLine();

                if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, "Deal & Play", 180 * ImGuiHelpers.GlobalScale, true))
                    _games.HostBlackjackDealAndPlay(_activeGameId);

                ImGui.SameLine();

                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StepForward, "Next Round", 180 * ImGuiHelpers.GlobalScale, true))
                    _games.HostBlackjackNextRound(_activeGameId);
            }

            ImGuiHelpers.ScaledDummy(6);

            ImGui.TextUnformatted("Players");

            using (ImRaii.Table("bj_host_players", 5, ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingStretchProp))
            {
                ImGui.TableSetupColumn("Player", ImGuiTableColumnFlags.WidthStretch);
                ImGui.TableSetupColumn("Bet", ImGuiTableColumnFlags.WidthFixed, 90 * ImGuiHelpers.GlobalScale);
                ImGui.TableSetupColumn("Win", ImGuiTableColumnFlags.WidthFixed, 90 * ImGuiHelpers.GlobalScale);
                ImGui.TableSetupColumn("Total", ImGuiTableColumnFlags.WidthFixed, 70 * ImGuiHelpers.GlobalScale);
                ImGui.TableSetupColumn("Status", ImGuiTableColumnFlags.WidthFixed, 80 * ImGuiHelpers.GlobalScale);
                ImGui.TableHeadersRow();

                foreach (var p in host.Players.Values.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase))
                {
                    var total = BlackjackTotal(p.Cards);
                    var status = p.BetConfirmed ? "Ready" : "Waiting";

                    ImGui.TableNextRow();

                    ImGui.TableNextColumn();
                    ImGui.TextUnformatted(p.Name);

                    ImGui.TableNextColumn();
                    ImGui.TextUnformatted(p.Bet.ToString());

                    ImGui.TableNextColumn();
                    ImGui.TextUnformatted((p.Bet * 2).ToString());

                    ImGui.TableNextColumn();
                    ImGui.TextUnformatted(total.ToString());

                    ImGui.TableNextColumn();
                    if (p.BetConfirmed) ImGui.TextColored(ImGuiColors.ParsedGreen, status);
                    else ImGui.TextColored(ImGuiColors.DalamudGrey, status);
                }
            }
        }
    }

    private void DrawInvites()
    {
        if (_games.Invites.IsEmpty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No invites yet.");
            return;
        }

        foreach (var inv in _games.Invites.Values)
        {
            using var id = ImRaii.PushId(inv.GameId.ToString());

            ImGui.TextUnformatted(inv.Kind + " from " + inv.HostName);

            ImGui.SameLine();
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignInAlt, "Accept invite", 160 * ImGuiHelpers.GlobalScale, true))
            {
                _activeGameId = inv.GameId;
                _games.Join(inv.GameId);

                // Jump straight into the game's UI
                _selectBlackjackTab = true;
            }

            ImGui.Separator();
        }
    }

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

        using (ImRaii.Table("bj_layout", 2, ImGuiTableFlags.SizingStretchProp))
        {
            ImGui.TableSetupColumn("Left", ImGuiTableColumnFlags.WidthStretch, 0.62f);
            ImGui.TableSetupColumn("Right", ImGuiTableColumnFlags.WidthStretch, 0.38f);

            ImGui.TableNextRow();

            ImGui.TableNextColumn();
            using (ImRaii.Child("bj_left", new Vector2(0, 0), false))
            {
                DrawSectionHeader("Dealer");

                var dealerUp = priv?.DealerUpCard ?? pub?.DealerUpCard ?? (byte)0;

                if (pub != null && pub.Stage == BjStage.Results && pub.DealerCardsReveal != null && pub.DealerCardsReveal.Count > 0)
                {
                    DrawCardRow(pub.DealerCardsReveal.ToArray(), showAsHidden: false);
                    ImGui.TextColored(ImGuiColors.DalamudGrey, "Total: " + pub.DealerTotalReveal);
                }
                else
                {
                    DrawCardRow([dealerUp], showAsHidden: false);
                    if (priv?.DealerHoleCardIfHost != null)
                    {
                        ImGui.SameLine();
                        DrawSingleCard(priv.DealerHoleCardIfHost.Value, showAsHidden: false);
                    }
                }

                ImGuiHelpers.ScaledDummy(10);

                DrawSectionHeader("Table");

                if (pub != null)
                {
                    using (ImRaii.Table("bj_table_players", 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.SizingStretchProp))
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

                    using (ImRaii.Table("bj_results", 4, ImGuiTableFlags.RowBg | ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.SizingStretchProp))
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
                    }
                }
            }

            ImGui.TableNextColumn();
            using (ImRaii.Child("bj_right", new Vector2(0, 0), true))
            {
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

    private void DrawSingleCard(byte card, bool showAsHidden)
    {
        float scale = ImGuiHelpers.GlobalScale;

        float cardH = 400f * scale;
        float cardW = cardH * (2f / 3f);

        var p = ImGui.GetCursorScreenPos();
        var dl = ImGui.GetWindowDrawList();

        EnsureCardTextures();

        DrawSingleCardAt(dl, p, new Vector2(cardW, cardH), card, showAsHidden);
        ImGui.Dummy(new Vector2(cardW, cardH));
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

    private void DrawSingleCardAt(ImDrawListPtr dl, Vector2 p, Vector2 size, byte card, bool showAsHidden)
    {
        dl.AddRectFilled(p + new Vector2(2, 2) * ImGuiHelpers.GlobalScale, p + size + new Vector2(2, 2) * ImGuiHelpers.GlobalScale,
            ImGui.GetColorU32(new Vector4(0f, 0f, 0f, 0.35f)), 12f);

        dl.AddRectFilled(p, p + size, ImGui.GetColorU32(new Vector4(0.12f, 0.12f, 0.12f, 1f)), 12f);
        dl.AddRect(p, p + size, ImGui.GetColorU32(new Vector4(0.65f, 0.65f, 0.65f, 1f)), 12f, 0, 2.0f);

        var pad = new Vector2(6, 6) * ImGuiHelpers.GlobalScale;
        var imgMin = p + pad;
        var imgMax = p + size - pad;

        if (_cardAtlas != null && _cardBack != null)
        {
            if (showAsHidden)
            {
                dl.AddImage(_cardBack.Handle, imgMin, imgMax);
            }
            else
            {
                GetCardUv(card, out var uv0, out var uv1);
                dl.AddImage(_cardAtlas.Handle, imgMin, imgMax, uv0, uv1);
            }

            return;
        }

        string label = showAsHidden ? "??" : CardLabelAscii(card);
        var textSize = ImGui.CalcTextSize(label);
        var textPos = p + (size - textSize) / 2f;
        dl.AddText(textPos, ImGui.GetColorU32(ImGuiColors.ParsedGrey), label);
    }


    private static string CardLabelAscii(byte c)
    {
        int rank = c % 13;
        int suit = c / 13;

        string r = rank switch
        {
            0 => "A",
            10 => "J",
            11 => "Q",
            12 => "K",
            _ => (rank + 1).ToString()
        };

        string s = suit switch
        {
            0 => "S",
            1 => "H",
            2 => "D",
            _ => "C"
        };

        return r + s;
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

    private static void DrawSectionHeader(string text)
    {
        using (ImRaii.PushStyle(ImGuiStyleVar.FramePadding, new Vector2(8, 6) * ImGuiHelpers.GlobalScale))
        using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, new Vector2(8, 6) * ImGuiHelpers.GlobalScale))
        {
            ImGui.TextColored(ImGuiColors.ParsedGrey, text);
        }
    }

    private static void DrawPill(string text, Vector4 col)
    {
        var dl = ImGui.GetWindowDrawList();
        var p = ImGui.GetCursorScreenPos();

        var pad = new Vector2(10, 6) * ImGuiHelpers.GlobalScale;
        var ts = ImGui.CalcTextSize(text);
        var size = ts + pad * 2;

        dl.AddRectFilled(p, p + size, ImGui.GetColorU32(new Vector4(col.X, col.Y, col.Z, 0.18f)), 999f);
        dl.AddRect(p, p + size, ImGui.GetColorU32(new Vector4(col.X, col.Y, col.Z, 0.45f)), 999f);

        dl.AddText(p + pad, ImGui.GetColorU32(col), text);
        ImGui.Dummy(size);
    }

    private static Vector4 OutcomeColor(string outcome)
    {
        if (string.Equals(outcome, "Win", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedGreen;
        if (string.Equals(outcome, "Push", StringComparison.OrdinalIgnoreCase)) return ImGuiColors.ParsedOrange;
        return ImGuiColors.DalamudRed;
    }

    private static string CardLabel(byte c)
    {
        int rank = c % 13;
        int suit = c / 13;

        string r = rank switch
        {
            0 => "A",
            10 => "J",
            11 => "Q",
            12 => "K",
            _ => (rank + 1).ToString()
        };

        string s = suit switch
        {
            0 => "♠",
            1 => "♥",
            2 => "♦",
            _ => "♣"
        };

        return r + s;
    }

    private void EnsureCardTextures()
    {
        if (_cardTexturesTried) return;
        _cardTexturesTried = true;

        try
        {
            _cardAtlas = _uiSharedService.LoadImage(ReadEmbedded("RavaSync.Resources.cards_atlas.png"));
            _cardBack = _uiSharedService.LoadImage(ReadEmbedded("RavaSync.Resources.card_back.png"));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load card textures (falling back to text labels).");
            _cardAtlas = null;
            _cardBack = null;
        }
    }

    private static byte[] ReadEmbedded(string resourceName)
    {
        using var s = typeof(SyncshellGamesUi).Assembly.GetManifestResourceStream(resourceName);
        if (s == null) throw new FileNotFoundException("Missing embedded resource: " + resourceName);

        using var ms = new MemoryStream();
        s.CopyTo(ms);
        return ms.ToArray();
    }

    private static int SuitToAtlasRow(int suit)
    {
        // Row 0 = Diamonds
        // Row 1 = Spades
        // Row 2 = Hearts
        // Row 3 = Clubs
        //
        // 0=Spades, 1=Hearts, 2=Diamonds, 3=Clubs
        return suit switch
        {
            0 => 1, // Spades -> row 1
            1 => 2, // Hearts -> row 2
            2 => 0, // Diamonds -> row 0
            _ => 3, // Clubs -> row 3
        };
    }

    private static void GetCardUv(byte card, out Vector2 uv0, out Vector2 uv1)
    {
        // Atlas is 13 columns x 4 rows
        const float cols = 13f;
        const float rows = 4f;

        int rank = card % 13;     // 0..12 (A..K)
        int suit = card / 13;     // 0..3  (Spades/Hearts/Diamonds/Clubs)

        int row = SuitToAtlasRow(suit);

        float cellW = 1f / cols;
        float cellH = 1f / rows;

        const float insetU = 0.5f / 6656f;
        const float insetV = 0.5f / 3072f;

        float u0 = rank * cellW + insetU;
        float v0 = row * cellH + insetV;
        float u1 = (rank + 1) * cellW - insetU;
        float v1 = (row + 1) * cellH - insetV;

        uv0 = new Vector2(u0, v0);
        uv1 = new Vector2(u1, v1);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _cardAtlas?.Dispose();
            _cardBack?.Dispose();
            _cardAtlas = null;
            _cardBack = null;
        }

        base.Dispose(disposing);
    }


}
