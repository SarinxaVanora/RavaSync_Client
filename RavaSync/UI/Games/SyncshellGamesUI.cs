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

public sealed partial class SyncshellGamesUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiSharedService;
    private readonly PairManager _pairManager;
    private readonly SyncshellGameService _games;

    private IDalamudTextureWrap? _cardAtlas;
    private IDalamudTextureWrap? _cardBack;
    private bool _cardTexturesTried = false;

    private int _hostGameKind = 0;
    private Guid _activeGameId = Guid.Empty;

    private bool _selectHostTab = false;
    private bool _selectBlackjackTab = false;
    private bool _selectPokerTab = false;

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

        var pokerFlags = _selectPokerTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var pokerTab = ImRaii.TabItem("Poker", pokerFlags))
        {
            if (pokerTab)
                DrawPoker();
        }

        _selectPokerTab = false;
        _selectHostTab = false;
        _selectBlackjackTab = false;
    }

    private void DrawHost()
    {
        string[] kinds = ["Blackjack", "Poker (Texas Hold 'em)", "Bingo (soon)"];
        ImGui.SetNextItemWidth(250 * ImGuiHelpers.GlobalScale);
        ImGui.Combo("Game Type", ref _hostGameKind, kinds, kinds.Length);

        bool pokerSelected = _hostGameKind == 1;
        bool pokerBuyInValid = _pokerHostBuyIn >= 1;

        if (pokerSelected)
        {
            ImGuiHelpers.ScaledDummy(6);

            using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 8f * ImGuiHelpers.GlobalScale }))
            {
                ImGui.SetNextItemWidth(140 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("Table buy-in", ref _pokerHostBuyIn);

                ImGui.SameLine();
                ImGui.SetNextItemWidth(90 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("SB", ref _pokerHostSmallBlind);

                ImGui.SameLine();
                ImGui.SetNextItemWidth(90 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("BB", ref _pokerHostBigBlind);
            }

            if (_pokerHostBuyIn < 0) _pokerHostBuyIn = 0;
            if (_pokerHostSmallBlind < 1) _pokerHostSmallBlind = 1;
            if (_pokerHostBigBlind < _pokerHostSmallBlind) _pokerHostBigBlind = _pokerHostSmallBlind;

            if (!pokerBuyInValid)
                ImGui.TextColored(ImGuiColors.DalamudRed, "Set a buy-in (>= 1) before you can host Poker.");
        }

        bool disableHost = pokerSelected && !pokerBuyInValid;

        ImGui.BeginDisabled(disableHost);

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Play, "Host game", 200 * ImGuiHelpers.GlobalScale, true))
        {
            if (_hostGameKind == 0)
            {
                _activeGameId = _games.HostBlackjack(GroupFullInfo);
                if (_activeGameId != Guid.Empty)
                    _selectBlackjackTab = true;
            }
            else if (_hostGameKind == 1)
            {
                _activeGameId = _games.HostPoker(GroupFullInfo, Math.Max(1, _pokerHostBuyIn), _pokerHostSmallBlind, _pokerHostBigBlind);

                if (_activeGameId != Guid.Empty)
                {
                    _selectPokerTab = true;
                }
            }
        }

        ImGui.EndDisabled();

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

            if (ImGui.BeginTable("bj_host_players", 5, ImGuiTableFlags.BordersInnerV | ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingStretchProp))
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

                ImGui.EndTable();
            }
        }

        if (_activeGameId != Guid.Empty && _games.TryGetHostedPoker(_activeGameId, out var pokerHost))
        {
            ImGui.Separator();
            ImGui.TextUnformatted("Poker Host Controls");

            using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, ImGui.GetStyle().ItemSpacing with { X = 8f * ImGuiHelpers.GlobalScale }))
            {
                ImGui.SetNextItemWidth(140 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("Table buy-in", ref _pokerHostBuyIn);

                ImGui.SameLine();
                ImGui.SetNextItemWidth(90 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("SB", ref _pokerHostSmallBlind);

                ImGui.SameLine();
                ImGui.SetNextItemWidth(90 * ImGuiHelpers.GlobalScale);
                ImGui.InputInt("BB", ref _pokerHostBigBlind);

                if (_pokerHostBuyIn < 0) _pokerHostBuyIn = 0;
                if (_pokerHostSmallBlind < 1) _pokerHostSmallBlind = 1;
                if (_pokerHostBigBlind < _pokerHostSmallBlind) _pokerHostBigBlind = _pokerHostSmallBlind;

                bool inGameBuyInValid = _pokerHostBuyIn >= 1;

                ImGui.SameLine();
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, "Apply", 120 * ImGuiHelpers.GlobalScale, true))
                {
                    if (inGameBuyInValid)
                    {
                        _games.HostPokerConfigure(_activeGameId,
                            Math.Max(1, _pokerHostBuyIn),
                            Math.Max(1, _pokerHostSmallBlind),
                            Math.Max(Math.Max(1, _pokerHostSmallBlind), _pokerHostBigBlind));
                    }
                }

                ImGui.SameLine();

                ImGui.BeginDisabled(!inGameBuyInValid);
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, "Start Hand", 160 * ImGuiHelpers.GlobalScale, true))
                {
                    _games.HostPokerConfigure(_activeGameId,
                        Math.Max(1, _pokerHostBuyIn),
                        Math.Max(1, _pokerHostSmallBlind),
                        Math.Max(Math.Max(1, _pokerHostSmallBlind), _pokerHostBigBlind));

                    _games.HostPokerStartHand(_activeGameId, Math.Max(1, _pokerHostBuyIn));
                }
                ImGui.EndDisabled();
            }

            if (_pokerHostBuyIn < 1)
                ImGui.TextColored(ImGuiColors.DalamudRed, "Buy-in must be set before starting a hand.");

            ImGuiHelpers.ScaledDummy(6);

            ImGui.TextColored(ImGuiColors.DalamudGrey,
                $"Current table: buy-in {pokerHost.BuyIn}   |   blinds {pokerHost.SmallBlind}/{pokerHost.BigBlind}");
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

            if (inv.Kind == SyncshellGameKind.Poker)
            {
                ImGui.SameLine();
                ImGui.TextColored(ImGuiColors.DalamudGrey, $"  Buy-in: {inv.TableBuyIn}");
            }

            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignInAlt, "Accept invite", 160 * ImGuiHelpers.GlobalScale, true))
            {
                _activeGameId = inv.GameId;
                _games.Join(inv.GameId, inv.Kind == SyncshellGameKind.Poker ? inv.TableBuyIn : 0);

                if (inv.Kind == SyncshellGameKind.Blackjack) _selectBlackjackTab = true;
                else if (inv.Kind == SyncshellGameKind.Poker) _selectPokerTab = true;
            }

            ImGui.Separator();
        }
    }

    private void DrawCardSlotsStackedSized(int totalSlots, IReadOnlyList<byte> cards, bool showHiddenForMissing, float cardH, float stepX)
    {
        EnsureCardTextures();

        float scale = ImGuiHelpers.GlobalScale;

        int count = Math.Min(totalSlots, 5);

        float cardW = cardH * (2f / 3f);

        var start = ImGui.GetCursorScreenPos();
        var dl = ImGui.GetWindowDrawList();

        var size = new Vector2(cardW, cardH);

        for (int i = 0; i < count; i++)
        {
            bool has = cards != null && i < cards.Count;

            byte card = has ? cards[i] : (byte)0;
            bool hidden = has ? false : showHiddenForMissing;

            var pos = start + new Vector2(i * stepX, 0);
            DrawSingleCardAt(dl, pos, size, card, hidden);
        }

        ImGui.Dummy(new Vector2(cardW + (count - 1) * stepX, cardH));
    }

    private void DrawCardRowInlineSized(byte[] cards, bool showAsHidden, float cardH)
    {
        if (cards == null || cards.Length == 0)
            return;

        EnsureCardTextures();

        float scale = ImGuiHelpers.GlobalScale;

        float cardW = cardH * (2f / 3f);
        float spacing = 10f * scale;

        var p = ImGui.GetCursorScreenPos();
        var dl = ImGui.GetWindowDrawList();

        for (int i = 0; i < cards.Length; i++)
        {
            var cardPos = p + new Vector2(i * (cardW + spacing), 0);
            DrawSingleCardAt(dl, cardPos, new Vector2(cardW, cardH), cards[i], showAsHidden);
        }

        ImGui.Dummy(new Vector2(cards.Length * (cardW + spacing) - spacing, cardH));
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
