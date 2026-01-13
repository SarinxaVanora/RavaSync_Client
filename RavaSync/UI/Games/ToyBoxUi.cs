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

public sealed partial class ToyBoxUi : WindowMediatorSubscriberBase
{
    private readonly UiSharedService _uiSharedService;
    private readonly PairManager _pairManager;
    private readonly ToyBox _games;

    private IDalamudTextureWrap? _cardAtlas;
    private IDalamudTextureWrap? _cardBack;
    private bool _cardTexturesTried = false;

    private int _hostGameKind = 0;
    private Guid _activeGameId = Guid.Empty;

    // Host lobby options
    private string _hostLobbyName = string.Empty;
    private int _hostMaxPlayers = 0;
    private bool _hostUsePassword = false;
    private string _hostPassword = string.Empty;

    // Join password popup
    private Guid _pendingJoinGameId = Guid.Empty;
    private string _pendingJoinPassword = string.Empty;
    private bool _openJoinPasswordPopup = false;

    private bool _selectLobbiesTab = false;
    private bool _selectHostTab = false;
    private bool _selectBlackjackTab = false;
    private bool _selectPokerTab = false;
    private bool _selectBingoTab = false;


    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();

    public ToyBoxUi(ILogger<ToyBoxUi> logger, MareMediator mediator,
            UiSharedService uiSharedService, PairManager pairManager, ToyBox games,
            PerformanceCollectorService performanceCollectorService)
            : base(logger, mediator, "Toy Box", performanceCollectorService)
    {
        _uiSharedService = uiSharedService;
        _pairManager = pairManager;
        _games = games;

        SizeConstraints = new WindowSizeConstraints()
        {
            MinimumSize = new(820, 520),
            MaximumSize = new(1200, 1200),
        };
    }

    public override void OnOpen()
    {
        base.OnOpen();
        _selectHostTab = false;
        _selectBlackjackTab = false;
        _selectPokerTab = false;
        _selectBingoTab = false;
        _selectLobbiesTab = true;
    }


    protected override void DrawInternal()
    {

        using (_uiSharedService.UidFont.Push())
            ImGui.TextUnformatted("Toy Box Games");

        ImGui.Separator();

        using var tabs = ImRaii.TabBar("toybox_games_tabs_");
        if (!tabs) return;

        // Force-select Host tab right after hosting
        var hostFlags = _selectHostTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var hostTab = ImRaii.TabItem("Host", hostFlags))
        {
            if (hostTab)
                DrawHost();
        }

        var lobbiesFlags = _selectLobbiesTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var invitesTab = ImRaii.TabItem("Lobbies", lobbiesFlags))
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
        var bingoFlags = _selectBingoTab ? ImGuiTabItemFlags.SetSelected : ImGuiTabItemFlags.None;
        using (var bingoTab = ImRaii.TabItem("Bingo", bingoFlags))
        {
            if (bingoTab)
                DrawBingo();
        }


        _selectPokerTab = false;
        _selectLobbiesTab = false;
        _selectHostTab = false;
        _selectBlackjackTab = false;
        _selectBingoTab = false;
    }

    private void DrawHost()
    {
        string[] kinds = ["Blackjack", "Poker (Texas Hold 'em)", "Bingo"];
        ImGui.SetNextItemWidth(250 * ImGuiHelpers.GlobalScale);
        ImGui.Combo("Game Type", ref _hostGameKind, kinds, kinds.Length);

        ImGuiHelpers.ScaledDummy(8);

        float scale = ImGuiHelpers.GlobalScale;

        // Row: Name [ ]   Max Players [ ] (0 = unlimited)   Password protect? [x]
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Name:");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(260 * scale);
        ImGui.InputText("##toybox_lobby_name", ref _hostLobbyName, 48);

        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted("Max Players");
        ImGui.SameLine();
        ImGui.SetNextItemWidth(100 * scale);
        ImGui.InputInt("##toybox_max_players", ref _hostMaxPlayers);
        if (_hostMaxPlayers < 0) _hostMaxPlayers = 0;

        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.TextColored(ImGuiColors.DalamudGrey, "(0 = unlimited)");

        ImGui.SameLine();
        ImGui.Checkbox("Password protect?##toybox_pw_toggle", ref _hostUsePassword);

        if (_hostUsePassword)
        {
            ImGuiHelpers.ScaledDummy(6);
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Password:");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(260 * scale);
            ImGui.InputText("##toybox_lobby_pw", ref _hostPassword, 128, ImGuiInputTextFlags.Password);
        }


        bool pokerSelected = _hostGameKind == 1;
        bool pokerBuyInValid = _pokerHostBuyIn >= 1;

        if (pokerSelected)
        {
            ImGuiHelpers.ScaledDummy(6);

            float scale2 = ImGuiHelpers.GlobalScale;

            // Row: Buy-in [ ]   Small Blind [ ]   Big Blind [ ]
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Buy-in");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(110 * scale2);
            ImGui.InputInt("##toybox_poker_buyin", ref _pokerHostBuyIn);

            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Small blind");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(90 * scale2);
            ImGui.InputInt("##toybox_poker_sb", ref _pokerHostSmallBlind);

            ImGui.SameLine();
            ImGui.AlignTextToFramePadding();
            ImGui.TextUnformatted("Big blind");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(90 * scale2);
            ImGui.InputInt("##toybox_poker_bb", ref _pokerHostBigBlind);

            if (_pokerHostBuyIn < 1) _pokerHostBuyIn = 1;
            if (_pokerHostSmallBlind < 1) _pokerHostSmallBlind = 1;
            if (_pokerHostBigBlind < _pokerHostSmallBlind) _pokerHostBigBlind = _pokerHostSmallBlind;

            if (!pokerBuyInValid)
                ImGui.TextColored(ImGuiColors.DalamudRed, "Buy-in must be set before hosting Poker.");
        }


        bool disableHost = pokerSelected && !pokerBuyInValid;

        ImGui.BeginDisabled(disableHost);
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Play, "Host game", 200 * ImGuiHelpers.GlobalScale, true))
        {
            var pw = _hostUsePassword ? _hostPassword : null;

            if (_hostGameKind == 0)
            {
                _activeGameId = _games.HostBlackjack(_hostLobbyName, _hostMaxPlayers, pw);
                if (_activeGameId != Guid.Empty) _selectBlackjackTab = true;
            }
            else if (_hostGameKind == 1)
            {
                _activeGameId = _games.HostPoker(Math.Max(1, _pokerHostBuyIn), _pokerHostSmallBlind, _pokerHostBigBlind,
                    _hostLobbyName, _hostMaxPlayers, pw);

                if (_activeGameId != Guid.Empty) _selectPokerTab = true;
            }
            else if (_hostGameKind == 2)
            {
                _activeGameId = _games.HostBingo(_hostLobbyName, _hostMaxPlayers, pw);
                if (_activeGameId != Guid.Empty) _selectBingoTab = true;
            }
        }
        ImGui.EndDisabled();
    }


    private void DrawInvites()
    {
        // Direct (right-click) invites
        if (!_games.DirectInvites.IsEmpty)
        {
            ImGui.TextUnformatted("Direct Invites");
            ImGui.Separator();

            foreach (var inv in _games.DirectInvites.Values.OrderByDescending(v => v.ReceivedTicks))
            {
                using var id = ImRaii.PushId("direct_" + inv.GameId);

                var title = string.IsNullOrWhiteSpace(inv.LobbyName)
                    ? $"{inv.Kind} invite from {inv.HostName}"
                    : $"{inv.LobbyName} ({inv.Kind}) from {inv.HostName}";

                ImGui.TextUnformatted(title);

                if (_uiSharedService.IconTextButton(FontAwesomeIcon.Check, "Accept invite", 170 * ImGuiHelpers.GlobalScale, true))
                {
                    _activeGameId = inv.GameId;
                    if (inv.Kind == SyncshellGameKind.Bingo)
                    {
                        _games.JoinBingo(inv.GameId, 0, PackArgb(_bingoMarkerColor), null, fromDirectInvite: true);

                        _selectBingoTab = true;
                    }
                    else
                    {
                        _games.AcceptDirectInvite(inv.GameId);
                        if (inv.Kind == SyncshellGameKind.Blackjack) _selectBlackjackTab = true;
                        else if (inv.Kind == SyncshellGameKind.Poker) _selectPokerTab = true;
                    }
                }

                ImGui.Separator();
            }

            ImGuiHelpers.ScaledDummy(10);
        }

        // Lobbies (broadcast)
        ImGui.TextUnformatted("Lobbies");
        ImGui.Separator();

        if (_games.Invites.IsEmpty)
        {
            ImGui.TextColored(ImGuiColors.DalamudGrey, "No lobbies nearby.");
            DrawJoinPasswordPopup();
            return;
        }

        foreach (var inv in _games.Invites.Values.OrderByDescending(i => i.LastSeenTicks))
        {
            using var id = ImRaii.PushId(inv.GameId.ToString());

            var title = string.IsNullOrWhiteSpace(inv.LobbyName)
                ? $"{inv.Kind} from {inv.HostName}"
                : $"{inv.LobbyName} ({inv.Kind} • {inv.HostName})";

            ImGui.TextUnformatted(title);

            if (inv.PasswordProtected)
            {
                ImGui.SameLine();
                _uiSharedService.IconText(FontAwesomeIcon.Lock, ImGuiColors.DalamudGrey);
            }

            ImGui.SameLine();
            var maxText = inv.MaxPlayers > 0 ? inv.MaxPlayers.ToString() : "∞";
            ImGui.TextColored(ImGuiColors.DalamudGrey, $"  {inv.CurrentPlayers}/{maxText}");

            if (inv.Kind == SyncshellGameKind.Poker)
            {
                ImGui.SameLine();
                ImGui.TextColored(ImGuiColors.DalamudGrey, $"  Buy-in: {inv.TableBuyIn}");
            }
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignInAlt, inv.PasswordProtected ? "Join (password)" : "Join", 170 * ImGuiHelpers.GlobalScale, true))
            {
                if (inv.PasswordProtected)
                {
                    _pendingJoinGameId = inv.GameId;
                    _pendingJoinPassword = string.Empty;
                    _openJoinPasswordPopup = true;
                }
                else
                {
                    _activeGameId = inv.GameId;

                    if (inv.Kind == SyncshellGameKind.Bingo)
                    {
                        _games.JoinBingo(inv.GameId, 0, PackArgb(_bingoMarkerColor));
                        _selectBingoTab = true;
                    }
                    else
                    {
                        _games.Join(inv.GameId, inv.Kind == SyncshellGameKind.Poker ? inv.TableBuyIn : 0);
                        if (inv.Kind == SyncshellGameKind.Blackjack) _selectBlackjackTab = true;
                        else if (inv.Kind == SyncshellGameKind.Poker) _selectPokerTab = true;
                    }
                }

            }

            ImGui.Separator();
        }

        DrawJoinPasswordPopup();
    }

    private void DrawJoinPasswordPopup()
    {
        if (_openJoinPasswordPopup)
        {
            ImGui.OpenPopup("Join lobby##toybox_pw");
            _openJoinPasswordPopup = false;
        }

        bool open = true;
        if (!ImGui.BeginPopupModal("Join lobby##toybox_pw", ref open, ImGuiWindowFlags.AlwaysAutoResize))
            return;

        ImGui.TextUnformatted("This lobby is password protected.");
        ImGuiHelpers.ScaledDummy(4);

        ImGui.SetNextItemWidth(260 * ImGuiHelpers.GlobalScale);
        ImGui.InputText("Password", ref _pendingJoinPassword, 128, ImGuiInputTextFlags.Password);

        ImGuiHelpers.ScaledDummy(8);

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.SignInAlt, "Join", 120 * ImGuiHelpers.GlobalScale, true))
        {
            if (_games.Invites.TryGetValue(_pendingJoinGameId, out var inv))
            {
                _activeGameId = inv.GameId;
                if (inv.Kind == SyncshellGameKind.Bingo)
                {
                    _games.JoinBingo(inv.GameId, 0, PackArgb(_bingoMarkerColor), _pendingJoinPassword);
                    _selectBingoTab = true;
                }
                else
                {
                    _games.Join(inv.GameId, inv.Kind == SyncshellGameKind.Poker ? inv.TableBuyIn : 0, _pendingJoinPassword);

                    if (inv.Kind == SyncshellGameKind.Blackjack) _selectBlackjackTab = true;
                    else if (inv.Kind == SyncshellGameKind.Poker) _selectPokerTab = true;
                }
            }

            _pendingJoinPassword = string.Empty;
            _pendingJoinGameId = Guid.Empty;
            ImGui.CloseCurrentPopup();
        }

        ImGui.SameLine();
        if (ImGui.Button("Cancel", new Vector2(80 * ImGuiHelpers.GlobalScale, 0)))
        {
            _pendingJoinPassword = string.Empty;
            _pendingJoinGameId = Guid.Empty;
            ImGui.CloseCurrentPopup();
        }

        ImGui.EndPopup();
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
        using var s = typeof(ToyBoxUi).Assembly.GetManifestResourceStream(resourceName);
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
