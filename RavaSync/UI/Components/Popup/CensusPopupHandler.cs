using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility;
using RavaSync.Services.ServerConfiguration;
using System.Numerics;

namespace RavaSync.UI.Components.Popup;

public class CensusPopupHandler : IPopupHandler
{
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly UiSharedService _uiSharedService;

    public CensusPopupHandler(ServerConfigurationManager serverConfigurationManager, UiSharedService uiSharedService)
    {
        _serverConfigurationManager = serverConfigurationManager;
        _uiSharedService = uiSharedService;
    }

    private Vector2 _size = new(600, 450);
    public Vector2 PopupSize => _size;

    public bool ShowClose => false;

    public void DrawContent()
    {
        var start = 0f;
        using (_uiSharedService.UidFont.Push())
        {
            start = ImGui.GetCursorPosY() - ImGui.CalcTextSize(_uiSharedService.L("UI.CensusPopupHandler.a4b08f02", "RavaSync Census Data")).Y;
            UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.7529e424", "RavaSync Census Participation"));
        }
        ImGuiHelpers.ScaledDummy(5f);
        UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.286d2256", "If you are seeing this popup you are updating from a RavaSync version that did not collect census data. Please read the following carefully."));
        ImGui.Separator();
        UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.933c4445", "RavaSync Census is a data collecting service that can be used for statistical purposes. ") +
            "All data collected through RavaSync Census is temporary and will be stored associated with your UID on the connected service as long as you are connected. " +
            "The data cannot be used for long term tracking of individuals.");
        UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.ba35fbcd", "If enabled, RavaSync Census will collect following data:") + Environment.NewLine
            + "- Currently connected World" + Environment.NewLine
            + "- Current Gender (reflecting Glamourer changes)" + Environment.NewLine
            + "- Current Race (reflecting Glamourer changes)" + Environment.NewLine
            + "- Current Clan (i.e. Seeker of the Sun, Keeper of the Moon, etc., reflecting Glamourer changes)");
        UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.4e06481d", "To consent to collecting census data press the appropriate button below."));
        UiSharedService.TextWrapped(_uiSharedService.L("UI.CensusPopupHandler.d44f5b2c", "This setting can be changed anytime in the RavaSync Settings."));
        var width = ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;
        var buttonSize = ImGuiHelpers.GetButtonSize(_uiSharedService.L("UI.CensusPopupHandler.6aa65e52", "I consent to send my census data"));
        ImGuiHelpers.ScaledDummy(5f);
        if (ImGui.Button(_uiSharedService.L("UI.CensusPopupHandler.3871a82b", "I consent to send my census data"), new Vector2(width, buttonSize.Y * 2.5f)))
        {
            _serverConfigurationManager.SendCensusData = true;
            _serverConfigurationManager.ShownCensusPopup = true;
            ImGui.CloseCurrentPopup();
        }
        ImGuiHelpers.ScaledDummy(1f);
        if (ImGui.Button(_uiSharedService.L("UI.CensusPopupHandler.e016c9ab", "I do not consent to send my census data"), new Vector2(width, buttonSize.Y)))
        {
            _serverConfigurationManager.SendCensusData = false;
            _serverConfigurationManager.ShownCensusPopup = true;
            ImGui.CloseCurrentPopup();
        }
        var height = ImGui.GetCursorPosY() - start;
        _size = _size with { Y = height };
    }
}
