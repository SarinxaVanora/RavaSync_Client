using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.ImGuiFileDialog;
using Dalamud.Interface.Textures.TextureWraps;
using Dalamud.Interface.Utility;
using RavaSync.API.Data;
using RavaSync.API.Dto.User;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using Microsoft.Extensions.Logging;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;

namespace RavaSync.UI;

public class EditProfileUi : WindowMediatorSubscriberBase
{
    private readonly ApiController _apiController;
    private readonly FileDialogManager _fileDialogManager;
    private readonly MareProfileManager _mareProfileManager;
    private readonly UiSharedService _uiSharedService;
    private bool _adjustedForScollBarsLocalProfile = false;
    private bool _adjustedForScollBarsOnlineProfile = false;
    private string _descriptionText = string.Empty;
    private IDalamudTextureWrap? _pfpTextureWrap;
    private string _profileDescription = string.Empty;
    private byte[] _profileImage = [];
    private bool _showFileDialogError = false;
    private bool _wasOpen;
    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public EditProfileUi(ILogger<EditProfileUi> logger, MareMediator mediator,
        ApiController apiController, UiSharedService uiSharedService, FileDialogManager fileDialogManager,
        MareProfileManager mareProfileManager, PerformanceCollectorService performanceCollectorService)
        : base(logger, mediator, "RavaSync Edit Profile###RavaSyncEditProfileUI", performanceCollectorService)
    {
        IsOpen = false;
        this.SizeConstraints = new()
        {
            MinimumSize = new(768, 512),
            MaximumSize = new(768, 2000)
        };
        _apiController = apiController;
        _uiSharedService = uiSharedService;
        _fileDialogManager = fileDialogManager;
        _mareProfileManager = mareProfileManager;

        Mediator.Subscribe<GposeStartMessage>(this, (_) => { _wasOpen = IsOpen; IsOpen = false; });
        Mediator.Subscribe<GposeEndMessage>(this, (_) => IsOpen = _wasOpen);
        Mediator.Subscribe<DisconnectedMessage>(this, (_) => IsOpen = false);
        Mediator.Subscribe<ClearProfileDataMessage>(this, (msg) =>
        {
            if (msg.UserData == null || string.Equals(msg.UserData.UID, _apiController.UID, StringComparison.Ordinal))
            {
                _pfpTextureWrap?.Dispose();
                _pfpTextureWrap = null;
            }
        });
    }

    protected override void DrawInternal()
    {
        _uiSharedService.BigText(_uiSharedService.L("UI.EditProfileUi.d41d8cd9", ""));

        var profile = _mareProfileManager.GetMareProfile(new UserData(_apiController.UID));

        if (profile.IsFlagged)
        {
            UiSharedService.ColorTextWrapped(profile.Description, ImGuiColors.DalamudRed);
            return;
        }

        if (!_profileImage.SequenceEqual(profile.ImageData.Value))
        {
            _profileImage = profile.ImageData.Value;
            _pfpTextureWrap?.Dispose();
            _pfpTextureWrap = _uiSharedService.LoadImage(_profileImage);
        }

        if (!string.Equals(_profileDescription, profile.Description, StringComparison.OrdinalIgnoreCase))
        {
            _profileDescription = profile.Description;
            _descriptionText = _profileDescription;
        }

        if (_pfpTextureWrap != null)
        {
            ImGui.Image(_pfpTextureWrap.Handle, ImGuiHelpers.ScaledVector2(_pfpTextureWrap.Width, _pfpTextureWrap.Height));
        }

        var spacing = ImGui.GetStyle().ItemSpacing.X;
        ImGuiHelpers.ScaledRelativeSameLine(256, spacing);
        using (_uiSharedService.GameFont.Push())
        {
            var descriptionTextSize = ImGui.CalcTextSize(profile.Description, wrapWidth: 256f);
            var childFrame = ImGuiHelpers.ScaledVector2(256 + ImGui.GetStyle().WindowPadding.X + ImGui.GetStyle().WindowBorderSize, 256);
            if (descriptionTextSize.Y > childFrame.Y)
            {
                _adjustedForScollBarsOnlineProfile = true;
            }
            else
            {
                _adjustedForScollBarsOnlineProfile = false;
            }
            childFrame = childFrame with
            {
                X = childFrame.X + (_adjustedForScollBarsOnlineProfile ? ImGui.GetStyle().ScrollbarSize : 0),
            };
            if (ImGui.BeginChildFrame(101, childFrame))
            {
                UiSharedService.TextWrapped(profile.Description);
            }
            ImGui.EndChildFrame();
        }

        var nsfw = profile.IsNSFW;
        ImGui.BeginDisabled();
        ImGui.Checkbox(_uiSharedService.L("UI.EditProfileUi.2b301f6d", "Is NSFW"), ref nsfw);
        ImGui.EndDisabled();

        ImGui.Separator();
        _uiSharedService.BigText(_uiSharedService.L("UI.EditProfileUi.d41d8cd9", ""));

        ImGui.TextWrapped(string.Format(_uiSharedService.L("UI.EditProfileUi.9cbb3e8f", "- All users that are paired and unpaused with you will be able to see your profile picture and description.{0}- Other users have the possibility to report your profile for breaking the rules.{1}- !!! AVOID: anything as profile image that can be considered highly illegal or obscene (bestiality, anything that could be considered a sexual act with a minor (that includes Lalafells), etc.){2}- !!! AVOID: slurs of any kind in the description that can be considered highly offensive{3}- In case of valid reports from other users this can lead to disabling your profile forever or terminating your RavaSync account indefinitely.{4}- Judgement of your profile validity from reports through staff is not up to debate and the decisions to disable your profile/account permanent.{5}- If your profile picture or profile description could be considered NSFW, enable the toggle below."), Environment.NewLine, Environment.NewLine, Environment.NewLine, Environment.NewLine, Environment.NewLine, Environment.NewLine));
        ImGui.Separator();
        _uiSharedService.BigText(_uiSharedService.L("UI.EditProfileUi.d41d8cd9", ""));

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.FileUpload, _uiSharedService.L("UI.EditProfileUi.c9ab69c4", "Upload new profile picture")))
        {
            _fileDialogManager.OpenFileDialog("Select new Profile picture", ".png", (success, file) =>
            {
                if (!success) return;
                _ = Task.Run(async () =>
                {
                    var fileContent = File.ReadAllBytes(file);
                    using MemoryStream ms = new(fileContent);
                    var format = await Image.DetectFormatAsync(ms).ConfigureAwait(false);
                    if (!format.FileExtensions.Contains("png", StringComparer.OrdinalIgnoreCase))
                    {
                        _showFileDialogError = true;
                        return;
                    }
                    using var image = Image.Load<Rgba32>(fileContent);

                    if (image.Width > 256 || image.Height > 256 || (fileContent.Length > 250 * 1024))
                    {
                        _showFileDialogError = true;
                        return;
                    }

                    _showFileDialogError = false;
                    await _apiController.UserSetProfile(new UserProfileDto(new UserData(_apiController.UID), Disabled: false, IsNSFW: null, Convert.ToBase64String(fileContent), Description: null))
                        .ConfigureAwait(false);
                });
            });
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.EditProfileUi.e9fd1cfb", "Select and upload a new profile picture"));
        ImGui.SameLine();
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.EditProfileUi.25252649", "Clear uploaded profile picture")))
        {
            _ = _apiController.UserSetProfile(new UserProfileDto(new UserData(_apiController.UID), Disabled: false, IsNSFW: null, "", Description: null));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.EditProfileUi.96a04658", "Clear your currently uploaded profile picture"));
        if (_showFileDialogError)
        {
            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.EditProfileUi.9125f2b4", "The profile picture must be a PNG file with a maximum height and width of 256px and 250KiB size"), ImGuiColors.DalamudRed);
        }
        var isNsfw = profile.IsNSFW;
        if (ImGui.Checkbox(_uiSharedService.L("UI.EditProfileUi.2ccd1904", "Profile is NSFW"), ref isNsfw))
        {
            _ = _apiController.UserSetProfile(new UserProfileDto(new UserData(_apiController.UID), Disabled: false, isNsfw, ProfilePictureBase64: null, Description: null));
        }
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.EditProfileUi.9648e52a", "If your profile description or image can be considered NSFW, toggle this to ON"));
        var widthTextBox = 400;
        var posX = ImGui.GetCursorPosX();
        ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.EditProfileUi.7d0decaa", "Description {0}/1500"), _descriptionText.Length));
        ImGui.SetCursorPosX(posX);
        ImGuiHelpers.ScaledRelativeSameLine(widthTextBox, ImGui.GetStyle().ItemSpacing.X);
        ImGui.TextUnformatted(_uiSharedService.L("UI.EditProfileUi.5f3fda56", "Preview (approximate)"));
        using (_uiSharedService.GameFont.Push())
            ImGui.InputTextMultiline("##description", ref _descriptionText, 1500, ImGuiHelpers.ScaledVector2(widthTextBox, 200));

        ImGui.SameLine();

        using (_uiSharedService.GameFont.Push())
        {
            var descriptionTextSizeLocal = ImGui.CalcTextSize(_descriptionText, wrapWidth: 256f);
            var childFrameLocal = ImGuiHelpers.ScaledVector2(256 + ImGui.GetStyle().WindowPadding.X + ImGui.GetStyle().WindowBorderSize, 200);
            if (descriptionTextSizeLocal.Y > childFrameLocal.Y)
            {
                _adjustedForScollBarsLocalProfile = true;
            }
            else
            {
                _adjustedForScollBarsLocalProfile = false;
            }
            childFrameLocal = childFrameLocal with
            {
                X = childFrameLocal.X + (_adjustedForScollBarsLocalProfile ? ImGui.GetStyle().ScrollbarSize : 0),
            };
            if (ImGui.BeginChildFrame(102, childFrameLocal))
            {
                UiSharedService.TextWrapped(_descriptionText);
            }
            ImGui.EndChildFrame();
        }

        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, _uiSharedService.L("UI.EditProfileUi.cf388f34", "Save Description")))
        {
            _ = _apiController.UserSetProfile(new UserProfileDto(new UserData(_apiController.UID), Disabled: false, IsNSFW: null, ProfilePictureBase64: null, _descriptionText));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.EditProfileUi.297e73ba", "Sets your profile description text"));
        ImGui.SameLine();
        if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.EditProfileUi.d6b9fd92", "Clear Description")))
        {
            _ = _apiController.UserSetProfile(new UserProfileDto(new UserData(_apiController.UID), Disabled: false, IsNSFW: null, ProfilePictureBase64: null, ""));
        }
        UiSharedService.AttachToolTip(_uiSharedService.L("UI.EditProfileUi.9d027acf", "Clears your profile description text"));
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _pfpTextureWrap?.Dispose();
    }
}