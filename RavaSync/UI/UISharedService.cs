using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.GameFonts;
using Dalamud.Interface.ImGuiFileDialog;
using Dalamud.Interface.ManagedFontAtlas;
using Dalamud.Interface.Textures.TextureWraps;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using Dalamud.Utility;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RavaSync.FileCache;
using RavaSync.Fonts;
using RavaSync.Interop.Ipc;
using RavaSync.Localization;
using RavaSync.MareConfiguration;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Themes;
using RavaSync.Utils;
using RavaSync.WebAPI;
using RavaSync.WebAPI.SignalR;
using System.IdentityModel.Tokens.Jwt;
using System.Numerics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;

namespace RavaSync.UI;

public partial class UiSharedService : DisposableMediatorSubscriberBase
{
    public const string TooltipSeparator = "--SEP--";
    public static readonly ImGuiWindowFlags PopupWindowFlags = ImGuiWindowFlags.NoResize |
                                               ImGuiWindowFlags.NoScrollbar |
                                           ImGuiWindowFlags.NoScrollWithMouse;

    public readonly FileDialogManager FileDialogManager;
    private const string _notesEnd = "##MARE_SYNCHRONOS_USER_NOTES_END##";
    private const string _notesStart = "##MARE_SYNCHRONOS_USER_NOTES_START##";
    private readonly ApiController _apiController;
    private readonly CacheMonitor _cacheMonitor;
    private readonly MareConfigService _configService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IpcManager _ipcManager;
    private readonly Dalamud.Localization _localization;
    private readonly IDalamudPluginInterface _pluginInterface;
    private readonly Dictionary<string, object?> _selectedComboItems = new(StringComparer.Ordinal);
    private readonly ServerConfigurationManager _serverConfigurationManager;
    private readonly ITextureProvider _textureProvider;
    private readonly TokenProvider _tokenProvider;
    private bool _brioExists = false;
    private bool _cacheDirectoryHasOtherFilesThanCache = false;
    private bool _cacheDirectoryIsValidPath = true;
    private bool _customizePlusExists = false;
    private string _customServerName = "";
    private string _customServerUri = "";
    private Task<Uri?>? _discordOAuthCheck;
    private Task<string?>? _discordOAuthGetCode;
    private CancellationTokenSource _discordOAuthGetCts = new();
    private Task<Dictionary<string, string>>? _discordOAuthUIDs;
    private bool _glamourerExists = false;
    private bool _heelsExists = false;
    private bool _honorificExists = false;
    private bool _isDirectoryWritable = false;
    private bool _isOneDrive = false;
    private bool _isPenumbraDirectory = false;
    private bool _moodlesExists = false;
    private Dictionary<string, DateTime> _oauthTokenExpiry = new();
    private bool _penumbraExists = false;
    private bool _petNamesExists = false;
    private int _serverSelectionIndex = -1;
    private readonly IThemeManager _themeManager;
    private readonly IFontManager _fontManager;
    bool _didPrewarmFonts = false;
    private const string RavaCacheSubdirName = "RavaFiles";
    private bool _themeBootstrapped = false;


    public UiSharedService(ILogger<UiSharedService> logger, IpcManager ipcManager, ApiController apiController,
        CacheMonitor cacheMonitor, FileDialogManager fileDialogManager,
        MareConfigService configService, DalamudUtilService dalamudUtil, IDalamudPluginInterface pluginInterface,
        ITextureProvider textureProvider,
        Dalamud.Localization localization,
        ServerConfigurationManager serverManager, TokenProvider tokenProvider, MareMediator mediator, IThemeManager themeManager, IFontManager fontManager) : base(logger, mediator)
    {
        _ipcManager = ipcManager;
        _apiController = apiController;
        _cacheMonitor = cacheMonitor;
        FileDialogManager = fileDialogManager;
        _configService = configService;
        _dalamudUtil = dalamudUtil;
        _pluginInterface = pluginInterface;
        _textureProvider = textureProvider;
        _localization = localization;
        _serverConfigurationManager = serverManager;
        _tokenProvider = tokenProvider;
        _themeManager = themeManager;
        _fontManager = fontManager;
        var lang = _configService.Current.LanguageCode;
        if (string.IsNullOrWhiteSpace(lang)) lang = "en";
        LoadLocalization(lang);

        _isDirectoryWritable = IsDirectoryWritable(_configService.Current.CacheFolder);
        EnsureThemeBootstrapped();

        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, (_) =>
        {
            _penumbraExists = _ipcManager.Penumbra.APIAvailable;
            _glamourerExists = _ipcManager.Glamourer.APIAvailable;
            _customizePlusExists = _ipcManager.CustomizePlus.APIAvailable;
            _heelsExists = _ipcManager.Heels.APIAvailable;
            _honorificExists = _ipcManager.Honorific.APIAvailable;
            _moodlesExists = _ipcManager.Moodles.APIAvailable;
            _petNamesExists = _ipcManager.PetNames.APIAvailable;
            _brioExists = _ipcManager.Brio.APIAvailable;

        if (!_didPrewarmFonts)
        {
            _didPrewarmFonts = true;

            var keep = _configService.Current.FontId;

            _fontManager.LoadAll();

            if (!string.IsNullOrWhiteSpace(keep))
                _fontManager.TryApply(keep);
        }});

        UidFont = _pluginInterface.UiBuilder.FontAtlas.NewDelegateFontHandle(e =>
        {
            e.OnPreBuild(tk => tk.AddDalamudAssetFont(Dalamud.DalamudAsset.NotoSansJpMedium, new()
            {
                SizePx = 30
            }));
        });
        GameFont = _pluginInterface.UiBuilder.FontAtlas.NewGameFontHandle(new(GameFontFamilyAndSize.Axis12));
        IconFont = _pluginInterface.UiBuilder.IconFontFixedWidthHandle;

        _fontManager.SetUserSizePx(_configService.Current.FontSizePx);

        var chosen = _configService.Current.FontId;

        if (string.IsNullOrWhiteSpace(chosen))
        {
            if (!string.IsNullOrWhiteSpace(_themeManager.Current?.Id)
                && !string.Equals(_themeManager.Current.Id, ThemeManager.NoneId, StringComparison.OrdinalIgnoreCase))
            {
                chosen = _themeManager.Current?.Typography?.DisplayFont ?? "";
            }
            else
            {
                chosen = "";
            }
        }

        var initialSize = _fontManager.GetUserSizePx();

        _fontManager.LoadOnlyFace(chosen ?? "", new[] { initialSize });

        if (!string.IsNullOrWhiteSpace(chosen))
            _fontManager.TryApply(chosen);

        var persistId = _fontManager.Current?.Id;
        if (!string.IsNullOrWhiteSpace(persistId) &&
            !string.Equals(_configService.Current.FontId, persistId, StringComparison.OrdinalIgnoreCase))
        {
            _configService.Current.FontId = persistId;
            _configService.Save();
        }


    }

    private void EnsureThemeBootstrapped()
    {
        if (_themeBootstrapped) return;
        _themeBootstrapped = true;

        _themeManager.LoadAll();

        var id = _configService.Current.SelectedThemeId;

        if (string.IsNullOrWhiteSpace(id))
        {
            id = ThemeManager.NoneId;
            _configService.Current.SelectedThemeId = id;
            _configService.Save();
        }

        if (!_themeManager.TryApply(id))
        {
            _configService.Current.SelectedThemeId = ThemeManager.NoneId;
            _configService.Save();
            _themeManager.TryApply(ThemeManager.NoneId);
        }
    }

    public IDisposable BeginThemed()
    {
        EnsureThemeBootstrapped();

        var stack = new List<IDisposable>();

        var curId = _themeManager.Current?.Id;

        if (string.IsNullOrWhiteSpace(curId)
            || string.Equals(curId, ThemeManager.NoneId, StringComparison.OrdinalIgnoreCase))
            return new Scope(stack);

        stack.Add(_themeManager.PushImGuiScope());

        var handle = _fontManager.GetCurrentHandle();
        if (handle is not null)
            stack.Add(handle.Push());

        return new Scope(stack);
    }


    private sealed class Scope : IDisposable
    {
        private readonly List<IDisposable> _stack;
        public Scope(List<IDisposable> stack) => _stack = stack;
        public void Dispose()
        {
            for (int i = _stack.Count - 1; i >= 0; --i)
                _stack[i].Dispose();
        }
    }

    public static string DoubleNewLine => Environment.NewLine + Environment.NewLine;
    public ApiController ApiController => _apiController;

    public bool EditTrackerPosition { get; set; }

    public IFontHandle GameFont { get; init; }
    public bool HasValidPenumbraModPath => !(_ipcManager.Penumbra.ModDirectory ?? string.Empty).IsNullOrEmpty() && Directory.Exists(_ipcManager.Penumbra.ModDirectory);

    public IFontHandle IconFont { get; init; }
    public bool IsInGpose => _dalamudUtil.IsInGpose;

    public Dictionary<uint, string> JobData => _dalamudUtil.JobData.Value;
    public string PlayerName => _dalamudUtil.GetPlayerName();

    public IFontHandle UidFont { get; init; }
    public Dictionary<ushort, string> WorldData => _dalamudUtil.WorldData.Value;
    public uint WorldId => _dalamudUtil.GetHomeWorldId();

    public static void AttachToolTip(string text)
    {
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            ImGui.BeginTooltip();
            ImGui.PushTextWrapPos(ImGui.GetFontSize() * 35f);
            if (text.Contains(TooltipSeparator, StringComparison.Ordinal))
            {
                var splitText = text.Split(TooltipSeparator, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < splitText.Length; i++)
                {
                    ImGui.TextUnformatted(splitText[i]);
                    if (i != splitText.Length - 1) ImGui.Separator();
                }
            }
            else
            {
                ImGui.TextUnformatted(text);
            }
            ImGui.PopTextWrapPos();
            ImGui.EndTooltip();
        }
    }

    public static string ByteToString(long bytes, bool addSuffix = true)
    {
        string[] suffix = ["B", "KiB", "MiB", "GiB", "TiB"];
        int i;
        double dblSByte = bytes;
        for (i = 0; i < suffix.Length && bytes >= 1024; i++, bytes /= 1024)
        {
            dblSByte = bytes / 1024.0;
        }

        return addSuffix ? $"{dblSByte:0.00} {suffix[i]}" : $"{dblSByte:0.00}";
    }

    public static void CenterNextWindow(float width, float height, ImGuiCond cond = ImGuiCond.None)
    {
        var center = ImGui.GetMainViewport().GetCenter();
        ImGui.SetNextWindowPos(new Vector2(center.X - width / 2, center.Y - height / 2), cond);
    }

    public static uint Color(byte r, byte g, byte b, byte a)
    { uint ret = a; ret <<= 8; ret += b; ret <<= 8; ret += g; ret <<= 8; ret += r; return ret; }

    public static uint Color(Vector4 color)
    {
        uint ret = (byte)(color.W * 255);
        ret <<= 8;
        ret += (byte)(color.Z * 255);
        ret <<= 8;
        ret += (byte)(color.Y * 255);
        ret <<= 8;
        ret += (byte)(color.X * 255);
        return ret;
    }

    public static void ColorText(string text, Vector4 color)
    {
        using var raiicolor = ImRaii.PushColor(ImGuiCol.Text, color);
        ImGui.TextUnformatted(text);
    }

    public static void ColorTextWrapped(string text, Vector4 color, float wrapPos = 0)
    {
        using var raiicolor = ImRaii.PushColor(ImGuiCol.Text, color);
        TextWrapped(text, wrapPos);
    }

    public static bool CtrlPressed() => (GetKeyState(0xA2) & 0x8000) != 0 || (GetKeyState(0xA3) & 0x8000) != 0;

    public static void DrawGrouped(Action imguiDrawAction, float rounding = 5f, float? expectedWidth = null)
    {
        var cursorPos = ImGui.GetCursorPos();
        using (ImRaii.Group())
        {
            if (expectedWidth != null)
            {
                ImGui.Dummy(new(expectedWidth.Value, 0));
                ImGui.SetCursorPos(cursorPos);
            }

            imguiDrawAction.Invoke();
        }

        ImGui.GetWindowDrawList().AddRect(
            ImGui.GetItemRectMin() - ImGui.GetStyle().ItemInnerSpacing,
            ImGui.GetItemRectMax() + ImGui.GetStyle().ItemInnerSpacing,
            Color(ImGuiColors.DalamudGrey2), rounding);
    }

    public static void DrawGroupedCenteredColorText(string text, Vector4 color, float? maxWidth = null)
    {
        var availWidth = ImGui.GetContentRegionAvail().X;
        var textWidth = ImGui.CalcTextSize(text, wrapWidth: availWidth).X;
        if (maxWidth != null && textWidth > maxWidth * ImGuiHelpers.GlobalScale) textWidth = maxWidth.Value * ImGuiHelpers.GlobalScale;
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (availWidth / 2f) - (textWidth / 2f));
        DrawGrouped(() =>
        {
            ColorTextWrapped(text, color, ImGui.GetCursorPosX() + textWidth);
        }, expectedWidth: maxWidth == null ? null : maxWidth * ImGuiHelpers.GlobalScale);
    }

    public static void DrawOutlinedFont(string text, Vector4 fontColor, Vector4 outlineColor, int thickness)
    {
        var original = ImGui.GetCursorPos();

        using (ImRaii.PushColor(ImGuiCol.Text, outlineColor))
        {
            ImGui.SetCursorPos(original with { Y = original.Y - thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X - thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { Y = original.Y + thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X + thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X - thickness, Y = original.Y - thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X + thickness, Y = original.Y + thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X - thickness, Y = original.Y + thickness });
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original with { X = original.X + thickness, Y = original.Y - thickness });
            ImGui.TextUnformatted(text);
        }

        using (ImRaii.PushColor(ImGuiCol.Text, fontColor))
        {
            ImGui.SetCursorPos(original);
            ImGui.TextUnformatted(text);
            ImGui.SetCursorPos(original);
            ImGui.TextUnformatted(text);
        }
    }

    public static void DrawOutlinedFont(ImDrawListPtr drawList, string text, Vector2 textPos, uint fontColor, uint outlineColor, int thickness)
    {
        drawList.AddText(textPos with { Y = textPos.Y - thickness },
            outlineColor, text);
        drawList.AddText(textPos with { X = textPos.X - thickness },
            outlineColor, text);
        drawList.AddText(textPos with { Y = textPos.Y + thickness },
            outlineColor, text);
        drawList.AddText(textPos with { X = textPos.X + thickness },
            outlineColor, text);
        drawList.AddText(new Vector2(textPos.X - thickness, textPos.Y - thickness),
            outlineColor, text);
        drawList.AddText(new Vector2(textPos.X + thickness, textPos.Y + thickness),
            outlineColor, text);
        drawList.AddText(new Vector2(textPos.X - thickness, textPos.Y + thickness),
            outlineColor, text);
        drawList.AddText(new Vector2(textPos.X + thickness, textPos.Y - thickness),
            outlineColor, text);

        drawList.AddText(textPos, fontColor, text);
        drawList.AddText(textPos, fontColor, text);
    }

    public static void DrawTree(string leafName, Action drawOnOpened, ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags.None)
    {
        using var tree = ImRaii.TreeNode(leafName, flags);
        if (tree)
        {
            drawOnOpened();
        }
    }

    public static Vector4 GetBoolColor(bool input) => input ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudRed;

    public static string GetNotes(List<Pair> pairs)
    {
        StringBuilder sb = new();
        sb.AppendLine(_notesStart);
        foreach (var entry in pairs)
        {
            var note = entry.GetNote();
            if (note.IsNullOrEmpty()) continue;

            sb.Append(entry.UserData.UID).Append(":\"").Append(entry.GetNote()).AppendLine("\"");
        }
        sb.AppendLine(_notesEnd);

        return sb.ToString();
    }

    public static float GetWindowContentRegionWidth()
    {
        return ImGui.GetWindowContentRegionMax().X - ImGui.GetWindowContentRegionMin().X;
    }

    public static bool IsDirectoryWritable(string dirPath, bool throwIfFails = false)
    {
        try
        {
            using FileStream fs = File.Create(
                       Path.Combine(
                           dirPath,
                           Path.GetRandomFileName()
                       ),
                       1,
                       FileOptions.DeleteOnClose);
            return true;
        }
        catch
        {
            if (throwIfFails)
                throw;

            return false;
        }
    }

    public static void ScaledNextItemWidth(float width)
    {
        ImGui.SetNextItemWidth(width * ImGuiHelpers.GlobalScale);
    }

    public static void ScaledSameLine(float offset)
    {
        ImGui.SameLine(offset * ImGuiHelpers.GlobalScale);
    }

    public static void SetScaledWindowSize(float width, bool centerWindow = true)
    {
        var newLineHeight = ImGui.GetCursorPosY();
        ImGui.NewLine();
        newLineHeight = ImGui.GetCursorPosY() - newLineHeight;
        var y = ImGui.GetCursorPos().Y + ImGui.GetWindowContentRegionMin().Y - newLineHeight * 2 - ImGui.GetStyle().ItemSpacing.Y;

        SetScaledWindowSize(width, y, centerWindow, scaledHeight: true);
    }

    public static void SetScaledWindowSize(float width, float height, bool centerWindow = true, bool scaledHeight = false)
    {
        ImGui.SameLine();
        var x = width * ImGuiHelpers.GlobalScale;
        var y = scaledHeight ? height : height * ImGuiHelpers.GlobalScale;

        if (centerWindow)
        {
            CenterWindow(x, y);
        }

        ImGui.SetWindowSize(new Vector2(x, y));
    }

    public static bool ShiftPressed() => (GetKeyState(0xA1) & 0x8000) != 0 || (GetKeyState(0xA0) & 0x8000) != 0;

    public static void TextWrapped(string text, float wrapPos = 0)
    {
        ImGui.PushTextWrapPos(wrapPos);
        ImGui.TextUnformatted(text);
        ImGui.PopTextWrapPos();
    }

    public static Vector4 UploadColor((long, long) data) => data.Item1 == 0 ? ImGuiColors.DalamudGrey :
        data.Item1 == data.Item2 ? ImGuiColors.ParsedGreen : ImGuiColors.DalamudYellow;

    public bool ApplyNotesFromClipboard(string notes, bool overwrite)
    {
        var splitNotes = notes.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries).ToList();
        var splitNotesStart = splitNotes.FirstOrDefault();
        var splitNotesEnd = splitNotes.LastOrDefault();
        if (!string.Equals(splitNotesStart, _notesStart, StringComparison.Ordinal) || !string.Equals(splitNotesEnd, _notesEnd, StringComparison.Ordinal))
        {
            return false;
        }

        splitNotes.RemoveAll(n => string.Equals(n, _notesStart, StringComparison.Ordinal) || string.Equals(n, _notesEnd, StringComparison.Ordinal));

        foreach (var note in splitNotes)
        {
            try
            {
                var splittedEntry = note.Split(":", 2, StringSplitOptions.RemoveEmptyEntries);
                var uid = splittedEntry[0];
                var comment = splittedEntry[1].Trim('"');
                if (_serverConfigurationManager.GetNoteForUid(uid) != null && !overwrite) continue;
                _serverConfigurationManager.SetNoteForUid(uid, comment);
            }
            catch
            {
                Logger.LogWarning("Could not parse {note}", note);
            }
        }

        _serverConfigurationManager.SaveNotes();

        return true;
    }

    public void BigText(string text, Vector4? color = null)
    {
        FontText(text, UidFont, color);
    }

    public void BooleanToColoredIcon(bool value, bool inline = true)
    {
        using var colorgreen = ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.HealerGreen, value);
        using var colorred = ImRaii.PushColor(ImGuiCol.Text, ImGuiColors.DalamudRed, !value);

        if (inline) ImGui.SameLine();

        if (value)
        {
            IconText(FontAwesomeIcon.Check);
        }
        else
        {
            IconText(FontAwesomeIcon.Times);
        }
    }

    public void DrawCacheDirectorySetting()
    {
        ColorTextWrapped(L("UI.UISharedService.fa2a1e62", "Note: The storage folder should be somewhere close to root (i.e. C:\\RavaStorage) in a new empty folder. DO NOT point this to your game folder. DO NOT point this to your Penumbra folder."), ImGuiColors.DalamudYellow);
        var cacheDirectory = _configService.Current.CacheFolder;
        ImGui.InputText($"{L("UI.UISharedService.743E5C4C", "Storage Folder")}##cache", ref cacheDirectory, 255, ImGuiInputTextFlags.ReadOnly);

        ImGui.SameLine();
        using (ImRaii.Disabled(_cacheMonitor.MareWatcher != null))
        {
            if (IconButton(FontAwesomeIcon.Folder))
            {
                FileDialogManager.OpenFolderDialog(L("UI.UISharedService.88e93242", "Pick RavaSync Storage Folder"), (success, path) =>
                {
                    if (!success) return;
                    if (string.IsNullOrWhiteSpace(path)) return;

                    var cachePath = SetupSubDir(path);

                    _isOneDrive =
                        path.Contains("onedrive", StringComparison.OrdinalIgnoreCase) ||
                        cachePath.Contains("onedrive", StringComparison.OrdinalIgnoreCase);

                    _isPenumbraDirectory = string.Equals(
                        Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar),
                        (_ipcManager.Penumbra.ModDirectory ?? string.Empty)
                            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar),
                        StringComparison.OrdinalIgnoreCase);

                    // Ensure our cache dir exists before validation
                    try { Directory.CreateDirectory(cachePath); } catch { }

                    // Validate ONLY the cachePath folder
                    _cacheDirectoryHasOtherFilesThanCache = false;

                    try
                    {
                        var files = Directory.GetFiles(cachePath, "*", SearchOption.TopDirectoryOnly);
                        foreach (var file in files)
                        {
                            var fileName = Path.GetFileNameWithoutExtension(file);

                            if (fileName.Length != 40 && !string.Equals(fileName, "desktop", StringComparison.OrdinalIgnoreCase))
                            {
                                _cacheDirectoryHasOtherFilesThanCache = true;
                                Logger.LogWarning("Found illegal file in {path}: {file}", cachePath, file);
                                break;
                            }
                        }

                        var dirs = Directory.GetDirectories(cachePath, "*", SearchOption.TopDirectoryOnly);
                        if (dirs.Any())
                        {
                            _cacheDirectoryHasOtherFilesThanCache = true;
                            Logger.LogWarning("Found folders in {path} not belonging to RavaSync cache: {dirs}",
                                cachePath, string.Join(", ", dirs));
                        }
                    }
                    catch (Exception ex)
                    {
                        // If we can't enumerate, treat as invalid
                        Logger.LogWarning(ex, "Could not validate cache directory contents at {path}", cachePath);
                        _cacheDirectoryHasOtherFilesThanCache = true;
                    }

                    _isDirectoryWritable = IsDirectoryWritable(cachePath);
                    _cacheDirectoryIsValidPath = PathRegex().IsMatch(cachePath);

                    if (!string.IsNullOrEmpty(cachePath)
                        && Directory.Exists(cachePath)
                        && _isDirectoryWritable
                        && !_isPenumbraDirectory
                        && !_isOneDrive
                        && !_cacheDirectoryHasOtherFilesThanCache
                        && _cacheDirectoryIsValidPath)
                    {
                        _configService.Current.CacheFolder = cachePath;

                        _configService.Current.CacheFolderSubdirMigrationDone = true;

                        _configService.Save();
                        _cacheMonitor.StartMareWatcher(cachePath);
                        _cacheMonitor.InvokeScan();
                    }

                }, _dalamudUtil.IsWine ? @"Z:\" : @"C:\");

            }
        }
        if (_cacheMonitor.MareWatcher != null)
        {
            AttachToolTip(L("UI.UISharedService.945f98ea", "Stop the Monitoring before changing the Storage folder. As long as monitoring is active, you cannot change the Storage folder location."));
        }

        if (_isPenumbraDirectory)
        {
            ColorTextWrapped(L("UI.UISharedService.4a400796", "Do not point the storage path directly to the Penumbra directory. If necessary, make a subfolder in it."), ImGuiColors.DalamudRed);
        }
        else if (_isOneDrive)
        {
            ColorTextWrapped(L("UI.UISharedService.974de3e8", "Do not point the storage path to a folder in OneDrive. Do not use OneDrive folders for any Mod related functionality."), ImGuiColors.DalamudRed);
        }
        else if (!_isDirectoryWritable)
        {
            ColorTextWrapped(L("UI.UISharedService.b5e12f2c", "The folder you selected does not exist or cannot be written to. Please provide a valid path."), ImGuiColors.DalamudRed);
        }
        else if (_cacheDirectoryHasOtherFilesThanCache)
        {
            ColorTextWrapped(L("UI.UISharedService.78f98f45", "Your selected directory has files or directories inside that are not RavaSync related. Use an empty directory or a previous RavaSync Storage directory only."), ImGuiColors.DalamudRed);
        }
        else if (!_cacheDirectoryIsValidPath)
        {
            ColorTextWrapped(L("UI.UISharedService.f8942e27", "Your selected directory contains illegal characters unreadable by FFXIV. ") +
                             "Restrict yourself to latin letters (A-Z), underscores (_), dashes (-) and arabic numbers (0-9).", ImGuiColors.DalamudRed);
        }

        float maxCacheSize = (float)_configService.Current.MaxLocalCacheInGiB;
        if (ImGui.SliderFloat(L("UI.UISharedService.e07b77f5", "Maximum Storage Size in GiB"), ref maxCacheSize, 1f, 200f, "%.2f GiB"))
        {
            _configService.Current.MaxLocalCacheInGiB = maxCacheSize;
            _configService.Save();
        }
        DrawHelpText(L("UI.UISharedService.f04633e6", "The storage is automatically governed by RavaSync. It will clear itself automatically once it reaches the set capacity by removing the oldest unused files. You typically do not need to clear it yourself."));
    }

    public T? DrawCombo<T>(string comboName, IEnumerable<T> comboItems, Func<T?, string> toName,
        Action<T?>? onSelected = null, T? initialSelectedItem = default)
    {
        if (!comboItems.Any()) return default;

        if (!_selectedComboItems.TryGetValue(comboName, out var selectedItem) && selectedItem == null)
        {
            selectedItem = initialSelectedItem;
            _selectedComboItems[comboName] = selectedItem;
        }

        if (ImGui.BeginCombo(comboName, selectedItem == null ? L("UI.UISharedService.1853a0f1", "Unset Value") : toName((T?)selectedItem)))
        {
            foreach (var item in comboItems)
            {
                bool isSelected = EqualityComparer<T>.Default.Equals(item, (T?)selectedItem);
                if (ImGui.Selectable(toName(item), isSelected))
                {
                    _selectedComboItems[comboName] = item!;
                    onSelected?.Invoke(item!);
                }
            }

            ImGui.EndCombo();
        }

        return (T?)_selectedComboItems[comboName];
    }

    public void DrawFileScanState()
    {
        ImGui.AlignTextToFramePadding();
        ImGui.TextUnformatted(L("UI.UISharedService.7844dbd3", "File Scanner Status"));
        ImGui.SameLine();
        if (_cacheMonitor.IsScanRunning)
        {
            ImGui.AlignTextToFramePadding();

            ImGui.TextUnformatted(L("UI.UISharedService.e45a0d1e", "Scan is running"));
            ImGui.TextUnformatted(L("UI.UISharedService.7d4e5a9b", "Current Progress:"));
            ImGui.SameLine();
            ImGui.TextUnformatted(_cacheMonitor.TotalFiles == 1
                ? L("UI.UISharedService.c5f28e99", "Collecting files")
                : string.Format(L("UI.UISharedService.0bf75abb", "Processing {0}/{1} from storage ({2} scanned in)"), _cacheMonitor.CurrentFileProgress, _cacheMonitor.TotalFilesStorage, _cacheMonitor.TotalFiles));
            AttachToolTip(L("UI.UISharedService.689e2427", "Note: it is possible to have more files in storage than scanned in, this is due to the scanner normally ignoring those files but the game loading them in and using them on your character, so they get added to the local storage."));
        }
        else if (_cacheMonitor.HaltScanLocks.Any(f => f.Value > 0))
        {
            ImGui.AlignTextToFramePadding();

            ImGui.TextUnformatted(L("UI.UISharedService.1077a6a1", "Halted (") + string.Join(", ", _cacheMonitor.HaltScanLocks.Where(f => f.Value > 0).Select(locker => locker.Key + ": " + locker.Value + L("UI.UISharedService.1f5f2ade", " halt requests"))) + ")");
            ImGui.SameLine();
            if (ImGui.Button($"{L("UI.UISharedService.33BB6D1B", "Reset halt requests")}##clearlocks"))
            {
                _cacheMonitor.ResetLocks();
            }
        }
        else
        {
            ImGui.TextUnformatted(L("UI.UISharedService.cc1ebdd0", "Idle"));
            if (_configService.Current.InitialScanComplete)
            {
                ImGui.SameLine();
                if (IconTextButton(FontAwesomeIcon.Play, L("UI.UISharedService.86c47f2f", "Force rescan")))
                {
                    _cacheMonitor.InvokeScan();
                }
            }
        }
    }

    public void DrawHelpText(string helpText)
    {
        ImGui.SameLine();
        IconText(FontAwesomeIcon.QuestionCircle, ImGui.GetColorU32(ImGuiCol.TextDisabled));
        AttachToolTip(helpText);
    }

    public void DrawOAuth(ServerStorage selectedServer)
    {
        var oauthToken = selectedServer.OAuthToken;
        _ = ImRaii.PushIndent(10f);
        if (oauthToken == null)
        {
            if (_discordOAuthCheck == null)
            {
                if (IconTextButton(FontAwesomeIcon.QuestionCircle, L("UI.UISharedService.46def04e", "Check if Server supports Discord OAuth2")))
                {
                    _discordOAuthCheck = _serverConfigurationManager.CheckDiscordOAuth(selectedServer.ServerUri);
                }
            }
            else
            {
                if (!_discordOAuthCheck.IsCompleted)
                {
                    ColorTextWrapped(string.Format(L("UI.UISharedService.fdc663c2", "Checking OAuth2 compatibility with {0}"), selectedServer.ServerUri), ImGuiColors.DalamudYellow);
                }
                else
                {
                    if (_discordOAuthCheck.Result != null)
                    {
                        ColorTextWrapped(L("UI.UISharedService.9af27961", "Server is compatible with Discord OAuth2"), ImGuiColors.HealerGreen);
                    }
                    else
                    {
                        ColorTextWrapped(L("UI.UISharedService.e76fe290", "Server is not compatible with Discord OAuth2"), ImGuiColors.DalamudRed);
                    }
                }
            }

            if (_discordOAuthCheck != null && _discordOAuthCheck.IsCompleted)
            {
                if (IconTextButton(FontAwesomeIcon.ArrowRight, L("UI.UISharedService.3935ceab", "Authenticate with Server")))
                {
                    _discordOAuthGetCode = _serverConfigurationManager.GetDiscordOAuthToken(_discordOAuthCheck.Result!, selectedServer.ServerUri, _discordOAuthGetCts.Token);
                }
                else if (_discordOAuthGetCode != null && !_discordOAuthGetCode.IsCompleted)
                {
                    TextWrapped(L("UI.UISharedService.678c2373", "A browser window has been opened, follow it to authenticate. Click the button below if you accidentally closed the window and need to restart the authentication."));
                    if (IconTextButton(FontAwesomeIcon.Ban, L("UI.UISharedService.5a1987cb", "Cancel Authentication")))
                    {
                        _discordOAuthGetCts = _discordOAuthGetCts.CancelRecreate();
                        _discordOAuthGetCode = null;
                    }
                }
                else if (_discordOAuthGetCode != null && _discordOAuthGetCode.IsCompleted)
                {
                    TextWrapped(L("UI.UISharedService.289442f1", "Discord OAuth is completed, status: "));
                    ImGui.SameLine();
                    if (_discordOAuthGetCode.Result != null)
                    {
                        selectedServer.OAuthToken = _discordOAuthGetCode.Result;
                        _discordOAuthGetCode = null;
                        _serverConfigurationManager.Save();
                        ColorTextWrapped(L("UI.UISharedService.62e42cc4", "Success"), ImGuiColors.HealerGreen);
                    }
                    else
                    {
                        ColorTextWrapped(L("UI.UISharedService.ffad77e7", "Failed, please check /xllog for more information"), ImGuiColors.DalamudRed);
                    }
                }
            }
        }

        if (oauthToken != null)
        {
            if (!_oauthTokenExpiry.TryGetValue(oauthToken, out DateTime tokenExpiry))
            {
                try
                {
                    var handler = new JwtSecurityTokenHandler();
                    var jwt = handler.ReadJwtToken(oauthToken);
                    tokenExpiry = _oauthTokenExpiry[oauthToken] = jwt.ValidTo;
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex, "Could not parse OAuth token, deleting");
                    selectedServer.OAuthToken = null;
                    _serverConfigurationManager.Save();
                }
            }

            if (tokenExpiry > DateTime.UtcNow)
            {
                ColorTextWrapped(string.Format(L("UI.UISharedService.bfac322d", "OAuth2 is enabled, linked to: Discord User {0}"), _serverConfigurationManager.GetDiscordUserFromToken(selectedServer)), ImGuiColors.HealerGreen);
                TextWrapped(string.Format(L("UI.UISharedService.24916687", "The OAuth2 token will expire on {0:yyyy-MM-dd} and automatically renew itself during login on or after {1:yyyy-MM-dd}."), tokenExpiry, (tokenExpiry - TimeSpan.FromDays(7))));
                using (ImRaii.Disabled(!CtrlPressed()))
                {
                    if (IconTextButton(FontAwesomeIcon.Exclamation, L("UI.UISharedService.b4900dec", "Renew OAuth2 token manually")) && CtrlPressed())
                    {
                        _ = _tokenProvider.TryUpdateOAuth2LoginTokenAsync(selectedServer, forced: true)
                            .ContinueWith((_) => _apiController.CreateConnectionsAsync());
                    }
                }
                DrawHelpText(L("UI.UISharedService.45df43a2", "Hold CTRL to manually refresh your OAuth2 token. Normally you do not need to do this."));
                ImGuiHelpers.ScaledDummy(10f);

                if ((_discordOAuthUIDs == null || _discordOAuthUIDs.IsCompleted)
                    && IconTextButton(FontAwesomeIcon.Question, L("UI.UISharedService.f64f7977", "Check Discord Connection")))
                {
                    _discordOAuthUIDs = _serverConfigurationManager.GetUIDsWithDiscordToken(selectedServer.ServerUri, oauthToken);
                }
                else if (_discordOAuthUIDs != null)
                {
                    if (!_discordOAuthUIDs.IsCompleted)
                    {
                        ColorTextWrapped(L("UI.UISharedService.16b75398", "Checking UIDs on Server"), ImGuiColors.DalamudYellow);
                    }
                    else
                    {
                        var foundUids = _discordOAuthUIDs.Result?.Count ?? 0;
                        var primaryUid = _discordOAuthUIDs.Result?.FirstOrDefault() ?? new KeyValuePair<string, string>(string.Empty, string.Empty);
                        var vanity = string.IsNullOrEmpty(primaryUid.Value) ? "-" : primaryUid.Value;
                        if (foundUids > 0)
                        {
                            ColorTextWrapped(string.Format(L("UI.UISharedService.4bf10b0a", "Found {0} associated UIDs on the server, Primary UID: {1} (Vanity UID: {2})"), foundUids, primaryUid.Key, vanity),
                                ImGuiColors.HealerGreen);
                        }
                        else
                        {
                            ColorTextWrapped(L("UI.UISharedService.7a05b968", "Found no UIDs associated to this linked OAuth2 account"), ImGuiColors.DalamudRed);
                        }
                    }
                }
            }
            else
            {
                ColorTextWrapped(L("UI.UISharedService.a280d388", "The OAuth2 token is stale and expired. Please renew the OAuth2 connection."), ImGuiColors.DalamudRed);
                if (IconTextButton(FontAwesomeIcon.Exclamation, L("UI.UISharedService.182d1d72", "Renew OAuth2 connection")))
                {
                    selectedServer.OAuthToken = null;
                    _serverConfigurationManager.Save();
                    _ = _serverConfigurationManager.CheckDiscordOAuth(selectedServer.ServerUri)
                        .ContinueWith(async (urlTask) =>
                        {
                            var url = await urlTask.ConfigureAwait(false);
                            var token = await _serverConfigurationManager.GetDiscordOAuthToken(url!, selectedServer.ServerUri, CancellationToken.None).ConfigureAwait(false);
                            selectedServer.OAuthToken = token;
                            _serverConfigurationManager.Save();
                            await _apiController.CreateConnectionsAsync().ConfigureAwait(false);
                        });
                }
            }

            DrawUnlinkOAuthButton(selectedServer);
        }
    }

    public bool DrawOtherPluginState()
    {
        ImGui.TextUnformatted(L("UI.UISharedService.0f1c0453", "Mandatory Plugins:"));

        ImGui.SameLine(150);
        ColorText(L("UI.UISharedService.d2129b83", "Penumbra"), GetBoolColor(_penumbraExists));
        AttachToolTip(L("UI.UISharedService.2d8b6db0", "Penumbra is ") + (_penumbraExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.56519852", "Glamourer"), GetBoolColor(_glamourerExists));
        AttachToolTip(L("UI.UISharedService.7115133c", "Glamourer is ") + (_glamourerExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.TextUnformatted(L("UI.UISharedService.52f5707f", "Optional Plugins:"));
        ImGui.SameLine(150);
        ColorText(L("UI.UISharedService.f9105374", "SimpleHeels"), GetBoolColor(_heelsExists));
        AttachToolTip(L("UI.UISharedService.d34f1d6c", "SimpleHeels is ") + (_heelsExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.04d09974", "Customize+"), GetBoolColor(_customizePlusExists));
        AttachToolTip(L("UI.UISharedService.34bdebff", "Customize+ is ") + (_customizePlusExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.2ab5b71f", "Honorific"), GetBoolColor(_honorificExists));
        AttachToolTip(L("UI.UISharedService.698dc824", "Honorific is ") + (_honorificExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.3b39cf55", "Moodles"), GetBoolColor(_moodlesExists));
        AttachToolTip(L("UI.UISharedService.4e9b8581", "Moodles is ") + (_moodlesExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.1fd2d64f", "PetNicknames"), GetBoolColor(_petNamesExists));
        AttachToolTip(L("UI.UISharedService.d388ba52", "PetNicknames is ") + (_petNamesExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        ImGui.SameLine();
        ColorText(L("UI.UISharedService.6473040f", "Brio"), GetBoolColor(_brioExists));
        AttachToolTip(L("UI.UISharedService.d3f2d22b", "Brio is ") + (_brioExists ? L("UI.UISharedService.9cd69517", "available and up to date.") : L("UI.UISharedService.50df3ed9", "unavailable or not up to date.")));

        if (!_penumbraExists || !_glamourerExists)
        {
            ImGui.TextColored(ImGuiColors.DalamudRed, L("UI.UISharedService.c325f773", "You need to install both Penumbra and Glamourer and keep them up to date to use RavaSync."));
            return false;
        }

        return true;
    }

    public int DrawServiceSelection(bool selectOnChange = false, bool showConnect = true)
    {
        string[] comboEntries = _serverConfigurationManager.GetServerNames();

        if (_serverSelectionIndex == -1)
        {
            _serverSelectionIndex = Array.IndexOf(_serverConfigurationManager.GetServerApiUrls(), _serverConfigurationManager.CurrentApiUrl);
        }
        if (_serverSelectionIndex == -1 || _serverSelectionIndex >= comboEntries.Length)
        {
            _serverSelectionIndex = 0;
        }
        for (int i = 0; i < comboEntries.Length; i++)
        {
            if (string.Equals(_serverConfigurationManager.CurrentServer?.ServerName, comboEntries[i], StringComparison.OrdinalIgnoreCase))
                comboEntries[i] += " [Current]";
        }
        if (ImGui.BeginCombo(L("UI.UISharedService.a5f2b079", "Select Service"), comboEntries[_serverSelectionIndex]))
        {
            for (int i = 0; i < comboEntries.Length; i++)
            {
                bool isSelected = _serverSelectionIndex == i;
                if (ImGui.Selectable(comboEntries[i], isSelected))
                {
                    _serverSelectionIndex = i;
                    if (selectOnChange)
                    {
                        _serverConfigurationManager.SelectServer(i);
                    }
                }

                if (isSelected)
                {
                    ImGui.SetItemDefaultFocus();
                }
            }

            ImGui.EndCombo();
        }

        if (showConnect)
        {
            ImGui.SameLine();
            var text = "Connect";
            if (_serverSelectionIndex == _serverConfigurationManager.CurrentServerIndex) text = "Reconnect";
            if (IconTextButton(FontAwesomeIcon.Link, text))
            {
                _serverConfigurationManager.SelectServer(_serverSelectionIndex);
                _ = _apiController.CreateConnectionsAsync();
            }
        }

        if (ImGui.TreeNode(L("UI.UISharedService.424a011c", "Add Custom Service")))
        {
            ImGui.SetNextItemWidth(250);
            ImGui.InputText(L("UI.UISharedService.96953994", "Custom Service URI"), ref _customServerUri, 255);
            ImGui.SetNextItemWidth(250);
            ImGui.InputText(L("UI.UISharedService.d656c037", "Custom Service Name"), ref _customServerName, 255);
            if (IconTextButton(FontAwesomeIcon.Plus, L("UI.UISharedService.decdc424", "Add Custom Service"))
                && !string.IsNullOrEmpty(_customServerUri)
                && !string.IsNullOrEmpty(_customServerName))
            {
                _serverConfigurationManager.AddServer(new ServerStorage()
                {
                    ServerName = _customServerName,
                    ServerUri = _customServerUri,
                    UseOAuth2 = false
                });
                _customServerName = string.Empty;
                _customServerUri = string.Empty;
                _configService.Save();
            }
            ImGui.TreePop();
        }

        return _serverSelectionIndex;
    }

    public void DrawUIDComboForAuthentication(int indexOffset, Authentication item, string serverUri, ILogger? logger = null)
    {
        using (ImRaii.Disabled(_discordOAuthUIDs == null))
        {
            var aliasPairs = _discordOAuthUIDs?.Result?.Select(t => new UIDAliasPair(t.Key, t.Value)).ToList() ?? [new UIDAliasPair(item.UID ?? null, null)];
            var uidComboName = "UID###" + item.CharacterName + item.WorldId + serverUri + indexOffset + aliasPairs.Count;
            DrawCombo(uidComboName, aliasPairs,
                (v) =>
                {
                    if (v is null)
                        return "No UID set";

                    if (!string.IsNullOrEmpty(v.Alias))
                    {
                        return $"{v.UID} ({v.Alias})";
                    }

                    if (string.IsNullOrEmpty(v.UID))
                        return "No UID set";

                    return $"{v.UID}";
                },
                (v) =>
                {
                    if (!string.Equals(v?.UID ?? null, item.UID, StringComparison.Ordinal))
                    {
                        item.UID = v?.UID ?? null;
                        _serverConfigurationManager.Save();
                    }
                },
                aliasPairs.Find(f => string.Equals(f.UID, item.UID, StringComparison.Ordinal)) ?? default);
        }

        if (_discordOAuthUIDs == null)
        {
            AttachToolTip(L("UI.UISharedService.c1d6057f", "Use the button above to update your UIDs from the service before you can assign UIDs to characters."));
        }
    }

    public void DrawUnlinkOAuthButton(ServerStorage selectedServer)
    {
        using (ImRaii.Disabled(!CtrlPressed()))
        {
            if (IconTextButton(FontAwesomeIcon.Trash, L("UI.UISharedService.003a8c79", "Unlink OAuth2 Connection")) && UiSharedService.CtrlPressed())
            {
                selectedServer.OAuthToken = null;
                _serverConfigurationManager.Save();
                ResetOAuthTasksState();
            }
        }
        DrawHelpText(L("UI.UISharedService.9bae9b62", "Hold CTRL to unlink the current OAuth2 connection."));
    }

    public void DrawUpdateOAuthUIDsButton(ServerStorage selectedServer)
    {
        if (!selectedServer.UseOAuth2)
            return;

        using (ImRaii.Disabled(string.IsNullOrEmpty(selectedServer.OAuthToken)))
        {
            if ((_discordOAuthUIDs == null || _discordOAuthUIDs.IsCompleted)
                && IconTextButton(FontAwesomeIcon.ArrowsSpin, L("UI.UISharedService.735f6a93", "Update UIDs from Service"))
                && !string.IsNullOrEmpty(selectedServer.OAuthToken))
            {
                _discordOAuthUIDs = _serverConfigurationManager.GetUIDsWithDiscordToken(selectedServer.ServerUri, selectedServer.OAuthToken);
            }
        }
        DateTime tokenExpiry = DateTime.MinValue;
        if (!string.IsNullOrEmpty(selectedServer.OAuthToken) && !_oauthTokenExpiry.TryGetValue(selectedServer.OAuthToken, out tokenExpiry))
        {
            try
            {
                var handler = new JwtSecurityTokenHandler();
                var jwt = handler.ReadJwtToken(selectedServer.OAuthToken);
                tokenExpiry = _oauthTokenExpiry[selectedServer.OAuthToken] = jwt.ValidTo;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Could not parse OAuth token, deleting");
                selectedServer.OAuthToken = null;
                _serverConfigurationManager.Save();
                tokenExpiry = DateTime.MinValue;
            }
        }
        if (string.IsNullOrEmpty(selectedServer.OAuthToken) || tokenExpiry < DateTime.UtcNow)
        {
            ColorTextWrapped(L("UI.UISharedService.6764d997", "You have no OAuth token or the OAuth token is expired. Please use the Service Configuration to link your OAuth2 account or refresh the token."), ImGuiColors.DalamudRed);
        }
    }

    public Vector2 GetIconButtonSize(FontAwesomeIcon icon)
    {
        using var font = IconFont.Push();
        return ImGuiHelpers.GetButtonSize(icon.ToIconString());
    }

    public Vector2 GetIconSize(FontAwesomeIcon icon)
    {
        using var font = IconFont.Push();
        return ImGui.CalcTextSize(icon.ToIconString());
    }

    public float GetIconTextButtonSize(FontAwesomeIcon icon, string text)
    {
        Vector2 vector;
        using (IconFont.Push())
            vector = ImGui.CalcTextSize(icon.ToIconString());

        Vector2 vector2 = ImGui.CalcTextSize(text);
        float num = 3f * ImGuiHelpers.GlobalScale;
        return vector.X + vector2.X + ImGui.GetStyle().FramePadding.X * 2f + num;
    }

    public bool IconButton(FontAwesomeIcon icon, float? height = null)
    {
        string text = icon.ToIconString();

        ImGui.PushID(text);
        Vector2 vector;
        using (IconFont.Push())
            vector = ImGui.CalcTextSize(text);
        ImDrawListPtr windowDrawList = ImGui.GetWindowDrawList();
        Vector2 cursorScreenPos = ImGui.GetCursorScreenPos();
        float x = vector.X + ImGui.GetStyle().FramePadding.X * 2f;
        float frameHeight = height ?? ImGui.GetFrameHeight();
        bool result = ImGui.Button(string.Empty, new Vector2(x, frameHeight));
        Vector2 pos = new Vector2(cursorScreenPos.X + ImGui.GetStyle().FramePadding.X,
            cursorScreenPos.Y + (height ?? ImGui.GetFrameHeight()) / 2f - (vector.Y / 2f));
        using (IconFont.Push())
            windowDrawList.AddText(pos, ImGui.GetColorU32(ImGuiCol.Text), text);
        ImGui.PopID();

        return result;
    }

    public void IconText(FontAwesomeIcon icon, uint color)
    {
        FontText(icon.ToIconString(), IconFont, color);
    }

    public void IconText(FontAwesomeIcon icon, Vector4? color = null)
    {
        IconText(icon, color == null ? ImGui.GetColorU32(ImGuiCol.Text) : ImGui.GetColorU32(color.Value));
    }

    public bool IconTextButton(FontAwesomeIcon icon, string text, float? width = null, bool isInPopup = false)
    {
        return IconTextButtonInternal(icon, text,
            isInPopup ? ColorHelpers.RgbaUintToVector4(ImGui.GetColorU32(ImGuiCol.PopupBg)) : null,
            width <= 0 ? null : width);
    }

    public IDalamudTextureWrap LoadImage(byte[] imageData)
    {
        return _textureProvider.CreateFromImageAsync(imageData).Result;
    }

    public void LoadLocalization(string languageCode)
    {
        var code = (languageCode ?? "en").Trim();

        if (string.IsNullOrWhiteSpace(code))
            code = "en";
        try
        {
            _localization.SetupWithFallbacks();

            if (code.Equals("en", StringComparison.OrdinalIgnoreCase))
            {
                Strings.ToS = new Strings.ToSStrings();
                return;
            }

            string? raw = null;

            raw = TryReadEmbeddedLocalizationJson(code);

            if (raw == null)
            {
                var asmDir = Path.GetDirectoryName(_pluginInterface.AssemblyLocation.FullName);
                if (!string.IsNullOrEmpty(asmDir))
                {
                    var locDir = Path.Combine(asmDir, "Localization");
                    var locPath = Path.Combine(locDir, $"{code}.json");
                    if (File.Exists(locPath))
                        raw = File.ReadAllText(locPath, Encoding.UTF8);
                    else
                        Logger.LogWarning("Localization file not found: {Path}. Falling back to English.", locPath);
                }
            }

            if (raw == null)
            {
                Strings.ToS = new Strings.ToSStrings();
                return;
            }

            var normalized = NormalizeToCheapLocSchema(raw);
            SetupCheapLoc(normalized, typeof(UiSharedService).Assembly);
            Strings.ToS = new Strings.ToSStrings();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to load localization '{Lang}'. Falling back to English.", code);
            _localization.SetupWithFallbacks();
            Strings.ToS = new Strings.ToSStrings();
        }
    }

    private string? TryReadEmbeddedLocalizationJson(string code)
    {
        try
        {
            var asm = typeof(UiSharedService).Assembly;
            var names = asm.GetManifestResourceNames();

            var wanted = names.FirstOrDefault(n =>
                n.EndsWith($".Localization.{code}.json", StringComparison.OrdinalIgnoreCase) ||
                n.EndsWith($".Localization.{code.ToLowerInvariant()}.json", StringComparison.OrdinalIgnoreCase));

            if (wanted == null)
            {
                wanted = names.FirstOrDefault(n =>
                    n.EndsWith($".{code}.json", StringComparison.OrdinalIgnoreCase));
            }

            if (wanted == null)
            {
                Logger.LogWarning("Embedded localization resource not found for '{Lang}'. Available: {Count}", code, names.Length);
                return null;
            }

            using var stream = asm.GetManifestResourceStream(wanted);
            if (stream == null) return null;

            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            return reader.ReadToEnd();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to read embedded localization for '{Lang}'", code);
            return null;
        }
    }

    private static string NormalizeToCheapLocSchema(string rawJson)
    {
        var token = JToken.Parse(rawJson);

        if (token is not JObject obj)
            return rawJson;

        var outObj = new JObject();

        foreach (var prop in obj.Properties())
        {
            var v = prop.Value;

            if (v.Type == JTokenType.String)
            {
                outObj[prop.Name] = new JObject
                {
                    ["message"] = v.Value<string>() ?? string.Empty,
                    ["description"] = prop.Name,
                };
                continue;
            }

            if (v is JObject o)
            {
                var msg = o["message"]?.Value<string>()
                          ?? o["Message"]?.Value<string>()
                          ?? o["text"]?.Value<string>()
                          ?? o["Text"]?.Value<string>();

                outObj[prop.Name] = new JObject
                {
                    ["message"] = msg ?? string.Empty,
                    ["description"] = o["description"]?.Value<string>() ?? o["Description"]?.Value<string>() ?? prop.Name,
                };
                continue;
            }

            outObj[prop.Name] = new JObject
            {
                ["message"] = v.ToString(Formatting.None),
                ["description"] = prop.Name,
            };
        }

        return outObj.ToString(Formatting.None);
    }

    private static void SetupCheapLoc(string normalizedJson, Assembly callingAssembly)
    {
        // CheapLoc is shipped with Dalamud; we call it via reflection so we don't need a direct dependency.
        var cheapLocAsm = AppDomain.CurrentDomain
            .GetAssemblies()
            .FirstOrDefault(a => string.Equals(a.GetName().Name, "CheapLoc", StringComparison.OrdinalIgnoreCase));

        if (cheapLocAsm == null)
            throw new InvalidOperationException("CheapLoc assembly not found in AppDomain.");

        var locType = cheapLocAsm.GetType("CheapLoc.Loc", throwOnError: true);
        var setup = locType.GetMethod("Setup", new[] { typeof(string), typeof(Assembly) });

        if (setup == null)
            throw new MissingMethodException("CheapLoc.Loc.Setup(string, Assembly) not found.");

        setup.Invoke(null, new object[] { normalizedJson, callingAssembly });
    }

    public string L(string key, string fallback)
    {
        if (string.IsNullOrWhiteSpace(key)) return fallback ?? string.Empty;

        try
        {
            return CheapLoc.Loc.Localize(key, fallback ?? string.Empty);
        }
        catch
        {
            return fallback ?? string.Empty;
        }
    }

    public string L(string key) => L(key, key);


    /// <summary>
    /// Localize then format (string.Format) with provided args. If formatting fails, returns the localized text unformatted.
    /// </summary>
    public string LF(string key, string fallback, params object[] args)
    {
        var s = L(key, fallback);
        if (args == null || args.Length == 0) return s;
        try
        {
            return string.Format(s, args);
        }
        catch
        {
            return s;
        }
    }


    internal static void DistanceSeparator()
    {
        ImGuiHelpers.ScaledDummy(5);
        ImGui.Separator();
        ImGuiHelpers.ScaledDummy(5);
    }

    [LibraryImport("user32")]
    internal static partial short GetKeyState(int nVirtKey);

    internal void ResetOAuthTasksState()
    {
        _discordOAuthCheck = null;
        _discordOAuthGetCts = _discordOAuthGetCts.CancelRecreate();
        _discordOAuthGetCode = null;
        _discordOAuthUIDs = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing) return;

        base.Dispose(disposing);

        UidFont.Dispose();
        GameFont.Dispose();
    }

    private static void CenterWindow(float width, float height, ImGuiCond cond = ImGuiCond.None)
    {
        var center = ImGui.GetMainViewport().GetCenter();
        ImGui.SetWindowPos(new Vector2(center.X - width / 2, center.Y - height / 2), cond);
    }

    [GeneratedRegex(@"^(?:[a-zA-Z]:\\[\w\s\-\\]+?|\/(?:[\w\s\-\/])+?)$", RegexOptions.ECMAScript, 5000)]
    private static partial Regex PathRegex();

    private static void FontText(string text, IFontHandle font, Vector4? color = null)
    {
        FontText(text, font, color == null ? ImGui.GetColorU32(ImGuiCol.Text) : ImGui.GetColorU32(color.Value));
    }

    private static void FontText(string text, IFontHandle font, uint color)
    {
        using var pushedFont = font.Push();
        using var pushedColor = ImRaii.PushColor(ImGuiCol.Text, color);
        ImGui.TextUnformatted(text);
    }

    private bool IconTextButtonInternal(FontAwesomeIcon icon, string text, Vector4? defaultColor = null, float? width = null)
    {
        int num = 0;
        if (defaultColor.HasValue)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, defaultColor.Value);
            num++;
        }

        ImGui.PushID(text);
        Vector2 vector;
        using (IconFont.Push())
            vector = ImGui.CalcTextSize(icon.ToIconString());
        Vector2 vector2 = ImGui.CalcTextSize(text);
        ImDrawListPtr windowDrawList = ImGui.GetWindowDrawList();
        Vector2 cursorScreenPos = ImGui.GetCursorScreenPos();
        float num2 = 3f * ImGuiHelpers.GlobalScale;
        float x = width ?? vector.X + vector2.X + ImGui.GetStyle().FramePadding.X * 2f + num2;
        float frameHeight = ImGui.GetFrameHeight();
        bool result = ImGui.Button(string.Empty, new Vector2(x, frameHeight));
        Vector2 pos = new Vector2(cursorScreenPos.X + ImGui.GetStyle().FramePadding.X, cursorScreenPos.Y + ImGui.GetStyle().FramePadding.Y);
        using (IconFont.Push())
            windowDrawList.AddText(pos, ImGui.GetColorU32(ImGuiCol.Text), icon.ToIconString());
        Vector2 pos2 = new Vector2(pos.X + vector.X + num2, cursorScreenPos.Y + ImGui.GetStyle().FramePadding.Y);
        windowDrawList.AddText(pos2, ImGui.GetColorU32(ImGuiCol.Text), text);
        ImGui.PopID();
        if (num > 0)
        {
            ImGui.PopStyleColor(num);
        }

        return result;
    }

    public static Vector4 Vector4FromColorString(string hex)
    {
        if (string.IsNullOrWhiteSpace(hex))
            return new Vector4(1f, 1f, 1f, 1f);

        if (hex.StartsWith("#")) hex = hex[1..];
        if (hex.Length == 6) hex += "FF"; // add alpha if missing

        try
        {
            var r = Convert.ToInt32(hex[..2], 16) / 255f;
            var g = Convert.ToInt32(hex.Substring(2, 2), 16) / 255f;
            var b = Convert.ToInt32(hex.Substring(4, 2), 16) / 255f;
            var a = Convert.ToInt32(hex.Substring(6, 2), 16) / 255f;
            return new Vector4(r, g, b, a);
        }
        catch
        {
            return new Vector4(1f, 1f, 1f, 1f);
        }
    }

    private static string SetupSubDir(string selectedFolder)
    {
        if (string.IsNullOrWhiteSpace(selectedFolder)) return string.Empty;

        var full = Path.GetFullPath(selectedFolder);
        full = Path.TrimEndingDirectorySeparator(full);

        // If they already selected the subdir itself, keep it.
        var leaf = Path.GetFileName(full);
        if (leaf.Equals(RavaCacheSubdirName, StringComparison.OrdinalIgnoreCase))
            return full;

        return Path.Combine(full, RavaCacheSubdirName);
    }

    public static string ColorStringFromVector4(Vector4 c, bool forceAlpha = false)
    {
        byte r = (byte)Math.Clamp((int)Math.Round(c.X * 255f), 0, 255);
        byte g = (byte)Math.Clamp((int)Math.Round(c.Y * 255f), 0, 255);
        byte b = (byte)Math.Clamp((int)Math.Round(c.Z * 255f), 0, 255);
        byte a = (byte)Math.Clamp((int)Math.Round(c.W * 255f), 0, 255);
    
        // Emit #RRGGBB if alpha is opaque, otherwise #RRGGBBAA (or always include if forceAlpha)
        return (!forceAlpha && a == 255)
            ? $"#{r:X2}{g:X2}{b:X2}"
            : $"#{r:X2}{g:X2}{b:X2}{a:X2}";
    }

    public sealed record IconScaleData(Vector2 IconSize, Vector2 NormalizedIconScale, float OffsetX, float IconScaling);
    private record UIDAliasPair(string? UID, string? Alias);

}