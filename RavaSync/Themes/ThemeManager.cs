using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility.Raii;
using System.Numerics;
using System.Text.Json;

namespace RavaSync.Themes;

public interface IThemeManager
{
    IReadOnlyList<Theme> BuiltIn { get; }
    IReadOnlyList<Theme> Custom { get; }
    Theme Current { get; }
    event Action<Theme> ThemeChanged;

    void LoadAll();
    bool TryApply(string id);
    void SaveCustom(Theme theme);
    IDisposable PushImGuiScope();
    bool IsBuiltIn(string id);
    bool IsCustom(string id);
    Theme? GetById(string id);
    void DeleteCustom(string id);
    bool RenameCustom(string oldId, string newName);
}

public sealed class ThemeManager : IThemeManager
{
    private readonly string _appDataDir;
    private readonly string _pluginDir;
    private readonly string _pluginThemesDir; 
    private readonly List<Theme> _builtIn = new();
    private readonly List<Theme> _custom = new();
    private Theme _current = new Theme { Id = NoneId, Name = "No Theme" };
    public event Action<Theme>? ThemeChanged;
    public const string NoneId = "none";

    public ThemeManager(string appDataDir, string pluginDir) 
    {
        _appDataDir = Path.Combine(appDataDir, "Themes");
        Directory.CreateDirectory(_appDataDir);

        // set plugin dirs
        _pluginDir = pluginDir;
        _pluginThemesDir = Path.Combine(_pluginDir, "Themes");
    }

    public IReadOnlyList<Theme> BuiltIn => _builtIn;
    public IReadOnlyList<Theme> Custom => _custom;
    public Theme Current => _current;

    public bool IsBuiltIn(string id) =>
    _builtIn.Any(t => t.Id.Equals(id, StringComparison.OrdinalIgnoreCase));

    public bool IsCustom(string id) =>
        _custom.Any(t => t.Id.Equals(id, StringComparison.OrdinalIgnoreCase));

    public Theme? GetById(string id) =>
        _custom.Concat(_builtIn).FirstOrDefault(t => t.Id.Equals(id, StringComparison.OrdinalIgnoreCase));

    public void DeleteCustom(string id)
    {
        var path = Path.Combine(_appDataDir, $"{id}.json");
        if (File.Exists(path))
            File.Delete(path);
        LoadAll();
    }

    public bool RenameCustom(string oldId, string newName)
    {
        if (string.IsNullOrWhiteSpace(oldId) || string.IsNullOrWhiteSpace(newName))
            return false;

        // must be a custom theme
        var cur = _custom.FirstOrDefault(t => t.Id.Equals(oldId, StringComparison.OrdinalIgnoreCase));
        if (cur == null) return false;

        var newId = Slug(newName);
        var oldPath = Path.Combine(_appDataDir, $"{oldId}.json");
        var newPath = Path.Combine(_appDataDir, $"{newId}.json");

        try
        {
            // If renaming to the same id, just update the Name field.
            if (!oldId.Equals(newId, StringComparison.OrdinalIgnoreCase) && File.Exists(newPath))
            {
                // avoid collision: append suffix
                newId = $"{newId}_copy";
                newPath = Path.Combine(_appDataDir, $"{newId}.json");
            }

            // load, update fields
            var t = JsonSerializer.Deserialize<Theme>(File.ReadAllText(oldPath)) ?? cur;
            t.Name = newName.Trim();
            t.Id = newId;

            // write new file first
            File.WriteAllText(newPath, JsonSerializer.Serialize(t, new JsonSerializerOptions { WriteIndented = true }));

            // remove old if path differs
            if (!oldPath.Equals(newPath, StringComparison.OrdinalIgnoreCase) && File.Exists(oldPath))
                File.Delete(oldPath);

            // reload caches
            LoadAll();

            // update current if we had this one active
            if (string.Equals(_current.Id, oldId, StringComparison.OrdinalIgnoreCase))
                _current = GetById(newId) ?? t;

            ThemeChanged?.Invoke(_current);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public void LoadAll()
    {
        _builtIn.Clear();
        _custom.Clear();

        // 1) built-ins: prefer <plugin>\Themes\*.json, fall back to <plugin>\theme.*.json
        if (Directory.Exists(_pluginThemesDir))
        {
            foreach (var file in Directory.EnumerateFiles(_pluginThemesDir, "theme.*.json"))
                TryAddTheme(file, _builtIn);
        }
        else
        {
            foreach (var file in Directory.EnumerateFiles(_pluginDir, "theme.*.json"))
                TryAddTheme(file, _builtIn);
        }

        // 2) user themes: %AppData%\RavaSync\Themes\*.json
        if (Directory.Exists(_appDataDir))
        {
            foreach (var file in Directory.EnumerateFiles(_appDataDir, "*.json"))
                TryAddTheme(file, _custom);
        }

        // sanity: drop empties
        _builtIn.RemoveAll(b => string.IsNullOrWhiteSpace(b.Id));
        _custom.RemoveAll(c => string.IsNullOrWhiteSpace(c.Id));

        if (_builtIn.Count == 0 && _custom.Count == 0)
        {
            _builtIn.Add(new Theme { Id = "fallback", Name = "Fallback" });
        }
    }

    private static void TryAddTheme(string path, List<Theme> target)
    {
        try
        {
            var t = JsonSerializer.Deserialize<Theme>(File.ReadAllText(path));
            if (t != null) target.Add(t);
        }
        catch
        {
        }
    }


    public bool TryApply(string id)
    {
        if (string.Equals(id, NoneId, StringComparison.OrdinalIgnoreCase))
        {
            _current = new Theme { Id = NoneId, Name = "No Theme" };
            ThemeChanged?.Invoke(_current);
            return true;
        }

        var theme = _custom.Concat(_builtIn).FirstOrDefault(t => t.Id.Equals(id, StringComparison.OrdinalIgnoreCase));
        if (theme == null) return false;

        _current = ResolveInheritance(theme);
        ThemeChanged?.Invoke(_current);
        return true;
    }


    public void SaveCustom(Theme theme)
    {
        if (string.IsNullOrWhiteSpace(theme.Id)) theme.Id = Slug(theme.Name);
        var path = Path.Combine(_appDataDir, $"{theme.Id}.json");
        File.WriteAllText(path, JsonSerializer.Serialize(theme, new JsonSerializerOptions { WriteIndented = true }));
        LoadAll();
    }

    private static string Slug(string value) =>
        string.Join("_", value.Split(Path.GetInvalidFileNameChars().Concat(new[] { ' ' }).ToArray(), StringSplitOptions.RemoveEmptyEntries))
            .ToLowerInvariant();

    private Theme ResolveInheritance(Theme t)
    {
        if (string.IsNullOrEmpty(t.Inherits)) return t;
        var parent = _custom.Concat(_builtIn).FirstOrDefault(x => x.Id == t.Inherits);
        if (parent == null) return t;
        var merged = JsonSerializer.Deserialize<Theme>(JsonSerializer.Serialize(parent))!;
        var child = JsonSerializer.Deserialize<Theme>(JsonSerializer.Serialize(t))!;
        merged.Colors = Merge(merged.Colors, child.Colors);
        merged.Typography = child.Typography ?? merged.Typography;
        merged.Effects = child.Effects ?? merged.Effects;
        merged.Assets = child.Assets ?? merged.Assets;
        return merged;
    }

    private static Colors Merge(Colors a, Colors b)
    {
        string Pick(string ai, string bi) => string.IsNullOrWhiteSpace(bi) ? ai : bi;
        return new Colors
        {
            Primary = Pick(a.Primary, b.Primary),
            Accent = Pick(a.Accent, b.Accent),
            Background = Pick(a.Background, b.Background),
            BackgroundAlt = Pick(a.BackgroundAlt, b.BackgroundAlt),
            Surface = Pick(a.Surface, b.Surface),
            Text = Pick(a.Text, b.Text),
            MutedText = Pick(a.MutedText, b.MutedText),
            Success = Pick(a.Success, b.Success),
            Warning = Pick(a.Warning, b.Warning),
            Danger = Pick(a.Danger, b.Danger),
            Info = Pick(a.Info, b.Info),
            Border = Pick(a.Border, b.Border),
            Highlight = Pick(a.Highlight, b.Highlight)
        };
    }

    // Map theme → ImGui palette and push
    public IDisposable PushImGuiScope()
    {
        if (string.Equals(Current.Id, NoneId, StringComparison.OrdinalIgnoreCase))
            return new ThemeScope(new());

        var c = Current.Colors;
        Vector4 RGBA(string hex, float a = 1f)
        {
            if (hex.StartsWith("#")) hex = hex[1..];
            var r = Convert.ToInt32(hex[..2], 16) / 255f;
            var g = Convert.ToInt32(hex.Substring(2, 2), 16) / 255f;
            var b = Convert.ToInt32(hex.Substring(4, 2), 16) / 255f;
            return new Vector4(r, g, b, a);
        }

        var stack = new List<IDisposable>(32);

        // Text & window
        stack.Add(ImRaii.PushColor(ImGuiCol.Text, RGBA(c.Text)));
        stack.Add(ImRaii.PushColor(ImGuiCol.TextDisabled, RGBA(c.MutedText)));
        stack.Add(ImRaii.PushColor(ImGuiCol.WindowBg, RGBA(c.Background)));
        stack.Add(ImRaii.PushColor(ImGuiCol.ChildBg, RGBA(c.BackgroundAlt)));
        stack.Add(ImRaii.PushColor(ImGuiCol.PopupBg, RGBA(c.Surface)));

        // Frames & borders
        stack.Add(ImRaii.PushColor(ImGuiCol.Border, RGBA(c.Border)));
        stack.Add(ImRaii.PushColor(ImGuiCol.FrameBg, RGBA(c.Surface)));
        stack.Add(ImRaii.PushColor(ImGuiCol.FrameBgHovered, RGBA(c.Highlight)));
        stack.Add(ImRaii.PushColor(ImGuiCol.FrameBgActive, RGBA(c.Accent)));

        // Buttons
        stack.Add(ImRaii.PushColor(ImGuiCol.Button, RGBA(c.Primary, 0.85f)));
        stack.Add(ImRaii.PushColor(ImGuiCol.ButtonHovered, RGBA(c.Highlight)));
        stack.Add(ImRaii.PushColor(ImGuiCol.ButtonActive, RGBA(c.Accent)));

        // Tabs
        stack.Add(ImRaii.PushColor(ImGuiCol.Tab, RGBA(c.Surface)));
        stack.Add(ImRaii.PushColor(ImGuiCol.TabHovered, RGBA(c.Highlight)));
        stack.Add(ImRaii.PushColor(ImGuiCol.TabActive, RGBA(c.Primary)));

        // Headers (collapsing / selectable)
        stack.Add(ImRaii.PushColor(ImGuiCol.Header, RGBA(c.Surface)));
        stack.Add(ImRaii.PushColor(ImGuiCol.HeaderHovered, RGBA(c.Highlight)));
        stack.Add(ImRaii.PushColor(ImGuiCol.HeaderActive, RGBA(c.Accent)));

        // Title bars
        stack.Add(ImRaii.PushColor(ImGuiCol.TitleBg, RGBA(c.BackgroundAlt)));
        stack.Add(ImRaii.PushColor(ImGuiCol.TitleBgActive, RGBA(c.Surface)));
        stack.Add(ImRaii.PushColor(ImGuiCol.TitleBgCollapsed, RGBA(c.BackgroundAlt)));

        // Separators / sliders
        stack.Add(ImRaii.PushColor(ImGuiCol.Separator, RGBA(c.Border)));
        stack.Add(ImRaii.PushColor(ImGuiCol.SliderGrab, RGBA(c.Primary)));
        stack.Add(ImRaii.PushColor(ImGuiCol.SliderGrabActive, RGBA(c.Accent)));

        // Notifications (we keep success/warn/danger handy)
        stack.Add(ImRaii.PushColor(ImGuiCol.CheckMark, RGBA(c.Success)));
        stack.Add(ImRaii.PushColor(ImGuiCol.PlotHistogram, RGBA(c.Info)));

        // Return one disposer that pops all
        return new ThemeScope(stack);
    }

    private sealed class ThemeScope : IDisposable
    {
        private readonly List<IDisposable> _stack;
        public ThemeScope(List<IDisposable> stack) => _stack = stack;
        public void Dispose() { for (int i = _stack.Count - 1; i >= 0; --i) _stack[i].Dispose(); }
    }
}
