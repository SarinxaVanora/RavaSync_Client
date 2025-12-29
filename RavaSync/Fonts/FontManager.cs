using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Dalamud.Bindings.ImGui;
using Dalamud.Interface.ManagedFontAtlas;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Plugin;

namespace RavaSync.Fonts
{
    public interface IFontManager
    {
        IReadOnlyList<FontFace> Available { get; }
        FontFace? Current { get; }
        event Action<FontFace>? FontChanged;

        string UserFontsDirectory { get; }
        bool TryImportFont(string sourcePath, out string? importedId);

        void LoadAll();
        void LoadOnlyFace(string faceIdOrName, IEnumerable<float>? sizesPx = null);

        bool TryApply(string idOrName);
        void DrawDropdown(string label, float width = 300f);

        IFontHandle? GetCurrentHandle();
        ImFontPtr GetCurrentFont();

        float GetUserSizePx();
        void SetUserSizePx(float px);
        IReadOnlyList<float> GetSupportedSizesPx();
    }

    public sealed class FontFace
    {
        public string Id { get; init; } = "";
        public string Name { get; init; } = "";
        public string Path { get; init; } = "";

        // size(px) -> handle
        public Dictionary<float, IFontHandle> Handles { get; init; } = new();

        public override string ToString() => Name;
    }

    public sealed class FontManager : IFontManager
    {
        private readonly IDalamudPluginInterface _pi;
        private readonly string _builtInFontsDir;
        private readonly string _userFontsDir;

        // keep the preset sizes small to avoid huge atlases
        private static readonly float[] PresetSizesPx = new float[] { 6f, 8f, 10f, 12f, 14f, 15f, 16f, 17f, 18f, 20f, 22f, 24f };

        private readonly List<FontFace> _faces = new();
        private FontFace? _currentFace;
        private float _currentSizePx = 16f;

        public IReadOnlyList<float> GetSupportedSizesPx() => PresetSizesPx;
        public IReadOnlyList<FontFace> Available => _faces;
        public FontFace? Current => _currentFace;
        public event Action<FontFace>? FontChanged;

        public string UserFontsDirectory => _userFontsDir;

        public FontManager(IDalamudPluginInterface pi, float defaultSizePx = 16f)
        {
            _pi = pi;

            _builtInFontsDir = Path.Combine(_pi.AssemblyLocation.Directory!.FullName, "Fonts");
            _userFontsDir = Path.Combine(_pi.ConfigDirectory.FullName, "Fonts");

            Directory.CreateDirectory(_builtInFontsDir);
            Directory.CreateDirectory(_userFontsDir);

            _currentSizePx = SnapSize(defaultSizePx);
        }

        public void LoadAll()
        {
            var previous = _currentFace?.Id;

            // don’t keep stale handles around if the underlying files changed
            DisposeAllHandles();
            _faces.Clear();

            // built-in first, user overrides same filename
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var file in EnumerateFontFiles(_builtInFontsDir))
                dict[Path.GetFileName(file)] = file;

            foreach (var file in EnumerateFontFiles(_userFontsDir))
                dict[Path.GetFileName(file)] = file;

            foreach (var kv in dict.OrderBy(k => Path.GetFileNameWithoutExtension(k.Key)))
            {
                _faces.Add(new FontFace
                {
                    Id = kv.Key,
                    Name = Path.GetFileNameWithoutExtension(kv.Key),
                    Path = kv.Value,
                });
            }

            _currentFace = !string.IsNullOrWhiteSpace(previous)
                ? _faces.FirstOrDefault(f => f.Id.Equals(previous, StringComparison.OrdinalIgnoreCase))
                : null;

            _currentFace ??= _faces.FirstOrDefault();
            _currentSizePx = SnapSize(_currentSizePx);
        }

        public void LoadOnlyFace(string faceIdOrName, IEnumerable<float>? sizesPx = null)
        {
            DisposeAllHandles();
            _faces.Clear();

            var files = EnumerateFontFiles(_builtInFontsDir)
                .Concat(EnumerateFontFiles(_userFontsDir))
                .ToList();

            if (files.Count == 0)
            {
                _currentFace = null;
                return;
            }

            string? resolvedFile = null;

            if (!string.IsNullOrWhiteSpace(faceIdOrName))
            {
                resolvedFile = files.FirstOrDefault(f =>
                    string.Equals(Path.GetFileName(f), faceIdOrName, StringComparison.OrdinalIgnoreCase));

                resolvedFile ??= files.FirstOrDefault(f =>
                    string.Equals(Path.GetFileNameWithoutExtension(f), faceIdOrName, StringComparison.OrdinalIgnoreCase));
            }

            resolvedFile ??= files[0];

            var face = new FontFace
            {
                Id = Path.GetFileName(resolvedFile),
                Name = Path.GetFileNameWithoutExtension(resolvedFile),
                Path = resolvedFile,
            };

            var sizesToBake = (sizesPx is null || !sizesPx.Any())
                ? new[] { _currentSizePx }
                : sizesPx;

            var snapped = sizesToBake
                .Select(SnapSize)
                .Distinct()
                .OrderBy(x => x)
                .ToList();

            foreach (var sz in snapped)
                _ = EnsureHandle(face, sz);

            _faces.Add(face);
            _currentFace = face;
            _currentSizePx = SnapSize(_currentSizePx);
        }

        public bool TryApply(string idOrName)
        {
            var f = ResolveFace(idOrName);
            if (f is null) return false;

            _currentFace = f;
            _ = EnsureHandle(f, SnapSize(_currentSizePx));
            FontChanged?.Invoke(f);
            return true;
        }

        public void DrawDropdown(string label, float width = 300f)
        {
            ImGui.PushItemWidth(width);
            var curName = _currentFace?.Name ?? "(none)";

            if (ImGui.BeginCombo(label, curName))
            {
                for (int i = 0; i < _faces.Count; i++)
                {
                    bool selected = _currentFace != null && _faces[i].Id == _currentFace.Id;

                    using (PushFaceAtCurrentSize(_faces[i]))
                    {
                        if (ImGui.Selectable(_faces[i].Name, selected))
                        {
                            _currentFace = _faces[i];
                            _ = EnsureHandle(_currentFace, SnapSize(_currentSizePx));
                            FontChanged?.Invoke(_currentFace);
                        }
                    }

                    if (selected) ImGui.SetItemDefaultFocus();
                }
                ImGui.EndCombo();
            }

            ImGui.PopItemWidth();
        }

        public IFontHandle? GetCurrentHandle()
        {
            var face = _currentFace;
            if (face == null) return null;

            return EnsureHandle(face, SnapSize(_currentSizePx));
        }

        public ImFontPtr GetCurrentFont() => default; // compat shim

        public float GetUserSizePx() => _currentSizePx;

        public void SetUserSizePx(float px)
        {
            _currentSizePx = SnapSize(px);

            if (_currentFace is not null)
                _ = EnsureHandle(_currentFace, _currentSizePx);
        }

        public bool TryImportFont(string sourcePath, out string? importedId)
        {
            importedId = null;

            if (string.IsNullOrWhiteSpace(sourcePath) || !File.Exists(sourcePath))
                return false;

            var ext = Path.GetExtension(sourcePath);
            if (!ext.Equals(".ttf", StringComparison.OrdinalIgnoreCase) &&
                !ext.Equals(".otf", StringComparison.OrdinalIgnoreCase))
                return false;

            Directory.CreateDirectory(_userFontsDir);

            var baseName = Path.GetFileNameWithoutExtension(sourcePath);
            var safeBase = string.Concat(baseName.Where(c => !Path.GetInvalidFileNameChars().Contains(c)));
            if (string.IsNullOrWhiteSpace(safeBase)) safeBase = "font";

            var fileName = safeBase + ext.ToLowerInvariant();
            var dest = Path.Combine(_userFontsDir, fileName);

            int n = 1;
            while (File.Exists(dest))
            {
                fileName = $"{safeBase} ({n}){ext.ToLowerInvariant()}";
                dest = Path.Combine(_userFontsDir, fileName);
                n++;
            }

            File.Copy(sourcePath, dest, overwrite: false);
            importedId = fileName;
            return true;
        }

        private FontFace? ResolveFace(string idOrName)
        {
            if (string.IsNullOrWhiteSpace(idOrName)) return null;

            // id (filename)
            var byId = _faces.FirstOrDefault(x => x.Id.Equals(idOrName, StringComparison.OrdinalIgnoreCase));
            if (byId != null) return byId;

            // name (filename without extension)
            return _faces.FirstOrDefault(x => x.Name.Equals(idOrName, StringComparison.OrdinalIgnoreCase));
        }

        private IFontHandle? EnsureHandle(FontFace face, float sizePx)
        {
            sizePx = SnapSize(sizePx);

            if (face.Handles.TryGetValue(sizePx, out var existing))
                return existing;

            if (string.IsNullOrWhiteSpace(face.Path) || !File.Exists(face.Path))
                return null;

            var file = face.Path;
            var localSize = sizePx;

            var handle = _pi.UiBuilder.FontAtlas.NewDelegateFontHandle(builder =>
            {
                builder.OnPreBuild(tk =>
                {
                    if (!File.Exists(file)) return;

                    var cfg = new SafeFontConfig
                    {
                        SizePx = localSize,
                        OversampleH = 2,
                        OversampleV = 2,
                        PixelSnapH = true,
                        RasterizerMultiply = 1.0f,
                        RasterizerGamma = 1.7f,
                        GlyphMaxAdvanceX = float.MaxValue,
                    };
                    tk.AddFontFromFile(file, cfg);
                });
            });

            face.Handles[localSize] = handle;
            return handle;
        }

        private float SnapSize(float px)
        {
            float best = PresetSizesPx[0];
            float bestDelta = Math.Abs(px - best);

            for (int i = 1; i < PresetSizesPx.Length; i++)
            {
                float d = Math.Abs(px - PresetSizesPx[i]);
                if (d < bestDelta)
                {
                    best = PresetSizesPx[i];
                    bestDelta = d;
                }
            }
            return best;
        }

        private IDisposable PushFaceAtCurrentSize(FontFace face)
        {
            var h = EnsureHandle(face, _currentSizePx);
            return h is not null ? h.Push() : new Noop();
        }

        private void DisposeAllHandles()
        {
            foreach (var f in _faces)
            {
                foreach (var h in f.Handles.Values)
                {
                    try { h.Dispose(); } catch { }
                }
                f.Handles.Clear();
            }
        }

        private static IEnumerable<string> EnumerateFontFiles(string dir)
        {
            if (!Directory.Exists(dir)) yield break;

            foreach (var f in Directory.EnumerateFiles(dir))
            {
                var ext = Path.GetExtension(f);
                if (ext.Equals(".ttf", StringComparison.OrdinalIgnoreCase) ||
                    ext.Equals(".otf", StringComparison.OrdinalIgnoreCase))
                    yield return f;
            }
        }

        private sealed class Noop : IDisposable { public void Dispose() { } }
    }
}
