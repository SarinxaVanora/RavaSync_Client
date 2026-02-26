using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface.Utility.Raii;
using RavaSync.API.Data.Enum;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;
using RavaSync.FileCache;
using System.Numerics;

namespace RavaSync.UI;

public class DataAnalysisUi : WindowMediatorSubscriberBase
{
    private readonly CharacterAnalyzer _characterAnalyzer;
    private readonly Progress<(string, int)> _conversionProgress = new();
    private readonly IpcManager _ipcManager;
    private readonly UiSharedService _uiSharedService;
    private readonly PlayerPerformanceConfigService _playerPerformanceConfig;
    private readonly TransientResourceManager _transientResourceManager;
    private readonly TransientConfigService _transientConfigService;
    private readonly Dictionary<string, string[]> _texturesToConvert = new(StringComparer.Ordinal);
    private Dictionary<ObjectKind, Dictionary<string, CharacterAnalyzer.FileDataEntry>>? _cachedAnalysis;
    private CancellationTokenSource _conversionCancellationTokenSource = new();
    private string _conversionCurrentFileName = string.Empty;
    private int _conversionCurrentFileProgress = 0;
    private Task? _conversionTask;
    private bool _enableBc7ConversionMode = false;
    private bool _hasUpdate = false;
    private bool _modalOpen = false;
    private string _selectedFileTypeTab = string.Empty;
    private string _selectedHash = string.Empty;
    private ObjectKind _selectedObjectTab;
    private bool _showModal = false;
    private CancellationTokenSource _transientRecordCts = new();
    protected override IDisposable? BeginThemeScope() => _uiSharedService.BeginThemed();
    public DataAnalysisUi(ILogger<DataAnalysisUi> logger, MareMediator mediator,
        CharacterAnalyzer characterAnalyzer, IpcManager ipcManager,
        PerformanceCollectorService performanceCollectorService, UiSharedService uiSharedService,
        PlayerPerformanceConfigService playerPerformanceConfig, TransientResourceManager transientResourceManager,
        TransientConfigService transientConfigService)
        : base(logger, mediator, "RavaSync Character Data Analysis", performanceCollectorService)
    {
        _characterAnalyzer = characterAnalyzer;
        _ipcManager = ipcManager;
        _uiSharedService = uiSharedService;
        _playerPerformanceConfig = playerPerformanceConfig;
        _transientResourceManager = transientResourceManager;
        _transientConfigService = transientConfigService;
        Mediator.Subscribe<CharacterDataAnalyzedMessage>(this, (_) =>
        {
            _hasUpdate = true;
        });
        SizeConstraints = new()
        {
            MinimumSize = new()
            {
                X = 800,
                Y = 600
            },
            MaximumSize = new()
            {
                X = 3840,
                Y = 2160
            }
        };

        _conversionProgress.ProgressChanged += ConversionProgress_ProgressChanged;
    }

    protected override void DrawInternal()
    {
        if (_conversionTask != null && !_conversionTask.IsCompleted)
        {
            _showModal = true;
            if (ImGui.BeginPopupModal(_uiSharedService.L("UI.CompactUI.fa170e36", "BC7 Conversion in Progress")))
            {
                ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.31646ba1", "BC7 Conversion in progress: ") + _conversionCurrentFileProgress + "/" + _texturesToConvert.Count);
                UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.7b06d002", "Current file: ") + _conversionCurrentFileName);
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.StopCircle, _uiSharedService.L("UI.DataAnalysisUi.56c042d6", "Cancel conversion")))
                {
                    _conversionCancellationTokenSource.Cancel();
                }
                UiSharedService.SetScaledWindowSize(500);
                ImGui.EndPopup();
            }
            else
            {
                _modalOpen = false;
            }
        }
        else if (_conversionTask != null && _conversionTask.IsCompleted && _texturesToConvert.Count > 0)
        {
            _conversionTask = null;
            _texturesToConvert.Clear();
            _showModal = false;
            _modalOpen = false;
            _enableBc7ConversionMode = false;
        }

        if (_showModal && !_modalOpen)
        {
            ImGui.OpenPopup(_uiSharedService.L("UI.CompactUI.fa170e36", "BC7 Conversion in Progress"));
            _modalOpen = true;
        }

        if (_hasUpdate)
        {
            _cachedAnalysis = _characterAnalyzer.LastAnalysis.DeepClone();
            _hasUpdate = false;
        }

        using var tabBar = ImRaii.TabBar("analysisRecordingTabBar");
        using (var tabItem = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.DataAnalysisUi.739e6d2a", "Analysis"), "##739e6d2a")))
        {
            if (tabItem)
            {
                using var id = ImRaii.PushId("analysis");
                DrawAnalysis();
            }
        }
        using (var tabItem = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.DataAnalysisUi.2cebfef2", "Transient Files"), "##2cebfef2")))
        {
            if (tabItem)
            {
                using var tabbar = ImRaii.TabBar("transientData");

                using (var transientData = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.DataAnalysisUi.d6440fd4", "Stored Transient File Data"), "##d6440fd4")))
                {
                    using var id = ImRaii.PushId("data");

                    if (transientData)
                    {
                        DrawStoredData();
                    }
                }
                using (var transientRecord = ImRaii.TabItem(string.Concat(_uiSharedService.L("UI.DataAnalysisUi.19f5edd8", "Record Transient Data"), "##19f5edd8")))
                {
                    using var id = ImRaii.PushId("recording");

                    if (transientRecord)
                    {
                        DrawRecording();
                    }
                }
            }
        }
    }

    private bool _showAlreadyAddedTransients = false;
    private bool _acknowledgeReview = false;
    private string _selectedStoredCharacter = string.Empty;
    private string _selectedJobEntry = string.Empty;
    private readonly List<string> _storedPathsToRemove = [];
    private readonly Dictionary<string, string> _filePathResolve = [];
    private string _filterGamePath = string.Empty;
    private string _filterFilePath = string.Empty;

    private void DrawStoredData()
    {
        UiSharedService.DrawTree(_uiSharedService.L("UI.CharaDataHubUi.843fc806", "What is this? (Explanation / Help)"), () =>
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.ac3a8fce", "This tab allows you to see which transient files are attached to your character."));
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.97c1aa24", "Transient files are files that cannot be resolved to your character permanently. RavaSync gathers these files in the background while you execute animations, VFX, sound effects, etc."));
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.171D22F9", "When sending your character data to others, RavaSync will combine the files listed in \"All Jobs\" and the corresponding currently used job."));
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.1ed4c6cc", "The purpose of this tab is primarily informational for you to see which files you are carrying with you. You can remove added game paths, however if you are using the animations etc. again, ")
                + "RavaSync will automatically attach these after using them. If you disable associated mods in Penumbra, the associated entries here will also be deleted automatically.");
        });

        ImGuiHelpers.ScaledDummy(5);

        var config = _transientConfigService.Current.TransientConfigs;
        Vector2 availableContentRegion = Vector2.Zero;
        using (ImRaii.Group())
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.ee9946c8", "Character"));
            ImGui.Separator();
            ImGuiHelpers.ScaledDummy(3);
            availableContentRegion = ImGui.GetContentRegionAvail();
            using (ImRaii.ListBox("##characters", new Vector2(200, availableContentRegion.Y)))
            {
                foreach (var entry in config)
                {
                    var name = entry.Key.Split("_");
                    if (!_uiSharedService.WorldData.TryGetValue(ushort.Parse(name[1]), out var worldname))
                    {
                        continue;
                    }
                    if (ImGui.Selectable(name[0] + " (" + worldname + ")", string.Equals(_selectedStoredCharacter, entry.Key, StringComparison.Ordinal)))
                    {
                        _selectedStoredCharacter = entry.Key;
                        _selectedJobEntry = string.Empty;
                        _storedPathsToRemove.Clear();
                        _filePathResolve.Clear();
                        _filterFilePath = string.Empty;
                        _filterGamePath = string.Empty;
                    }
                }
            }
        }
        ImGui.SameLine();
        bool selectedData = config.TryGetValue(_selectedStoredCharacter, out var transientStorage) && transientStorage != null;
        using (ImRaii.Group())
        {
            ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.30c8cb83", "Job"));
            ImGui.Separator();
            ImGuiHelpers.ScaledDummy(3);
            using (ImRaii.ListBox("##data", new Vector2(150, availableContentRegion.Y)))
            {
                if (selectedData)
                {
                    if (ImGui.Selectable(_uiSharedService.L("UI.DataAnalysisUi.530f7f5b", "All Jobs"), string.Equals(_selectedJobEntry, "alljobs", StringComparison.Ordinal)))
                    {
                        _selectedJobEntry = "alljobs";
                    }
                    foreach (var job in transientStorage!.JobSpecificCache)
                    {
                        if (!_uiSharedService.JobData.TryGetValue(job.Key, out var jobName)) continue;
                        if (ImGui.Selectable(jobName, string.Equals(_selectedJobEntry, job.Key.ToString(), StringComparison.Ordinal)))
                        {
                            _selectedJobEntry = job.Key.ToString();
                            _storedPathsToRemove.Clear();
                            _filePathResolve.Clear();
                            _filterFilePath = string.Empty;
                            _filterGamePath = string.Empty;
                        }
                    }
                }
            }
        }
        ImGui.SameLine();
        using (ImRaii.Group())
        {
            var selectedList = string.Equals(_selectedJobEntry, "alljobs", StringComparison.Ordinal)
                ? config[_selectedStoredCharacter].GlobalPersistentCache
                : (string.IsNullOrEmpty(_selectedJobEntry) ? [] : config[_selectedStoredCharacter].JobSpecificCache[uint.Parse(_selectedJobEntry)]);
            ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.680a91e4", "Attached Files (Total Files: {0})"), selectedList.Count));
            ImGui.Separator();
            ImGuiHelpers.ScaledDummy(3);
            using (ImRaii.Disabled(string.IsNullOrEmpty(_selectedJobEntry)))
            {

                var restContent = availableContentRegion.X - ImGui.GetCursorPosX();
                using var group = ImRaii.Group();
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.ArrowRight, _uiSharedService.L("UI.DataAnalysisUi.40e0b95f", "Resolve Game Paths to used File Paths")))
                {
                    _ = Task.Run(async () =>
                    {
                        var paths = selectedList.ToArray();
                        var resolved = await _ipcManager.Penumbra.ResolvePathsAsync(paths, []).ConfigureAwait(false);
                        _filePathResolve.Clear();

                        for (int i = 0; i < resolved.forward.Length; i++)
                        {
                            _filePathResolve[paths[i]] = resolved.forward[i];
                        }
                    });
                }
                ImGui.SameLine();
                ImGuiHelpers.ScaledDummy(20, 1);
                ImGui.SameLine();
                using (ImRaii.Disabled(!_storedPathsToRemove.Any()))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.DataAnalysisUi.84d6676c", "Remove selected Game Paths")))
                    {
                        foreach (var item in _storedPathsToRemove)
                        {
                            selectedList.Remove(item);
                        }

                        _transientConfigService.Save();
                        _transientResourceManager.RebuildSemiTransientResources();
                        _filterFilePath = string.Empty;
                        _filterGamePath = string.Empty;
                    }
                }
                ImGui.SameLine();
                using (ImRaii.Disabled(!UiSharedService.CtrlPressed()))
                {
                    if (_uiSharedService.IconTextButton(FontAwesomeIcon.Trash, _uiSharedService.L("UI.DataAnalysisUi.daf9b600", "Clear ALL Game Paths")))
                    {
                        selectedList.Clear();
                        _transientConfigService.Save();
                        _transientResourceManager.RebuildSemiTransientResources();
                        _filterFilePath = string.Empty;
                        _filterGamePath = string.Empty;
                    }
                }
                UiSharedService.AttachToolTip(_uiSharedService.L("UI.DataAnalysisUi.21cbe680", "Hold CTRL to delete all game paths from the displayed list")
                    + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DataAnalysisUi.d77493d0", "You usually do not need to do this. All animation and VFX data will be automatically handled through RavaSync."));
                ImGuiHelpers.ScaledDummy(5);
                ImGuiHelpers.ScaledDummy(30);
                ImGui.SameLine();
                ImGui.SetNextItemWidth((restContent - 30) / 2f);
                ImGui.InputTextWithHint("##filterGamePath", _uiSharedService.L("UI.DataAnalysisUi.9dee82d5", "Filter by Game Path"), ref _filterGamePath, 255);
                ImGui.SameLine();
                ImGui.SetNextItemWidth((restContent - 30) / 2f);
                ImGui.InputTextWithHint("##filterFilePath", _uiSharedService.L("UI.DataAnalysisUi.3c8be0b7", "Filter by File Path"), ref _filterFilePath, 255);

                using (var dataTable = ImRaii.Table("##table", 3, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollY | ImGuiTableFlags.RowBg))
                {
                    if (dataTable)
                    {
                        ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, 30);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.112540ae", "Game Path"), ImGuiTableColumnFlags.WidthFixed, (restContent - 30) / 2f);
                        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.c08ba79d", "File Path"), ImGuiTableColumnFlags.WidthFixed, (restContent - 30) / 2f);
                        ImGui.TableSetupScrollFreeze(0, 1);
                        ImGui.TableHeadersRow();
                        int id = 0;
                        foreach (var entry in selectedList)
                        {
                            if (!string.IsNullOrWhiteSpace(_filterGamePath) && !entry.Contains(_filterGamePath, StringComparison.OrdinalIgnoreCase))
                                continue;
                            bool hasFileResolve = _filePathResolve.TryGetValue(entry, out var filePath);

                            if (hasFileResolve && !string.IsNullOrEmpty(_filterFilePath) && !filePath!.Contains(_filterFilePath, StringComparison.OrdinalIgnoreCase))
                                continue;

                            using var imguiid = ImRaii.PushId(id++);
                            ImGui.TableNextColumn();
                            bool isSelected = _storedPathsToRemove.Contains(entry, StringComparer.Ordinal);
                            if (ImGui.Checkbox("##", ref isSelected))
                            {
                                if (isSelected)
                                    _storedPathsToRemove.Add(entry);
                                else
                                    _storedPathsToRemove.Remove(entry);
                            }
                            ImGui.TableNextColumn();
                            ImGui.TextUnformatted(entry);
                            UiSharedService.AttachToolTip(entry + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DataAnalysisUi.fef1799e", "Click to copy to clipboard"));
                            if (ImGui.IsItemClicked(ImGuiMouseButton.Left))
                            {
                                ImGui.SetClipboardText(entry);
                            }
                            ImGui.TableNextColumn();
                            if (hasFileResolve)
                            {
                                ImGui.TextUnformatted(filePath ?? _uiSharedService.L("UI.DataAnalysisUi.d01f84ff", "Unk"));
                                UiSharedService.AttachToolTip(filePath ?? _uiSharedService.L("UI.DataAnalysisUi.d01f84ff", "Unk") + UiSharedService.TooltipSeparator + _uiSharedService.L("UI.DataAnalysisUi.fef1799e", "Click to copy to clipboard"));
                                if (ImGui.IsItemClicked(ImGuiMouseButton.Left))
                                {
                                    ImGui.SetClipboardText(filePath);
                                }
                            }
                            else
                            {
                                ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.3bc15c8a", "-"));
                                UiSharedService.AttachToolTip(_uiSharedService.L("UI.DataAnalysisUi.125e885c", "Resolve Game Paths to used File Paths to display the associated file paths."));
                            }
                        }
                    }
                }
            }
        }
    }

    private void DrawRecording()
    {
        UiSharedService.DrawTree(_uiSharedService.L("UI.CharaDataHubUi.843fc806", "What is this? (Explanation / Help)"), () =>
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.f130cfc0", "This tab allows you to attempt to fix mods that do not sync correctly, especially those with modded models and animations.") + Environment.NewLine + Environment.NewLine
                + "To use this, start the recording, execute one or multiple emotes/animations you want to attempt to fix and check if new data appears in the table below." + Environment.NewLine
                + "If it doesn't, RavaSync is not able to catch the data or already has recorded the animation files (check 'Show previously added transient files' to see if not all is already present)." + Environment.NewLine + Environment.NewLine
                + "For most animations, vfx, etc. it is enough to just run them once unless they have random variations. Longer animations do not require to play out in their entirety to be captured.");
            ImGuiHelpers.ScaledDummy(5);
            UiSharedService.DrawGroupedCenteredColorText(_uiSharedService.L("UI.DataAnalysisUi.50f07251", "Important Note: If you need to fix an animation that should apply across multiple jobs, you need to repeat this process with at least one additional job, ") +
                "otherwise the animation will only be fixed for the currently active job. This goes primarily for emotes that are used across multiple jobs.",
                ImGuiColors.DalamudYellow, 800);
            ImGuiHelpers.ScaledDummy(5);
            UiSharedService.DrawGroupedCenteredColorText(_uiSharedService.L("UI.DataAnalysisUi.deaccb18", "WARNING: WHILE RECORDING TRANSIENT DATA, DO NOT CHANGE YOUR APPEARANCE, ENABLED MODS OR ANYTHING. JUST DO THE ANIMATION(S) OR WHATEVER YOU NEED DOING AND STOP THE RECORDING."),
                ImGuiColors.DalamudRed, 800);
            ImGuiHelpers.ScaledDummy(5);
        });
        using (ImRaii.Disabled(_transientResourceManager.IsTransientRecording))
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Play, _uiSharedService.L("UI.DataAnalysisUi.c39cfaf4", "Start Transient Recording")))
            {
                _transientRecordCts.Cancel();
                _transientRecordCts.Dispose();
                _transientRecordCts = new();
                _transientResourceManager.StartRecording(_transientRecordCts.Token);
                _acknowledgeReview = false;
            }
        }
        ImGui.SameLine();
        using (ImRaii.Disabled(!_transientResourceManager.IsTransientRecording))
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Stop, _uiSharedService.L("UI.DataAnalysisUi.416bc222", "Stop Transient Recording")))
            {
                _transientRecordCts.Cancel();
            }
        }
        if (_transientResourceManager.IsTransientRecording)
        {
            ImGui.SameLine();
            UiSharedService.ColorText($"RECORDING - Time Remaining: {_transientResourceManager.RecordTimeRemaining.Value}", ImGuiColors.DalamudYellow);
            ImGuiHelpers.ScaledDummy(5);
            UiSharedService.DrawGroupedCenteredColorText(_uiSharedService.L("UI.DataAnalysisUi.4f04fb6d", "DO NOT CHANGE YOUR APPEARANCE OR MODS WHILE RECORDING, YOU CAN ACCIDENTALLY MAKE SOME OF YOUR APPEARANCE RELATED MODS PERMANENT."), ImGuiColors.DalamudRed, 800);
        }

        ImGuiHelpers.ScaledDummy(5);
        ImGui.Checkbox(_uiSharedService.L("UI.DataAnalysisUi.208f04f4", "Show previously added transient files in the recording"), ref _showAlreadyAddedTransients);
        _uiSharedService.DrawHelpText(_uiSharedService.L("UI.DataAnalysisUi.25cd007a", "Use this only if you want to see what was previously already caught by Mare"));
        ImGuiHelpers.ScaledDummy(5);

        using (ImRaii.Disabled(_transientResourceManager.IsTransientRecording || _transientResourceManager.RecordedTransients.All(k => !k.AddTransient) || !_acknowledgeReview))
        {
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.Save, _uiSharedService.L("UI.DataAnalysisUi.2a2f4e3c", "Save Recorded Transient Data")))
            {
                _transientResourceManager.SaveRecording();
                _acknowledgeReview = false;
            }
        }
        ImGui.SameLine();
        ImGui.Checkbox(_uiSharedService.L("UI.DataAnalysisUi.134f35ed", "I acknowledge I have reviewed the recorded data"), ref _acknowledgeReview);
        if (_transientResourceManager.RecordedTransients.Any(k => !k.AlreadyTransient))
        {
            ImGuiHelpers.ScaledDummy(5);
            UiSharedService.DrawGroupedCenteredColorText(_uiSharedService.L("UI.DataAnalysisUi.421d41c8", "Please review the recorded mod files before saving and deselect files that got into the recording on accident."), ImGuiColors.DalamudYellow);
            ImGuiHelpers.ScaledDummy(5);
        }

        ImGuiHelpers.ScaledDummy(5);
        var width = ImGui.GetContentRegionAvail();
        using var table = ImRaii.Table("Recorded Transients", 4, ImGuiTableFlags.ScrollY | ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.RowBg);
        if (table)
        {
            int id = 0;
            ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, 30);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.89ff3122", "Owner"), ImGuiTableColumnFlags.WidthFixed, 100);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.112540ae", "Game Path"), ImGuiTableColumnFlags.WidthFixed, (width.X - 30 - 100) / 2f);
            ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.c08ba79d", "File Path"), ImGuiTableColumnFlags.WidthFixed, (width.X - 30 - 100) / 2f);
            ImGui.TableSetupScrollFreeze(0, 1);
            ImGui.TableHeadersRow();
            var transients = _transientResourceManager.RecordedTransients.ToList();
            transients.Reverse();
            foreach (var value in transients)
            {
                if (value.AlreadyTransient && !_showAlreadyAddedTransients)
                    continue;

                using var imguiid = ImRaii.PushId(id++);
                if (value.AlreadyTransient)
                {
                    ImGui.PushStyleColor(ImGuiCol.Text, ImGuiColors.DalamudGrey);
                }
                ImGui.TableNextColumn();
                bool addTransient = value.AddTransient;
                if (ImGui.Checkbox("##add", ref addTransient))
                {
                    value.AddTransient = addTransient;
                }
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(value.Owner.Name);
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(value.GamePath);
                UiSharedService.AttachToolTip(value.GamePath);
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(value.FilePath);
                UiSharedService.AttachToolTip(value.FilePath);
                if (value.AlreadyTransient)
                {
                    ImGui.PopStyleColor();
                }
            }
        }
    }

    private void DrawAnalysis()
    {
        UiSharedService.DrawTree(_uiSharedService.L("UI.CharaDataHubUi.843fc806", "What is this? (Explanation / Help)"), () =>
        {
            UiSharedService.TextWrapped(_uiSharedService.L("UI.DataAnalysisUi.565967ec", "This tab shows you all files and their sizes that are currently in use through your character and associated entities in Mare"));
        });

        if (_cachedAnalysis!.Count == 0) return;

        bool isAnalyzing = _characterAnalyzer.IsAnalysisRunning;
        if (isAnalyzing)
        {
            UiSharedService.ColorTextWrapped(string.Format(_uiSharedService.L("UI.DataAnalysisUi.b1cc03c4", "Analyzing {0}/{1}"), _characterAnalyzer.CurrentFile, _characterAnalyzer.TotalFiles),
                ImGuiColors.DalamudYellow);
            if (_uiSharedService.IconTextButton(FontAwesomeIcon.StopCircle, _uiSharedService.L("UI.DataAnalysisUi.7f8af3fa", "Cancel analysis")))
            {
                _characterAnalyzer.CancelAnalyze();
            }
        }
        else
        {
            if (_cachedAnalysis!.Any(c => c.Value.Any(f => !f.Value.IsComputed)))
            {
                UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.DataAnalysisUi.8ffe123a", "Some entries in the analysis have file size not determined yet, press the button below to analyze your current data"),
                    ImGuiColors.DalamudYellow);
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, _uiSharedService.L("UI.DataAnalysisUi.76ae5cd9", "Start analysis (missing entries)")))
                {
                    _ = _characterAnalyzer.ComputeAnalysis(print: false);
                }
            }
            else
            {
                if (_uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, _uiSharedService.L("UI.DataAnalysisUi.54a86882", "Start analysis (recalculate all entries)")))
                {
                    _ = _characterAnalyzer.ComputeAnalysis(print: false, recalculate: true);
                }
            }
        }

        ImGui.Separator();

        ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.8effc94e", "Total files:"));
        ImGui.SameLine();
        ImGui.TextUnformatted(_cachedAnalysis!.Values.Sum(c => c.Values.Count).ToString());
        ImGui.SameLine();
        using (var font = ImRaii.PushFont(UiBuilder.IconFont))
        {
            ImGui.TextUnformatted(FontAwesomeIcon.InfoCircle.ToIconString());
        }
        if (ImGui.IsItemHovered())
        {
            string text = "";
            var groupedfiles = _cachedAnalysis.Values.SelectMany(f => f.Values).GroupBy(f => f.FileType, StringComparer.Ordinal);
            text = string.Join(Environment.NewLine, groupedfiles.OrderBy(f => f.Key, StringComparer.Ordinal)
                .Select(f => f.Key + ": " + f.Count() + " files, size: " + UiSharedService.ByteToString(f.Sum(v => v.OriginalSize))
                + ", compressed: " + UiSharedService.ByteToString(f.Sum(v => v.CompressedSize))));
            ImGui.SetTooltip(text);
        }
        ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.59362608", "Total size (actual):"));
        ImGui.SameLine();
        ImGui.TextUnformatted(UiSharedService.ByteToString(_cachedAnalysis!.Sum(c => c.Value.Sum(c => c.Value.OriginalSize))));
        ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.6bee48de", "Total size (compressed for up/download only):"));
        ImGui.SameLine();
        ImGui.TextUnformatted(UiSharedService.ByteToString(_cachedAnalysis!.Sum(c => c.Value.Sum(c => c.Value.CompressedSize))));
        ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.438622a0", "Total modded model triangles: {0}"), _cachedAnalysis.Sum(c => c.Value.Sum(f => f.Value.Triangles))));
        ImGui.Separator();
        using var tabbar = ImRaii.TabBar("objectSelection");
        foreach (var kvp in _cachedAnalysis)
        {
            using var id = ImRaii.PushId(kvp.Key.ToString());
            string tabText = kvp.Key.ToString();
            if (kvp.Value.Any(f => !f.Value.IsComputed)) tabText += " (!)";
            using var tab = ImRaii.TabItem(tabText + "###" + kvp.Key.ToString());
            if (tab.Success)
            {
                var groupedfiles = kvp.Value.Select(v => v.Value).GroupBy(f => f.FileType, StringComparer.Ordinal)
                    .OrderBy(k => k.Key, StringComparer.Ordinal).ToList();

                ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.7fef92ec", "Files for ") + kvp.Key);
                ImGui.SameLine();
                ImGui.TextUnformatted(kvp.Value.Count.ToString());
                ImGui.SameLine();

                using (var font = ImRaii.PushFont(UiBuilder.IconFont))
                {
                    ImGui.TextUnformatted(FontAwesomeIcon.InfoCircle.ToIconString());
                }
                if (ImGui.IsItemHovered())
                {
                    string text = "";
                    text = string.Join(Environment.NewLine, groupedfiles
                        .Select(f => f.Key + ": " + f.Count() + " files, size: " + UiSharedService.ByteToString(f.Sum(v => v.OriginalSize))
                        + ", compressed: " + UiSharedService.ByteToString(f.Sum(v => v.CompressedSize))));
                    ImGui.SetTooltip(text);
                }
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.296bb5a4", "{0} size (actual):"), kvp.Key));
                ImGui.SameLine();
                ImGui.TextUnformatted(UiSharedService.ByteToString(kvp.Value.Sum(c => c.Value.OriginalSize)));
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.0ed3ed2b", "{0} size (compressed for up/download only):"), kvp.Key));
                ImGui.SameLine();
                ImGui.TextUnformatted(UiSharedService.ByteToString(kvp.Value.Sum(c => c.Value.CompressedSize)));
                ImGui.Separator();

                var vramUsage = groupedfiles.SingleOrDefault(v => string.Equals(v.Key, "tex", StringComparison.Ordinal));
                if (vramUsage != null)
                {
                    var actualVramUsage = vramUsage.Sum(f => f.OriginalSize);
                    ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.1f914802", "{0} VRAM usage:"), kvp.Key));
                    ImGui.SameLine();
                    ImGui.TextUnformatted(UiSharedService.ByteToString(actualVramUsage));
                    if (_playerPerformanceConfig.Current.WarnOnExceedingThresholds
                        || _playerPerformanceConfig.Current.ShowPerformanceIndicator)
                    {
                        using var _ = ImRaii.PushIndent(10f);
                        var currentVramWarning = _playerPerformanceConfig.Current.VRAMSizeWarningThresholdMiB;
                        ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.5baf2076", "Configured VRAM warning threshold: {0} MiB."), currentVramWarning));
                        if (currentVramWarning * 1024 * 1024 < actualVramUsage)
                        {
                            UiSharedService.ColorText(_uiSharedService.L("UI.DataAnalysisUi.97c5197b", "You exceed your own threshold by ") +
                                $"{UiSharedService.ByteToString(actualVramUsage - (currentVramWarning * 1024 * 1024))}.",
                                ImGuiColors.DalamudYellow);
                        }
                    }
                }

                var actualTriCount = kvp.Value.Sum(f => f.Value.Triangles);
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.98ed1bf7", "{0} modded model triangles: {1}"), kvp.Key, actualTriCount));
                if (_playerPerformanceConfig.Current.WarnOnExceedingThresholds
                    || _playerPerformanceConfig.Current.ShowPerformanceIndicator)
                {
                    using var _ = ImRaii.PushIndent(10f);
                    var currentTriWarning = _playerPerformanceConfig.Current.TrisWarningThresholdThousands;
                    ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.4eb54e17", "Configured triangle warning threshold: {0} triangles."), currentTriWarning * 1000));
                    if (currentTriWarning * 1000 < actualTriCount)
                    {
                        UiSharedService.ColorText(_uiSharedService.L("UI.DataAnalysisUi.97c5197b", "You exceed your own threshold by ") +
                            $"{actualTriCount - (currentTriWarning * 1000)} triangles.",
                            ImGuiColors.DalamudYellow);
                    }
                }

                ImGui.Separator();
                if (_selectedObjectTab != kvp.Key)
                {
                    _selectedHash = string.Empty;
                    _selectedObjectTab = kvp.Key;
                    _selectedFileTypeTab = string.Empty;
                    _enableBc7ConversionMode = false;
                    _texturesToConvert.Clear();
                }

                using var fileTabBar = ImRaii.TabBar("fileTabs");

                foreach (IGrouping<string, CharacterAnalyzer.FileDataEntry>? fileGroup in groupedfiles)
                {
                    string fileGroupText = fileGroup.Key + " [" + fileGroup.Count() + "]";
                    var requiresCompute = fileGroup.Any(k => !k.IsComputed);
                    using var tabcol = ImRaii.PushColor(ImGuiCol.Tab, UiSharedService.Color(ImGuiColors.DalamudYellow), requiresCompute);
                    if (requiresCompute)
                    {
                        fileGroupText += " (!)";
                    }
                    ImRaii.IEndObject fileTab;
                    using (var textcol = ImRaii.PushColor(ImGuiCol.Text, UiSharedService.Color(new(0, 0, 0, 1)),
                        requiresCompute && !string.Equals(_selectedFileTypeTab, fileGroup.Key, StringComparison.Ordinal)))
                    {
                        fileTab = ImRaii.TabItem(fileGroupText + "###" + fileGroup.Key);
                    }

                    if (!fileTab) { fileTab.Dispose(); continue; }

                    if (!string.Equals(fileGroup.Key, _selectedFileTypeTab, StringComparison.Ordinal))
                    {
                        _selectedFileTypeTab = fileGroup.Key;
                        _selectedHash = string.Empty;
                        _enableBc7ConversionMode = false;
                        _texturesToConvert.Clear();
                    }

                    ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.32d85e0b", "{0} files"), fileGroup.Key));
                    ImGui.SameLine();
                    ImGui.TextUnformatted(fileGroup.Count().ToString());

                    ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.5f3e9b27", "{0} files size (actual):"), fileGroup.Key));
                    ImGui.SameLine();
                    ImGui.TextUnformatted(UiSharedService.ByteToString(fileGroup.Sum(c => c.OriginalSize)));

                    ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.d1257ec9", "{0} files size (compressed for up/download only):"), fileGroup.Key));
                    ImGui.SameLine();
                    ImGui.TextUnformatted(UiSharedService.ByteToString(fileGroup.Sum(c => c.CompressedSize)));

                    if (string.Equals(_selectedFileTypeTab, "tex", StringComparison.Ordinal))
                    {
                        ImGui.Checkbox(_uiSharedService.L("UI.DataAnalysisUi.aa688a36", "Enable BC7 Conversion Mode"), ref _enableBc7ConversionMode);
                        if (_enableBc7ConversionMode)
                        {
                            UiSharedService.ColorText(_uiSharedService.L("UI.DataAnalysisUi.a564645a", "WARNING BC7 CONVERSION:"), ImGuiColors.DalamudYellow);
                            ImGui.SameLine();
                            UiSharedService.ColorText(_uiSharedService.L("UI.DataAnalysisUi.c9cee650", "Converting textures to BC7 is irreversible!"), ImGuiColors.DalamudRed);
                            UiSharedService.ColorTextWrapped(_uiSharedService.L("UI.DataAnalysisUi.c6931d0b", "- Converting textures to BC7 will reduce their size (compressed and uncompressed) drastically. It is recommended to be used for large (4k+) textures.") +
                            Environment.NewLine + "- Some textures, especially ones utilizing colorsets, might not be suited for BC7 conversion and might produce visual artifacts." +
                            Environment.NewLine + "- Before converting textures, make sure to have the original files of the mod you are converting so you can reimport it in case of issues." +
                            Environment.NewLine + "- Conversion will convert all found texture duplicates (entries with more than 1 file path) automatically." +
                            Environment.NewLine + "- Converting textures to BC7 is a very expensive operation and, depending on the amount of textures to convert, will take a while to complete."
                                , ImGuiColors.DalamudYellow);
                            if (_texturesToConvert.Count > 0 && _uiSharedService.IconTextButton(FontAwesomeIcon.PlayCircle, _uiSharedService.L("UI.DataAnalysisUi.640d82d9", "Start conversion of ") + _texturesToConvert.Count + _uiSharedService.L("UI.DataAnalysisUi.ef5c7e65", " texture(s)")))
                            {
                                _conversionCancellationTokenSource = _conversionCancellationTokenSource.CancelRecreate();
                                _conversionTask = _ipcManager.Penumbra.ConvertTextureFiles(_logger, _texturesToConvert, _conversionProgress, _conversionCancellationTokenSource.Token);
                            }
                        }
                    }

                    ImGui.Separator();
                    DrawTable(fileGroup);

                    fileTab.Dispose();
                }
            }
        }

        ImGui.Separator();

        ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.7a408130", "Selected file:"));
        ImGui.SameLine();
        UiSharedService.ColorText(_selectedHash, ImGuiColors.DalamudYellow);

        if (_cachedAnalysis[_selectedObjectTab].TryGetValue(_selectedHash, out CharacterAnalyzer.FileDataEntry? item))
        {
            var filePaths = item.FilePaths;
            ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.1d85ba89", "Local file path:"));
            ImGui.SameLine();
            UiSharedService.TextWrapped(filePaths[0]);
            if (filePaths.Count > 1)
            {
                ImGui.SameLine();
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.7996d70a", "(and {0} more)"), filePaths.Count - 1));
                ImGui.SameLine();
                _uiSharedService.IconText(FontAwesomeIcon.InfoCircle);
                UiSharedService.AttachToolTip(string.Join(Environment.NewLine, filePaths.Skip(1)));
            }

            var gamepaths = item.GamePaths;
            ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.7bd09b51", "Used by game path:"));
            ImGui.SameLine();
            UiSharedService.TextWrapped(gamepaths[0]);
            if (gamepaths.Count > 1)
            {
                ImGui.SameLine();
                ImGui.TextUnformatted(string.Format(_uiSharedService.L("UI.DataAnalysisUi.7996d70a", "(and {0} more)"), gamepaths.Count - 1));
                ImGui.SameLine();
                _uiSharedService.IconText(FontAwesomeIcon.InfoCircle);
                UiSharedService.AttachToolTip(string.Join(Environment.NewLine, gamepaths.Skip(1)));
            }
        }
    }

    public override void OnOpen()
    {
        _hasUpdate = true;
        _selectedHash = string.Empty;
        _enableBc7ConversionMode = false;
        _texturesToConvert.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        _conversionProgress.ProgressChanged -= ConversionProgress_ProgressChanged;
    }

    private void ConversionProgress_ProgressChanged(object? sender, (string, int) e)
    {
        _conversionCurrentFileName = e.Item1;
        _conversionCurrentFileProgress = e.Item2;
    }

    private void DrawTable(IGrouping<string, CharacterAnalyzer.FileDataEntry> fileGroup)
    {
        var tableColumns = string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal)
            ? (_enableBc7ConversionMode ? 7 : 6)
            : (string.Equals(fileGroup.Key, "mdl", StringComparison.Ordinal) ? 6 : 5);
        using var table = ImRaii.Table("Analysis", tableColumns, ImGuiTableFlags.Sortable | ImGuiTableFlags.RowBg | ImGuiTableFlags.ScrollY | ImGuiTableFlags.SizingFixedFit,
            new Vector2(0, 300));
        if (!table.Success) return;
        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.873507a0", "Hash"));
        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.dffaef89", "Filepaths"));
        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.020ffbd5", "Gamepaths"));
        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.494a4f2e", "Original Size"));
        ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.2a085c8c", "Compressed Size"));
        if (string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal))
        {
            ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.041a5dec", "Format"));
            if (_enableBc7ConversionMode) ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.65f83ba3", "Convert to BC7"));
        }
        if (string.Equals(fileGroup.Key, "mdl", StringComparison.Ordinal))
        {
            ImGui.TableSetupColumn(_uiSharedService.L("UI.DataAnalysisUi.9af595ff", "Triangles"));
        }
        ImGui.TableSetupScrollFreeze(0, 1);
        //ImGui.TableHeadersRow();

        if (string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal) && _enableBc7ConversionMode)
        {
            ImGui.TableNextRow(ImGuiTableRowFlags.Headers);

            int col = 0;
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.873507a0", "Hash"));
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.dffaef89", "Filepaths"));
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.020ffbd5", "Gamepaths"));
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.494a4f2e", "Original Size"));
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.2a085c8c", "Compressed Size"));
            ImGui.TableSetColumnIndex(col++); ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.041a5dec", "Format"));

            // Convert header with "All" + checkbox, right-aligned
            ImGui.TableSetColumnIndex(col++);
            var headerStartX = ImGui.GetCursorPosX();
            ImGui.TableHeader(_uiSharedService.L("UI.DataAnalysisUi.3f15ce79", "Convert"));

            // Compute select-all state
            int eligible = 0, selected = 0;
            foreach (var it in fileGroup)
            {
                if (string.Equals(it.Format.Value, "BC7", StringComparison.Ordinal)) continue;
                eligible++;
                if (_texturesToConvert.ContainsKey(it.FilePaths[0])) selected++;
            }
            bool anyEligible = eligible > 0;
            bool allChecked = anyEligible && selected == eligible;
            bool someChecked = anyEligible && selected > 0 && selected < eligible;

            // Layout: right-align the "All" label + checkbox as a unit
            float colW = ImGui.GetColumnWidth();
            var style = ImGui.GetStyle();
            float innerPad = style.CellPadding.X;
            float boxSize = ImGui.GetFrameHeight();
            var label = "All";
            var labelSz = ImGui.CalcTextSize(label);
            float spacing = style.ItemInnerSpacing.X;
            float totalW = labelSz.X + spacing + boxSize;

            float xRight = headerStartX + colW - innerPad - totalW;
            xRight = MathF.Max(xRight, headerStartX + innerPad); // clamp just in case
            float curY = ImGui.GetCursorPosY();
            ImGui.SetCursorPos(new Vector2(xRight, curY));

            // Draw "All" + checkbox
            using (ImRaii.PushId($"bc7-selall:{_selectedObjectTab}:{_selectedFileTypeTab}"))
            using (ImRaii.Disabled(!anyEligible))
            {
                if (someChecked) ImGui.PushStyleVar(ImGuiStyleVar.Alpha, 0.7f);

                ImGui.TextUnformatted(label);
                ImGui.SameLine(0, spacing);

                bool selectAll = allChecked;
                if (ImGui.Checkbox("##selectAllConvert", ref selectAll) && anyEligible)
                {
                    if (selectAll)
                    {
                        foreach (var it in fileGroup)
                        {
                            if (string.Equals(it.Format.Value, "BC7", StringComparison.Ordinal)) continue;
                            var key = it.FilePaths[0];
                            if (!_texturesToConvert.ContainsKey(key))
                                _texturesToConvert[key] = it.FilePaths.Skip(1).ToArray();
                        }
                    }
                    else
                    {
                        foreach (var it in fileGroup)
                        {
                            if (string.Equals(it.Format.Value, "BC7", StringComparison.Ordinal)) continue;
                            _texturesToConvert.Remove(it.FilePaths[0]);
                        }
                    }
                }

                if (someChecked) ImGui.PopStyleVar();
            }

            UiSharedService.AttachToolTip(anyEligible
                ? "Select/Deselect all visible non-BC7 textures"
                : "No eligible (non-BC7) textures");
        }
        else
        {
            ImGui.TableHeadersRow();
        }

        var sortSpecs = ImGui.TableGetSortSpecs();
        if (sortSpecs.SpecsDirty)
        {
            var idx = sortSpecs.Specs.ColumnIndex;

            if (idx == 0 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Key, StringComparer.Ordinal).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 0 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Key, StringComparer.Ordinal).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 1 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.FilePaths.Count).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 1 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.FilePaths.Count).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 2 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.GamePaths.Count).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 2 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.GamePaths.Count).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 3 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.OriginalSize).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 3 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.OriginalSize).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 4 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.CompressedSize).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (idx == 4 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.CompressedSize).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (string.Equals(fileGroup.Key, "mdl", StringComparison.Ordinal) && idx == 5 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.Triangles).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (string.Equals(fileGroup.Key, "mdl", StringComparison.Ordinal) && idx == 5 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.Triangles).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal) && idx == 5 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Ascending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderBy(k => k.Value.Format.Value, StringComparer.Ordinal).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);
            if (string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal) && idx == 5 && sortSpecs.Specs.SortDirection == ImGuiSortDirection.Descending)
                _cachedAnalysis![_selectedObjectTab] = _cachedAnalysis[_selectedObjectTab].OrderByDescending(k => k.Value.Format.Value, StringComparer.Ordinal).ToDictionary(d => d.Key, d => d.Value, StringComparer.Ordinal);

            sortSpecs.SpecsDirty = false;
        }

        foreach (var item in fileGroup)
        {
            using var text = ImRaii.PushColor(ImGuiCol.Text, new Vector4(0, 0, 0, 1), string.Equals(item.Hash, _selectedHash, StringComparison.Ordinal));
            using var text2 = ImRaii.PushColor(ImGuiCol.Text, new Vector4(1, 1, 1, 1), !item.IsComputed);
            ImGui.TableNextColumn();
            if (!item.IsComputed)
            {
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg1, UiSharedService.Color(ImGuiColors.DalamudRed));
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, UiSharedService.Color(ImGuiColors.DalamudRed));
            }
            if (string.Equals(_selectedHash, item.Hash, StringComparison.Ordinal))
            {
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg1, UiSharedService.Color(ImGuiColors.DalamudYellow));
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, UiSharedService.Color(ImGuiColors.DalamudYellow));
            }
            ImGui.TextUnformatted(item.Hash);
            if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            ImGui.TableNextColumn();
            ImGui.TextUnformatted(item.FilePaths.Count.ToString());
            if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            ImGui.TableNextColumn();
            ImGui.TextUnformatted(item.GamePaths.Count.ToString());
            if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            ImGui.TableNextColumn();
            ImGui.TextUnformatted(UiSharedService.ByteToString(item.OriginalSize));
            if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            ImGui.TableNextColumn();
            ImGui.TextUnformatted(UiSharedService.ByteToString(item.CompressedSize));
            if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            if (string.Equals(fileGroup.Key, "tex", StringComparison.Ordinal))
            {
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(item.Format.Value);
                if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
                if (_enableBc7ConversionMode)
                {
                    ImGui.TableNextColumn();
                    if (string.Equals(item.Format.Value, "BC7", StringComparison.Ordinal))
                    {
                        ImGui.TextUnformatted(_uiSharedService.L("UI.DataAnalysisUi.d41d8cd9", ""));
                        continue;
                    }
                    var filePath = item.FilePaths[0];
                    bool toConvert = _texturesToConvert.ContainsKey(filePath);
                    if (ImGui.Checkbox("###convert" + item.Hash, ref toConvert))
                    {
                        if (toConvert && !_texturesToConvert.ContainsKey(filePath))
                        {
                            _texturesToConvert[filePath] = item.FilePaths.Skip(1).ToArray();
                        }
                        else if (!toConvert && _texturesToConvert.ContainsKey(filePath))
                        {
                            _texturesToConvert.Remove(filePath);
                        }
                    }
                }
            }
            if (string.Equals(fileGroup.Key, "mdl", StringComparison.Ordinal))
            {
                ImGui.TableNextColumn();
                ImGui.TextUnformatted(item.Triangles.ToString());
                if (ImGui.IsItemClicked()) _selectedHash = item.Hash;
            }
        }
    }
}