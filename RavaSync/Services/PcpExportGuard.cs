using Dalamud.Plugin;
using RavaSync.Interop.Ipc;
using RavaSync.MareConfiguration.Models;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Penumbra.Api.Helpers;
using Penumbra.Api.IpcSubscribers;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace RavaSync.Services;

public sealed class PcpExportGuard : DisposableMediatorSubscriberBase
{
    private readonly IDalamudPluginInterface _pi;
    private readonly PairManager _pairManager;
    private readonly IpcCallerPenumbra _penumbra;
    private readonly DalamudUtilService _dalamudUtilService;

    private readonly object _watcherLock = new();
    private FileSystemWatcher? _pcpWatcher;
    private string? _currentWatchedDirectory;

    private readonly EventSubscriber<JObject, ushort, string> _creatingPcpSub;
    private readonly EventSubscriber<JObject, string, Guid> _parsingPcpSub;

    private readonly DeleteMod _deleteMod;

    private const string BlockMarkerKey = "blockedBy";
    private const string BlockMarkerValue = "RavaSync";

    public PcpExportGuard(
        ILogger<PcpExportGuard> logger,
        IDalamudPluginInterface pi,
        PairManager pairManager,
        IpcCallerPenumbra penumbra,
        DalamudUtilService dalamudUtilService,
        MareMediator mediator)
        : base(logger, mediator)
    {
        _pi = pi;
        _pairManager = pairManager;
        _penumbra = penumbra;
        _dalamudUtilService = dalamudUtilService;

        _creatingPcpSub = new EventSubscriber<JObject, ushort, string>(
            _pi,
            "Penumbra.CreatingPcp.V2",
            OnCreatingPcp);

        _parsingPcpSub = new EventSubscriber<JObject, string, Guid>(
            _pi,
            "Penumbra.ParsingPcp",
            OnParsingPcp);

        _deleteMod = new DeleteMod(_pi);

        // Initial guess – may or may not be where PCPs are going, but harmless
        TrySetupWatcher(_penumbra.ModDirectory);

        Mediator.Subscribe<PenumbraDirectoryChangedMessage>(this, msg =>
        {
            TrySetupWatcher(msg.ModDirectory);
        });
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (!disposing) return;

        try { _creatingPcpSub?.Dispose(); } catch { }
        try { _parsingPcpSub?.Dispose(); } catch { }

        lock (_watcherLock)
        {
            if (_pcpWatcher != null)
            {
                try
                {
                    _pcpWatcher.EnableRaisingEvents = false;
                    _pcpWatcher.Created -= OnPcpFileCreated;
                    _pcpWatcher.Changed -= OnPcpFileChanged;
                    _pcpWatcher.Dispose();
                }
                catch { }

                _pcpWatcher = null;
                _currentWatchedDirectory = null;
            }
        }
    }

    private void OnCreatingPcp(JObject json, ushort modIndex, string directory)
    {
        try
        {
            var actorName = (string?)json["Actor"]?["PlayerName"] ?? "<unknown>";

            Logger.LogDebug("PCP export requested for {Actor} to {Directory}", actorName, directory);

            var dirOnly = Path.GetDirectoryName(directory);
            TrySetupWatcher(dirOnly);

            if (!ShouldBlockPcpFromJson(json))
                return;

            Logger.LogInformation("Blocking PCP export via IPC for {Actor} to {Directory}", actorName, directory);

            json.RemoveAll();
            json[BlockMarkerKey] = BlockMarkerValue;
            json["reason"] = "This PCP export was blocked for privacy reasons.";
            json["timestamp"] = DateTimeOffset.UtcNow.ToString("o");
            json["scrambled"] = Guid.NewGuid().ToString("N");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error while handling CreatingPcp");
        }
    }


    private void OnParsingPcp(JObject json, string identifier, Guid collectionId)
    {
        try
        {
            var blockedBy = (string?)json[BlockMarkerKey];
            if (!string.Equals(blockedBy, BlockMarkerValue, StringComparison.Ordinal))
                return;

            Logger.LogInformation("Deleting blocked PCP mod {Identifier} during ParsingPcp", identifier);

            try
            {
                _ = _deleteMod.Invoke(identifier, identifier);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to delete blocked PCP mod {Identifier}", identifier);
            }

            NotifyUserBlocked();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Error while handling ParsingPcp");
        }
    }

    private void TrySetupWatcher(string? directory)
    {
        lock (_watcherLock)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
                {
                    Logger.LogDebug("No valid directory for PCP watch: {Dir}", directory);
                    return;
                }

                // don't rebuild if we're already watching this exact folder
                if (string.Equals(_currentWatchedDirectory, directory, StringComparison.OrdinalIgnoreCase)
                    && _pcpWatcher != null)
                {
                    return;
                }

                // tear down old watcher
                if (_pcpWatcher != null)
                {
                    try
                    {
                        _pcpWatcher.EnableRaisingEvents = false;
                        _pcpWatcher.Created -= OnPcpFileCreated;
                        _pcpWatcher.Changed -= OnPcpFileChanged;
                        _pcpWatcher.Dispose();
                    }
                    catch { }

                    _pcpWatcher = null;
                    _currentWatchedDirectory = null;
                }

                _pcpWatcher = new FileSystemWatcher(directory, "*.pcp")
                {
                    IncludeSubdirectories = false,
                    NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime | NotifyFilters.Size
                };

                _pcpWatcher.Created += OnPcpFileCreated;
                _pcpWatcher.Changed += OnPcpFileChanged;
                _pcpWatcher.EnableRaisingEvents = true;

                _currentWatchedDirectory = directory;

                Logger.LogInformation("PCP watcher enabled on {Dir}", directory);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to set up PCP watcher for {Dir}", directory);
            }
        }
    }

    private void OnPcpFileCreated(object sender, FileSystemEventArgs e)
        => ProcessPcpFileAsync(e.FullPath);

    private void OnPcpFileChanged(object sender, FileSystemEventArgs e)
        => ProcessPcpFileAsync(e.FullPath);

    private void ProcessPcpFileAsync(string path)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                if (!File.Exists(path)) return;

                // tiny delay so we don't race Penumbra writing the file
                await Task.Delay(150).ConfigureAwait(false);

                Logger.LogDebug("Inspecting PCP file {Path}", path);

                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var zip = new ZipArchive(fs, ZipArchiveMode.Read);

                var entry = zip.GetEntry("character.json");
                if (entry == null) return;

                using var entryStream = entry.Open();
                using var reader = new StreamReader(entryStream);
                var jsonText = reader.ReadToEnd();

                var json = JObject.Parse(jsonText);

                if (!ShouldDeletePcpFromZip(json))
                    return;

                Logger.LogInformation("Deleting PCP file {Path} as it contains blocked data", path);

                try { fs.Close(); } catch { }

                File.Delete(path);
                NotifyUserBlocked();
            }
            catch (IOException ioEx)
            {
                // if we race too hard or file is gone, just log and bail
                Logger.LogDebug(ioEx, "IO issue while inspecting PCP file {Path}", path);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Error while inspecting PCP file {Path}", path);
            }
        });
    }

    private bool ShouldBlockPcpFromJson(JObject json)
    {
        // already blocked once, don't loop
        if (string.Equals((string?)json[BlockMarkerKey], BlockMarkerValue, StringComparison.Ordinal))
            return false;

        var actor = json["Actor"] as JObject;
        if (actor == null)
            return true;

        if (IsNpcActor(actor))
            return false;

        var actorName = actor["PlayerName"]?.ToObject<string>() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(actorName))
            return true;

        var localName = GetLocalPlayerNameSafe();
        if (string.IsNullOrWhiteSpace(localName))
            return true;

        var isOwnCharacter = string.Equals(actorName, localName, StringComparison.OrdinalIgnoreCase);

        // only block if it's not us
        return !isOwnCharacter;
    }

    private static bool IsNpcActor(JObject actor)
    {
        var isPlayerToken = actor["IsPlayer"] ?? actor["IsPlayerCharacter"];
        if (isPlayerToken != null &&
            bool.TryParse(isPlayerToken.ToString(), out var isPlayerFlag))
        {
            if (!isPlayerFlag)
                return true;
        }

        var kind = (string?)actor["ObjectKind"] ?? (string?)actor["Kind"];
        if (!string.IsNullOrEmpty(kind))
        {
            if (kind.Equals("Player", StringComparison.OrdinalIgnoreCase)
                || kind.Equals("Pc", StringComparison.OrdinalIgnoreCase))
                return false;

            return true;
        }

        var actorName = actor["PlayerName"]?.ToObject<string>() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(actorName))
            return true;

        return false;
    }

    private static bool ShouldDeletePcpFromZip(JObject json)
    {
        var blockedBy = (string?)json[BlockMarkerKey];
        if (string.Equals(blockedBy, BlockMarkerValue, StringComparison.Ordinal))
            return true;

        return false;
    }

    private string GetLocalPlayerNameSafe()
    {
        try
        {
            if (_dalamudUtilService.IsOnFrameworkThread)
                return _dalamudUtilService.GetPlayerName();

            return _dalamudUtilService.GetPlayerNameAsync()
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to resolve local player name for PCP guard; failing closed.");
            return string.Empty;
        }
    }

    private void NotifyUserBlocked()
    {
        Mediator.Publish(new NotificationMessage(
            "PCP export blocked",
            "If you require a copy of a person's appearance, please contact them directly and request an export, " +
            "or ask for an MCDF/MCDO.",
            NotificationType.Warning,
            TimeSpan.FromSeconds(10)));
    }
}
