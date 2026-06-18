using Dalamud.Game.ClientState;
using Dalamud.Game.ClientState.Objects;
using Dalamud.Game.ClientState.Objects.SubKinds;
using GameControl = FFXIVClientStructs.FFXIV.Client.Game.Control.Control;
using FFXIVClientStructs.FFXIV.Client.Graphics.Scene;
using GameKernelDevice = FFXIVClientStructs.FFXIV.Client.Graphics.Kernel.Device;
using GameRenderTargetManager = FFXIVClientStructs.FFXIV.Client.Graphics.Render.RenderTargetManager;
using Dalamud.Plugin.Services;
using MessagePack;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using RavaSync.Services.Mediator;
using RavaSync.Services.Mesh;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Services.RavaCast;

public sealed class RavaCastService : DisposableMediatorSubscriberBase, IHostedService
{
    private readonly ILogger<RavaCastService> _logger;
    private readonly IRavaMesh _mesh;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IObjectTable _objects;
    private readonly IClientState _clientState;
    private readonly IFramework _framework;
    private readonly MareConfigService _config;
    private readonly RavaCastBrowserSurface _surface;

    private static readonly byte[] Magic = [(byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'C', (byte)'A', (byte)'S', (byte)'T', 0];
    private const int BroadcastIntervalMs = 2500;
    private const int CastTtlMs = 9000;
    private const int SurfaceSyncIntervalMs = 1000;
    private const int DirectStreamSignalChunkChars = 900;
    private const int DirectStreamSignalAssemblyTtlMs = 30000;
    private const int LivePlaneBroadcastIntervalMs = 250;
    private const int LivePlaneFinalDebounceMs = 300;

    private long _lastBroadcastTick;
    private long _lastPruneTick;
    private long _lastSurfaceSyncTick;
    private long _lastHostedPlaneBroadcastTick;
    private long _pendingHostedPlaneFinalBroadcastTick;
    private long _pendingHostedBrowserNavigationSyncTick;
    private readonly ConcurrentDictionary<Guid, RavaCastSummary> _activeCasts = new();
    private readonly ConcurrentDictionary<string, bool> _joinedViewers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _directStreamReadyViewers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Guid, bool> _pendingJoinMuted = new();
    private readonly ConcurrentDictionary<string, DirectStreamSignalAssembly> _directStreamSignalAssemblies = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _completedDirectStreamSignalAssemblies = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _directStreamSignalSendGates = new(StringComparer.Ordinal);

    private HostedCast? _hosted;
    private JoinedCast? _joined;
    private float _localVolume = 0.50f;
    private volatile bool _worldBrowserInputSuspended;

    public bool WorldBrowserInputSuspended => _worldBrowserInputSuspended;

    public void SetWorldBrowserInputSuspended(bool suspended)
    {
        _worldBrowserInputSuspended = suspended;
        if (suspended)
            _surface.SendBrowserFocus(false);
    }

    private sealed class HostedCast
    {
        public Guid CastId { get; init; } = Guid.NewGuid();
        public string HostSessionId { get; init; } = string.Empty;
        public string HostName { get; init; } = "Player";
        public string CastName { get; set; } = "RavaCast";
        public string Url { get; set; } = string.Empty;
        public string SourceDomain { get; set; } = string.Empty;
        public string MediaTitle { get; set; } = string.Empty;
        public bool IsPlaying { get; set; } = true;
        public double PositionSeconds { get; set; }
        public double? DurationSeconds { get; set; }
        public DateTime StateUtc { get; set; } = DateTime.UtcNow;
        public RavaCastPlane Plane { get; set; } = EmptyPlane;
        public List<string> Queue { get; } = [];
        public RavaCastCookiePayload[] ConsentCookies { get; set; } = [];
        public string PasswordSalt { get; set; } = string.Empty;
        public string PasswordHash { get; set; } = string.Empty;
        public bool PasswordProtected => !string.IsNullOrEmpty(PasswordHash);
        public RavaCastMode Mode { get; set; } = RavaCastMode.UrlShare;
        public RavaCastDirectStreamQuality DirectStreamQuality { get; set; } = RavaCastDirectStreamQuality.Normal720p30;
        public bool DirectStreamPublisherRequested { get; set; }
        public string DirectStreamStatus { get; set; } = string.Empty;
        public string DirectStreamDetail { get; set; } = string.Empty;
    }

    private sealed class JoinedCast
    {
        public Guid CastId { get; set; }
        public string HostSessionId { get; set; } = string.Empty;
        public string ViewerSessionId { get; set; } = string.Empty;
        public string HostName { get; set; } = string.Empty;
        public string CastName { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public string SourceDomain { get; set; } = string.Empty;
        public string MediaTitle { get; set; } = string.Empty;
        public bool IsPlaying { get; set; } = true;
        public double PositionSeconds { get; set; }
        public double? DurationSeconds { get; set; }
        public DateTime StateUtc { get; set; } = DateTime.UtcNow;
        public int JoinedCount { get; set; }
        public RavaCastPlane Plane { get; set; } = EmptyPlane;
        public IReadOnlyList<string> Queue { get; set; } = [];
        public IReadOnlyList<RavaCastCookiePayload> ConsentCookies { get; set; } = [];
        public bool PasswordProtected { get; set; }
        public string PasswordSalt { get; set; } = string.Empty;
        public bool IsMuted { get; set; }
        public float Volume { get; set; } = 0.5f;
        public RavaCastMode Mode { get; set; } = RavaCastMode.UrlShare;
        public RavaCastDirectStreamQuality DirectStreamQuality { get; set; } = RavaCastDirectStreamQuality.Normal720p30;
        public string DirectStreamStatus { get; set; } = string.Empty;
        public string DirectStreamDetail { get; set; } = string.Empty;
        public bool DirectStreamReceiverRequested { get; set; }
    }

    private sealed class DirectStreamSignalAssembly
    {
        public DirectStreamSignalAssembly(Guid castId, string signalId, string fromSessionId, string toSessionId, string type, int chunkCount)
        {
            CastId = castId;
            SignalId = signalId;
            FromSessionId = fromSessionId;
            ToSessionId = toSessionId;
            Type = type;
            ChunkCount = Math.Clamp(chunkCount, 1, 256);
            Chunks = new string[ChunkCount];
            Received = new bool[ChunkCount];
            CreatedTick = Environment.TickCount64;
        }

        public Guid CastId { get; }
        public string SignalId { get; }
        public string FromSessionId { get; }
        public string ToSessionId { get; }
        public string Type { get; }
        public int ChunkCount { get; }
        public string[] Chunks { get; }
        public bool[] Received { get; }
        public int ReceivedCount { get; private set; }
        public long CreatedTick { get; }

        public bool TryAdd(int index, string part)
        {
            if (index < 0 || index >= ChunkCount) return false;
            if (!Received[index])
            {
                Received[index] = true;
                Chunks[index] = part ?? string.Empty;
                ReceivedCount++;
            }
            return ReceivedCount >= ChunkCount;
        }

        public string BuildPayloadJson() => string.Concat(Chunks);
    }


    public static readonly RavaCastPlane EmptyPlane = new(0, string.Empty, Vector3.Zero, Vector3.Zero, Vector3.Zero, Vector3.Zero);

    public RavaCastService(ILogger<RavaCastService> logger, MareMediator mediator, IRavaMesh mesh, DalamudUtilService dalamudUtil,
        IObjectTable objects, IClientState clientState, IFramework framework, MareConfigService config, RavaCastBrowserSurface surface)
        : base(logger, mediator)
    {
        _logger = logger;
        _mesh = mesh;
        _dalamudUtil = dalamudUtil;
        _objects = objects;
        _clientState = clientState;
        _framework = framework;
        _config = config;
        _surface = surface;

        _localVolume = GetDefaultVolume();

        Mediator.Subscribe<SyncshellGameMeshMessage>(this, OnGameMesh);
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, _ => StopCurrentCastForZoneSwitch());
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _surface.DirectStreamSignalProduced -= OnDirectStreamSignalProduced;
        _surface.DirectStreamSignalProduced += OnDirectStreamSignalProduced;
        _framework.Update += FrameworkOnUpdate;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _framework.Update -= FrameworkOnUpdate;
        _surface.DirectStreamSignalProduced -= OnDirectStreamSignalProduced;
        _surface.StopDirectStreamPublisher();
        _surface.StopDirectStreamReceiver();
        _surface.Close();
        _activeCasts.Clear();
        _joinedViewers.Clear();
        _directStreamReadyViewers.Clear();
        CancelPendingHostedPlaneBroadcast();
        CancelPendingHostedBrowserNavigationSync();
        _pendingJoinMuted.Clear();
        _directStreamSignalAssemblies.Clear();
        _completedDirectStreamSignalAssemblies.Clear();
        _directStreamSignalSendGates.Clear();
        return Task.CompletedTask;
    }

    private void StopCurrentCastForZoneSwitch()
    {
        if (_hosted is not null)
        {
            // Healthy RavaCast zone-cleanup path; do not log unless it fails.
            EndBroadcast();
            return;
        }

        if (_joined is not null)
        {
            // Healthy RavaCast zone-cleanup path; do not log unless it fails.
            Leave();
        }
    }

    private void FrameworkOnUpdate(IFramework f)
    {
        var now = Environment.TickCount64;

        if (_hosted is not null && _pendingHostedPlaneFinalBroadcastTick > 0 && now >= _pendingHostedPlaneFinalBroadcastTick)
        {
            _pendingHostedPlaneFinalBroadcastTick = 0;
            _lastHostedPlaneBroadcastTick = now;
            BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
            BroadcastHostedStateToJoined();
        }

        if (_hosted is not null && _pendingHostedBrowserNavigationSyncTick > 0 && now >= _pendingHostedBrowserNavigationSyncTick)
        {
            _pendingHostedBrowserNavigationSyncTick = 0;
            SyncHostedUrlFromSurface();
        }

        if (_hosted is not null && now - _lastBroadcastTick >= BroadcastIntervalMs)
        {
            _lastBroadcastTick = now;
            BroadcastHostedStateNearby(RavaCastOp.Advertise);
        }

        if (now - _lastPruneTick >= 1000)
        {
            _lastPruneTick = now;
            foreach (var kv in _activeCasts.ToArray())
            {
                if (now - kv.Value.LastSeenTick > CastTtlMs)
                    _activeCasts.TryRemove(kv.Key, out _);
            }
        }

        if (now - _lastSurfaceSyncTick >= SurfaceSyncIntervalMs)
        {
            _lastSurfaceSyncTick = now;

            // Hard rule: framework ticks must never promote ordinary browser redirects/history churn
            // into new RavaCast navigation requests. Modern media sites mutate location constantly; treating
            // that as a fresh URL Share navigation made viewers reload or fall behind. Explicit URL-bar/Back
            // actions schedule a one-shot URL sync instead. Periodic sync is for playback time and consent only.
            if (_hosted is { Mode: RavaCastMode.UrlShare })
            {
                SyncHostedMediaStateFromSurface();
                SyncHostedConsentCookiesFromSurface();
            }
        }
    }

    public IReadOnlyList<RavaCastSummary> GetActiveCasts()
    {
        var mySession = GetMySessionId();
        return _activeCasts.Values
            .Where(c => !string.Equals(c.HostSessionId, mySession, StringComparison.Ordinal))
            .OrderBy(c => c.HostName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(c => c.CastName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public RavaCastSessionView? GetCurrentSession()
    {
        if (_hosted is not null)
        {
            var ds = _surface.DirectStreamStatus;
            return new RavaCastSessionView(_hosted.CastId, _hosted.HostSessionId, _hosted.HostName, _hosted.CastName, _hosted.Url,
                _hosted.SourceDomain, _hosted.MediaTitle, _hosted.IsPlaying, _hosted.PositionSeconds, _hosted.DurationSeconds,
                _hosted.StateUtc, _joinedViewers.Count, _hosted.Plane, _hosted.Queue.ToArray(), _hosted.ConsentCookies, true, _surface.Muted, _localVolume)
            {
                Mode = _hosted.Mode,
                PasswordProtected = _hosted.PasswordProtected,
                PasswordSalt = _hosted.PasswordSalt,
                DirectStreamQuality = _hosted.DirectStreamQuality,
                DirectStreamStatus = GetHostedDirectStreamStatus(ds),
                DirectStreamDetail = GetHostedDirectStreamDetail(ds),
                DirectStreamNativeMediaAvailable = ds.NativeMediaAvailable,
                DirectStreamConnectedPeers = ds.ConnectedPeerCount
            };
        }

        if (_joined is not null)
        {
            var ds = _surface.DirectStreamStatus;
            return new RavaCastSessionView(_joined.CastId, _joined.HostSessionId, _joined.HostName, _joined.CastName, _joined.Url,
                _joined.SourceDomain, _joined.MediaTitle, _joined.IsPlaying, _joined.PositionSeconds, _joined.DurationSeconds,
                _joined.StateUtc, _joined.JoinedCount, _joined.Plane, _joined.Queue, _joined.ConsentCookies, false, _joined.IsMuted, _joined.Volume)
            {
                Mode = _joined.Mode,
                PasswordProtected = _joined.PasswordProtected,
                PasswordSalt = _joined.PasswordSalt,
                DirectStreamQuality = _joined.DirectStreamQuality,
                DirectStreamStatus = _joined.Mode == RavaCastMode.DirectStream ? ds.StatusText : (string.IsNullOrWhiteSpace(_joined.DirectStreamStatus) ? ds.StatusText : _joined.DirectStreamStatus),
                DirectStreamDetail = _joined.Mode == RavaCastMode.DirectStream ? ds.Detail ?? string.Empty : (string.IsNullOrWhiteSpace(_joined.DirectStreamDetail) ? ds.Detail ?? string.Empty : _joined.DirectStreamDetail),
                DirectStreamNativeMediaAvailable = ds.NativeMediaAvailable,
                DirectStreamConnectedPeers = ds.ConnectedPeerCount
            };
        }

        return null;
    }

    public RavaCastRenderState? GetRenderState()
    {
        var current = GetCurrentSession();
        if (current is null) return null;
        if (current.Plane.TerritoryId != 0 && current.Plane.TerritoryId != _dalamudUtil.CurrentTerritoryId) return null;
        return new RavaCastRenderState(current.CastId, current.CastName, current.SourceDomain, current.MediaTitle, current.Url,
            current.IsPlaying, current.PositionSeconds, current.DurationSeconds, current.Plane, current.IsOwner);
    }

    public bool TryStartBroadcast(string castName, string url, RavaCastPlane plane, out string error)
        => TryStartBroadcast(castName, url, plane, RavaCastMode.UrlShare, RavaCastDirectStreamQuality.Normal720p30, null, out error);

    public bool TryStartBroadcast(string castName, string url, RavaCastPlane plane, RavaCastMode mode, RavaCastDirectStreamQuality quality, out string error)
        => TryStartBroadcast(castName, url, plane, mode, quality, null, out error);

    public bool TryStartBroadcast(string castName, string url, RavaCastPlane plane, RavaCastMode mode, RavaCastDirectStreamQuality quality, string? password, out string error)
    {
        error = string.Empty;
        if (!TryValidatePublicWebUrl(url, out var uri, out error)) return false;
        if (!TryEnsurePlaybackBackendReady(out error)) return false;

        var hostSession = GetMySessionId();
        if (string.IsNullOrWhiteSpace(hostSession))
        {
            error = "RavaCast could not identify your current character session yet.";
            return false;
        }

        _surface.StopDirectStreamReceiver();
        _joined = null;
        _surface.Close();
        _joinedViewers.Clear();
        _directStreamReadyViewers.Clear();
        CancelPendingHostedPlaneBroadcast();
        CancelPendingHostedBrowserNavigationSync();

        var passwordSalt = string.Empty;
        var passwordHash = string.Empty;
        if (!string.IsNullOrWhiteSpace(password))
        {
            passwordSalt = Guid.NewGuid().ToString("N");
            passwordHash = HashPassword(password, passwordSalt);
        }

        _hosted = new HostedCast
        {
            HostSessionId = hostSession,
            HostName = _objects.LocalPlayer?.Name.TextValue ?? "Player",
            CastName = string.IsNullOrWhiteSpace(castName) ? "RavaCast" : castName.Trim(),
            Url = uri.ToString(),
            SourceDomain = uri.Host,
            MediaTitle = uri.Host,
            IsPlaying = true,
            PositionSeconds = 0,
            DurationSeconds = null,
            StateUtc = DateTime.UtcNow,
            Plane = plane,
            PasswordSalt = passwordSalt,
            PasswordHash = passwordHash,
            Mode = mode,
            DirectStreamQuality = quality
        };

        _surface.Open(_hosted.Url, muted: false, _localVolume);
        if (_hosted.Mode == RavaCastMode.DirectStream)
            StartHostedDirectStreamPublisher(forceRestart: true);
        BroadcastHostedStateNearby(RavaCastOp.Advertise);
        return true;
    }

    public void EndBroadcast()
    {
        if (_hosted is null) return;
        StopHostedDirectStream("Cast ended by owner.");
        var env = new RavaCastEnvelope(_hosted.CastId, RavaCastOp.ScreenClosed, null);
        SendEnvelopeNearby(_hosted.HostSessionId, env);
        foreach (var viewer in _joinedViewers.Keys.ToArray())
            SendEnvelope(_hosted.HostSessionId, viewer, env);
        RemoveDirectStreamSignalAssembliesForCast(_hosted.CastId);
        _hosted = null;
        _joinedViewers.Clear();
        _directStreamReadyViewers.Clear();
        CancelPendingHostedPlaneBroadcast();
        CancelPendingHostedBrowserNavigationSync();
        _surface.Close();
    }

    private void SyncHostedBrowserStateFromSurface()
    {
        SyncHostedMediaStateFromSurface();
    }

    private void SyncHostedUrlFromSurface()
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.UrlShare) return;
        var currentUrl = _surface.CurrentUrl;
        if (string.IsNullOrWhiteSpace(currentUrl)) return;
        if (!TryValidatePublicWebUrl(currentUrl, out var uri, out _)) return;
        var nextUrl = uri.ToString();
        if (string.Equals(_hosted.Url, nextUrl, StringComparison.OrdinalIgnoreCase)) return;

        _hosted.Url = nextUrl;
        _hosted.SourceDomain = uri.Host;
        _hosted.MediaTitle = !string.IsNullOrWhiteSpace(_surface.CurrentMedia.Title) ? _surface.CurrentMedia.Title : uri.Host;
        _hosted.IsPlaying = true;
        _hosted.PositionSeconds = 0;
        _hosted.DurationSeconds = null;
        _hosted.StateUtc = DateTime.UtcNow;
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    private void SyncHostedMediaStateFromSurface()
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.UrlShare) return;
        var media = _surface.CurrentMedia;
        // Do not turn passive location changes from media sites into URL Share navigation.
        // Explicit URL-bar and Back actions call SyncHostedUrlFromSurface via a one-shot schedule.
        var title = !string.IsNullOrWhiteSpace(media.Title) ? media.Title : _hosted.SourceDomain;
        var position = Math.Max(0, media.PositionSeconds);
        var duration = media.DurationSeconds is > 0 ? media.DurationSeconds : null;
        var changed = Math.Abs(_hosted.PositionSeconds - position) >= 0.75
            || _hosted.IsPlaying != media.IsPlaying
            || Math.Abs((_hosted.DurationSeconds ?? -1) - (duration ?? -1)) >= 0.75
            || (!string.IsNullOrWhiteSpace(title) && !string.Equals(_hosted.MediaTitle, title, StringComparison.Ordinal));

        if (!changed) return;
        _hosted.MediaTitle = title;
        _hosted.IsPlaying = media.IsPlaying;
        _hosted.PositionSeconds = position;
        _hosted.DurationSeconds = duration;
        _hosted.StateUtc = media.StateUtc == default ? DateTime.UtcNow : media.StateUtc;
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    private void SyncHostedConsentCookiesFromSurface()
    {
        if (_hosted is null) return;
        var cookies = _surface.GetShareableConsentCookies();
        if (ConsentCookiesEqual(_hosted.ConsentCookies, cookies)) return;
        _hosted.ConsentCookies = cookies;
        BroadcastHostedConsentCookies();
    }

    private void BroadcastHostedConsentCookies()
    {
        if (_hosted is null || _hosted.ConsentCookies.Length == 0) return;
        var payload = MessagePackSerializer.Serialize(_hosted.ConsentCookies);
        var env = new RavaCastEnvelope(_hosted.CastId, RavaCastOp.ConsentCookies, payload);
        SendEnvelopeNearby(_hosted.HostSessionId, env);
        foreach (var viewer in _joinedViewers.Keys.ToArray())
            SendEnvelope(_hosted.HostSessionId, viewer, env);
    }

    private void HandleConsentCookies(RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        var cookies = MessagePackSerializer.Deserialize<RavaCastCookiePayload[]>(env.Payload) ?? [];
        if (_joined is not null && _joined.CastId == env.CastId)
        {
            _joined.ConsentCookies = cookies;
            _surface.ApplySharedConsentCookies(_joined.Url, cookies);
        }
        if (_activeCasts.TryGetValue(env.CastId, out var summary))
            _activeCasts[env.CastId] = summary with { ConsentCookies = cookies };
    }

    private static bool ConsentCookiesEqual(IReadOnlyList<RavaCastCookiePayload>? left, IReadOnlyList<RavaCastCookiePayload>? right)
    {
        left ??= [];
        right ??= [];
        if (left.Count != right.Count) return false;
        for (var i = 0; i < left.Count; i++)
        {
            var a = left[i];
            var b = right[i];
            if (!string.Equals(a.Name, b.Name, StringComparison.Ordinal)
                || !string.Equals(a.Value, b.Value, StringComparison.Ordinal)
                || !string.Equals(a.Domain, b.Domain, StringComparison.OrdinalIgnoreCase)
                || !string.Equals(a.Path, b.Path, StringComparison.Ordinal)
                || a.ExpiresUnixMs != b.ExpiresUnixMs
                || a.Secure != b.Secure
                || !string.Equals(a.SameSite, b.SameSite, StringComparison.OrdinalIgnoreCase))
                return false;
        }
        return true;
    }


    public bool UpdateHostedUrl(string url, out string error)
    {
        error = string.Empty;
        if (_hosted is null)
        {
            error = "No active RavaCast broadcast.";
            return false;
        }

        if (!TryValidatePublicWebUrl(url, out var uri, out error)) return false;

        _hosted.Url = uri.ToString();
        _hosted.SourceDomain = uri.Host;
        _hosted.MediaTitle = uri.Host;
        _hosted.IsPlaying = true;
        _hosted.PositionSeconds = 0;
        _hosted.DurationSeconds = null;
        _hosted.StateUtc = DateTime.UtcNow;
        _surface.Open(_hosted.Url, muted: _surface.Muted, _localVolume);
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
        return true;
    }

    public void UpdateHostedPlane(RavaCastPlane plane, bool broadcast = true)
    {
        if (_hosted is null) return;
        _hosted.Plane = plane;

        if (broadcast)
        {
            BroadcastHostedPlaneState();
            return;
        }

        ScheduleHostedPlaneFinalBroadcast();
    }

    private void BroadcastHostedPlaneState()
    {
        if (_hosted is null) return;
        CancelPendingHostedPlaneBroadcast();
        _lastHostedPlaneBroadcastTick = Environment.TickCount64;
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    private void ScheduleHostedPlaneFinalBroadcast()
    {
        if (_hosted is null) return;
        _pendingHostedPlaneFinalBroadcastTick = Environment.TickCount64 + LivePlaneFinalDebounceMs;
    }

    private void CancelPendingHostedPlaneBroadcast()
        => _pendingHostedPlaneFinalBroadcastTick = 0;

    private void CancelPendingHostedBrowserNavigationSync()
        => _pendingHostedBrowserNavigationSyncTick = 0;

    public bool ShouldBroadcastHostedPlaneNow(bool force)
    {
        if (force) return true;
        var now = Environment.TickCount64;
        return now - _lastHostedPlaneBroadcastTick >= LivePlaneBroadcastIntervalMs;
    }


    public void SetHostedMode(RavaCastMode mode)
    {
        if (_hosted is null || _hosted.Mode == mode) return;
        _hosted.Mode = mode;
        if (mode == RavaCastMode.DirectStream)
            StartHostedDirectStreamPublisher();
        else
            StopHostedDirectStream("Direct Stream switched back to URL Share.");
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    public void SetHostedDirectStreamQuality(RavaCastDirectStreamQuality quality)
    {
        if (_hosted is null || _hosted.DirectStreamQuality == quality) return;
        _hosted.DirectStreamQuality = quality;
        if (_hosted.Mode == RavaCastMode.DirectStream)
            StartHostedDirectStreamPublisher(forceRestart: true);
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    public void SetHostedPlayback(bool playing)
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.UrlShare) return;
        _hosted.IsPlaying = playing;
        _hosted.StateUtc = DateTime.UtcNow;
        _surface.ApplyMediaState(_hosted.PositionSeconds, playing, force: true);
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    public void SeekHosted(double positionSeconds)
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.UrlShare) return;
        _hosted.PositionSeconds = Math.Max(0, positionSeconds);
        _hosted.StateUtc = DateTime.UtcNow;
        _surface.ApplyMediaState(_hosted.PositionSeconds, _hosted.IsPlaying, force: true);
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    public bool QueueHostedUrl(string url, out string error)
    {
        error = string.Empty;
        if (_hosted is null)
        {
            error = "No active RavaCast broadcast.";
            return false;
        }

        if (!TryValidatePublicWebUrl(url, out var uri, out error)) return false;
        _hosted.Queue.Add(uri.ToString());
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
        return true;
    }

    public void ClearHostedQueue()
    {
        if (_hosted is null) return;
        _hosted.Queue.Clear();
        BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
        BroadcastHostedStateToJoined();
    }

    public bool PlayNextQueued(out string error)
    {
        error = string.Empty;
        if (_hosted is null)
        {
            error = "No active RavaCast broadcast.";
            return false;
        }

        if (_hosted.Queue.Count == 0)
        {
            error = "The RavaCast queue is empty.";
            return false;
        }

        var next = _hosted.Queue[0];
        _hosted.Queue.RemoveAt(0);
        return UpdateHostedUrl(next, out error);
    }

    public void Join(Guid castId, bool muted) => Join(castId, muted, null);

    public void Join(Guid castId, bool muted, string? passwordPlaintext)
    {
        if (!TryEnsurePlaybackBackendReady(out _)) return;
        if (!_activeCasts.TryGetValue(castId, out var summary)) return;
        var mySession = GetMySessionId();
        if (string.IsNullOrWhiteSpace(mySession)) return;

        _pendingJoinMuted[castId] = muted;
        string? passwordHash = null;
        if (summary.PasswordProtected)
            passwordHash = HashPassword(passwordPlaintext ?? string.Empty, summary.PasswordSalt);
        var payload = new RavaCastJoinPayload(mySession, _objects.LocalPlayer?.Name.TextValue ?? "Player", muted, passwordHash);
        SendEnvelope(mySession, summary.HostSessionId, new RavaCastEnvelope(castId, RavaCastOp.Join, MessagePackSerializer.Serialize(payload)));

        // Password-protected lobbies must wait for the host's accepted StateSnapshot before opening
        // the browser/Direct Stream receiver. Otherwise a wrong password would still let the viewer
        // open the advertised URL locally before the host can reject the join.
        if (summary.PasswordProtected)
            return;

        ApplyState(summary, muted, mySession);
        if (summary.Mode == RavaCastMode.DirectStream)
            StartJoinedDirectStreamReceiver(summary);
    }

    public void RequestState()
    {
        if (_joined is null) return;
        var mySession = GetMySessionId();
        if (string.IsNullOrWhiteSpace(mySession)) return;
        SendEnvelope(mySession, _joined.HostSessionId, new RavaCastEnvelope(_joined.CastId, RavaCastOp.RequestState, null));
    }

    public void SyncHostedNavigationFromBrowserSoon(int delayMs = 650)
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.UrlShare) return;
        _pendingHostedBrowserNavigationSyncTick = Environment.TickCount64 + Math.Clamp(delayMs, 100, 2500);
    }

    public bool NavigateCurrentBrowserFromText(string value, out string error)
    {
        error = string.Empty;
        var target = NormaliseBrowserNavigationText(value);
        if (string.IsNullOrWhiteSpace(target))
        {
            error = "Enter a URL, domain, or search text.";
            return false;
        }

        if (!TryValidatePublicWebUrl(target, out var uri, out error))
            return false;

        if (_hosted is not null)
            return UpdateHostedUrl(uri.ToString(), out error);

        if (_joined is null)
        {
            error = "No active RavaCast browser.";
            return false;
        }

        if (_joined.Mode == RavaCastMode.DirectStream)
        {
            error = "Direct Stream viewers receive the host's browser; navigation stays with the host.";
            return false;
        }

        _joined.Url = uri.ToString();
        _joined.SourceDomain = uri.Host;
        _joined.MediaTitle = uri.Host;
        _joined.IsPlaying = true;
        _joined.PositionSeconds = 0;
        _joined.DurationSeconds = null;
        _joined.StateUtc = DateTime.UtcNow;
        _surface.Open(_joined.Url, _joined.IsMuted, _joined.Volume);
        return true;
    }

    public void GoBackCurrentBrowser()
    {
        _surface.GoBack();
        if (_hosted is not null)
            SyncHostedNavigationFromBrowserSoon();
    }

    public void GoForwardCurrentBrowser()
    {
        // IRavaCastTextureBackend only exposes Back/Reload directly at the moment. Use the
        // browser-standard Alt+Right accelerator so we do not need to change every backend just
        // to support the Current Cast Forward button.
        _surface.SendBrowserKey(39, true, null, shift: false, ctrl: false, alt: true);
        _surface.SendBrowserKey(39, false, null, shift: false, ctrl: false, alt: true);
        if (_hosted is not null)
            SyncHostedNavigationFromBrowserSoon();
    }

    public void ReloadCurrentBrowser()
        => _surface.ReloadPage();

    public static string NormaliseBrowserNavigationText(string value)
    {
        var text = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text)) return string.Empty;
        if (Uri.TryCreate(text, UriKind.Absolute, out var absolute) && !string.IsNullOrWhiteSpace(absolute.Scheme))
            return absolute.ToString();
        if (text.Contains(' ') || !text.Contains('.'))
            return "https://www.google.com/search?q=" + Uri.EscapeDataString(text);
        return "https://" + text.TrimStart('/');
    }

    public void Leave()
    {
        if (_joined is not null)
        {
            var mySession = GetMySessionId();
            if (!string.IsNullOrWhiteSpace(mySession))
            {
                var payload = new RavaCastLeavePayload(mySession);
                SendEnvelope(mySession, _joined.HostSessionId, new RavaCastEnvelope(_joined.CastId, RavaCastOp.Leave, MessagePackSerializer.Serialize(payload)));
                SendEnvelope(mySession, _joined.HostSessionId, new RavaCastEnvelope(_joined.CastId, RavaCastOp.DirectStreamViewerLeft, MessagePackSerializer.Serialize(new RavaCastDirectStreamViewerPayload(mySession, _objects.LocalPlayer?.Name.TextValue ?? "Player"))));
            }
        }

        _surface.StopDirectStreamReceiver();
        _joined = null;
        _surface.Close();
    }

    public void SetLocalMuted(bool muted)
    {
        _surface.SetMuted(muted);
        if (_joined is not null) _joined.IsMuted = muted;
    }

    public void SetLocalVolume(float volume)
    {
        volume = NormaliseVolume(volume);
        if (Math.Abs(_localVolume - volume) < 0.001f) return;
        _localVolume = volume;
        _surface.SetVolume(volume);
        if (_joined is not null) _joined.Volume = volume;
    }

    public void PersistLocalVolume(float volume)
    {
        volume = NormaliseVolume(volume);
        if (Math.Abs(_config.Current.RavaCastDefaultVolume - volume) < 0.001f) return;
        _config.Current.RavaCastDefaultVolume = volume;
        _config.Save();
    }

    public void ReloadSurfaceForBrowserSettingsChange()
    {
        if (_hosted is not null)
        {
            var hosted = _hosted;
            var muted = _surface.Muted;
            var volume = _localVolume;

            StopHostedDirectStream("Browser settings changed.");
            _surface.Close();
            _surface.Open(hosted.Url, muted, volume);

            if (hosted.Mode == RavaCastMode.DirectStream)
                StartHostedDirectStreamPublisher(forceRestart: true);

            BroadcastHostedStateNearby(RavaCastOp.StateSnapshot);
            BroadcastHostedStateToJoined();
            return;
        }

        if (_joined is not null)
        {
            var joined = _joined;
            var summary = new RavaCastSummary(joined.CastId, joined.HostSessionId, joined.HostName, joined.CastName, joined.Url, joined.SourceDomain,
                joined.MediaTitle, joined.IsPlaying, joined.PositionSeconds, joined.DurationSeconds, joined.StateUtc, joined.JoinedCount,
                joined.Plane, joined.Queue, joined.ConsentCookies, Environment.TickCount64)
            {
                Mode = joined.Mode,
                PasswordProtected = joined.PasswordProtected,
                PasswordSalt = joined.PasswordSalt,
                DirectStreamQuality = joined.DirectStreamQuality,
                DirectStreamStatus = joined.DirectStreamStatus,
                DirectStreamDetail = joined.DirectStreamDetail
            };

            var muted = joined.IsMuted;
            var viewerSession = joined.ViewerSessionId;
            _surface.StopDirectStreamReceiver();
            _surface.Close();
            ApplyState(summary, muted, viewerSession);
        }
    }

    private bool TryEnsurePlaybackBackendReady(out string error)
    {
        var backend = _surface.BackendStatus;
        if (backend.IsAvailable)
        {
            error = string.Empty;
            return true;
        }

        error = string.IsNullOrWhiteSpace(backend.Detail)
            ? "RavaCast WebView2 renderer is missing from the plugin package. RavaCast.Renderer.exe must be bundled beside RavaSync.dll."
            : backend.Detail;
        return false;
    }

    private float GetDefaultVolume()
    {
        var raw = _config.Current.RavaCastDefaultVolume;
        var volume = raw <= 0.001f || float.IsNaN(raw) || float.IsInfinity(raw) ? 0.50f : NormaliseVolume(raw);
        if (Math.Abs(_config.Current.RavaCastDefaultVolume - volume) > 0.001f)
        {
            _config.Current.RavaCastDefaultVolume = volume;
            _config.Save();
        }
        return volume;
    }

    private static float NormaliseVolume(float volume)
    {
        if (float.IsNaN(volume) || float.IsInfinity(volume)) return 0.50f;
        return Math.Clamp(volume, 0.01f, 1f);
    }

    public RavaCastPlane BuildPlane(string screenName, Vector3 centre, float width, float height, float yawRadians, float pitchRadians = 0f)
    {
        width = Math.Max(0.1f, width);
        height = Math.Max(0.1f, height);
        yawRadians = WrapRadians(yawRadians);
        pitchRadians = Math.Clamp(float.IsFinite(pitchRadians) ? pitchRadians : 0f, DegreesToRadians(-80f), DegreesToRadians(80f));

        // Yaw represents the direction from the screen toward the viewer. Right must be the
        // viewer's right, otherwise the projected browser texture is readable only from the
        // back side and appears horizontally reversed from the player/camera side.
        // Pitch tilts the vertical axis around that right vector, allowing the top edge to lean
        // toward or away from the viewer while preserving the same four-corner wire format.
        var right = ScreenRightFromYaw(yawRadians);
        var normal = ScreenNormalFromYaw(yawRadians);
        var up = (Vector3.UnitY * MathF.Cos(pitchRadians)) + (normal * MathF.Sin(pitchRadians));
        if (!IsFinite(up) || up.LengthSquared() <= 0.0001f) up = Vector3.UnitY;
        else up = Vector3.Normalize(up);

        var halfRight = right * (width / 2f);
        var halfUp = up * (height / 2f);
        var territoryRaw = _dalamudUtil.CurrentTerritoryId;
        var territory = territoryRaw > ushort.MaxValue ? ushort.MaxValue : (ushort)territoryRaw;
        return new RavaCastPlane(territory, string.IsNullOrWhiteSpace(screenName) ? "RavaCast Screen" : screenName.Trim(),
            centre - halfRight + halfUp,
            centre + halfRight + halfUp,
            centre + halfRight - halfUp,
            centre - halfRight - halfUp);
    }

    public bool TryGetPlayerSuggestedPlacement(out Vector3 centre, out float yaw)
    {
        centre = Vector3.Zero;
        yaw = 0f;
        if (!_clientState.IsLoggedIn || _objects.LocalPlayer is null) return false;
        var player = _objects.LocalPlayer;
        var forward = new Vector3(MathF.Sin(player.Rotation), 0, MathF.Cos(player.Rotation));
        centre = player.Position + forward * 2.35f + Vector3.UnitY * 1.45f;
        yaw = TryGetScreenYawFacingCameraOrPlayer(centre, out var facingYaw) ? facingYaw : WrapRadians(player.Rotation + MathF.PI);
        return true;
    }

    public unsafe bool TryPickScreenPlacementFromCursor(Vector2 screenPoint, Vector2 viewportSize, float preferredCentreY, float screenHeight, out Vector3 centre, out float yaw, out string error)
    {
        centre = Vector3.Zero;
        yaw = 0f;
        error = string.Empty;

        if (!_clientState.IsLoggedIn || _objects.LocalPlayer is null)
        {
            error = "You need to be logged in before RavaCast can place a screen.";
            return false;
        }

        if (viewportSize.X <= 1f || viewportSize.Y <= 1f)
        {
            error = "RavaCast could not read the game view size.";
            return false;
        }

        if (!TryReadViewProjectionMatrix(out var viewProjection) || !Matrix4x4.Invert(viewProjection, out var inverseViewProjection))
        {
            error = "RavaCast could not read the camera this frame.";
            return false;
        }

        var depthError = string.Empty;
        if (TrySampleSceneDepthAtScreenPoint(screenPoint, viewportSize, out var depth, out depthError)
            && TryResolveDepthWorldPoint(screenPoint, viewportSize, depth, inverseViewProjection, out var depthWorld, out depthError))
        {
            // The click path means exactly that: use the resolved world coordinate under the cursor.
            // Do not add half the screen height here. That made floor/wall clicks visibly drift away
            // from the actual picked depth point and made placement feel random.
            centre = depthWorld;
        }
        else if (TryBuildCameraRay(screenPoint, viewportSize, inverseViewProjection, out var rayOrigin, out var rayDirection)
            && TryIntersectHorizontalPlane(rayOrigin, rayDirection, ResolvePlacementHeight(preferredCentreY, screenHeight), out var planeWorld)
            && IsFinite(planeWorld))
        {
            centre = planeWorld;
            // Normal placement fallback; keep RavaCast logs quiet on healthy paths.
        }
        else
        {
            error = "RavaCast could not place the screen from that click. Try clicking the ground or another visible part of the game world.";
            return false;
        }

        if (!TryGetScreenYawFacingCameraOrPlayer(centre, out yaw))
            yaw = WrapRadians(_objects.LocalPlayer!.Rotation + MathF.PI);

        if (!IsFinite(centre) || !float.IsFinite(yaw))
        {
            error = "RavaCast could not use that screen position.";
            return false;
        }

        return true;
    }

    public unsafe bool TryProjectScreenPlaneToViewport(RavaCastPlane plane, Vector2 viewportSize, out Vector2 topLeft, out Vector2 topRight, out Vector2 bottomRight, out Vector2 bottomLeft, out string error)
    {
        topLeft = topRight = bottomRight = bottomLeft = Vector2.Zero;
        error = string.Empty;
        if (viewportSize.X <= 1f || viewportSize.Y <= 1f)
        {
            error = "RavaCast could not read the game view size.";
            return false;
        }

        if (!TryReadViewProjectionMatrix(out var viewProj))
        {
            error = "RavaCast could not read the camera this frame.";
            return false;
        }

        if (!TryProjectWorldToViewport(plane.TopLeft, viewProj, viewportSize, out topLeft)
            || !TryProjectWorldToViewport(plane.TopRight, viewProj, viewportSize, out topRight)
            || !TryProjectWorldToViewport(plane.BottomRight, viewProj, viewportSize, out bottomRight)
            || !TryProjectWorldToViewport(plane.BottomLeft, viewProj, viewportSize, out bottomLeft))
        {
            error = "Screen handles are off-screen or behind the camera.";
            return false;
        }

        return true;
    }

    private static unsafe bool TryReadViewProjectionMatrix(out Matrix4x4 viewProj)
    {
        viewProj = Matrix4x4.Identity;
        try
        {
            var control = GameControl.Instance();
            if (control is null) return false;

            var result = Matrix4x4.Identity;
            var src = (float*)&control->ViewProjectionMatrix;
            var dst = (float*)&result;
            for (var i = 0; i < 16; i++)
                dst[i] = src[i];

            viewProj = result;
            return true;
        }
        catch
        {
            viewProj = Matrix4x4.Identity;
            return false;
        }
    }

    private unsafe bool TrySampleSceneDepthAtScreenPoint(Vector2 screenPoint, Vector2 viewportSize, out float depth, out string error)
    {
        depth = 0f;
        error = string.Empty;

        try
        {
            var kernelDevice = GameKernelDevice.Instance();
            var renderTargets = GameRenderTargetManager.Instance();
            var depthStencil = renderTargets is not null ? renderTargets->DepthStencil : null;
            if (kernelDevice is null || kernelDevice->D3D11Forwarder == null || depthStencil is null || depthStencil->D3D11Texture2D == null)
            {
                error = "scene depth texture unavailable";
                return false;
            }

            var devicePtr = (IntPtr)kernelDevice->D3D11Forwarder;
            var depthPtr = (IntPtr)depthStencil->D3D11Texture2D;
            if (devicePtr == IntPtr.Zero || depthPtr == IntPtr.Zero)
            {
                error = "scene depth texture unavailable";
                return false;
            }

            Marshal.AddRef(devicePtr);
            using var d3dDevice = new SharpDX.Direct3D11.Device(devicePtr);
            using var context = d3dDevice.ImmediateContext;

            Marshal.AddRef(depthPtr);
            using var depthTexture = new SharpDX.Direct3D11.Texture2D(depthPtr);
            var desc = depthTexture.Description;
            if (desc.Width <= 1 || desc.Height <= 1)
            {
                error = "scene depth texture size invalid";
                return false;
            }

            if (desc.SampleDescription.Count > 1)
            {
                error = $"scene depth is multisampled ({desc.SampleDescription.Count}x)";
                return false;
            }

            var x = Math.Clamp((int)MathF.Round(screenPoint.X / Math.Max(1f, viewportSize.X) * (desc.Width - 1)), 0, desc.Width - 1);
            var y = Math.Clamp((int)MathF.Round(screenPoint.Y / Math.Max(1f, viewportSize.Y) * (desc.Height - 1)), 0, desc.Height - 1);
            var stagingDesc = new SharpDX.Direct3D11.Texture2DDescription
            {
                Width = 1,
                Height = 1,
                MipLevels = 1,
                ArraySize = 1,
                Format = desc.Format,
                SampleDescription = new SharpDX.DXGI.SampleDescription(1, 0),
                Usage = SharpDX.Direct3D11.ResourceUsage.Staging,
                BindFlags = SharpDX.Direct3D11.BindFlags.None,
                CpuAccessFlags = SharpDX.Direct3D11.CpuAccessFlags.Read,
                OptionFlags = SharpDX.Direct3D11.ResourceOptionFlags.None
            };

            using var staging = new SharpDX.Direct3D11.Texture2D(d3dDevice, stagingDesc);
            var region = new SharpDX.Direct3D11.ResourceRegion(x, y, 0, x + 1, y + 1, 1);
            context.CopySubresourceRegion(depthTexture, 0, region, staging, 0);
            var box = context.MapSubresource(staging, 0, SharpDX.Direct3D11.MapMode.Read, SharpDX.Direct3D11.MapFlags.None);
            try
            {
                if (!TryReadDepthValue(desc.Format, box.DataPointer, out depth))
                {
                    error = "unsupported scene depth format: " + desc.Format;
                    return false;
                }
            }
            finally
            {
                context.UnmapSubresource(staging, 0);
            }

            if (!float.IsFinite(depth) || depth <= 0.000001f || depth >= 0.999999f)
            {
                error = $"scene depth at click was empty ({depth:0.000000})";
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }
    }

    private static unsafe bool TryReadDepthValue(SharpDX.DXGI.Format format, IntPtr ptr, out float depth)
    {
        depth = 0f;
        if (ptr == IntPtr.Zero) return false;

        switch (format)
        {
            case SharpDX.DXGI.Format.D32_Float:
            case SharpDX.DXGI.Format.R32_Float:
            case SharpDX.DXGI.Format.R32_Typeless:
                depth = *(float*)ptr;
                return true;
            case SharpDX.DXGI.Format.D24_UNorm_S8_UInt:
            case SharpDX.DXGI.Format.R24G8_Typeless:
                depth = (*(uint*)ptr & 0x00FFFFFFu) / 16777215f;
                return true;
            case SharpDX.DXGI.Format.D16_UNorm:
            case SharpDX.DXGI.Format.R16_UNorm:
            case SharpDX.DXGI.Format.R16_Typeless:
                depth = *(ushort*)ptr / 65535f;
                return true;
            default:
                return false;
        }
    }

    private static unsafe bool TryResolveDepthWorldPoint(Vector2 screenPoint, Vector2 viewportSize, float rawDepth, Matrix4x4 inverseViewProjection, out Vector3 world, out string error)
    {
        world = Vector3.Zero;
        error = string.Empty;

        var cameraKnown = TryGetCameraPosition(out var cameraPos);
        var bestScore = float.MaxValue;
        var best = Vector3.Zero;
        var found = false;

        // Some FFXIV/DX paths expose scene depth in the opposite direction to the clip depth we
        // need for unprojection. Test both raw and inverted depths and reject anything that lands
        // on/inside the camera, which was the cause of screens spawning at the camera position.
        Span<float> candidates = stackalloc float[2] { rawDepth, 1f - rawDepth };
        for (var i = 0; i < candidates.Length; i++)
        {
            var depth = Math.Clamp(candidates[i], 0.00001f, 0.99999f);
            if (!TryUnprojectScreenPoint(screenPoint, viewportSize, depth, inverseViewProjection, out var candidate) || !IsFinite(candidate))
                continue;

            var distance = cameraKnown ? Vector3.Distance(cameraPos, candidate) : 1f;
            if (!float.IsFinite(distance) || distance < 0.35f || distance > 120f)
                continue;

            // Prefer the point that reprojects closest to the original mouse coordinate, then prefer
            // saner distances. Reprojection is important because a bogus matrix/depth read can still
            // return a finite point that is nowhere near the clicked pixel.
            var reprojectionScore = 0f;
            if (TryReadViewProjectionMatrix(out var viewProj) && TryProjectWorldToViewport(candidate, viewProj, viewportSize, out var reproj))
                reprojectionScore = Vector2.DistanceSquared(reproj, screenPoint);
            else
                reprojectionScore = 256f;

            if (reprojectionScore > 4096f)
                continue;

            var score = reprojectionScore + MathF.Abs(distance - 4f) * 0.05f;
            if (score >= bestScore) continue;

            bestScore = score;
            best = candidate;
            found = true;
        }

        if (!found)
        {
            error = $"scene depth resolved only to the camera/invalid space ({rawDepth:0.000000})";
            return false;
        }

        world = best;
        return true;
    }

    private static bool TryUnprojectScreenPoint(Vector2 screenPoint, Vector2 viewportSize, float depth, Matrix4x4 inverseViewProjection, out Vector3 world)
    {
        world = Vector3.Zero;
        if (viewportSize.X <= 1f || viewportSize.Y <= 1f) return false;
        var ndcX = Math.Clamp((screenPoint.X / viewportSize.X) * 2f - 1f, -1f, 1f);
        var ndcY = Math.Clamp(1f - (screenPoint.Y / viewportSize.Y) * 2f, -1f, 1f);
        world = TransformClipPoint(new Vector3(ndcX, ndcY, Math.Clamp(depth, 0f, 1f)), inverseViewProjection);
        return IsFinite(world);
    }

    private static bool TryBuildCameraRay(Vector2 screenPoint, Vector2 viewportSize, Matrix4x4 inverseViewProjection, out Vector3 origin, out Vector3 direction)
    {
        origin = Vector3.Zero;
        direction = Vector3.UnitZ;
        if (viewportSize.X <= 1f || viewportSize.Y <= 1f) return false;

        var ndcX = Math.Clamp((screenPoint.X / viewportSize.X) * 2f - 1f, -1f, 1f);
        var ndcY = Math.Clamp(1f - (screenPoint.Y / viewportSize.Y) * 2f, -1f, 1f);
        var near = TransformClipPoint(new Vector3(ndcX, ndcY, 0f), inverseViewProjection);
        var far = TransformClipPoint(new Vector3(ndcX, ndcY, 1f), inverseViewProjection);
        var delta = far - near;
        if (!IsFinite(near) || !IsFinite(far) || delta.LengthSquared() <= 0.000001f) return false;

        origin = TryGetCameraPosition(out var cameraPos) ? cameraPos : near;
        direction = Vector3.Normalize(delta);
        return IsFinite(origin) && IsFinite(direction);
    }

    private static bool TryIntersectHorizontalPlane(Vector3 rayOrigin, Vector3 rayDirection, float y, out Vector3 world)
    {
        world = Vector3.Zero;
        if (Math.Abs(rayDirection.Y) <= 0.00001f) return false;
        var t = (y - rayOrigin.Y) / rayDirection.Y;
        if (!float.IsFinite(t) || t <= 0.05f || t > 100f) return false;
        world = rayOrigin + rayDirection * t;
        return IsFinite(world);
    }

    private float ResolvePlacementHeight(float preferredCentreY, float screenHeight)
    {
        if (float.IsFinite(preferredCentreY) && Math.Abs(preferredCentreY) > 0.001f)
            return preferredCentreY;
        var player = _objects.LocalPlayer;
        var height = Math.Clamp(screenHeight, 0.5f, 4.0f);
        return (player?.Position.Y ?? 0f) + height / 2f;
    }

    private bool TryGetScreenYawFacingCameraOrPlayer(Vector3 centre, out float yaw)
    {
        yaw = 0f;
        var target = Vector3.Zero;
        if (TryGetCameraPosition(out var cameraPos))
            target = cameraPos;
        else if (_objects.LocalPlayer is not null)
            target = _objects.LocalPlayer.Position;
        else
            return false;

        var normal = target - centre;
        normal.Y = 0f;
        if (!IsFinite(normal) || normal.LengthSquared() <= 0.0001f)
            return false;

        normal = Vector3.Normalize(normal);
        yaw = WrapRadians(MathF.Atan2(normal.X, normal.Z));
        return float.IsFinite(yaw);
    }

    private static bool TryProjectWorldToViewport(Vector3 world, Matrix4x4 viewProjection, Vector2 viewportSize, out Vector2 screen)
    {
        screen = Vector2.Zero;
        var clip = Vector4.Transform(new Vector4(world, 1f), viewProjection);
        if (!float.IsFinite(clip.W) || Math.Abs(clip.W) <= 0.000001f)
            return false;

        var ndcX = clip.X / clip.W;
        var ndcY = clip.Y / clip.W;
        var ndcZ = clip.Z / clip.W;
        if (!float.IsFinite(ndcX) || !float.IsFinite(ndcY) || !float.IsFinite(ndcZ) || clip.W <= 0f)
            return false;

        screen = new Vector2(
            (ndcX + 1f) * 0.5f * viewportSize.X,
            (1f - ndcY) * 0.5f * viewportSize.Y);
        return IsFinite(new Vector3(screen.X, screen.Y, 0f));
    }

    private static Vector3 TransformClipPoint(Vector3 clip, Matrix4x4 inverseViewProjection)
    {
        var v = Vector4.Transform(new Vector4(clip, 1f), inverseViewProjection);
        if (Math.Abs(v.W) > 0.000001f)
            return new Vector3(v.X / v.W, v.Y / v.W, v.Z / v.W);
        return new Vector3(v.X, v.Y, v.Z);
    }

    private static unsafe bool TryGetCameraPosition(out Vector3 cameraPos)
    {
        cameraPos = Vector3.Zero;
        try
        {
            var camera = CameraManager.Instance()->CurrentCamera;
            if (camera is null) return false;
            cameraPos = new Vector3(camera->Position.X, camera->Position.Y, camera->Position.Z);
            return IsFinite(cameraPos);
        }
        catch
        {
            return false;
        }
    }

    private static Vector3 ScreenRightFromYaw(float yaw) => new(-MathF.Cos(yaw), 0f, MathF.Sin(yaw));
    private static Vector3 ScreenNormalFromYaw(float yaw) => new(MathF.Sin(yaw), 0f, MathF.Cos(yaw));
    private static float DegreesToRadians(float degrees) => degrees * (MathF.PI / 180f);

    private static float WrapRadians(float radians)
    {
        while (radians > MathF.PI) radians -= MathF.Tau;
        while (radians < -MathF.PI) radians += MathF.Tau;
        return radians;
    }

    private static bool IsFinite(Vector3 value) => float.IsFinite(value.X) && float.IsFinite(value.Y) && float.IsFinite(value.Z);

    private void OnGameMesh(SyncshellGameMeshMessage msg)
    {
        if (!TryReadEnvelope(msg.Payload, out var env) || env is null) return;

        try
        {
            switch (env.Op)
            {
                case RavaCastOp.Advertise:
                    HandleAdvertise(env);
                    break;
                case RavaCastOp.Join:
                    HandleJoin(msg.FromSessionId, env);
                    break;
                case RavaCastOp.Leave:
                    HandleLeave(env);
                    break;
                case RavaCastOp.StateSnapshot:
                    HandleStateSnapshot(env);
                    break;
                case RavaCastOp.RequestState:
                    HandleRequestState(msg.FromSessionId, env);
                    break;
                case RavaCastOp.ScreenClosed:
                    HandleScreenClosed(env);
                    break;
                case RavaCastOp.ConsentCookies:
                    HandleConsentCookies(env);
                    break;
                case RavaCastOp.DirectStreamStart:
                    HandleDirectStreamStart(msg.FromSessionId, env);
                    break;
                case RavaCastOp.DirectStreamStop:
                    HandleDirectStreamStop(env);
                    break;
                case RavaCastOp.DirectStreamViewerReady:
                    HandleDirectStreamViewerReady(msg.FromSessionId, env);
                    break;
                case RavaCastOp.DirectStreamViewerLeft:
                    HandleDirectStreamViewerLeft(msg.FromSessionId, env);
                    break;
                case RavaCastOp.DirectStreamOffer:
                case RavaCastOp.DirectStreamAnswer:
                case RavaCastOp.DirectStreamIce:
                    HandleDirectStreamSignal(msg.FromSessionId, env);
                    break;
                case RavaCastOp.DirectStreamSignalChunk:
                    HandleDirectStreamSignalChunk(msg.FromSessionId, env);
                    break;
                case RavaCastOp.DirectStreamStats:
                    HandleDirectStreamStats(env);
                    break;
                case RavaCastOp.DirectStreamError:
                    HandleDirectStreamError(msg.FromSessionId, env);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to handle RavaCast mesh payload");
        }
    }

    private void HandleAdvertise(RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        var state = MessagePackSerializer.Deserialize<RavaCastStatePayload>(env.Payload);
        var summary = ToSummary(env.CastId, state, Environment.TickCount64);
        _activeCasts[env.CastId] = summary;

        if (_joined is not null && _joined.CastId == env.CastId)
            ApplyState(summary, _joined.IsMuted);
    }

    private void HandleStateSnapshot(RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        var state = MessagePackSerializer.Deserialize<RavaCastStatePayload>(env.Payload);
        var summary = ToSummary(env.CastId, state, Environment.TickCount64);
        _activeCasts[env.CastId] = summary;

        if (_pendingJoinMuted.TryRemove(env.CastId, out var muted))
            ApplyState(summary, muted);
        else if (_joined is not null && _joined.CastId == env.CastId)
            ApplyState(summary, _joined.IsMuted);
    }

    private void HandleJoin(string fromSessionId, RavaCastEnvelope env)
    {
        if (_hosted is null || _hosted.CastId != env.CastId || env.Payload is null) return;
        var payload = MessagePackSerializer.Deserialize<RavaCastJoinPayload>(env.Payload);
        var viewerSession = !string.IsNullOrWhiteSpace(payload.ViewerSessionId) ? payload.ViewerSessionId : fromSessionId;
        if (string.IsNullOrWhiteSpace(viewerSession)) return;
        if (_hosted.PasswordProtected && !string.Equals(payload.PasswordHash ?? string.Empty, _hosted.PasswordHash, StringComparison.Ordinal))
        {
            _logger.LogWarning("Rejected RavaCast join from {viewerSession}: wrong password for cast {castId}.", viewerSession, env.CastId);
            return;
        }
        _joinedViewers[viewerSession] = true;
        SendHostedState(viewerSession, RavaCastOp.StateSnapshot);
        if (_hosted.Mode == RavaCastMode.DirectStream)
        {
            StartHostedDirectStreamPublisher(notifyExistingViewers: false);
            SendDirectStreamStart(viewerSession);
        }
        BroadcastHostedStateNearby(RavaCastOp.Advertise);
    }

    private void HandleLeave(RavaCastEnvelope env)
    {
        if (_hosted is null || _hosted.CastId != env.CastId || env.Payload is null) return;
        var payload = MessagePackSerializer.Deserialize<RavaCastLeavePayload>(env.Payload);
        if (!string.IsNullOrWhiteSpace(payload.ViewerSessionId))
        {
            _joinedViewers.TryRemove(payload.ViewerSessionId, out _);
            _directStreamReadyViewers.TryRemove(payload.ViewerSessionId, out _);
            _surface.RemoveDirectStreamPeer(payload.ViewerSessionId);
            StopHostedDirectStreamIfNoViewers();
        }
        BroadcastHostedStateNearby(RavaCastOp.Advertise);
    }

    private void HandleRequestState(string fromSessionId, RavaCastEnvelope env)
    {
        if (_hosted is null || _hosted.CastId != env.CastId) return;
        var target = !string.IsNullOrWhiteSpace(fromSessionId) ? fromSessionId : string.Empty;
        if (!string.IsNullOrWhiteSpace(target))
            SendHostedState(target, RavaCastOp.StateSnapshot);
    }

    private void HandleScreenClosed(RavaCastEnvelope env)
    {
        _activeCasts.TryRemove(env.CastId, out _);
        RemoveDirectStreamSignalAssembliesForCast(env.CastId);
        if (_joined is not null && _joined.CastId == env.CastId)
        {
            _surface.StopDirectStreamReceiver();
            _joined = null;
            _surface.Close();
        }
    }

    private void ApplyState(RavaCastSummary summary, bool muted, string? viewerSessionIdOverride = null)
    {
        var previous = _joined;
        var sameCast = previous is not null && previous.CastId == summary.CastId;
        var sameUrl = previous is not null && string.Equals(previous.Url, summary.Url, StringComparison.OrdinalIgnoreCase);
        var effectiveMuted = sameCast ? previous!.IsMuted : muted;
        var effectiveVolume = sameCast ? previous!.Volume : _localVolume;
        var effectiveViewerSessionId = !string.IsNullOrWhiteSpace(viewerSessionIdOverride)
            ? viewerSessionIdOverride
            : sameCast && !string.IsNullOrWhiteSpace(previous!.ViewerSessionId)
                ? previous.ViewerSessionId
                : GetMySessionId();
        var sameDirectStreamRequest = sameCast && previous!.Mode == summary.Mode && previous.DirectStreamQuality == summary.DirectStreamQuality;
        var directStreamReceiverRequested = sameDirectStreamRequest && previous.DirectStreamReceiverRequested;
        var effectiveDirectStreamStatus = sameCast && !string.IsNullOrWhiteSpace(previous!.DirectStreamStatus) ? previous.DirectStreamStatus : summary.DirectStreamStatus;
        var effectiveDirectStreamDetail = sameCast && !string.IsNullOrWhiteSpace(previous!.DirectStreamDetail) ? previous.DirectStreamDetail : summary.DirectStreamDetail;
        // URL Share viewers must not re-open just because the browser's current URL changed after a normal
        // site redirect/history update. The host's shared URL is the navigation request; the renderer's CurrentUrl
        // is the live browser URL and can legitimately differ (YouTube is especially noisy here). Re-opening on
        // CurrentUrl mismatch caused joined URL Share viewers to reload forever.
        var shouldOpenSurface = !sameCast || !sameUrl || !_surface.IsOpen;
        // URL Share should not run a network-style clock against the page. The local browser owns
        // its own audio/video clock; RavaCast only uses host media state as the initial join/new-URL
        // position, then leaves the browser to stay internally synced.
        var effectivePlaybackPosition = Math.Max(0, summary.PositionSeconds);

        _joined = new JoinedCast
        {
            CastId = summary.CastId,
            HostSessionId = summary.HostSessionId,
            ViewerSessionId = effectiveViewerSessionId,
            HostName = summary.HostName,
            CastName = summary.CastName,
            Url = summary.Url,
            SourceDomain = summary.SourceDomain,
            MediaTitle = summary.MediaTitle,
            IsPlaying = summary.IsPlaying,
            PositionSeconds = effectivePlaybackPosition,
            DurationSeconds = summary.DurationSeconds,
            StateUtc = summary.StateUtc,
            JoinedCount = summary.JoinedCount,
            Plane = summary.Plane,
            Queue = summary.Queue,
            ConsentCookies = summary.ConsentCookies,
            PasswordProtected = summary.PasswordProtected,
            PasswordSalt = summary.PasswordSalt,
            IsMuted = effectiveMuted,
            Volume = effectiveVolume,
            Mode = summary.Mode,
            DirectStreamQuality = summary.DirectStreamQuality,
            DirectStreamStatus = effectiveDirectStreamStatus,
            DirectStreamDetail = effectiveDirectStreamDetail,
            DirectStreamReceiverRequested = directStreamReceiverRequested
        };

        if (summary.Mode == RavaCastMode.DirectStream)
        {
            // Direct Stream viewers must not fall back to opening/controlling the shared URL locally. The
            // receiver surface is supplied by the Direct Stream bridge; if that bridge fails, show the error
            // instead of silently behaving like URL Share.
            if (previous?.DirectStreamReceiverRequested != true && _surface.IsOpen)
                _surface.Close();
            StartJoinedDirectStreamReceiver(summary);
            return;
        }

        // Switching back to URL Share must forcibly leave any previous Direct Stream receiver visual state.
        // A stale receiver texture can otherwise keep the RavaCast surface black while the local browser/audio are fine.
        if (previous?.DirectStreamReceiverRequested == true)
            _surface.StopDirectStreamReceiver();

        _surface.ApplySharedConsentCookies(summary.Url, summary.ConsentCookies);
        if (shouldOpenSurface)
            _surface.Open(summary.Url, effectiveMuted, effectiveVolume);
        _surface.ApplyMediaState(effectivePlaybackPosition, summary.IsPlaying, force: shouldOpenSurface || !sameCast || !sameUrl);

        // Only navigate the local URL Share browser when the host URL actually changed
        // or the surface had to be opened. After that, leave the viewer's WebView alone
        // so normal site redirects/history/playback state do not get hammered back to
        // the original URL on every state refresh.
    }

    private RavaCastSummary ToSummary(Guid castId, RavaCastStatePayload state, long lastSeenTick)
    {
        var stateUtc = DateTimeOffset.FromUnixTimeMilliseconds(state.StateUnixMs <= 0 ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : state.StateUnixMs).UtcDateTime;
        return new RavaCastSummary(castId, state.HostSessionId ?? string.Empty, state.HostName ?? "Player", state.CastName ?? "RavaCast",
            state.Url ?? string.Empty, state.SourceDomain ?? string.Empty, state.MediaTitle ?? string.Empty, state.IsPlaying,
            Math.Max(0, state.PositionSeconds), state.DurationSeconds, stateUtc, Math.Max(0, state.JoinedCount), state.Plane.ToPlane(), state.Queue ?? [], state.ConsentCookies ?? [], lastSeenTick)
        {
            Mode = state.Mode,
            PasswordProtected = state.PasswordProtected,
            PasswordSalt = state.PasswordSalt ?? string.Empty,
            DirectStreamQuality = state.DirectStreamQuality,
            DirectStreamStatus = state.DirectStreamStatus ?? string.Empty,
            DirectStreamDetail = state.DirectStreamDetail ?? string.Empty,
            DirectStreamNativeMediaAvailable = state.DirectStreamNativeMediaAvailable
        };
    }

    private RavaCastStatePayload BuildStatePayload()
    {
        if (_hosted is null) throw new InvalidOperationException("No hosted RavaCast");
        var ds = _surface.DirectStreamStatus;
        return new RavaCastStatePayload(_hosted.HostSessionId, _hosted.HostName, _hosted.CastName, _hosted.Url, _hosted.SourceDomain,
            _hosted.MediaTitle, _hosted.IsPlaying, Math.Max(0, _hosted.PositionSeconds), _hosted.DurationSeconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            _joinedViewers.Count, RavaCastPlanePayload.FromPlane(_hosted.Plane), _hosted.Queue.ToArray(), _hosted.ConsentCookies)
        {
            Mode = _hosted.Mode,
            PasswordProtected = _hosted.PasswordProtected,
            PasswordSalt = _hosted.PasswordSalt,
            DirectStreamQuality = _hosted.DirectStreamQuality,
            DirectStreamStatus = GetHostedDirectStreamStatus(ds),
            DirectStreamDetail = GetHostedDirectStreamDetail(ds),
            DirectStreamNativeMediaAvailable = ds.NativeMediaAvailable
        };
    }

    private string GetHostedDirectStreamStatus(RavaCastDirectStreamBackendStatus ds)
    {
        if (_hosted is null) return ds.StatusText;
        if (_hosted.Mode != RavaCastMode.DirectStream)
            return string.IsNullOrWhiteSpace(_hosted.DirectStreamStatus) ? ds.StatusText : _hosted.DirectStreamStatus;
        if (!ds.PublisherActive && !ds.ReceiverActive && !string.IsNullOrWhiteSpace(_hosted.DirectStreamStatus))
            return _hosted.DirectStreamStatus;
        return ds.StatusText;
    }

    private string GetHostedDirectStreamDetail(RavaCastDirectStreamBackendStatus ds)
    {
        if (_hosted is null) return ds.Detail ?? string.Empty;
        if (_hosted.Mode != RavaCastMode.DirectStream)
            return string.IsNullOrWhiteSpace(_hosted.DirectStreamDetail) ? ds.Detail ?? string.Empty : _hosted.DirectStreamDetail;
        if (!ds.PublisherActive && !ds.ReceiverActive && !string.IsNullOrWhiteSpace(_hosted.DirectStreamDetail))
            return _hosted.DirectStreamDetail;
        return ds.Detail ?? string.Empty;
    }


    private void BroadcastHostedStateNearby(RavaCastOp op)
    {
        if (_hosted is null) return;
        var env = new RavaCastEnvelope(_hosted.CastId, op, MessagePackSerializer.Serialize(BuildStatePayload()));
        SendEnvelopeNearby(_hosted.HostSessionId, env);
    }

    private void BroadcastHostedStateToJoined()
    {
        if (_hosted is null) return;
        foreach (var viewer in _joinedViewers.Keys.ToArray())
            SendHostedState(viewer, RavaCastOp.StateSnapshot);
    }

    private void SendHostedState(string targetSessionId, RavaCastOp op)
    {
        if (_hosted is null || string.IsNullOrWhiteSpace(targetSessionId)) return;
        var env = new RavaCastEnvelope(_hosted.CastId, op, MessagePackSerializer.Serialize(BuildStatePayload()));
        SendEnvelope(_hosted.HostSessionId, targetSessionId, env);
    }


    private void StartHostedDirectStreamPublisher(bool notifyExistingViewers = true, bool forceRestart = false)
    {
        if (_hosted is null) return;

        var viewers = _joinedViewers.Keys.ToArray();
        if (viewers.Length == 0)
        {
            // Do not spin up BridgeHost/libdatachannel/FFmpeg just because the owner selected Direct Stream.
            // The heavy media path starts lazily when the first viewer actually joins. This keeps selecting
            // Direct Stream essentially free for the game and avoids the constant host-side hitches reported
            // while the stream is only being prepared.
            _hosted.DirectStreamPublisherRequested = false;
            _hosted.DirectStreamStatus = "Ready — waiting for viewers";
            _hosted.DirectStreamDetail = "Direct Stream will start when someone joins.";
            return;
        }

        var status = _surface.DirectStreamStatus;
        if (!status.PublisherActive && DirectStreamStatusMeansStoppedOrFailed(status))
            _hosted.DirectStreamPublisherRequested = false;

        if ((status.PublisherActive || _hosted.DirectStreamPublisherRequested) && !forceRestart)
        {
            if (notifyExistingViewers)
                foreach (var viewer in viewers)
                    SendDirectStreamStart(viewer);
            return;
        }

        if ((status.PublisherActive || _hosted.DirectStreamPublisherRequested) && forceRestart)
        {
            _surface.StopDirectStreamPublisher();
            _directStreamReadyViewers.Clear();
            _hosted.DirectStreamPublisherRequested = false;
        }

        _hosted.DirectStreamPublisherRequested = true;
        if (_surface.StartDirectStreamPublisher(_hosted.CastId, _hosted.DirectStreamQuality, out var error))
        {
            _hosted.DirectStreamStatus = "Starting Direct Stream";
            _hosted.DirectStreamDetail = string.Empty;
            if (notifyExistingViewers)
                foreach (var viewer in viewers)
                    SendDirectStreamStart(viewer);
            return;
        }

        // Direct Stream is an explicit mode. Do not silently downgrade to URL Share when the media bridge
        // fails; that hides the real problem and can make viewers think they are testing Direct Stream when
        // they are only seeing their local URL-share browser. Keep the cast in Direct Stream and surface the
        // bridge/startup error clearly.
        _hosted.DirectStreamPublisherRequested = false;
        _surface.StopDirectStreamPublisher();
        _hosted.DirectStreamStatus = "Direct Stream failed";
        _hosted.DirectStreamDetail = error;
        _logger.LogWarning("RavaCast Direct Stream publisher could not start: {error}", error);
    }

    private static bool DirectStreamStatusMeansStoppedOrFailed(RavaCastDirectStreamBackendStatus status)
    {
        var text = status.StatusText ?? string.Empty;
        var detail = status.Detail ?? string.Empty;
        return text.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || text.Contains("error", StringComparison.OrdinalIgnoreCase)
            || text.Contains("stopped", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("error", StringComparison.OrdinalIgnoreCase);
    }

    private void StopHostedDirectStreamIfNoViewers()
    {
        if (_hosted is null || _hosted.Mode != RavaCastMode.DirectStream || !_joinedViewers.IsEmpty) return;
        _surface.StopDirectStreamPublisher();
        _directStreamReadyViewers.Clear();
        _hosted.DirectStreamPublisherRequested = false;
        _hosted.DirectStreamStatus = "Ready — waiting for viewers";
        _hosted.DirectStreamDetail = "Direct Stream will restart when another viewer joins.";
    }

    private void StopHostedDirectStream(string reason)
    {
        if (_hosted is null) return;
        var payload = MessagePackSerializer.Serialize(new RavaCastDirectStreamStopPayload(reason));
        var env = new RavaCastEnvelope(_hosted.CastId, RavaCastOp.DirectStreamStop, payload);
        foreach (var viewer in _joinedViewers.Keys.ToArray())
            SendEnvelope(_hosted.HostSessionId, viewer, env);
        _surface.StopDirectStreamPublisher();
        _directStreamReadyViewers.Clear();
        _hosted.DirectStreamPublisherRequested = false;
        _hosted.DirectStreamStatus = "Direct Stream stopped";
        _hosted.DirectStreamDetail = reason;
    }

    private void SendDirectStreamStart(string viewerSessionId)
    {
        if (_hosted is null || string.IsNullOrWhiteSpace(viewerSessionId)) return;
        var payload = new RavaCastDirectStreamStartPayload(_hosted.HostSessionId, _hosted.DirectStreamQuality);
        SendEnvelope(_hosted.HostSessionId, viewerSessionId, new RavaCastEnvelope(_hosted.CastId, RavaCastOp.DirectStreamStart, MessagePackSerializer.Serialize(payload)));
    }

    private void StartJoinedDirectStreamReceiver(RavaCastSummary summary)
    {
        if (_joined is null || _joined.CastId != summary.CastId) return;
        var mySession = !string.IsNullOrWhiteSpace(_joined.ViewerSessionId) ? _joined.ViewerSessionId : GetMySessionId();
        if (string.IsNullOrWhiteSpace(mySession)) return;
        _joined.ViewerSessionId = mySession;

        if (_joined.DirectStreamReceiverRequested)
            return;

        _joined.DirectStreamReceiverRequested = true;
        if (_surface.StartDirectStreamReceiver(summary.CastId, summary.HostSessionId, mySession, summary.DirectStreamQuality, out var error))
        {
            _joined.DirectStreamStatus = "Connecting to host video";
            _joined.DirectStreamDetail = string.Empty;
            SendDirectStreamViewerReady(summary.HostSessionId, summary.CastId, mySession);
            _ = SendDirectStreamViewerReadyRetryAsync(summary.HostSessionId, summary.CastId, mySession);
            return;
        }

        _joined.DirectStreamReceiverRequested = false;
        _joined.DirectStreamStatus = "Could not connect to host video";
        _joined.DirectStreamDetail = error;
        SendDirectStreamError(summary.HostSessionId, summary.CastId, error);
    }

    private void SendDirectStreamViewerReady(string hostSessionId, Guid castId, string mySession)
    {
        if (string.IsNullOrWhiteSpace(hostSessionId) || string.IsNullOrWhiteSpace(mySession)) return;
        RunOnFrameworkThreadSafe(() => SendDirectStreamViewerReadyOnFramework(hostSessionId, castId, mySession), "Direct Stream viewer ready");
    }

    private void SendDirectStreamViewerReadyOnFramework(string hostSessionId, Guid castId, string mySession)
    {
        var ready = new RavaCastDirectStreamViewerPayload(mySession, _objects.LocalPlayer?.Name.TextValue ?? "Player");
        SendEnvelope(mySession, hostSessionId, new RavaCastEnvelope(castId, RavaCastOp.DirectStreamViewerReady, MessagePackSerializer.Serialize(ready)));
    }

    private async Task SendDirectStreamViewerReadyRetryAsync(string hostSessionId, Guid castId, string mySession)
    {
        try
        {
            var delays = new[] { 250, 1000, 2500, 5000, 8000 };
            foreach (var delay in delays)
            {
                await Task.Delay(delay).ConfigureAwait(false);

                if (_joined is null || _joined.CastId != castId || !_joined.DirectStreamReceiverRequested) return;
                SendDirectStreamViewerReady(hostSessionId, castId, mySession);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to retry RavaCast Direct Stream viewer ready message");
        }
    }

    private void HandleDirectStreamStart(string fromSessionId, RavaCastEnvelope env)
    {
        if (_joined is null || _joined.CastId != env.CastId || env.Payload is null) return;
        if (!string.Equals(_joined.HostSessionId, fromSessionId, StringComparison.Ordinal)) return;
        var payload = MessagePackSerializer.Deserialize<RavaCastDirectStreamStartPayload>(env.Payload);
        var summary = new RavaCastSummary(_joined.CastId, _joined.HostSessionId, _joined.HostName, _joined.CastName, _joined.Url, _joined.SourceDomain,
            _joined.MediaTitle, true, 0, null, _joined.StateUtc, _joined.JoinedCount,
            _joined.Plane, _joined.Queue, _joined.ConsentCookies, Environment.TickCount64)
        {
            Mode = RavaCastMode.DirectStream,
            DirectStreamQuality = payload.Quality
        };
        StartJoinedDirectStreamReceiver(summary);
    }

    private void HandleDirectStreamStop(RavaCastEnvelope env)
    {
        if (_joined is null || _joined.CastId != env.CastId) return;
        var reason = "Direct Stream stopped by host.";
        if (env.Payload is not null)
        {
            try { reason = MessagePackSerializer.Deserialize<RavaCastDirectStreamStopPayload>(env.Payload).Reason; } catch { }
        }
        _surface.StopDirectStreamReceiver();
        _joined.DirectStreamReceiverRequested = false;
        _joined.DirectStreamStatus = "Direct Stream stopped";
        _joined.DirectStreamDetail = reason;
    }

    private void HandleDirectStreamViewerReady(string fromSessionId, RavaCastEnvelope env)
    {
        if (_hosted is null || _hosted.CastId != env.CastId || env.Payload is null) return;
        var payload = MessagePackSerializer.Deserialize<RavaCastDirectStreamViewerPayload>(env.Payload);
        var viewer = !string.IsNullOrWhiteSpace(payload.ViewerSessionId) ? payload.ViewerSessionId : fromSessionId;
        if (string.IsNullOrWhiteSpace(viewer)) return;

        // Treat the first media-ready message as a valid joined-viewer heartbeat too. Do not process
        // repeated ready heartbeats as new peers: moving/resizing the live screen sends state updates,
        // and older builds answered every state update with another ViewerReady. That created a feedback
        // loop of AddPeer/StateSnapshot traffic and could tank Direct Stream frame rate while placing.
        _joinedViewers[viewer] = true;
        if (!_directStreamReadyViewers.TryAdd(viewer, true))
            return;

        StartHostedDirectStreamPublisher(notifyExistingViewers: false);
        _surface.AddDirectStreamPeer(viewer);
        _hosted.DirectStreamStatus = "Viewer connected";
        _hosted.DirectStreamDetail = string.IsNullOrWhiteSpace(payload.ViewerName) ? "A viewer is connecting to your stream." : $"{payload.ViewerName} is connecting to your stream.";
        BroadcastHostedStateNearby(RavaCastOp.Advertise);
        BroadcastHostedStateToJoined();
    }

    private void HandleDirectStreamViewerLeft(string fromSessionId, RavaCastEnvelope env)
    {
        if (_hosted is null || _hosted.CastId != env.CastId) return;
        var viewer = fromSessionId;
        if (env.Payload is not null)
        {
            try
            {
                var payload = MessagePackSerializer.Deserialize<RavaCastDirectStreamViewerPayload>(env.Payload);
                if (!string.IsNullOrWhiteSpace(payload.ViewerSessionId)) viewer = payload.ViewerSessionId;
            }
            catch { }
        }
        if (!string.IsNullOrWhiteSpace(viewer))
        {
            _joinedViewers.TryRemove(viewer, out _);
            _directStreamReadyViewers.TryRemove(viewer, out _);
            _surface.RemoveDirectStreamPeer(viewer);
            StopHostedDirectStreamIfNoViewers();
        }
    }

    private void HandleDirectStreamSignal(string fromSessionId, RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        var payload = MessagePackSerializer.Deserialize<RavaCastDirectStreamSignalPayload>(env.Payload);
        ProcessDirectStreamSignalPayload(fromSessionId, env.CastId, payload);
    }

    private void HandleDirectStreamSignalChunk(string fromSessionId, RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        var chunk = MessagePackSerializer.Deserialize<RavaCastDirectStreamSignalChunkPayload>(env.Payload);
        if (string.IsNullOrWhiteSpace(chunk.SignalId) || string.IsNullOrWhiteSpace(chunk.Type)) return;
        if (chunk.ChunkCount <= 0 || chunk.ChunkCount > 256) return;
        if (chunk.ChunkIndex < 0 || chunk.ChunkIndex >= chunk.ChunkCount) return;

        var signalFrom = !string.IsNullOrWhiteSpace(chunk.FromSessionId) ? chunk.FromSessionId : fromSessionId;
        var signalTo = !string.IsNullOrWhiteSpace(chunk.ToSessionId) ? chunk.ToSessionId : GetExpectedDirectStreamLocalSessionId();
        if (!IsDirectStreamSignalForThisClient(signalTo))
            return;

        PruneDirectStreamSignalAssemblies();
        var key = BuildDirectStreamSignalAssemblyKey(env.CastId, chunk.SignalId, signalFrom, signalTo, chunk.Type);
        if (_completedDirectStreamSignalAssemblies.ContainsKey(key)) return;
        var assembly = _directStreamSignalAssemblies.GetOrAdd(key, _ => new DirectStreamSignalAssembly(env.CastId, chunk.SignalId, signalFrom, signalTo, chunk.Type, chunk.ChunkCount));

        bool complete;
        lock (assembly)
            complete = assembly.TryAdd(chunk.ChunkIndex, chunk.PayloadPart);

        if (!complete) return;
        _directStreamSignalAssemblies.TryRemove(key, out _);
        _completedDirectStreamSignalAssemblies[key] = Environment.TickCount64;
        var payload = new RavaCastDirectStreamSignalPayload(signalFrom, signalTo, chunk.Type, assembly.BuildPayloadJson());
        ProcessDirectStreamSignalPayload(fromSessionId, env.CastId, payload);
    }

    private void ProcessDirectStreamSignalPayload(string fromSessionId, Guid castId, RavaCastDirectStreamSignalPayload payload)
    {
        var signalFrom = !string.IsNullOrWhiteSpace(payload.FromSessionId) ? payload.FromSessionId : fromSessionId;
        var signalTo = payload.ToSessionId ?? string.Empty;
        if (!IsDirectStreamSignalForThisClient(signalTo))
            return;

        if (_hosted is not null && _hosted.CastId == castId)
        {
            var peerId = ResolveHostedDirectStreamPeer(signalFrom, fromSessionId);
            if (string.IsNullOrWhiteSpace(peerId)) return;
            _surface.HandleDirectStreamSignal(peerId, payload.Type, payload.PayloadJson);
            return;
        }

        if (_joined is not null && _joined.CastId == castId && IsJoinedDirectStreamHost(signalFrom, fromSessionId))
            _surface.HandleDirectStreamSignal(_joined.HostSessionId, payload.Type, payload.PayloadJson);
    }

    private bool IsDirectStreamSignalForThisClient(string signalTo)
    {
        if (string.IsNullOrWhiteSpace(signalTo)) return true;

        var mySession = GetMySessionId();
        if (!string.IsNullOrWhiteSpace(mySession) && string.Equals(signalTo, mySession, StringComparison.Ordinal)) return true;
        if (_hosted is not null && string.Equals(signalTo, _hosted.HostSessionId, StringComparison.Ordinal)) return true;
        if (_joined is not null && !string.IsNullOrWhiteSpace(_joined.ViewerSessionId) && string.Equals(signalTo, _joined.ViewerSessionId, StringComparison.Ordinal)) return true;
        return false;
    }

    private string GetExpectedDirectStreamLocalSessionId()
    {
        if (_hosted is not null && !string.IsNullOrWhiteSpace(_hosted.HostSessionId)) return _hosted.HostSessionId;
        if (_joined is not null && !string.IsNullOrWhiteSpace(_joined.ViewerSessionId)) return _joined.ViewerSessionId;
        return GetMySessionId();
    }

    private string ResolveHostedDirectStreamPeer(string signalFrom, string meshFromSessionId)
    {
        if (!string.IsNullOrWhiteSpace(signalFrom) && _joinedViewers.ContainsKey(signalFrom)) return signalFrom;
        if (!string.IsNullOrWhiteSpace(meshFromSessionId) && _joinedViewers.ContainsKey(meshFromSessionId)) return meshFromSessionId;
        return string.Empty;
    }

    private bool IsJoinedDirectStreamHost(string signalFrom, string meshFromSessionId)
    {
        if (_joined is null) return false;
        if (!string.IsNullOrWhiteSpace(signalFrom) && string.Equals(_joined.HostSessionId, signalFrom, StringComparison.Ordinal)) return true;
        if (!string.IsNullOrWhiteSpace(meshFromSessionId) && string.Equals(_joined.HostSessionId, meshFromSessionId, StringComparison.Ordinal)) return true;
        return false;
    }

    private static string BuildDirectStreamSignalAssemblyKey(Guid castId, string signalId, string fromSessionId, string toSessionId, string type)
        => $"{castId:D}|{signalId}|{fromSessionId}|{toSessionId}|{type}";

    private void PruneDirectStreamSignalAssemblies()
    {
        var now = Environment.TickCount64;
        foreach (var kv in _directStreamSignalAssemblies.ToArray())
            if (now - kv.Value.CreatedTick > DirectStreamSignalAssemblyTtlMs)
                _directStreamSignalAssemblies.TryRemove(kv.Key, out _);

        foreach (var kv in _completedDirectStreamSignalAssemblies.ToArray())
            if (now - kv.Value > DirectStreamSignalAssemblyTtlMs)
                _completedDirectStreamSignalAssemblies.TryRemove(kv.Key, out _);
    }

    private void RemoveDirectStreamSignalAssembliesForCast(Guid castId)
    {
        foreach (var kv in _directStreamSignalAssemblies.ToArray())
            if (kv.Value.CastId == castId)
                _directStreamSignalAssemblies.TryRemove(kv.Key, out _);

        var prefix = castId.ToString("D") + "|";
        foreach (var kv in _completedDirectStreamSignalAssemblies.ToArray())
            if (kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                _completedDirectStreamSignalAssemblies.TryRemove(kv.Key, out _);
    }

    private void HandleDirectStreamStats(RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        try
        {
            _ = MessagePackSerializer.Deserialize<RavaCastDirectStreamStatsPayload>(env.Payload);
            // Direct Stream stats are healthy-path telemetry; do not log them by default.
        }
        catch { }
    }

    private void HandleDirectStreamError(string fromSessionId, RavaCastEnvelope env)
    {
        if (env.Payload is null) return;
        try
        {
            var payload = MessagePackSerializer.Deserialize<RavaCastDirectStreamErrorPayload>(env.Payload);
            _logger.LogWarning("RavaCast Direct Stream error from {session}: {message}", string.IsNullOrWhiteSpace(payload.SessionId) ? fromSessionId : payload.SessionId, payload.Message);
            if (_hosted is not null && _hosted.CastId == env.CastId)
            {
                _hosted.DirectStreamStatus = "Direct Stream viewer error";
                _hosted.DirectStreamDetail = payload.Message;
            }
            if (_joined is not null && _joined.CastId == env.CastId)
            {
                _joined.DirectStreamStatus = "Direct Stream error";
                _joined.DirectStreamDetail = payload.Message;
            }
        }
        catch { }
    }

    private void OnDirectStreamSignalProduced(object? sender, RavaCastDirectStreamSignalProducedEventArgs e)
    {
        if (string.IsNullOrWhiteSpace(e.PeerId)) return;

        Guid castId;
        string localSessionId;
        if (_hosted is not null)
        {
            castId = _hosted.CastId;
            localSessionId = _hosted.HostSessionId;
        }
        else if (_joined is not null)
        {
            castId = _joined.CastId;
            localSessionId = !string.IsNullOrWhiteSpace(_joined.ViewerSessionId) ? _joined.ViewerSessionId : GetMySessionId();
            if (!string.IsNullOrWhiteSpace(localSessionId)) _joined.ViewerSessionId = localSessionId;
        }
        else
        {
            return;
        }

        if (castId == Guid.Empty || string.IsNullOrWhiteSpace(localSessionId)) return;
        SendDirectStreamSignal(localSessionId, e.PeerId, castId, e.SignalType, e.PayloadJson);
    }

    private void SendDirectStreamSignal(string fromSessionId, string targetSessionId, Guid castId, string signalType, string payloadJson)
        => _ = SendDirectStreamSignalAsync(fromSessionId, targetSessionId, castId, signalType, payloadJson);

    private async Task SendDirectStreamSignalAsync(string fromSessionId, string targetSessionId, Guid castId, string signalType, string payloadJson)
    {
        var gateKey = $"{castId:D}|{fromSessionId}|{targetSessionId}";
        var gate = _directStreamSignalSendGates.GetOrAdd(gateKey, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (ShouldChunkDirectStreamSignal(signalType, payloadJson))
            {
                await SendDirectStreamSignalChunksAsync(fromSessionId, targetSessionId, castId, signalType, payloadJson ?? string.Empty).ConfigureAwait(false);
                return;
            }

            var op = DirectStreamSignalOp(signalType);
            var payload = new RavaCastDirectStreamSignalPayload(fromSessionId, targetSessionId, signalType, payloadJson ?? string.Empty);
            await SendEnvelopeAsync(fromSessionId, targetSessionId, new RavaCastEnvelope(castId, op, MessagePackSerializer.Serialize(payload))).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send RavaCast Direct Stream signal {type} to {targetSessionId}", signalType, targetSessionId);
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task SendDirectStreamSignalChunksAsync(string fromSessionId, string targetSessionId, Guid castId, string signalType, string payloadJson)
    {
        var signalId = Guid.NewGuid().ToString("N");
        var safePayload = payloadJson ?? string.Empty;
        var count = Math.Max(1, (safePayload.Length + DirectStreamSignalChunkChars - 1) / DirectStreamSignalChunkChars);
        var repeatCount = IsDirectStreamDescriptionSignal(signalType) ? 2 : 1;

        for (var pass = 0; pass < repeatCount; pass++)
        {
            if (pass > 0)
            {
                try { await Task.Delay(180).ConfigureAwait(false); }
                catch { return; }
            }

            for (var i = 0; i < count; i++)
            {
                var offset = i * DirectStreamSignalChunkChars;
                var length = Math.Min(DirectStreamSignalChunkChars, Math.Max(0, safePayload.Length - offset));
                var part = length > 0 ? safePayload.Substring(offset, length) : string.Empty;
                var chunk = new RavaCastDirectStreamSignalChunkPayload(signalId, fromSessionId, targetSessionId, signalType, i, count, part);
                await SendEnvelopeAsync(fromSessionId, targetSessionId, new RavaCastEnvelope(castId, RavaCastOp.DirectStreamSignalChunk, MessagePackSerializer.Serialize(chunk))).ConfigureAwait(false);
            }
        }
    }

    private static bool ShouldChunkDirectStreamSignal(string signalType, string payloadJson)
        => IsDirectStreamDescriptionSignal(signalType) || (payloadJson?.Length ?? 0) > DirectStreamSignalChunkChars;

    private static bool IsDirectStreamDescriptionSignal(string signalType)
    {
        var type = (signalType ?? string.Empty).Trim().ToLowerInvariant();
        return type is "offer" or "answer";
    }

    private void SendDirectStreamError(string targetSessionId, Guid castId, string message)
    {
        var mySession = GetMySessionId();
        if (string.IsNullOrWhiteSpace(mySession) || string.IsNullOrWhiteSpace(targetSessionId)) return;
        var payload = new RavaCastDirectStreamErrorPayload(mySession, message);
        SendEnvelope(mySession, targetSessionId, new RavaCastEnvelope(castId, RavaCastOp.DirectStreamError, MessagePackSerializer.Serialize(payload)));
    }

    private static RavaCastOp DirectStreamSignalOp(string signalType)
        => (signalType ?? string.Empty).Trim().ToLowerInvariant() switch
        {
            "offer" => RavaCastOp.DirectStreamOffer,
            "answer" => RavaCastOp.DirectStreamAnswer,
            "ice" or "candidate" or "icecandidate" => RavaCastOp.DirectStreamIce,
            _ => RavaCastOp.DirectStreamIce
        };

    private void SendEnvelopeNearby(string fromSessionId, RavaCastEnvelope env)
        => RunOnFrameworkThreadSafe(() => SendEnvelopeNearbyOnFramework(fromSessionId, env), "nearby envelope send");

    private void SendEnvelopeNearbyOnFramework(string fromSessionId, RavaCastEnvelope env)
    {
        var local = _objects.LocalPlayer;
        if (local is null) return;

        foreach (var pc in _objects.OfType<IPlayerCharacter>().Where(p => p.Address != IntPtr.Zero && p.Address != local.Address).ToArray())
        {
            try
            {
                var ident = _dalamudUtil.GetIdentFromGameObject(pc);
                if (string.IsNullOrWhiteSpace(ident)) continue;
                var sessionId = RavaSessionId.FromIdent(ident);
                SendEnvelope(fromSessionId, sessionId, env);
            }
            catch
            {
                // object table can shift between enumeration and ident lookup; harmless
            }
        }
    }

    private void RunOnFrameworkThreadSafe(Action action, string operation)
    {
        if (_framework.IsInFrameworkUpdateThread)
        {
            try { action(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to run RavaCast {operation}", operation); }
            return;
        }

        _ = RunOnFrameworkThreadSafeAsync(action, operation);
    }

    private async Task RunOnFrameworkThreadSafeAsync(Action action, string operation)
    {
        try
        {
            await _dalamudUtil.RunOnFrameworkThread(action).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to run RavaCast {operation} on framework thread", operation);
        }
    }

    private void SendEnvelope(string fromSessionId, string targetSessionId, RavaCastEnvelope env)
        => _ = SendEnvelopeSafeAsync(fromSessionId, targetSessionId, env);

    private async Task SendEnvelopeSafeAsync(string fromSessionId, string targetSessionId, RavaCastEnvelope env)
    {
        try
        {
            await SendEnvelopeAsync(fromSessionId, targetSessionId, env).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send RavaCast mesh envelope");
        }
    }

    private Task SendEnvelopeAsync(string fromSessionId, string targetSessionId, RavaCastEnvelope env)
    {
        if (string.IsNullOrWhiteSpace(targetSessionId)) return Task.CompletedTask;
        var payload = BuildWirePayload(env);
        return _mesh.SendAsync(targetSessionId, new RavaGame(fromSessionId, payload));
    }

    private static byte[] BuildWirePayload(RavaCastEnvelope env)
    {
        var inner = MessagePackSerializer.Serialize(env);
        var buf = new byte[Magic.Length + inner.Length];
        Buffer.BlockCopy(Magic, 0, buf, 0, Magic.Length);
        Buffer.BlockCopy(inner, 0, buf, Magic.Length, inner.Length);
        return buf;
    }

    private static bool TryReadEnvelope(byte[] payload, out RavaCastEnvelope? env)
    {
        env = null;
        if (payload == null || payload.Length <= Magic.Length) return false;
        for (int i = 0; i < Magic.Length; i++)
            if (payload[i] != Magic[i]) return false;
        env = MessagePackSerializer.Deserialize<RavaCastEnvelope>(payload.AsSpan(Magic.Length).ToArray());
        return env is not null;
    }

    private string GetMySessionId()
    {
        try
        {
            if (!_clientState.IsLoggedIn || _objects.LocalPlayer is null) return string.Empty;
            var ident = _dalamudUtil.GetIdentFromGameObject(_objects.LocalPlayer);
            return string.IsNullOrWhiteSpace(ident) ? string.Empty : RavaSessionId.FromIdent(ident);
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string HashPassword(string password, string salt)
    {
        if (string.IsNullOrEmpty(password)) return string.Empty;
        salt ??= string.Empty;
        var data = System.Text.Encoding.UTF8.GetBytes($"{salt}:{password}");
        var hash = System.Security.Cryptography.SHA256.HashData(data);
        return string.Concat(hash.Select(b => b.ToString("x2")));
    }

    public static bool TryValidatePublicWebUrl(string url, out Uri uri, out string error)
    {
        uri = null!;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(url))
        {
            error = "Enter a public web URL first.";
            return false;
        }

        if (!Uri.TryCreate(url.Trim(), UriKind.Absolute, out var parsed))
        {
            error = "That does not look like a valid absolute URL.";
            return false;
        }

        if (!string.Equals(parsed.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(parsed.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            error = "RavaCast only supports public http/https web URLs. Local files are not supported.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(parsed.Host) || parsed.IsLoopback || string.Equals(parsed.Host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            error = "RavaCast only supports publicly available web sources. localhost/private sources are not supported.";
            return false;
        }

        if (IPAddress.TryParse(parsed.Host, out var ip) && IsPrivateAddress(ip))
        {
            error = "RavaCast only supports publicly available web sources. Private-network sources are not supported.";
            return false;
        }

        uri = parsed;
        return true;
    }

    private static bool IsPrivateAddress(IPAddress ip)
    {
        if (IPAddress.IsLoopback(ip)) return true;
        var bytes = ip.GetAddressBytes();
        if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
        {
            return bytes[0] == 10
                || (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31)
                || (bytes[0] == 192 && bytes[1] == 168)
                || (bytes[0] == 169 && bytes[1] == 254)
                || bytes[0] == 0;
        }

        if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            return ip.IsIPv6LinkLocal || ip.IsIPv6SiteLocal || ip.IsIPv6Multicast
                || bytes[0] == 0xfc || bytes[0] == 0xfd;
        }

        return false;
    }
}
