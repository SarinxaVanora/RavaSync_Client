using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.PlayerData.Pairs;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RavaSync.Services;

public sealed class ThresholdPerfMeshRelayService : DisposableMediatorSubscriberBase
{
    private static readonly byte[] PerfMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'P', (byte)'E', (byte)'R', (byte)'F', 0 };
    private const long BroadcastIntervalMs = 2000;

    [MessagePackObject]
    public sealed class PerfPayload
    {
        [Key(0)] public string FromUid { get; set; } = string.Empty;
        [Key(1)] public long VramBytes { get; set; }
        [Key(2)] public long Triangles { get; set; }
        [Key(3)] public string DataHash { get; set; } = string.Empty;
    }

    private readonly IRavaMesh _mesh;
    private readonly RavaDiscoveryService _discoveryService;
    private readonly CharacterAnalyzer _characterAnalyzer;
    private readonly PairManager _pairManager;
    private readonly PlayerPerformanceService _playerPerformanceService;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly ApiController _apiController;

    private string _ownUid = string.Empty;
    private string _latestLocalDataHash = string.Empty;
    private string _lastBroadcastDataHash = string.Empty;
    private long _lastBroadcastVram = -1;
    private long _lastBroadcastTris = -1;
    private long _nextBroadcastTick;

    public ThresholdPerfMeshRelayService(
        ILogger<ThresholdPerfMeshRelayService> logger,
        MareMediator mediator,
        IRavaMesh mesh,
        RavaDiscoveryService discoveryService,
        CharacterAnalyzer characterAnalyzer,
        PairManager pairManager,
        PlayerPerformanceService playerPerformanceService,
        DalamudUtilService dalamudUtil,
        ApiController apiController) : base(logger, mediator)
    {
        _mesh = mesh;
        _discoveryService = discoveryService;
        _characterAnalyzer = characterAnalyzer;
        _pairManager = pairManager;
        _playerPerformanceService = playerPerformanceService;
        _dalamudUtil = dalamudUtil;
        _apiController = apiController;

        Mediator.Subscribe<ConnectedMessage>(this, msg =>
        {
            _ownUid = msg.Connection.User?.UID ?? string.Empty;
            _nextBroadcastTick = 0;
        });

        Mediator.Subscribe<DisconnectedMessage>(this, _ =>
        {
            _ownUid = string.Empty;
            _latestLocalDataHash = string.Empty;
            _lastBroadcastDataHash = string.Empty;
            _lastBroadcastVram = -1;
            _lastBroadcastTris = -1;
            _nextBroadcastTick = 0;
        });

        Mediator.Subscribe<CharacterDataCreatedMessage>(this, msg =>
        {
            _latestLocalDataHash = msg.CharacterData?.DataHash.Value ?? string.Empty;
            _nextBroadcastTick = 0;
        });

        Mediator.Subscribe<DelayedFrameworkUpdateMessage>(this, _ => BroadcastIfNeeded(Environment.TickCount64));
        Mediator.Subscribe<SyncshellGameMeshMessage>(this, OnGameMeshMessage);
    }

    private void BroadcastIfNeeded(long nowTick)
    {
        if (nowTick < _nextBroadcastTick)
            return;

        _nextBroadcastTick = nowTick + BroadcastIntervalMs;

        if (!_apiController.IsConnected || !_dalamudUtil.GetIsPlayerPresent())
            return;

        if (string.IsNullOrWhiteSpace(_ownUid))
            return;

        var dataHash = _latestLocalDataHash;
        if (string.IsNullOrWhiteSpace(dataHash))
            return;

        if (!_characterAnalyzer.TryGetDisplayedHeaderMetrics(dataHash, out var vramBytes, out var triangles))
            return;

        var knownPeerSessionIds = _discoveryService.GetKnownPeerSessionIds();
        if (knownPeerSessionIds.Length == 0)
            return;

        var visiblePeerSessionIds = CollectVisiblePeerSessionIds();
        var targetSessionIds = knownPeerSessionIds
            .Where(sessionId => !visiblePeerSessionIds.Contains(sessionId))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (targetSessionIds.Length == 0)
            return;

        var payload = BuildPerfPayload(_ownUid, dataHash, vramBytes, triangles);

        foreach (var sessionId in targetSessionIds)
        {
            _ = _mesh.SendAsync(sessionId, new RavaGame(string.Empty, payload));
        }

        _lastBroadcastDataHash = dataHash;
        _lastBroadcastVram = vramBytes;
        _lastBroadcastTris = triangles;

        if (Logger.IsEnabled(LogLevel.Trace))
            Logger.LogTrace("Broadcasted mesh perf update to {count} non-visible nearby peers (hash={hash}, vram={vram}, tris={tris})",
                targetSessionIds.Length,
                dataHash,
                vramBytes,
                triangles);
    }

    private void OnGameMeshMessage(SyncshellGameMeshMessage msg)
    {
        if (!TryParsePerfPayload(msg.Payload, out var payload) || payload == null)
            return;

        if (string.IsNullOrWhiteSpace(payload.FromUid))
            return;

        if (string.Equals(payload.FromUid, _ownUid, StringComparison.Ordinal))
            return;

        var pair = _pairManager.GetPairByUID(payload.FromUid);
        if (pair == null)
            return;

        _playerPerformanceService.HandleIncomingPerformanceMetrics(
            pair,
            payload.DataHash,
            payload.VramBytes,
            payload.Triangles);
    }

    private HashSet<string> CollectVisiblePeerSessionIds()
    {
        var result = new HashSet<string>(StringComparer.Ordinal);

        _pairManager.ForEachVisiblePair(pair =>
        {
            try
            {
                var ident = pair.Ident;
                if (string.IsNullOrWhiteSpace(ident) && pair.PlayerCharacter != nint.Zero)
                    ident = _dalamudUtil.GetIdentFromAddress(pair.PlayerCharacter) ?? string.Empty;

                if (string.IsNullOrWhiteSpace(ident))
                    return;

                var sessionId = RavaSessionId.FromIdent(ident);
                if (!string.IsNullOrWhiteSpace(sessionId))
                    result.Add(sessionId);
            }
            catch
            {
                // best effort
            }
        });

        return result;
    }

    private static byte[] BuildPerfPayload(string fromUid, string dataHash, long vramBytes, long triangles)
    {
        var packed = MessagePackSerializer.Serialize(new PerfPayload
        {
            FromUid = fromUid ?? string.Empty,
            DataHash = dataHash ?? string.Empty,
            VramBytes = Math.Max(0, vramBytes),
            Triangles = Math.Max(0, triangles)
        });

        var buffer = new byte[PerfMagic.Length + packed.Length];
        Buffer.BlockCopy(PerfMagic, 0, buffer, 0, PerfMagic.Length);
        Buffer.BlockCopy(packed, 0, buffer, PerfMagic.Length, packed.Length);
        return buffer;
    }

    private static bool TryParsePerfPayload(byte[] payload, out PerfPayload? parsed)
    {
        parsed = null;

        if (payload == null || payload.Length <= PerfMagic.Length)
            return false;

        for (var i = 0; i < PerfMagic.Length; i++)
        {
            if (payload[i] != PerfMagic[i])
                return false;
        }

        try
        {
            parsed = MessagePackSerializer.Deserialize<PerfPayload>(payload.AsSpan(PerfMagic.Length).ToArray());
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }
}
