using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using System.Collections.Concurrent;

namespace RavaSync.Services;

public sealed class MissingFileMeshService : DisposableMediatorSubscriberBase
{
    private static readonly byte[] RepairMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'B', (byte)'2', (byte)'F', (byte)'I', (byte)'X', 0 };
    private static readonly TimeSpan OutboundDedupWindow = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan InboundDedupWindow = TimeSpan.FromMinutes(2);

    [MessagePackObject]
    public sealed class MissingCentralFileRepairPayload
    {
        [Key(0)] public string RequesterUid { get; set; } = string.Empty;
        [Key(1)] public string TargetUid { get; set; } = string.Empty;
        [Key(2)] public string DataHash { get; set; } = string.Empty;
        [Key(3)] public List<string> Hashes { get; set; } = [];
        [Key(4)] public string Reason { get; set; } = string.Empty;
    }

    private readonly IRavaMesh _mesh;
    private readonly FileUploadManager _fileUploadManager;
    private readonly ApiController _apiController;
    private readonly ConcurrentDictionary<string, DateTime> _lastOutboundByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime> _lastInboundByKey = new(StringComparer.OrdinalIgnoreCase);
    private string _ownUid = string.Empty;

    public MissingFileMeshService(ILogger<MissingFileMeshService> logger, MareMediator mediator, IRavaMesh mesh, FileUploadManager fileUploadManager, ApiController apiController) : base(logger, mediator)
    {
        _mesh = mesh;
        _fileUploadManager = fileUploadManager;
        _apiController = apiController;

        Mediator.Subscribe<ConnectedMessage>(this, msg =>
        {
            _ownUid = msg.Connection.User?.UID ?? string.Empty;
            _lastOutboundByKey.Clear();
            _lastInboundByKey.Clear();
        });

        Mediator.Subscribe<DisconnectedMessage>(this, _ =>
        {
            _ownUid = string.Empty;
            _lastOutboundByKey.Clear();
            _lastInboundByKey.Clear();
        });

        Mediator.Subscribe<RemoteMissingFileMessage>(this, OnRemoteMissingCentralFileRepair);
        Mediator.Subscribe<SyncshellGameMeshMessage>(this, OnGameMeshMessage);
    }

    private void OnRemoteMissingCentralFileRepair(RemoteMissingFileMessage msg)
    {
        if (!_apiController.IsConnected)
            return;

        if (string.IsNullOrWhiteSpace(msg.TargetUid) || string.IsNullOrWhiteSpace(msg.TargetIdent) || msg.Hashes == null || msg.Hashes.Count == 0)
            return;

        var sessionId = RavaSessionId.FromIdent(msg.TargetIdent);
        if (string.IsNullOrWhiteSpace(sessionId))
            return;

        var hashes = msg.Hashes.Where(h => !string.IsNullOrWhiteSpace(h)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        if (hashes.Count == 0)
            return;

        var dedupKey = string.Join('|', msg.TargetUid, msg.DataHash ?? string.Empty, string.Join(',', hashes));
        var now = DateTime.UtcNow;

        if (_lastOutboundByKey.TryGetValue(dedupKey, out var last) && now - last < OutboundDedupWindow)
            return;

        _lastOutboundByKey[dedupKey] = now;

        var payload = BuildRepairPayload(_ownUid, msg.TargetUid, msg.DataHash, hashes, msg.Reason);

        Logger.LogWarning("Requesting central B2 repair from {uid}/{session}: {count} hash(es), reason={reason}, hashes={hashes}",
            msg.TargetUid,
            sessionId,
            hashes.Count,
            msg.Reason,
            string.Join(", ", hashes.Take(20)));

        _ = _mesh.SendAsync(sessionId, new RavaGame(string.Empty, payload));
    }

    private void OnGameMeshMessage(SyncshellGameMeshMessage msg)
    {
        if (!TryParseRepairPayload(msg.Payload, out var payload) || payload == null)
            return;

        if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        var hashes = payload.Hashes?
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(h => h, StringComparer.OrdinalIgnoreCase)
            .ToList() ?? [];

        if (hashes.Count == 0)
            return;

        var dedupKey = string.Join('|', payload.RequesterUid ?? string.Empty, payload.DataHash ?? string.Empty, string.Join(',', hashes));
        var now = DateTime.UtcNow;

        if (_lastInboundByKey.TryGetValue(dedupKey, out var last) && now - last < InboundDedupWindow)
            return;

        _lastInboundByKey[dedupKey] = now;

        Logger.LogWarning("Received central B2 repair request from {from} for {count} hash(es); force-uploading: {hashes}",
            string.IsNullOrWhiteSpace(payload.RequesterUid) ? msg.FromSessionId : payload.RequesterUid,
            hashes.Count,
            string.Join(", ", hashes.Take(20)));

        _ = Task.Run(async () =>
        {
            try
            {
                var failed = await _fileUploadManager.ForceUploadMissingHashesAsync(hashes, $"mesh central B2 repair requested by {payload.RequesterUid}", CancellationToken.None).ConfigureAwait(false);

                if (failed.Count > 0)
                {
                    Logger.LogWarning("Central B2 repair upload failed for {count}/{total} hash(es): {hashes}",
                        failed.Count,
                        hashes.Count,
                        string.Join(", ", failed.Take(20)));
                }
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Central B2 repair upload failed");
            }
        });
    }

    private static byte[] BuildRepairPayload(string requesterUid, string targetUid, string dataHash, IReadOnlyCollection<string> hashes, string reason)
    {
        var packed = MessagePackSerializer.Serialize(new MissingCentralFileRepairPayload
        {
            RequesterUid = requesterUid ?? string.Empty,
            TargetUid = targetUid ?? string.Empty,
            DataHash = dataHash ?? string.Empty,
            Hashes = hashes.Where(h => !string.IsNullOrWhiteSpace(h)).Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
            Reason = reason ?? string.Empty,
        });

        var buffer = new byte[RepairMagic.Length + packed.Length];
        Buffer.BlockCopy(RepairMagic, 0, buffer, 0, RepairMagic.Length);
        Buffer.BlockCopy(packed, 0, buffer, RepairMagic.Length, packed.Length);
        return buffer;
    }

    private static bool TryParseRepairPayload(byte[] payload, out MissingCentralFileRepairPayload? parsed)
    {
        parsed = null;

        if (payload == null || payload.Length <= RepairMagic.Length)
            return false;

        for (var i = 0; i < RepairMagic.Length; i++)
        {
            if (payload[i] != RepairMagic[i])
                return false;
        }

        try
        {
            parsed = MessagePackSerializer.Deserialize<MissingCentralFileRepairPayload>(payload.AsMemory(RepairMagic.Length).ToArray());
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }
}