using MessagePack;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Dto.User;
using RavaSync.PlayerData.Pairs;
using RavaSync.FileCache;
using RavaSync.Services.Discovery;
using RavaSync.Services.Mesh;
using RavaSync.Services.Mediator;
using RavaSync.WebAPI;
using RavaSync.WebAPI.Files;
using System.Collections.Concurrent;
using System.Text.Json;

namespace RavaSync.Services;

public sealed class MissingFileMeshService : DisposableMediatorSubscriberBase
{
    private static readonly byte[] RepairMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'B', (byte)'2', (byte)'F', (byte)'I', (byte)'X', 0 };
    private static readonly byte[] PreflightMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'F', (byte)'I', (byte)'L', (byte)'E', (byte)'P', (byte)'R', (byte)'E', 0 };
    private static readonly byte[] LegacyCharacterDataMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'C', (byte)'H', (byte)'A', (byte)'R', (byte)'A', (byte)'D', (byte)'A', (byte)'T', (byte)'A', 0 };
    private static readonly byte[] CharacterPushMagic = new byte[] { (byte)'R', (byte)'A', (byte)'V', (byte)'A', (byte)'C', (byte)'H', (byte)'A', (byte)'R', (byte)'A', (byte)'P', (byte)'U', (byte)'S', (byte)'H', (byte)'2', 0 };
    private static readonly TimeSpan PreflightTimeout = TimeSpan.FromMilliseconds(650);
    private static readonly TimeSpan CharacterOfferTimeout = TimeSpan.FromMilliseconds(1200);
    private static readonly TimeSpan CharacterReadyAckTimeout = TimeSpan.FromMilliseconds(650);
    private const int CharacterReadyAckAttempts = 3;
    private static readonly TimeSpan PendingInboundCharacterPushNoMissingLifetime = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan PendingInboundCharacterPushUploadBarrierLifetime = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan RecentMeshReadyLifetime = TimeSpan.FromMinutes(3);
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

    [MessagePackObject]
    public sealed class FilePresencePreflightPayload
    {
        [Key(0)] public string RequestId { get; set; } = string.Empty;
        [Key(1)] public string RequesterUid { get; set; } = string.Empty;
        [Key(2)] public string TargetUid { get; set; } = string.Empty;
        [Key(3)] public string DataHash { get; set; } = string.Empty;
        [Key(4)] public List<string> Hashes { get; set; } = [];
        [Key(5)] public List<string> MissingHashes { get; set; } = [];
        [Key(6)] public bool IsResponse { get; set; }
    }

    [MessagePackObject]
    public sealed class CharacterDataMeshPayload
    {
        [Key(0)] public string RequestId { get; set; } = string.Empty;
        [Key(1)] public string SenderUid { get; set; } = string.Empty;
        [Key(2)] public string TargetUid { get; set; } = string.Empty;
        [Key(3)] public string DataHash { get; set; } = string.Empty;
        [Key(4)] public byte[] CharacterDataJsonUtf8 { get; set; } = [];
        [Key(5)] public bool IsAck { get; set; }
        [Key(6)] public List<string> RequiredHashes { get; set; } = [];
        [Key(7)] public List<string> MissingHashes { get; set; } = [];
        [Key(8)] public bool IsResponse { get; set; }
        [Key(9)] public bool IsReady { get; set; }
        [Key(10)] public int ProtocolVersion { get; set; }
        [Key(11)] public Dictionary<string, long> HashSizeHints { get; set; } = [];
    }

    public sealed class MeshCharacterPushOfferResult
    {
        public string RequestId { get; init; } = string.Empty;
        public HashSet<string> AcceptedUids { get; } = new(StringComparer.Ordinal);
        public HashSet<string> MissingHashes { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, List<string>> MissingHashesByUid { get; } = new(StringComparer.Ordinal);
    }

    private sealed record PendingInboundCharacterPush(string FromSessionId, string SenderUid, string RequestId, string DataHash, byte[] CharacterDataJsonUtf8, int MissingHashCount, DateTime CreatedUtc);

    private readonly IRavaMesh _mesh;
    private readonly FileUploadManager _fileUploadManager;
    private readonly FileCacheManager _fileCacheManager;
    private readonly ApiController _apiController;
    private readonly PairManager _pairManager;
    private readonly ConcurrentDictionary<string, DateTime> _lastOutboundByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime> _lastInboundByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<FilePresencePreflightPayload>> _pendingPreflightByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<CharacterDataMeshPayload>> _pendingCharacterOfferByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _pendingCharacterReadyAckByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, PendingInboundCharacterPush> _pendingInboundCharacterPushByKey = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, long> RecentMeshReadyByUidAndHash = new(StringComparer.OrdinalIgnoreCase);
    private string _ownUid = string.Empty;

    public static bool HasRecentMeshReadyForCharacterData(string? senderUid, string? dataHash)
    {
        if (string.IsNullOrWhiteSpace(senderUid) || string.IsNullOrWhiteSpace(dataHash))
            return false;

        var key = BuildRecentMeshReadyKey(senderUid, dataHash);
        var nowTicks = DateTime.UtcNow.Ticks;

        if (!RecentMeshReadyByUidAndHash.TryGetValue(key, out var expiresTicks))
            return false;

        if (expiresTicks <= nowTicks)
        {
            RecentMeshReadyByUidAndHash.TryRemove(key, out _);
            return false;
        }

        return true;
    }

    private static void MarkRecentMeshReadyForCharacterData(string? senderUid, string? dataHash)
    {
        if (string.IsNullOrWhiteSpace(senderUid) || string.IsNullOrWhiteSpace(dataHash))
            return;

        RecentMeshReadyByUidAndHash[BuildRecentMeshReadyKey(senderUid, dataHash)] = DateTime.UtcNow.Add(RecentMeshReadyLifetime).Ticks;
    }


    public MissingFileMeshService(ILogger<MissingFileMeshService> logger, MareMediator mediator, IRavaMesh mesh, FileUploadManager fileUploadManager, FileCacheManager fileCacheManager, ApiController apiController, PairManager pairManager) : base(logger, mediator)
    {
        _mesh = mesh;
        _fileUploadManager = fileUploadManager;
        _fileCacheManager = fileCacheManager;
        _apiController = apiController;
        _pairManager = pairManager;

        Mediator.Subscribe<ConnectedMessage>(this, msg =>
        {
            _ownUid = msg.Connection.User?.UID ?? string.Empty;
            _lastOutboundByKey.Clear();
            _lastInboundByKey.Clear();
            ClearPendingPreflights();
            ClearPendingCharacterPushes();
        });

        Mediator.Subscribe<DisconnectedMessage>(this, _ =>
        {
            _ownUid = string.Empty;
            _lastOutboundByKey.Clear();
            _lastInboundByKey.Clear();
            ClearPendingPreflights();
            ClearPendingCharacterPushes();
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

        var hashes = NormalizeHashes(msg.Hashes).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        if (hashes.Count == 0)
            return;

        var dedupKey = string.Join('|', msg.TargetUid, msg.DataHash ?? string.Empty, string.Join(',', hashes));
        var now = DateTime.UtcNow;

        if (_lastOutboundByKey.TryGetValue(dedupKey, out var last) && now - last < OutboundDedupWindow)
            return;

        _lastOutboundByKey[dedupKey] = now;

        var payload = BuildRepairPayload(_ownUid, msg.TargetUid, msg.DataHash, hashes, msg.Reason);

        Logger.LogDebug("Requesting central B2 repair from {uid}/{session}: {count} hash(es), reason={reason}, hashes={hashes}",
            msg.TargetUid,
            sessionId,
            hashes.Count,
            msg.Reason,
            string.Join(", ", hashes.Take(20)));

        _ = _mesh.SendAsync(sessionId, new RavaGame(string.Empty, payload));
    }

    private void OnGameMeshMessage(SyncshellGameMeshMessage msg)
    {
        CleanupExpiredInboundCharacterPushes();

        if (TryParseCharacterDataPayload(msg.Payload, out var characterPayload) && characterPayload != null)
        {
            HandleCharacterDataMeshMessage(msg, characterPayload);
            return;
        }

        if (TryParsePreflightPayload(msg.Payload, out var preflightPayload) && preflightPayload != null)
        {
            HandlePreflightMeshMessage(msg, preflightPayload);
            return;
        }

        if (!TryParseRepairPayload(msg.Payload, out var payload) || payload == null)
            return;

        if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        var hashes = NormalizeHashes(payload.Hashes).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        if (hashes.Count == 0)
            return;

        var dedupKey = string.Join('|', payload.RequesterUid ?? string.Empty, payload.DataHash ?? string.Empty, string.Join(',', hashes));
        var now = DateTime.UtcNow;

        if (_lastInboundByKey.TryGetValue(dedupKey, out var last) && now - last < InboundDedupWindow)
            return;

        _lastInboundByKey[dedupKey] = now;

        Logger.LogDebug("Received central B2 repair request from {from} for {count} hash(es); force-uploading: {hashes}",
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

    public async Task<HashSet<string>?> RequestMissingHashesForPushAsync(IReadOnlyDictionary<string, string> targetSessionByUid, string dataHash, IReadOnlyCollection<string> hashes, CancellationToken ct)
    {
        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || targetSessionByUid == null || targetSessionByUid.Count == 0)
            return null;

        var hashList = NormalizeHashes(hashes).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        if (hashList.Count == 0)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var requestId = Guid.NewGuid().ToString("N");
        var pending = new List<(string Key, TaskCompletionSource<FilePresencePreflightPayload> Completion)>();

        try
        {
            foreach (var kvp in targetSessionByUid)
            {
                ct.ThrowIfCancellationRequested();

                var targetUid = kvp.Key;
                var targetSessionId = kvp.Value;
                if (string.IsNullOrWhiteSpace(targetUid) || string.IsNullOrWhiteSpace(targetSessionId))
                    continue;

                var key = BuildPendingPreflightKey(requestId, targetUid);
                var completion = new TaskCompletionSource<FilePresencePreflightPayload>(TaskCreationOptions.RunContinuationsAsynchronously);

                if (!_pendingPreflightByKey.TryAdd(key, completion))
                    continue;

                pending.Add((key, completion));

                var payload = BuildPreflightPayload(new FilePresencePreflightPayload
                {
                    RequestId = requestId,
                    RequesterUid = _ownUid,
                    TargetUid = targetUid,
                    DataHash = dataHash ?? string.Empty,
                    Hashes = hashList,
                    MissingHashes = [],
                    IsResponse = false
                });

                _ = _mesh.SendAsync(targetSessionId, new RavaGame(string.Empty, payload));
            }

            if (pending.Count == 0)
                return null;

            var allResponses = Task.WhenAll(pending.Select(p => p.Completion.Task));
            var timeoutTask = Task.Delay(PreflightTimeout, ct);
            var completed = await Task.WhenAny(allResponses, timeoutTask).ConfigureAwait(false);

            if (completed != allResponses)
                return null;

            FilePresencePreflightPayload[] responses;
            try
            {
                responses = await allResponses.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null;
            }

            var missing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var response in responses)
            {
                foreach (var hash in NormalizeHashes(response.MissingHashes))
                    missing.Add(hash);
            }

            return missing;
        }
        finally
        {
            foreach (var item in pending)
                _pendingPreflightByKey.TryRemove(item.Key, out _);
        }
    }

    private Dictionary<string, long> BuildLocalHashSizeHints(IEnumerable<string> hashes)
    {
        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        foreach (var hash in NormalizeHashes(hashes))
        {
            try
            {
                var cache = _fileCacheManager.GetFileCacheByHash(hash);
                if (cache == null)
                    continue;

                long size = 0;
                if (cache.Size.HasValue && cache.Size.Value > 0)
                    size = cache.Size.Value;
                else if (!string.IsNullOrWhiteSpace(cache.ResolvedFilepath))
                {
                    var fi = new FileInfo(cache.ResolvedFilepath);
                    if (fi.Exists && fi.Length > 0)
                        size = fi.Length;
                }

                if (size > 0)
                    result[hash] = size;
            }
            catch
            {
                // Size hints are UI/progress hints only. Missing hints must never block the push.
            }
        }

        return result;
    }

    private static HashSet<string> CollectCharacterDataHashes(CharacterData data)
    {
        if (data?.FileReplacements == null || data.FileReplacements.Count == 0)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        return data.FileReplacements
            .SelectMany(kvp => kvp.Value ?? [])
            .Where(fr => fr != null && string.IsNullOrWhiteSpace(fr.FileSwapPath) && !string.IsNullOrWhiteSpace(fr.Hash))
            .Select(fr => fr.Hash.Trim().ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<MeshCharacterPushOfferResult?> OfferCharacterDataForUsersAsync(IReadOnlyDictionary<string, string> targetSessionByUid, CharacterData characterData, IReadOnlyCollection<UserData> targetUsers, IReadOnlyCollection<string> requiredHashes, CancellationToken ct, IReadOnlyDictionary<string, CharacterData>? characterDataByUid = null)
    {
        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || targetSessionByUid == null || targetSessionByUid.Count == 0 || characterData == null)
            return null;

        var targetUids = (targetUsers ?? Array.Empty<UserData>())
            .Select(u => u.UID)
            .Where(uid => !string.IsNullOrWhiteSpace(uid))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (targetUids.Count == 0)
            return null;

        var requestId = Guid.NewGuid().ToString("N");
        var dataHash = characterData.DataHash.Value ?? string.Empty;
        var hashList = NormalizeHashes(requiredHashes).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        var hashSizeHints = BuildLocalHashSizeHints(hashList);
        var pending = new List<(string Key, string TargetUid, TaskCompletionSource<CharacterDataMeshPayload> Completion)>();

        try
        {
            foreach (var targetUid in targetUids)
            {
                ct.ThrowIfCancellationRequested();

                if (!targetSessionByUid.TryGetValue(targetUid, out var targetSessionId) || string.IsNullOrWhiteSpace(targetSessionId))
                    continue;

                var key = BuildPendingCharacterPushKey(requestId, targetUid);
                var completion = new TaskCompletionSource<CharacterDataMeshPayload>(TaskCreationOptions.RunContinuationsAsynchronously);

                if (!_pendingCharacterOfferByKey.TryAdd(key, completion))
                    continue;

                pending.Add((key, targetUid, completion));

                var dataForTarget = GetCharacterDataForTarget(characterDataByUid, targetUid, characterData);
                var payload = BuildCharacterDataPayload(new CharacterDataMeshPayload
                {
                    ProtocolVersion = 2,
                    RequestId = requestId,
                    SenderUid = _ownUid,
                    TargetUid = targetUid,
                    DataHash = dataForTarget.DataHash.Value ?? dataHash,
                    CharacterDataJsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(dataForTarget),
                    RequiredHashes = hashList,
                    MissingHashes = [],
                    HashSizeHints = hashSizeHints,
                    IsResponse = false,
                    IsReady = false,
                    IsAck = false
                });

                _ = _mesh.SendAsync(targetSessionId, new RavaGame(string.Empty, payload));
            }

            if (pending.Count == 0)
                return null;

            var allResponses = Task.WhenAll(pending.Select(p => p.Completion.Task));
            var timeoutTask = Task.Delay(CharacterOfferTimeout, ct);
            await Task.WhenAny(allResponses, timeoutTask).ConfigureAwait(false);

            var result = new MeshCharacterPushOfferResult { RequestId = requestId };

            foreach (var item in pending)
            {
                if (!item.Completion.Task.IsCompletedSuccessfully)
                    continue;

                var response = item.Completion.Task.Result;
                result.AcceptedUids.Add(item.TargetUid);

                var missingForUid = NormalizeHashes(response.MissingHashes)
                    .OrderBy(h => h, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                result.MissingHashesByUid[item.TargetUid] = missingForUid;

                foreach (var hash in missingForUid)
                    result.MissingHashes.Add(hash);
            }

            return result.AcceptedUids.Count > 0 ? result : null;
        }
        finally
        {
            foreach (var item in pending)
                _pendingCharacterOfferByKey.TryRemove(item.Key, out _);
        }
    }

    public async Task<HashSet<string>> SignalCharacterDataReadyAsync(IReadOnlyDictionary<string, string> targetSessionByUid, string requestId, IEnumerable<string> targetUids, CharacterData finalCharacterData, CancellationToken ct, IReadOnlyDictionary<string, CharacterData>? finalCharacterDataByUid = null)
    {
        var acknowledged = new HashSet<string>(StringComparer.Ordinal);

        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || string.IsNullOrWhiteSpace(requestId) || targetSessionByUid == null || targetSessionByUid.Count == 0 || finalCharacterData == null)
            return acknowledged;

        var targetList = targetUids
            .Where(uid => !string.IsNullOrWhiteSpace(uid))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (targetList.Count == 0)
            return acknowledged;

        var finalDataHash = finalCharacterData.DataHash.Value ?? string.Empty;
        var finalHashList = CollectCharacterDataHashes(finalCharacterData).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        var finalHashSizeHints = BuildLocalHashSizeHints(finalHashList);

        var pending = new List<(string Key, string TargetUid, TaskCompletionSource<bool> Completion)>();

        try
        {
            foreach (var targetUid in targetList)
            {
                ct.ThrowIfCancellationRequested();

                if (!targetSessionByUid.TryGetValue(targetUid, out var targetSessionId) || string.IsNullOrWhiteSpace(targetSessionId))
                    continue;

                var key = BuildPendingCharacterPushKey(requestId, targetUid);
                var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

                if (!_pendingCharacterReadyAckByKey.TryAdd(key, completion))
                    continue;

                pending.Add((key, targetUid, completion));
            }

            if (pending.Count == 0)
                return acknowledged;

            for (var attempt = 1; attempt <= CharacterReadyAckAttempts; attempt++)
            {
                var remaining = pending
                    .Where(item => !item.Completion.Task.IsCompletedSuccessfully)
                    .ToList();

                if (remaining.Count == 0)
                    break;

                foreach (var item in remaining)
                {
                    ct.ThrowIfCancellationRequested();

                    if (!targetSessionByUid.TryGetValue(item.TargetUid, out var targetSessionId) || string.IsNullOrWhiteSpace(targetSessionId))
                        continue;

                    var dataForTarget = GetCharacterDataForTarget(finalCharacterDataByUid, item.TargetUid, finalCharacterData);
                    var payload = BuildCharacterDataPayload(new CharacterDataMeshPayload
                    {
                        ProtocolVersion = 2,
                        RequestId = requestId,
                        SenderUid = _ownUid,
                        TargetUid = item.TargetUid,
                        DataHash = dataForTarget.DataHash.Value ?? finalDataHash,
                        CharacterDataJsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(dataForTarget),
                        RequiredHashes = finalHashList,
                        MissingHashes = [],
                        HashSizeHints = finalHashSizeHints,
                        IsResponse = false,
                        IsReady = true,
                        IsAck = false
                    });

                    _ = _mesh.SendAsync(targetSessionId, new RavaGame(string.Empty, payload));
                }

                var allRemaining = Task.WhenAll(remaining.Select(p => p.Completion.Task));
                var timeoutTask = Task.Delay(CharacterReadyAckTimeout, ct);
                await Task.WhenAny(allRemaining, timeoutTask).ConfigureAwait(false);
            }

            foreach (var item in pending)
            {
                if (item.Completion.Task.IsCompletedSuccessfully && item.Completion.Task.Result)
                    acknowledged.Add(item.TargetUid);
            }

            return acknowledged;
        }
        finally
        {
            foreach (var item in pending)
                _pendingCharacterReadyAckByKey.TryRemove(item.Key, out _);
        }
    }

    private void HandleCharacterDataMeshMessage(SyncshellGameMeshMessage msg, CharacterDataMeshPayload payload)
    {
        if (payload.ProtocolVersion < 2)
        {
            HandleLegacyCharacterDataMeshMessage(msg, payload);
            return;
        }

        if (payload.IsAck)
        {
            if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.SenderUid, _ownUid, StringComparison.Ordinal))
                return;

            var key = BuildPendingCharacterPushKey(payload.RequestId, payload.TargetUid);
            if (_pendingCharacterReadyAckByKey.TryRemove(key, out var readyCompletion))
                readyCompletion.TrySetResult(true);

            return;
        }

        if (payload.IsResponse)
        {
            if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.SenderUid, _ownUid, StringComparison.Ordinal))
                return;

            var key = BuildPendingCharacterPushKey(payload.RequestId, payload.TargetUid);
            if (_pendingCharacterOfferByKey.TryRemove(key, out var offerCompletion))
                offerCompletion.TrySetResult(payload);

            return;
        }

        if (payload.IsReady)
        {
            HandleCharacterDataReadyMessage(msg, payload);
            return;
        }

        HandleCharacterDataOfferMessage(msg, payload);
    }

    private void HandleLegacyCharacterDataMeshMessage(SyncshellGameMeshMessage msg, CharacterDataMeshPayload payload)
    {
        if (payload.IsAck)
        {
            if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.SenderUid, _ownUid, StringComparison.Ordinal))
                return;

            var key = BuildPendingCharacterPushKey(payload.RequestId, payload.TargetUid);
            if (_pendingCharacterReadyAckByKey.TryRemove(key, out var completion))
                completion.TrySetResult(true);

            return;
        }

        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        if (string.IsNullOrWhiteSpace(payload.SenderUid) || payload.CharacterDataJsonUtf8 == null || payload.CharacterDataJsonUtf8.Length == 0)
            return;

        if (TryReceiveCharacterData(payload.SenderUid, payload.CharacterDataJsonUtf8))
        {
            var ack = BuildCharacterDataPayload(new CharacterDataMeshPayload
            {
                RequestId = payload.RequestId,
                SenderUid = payload.SenderUid,
                TargetUid = _ownUid,
                DataHash = payload.DataHash ?? string.Empty,
                CharacterDataJsonUtf8 = [],
                IsAck = true
            }, legacy: true);

            _ = _mesh.SendAsync(msg.FromSessionId, new RavaGame(string.Empty, ack));
        }
    }

    private void HandleCharacterDataOfferMessage(SyncshellGameMeshMessage msg, CharacterDataMeshPayload payload)
    {
        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        if (string.IsNullOrWhiteSpace(payload.SenderUid) || string.IsNullOrWhiteSpace(payload.RequestId) || payload.CharacterDataJsonUtf8 == null || payload.CharacterDataJsonUtf8.Length == 0)
            return;

        FileDownloadManager.RegisterDirectCdnSizeHints(payload.HashSizeHints);

        var missing = NormalizeHashes(payload.RequiredHashes).Where(h => !HasUsableLocalHash(h)).OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        var key = BuildPendingCharacterPushKey(payload.RequestId, payload.SenderUid);

        RemoveOlderPendingInboundCharacterPushes(payload.SenderUid, payload.RequestId);

        _pendingInboundCharacterPushByKey[key] = new PendingInboundCharacterPush(
            msg.FromSessionId,
            payload.SenderUid,
            payload.RequestId,
            payload.DataHash ?? string.Empty,
            payload.CharacterDataJsonUtf8,
            missing.Count,
            DateTime.UtcNow);

        var response = BuildCharacterDataPayload(new CharacterDataMeshPayload
        {
            ProtocolVersion = 2,
            RequestId = payload.RequestId,
            SenderUid = payload.SenderUid,
            TargetUid = _ownUid,
            DataHash = payload.DataHash ?? string.Empty,
            CharacterDataJsonUtf8 = [],
            RequiredHashes = [],
            MissingHashes = missing,
            HashSizeHints = [],
            IsResponse = true,
            IsReady = false,
            IsAck = false
        });

        _ = _mesh.SendAsync(msg.FromSessionId, new RavaGame(string.Empty, response));
    }

    private void HandleCharacterDataReadyMessage(SyncshellGameMeshMessage msg, CharacterDataMeshPayload payload)
    {
        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        var key = BuildPendingCharacterPushKey(payload.RequestId, payload.SenderUid);
        if (!_pendingInboundCharacterPushByKey.TryRemove(key, out var pending))
        {
            Logger.LogDebug("Mesh character data ready arrived without a pending offer. Sender={sender}, RequestId={requestId}", payload.SenderUid, payload.RequestId);
            return;
        }

        FileDownloadManager.RegisterDirectCdnSizeHints(payload.HashSizeHints);

        var characterJson = payload.CharacterDataJsonUtf8 != null && payload.CharacterDataJsonUtf8.Length > 0
            ? payload.CharacterDataJsonUtf8
            : pending.CharacterDataJsonUtf8;

        var effectiveDataHash = !string.IsNullOrWhiteSpace(payload.DataHash)
            ? payload.DataHash
            : pending.DataHash;

        MarkRecentMeshReadyForCharacterData(pending.SenderUid, effectiveDataHash);

        if (!TryReceiveCharacterData(pending.SenderUid, characterJson))
            return;

        var ack = BuildCharacterDataPayload(new CharacterDataMeshPayload
        {
            ProtocolVersion = 2,
            RequestId = payload.RequestId,
            SenderUid = payload.SenderUid,
            TargetUid = _ownUid,
            DataHash = effectiveDataHash,
            CharacterDataJsonUtf8 = [],
            RequiredHashes = [],
            MissingHashes = [],
            HashSizeHints = [],
            IsResponse = false,
            IsReady = false,
            IsAck = true
        });

        _ = _mesh.SendAsync(string.IsNullOrWhiteSpace(pending.FromSessionId) ? msg.FromSessionId : pending.FromSessionId, new RavaGame(string.Empty, ack));
    }

    private bool TryReceiveCharacterData(string senderUid, byte[] characterDataJsonUtf8)
    {
        try
        {
            var characterData = JsonSerializer.Deserialize<CharacterData>(characterDataJsonUtf8);
            if (characterData == null)
                return false;

            _pairManager.ReceiveCharaData(new OnlineUserCharaDataDto(new UserData(senderUid), characterData));
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Mesh character data receive failed; sender will fall back to server route if ACK times out.");
            return false;
        }
    }

    private void HandlePreflightMeshMessage(SyncshellGameMeshMessage msg, FilePresencePreflightPayload payload)
    {
        if (payload.IsResponse)
        {
            if (string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.RequesterUid, _ownUid, StringComparison.Ordinal))
                return;

            var key = BuildPendingPreflightKey(payload.RequestId, payload.TargetUid);
            if (_pendingPreflightByKey.TryRemove(key, out var completion))
                completion.TrySetResult(payload);

            return;
        }

        if (!_apiController.IsConnected || string.IsNullOrWhiteSpace(_ownUid) || !string.Equals(payload.TargetUid, _ownUid, StringComparison.Ordinal))
            return;

        var hashes = NormalizeHashes(payload.Hashes).ToList();
        if (hashes.Count == 0)
            return;

        var missing = hashes.Where(h => !HasUsableLocalHash(h)).ToList();

        var response = BuildPreflightPayload(new FilePresencePreflightPayload
        {
            RequestId = payload.RequestId,
            RequesterUid = payload.RequesterUid ?? string.Empty,
            TargetUid = _ownUid,
            DataHash = payload.DataHash ?? string.Empty,
            Hashes = [],
            MissingHashes = missing,
            IsResponse = true
        });

        _ = _mesh.SendAsync(msg.FromSessionId, new RavaGame(string.Empty, response));
    }

    private bool HasUsableLocalHash(string hash)
    {
        try
        {
            var cache = _fileCacheManager.GetFileCacheByHash(hash);
            if (cache == null || string.IsNullOrWhiteSpace(cache.ResolvedFilepath))
                return false;

            var fi = new FileInfo(cache.ResolvedFilepath);
            return fi.Exists && fi.Length > 0;
        }
        catch
        {
            return false;
        }
    }

    private void CleanupExpiredInboundCharacterPushes()
    {
        var now = DateTime.UtcNow;
        foreach (var kvp in _pendingInboundCharacterPushByKey.ToArray())
        {
            var lifetime = kvp.Value.MissingHashCount > 0
                ? PendingInboundCharacterPushUploadBarrierLifetime
                : PendingInboundCharacterPushNoMissingLifetime;

            if (now - kvp.Value.CreatedUtc > lifetime)
                _pendingInboundCharacterPushByKey.TryRemove(kvp.Key, out _);
        }

        var nowTicks = now.Ticks;
        foreach (var kvp in RecentMeshReadyByUidAndHash.ToArray())
        {
            if (kvp.Value <= nowTicks)
                RecentMeshReadyByUidAndHash.TryRemove(kvp.Key, out _);
        }
    }

    private void RemoveOlderPendingInboundCharacterPushes(string senderUid, string requestIdToKeep)
    {
        if (string.IsNullOrWhiteSpace(senderUid))
            return;

        foreach (var kvp in _pendingInboundCharacterPushByKey.ToArray())
        {
            if (string.Equals(kvp.Value.SenderUid, senderUid, StringComparison.Ordinal)
                && !string.Equals(kvp.Value.RequestId, requestIdToKeep, StringComparison.Ordinal))
            {
                _pendingInboundCharacterPushByKey.TryRemove(kvp.Key, out _);
            }
        }
    }

    private void ClearPendingCharacterPushes()
    {
        foreach (var kvp in _pendingCharacterOfferByKey.ToArray())
        {
            if (_pendingCharacterOfferByKey.TryRemove(kvp.Key, out var completion))
                completion.TrySetCanceled();
        }

        foreach (var kvp in _pendingCharacterReadyAckByKey.ToArray())
        {
            if (_pendingCharacterReadyAckByKey.TryRemove(kvp.Key, out var completion))
                completion.TrySetCanceled();
        }

        _pendingInboundCharacterPushByKey.Clear();
    }

    private void ClearPendingPreflights()
    {
        foreach (var kvp in _pendingPreflightByKey.ToArray())
        {
            if (_pendingPreflightByKey.TryRemove(kvp.Key, out var completion))
                completion.TrySetCanceled();
        }
    }

    private static CharacterData GetCharacterDataForTarget(IReadOnlyDictionary<string, CharacterData>? dataByUid, string uid, CharacterData fallback)
    {
        if (dataByUid != null && !string.IsNullOrWhiteSpace(uid) && dataByUid.TryGetValue(uid, out var data) && data != null)
            return data;

        return fallback;
    }

    private static string BuildPendingCharacterPushKey(string requestId, string uid)
        => string.Join('|', requestId ?? string.Empty, uid ?? string.Empty);

    private static string BuildRecentMeshReadyKey(string senderUid, string dataHash)
        => string.Join('|', senderUid ?? string.Empty, (dataHash ?? string.Empty).Trim().ToUpperInvariant());

    private static string BuildPendingPreflightKey(string requestId, string targetUid)
        => string.Join('|', requestId ?? string.Empty, targetUid ?? string.Empty);

    private static byte[] BuildCharacterDataPayload(CharacterDataMeshPayload payload, bool legacy = false)
    {
        var magic = legacy ? LegacyCharacterDataMagic : CharacterPushMagic;
        var packed = MessagePackSerializer.Serialize(payload);
        var buffer = new byte[magic.Length + packed.Length];
        Buffer.BlockCopy(magic, 0, buffer, 0, magic.Length);
        Buffer.BlockCopy(packed, 0, buffer, magic.Length, packed.Length);
        return buffer;
    }

    private static bool TryParseCharacterDataPayload(byte[] payload, out CharacterDataMeshPayload? parsed)
    {
        parsed = null;
        if (TryParsePayloadWithMagic(payload, CharacterPushMagic, out parsed))
            return true;

        if (TryParsePayloadWithMagic(payload, LegacyCharacterDataMagic, out parsed))
            return true;

        return false;
    }

    private static bool TryParsePayloadWithMagic<T>(byte[] payload, byte[] magic, out T? parsed)
    {
        parsed = default;

        if (payload == null || payload.Length <= magic.Length)
            return false;

        for (var i = 0; i < magic.Length; i++)
        {
            if (payload[i] != magic[i])
                return false;
        }

        try
        {
            parsed = MessagePackSerializer.Deserialize<T>(payload.AsMemory(magic.Length).ToArray());
            return parsed != null;
        }
        catch
        {
            return false;
        }
    }

    private static byte[] BuildPreflightPayload(FilePresencePreflightPayload payload)
    {
        var packed = MessagePackSerializer.Serialize(payload);
        var buffer = new byte[PreflightMagic.Length + packed.Length];
        Buffer.BlockCopy(PreflightMagic, 0, buffer, 0, PreflightMagic.Length);
        Buffer.BlockCopy(packed, 0, buffer, PreflightMagic.Length, packed.Length);
        return buffer;
    }

    private static bool TryParsePreflightPayload(byte[] payload, out FilePresencePreflightPayload? parsed)
        => TryParsePayloadWithMagic(payload, PreflightMagic, out parsed);

    private static byte[] BuildRepairPayload(string requesterUid, string targetUid, string dataHash, IReadOnlyCollection<string> hashes, string reason)
    {
        var packed = MessagePackSerializer.Serialize(new MissingCentralFileRepairPayload
        {
            RequesterUid = requesterUid ?? string.Empty,
            TargetUid = targetUid ?? string.Empty,
            DataHash = dataHash ?? string.Empty,
            Hashes = NormalizeHashes(hashes).ToList(),
            Reason = reason ?? string.Empty,
        });

        var buffer = new byte[RepairMagic.Length + packed.Length];
        Buffer.BlockCopy(RepairMagic, 0, buffer, 0, RepairMagic.Length);
        Buffer.BlockCopy(packed, 0, buffer, RepairMagic.Length, packed.Length);
        return buffer;
    }

    private static bool TryParseRepairPayload(byte[] payload, out MissingCentralFileRepairPayload? parsed)
        => TryParsePayloadWithMagic(payload, RepairMagic, out parsed);

    private static HashSet<string> NormalizeHashes(IEnumerable<string>? hashes)
    {
        return (hashes ?? Array.Empty<string>())
            .Where(h => !string.IsNullOrWhiteSpace(h))
            .Select(h => h.Trim().ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }
}
