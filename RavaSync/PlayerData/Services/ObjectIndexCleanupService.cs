using Microsoft.Extensions.Logging;
using RavaSync.Interop.Ipc;
using RavaSync.Services;
using System.Collections.Concurrent;

namespace RavaSync.PlayerData.Services;

public sealed class ObjectIndexCleanupService
{
    private const int CleanupDebounceMs = 200;

    private readonly ConcurrentDictionary<int, CleanupRequest> _pendingCleanupByObjectIndex = new();
    private readonly DalamudUtilService _dalamudUtil;
    private readonly IpcManager _ipcManager;
    private readonly ILogger<ObjectIndexCleanupService> _logger;

    public ObjectIndexCleanupService(ILogger<ObjectIndexCleanupService> logger, DalamudUtilService dalamudUtil, IpcManager ipcManager)
    {
        _logger = logger;
        _dalamudUtil = dalamudUtil;
        _ipcManager = ipcManager;
    }

    public Task CleanupIfEmptyAsync(int objectIndex, Guid applicationId)
    {
        return QueueCleanupAsync(objectIndex, expectedIdent: null, requireEmptySlot: true, applicationId);
    }

    public Task CleanupIfNotOwnedByIdentAsync(int objectIndex, string expectedIdent, Guid applicationId)
    {
        return QueueCleanupAsync(objectIndex, expectedIdent, requireEmptySlot: false, applicationId);
    }

    private Task QueueCleanupAsync(int objectIndex, string? expectedIdent, bool requireEmptySlot, Guid applicationId)
    {
        var request = new CleanupRequest(expectedIdent, requireEmptySlot, applicationId);

        while (true)
        {
            if (_pendingCleanupByObjectIndex.TryGetValue(objectIndex, out var existing))
            {
                existing.Cancel();
                if (!_pendingCleanupByObjectIndex.TryUpdate(objectIndex, request, existing))
                    continue;
            }
            else if (!_pendingCleanupByObjectIndex.TryAdd(objectIndex, request))
            {
                continue;
            }

            request.Task = RunQueuedCleanupAsync(objectIndex, request);
            return request.Task;
        }
    }

    private async Task RunQueuedCleanupAsync(int objectIndex, CleanupRequest request)
    {
        try
        {
            await Task.Delay(CleanupDebounceMs, request.CancellationToken).ConfigureAwait(false);

            if (!await ShouldCleanupAsync(objectIndex, request.ExpectedIdent, request.RequireEmptySlot, request.ApplicationId).ConfigureAwait(false))
                return;

            // Re-check immediately before clearing in case the slot was reused during the debounce window.
            if (!await ShouldCleanupAsync(objectIndex, request.ExpectedIdent, request.RequireEmptySlot, request.ApplicationId).ConfigureAwait(false))
                return;

            await ClearObjectIndexStateAsync(objectIndex, request.ApplicationId, request.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[{applicationId}] Failed cleanup for old ObjectIndex {idx}", request.ApplicationId, objectIndex);
        }
        finally
        {
            request.Dispose();
            _pendingCleanupByObjectIndex.TryRemove(new KeyValuePair<int, CleanupRequest>(objectIndex, request));
        }
    }

    private async Task<bool> ShouldCleanupAsync(int objectIndex, string? expectedIdent, bool requireEmptySlot, Guid applicationId)
    {
        var result = await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            var current = _dalamudUtil.GetCharacterFromObjectTableByIndex(objectIndex);
            var isEmpty = current == null || current.Address == IntPtr.Zero;

            if (requireEmptySlot)
            {
                return new CleanupCheckResult(isEmpty, isEmpty
                    ? null
                    : $"slot {objectIndex} is no longer empty");
            }

            if (isEmpty)
                return new CleanupCheckResult(true, null);

            var currentIdent = _dalamudUtil.GetIdentFromGameObject(current);
            var shouldCleanup = !string.Equals(currentIdent, expectedIdent, StringComparison.Ordinal);
            var skipReason = shouldCleanup
                ? null
                : $"slot {objectIndex} is still owned by {expectedIdent}";
            return new CleanupCheckResult(shouldCleanup, skipReason);
        }).ConfigureAwait(false);

        if (!result.ShouldCleanup && result.SkipReason != null)
        {
            _logger.LogDebug("[{applicationId}] Skipping cleanup for ObjectIndex {idx} because {reason}",
                applicationId, objectIndex, result.SkipReason);
        }

        return result.ShouldCleanup;
    }

    private async Task ClearObjectIndexStateAsync(int objectIndex, Guid applicationId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.Penumbra.AssignEmptyCollectionAsync(_logger, objectIndex).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.Honorific.ClearTitleByObjectIndexAsync(objectIndex).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.PetNames.ClearPlayerDataByObjectIndexAsync(objectIndex).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.Heels.UnregisterByObjectIndexAsync(objectIndex).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.CustomizePlus.RevertByObjectIndexAsync((ushort)objectIndex).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        await _ipcManager.Glamourer.RevertByObjectIndexAsync(_logger, objectIndex, applicationId).ConfigureAwait(false);
    }

    private readonly record struct CleanupCheckResult(bool ShouldCleanup, string? SkipReason);

    private sealed class CleanupRequest : IDisposable
    {
        private readonly CancellationTokenSource _cts = new();

        public CleanupRequest(string? expectedIdent, bool requireEmptySlot, Guid applicationId)
        {
            ExpectedIdent = expectedIdent;
            RequireEmptySlot = requireEmptySlot;
            ApplicationId = applicationId;
        }

        public Guid ApplicationId { get; }
        public CancellationToken CancellationToken => _cts.Token;
        public string? ExpectedIdent { get; }
        public bool RequireEmptySlot { get; }
        public Task Task { get; set; } = Task.CompletedTask;

        public void Cancel()
        {
            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void Dispose()
        {
            _cts.Dispose();
        }
    }
}
