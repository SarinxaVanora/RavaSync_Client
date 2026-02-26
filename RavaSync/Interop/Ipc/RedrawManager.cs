using Dalamud.Game.ClientState.Objects.Types;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace RavaSync.Interop.Ipc;

public class RedrawManager : IMediatorSubscriber
{
    private readonly MareMediator _mareMediator;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly ConcurrentDictionary<nint, bool> _penumbraRedrawRequests = [];
    private readonly ConcurrentDictionary<nint, RedrawQueue> _redrawQueues = new();
    private readonly ConcurrentDictionary<nint, WeakReference<GameObjectHandler>> _handlersByAddress = new();
    private CancellationTokenSource _disposalCts = new();


    private sealed class RedrawQueue
    {
        public readonly ConcurrentQueue<WorkItem> Items = new();
        public int RunnerActive; public SemaphoreSlim RedrawSemaphore { get; init; } = new(2, 2);

    }

    private sealed class WorkItem
    {
        public required ILogger Logger;
        public required GameObjectHandler Handler;
        public required Guid ApplicationId;
        public required Action<ICharacter> Action;
        public required TaskCompletionSource<bool> Tcs;
        public required CancellationToken Token;
    }

    public MareMediator Mediator => _mareMediator;
    public SemaphoreSlim RedrawSemaphore { get; init; } = new(2, 2);

    public RedrawManager(MareMediator mareMediator, DalamudUtilService dalamudUtil)
    {
        _mareMediator = mareMediator;
        _dalamudUtil = dalamudUtil;

        _mareMediator.Subscribe<GameObjectHandlerCreatedMessage>(this, msg =>
        {
            var h = msg.GameObjectHandler;
            if (h.Address != nint.Zero)
                _handlersByAddress[h.Address] = new WeakReference<GameObjectHandler>(h);
        });

        _mareMediator.Subscribe<GameObjectHandlerDestroyedMessage>(this, msg =>
        {
            var h = msg.GameObjectHandler;
            if (h.Address != nint.Zero)
                _handlersByAddress.TryRemove(h.Address, out _);
        });
    }

    public bool TryGetHandler(nint address, out GameObjectHandler handler)
    {
        handler = null!;

        if (address == nint.Zero)
            return false;

        if (_handlersByAddress.TryGetValue(address, out var weak)
            && weak.TryGetTarget(out var h)
            && h != null
            && h.Address == address)
        {
            handler = h;
            return true;
        }

        return false;
    }

    public async Task ExternalPenumbraRedrawAsync(ILogger logger, ICharacter character, Guid applicationId, Action<ICharacter> action, CancellationToken token)
    {
        if (character == null) return;

        var addr = character.Address;
        if (addr == nint.Zero) return;

        if (TryGetHandler(addr, out var handler))
        {
            await PenumbraRedrawInternalAsync(logger, handler, applicationId, action, token).ConfigureAwait(false);
            return;
        }
        await _dalamudUtil.RunOnFrameworkThread(() =>
        {
            try
            {
                action(character);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "[{appid}] External redraw fallback failed for addr {addr}", applicationId, addr);
            }

            return 0;
        }).ConfigureAwait(false);
    }

    public Task PenumbraAfterGPoseAsync(ILogger logger, ICharacter character, Guid applicationId, Action<ICharacter> action, CancellationToken token)
        => ExternalPenumbraRedrawAsync(logger, character, applicationId, action, token);


    public async Task PenumbraRedrawInternalAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, Action<ICharacter> action, CancellationToken token)
    {
        var queue = _redrawQueues.GetOrAdd(handler.Address, _ => new RedrawQueue());

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var reg = token.Register(() => tcs.TrySetCanceled(token));

        queue.Items.Enqueue(new WorkItem
        {
            Logger = logger,
            Handler = handler,
            ApplicationId = applicationId,
            Action = action,
            Tcs = tcs,
            Token = token
        });

        if (Interlocked.CompareExchange(ref queue.RunnerActive, 1, 0) == 0)
        {
            _ = Task.Run(() => ProcessQueueAsync(handler.Address, queue));
        }

        await tcs.Task.ConfigureAwait(false);
    }

    private async Task ProcessQueueAsync(nint address, RedrawQueue queue)
    {
        try
        {
            await Task.Delay(25).ConfigureAwait(false);

            while (!_disposalCts.IsCancellationRequested)
            {
                var batch = new List<WorkItem>(32);
                while (queue.Items.TryDequeue(out var wi))
                    batch.Add(wi);

                if (batch.Count == 0)
                    break;

                var handler = batch[0].Handler;
                var logger = batch[0].Logger;
                var applicationId = batch[0].ApplicationId;

                _mareMediator.Publish(new PenumbraStartRedrawMessage(address));
                _penumbraRedrawRequests[address] = true;

                try
                {
                    using CancellationTokenSource localTimeout = new();
                    using CancellationTokenSource linked =
                        CancellationTokenSource.CreateLinkedTokenSource(localTimeout.Token, _disposalCts.Token);

                    localTimeout.CancelAfter(TimeSpan.FromSeconds(15));
                    var linkedToken = linked.Token;

                    await handler.ActOnFrameworkAfterEnsureNoDrawAsync(chara =>
                    {
                        foreach (var wi in batch)
                        {
                            if (wi.Tcs.Task.IsCompleted || wi.Token.IsCancellationRequested)
                            {
                                wi.Tcs.TrySetCanceled(wi.Token);
                                continue;
                            }

                            try
                            {
                                wi.Action(chara);
                                wi.Tcs.TrySetResult(true);
                            }
                            catch (Exception ex)
                            {
                                wi.Logger.LogDebug(ex, "[{appid}] Redraw batch action failed", wi.ApplicationId);
                                wi.Tcs.TrySetResult(false);
                            }
                        }
                    }, linkedToken).ConfigureAwait(false);

                    if (!_disposalCts.IsCancellationRequested)
                    {
                        await _dalamudUtil
                            .WaitWhileCharacterIsDrawing(logger, handler, applicationId, 15000, linkedToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.LogDebug("[{appid}] Penumbra redraw batch cancelled for addr {addr}", applicationId, address);

                    foreach (var wi in batch)
                        wi.Tcs.TrySetCanceled();
                }
                finally
                {
                    _penumbraRedrawRequests[address] = false;
                    _mareMediator.Publish(new PenumbraEndRedrawMessage(address));
                }

                await Task.Yield();
            }
        }
        finally
        {
            Interlocked.Exchange(ref queue.RunnerActive, 0);

            if (!queue.Items.IsEmpty && Interlocked.CompareExchange(ref queue.RunnerActive, 1, 0) == 0)
            {
                _ = Task.Run(() => ProcessQueueAsync(address, queue));
            }
        }
    }


    internal void Cancel()
    {
        _disposalCts = _disposalCts.CancelRecreate();
    }
}
