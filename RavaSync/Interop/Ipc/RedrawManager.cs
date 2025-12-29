using Dalamud.Game.ClientState.Objects.Types;
using RavaSync.PlayerData.Handlers;
using RavaSync.Services;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace RavaSync.Interop.Ipc;

public class RedrawManager
{
    private readonly MareMediator _mareMediator;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly ConcurrentDictionary<nint, bool> _penumbraRedrawRequests = [];
    private CancellationTokenSource _disposalCts = new();

    public SemaphoreSlim RedrawSemaphore { get; init; } = new(2, 2);

    public RedrawManager(MareMediator mareMediator, DalamudUtilService dalamudUtil)
    {
        _mareMediator = mareMediator;
        _dalamudUtil = dalamudUtil;
    }

    public async Task PenumbraRedrawInternalAsync(ILogger logger, GameObjectHandler handler, Guid applicationId, Action<ICharacter> action, CancellationToken token)
    {
        _mareMediator.Publish(new PenumbraStartRedrawMessage(handler.Address));

        _penumbraRedrawRequests[handler.Address] = true;

        //try
        //{
        //    using CancellationTokenSource cancelToken = new CancellationTokenSource();
        //    using CancellationTokenSource combinedCts = CancellationTokenSource.CreateLinkedTokenSource(cancelToken.Token, token, _disposalCts.Token);
        //    var combinedToken = combinedCts.Token;
        //    cancelToken.CancelAfter(TimeSpan.FromSeconds(15));
        //    await handler.ActOnFrameworkAfterEnsureNoDrawAsync(action, combinedToken).ConfigureAwait(false);

        //    if (!_disposalCts.Token.IsCancellationRequested)
        //        await _dalamudUtil.WaitWhileCharacterIsDrawing(logger, handler, applicationId, 15000, combinedToken).ConfigureAwait(false);
        //}
        //finally
        //{
        //    _penumbraRedrawRequests[handler.Address] = false;
        //    _mareMediator.Publish(new PenumbraEndRedrawMessage(handler.Address));
        //}

        try
        {
            try
            {
                using CancellationTokenSource localTimeout = new();
                using CancellationTokenSource linked =
                    CancellationTokenSource.CreateLinkedTokenSource(localTimeout.Token, token, _disposalCts.Token);

                // enforce a 15s timeout
                localTimeout.CancelAfter(TimeSpan.FromSeconds(15));
                var linkedToken = linked.Token;

                await handler
                    .ActOnFrameworkAfterEnsureNoDrawAsync(action, linkedToken)
                    .ConfigureAwait(false);

                if (!_disposalCts.IsCancellationRequested)
                {
                    await _dalamudUtil
                        .WaitWhileCharacterIsDrawing(logger, handler, applicationId, 15000, linkedToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogDebug("[{appid}] Penumbra redraw cancelled for {name}", applicationId, handler.Name);
            }
        }
        finally
        {
            _penumbraRedrawRequests[handler.Address] = false;
            _mareMediator.Publish(new PenumbraEndRedrawMessage(handler.Address));
        }
    }

    internal void Cancel()
    {
        _disposalCts = _disposalCts.CancelRecreate();
    }
}
