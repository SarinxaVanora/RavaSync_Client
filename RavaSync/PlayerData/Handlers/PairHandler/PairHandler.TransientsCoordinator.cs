using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RavaSync.API.Data;
using RavaSync.API.Data.Enum;
using RavaSync.FileCache;
using RavaSync.Interop.Ipc;
using RavaSync.PlayerData.Factories;
using RavaSync.PlayerData.Pairs;
using RavaSync.PlayerData.Services;
using RavaSync.Services;
using RavaSync.Services.Events;
using RavaSync.Services.Mediator;
using RavaSync.Services.ServerConfiguration;
using RavaSync.Utils;
using RavaSync.WebAPI.Files;
using RavaSync.WebAPI.Files.Models;

namespace RavaSync.PlayerData.Handlers;

public sealed partial class PairHandler
{
    private sealed class TransientsCoordinator : CoordinatorBase
    {
        public TransientsCoordinator(PairHandler owner) : base(owner)
        {
        }

        public async Task<bool> OnePassRedrawAsync(Guid applicationId, CancellationToken token, bool criticalRedraw = false)
        {
            for (var attempt = 1; ; attempt++)
            {
                token.ThrowIfCancellationRequested();

                if (!IsVisible)
                    return false;

                if (criticalRedraw && !_ipcManager.Penumbra.APIAvailable)
                {
                    Logger.LogDebug("[{applicationId}] Critical Penumbra redraw cannot run for {player}: Penumbra API unavailable", applicationId, Pair.UserData.AliasOrUID);
                    return false;
                }

                var fired = await TryOnePassRedrawAsync(applicationId, token, criticalRedraw, attempt).ConfigureAwait(false);
                if (fired)
                    return true;

                if (!criticalRedraw)
                    return false;

                if (!IsVisible)
                    return false;

                if (attempt == 1 || attempt % 5 == 0)
                {
                    Logger.LogDebug(
                        "[{applicationId}] Critical Penumbra redraw attempt {attempt} did not acknowledge for {player}; lifecycle redraw remains pending and will retry while visible",
                        applicationId,
                        attempt,
                        Pair.UserData.AliasOrUID);
                }

                await Task.Delay(Math.Min(25 + attempt * 20, 150), token).ConfigureAwait(false);
            }
        }

        private async Task<bool> TryOnePassRedrawAsync(Guid applicationId, CancellationToken token, bool criticalRedraw, int attempt)
        {
            var handler = _charaHandler;
            if (handler == null || handler.Address == nint.Zero)
                return false;

            var objIndex = await _dalamudUtil
                .RunOnFrameworkThread(() =>
                {
                    var currentHandler = _charaHandler;
                    if (currentHandler == null || currentHandler.Address == nint.Zero)
                        return -1;

                    var obj = currentHandler.GetGameObject();
                    return obj is Dalamud.Game.ClientState.Objects.Types.ICharacter chara ? chara.ObjectIndex : -1;
                })
                .ConfigureAwait(false);

            if (objIndex < 0)
            {
                Logger.LogTrace("[{applicationId}] Redraw attempt {attempt} could not resolve a live character object for {player}", applicationId, attempt, Pair.UserData.AliasOrUID);
                return false;
            }

            var addedInFlight = false;
            if (!criticalRedraw)
            {
                if (!RedrawObjectIndicesInFlight.TryAdd(objIndex, 0))
                {
                    Logger.LogTrace("[{applicationId}] Skipping redraw for idx {idx}: redraw already in flight", applicationId, objIndex);
                    return false;
                }

                addedInFlight = true;
            }

            var acquired = false;
            try
            {
                if (!criticalRedraw)
                {
                    await GlobalRedrawSemaphore.WaitAsync(token).ConfigureAwait(false);
                    acquired = true;

                    var seed = ((PlayerNameHash?.GetHashCode(StringComparison.Ordinal) ?? 0) ^ objIndex) & 0x1F;
                    var delayMs = SyncStorm.IsActive ? 10 + seed : 4 + (seed & 0x0F);
                    await Task.Delay(delayMs, token).ConfigureAwait(false);
                }

                handler = _charaHandler;
                if (handler == null || handler.Address == nint.Zero)
                    return false;

                var fired = criticalRedraw
                    ? await _ipcManager.Penumbra.RedrawDirectAndWaitAsync(Logger, handler, applicationId, token).ConfigureAwait(false)
                    : await _ipcManager.Penumbra.RedrawAsync(Logger, handler, applicationId, token, criticalRedraw: false).ConfigureAwait(false);

                if (!fired)
                    Logger.LogTrace("[{applicationId}] Penumbra redraw IPC did not fire for {player} on attempt {attempt}", applicationId, Pair.UserData.AliasOrUID, attempt);

                return fired;
            }
            finally
            {
                if (acquired)
                    GlobalRedrawSemaphore.Release();

                if (addedInFlight)
                    RedrawObjectIndicesInFlight.TryRemove(objIndex, out _);
            }
        }
    }
}
