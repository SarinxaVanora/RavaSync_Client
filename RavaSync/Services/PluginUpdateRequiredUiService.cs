using Dalamud.Interface;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using RavaSync.UI;

namespace RavaSync.Services;

public sealed class PluginUpdateRequiredUiService : IDisposable
{
    private static readonly TimeSpan RecheckDelay = TimeSpan.FromSeconds(3);

    private readonly ILogger<PluginUpdateRequiredUiService> _logger;
    private readonly IUiBuilder _uiBuilder;
    private readonly WindowSystem _windowSystem;
    private readonly PluginUpdateRequiredUi _window;
    private readonly PluginUpdateGate _pluginUpdateGate;
    private bool _started;
    private bool _continuingStartup;
    private CancellationTokenSource? _recheckCts;
    private Task? _recheckTask;
    private Func<CancellationToken, Task>? _continueStartupAsync;

    public PluginUpdateRequiredUiService(ILogger<PluginUpdateRequiredUiService> logger, IUiBuilder uiBuilder, PluginUpdateRequiredUi window, PluginUpdateGate pluginUpdateGate)
    {
        _logger = logger;
        _uiBuilder = uiBuilder;
        _window = window;
        _pluginUpdateGate = pluginUpdateGate;
        _windowSystem = new WindowSystem("RavaSync Update Required");
    }

    public void Start(Func<CancellationToken, Task>? continueStartupAsync = null)
    {
        if (_started)
            return;

        _continueStartupAsync = continueStartupAsync;
        _continuingStartup = false;
        _recheckCts = new CancellationTokenSource();
        _window.StatusText = "Waiting for the update to finish installing...";
        _window.IsOpen = true;
        _windowSystem.AddWindow(_window);
        _uiBuilder.Draw += Draw;
        _uiBuilder.OpenMainUi += Open;
        _uiBuilder.OpenConfigUi += Open;
        _started = true;

        if (_continueStartupAsync != null)
            _recheckTask = Task.Run(() => RecheckLoopAsync(_recheckCts.Token), CancellationToken.None);
    }

    private void Open()
    {
        _window.IsOpen = true;
    }

    private void Draw()
    {
        try
        {
            _windowSystem.Draw();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ignoring RavaSync update-required UI draw exception; draw will retry next frame.");
        }
    }

    private async Task RecheckLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(RecheckDelay, cancellationToken).ConfigureAwait(false);

                if (!_started || _continuingStartup || _continueStartupAsync == null)
                    return;

                var stillBlocked = await _pluginUpdateGate.CheckForUpdateAsync(cancellationToken, forceRefresh: true).ConfigureAwait(false);
                if (stillBlocked)
                {
                    _window.StatusText = "Still waiting for Dalamud to report the update as installed...";
                    continue;
                }

                _continuingStartup = true;
                _window.StatusText = "Update cleared. Finishing RavaSync startup...";
                await _continueStartupAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed while re-checking whether the RavaSync update gate can be cleared.");
                _window.StatusText = "Update re-check failed; trying again shortly...";
            }
        }
    }

    public void Dispose()
    {
        if (!_started)
            return;

        _recheckCts?.Cancel();
        _recheckCts?.Dispose();
        _recheckCts = null;
        _recheckTask = null;
        _continueStartupAsync = null;
        _uiBuilder.Draw -= Draw;
        _uiBuilder.OpenMainUi -= Open;
        _uiBuilder.OpenConfigUi -= Open;
        _windowSystem.RemoveAllWindows();
        _started = false;
    }
}
