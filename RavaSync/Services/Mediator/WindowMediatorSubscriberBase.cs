using System;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;

namespace RavaSync.Services.Mediator;

public abstract class WindowMediatorSubscriberBase : Window, IMediatorSubscriber, IDisposable
{
    protected readonly ILogger _logger;
    private readonly PerformanceCollectorService _performanceCollectorService;

    protected WindowMediatorSubscriberBase(ILogger logger, MareMediator mediator, string name,
        PerformanceCollectorService performanceCollectorService) : base(name)
    {
        _logger = logger;
        Mediator = mediator;
        _performanceCollectorService = performanceCollectorService;
        _logger.LogTrace("Creating {type}", GetType());

        Mediator.Subscribe<UiToggleMessage>(this, (msg) =>
        {
            if (msg.UiType == GetType())
            {
                Toggle();
            }
        });
    }

    public MareMediator Mediator { get; }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    public override void Draw()
    {
        // open a theme scope if a derived class provides one
        var scope = BeginThemeScope();
        try
        {
            _performanceCollectorService.LogPerformance(this, $"Draw", DrawInternal);
        }
        finally
        {
            scope?.Dispose();
        }
    }

    // derived windows can override and return a scope (e.g., _uiShared.BeginThemed())
    protected virtual IDisposable? BeginThemeScope() => null;

    protected abstract void DrawInternal();

    public virtual Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    protected virtual void Dispose(bool disposing)
    {
        _logger.LogTrace("Disposing {type}", GetType());
        Mediator.UnsubscribeAll(this);
    }
}
