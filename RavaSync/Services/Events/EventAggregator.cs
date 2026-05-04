using System.Threading.Channels;
using RavaSync.Services.Mediator;
using RavaSync.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RavaSync.Services.Events;

public class EventAggregator : MediatorSubscriberBase, IHostedService
{
    private readonly RollingList<Event> _events = new(500);
    private readonly SemaphoreSlim _lock = new(1);
    private readonly Channel<Event> _fileWriteChannel = Channel.CreateUnbounded<Event>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false,
    });
    private readonly string _configDirectory;
    private readonly ILogger<EventAggregator> _logger;
    private CancellationTokenSource? _fileWriterCts;
    private Task? _fileWriterTask;

    public Lazy<List<Event>> EventList { get; private set; }
    public bool NewEventsAvailable => !EventList.IsValueCreated;
    public string EventLogFolder => Path.Combine(_configDirectory, "eventlog");
    private string CurrentLogName => $"{DateTime.Now:yyyy-MM-dd}-events.log";
    private DateTime _currentTime;

    public EventAggregator(string configDirectory, ILogger<EventAggregator> logger, MareMediator mareMediator) : base(logger, mareMediator)
    {
        Mediator.Subscribe<EventMessage>(this, (msg) =>
        {
            _ = ProcessEventAsync(msg.Event);
        });


        EventList = CreateEventLazy();
        _configDirectory = configDirectory;
        _logger = logger;
        _currentTime = DateTime.Now - TimeSpan.FromDays(1);
    }

    private void RecreateLazy()
    {
        if (!EventList.IsValueCreated) return;

        EventList = CreateEventLazy();
    }

    private async Task ProcessEventAsync(Event evt)
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            Logger.LogTrace("Received Event: {evt}", evt.ToString());
            _events.Add(evt);
        }
        finally
        {
            _lock.Release();
        }

        _fileWriteChannel.Writer.TryWrite(evt);
        RecreateLazy();
    }


    private Lazy<List<Event>> CreateEventLazy()
    {
        return new Lazy<List<Event>>(() =>
        {
            _lock.Wait();
            try
            {
                return [.. _events];
            }
            finally
            {
                _lock.Release();
            }
        });
    }


    private async Task ProcessFileWritesAsync(CancellationToken cancellationToken)
    {
        var buffer = new List<Event>(64);

        try
        {
            while (await _fileWriteChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                buffer.Clear();

                while (buffer.Count < 64 && _fileWriteChannel.Reader.TryRead(out var evt))
                {
                    buffer.Add(evt);
                }

                if (buffer.Count > 0)
                {
                    WriteToFile(buffer);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Event file writer stopped unexpectedly");
        }
        finally
        {
            buffer.Clear();
            while (_fileWriteChannel.Reader.TryRead(out var evt))
            {
                buffer.Add(evt);
            }

            if (buffer.Count > 0)
            {
                WriteToFile(buffer);
            }
        }
    }

    private void WriteToFile(IReadOnlyCollection<Event> receivedEvents)
    {
        if (receivedEvents.Count == 0) return;

        if (DateTime.Now.Day != _currentTime.Day)
        {
            try
            {
                _currentTime = DateTime.Now;
                var filesInDirectory = Directory.EnumerateFiles(EventLogFolder, "*.log");
                if (filesInDirectory.Skip(10).Any())
                {
                    File.Delete(filesInDirectory.OrderBy(f => new FileInfo(f).LastWriteTimeUtc).First());
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not delete last events");
            }
        }

        var eventLogFile = Path.Combine(EventLogFolder, CurrentLogName);
        try
        {
            if (!Directory.Exists(EventLogFolder)) Directory.CreateDirectory(EventLogFolder);
            File.AppendAllLines(eventLogFile, receivedEvents.Select(e => e.ToString()));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, $"Could not write to event file {eventLogFile}");
        }
    }


    public Task StartAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Starting EventAggregatorService");
        _fileWriterCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _fileWriterTask = Task.Run(() => ProcessFileWritesAsync(_fileWriterCts.Token), CancellationToken.None);
        Logger.LogInformation("Started EventAggregatorService");
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _fileWriteChannel.Writer.TryComplete();

        var cts = _fileWriterCts;
        _fileWriterCts = null;

        if (cts != null)
        {
            try
            {
                await cts.CancelAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // ignored
            }

            cts.Dispose();
        }

        if (_fileWriterTask != null)
        {
            try
            {
                await _fileWriterTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // expected on shutdown
            }
            finally
            {
                _fileWriterTask = null;
            }
        }
    }
}
