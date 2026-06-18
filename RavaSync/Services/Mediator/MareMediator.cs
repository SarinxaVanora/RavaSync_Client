using RavaSync.MareConfiguration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Text;

namespace RavaSync.Services.Mediator;

public sealed class MareMediator : IHostedService
{
    private readonly object _addRemoveLock = new();
    private readonly ConcurrentDictionary<object, DateTime> _lastErrorTime = [];
    private readonly ILogger<MareMediator> _logger;
    private readonly CancellationTokenSource _loopCts = new();
    private readonly ConcurrentQueue<MessageBase> _messageQueue = new();
    private readonly PerformanceCollectorService _performanceCollector;
    private readonly MareConfigService _mareConfigService;
    private readonly ConcurrentDictionary<Type, HashSet<SubscriberAction>> _subscriberDict = [];
    private readonly ConcurrentDictionary<Type, SubscriberAction[]> _subscriberSnapshotCache = [];
    private bool _processQueue = false;
    public MareMediator(ILogger<MareMediator> logger, PerformanceCollectorService performanceCollector, MareConfigService mareConfigService)
    {
        _logger = logger;
        _performanceCollector = performanceCollector;
        _mareConfigService = mareConfigService;
    }

    public void PrintSubscriberInfo()
    {
        foreach (var subscriber in _subscriberDict.SelectMany(c => c.Value.Select(v => v.Subscriber))
            .DistinctBy(p => p).OrderBy(p => p.GetType().FullName, StringComparer.Ordinal).ToList())
        {
            _logger.LogInformation("Subscriber {type}: {sub}", subscriber.GetType().Name, subscriber.ToString());
            StringBuilder sb = new();
            sb.Append("=> ");
            foreach (var item in _subscriberDict.Where(item => item.Value.Any(v => v.Subscriber == subscriber)).ToList())
            {
                sb.Append(item.Key.Name).Append(", ");
            }

            if (!string.Equals(sb.ToString(), "=> ", StringComparison.Ordinal))
                _logger.LogInformation("{sb}", sb.ToString());
            _logger.LogInformation("---");
        }
    }

    public void Publish<T>(T message) where T : MessageBase
    {
        if (message.KeepThreadContext)
        {
            ExecuteMessage(message);
        }
        else
        {
            _messageQueue.Enqueue(message);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting MareMediator");

        _ = Task.Run(async () =>
        {
            while (!_loopCts.Token.IsCancellationRequested)
            {
                while (!_processQueue)
                {
                    await Task.Delay(100, _loopCts.Token).ConfigureAwait(false);
                }

                await Task.Delay(100, _loopCts.Token).ConfigureAwait(false);

                HashSet<MessageBase> processedMessages = [];
                while (_messageQueue.TryDequeue(out var message))
                {
                    if (processedMessages.Contains(message)) { continue; }
                    processedMessages.Add(message);

                    ExecuteMessage(message);
                }
            }
        });

        _logger.LogInformation("Started MareMediator");

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _messageQueue.Clear();
        _loopCts.Cancel();
        _loopCts.Dispose();
        return Task.CompletedTask;
    }

    public void Subscribe<T>(IMediatorSubscriber subscriber, Action<T> action) where T : MessageBase
    {
        var messageType = typeof(T);
        lock (_addRemoveLock)
        {
            _subscriberDict.TryAdd(messageType, []);

            if (!_subscriberDict[messageType].Add(new SubscriberAction<T>(subscriber, action)))
            {
                throw new InvalidOperationException("Already subscribed");
            }

            _subscriberSnapshotCache.TryRemove(messageType, out _);
        }
    }

    public void Unsubscribe<T>(IMediatorSubscriber subscriber) where T : MessageBase
    {
        var messageType = typeof(T);
        lock (_addRemoveLock)
        {
            if (_subscriberDict.ContainsKey(messageType))
            {
                _subscriberDict[messageType].RemoveWhere(p => p.Subscriber == subscriber);
                _subscriberSnapshotCache.TryRemove(messageType, out _);
            }
        }
    }

    internal void UnsubscribeAll(IMediatorSubscriber subscriber)
    {
        lock (_addRemoveLock)
        {
            foreach (Type kvp in _subscriberDict.Select(k => k.Key))
            {
                int unSubbed = _subscriberDict[kvp]?.RemoveWhere(p => p.Subscriber == subscriber) ?? 0;
                if (unSubbed > 0)
                {
                    _subscriberSnapshotCache.TryRemove(kvp, out _);
                    _logger.LogDebug("{sub} unsubscribed from {msg}", subscriber.GetType().Name, kvp.Name);
                }
            }
        }
    }

    private SubscriberAction[] CreateSubscriberSnapshot(Type messageType)
    {
        lock (_addRemoveLock)
        {
            if (!_subscriberDict.TryGetValue(messageType, out var subscribers) || subscribers == null || subscribers.Count == 0)
                return [];

            return subscribers
                .Where(s => s.Subscriber != null)
                .OrderBy(static s => s.HighPriority ? 0 : 1)
                .ToArray();
        }
    }

    private void ExecuteMessage(MessageBase message)
    {
        var msgType = message.GetType();
        if (!_subscriberDict.TryGetValue(msgType, out var subscribers) || subscribers == null || subscribers.Count == 0) return;

        var subscribersCopy = _subscriberSnapshotCache.GetOrAdd(msgType, CreateSubscriberSnapshot);
        if (subscribersCopy.Length == 0) return;

        foreach (SubscriberAction subscriber in subscribersCopy)
        {
            try
            {
                if (_mareConfigService.Current.LogPerformance)
                {
                    var isSameThread = message.KeepThreadContext ? "$" : string.Empty;
                    _performanceCollector.LogPerformance(this, $"{isSameThread}Execute>{msgType.Name}+{subscriber.Subscriber.GetType().Name}>{subscriber.Subscriber}",
                        () => subscriber.Invoke(message));
                }
                else
                {
                    subscriber.Invoke(message);
                }
            }
            catch (Exception ex)
            {
                if (_lastErrorTime.TryGetValue(subscriber, out var lastErrorTime) && lastErrorTime.Add(TimeSpan.FromSeconds(10)) > DateTime.UtcNow)
                    continue;

                _logger.LogError(ex.InnerException ?? ex, "Error executing {type} for subscriber {subscriber}",
                    msgType.Name, subscriber.Subscriber.GetType().Name);
                _lastErrorTime[subscriber] = DateTime.UtcNow;
            }
        }
    }

    public void StartQueueProcessing()
    {
        _logger.LogInformation("Starting Message Queue Processing");
        _processQueue = true;
    }

    private abstract class SubscriberAction
    {
        protected SubscriberAction(IMediatorSubscriber subscriber)
        {
            Subscriber = subscriber;
            HighPriority = subscriber is IHighPriorityMediatorSubscriber;
        }

        public bool HighPriority { get; }
        public IMediatorSubscriber Subscriber { get; }
        public abstract void Invoke(MessageBase message);
    }

    private sealed class SubscriberAction<T> : SubscriberAction where T : MessageBase
    {
        private readonly Action<T> _action;

        public SubscriberAction(IMediatorSubscriber subscriber, Action<T> action) : base(subscriber)
        {
            _action = action;
        }

        public override void Invoke(MessageBase message) => _action((T)message);
    }
}
