using System.Collections;

namespace RavaSync.Utils;

public class RollingList<T> : IEnumerable<T>
{
    private readonly object _addLock = new();
    private readonly LinkedList<T> _list = new();

    public RollingList(int maximumCount)
    {
        if (maximumCount <= 0)
            throw new ArgumentException(message: null, nameof(maximumCount));

        MaximumCount = maximumCount;
    }

    public int Count
    {
        get
        {
            lock (_addLock)
            {
                return _list.Count;
            }
        }
    }

    public int MaximumCount { get; }

    public T this[int index]
    {
        get
        {
            lock (_addLock)
            {
                if (index < 0 || index >= _list.Count)
                    throw new ArgumentOutOfRangeException(nameof(index));

                return _list.Skip(index).First();
            }
        }
    }

    public void Add(T value)
    {
        lock (_addLock)
        {
            if (_list.Count == MaximumCount)
                _list.RemoveFirst();

            _list.AddLast(value);
        }
    }

    public List<T> Snapshot()
    {
        lock (_addLock)
        {
            return _list.ToList();
        }
    }

    public IEnumerator<T> GetEnumerator()
    {
        return Snapshot().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}