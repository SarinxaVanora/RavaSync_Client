using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace RavaSync.Services.Optimisation.Reduction
{
    public class LinkedHashSet<T> : IReadOnlyCollection<T> where T : IComparable<T>
    {
        private readonly Dictionary<T, LinkedHashNode<T>> elements;
        private LinkedHashNode<T> first, last;

        
        
        
        public LinkedHashSet()
        {
            elements = new Dictionary<T, LinkedHashNode<T>>();
        }

        
        
        
        
        public LinkedHashSet(IEnumerable<T> initialValues) : this()
        {
            UnionWith(initialValues);
        }

        public LinkedHashNode<T> First => first;

        public LinkedHashNode<T> Last => last;

        #region Implementation of IEnumerable

        
        
        
        
        
        
        
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return GetEnumerator();
        }

        
        
        
        
        
        
        
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Implementation of ICollection<T>

        
        
        
        
        
        
        public int Count => elements.Count;

        
        
        
        
        public void Clear()
        {
            elements.Clear();
            first = null;
            last = null;
        }

        
        
        
        
        
        
        
        public bool Contains(T item)
        {
            return elements.ContainsKey(item);
        }

        
        
        
        
        public void CopyTo(T[] array, int arrayIndex)
        {
            int index = arrayIndex;

            foreach (T item in this)
            {
                array[index++] = item;
            }
        }

        
        
        
        
        
        
        
        public bool Remove(T item)
        {
            if (elements.TryGetValue(item, out LinkedHashNode<T> node))
            {
                elements.Remove(item);
                Unlink(node);
                return true;
            }

            return false;
        }

        #endregion


        #region Implementation of ISet<T>

        
        
        
        
        public void UnionWith(IEnumerable<T> other)
        {
            foreach (T item in other)
            {
                Add(item);
            }
        }

        
        
        
        
        public void IntersectWith(IEnumerable<T> other)
        {
            ISet<T> otherSet = AsSet(other);

            LinkedHashNode<T> current = first;
            while (current != null)
            {
                if (!otherSet.Contains(current.Value))
                {
                    elements.Remove(current.Value);
                    Unlink(current);
                }
                current = current.Next;
            }
        }

        
        
        
        
        public void ExceptWith(IEnumerable<T> other)
        {
            foreach (T item in other)
            {
                Remove(item);
            }
        }

        
        
        
        
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            foreach (T item in other)
            {
                if (elements.TryGetValue(item, out LinkedHashNode<T> node))
                {
                    elements.Remove(item);
                    Unlink(node);
                }
                else
                {
                    Add(item);
                }
            }
        }

        
        
        
        
        
        
        
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            int numberOfOthers = CountOthers(other, out int numberOfOthersPresent);

            
            return numberOfOthersPresent == numberOfOthers;
        }

        
        
        
        
        
        
        
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            int numberOfOthers = CountOthers(other, out int numberOfOthersPresent);

            
            return numberOfOthersPresent == numberOfOthers && numberOfOthers < Count;
        }

        
        
        
        
        
        
        
        public bool SetEquals(IEnumerable<T> other)
        {
            int numberOfOthers = CountOthers(other, out int numberOfOthersPresent);

            return numberOfOthers == Count && numberOfOthersPresent == Count;
        }

        
        
        
        
        
        
        
        public bool Add(T item)
        {
            if (item is null)
            {
                Trace.TraceWarning($"LinkedHashSet<{typeof(T).Name}> ignored null item in Add.");
                return false;
            }

            if (elements.ContainsKey(item))
            {
                return false;
            }

            LinkedHashNode<T> node = new LinkedHashNode<T>(item) { Previous = last };

            if (first == null)
            {
                first = node;
            }

            if (last != null)
            {
                last.Next = node;
            }

            last = node;

            elements.Add(item, node);

            return true;
        }

        public bool AddAfter(T item, LinkedHashNode<T> itemInPlace)
        {
            if (item is null)
            {
                Trace.TraceWarning($"LinkedHashSet<{typeof(T).Name}> ignored null item in AddAfter.");
                return false;
            }

            if (elements.ContainsKey(item))
            {
                return false;
            }

            LinkedHashNode<T> node = new LinkedHashNode<T>(item) { Previous = itemInPlace };

            if (itemInPlace.Next != null)
            {
                node.Next = itemInPlace.Next;
                itemInPlace.Next.Previous = node;
            }
            else
            {
                last = node;
            }

            itemInPlace.Next = node;

            elements.Add(item, node);

            return true;
        }

        public bool PushAfter(T item, LinkedHashNode<T> itemInPlace)
        {
            if (item is null)
            {
                Trace.TraceWarning($"LinkedHashSet<{typeof(T).Name}> ignored null item in PushAfter.");
                return false;
            }

            if (elements.ContainsKey(item))
            {
                return false;
            }

            LinkedHashNode<T> node = Last;
            Unlink(node);
            elements.Remove(node.Value);
            node.Value = item;
            node.Next = null;
            node.Previous = itemInPlace;

            if (itemInPlace.Next != null)
            {
                node.Next = itemInPlace.Next;
                itemInPlace.Next.Previous = node;
            }
            else
            {
                last = node;
            }

            itemInPlace.Next = node;

            elements.Add(item, node);

            return true;
        }

        public bool AddBefore(T item, LinkedHashNode<T> itemInPlace)
        {
            if (item is null)
            {
                Trace.TraceWarning($"LinkedHashSet<{typeof(T).Name}> ignored null item in AddBefore.");
                return false;
            }

            if (elements.ContainsKey(item))
            {
                return false;
            }

            LinkedHashNode<T> node = new LinkedHashNode<T>(item) { Next = itemInPlace };

            if (itemInPlace.Previous != null)
            {
                node.Previous = itemInPlace.Previous;
                itemInPlace.Previous.Next = node;
            }
            else
            {
                first = node;
            }

            itemInPlace.Previous = node;

            elements.Add(item, node);

            return true;
        }

        public bool PushBefore(T item, LinkedHashNode<T> itemInPlace)
        {
            if (item is null)
            {
                Trace.TraceWarning($"LinkedHashSet<{typeof(T).Name}> ignored null item in PushBefore.");
                return false;
            }

            if (elements.ContainsKey(item))
            {
                return false;
            }

            LinkedHashNode<T> node = Last;
            Unlink(node);
            elements.Remove(node.Value);
            node.Value = item;
            node.Previous = null;
            node.Next = itemInPlace;

            if (itemInPlace.Previous != null)
            {
                node.Previous = itemInPlace.Previous;
                itemInPlace.Previous.Next = node;
            }
            else
            {
                first = node;
            }

            itemInPlace.Previous = node;

            elements.Add(item, node);

            return true;
        }

        #endregion

        
        
        
        
        
        
        public Enumerator GetEnumerator()
        {
            return new Enumerator(this);
        }


        
        
        
        
        private int CountOthers(IEnumerable<T> items, out int numberOfOthersPresent)
        {
            numberOfOthersPresent = 0;
            int numberOfOthers = 0;

            foreach (T item in items)
            {
                numberOfOthers++;
                if (Contains(item))
                {
                    numberOfOthersPresent++;
                }
            }
            return numberOfOthers;
        }


        
        
        
        
        private static ISet<T> AsSet(IEnumerable<T> items)
        {
            return items as ISet<T> ?? new HashSet<T>(items);
        }


        
        
        
        
        
        private void Unlink(LinkedHashNode<T> node)
        {
            if (node.Previous != null)
            {
                node.Previous.Next = node.Next;
            }

            if (node.Next != null)
            {
                node.Next.Previous = node.Previous;
            }

            if (ReferenceEquals(node, first))
            {
                first = node.Next;
            }

            if (ReferenceEquals(node, last))
            {
                last = node.Previous;
            }
        }

        public class LinkedHashNode<TElement>
        {
            public TElement Value;
            public LinkedHashNode<TElement> Next;
            public LinkedHashNode<TElement> Previous;

            public LinkedHashNode(TElement value)
            {
                Value = value;
            }

            public override string ToString()
            {
                return Value.ToString();
            }
        }

        public struct Enumerator : IEnumerator<T>
        {
            private LinkedHashNode<T> _node;
            private T _current;

            internal Enumerator(LinkedHashSet<T> set)
            {
                _current = default(T);
                _node = set.first;
            }

            
            public bool MoveNext()
            {
                if (_node == null)
                {
                    return false;
                }

                _current = _node.Value;
                _node = _node.Next;
                return true;
            }

            
            public T Current => _current;

            
            object IEnumerator.Current => Current;

            
            void IEnumerator.Reset()
            {
                throw new NotSupportedException();
            }

            
            public void Dispose()
            {
            }
        }

        public void AddMin(T item)
        {
            LinkedHashNode<T> current = Last;
            while (current != null && item.CompareTo(current.Value) < 0)
            {
                current = current.Previous;
            }

            if (current == Last)
            {
                return;
            }

            if (current == null)
            {
                AddBefore(item, First);
            }
            else
            {
                AddAfter(item, current);
            }
        }

        public void PushMin(T item)
        {
            LinkedHashNode<T> current = Last;
            while (current != null && item.CompareTo(current.Value) < 0)
            {
                current = current.Previous;
            }

            if (current == Last)
            {
                return;
            }

            if (current == null)
            {
                PushBefore(item, First);
            }
            else
            {
                PushAfter(item, current);
            }
        }
    }
}