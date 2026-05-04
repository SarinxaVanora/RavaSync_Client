using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace RavaSync.Services.Optimisation.Reduction
{
    public class FastHashSet<T> : ICollection<T>, IEnumerable<T>, IEnumerable, IReadOnlyCollection<T>, ISet<T>
    {
        private const int MaxSlotsArraySize = int.MaxValue - 2;
        private const int InitialArraySize = 8;
        private const int InitialSlotsArraySize = 17;
        private const int NullIndex = 0;
        private const int BlankNextIndexIndicator = int.MaxValue;
        private const int HighBitNotSet = unchecked(0b0111_1111_1111_1111_1111_1111_1111_1111);
        private const int MarkNextIndexBitMask = unchecked((int)0b1000_0000_0000_0000_0000_0000_0000_0000);
        private const int MarkNextIndexBitMaskInverted = ~MarkNextIndexBitMask;
        private const int LargestPrimeLessThanMaxInt = 2147483629;
        
        private static readonly int[] bucketsSizeArray = { 11, 23, 47, 89, 173, 347, 691, 1367, 2741, 5471, 10_937, 19_841, 40_241, 84_463, 174_767,
			349_529, 699_053, 1_398_107, 2_796_221, 5_592_407, 11_184_829, 22_369_661, 44_739_259, 89_478_503, 17_8956_983, 35_7913_951, 715_827_947, 143_1655_777, LargestPrimeLessThanMaxInt};

        private static readonly int[] bucketsSizeArrayForCacheOptimization = { 3_371, 62_851, 701_819 };
        private const double LoadFactorConst = .75;
        private int currentIndexIntoBucketsSizeArray;
        private int bucketsModSize;

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
        private int incrementForEverySetModification;
#endif
        private int resizeBucketsCountThreshold;
        private int count;
        private int nextBlankIndex;
        private int firstBlankAtEndIndex;
        private readonly IEqualityComparer<T> comparer;
        private int[] buckets;
        private TNode[] slots;
#if !Exclude_No_Hash_Array_Implementation 
        private T[] noHashArray;
#endif

        internal enum FoundType
        {
            FoundFirstTime,
            FoundNotFirstTime,
            NotFound
        }

        internal struct TNode
        {         
            public int hashOrNextIndexForBlanks;
            public int nextIndex;
            public T item;

            public TNode(T elem, int nextIndex, int hash)
            {
                item = elem;

                this.nextIndex = nextIndex;

                hashOrNextIndexForBlanks = hash;
            }
        }
        public FastHashSet()
        {
            comparer = EqualityComparer<T>.Default;
            SetInitialCapacity(InitialArraySize);
        }
        public FastHashSet(IEnumerable<T> collection)
        {
            comparer = EqualityComparer<T>.Default;
            AddInitialEnumerable(collection);
        }

        public FastHashSet(IEqualityComparer<T> comparer)
        {
            this.comparer = comparer ?? EqualityComparer<T>.Default;
            SetInitialCapacity(InitialArraySize);
        }

        public FastHashSet(int capacity)
        {
            comparer = EqualityComparer<T>.Default;
            SetInitialCapacity(capacity);
        }
        
        public FastHashSet(IEnumerable<T> collection, IEqualityComparer<T> comparer)
        {
            this.comparer = comparer ?? EqualityComparer<T>.Default;
            AddInitialEnumerable(collection);
        }

        public FastHashSet(int capacity, IEqualityComparer<T> comparer)
        {
            this.comparer = comparer ?? EqualityComparer<T>.Default;
            SetInitialCapacity(capacity);
        }
        
#if false 
		public FastHashSet(IEnumerable<T> collection, bool areAllCollectionItemsDefinitelyUnique, int capacity, IEqualityComparer<T> comparer = null)
		{
			this.comparer = comparer ?? EqualityComparer<T>.Default;
			SetInitialCapacity(capacity);

			if (areAllCollectionItemsDefinitelyUnique)
			{
				
				AddInitialUniqueValuesEnumerable(collection);
			}
			else
			{
				AddInitialEnumerable(collection);
			}
		}
#endif

        private void AddInitialUniqueValuesEnumerable(IEnumerable<T> collection)
        {
            int itemsCount = 0;
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                nextBlankIndex = 1;
                foreach (T item in collection)
                {
                    int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                    int hashIndex = hash % bucketsModSize;

                    int index = buckets[hashIndex];
                    buckets[hashIndex] = nextBlankIndex;

                    ref TNode t = ref slots[nextBlankIndex];

                    t.hashOrNextIndexForBlanks = hash;
                    t.nextIndex = index;
                    t.item = item;

                    nextBlankIndex++;
                    itemsCount++;
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                foreach (T item in collection)
                {
                    noHashArray[itemsCount++] = item;
                }
            }
#endif
            count = itemsCount;
            firstBlankAtEndIndex = nextBlankIndex;
        }

        private void AddInitialEnumerableWithEnoughCapacity(IEnumerable<T> collection)
        {
            
            foreach (T item in collection)
            {
                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        goto Found; 
                    }

                    index = t.nextIndex;
                }

                ref TNode tBlank = ref slots[nextBlankIndex];

                tBlank.hashOrNextIndexForBlanks = hash;
                tBlank.nextIndex = buckets[hashIndex];
                tBlank.item = item;

                buckets[hashIndex] = nextBlankIndex;

                nextBlankIndex++;

#if !Exclude_Cache_Optimize_Resize
                count++;

                if (count >= resizeBucketsCountThreshold)
                {
                    ResizeBucketsArrayForward(GetNewBucketsArraySize());
                }
#endif
            Found:;
            }
            firstBlankAtEndIndex = nextBlankIndex;
#if Exclude_Cache_Optimize_Resize
			count = nextBlankIndex - 1;
#endif
        }

        private void AddInitialEnumerable(IEnumerable<T> collection)
        {
            FastHashSet<T> fhset = collection as FastHashSet<T>;
            if (fhset != null && Equals(fhset.Comparer, Comparer))
            {
                int count = fhset.Count;
                SetInitialCapacity(count);

#if !Exclude_No_Hash_Array_Implementation
                if (IsHashing)
                {
                    if (fhset.IsHashing)
                    {
#endif
                        

                        nextBlankIndex = 1;
                        int maxNodeIndex = fhset.slots.Length - 1;
                        if (fhset.firstBlankAtEndIndex <= maxNodeIndex)
                        {
                            maxNodeIndex = fhset.firstBlankAtEndIndex - 1;
                        }

                        for (int i = 1; i <= maxNodeIndex; i++)
                        {
                            ref TNode t2 = ref fhset.slots[i];
                            if (t2.nextIndex != BlankNextIndexIndicator)
                            {
                                int hash = t2.hashOrNextIndexForBlanks;
                                int hashIndex = hash % bucketsModSize;

                                ref TNode t = ref slots[nextBlankIndex];

                                t.hashOrNextIndexForBlanks = hash;
                                t.nextIndex = buckets[hashIndex];
                                t.item = t2.item;

                                buckets[hashIndex] = nextBlankIndex;

                                nextBlankIndex++;
                            }
                        }
                        this.count = count;
                        firstBlankAtEndIndex = nextBlankIndex;
#if !Exclude_No_Hash_Array_Implementation
                    }
                    else
                    {
                        

                        nextBlankIndex = 1;
                        for (int i = 0; i < fhset.count; i++)
                        {
                            ref T item = ref noHashArray[i];

                            int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                            int hashIndex = hash % bucketsModSize;

                            ref TNode t = ref slots[nextBlankIndex];

                            t.hashOrNextIndexForBlanks = hash;
                            t.nextIndex = buckets[hashIndex];
                            t.item = item;

                            buckets[hashIndex] = nextBlankIndex;

                            nextBlankIndex++;
                        }
                    }
                }
                else
                {
                    

                    AddInitialUniqueValuesEnumerable(collection);
                }
#endif
            }
            else
            {
                

                HashSet<T> hset = collection as HashSet<T>;
                if (hset != null && Equals(hset.Comparer, Comparer))
                {
                    
                    
                    

                    int usedCount = hset.Count;
                    SetInitialCapacity(usedCount);

                    AddInitialUniqueValuesEnumerable(collection);
                }
                else
                {
                    ICollection<T> coll = collection as ICollection<T>;
                    if (coll != null)
                    {
                        SetInitialCapacity(coll.Count);
#if !Exclude_No_Hash_Array_Implementation
                        if (IsHashing)
                        {
#endif
                            

                            AddInitialEnumerableWithEnoughCapacity(collection);

                            TrimExcess();
#if !Exclude_No_Hash_Array_Implementation
                        }
                        else
                        {
                            foreach (T item in collection)
                            {
                                Add(item);
                            }
                        }
#endif
                    }
                    else
                    {
                        SetInitialCapacity(InitialArraySize);

                        foreach (T item in collection)
                        {
                            Add(in item);
                        }
                    }
                }
            }
        }

        private void SetInitialCapacity(int capacity)
        {
#if !Exclude_No_Hash_Array_Implementation
            if (capacity > InitialArraySize)
            {
#endif
                
                InitHashing(capacity);
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                CreateNoHashArray(); 
            }
#endif
        }

#if !Exclude_No_Hash_Array_Implementation
        
        
        private void SwitchToHashing(int capacityIncrease = -1)
        {
            InitHashing(capacityIncrease);

            if (noHashArray != null)
            {
                
                for (int i = 0; i < count; i++)
                {
                    ref T item = ref noHashArray[i];

                    int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                    int hashIndex = hash % bucketsModSize;

                    ref TNode t = ref slots[nextBlankIndex];

                    t.hashOrNextIndexForBlanks = hash;
                    t.nextIndex = buckets[hashIndex];
                    t.item = item;

                    buckets[hashIndex] = nextBlankIndex;

                    nextBlankIndex++;
                }
                noHashArray = null; 
            }

            firstBlankAtEndIndex = nextBlankIndex;
        }
#endif

        private void InitHashing(int capacity = -1)
        {
            int newSlotsArraySize;
            int newBucketsArraySize;
            int newBucketsArrayModSize;

            bool setThresh = false;
            if (capacity == -1)
            {
                newSlotsArraySize = InitialSlotsArraySize;

                newBucketsArraySize = bucketsSizeArray[0];
                if (newBucketsArraySize < newSlotsArraySize)
                {
                    for (currentIndexIntoBucketsSizeArray = 1; currentIndexIntoBucketsSizeArray < bucketsSizeArray.Length; currentIndexIntoBucketsSizeArray++)
                    {
                        newBucketsArraySize = bucketsSizeArray[currentIndexIntoBucketsSizeArray];
                        if (newBucketsArraySize >= newSlotsArraySize)
                        {
                            break;
                        }
                    }
                }
                newBucketsArrayModSize = newBucketsArraySize;
            }
            else
            {
                newSlotsArraySize = capacity + 1; 

                newBucketsArraySize = FastHashSetUtil.GetEqualOrClosestHigherPrime((int)(newSlotsArraySize / LoadFactorConst));

#if !Exclude_Cache_Optimize_Resize
                if (newBucketsArraySize > bucketsSizeArrayForCacheOptimization[0])
                {
                    newBucketsArrayModSize = bucketsSizeArrayForCacheOptimization[0];
                    setThresh = true;
                }
                else
#endif
                {
                    newBucketsArrayModSize = newBucketsArraySize;
                }
            }

            if (newSlotsArraySize == 0)
            {
                
                
                throw new InvalidOperationException("Exceeded maximum number of items allowed for this container.");
            }

            slots = new TNode[newSlotsArraySize]; 
            buckets = new int[newBucketsArraySize]; 
            bucketsModSize = newBucketsArrayModSize;

            if (setThresh)
            {
                resizeBucketsCountThreshold = (int)(newBucketsArrayModSize * LoadFactorConst);
            }
            else
            {
                CalcUsedItemsLoadFactorThreshold();
            }

            nextBlankIndex = 1; 

            firstBlankAtEndIndex = nextBlankIndex;
        }

#if !Exclude_No_Hash_Array_Implementation
        private void CreateNoHashArray()
        {
            noHashArray = new T[InitialArraySize];
        }
#endif

        private void CalcUsedItemsLoadFactorThreshold()
        {
            if (buckets != null)
            {
                if (buckets.Length == bucketsModSize)
                {
                    resizeBucketsCountThreshold = slots.Length; 
                }
                else
                {
                    
                    resizeBucketsCountThreshold = (int)(bucketsModSize * LoadFactorConst);
                }
            }
        }
        
        bool ICollection<T>.IsReadOnly => false;

        public void CopyTo(T[] array, int arrayIndex)
        {
            CopyTo(array, arrayIndex, count);
        }

        public void CopyTo(T[] array)
        {
            CopyTo(array, 0, count);
        }

        public void CopyTo(T[] array, int arrayIndex, int count)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array), "Value cannot be null.");
            }

            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(arrayIndex), "Non negative number is required.");
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Non negative number is required.");
            }

            if (arrayIndex + count > array.Length)
            {
                throw new ArgumentException("Destination array is not long enough to copy all the items in the collection. Check array index and length.");
            }

            if (count == 0)
            {
                return;
            }

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int pastNodeIndex = slots.Length;
                if (firstBlankAtEndIndex < pastNodeIndex)
                {
                    pastNodeIndex = firstBlankAtEndIndex;
                }

                int cnt = 0;
                for (int i = 1; i < pastNodeIndex; i++)
                {
                    if (slots[i].nextIndex != BlankNextIndexIndicator)
                    {
                        array[arrayIndex++] = slots[i].item;
                        if (++cnt == count)
                        {
                            break;
                        }
                    }
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int cnt = this.count;
                if (cnt > count)
                {
                    cnt = count;
                }

                
                

                for (int i = 0; i < cnt; i++)
                {
                    array[arrayIndex++] = noHashArray[i];
                }
            }
#endif
        }

        public IEqualityComparer<T> Comparer =>
                comparer;
        public int Count => count;
        public double LoadFactor => LoadFactorConst;
        
        public int ExcessCapacity
        {
            get
            {
                int excessCapacity;
#if !Exclude_No_Hash_Array_Implementation
                if (IsHashing)
                {
#endif
                    excessCapacity = slots.Length - firstBlankAtEndIndex;
#if !Exclude_No_Hash_Array_Implementation
                }
                else
                {
                    excessCapacity = noHashArray.Length - count;
                }
#endif
                return excessCapacity;
            }
        }

        
        public int Capacity
        {
            get
            {
#if !Exclude_No_Hash_Array_Implementation
                if (IsHashing)
                {
#endif
                    return slots.Length - 1; 
#if !Exclude_No_Hash_Array_Implementation
                }
                else
                {
                    return noHashArray.Length;
                }
#endif
            }
        }

        public int NextCapacityIncreaseSize => GetNewSlotsArraySizeIncrease(out int oldSlotsArraySize);

        public int NextCapacityIncreaseAtCount => resizeBucketsCountThreshold;

        public bool IsHashing => noHashArray == null;
        public int EnsureCapacity(int capacity)
        {
            
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            int currentCapacity;

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                currentCapacity = slots.Length - count;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                currentCapacity = noHashArray.Length - count;
            }
#endif

            if (currentCapacity < capacity)
            {
                IncreaseCapacity(capacity - currentCapacity);
            }

            
            int calcedNewBucketsArraySize = (int)(slots.Length / LoadFactorConst) + 1;

            if (calcedNewBucketsArraySize < 0 && calcedNewBucketsArraySize > LargestPrimeLessThanMaxInt)
            {
                calcedNewBucketsArraySize = LargestPrimeLessThanMaxInt;
            }
            else
            {
                calcedNewBucketsArraySize = FastHashSetUtil.GetEqualOrClosestHigherPrime(calcedNewBucketsArraySize);
            }

            if (buckets.Length < calcedNewBucketsArraySize)
            {
                
                
                currentIndexIntoBucketsSizeArray = -1;

                ResizeBucketsArrayForward(calcedNewBucketsArraySize);
            }

            return slots.Length - count;
        }

        
        private bool CheckForModSizeIncrease()
        {
            if (bucketsModSize < buckets.Length)
            {
                
                int partLength = (int)(buckets.Length * .75);

                int size0 = bucketsSizeArrayForCacheOptimization[0];
                int size1 = bucketsSizeArrayForCacheOptimization[1];
                if (bucketsModSize == size0)
                {
                    if (size1 <= partLength)
                    {
                        bucketsModSize = size1;
                        return true;
                    }
                    else
                    {
                        bucketsModSize = buckets.Length;
                        return true;
                    }
                }
                else
                {
                    int size2 = bucketsSizeArrayForCacheOptimization[2];
                    if (bucketsModSize == size1)
                    {
                        if (size2 <= partLength)
                        {
                            bucketsModSize = size2;
                            return true;
                        }
                        else
                        {
                            bucketsModSize = buckets.Length;
                            return true;
                        }
                    }
                    else if (bucketsModSize == size2)
                    {
                        bucketsModSize = buckets.Length;
                        return true;
                    }
                }
            }
            return false;
        }

        private int GetNewSlotsArraySizeIncrease(out int oldArraySize)
        {
            if (slots != null)
            {
                oldArraySize = slots.Length;
            }
            else
            {
                oldArraySize = InitialSlotsArraySize; 
            }

            int increaseInSize;

            if (oldArraySize == 1)
            {
                increaseInSize = InitialSlotsArraySize - 1;
            }
            else
            {
                increaseInSize = oldArraySize - 1;
            }

            int maxIncreaseInSize = MaxSlotsArraySize - oldArraySize;

            if (increaseInSize > maxIncreaseInSize)
            {
                increaseInSize = maxIncreaseInSize;
            }
            return increaseInSize;
        }

        
        private int GetNewBucketsArraySize()
        {
            int newArraySize;

            if (currentIndexIntoBucketsSizeArray >= 0)
            {
                if (currentIndexIntoBucketsSizeArray + 1 < bucketsSizeArray.Length)
                {
                    newArraySize = bucketsSizeArray[currentIndexIntoBucketsSizeArray + 1];
                }
                else
                {
                    newArraySize = buckets.Length;
                }
            }
            else
            {
                
                
                newArraySize = buckets.Length;
                if (newArraySize < int.MaxValue / 2)
                {
                    newArraySize = FastHashSetUtil.GetEqualOrClosestHigherPrime(newArraySize + newArraySize);
                }
                else
                {
                    newArraySize = LargestPrimeLessThanMaxInt;
                }
            }

            return newArraySize;
        }

        
        private void IncreaseCapacity(int capacityIncrease = -1)
        {
            
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int newSlotsArraySizeIncrease;
                int oldSlotsArraySize;

                if (capacityIncrease == -1)
                {
                    newSlotsArraySizeIncrease = GetNewSlotsArraySizeIncrease(out oldSlotsArraySize);
                }
                else
                {
                    newSlotsArraySizeIncrease = capacityIncrease;
                    oldSlotsArraySize = slots.Length;
                }

                if (newSlotsArraySizeIncrease <= 0)
                {
                    throw new InvalidOperationException("Exceeded maximum number of items allowed for this container.");
                }

                int newSlotsArraySize = oldSlotsArraySize + newSlotsArraySizeIncrease;

                TNode[] newSlotsArray = new TNode[newSlotsArraySize];
                Array.Copy(slots, 0, newSlotsArray, 0, slots.Length); 
                slots = newSlotsArray;

#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                SwitchToHashing(capacityIncrease);
            }
#endif
        }

        private void ResizeBucketsArrayForward(int newBucketsArraySize)
        {
            if (newBucketsArraySize == buckets.Length)
            {
                
            }
            else
            {
                if (!CheckForModSizeIncrease()) 
                {
                    buckets = new int[newBucketsArraySize];
                    bucketsModSize = newBucketsArraySize;

                    if (currentIndexIntoBucketsSizeArray >= 0)
                    {
                        currentIndexIntoBucketsSizeArray++; 
                    }
                }
                else
                {
                    Array.Clear(buckets, 0, bucketsModSize);
                }

                CalcUsedItemsLoadFactorThreshold();

                int bucketsArrayLength = buckets.Length;

                int pastNodeIndex = slots.Length;
                if (firstBlankAtEndIndex < pastNodeIndex)
                {
                    pastNodeIndex = firstBlankAtEndIndex;
                }

                
                if (firstBlankAtEndIndex == count + 1)
                {
                    
                    for (int i = 1; i < pastNodeIndex; i++)
                    {
                        ref TNode t = ref slots[i];

                        int hashIndex = t.hashOrNextIndexForBlanks % bucketsArrayLength;
                        t.nextIndex = buckets[hashIndex];

                        buckets[hashIndex] = i;
                    }
                }
                else
                {
                    
                    for (int i = 1; i < pastNodeIndex; i++)
                    {
                        ref TNode t = ref slots[i];
                        if (t.nextIndex != BlankNextIndexIndicator) 
                        {
                            int hashIndex = t.hashOrNextIndexForBlanks % bucketsArrayLength;
                            t.nextIndex = buckets[hashIndex];

                            buckets[hashIndex] = i;
                        }
                    }
                }
            }
        }

        private void ResizeBucketsArrayForwardKeepMarks(int newBucketsArraySize)
        {
            if (newBucketsArraySize == buckets.Length)
            {
                
            }
            else
            {

                if (!CheckForModSizeIncrease()) 
                {
                    buckets = new int[newBucketsArraySize];
                    bucketsModSize = newBucketsArraySize;

                    if (currentIndexIntoBucketsSizeArray >= 0)
                    {
                        currentIndexIntoBucketsSizeArray++; 
                    }
                }

                CalcUsedItemsLoadFactorThreshold();

                int bucketsArrayLength = buckets.Length;

                int pastNodeIndex = slots.Length;
                if (firstBlankAtEndIndex < pastNodeIndex)
                {
                    pastNodeIndex = firstBlankAtEndIndex;
                }

                
                if (firstBlankAtEndIndex == count + 1)
                {
                    
                    for (int i = 1; i < pastNodeIndex; i++)
                    {
                        ref TNode t = ref slots[i];

                        int hashIndex = t.hashOrNextIndexForBlanks % bucketsArrayLength;
                        t.nextIndex = buckets[hashIndex] | (t.nextIndex & MarkNextIndexBitMask);

                        buckets[hashIndex] = i;
                    }
                }
                else
                {
                    
                    for (int i = 1; i < pastNodeIndex; i++)
                    {
                        ref TNode t = ref slots[i];
                        if (t.nextIndex != BlankNextIndexIndicator) 
                        {
                            int hashIndex = t.hashOrNextIndexForBlanks % bucketsArrayLength;
                            t.nextIndex = buckets[hashIndex] | (t.nextIndex & MarkNextIndexBitMask);

                            buckets[hashIndex] = i;
                        }
                    }
                }
            }
        }
        
        public void Clear()
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
#endif
            {
                firstBlankAtEndIndex = 1;
                nextBlankIndex = 1;
                Array.Clear(buckets, 0, buckets.Length);
            }

            count = 0;
        }
        public void TrimExcess()
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                if (slots.Length > firstBlankAtEndIndex && firstBlankAtEndIndex > 0)
                {
                    Array.Resize(ref slots, firstBlankAtEndIndex);
                    
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                if (noHashArray != null && noHashArray.Length > count && count > 0)
                {
                    Array.Resize(ref noHashArray, count);
                }
            }
#endif
        }

        void ICollection<T>.Add(T item)
        {
            Add(in item);
        }

        public bool Add(in T item)
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif

                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        return false; 
                    }

                    index = t.nextIndex;
                }

                if (nextBlankIndex >= slots.Length)
                {
                    
                    IncreaseCapacity();
                }

                int firstIndex = buckets[hashIndex];
                buckets[hashIndex] = nextBlankIndex;

                ref TNode tBlank = ref slots[nextBlankIndex];
                if (nextBlankIndex >= firstBlankAtEndIndex)
                {
                    
                    nextBlankIndex = ++firstBlankAtEndIndex;
                }
                else
                {
                    
                    nextBlankIndex = tBlank.hashOrNextIndexForBlanks;
                }

                tBlank.hashOrNextIndexForBlanks = hash;
                tBlank.nextIndex = firstIndex;
                tBlank.item = item;

                count++;

                if (count >= resizeBucketsCountThreshold)
                {
                    ResizeBucketsArrayForward(GetNewBucketsArraySize());
                }

                return true;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        return false;
                    }
                }

                if (i == noHashArray.Length)
                {
                    SwitchToHashing();

                    int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                    int hashIndex = hash % bucketsModSize;

                    ref TNode tBlank = ref slots[nextBlankIndex];

                    tBlank.hashOrNextIndexForBlanks = hash;
                    tBlank.nextIndex = buckets[hashIndex];
                    tBlank.item = item;

                    buckets[hashIndex] = nextBlankIndex;

                    nextBlankIndex = ++firstBlankAtEndIndex;

                    count++;

                    return true;
                }
                else
                {
                    
                    noHashArray[i] = item;
                    count++;
                    return true;
                }
            }
#endif
        }

        public bool Add(T item)
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif

                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        return false; 
                    }

                    index = t.nextIndex;
                }

                if (nextBlankIndex >= slots.Length)
                {
                    
                    IncreaseCapacity();
                }

                int firstIndex = buckets[hashIndex];
                buckets[hashIndex] = nextBlankIndex;

                ref TNode tBlank = ref slots[nextBlankIndex];
                if (nextBlankIndex >= firstBlankAtEndIndex)
                {
                    
                    nextBlankIndex = ++firstBlankAtEndIndex;
                }
                else
                {
                    
                    nextBlankIndex = tBlank.hashOrNextIndexForBlanks;
                }

                tBlank.hashOrNextIndexForBlanks = hash;
                tBlank.nextIndex = firstIndex;
                tBlank.item = item;

                count++;

                if (count >= resizeBucketsCountThreshold)
                {
                    ResizeBucketsArrayForward(GetNewBucketsArraySize());
                }

                return true;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        return false;
                    }
                }

                if (i == noHashArray.Length)
                {
                    SwitchToHashing();

                    int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                    int hashIndex = hash % bucketsModSize;

                    ref TNode tBlank = ref slots[nextBlankIndex];

                    tBlank.hashOrNextIndexForBlanks = hash;
                    tBlank.nextIndex = buckets[hashIndex];
                    tBlank.item = item;

                    buckets[hashIndex] = nextBlankIndex;

                    nextBlankIndex = ++firstBlankAtEndIndex;

                    count++;

                    return true;
                }
                else
                {
                    
                    noHashArray[i] = item;
                    count++;
                    return true;
                }
            }
#endif
        }

        private int AddToHashSetIfNotFound(in T item, int hash, out bool isFound)
        {
            

            int hashIndex = hash % bucketsModSize;

            for (int index = buckets[hashIndex]; index != NullIndex;)
            {
                ref TNode t = ref slots[index];

                if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                {
                    isFound = true;
                    return index; 
                }

                index = t.nextIndex;
            }

            if (nextBlankIndex >= slots.Length)
            {
                
                IncreaseCapacity();
                ResizeBucketsArrayForward(GetNewBucketsArraySize());

                
                hashIndex = hash % bucketsModSize;
            }

            int firstIndex = buckets[hashIndex];
            buckets[hashIndex] = nextBlankIndex;

            int addedNodeIndex = nextBlankIndex;
            ref TNode tBlank = ref slots[nextBlankIndex];
            if (nextBlankIndex >= firstBlankAtEndIndex)
            {
                
                nextBlankIndex = ++firstBlankAtEndIndex;
            }
            else
            {
                
                nextBlankIndex = tBlank.hashOrNextIndexForBlanks;
            }

            tBlank.hashOrNextIndexForBlanks = hash;
            tBlank.nextIndex = firstIndex;
            tBlank.item = item;

            count++;

            isFound = false;
            return addedNodeIndex; 
        }

        private int AddToHashSetIfNotFoundAndMark(in T item, int hash)
        {
            

            int hashIndex = hash % bucketsModSize;

            for (int index = buckets[hashIndex]; index != NullIndex;)
            {
                ref TNode t = ref slots[index];

                if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                {
                    return NullIndex; 
                }

                index = t.nextIndex & MarkNextIndexBitMaskInverted;
            }

            if (nextBlankIndex >= slots.Length)
            {
                
                IncreaseCapacity();
                ResizeBucketsArrayForwardKeepMarks(GetNewBucketsArraySize());

                
                hashIndex = hash % bucketsModSize;
            }

            int firstIndex = buckets[hashIndex];
            buckets[hashIndex] = nextBlankIndex;

            int addedNodeIndex = nextBlankIndex;
            ref TNode tBlank = ref slots[nextBlankIndex];
            if (nextBlankIndex >= firstBlankAtEndIndex)
            {
                
                nextBlankIndex = ++firstBlankAtEndIndex;
            }
            else
            {
                
                nextBlankIndex = tBlank.hashOrNextIndexForBlanks;
            }

            tBlank.hashOrNextIndexForBlanks = hash;
            tBlank.nextIndex = firstIndex | MarkNextIndexBitMask;
            tBlank.item = item;

            count++;

            return addedNodeIndex; 
        }

        public bool Contains(in T item)
        {
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        return true; 
                    }

                    index = t.nextIndex;
                }
                return false;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        return true; 
                    }
                }
                return false;
            }
#endif
        }

        public bool Contains(T item)
        {
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        return true; 
                    }

                    index = t.nextIndex;
                }
                return false;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        return true; 
                    }
                }
                return false;
            }
#endif
        }
        public bool Remove(T item)
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                int priorIndex = NullIndex;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        

                        if (priorIndex == NullIndex)
                        {
                            buckets[hashIndex] = t.nextIndex;
                        }
                        else
                        {
                            slots[priorIndex].nextIndex = t.nextIndex;
                        }

                        
                        if (index == firstBlankAtEndIndex - 1)
                        {
                            if (nextBlankIndex == firstBlankAtEndIndex)
                            {
                                nextBlankIndex--;
                            }
                            firstBlankAtEndIndex--;
                        }
                        else
                        {
                            t.hashOrNextIndexForBlanks = nextBlankIndex;
                            nextBlankIndex = index;
                        }

                        t.nextIndex = BlankNextIndexIndicator;

                        count--;

                        return true;
                    }

                    priorIndex = index;

                    index = t.nextIndex;
                }
                return false; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        
                        for (int j = i + 1; j < count; j++, i++)
                        {
                            noHashArray[i] = noHashArray[j];
                        }
                        count--;
                        return true;
                    }
                }
                return false;
            }
#endif
        }

        public bool RemoveIf(in T item, Predicate<T> removeIfPredIsTrue)
        {
            if (removeIfPredIsTrue == null)
            {
                throw new ArgumentNullException(nameof(removeIfPredIsTrue), "Value cannot be null.");
            }

            

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int hash = (comparer.GetHashCode(item) & HighBitNotSet);
                int hashIndex = hash % bucketsModSize;

                int priorIndex = NullIndex;

                for (int index = buckets[hashIndex]; index != NullIndex;)
                {
                    ref TNode t = ref slots[index];

                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        if (removeIfPredIsTrue.Invoke(t.item))
                        {
                            

                            if (priorIndex == NullIndex)
                            {
                                buckets[hashIndex] = t.nextIndex;
                            }
                            else
                            {
                                slots[priorIndex].nextIndex = t.nextIndex;
                            }

                            
                            if (index == firstBlankAtEndIndex - 1)
                            {
                                if (nextBlankIndex == firstBlankAtEndIndex)
                                {
                                    nextBlankIndex--;
                                }
                                firstBlankAtEndIndex--;
                            }
                            else
                            {
                                t.hashOrNextIndexForBlanks = nextBlankIndex;
                                nextBlankIndex = index;
                            }

                            t.nextIndex = BlankNextIndexIndicator;

                            count--;

                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }

                    priorIndex = index;

                    index = t.nextIndex;
                }
                return false; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        if (removeIfPredIsTrue.Invoke(noHashArray[i]))
                        {
                            
                            for (int j = i + 1; j < count; j++, i++)
                            {
                                noHashArray[i] = noHashArray[j];
                            }
                            count--;
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
                return false;
            }
#endif
        }

        public ref T FindOrAdd(in T item, out bool isFound)
        {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            isFound = false;
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int addedOrFoundItemIndex = AddToHashSetIfNotFound(in item, (comparer.GetHashCode(item) & HighBitNotSet), out isFound);
                return ref slots[addedOrFoundItemIndex].item;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        isFound = true;
                        return ref noHashArray[i];
                    }
                }

                if (i == noHashArray.Length)
                {
                    SwitchToHashing();
                    return ref FindOrAdd(in item, out isFound);
                }
                else
                {
                    
                    noHashArray[i] = item;
                    count++;
                    return ref noHashArray[i];
                }
            }
#endif
        }

        public ref T Find(in T item, out bool isFound)
        {
            isFound = false;
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                FindInSlotsArray(item, out int foundNodeIndex, out int priorNodeIndex, out int bucketsIndex);
                if (foundNodeIndex != NullIndex)
                {
                    isFound = true;
                }

                return ref slots[foundNodeIndex].item;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        isFound = true;
                        return ref noHashArray[i];
                    }
                }

                
                return ref noHashArray[0];
            }
#endif
        }

        public ref T FindAndRemoveIf(in T item, Predicate<T> removeIfPredIsTrue, out bool isFound, out bool isRemoved)
        {
            if (removeIfPredIsTrue == null)
            {
                throw new ArgumentNullException(nameof(removeIfPredIsTrue), "Value cannot be null.");
            }

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            isFound = false;
            isRemoved = false;

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                FindInSlotsArray(item, out int foundNodeIndex, out int priorNodeIndex, out int bucketsIndex);
                if (foundNodeIndex != NullIndex)
                {
                    isFound = true;
                    ref TNode t = ref slots[foundNodeIndex];
                    if (removeIfPredIsTrue.Invoke(t.item))
                    {
                        if (priorNodeIndex == NullIndex)
                        {
                            buckets[bucketsIndex] = t.nextIndex;
                        }
                        else
                        {
                            slots[priorNodeIndex].nextIndex = t.nextIndex;
                        }

                        
                        if (foundNodeIndex == firstBlankAtEndIndex - 1)
                        {
                            if (nextBlankIndex == firstBlankAtEndIndex)
                            {
                                nextBlankIndex--;
                            }
                            firstBlankAtEndIndex--;
                        }
                        else
                        {
                            t.hashOrNextIndexForBlanks = nextBlankIndex;
                            nextBlankIndex = foundNodeIndex;
                        }

                        t.nextIndex = BlankNextIndexIndicator;

                        count--;

                        isRemoved = true;

                        foundNodeIndex = NullIndex;
                    }
                }

                return ref slots[foundNodeIndex].item;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(item, noHashArray[i]))
                    {
                        isFound = true;
                        if (removeIfPredIsTrue.Invoke(noHashArray[i]))
                        {
                            
                            for (int j = i + 1; j < count; j++, i++)
                            {
                                noHashArray[i] = noHashArray[j];
                            }
                            count--;

                            isRemoved = true;
                            return ref noHashArray[0];
                        }
                        else
                        {
                            return ref noHashArray[i];
                        }
                    }
                }

                
                return ref noHashArray[0];
            }
#endif
        }

        private void FindInSlotsArray(in T item, out int foundNodeIndex, out int priorNodeIndex, out int bucketsIndex)
        {
            foundNodeIndex = NullIndex;
            priorNodeIndex = NullIndex;

            int hash = (comparer.GetHashCode(item) & HighBitNotSet);
            int hashIndex = hash % bucketsModSize;

            bucketsIndex = hashIndex;

            int priorIndex = NullIndex;

            for (int index = buckets[hashIndex]; index != NullIndex;)
            {
                ref TNode t = ref slots[index];

                if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                {
                    foundNodeIndex = index;
                    priorNodeIndex = priorIndex;
                    return; 
                }

                priorIndex = index;

                index = t.nextIndex;
            }
            return; 
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindInSlotsArray(in T item, int hash)
        {
            int hashIndex = hash % bucketsModSize;

            for (int index = buckets[hashIndex]; index != NullIndex;)
            {
                ref TNode t = ref slots[index];

                if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                {
                    return true; 
                }

                index = t.nextIndex;
            }
            return false;
        }

#if !Exclude_No_Hash_Array_Implementation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindInNoHashArray(in T item)
        {
            for (int i = 0; i < count; i++)
            {
                if (comparer.Equals(item, noHashArray[i]))
                {
                    return true; 
                }
            }
            return false;
        }
#endif

        private void UnmarkAllNextIndexValues(int maxNodeIndex)
        {
            
            for (int i = 1; i <= maxNodeIndex; i++)
            {
                slots[i].nextIndex &= MarkNextIndexBitMaskInverted;
            }
        }

        private void UnmarkAllNextIndexValuesAndRemoveAnyMarkedOrUnmarked(bool removeMarked)
        {
            int index;
            int nextIndex;
            int priorIndex;
            int lastNonBlankIndex = firstBlankAtEndIndex - 1;
            for (int i = 0; i < buckets.Length; i++)
            {
                priorIndex = NullIndex; 
                index = buckets[i];

                while (index != NullIndex)
                {
                    ref TNode t = ref slots[index];
                    nextIndex = t.nextIndex;
                    bool isMarked = (nextIndex & MarkNextIndexBitMask) != 0;
                    if (isMarked)
                    {
                        
                        nextIndex &= MarkNextIndexBitMaskInverted;
                        t.nextIndex = nextIndex;
                    }

                    if (removeMarked == isMarked)
                    {
                        

                        count--;

                        
                        if (index == lastNonBlankIndex)
                        {
                            
                            lastNonBlankIndex--;
                            if (nextBlankIndex == firstBlankAtEndIndex)
                            {
                                nextBlankIndex--;
                            }
                            firstBlankAtEndIndex--;
                        }
                        else
                        {
                            

                            t.nextIndex = BlankNextIndexIndicator;

                            t.hashOrNextIndexForBlanks = nextBlankIndex;
                            nextBlankIndex = index;
                        }

                        if (priorIndex == NullIndex)
                        {
                            buckets[i] = nextIndex;
                        }
                        else
                        {
                            slots[priorIndex].nextIndex = nextIndex;
                        }

                        
                    }
                    else
                    {
                        priorIndex = index; 
                    }

                    index = nextIndex;
                }
            }
        }

        private FoundType FindInSlotsArrayAndMark(in T item, out int foundNodeIndex)
        {
            int hash = (comparer.GetHashCode(item) & HighBitNotSet);
            int hashIndex = hash % bucketsModSize;

            int index = buckets[hashIndex];

            if (index == NullIndex)
            {
                foundNodeIndex = NullIndex;
                return FoundType.NotFound;
            }
            else
            {
                

                int nextIndex;
                while (true)
                {
                    ref TNode t = ref slots[index];
                    nextIndex = t.nextIndex;

                    
                    if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                    {
                        foundNodeIndex = index;
                        if ((nextIndex & MarkNextIndexBitMask) == 0)
                        {
                            
                            t.nextIndex |= MarkNextIndexBitMask;

                            return FoundType.FoundFirstTime;
                        }
                        return FoundType.FoundNotFirstTime;
                    }

                    nextIndex &= MarkNextIndexBitMaskInverted;
                    if (nextIndex == NullIndex)
                    {
                        foundNodeIndex = NullIndex;
                        return FoundType.NotFound; 
                    }
                    else
                    {
                        index = nextIndex;
                    }
                }
            }
        }
        
        public List<ChainLevelAndCount> GetChainLevelsCounts(out double avgNodeVisitsPerChain)
        {
            Dictionary<int, int> itemsInChainToCountDict = new Dictionary<int, int>();

            
            int chainCount = 0;
            if (buckets != null)
            {
                for (int i = 0; i < buckets.Length; i++)
                {
                    int index = buckets[i];
                    if (index != NullIndex)
                    {
                        chainCount++;
                        int itemsInChain = 1;

                        while (slots[index].nextIndex != NullIndex)
                        {
                            index = slots[index].nextIndex;
                            itemsInChain++;
                        }

                        itemsInChainToCountDict.TryGetValue(itemsInChain, out int cnt);
                        cnt++;
                        itemsInChainToCountDict[itemsInChain] = cnt;
                    }
                }
            }

            double totalAvgNodeVisitsIfVisitingAllChains = 0;
            List<ChainLevelAndCount> lst = new List<ChainLevelAndCount>(itemsInChainToCountDict.Count);
            foreach (KeyValuePair<int, int> keyVal in itemsInChainToCountDict)
            {
                lst.Add(new ChainLevelAndCount(keyVal.Key, keyVal.Value));
                if (keyVal.Key == 1)
                {
                    totalAvgNodeVisitsIfVisitingAllChains += keyVal.Value;
                }
                else
                {
                    totalAvgNodeVisitsIfVisitingAllChains += keyVal.Value * (keyVal.Key + 1.0) / 2.0;
                }
            }

            if (chainCount == 0)
            {
                avgNodeVisitsPerChain = 0;
            }
            else
            {
                avgNodeVisitsPerChain = totalAvgNodeVisitsIfVisitingAllChains / chainCount;
            }

            lst.Sort();

            return lst;
        }

        public void ReorderChainedNodesToBeAdjacent()
        {
            if (slots != null)
            {
                TNode[] newSlotsArray = new TNode[slots.Length];

                
                int index;
                int newIndex = 1;
                for (int i = 0; i < buckets.Length; i++)
                {
                    index = buckets[i];
                    if (index != NullIndex)
                    {
                        buckets[i] = newIndex;
                        while (true)
                        {
                            ref TNode t = ref slots[index];
                            ref TNode tNew = ref newSlotsArray[newIndex];
                            index = t.nextIndex;
                            newIndex++;

                            
                            tNew.hashOrNextIndexForBlanks = t.hashOrNextIndexForBlanks;
                            tNew.item = t.item;
                            if (index == NullIndex)
                            {
                                tNew.nextIndex = NullIndex;
                                break;
                            }
                            tNew.nextIndex = newIndex;
                        }
                    }
                }

                newIndex++;
                nextBlankIndex = newIndex;
                firstBlankAtEndIndex = newIndex;
                slots = newSlotsArray;
            }
        }

        public bool TryGetValue(T equalValue, out T actualValue)
        {
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                FindInSlotsArray(equalValue, out int foundNodeIndex, out int priorNodeIndex, out int bucketsIndex);
                if (foundNodeIndex > 0)
                {
                    actualValue = slots[foundNodeIndex].item;
                    return true;
                }

                actualValue = default;
                return false;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = 0; i < count; i++)
                {
                    if (comparer.Equals(equalValue, noHashArray[i]))
                    {
                        actualValue = noHashArray[i];
                        return true;
                    }
                }

                actualValue = default;
                return false;
            }
#endif
        }

        public void UnionWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            if (other == this)
            {
                return;
            }
#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                foreach (T item in other)
                {
                    AddToHashSetIfNotFound(in item, (comparer.GetHashCode(item) & HighBitNotSet), out bool isFound);
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;

                foreach (T item in other)
                {
                    
                    
                    for (i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            goto found; 
                        }
                    }

                    
                    if (i == noHashArray.Length)
                    {
                        SwitchToHashing();
                        AddToHashSetIfNotFound(in item, (comparer.GetHashCode(item) & HighBitNotSet), out bool isFound);
                    }
                    else
                    {
                        
                        noHashArray[i] = item;
                        count++;
                    }

                found:;
                }
            }
#endif
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif
            if (other == this)
            {
                Clear();
            }
            else
            {
                foreach (T item in other)
                {
                    Remove(item);
                }
            }
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return;
            }

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int foundItemCount = 0; 
                foreach (T item in other)
                {
                    FoundType foundType = FindInSlotsArrayAndMark(in item, out int foundIndex);
                    if (foundType == FoundType.FoundFirstTime)
                    {
                        foundItemCount++;

                        if (foundItemCount == count)
                        {
                            break;
                        }
                    }
                }

                if (foundItemCount == 0)
                {
                    Clear();
                }
                else
                {
                    UnmarkAllNextIndexValuesAndRemoveAnyMarkedOrUnmarked(false);
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                uint foundItemBits = 0;

                int i;

                int foundItemCount = 0; 
                foreach (T item in other)
                {
                    for (i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            uint mask = (1u << i);
                            if ((foundItemBits & mask) == 0)
                            {
                                foundItemBits |= mask;
                                foundItemCount++;
                            }
                            goto found; 
                        }
                    }

                found:
                    if (foundItemCount == count)
                    {
                        
                        return;
                    }
                }

                if (foundItemCount == 0)
                {
                    count = 0; 
                }
                else
                {
                    
                    
                    for (i = count - 1; i >= 0; i--)
                    {
                        uint mask = (1u << i);
                        if ((foundItemBits & mask) == 0)
                        {
                            if (i < count - 1)
                            {
                                
                                

                                int j = i + 1; 

                                i--;
                                while (i >= 0)
                                {
                                    uint mask2 = (1u << i);
                                    if ((foundItemBits & mask2) != 0)
                                    {
                                        break;
                                    }
                                    i--;
                                }
                                i++;

                                int k = i;
                                for (; j < count; j++, k++)
                                {
                                    noHashArray[k] = noHashArray[j];
                                }
                            }

                            count--;
                        }
                    }
                }
            }
#endif
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return false;
            }

            ICollection<T> collection = other as ICollection<T>;
            if (collection != null)
            {
                if (count == 0 && collection.Count > 0)
                {
                    return true; 
                }

                if (count >= collection.Count)
                {
                    return false;
                }
            }
            else
            {
                if (count == 0)
                {
                    foreach (T item in other)
                    {
                        return true;
                    }
                    return false;
                }
            }

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int foundItemCount = 0; 
                int maxFoundIndex = 0;
                bool notFoundAtLeastOne = false;
                foreach (T item in other)
                {
                    FoundType foundType = FindInSlotsArrayAndMark(in item, out int foundIndex);
                    if (foundType == FoundType.FoundFirstTime)
                    {
                        foundItemCount++;
                        if (maxFoundIndex < foundIndex)
                        {
                            maxFoundIndex = foundIndex;
                        }
                    }
                    else if (foundType == FoundType.NotFound)
                    {
                        notFoundAtLeastOne = true;
                    }

                    if (notFoundAtLeastOne && foundItemCount == count)
                    {
                        
                        break; 
                    }
                }

                UnmarkAllNextIndexValues(maxFoundIndex);

                return notFoundAtLeastOne && foundItemCount == count; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                uint foundItemBits = 0;

                int foundItemCount = 0; 
                bool notFoundAtLeastOne = false;
                foreach (T item in other)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            uint mask = (1u << i);
                            if ((foundItemBits & mask) == 0)
                            {
                                foundItemBits |= mask;
                                foundItemCount++;
                            }
                            goto found; 
                        }
                    }

                    
                    notFoundAtLeastOne = true;

                found:
                    if (notFoundAtLeastOne && foundItemCount == count)
                    {
                        
                        return true;
                    }
                }

                return false;
            }
#endif
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return true;
            }

            if (count == 0)
            {
                return true; 
            }

            ICollection<T> collection = other as ICollection<T>;
            if (collection != null)
            {
                if (count > collection.Count)
                {
                    return false;
                }
            }

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int foundItemCount = 0; 
                int maxFoundIndex = 0;
                foreach (T item in other)
                {
                    FoundType foundType = FindInSlotsArrayAndMark(in item, out int foundIndex);
                    if (foundType == FoundType.FoundFirstTime)
                    {
                        foundItemCount++;
                        if (maxFoundIndex < foundIndex)
                        {
                            maxFoundIndex = foundIndex;
                        }

                        if (foundItemCount == count)
                        {
                            break;
                        }
                    }
                }

                UnmarkAllNextIndexValues(maxFoundIndex);

                return foundItemCount == count; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                uint foundItemBits = 0;

                int foundItemCount = 0; 
                foreach (T item in other)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            uint mask = (1u << i);
                            if ((foundItemBits & mask) == 0)
                            {
                                foundItemBits |= mask;
                                foundItemCount++;
                            }
                            goto found; 
                        }
                    }

                found:
                    if (foundItemCount == count)
                    {
                        break;
                    }
                }

                return foundItemCount == count; 
            }
#endif
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return false;
            }

            if (count == 0)
            {
                return false; 
            }

            ICollection<T> collection = other as ICollection<T>;
            if (collection != null)
            {
                if (collection.Count == 0)
                {
                    return true; 
                }
            }
            else
            {
                foreach (T item in other)
                {
                    goto someItemsInOther;
                }
                return true;
            }

        someItemsInOther:

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int foundItemCount = 0; 
                int maxFoundIndex = 0;
                foreach (T item in other)
                {
                    FoundType foundType = FindInSlotsArrayAndMark(in item, out int foundIndex);
                    if (foundType == FoundType.FoundFirstTime)
                    {
                        foundItemCount++;
                        if (maxFoundIndex < foundIndex)
                        {
                            maxFoundIndex = foundIndex;
                        }

                        if (foundItemCount == count)
                        {
                            break;
                        }
                    }
                    else if (foundType == FoundType.NotFound)
                    {
                        
                        UnmarkAllNextIndexValues(maxFoundIndex);
                        return false;
                    }
                }

                UnmarkAllNextIndexValues(maxFoundIndex);

                return foundItemCount < count; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                uint foundItemBits = 0;

                int foundItemCount = 0; 
                foreach (T item in other)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            uint mask = (1u << i);
                            if ((foundItemBits & mask) == 0)
                            {
                                foundItemBits |= mask;
                                foundItemCount++;
                            }
                            goto found; 
                        }
                    }

                    
                    return false;

                found:
                    if (foundItemCount == count)
                    {
                        break;
                    }
                }

                return foundItemCount < count; 
            }
#endif
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return true;
            }

            ICollection<T> collection = other as ICollection<T>;
            if (collection != null)
            {
                if (collection.Count == 0)
                {
                    return true; 
                }
            }
            else
            {
                foreach (T item in other)
                {
                    goto someItemsInOther;
                }
                return true;
            }

        someItemsInOther:

            if (count == 0)
            {
                return false; 
            }

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                foreach (T item in other)
                {
                    if (!FindInSlotsArray(in item, (comparer.GetHashCode(item) & HighBitNotSet)))
                    {
                        return false;
                    }
                }

                return true; 
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;

                foreach (T item in other)
                {
                    for (i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            goto found; 
                        }
                    }

                    
                    return false;

                found:;

                }

                return true; 
            }
#endif
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return count > 0; 
            }

            foreach (T item in other)
            {
                if (Contains(in item))
                {
                    return true;
                }
            }
            return false;
        }

        
        public bool SetEquals(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                return true;
            }

            

            ICollection c = other as ICollection;

            if (c != null)
            {
                if (c.Count < count)
                {
                    return false;
                }

                HashSet<T> hset = other as HashSet<T>;
                if (hset != null && Equals(hset.Comparer, Comparer))
                {
                    if (hset.Count != count)
                    {
                        return false;
                    }

                    foreach (T item in other)
                    {
                        if (!Contains(in item))
                        {
                            return false;
                        }
                    }
                    return true;
                }

                FastHashSet<T> fhset = other as FastHashSet<T>;
                if (fhset != null && Equals(fhset.Comparer, Comparer))
                {
                    if (fhset.Count != count)
                    {
                        return false;
                    }

#if !Exclude_No_Hash_Array_Implementation
                    if (IsHashing)
                    {
#endif
                        int pastNodeIndex = slots.Length;
                        if (firstBlankAtEndIndex < pastNodeIndex)
                        {
                            pastNodeIndex = firstBlankAtEndIndex;
                        }

#if !Exclude_No_Hash_Array_Implementation
                        if (fhset.IsHashing)
                        {
#endif
                            for (int i = 1; i < pastNodeIndex; i++)
                            {
                                
                                
                                if (slots[i].nextIndex != BlankNextIndexIndicator) 
                                {
                                    if (!fhset.FindInSlotsArray(in slots[i].item, slots[i].hashOrNextIndexForBlanks))
                                    {
                                        return false;
                                    }
                                }
                            }
#if !Exclude_No_Hash_Array_Implementation
                        }
                        else
                        {
                            for (int i = 1; i < pastNodeIndex; i++)
                            {
                                if (slots[i].nextIndex != BlankNextIndexIndicator) 
                                {
                                    if (!fhset.FindInNoHashArray(in slots[i].item))
                                    {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        foreach (T item in other)
                        {
                            if (!FindInNoHashArray(in item))
                            {
                                return false;
                            }
                        }
                    }
                    return true;
#endif
                }

            }


#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int foundItemCount = 0; 
                int maxFoundIndex = 0;
                foreach (T item in other)
                {
                    FoundType foundType = FindInSlotsArrayAndMark(in item, out int foundIndex);
                    if (foundType == FoundType.FoundFirstTime)
                    {
                        foundItemCount++;
                        if (maxFoundIndex < foundIndex)
                        {
                            maxFoundIndex = foundIndex;
                        }
                    }
                    else if (foundType == FoundType.NotFound)
                    {
                        UnmarkAllNextIndexValues(maxFoundIndex);
                        return false;
                    }
                }

                UnmarkAllNextIndexValues(maxFoundIndex);

                return foundItemCount == count;
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                uint foundItemBits = 0;

                int foundItemCount = 0; 
                foreach (T item in other)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (comparer.Equals(item, noHashArray[i]))
                        {
                            uint mask = (1u << i);
                            if ((foundItemBits & mask) == 0)
                            {
                                foundItemBits |= mask;
                                foundItemCount++;
                            }
                            goto found; 
                        }
                    }
                    
                    return false;
                found:;
                }

                return foundItemCount == count;
            }
#endif
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other), "Value cannot be null.");
            }

            if (other == this)
            {
                Clear();
            }

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

#if !Exclude_No_Hash_Array_Implementation
            if (!IsHashing)
            {
                
                SwitchToHashing();
            }
#endif

            
            int addedNodeIndex;
            int maxAddedNodeIndex = NullIndex;
            foreach (T item in other)
            {
                addedNodeIndex = AddToHashSetIfNotFoundAndMark(in item, (comparer.GetHashCode(item) & HighBitNotSet));
                if (addedNodeIndex > maxAddedNodeIndex)
                {
                    maxAddedNodeIndex = addedNodeIndex;
                }
            }

            foreach (T item in other)
            {
                RemoveIfNotMarked(in item);
            }

            UnmarkAllNextIndexValues(maxAddedNodeIndex);
        }

        private void RemoveIfNotMarked(in T item)
        {
            
            int hash = (comparer.GetHashCode(item) & HighBitNotSet);
            int hashIndex = hash % bucketsModSize;

            int priorIndex = NullIndex;

            for (int index = buckets[hashIndex]; index != NullIndex;)
            {
                ref TNode t = ref slots[index];

                if (t.hashOrNextIndexForBlanks == hash && comparer.Equals(t.item, item))
                {
                    
                    if ((t.nextIndex & MarkNextIndexBitMask) == 0)
                    {
                        if (priorIndex == NullIndex)
                        {
                            buckets[hashIndex] = t.nextIndex;
                        }
                        else
                        {
                            
                            
                            slots[priorIndex].nextIndex = t.nextIndex | (slots[priorIndex].nextIndex & MarkNextIndexBitMask);
                        }

                        
                        if (index == firstBlankAtEndIndex - 1)
                        {
                            if (nextBlankIndex == firstBlankAtEndIndex)
                            {
                                nextBlankIndex--;
                            }
                            firstBlankAtEndIndex--;
                        }
                        else
                        {
                            t.hashOrNextIndexForBlanks = nextBlankIndex;
                            nextBlankIndex = index;
                        }

                        t.nextIndex = BlankNextIndexIndicator;

                        count--;

                        return;
                    }
                }

                priorIndex = index;

                index = t.nextIndex & MarkNextIndexBitMaskInverted;
            }
            return; 
        }
        public int RemoveWhere(Predicate<T> match)
        {
            if (match == null)
            {
                throw new ArgumentNullException(nameof(match), "Value cannot be null.");
            }

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            incrementForEverySetModification++;
#endif

            int removeCount = 0;

#if !Exclude_No_Hash_Array_Implementation
            if (IsHashing)
            {
#endif
                int priorIndex;
                int nextIndex;
                for (int i = 0; i < buckets.Length; i++)
                {
                    priorIndex = NullIndex; 

                    for (int index = buckets[i]; index != NullIndex;)
                    {
                        ref TNode t = ref slots[index];

                        nextIndex = t.nextIndex;
                        if (match.Invoke(t.item))
                        {
                            

                            if (priorIndex == NullIndex)
                            {
                                buckets[i] = nextIndex;
                            }
                            else
                            {
                                slots[priorIndex].nextIndex = nextIndex;
                            }

                            
                            if (index == firstBlankAtEndIndex - 1)
                            {
                                if (nextBlankIndex == firstBlankAtEndIndex)
                                {
                                    nextBlankIndex--;
                                }
                                firstBlankAtEndIndex--;
                            }
                            else
                            {
                                t.hashOrNextIndexForBlanks = nextBlankIndex;
                                nextBlankIndex = index;
                            }

                            t.nextIndex = BlankNextIndexIndicator;

                            count--;
                            removeCount++;
                        }

                        priorIndex = index;

                        index = nextIndex;
                    }
                }
#if !Exclude_No_Hash_Array_Implementation
            }
            else
            {
                int i;
                for (i = count - 1; i >= 0; i--)
                {
                    if (match.Invoke(noHashArray[i]))
                    {
                        removeCount++;

                        if (i < count - 1)
                        {
                            int j = i + 1;
                            int k = i;
                            for (; j < count; j++, k++)
                            {
                                noHashArray[k] = noHashArray[j];
                            }
                        }

                        count--;
                    }
                }
            }
#endif

            return removeCount;
        }

        private class FastHashSetEqualityComparer : IEqualityComparer<FastHashSet<T>>
        {
            public bool Equals(FastHashSet<T> x, FastHashSet<T> y)
            {
                if (x == null && y == null)
                {
                    return true;
                }

                if (y == null)
                {
                    return false;
                }

                if (x != null)
                {
                    return x.SetEquals(y);
                }
                else
                {
                    return false;
                }
            }

            public int GetHashCode(FastHashSet<T> set)
            {
                if (set == null)
                {
                    
                    return 0; 
                }
                else
                {
                    unchecked
                    {
                        int hashCode = 0;
#if !Exclude_No_Hash_Array_Implementation
                        if (set.IsHashing)
                        {
#endif
                            int pastNodeIndex = set.slots.Length;
                            if (set.firstBlankAtEndIndex < pastNodeIndex)
                            {
                                pastNodeIndex = set.firstBlankAtEndIndex;
                            }

                            for (int i = 1; i < pastNodeIndex; i++)
                            {
                                if (set.slots[i].nextIndex != 0) 
                                {
                                    
                                    
                                    hashCode += set.slots[i].hashOrNextIndexForBlanks;
                                }
                            }
#if !Exclude_No_Hash_Array_Implementation
                        }
                        else
                        {
                            for (int i = 0; i < set.count; i++)
                            {
                                
                                hashCode += set.noHashArray[i].GetHashCode();
                            }
                        }
#endif
                        return hashCode;
                    }
                }
            }
        }

        public static IEqualityComparer<FastHashSet<T>> CreateSetComparer()
        {
            return new FastHashSetEqualityComparer();
        }
        public IEnumerator<T> GetEnumerator()
        {
            return new FastHashSetEnumerator<T>(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new FastHashSetEnumerator<T>(this);
        }

        private class FastHashSetEnumerator<T2> : IEnumerator<T2>
        {
            private readonly FastHashSet<T2> set;
            private int currentIndex = -1;

#if !Exclude_Check_For_Is_Disposed_In_Enumerator
            private bool isDisposed;
#endif

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
            private readonly int incrementForEverySetModification;
#endif

            public FastHashSetEnumerator(FastHashSet<T2> set)
            {
                this.set = set;
#if !Exclude_No_Hash_Array_Implementation
                if (set.IsHashing)
                {
#endif
                    currentIndex = NullIndex; 
#if !Exclude_No_Hash_Array_Implementation
                }
                else
                {
                    currentIndex = -1;
                }
#endif

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
                incrementForEverySetModification = set.incrementForEverySetModification;
#endif
            }
            public bool MoveNext()
            {
#if !Exclude_Check_For_Is_Disposed_In_Enumerator
                if (isDisposed)
                {
                    
                    
                    return false;
                }
#endif

#if !Exclude_Check_For_Set_Modifications_In_Enumerator
                if (incrementForEverySetModification != set.incrementForEverySetModification)
                {
                    throw new InvalidOperationException("Collection was modified; enumeration operation may not execute.");
                }
#endif

#if !Exclude_No_Hash_Array_Implementation
                if (set.IsHashing)
                {
#endif
                    while (true)
                    {
                        currentIndex++;
                        if (currentIndex < set.firstBlankAtEndIndex)
                        {
                            if (set.slots[currentIndex].nextIndex != BlankNextIndexIndicator)
                            {
                                return true;
                            }
                        }
                        else
                        {
                            currentIndex = set.firstBlankAtEndIndex;
                            return false;
                        }
                    }
#if !Exclude_No_Hash_Array_Implementation
                }
                else
                {
                    currentIndex++;
                    if (currentIndex < set.count)
                    {
                        return true;
                    }
                    else
                    {
                        currentIndex--;
                        return false;
                    }
                }
#endif
            }

            public void Reset()
            {
#if !Exclude_Check_For_Set_Modifications_In_Enumerator
                if (incrementForEverySetModification != set.incrementForEverySetModification)
                {
                    throw new InvalidOperationException("Collection was modified; enumeration operation may not execute.");
                }
#endif

#if !Exclude_No_Hash_Array_Implementation
                if (set.IsHashing)
                {
#endif
                    currentIndex = NullIndex; 
#if !Exclude_No_Hash_Array_Implementation
                }
                else
                {
                    currentIndex = -1;
                }
#endif
            }

            void IDisposable.Dispose()
            {
#if !Exclude_Check_For_Is_Disposed_In_Enumerator
                isDisposed = true;
#endif
            }

            public T2 Current
            {
                get
                {
#if !Exclude_No_Hash_Array_Implementation
                    if (set.IsHashing)
                    {
#endif
                        
                        

                        if (currentIndex > NullIndex && currentIndex < set.firstBlankAtEndIndex)
                        {
                            return set.slots[currentIndex].item;
                        }
#if !Exclude_No_Hash_Array_Implementation
                    }
                    else
                    {
                        if (currentIndex >= 0 && currentIndex < set.count)
                        {
                            return set.noHashArray[currentIndex];
                        }
                    }
#endif
                    return default;
                }
            }
            public ref T2 CurrentRef
            {
                get
                {
#if !Exclude_No_Hash_Array_Implementation
                    if (set.IsHashing)
                    {
#endif
                        
                        

                        if (currentIndex > NullIndex && currentIndex < set.firstBlankAtEndIndex)
                        {
                            return ref set.slots[currentIndex].item;
                        }
                        else
                        {
                            
                            return ref set.slots[0].item;
                        }
#if !Exclude_No_Hash_Array_Implementation
                    }
                    else
                    {
                        if (currentIndex >= 0 && currentIndex < set.count)
                        {
                            return ref set.noHashArray[currentIndex];
                        }
                        else
                        {
                            
                            return ref set.noHashArray[0];
                        }
                    }
#endif
                }
            }

            public bool IsCurrentValid
            {
                get
                {
#if !Exclude_No_Hash_Array_Implementation
                    if (set.IsHashing)
                    {
#endif
                        
                        

                        if (currentIndex > NullIndex && currentIndex < set.firstBlankAtEndIndex)
                        {
                            return true;
                        }
#if !Exclude_No_Hash_Array_Implementation
                    }
                    else
                    {
                        if (currentIndex >= 0 && currentIndex < set.count)
                        {
                            return true;
                        }
                    }
#endif
                    return false;
                }
            }

            object IEnumerator.Current => Current;
        }

        public static class FastHashSetUtil
        {
            public static int GetEqualOrClosestHigherPrime(int n)
            {
                if (n >= LargestPrimeLessThanMaxInt)
                {
                    
                    return LargestPrimeLessThanMaxInt;
                }

                if ((n & 1) == 0)
                {
                    n++; 
                }

                bool found;

                do
                {
                    found = true;

                    int sqrt = (int)Math.Sqrt(n);
                    for (int i = 3; i <= sqrt; i += 2)
                    {
                        int div = n / i;
                        if (div * i == n) 
                        {
                            found = false;
                            n += 2;
                            break;
                        }
                    }
                } while (!found);

                return n;
            }
        }
    }

    public struct ChainLevelAndCount : IComparable<ChainLevelAndCount>
    {
        public ChainLevelAndCount(int level, int count)
        {
            Level = level;
            Count = count;
        }

        public int Level;
        public int Count;

        public int CompareTo(ChainLevelAndCount other)
        {
            return Level.CompareTo(other.Level);
        }
    }

#if DEBUG
    public static class DebugOutput
    {
        public static void OutputEnumerableItems<T2>(IEnumerable<T2> e, string enumerableName)
        {
            System.Diagnostics.Debug.WriteLine("---start items: " + enumerableName + "---");
            int count = 0;
            foreach (T2 item in e)
            {
                System.Diagnostics.Debug.WriteLine(item.ToString());
                count++;
            }
            System.Diagnostics.Debug.WriteLine("---end items: " + enumerableName + "; count = " + count.ToString("N0") + "---");
        }

        public static void OutputSortedEnumerableItems<T2>(IEnumerable<T2> e, string enumerableName)
        {
            List<T2> lst = new List<T2>(e);
            lst.Sort();
            System.Diagnostics.Debug.WriteLine("---start items (sorted): " + enumerableName + "---");
            int count = 0;
            foreach (T2 item in lst)
            {
                System.Diagnostics.Debug.WriteLine(item.ToString());
                count++;
            }
            System.Diagnostics.Debug.WriteLine("---end items: " + enumerableName + "; count = " + count.ToString("N0") + "---");
        }
    }
#endif
}