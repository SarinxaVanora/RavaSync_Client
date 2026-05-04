using System;

namespace RavaSync.Services.Optimisation.Reduction
{
    public static class OrderStatistics
    {
        private static T FindMedian<T>(T[] arr, int i, int n)
        {
            if (i <= n)
            {
                Array.Sort(arr, i, n); 
            }
            else
            {
                Array.Sort(arr, n, i);
            }

            return arr[n / 2]; 
        }

        
        
        
        
        public static T FindKthSmallest<T>(T[] arr, int l, int r, int k) where T : IComparable<T>
        {
            
            
            if (k > 0 && k <= r - l + 1)
            {
                int n = r - l + 1; 

                
                
                
                int i;

                
                T[] median = new T[(n + 4) / 5];
                for (i = 0; i < n / 5; i++)
                {
                    median[i] = FindMedian(arr, l + i * 5, 5);
                }

                
                if (i * 5 < n)
                {
                    median[i] = FindMedian(arr, l + i * 5, n % 5);
                    i++;
                }

                
                
                
                T medOfMed = (i == 1) ? median[i - 1] : FindKthSmallest(median, 0, i - 1, i / 2);

                
                
                int pos = Partition(arr, l, r, medOfMed);

                
                if (pos - l == k - 1)
                {
                    return arr[pos];
                }

                if (pos - l > k - 1) 
                {
                    return FindKthSmallest(arr, l, pos - 1, k);
                }

                
                return FindKthSmallest(arr, pos + 1, r, k - pos + l - 1);
            }

            
            return default(T);
        }

        private static void Swap<T>(ref T[] arr, int i, int j)
        {
            T temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }

        
        
        private static int Partition<T>(T[] arr, int l, int r, T x) where T : IComparable<T>
        {
            
            int i;
            for (i = l; i < r; i++)
            {
                if (arr[i].CompareTo(x) == 0)
                {
                    break;
                }
            }

            Swap(ref arr, i, r);

            
            i = l;
            for (int j = l; j <= r - 1; j++)
            {
                if (arr[j].CompareTo(x) <= 0)
                {
                    Swap(ref arr, i, j);
                    i++;
                }
            }
            Swap(ref arr, i, r);
            return i;
        }

    }
}