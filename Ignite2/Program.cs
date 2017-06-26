using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using CacheWrapper;

namespace Ignite2
{
    public class Program
    {
        static void Main(string[] args)
        {
            var conf = new IgniteConfiguration();

            using (var ignite = Ignition.Start(conf))
            {
                var cache = ignite.GetOrCreateCacheWrapper<string, Trade>(new CacheWrapperConfiguration { Name = "MyCache", CacheMode = CacheMode.Partitioned, Backups = 1});

                if (cache.IgniteCache.GetSize(CachePeekMode.Primary) == 0)
                {
                    Console.WriteLine("[Test] cache is empty - doing initial load");

                    //cache.Put("TEST", GenerateTestData(1).First().Value);
                    cache.PutAll(GenerateTestData(1000));
                    Console.WriteLine("[Test] cache is empty - initial load done");
                }
                else
                {
                    Thread.Sleep(2000);
                    foreach (var kvp in cache.ScanQuery(new ScanQueryFilter(), CancellationToken.None))
                    {
                        Console.WriteLine("Scan Query results =  " + kvp.Key);
                    }
                }
                Console.ReadLine();
            }
        }

        public static IEnumerable<KeyValuePair<string, Trade>> GenerateTestData(int count)
        {
            foreach (var i in Enumerable.Range(1, count))
            {
                var trade = new Trade
                {
                    Id = string.Format("XXX-{0:D8}", i),
                    Product = new Product { Instrument = "Swap" }
                };

                foreach (var j in Enumerable.Range(1, 200))
                {
                    trade.Product.Flows.Add(new Flow
                    {
                        FlowId = j,
                        Currency = j%4==0 ? "EUR" : "USD",
                        Index = j%4==0 ? (j%2==0 ? "EON" : "EIB") : (j%2 == 0) ? "OIS" : "LIB",
                        Nominal = j*100000,
                        StartDate = DateTime.Now.AddDays(1),
                        EndDate = DateTime.Now.AddDays(2),
                        FixingDate = DateTime.Now.AddDays(2),
                        Amount = double.NaN
                    });
                }

                yield return new KeyValuePair<string, Trade>(trade.Id, trade);
            }
        }
    }

    [Serializable]
    public class Trade
    {
        public string Id { get; set; }
        public Product Product { get; set; }
    }

    [Serializable]
    public class Product
    {
        public string Instrument { get; set; }
        public List<Flow> Flows { get; }

        public Product()
        {
            Flows = new List<Flow>();
        }
    }

    [Serializable]
    public class Flow
    {
        public int FlowId { get; set; }
        public string Currency { get; set; }
        public string Index { get; set; }
        public DateTime FixingDate { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public double? Fixing { get; set; }
        public double Nominal { get; set; }
        public double Amount { get; set; }
    }

    [Serializable]
    public class ScanQueryFilter : ICacheEntryFilter<String, Trade>
    {
        #region Implementation of ICacheEntryFilter<in string,in Trade>

        public bool Invoke(ICacheEntry<string, Trade> entry)
        {
            return FilterTrade(entry.Value);
        }

        public static bool FilterTrade(Trade value)
        {
            return value.Product.Flows.Any(f => f.Currency == "EUR" && f.Index == "EON1");
        }

        #endregion
    }

    [Serializable]
    public class ScanQueryCachingFilter : ICacheEntryFilter<string, IBinaryObject>
    {
        private static readonly ConcurrentDictionary<string, Trade> CachedTrades 
            = new ConcurrentDictionary<string, Trade>();

        #region Implementation of ICacheEntryFilter<in string,in Trade>

        public bool Invoke(ICacheEntry<string, IBinaryObject> entry)
        {
            var value = CachedTrades.GetOrAdd(entry.Key, k => entry.Value.Deserialize<Trade>());

            return ScanQueryFilter.FilterTrade(value);
        }

        #endregion
    }

    [Serializable]
    public class ScanQueryKeyOnlyFilter : ICacheEntryFilter<string, int>
    {
        private static readonly ConcurrentDictionary<string, Trade> CachedTrades 
            = new ConcurrentDictionary<string, Trade>();

        private readonly string _dataCacheName;
        
        [NonSerialized]
        private ICache<string, Trade> _dataCache;

        public ScanQueryKeyOnlyFilter(string dataCacheName)
        {
            _dataCacheName = dataCacheName;
        }


        public bool Invoke(ICacheEntry<string, int> entry)
        {
            _dataCache = _dataCache ?? Ignition.GetIgnite().GetCache<string, Trade>(_dataCacheName);

            var value = CachedTrades.GetOrAdd(entry.Key, k => _dataCache[k]);

            return ScanQueryFilter.FilterTrade(value);
        }
    }
}
