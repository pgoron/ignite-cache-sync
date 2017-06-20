using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using CacheWrapper;

namespace Ignite2
{
    class Program
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

        static IEnumerable<KeyValuePair<string, Trade>> GenerateTestData(int count)
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
    class Trade
    {
        public string Id { get; set; }
        public Product Product { get; set; }
    }

    [Serializable]
    class Product
    {
        public string Instrument { get; set; }
        public List<Flow> Flows { get; }

        public Product()
        {
            Flows = new List<Flow>();
        }
    }

    [Serializable]
    class Flow
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
    class ScanQueryFilter : ICacheEntryFilter<String, Trade>
    {
        #region Implementation of ICacheEntryFilter<in string,in Trade>

        public bool Invoke(ICacheEntry<string, Trade> entry)
        {
            return entry.Value.Product.Flows.Any(f => f.Currency == "EUR" && f.Index == "EON");
        }

        #endregion
    }
}
