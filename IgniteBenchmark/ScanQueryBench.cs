
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using BenchmarkDotNet.Attributes;
using CacheWrapper;
using Ignite2;
using CacheWrapper = Ignite2.CacheWrapper;

namespace IgniteBenchmark
{
    public class ScanQueryBench
    {
        private readonly ICache<string, byte[]> _cache;
        private readonly ICache<string, int> _zeroValueCache;
        private readonly ICacheWrapper<string, Trade> _wrappedCache;

        public ScanQueryBench()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    },
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                }
            };

            Ignition.Start(cfg);

            // Start more nodes.
            cfg.AutoGenerateIgniteInstanceName = true;
            cfg.ClientMode = true;
            var ignite = Ignition.Start(cfg);

            // Prepare caches.
            _cache = ignite.CreateCache<string, byte[]>("cache");

            long totalSize = 0;

            using (var ldr = ignite.GetDataStreamer<string, byte[]>(_cache.Name))
            {
                Parallel.ForEach(Ignite2.Program.GenerateTestData(1000), x =>
                {
                    var bytes = Serializer.ObjectToByteArray(x.Value);
                    Console.WriteLine(bytes.Length);
                    ldr.AddData(x.Key, bytes);
                    Interlocked.Add(ref totalSize, bytes.LongLength);
                });

                ldr.Flush();
            }

            Console.WriteLine("Total object size: " + totalSize);
            //Console.ReadKey();

            //_cache.PutAll(Ignite2.Program.GenerateTestData(100).AsParallel()
            //    .Select(x => new KeyValuePair<string, byte[]>(x.Key, Serializer.ObjectToByteArray(x.Value))));

            _zeroValueCache = ignite.CreateCache<string, int>("zeroVal");
            _zeroValueCache.PutAll(_cache.Select(x => new KeyValuePair<string, int>(x.Key, 0)));

            //_wrappedCache = GetCacheWrapper(_cache);
            //_wrappedCache.Sync();  // TODO: This does not work properly with two nodes in one process.
        }

        private static ICacheWrapper<string, Trade> GetCacheWrapper(ICache<string, byte[]> igniteCache)
        {
            return igniteCache.Ignite.GetOrCreateCacheWrapper<string, Trade>(new CacheWrapperConfiguration { Name = igniteCache.Name });
        }

        //[Benchmark]
        public void NormalScanQuery()
        {
            var res = _cache.Query(new ScanQuery<string, byte[]>
            {
                Filter = new ScanQueryFilterByte()
            }).GetAll();

            ValidateResults(res);
        }

        [Benchmark]
        public void CachedScanQuery()
        {
            var res = _cache.Query(new ScanQuery<string, byte[]>
            {
                Filter = new ScanQueryCachingFilter()
            }).GetAll();

            ValidateResults(res);
        }

        //[Benchmark]
        public void TwoCacheScanQuery()
        {
            var keys = _zeroValueCache.Query(new ScanQuery<string, int>
            {
                Filter = new ScanQueryKeyOnlyFilter(_cache.Name)
            }).GetAll();

            var res = _cache.GetAll(keys.Select(x => x.Key));

            ValidateResults(res);
        }

        //[Benchmark]
        public void ZeroValueScanQuery()
        {
            var res = _zeroValueCache.Query(new ScanQuery<string, int>
            {
                Filter = new ZeroValueFilter()
            }).GetAll();

            ValidateResults(res);
        }

        //[Benchmark]
        public void ComputeScanQuery()
        {
            var res = _wrappedCache.ScanQuery(new ScanQueryFilter(), CancellationToken.None).ToList();

            ValidateResults(res);
        }

        private static void ValidateResults<T>(ICollection<T> res)
        {
            if (res.Count != 100)
            {
                throw new InvalidOperationException("Invalid results: " + res.Count);
            }
        }
    }

    public class ZeroValueFilter : ICacheEntryFilter<string, int>
    {
        public bool Invoke(ICacheEntry<string, int> entry)
        {
            return entry.Value == 0;
        }
    }
}
