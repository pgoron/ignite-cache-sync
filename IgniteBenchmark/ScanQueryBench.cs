﻿
using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using BenchmarkDotNet.Attributes;
using Ignite2;

namespace IgniteBenchmark
{
    public class ScanQueryBench
    {
        private readonly ICache<string, Trade> _cache;
        private readonly ICache<string, IBinaryObject> _binCache;
        private readonly ICache<string, int> _zeroValueCache;

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

            var ignite = Ignition.Start(cfg);

            // Start more nodes.
            cfg.AutoGenerateIgniteInstanceName = true;
            Ignition.Start(cfg);

            // Prepare caches.
            _cache = ignite.CreateCache<string, Trade>("cache");
            _cache.PutAll(Ignite2.Program.GenerateTestData(100));

            _binCache = _cache.WithKeepBinary<string, IBinaryObject>();

            _zeroValueCache = ignite.CreateCache<string, int>("zeroVal");
            _zeroValueCache.PutAll(_cache.Select(x => new KeyValuePair<string, int>(x.Key, 0)));
        }

        [Benchmark]
        public void NormalScanQuery()
        {
            var res = _cache.Query(new ScanQuery<string, Trade>
            {
                Filter = new ScanQueryFilter()
            }).GetAll();

            ValidateResults(res);
        }

        [Benchmark]
        public void CachedScanQuery()
        {
            var res = _binCache.Query(new ScanQuery<string, IBinaryObject>
            {
                Filter = new ScanQueryCachingFilter()
            }).GetAll();

            ValidateResults(res);
        }

        [Benchmark]
        public void TwoCacheScanQuery()
        {
            var keys = _zeroValueCache.Query(new ScanQuery<string, int>
            {
                Filter = new ScanQueryKeyOnlyFilter(_cache.Name)
            }).GetAll();

            var res = _cache.GetAll(keys.Select(x => x.Key));

            ValidateResults(res);
        }

        [Benchmark]
        public void ZeroValueScanQuery()
        {
            var res = _zeroValueCache.Query(new ScanQuery<string, int>
            {
                Filter = new ZeroValueFilter()
            }).GetAll();

            ValidateResults(res);
        }

        private static void ValidateResults<T>(ICollection<ICacheEntry<string, T>> res)
        {
            if (res.Count != 0)
            {
                throw new InvalidOperationException("Invalid results: " + res.Count);
            }
        }
    }

    public class ZeroValueFilter : ICacheEntryFilter<string, int>
    {
        public bool Invoke(ICacheEntry<string, int> entry)
        {
            return entry.Value == 1;
        }
    }
}