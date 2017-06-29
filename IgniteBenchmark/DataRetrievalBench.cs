using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using CacheWrapper;
using Ignite2;

namespace IgniteBenchmark
{
    public class DataRetrievalBench
    {
        private const int Count = 100;

        private readonly ICache<string, byte[]> _cache;

        private readonly List<string> _keys = new List<string>();

        public DataRetrievalBench()
        {
            //Environment.SetEnvironmentVariable("IGNITE_H2_DEBUG_CONSOLE", "true");
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    },
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                },
                //IgniteHome = @"c:\w\incubator-ignite",
                //JvmOptions = JvmDebugOpts
            };

            Ignition.Start(cfg);

            // Start more nodes.
            cfg.AutoGenerateIgniteInstanceName = true;
            cfg.ClientMode = true;
            var ignite = Ignition.Start(cfg);
            _cache = ignite.GetOrCreateCache<string, byte[]>(new CacheConfiguration("cache",
                new QueryEntity{KeyTypeName = "java.lang.String", ValueTypeName = "[B", TableName = "trades"}));

            var trade = Ignite2.Program.GenerateTestData(1).Single().Value;
            var bytes = Serializer.ObjectToByteArray(trade);

            using (var ldr = ignite.GetDataStreamer<string, byte[]>(_cache.Name))
            {
                ldr.PerNodeBufferSize = 10;
                for (int i = 0; i < Count; i++)
                {
                    var key = "k-" + i;
                    ldr.AddData(key, bytes);
                    _keys.Add(key);
                    if (i % 1000 == 0)
                    {
                        Console.WriteLine(i);
                    }
                }

                ldr.Flush();
            }

            Console.WriteLine("ldr finished");
        }

        [Benchmark]
        public void GetAllBenchmark()
        {
            var res = _cache.GetAll(_keys);
            ValidateResult(res.Count);
        }

        [Benchmark]
        public void IterateBenchmark()
        {
            var res = new List<object>(_keys.Count);

            foreach (var key in _keys)
            {
                res.Add(_cache.Get(key));
            }

            ValidateResult(res.Count);
        }

        private static void ValidateResult(int count)
        {
            if (count != Count)
            {
                throw new Exception("Incorrect result: " + count);
            }
        }

        //[Benchmark]
        public void SqlBenchmark()
        {
            var sqlFieldsQuery = new SqlFieldsQuery(
                "select _val from trades where _key in (SELECT * FROM TABLE(KEY varchar = ?))")
            {
                Arguments = new object[] {_keys.ToArray()}
            };
            var res = _cache.QueryFields(sqlFieldsQuery).GetAll();
            ValidateResult(res.Count);
        }
    }
}
