using System;
using System.Diagnostics;
using System.Threading;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace IgniteBenchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            //BenchmarkRunner.Run<ScanQueryBench>(GetConfig());
            BenchmarkRunner.Run<DataRetrievalBench>(GetConfig());
            //new DataRetrievalBench().SqlBenchmark();
        }

        private static IConfig GetConfig()
        {
            return ManualConfig
                .Create(DefaultConfig.Instance)
                .With(Job.RyuJitX64.WithLaunchCount(1).WithWarmupCount(1));
        }
    }
}
