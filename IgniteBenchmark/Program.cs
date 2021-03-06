﻿using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace IgniteBenchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<ScanQueryBench>(GetConfig());
            
            //new ScanQueryBench().ComputeScanQuery();
            //new ScanQueryBench().NormalScanQuery();
            //new ScanQueryBench().CachedScanQuery();
            //new ScanQueryBench().TwoCacheScanQuery();
        }

        private static IConfig GetConfig()
        {
            return ManualConfig
                .Create(DefaultConfig.Instance)
                .With(Job.RyuJitX64.WithLaunchCount(1).WithWarmupCount(1));
        }
    }
}
