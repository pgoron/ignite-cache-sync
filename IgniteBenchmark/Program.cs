using System;
using System.Diagnostics;
using System.Threading;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace IgniteBenchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            var scanQueryBench = new ScanQueryBench();

            if (args.Length > 0)
            {
                Console.WriteLine("STARTED");
                Thread.Sleep(Timeout.Infinite);
            }

            //BenchmarkRunner.Run<ScanQueryBench>(GetConfig());
            
            //new ScanQueryBench().ComputeScanQuery();
            //new ScanQueryBench().NormalScanQuery();


            Console.WriteLine("WARMUP...");
            scanQueryBench.TwoCacheScanQuery();

            Console.WriteLine("BENCH...");
            var sw = Stopwatch.StartNew();
            scanQueryBench.TwoCacheScanQuery();
            Console.WriteLine(sw.Elapsed);

            //new ScanQueryBench().TwoCacheScanQuery();

            Console.ReadKey();
        }

        private static IConfig GetConfig()
        {
            return ManualConfig
                .Create(DefaultConfig.Instance)
                .With(Job.RyuJitX64.WithLaunchCount(1).WithWarmupCount(1));
        }
    }
}
