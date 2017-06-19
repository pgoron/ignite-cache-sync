using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.DataStructures.Configuration;
using Apache.Ignite.Core.Events;
using Apache.Ignite.Core.Lifecycle;
using Apache.Ignite.Core.Log;
using Apache.Ignite.Core.Messaging;

namespace Ignite2
{
    class Logger : ILogger
    {
        #region Implementation of ILogger

        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            //if (level == LogLevel.Debug && !message.Contains("MyCache"))
            //    return;

            if (args == null)
            {
                Console.WriteLine("{0:O} [{1}] {2}", DateTime.Now, level, message);
            }
            else
            {
                Console.WriteLine(message, args);
            }
            if (nativeErrorInfo != null)
            {
                Console.WriteLine(nativeErrorInfo);
            }
            if (ex != null)
            {
                Console.WriteLine(ex);
            }
        }

        public bool IsEnabled(LogLevel level)
        {
            return level != LogLevel.Debug && level != LogLevel.Trace;
        }

        #endregion
    }

    public class EventListener<T> : IEventListener<T> where T : IEvent
    {
        private readonly Func<T, bool> _invoker;

        public EventListener(Func<T, bool> invoker)
        {
            _invoker = invoker;
        }

        public bool Invoke(T evt)
        {
            Console.WriteLine("START " + evt);
            var ret = _invoker(evt);
            Console.WriteLine("END " + evt);
            return ret;
        }
    }

    public class LifeCycleHandle : ILifecycleHandler
    {
        #region Implementation of ILifecycleHandler

        public void OnLifecycleEvent(LifecycleEventType evt)
        {
            Console.WriteLine(evt);
        }

        #endregion
    }
    public class CacheWrapper : IEventListener<CacheEvent>, IEventListener<CacheRebalancingEvent>
    {
        private IIgnite _ignite;
        private ICache<string, byte[]> _cache;
        public ConcurrentDictionary<string, string> _localCache = new ConcurrentDictionary<string, string>();

        public static CacheWrapper Instance = new CacheWrapper();

        private Timer _timer;

        public CacheWrapper()
        {
            var igniteConf = new IgniteConfiguration
            {
                CacheConfiguration = new List<CacheConfiguration>
                {
                    new CacheConfiguration
                    {
                        Backups = 1,
                        CacheMode = CacheMode.Partitioned,
                        Name = "MyCache",
                        RebalanceMode = CacheRebalanceMode.Sync,
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.PrimarySync,
                        
                    }
                },
                Logger = new Logger(),
                EventStorageSpi = new NoopEventStorageSpi(),
                IncludedEventTypes = new List<int>(),
                LifecycleHandlers = new List<ILifecycleHandler> { new LifeCycleHandle() }
            };
            _ignite = Ignition.Start(igniteConf);



            var events = _ignite.GetEvents();

            events.EnableLocal(EventType.CacheObjectPut, EventType.CacheObjectRemoved, EventType.CacheObjectExpired, EventType.CacheRebalanceObjectUnloaded, EventType.CacheRebalanceObjectLoaded);
            events.EnableLocal(EventType.CacheRebalanceStopped);
            //events.LocalListen(new EventListener<CacheEvent>(Invoke), EventType.CacheObjectPut, EventType.CacheObjectRemoved, EventType.CacheObjectExpired, EventType.CacheRebalanceObjectUnloaded, EventType.CacheRebalanceObjectLoaded);
            //events.LocalListen(new EventListener<CacheRebalancingEvent>(Invoke), EventType.CacheRebalanceStopped);
            events.LocalListen<CacheEvent>(this, EventType.CacheObjectPut, EventType.CacheObjectRemoved, EventType.CacheObjectExpired, EventType.CacheRebalanceObjectUnloaded, EventType.CacheRebalanceObjectLoaded);
            events.LocalListen<CacheRebalancingEvent>(this, EventType.CacheRebalanceStopped);

            _cache = _ignite.GetOrCreateCache<string, byte[]>("MyCache");
            _timer =
                new Timer(
                    (o) =>
                    {
                        Console.WriteLine("Cache Size : main={0} / backup={1} / local={2}",
                            _cache.GetLocalSize(CachePeekMode.Primary), _cache.GetLocalSize(CachePeekMode.Backup), _localCache.Count);
                    },
                    null, new TimeSpan(0, 0, 1), new TimeSpan(0, 0, 5));

            Console.WriteLine("Rebalance Delay : " + _cache.GetConfiguration().RebalanceDelay);

            // reason of the sleep here :
            // when a node starts and joins an existing cluster, initial rebalancing of data between 
            // the joining node and existing nodes is done during initialization (Ignition.Start()),
            // but all local partitions will be flagged backup during the few seconds before joining
            // nodes became official primary node.
            Thread.Sleep(2000);
            
            //
            // Initial manual sync between local .net cache and ignite cache is mandatory
            // because I didn't find way to listen for cache events during node initialization.
            // We can listen for local events only after having called Ignition.Start() and
            // we miss events generated by initial rebalancing.
            //
            Console.WriteLine("START Syncing local cache");
            var cnt = 0;
            foreach (var entry in _cache.GetLocalEntries(CachePeekMode.Primary))
            {
                cnt++;
                if (_ignite.GetAffinity(_cache.Name).IsPrimary(_ignite.GetCluster().GetLocalNode(), entry.Key))
                {
                    Console.WriteLine("Initializing local cache (key={0})", entry.Key);
                    _localCache[entry.Key] = Encoding.UTF8.GetString(entry.Value);
                }
            }
            Console.WriteLine("END Syncing local cache (local={0}/{1})", _localCache.Count, cnt);
            

            // Populate cache when the first node starts
            if (_ignite.GetCluster().GetNodes().Count == 1)
            {
                for (var i = 0; i < 100; i++)
                {
                    _cache.Put(i.ToString(), Encoding.UTF8.GetBytes("Hello World ! " + i));
                }
            }
            else
            {
                Thread.Sleep(2000);
                ThreadPool.QueueUserWorkItem((state) =>
                {

                    var topic = DateTime.Now.Ticks.ToString();
                    var responseListener = new CustomScanQueryResponseListener();
                    try
                    {
                        Console.WriteLine("Listening for responses");
                        _ignite.GetMessaging().LocalListen(responseListener, topic);

                        Console.WriteLine("Send CustomScanQueryTask");
                        var results = _ignite.GetCluster()
                            //.ForDataNodes("MyCache")
                            .ForCacheNodes("MyCache")
                            .GetCompute()
                            .Broadcast(new CustomScanQueryTask
                            {
                                CacheName = "MyCache",
                                Predicate = new Predicate() { content = "5" },
                                Topic = topic
                            });

                        Console.WriteLine("{0} responses received via listener, {1} responses returned by task", responseListener.Responses.Count, results.Sum());
                    }
                    finally
                    {
                        _ignite.GetMessaging().StopLocalListen(responseListener, topic);
                    }
                });


            }

            
        }

        #region Implementation of IEventListener<in CacheEvent>

        /// <summary>
        /// handler called when EventType.CacheRebalanceStopped is generated by Ignite
        /// </summary>
        /// <param name="evt"></param>
        /// <returns></returns>
        public bool Invoke(CacheRebalancingEvent evt)
        {
            try
            {
                Console.WriteLine("CacheEvent[{0},{1},{2},{3}]", evt.Name, evt.CacheName, evt.Partition, evt.DiscoveryEventName);

                // remove all .net cache entries for wich current node is not primary for the cache entry key
                foreach (var entry in _localCache)
                {
                    if (!_ignite.GetAffinity(_cache.Name).IsPrimary(_ignite.GetCluster().GetLocalNode(), entry.Key))
                    {
                        Console.WriteLine("Cleaning local cache (key={0})", entry.Key);
                        string oldValue;
                        _localCache.TryRemove(entry.Key, out oldValue);
                    }
                }

                // make sure all cache entries for which current node is primary are presents in .net local cache
                foreach (var entry in _cache.GetLocalEntries(CachePeekMode.Primary))
                {
                    if (!_localCache.ContainsKey(entry.Key))
                    {
                        Console.WriteLine("Initializing local cache (key={0})", entry.Key);
                        _localCache[entry.Key] = Encoding.UTF8.GetString(entry.Value);
                    }
                }

                Console.WriteLine("Cache Size : main={0} / backup={1} / local={2}",
                    _cache.GetLocalSize(CachePeekMode.Primary), _cache.GetLocalSize(CachePeekMode.Backup), _localCache.Count);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            return true;
        }


        public bool Invoke(CacheEvent evt)
        {
            try
            {
                Console.WriteLine("CacheEvent[{0},{1},{2},{3}]", evt.Name, evt.CacheName, evt.Partition, evt.Key);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            if (evt.HasNewValue
                && _ignite.GetAffinity(_cache.Name).IsPrimary(_ignite.GetCluster().GetLocalNode(), evt.Key))
            {
                Console.WriteLine("Updating local cache (key={0})", evt.Key);
                _localCache[(string) evt.Key] = Encoding.UTF8.GetString((byte[])evt.NewValue);
                return true;
            }

            if ((evt.HasOldValue && !evt.HasNewValue)
                || evt.Type == EventType.CacheRebalanceObjectUnloaded)
            {
                Console.WriteLine("Cleaning local cache (key={0})", evt.Key);
                string oldValue;
                _localCache.TryRemove((string)evt.Key, out oldValue);
                return true;
            }

            return true;
        }

        #endregion
    }

    [Serializable]
    public class Predicate
    {
        public string content { get; set; }

        public bool Invoke(string val)
        {
            return val.Contains(content);
        }
    }

    [Serializable]
    public class CustomScanQueryTask : IComputeFunc<int> /*IComputeAction*/
    {
        public String CacheName { get; set; }
        public Predicate Predicate { get; set; }
        public string Topic { get; set; }

        #region Implementation of IComputeAction

        public int Invoke()
        {
            var messaging = Ignition.TryGetIgnite().GetMessaging();

            var resCount = 0;
            
            Console.WriteLine("Executing CustomScanQueryTask");
            foreach (var kvp in CacheWrapper.Instance._localCache.Where(kvp => Predicate.Invoke(kvp.Value)))
            {
                Console.WriteLine("Matching value = {0}", kvp.Value);
                if (Topic != null)
                {
                    messaging.Send(kvp.Value, Topic);
                }
                resCount++;
            }

            return resCount;
        }

        #endregion
    }

    public class CustomScanQueryResponseListener : IMessageListener<string>
    {
        public readonly List<string> Responses = new List<string>();

        #region Implementation of IMessageListener<in string>

        public bool Invoke(Guid nodeId, string message)
        {
            Console.WriteLine("Response received from node : " + nodeId);
            Responses.Add(message);
            return true;
        }

        #endregion
    }
}
