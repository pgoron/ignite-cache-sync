using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Events;
using Apache.Ignite.Core.Messaging;

namespace CacheWrapper
{
    public interface ICacheWrapper
    {
        void Sync();

        void HandleCacheEvent(CacheEvent evt);
    }

    public interface ICacheWrapper<TK, TV> : IEnumerable<KeyValuePair<TK, TV>>, ICacheWrapper
    {
        ICache<TK, byte[]> IgniteCache { get; }

        void Put(TK key, TV value);
        void PutAll(IEnumerable<KeyValuePair<TK, TV>> values);

        IEnumerable<KeyValuePair<TK, byte[]>> ScanQuery(ICacheEntryFilter<TK, TV> filter, CancellationToken ctk);
    }

    internal class CacheWrapper<TK, TV> : ICacheWrapper<TK, TV>, ICacheWrapper
    {
        private readonly CacheWrapperConfiguration _configuration;
        private readonly ICache<TK, byte[]> _igniteCache;
        private readonly ConcurrentDictionary<TK, TV> _localCache;
        private readonly Timer _statsTimer;

        public ICache<TK, byte[]> IgniteCache { get { return _igniteCache;} }

        public CacheWrapper(CacheWrapperConfiguration configuration, ICache<TK, byte[]> igniteCache)
        {
            _configuration = configuration;
            _igniteCache = igniteCache;
            _localCache = new ConcurrentDictionary<TK, TV>();
            _statsTimer = new Timer((state => PrintStats()), null, 0, 5000);
        }

        public void Put(TK key, TV value)
        {
            var val = Serializer.ObjectToByteArray(value);
            if (_igniteCache.Ignite.GetAffinity(_configuration.Name)
                    .IsPrimary(_igniteCache.Ignite.GetCluster().GetLocalNode(), key))
            {
                _localCache.TryAdd(key, value);
            }
            _igniteCache.Put(key, val);
            Console.WriteLine("serialized {0} : {1} bytes", key, val.Length);
        }

        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> values)
        {
            _igniteCache.PutAll(values.Select(kvp =>
            {
                if (_igniteCache.Ignite.GetAffinity(_configuration.Name)
                        .IsPrimary(_igniteCache.Ignite.GetCluster().GetLocalNode(), kvp.Key))
                {
                    _localCache.TryAdd(kvp.Key, kvp.Value);
                }

                var val = Serializer.ObjectToByteArray(kvp.Value);
                Console.WriteLine("serialized {0} : {1} bytes", kvp.Key, val.Length);

                return new KeyValuePair<TK, byte[]>(kvp.Key, val);
            }));

            PrintStats();
        }

        public IEnumerable<KeyValuePair<TK, byte[]>> ScanQuery(ICacheEntryFilter<TK, TV> filter, CancellationToken ctk)
        {
            var results = new BlockingCollection<KeyValuePair<TK, byte[]>>();

            var resultCount = 0;

            var scanQueryTask = Task.Run(() =>
            {
                var topic = Guid.NewGuid().ToString();
                var responseListener = new CustomScanQueryResponseListener<TK, TV>(kvp => results.Add(kvp, ctk));
                try
                {
                    //Console.WriteLine("Listening for responses");
                    _igniteCache.Ignite.GetMessaging().LocalListen(responseListener, topic);

                    //Console.WriteLine("Send CustomScanQueryTask");
                    var result = _igniteCache.Ignite.GetCluster()
                        .ForCacheNodes(_igniteCache.Name)
                        .GetCompute()
                        .Broadcast(new CustomScanQueryTask<TK, TV>
                        {
                            CacheName = _igniteCache.Name,
                            Predicate = filter,
                            Topic = topic
                        });

                    resultCount = result.Sum();
                    results.CompleteAdding();

                    //Console.WriteLine("All broadcast tasks finished");
                }
                finally
                {
                    _igniteCache.Ignite.GetMessaging().StopLocalListen(responseListener, topic);
                }
            }, ctk);

            var receivedResponses = 0;
            while (!scanQueryTask.IsCompleted && !scanQueryTask.IsCanceled && !scanQueryTask.IsFaulted)
            {
                KeyValuePair<TK, byte[]> value;
                if (results.TryTake(out value, 1000, ctk))
                {
                    yield return value;
                    receivedResponses++;
                }
            }

            if (receivedResponses != resultCount)
            {
                throw new InvalidOperationException("mismatch response count");
            }
        }

        public void Sync()
        {
            var keys = new List<TK>();

            // would be better if ignite has a primitive to return only keys to not transfert values from jvm to clr
            foreach (var cacheEntry in _igniteCache.GetLocalEntries(CachePeekMode.Primary))
            {
                keys.Add(cacheEntry.Key);
                _localCache.GetOrAdd(cacheEntry.Key, key =>
                {
                    //Console.WriteLine("deserializing {0} : {1} bytes", key, cacheEntry.Value.Length);
                    return Serializer.ByteArrayToObject<TV>(cacheEntry.Value);
                });
            }

            _localCache.Keys.Except(keys).ToList().ForEach(k =>
            {
                TV value;
                _localCache.TryRemove(k, out value);
            });

            PrintStats();
        }


        public void HandleCacheEvent(CacheEvent evt)
        {
            if (evt.HasNewValue)
            {
                if (IgniteCache.Ignite.GetAffinity(IgniteCache.Name)
                    .IsPrimary(IgniteCache.Ignite.GetCluster().GetLocalNode(), evt.Key))
                {
                    if (evt.HasOldValue)
                    {
                        var val = Serializer.ByteArrayToObject<TV>(((byte[]) evt.NewValue));
                        _localCache[(TK) evt.Key] = val;
                        Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' updated in local cache", evt.Name, evt.Key);
                    }
                    else
                    {
                        var added = false;
                        _localCache.GetOrAdd((TK) evt.Key, key =>
                        {
                            Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' added to local cache", evt.Name, evt.Key);
                            added = true;
                            return Serializer.ByteArrayToObject<TV>(((byte[]) evt.NewValue));
                        });

                        if (!added)
                        {
                            Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' skipped : local cache already is up-to-date", evt.Name, evt.Key);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' skipped : current node is not primary for the given key", evt.Name, evt.Key);
                }
            }
            else
            {
                if (evt.HasOldValue)
                {
                    TV val;
                    if (_localCache.TryRemove((TK) evt.Key, out val))
                    {
                        Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' removed from local cache", evt.Name, evt.Key);
                    }
                }
                else
                {
                    Console.WriteLine("[CacheWrapper] CacheEvent='{0}' key='{1}' unsupported cache event", evt.Name, evt.Key);
                }
            }
        }


        private void PrintStats()
        {
            Console.WriteLine("[CacheWrapper] {0} stats (primary={1}/backup={2}/local={3})", _configuration.Name, _igniteCache.GetLocalSize(CachePeekMode.Primary), _igniteCache.GetLocalSize(CachePeekMode.Backup), _localCache.Count);
        }

        #region Implementation of IEnumerable

        public IEnumerator<KeyValuePair<TK, TV>> GetEnumerator()
        {
            return _localCache.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }




    public class CustomScanQueryResponseListener<TK, TV> : IMessageListener<KeyValuePair<TK, byte[]>>
    {
        private Action<KeyValuePair<TK, byte[]>> _action;

        public CustomScanQueryResponseListener(Action<KeyValuePair<TK, byte[]>> action)
        {
            _action = action;
        }

        #region Implementation of IMessageListener<in string>

        public bool Invoke(Guid nodeId, KeyValuePair<TK, byte[]> response)
        {
            _action(response);
            return true;
        }

        #endregion
    }


    [Serializable]
    public class CustomScanQueryTask<TK,TV> : IComputeFunc<int>
    {
        public String CacheName { get; set; }
        public ICacheEntryFilter<TK, TV> Predicate { get; set; }
        public string Topic { get; set; }

        #region Implementation of IComputeAction

        public int Invoke()
        {
            var ignite = Ignition.TryGetIgnite();
            var cacheWrapperManager = CacheWrapperManager.GetOrAddCacheWrapperManager(ignite);
            var cacheWrapper = cacheWrapperManager.GetOrCreateCacheWrapper<TK, TV>(CacheName);
            var messaging = ignite.GetMessaging();

            var resCount = 0;

            //Console.WriteLine("Executing CustomScanQueryTask");
            var scanSw = new Stopwatch();
            var sendSw = new Stopwatch();
            var totalSw = new Stopwatch();
            totalSw.Start();
            scanSw.Start();
            foreach (var kvp in cacheWrapper.Where(kvp => Predicate.Invoke(new CacheEntry<TK, TV>(kvp))))
            {
                scanSw.Stop();

                //Console.WriteLine("Matching value = {0}", kvp.Key);
                if (Topic != null)
                {
                    sendSw.Start();
                    messaging.Send(new KeyValuePair<TK, byte[]>(kvp.Key, cacheWrapper.IgniteCache.Get(kvp.Key)), Topic);
                    sendSw.Stop();
                }
                resCount++;

                scanSw.Start();
            }
            scanSw.Stop();
            totalSw.Stop();
            
            //Console.WriteLine("[CacheWrapper] Query execution summary : Scan={0}, Send={1}, Total={2}, MatchCount={3}", scanSw.Elapsed, sendSw.Elapsed, totalSw.Elapsed, resCount);
            return resCount;
        }

        #endregion
    }

    public class CacheEntry<TK, TV> : ICacheEntry<TK, TV>
    {
        private KeyValuePair<TK, TV> _kvp;

        public CacheEntry(KeyValuePair<TK, TV> kvp)
        {
            _kvp = kvp;
        }

        #region Implementation of ICacheEntry<out TK,out TV>

        public TK Key { get { return _kvp.Key; } }
        public TV Value { get { return _kvp.Value; } }

        #endregion
    }
}
