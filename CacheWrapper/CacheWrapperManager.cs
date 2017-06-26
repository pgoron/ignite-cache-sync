using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Events;

namespace CacheWrapper
{
    public class CacheWrapperManager
    {
        private const string CacheWrapperManagerInstallKey = "cachewrapper-manager";
        private const string CacheWrapperConfCacheName = "cachewrapper-conf";

        private static readonly ConcurrentDictionary<IIgnite, CacheWrapperManager> _cacheWrapperManagers = new ConcurrentDictionary<IIgnite, CacheWrapperManager>();
        private readonly ConcurrentDictionary<string, Lazy<ICacheWrapper>> _cacheWrappers;

        private readonly IIgnite _ignite;
        private readonly ICache<string, CacheWrapperConfiguration> _cacheWrapperConfCache;

        internal static CacheWrapperManager GetOrAddCacheWrapperManager(IIgnite ignite)
        {
            return _cacheWrapperManagers.GetOrAdd(ignite, i => new CacheWrapperManager(i));
        }

        private CacheWrapperManager(IIgnite ignite)
        {
            Console.WriteLine("[CacheWrapperManager] initializing a new CacheWrapperManagr");
            _cacheWrappers = new ConcurrentDictionary<string, Lazy<ICacheWrapper>>();
            _ignite = ignite;

            //var events = _ignite.GetEvents();
            //events.EnableLocal(EventType.CacheStarted, EventType.CacheStopped, EventType.CacheRebalanceStopped, EventType.CacheObjectPut, EventType.CacheObjectRemoved, EventType.CacheRebalanceObjectLoaded, EventType.CacheRebalanceObjectUnloaded);
            //events.LocalListen(new CacheStartedStoppedEventListener(this), EventType.CacheStarted, EventType.CacheStopped);
            //events.LocalListen(new CacheRebalanceStoppedEventListener(this), EventType.CacheRebalanceStopped);
            //events.LocalListen(new CacheObjectPutRemovedEventListener(this), EventType.CacheObjectPut, EventType.CacheObjectRemoved, EventType.CacheRebalanceObjectLoaded, EventType.CacheRebalanceObjectUnloaded);

            _cacheWrapperConfCache = _ignite.GetOrCreateCache<string, CacheWrapperConfiguration>(new CacheConfiguration(CacheWrapperConfCacheName)
            {
                CacheMode = CacheMode.Replicated,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync
            });

            foreach (var cacheEntry in _cacheWrapperConfCache)
            {
                if (!_cacheWrappers.ContainsKey(cacheEntry.Key))
                {
                    OnCacheStartedEvent(cacheEntry.Key);
                }
            }
        }

        public ICacheWrapper<TK, TV> GetOrCreateCacheWrapper<TK, TV>(string cacheName)
        {
            CacheWrapperConfiguration cacheWrapperConfiguration;
            if (_cacheWrapperConfCache.TryGet(cacheName, out cacheWrapperConfiguration))
            {
                return (ICacheWrapper<TK, TV>) GetOrCreateLocalCacheWrapper(cacheWrapperConfiguration);
            }
            return null;
        }

        public ICacheWrapper<TK, TV> GetOrCreateCacheWrapper<TK, TV>(CacheWrapperConfiguration cacheWrapperConfiguration)
        {
            cacheWrapperConfiguration.KeyTypeFullName = typeof (TK).AssemblyQualifiedName;
            cacheWrapperConfiguration.ValueTypeFullName = typeof (TV).AssemblyQualifiedName;

            // save wrapper configuration and notify cluster members
            var previousCacheWrapperConfiguration = _cacheWrapperConfCache.GetAndPutIfAbsent(cacheWrapperConfiguration.Name, cacheWrapperConfiguration).Value;
            if (previousCacheWrapperConfiguration != null)
            {
                if (previousCacheWrapperConfiguration.KeyTypeFullName != cacheWrapperConfiguration.KeyTypeFullName
                    || previousCacheWrapperConfiguration.ValueTypeFullName != cacheWrapperConfiguration.ValueTypeFullName)
                {
                    throw new InvalidOperationException("Incompatible Key and/or Value type for CacheWrapper " +
                                                        cacheWrapperConfiguration.Name);
                }
                cacheWrapperConfiguration = previousCacheWrapperConfiguration;
            }
            else
            {
                Console.WriteLine("[CacheWrapperManager] CacheWrapper={0} declared in cluster.", cacheWrapperConfiguration.Name);
            }

            return (ICacheWrapper<TK, TV>) GetOrCreateLocalCacheWrapper(cacheWrapperConfiguration);
        }

        private ICacheWrapper GetOrCreateLocalCacheWrapper(CacheWrapperConfiguration cacheWrapperConfiguration)
        {
            return _cacheWrappers.GetOrAdd(cacheWrapperConfiguration.Name, s => new Lazy<ICacheWrapper>(() =>
            {
                var cacheWrapperType = typeof(CacheWrapper<,>).MakeGenericType(
                    Type.GetType(cacheWrapperConfiguration.KeyTypeFullName), Type.GetType(cacheWrapperConfiguration.ValueTypeFullName));

                var method = typeof(IIgnite).GetMethod("GetOrCreateCache", new[] { typeof(CacheConfiguration) });
                method = method.MakeGenericMethod(Type.GetType(cacheWrapperConfiguration.KeyTypeFullName), typeof(byte[]));
                var igniteCache = method.Invoke(_ignite, new object[] { cacheWrapperConfiguration });

                var cacheWrapper = (ICacheWrapper)Activator.CreateInstance(cacheWrapperType, cacheWrapperConfiguration, igniteCache);

                Console.WriteLine("[CacheWrapperManager] local CacheWrapper={0} created.", cacheWrapperConfiguration.Name);
                Console.WriteLine(new StackTrace());

                return cacheWrapper;
            })).Value;
        }

        internal void OnCacheRebalanceStoppedEvent(string cacheName)
        {
            Lazy<ICacheWrapper> cacheWrapper;
            if (_cacheWrappers.TryGetValue(cacheName, out cacheWrapper))
            {
                Console.WriteLine("[CacheWrapperManager] Syncing cache wrapper {0} due a CacheRebalancedStoppedEvent", cacheName);
                ThreadPool.QueueUserWorkItem(state => cacheWrapper.Value.Sync());
            }
        }

        internal void OnCacheStartedEvent(string cacheName)
        {
            CacheWrapperConfiguration cacheWrapperConfiguration;
            if (_cacheWrapperConfCache.TryGet(cacheName, out cacheWrapperConfiguration))
            {
                Console.WriteLine("[CacheWrapperManager] Found existing cache wrapper {0}, creating local cache.", cacheName);
                var cacheWrapper = GetOrCreateLocalCacheWrapper(cacheWrapperConfiguration);

                ThreadPool.QueueUserWorkItem(state => cacheWrapper.Sync());
            }
        }

        internal void OnCacheStoppedEvent(string cacheName)
        {
            Lazy<ICacheWrapper> cacheWrapper;
            if (_cacheWrappers.TryRemove(cacheName, out cacheWrapper))
            {
                Console.WriteLine("[CacheWrapperManager] cacheName={0} stopped.");
            }
        }

        internal void OnCacheEvent(CacheEvent evt)
        {
            CacheWrapperConfiguration cacheWrapperConfiguration;
            if (_cacheWrapperConfCache.TryGet(evt.CacheName, out cacheWrapperConfiguration))
            {
                var cacheWrapper = GetOrCreateLocalCacheWrapper(cacheWrapperConfiguration);

                cacheWrapper.HandleCacheEvent(evt);
            }
        }
    }

    class CacheStartedStoppedEventListener : IEventListener<CacheEvent>
    {
        private readonly CacheWrapperManager _cacheWrapperManager;

        public CacheStartedStoppedEventListener(CacheWrapperManager cacheWrapperManager)
        {
            _cacheWrapperManager = cacheWrapperManager;
        }

        #region Implementation of IEventListener<in CacheEvent>
        public bool Invoke(CacheEvent evt)
        {
            Console.WriteLine("[CacheWrapperManager] {0}", evt);

            if (evt.Type == EventType.CacheStarted)
            {
                _cacheWrapperManager.OnCacheStartedEvent(evt.CacheName);
            }
            else if (evt.Type == EventType.CacheStopped)
            {
                _cacheWrapperManager.OnCacheStoppedEvent(evt.CacheName);
            }

            return true;
        }
        #endregion
    }

    class CacheRebalanceStoppedEventListener : IEventListener<CacheRebalancingEvent>
    {
        private readonly CacheWrapperManager _cacheWrapperManager;

        public CacheRebalanceStoppedEventListener(CacheWrapperManager cacheWrapperManager)
        {
            _cacheWrapperManager = cacheWrapperManager;
        }

        #region Implementation of IEventListener<in CacheRebalancingEvent>
        public bool Invoke(CacheRebalancingEvent evt)
        {
            Console.WriteLine("[CacheWrapperManager] {0}", evt);

            if (evt.Type == EventType.CacheRebalanceStopped)
            {
                _cacheWrapperManager.OnCacheRebalanceStoppedEvent(evt.CacheName);
            }
            return true;
        }
        #endregion
    }

    class CacheObjectPutRemovedEventListener : IEventListener<CacheEvent>
    {
        private readonly CacheWrapperManager _cacheWrapperManager;

        public CacheObjectPutRemovedEventListener(CacheWrapperManager cacheWrapperManager)
        {
            _cacheWrapperManager = cacheWrapperManager;
        }

        #region Implementation of IEventListener<in CacheEvent>
        public bool Invoke(CacheEvent evt)
        {   
            _cacheWrapperManager.OnCacheEvent(evt);

            return true;
        }
        #endregion
    }
}
