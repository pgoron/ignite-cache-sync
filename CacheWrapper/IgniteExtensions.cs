using System;
using Apache.Ignite.Core;

namespace CacheWrapper
{
    public static class IgniteExtensions
    {
        public static void AddCacheWrapperSupport(this IIgnite ignite)
        {
            CacheWrapperManager.GetOrAddCacheWrapperManager(ignite);
        }
        
        public static ICacheWrapper<TK, TV> GetOrCreateCacheWrapper<TK,TV>(this IIgnite ignite, CacheWrapperConfiguration cacheConfiguration)
        {
            var cacheWrapperManager = CacheWrapperManager.GetOrAddCacheWrapperManager(ignite);
            return cacheWrapperManager.GetOrCreateCacheWrapper<TK, TV>(cacheConfiguration);
        }
    }
}
