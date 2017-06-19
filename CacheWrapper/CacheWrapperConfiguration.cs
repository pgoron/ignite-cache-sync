using System;
using System.IO;
using Apache.Ignite.Core.Cache.Configuration;

namespace CacheWrapper
{
    [Serializable]
    public class CacheWrapperConfiguration : CacheConfiguration
    {
        public SerializationMode SerializationMode { get; set; }
        public CompressionMode CompressionMode { get; set; }
        public ISerializer CustomSerializer { get; set; }

        internal string KeyTypeFullName { get; set; }
        internal string ValueTypeFullName { get; set; }

        public CacheWrapperConfiguration()
        {
            SerializationMode = SerializationMode.DotNetSerializer;
            CompressionMode = CompressionMode.GZipCompression;
            CustomSerializer = null;
        }
    }

    public enum SerializationMode
    {
        DotNetSerializer,
        DataContractSerializer,
        NetDataContractSerializer,
        CustomSerializer
    }

    public enum CompressionMode
    {
        NoCompression,
        GZipCompression
    }

    public interface ISerializer
    {
        void Serialize(object Object, Stream stream);
        object Deserialize(Stream stream);
    }

}
