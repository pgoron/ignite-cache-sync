using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;

namespace CacheWrapper
{
    public class Serializer
    {
        public static TV ByteArrayToObject<TV>(byte[] data)
        {
            var ms = new MemoryStream(data);
            var compressionStream = new GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
            var bf = new BinaryFormatter();
            return (TV) bf.Deserialize(compressionStream);
        }

        public static byte[] ObjectToByteArray<TV>(TV value)
        {
            var ms = new MemoryStream();
            var compressionStream = new GZipStream(ms, System.IO.Compression.CompressionMode.Compress);
            var bf = new BinaryFormatter();
            bf.Serialize(compressionStream, value);
            compressionStream.Dispose();
            return ms.ToArray();
        }
    }
}