using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Orleans.Serialization;

namespace Orleans.Storage
{
    public class OrleansGrainStorageSerializer : IGrainStorageSerializer
    {
        private static List<string> supportedTags => new List<string> { WellKnownSerializerTag.Binary };

        private readonly SerializationManager serializationManager;

        public List<string> SupportedTags => supportedTags;

        public OrleansGrainStorageSerializer(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        public string Serialize(Type t, object value, out BinaryData output)
        {
#if NETCOREAPP
            var buffer = new System.Buffers.ArrayBufferWriter<byte>();
#else
            var buffer = new Orleans.Serialization.ArrayBufferWriter<byte>();
#endif
            var writer = new BinaryTokenStreamWriter2<IBufferWriter<byte>>(buffer);
            this.serializationManager.Serialize(value, writer);
            writer.Commit();
            output = new BinaryData(buffer.WrittenMemory);
            return WellKnownSerializerTag.Binary;
        }

        public object Deserialize(Type expected, BinaryData input, string tag)
        {
            if (!tag.Equals(WellKnownSerializerTag.Binary, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException($"Unsupported tag '{tag}'", nameof(tag));
            }

            var reader = new BinaryTokenStreamReader2(new ReadOnlySequence<byte>(input.ToMemory()));
            return this.serializationManager.Deserialize(reader);
        }
    }

    public class JsonGrainStorageSerializer : IGrainStorageSerializer
    {
        private static List<string> supportedTags => new List<string> { WellKnownSerializerTag.Json };

        public List<string> SupportedTags => supportedTags;

        public string Serialize(Type t, object value, out BinaryData output)
        {
            var data = JsonConvert.SerializeObject(value);
            output = new BinaryData(data);
            return WellKnownSerializerTag.Json;
        }

        public object Deserialize(Type expected, BinaryData input, string tag)
        {
            if (!tag.Equals(WellKnownSerializerTag.Json, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException($"Unsupported tag '{tag}'", nameof(tag));
            }

            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(input.ToArray()));
        }
    }

    public class GrainStorageSerializer : IGrainStorageSerializer
    {
        private readonly IGrainStorageSerializer serializer;
        private readonly Dictionary<string, IGrainStorageSerializer> deserializers = new Dictionary<string, IGrainStorageSerializer>();

        public List<string> SupportedTags => this.deserializers.Keys.ToList();

        public GrainStorageSerializer(IGrainStorageSerializer serializer, params IGrainStorageSerializer[] fallbackDeserializers)
        {
            this.serializer = serializer;
            InsertDeserializer(serializer);

            foreach (var deserializer in fallbackDeserializers)
            {
                InsertDeserializer(deserializer);
            }

            void InsertDeserializer(IGrainStorageSerializer deserializer)
            {
                foreach (var tag in deserializer.SupportedTags)
                {
                    this.deserializers[tag] = deserializer;
                }
            }
        }

        public string Serialize(Type t, object value, out BinaryData output) => this.serializer.Serialize(t, value, out output);

        public object Deserialize(Type expected, BinaryData input, string tag)
        {
            if (!this.deserializers.TryGetValue(tag, out var deserializer))
            {
                throw new ArgumentException($"Unsupported tag '{tag}'", nameof(tag));
            }

            return deserializer.Deserialize(expected, input, tag);
        }
    }
}
