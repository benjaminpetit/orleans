using System;
using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Storage
{
    public class OrleansGrainStorageSerializer : IGrainStorageSerializer
    {
        private readonly SerializationManager serializationManager;

        public OrleansGrainStorageSerializer(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        public void Serialize(Type t, object value, IBufferWriter<byte> output)
        {
            var writer = new BinaryTokenStreamWriter2<IBufferWriter<byte>>(output);
            this.serializationManager.Serialize(value, writer);
            writer.Commit();
        }

        public object Deserialize(Type expected, ReadOnlySequence<byte> input)
        {
            var reader = new BinaryTokenStreamReader2(input);
            return this.serializationManager.Deserialize(reader);
        }
    }
}
