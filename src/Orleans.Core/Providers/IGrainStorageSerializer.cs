using System;
using System.Buffers;
using System.Collections.Generic;
using Orleans.Serialization;

namespace Orleans.Storage
{
    public static class WellKnownSerializerTag
    {
        public const string Binary = "binary";
        public const string Json = "json";
        public const string Xml = "xml";
        public const string Text = "text";
    }

    public interface IGrainStorageSerializer
    {
        List<string> SupportedTags { get; }

        string Serialize(Type t, object value, IBufferWriter<byte> output);

        object Deserialize(Type expected, ReadOnlySequence<byte> input, string tag);
    }

    public static class GrainStateSerializerExtensions
    {
        public static (string tag, ReadOnlyMemory<byte> output) Serialize(this IGrainStorageSerializer self, Type t, object value)
        {
#if NETCOREAPP
            var writer = new System.Buffers.ArrayBufferWriter<byte>();
#else
            var writer = new Orleans.Serialization.ArrayBufferWriter<byte>();
#endif
            var tag = self.Serialize(t, value, writer);
            return (tag, writer.WrittenMemory);
        }

        public static object Deserialize(this IGrainStorageSerializer self, Type expected, ReadOnlyMemory<byte> input, string tag)
        {
            return self.Deserialize(expected, new ReadOnlySequence<byte>(input), tag);
        }
    }
}
