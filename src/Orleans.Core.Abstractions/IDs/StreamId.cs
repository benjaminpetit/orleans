using System;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Text;
using Orleans.Concurrency;
using Orleans.Streams;

namespace Orleans.Runtime
{
    [Immutable]
    [Serializable]
    [StructLayout(LayoutKind.Auto)]
    public readonly struct StreamId : IEquatable<StreamId>, IComparable<StreamId>, ISerializable
    {
        private readonly ushort keyIndex;
        private readonly uint hash;

        public ReadOnlyMemory<byte> FullKey { get; }

        public ReadOnlyMemory<byte> Namespace => FullKey.Slice(0, this.keyIndex);

        public ReadOnlyMemory<byte> Key => FullKey.Slice(this.keyIndex, FullKey.Length - this.keyIndex);

        internal StreamId(Memory<byte> fullKey, ushort keyIndex, uint hash)
        {
            FullKey = fullKey;
            this.keyIndex = keyIndex;
            this.hash = hash;
        }

        internal StreamId(Memory<byte> fullKey, ushort keyIndex)
            : this(fullKey, keyIndex, JenkinsHash.ComputeHash(fullKey.ToArray()))
        {
        }

        private StreamId(SerializationInfo info, StreamingContext context)
        {
            FullKey = new Memory<byte>((byte[]) info.GetValue("fk", typeof(byte[])));
            this.keyIndex = (ushort) info.GetValue("ki", typeof(ushort));
            this.hash = (uint) info.GetValue("fh", typeof(uint));
        }


        public static StreamId Create(byte[] ns, byte[] key)
        {
            var fullKeysBytes = new byte[ns.Length + key.Length];
            Array.Copy(ns, 0, fullKeysBytes, 0, ns.Length);
            Array.Copy(key, 0, fullKeysBytes, ns.Length, key.Length);
            return new StreamId(fullKeysBytes, (ushort) ns.Length);
        }

        public static StreamId Create(string ns, Guid key) => Create(Encoding.UTF8.GetBytes(ns), key.ToByteArray());

        public static StreamId Create(string ns, string key) => Create(Encoding.UTF8.GetBytes(ns), Encoding.UTF8.GetBytes(key));

        public static StreamId Create(IStreamIdentity streamIdentity) => Create(streamIdentity.Namespace, streamIdentity.Guid);

        public int CompareTo(StreamId other) => FullKey.Span.SequenceCompareTo(other.FullKey.Span);

        public bool Equals(StreamId other) => FullKey.Span.SequenceEqual(other.FullKey.Span);

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("fk", FullKey.ToArray());
            info.AddValue("ki", this.keyIndex);
            info.AddValue("fh", this.hash);
        }
    }

    [Immutable]
    [Serializable]
    [StructLayout(LayoutKind.Auto)]
    internal readonly struct InternalStreamId
    {
        public string ProviderName { get; }

        public StreamId StreamId { get; }

        public InternalStreamId(string providerName, StreamId streamId)
        {
            ProviderName = providerName;
            StreamId = streamId;
        }

        public static implicit operator StreamId(InternalStreamId internalStreamId) => internalStreamId.StreamId;
    }

    // TODO bpetit remove
    public static class StreamIdExtensions
    {
        public static Guid GetGuid(this StreamId streamId) => new Guid(streamId.Key.ToArray());

        public static string GetNamespace(this StreamId streamId) => Encoding.UTF8.GetString(streamId.Namespace.ToArray());

        internal static Guid GetGuid(this InternalStreamId internalStreamId) => new Guid(internalStreamId.StreamId.Key.ToArray());

        internal static string GetNamespace(this InternalStreamId internalStreamId) => Encoding.UTF8.GetString(internalStreamId.StreamId.Namespace.ToArray());
    }
}
