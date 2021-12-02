using System;

namespace Orleans
{
    /// <summary>Defines the state of a grain</summary>
    public interface IGrainState<T>
    {
        string Name { get; set; }

        T State { get; set; }

        /// <summary>An e-tag that allows optimistic concurrency checks at the storage provider level.</summary>
        string ETag { get; set; }

        bool RecordExists { get; set; }
    }

    /// <summary>
    /// Default implementation of <see cref="IGrainState{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of application level payload.</typeparam>
    [Serializable]
    [GenerateSerializer]
    public class GrainState<T> : IGrainState<T>
    {
        [Id(0)]
        public string Name { get; set; }

        [Id(1)]
        public T State { get; set; }

        /// <inheritdoc />
        [Id(2)]
        public string ETag { get; set; }

        [Id(3)]
        public bool RecordExists { get; set; }

        /// <summary>Initializes a new instance of <see cref="GrainState{T}"/>.</summary>
        /// <param name="name">The name of the state</param>
        public GrainState(string name) : this(name, default)
        {
        }

        /// <summary>Initializes a new instance of <see cref="GrainState{T}"/>.</summary>
        /// <param name="name">The name of the state</param>
        /// <param name="state"> The initial value of the state.</param>
        public GrainState(string name, T state) : this(name, state, null)
        {
        }

        /// <summary>Initializes a new instance of <see cref="GrainState{T}"/>.</summary>
        /// <param name="name">The name of the state</param>
        /// <param name="state">The initial value of the state.</param>
        /// <param name="eTag">The initial e-tag value that allows optimistic concurrency checks at the storage provider level.</param>
        public GrainState(string name, T state, string eTag)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            Name = name;
            State = state;
            ETag = eTag;
        }
    }
}
