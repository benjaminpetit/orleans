
using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.TestingHost
{
    /// <summary>
    /// Grain that tracks storage exceptions to be injected.
    /// </summary>
    public interface IStorageFaultGrain : IGrainWithStringKey
    {
        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain reads state from a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        Task AddFaultOnRead(GrainId grainId, Exception exception);
        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain writes state to a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        Task AddFaultOnWrite(GrainId grainId, Exception exception);
        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain clears state in a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        Task AddFaultOnClear(GrainId grainId, Exception exception);

        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for reading.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        Task OnRead(GrainId grainId);
        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for writing.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        Task OnWrite(GrainId grainId);
        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for clearing state.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        Task OnClear(GrainId grainId);
    }
}
