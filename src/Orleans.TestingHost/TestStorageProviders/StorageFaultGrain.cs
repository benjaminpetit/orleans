
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.TestingHost
{
    /// <summary>
    /// Grain that tracks storage exceptions to be injected.
    /// </summary>
    public class StorageFaultGrain : Grain, IStorageFaultGrain
    {
        private ILogger logger;
        private Dictionary<GrainId, Exception> readFaults;
        private Dictionary<GrainId, Exception> writeFaults;
        private Dictionary<GrainId, Exception> clearfaults;

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        /// It is called before any messages have been dispatched to the grain.
        /// For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            logger = this.ServiceProvider.GetService<ILoggerFactory>().CreateLogger($"{typeof (StorageFaultGrain).FullName}-{IdentityString}-{RuntimeIdentity}");
            readFaults = new Dictionary<GrainId, Exception>();
            writeFaults = new Dictionary<GrainId, Exception>();
            clearfaults = new Dictionary<GrainId, Exception>();
            logger.Info("Activate.");
        }

        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain reads state from a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        public Task AddFaultOnRead(GrainId grainId, Exception exception)
        {
            readFaults.Add(grainId, exception);
            logger.Info($"Added ReadState fault for {grainId}.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain writes state to a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        public Task AddFaultOnWrite(GrainId grainId, Exception exception)
        {
            writeFaults.Add(grainId, exception);
            logger.Info($"Added WriteState fault for {grainId}.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Adds a storage exception to be thrown when the referenced grain clears state in a storage provider
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        public Task AddFaultOnClear(GrainId grainId, Exception exception)
        {
            clearfaults.Add(grainId, exception);
            logger.Info($"Added ClearState fault for {grainId}.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for reading.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        public Task OnRead(GrainId grainId)
        {
            Exception exception;
            if (readFaults.TryGetValue(grainId, out exception))
            {
                readFaults.Remove(grainId);
                throw exception;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for writing.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        public Task OnWrite(GrainId grainId)
        {
            Exception exception;
            if (writeFaults.TryGetValue(grainId, out exception))
            {
                writeFaults.Remove(grainId);
                throw exception;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Throws a storage exception if one has been added for the grain reference for clearing state.
        /// </summary>
        /// <param name="grainId"></param>
        /// <returns></returns>
        public Task OnClear(GrainId grainId)
        {
            Exception exception;
            if (clearfaults.TryGetValue(grainId, out exception))
            {
                clearfaults.Remove(grainId);
                throw exception;
            }
            return Task.CompletedTask;
        }
    }
}
