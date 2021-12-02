
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.TestingHost
{
    /// <summary>
    /// Options for fault injection grain storage
    /// </summary>
    public class FaultInjectionGrainStorageOptions
    {
        public static TimeSpan DEFAULT_LATENCY = TimeSpan.FromMilliseconds(10);
        /// <summary>
        /// Latency applied on storage operation
        /// </summary>
        public TimeSpan Latency { get; set; } = DEFAULT_LATENCY;
    }
    /// <summary>
    /// Fault injection decorator for storage providers.  This allows users to inject storage exceptions to test error handling scenarios.
    /// </summary>
    public class FaultInjectionGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly IGrainStorage realStorageProvider;
        private IGrainFactory grainFactory;
        private ILogger logger;
        private readonly FaultInjectionGrainStorageOptions options;
        
        /// <summary>
        /// Default constructor which creates the decorated storage provider
        /// </summary>
        public FaultInjectionGrainStorage(IGrainStorage realStorageProvider, string name, ILoggerFactory loggerFactory, 
            IGrainFactory grainFactory, FaultInjectionGrainStorageOptions faultInjectionOptions)
        {
            this.realStorageProvider = realStorageProvider;
            this.logger = loggerFactory.CreateLogger($"{this.GetType().FullName}.{name}");
            this.grainFactory = grainFactory;
            this.options = faultInjectionOptions;
        }

        private Task InsertDelay()
        {
            return Task.Delay(this.options.Latency);
        }
           
        /// <summary>Faults if exception is provided, otherwise calls through to  decorated storage provider.</summary>
        /// <returns>Completion promise for the Read operation on the specified grain.</returns>
        public async Task ReadStateAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            IStorageFaultGrain faultGrain = grainFactory.GetGrain<IStorageFaultGrain>(grainId.Type.ToString());
            try
            {
                await InsertDelay();
                await faultGrain.OnRead(grainId);
            }
            catch (Exception)
            {
                logger.Info($"Fault injected for ReadState for grain {grainId} of type {grainId.Type}, ");
                throw;
            }
            logger.Info($"ReadState for grain {grainId} of type {grainId.Type}");
            await realStorageProvider.ReadStateAsync(grainId, grainState);
        }

        /// <summary>Faults if exception is provided, otherwise calls through to  decorated storage provider.</summary>
        /// <returns>Completion promise for the Write operation on the specified grain.</returns>
        public async Task WriteStateAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            IStorageFaultGrain faultGrain = grainFactory.GetGrain<IStorageFaultGrain>(grainId.Type.ToString());
            try
            {
                await InsertDelay();
                await faultGrain.OnWrite(grainId);
            }
            catch (Exception)
            {
                logger.Info($"Fault injected for WriteState for grain {grainId} of type {grainId.Type}");
                throw;
            }
            logger.Info($"WriteState for grain {grainId} of type {grainId.Type}");
            await realStorageProvider.WriteStateAsync(grainId, grainState);
        }

        /// <summary>Faults if exception is provided, otherwise calls through to  decorated storage provider.</summary>
        /// <returns>Completion promise for the Delete operation on the specified grain.</returns>
        public async Task ClearStateAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            IStorageFaultGrain faultGrain = grainFactory.GetGrain<IStorageFaultGrain>(grainId.Type.ToString());
            try
            {
                await InsertDelay();
                await faultGrain.OnClear(grainId);
            }
            catch (Exception)
            {
                logger.Info($"Fault injected for ClearState for grain {grainId} of type {grainId.Type}");
                throw;
            }
            logger.Info($"ClearState for grain {grainId} of type {grainId.Type}");
            await realStorageProvider.ClearStateAsync(grainId, grainState);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            (realStorageProvider as ILifecycleParticipant<ISiloLifecycle>)?.Participate(lifecycle);
        }
    }

    /// <summary>
    /// Factory to create FaultInjectionGrainStorage
    /// </summary>
    public static class FaultInjectionGrainStorageFactory
    {
        /// <summary>Create FaultInjectionGrainStorage</summary>
        /// <param name="services"></param>
        /// <param name="name"></param>
        /// <param name="injectedGrainStorageFactory"></param>
        /// <returns></returns>
        public static IGrainStorage Create(IServiceProvider services, string name, Func<IServiceProvider, string, IGrainStorage> injectedGrainStorageFactory)
        {
            return new FaultInjectionGrainStorage(injectedGrainStorageFactory(services,name), name, services.GetRequiredService<ILoggerFactory>(), services.GetRequiredService<IGrainFactory>(),
                services.GetRequiredService<IOptionsMonitor<FaultInjectionGrainStorageOptions>>().Get(name));
        }
    }
}
