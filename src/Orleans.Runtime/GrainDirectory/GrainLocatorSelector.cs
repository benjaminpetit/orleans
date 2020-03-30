using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    /// <summary>
    /// Special IGrainLocator implementation that will choose between the DhtGrainLocator and the new GrainLocator
    /// This class will be removed once the DhtGrainLocator will be updated to support the IGrainDirectory interface
    /// </summary>
    internal class GrainLocatorSelector : IGrainLocator
    {
        private IGrainDirectoryResolver grainDirectoryResolver;
        private CachedGrainLocator cachedGrainLocator;
        private DhtGrainLocator dhtGrainLocator;

        public GrainLocatorSelector(IGrainDirectoryResolver grainDirectoryResolver, CachedGrainLocator cachedGrainLocator, DhtGrainLocator dhtGrainLocator)
        {
            this.grainDirectoryResolver = grainDirectoryResolver;
            this.cachedGrainLocator = cachedGrainLocator;
            this.dhtGrainLocator = dhtGrainLocator;
        }

        public Task<List<ActivationAddress>> Lookup(GrainId grainId) => GetGrainLocator(grainId).Lookup(grainId);

        public Task<ActivationAddress> Register(ActivationAddress address) => GetGrainLocator(address.Grain).Register(address);

        public bool TryLocalLookup(GrainId grainId, out List<ActivationAddress> addresses) => GetGrainLocator(grainId).TryLocalLookup(grainId, out addresses);

        public Task Unregister(ActivationAddress address, UnregistrationCause cause) => GetGrainLocator(address.Grain).Unregister(address, cause);

        public Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause)
        {
            var dhtLocatorActivations = new List<ActivationAddress>();
            var cachedLocatorActivations = new List<ActivationAddress>();

            foreach (var address in addresses)
            {
                if (IsUsingCustomGrainLocator(address.Grain))
                    cachedLocatorActivations.Add(address);
                else
                    dhtLocatorActivations.Add(address);
            }

            var cachedTask = cachedLocatorActivations.Count > 0
                ? this.cachedGrainLocator.UnregisterMany(cachedLocatorActivations, cause)
                : Task.CompletedTask;

            var dhtTask = dhtLocatorActivations.Count > 0
                ? this.dhtGrainLocator.UnregisterMany(dhtLocatorActivations, cause)
                : Task.CompletedTask;

            return Task.WhenAll(cachedTask, dhtTask);
        }

        private IGrainLocator GetGrainLocator(GrainId grainId)
        {
            return !grainId.IsClient && IsUsingCustomGrainLocator(grainId)
                ? (IGrainLocator) this.cachedGrainLocator
                : (IGrainLocator) this.dhtGrainLocator;
        }

        private bool IsUsingCustomGrainLocator(GrainId grainId) => this.grainDirectoryResolver.Resolve(grainId) == default;
    }
}
