using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    internal static class GrainLocatorFactory
    {
        public static IGrainLocator GetGrainLocator(IServiceProvider sp)
        {
            var customDirectory = sp.GetService<IPluggableGrainDirectory>();
            return customDirectory != null
                ? new CustomGrainLocator(customDirectory)
                : (IGrainLocator)new InClusterGrainLocator(sp.GetRequiredService<ILocalGrainDirectory>());
        }
    }

    internal class InClusterGrainLocator : IGrainLocator
    {
        private readonly ILocalGrainDirectory localGrainDirectory;

        public InClusterGrainLocator(ILocalGrainDirectory localGrainDirectory)
        {
            this.localGrainDirectory = localGrainDirectory;
        }

        public async Task<List<ActivationAddress>> Lookup(GrainId grainId)
            => (await this.localGrainDirectory.LookupAsync(grainId)).Addresses;

        public bool TryLocalLookup(GrainId grainId, out List<ActivationAddress> addresses)
        {
            if (this.localGrainDirectory.LocalLookup(grainId, out var addressesAndTag))
            {
                addresses = addressesAndTag.Addresses;
                return true;
            }
            addresses = null;
            return false;
        }

        public async Task<ActivationAddress> Register(ActivationAddress address)
            => (await this.localGrainDirectory.RegisterAsync(address, singleActivation: true)).Address;

        public Task Unregister(ActivationAddress address, UnregistrationCause cause)
            => this.localGrainDirectory.UnregisterAsync(address, cause);

        public Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause)
            => this.localGrainDirectory.UnregisterManyAsync(addresses, cause);
    }

    internal class CustomGrainLocator : IGrainLocator
    {
        private readonly IPluggableGrainDirectory grainDirectory;

        private readonly IGrainDirectoryCache cache;

        public CustomGrainLocator(IPluggableGrainDirectory grainDirectory)
        {
            this.grainDirectory = grainDirectory;
            this.cache = new LRUBasedGrainDirectoryCache(GrainDirectoryOptions.DEFAULT_CACHE_SIZE, GrainDirectoryOptions.DEFAULT_MAXIMUM_CACHE_TTL);
        }

        public async Task<List<ActivationAddress>> Lookup(GrainId grainId)
        {
            var entries = await this.grainDirectory.Lookup(grainId.ToParsableString());

            if (entries == null || entries.Count == 0)
                return new List<ActivationAddress>();

            var results = entries.Select(e => ConvertToActivationAddress(e)).ToList();
            this.cache.AddOrUpdate(grainId, results.Select(item => Tuple.Create(item.Silo, item.Activation)).ToList(), 0);

            return results;
        }

        public async Task<ActivationAddress> Register(ActivationAddress address)
        {
            var result = await this.grainDirectory.Register(ConvertToGrainAddress(address));
            var activationAddress = ConvertToActivationAddress(result);
            this.cache.AddOrUpdate(
                activationAddress.Grain,
                new List<Tuple<SiloAddress,ActivationId>>() { Tuple.Create(activationAddress.Silo, activationAddress.Activation) },
                0);
            return activationAddress;
        }

        public bool TryLocalLookup(GrainId grainId, out List<ActivationAddress> addresses)
        {
            if (this.cache.LookUp(grainId, out var results))
            {
                addresses = results
                    .Select(tuple => ActivationAddress.GetAddress(tuple.Item1, grainId, tuple.Item2))
                    .ToList();
                return true;
            }

            addresses = null;
            return false;
        }

        public async Task Unregister(ActivationAddress address, UnregistrationCause cause)
        {
            await this.grainDirectory.Unregister(ConvertToGrainAddress(address));
            this.cache.Remove(address.Grain);
        }

        public async Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause)
        {
            var tasks = addresses.Select(addr => Unregister(addr, cause)).ToList();
            await Task.WhenAll(tasks);
            foreach (var addr in addresses)
                this.cache.Remove(addr.Grain);
        }

        private static ActivationAddress ConvertToActivationAddress(GrainAddress addr)
        {
            try
            {
                return ActivationAddress.GetAddress(
                        SiloAddress.FromParsableString(addr.SiloAddress),
                        GrainId.FromParsableString(addr.GrainId),
                        ActivationId.GetActivationId(UniqueKey.Parse(addr.ActivationId.AsSpan())));
            }
            catch (Exception)
            {

                throw;
            }
        }

        private static GrainAddress ConvertToGrainAddress(ActivationAddress addr)
        {
            return new GrainAddress
            {
                SiloAddress = addr.Silo.ToParsableString(),
                GrainId = addr.Grain.ToParsableString(),
                ActivationId = (addr.Activation.Key.ToHexString())
            };
        }
    }
}
