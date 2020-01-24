using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
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

        public CustomGrainLocator(IPluggableGrainDirectory grainDirectory)
        {
            this.grainDirectory = grainDirectory;
        }

        public async Task<List<ActivationAddress>> Lookup(GrainId grainId)
        {
            var entries = await this.grainDirectory.Lookup(grainId.ToParsableString());

            if (entries == null || entries.Count == 0)
                return new List<ActivationAddress>();

            return entries.Select(e => ConvertToActivationAddress(e)).ToList();
        }

        public async Task<ActivationAddress> Register(ActivationAddress address)
        {
            var result = await this.grainDirectory.Register(ConvertToGrainAddress(address));
            return ConvertToActivationAddress(result);
        }

        public bool TryLocalLookup(GrainId grainId, out List<ActivationAddress> addresses)
        {
            addresses = null;
            return false;
        }

        public async Task Unregister(ActivationAddress address, UnregistrationCause cause)
        {
            await this.grainDirectory.Unregister(ConvertToGrainAddress(address));
        }

        public async Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause)
        {
            var tasks = addresses.Select(addr => Unregister(addr, cause)).ToList();
            await Task.WhenAll(tasks);
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
