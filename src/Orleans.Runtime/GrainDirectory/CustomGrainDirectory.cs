using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.GrainDirectory;
using Orleans.Runtime.Providers;

namespace Orleans.Runtime.GrainDirectory
{
    internal class CustomGrainDirectory : ILocalGrainDirectory
    {
        private readonly IGrainDirectory directory;

        public CustomGrainDirectory(IGrainDirectory directory)
        {
            this.directory = directory;
        }

        public void Start()
        {
            // We don't need to do anything for now
        }

        public Task Stop(bool doOnStopHandoff)
        {
            // We don't need to do anything for now
            return Task.CompletedTask;
        }

        public void RegisterSystemTargets(SiloProviderRuntime siloProviderRuntime, ILogger logger)
        {
            // We don't need to do anything for now
        }

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // We don't need to do anything for now
        }

        public List<ActivationAddress> GetLocalCacheData(GrainId grain)
        {
            // No cache for now
            return null;
        }

        public AddressesAndTag GetLocalDirectoryData(GrainId grain)
        {
            // No data stored locally except for cache
            return new AddressesAndTag { Addresses = this.GetLocalCacheData(grain) };
        }

        public void InvalidateCacheEntry(ActivationAddress activation, bool invalidateDirectoryAlso = false)
        {
            // No cache for now
        }

        public bool IsSiloInCluster(SiloAddress silo)
        {
            // Multicluster directory not supported with PluggableGrainDirectory ; assume all silos
            // are in the same cluster
            return true;
        }

        public bool LocalLookup(GrainId grain, out AddressesAndTag addresses)
        {
            // No data stored locally except for cache
            addresses = this.GetLocalDirectoryData(grain);

            if (addresses.Addresses == null)
            {
                return false;
            }

            return true;
        }

        public async Task<AddressesAndTag> LookupAsync(GrainId grainId, int hopCount = 0)
        {
            var result = await this.directory.Lookup(grainId.ToParsableString());

            if (result.IsFound)
                return result.DirectoryEntry.ToAddressesAndTag();
            else
                return new AddressesAndTag();
        }

        public async Task<AddressAndTag> RegisterAsync(ActivationAddress address, bool singleActivation, int hopCount = 0)
        {
            var entry = ActivationAddressHelper.ToEntry(address);
            var result = await this.directory.Register(entry);

            switch (result)
            {
                case SuccessResult success:
                    return success.DirectoryEntry.ToAddressAndTag();
                case RedirectResult redirect:
                    return redirect.DirectoryEntry.ToAddressAndTag();
                case FailureResult failure:
                    throw failure.Exception;
                default:
                    throw new Exception($"Unhandled result type: {result.GetType()}");
            }
        }

        public async Task UnregisterAsync(ActivationAddress address, UnregistrationCause cause, int hopCount = 0)
        {
            await this.directory.Unregister(ActivationAddressHelper.ToEntry(address));
            var result = await this.directory.Unregister(address.ToEntry());

            if (result is FailureResult failure)
            {
                throw failure.Exception;
            }
        }

        public Task UnregisterAfterNonexistingActivation(ActivationAddress address, SiloAddress origin)
        {
            return this.UnregisterAsync(address, UnregistrationCause.NonexistentActivation);
        }

        public async Task UnregisterManyAsync(List<ActivationAddress> addresses, UnregistrationCause cause, int hopCount = 0)
        {
            var results = await this.directory.Unregister(addresses.ToEntries());

            List<FailureResult> failures = null;
            foreach (var r in results)
            {
                if (r is FailureResult failure)
                {
                    if (failures == null)
                        failures = new List<FailureResult>();
                    failures.Add(failure);
                }
            }

            if (failures != null)
            {
                throw new AggregateException(failures.Select(f => f.Exception));
            }
        }

        public Task DeleteGrainAsync(GrainId grainId, int hopCount = 0)
        {
            throw new NotImplementedException();
        }

        public Task<AddressesAndTag> LookupInCluster(GrainId grain, string clusterId)
        {
            throw new NotImplementedException("PluggableGrainDirectory does not support multi cluster");
        }
    }
}
