using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    internal class GrainLocatorShim : IGrainLocator
    {
        private readonly ILocalGrainDirectory localGrainDirectory;

        public GrainLocatorShim(ILocalGrainDirectory localGrainDirectory)
        {
            this.localGrainDirectory = localGrainDirectory;
        }

        public Task<AddressesAndTag> Lookup(GrainId grainId)
            => this.localGrainDirectory.LookupAsync(grainId);

        public Task<AddressAndTag> Register(ActivationAddress address)
            => this.localGrainDirectory.RegisterAsync(address, singleActivation: true);

        public Task Unregister(ActivationAddress address, UnregistrationCause cause)
            => this.localGrainDirectory.UnregisterAsync(address, cause);

        public Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause)
            => this.localGrainDirectory.UnregisterManyAsync(addresses, cause);
    }
}
