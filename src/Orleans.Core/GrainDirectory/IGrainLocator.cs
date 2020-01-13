using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{
    interface IGrainLocator
    {
        Task<AddressAndTag> Register(ActivationAddress address);

        Task Unregister(ActivationAddress address, UnregistrationCause cause);

        Task UnregisterMany(List<ActivationAddress> addresses, UnregistrationCause cause);

        Task<AddressesAndTag> Lookup(GrainId grainId);

        bool TryLocalLookup(GrainId grainId, out AddressesAndTag addresses);
    }
}
