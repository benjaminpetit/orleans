using System;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory.AzureStorage
{
    public class AzureTableGrainDirectory : IGrainDirectory
    {
        public Task<GrainAddress> Lookup(string grainId)
        {
            throw new NotImplementedException();
        }

        public Task<GrainAddress> Register(GrainAddress address)
        {
            throw new NotImplementedException();
        }

        public Task Unregister(GrainAddress address)
        {
            throw new NotImplementedException();
        }
    }
}
