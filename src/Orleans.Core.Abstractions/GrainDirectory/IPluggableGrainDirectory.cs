using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    public interface IPluggableGrainDirectory
    {
        Task<GrainAddress> Register(GrainAddress address);

        Task Unregister(GrainAddress address);

        Task<List<GrainAddress>> Lookup(string grainId);
    }

    public class GrainAddress
    {
        public string SiloAddress { get; set; }
        public string GrainId { get; set; }
        public string ActivationId { get; set; }
    }
}
