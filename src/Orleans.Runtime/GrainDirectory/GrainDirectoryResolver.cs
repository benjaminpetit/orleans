using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Orleans.Utilities;

namespace Orleans.Runtime.GrainDirectory
{
    internal interface IGrainDirectoryResolver
    {
        IGrainDirectory ResolveGrainDirectory(GrainId grainId);
    }

    internal class GrainDirectoryResolver : IGrainDirectoryResolver
    {
        private readonly CachedReadConcurrentDictionary<int, IGrainDirectory> directoryPerTypeCode = new CachedReadConcurrentDictionary<int, IGrainDirectory>();
        private readonly CachedReadConcurrentDictionary<string, IGrainDirectory> directoryPerName = new CachedReadConcurrentDictionary<string, IGrainDirectory>();
        private readonly IServiceProvider serviceProvider;
        private readonly GrainTypeManager grainTypeManager;

        public GrainDirectoryResolver(IServiceProvider serviceProvider, GrainTypeManager grainTypeManager)
        {
            this.serviceProvider = serviceProvider;
            this.grainTypeManager = grainTypeManager;
        }

        public IGrainDirectory ResolveGrainDirectory(GrainId grainId) => this.directoryPerTypeCode.GetOrAdd(grainId.TypeCode, GetGrainDirectoryByTypeCode);

        private IGrainDirectory GetGrainDirectoryByTypeCode(int typeCode)
        {
            if (this.grainTypeManager.ClusterGrainInterfaceMap.TryGetDirectory(typeCode, out var directoryName))
            {
                throw new OrleansException($"Unexpected: Cannot find an implementation class for grain interface {typeCode}");
            }
            return this.directoryPerName.GetOrAdd(directoryName, GetGrainDirectoryByName);
        }

        private IGrainDirectory GetGrainDirectoryByName(string directoryName)
        {
            var directory = this.serviceProvider.GetServiceByName<IGrainDirectory>(directoryName);
            if (directory == default)
            {
                throw new OrleansException($"Unexpected: Cannot find a directory named \"{directoryName}\"");
            }
            return directory;
        }
    }
}
