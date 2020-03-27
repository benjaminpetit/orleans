using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.GrainDirectory;
using Orleans.Utilities;

namespace Orleans.Runtime.GrainDirectory
{
    internal interface IGrainDirectoryResolver
    {
        IGrainDirectory Resolve(GrainId grainId);
    }

    internal class GrainDirectoryResolver : IGrainDirectoryResolver
    {
        private readonly Dictionary<string, IGrainDirectory> directoryPerName = new Dictionary<string, IGrainDirectory>();
        private readonly CachedReadConcurrentDictionary<int, IGrainDirectory> directoryPerTypeCode = new CachedReadConcurrentDictionary<int, IGrainDirectory>();
        private readonly GrainTypeManager grainTypeManager;

        public GrainDirectoryResolver(IServiceProvider serviceProvider, GrainTypeManager grainTypeManager)
        {
            this.grainTypeManager = grainTypeManager;

            // Load all registered directories
            var services = serviceProvider
                .GetRequiredService<IKeyedServiceCollection<string, IGrainDirectory>>()
                .GetServices(serviceProvider);
            foreach (var svc in services)
            {
                this.directoryPerName.Add(svc.Key, svc.GetService(serviceProvider));
            }
        }

        public IGrainDirectory Resolve(GrainId grainId) => this.directoryPerTypeCode.GetOrAdd(grainId.TypeCode, GetGrainDirectoryPerTypeCode);

        private IGrainDirectory GetGrainDirectoryPerTypeCode(int typeCode)
        {
            if (!this.grainTypeManager.ClusterGrainInterfaceMap.TryGetDirectory(typeCode, out var directoryName))
            {
                throw new OrleansException($"Unexpected: Cannot find an the directory for grain class {typeCode}");
            }

            if (string.IsNullOrEmpty(directoryName))
            {
                return default;
            }

            if (!this.directoryPerName.TryGetValue(directoryName, out var directory))
            {
                throw new OrleansException($"Unexpected: Cannot find an the directory named {directoryName}");
            }

            return directory;
        }
    }
}
