using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;

namespace Orleans.Persistence.AzureStorage.Directory
{
    internal class AzureTableGrainDirectory : IPluggableGrainDirectory
    {
        private readonly AzureTableDataManager<DynamicTableEntity> tableDataManager;
        private readonly string clusterId;

        public AzureTableGrainDirectory(
            AzureTableStorageOptions options,
            ClusterOptions clusterOptions,
            ILoggerFactory loggerFactory)
        {
            this.tableDataManager = new AzureTableDataManager<DynamicTableEntity>("Directory", options.ConnectionString, loggerFactory);
            this.clusterId = clusterOptions.ClusterId;
        }

        public async Task<List<GrainAddress>> Lookup(string grainId)
        {
            var entry = (await this.tableDataManager.ReadSingleTableEntryAsync(this.clusterId, grainId)).Item1;
            var address = new GrainAddress
            {
                GrainId = grainId,
                SiloAddress = entry["SiloAddress"].StringValue,
                ActivationId = entry["ActivationId"].StringValue
            };
            return new List<GrainAddress> { address };
        }

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            var properties = new Dictionary<string, EntityProperty>();
            properties.Add("SiloAddress", new EntityProperty(address.SiloAddress));
            properties.Add("ActivationId", new EntityProperty(address.ActivationId));
            var entry = new DynamicTableEntity(this.clusterId, address.GrainId, "*", properties);

            try
            {
                await this.tableDataManager.InsertTableEntryAsync(entry);
            }
            catch (Exception)
            {
                return (await Lookup(address.GrainId)).FirstOrDefault();
            }

            return address;
        }

        public async Task Unregister(GrainAddress address)
        {
            var properties = new Dictionary<string, EntityProperty>();
            properties.Add("SiloAddress", new EntityProperty(address.SiloAddress));
            properties.Add("ActivationId", new EntityProperty(address.ActivationId));
            var entry = new DynamicTableEntity(this.clusterId, address.GrainId, "*", properties);

            await this.tableDataManager.DeleteTableEntryAsync(entry, "*");
        }
    }
}
