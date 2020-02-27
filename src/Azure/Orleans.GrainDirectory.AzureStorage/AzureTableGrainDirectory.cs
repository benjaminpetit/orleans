using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory.AzureStorage.Utilities;
using Orleans.Runtime;

namespace Orleans.GrainDirectory.AzureStorage
{
    public class AzureTableGrainDirectory : IGrainDirectory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly AzureTableDataManager<GrainDirectoryEntity> tableDataManager;
        private readonly string clusterId;

        private class GrainDirectoryEntity : TableEntity
        {
            public string GrainId { get; set; }

            public string SiloAddress { get; set; }

            public string ActivationId { get; set; }

            public GrainAddress ToGrainAddress()
            {
                return new GrainAddress
                {
                    GrainId = this.GrainId,
                    SiloAddress = this.SiloAddress,
                    ActivationId = this.ActivationId,
                };
            }

            public static GrainDirectoryEntity FromGrainAddress(string clusterId, GrainAddress address)
            {
                return new GrainDirectoryEntity
                {
                    PartitionKey = clusterId,
                    RowKey = AzureTableUtils.SanitizeTableProperty(address.GrainId),
                    GrainId = address.GrainId,
                    SiloAddress = address.SiloAddress,
                    ActivationId = address.ActivationId,
                };
            }
        }

        public AzureTableGrainDirectory(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<AzureTableGrainDirectoryOptions> directoryOptions,
            ILoggerFactory loggerFactory)
        {
            this.tableDataManager = new AzureTableDataManager<GrainDirectoryEntity>(
                tableName: directoryOptions.Value.TableName,
                storageConnectionString: directoryOptions.Value.ConnectionString,
                loggerFactory: loggerFactory);
            this.clusterId = clusterOptions.Value.ClusterId;
        }

        public async Task<GrainAddress> Lookup(string grainId)
        {
            var result = await this.tableDataManager.ReadSingleTableEntryAsync(this.clusterId, AzureTableUtils.SanitizeTableProperty(grainId));

            if (result == null)
                return null;

            return result.Item1.ToGrainAddress();
        }

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            var entry = GrainDirectoryEntity.FromGrainAddress(this.clusterId, address);
            var result = await this.tableDataManager.InsertTableEntryAsync(entry);
            // Possible race condition?
            return result.isSuccess ? address : await Lookup(address.GrainId);
        }

        public async Task Unregister(GrainAddress address)
        {
            var result = await this.tableDataManager.ReadSingleTableEntryAsync(this.clusterId, AzureTableUtils.SanitizeTableProperty(address.GrainId));

            // No entry found
            if (result == null)
                return;

            // Check if the entry in storage match the one we were asked to delete
            var entity = result.Item1;
            if (entity.ActivationId == address.ActivationId)
                await this.tableDataManager.DeleteTableEntryAsync(GrainDirectoryEntity.FromGrainAddress(this.clusterId, address), entity.ETag);
        }

        // Called by lifecycle, should not be called explicitely, except for tests
        public async Task InitializeIfNeeded(CancellationToken ct = default)
        {
            await this.tableDataManager.InitTableAsync();
        }

        public void Participate(ISiloLifecycle lifecycle)
        {

            lifecycle.Subscribe(nameof(AzureTableGrainDirectory), ServiceLifecycleStage.RuntimeInitialize, InitializeIfNeeded);
        }
    }
}
