using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;
using Orleans.Hosting;

namespace Orleans.Persistence.AzureStorage.Directory
{
    internal class AzureTableGrainDirectory : IPluggableGrainDirectory
    {
        private readonly AzureTableDataManager<DynamicTableEntity> tableDataManager;
        private readonly string clusterId;
        private bool isInitialized = false;

        public AzureTableGrainDirectory(
            IOptions<AzureTableGrainDirectoryOptions> options,
            IOptions<ClusterOptions> clusterOptions,
            ILoggerFactory loggerFactory)
        {
            this.tableDataManager = new AzureTableDataManager<DynamicTableEntity>("Directory", options.Value.ConnectionString, loggerFactory);
            this.clusterId = clusterOptions.Value.ClusterId;
        }

        public async Task<List<GrainAddress>> Lookup(string grainId)
        {
            await InitializeIfNeeded();

            var entry = (await this.tableDataManager.ReadSingleTableEntryAsync(this.clusterId, grainId))?.Item1;

            if (entry == null)
                return null;

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
            await InitializeIfNeeded();

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
            await InitializeIfNeeded();

            var properties = new Dictionary<string, EntityProperty>();
            properties.Add("SiloAddress", new EntityProperty(address.SiloAddress));
            properties.Add("ActivationId", new EntityProperty(address.ActivationId));
            var entry = new DynamicTableEntity(this.clusterId, address.GrainId, "*", properties);

            await this.tableDataManager.DeleteTableEntryAsync(entry, "*");
        }

        private async ValueTask InitializeIfNeeded()
        {
            if (this.isInitialized)
                return;
            await this.tableDataManager.InitTableAsync();
        }
    }

    public class AzureTableGrainDirectoryOptions
    {
        [RedactConnectionString]
        public string ConnectionString { get; set; }
    }

    public static class AzureGrainDirectoryExtensions
    {
        public static ISiloHostBuilder UseAzureTableGrainDirectory(this ISiloHostBuilder builder, string connectionString)
        {
            builder.Configure<AzureTableGrainDirectoryOptions>(options => options.ConnectionString = connectionString);
            builder.ConfigureServices(services => services.AddSingleton<IPluggableGrainDirectory, AzureTableGrainDirectory>());
            return builder;
        }
    }
}
