using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace TestExtensions
{
    public class DefaultClusterFixture : IDisposable
    {
        static DefaultClusterFixture()
        {
            TestDefaultConfiguration.InitializeDefaults();
        }

        public DefaultClusterFixture()
        {
            var builder = new TestClusterBuilder();
            TestDefaultConfiguration.ConfigureTestCluster(builder);
            
            builder.AddSiloBuilderConfigurator<SiloHostConfigurator>();

            var testCluster = builder.Build();
            if (testCluster?.Primary == null)
            {
                testCluster?.Deploy();
            }

            this.HostedCluster = testCluster;
            this.Logger = this.Client?.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("Application");
        }
        
        public TestCluster HostedCluster { get; }

        public IGrainFactory GrainFactory => this.HostedCluster?.GrainFactory;

        public IClusterClient Client => this.HostedCluster?.Client;

        public ILogger Logger { get; }

        public virtual void Dispose()
        {
            Task.Run(() => this.HostedCluster?.StopAsync()).GetAwaiter().GetResult();
        }
        
        public class SiloHostConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .UseInMemoryReminderService()
                    .AddMemoryGrainStorageAsDefault()
                    .AddMemoryGrainStorage("MemoryStore");
            }
        }
    }
}
