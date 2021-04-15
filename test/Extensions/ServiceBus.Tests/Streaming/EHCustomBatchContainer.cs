using System;
using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans;
using Orleans.Configuration;
using Orleans.TestingHost;
using ServiceBus.Tests.TestStreamProviders.EventHub;
using TestExtensions;
using Xunit.Abstractions;
using Orleans.Streams;
using Orleans.ServiceBus.Providers;
using Tester;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Serialization;
using Orleans.Providers.Streams.Common;
using System.Collections.Generic;
using Xunit;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;
using Microsoft.Extensions.DependencyInjection;

namespace ServiceBus.Tests.StreamingTests
{
    [TestCategory("EventHub"), TestCategory("Streaming"), TestCategory("Functional")]
    public class EHCustomBatchContainerTests : TestClusterPerTest
    {
        private const string StreamProviderName = "CustomBatchContainerProvider";
        private const string EHPath = "ehorleanstest";
        private const string EHConsumerGroup = "orleansnightly";

        private readonly ITestOutputHelper output;

        public EHCustomBatchContainerTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            TestUtils.CheckForEventHub();
            builder.AddSiloBuilderConfigurator<MySiloBuilderConfigurator>();
            builder.AddClientBuilderConfigurator<MyClientBuilderConfigurator>();
        }

        #region Configuration stuff

        private class MySiloBuilderConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder hostBuilder)
            {
                hostBuilder
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddEventHubStreams(StreamProviderName, b =>
                    {
                        b.ConfigureCacheEviction(ob => ob.Configure(options =>
                        {
                            options.DataMaxAgeInCache = TimeSpan.FromSeconds(5);
                            options.DataMinTimeInCache = TimeSpan.FromSeconds(0);
                        }));
                        b.ConfigureEventHub(ob => ob.Configure(options =>
                        {
                            options.ConnectionString = TestDefaultConfiguration.EventHubConnectionString;
                            options.ConsumerGroup = EHConsumerGroup;
                            options.Path = EHPath;
                        }));
                        b.UseAzureTableCheckpointer(ob => ob.Configure(options =>
                        {
                            options.ConnectionString = TestDefaultConfiguration.DataConnectionString;
                            options.PersistInterval = TimeSpan.FromSeconds(10);
                        }));
                        b.UseDataAdapter((sp, n) => ActivatorUtilities.CreateInstance<CustomDataAdapter>(sp));
                    });
            }
        }

        private class MyClientBuilderConfigurator : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder
                    .AddEventHubStreams(StreamProviderName, b =>
                    {
                        b.ConfigureEventHub(ob => ob.Configure(options =>
                        {
                            options.ConnectionString = TestDefaultConfiguration.EventHubConnectionString;
                            options.ConsumerGroup = EHConsumerGroup;
                            options.Path = EHPath;
                        }));
                        b.UseDataAdapter((sp, n) => ActivatorUtilities.CreateInstance<CustomDataAdapter>(sp));
                    });
            }
        }

        private class CustomDataAdapter : EventHubDataAdapter
        {
            public CustomDataAdapter(SerializationManager serializationManager)
                : base(serializationManager)
            {
            }

            public override IBatchContainer GetBatchContainer(ref CachedMessage cachedMessage)
                => new CustomBatchContainer(base.GetBatchContainer(ref cachedMessage));

            protected override IBatchContainer GetBatchContainer(EventHubMessage eventHubMessage)
                => new CustomBatchContainer(base.GetBatchContainer(eventHubMessage));

            //public override string GetPartitionKey(Guid streamGuid, string streamNamespace) => Guid.Empty.ToString();
        }

        private class CustomBatchContainer : IBatchContainer
        {
            private IBatchContainer batchContainer;

            public CustomBatchContainer(IBatchContainer batchContainer)
            {
                this.batchContainer = batchContainer;
            }

            public Guid StreamGuid => this.batchContainer.StreamGuid;

            public string StreamNamespace => this.batchContainer.StreamNamespace;

            public StreamSequenceToken SequenceToken => this.batchContainer.SequenceToken;

            public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() => this.batchContainer.GetEvents<T>();

            public bool ImportRequestContext() => this.batchContainer.ImportRequestContext();

            public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
            {
                var events = this.GetEvents<byte[]>();

                foreach (var evt in events)
                {
                    if (evt.Item1[0] == 1)
                        return true;
                }

                return false;
            }
        }
        #endregion

        [Fact]
        public async Task PreviousEventEvictedFromCacheTest()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(key, nameof(ICustomContainerImplicitSubscriptionGrain));
            var grain = this.Client.GetGrain<ICustomContainerImplicitSubscriptionGrain>(key);

            // We need multiple streams, so at least another one will be handled by the same PullingAgent than "stream"
            var otherStreams = new List<IAsyncStream<byte[]>>();
            for (var i = 0; i < 20; i++)
                otherStreams.Add(streamProvider.GetStream<byte[]>(Guid.NewGuid(), nameof(ICustomContainerImplicitSubscriptionGrain)));

            // Data that will be sent to the grains
            var interestingData = new byte[1024];
            interestingData[0] = 1;

            // Should be delivered
            await stream.OnNextAsync(interestingData);

            // Wait a bit so cache expire, and launch a bunch of events to trigger the cleaning
            await Task.Delay(TimeSpan.FromSeconds(6));
            otherStreams.ForEach(s => s.OnNextAsync(interestingData));

            // Should be delivered
            await stream.OnNextAsync(interestingData);

            await Task.Delay(1000);

            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(2, await grain.GetEventCounter());
        }

        [Fact]
        public async Task PreviousEventEvictedFromCacheWithFilterTest()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(key, nameof(ICustomContainerImplicitSubscriptionGrain));
            var grain = this.Client.GetGrain<ICustomContainerImplicitSubscriptionGrain>(key);

            // We need multiple streams, so at least another one will be handled by the same PullingAgent than "stream"
            var otherStreams = new List<IAsyncStream<byte[]>>();
            for (var i = 0; i < 20; i++) 
                otherStreams.Add(streamProvider.GetStream<byte[]>(Guid.NewGuid(), nameof(ICustomContainerImplicitSubscriptionGrain)));

            // Data that will always be filtered
            var skippedData = new byte[1024];
            skippedData[0] = 2;

            // Data that will be sent to the grains
            var interestingData = new byte[1024];
            interestingData[0] = 1;

            // Should not reach the grain
            await stream.OnNextAsync(skippedData);

            // Wait a bit so cache expire, and launch a bunch of events to trigger the cleaning
            await Task.Delay(TimeSpan.FromSeconds(6));
            otherStreams.ForEach(s => s.OnNextAsync(skippedData));

            // Should be delivered
            await stream.OnNextAsync(interestingData); 

            await Task.Delay(1000);

            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(1, await grain.GetEventCounter());
        }
    }
}
