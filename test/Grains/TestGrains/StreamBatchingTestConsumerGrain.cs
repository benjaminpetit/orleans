using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using Orleans.Streams.Core;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains.Batching
{
    [ImplicitStreamSubscription(StreamBatchingTestConst.BatchingNameSpace)]
    [ImplicitStreamSubscription(StreamBatchingTestConst.NonBatchingNameSpace)]
    public class BatchingStreamBatchingTestConsumerGrain : Grain, IStreamBatchingTestConsumerGrain, IStreamSubscriptionObserver
    {
        private readonly ConsumptionReport report = new ConsumptionReport();
        
        public Task<ConsumptionReport> GetConsumptionReport() => Task.FromResult(this.report);

        public Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
        {
            StreamSubscriptionHandle<string> handle = handleFactory.Create<string>();
            return (Encoding.UTF8.GetString(handle.StreamId.Namespace.ToArray()) == StreamBatchingTestConst.BatchingNameSpace)
                ? handle.ResumeAsync(OnNextBatch)
                : handle.ResumeAsync(OnNext);
        }

        private async Task OnNextBatch(IList<SequentialItem<string>> items)
        {
            this.report.Consumed += items.Count;
            this.report.MaxBatchSize = Math.Max(this.report.MaxBatchSize, items.Count);
            await Task.Delay(500);
        }

        private Task OnNext(string item, StreamSequenceToken token)
        {
            this.report.Consumed++;
            this.report.MaxBatchSize = 1;
            return Task.CompletedTask;
        }
    }
}

