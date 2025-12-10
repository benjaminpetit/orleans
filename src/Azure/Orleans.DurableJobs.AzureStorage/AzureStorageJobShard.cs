using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;

namespace Orleans.DurableJobs.AzureStorage;

[DebuggerDisplay("ShardId={Id}, StartTime={StartTime}, EndTime={EndTime}")]
internal sealed class AzureStorageJobShard : JobShard
{
    public AzureStorageJobShard(
        string id,
        DateTimeOffset startTime,
        DateTimeOffset endTime,
        AppendBlobClient blobClient,
        IDictionary<string, string>? metadata,
        ETag? eTag,
        AzureStorageJobShardOptions options,
        ILogger<AzureStorageJobShard> logger,
        ILogger<AzureStorageJobShardStorage> storageLogger)
        : base(id, startTime, endTime, new AzureStorageJobShardStorage(id, blobClient, eTag, options, storageLogger))
    {
        Metadata = metadata;
    }

    public async ValueTask InitializeAsync(CancellationToken cancellationToken)
    {
        var storage = (AzureStorageJobShardStorage)_storage!;
        await foreach (var (job, dequeueCount) in storage.LoadJobsAsync(cancellationToken))
        {
            EnqueueJob(job, dequeueCount);
        }
    }

    public async Task UpdateBlobMetadata(IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        var storage = (AzureStorageJobShardStorage)_storage!;
        await storage.UpdateBlobMetadata(metadata, cancellationToken);
        Metadata = metadata;
    }

    internal async Task StopProcessorAsync(CancellationToken cancellationToken)
    {
        var storage = (AzureStorageJobShardStorage)_storage!;
        await storage.StopProcessorAsync(cancellationToken);
    }
}
