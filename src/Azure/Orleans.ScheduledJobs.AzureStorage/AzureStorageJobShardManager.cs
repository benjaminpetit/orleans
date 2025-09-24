using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs.AzureStorage;

public class AzureStorageJobShardOptions
{
    /// <summary>
    /// The maximum duration of a job shard.
    /// </summary>
    public TimeSpan MaxShardDuration { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets the <see cref="BlobContainerClient"/> instance used to store job shards.
    /// </summary>
    public BlobContainerClient BlobContainerClient { get; set; } = null!;
}

internal sealed class AzureStorageJobShardManager : JobShardManager
{
    private readonly BlobContainerClient _client;
    private readonly TimeSpan _maxShardDuration;
    private readonly IClusterMembershipService _clusterMembership;

    public AzureStorageJobShardManager(BlobContainerClient client, TimeSpan maxShardDuration, IClusterMembershipService clusterMembership)
    {
        _client = client;
        _maxShardDuration = maxShardDuration;
        _clusterMembership = clusterMembership;
    }

    public AzureStorageJobShardManager(IOptions<AzureStorageJobShardOptions> options, IClusterMembershipService clusterMembership)
        : this(options.Value.BlobContainerClient, options.Value.MaxShardDuration, clusterMembership)
    {
    }

    public override async Task<List<JobShard>> GetJobShardsAsync(SiloAddress siloAddress, DateTime maxDateTime)
    {
        var blobs = _client.GetBlobsAsync(prefix: $"${maxDateTime:yyyyMMddHHmm}", traits: BlobTraits.Tags);
        var result = new List<JobShard>();
        await foreach (var blob in blobs)
        {
            // Get the owner of the shard
            var (owner, minDueTime, maxDueTime) = ParseMetadata(blob.Metadata);

            if (owner == null || _clusterMembership.CurrentSnapshot.GetSiloStatus(owner) == SiloStatus.Dead)
            {
                // The owner is dead or unknown, we can take over this shard
                var blobClient = _client.GetAppendBlobClient(blob.Name);
                var metadata = blob.Metadata;
                metadata["Owner"] = siloAddress.ToParsableString();
                try
                {
                    await blobClient.SetMetadataAsync(metadata, conditions: new BlobRequestConditions { IfMatch = blob.Properties.ETag });
                }
                catch (RequestFailedException)
                {
                    // Someone else took over the shard
                    continue;
                }
                result.Add(new AzureStorageJobShard(blob.Name, minDueTime, maxDueTime, blobClient, false));
            }
        }
        return result;
    }

    public override async Task<JobShard> RegisterShard(SiloAddress siloAddress, DateTime minDueTime)
    {
        for (var i = 0;; i++) // TODO limit the number of attempts
        {
            var shardId = $"{minDueTime:yyyyMMddHHmm}-{siloAddress}-{i}";
            var blobClient = _client.GetAppendBlobClient(shardId);
            var maxDueTime = minDueTime.Add(_maxShardDuration); 
            var metadata = CreateMetadata(siloAddress, minDueTime, maxDueTime);
            try
            {
                await blobClient.CreateAsync(metadata: metadata);
            }
            catch (RequestFailedException)
            {
                if (i > 100) throw; // Prevent infinite loop
                // Blob already exists, try again with a different name
                continue;
            }
            return new AzureStorageJobShard(shardId, minDueTime, maxDueTime, blobClient, true);
        }
    }

    private static Dictionary<string, string> CreateMetadata(SiloAddress siloAddress, DateTime minDueTime, DateTime maxDueTime)
    {
        return new Dictionary<string, string>
        {
            { "Owner", siloAddress.ToParsableString() },
            { "MinDueTime", minDueTime.ToString("o") },
            { "MaxDueTime", maxDueTime.ToString("o") }
        };
    }

    private static (SiloAddress? owner, DateTime minDueTime, DateTime maxDueTime) ParseMetadata(IDictionary<string, string> metadata)
    {
        var owner = metadata.TryGetValue("Owner", out var ownerStr) ? SiloAddress.FromParsableString(ownerStr) : null;
        var minDueTime = metadata.TryGetValue("MinDueTime", out var minDueTimeStr) && DateTime.TryParse(minDueTimeStr, out var minDt) ? minDt : DateTime.MinValue;
        var maxDueTime = metadata.TryGetValue("MaxDueTime", out var maxDueTimeStr) && DateTime.TryParse(maxDueTimeStr, out var maxDt) ? maxDt : DateTime.MaxValue;
        return (owner, minDueTime, maxDueTime);
    }
}
