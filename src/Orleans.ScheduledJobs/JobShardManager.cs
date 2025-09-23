using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

internal abstract class JobShardManager
{
    public abstract Task<List<JobShard>> GetJobShardsAsync(SiloAddress siloAddress, DateTime maxDueTime);

    public abstract Task<JobShard> RegisterShard(SiloAddress siloAddress, DateTime minDueTime);

    public abstract Task DeleteShard(string shardId);
}

internal class InMemoryJobShardManager : JobShardManager
{
    private readonly Dictionary<string, List<InMemoryJobShard>> _shardStore = new();

    public override Task<List<JobShard>> GetJobShardsAsync(SiloAddress siloAddress, DateTime maxDueTime)
    {
        var key = siloAddress.ToString();
        if (_shardStore.TryGetValue(key, out var shards))
        {
            var result = new List<JobShard>();
            foreach (var shard in shards)
            {
                if (shard.EndTime <= maxDueTime)
                {
                    result.Add(shard);
                }
            }
            return Task.FromResult(result);
        }
        return Task.FromResult(new List<JobShard>());
    }

    public override Task<JobShard> RegisterShard(SiloAddress siloAddress, DateTime minDueTime)
    {
        var key = siloAddress.ToString();
        if (!_shardStore.ContainsKey(key))
        {
            _shardStore[key] = new List<InMemoryJobShard>();
        }
        var shardId = $"{key}-{Guid.NewGuid()}";
        var newShard = new InMemoryJobShard(shardId, minDueTime);
        _shardStore[key].Add(newShard);
        return Task.FromResult((JobShard)newShard);
    }

    public override Task DeleteShard(string shardId)
    {
        foreach (var key in _shardStore.Keys)
        {
            var shards = _shardStore[key];
            var shardToRemove = shards.Find(s => s.Id == shardId);
            if (shardToRemove != null)
            {
                shards.Remove(shardToRemove);
                break;
            }
        }
        return Task.CompletedTask;
    }
}
