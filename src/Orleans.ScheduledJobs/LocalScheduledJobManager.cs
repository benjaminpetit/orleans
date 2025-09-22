using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

public class LocalScheduledJobManager : SystemTarget
{
    private readonly JobShardManager _shardManager;
    private readonly ConcurrentDictionary<DateTime, ConcurrentBag<JobShard>> _shardCache = new();
    private readonly int MaxJobCountPerShard = 1000;

    public async Task<IScheduledJob> ScheduleJobAsync(IGrainBase grain, string jobName, DateTime scheduledTime)
    {
        var key = GetShardKey(scheduledTime);
        // Try to get all the available shards for this key
        var shards = _shardCache.GetOrAdd(key, _ => new ConcurrentBag<JobShard>());
        // Find a shard that can accept this job
        foreach (var shard in shards)
        {
            if (shard.NextScheduledTime <= scheduledTime && shard.MaxScheduledTime >= scheduledTime && shard.JobCount <= MaxJobCountPerShard)
            {
                return await shard.ScheduleJobAsync(grain, jobName, scheduledTime);
            }
        }
        // No available shard found, create a new one
        var newShard = await CreateJobShardAsync(key);
        shards.Add(newShard);
        var job = await newShard.ScheduleJobAsync(grain, jobName, scheduledTime);
        RunShard(newShard).Ignore();
        return job;
    }

    private async Task RunShard(JobShard shard)
    {
        while (shard.JobCount > 0 || shard.MaxScheduledTime > DateTime.UtcNow.AddMinutes(1))
        {
            var job = await shard.GetNextJobAsync();
            if (job != null)
            {
                try
                {
                    // TODO: Do it in parallel, with concurrency limit
                    var target = this.RuntimeClient.InternalGrainFactory
                        .GetGrain(job.TargetGrainId)
                        .AsReference<IScheduledJobReceiver>();
                    await target.ReceiveScheduledJobAsync(job);
                    await shard.RemoveJobAsync(job.JobId);
                }
                catch (Exception ex)
                {
                    // Log the exception
                    Console.WriteLine($"Error executing job {job.JobId}: {ex}");
                }
            }
            else
            {
                await Task.Delay(1000); // Wait for new jobs
            }
        }
        await _shardManager.DeleteShard(shard.ShardId);
    }

    private DateTime GetShardKey(DateTime scheduledTime)
    {
        return new DateTime(scheduledTime.Year, scheduledTime.Month, scheduledTime.Day, scheduledTime.Hour, scheduledTime.Minute, 0, DateTimeKind.Utc);
    }

    private async Task<JobShard> CreateJobShardAsync(DateTime shardKey)
    {
        var shard = await _shardManager.RegisterShard(this.Silo, shardKey);
        return shard;
    }
}
