using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Scheduler;

namespace Orleans.ScheduledJobs;

public interface ILocalScheduledJobManager
{
    Task<IScheduledJob> ScheduleJobAsync(IGrainBase grain, string jobName, DateTime scheduledTime);
}

internal class LocalScheduledJobManager : SystemTarget, ILocalScheduledJobManager, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly JobShardManager _shardManager;
    private CancellationTokenSource _cts = new();

    public LocalScheduledJobManager(JobShardManager shardManager, SystemTargetShared shared)
        : base(SystemTargetGrainId.CreateGrainType("scheduledjobs-manager"), shared)
    {
        _shardManager = shardManager;
    }

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
        this.QueueTask(() => RunShard(newShard)).Ignore();
        return job;
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
            nameof(LocalScheduledJobManager),
            ServiceLifecycleStage.Active,
            ct => Start(ct),
            ct => Stop(ct));
    }

    private Task Stop(CancellationToken ct)
    {
        // TODO Wait for running shards to complete
        _cts.Cancel();
        return Task.CompletedTask;
    }

    private Task Start(CancellationToken ct)
    {
        this.QueueTask(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var shards = await _shardManager.GetJobShardsAsync(this.Silo, DateTime.UtcNow.AddHours(1));
                if (shards.Count > 0)
                {
                    foreach (var shard in shards)
                    {
                        RunShard(shard).Ignore();
                    }
                }
                await Task.Delay(TimeSpan.FromMinutes(1), ct);
            }
        });
        return Task.CompletedTask;
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
