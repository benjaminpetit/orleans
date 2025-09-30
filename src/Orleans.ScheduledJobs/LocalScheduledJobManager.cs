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
    Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTime scheduledAt);

    Task<bool> TryCancelScheduledJobAsync(IScheduledJob job);
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

    public async Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTime scheduledAt)
    {
        var key = GetShardKey(scheduledAt);
        // Try to get all the available shards for this key
        var shards = _shardCache.GetOrAdd(key, _ => new ConcurrentBag<JobShard>());
        // Find a shard that can accept this job
        foreach (var shard in shards)
        {
            if (shard.StartTime <= scheduledAt && shard.EndTime >= scheduledAt && await shard.GetJobCount() <= MaxJobCountPerShard)
            {
                return await shard.ScheduleJobAsync(target, jobName, scheduledAt);
            }
        }
        // No available shard found, create a new one
        var newShard = await CreateJobShardAsync(key);
        shards.Add(newShard);
        var job = await newShard.ScheduleJobAsync(target, jobName, scheduledAt);
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
        this.QueueTask(WatchForShardsAsync);
        return Task.CompletedTask;
    }

    private async Task WatchForShardsAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var shards = await _shardManager.GetJobShardsAsync(this.Silo, DateTime.UtcNow.AddHours(1));
                if (shards.Count > 0)
                {
                    foreach (var shard in shards)
                    {
                        RunShard(shard).Ignore(); // TODO: keep track of running shards
                    }
                }
                await Task.Delay(TimeSpan.FromMinutes(1), _cts.Token);
            }
        }
        catch (TaskCanceledException)
        {
            // Ignore, shutting down
        }
    }

    private async Task RunShard(JobShard shard)
    {
        try
        {
            if (shard.StartTime > DateTime.UtcNow)
            {
                // Wait until the shard's start time
                var delay = shard.StartTime - DateTime.UtcNow;
                await Task.Delay(delay, _cts.Token);
            }

            // Process all jobs in the shard
            await foreach (var job in shard.ReadJobsAsync().WithCancellation(_cts.Token))
            {
                try
                {
                    // TODO: Do it in parallel, with concurrency limit
                    var target = this.RuntimeClient.InternalGrainFactory
                        .GetGrain(job.TargetGrainId)
                        .AsReference<IScheduledJobReceiver>();
                    await target.ReceiveScheduledJobAsync(job);
                    await shard.RemoveJobAsync(job.Id);
                }
                catch (Exception ex)
                {
                    // TODO Log the exception
                    Console.WriteLine($"Error executing job {job.Id}: {ex}");
                }
            }
            // Unregister the shard
            await _shardManager.UnregisterShard(this.Silo, shard);
        }
        catch (TaskCanceledException)
        {
            // Ignore, shutting down
        }
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

    public Task<bool> TryCancelScheduledJobAsync(IScheduledJob job)
    {
        // TODO: Implement job cancellation
        return Task.FromResult(false);
    }
}
