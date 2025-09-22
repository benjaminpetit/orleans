using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

internal abstract class JobShard
{
    public string ShardId { get; protected set; }

    public DateTime NextScheduledTime { get; protected set; }

    public DateTime MaxScheduledTime { get; protected set; }

    public int JobCount { get; protected set; }

    public abstract Task<IScheduledJob> ScheduleJobAsync(IGrainBase grain, string jobName, DateTime scheduledTime);

    public abstract ValueTask<IScheduledJob> GetNextJobAsync();

    public abstract Task RemoveJobAsync(string jobId);
}

internal class InMemoryJobShard : JobShard
{
    private readonly SortedSet<ScheduledJob> _jobs = new(new ScheduledJobComparer());

    public InMemoryJobShard(string shardId, DateTime minDueTime)
    {
        ShardId = shardId;
        NextScheduledTime = minDueTime;
        MaxScheduledTime = minDueTime.AddHours(1); // Example: each shard handles jobs for 1 hour
        JobCount = 0;
    }

    public override Task<IScheduledJob> ScheduleJobAsync(IGrainBase grain, string jobName, DateTime scheduledTime)
    {
        if (scheduledTime < NextScheduledTime || scheduledTime > MaxScheduledTime)
            throw new ArgumentOutOfRangeException(nameof(scheduledTime), "Scheduled time is out of shard bounds.");
        var job = new ScheduledJob
        {
            JobId = Guid.NewGuid().ToString(),
            TargetGrainId = grain.GrainContext.GrainId,
            JobName = jobName,
            ScheduledTime = scheduledTime
        };
        _jobs.Add(job);
        JobCount++;
        return Task.FromResult((IScheduledJob)job);
    }

    public override ValueTask<IScheduledJob> GetNextJobAsync()
    {
        if (_jobs.Count == 0) return ValueTask.FromResult<IScheduledJob>(null);
        var nextJob = _jobs.Min;
        return ValueTask.FromResult((IScheduledJob)nextJob);
    }

    public override Task RemoveJobAsync(string jobId)
    {
        var jobToRemove = _jobs.FirstOrDefault(j => j.JobId == jobId);
        if (jobToRemove != null)
        {
            _jobs.Remove(jobToRemove);
            JobCount--;
        }
        return Task.CompletedTask;
    }

    private class ScheduledJob : IScheduledJob
    {
        public string JobId { get; init; }
        public GrainId TargetGrainId { get; init; }
        public string JobName { get; init; }
        public DateTime ScheduledTime { get; init; }
    }

    private class ScheduledJobComparer : IComparer<ScheduledJob>
    {
        public int Compare(ScheduledJob x, ScheduledJob y)
        {
            return x.ScheduledTime.CompareTo(y.ScheduledTime);
        }
    }
}
