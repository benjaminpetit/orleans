using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

[GenerateSerializer]
internal abstract class JobShard
{
    [Id(0)]
    public string ShardId { get; protected set; }

    [Id(1)]
    public DateTime NextScheduledTime { get; protected set; }

    [Id(2)]
    public DateTime MaxScheduledTime { get; protected set; }

    [Id(3)]
    public int JobCount { get; protected set; }

    public abstract Task<IScheduledJob> ScheduleJobAsync(IGrainBase grain, string jobName, DateTime scheduledTime);

    public abstract ValueTask<IScheduledJob> GetNextJobAsync();

    public abstract Task RemoveJobAsync(string jobId);
}

[DebuggerDisplay("ShardId={ShardId}, NextScheduledTime={NextScheduledTime}, MaxScheduledTime={MaxScheduledTime}, JobCount={JobCount}")]
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
        _jobs.Remove(nextJob);
        JobCount--;
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

    private class ScheduledJobComparer : IComparer<ScheduledJob>
    {
        public int Compare(ScheduledJob x, ScheduledJob y)
        {
            var dateCompare = x.ScheduledTime.CompareTo(y.ScheduledTime);

            if (dateCompare != 0)
                return dateCompare;

            return string.Compare(x.JobId, y.JobId, StringComparison.Ordinal);
        }
    }
}
