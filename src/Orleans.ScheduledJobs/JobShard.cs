using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Utilities;

namespace Orleans.ScheduledJobs;

public abstract class JobShard
{
    // split?

    public string Id { get; protected set; }

    public DateTimeOffset StartTime { get; protected set; }

    public DateTimeOffset EndTime { get; protected set; }

    public IDictionary<string, string>? Metadata { get; protected set; }

    public abstract ValueTask<int> GetJobCount();

    protected JobShard(string id, DateTimeOffset startTime, DateTimeOffset endTime)
    {
        Id = id;
        StartTime = startTime;
        EndTime = endTime;
    }

    // Move to the ShardManager?
    public abstract Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTimeOffset dueTime);

    public abstract IAsyncEnumerable<IScheduledJob> ConsumeScheduledJobsAsync();

    public abstract Task RemoveJobAsync(string jobId);

    public abstract Task MarkAsComplete();
}

[DebuggerDisplay("ShardId={Id}, StartTime={StartTime}, EndTime={EndTime}, JobCount={JobCount}")]
internal class InMemoryJobShard : JobShard
{
    private readonly InMemoryJobQueue _jobQueue;
    private bool _isComplete = false;

    public InMemoryJobShard(string shardId, DateTimeOffset minDueTime, DateTimeOffset maxDueTime)
        : base(shardId, minDueTime, maxDueTime)
    {
        _jobQueue = new InMemoryJobQueue();
    }

    public override Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTimeOffset dueTime)
    {
        if (_isComplete)
            throw new InvalidOperationException("Cannot schedule job on a complete shard.");

        if (dueTime < StartTime || dueTime > EndTime)
            throw new ArgumentOutOfRangeException(nameof(dueTime), "Scheduled time is out of shard bounds.");

        IScheduledJob job = new ScheduledJob
        {
            Id = Guid.NewGuid().ToString(),
            TargetGrainId = target,
            Name = jobName,
            DueTime = dueTime,
            ShardId = Id
        };
        _jobQueue.Enqueue(job);
        return Task.FromResult(job);
    }

    public override IAsyncEnumerable<IScheduledJob> ConsumeScheduledJobsAsync() // todo rename
    {
        return _jobQueue; 
    }

    public override Task RemoveJobAsync(string jobId)
    {
        _jobQueue.CancelJob(jobId);
        return Task.CompletedTask;
    }

    public override ValueTask<int> GetJobCount() => ValueTask.FromResult(_jobQueue.Count);

    public override Task MarkAsComplete()
    {
        _isComplete = true;
        _jobQueue.MarkAsComplete();
        return Task.CompletedTask;
    }
}
