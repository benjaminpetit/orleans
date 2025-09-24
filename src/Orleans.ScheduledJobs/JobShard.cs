using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Utilities;

namespace Orleans.ScheduledJobs;

public abstract class JobShard
{
    public string Id { get; protected set; }

    public DateTime StartTime { get; protected set; }

    public DateTime EndTime { get; protected set; }

    public abstract ValueTask<int> GetJobCount();

    protected JobShard(string id, DateTime startTime, DateTime endTime)
    {
        Id = id;
        StartTime = startTime;
        EndTime = endTime;
    }

    public abstract Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTime scheduledAt);

    public abstract IAsyncEnumerable<IScheduledJob> ReadJobsAsync();

    public abstract Task RemoveJobAsync(string jobId);
}

[DebuggerDisplay("ShardId={Id}, StartTime={StartTime}, EndTime={EndTime}, JobCount={JobCount}")]
internal class InMemoryJobShard : JobShard
{
    private readonly ScheduledJobQueue _jobQueue;

    public InMemoryJobShard(string shardId, DateTime minDueTime)
        : base(shardId, minDueTime, minDueTime.AddHours(1))
    {
        _jobQueue = new ScheduledJobQueue(EndTime - DateTime.UtcNow);
    }

    public override Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTime scheduledAt)
    {
        if (scheduledAt < StartTime || scheduledAt > EndTime)
            throw new ArgumentOutOfRangeException(nameof(scheduledAt), "Scheduled time is out of shard bounds.");

        var job = new ScheduledJob
        {
            Id = Guid.NewGuid().ToString(),
            TargetGrainId = target,
            Name = jobName,
            ScheduledAt = scheduledAt,
            ShardId = Id
        };
        _jobQueue.Enqueue(job);
        return Task.FromResult((IScheduledJob)job);
    }

    public override IAsyncEnumerable<IScheduledJob> ReadJobsAsync()
    {
        return _jobQueue; 
    }

    public override Task RemoveJobAsync(string jobId)
    {
        // In a real implementation, you would need to implement a way to remove a job from the queue.
        return Task.CompletedTask;
    }

    public override ValueTask<int> GetJobCount() => ValueTask.FromResult(_jobQueue.Count);
}

public class ScheduledJobQueue : IAsyncEnumerable<ScheduledJob>
{
    private readonly PriorityQueue<ScheduledJob, DateTime> _queue = new();
    //private TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly object _syncLock = new();
    private bool _isFrozen = false;
    private readonly Timer? _freezeTimer;

    public int Count => _queue.Count;

    public ScheduledJobQueue(TimeSpan? timeout = default)
    {
        if (timeout.HasValue)
        {
            _freezeTimer = new Timer(_ => Freeze(), null, timeout.Value, Timeout.InfiniteTimeSpan);
        }
    }

    public void Enqueue(ScheduledJob job)
    {
        lock (_syncLock)
        {
            if (_isFrozen)
                throw new InvalidOperationException("Cannot enqueue job to a frozen queue.");

            _queue.Enqueue(job, job.ScheduledAt);
            //_tcs.TrySetResult(true);
        }
    }

    public void Freeze()
    {
        lock (_syncLock)
        {
            _freezeTimer?.Dispose();
            _isFrozen = true;
            //_tcs.TrySetResult(true);
        }
    }

    public async IAsyncEnumerator<ScheduledJob> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (true)
        {
            lock (_syncLock)
            {
                if (_queue.Count == 0)
                {
                    if (_isFrozen)
                        yield break; // Exit if the queue is frozen and empty
                }
                else
                {
                    var nextJob = _queue.Peek();
                    if (nextJob.ScheduledAt <= DateTime.UtcNow)
                    {
                        yield return _queue.Dequeue();
                        continue; // Immediately check for the next job
                    }
                }
            }
            await timer.WaitForNextTickAsync(cancellationToken);
        }
        //while (true)
        //{
        //    DateTime nextTime;
        //    ScheduledJob? nextJob;
        //    TaskCompletionSource<bool> currentTcs;

        //    lock (_syncLock)
        //    {
        //        if (_queue.Count == 0)
        //        {
        //            if (_isFrozen)
        //                yield break; // Exit if the queue is frozen and empty

        //            // No jobs in the queue, reset the TCS and wait for a new job
        //            _tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        //            currentTcs = _tcs;
        //            nextJob = null;
        //            nextTime = DateTime.MinValue;
        //        }
        //        else
        //        {
        //            // Get the next job's scheduled time
        //            nextJob = _queue.Peek();
        //            nextTime = nextJob.ScheduledAt;
        //            currentTcs = _tcs;

        //            if (nextTime <= DateTime.UtcNow)
        //            {
        //                // It's time to process the next job
        //                yield return _queue.Dequeue();
        //                continue; // Immediately check for the next job
        //            }
        //        }
        //    }

        //    if (nextJob == null)
        //    {
        //        // Wait until a new job is enqueued
        //        await currentTcs.Task.WaitAsync(cancellationToken);
        //    }
        //    else
        //    {
        //        // Wait until the next job's scheduled time or a new job is enqueued
        //        var now = DateTime.UtcNow;
        //        if (nextTime > now)
        //        {
        //            var delayTask = Task.Delay(nextTime - now, cancellationToken);
        //            var signalTask = currentTcs.Task;
        //            await Task.WhenAny(delayTask, signalTask);
        //        }
        //    }
        //}
    }
}
