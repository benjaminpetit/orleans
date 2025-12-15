using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.DurableJobs.Storage;

/// <summary>
/// In-memory storage implementation for testing scenarios.
/// Jobs are stored in a concurrent dictionary to survive shard ownership transfers.
/// </summary>
internal sealed class InMemoryJobShardStorage : IJobShardStorage
{
    private readonly ConcurrentDictionary<string, JobStorageRecord> _jobs = new();
    
    public string ShardId { get; }
    public DateTimeOffset StartTime { get; }
    public DateTimeOffset EndTime { get; }
    public IDictionary<string, string>? Metadata { get; }
    
    public InMemoryJobShardStorage(
        string shardId,
        DateTimeOffset startTime,
        DateTimeOffset endTime,
        IDictionary<string, string>? metadata)
    {
        ShardId = shardId;
        StartTime = startTime;
        EndTime = endTime;
        Metadata = metadata;
    }
    
    public Task AddJobAsync(JobStorageRecord record, CancellationToken ct)
    {
        _jobs[record.JobId] = record;
        return Task.CompletedTask;
    }
    
    public Task RemoveJobAsync(string jobId, CancellationToken ct)
    {
        _jobs.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }
    
    public Task UpdateJobDueTimeAsync(string jobId, DateTimeOffset newDueTime, int newDequeueCount, CancellationToken ct)
    {
        if (_jobs.TryGetValue(jobId, out var existing))
        {
            var updated = new JobStorageRecord(
                existing.JobId,
                existing.JobName,
                existing.TargetGrainId,
                newDueTime,
                existing.Metadata,
                newDequeueCount);
            _jobs[jobId] = updated;
        }
        return Task.CompletedTask;
    }
    
    public Task<IReadOnlyList<JobStorageRecord>> LoadAllJobsAsync(CancellationToken ct)
        => Task.FromResult<IReadOnlyList<JobStorageRecord>>(_jobs.Values.ToList());
    
    public ValueTask DisposeAsync()
        => ValueTask.CompletedTask;
}
