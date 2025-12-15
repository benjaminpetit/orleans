using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.DurableJobs.Storage;

/// <summary>
/// No-op storage implementation for in-memory scenarios.
/// All state is maintained in the in-memory queue, nothing is persisted.
/// </summary>
internal sealed class InMemoryJobShardStorage : IJobShardStorage
{
    public Task AddJobAsync(JobStorageRecord record, CancellationToken ct)
        => Task.CompletedTask;
    
    public Task RemoveJobAsync(string jobId, CancellationToken ct)
        => Task.CompletedTask;
    
    public Task UpdateJobDueTimeAsync(string jobId, DateTimeOffset newDueTime, int newDequeueCount, CancellationToken ct)
        => Task.CompletedTask;
    
    public Task<IReadOnlyList<JobStorageRecord>> LoadAllJobsAsync(CancellationToken ct)
        => Task.FromResult<IReadOnlyList<JobStorageRecord>>(Array.Empty<JobStorageRecord>());
    
    public ValueTask DisposeAsync()
        => ValueTask.CompletedTask;
}
