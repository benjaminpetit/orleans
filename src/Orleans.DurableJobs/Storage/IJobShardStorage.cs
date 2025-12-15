using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.DurableJobs.Storage;

/// <summary>
/// Pure storage abstraction for job persistence.
/// Implementations handle storage-specific concerns (batching, serialization, etc.)
/// without any business logic.
/// </summary>
public interface IJobShardStorage : IAsyncDisposable
{
    /// <summary>
    /// Persists a new job to storage.
    /// </summary>
    /// <exception cref="Exception">Thrown when persistence fails.</exception>
    Task AddJobAsync(JobStorageRecord record, CancellationToken ct);
    
    /// <summary>
    /// Removes a job from storage.
    /// </summary>
    /// <exception cref="Exception">Thrown when removal fails.</exception>
    Task RemoveJobAsync(string jobId, CancellationToken ct);
    
    /// <summary>
    /// Updates the due time and dequeue count of an existing job (used for retries).
    /// </summary>
    /// <exception cref="Exception">Thrown when update fails.</exception>
    Task UpdateJobDueTimeAsync(string jobId, DateTimeOffset newDueTime, int newDequeueCount, CancellationToken ct);
    
    /// <summary>
    /// Loads all jobs from storage for initialization/recovery.
    /// Called once during shard initialization.
    /// </summary>
    /// <exception cref="Exception">Thrown when loading fails (corrupt data, network issues, etc.).</exception>
    Task<IReadOnlyList<JobStorageRecord>> LoadAllJobsAsync(CancellationToken ct);
}

/// <summary>
/// Immutable storage record representing a persisted job.
/// This is a simple DTO with no behavior.
/// </summary>
public sealed record JobStorageRecord(
    string JobId,
    string JobName,
    GrainId TargetGrainId,
    DateTimeOffset DueTime,
    IReadOnlyDictionary<string, string>? Metadata,
    int DequeueCount);
