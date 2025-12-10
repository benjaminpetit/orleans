using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.DurableJobs;

/// <summary>
/// Provides persistence operations for job shard storage.
/// Implementations handle the underlying storage mechanism for durable jobs.
/// </summary>
public interface IJobShardStorage : IAsyncDisposable
{
    /// <summary>
    /// Persists a new job to storage.
    /// </summary>
    /// <param name="jobId">The unique identifier of the job.</param>
    /// <param name="jobName">The name of the job.</param>
    /// <param name="dueTime">The time when the job should be executed.</param>
    /// <param name="target">The grain identifier of the target grain.</param>
    /// <param name="metadata">Optional metadata to associate with the job.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task PersistAddJobAsync(string jobId, string jobName, DateTimeOffset dueTime, GrainId target, 
        IReadOnlyDictionary<string, string>? metadata, CancellationToken cancellationToken);

    /// <summary>
    /// Persists the removal of a job from storage.
    /// </summary>
    /// <param name="jobId">The unique identifier of the job to remove.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task PersistRemoveJobAsync(string jobId, CancellationToken cancellationToken);

    /// <summary>
    /// Persists a job retry with new due time.
    /// </summary>
    /// <param name="jobId">The unique identifier of the job to retry.</param>
    /// <param name="newDueTime">The new due time for the job.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task PersistRetryJobAsync(string jobId, DateTimeOffset newDueTime, CancellationToken cancellationToken);

    /// <summary>
    /// Loads all jobs from storage. Used for deferred loading.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of tuples containing the job and its dequeue count.</returns>
    IAsyncEnumerable<(DurableJob Job, int DequeueCount)> LoadJobsAsync(CancellationToken cancellationToken);
}
