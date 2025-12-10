using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization.Buffers.Adaptors;

namespace Orleans.DurableJobs.AzureStorage;

/// <summary>
/// Azure Storage implementation of <see cref="IJobShardStorage"/> using append blobs.
/// Provides persistence for durable jobs with batching and background processing.
/// </summary>
internal sealed partial class AzureStorageJobShardStorage : IJobShardStorage
{
    private readonly Channel<StorageOperation> _storageOperationChannel;
    private readonly Task _storageProcessorTask;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly AzureStorageJobShardOptions _options;
    private readonly ILogger<AzureStorageJobShardStorage> _logger;
    private readonly string _shardId;

    internal AppendBlobClient BlobClient { get; init; }
    internal ETag? ETag { get; private set; }
    internal int CommitedBlockCount { get; private set; }

    public AzureStorageJobShardStorage(
        string shardId,
        AppendBlobClient blobClient,
        ETag? eTag,
        AzureStorageJobShardOptions options,
        ILogger<AzureStorageJobShardStorage> logger)
    {
        _shardId = shardId;
        BlobClient = blobClient;
        ETag = eTag;
        _options = options;
        _logger = logger;
        
        // Create unbounded channel for storage operations
        _storageOperationChannel = Channel.CreateUnbounded<StorageOperation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        
        // Start the background task that processes storage operations
        _storageProcessorTask = ProcessStorageOperationsAsync();
    }

    public async Task PersistAddJobAsync(string jobId, string jobName, DateTimeOffset dueTime, GrainId target, IReadOnlyDictionary<string, string>? metadata, CancellationToken cancellationToken)
    {
        LogAddingJob(_logger, jobId, jobName, _shardId, dueTime);
        var operation = JobOperation.CreateAddOperation(jobId, jobName, dueTime, target, metadata);
        await EnqueueStorageOperationAsync(StorageOperation.CreateAppendOperation(operation), cancellationToken);
    }

    public async Task PersistRemoveJobAsync(string jobId, CancellationToken cancellationToken)
    {
        LogRemovingJob(_logger, jobId, _shardId);
        var operation = JobOperation.CreateRemoveOperation(jobId);
        await EnqueueStorageOperationAsync(StorageOperation.CreateAppendOperation(operation), cancellationToken);
    }

    public async Task PersistRetryJobAsync(string jobId, DateTimeOffset newDueTime, CancellationToken cancellationToken)
    {
        LogRetryingJob(_logger, jobId, _shardId, newDueTime);
        var operation = JobOperation.CreateRetryOperation(jobId, newDueTime);
        await EnqueueStorageOperationAsync(StorageOperation.CreateAppendOperation(operation), cancellationToken);
    }

    public async IAsyncEnumerable<(DurableJob Job, int DequeueCount)> LoadJobsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        LogLoadingJobs(_logger, _shardId);
        var sw = Stopwatch.StartNew();
        
        // Load existing blob
        var response = await BlobClient.DownloadAsync(cancellationToken: cancellationToken);
        using var stream = response.Value.Content;

        // Rebuild state by replaying operations
        var addedJobs = new Dictionary<string, JobOperation>();
        var deletedJobs = new HashSet<string>();
        var jobRetryCounters = new Dictionary<string, (int dequeueCount, DateTimeOffset? newDueTime)>();

        await foreach (var operation in NetstringJsonSerializer<JobOperation>.DecodeAsync(stream, JobOperationJsonContext.Default.JobOperation, cancellationToken))
        {
            switch (operation.Type)
            {
                case JobOperation.OperationType.Add:
                    if (!deletedJobs.Contains(operation.Id))
                    {
                        addedJobs[operation.Id] = operation;
                    }
                    break;
                case JobOperation.OperationType.Remove:
                    deletedJobs.Add(operation.Id);
                    addedJobs.Remove(operation.Id);
                    jobRetryCounters.Remove(operation.Id);
                    break;
                case JobOperation.OperationType.Retry:
                    if (!deletedJobs.Contains(operation.Id))
                    {
                        if (!jobRetryCounters.ContainsKey(operation.Id))
                        {
                            jobRetryCounters[operation.Id] = (1, operation.DueTime);
                        }
                        else
                        {
                            var entry = jobRetryCounters[operation.Id];
                            jobRetryCounters[operation.Id] = (entry.dequeueCount + 1, operation.DueTime);
                        }
                    }
                    break;
            }
        }

        // Update ETag from the response
        ETag = response.Value.Details.ETag;
        
        sw.Stop();
        LogJobsLoaded(_logger, _shardId, addedJobs.Count, sw.ElapsedMilliseconds);

        // Return all jobs
        foreach (var op in addedJobs.Values)
        {
            var retryCounter = 0;
            var dueTime = op.DueTime!.Value;
            if (jobRetryCounters.TryGetValue(op.Id, out var retryEntries))
            {
                retryCounter = retryEntries.dequeueCount;
                dueTime = retryEntries.newDueTime ?? dueTime;
            }

            var job = new DurableJob
            {
                Id = op.Id,
                Name = op.Name!,
                DueTime = dueTime,
                TargetGrainId = op.TargetGrainId!.Value,
                ShardId = _shardId,
                Metadata = op.Metadata,
            };

            yield return (job, retryCounter);
        }
    }

    public async Task UpdateBlobMetadata(IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        LogUpdatingMetadata(_logger, _shardId);
        await EnqueueStorageOperationAsync(StorageOperation.CreateMetadataOperation(metadata), cancellationToken);
    }

    private async Task EnqueueStorageOperationAsync(StorageOperation operation, CancellationToken cancellationToken)
    {
        await _storageOperationChannel.Writer.WriteAsync(operation, cancellationToken);
        await operation.CompletionSource.Task;
    }

    private async Task ProcessStorageOperationsAsync()
    {
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.ForceYielding);

        var cancellationToken = _shutdownCts.Token;
        // TODO: AppendBlob has a limit of 50,000 blocks. Implement blob rotation when this limit is approached.
        var batchOperations = new List<StorageOperation>(_options.MaxBatchSize);
        
        try
        {
            while (await _storageOperationChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                // Read first operation
                if (!_storageOperationChannel.Reader.TryRead(out var firstOperation))
                {
                    continue;
                }

                // Handle metadata operations immediately (cannot be batched)
                if (firstOperation.Type is StorageOperationType.UpdateMetadata)
                {
                    try
                    {
                        await UpdateMetadataAsync(firstOperation.Metadata!, cancellationToken);
                        LogMetadataUpdated(_logger, _shardId);
                        firstOperation.CompletionSource.TrySetResult();
                    }
                    catch (Exception ex)
                    {
                        LogErrorUpdatingMetadata(_logger, ex, _shardId);
                        firstOperation.CompletionSource?.TrySetException(ex);
                    }
                    continue;
                }

                // Collect job operations for batching
                batchOperations.Add(firstOperation);

                // Try to collect more operations up to the maximum batch size
                if (TryCollectJobOperationsForBatch(batchOperations))
                {
                    // Not enough operations to meet the minimum batch size, wait for more or timeout
                    if (batchOperations.Count < _options.MinBatchSize)
                    {
                        LogWaitingForBatch(_logger, batchOperations.Count, _options.MinBatchSize, _shardId);
                    }
                    await Task.Delay(_options.BatchFlushInterval, cancellationToken);
                    TryCollectJobOperationsForBatch(batchOperations);
                }

                // Process the batch of job operations
                if (batchOperations.Count > 0)
                {
                    try
                    {
                        LogFlushingBatch(_logger, batchOperations.Count, _shardId);
                        await AppendJobOperationBatchAsync(batchOperations, cancellationToken);

                        // Mark all operations as completed
                        foreach (var op in batchOperations)
                        {
                            op.CompletionSource.TrySetResult();
                        }
                    }
                    catch (Exception ex)
                    {
                        LogErrorWritingBatch(_logger, ex, batchOperations.Count, _shardId);
                        
                        // Mark all operations as failed
                        foreach (var op in batchOperations)
                        {
                            op.CompletionSource?.TrySetException(ex);
                        }
                    }
                    finally
                    {
                        batchOperations.Clear();
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore
        }
        finally
        {
            // Expected during shutdown - cancel all pending operations
            while (_storageOperationChannel.Reader.TryRead(out var operation))
            {
                operation.CompletionSource?.TrySetCanceled(cancellationToken);
            }
        }

        // Local function to collect job operations for batching. Returns true if more operations can be collected.
        bool TryCollectJobOperationsForBatch(List<StorageOperation> batchOperations)
        {
            // Collect more jobs, up to a maximum batch size
            while (batchOperations.Count < _options.MaxBatchSize && _storageOperationChannel.Reader.TryPeek(out var nextOperation))
            {
                if (nextOperation.Type is StorageOperationType.UpdateMetadata)
                {
                    // Stop batching if we encounter a metadata operation
                    return false;
                }
                _storageOperationChannel.Reader.TryRead(out var operation);
                Debug.Assert(operation != null);
                batchOperations.Add(operation!);
            }
            return batchOperations.Count != _options.MaxBatchSize;
        }
    }

    private async Task AppendJobOperationBatchAsync(List<StorageOperation> operations, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        using var stream = PooledBufferStream.Rent();
        try
        {
            stream.Position = 0; // TODO Remove that once PooledBufferStream fixed
            
            // Encode all job operations into a single stream
            foreach (var operation in operations)
            {
                NetstringJsonSerializer<JobOperation>.Encode(operation.JobOperation!.Value, stream, JobOperationJsonContext.Default.JobOperation);
            }
            var str = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            stream.Position = 0;
            var result = await BlobClient.AppendBlockAsync(
                stream,
                new AppendBlobAppendBlockOptions { Conditions = new AppendBlobRequestConditions { IfMatch = ETag } },
                cancellationToken);
            ETag = result.Value.ETag;
            CommitedBlockCount = result.Value.BlobCommittedBlockCount;
            
            sw.Stop();
            LogBatchWritten(_logger, operations.Count, _shardId, sw.ElapsedMilliseconds, CommitedBlockCount);
            
            // Warn if approaching the 50,000 block limit (warn at 80%)
            if (CommitedBlockCount > 40000)
            {
                LogApproachingBlockLimit(_logger, _shardId, CommitedBlockCount);
            }
            
            // Warn if batch is unusually large
            if (operations.Count > _options.MaxBatchSize * 0.8)
            {
                LogLargeBatch(_logger, _shardId, operations.Count, _options.MaxBatchSize);
            }
        }
        finally
        {
            PooledBufferStream.Return(stream);
        }
    }

    private async Task UpdateMetadataAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        var result = await BlobClient.SetMetadataAsync(
            metadata,
            new BlobRequestConditions { IfMatch = ETag },
            cancellationToken);
        ETag = result.Value.ETag;
    }

    /// <summary>
    /// Stops the background storage processor and waits for all pending operations to complete.
    /// After calling this method, no new storage operations can be enqueued.
    /// This method is idempotent and can be called multiple times safely.
    /// </summary>
    internal async Task StopProcessorAsync(CancellationToken cancellationToken)
    {
        LogStoppingProcessor(_logger, _shardId);
        
        // Complete the channel to stop accepting new operations (idempotent operation)
        if (_storageOperationChannel.Writer.TryComplete())
        {
            _shutdownCts.Cancel();
        }

        // Wait for the background processor to finish all pending operations
        try
        {
            await _storageProcessorTask.WaitAsync(cancellationToken);
            LogProcessorStopped(_logger, _shardId);
        }
        catch (OperationCanceledException)
        {
            // Expected during normal shutdown
            LogProcessorStopped(_logger, _shardId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopProcessorAsync(CancellationToken.None);
        _shutdownCts.Dispose();
    }

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Adding job '{JobId}' (Name='{JobName}') to shard '{ShardId}' with due time {DueTime}"
    )]
    private static partial void LogAddingJob(ILogger logger, string jobId, string jobName, string shardId, DateTimeOffset dueTime);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Removing job '{JobId}' from shard '{ShardId}'"
    )]
    private static partial void LogRemovingJob(ILogger logger, string jobId, string shardId);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Retrying job '{JobId}' in shard '{ShardId}' with new due time {NewDueTime}"
    )]
    private static partial void LogRetryingJob(ILogger logger, string jobId, string shardId, DateTimeOffset newDueTime);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Loading jobs from shard '{ShardId}'"
    )]
    private static partial void LogLoadingJobs(ILogger logger, string shardId);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Loaded {JobCount} job(s) from shard '{ShardId}' in {ElapsedMs}ms"
    )]
    private static partial void LogJobsLoaded(ILogger logger, string shardId, int jobCount, long elapsedMs);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Updating metadata for shard '{ShardId}'"
    )]
    private static partial void LogUpdatingMetadata(ILogger logger, string shardId);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Metadata updated for shard '{ShardId}'"
    )]
    private static partial void LogMetadataUpdated(ILogger logger, string shardId);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error updating metadata for shard '{ShardId}'"
    )]
    private static partial void LogErrorUpdatingMetadata(ILogger logger, Exception exception, string shardId);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Waiting for batch to reach minimum size (Current={CurrentSize}, Minimum={MinSize}) for shard '{ShardId}'"
    )]
    private static partial void LogWaitingForBatch(ILogger logger, int currentSize, int minSize, string shardId);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Flushing batch of {BatchSize} operation(s) for shard '{ShardId}'"
    )]
    private static partial void LogFlushingBatch(ILogger logger, int batchSize, string shardId);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error writing batch of {BatchSize} operation(s) for shard '{ShardId}'"
    )]
    private static partial void LogErrorWritingBatch(ILogger logger, Exception exception, int batchSize, string shardId);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Batch of {BatchSize} operation(s) written to shard '{ShardId}' in {ElapsedMs}ms (Total blocks: {BlockCount})"
    )]
    private static partial void LogBatchWritten(ILogger logger, int batchSize, string shardId, long elapsedMs, int blockCount);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Shard '{ShardId}' is approaching the append blob block limit ({BlockCount}/50000). Consider implementing blob rotation."
    )]
    private static partial void LogApproachingBlockLimit(ILogger logger, string shardId, int blockCount);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Large batch ({BatchSize}/{MaxBatchSize}) written to shard '{ShardId}'. Consider increasing MaxBatchSize if this is common."
    )]
    private static partial void LogLargeBatch(ILogger logger, string shardId, int batchSize, int maxBatchSize);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Stopping storage processor for shard '{ShardId}'"
    )]
    private static partial void LogStoppingProcessor(ILogger logger, string shardId);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Storage processor stopped for shard '{ShardId}'"
    )]
    private static partial void LogProcessorStopped(ILogger logger, string shardId);
}

internal enum StorageOperationType
{
    AppendJobOperation,
    UpdateMetadata
}

internal sealed class StorageOperation
{
    public required StorageOperationType Type { get; init; }
    public JobOperation? JobOperation { get; init; }
    public IDictionary<string, string>? Metadata { get; init; }
    public TaskCompletionSource CompletionSource { get; init; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public static StorageOperation CreateAppendOperation(JobOperation jobOperation)
    {
        return new StorageOperation
        {
            Type = StorageOperationType.AppendJobOperation,
            JobOperation = jobOperation
        };
    }

    public static StorageOperation CreateMetadataOperation(IDictionary<string, string> metadata)
    {
        return new StorageOperation
        {
            Type = StorageOperationType.UpdateMetadata,
            Metadata = metadata
        };
    }
}
