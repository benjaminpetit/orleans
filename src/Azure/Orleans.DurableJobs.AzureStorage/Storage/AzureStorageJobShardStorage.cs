using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Orleans.DurableJobs.Storage;
using Orleans.Serialization.Buffers.Adaptors;

namespace Orleans.DurableJobs.AzureStorage.Storage;

/// <summary>
/// Azure Blob Storage implementation for job persistence.
/// Encapsulates all Azure-specific logic including batching, channels, and append blob operations.
/// </summary>
public sealed partial class AzureStorageJobShardStorage : IJobShardStorage
{
    private readonly AppendBlobClient _blobClient;
    private readonly Channel<StorageOperation> _operationChannel;
    private readonly Task _backgroundProcessor;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly AzureStorageJobShardOptions _options;
    private readonly ILogger<AzureStorageJobShardStorage> _logger;
    private readonly string _shardId;
    private ETag? _etag;
    
    /// <summary>
    /// Gets the number of committed blocks in the append blob.
    /// This is useful for testing batching behavior.
    /// </summary>
    public int CommittedBlockCount { get; private set; }
    
    /// <summary>
    /// Gets the blob client used for storage operations.
    /// </summary>
    public AppendBlobClient BlobClient => _blobClient;
    
    public AzureStorageJobShardStorage(
        string shardId,
        AppendBlobClient blobClient,
        ETag? initialETag,
        AzureStorageJobShardOptions options,
        ILogger<AzureStorageJobShardStorage> logger)
    {
        _shardId = shardId;
        _blobClient = blobClient;
        _etag = initialETag;
        _options = options;
        _logger = logger;
        
        // Create unbounded channel for storage operations
        _operationChannel = Channel.CreateUnbounded<StorageOperation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        
        // Start background processor for batching
        _backgroundProcessor = ProcessStorageOperationsAsync();
    }
    
    public async Task<IReadOnlyList<JobStorageRecord>> LoadAllJobsAsync(CancellationToken ct)
    {
        LogInitializing(_logger, _shardId);
        var sw = Stopwatch.StartNew();
        
        // Download blob content
        var response = await _blobClient.DownloadAsync(cancellationToken: ct);
        using var stream = response.Value.Content;
        
        // Replay operations to reconstruct state
        var addedJobs = new Dictionary<string, JobOperation>();
        var deletedJobs = new HashSet<string>();
        var jobRetryInfo = new Dictionary<string, (int dequeueCount, DateTimeOffset? newDueTime)>();
        
        await foreach (var operation in NetstringJsonSerializer<JobOperation>.DecodeAsync(
            stream, 
            JobOperationJsonContext.Default.JobOperation, 
            ct))
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
                    jobRetryInfo.Remove(operation.Id);
                    break;
                    
                case JobOperation.OperationType.Retry:
                    if (!deletedJobs.Contains(operation.Id))
                    {
                        if (!jobRetryInfo.ContainsKey(operation.Id))
                        {
                            jobRetryInfo[operation.Id] = (1, operation.DueTime);
                        }
                        else
                        {
                            var entry = jobRetryInfo[operation.Id];
                            jobRetryInfo[operation.Id] = (entry.dequeueCount + 1, operation.DueTime);
                        }
                    }
                    break;
            }
        }
        
        // Convert to JobStorageRecord list
        var records = new List<JobStorageRecord>();
        foreach (var op in addedJobs.Values)
        {
            var dequeueCount = 0;
            var dueTime = op.DueTime!.Value;
            
            if (jobRetryInfo.TryGetValue(op.Id, out var retryEntry))
            {
                dequeueCount = retryEntry.dequeueCount;
                dueTime = retryEntry.newDueTime ?? dueTime;
            }
            
            records.Add(new JobStorageRecord(
                op.Id,
                op.Name!,
                op.TargetGrainId!.Value,
                dueTime,
                op.Metadata,
                dequeueCount));
        }
        
        _etag = response.Value.Details.ETag;
        
        sw.Stop();
        LogInitialized(_logger, _shardId, records.Count, sw.ElapsedMilliseconds);
        
        return records;
    }
    
    public async Task AddJobAsync(JobStorageRecord record, CancellationToken ct)
    {
        LogAddingJob(_logger, record.JobId, record.JobName, _shardId, record.DueTime);
        var operation = JobOperation.CreateAddOperation(
            record.JobId,
            record.JobName,
            record.DueTime,
            record.TargetGrainId,
            record.Metadata);
        await EnqueueAndWaitAsync(StorageOperation.CreateAppendOperation(operation), ct);
    }
    
    public async Task RemoveJobAsync(string jobId, CancellationToken ct)
    {
        LogRemovingJob(_logger, jobId, _shardId);
        var operation = JobOperation.CreateRemoveOperation(jobId);
        await EnqueueAndWaitAsync(StorageOperation.CreateAppendOperation(operation), ct);
    }
    
    public async Task UpdateJobDueTimeAsync(string jobId, DateTimeOffset newDueTime, int newDequeueCount, CancellationToken ct)
    {
        LogRetryingJob(_logger, jobId, _shardId, newDueTime);
        var operation = JobOperation.CreateRetryOperation(jobId, newDueTime);
        await EnqueueAndWaitAsync(StorageOperation.CreateAppendOperation(operation), ct);
    }
    
    /// <summary>
    /// Updates the blob metadata. This is useful for ownership tracking.
    /// </summary>
    public async Task UpdateBlobMetadataAsync(IDictionary<string, string> metadata, CancellationToken ct)
    {
        LogUpdatingMetadata(_logger, _shardId);
        await EnqueueAndWaitAsync(StorageOperation.CreateMetadataOperation(metadata), ct);
    }
    
    private async Task EnqueueAndWaitAsync(StorageOperation operation, CancellationToken ct)
    {
        await _operationChannel.Writer.WriteAsync(operation, ct);
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
            while (await _operationChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                // Read first operation
                if (!_operationChannel.Reader.TryRead(out var firstOperation))
                {
                    continue;
                }
                
                // Handle metadata operations immediately (cannot be batched)
                if (firstOperation.Type is StorageOperationType.UpdateMetadata)
                {
                    try
                    {
                        await UpdateMetadataInternalAsync(firstOperation.Metadata!, cancellationToken);
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
            // Expected during shutdown
        }
        finally
        {
            // Cancel all pending operations
            while (_operationChannel.Reader.TryRead(out var operation))
            {
                operation.CompletionSource?.TrySetCanceled(cancellationToken);
            }
        }
        
        // Local function to collect job operations for batching. Returns true if more operations can be collected.
        bool TryCollectJobOperationsForBatch(List<StorageOperation> batchOperations)
        {
            // Collect more jobs, up to a maximum batch size
            while (batchOperations.Count < _options.MaxBatchSize && _operationChannel.Reader.TryPeek(out var nextOperation))
            {
                if (nextOperation.Type is StorageOperationType.UpdateMetadata)
                {
                    // Stop batching if we encounter a metadata operation
                    return false;
                }
                _operationChannel.Reader.TryRead(out var operation);
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
            
            stream.Position = 0;
            var result = await _blobClient.AppendBlockAsync(
                stream,
                new AppendBlobAppendBlockOptions { Conditions = new AppendBlobRequestConditions { IfMatch = _etag } },
                cancellationToken);
            _etag = result.Value.ETag;
            CommittedBlockCount = result.Value.BlobCommittedBlockCount;
            
            sw.Stop();
            LogBatchWritten(_logger, operations.Count, _shardId, sw.ElapsedMilliseconds, CommittedBlockCount);
            
            // Warn if approaching the 50,000 block limit (warn at 80%)
            if (CommittedBlockCount > 40000)
            {
                LogApproachingBlockLimit(_logger, _shardId, CommittedBlockCount);
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
    
    private async Task UpdateMetadataInternalAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        var result = await _blobClient.SetMetadataAsync(
            metadata,
            new BlobRequestConditions { IfMatch = _etag },
            cancellationToken);
        _etag = result.Value.ETag;
    }
    
    public async ValueTask DisposeAsync()
    {
        LogStoppingProcessor(_logger, _shardId);
        
        // Complete channel and signal shutdown
        _operationChannel.Writer.TryComplete();
        _shutdownCts.Cancel();
        
        // Wait for background processor to finish
        try
        {
            await _backgroundProcessor;
            LogProcessorStopped(_logger, _shardId);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
            LogProcessorStopped(_logger, _shardId);
        }
        
        _shutdownCts.Dispose();
    }
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
