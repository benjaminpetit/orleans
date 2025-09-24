using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs.AzureStorage;

internal sealed class AzureStorageJobShard : JobShard
{
    private readonly AppendBlobClient _blobClient;
    private readonly ScheduledJobQueue _jobQueue = new ();
    private ETag? _etag;

    public AzureStorageJobShard(string id, DateTime startTime, DateTime endTime, AppendBlobClient blobClient)
        : base(id, startTime, endTime)
    {
        _blobClient = blobClient;
    }

    public override async ValueTask<int> GetJobCount()
    {
        await EnsureIsInitializedAsync();
        return _jobQueue.Count;
    }

    public override IAsyncEnumerable<IScheduledJob> ReadJobsAsync()
    {
        return _jobQueue;
    }

    public override async Task RemoveJobAsync(string jobId)
    {
        await EnsureIsInitializedAsync();
        var operation = JobOperation.CreateRemoveOperation(jobId);
        await AppendOperation(operation);
    }

    public override async Task<IScheduledJob> ScheduleJobAsync(GrainId target, string jobName, DateTime scheduledAt)
    {
        await EnsureIsInitializedAsync();
        var jobId = Guid.NewGuid().ToString();
        var operation = JobOperation.CreateAddOperation(jobId, jobName, scheduledAt, target);
        await AppendOperation(operation);
        return new ScheduledJob
        {
            Id = jobId,
            Name = jobName,
            ScheduledAt = scheduledAt,
            TargetGrainId = target,
            ShardId = Id
        };
    }

    private async Task AppendOperation(JobOperation operation)
    {
        var result = await _blobClient.AppendBlockAsync(
                    BinaryData.FromObjectAsJson(operation).ToStream(),
                    new AppendBlobAppendBlockOptions { Conditions = new AppendBlobRequestConditions { IfMatch = _etag } });
        _etag = result.Value.ETag;
    }

    private async ValueTask EnsureIsInitializedAsync()
    {
        if (_etag is not null) return; // already initialized

        if (!await _blobClient.ExistsAsync())
        {
            // Create new blob
            var response = await _blobClient.CreateIfNotExistsAsync();
            _etag = response.Value.ETag;
            return;
        }
        else
        {
            // Load existing blob
            var response = await _blobClient.DownloadAsync();
            using var stream = response.Value.Content;
            using var reader = new StreamReader(stream);

            // Rebuild state by replaying operations
            var dictionary = new Dictionary<string, JobOperation>();
            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(line)) continue;
                var operation = JsonSerializer.Deserialize<JobOperation>(line);
                switch (operation.Type)
                {
                    case JobOperation.OperationType.Add:
                        dictionary[operation.Id] = operation;
                        break;
                    case JobOperation.OperationType.Remove:
                        dictionary.Remove(operation.Id);
                        break;
                }
            }
            // Rebuild the priority queue
            foreach (var op in dictionary.Values)
            {
                _jobQueue.Enqueue(new ScheduledJob
                {
                    Id = op.Id,
                    Name = op.Name!,
                    ScheduledAt = op.ScheduledAt!.Value,
                    TargetGrainId = op.TargetGrainId!.Value,
                    ShardId = Id
                });
            }
            _jobQueue.Freeze();

            _etag = response.Value.Details.ETag;
        }
    }
}

internal struct JobOperation
{
    public enum OperationType
    {
        Add,
        Remove
    }

    public OperationType Type { get; init; }

    public string Id { get; init; }
    public string? Name { get; init; }
    public DateTime? ScheduledAt { get; init; }
    public GrainId? TargetGrainId { get; init; }

    public static JobOperation CreateAddOperation(string id, string name, DateTime scheduledAt, GrainId targetGrainId) =>
        new() { Type = OperationType.Add, Id = id, Name = name, ScheduledAt = scheduledAt, TargetGrainId = targetGrainId };

    public static JobOperation CreateRemoveOperation(string id) =>
        new() { Type = OperationType.Remove, Id = id };
}
