using System;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

public interface IScheduledJob
{
    string Id { get; init; }
    string Name { get; init; }
    DateTime ScheduledAt { get; init; }
    GrainId TargetGrainId { get; init; }
    string ShardId { get; init; }
}

[GenerateSerializer]
[Alias("Orleans.ScheduledJobs.ScheduledJob")]
internal class ScheduledJob : IScheduledJob
{
    [Id(0)]
    public required string Id { get; init; }
    [Id(1)]
    public required string Name { get; init; }
    [Id(2)]
    public DateTime ScheduledAt { get; init; }
    [Id(3)]
    public GrainId TargetGrainId { get; init; }
    [Id(4)]
    public required string ShardId { get; init; }
}
