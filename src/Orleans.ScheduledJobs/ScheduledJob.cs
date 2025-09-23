using System;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

public interface IScheduledJob
{
    string JobId { get; init; }
    string JobName { get; init; }
    DateTime ScheduledTime { get; init; }
    GrainId TargetGrainId { get; init; }
}

[GenerateSerializer]
public class ScheduledJob : IScheduledJob
{
    [Id(0)]
    public string JobId { get; init; }
    [Id(1)]
    public string JobName { get; init; }
    [Id(2)]
    public DateTime ScheduledTime { get; init; }
    [Id(3)]
    public GrainId TargetGrainId { get; init; }
}
