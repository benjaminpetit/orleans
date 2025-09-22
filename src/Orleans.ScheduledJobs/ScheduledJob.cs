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

public class ScheduledJob : IScheduledJob
{
    public string JobId { get; init; }
    public string JobName { get; init; }
    public DateTime ScheduledTime { get; init; }
    public GrainId TargetGrainId { get; init; }
}
