using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.ScheduledJobs;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains;

public class ScheduledJobGrain : Grain, IScheduledJobGrain, IScheduledJobReceiver
{
    private Dictionary<string, bool> jobRunStatus = new();
    private readonly ILocalScheduledJobManager _localScheduledJobManager;

    public ScheduledJobGrain(ILocalScheduledJobManager localScheduledJobManager)
    {
        _localScheduledJobManager = localScheduledJobManager;
    }

    public Task<bool> HasJobRan(string jobId)
    {
        return Task.FromResult(jobRunStatus.TryGetValue(jobId, out var ran) && ran);
    }

    public Task ReceiveScheduledJobAsync(IScheduledJob job)
    {
        jobRunStatus[job.JobId] = true;
        return Task.CompletedTask;
    }

    public async Task<IScheduledJob> ScheduleJobAsync(string jobName, DateTime scheduledTime)
    {
        var job = await _localScheduledJobManager.ScheduleJobAsync(this, jobName, scheduledTime);
        jobRunStatus[job.JobId] = false;
        return job;
    }
}
