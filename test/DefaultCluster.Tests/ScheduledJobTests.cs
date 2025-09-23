using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestExtensions;
using Xunit;

namespace DefaultCluster.Tests;

public class ScheduledJobTests : HostedTestClusterEnsureDefaultStarted
{
    public ScheduledJobTests(DefaultClusterFixture fixture) : base(fixture)
    {
    }

    [Fact, TestCategory("BVT"), TestCategory("ScheduledJobs")]
    public async Task Test_ScheduledJobGrain()
    {
        var grain = this.GrainFactory.GetGrain<UnitTests.GrainInterfaces.IScheduledJobGrain>("test-job-grain");
        var scheduledTime = DateTime.UtcNow.AddSeconds(5);
        var job1 = await grain.ScheduleJobAsync("TestJob", scheduledTime);
        Assert.NotNull(job1);
        Assert.Equal("TestJob", job1.JobName);
        Assert.Equal(scheduledTime, job1.ScheduledTime);
        var job2 = await grain.ScheduleJobAsync("TestJob2", scheduledTime);
        var job3 = await grain.ScheduleJobAsync("TestJob3", scheduledTime.AddSeconds(2));
        var job4 = await grain.ScheduleJobAsync("TestJob4", scheduledTime);
        var job5 = await grain.ScheduleJobAsync("TestJob5", scheduledTime.AddSeconds(1));
        // Wait for the job to run
        await Task.Delay(TimeSpan.FromSeconds(10));
        Assert.True(await grain.HasJobRan(job1.JobId), "The scheduled job did not run as expected.");
        Assert.True(await grain.HasJobRan(job2.JobId), "The scheduled job did not run as expected.");
        Assert.True(await grain.HasJobRan(job3.JobId), "The scheduled job did not run as expected.");
        Assert.True(await grain.HasJobRan(job4.JobId), "The scheduled job did not run as expected.");
        Assert.True(await grain.HasJobRan(job5.JobId), "The scheduled job did not run as expected.");
    }
}
