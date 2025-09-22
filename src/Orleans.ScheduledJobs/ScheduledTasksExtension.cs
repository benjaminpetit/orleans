using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration.Internal;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Orleans.ScheduledJobs;

public static class  ScheduledTasksExtension
{
    public static ISiloBuilder AddInMemoryScheduledTasks(this ISiloBuilder builder)
    {
        return builder.ConfigureServices(services =>
        {
            services.AddSingleton<InMemoryJobShardManager>();
            services.AddFromExisting<JobShardManager, InMemoryJobShardManager>();
            services.AddSingleton<LocalScheduledJobManager>();
            services.AddFromExisting<ILocalScheduledJobManager, LocalScheduledJobManager>();
            services.AddFromExisting<ILifecycleParticipant<ISiloLifecycle>, LocalScheduledJobManager>();
        });
    }
}
