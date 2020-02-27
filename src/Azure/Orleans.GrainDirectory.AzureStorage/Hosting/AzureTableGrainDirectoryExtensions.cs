using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;
using Orleans.GrainDirectory.AzureStorage;
using Orleans.Runtime;

namespace Orleans.Hosting
{
    public static class AzureTableGrainDirectoryExtensions
    {
        public static ISiloHostBuilder UseAzureTableGrainDirectoryAsDefault(
            this ISiloHostBuilder builder,
            Action<AzureTableGrainDirectoryOptions> configureOptions)
        {
            return builder.UseAzureTableGrainDirectoryAsDefault(ob => ob.Configure(configureOptions));
        }

        public static ISiloHostBuilder UseAzureTableGrainDirectoryAsDefault(
            this ISiloHostBuilder builder,
            Action<OptionsBuilder<AzureTableGrainDirectoryOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseAzureTableGrainDirectoryAsDefault(configureOptions));
        }

        public static ISiloBuilder UseAzureTableGrainDirectoryAsDefault(
            this ISiloBuilder builder,
            Action<AzureTableGrainDirectoryOptions> configureOptions)
        {
            return builder.UseAzureTableGrainDirectoryAsDefault(ob => ob.Configure(configureOptions));
        }

        public static ISiloBuilder UseAzureTableGrainDirectoryAsDefault(
            this ISiloBuilder builder,
            Action<OptionsBuilder<AzureTableGrainDirectoryOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseAzureTableGrainDirectoryAsDefault(configureOptions));
        }

        private static IServiceCollection UseAzureTableGrainDirectoryAsDefault(
            this IServiceCollection services,
            Action<OptionsBuilder<AzureTableGrainDirectoryOptions>> configureOptions)
        {
            configureOptions.Invoke(services.AddOptions<AzureTableGrainDirectoryOptions>());
            return services
                .AddTransient<IConfigurationValidator, AzureTableGrainDirectoryOptionsValidator>()
                .ConfigureFormatter<AzureTableGrainDirectoryOptions>()
                .AddSingleton<IGrainDirectory, AzureTableGrainDirectory>()
                .AddSingleton<ILifecycleParticipant<ISiloLifecycle>, AzureTableGrainDirectory>();
        }
    }
}
