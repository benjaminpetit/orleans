using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Storage;

namespace Orleans.Hosting
{
    /// <summary>
    /// Base interface for configuring the grain storage providers
    /// </summary>
    public interface IGrainStorageProviderConfigurator : INamedServiceConfigurator
    {
    }

    /// <summary>
    /// Class used to  configuring the grain storage providers
    /// </summary>
    public class GrainStorageProviderConfigurator : NamedServiceConfigurator, IGrainStorageProviderConfigurator
    {
        public GrainStorageProviderConfigurator(
            string name,
            Action<Action<IServiceCollection>> configureDelegate)
            : base(name, configureDelegate)
        {
        }
    }

    public static class GrainStorageProviderConfiguratorExtensions
    {
        /// <summary>
        /// Set the serializer to use
        /// </summary>
        public static void ConfigureSerializer(
            this IGrainStorageProviderConfigurator self,
            Func<IServiceProvider, string, IGrainStorageSerializer> factory)
        {
            self.ConfigureComponent(factory);
        }

        /// <summary>
        /// Configure the storage to use
        /// </summary>
        public static void ConfigureStorage<T>(
            this IGrainStorageProviderConfigurator self,
            Func<IServiceProvider, string, IGrainStorage> grainStorageFactory,
            Action<OptionsBuilder<T>> configureOptions)
             where T : class, new()
        {
            self.ConfigureComponent(grainStorageFactory, configureOptions);
        }

        /// <summary>
        /// Use the Orleans built-in serializer.
        /// Fast, but not backward compatible, and hard to use outside Orleans
        /// </summary>
        public static void UseOrleansSerializer(this IGrainStorageProviderConfigurator self, bool useJsonAsFallback = true)
        {
            self.ConfigureDelegate.Invoke(sp => sp.TryAddSingleton<OrleansGrainStorageSerializer>());
            if (useJsonAsFallback)
            {
                self.ConfigureSerializer((sp, name) => sp.GetService<OrleansGrainStorageSerializer>());
            }
            else
            {
                self.ConfigureDelegate.Invoke(sp => sp.TryAddSingleton<JsonGrainStorageSerializer>());
                self.ConfigureSerializer((sp, name) => sp.GetService<GrainStorageSerializer>());
            }
        }

        /// <summary>
        /// Use the Orleans built-in serializer.
        /// Fast, but not backward compatible, and hard to use outside Orleans
        /// </summary>
        public static void UseJsonSerializer(this IGrainStorageProviderConfigurator self, bool useOrleansBinaryAsFallback = true)
        {
            self.ConfigureDelegate.Invoke(sp => sp.TryAddSingleton<JsonGrainStorageSerializer>());
            self.ConfigureSerializer((sp, name) => sp.GetService<JsonGrainStorageSerializer>());
        }

        /// <summary>
        /// Add a grain storage provider
        /// </summary>
        public static ISiloHostBuilder AddGrainStorage(this ISiloHostBuilder self, string name, Action<IGrainStorageProviderConfigurator> configure)
        {
            var configurator = new GrainStorageProviderConfigurator(name, configureDelegate => self.ConfigureServices(configureDelegate));
            configure.Invoke(configurator);
            return self;
        }

        /// <summary>
        /// Add a grain storage provider
        /// </summary>
        public static ISiloBuilder AddGrainStorage(this ISiloBuilder self, string name, Action<IGrainStorageProviderConfigurator> configure)
        {
            var configurator = new GrainStorageProviderConfigurator(name, configureDelegate => self.ConfigureServices(configureDelegate));
            configure.Invoke(configurator);
            return self;
        }
    }
}
