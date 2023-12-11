using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Metadata;
using Orleans.Storage;

namespace Orleans.Runtime.Providers
{
    internal class ProvidersValidator : ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly ImmutableDictionary<GrainType, GrainProperties> _grains;
        private readonly IServiceProvider _serviceProvider;
        private readonly ProvidersValidatorOptions _options;

        public ProvidersValidator(
            IServiceProvider serviceProvider,
            SiloManifestProvider siloManifestProvider,
            IOptions<ProvidersValidatorOptions> options)
        {
            _grains = siloManifestProvider.SiloManifest.Grains;
            _serviceProvider = serviceProvider;
            _options = options.Value;
        }

        public void Participate(ISiloLifecycle observer)
        {
            if (_options.IsEnabled)
            {
                observer.Subscribe(nameof(ProvidersValidator), ServiceLifecycleStage.Active - 1, onStart: Validate);
            }
        }

        internal Task Validate(CancellationToken ct)
        {
            List<Exception> errors = null;
            foreach (var grain in _grains)
            {
                if (grain.Value.Properties.TryGetValue(WellKnownGrainTypeProperties.StorageProvider, out var providerNames))
                {
                    foreach (var providerName in providerNames.Split(','))
                    {
                        var provider = _serviceProvider.GetKeyedService<IGrainStorage>(providerName);
                        if (provider == null)
                        {
                            errors ??= new List<Exception>();
                            errors.Add(new OrleansConfigurationException($"Provider '{providerName}' needed for grain type '{grain.Key}' is not configured."));
                        }
                    }
                }
            }
            if (errors != null)
            {
                if (errors.Count == 1)
                {
                    throw errors[0];
                }
                throw new AggregateException(errors);
            }
            return Task.CompletedTask;
        }
    }
}

namespace Orleans.Configuration
{
    /// <summary>
    /// Configure the ProvidersValidator
    /// </summary>
    public class ProvidersValidatorOptions
    {
        /// <summary>
        /// If set to false, the validator will be disabled.
        /// </summary>
        public bool IsEnabled { get; set; } = true;
    }
}
