using System;
using Orleans.Providers;

namespace Orleans.Runtime.Providers
{
    internal class SiloProviderRuntime : IProviderRuntime
    {
        private readonly IGrainContextAccessor _grainContextAccessor;

        public SiloProviderRuntime(
            IGrainContextAccessor grainContextAccessor,
            IGrainFactory grainFactory,
            IServiceProvider serviceProvider)
        {
            _grainContextAccessor = grainContextAccessor;
            GrainFactory = grainFactory;
            ServiceProvider = serviceProvider;
        }

        public IGrainFactory GrainFactory { get; }

        public IServiceProvider ServiceProvider { get; }

        public (TExtensionInterface, TExtension) BindExtension<TExtensionInterface, TExtension>(Func<TExtension> newExtensionFunc)
            where TExtensionInterface : IGrainExtension
            where TExtension : TExtensionInterface
        {
            return _grainContextAccessor.GrainContext.GetComponent<IGrainExtensionBinder>().GetOrSetExtension<TExtensionInterface, TExtension>(newExtensionFunc);
        }
    }
}
