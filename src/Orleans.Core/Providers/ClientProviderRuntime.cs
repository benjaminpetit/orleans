using System;
using Orleans.Runtime;

namespace Orleans.Providers
{
    /// <summary>
    /// <see cref="IProviderRuntime"/> for clients.
    /// </summary>
    /// <seealso cref="Orleans.Providers.IProviderRuntime" />
    internal class ClientProviderRuntime : IProviderRuntime
    {
        private readonly IInternalGrainFactory grainFactory;
        private readonly ClientGrainContext clientContext;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientProviderRuntime"/> class.
        /// </summary>
        /// <param name="grainFactory">The grain factory.</param>
        /// <param name="serviceProvider">The service provider.</param>
        /// <param name="clientContext">The client context.</param>
        public ClientProviderRuntime(
            IInternalGrainFactory grainFactory,
            IServiceProvider serviceProvider,
            ClientGrainContext clientContext)
        {
            this.grainFactory = grainFactory;
            this.ServiceProvider = serviceProvider;
            this.clientContext = clientContext;
        }

        /// <inheritdoc/>
        public IGrainFactory GrainFactory => this.grainFactory;

        /// <inheritdoc/>
        public IServiceProvider ServiceProvider { get; }

        /// <inheritdoc/>
        public (TExtensionInterface, TExtension) BindExtension<TExtensionInterface, TExtension>(Func<TExtension> newExtensionFunc)
            where TExtensionInterface : IGrainExtension
            where TExtension : TExtensionInterface
        {
            return this.clientContext.GetOrSetExtension<TExtensionInterface, TExtension>(newExtensionFunc);
        }
    }
}
