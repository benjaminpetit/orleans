using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Orleans.Metadata;
using Orleans.Providers;

namespace Orleans.Runtime
{
    /// <summary>
    /// Specifies options for the <see cref="IPersistentState{TState}"/> constructor argument which it is applied to.
    /// </summary>
    /// <seealso cref="System.Attribute" />
    /// <seealso cref="Orleans.IFacetMetadata" />
    /// <seealso cref="Orleans.Runtime.IPersistentStateConfiguration" />
    [AttributeUsage(AttributeTargets.Parameter)]
    public class PersistentStateAttribute : Attribute, IFacetMetadata, IPersistentStateConfiguration, IGrainPropertiesProviderAttribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PersistentStateAttribute"/> class.
        /// </summary>
        /// <param name="stateName">Name of the state.</param>
        /// <param name="storageName">Name of the storage provider.</param>
        public PersistentStateAttribute(string stateName, string storageName = null)
        {
            ArgumentNullException.ThrowIfNull(stateName);
            this.StateName = stateName;
            this.StorageName = storageName ?? ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME;
        }

        /// <summary>
        /// Gets the name of the state.
        /// </summary>
        /// <value>The name of the state.</value>
        public string StateName { get; }

        /// <summary>
        /// Gets the name of the storage provider.
        /// </summary>
        /// <value>The name of the storage provider.</value>
        public string StorageName { get; }

        public void Populate(IServiceProvider services, Type grainClass, GrainType grainType, Dictionary<string, string> properties)
        {
            if (properties.TryGetValue(WellKnownGrainTypeProperties.StorageProvider, out var value))
            {
                properties[WellKnownGrainTypeProperties.StorageProvider] = string.Concat(value, ",", StorageName);
            }
            properties[WellKnownGrainTypeProperties.StorageProvider] = StorageName;
        }
    }
}
