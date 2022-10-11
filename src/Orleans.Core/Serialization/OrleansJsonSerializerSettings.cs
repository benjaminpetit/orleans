using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.GrainReferences;
using Orleans.Serialization.TypeSystem;

namespace Orleans.Serialization
{
    public static class OrleansJsonSerializerSettings
    {
        public static JsonSerializerSettings GetDefaultSerializerSettings()
        {
            return new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                DefaultValueHandling = DefaultValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore,
                NullValueHandling = NullValueHandling.Ignore,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                Formatting = Formatting.None,
                SerializationBinder = null,
            };
        }

        public static JsonSerializerSettings GetDefaultSerializerSettings(IServiceProvider services)
        {
            var settings = GetDefaultSerializerSettings();
            Configure(services, settings);
            return settings;
        }

        public static void Configure(IServiceProvider services, JsonSerializerSettings jsonSerializerSettings)
        {
            if (jsonSerializerSettings.SerializationBinder == null)
            {
                var typeResolver = services.GetRequiredService<TypeResolver>();
                jsonSerializerSettings.SerializationBinder = new OrleansJsonSerializationBinder(typeResolver);
            }

            jsonSerializerSettings.Converters.Add(new IPAddressConverter());
            jsonSerializerSettings.Converters.Add(new IPEndPointConverter());
            jsonSerializerSettings.Converters.Add(new GrainIdConverter());
            jsonSerializerSettings.Converters.Add(new ActivationIdConverter());
            jsonSerializerSettings.Converters.Add(new SiloAddressJsonConverter());
            jsonSerializerSettings.Converters.Add(new MembershipVersionJsonConverter());
            jsonSerializerSettings.Converters.Add(new UniqueKeyConverter());
            jsonSerializerSettings.Converters.Add(new GrainReferenceJsonConverter(services.GetRequiredService<GrainReferenceActivator>()));
        }
    }

    public interface IJsonSerializerSettingsOptions
    {
        public JsonSerializerSettings JsonSerializerSettings { get; set; }

    }

    public class ConfigureJsonSerializerSettingsOptions : IPostConfigureOptions<IJsonSerializerSettingsOptions>
    {
        private readonly IServiceProvider _serviceProvider;

        public ConfigureJsonSerializerSettingsOptions(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public void PostConfigure(string name, IJsonSerializerSettingsOptions options)
        {
            OrleansJsonSerializerSettings.Configure(_serviceProvider, options.JsonSerializerSettings);
        }
    }
}
