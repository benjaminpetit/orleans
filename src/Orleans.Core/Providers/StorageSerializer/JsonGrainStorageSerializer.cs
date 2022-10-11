using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Serialization;

namespace Orleans.Storage
{
    /// <summary>
    /// Options for <see cref="JsonGrainStorageSerializer"/>.
    /// </summary>
    public class JsonGrainStorageSerializerOptions : IJsonSerializerSettingsOptions
    {
        public JsonSerializerSettings JsonSerializerSettings { get; set; } = OrleansJsonSerializerSettings.GetDefaultSerializerSettings();
    }

    /// <summary>
    /// Grain storage serializer that uses Newtonsoft.Json
    /// </summary>
    public class JsonGrainStorageSerializer : IGrainStorageSerializer
    {
        private JsonSerializerSettings _jsonSettings;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonGrainStorageSerializer"/> class.
        /// </summary>
        /// <param name="options">The serializer options.</param>
        /// <param name="services">The service provider.</param>
        public JsonGrainStorageSerializer(IOptions<JsonGrainStorageSerializerOptions> options, IServiceProvider services)
        {
            this._jsonSettings = options.Value.JsonSerializerSettings;
        }

        /// <inheritdoc/>
        public BinaryData Serialize<T>(T value)
        {
            var data = JsonConvert.SerializeObject(value, this._jsonSettings);
            return new BinaryData(data);
        }

        /// <inheritdoc/>
        public T Deserialize<T>(BinaryData input)
        {
            return JsonConvert.DeserializeObject<T>(input.ToString(), this._jsonSettings);
        }
    }
}
