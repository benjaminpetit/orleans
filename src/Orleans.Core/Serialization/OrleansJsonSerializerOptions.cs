using System;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Orleans.Serialization
{
    public class OrleansJsonSerializerOptions : IJsonSerializerSettingsOptions
    {
        public JsonSerializerSettings JsonSerializerSettings { get; set; }

        public OrleansJsonSerializerOptions()
        {
            JsonSerializerSettings = OrleansJsonSerializerSettings.GetDefaultSerializerSettings();
        }
    }
}
