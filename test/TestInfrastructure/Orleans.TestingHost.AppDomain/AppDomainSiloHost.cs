using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Hosting;

namespace Orleans.TestingHost
{
    /// <summary>
    /// Allows programmatically hosting an Orleans silo in the current app domain, exposing some marshallable members via remoting.
    /// </summary>
    public class AppDomainSiloHost : MarshalByRefObject
    {
        private readonly ISiloHost host;

        /// <summary>Creates and initializes a silo in the current app domain.</summary>
        /// <param name="appDomainName">Name of this silo.</param>
        /// <param name="serializedConfigurationSources">Silo config data to be used for this silo.</param>
        public AppDomainSiloHost(string appDomainName, string serializedConfigurationSources)
        {
            try
            {
                var deserializedSources = TestClusterHostFactory.DeserializeConfigurationSources(serializedConfigurationSources);
                this.host = TestClusterHostFactory.CreateSiloHost(appDomainName, deserializedSources);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.ToString());
            }
        }

        /// <summary> SiloAddress for this silo. </summary>
        public SiloAddress SiloAddress => this.host.Services.GetRequiredService<ILocalSiloDetails>().SiloAddress;

        /// <summary> Gateway address for this silo. </summary>
        public SiloAddress GatewayAddress => this.host.Services.GetRequiredService<ILocalSiloDetails>().GatewayAddress;

        /// <summary>Starts the silo</summary>
        public void Start()
        {
            try
            {
                this.host.StartAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.ToString());
            }
        }

        /// <summary>Gracefully shuts down the silo</summary>
        public void Shutdown()
        {
            try
            {
                this.host.StopAsync().GetAwaiter().GetResult();
            }
            finally
            {
                this.host.Dispose();
            }
        }
    }
}
