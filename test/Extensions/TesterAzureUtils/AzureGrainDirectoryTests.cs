using System;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Xunit;

namespace Tester.AzureUtils
{
    [TestCategory("Azure"), TestCategory("Storage")]
    public class AzureGrainDirectoryTests : AzureStorageBasicTests
    {
    }

    // TODO Move that into a common project
    public abstract class GrainDirectoryTests
    {
        private IGrainDirectory grainDirectory;

        protected GrainDirectoryTests(IGrainDirectory grainDirectory)
        {
            this.grainDirectory = grainDirectory ?? throw new ArgumentNullException(nameof(grainDirectory));
        }

        [SkippableFact]
        public async Task RegisterLookupUnregisterLookup()
        {
            var expected = new GrainAddress
            {
                ActivationId = Guid.NewGuid().ToString("N"),
                GrainId = "user/someraondomuser_" + Guid.NewGuid().ToString("N"),
                SiloAddress = "10.0.23.12:1000@5678"
            };

            Assert.Equal(expected, await this.grainDirectory.Register(expected));

            Assert.Equal(expected, await this.grainDirectory.Lookup(expected.GrainId));

            await this.grainDirectory.Unregister(expected);

            Assert.Null(await this.grainDirectory.Lookup(expected.GrainId));
        }

        [SkippableFact]
        public async Task DoNotOverrideEntry()
        {
            var expected = new GrainAddress
            {
                ActivationId = Guid.NewGuid().ToString("N"),
                GrainId = "user/someraondomuser_" + Guid.NewGuid().ToString("N"),
                SiloAddress = "10.0.23.12:1000@5678"
            };

            var differentActivation = new GrainAddress
            {
                ActivationId = Guid.NewGuid().ToString("N"),
                GrainId = expected.GrainId,
                SiloAddress = "10.0.23.12:1000@5678"
            };

            var differentSilo = new GrainAddress
            {
                ActivationId = expected.ActivationId,
                GrainId = expected.GrainId,
                SiloAddress = "10.0.23.14:1000@4583"
            };

            Assert.Equal(expected, await this.grainDirectory.Register(expected));
            Assert.Equal(expected, await this.grainDirectory.Register(differentActivation));
            Assert.Equal(expected, await this.grainDirectory.Register(differentSilo));

            Assert.Equal(expected, await this.grainDirectory.Lookup(expected.GrainId));
        }

        [SkippableFact]
        public async Task LookupNotFound()
        {
            Assert.Null(await this.grainDirectory.Lookup("user/someraondomuser_" + Guid.NewGuid().ToString("N")));
        }
    }
}
