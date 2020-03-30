using System.Threading.Tasks;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.GrainDirectory;
using UnitTests.GrainInterfaces.Directory;

namespace UnitTests.Grains.Directory
{
    [GrainDirectory(GrainDirectoryName = DIRECTORY), TypeCodeOverride(TYPECODE)]
    public class AzureTableDirectoryGrain : Grain, IAzureTableDirectoryGrain
    {
        private int counter = 0;

        public const int TYPECODE = 1000;

        public const string DIRECTORY = "AzureTable";

        public Task<int> Ping() => Task.FromResult(++this.counter);

        public Task Reset()
        {
            counter = 0;
            return Task.CompletedTask;
        }
    }
}
