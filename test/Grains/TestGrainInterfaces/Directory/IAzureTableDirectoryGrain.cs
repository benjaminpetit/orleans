using Orleans;

namespace UnitTests.GrainInterfaces.Directory
{
    public interface IAzureTableDirectoryGrain : IGrainWithGuidKey, ICommonDirectoryGrain
    { }
}
