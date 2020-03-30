using Orleans;

namespace UnitTests.GrainInterfaces.Directory
{
    public interface IDefaultDirectoryGrain : IGrainWithGuidKey, ICommonDirectoryGrain
    { }
}
