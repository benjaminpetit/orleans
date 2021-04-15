using Orleans;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public interface ICustomContainerImplicitSubscriptionGrain : IGrainWithGuidKey
    {
        Task<int> GetEventCounter();

        Task<int> GetErrorCounter();

        Task Deactivate();
    }
}