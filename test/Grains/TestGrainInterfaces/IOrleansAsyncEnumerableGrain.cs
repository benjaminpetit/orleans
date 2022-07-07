using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces;

public interface IOrleansAsyncEnumerableGrain : IGrainWithGuidKey
{
    Task<IAsyncEnumerable<int>> GetAsyncEnumerable(int n);
}
