using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestExtensions;
using UnitTests.GrainInterfaces;
using Xunit;

namespace Tester;

[TestCategory("Functional")]
public class AsyncEnumerableTests : TestClusterPerTest
{
    [Fact]
    public async Task SimpleTest()
    {
        var grain = this.Client.GetGrain<IOrleansAsyncEnumerableGrain>(Guid.NewGuid());
        var enumerable = await grain.GetAsyncEnumerable(10);
        var results = new List<int>();
        await foreach (var item in enumerable)
        {
            results.Add(item);
            if (results.Count == 10) break;
        }
        Assert.Equal(10, results.Count);
    }
}
