using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Async;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains;

public class OrleansAsyncEnumerableGrain : Grain, IOrleansAsyncEnumerableGrain
{
    private int _count;
    private IDisposable _timer;
    private OrleansAsyncEnumerable<int> _asyncEnumerable;

    public Task<IAsyncEnumerable<int>> GetAsyncEnumerable(int n)
    {
        _asyncEnumerable = new OrleansAsyncEnumerable<int>();
        _count = n;
        _timer = this.RegisterTimer(PublishCallback, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        return Task.FromResult(_asyncEnumerable.GetValues());
    }

    private Task PublishCallback(object _)
    {
        _asyncEnumerable.Publish(_count);
        _count--;
        if (_count == 0)
        {
            _timer.Dispose();
        }
        return Task.CompletedTask;
    }
}
