using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Async;

public class OrleansAsyncEnumerable<T>
{
    private readonly ConcurrentQueue<T> _items = new();
    private readonly SingleWaiterAutoResetEvent _event = new();
    private readonly GrainReference _grainReference;

    public OrleansAsyncEnumerable()
    {
        var ctx = RuntimeContext.Current;
        if (ctx == null)
        {
            throw new ArgumentNullException("RuntimeContext.Current is null");
        }
        _grainReference = ctx.GrainReference;
        var extension = new OrleansAsyncEnumerableGrainExtension<T>(this);
        ctx.SetComponent<IOrleansAsyncEnumerableGrainExtension<T>>(extension);
    }

    public void Publish(T item)
    {
        _items.Enqueue(item);
        _event.Signal();
    }

    public IAsyncEnumerable<T> GetValues() => new GrainReferenceAsyncEnumerable<T>(_grainReference.AsReference<IOrleansAsyncEnumerableGrainExtension<T>>());

    internal async ValueTask<T> GetItem()
    {
        while (true)
        {
            if (_items.TryDequeue(out var item))
            {
                return item;
            }
            await _event.WaitAsync();
        }
    }
}

[GenerateSerializer]
internal class GrainReferenceAsyncEnumerable<T> : IAsyncEnumerable<T>
{
    [Id(0)]
    private IOrleansAsyncEnumerableGrainExtension<T> _grainReference;

    public GrainReferenceAsyncEnumerable(IOrleansAsyncEnumerableGrainExtension<T> grainReference)
    {
        _grainReference = grainReference;
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => new Enumerator(_grainReference);

    private class Enumerator : IAsyncEnumerator<T>
    {
        private IOrleansAsyncEnumerableGrainExtension<T> _grainReference;

        public Enumerator(IOrleansAsyncEnumerableGrainExtension<T> grainReference)
        {
            _grainReference = grainReference;
        }

        public T Current {get; private set;}

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public async ValueTask<bool> MoveNextAsync()
        {
            Current = await _grainReference.GetItem();
            return true;
        }
    }
}