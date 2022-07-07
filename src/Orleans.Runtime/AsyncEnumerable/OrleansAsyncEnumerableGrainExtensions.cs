using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Async;

internal interface IOrleansAsyncEnumerableGrainExtension<T> : IGrainExtension
{
    ValueTask<T> GetItem();
}

internal class OrleansAsyncEnumerableGrainExtension<T> : IOrleansAsyncEnumerableGrainExtension<T>
{
    private readonly OrleansAsyncEnumerable<T> _values;

    public OrleansAsyncEnumerableGrainExtension(OrleansAsyncEnumerable<T> values)
    {
        _values = values;
    }

    public ValueTask<T> GetItem() => _values.GetItem();
}