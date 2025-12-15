using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.DurableJobs.Storage;
using Orleans.Runtime;

namespace Orleans.DurableJobs;

/// <summary>
/// Manages the lifecycle of job shards for a specific silo.
/// Each silo instance has its own shard manager.
/// </summary>
public abstract class JobShardManager
{
    /// <summary>
    /// Gets the silo address this manager is associated with.
    /// </summary>
    protected SiloAddress SiloAddress { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="JobShardManager"/> class.
    /// </summary>
    /// <param name="siloAddress">The silo address this manager represents.</param>
    protected JobShardManager(SiloAddress siloAddress)
    {
        SiloAddress = siloAddress;
    }

    /// <summary>
    /// Assigns orphaned job shard storage to this silo.
    /// </summary>
    /// <param name="maxDueTime">Maximum due time for shards to consider.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of job shard storage instances assigned to this silo.</returns>
    public abstract Task<List<IJobShardStorage>> AssignJobShardsAsync(DateTimeOffset maxDueTime, CancellationToken cancellationToken);

    /// <summary>
    /// Creates a new job shard storage owned by this silo.
    /// </summary>
    /// <param name="minDueTime">The minimum due time for jobs in this shard.</param>
    /// <param name="maxDueTime">The maximum due time for jobs in this shard.</param>
    /// <param name="metadata">Optional metadata for the shard.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The newly created job shard storage.</returns>
    public abstract Task<IJobShardStorage> CreateShardAsync(DateTimeOffset minDueTime, DateTimeOffset maxDueTime, IDictionary<string, string> metadata, CancellationToken cancellationToken);

    /// <summary>
    /// Unregisters a shard storage owned by this silo.
    /// </summary>
    /// <param name="storage">The storage instance for the shard to unregister.</param>
    /// <param name="shouldDelete">
    /// If true, the shard is deleted completely (recommended when no jobs remain).
    /// If false, ownership is released so another silo can claim the shard (recommended when jobs remain).
    /// The caller is responsible for determining whether the shard has remaining jobs.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public abstract Task UnregisterShardAsync(IJobShardStorage storage, bool shouldDelete, CancellationToken cancellationToken);
}

internal class InMemoryJobShardManager : JobShardManager
{
    // Shared storage across all manager instances to support multi-silo scenarios
    private static readonly Dictionary<string, ShardOwnership> _globalShardStore = new();
    private static readonly SemaphoreSlim _asyncLock = new(1, 1);
    private readonly IClusterMembershipService? _membershipService;

    public InMemoryJobShardManager(SiloAddress siloAddress) : base(siloAddress)
    {
    }

    public InMemoryJobShardManager(SiloAddress siloAddress, IClusterMembershipService membershipService) : base(siloAddress)
    {
        _membershipService = membershipService;
    }

    /// <summary>
    /// Clears all shards from the global store. For testing purposes only.
    /// </summary>
    internal static async Task ClearAllShardsAsync()
    {
        await _asyncLock.WaitAsync();
        try
        {
            _globalShardStore.Clear();
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    public override async Task<List<IJobShardStorage>> AssignJobShardsAsync(DateTimeOffset maxDueTime, CancellationToken cancellationToken)
    {
        var alreadyOwnedShards = new List<IJobShardStorage>();
        var stolenShards = new List<IJobShardStorage>();
        
        await _asyncLock.WaitAsync(cancellationToken);
        try
        {
            var snapshot = _membershipService?.CurrentSnapshot;
            var deadSilos = new HashSet<string>();
            
            if (snapshot is not null)
            {
                foreach (var member in snapshot.Members.Values)
                {
                    if (member.Status == SiloStatus.Dead)
                    {
                        deadSilos.Add(member.SiloAddress.ToString());
                    }
                }
            }

            // Assign shards from dead silos or orphaned shards
            foreach (var kvp in _globalShardStore)
            {
                var shardId = kvp.Key;
                var ownership = kvp.Value;
                
                // Skip shards that are already owned by this silo
                if (ownership.OwnerSiloAddress == SiloAddress.ToString())
                {
                    if (ownership.Storage.StartTime <= maxDueTime)
                    {
                        alreadyOwnedShards.Add(ownership.Storage);
                    }
                }
                // Take over orphaned shards or shards from dead silos
                else if (ownership.OwnerSiloAddress is null || deadSilos.Contains(ownership.OwnerSiloAddress))
                {
                    if (ownership.Storage.StartTime <= maxDueTime)
                    {
                        ownership.OwnerSiloAddress = SiloAddress.ToString();
                        stolenShards.Add(ownership.Storage);
                    }
                }
            }
        }
        finally
        {
            _asyncLock.Release();
        }

        return [.. alreadyOwnedShards, .. stolenShards];
    }

    public override async Task<IJobShardStorage> CreateShardAsync(DateTimeOffset minDueTime, DateTimeOffset maxDueTime, IDictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        await _asyncLock.WaitAsync(cancellationToken);
        try
        {
            // Generate unique shard ID
            var shardId = $"{SiloAddress}-{Guid.NewGuid()}";
            
            // Create storage with metadata
            var storage = new InMemoryJobShardStorage(
                shardId,
                minDueTime,
                maxDueTime,
                metadata);
            
            // Track ownership
            _globalShardStore[shardId] = new ShardOwnership
            {
                Storage = storage,
                OwnerSiloAddress = SiloAddress.ToString()
            };
            
            return storage;
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    public override async Task UnregisterShardAsync(IJobShardStorage storage, bool shouldDelete, CancellationToken cancellationToken)
    {
        await _asyncLock.WaitAsync(cancellationToken);
        try
        {
            // Only remove shards that have no jobs remaining
            if (_globalShardStore.TryGetValue(storage.ShardId, out var ownership))
            {
                if (shouldDelete)
                {
                    _globalShardStore.Remove(storage.ShardId);
                }
                else
                {
                    // Mark as unowned so another silo can pick it up
                    ownership.OwnerSiloAddress = null;
                }
            }
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    private sealed class ShardOwnership
    {
        public required IJobShardStorage Storage { get; init; }
        public string? OwnerSiloAddress { get; set; }
    }
}
