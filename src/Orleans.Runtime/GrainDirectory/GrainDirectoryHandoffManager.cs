using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime.GrainDirectory
{
    /// <summary>
    /// Most methods of this class are synchronized since they might be called both
    /// from LocalGrainDirectory on CacheValidator.SchedulingContext and from RemoteGrainDirectory.
    /// </summary>
    internal class GrainDirectoryHandoffManager
    {
        private const int HANDOFF_CHUNK_SIZE = 500;
        private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(250);
        private const int MAX_OPERATION_DEQUEUE = 2;
        private readonly LocalGrainDirectory localGrainDirectory;
        private readonly ActivationDirectory activations;
        private readonly ISiloStatusOracle siloStatusOracle;
        private readonly IInternalGrainFactory grainFactory;
        private readonly Dictionary<SiloAddress, GrainDirectoryPartition> directoryPartitionsMap;
        private readonly List<SiloAddress> silosHoldingMyPartition;
        private readonly Dictionary<SiloAddress, Task> lastPromise;
        private readonly ILogger logger;
        private readonly Factory<GrainDirectoryPartition> createPartion;
        private readonly Queue<(string name, Func<Task> action)> pendingOperations = new Queue<(string name, Func<Task> action)>();
        private readonly AsyncLock executorLock = new AsyncLock();

        internal GrainDirectoryHandoffManager(
            LocalGrainDirectory localGrainDirectory,
            ActivationDirectory activations,
            ISiloStatusOracle siloStatusOracle,
            IInternalGrainFactory grainFactory,
            Factory<GrainDirectoryPartition> createPartion,
            ILoggerFactory loggerFactory)
        {
            logger = loggerFactory.CreateLogger<GrainDirectoryHandoffManager>();
            this.localGrainDirectory = localGrainDirectory;
            this.activations = activations;
            this.siloStatusOracle = siloStatusOracle;
            this.grainFactory = grainFactory;
            this.createPartion = createPartion;
            directoryPartitionsMap = new Dictionary<SiloAddress, GrainDirectoryPartition>();
            silosHoldingMyPartition = new List<SiloAddress>();
            lastPromise = new Dictionary<SiloAddress, Task>();
        }

        internal void ProcessSiloRemoveEvent(DirectoryMembershipSnapshot membershipSnapshot, SiloAddress removedSilo)
        {
            PurgeOldActivationEntries(membershipSnapshot);
            ReregisterAllActivations(membershipSnapshot);
        }

        internal void ProcessSiloAddEvent(DirectoryMembershipSnapshot membershipSnapshot, SiloAddress addedSilo)
        {
            PurgeOldActivationEntries(membershipSnapshot);
            ReregisterAllActivations(membershipSnapshot);
        }

        internal Task ProcessSiloStoppingEvent(DirectoryMembershipSnapshot membershipSnapshot)
        {
            // TODO REMOVE
            return Task.CompletedTask;
        }

        private void PurgeOldActivationEntries(DirectoryMembershipSnapshot membershipSnapshot)
        {
                this.localGrainDirectory.DirectoryPartition.Clear();
        }

        private void ReregisterAllActivations(DirectoryMembershipSnapshot membershipSnapshot)
        {
            EnqueueOperation(
                nameof(ReregisterAllActivations),
                () => ReregisterAllActivationsAsync(membershipSnapshot));
        }

        private async Task ReregisterAllActivationsAsync(DirectoryMembershipSnapshot membershipSnapshot)
        {
            var batches = new Dictionary<SiloAddress, List<ActivationAddress>>();

            lock (this.activations)
            {
                foreach (var activation in activations.Select(a => a.Value))
                {
                    // Do nothing if this activation is not using the directory
                    if (!activation.IsUsingGrainDirectory)
                        continue;

                    // No need to register again if we are the primary
                    var primary = membershipSnapshot.CalculateGrainDirectoryPartition(activation.Grain);
                    if (primary == null)
                        continue;

                    // Group in batches
                    if (!batches.TryGetValue(primary, out var list))
                        batches[primary] = list = new List<ActivationAddress>();
                    list.Add(activation.Address);
                }
            }

            foreach (var batch in batches)
            {
                await ReregisterActivations(membershipSnapshot, batch.Key, batch.Value, new List<ActivationAddress>());
            }
        }

        private async Task ReregisterActivations(DirectoryMembershipSnapshot membershipSnapshot, SiloAddress targetSilo, List<ActivationAddress> singleActivations, List<ActivationAddress> multiActivations)
        {
            if (!membershipSnapshot.IsLocalDirectoryRunning) return;

            if (this.siloStatusOracle.GetApproximateSiloStatus(targetSilo) == SiloStatus.Active)
            {
                if (singleActivations.Count > 0)
                {
                    if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Sending " + singleActivations.Count + " single activation entries to " + targetSilo);
                }

                if (multiActivations.Count > 0)
                {
                    if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Sending " + singleActivations.Count + " entries to " + targetSilo);
                }

                await localGrainDirectory.GetDirectoryReference(targetSilo).AcceptExistingRegistrations(singleActivations, multiActivations);
            }
            else
            {
                if (logger.IsEnabled(LogLevel.Warning)) logger.LogWarning("Silo " + targetSilo + " is no longer active and therefore cannot receive these activations");
                return;
            }
        }

        internal void AcceptExistingRegistrations(List<ActivationAddress> singleActivations, List<ActivationAddress> multiActivations)
        {
            this.EnqueueOperation(
                nameof(AcceptExistingRegistrations),
                () => AcceptExistingRegistrationsAsync(singleActivations, multiActivations));
        }

        private async Task AcceptExistingRegistrationsAsync(List<ActivationAddress> singleActivations, List<ActivationAddress> multiActivations)
        {
            if (!this.localGrainDirectory.Running) return;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogDebug(
                    $"{nameof(AcceptExistingRegistrations)}: accepting {singleActivations?.Count ?? 0} single-activation registrations and {multiActivations?.Count ?? 0} multi-activation registrations.");
            }

            if (singleActivations != null && singleActivations.Count > 0)
            {
                var tasks = singleActivations.Select(addr => this.localGrainDirectory.RegisterAsync(addr, true, 1)).ToArray();
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception exception)
                {
                    if (this.logger.IsEnabled(LogLevel.Warning))
                        this.logger.LogWarning($"Exception registering activations in {nameof(AcceptExistingRegistrations)}: {LogFormatter.PrintException(exception)}");
                    throw;
                }
                finally
                {
                    Dictionary<SiloAddress, List<ActivationAddress>> duplicates = new Dictionary<SiloAddress, List<ActivationAddress>>();
                    for (var i = tasks.Length - 1; i >= 0; i--)
                    {
                        // Retry failed tasks next time.
                        if (tasks[i].Status != TaskStatus.RanToCompletion) continue;

                        // Record the applications which lost the registration race (duplicate activations).
                        var winner = await tasks[i];
                        if (!winner.Address.Equals(singleActivations[i]))
                        {
                            var duplicate = singleActivations[i];

                            if (!duplicates.TryGetValue(duplicate.Silo, out var activations))
                            {
                                activations = duplicates[duplicate.Silo] = new List<ActivationAddress>(1);
                            }

                            activations.Add(duplicate);
                        }

                        // Remove tasks which completed.
                        singleActivations.RemoveAt(i);
                    }

                    // Destroy any duplicate activations.
                    DestroyDuplicateActivations(duplicates);
                }
            }

            // Multi-activation grains are much simpler because there is no need for duplicate activation logic.
            if (multiActivations != null && multiActivations.Count > 0)
            {
                var tasks = multiActivations.Select(addr => this.localGrainDirectory.RegisterAsync(addr, false, 1)).ToArray();
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (Exception exception)
                {
                    if (this.logger.IsEnabled(LogLevel.Warning))
                        this.logger.LogWarning($"Exception registering activations in {nameof(AcceptExistingRegistrations)}: {LogFormatter.PrintException(exception)}");
                    throw;
                }
                finally
                {
                    for (var i = tasks.Length - 1; i >= 0; i--)
                    {
                        // Retry failed tasks next time.
                        if (tasks[i].Status != TaskStatus.RanToCompletion) continue;

                        // Remove tasks which completed.
                        multiActivations.RemoveAt(i);
                    }
                }
            }
        }

        internal void AcceptHandoffPartition(SiloAddress source, Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy)
        {
            // TODO REMOVE
        }

        internal void RemoveHandoffPartition(SiloAddress source)
        {
            // TODO REMOVE

        }

        private void DestroyDuplicateActivations(Dictionary<SiloAddress, List<ActivationAddress>> duplicates)
        {
            if (duplicates == null || duplicates.Count == 0) return;
            this.EnqueueOperation(
                nameof(DestroyDuplicateActivations),
                () => DestroyDuplicateActivationsAsync(duplicates));
        }

        private async Task DestroyDuplicateActivationsAsync(Dictionary<SiloAddress, List<ActivationAddress>> duplicates)
        {
            while (duplicates.Count > 0)
            {
                var pair = duplicates.FirstOrDefault();
                if (this.siloStatusOracle.GetApproximateSiloStatus(pair.Key) == SiloStatus.Active)
                {
                    if (this.logger.IsEnabled(LogLevel.Debug))
                    {
                        this.logger.LogDebug(
                            $"{nameof(DestroyDuplicateActivations)} will destroy {duplicates.Count} duplicate activations on silo {pair.Key}: {string.Join("\n * ", pair.Value.Select(_ => _))}");
                    }

                    var remoteCatalog = this.grainFactory.GetSystemTarget<ICatalog>(Constants.CatalogId, pair.Key);
                    await remoteCatalog.DeleteActivations(pair.Value);
                }

                duplicates.Remove(pair.Key);
            }
        }

        private void EnqueueOperation(string name, Func<Task> action)
        {
            lock (this)
            {
                this.pendingOperations.Enqueue((name, action));
                if (this.pendingOperations.Count <= 2)
                {
                    this.localGrainDirectory.Scheduler.QueueTask(this.ExecutePendingOperations, this.localGrainDirectory.RemoteGrainDirectory.SchedulingContext);
                }
            }
        }

        private async Task ExecutePendingOperations()
        {
            using (await executorLock.LockAsync())
            {
                var dequeueCount = 0;
                while (true)
                {
                    // Get the next operation, or exit if there are none.
                    (string Name, Func<Task> Action) op;
                    lock (this)
                    {
                        if (this.pendingOperations.Count == 0) break;

                        op = this.pendingOperations.Peek();
                    }

                    dequeueCount++;

                    try
                    {
                        await op.Action();
                        // Success, reset the dequeue count
                        dequeueCount = 0;
                    }
                    catch (Exception exception)
                    {
                        if (dequeueCount < MAX_OPERATION_DEQUEUE)
                        {
                            if (this.logger.IsEnabled(LogLevel.Warning))
                                this.logger.LogWarning($"{op.Name} failed, will be retried: {LogFormatter.PrintException(exception)}.");
                            await Task.Delay(RetryDelay);
                        }
                        else
                        {
                            if (this.logger.IsEnabled(LogLevel.Warning))
                                this.logger.LogWarning($"{op.Name} failed, will NOT be retried: {LogFormatter.PrintException(exception)}");
                        }
                    }
                    if (dequeueCount == 0 || dequeueCount >= MAX_OPERATION_DEQUEUE)
                    {
                        lock (this)
                        {
                            // Remove the operation from the queue if it was a success
                            // or if we tried too many times
                            this.pendingOperations.Dequeue();
                        }
                    }
                }
            }
        }
    }
}
