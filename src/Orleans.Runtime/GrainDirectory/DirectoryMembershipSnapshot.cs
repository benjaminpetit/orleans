using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Runtime.Utilities;

namespace Orleans.Runtime.GrainDirectory
{
    /// <summary>
    /// A snapshot of cluster membership from the perspective of the local grain directory.
    /// </summary>
    internal class DirectoryMembershipSnapshot
    {
        private readonly ILogger log;
        private readonly SiloAddress seed;

        public DirectoryMembershipSnapshot(
            ILogger log,
            SiloAddress myAddress,
            SiloAddress seed,
            bool isLocalDirectoryRunning,
            ImmutableArray<SiloAddress> ring,
            ImmutableHashSet<SiloAddress> members)
        {
            this.log = log ?? throw new ArgumentNullException(nameof(log));
            this.MyAddress = myAddress ?? throw new ArgumentNullException(nameof(myAddress));
            this.seed = seed;
            this.IsLocalDirectoryRunning = isLocalDirectoryRunning;
            this.Ring = ring;
            this.Members = members ?? ImmutableHashSet<SiloAddress>.Empty;
        }

        /// <summary>
        /// Cluster members sorted by the hash value of their address
        /// </summary>
        [Pure]
        public ImmutableArray<SiloAddress> Ring { get; }

        /// <summary>
        /// Cluster members.
        /// </summary>
        [Pure]
        public ImmutableHashSet<SiloAddress> Members { get; }

        /// <summary>
        /// Indicates whether or not the grain directory was running.
        /// </summary>
        [Pure]
        public bool IsLocalDirectoryRunning { get; }

        /// <summary>
        /// The address of the local silo.
        /// </summary>
        public SiloAddress MyAddress { get; }

        /// <summary>
        /// Returns an updated copy of this instance.
        /// </summary>
        [Pure]
        public DirectoryMembershipSnapshot WithUpdate(bool isLocalDirectoryRunning, ImmutableArray<SiloAddress> ring, ImmutableHashSet<SiloAddress> members)
            => new DirectoryMembershipSnapshot(this.log, this.MyAddress, this.seed, isLocalDirectoryRunning, ring, members);

        /// <summary>
        /// Returns the <see cref="SiloAddress"/> which owns the directory partition of the provided grain.
        /// </summary>
        [Pure]
        internal SiloAddress CalculateGrainDirectoryPartition(GrainId grainId)
        {
            // give a special treatment for special grains
            if (grainId.IsSystemTarget)
            {
                if (Constants.SystemMembershipTableId.Equals(grainId))
                {
                    if (this.seed == null)
                    {
                        var errorMsg =
                            $"MembershipTable cannot run without Seed node. Please check your silo configuration make sure it specifies a SeedNode element. " +
                            $"This is in either the configuration file or the {nameof(NetworkingOptions)} configuration. " +
                            " Alternatively, you may want to use reliable membership, such as Azure Table.";
                        throw new ArgumentException(errorMsg, "grainId = " + grainId);
                    }
                }

                if (log.IsEnabled(LogLevel.Trace)) log.Trace("Silo {0} looked for a system target {1}, returned {2}", this.MyAddress, grainId, this.MyAddress);
                // every silo owns its system targets
                return this.MyAddress;
            }

            SiloAddress siloAddress = null;
            int hash = unchecked((int)grainId.GetUniformHashCode());

            // excludeMySelf from being a TargetSilo if we're not running and the excludeThisSIloIfStopping flag is true. see the comment in the Stop method.
            // excludeThisSIloIfStopping flag was removed because we believe that flag complicates things unnecessarily. We can add it back if it turns out that flag 
            // is doing something valuable. 
            bool excludeMySelf = !this.IsLocalDirectoryRunning;

            if (Ring.Length == 0)
            {
                // If the membership ring is empty, then we're the owner by default unless we're stopping.
                return !this.IsLocalDirectoryRunning ? null : this.MyAddress;
            }

            // need to implement a binary search, but for now simply traverse the list of silos sorted by their hashes
            for (var index = this.Ring.Length - 1; index >= 0; --index)
            {
                var item = this.Ring[index];
                if (IsSiloNextInTheRing(item, hash, excludeMySelf))
                {
                    siloAddress = item;
                    break;
                }
            }

            if (siloAddress == null)
            {
                // If not found in the traversal, last silo will do (we are on a ring).
                // We checked above to make sure that the list isn't empty, so this should always be safe.
                siloAddress = this.Ring[this.Ring.Length - 1];
                // Make sure it's not us...
                if (siloAddress.Equals(this.MyAddress) && excludeMySelf)
                {
                    siloAddress = this.Ring.Length > 1 ? this.Ring[this.Ring.Length - 2] : null;
                }
            }
            if (log.IsEnabled(LogLevel.Trace)) log.Trace("Silo {0} calculated directory partition owner silo {1} for grain {2}: {3} --> {4}", this.MyAddress, siloAddress, grainId, hash, siloAddress?.GetConsistentHashCode());
            return siloAddress;
        }

        private bool IsSiloNextInTheRing(SiloAddress siloAddr, int hash, bool excludeMySelf)
        {
            return siloAddr.GetConsistentHashCode() <= hash && (!excludeMySelf || !siloAddr.Equals(this.MyAddress));
        }

        [Pure]
        public List<SiloAddress> FindPredecessors(SiloAddress silo, int count)
        {
            int index = this.Ring.FindIndex(elem => elem.Equals(silo));
            if (index == -1)
            {
                log.Warn(ErrorCode.Runtime_Error_100201, "Got request to find predecessors of silo " + silo + ", which is not in the list of members");
                return null;
            }

            var result = new List<SiloAddress>();
            int numMembers = this.Ring.Length;
            for (int i = index - 1; ((i + numMembers) % numMembers) != index && result.Count < count; i--)
            {
                result.Add(this.Ring[(i + numMembers) % numMembers]);
            }

            return result;
        }

        [Pure]
        public List<SiloAddress> FindSuccessors(SiloAddress silo, int count)
        {
            int index = this.Ring.FindIndex(elem => elem.Equals(silo));
            if (index == -1)
            {
                log.Warn(ErrorCode.Runtime_Error_100203, "Got request to find successors of silo " + silo + ", which is not in the list of members");
                return null;
            }

            var result = new List<SiloAddress>();
            int numMembers = this.Ring.Length;
            for (int i = index + 1; i % numMembers != index && result.Count < count; i++)
            {
                result.Add(this.Ring[i % numMembers]);
            }

            return result;
        }
    }
}
