using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    internal static class ActivationAddressHelper
    {
        public static AddressesAndTag ToAddressesAndTag(this DirectoryEntry entry)
        {
            return new AddressesAndTag
            {
                Addresses = new List<ActivationAddress> { entry.ToActivationAddress() },
                VersionTag = int.TryParse(entry.ETag, out var tag) ? tag : 0
            };
        }

        public static AddressAndTag ToAddressAndTag(this DirectoryEntry entry)
        {
            return new AddressAndTag
            {
                Address = entry.ToActivationAddress(),
                VersionTag = int.TryParse(entry.ETag, out var tag) ? tag : 0
            };
        }

        public static ActivationAddress ToActivationAddress(this DirectoryEntry entry)
        {
            var split = entry.ActivationAddress.Split(',');

            // Simple checks
            if (split.Length != 3)
                throw new ArgumentException($"Unable to parse this activation address (wrong format): \"{entry.ActivationAddress}\"");
            if (!int.TryParse(split[0], out var version))
                throw new ArgumentException($"Unable to parse this activation address (unknown version): \"{entry.ActivationAddress}\"");

            try
            {
                var grainId = GrainId.FromParsableString(entry.GrainId);
                var activationId = ActivationId.GetActivationId(UniqueKey.Parse(split[1].AsSpan()));
                var silo = SiloAddress.FromParsableString(split[2]);

                return ActivationAddress.GetAddress(silo, grainId, activationId);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Unable to parse this activation address (see inner exception): \"{entry.ActivationAddress}\"", ex);
            }

        }

        public static DirectoryEntry ToEntry(this ActivationAddress address)
        {
            return new DirectoryEntry(
                address.Grain.ToParsableString(),
                $"1,{address.Activation.Key.ToHexString()},{address.Silo.ToParsableString()}",
                string.Empty);
        }

        public static List<DirectoryEntry> ToEntries(this List<ActivationAddress> addresses) => addresses.Select(a => a.ToEntry()).ToList();
    }
}
