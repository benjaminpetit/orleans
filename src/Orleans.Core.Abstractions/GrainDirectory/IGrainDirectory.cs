using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    public interface IGrainDirectory
    {
        Task<OperationResult> Register(DirectoryEntry entry);

        Task<List<OperationResult>> Register(List<DirectoryEntry> entries);

        Task<OperationResult> Unregister(DirectoryEntry entry);

        Task<List<OperationResult>> Unregister(List<DirectoryEntry> entries);

        Task<LookupResult> Lookup(string grainId);

        Task<List<LookupResult>> Lookup(List<string> grainId);
    }

    public struct DirectoryEntry
    {
        public string GrainId { get; }

        public string ActivationAddress { get; }

        public string ETag { get; }

        public DirectoryEntry(string grainId, string activationAddress, string eTag)
        {
            this.GrainId = grainId;
            this.ActivationAddress = activationAddress;
            this.ETag = eTag;
        }

        public override bool Equals(object obj)
        {
            return obj is DirectoryEntry entry &&
                   this.GrainId == entry.GrainId &&
                   this.ActivationAddress == entry.ActivationAddress &&
                   this.ETag == entry.ETag;
        }

        public override int GetHashCode() => this.GrainId.GetHashCode();

        public static bool operator == (DirectoryEntry entry1, DirectoryEntry entry2) => entry1.Equals(entry2);

        public static bool operator != (DirectoryEntry entry1, DirectoryEntry entry2) => !entry1.Equals(entry2);


    }

    public abstract class OperationResult
    {
        public string GrainId => this.DirectoryEntry.GrainId;

        public DirectoryEntry DirectoryEntry { get; }

        protected OperationResult(DirectoryEntry directoryEntry)
        {
            this.DirectoryEntry = directoryEntry;
        }
    }

    public sealed class SuccessResult : OperationResult
    {
        public SuccessResult(DirectoryEntry directoryEntry)
            : base(directoryEntry)
        {
        }
    }

    public sealed class FailureResult : OperationResult
    {
        public Exception Exception { get; }

        public FailureResult(DirectoryEntry directoryEntry, Exception exception)
            : base(directoryEntry)
        {
            this.Exception = exception;
        }
    }

    public sealed class RedirectResult : OperationResult
    {
        public DirectoryEntry ActualDirectoryEntry { get; }

        public RedirectResult(DirectoryEntry directoryEntry, DirectoryEntry actualDirectoryEntry)
             : base(directoryEntry)
        {
            this.ActualDirectoryEntry = actualDirectoryEntry;
        }
    }

    public sealed class LookupResult
    {
        public string GrainId { get; }

        public DirectoryEntry DirectoryEntry { get; }

        public bool IsFound => this.DirectoryEntry != default;

        public LookupResult(string grainId, DirectoryEntry directoryEntry = default)
        {
            this.GrainId = grainId;
            this.DirectoryEntry = directoryEntry;
        }
    }
}
