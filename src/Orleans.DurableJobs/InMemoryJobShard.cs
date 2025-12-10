using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Orleans.DurableJobs;

[DebuggerDisplay("ShardId={Id}, StartTime={StartTime}, EndTime={EndTime}")]
internal sealed class InMemoryJobShard : JobShard
{
    public InMemoryJobShard(string shardId, DateTimeOffset minDueTime, DateTimeOffset maxDueTime, IDictionary<string, string>? metadata)
        : base(shardId, minDueTime, maxDueTime, storage: null)
    {
        Metadata = metadata;
    }
}
