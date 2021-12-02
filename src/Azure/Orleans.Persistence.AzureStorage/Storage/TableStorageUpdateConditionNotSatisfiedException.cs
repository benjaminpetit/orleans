using System;
using System.Runtime.Serialization;
using Orleans.Runtime;

namespace Orleans.Storage
{
    /// <summary>
    /// Exception thrown when a storage provider detects an Etag inconsistency when attempting to perform a WriteStateAsync operation.
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    public class TableStorageUpdateConditionNotSatisfiedException : InconsistentStateException
    {
        private const string DefaultMessageFormat = "Table storage condition not Satisfied.  GrainType: {0}, GrainId: {1}, TableName: {2}, StoredETag: {3}, CurrentETag: {4}";

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        public TableStorageUpdateConditionNotSatisfiedException(
            string errorMsg,
            GrainId grainId,
            string tableName,
            string storedEtag,
            string currentEtag,
            Exception storageException)
            : base(errorMsg, storedEtag, currentEtag, storageException)
        {
            this.GrainId = grainId;
            this.TableName = tableName;
        }

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        public TableStorageUpdateConditionNotSatisfiedException(
            GrainId grainId,
            string tableName,
            string storedEtag,
            string currentEtag,
            Exception storageException)
            : this(CreateDefaultMessage(grainId, tableName, storedEtag, currentEtag), grainId, tableName, storedEtag, currentEtag, storageException)
        {
        }

        /// <summary>
        /// Id of grain
        /// </summary>
        [Id(0)]
        public GrainId GrainId { get; }

        /// <summary>
        /// Azure table name
        /// </summary>
        [Id(1)]
        public string TableName { get; }

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        public TableStorageUpdateConditionNotSatisfiedException()
        {
        }

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        public TableStorageUpdateConditionNotSatisfiedException(string msg)
            : base(msg)
        {
        }

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        public TableStorageUpdateConditionNotSatisfiedException(string msg, Exception exc)
            : base(msg, exc)
        {
        }

        private static string CreateDefaultMessage(
            GrainId grainId,
            string tableName,
            string storedEtag,
            string currentEtag)
        {
            return string.Format(DefaultMessageFormat, grainId, tableName, storedEtag, currentEtag);
        }

        /// <summary>
        /// Exception thrown when an azure table storage exception is thrown due to update conditions not being satisfied.
        /// </summary>
        protected TableStorageUpdateConditionNotSatisfiedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.GrainId = (GrainId) info.GetValue("GrainId", typeof(GrainId));
            this.TableName = info.GetString("TableName");
        }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null) throw new ArgumentNullException(nameof(info));

            info.AddValue("GrainId", this.GrainId);
            info.AddValue("TableName", this.TableName);
            base.GetObjectData(info, context);
        }
    }
}
