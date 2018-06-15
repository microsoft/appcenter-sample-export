using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AppCenter.Samples
{
    /// <summary>
    /// Class ExportOptions.
    /// </summary>
    public class ExportOptions
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ExportOptions"/> class.
        /// </summary>
        public ExportOptions()
        {
            AccessCondition = DefaultAccessCondition;
            BlobQueueLength = 10;
            BlobRequestOptions = DefaultBlobRequestOptions;
            DegreeOfParallelism = Environment.ProcessorCount;
            EventHubBufferCount = 50;
            EventHubBufferTimeSpan = TimeSpan.FromSeconds(1);
            MinLatency = TimeSpan.FromMinutes(20);
            OperationContext = DefaultOperationContext;
            PartitionQueueLength = 100;
        }

        /// <summary>
        /// Gets the default <see cref="ExportOptions"/>.
        /// </summary>
        /// <value>The default <see cref="ExportOptions"/>.</value>
        public static ExportOptions Default { get; } = new ExportOptions();

        /// <summary>
        /// Gets or sets the access condition.
        /// </summary>
        /// <value>The access condition.</value>
        public AccessCondition AccessCondition { get; set; }

        /// <summary>
        /// Gets or sets the BLOB request options.
        /// </summary>
        /// <value>The BLOB request options.</value>
        public BlobRequestOptions BlobRequestOptions { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of log batches to enqueue for writing to BLOB storage.
        /// </summary>
        /// <value>The length of the BLOB queue.</value>
        public int BlobQueueLength { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of writers to Azure BLOB storage.
        /// </summary>
        /// <value>The degree of parallelism.</value>
        public int DegreeOfParallelism { get; set; }

        /// <summary>
        /// Gets or sets the maximum size of a batch of logs to send to an Event Hub partition.
        /// </summary>
        /// <value>The event hub buffer count.</value>
        public int EventHubBufferCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum time to buffer logs before sending a batch to an Event Hub partition.
        /// </summary>
        /// <value>The event hub buffer time span.</value>
        public TimeSpan EventHubBufferTimeSpan { get; set; }

        /// <summary>
        /// Gets or sets how far behind the current time the exported BLOBs are read.
        /// </summary>
        /// <value>How far behind the current time the exported BLOBs are read.</value>
        public TimeSpan MinLatency { get; set; }

        /// <summary>
        /// Gets or sets the operation context.
        /// </summary>
        /// <value>The operation context.</value>
        public OperationContext OperationContext { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of log batches to enqueue for sending to an Event Hub partition.
        /// </summary>
        /// <value>The length of the partition queue.</value>
        public int PartitionQueueLength { get; set; }

        /// <summary>
        /// Gets the default access condition.
        /// </summary>
        /// <value>The default access condition.</value>
        private static AccessCondition DefaultAccessCondition { get; } = new AccessCondition();

        /// <summary>
        /// Gets the default BLOB request options.
        /// </summary>
        /// <value>The default BLOB request options.</value>
        private static BlobRequestOptions DefaultBlobRequestOptions { get; } = new BlobRequestOptions();

        /// <summary>
        /// Gets the default operation context.
        /// </summary>
        /// <value>The default operation context.</value>
        private static OperationContext DefaultOperationContext { get; } = new OperationContext();
    }
}
