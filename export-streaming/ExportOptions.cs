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
            BlobRequestOptions = DefaultBlobRequestOptions;
            OperationContext = DefaultOperationContext;
            MinLatency = TimeSpan.FromMinutes(20);
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
