using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace AppCenter.ExportParser
{
    public class ExportForwarder : ExportReader
    {
        private readonly CloudBlobContainer OutputBlobContainer;

        public ExportForwarder(string inputConnectionString, string outputConnectionString, string outputContainerName) : base(inputConnectionString)
        {
            OutputBlobContainer = GetBlobContainer(outputConnectionString, outputContainerName, true);
        }

        /// <summary>
        /// Returns a list of all log in the time period.
        /// </summary>
        /// <returns></returns>
        public void RetrieveAndForwardLogs(DateTimeOffset fromTime, DateTimeOffset toTime, Func<DeviceLog, bool> filter)
        {
            if (toTime < fromTime)
            {
                throw new ArgumentException("toDay should be after fromDate", nameof(toTime));
            }

            var minutesToExport = (int)(toTime - fromTime).TotalMinutes;

            Parallel.For(0, minutesToExport, ParallelOptions, i =>
            {
                var dayToExport = fromTime.AddMinutes(i);
                var blobsForMinute = GetBlobsForMinuteAsync(dayToExport).GetAwaiter().GetResult();
                foreach (var blob in blobsForMinute)
                {
                    var jsonLogs = blob.DownloadTextAsync().GetAwaiter().GetResult();
                    var logsToExport = JsonConvert.DeserializeObject<DeviceLog[]>(jsonLogs).Where(filter);
                    if (logsToExport.Any())
                    {
                        ForwardLogs(blob.Name, logsToExport);
                    }
                }                
            });
        }

        /// <summary>
        /// Forwards specified logs to output container.
        /// </summary>
        /// <param name="blobName"></param>
        /// <param name="logsToExport"></param>
        public void ForwardLogs(string blobName, IEnumerable<DeviceLog> logsToExport)
        {
            var outputBlob = OutputBlobContainer.GetBlockBlobReference(blobName);
            outputBlob.UploadTextAsync(JsonConvert.SerializeObject(logsToExport)).GetAwaiter().GetResult();
            foreach (var log in logsToExport)
            {
                ForwardLogAttachments(log);
            }
        }

        private void ForwardLogAttachments(DeviceLog logSchema)
        {
            // Export attachments
            if (logSchema.MessageType == MessageTypes.ErrorLog 
                || logSchema.MessageType == MessageTypes.ErrorAttachmentLog 
                || logSchema.MessageType == MessageTypes.HandledErrorLog)
            {
                var sharedAccessBlobPolicy = new SharedAccessBlobPolicy
                {
                    Permissions = SharedAccessBlobPermissions.Read,
                    SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(1),
                    SharedAccessStartTime = DateTimeOffset.UtcNow.AddHours(-1),
                };

                var attachmentBlobName = logSchema.Properties;
                var attachmentBlobSas = InputBlobContainer.GetSharedAccessSignature(sharedAccessBlobPolicy);
                var outputBlobReference = OutputBlobContainer.GetBlockBlobReference(attachmentBlobName);
                outputBlobReference.StartCopyAsync(new Uri(attachmentBlobSas), 
                    AccessCondition.GenerateIfExistsCondition(), 
                    AccessCondition.GenerateEmptyCondition(), 
                    RequestOptions, 
                    new OperationContext()).GetAwaiter().GetResult();
            }
        }
    }
}
