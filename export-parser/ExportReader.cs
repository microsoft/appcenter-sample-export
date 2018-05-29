using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace AppCenter.ExportParser
{
    public class ExportReader
    {
        public const string InputContainerName = "archive";
        protected readonly CloudBlobContainer InputBlobContainer;

        public ExportReader(string inputConnectionString)
        {
            InputBlobContainer = GetBlobContainer(inputConnectionString, InputContainerName);
            RequestOptions = new BlobRequestOptions();
            ParallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1
            };
        }

        /// <summary>
        /// Gets or sets parallel execution options
        /// </summary>
        public ParallelOptions ParallelOptions { get; set; }

        /// <summary>
        /// Gets or sets blob request options
        /// </summary>
        public BlobRequestOptions RequestOptions { get; set; }

        /// <summary>
        /// Returns a list of all log in the time period.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DeviceLog> GetLogsAsync(DateTimeOffset fromTime, DateTimeOffset toTime)
        {
            if (toTime < fromTime)
            {
                throw new ArgumentException("toDay should be after fromDate", nameof(toTime));
            }

            var results = new List<DeviceLog>();
            var minutesToExport = (int)(toTime - fromTime).TotalMinutes;

            var result = new ConcurrentQueue<DeviceLog>();
            Parallel.For(0, minutesToExport, ParallelOptions, i =>
            {
                var logsForMinute = new List<DeviceLog>();
                var dayToExport = fromTime.AddMinutes(i);
                foreach (var log in GetLogsForMinuteAsync(dayToExport).GetAwaiter().GetResult())
                {
                    result.Enqueue(log);
                }
            });

            return result;
        }

        /// <summary>
        /// Returns a list of logs for a minute.
        /// </summary>
        /// <returns></returns>
        protected async Task<IEnumerable<DeviceLog>> GetLogsForMinuteAsync(DateTimeOffset dayToExport)
        {
            var results = new List<DeviceLog>();
            var blobsForMinute = GetBlobsForMinuteAsync(dayToExport).GetAwaiter().GetResult();
            foreach (var blob in blobsForMinute)
            {
                var jsonLogs = await blob.DownloadTextAsync();
                results.AddRange(JsonConvert.DeserializeObject<DeviceLog[]>(jsonLogs));
            }

            return results;
        }

        /// <summary>
        /// Returns a list of blobs for a minute.
        /// </summary>
        /// <param name="blobTime">Date to use as a blob prefix.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected async Task<IEnumerable<CloudBlockBlob>> GetBlobsForMinuteAsync(DateTimeOffset blobTime)
        {
            var prefix = blobTime.ToString("yyyy/MM/dd/HH/mm") + "/logs";
            var resultItems = new List<CloudBlockBlob>();
            try
            {
                BlobContinuationToken continuationToken = null;
                do
                {
                    var segment = await InputBlobContainer.ListBlobsSegmentedAsync(prefix, true, BlobListingDetails.None, 100, continuationToken, RequestOptions, new OperationContext());
                    resultItems.AddRange(segment.Results.Where(b => b.Uri.AbsolutePath.EndsWith("v1.data")).OfType<CloudBlockBlob>());
                    continuationToken = segment.ContinuationToken;
                } while (continuationToken != null);
            }
            catch (Exception)
            {
                // TODO: Log
                throw;
            }
            return resultItems;
        }

        public static CloudBlobContainer GetBlobContainer(string connectionString, string containerName, bool createIfNotExists = false)
        {
            CloudBlobClient blobClient = null;
            if (connectionString.Contains("?"))
            {
                // Try to parse SAS token
                try
                {
                    blobClient = new CloudBlobClient(new Uri(connectionString));
                }
                catch { }
            }

            if (blobClient == null)
            {
                var storageAccount = CloudStorageAccount.Parse(connectionString);
                blobClient = storageAccount.CreateCloudBlobClient();
            }

            var container = blobClient.GetContainerReference(containerName);
            if (createIfNotExists)
            {
                container.CreateIfNotExistsAsync().GetAwaiter().GetResult();
            }
            return container;
        }
    }
}
