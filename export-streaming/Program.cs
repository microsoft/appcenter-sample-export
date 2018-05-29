namespace AppCenter.Samples
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data.HashFunction.MurmurHash;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using AppCenter.ExportParser;
    using Microsoft.Azure.EventHubs;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;
    using Serilog;
    using static System.Console;

    internal static class Program
    {
        private const int BlobQueueLength = 10;
        private const string InputConnectionString = "<export storage account connection string>";
        private const string InputContainerName = "archive";
        private const string OutputConnectionString = "<output storage account connection string>";
        private const string OutputContainerName = "output-container";
        private const string OutputEventHubsConnectionString = "<output event hub connection string>";
        private const int PartitionQueueLength = 1000;

        private static int DegreeOfParallelism { get; } = Environment.ProcessorCount;

        private static TimeSpan StreamingTime { get; } = TimeSpan.FromMinutes(10);

        public static async Task Main()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            using (var cts = new CancellationTokenSource(StreamingTime))
            {
                var cancellationToken = cts.Token;
                var start = DateTimeOffset.UtcNow - TimeSpan.FromHours(1);
                var exportOptions = new ExportOptions();

                // Setup data source
                var inputContainer = await CreateInputContainerAsync(InputConnectionString, cts.Token).ConfigureAwait(false);

                // Create the core observable
                var observable = inputContainer.CreateExport(start, exportOptions).Publish();

                // Setup BLOB output
                var blobSink = await CreateBlobSink(OutputConnectionString, OutputContainerName, BlobQueueLength, exportOptions, cancellationToken);
                observable
                    .Subscribe(
                        onNext: _ => blobSink.Post(_), 
                        onError: e => Log.Error(e, "Azure BLOB target stream completed in error"),
                        onCompleted: () => Log.Information("Azure BLOB target stream completed"),
                        token: cancellationToken);

                // Setup Event Hubs output
                var eventHubSinks = await CreateEventHubsSink(OutputEventHubsConnectionString, PartitionQueueLength);
                observable
                    .SelectMany(_ => _.Logs)
                    .Subscribe(
                        onNext: deviceLog =>
                        {
                            var partitionId = GetPartition(deviceLog, eventHubSinks.Length);
                            eventHubSinks[partitionId].Post(deviceLog);
                        },
                        onError: e => Log.Error(e, "Event Hubs target stream completed in error"),
                        onCompleted: () => Log.Information("Event Hubs target stream completed"),
                        token: cancellationToken);

                using (observable.Connect())
                {
                    WriteLine("Press ENTER to terminate...");
                    ReadLine();
                    cts.Cancel();
                }
            }
        }

        #region Event Hubs features

        private static IMurmurHash3 HashFunc { get; } = MurmurHash3Factory.Instance.Create(new MurmurHash3Config { HashSizeInBits = 32 });

        private static async Task<ImmutableArray<ITargetBlock<DeviceLog>>> CreateEventHubsSink(string connectionString, int queueLength)
        {
            var client = EventHubClient.CreateFromConnectionString(connectionString);
            var information = await client.GetRuntimeInformationAsync().ConfigureAwait(false);
            var senderOptions = new ExecutionDataflowBlockOptions { BoundedCapacity = queueLength };
            return information.PartitionIds
                .Select(partitionId =>
                {
                    var sender = client.CreatePartitionSender(partitionId);

                    // A block that sends batches to the Event Hub partition
                    var sendBlock = new ActionBlock<DeviceLog>(async deviceLog =>
                        {
                            try
                            {
                                var json = JsonConvert.SerializeObject(deviceLog, Formatting.None);
                                var bytes = Encoding.UTF8.GetBytes(json);
                                using (var eventData = new EventData(bytes))
                                    await sender.SendAsync(eventData).ConfigureAwait(false);
                            }
                            catch (Exception exception)
                            {
                                // Swallow errors
                                Log.Error(exception, "Error sending {DeviceLog} to Event Hub partition {PartitionId}", deviceLog, partitionId);
                            }
                        },
                        senderOptions);

                    return (ITargetBlock<DeviceLog>) sendBlock;
                })
                .ToImmutableArray();
        }

        private static int GetMurmurHash(Guid guid)
        {
            var bytes = guid.ToByteArray();
            var hash = HashFunc.ComputeHash(bytes).Hash;
            return BitConverter.ToInt32(hash, 0);
        }

        private static int GetPartition(DeviceLog deviceLog, int partitionCount)
        {
            if (!Guid.TryParse(deviceLog.InstallId, out var installId))
                return 0;
            var hash = GetMurmurHash(installId);
            return hash % partitionCount;
        }

        #endregion Event Hubs features

        #region Azure BLOB features

        private static async Task<ActionBlock<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> logs)>> CreateBlobSink(
            string connectionString, 
            string containerName, 
            int queueLength, 
            ExportOptions exportOptions, 
            CancellationToken cancellationToken)
        {
            var outputContainer = await CreateContainerAsync(connectionString, containerName, true, cancellationToken).ConfigureAwait(false);
            var serializer = new JsonSerializer { Formatting = Formatting.None };
            var jsonBlobWriter = new JsonBlobWriter(outputContainer, serializer, exportOptions, Encoding.UTF8);
            var dataflowBlockOptions = new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = queueLength,
                MaxDegreeOfParallelism = DegreeOfParallelism
            };
            var blobSink = new ActionBlock<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> logs)>(async _ =>
                {
                    try
                    {
                        await jsonBlobWriter.WriteAsync(_.Timestamp, _.logs, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Log.Error(exception, "Error writing logs to Azure storage container {Container}", outputContainer.Uri);
                    }
                },
                dataflowBlockOptions);
            return blobSink;
        }

        private static async Task<CloudBlobContainer> CreateContainerAsync(
            string connectionString, 
            string containerName, 
            bool createIfNotExists, 
            CancellationToken cancellationToken)
        {
            // This is probably a storage account connection string
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudBlobClient();

            var container = client.GetContainerReference(containerName);
            if (createIfNotExists)
            {
                var blobRequestOptions = new BlobRequestOptions();
                var operationContext = new OperationContext();
                await container.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Container, blobRequestOptions, operationContext, cancellationToken).ConfigureAwait(false);
            }

            return container;
        }

        private static Task<CloudBlobContainer> CreateInputContainerAsync(string connectionString, CancellationToken cancellationToken)
                            => CreateContainerAsync(connectionString, InputContainerName, false, cancellationToken);

        #endregion Azure BLOB features
    }
}
