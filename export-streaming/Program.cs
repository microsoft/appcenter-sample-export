using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.HashFunction.MurmurHash;
using System.Diagnostics;
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

namespace AppCenter.Samples
{
    internal static class Program
    {
        private const int BlobQueueLength = 10;
        private const string InputConnectionString = "<AppCenter export storage account>";
        private const string InputContainerName = "archive";
        private const string OutputConnectionString = "<output storage account connection string>";
        private const string OutputContainerName = "<output container name>";
        private const string OutputEventHubsConnectionString = "<event hub connection string>";
        private const int PartitionQueueLength = 100;

        private static int DegreeOfParallelism { get; } = Environment.ProcessorCount;

        private static TimeSpan StreamingTime { get; } = TimeSpan.FromMinutes(30);

        public static async Task Main()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            using (var cts = new CancellationTokenSource(StreamingTime))
            {
                var cancellationToken = cts.Token;
                var start = new DateTimeOffset(2018, 6, 1, 0, 0, 0, TimeSpan.Zero);
                var exportOptions = new ExportOptions();

                // Setup data source
                var inputContainer = await CreateInputContainerAsync(InputConnectionString, cts.Token).ConfigureAwait(false);

                // Create the core observable
                var observable = inputContainer.CreateExport(start, exportOptions).Publish();

                //// Setup BLOB output
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
                    .GroupBy(deviceLog => GetPartition(deviceLog, (uint) eventHubSinks.Length))
                    .SelectMany(partition =>
                        partition
                            .Buffer(TimeSpan.FromSeconds(1), 50)
                            .Where(batch => batch.Count > 0)
                            .Select(batch => (PartitionId: (int) partition.Key, Batch: batch)))
                    .Subscribe(
                        onNext: _ =>
                        {
                            try
                            {
                                eventHubSinks[_.PartitionId].Post(_.Batch);
                            }
                            catch (Exception exception)
                            {
                                Log.Error(exception, "Unexpected error posting to Event Hub sender");
                            }
                        },
                        onError: e => Log.Error(e, "Event Hubs target stream completed in error"),
                        onCompleted: () => Log.Information("Event Hubs target stream completed"),
                        token: cancellationToken);

                // Setup console output
                observable
                    .Subscribe(
                        onNext: _ => WriteLine("{0}: {1} log(s)", _.Timestamp, _.Logs.Count),
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

        private static async Task<ImmutableArray<ITargetBlock<IList<DeviceLog>>>> CreateEventHubsSink(string connectionString, int queueLength)
        {
            var client = EventHubClient.CreateFromConnectionString(connectionString);
            var information = await client.GetRuntimeInformationAsync().ConfigureAwait(false);
            var senderOptions = new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = queueLength,
                EnsureOrdered = true,
                SingleProducerConstrained = true
            };
            return information.PartitionIds
                .Select(partitionId =>
                {
                    var sender = client.CreatePartitionSender(partitionId);

                    // A block that sends batches to the Event Hub partition
                    var sendBlock = new ActionBlock<IList<DeviceLog>>(async batch =>
                        {
                            var timer = Stopwatch.StartNew();
                            var json = JsonConvert.SerializeObject(batch, Formatting.None);
                            var bytes = Encoding.UTF8.GetBytes(json);
                            try
                            {
                                using (var eventData = new EventData(bytes))
                                    await sender.SendAsync(eventData).ConfigureAwait(false);
                                Log.Information("Sent a {Bytes} byte device log batch to Event Hub partition {PartitionId} in {EventHubPartitionSendTime}", bytes.Length, partitionId, timer.Elapsed);
                            }
                            catch (Exception exception)
                            {
                                // Swallow errors
                                Log.Error(exception, "Error sending {Bytes} byte device log batch to Event Hub partition {PartitionId}", bytes.Length, partitionId);
                            }
                        },
                        senderOptions);

                    return (ITargetBlock<IList<DeviceLog>>) sendBlock;
                })
                .ToImmutableArray();
        }

        private static int GetMurmurHash(Guid guid)
        {
            var bytes = guid.ToByteArray();
            var hash = HashFunc.ComputeHash(bytes).Hash;
            return BitConverter.ToInt32(hash, 0);
        }

        private static uint GetPartition(DeviceLog deviceLog, uint partitionCount)
        {
            if (!Guid.TryParse(deviceLog.InstallId, out var installId))
                return 0;
            unchecked
            {
                var hash = (uint) GetMurmurHash(installId);
                return hash % partitionCount;
            }
        }

        #endregion Event Hubs features

        #region Azure BLOB features

        private static async Task<ActionBlock<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)>> CreateBlobSink(
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
            var blobSink = new ActionBlock<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)>(async _ =>
                {
                    var timer = Stopwatch.StartNew();
                    var (timestamp, logs) = _;
                    try
                    {
                        await jsonBlobWriter.WriteAsync(timestamp, logs, cancellationToken).ConfigureAwait(false);
                        Log.Information("Wrote {LogCount} log(s) to Azure BLOB container {ContainerName} in {BlobWriteTime}", logs.Count, containerName, timer.Elapsed);
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
